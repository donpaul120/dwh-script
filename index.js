/*
 * @author Paul Okeke
 * @date 23/06/2020
 * @since 1.0
 */
require('dotenv').config();
const moment = require('moment');

//Database Connection
const knex = require('knex')({
    client: process.env.DB_CLIENT,
    connection: {
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        options: {
            port: process.env.DB_PORT,
            enableArithAbort: false
        }
    },
    pool: {
        min: 2,
        max: 10
    }
});

const tableName = "DMR";
const dateFormat = "YYYY-MM-DD";
let lock = false;
let batchNo = 0;

const log = (msg) => console.log(msg);

async function getMinDMRDate(meterNumber) {
    const result = await knex.table(tableName)
        .where("DMR_METER_NO", meterNumber)
        .andWhere(function () {
            this.whereNotNull("DMR_READING")
        })
        .select(['DMR_DATE', 'DMR_READING'])
        .orderBy('DMR_DATE', 'ASC')
        .limit(1);
    return result.shift();
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function correctDMRTable(meterNumbers = []) {
    lock = true;
    const lastDate = moment(new Date()).subtract(2, "day").format(dateFormat);
    const batchStart = moment(new Date());

    for (const meterNumber of meterNumbers) {
        let tempDate = null;
        let tempReading = null;

        const singleMeterStart = moment(new Date())

        const result = await getMinDMRDate(meterNumber);

        log(result);

        if (!result) continue;

        /*
        * Set the tempDate for the initial reading and
        * set the initial meter reading as tempReading
        */
        tempDate = moment(result['DMR_DATE']).format(dateFormat);
        tempReading = result['DMR_READING'];

        do {
            log("Current Meter in batch: "+ batchNo + ` Time Taken: ${moment(new Date()).diff(singleMeterStart, 'minutes')}`);

            let hasError = false;
            const whereClause = {"DMR_METER_NO": meterNumber, "DMR_DATE": tempDate};

            log(meterNumber, tempDate);

            const dmrRecords = await knex.table(tableName).where(whereClause).catch(err => {
                hasError = true;
                log(err);
            });

            if (hasError) {
                await sleep(15000);
                continue;
            }

            const dmrRecord = dmrRecords.shift();

            if (dmrRecord) {
                if (!dmrRecord['DMR_READING']) {
                    log(`UPDATING ${meterNumber} for ${tempDate}`);
                    let updateError = false;
                    await updateDMRRecord(meterNumber, tempDate, tempReading).catch(err => {
                        updateError = true;
                        log(err);
                    });
                    if (updateError) {
                        await sleep(20000);
                        continue;
                    }
                } else {
                    tempReading = dmrRecord['DMR_READING'];
                    log(`${meterNumber} : NEXT DMR_READING : ${tempReading}`);
                }
            } else {
                log(`INSERTING NEW RECORD ${meterNumber} for ${tempDate}`);
                //We'll simply create a new record if we can get a dmr-reading for this date (tempDate)
                let insertError = false;
                await insertDMRRecord({
                    "DMR_METER_NO": meterNumber,
                    "DMR_DATE": tempDate,
                    "DMR_READING": tempReading,
                    "DMR_SOURCE": "DWH",
                    "DMR_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                }).catch((err) => {
                    log(err);
                    insertError = true;
                });

                if (insertError) {
                    await sleep(20000);
                    continue;
                }
            }

            tempDate = moment(tempDate).add(1, "day").format(dateFormat);
        }
        while (tempDate < lastDate)
    }
    log("Batch: "+ batchNo + ` Time Taken: ${moment(new Date()).diff(batchStart, 'minutes')}`);
    lock = false;
    return true;
}

async function updateDMRRecord(meterNo, date, dmrReading) {
    //TODO(how do we update the record since there are no IDS)
    return knex.table(tableName)
        .update({"DMR_READING": dmrReading})
        .where({"DMR_METER_NO": meterNo, "DMR_DATE": date});
}

async function insertDMRRecord(record) {
    return knex.table(tableName).insert(record);
}

async function getMeterNumbers(offset, limit) {
    const results = await knex.table(tableName).distinct("DMR_METER_NO").select(['DMR_METER_NO'])
        .limit(limit)
        .offset(offset);

    return results.map(row => {
        const meterNo = row['DMR_METER_NO'];
        return (Array.isArray(meterNo)) ? meterNo.shift() : meterNo;
    }).filter(i => i !== '0');
}


const hardCodedMeterNumbers = [
    201508003310,
    201505002871,
    201505002860,
    201505002875 ,
    201505002866 ,
    201512007840 ,
    201505002922 ,
    201505002854 ,
    201508003308 ,
    201505002916 ,
    201505002891 ,
    201505002924 ,
    201505002920 ,
    201512007949 ,
    201512008054 ,
    201505002900 ,
    201505002889 ,
    201505002897 ,
    201505000525 ,
    201512008102 ,
    201505002894 ,
    201512008004 ,
    201512008170 ,
    201512007694 ,
    201505000551 ,
    201505002913 ,
    201512008216 ,
    201505002845 ,
    201505002898 ,
    201505000647 ,
    201512008216 ,
    201505002848 ,
    201508003290 ,
    201505002923 ,
    201512007695 ,
    201512007702 ,
    201512007972 ,
    201512007699 ,
    201512008106 ,
    201512007828 ,
    201508003286 ,
    201512007842 ,
    201512007830 ,
    201512007846 ,
    201505002918 ,
    201512008058 ,
    201512008060 ,
    201505002892 ,
    201803012367 ,
    201505000743 ,
    201512008169 ,
    201505002842 ,
    201505002902 ,
    201512007703 ,
    201505002910 ,
    201505002869 ,
    201512007971 ,
    201505002904 ,
    201505002903 ,
    201512008200 ,
    201512008203 ,
    201512008205 ,
    201512008255 ,
    201512008257 ,
    201512008256 ,
    201803012384 ,
    201512007974 ,
    201505002867 ,
    201505002907 ,
    201505002868 ,
    201505002908 ,
    201505002873 ,
    201505002905 ,
    201505002865 ,
    201512007991 ,
    201512008089 ,
    201512007760 ,
    201512008011 ,
    201512008110 ,
    201505002847 ,
    201505002876 ,
    201505002912 ,
    201505002914 ,
    201512007966 ,
    201505002841 ,
    201505002849 ,
    201505000575 ,
    201505000584 ,
    201505000582 ,
    201505000574 ,
    201505000580 ,
    201505000578 ,
    201505000577 ,
    201505000573 ,
    201505000581 ,
];
(async function () {
    const totalRecords = hardCodedMeterNumbers.length;//(await knex.table(tableName).countDistinct('DMR_METER_NO as count')).shift().count;
    const noPerBatch = 1000;
    let index = 0;

    const interval = setInterval(async function () {
        if (!lock && index < totalRecords) {
            batchNo = index;
            let offset = index * noPerBatch;
            const meterNumbers = hardCodedMeterNumbers;//await getMeterNumbers(offset, noPerBatch);
            console.log(meterNumbers);
            index++;
            correctDMRTable(meterNumbers).then(() => {
                if (offset === totalRecords) {
                    clearInterval(interval);
                    process.exit(0);
                }
            });
        }
    }, 150000);
    console.log(totalRecords);
})();