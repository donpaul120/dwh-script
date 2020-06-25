/*
 * @author Paul Okeke
 * @date 23/06/2020
 * @since 1.0
 */
require('dotenv').config();
const moment = require('moment');

//Database Connection
const knex = require('knex')({
    client: 'mssql',
    connection: {
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        options: {
            port: process.env.DB_PORT,
            enableArithAbort: false
        }
    }
});

const tableName = "DMR";
const dateFormat = "YYYY-MM-DD";

const log = (msg) => console.log(msg);

async function getMinDMRDate(meterNumber) {
    console.log(meterNumber);
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

async function correctDMRTable(meterNumbers = []) {
    const lastDate = moment(new Date()).subtract(2, "day").format(dateFormat);
    for (const meterNumber of meterNumbers) {
        let tempDate = null;
        let tempReading = null;

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
            const dmrRecord = (await knex.table(tableName).where({
                "DMR_METER_NO": meterNumber,
                "DMR_DATE": tempDate
            })).shift();

            console.log(dmrRecord);

            if (dmrRecord) {
                if (!dmrRecord['DMR_READING']) {
                    log(`UPDATING ${meterNumber} for ${tempDate}`);
                    await updateDMRRecord(meterNumber, tempDate, tempReading)
                } else {
                    tempReading = dmrRecord['DMR_READING'];
                    log(`NEXT DMR_READING : ${tempReading}`);
                }
            } else {
                log(`INSERTING NEW RECORD ${meterNumber} for ${tempDate}`);
                //We'll simply create a new record if we can get a dmr-reading for this date (tempDate)
                await insertDMRRecord({
                    "DMR_METER_NO": meterNumber,
                    "DMR_DATE": tempDate,
                    "DMR_READING": tempReading,
                    "DMR_SOURCE": "DWH",
                    "DMR_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                });
            }

            tempDate = moment(tempDate).add(1, "day").format(dateFormat);
        }
        while (tempDate < lastDate)
    }
    return process.exit(0);
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

(function () {
    knex.table(tableName).distinct("DMR_METER_NO")
        .select(['DMR_METER_NO'])
        .then(function (result) {
            const meterNumbers = result.map(row => row['DMR_METER_NO'].shift()).filter(i => i !== '0');
            console.log(meterNumbers);
            correctDMRTable(meterNumbers).then();
        }).catch(err => {
        console.error(err);
    })
})();