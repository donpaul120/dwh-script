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
        max: 1000
    }
});

const tableName = "DMR";
const dateFormat = "YYYY-MM-DD";

const log = (msg, ...extras) => console.log(msg, ...extras);

async function getDMRForMeter(meterNumber) {
    return knex.table(tableName)
        .where("DMR_METER_NO", meterNumber)
        .select(['DMR_DATE', 'DMR_READING', 'DMR2_LAR', 'DMR2_CONS'])
        .orderBy('DMR_DATE', 'ASC');
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function processMeterNumbers(meterNumbers = []) {
    const lastDate = moment(new Date()).subtract(2, "day").format(dateFormat);

    for (let meterNo of meterNumbers) {
        const md = await getDMRForMeter(meterNo);

        const inserts = [];
        const updates = [];

        let tempDate = moment(md[0]['DMR_DATE']).format(dateFormat);
        let tempReading = 0, tempDrmLar = 0, tempDrmCons = 0;
        let i = 0;

        while (tempDate < lastDate) {
            const dmr = md[i];
            const dmrDate = (dmr) ? moment(dmr['DMR_DATE']).format(dateFormat) : lastDate;

            log(meterNo, `TempDate: ${tempDate}, DMRDate: ${dmrDate}`, tempReading);

            if (!dmr || dmrDate !== tempDate) {
                while (tempDate < dmrDate) {
                    inserts.push({
                        "DMR_METER_NO": meterNo,
                        "DMR_DATE": tempDate,
                        "DMR_READING": tempReading,
                        "DMR_SOURCE": "DWH",
                        "DMR2_LAR": tempReading,
                        "DMR2_CONS": 0,
                        "DMR_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                    });
                    tempDate = moment(tempDate).add(1, "day").format(dateFormat);
                }
                continue;
            } else if (dmr['DMR_READING'] == null || !dmr['DMR2_LAR'] || !dmr['DMR2_CONS']) {
                const reading = dmr['DMR_READING'] || tempReading;
                const consumption = reading - tempReading;
                const update = {"DMR_READING": reading, "DMR2_LAR":tempReading, "DMR2_CONS":consumption};
                const where = {"DMR_METER_NO": meterNo, "DMR_DATE": tempDate};
                updates.push({update, where});
            } else {
                tempReading = dmr['DMR_READING'];
                tempDrmLar = dmr['DMR2_LAR'];
                tempDrmCons = tempReading - tempDrmLar;
            }

            tempDate = moment(tempDate).add(1, "day").format(dateFormat);
            i++;
        }

        insertBatch(inserts);
        updateBatch(updates).then();
    }
    return true;
}

async function updateBatch(updateRecords) {
    if (!updateRecords.length) return;

    let batchCounter = 0;

    const doUpdate = async (record) => {
        log("Updating Record", record);
        return knex.table(tableName).update(record.update).where(record.where)
            .then(console.log)
            .catch(async (err) => {
                console.error(err);
                await sleep(10000);
                //TODO(check error type before retrying)
                await doUpdate(record);
            });
    };

    for (let record of updateRecords) {
        //let's sleep after 50 records
        if (batchCounter % 50 === 0) {
            log("Going to sleep now for updates");
            await sleep(15000);
            log("Resuming updates");
        }

        await doUpdate(record);

        batchCounter++;
    }
}

function insertBatch(records) {
    if (!records.length) return;
    knex.batchInsert(tableName, records).then(ids => {
        console.log(records);
        log(`Completed Batch Insert of ${records.length} DMR records`, ids);
    }).catch(error => {
        console.error(error);
    });
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


(async function () {
    const startTime = Date.now();
    const totalRecords = (await knex.table(tableName).countDistinct('DMR_METER_NO as count')).shift().count;

    log("TotalNumberOfRecords:", totalRecords);

    const noPerBatch = 1000;
    let index = 0;

    while (index < totalRecords) {
        let offset = index * noPerBatch;
        const meterNumbers = await getMeterNumbers(offset, noPerBatch);
        await processMeterNumbers(meterNumbers);
        index++;

        if (index >= totalRecords) {
            console.log("Elapsed Time::", Date.now() - startTime);
        }
    }

})();