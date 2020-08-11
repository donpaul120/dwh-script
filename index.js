/*
 * @author Paul Okeke
 * @date 23/06/2020
 * @since 1.0
 */
require('dotenv').config();
const moment = require('moment');
const {updateBatch, insertBatch, getMeterNumbers} = require('./Utils');

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

const tableName = "DMR2";
const dateFormat = "YYYY-MM-DD";

const log = (msg, ...extras) => console.log(msg, ...extras);

async function getDMRForMeter(meterNumber) {
    return knex.table(tableName)
        .where("DMR2_METER_NO", meterNumber)
        .select(['DMR2_DATE', 'DMR2_PAR'])
        .orderBy('DMR2_DATE', 'ASC');
}

async function processMeterNumbers(meterNumbers = []) {
    const lastDate = moment(new Date()).subtract(2, "day").format(dateFormat);

    for (let meterNo of meterNumbers) {
        const md = await getDMRForMeter(meterNo);

        const inserts = [];
        const updates = [];

        let tempDate = moment(md[0]['DMR2_DATE']).format(dateFormat);
        let tempReading = 0;
        let i = 0;

        while (tempDate < lastDate) {
            const dmr = md[i];
            const dmrDate = (dmr) ? moment(dmr['DMR2_DATE']).format(dateFormat) : lastDate;

            log(meterNo, `TempDate: ${tempDate}, DMRDate: ${dmrDate}`, tempReading);

            if (!dmr || dmrDate !== tempDate) {
                while (tempDate < dmrDate) {
                    inserts.push({
                        "DMR2_METER_NO": meterNo,
                        "DMR2_DATE": tempDate,
                        "DMR2_PAR": tempReading,
                        "DMR2_SOURCE": "DWH",
                        "DMR2_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                    });
                    tempDate = moment(tempDate).add(1, "day").format(dateFormat);
                }
                tempReading = (dmr) ? dmr['DMR2_PAR'] || tempReading : tempReading;
                continue;
            } else if (dmr['DMR2_PAR'] == null) {
                const update = {"DMR2_PAR": tempReading};
                const where = {"DMR2_METER_NO": meterNo, "DMR2_DATE": tempDate};
                updates.push({update, where});
            } else {
                tempReading = dmr['DMR2_PAR'];
            }

            tempDate = moment(tempDate).add(1, "day").format(dateFormat);
            i++;
        }

        insertBatch(knex, tableName, inserts);
        updateBatch(knex, tableName, updates).then();
    }
    return true;
}

(async function () {
    const startTime = Date.now();
    const totalRecords = (await knex.table(tableName).countDistinct('DMR2_METER_NO as count')).shift().count;

    log("TotalNumberOfRecords:", totalRecords);

    const noPerBatch = 1000;
    let index = 0;

    while (index < totalRecords) {
        let offset = index * noPerBatch;
        const meterNumbers = await getMeterNumbers(knex, tableName, 'DMR2_METER_NO', offset,  noPerBatch);
        await processMeterNumbers(meterNumbers);
        index++;

        if (index >= totalRecords) {
            console.log("Elapsed Time::", Date.now() - startTime);
        }
    }

})();