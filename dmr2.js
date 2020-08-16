/*
 * @author Paul Okeke
 * @date 23/06/2020
 * @since 1.0
 */
require('dotenv').config();
const {updateBatch, insertBatch, getMeterNumbers} = require('./Utils');
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
        .where("DMR2_METER_NO", meterNumber)
        .select(['DMR2_DATE', 'DMR2_PAR', 'DMR2_LAR', 'DMR2_CONS'])
        .orderBy('DMR2_DATE', 'ASC');
}

async function processMeterNumbers(meterNumbers = [], maxDate) {
    const lastDate = moment(maxDate).subtract(1, "day").format(dateFormat);

    for (let meterNo of meterNumbers) {
        const md = await getDMRForMeter(meterNo);

        const inserts = [];
        const updates = [];

        let tempDate = moment(md[0]['DMR2_DATE']).format(dateFormat);
        let tempReading = 0, tempDrmLar = 0;
        let i = 0;

        while (tempDate < lastDate) {
            const dmr = md[i];
            const dmrDate = (dmr) ? moment(dmr['DMR2_DATE']).format(dateFormat) : lastDate;

            const nextDmr = md[i + 1];
            const nextPar = (nextDmr && nextDmr['DMR2_LAR']) ? nextDmr['DMR2_LAR'] : tempReading;
            const nextParDate = (nextDmr) ? moment(nextDmr['DMR2_DATE']).format(dateFormat) : null;

            log(meterNo, `TempDate: ${tempDate}, DMRDate: ${dmrDate}`, tempReading);

            if (!dmr || dmrDate !== tempDate) {
                while (tempDate < dmrDate) {
                    const dateDiff = moment(dmrDate).diff(tempDate, 'days');
                    const DMR2_PAR = (dmr && dateDiff === 1) ? dmr['DMR2_LAR'] : tempReading;
                    const DMR2_CONS = DMR2_PAR - tempReading;
                    inserts.push({
                        "DMR2_METER_NO": meterNo,
                        "DMR2_DATE": tempDate,
                        "DMR2_LAR": tempReading,
                        DMR2_PAR,
                        "DMR2_SOURCE": "DWH",
                        DMR2_CONS,
                        "DMR2_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                    });
                    tempDate = moment(tempDate).add(1, "day").format(dateFormat);
                }
                tempReading = nextPar;
                continue;
            } else if (dmr['DMR2_PAR'] == null || !dmr['DMR2_LAR']) {
                const lar = dmr['DMR2_LAR'] || tempReading;
                const DMR2_PAR = (nextParDate && moment(nextParDate).diff(tempDate, 'days') === 0) ? nextPar : lar;
                const DMR2_CONS = DMR2_PAR - lar;
                const update = {DMR2_PAR, "DMR2_LAR": lar, DMR2_CONS, "DMR2_SOURCE": "DWH"};
                const where = {"DMR2_METER_NO": meterNo, "DMR2_DATE": tempDate};
                tempReading = DMR2_PAR;
                updates.push({update, where});
            } else {
                tempReading = dmr['DMR2_PAR'];
                tempDrmLar = dmr['DMR2_LAR'];
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
    const endDate = (await knex.table(tableName).max('DMR2_DATE as end_date').where("DMR2_SOURCE", 'TMR')).shift()['end_date'];

    log("TotalNumberOfRecords:", totalRecords);

    const noPerBatch = 1000;
    let index = 0;

    while (index < totalRecords) {
        let offset = index * noPerBatch;
        const meterNumbers = await getMeterNumbers(knex, tableName, 'DMR2_METER_NO', offset, noPerBatch);
        await processMeterNumbers(meterNumbers, endDate);
        index++;

        if (index >= totalRecords) {
            console.log("Elapsed Time::", Date.now() - startTime);
        }
    }

})();