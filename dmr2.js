/*
 * @author Paul Okeke
 * @date 23/06/2020
 * @since 1.0
 */
require('dotenv').config();
const {updateBatch, insertBatch, getMeterNumbers, log, lastMeterRecord} = require('./Utils');
const moment = require('moment');

//Database Connection
const knex = require('knex')({
    client: process.env.DB_CLIENT,
    connection: {
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        connectionTimeout: 3600000,
        requestTimeout: 3600000,
        options: {
            port: process.env.DB_PORT,
            enableArithAbort: false
        }
    },
    pool: {
        idleTimeoutMillis: 3600000,
        min: 2,
        max: 2000
    }
});

const tableName = "DMR";
const dateFormat = "YYYY-MM-DD";

// const log = (msg, ...extras) => console.log(msg, ...extras);
let meterNoCount = 0;

async function getDMRForMeter(...meterNumber) {
    return knex.table(tableName)
        .whereIn('DMR_METER_NO', meterNumber)
        .select(['DMR_METER_NO','DMR_DATE', 'DMR_PAR', 'DMR_LAR', 'DMR_CONS'])
        .orderBy('DMR_DATE', 'ASC');
}

/**
 * Process all meter numbers.
 *
 * The designed algorithm specified doesn't determine most of the flow within this
 * code as the algorithm doesn't put into exact consideration certain aspect of
 * database calls, locking, too many parameters etc.
 etc.
 *
 * @param meterNumbers
 * @returns {Promise<boolean>}
 */
async function processMeterNumbers(meterNumbers = []) {
    console.log(meterNumbers);
    //We need to fetch all the DMR for the meter numbers
    const allDmr = (await getDMRForMeter(...meterNumbers.map(m => m['DMR_METER_NO']))).reduce((acc, cValue)=> {
        console.log(cValue);
        acc[cValue['DMR_METER_NO']] = acc[cValue['DMR_METER_NO']] || [];
        acc[cValue['DMR_METER_NO']].push(cValue);
        return acc;
    }, Object.create(null));

    console.log(allDmr);

    for (let {DMR_METER_NO: meterNo, MIN_DATE: startDate, MAX_DATE: lastDate} of meterNumbers) {
        meterNoCount++;
        log(`MeterNoCount: ${meterNoCount},  MeterNumber: ${meterNo}`);
        const md = allDmr[meterNo];//await getDMRForMeter(meterNo).catch(err => log(`${meterNo}`, err));

        if (!md) continue;

        lastDate = moment(lastDate).format(dateFormat);

        const inserts = [];
        const updates = [];

        let tempDate = moment(startDate).format(dateFormat);
        let tempReading = 0, tempDrmLar = 0;
        let i = 0;

        while (tempDate < lastDate) {
            console.log(tempDate, lastDate);
            const dmr = md[i];
            const dmrDate = (dmr) ? moment(dmr['DMR_DATE']).format(dateFormat) : lastDate;

            const nextDmr = md[i + 1];
            const nextPar = (nextDmr && nextDmr['DMR_LAR']) ? nextDmr['DMR_LAR'] : tempReading;
            const nextParDate = (nextDmr) ? moment(nextDmr['DMR_DATE']).format(dateFormat) : null;

            log(meterNo, `TempDate: ${tempDate}, DMRDate: ${dmrDate}`, tempReading);

            if (!dmr || dmrDate !== tempDate) {
                while (tempDate < dmrDate) {
                    const dateDiff = moment(dmrDate).diff(tempDate, 'days');
                    const DMR_PAR = (dmr && dateDiff === 1) ? dmr['DMR_LAR'] : tempReading;
                    const DMR_CONS = DMR_PAR - tempReading;
                    inserts.push({
                        "DMR_METER_NO": meterNo,
                        "DMR_DATE": tempDate,
                        "DMR_LAR": tempReading,
                        DMR_PAR,
                        "DMR_SOURCE": "DWH",
                        DMR_CONS,
                        "DMR_DATE_CREATED": moment(tempDate).add(1, "day").format(dateFormat)
                    });
                    tempDate = moment(tempDate).add(1, "day").format(dateFormat);
                }
                tempReading = nextPar;
                continue;
            } else if (dmr['DMR_PAR'] == null || !dmr['DMR_LAR']) {
                const lar = dmr['DMR_LAR'] || tempReading;
                const DMR_PAR = (nextParDate && moment(nextParDate).diff(tempDate, 'days') === 0) ? nextPar : lar;
                const DMR_CONS = DMR_PAR - lar;
                const update = {DMR_PAR, "DMR_LAR": lar, DMR_CONS, "DMR_SOURCE": "DWH"};
                const where = {"DMR_METER_NO": meterNo, "DMR_DATE": tempDate};
                tempReading = DMR_PAR;
                updates.push({update, where});
            } else {
                tempReading = dmr['DMR_PAR'];
                tempDrmLar = dmr['DMR_LAR'];
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
    const totalRecords = (await knex.table(tableName).countDistinct('DMR_METER_NO as count')).shift().count;

    log("TotalNumberOfRecords:", totalRecords);

    const noPerBatch = 2000;
    let index = 0;

    while (index < totalRecords) {
        let offset = index * noPerBatch;
        log(`Current Index : ${index}`);
        const meterNumbers = await getMeterNumbers(knex, tableName, 'DMR_METER_NO', offset, noPerBatch);
        await processMeterNumbers(meterNumbers);
        index++;

        if (index >= totalRecords) {
            log("Elapsed Time::", Date.now() - startTime);
        }
    }

})();
