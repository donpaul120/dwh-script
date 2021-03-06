const fs = require('fs');
const util = require('util');
const stdout = process.stdout;
const logFile = fs.createWriteStream('combine.log', {flags: 'w'});
let insertedRecords = 0;
let nUpdateRecords = 0;

function log(msg, ...extras) {
    logFile.write(util.format(msg, extras) + '\n');
    stdout.write(util.format(msg, extras) + '\n');
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateBatch(knex, tableName, updateRecords) {
    if (!updateRecords.length) return;

    let batchCounter = 0;

    const doUpdate = async (record) => {
        log(`Updating Record: ${JSON.stringify(record)}`);
        return knex.table(tableName).update(record.update).where(record.where)
            .then(res => {
                nUpdateRecords++;
                log(`Updated Records = ${nUpdateRecords}`);
            })
            .catch(async (err) => {
                log(err);
                await sleep(10000);
                //TODO(check error type before retrying)
                await doUpdate(record);
            });
    };

    for (let record of updateRecords) {
        //let's sleep after 50 records
        if (batchCounter % 50 === 0) {
            log("Going to sleep(10000ms) now for updates");
            await sleep(10000);
            log("Resuming updates");
        }

        await doUpdate(record);

        batchCounter++;
    }
}

function insertBatch(knex, tableName, records) {
    if (!records.length) return;
    knex.batchInsert(tableName, records, 50).then(ids => {
        insertedRecords += records.length;
        // log(`Records Inserted: ${JSON.stringify(records)}`);
        log(`Completed Batch Insert of ${records.length} DMR records ${ids}`);
        log(`Processed Records = ${insertedRecords}`);
    }).catch(error => {
        log(`Failed Records : ${JSON.stringify(records)}`);
        log(`Failed Records Size: ${records.length}`);
        log(`Batch Insert Error : ${error}`);
    });
}


async function getMeterNumbers(knex, tableName, column, offset, limit) {
    const results = await knex.table(tableName)
        .min('DMR_DATE as MIN_DATE')
        .max('DMR_DATE as MAX_DATE')
        .select(column)
        .orderBy(`${tableName}.${column}`)
        .groupBy(`${tableName}.${column}`)
        .limit(limit)
        .offset(offset);

    return results.filter(i => i['DMR_METER_NO'] !== '0');
}

function lastMeterRecord(args) {
    return args.slice(args.length - 1).shift();
}

module.exports = {
    updateBatch, insertBatch, getMeterNumbers, log, lastMeterRecord
};
