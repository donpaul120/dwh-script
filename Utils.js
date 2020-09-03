const fs = require('fs');
const util = require('util');
const stdout = process.stdout;
const logFile = fs.createWriteStream('combine.log', {flags:'w'});
let insertedRecords = 0;
let updateRecords = 0;

function log(msg){
    logFile.write(util.format(JSON.stringify(msg)) + '\n');
    stdout.write(util.format(JSON.stringify(msg)) + '\n');
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateBatch(knex, tableName, updateRecords) {
    if (!updateRecords.length) return;

    let batchCounter = 0;

    const doUpdate = async (record) => {
        log(`Updating Record: ${record}`);
        return knex.table(tableName).update(record.update).where(record.where)
            .then(res => {
                updateRecords++;
                log(`Updated Records = ${updateRecords}`);
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
    knex.batchInsert(tableName, records).then(ids => {
        insertedRecords++;
        log(`Records Inserted: ${records}`);
        log(`Completed Batch Insert of ${records.length} DMR records ${ids}`);
        log(`Processed Records = ${insertedRecords}`);
    }).catch(error => {
        log(`Batch Insert Error : ${error}`);
    });
}


async function getMeterNumbers(knex, tableName, column, offset, limit) {
    const results = await knex.table(tableName).distinct(column).select([column])
        .limit(limit)
        .offset(offset);

    return results.map(row => {
        const meterNo = row[column];
        return (Array.isArray(meterNo)) ? meterNo.shift() : meterNo;
    }).filter(i => i !== '0');
}

module.exports = {
    updateBatch, insertBatch, getMeterNumbers, log
};