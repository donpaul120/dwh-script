async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateBatch(knex, tableName, updateRecords) {
    if (!updateRecords.length) return;

    let batchCounter = 0;

    const doUpdate = async (record) => {
        console.log("Updating Record", record);
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
            console.log("Going to sleep now for updates");
            await sleep(15000);
            console.log("Resuming updates");
        }

        await doUpdate(record);

        batchCounter++;
    }
}

function insertBatch(knex, tableName, records) {
    if (!records.length) return;
    knex.batchInsert(tableName, records).then(ids => {
        console.log(records);
        console.log(`Completed Batch Insert of ${records.length} DMR records`, ids);
    }).catch(error => {
        console.error(error);
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
    updateBatch, insertBatch, getMeterNumbers
};