const config = require('../env/config');

async function getLockCollection(db) {
    const { lockCollectionName, lockTtl } = await config.read();
    if (lockTtl <= 0) {
        return null;
    }

    const lockCollection = db.collection(lockCollectionName);
    lockCollection.createIndex({ createdAt: 1 }, { expireAfterSeconds: lockTtl });
    return lockCollection;
}

async function exist(db) {
    const lockCollection = await getLockCollection(db);
    if (!lockCollection) {
        return false;
    }
    const foundLocks = await lockCollection.find({}).toArray();

    return foundLocks.length > 0;
}

async function activate(db) {
    const lockCollection = await getLockCollection(db);
    if (!lockCollection) {
        throw new Error('Cannot get lock collection')
    }
    // This command simultaneously checks if a lock exists and inserts one if no lock exists in a single atomic operation
    const { upsertedCount } = await lockCollection.update(
      { lock: true }, // Some static value to match on
      { $setOnInsert: { lock: true, createdAt: new Date() } }, // $setOnInsert will not update any existing locks
      { upsert: true }
    );
    return upsertedCount === 1;
}

async function clear(db) {
    const lockCollection = await getLockCollection(db);
    if (lockCollection) {
        await lockCollection.deleteMany({});
    }
}

module.exports = {
    exist,
    activate,
    clear,
}
