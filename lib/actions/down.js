const _ = require("lodash");
const { promisify } = require("util");
const fnArgs = require("fn-args");

const status = require("./status");
const config = require("../env/config");
const migrationsDir = require("../env/migrationsDir");
const hasCallback = require("../utils/has-callback");
const lock = require("../utils/lock");

module.exports = async (db, client) => {
  const downgraded = [];
  const statusItems = await status(db);
  const appliedItems = statusItems.filter(item => item.appliedAt !== "PENDING");
  const lastAppliedItem = _.last(appliedItems);

  if (await lock.exist(db)) {
    throw new Error("Could not migrate down, a lock is in place.");
  }

  const lockObtained = await lock.activate(db);
  if (!lockObtained) {
    throw new Error(`Could not obtain a lock`);
  }

  if (lastAppliedItem) {
    try {
      const migration = await migrationsDir.loadMigration(lastAppliedItem.fileName);
      const down = hasCallback(migration.down) ? promisify(migration.down) : migration.down;

      if (hasCallback(migration.down) && fnArgs(migration.down).length < 3) {
        // support old callback-based migrations prior to migrate-mongo 7.x.x
        await down(db);
      } else {
        await down(db, client);
      }

    } catch (err) {
      await lock.clear(db);
      throw new Error(
        `Could not migrate down ${lastAppliedItem.fileName}: ${err.message}`
      );
    }
    const { changelogCollectionName } = await config.read();
    const changelogCollection = db.collection(changelogCollectionName);
    try {
      await changelogCollection.deleteOne({ fileName: lastAppliedItem.fileName });
      downgraded.push(lastAppliedItem.fileName);
    } catch (err) {
      throw new Error(`Could not update changelog: ${err.message}`);
    }
  }

  await lock.clear(db);
  return downgraded;
};
