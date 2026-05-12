'use strict';

const cron           = require('node-cron');
const { triggerSync } = require('./sync/runner');
const logger          = require('./utils/logger');
const { config }      = require('./config/config');

function scheduleSyncCron() {
  const schedule = config.syncCron;
  const timezone = config.syncTimezone;
  cron.schedule(schedule, async () => {
    logger.info(`Cron: starting scheduled sync (${schedule} ${timezone})`);
    await triggerSync();
  }, { timezone });
  logger.info(`Sync cron scheduled: ${schedule} (${timezone})`);
}

module.exports = { scheduleSyncCron };
