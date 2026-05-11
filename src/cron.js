'use strict';

const cron           = require('node-cron');
const { triggerSync } = require('./sync/runner');
const logger          = require('./utils/logger');
const { config }      = require('./config/config');

function scheduleSyncCron() {
  const schedule = config.syncCron;
  cron.schedule(schedule, async () => {
    logger.info(`Cron: starting scheduled sync (${schedule})`);
    await triggerSync();
  });
  logger.info(`Sync cron scheduled: ${schedule}`);
}

module.exports = { scheduleSyncCron };
