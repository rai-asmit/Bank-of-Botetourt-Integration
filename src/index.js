'use strict';

const { validate }        = require('./config/config');
const app                 = require('./server');
const logger              = require('./utils/logger');
const { scheduleSyncCron } = require('./cron');
const checkpoint          = require('./state/checkpoint');
const { triggerSync }     = require('./sync/runner');

// Fail fast if required env vars are missing
validate();

// Catch async errors that escaped try/catch
process.on('unhandledRejection', (reason) => {
  logger.error(`Unhandled promise rejection: ${reason}`);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  logger.error(`Uncaught exception: ${err.message}`);
  process.exit(1);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`);
  logger.info('POST /sync         — trigger a sync run');
  logger.info('GET  /sync/status  — check if sync is in progress');
  logger.info('GET  /health       — health check');
  scheduleSyncCron();

  // If a previous run crashed mid-flight, resume it now instead of waiting
  // for the next cron tick.
  const pending = checkpoint.loadActiveRun();
  if (pending) {
    logger.info(`Found unfinished run ${pending.runId} at phase ${pending.phase} — resuming on startup`);
    triggerSync().catch((err) => logger.error(`Resume-on-boot failed: ${err.message}`));
  }
});
