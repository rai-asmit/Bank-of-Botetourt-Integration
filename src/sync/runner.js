'use strict';

const { runSync }          = require('./orchestrator');
const { fetchFilesFromSFTP } = require('../sftp/sftpFetcher');
const { cleanupFiles }     = require('../utils/cleanupFiles');
const logger               = require('../utils/logger');
const fileLogger           = require('../utils/fileLogger');

let syncInProgress = false;
let lastSyncResult = null;

async function triggerSync() {
  if (syncInProgress) {
    return { alreadyInProgress: true };
  }

  syncInProgress = true;
  const runId = fileLogger.startRun();
  logger.info(`Sync triggered — runId=${runId}`);
  fileLogger.syncStart(runId);

  const fetched = { cifPath: null, ddaPath: null };

  try {
    logger.info('Downloading files from SFTP server...');
    const downloaded = await fetchFilesFromSFTP();
    fetched.cifPath = downloaded.cifPath;
    fetched.ddaPath = downloaded.ddaPath;
    logger.info(`CIF: ${fetched.cifPath} | DDA: ${fetched.ddaPath}`);

    const stats = await runSync(runId, fetched);
    logger.info(`Sync finished: ${JSON.stringify(stats)}`);

    lastSyncResult = { runId, completedAt: new Date().toISOString(), success: true, stats };
    return { runId, stats };
  } catch (err) {
    logger.error(`Sync error: ${err.message}`);
    fileLogger.syncError(runId, err.message);
    lastSyncResult = { runId, completedAt: new Date().toISOString(), success: false, error: err.message };
    return { runId, error: err.message };
  } finally {
    await cleanupFiles([fetched.cifPath, fetched.ddaPath]);
    syncInProgress = false;
  }
}

function getSyncStatus() {
  return { syncInProgress, lastSyncResult };
}

module.exports = { triggerSync, getSyncStatus };
