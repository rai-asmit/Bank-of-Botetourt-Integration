'use strict';

const fs = require('fs');
const path = require('path');
const { Router } = require('express');
const { runSync } = require('../sync/orchestrator');
const { fetchFilesFromSFTP } = require('../sftp/sftpFetcher');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const { cleanupFiles } = require('../utils/cleanupFiles');

const router = Router();

let syncInProgress = false;
let lastSyncResult = null; // { runId, completedAt, success, stats?, error? }


router.post('/', async (_req, res) => {
  if (syncInProgress) {
    return res.status(409).json({ error: 'A sync is already in progress. Try again when it finishes.' });
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
    return res.status(200).json({ runId, stats });
  } catch (err) {
    logger.error(`Sync error: ${err.message}`);
    fileLogger.syncError(runId, err.message);
    lastSyncResult = { runId, completedAt: new Date().toISOString(), success: false, error: err.message };
    return res.status(500).json({ error: err.message, runId });
  } finally {
    await cleanupFiles([fetched.cifPath, fetched.ddaPath]);
    syncInProgress = false;
  }
});


router.get('/status', (req, res) => {
  res.json({ syncInProgress, lastSyncResult });
});


module.exports = router;
