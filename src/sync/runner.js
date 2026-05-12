'use strict';

const { runSync }            = require('./orchestrator');
const { fetchFilesFromSFTP } = require('../sftp/sftpFetcher');
const { cleanupFiles }       = require('../utils/cleanupFiles');
const logger                 = require('../utils/logger');
const fileLogger             = require('../utils/fileLogger');
const checkpoint             = require('../state/checkpoint');

let syncInProgress = false;
let lastSyncResult = null;

async function triggerSync() {
  if (syncInProgress) {
    return { alreadyInProgress: true };
  }
  syncInProgress = true;

  let state = null;
  try {
    state = checkpoint.loadActiveRun();
    if (state) {
      logger.info(`Resuming run ${state.runId} at phase ${state.phase}`);
      fileLogger.syncResume(state.runId, state.phase);
    } else {
      const runId = fileLogger.startRun();
      state = checkpoint.createRun(runId);
      logger.info(`Sync triggered — runId=${runId}`);
      fileLogger.syncStart(runId);
    }

    // -----  DOWNLOAD phase  -----
    let fetched;
    if (checkpoint.isCompleted(state, 'DOWNLOAD')) {
      logger.info('DOWNLOAD already completed — reusing files from checkpoint');
      fetched = {
        cifPath: state.files.cif && state.files.cif.path,
        ddaPath: state.files.dda && state.files.dda.path,
        cdPath:  state.files.cd  && state.files.cd.path,
        lnaPath: state.files.lna && state.files.lna.path,
        sdaPath: state.files.sda && state.files.sda.path,
      };
      // sanity check — if disk no longer matches, fall through to a re-download
      if (!checkpoint.fileMatchesDisk(state.files.cif) || !checkpoint.fileMatchesDisk(state.files.dda)) {
        logger.warn('DOWNLOAD: checkpointed files missing on disk, re-downloading');
        checkpoint.markPhase(state, 'DOWNLOAD', { status: 'pending' });
      }
    }
    if (!checkpoint.isCompleted(state, 'DOWNLOAD')) {
      fileLogger.phaseStart(state.runId, 'DOWNLOAD');
      checkpoint.markPhase(state, 'DOWNLOAD', { status: 'in_progress' });
      logger.info('Downloading files from SFTP server...');
      fetched = await fetchFilesFromSFTP(state);
      checkpoint.setFiles(state, fetched);
      checkpoint.markPhase(state, 'DOWNLOAD', { status: 'completed' });
      fileLogger.phaseComplete(state.runId, 'DOWNLOAD', {});
      logger.info(`Files: CIF=${fetched.cifPath} | DDA=${fetched.ddaPath} | CD=${fetched.cdPath} | LNA=${fetched.lnaPath} | SDA=${fetched.sdaPath}`);
    }

    // -----  PARSE + sync phases  -----
    const stats = await runSync(state, fetched);
    logger.info(`Sync finished: ${JSON.stringify(stats)}`);

    const succeeded = checkpoint.isCompleted(state, 'COMPLETE');
    if (succeeded) {
      await cleanupFiles([
        state.files.cif && state.files.cif.path,
        state.files.dda && state.files.dda.path,
        state.files.cd  && state.files.cd.path,
        state.files.lna && state.files.lna.path,
        state.files.sda && state.files.sda.path,
      ]);
      checkpoint.clear(state.runId);
      logger.info('Cleanup complete — state cleared');
    } else {
      logger.warn('Sync did not reach COMPLETE — files & checkpoint preserved for next run');
    }

    lastSyncResult = {
      runId: state.runId,
      completedAt: new Date().toISOString(),
      success: succeeded,
      stats,
    };
    return { runId: state.runId, stats, success: succeeded };
  } catch (err) {
    // Only catastrophic I/O reaches here (SFTP totally unreachable, disk full,
    // etc.). State + on-disk files are preserved so the next tick can resume.
    logger.error(`Sync aborted (will resume next tick): ${err.message}`);
    if (state) fileLogger.syncError(state.runId, err.message);
    lastSyncResult = {
      runId: state && state.runId,
      completedAt: new Date().toISOString(),
      success: false,
      error: err.message,
    };
    return { runId: state && state.runId, error: err.message };
  } finally {
    syncInProgress = false;
  }
}

function getSyncStatus() {
  const active = checkpoint.loadActiveRun();
  return {
    syncInProgress,
    lastSyncResult,
    activeRun: active ? {
      runId: active.runId,
      phase: active.phase,
      phases: active.phases,
      stats:  active.stats,
      startedAt: active.startedAt,
    } : null,
  };
}

module.exports = { triggerSync, getSyncStatus };
