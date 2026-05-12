'use strict';

const { chunk, runBatches } = require('./hubspotClient');
const deadLetter = require('../utils/deadLetter');
const logger     = require('../utils/logger');
const checkpoint = require('../state/checkpoint');
const { summarize } = require('../utils/hubspotError');

/**
 * Run a batched operation that, on whole-batch failure, falls back to one-by-one
 * so a single poison record can't kill the rest. Permanently failing items are
 * written to the dead-letter file and the run keeps going.
 *
 * @param {object} opts
 * @param {Array}  opts.items        - items to process
 * @param {number} opts.batchSize    - batch size
 * @param {number} opts.concurrency  - parallel workers
 * @param {Function} opts.doBatch    - async (batch) => results[]
 * @param {Function} opts.doSingle   - async (item)  => result    (fallback)
 * @param {string} opts.runId
 * @param {string} opts.kind         - dead-letter kind tag
 * @param {string} opts.label        - log label
 * @param {object} [opts.state]      - if provided, batch progress is checkpointed
 * @param {string} [opts.phase]      - phase name in state.phases to update
 * @returns {{ results: Array, succeeded: number, failed: number }}
 */
async function resilientBatch({
  items, batchSize, concurrency, doBatch, doSingle,
  runId, kind, label, state, phase,
}) {
  if (!items || items.length === 0) {
    return { results: [], succeeded: 0, failed: 0 };
  }

  const batches = chunk(items, batchSize);
  const totalBatches = batches.length;
  const results = [];
  let doneBatches = 0;
  let succeeded   = 0;
  let failed      = 0;

  if (state && phase) {
    checkpoint.markPhase(state, phase, {
      status: 'in_progress', totalBatches, doneBatches, succeeded, failed,
    });
  }

  // Heartbeat so the user can see the run is alive while batches are being
  // retried one-by-one. Log a progress line every PROGRESS_EVERY batches, or
  // whenever PROGRESS_INTERVAL_MS has passed since the last line.
  const PROGRESS_EVERY = Math.max(1, Math.ceil(totalBatches / 20));
  const PROGRESS_INTERVAL_MS = 15000;
  let lastProgressAt = Date.now();
  logger.info(`▶️  [${label}] starting — ${items.length} records in ${totalBatches} batches of ${batchSize}`);

  await runBatches(batches, concurrency, async (batch) => {
    try {
      const r = await doBatch(batch);
      if (Array.isArray(r)) {
        results.push(...r);
        succeeded += r.length;
      } else {
        succeeded += batch.length;
      }
    } catch (err) {
      const { summary } = summarize(err);
      logger.warn(`⚠️  [${label}] batch of ${batch.length} rejected — ${summary} — retrying one-by-one`);
      for (const item of batch) {
        try {
          const r = await doSingle(item);
          if (r) results.push(r);
          succeeded++;
        } catch (singleErr) {
          failed++;
          const info = summarize(singleErr);
          logger.warn(`     [${label}] dead-letter — ${info.summary}`);
          deadLetter.write(runId, kind, {
            reason:  info.summary,
            error:   singleErr,
            payload: item,
          });
        }
      }
    } finally {
      doneBatches++;
      const now = Date.now();
      const dueByCount = doneBatches % PROGRESS_EVERY === 0;
      const dueByTime  = now - lastProgressAt >= PROGRESS_INTERVAL_MS;
      if (dueByCount || dueByTime || doneBatches === totalBatches) {
        const pct = Math.round((doneBatches / totalBatches) * 100);
        logger.info(`⏳ [${label}] ${doneBatches}/${totalBatches} batches (${pct}%) — ok: ${succeeded}, dead-letter: ${failed}`);
        lastProgressAt = now;
      }
      if (state && phase) {
        checkpoint.markPhase(state, phase, {
          status: 'in_progress', totalBatches, doneBatches, succeeded, failed,
        });
      }
    }
  });

  logger.info(`✅ [${label}] done — ok: ${succeeded}, dead-letter: ${failed}`);
  return { results, succeeded, failed };
}

module.exports = { resilientBatch };
