'use strict';

const {
  batchSearchDeals,
  batchCreateDeals,
  batchUpdateDeals,
  createSingleDeal,
  updateSingleDeal,
  findDealByCompositeKey,
} = require('../services/dealService');
const { resilientBatch } = require('../services/resilientBatch');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const deadLetter = require('../utils/deadLetter');

// Dedupe an array of {id, properties} by id — last write wins. Duplicate IDs
// in the same batch trigger a HubSpot 400 ("duplicate ids in batch"), so we
// collapse them here before calling the API.
function dedupeUpdates(updates, label, runId) {
  const seen = new Map();
  let duplicates = 0;
  for (const u of updates) {
    if (!u || !u.id) continue;
    if (seen.has(u.id)) duplicates++;
    seen.set(u.id, u);
  }
  if (duplicates > 0) {
    logger.warn(`${label}: collapsed ${duplicates} duplicate deal-id update(s) before batch call`);
    if (runId) deadLetter.write(runId, `dedupe_${label.toLowerCase()}`, {
      reason: 'duplicate deal ids merged',
      payload: { duplicates, kept: seen.size },
    });
  }
  return Array.from(seen.values());
}

// Dedupe creates by composite key (taxidhashed|date_opened|account_last_4). A
// repeated composite key inside a single create batch can cause HubSpot to
// reject the whole batch.
function dedupeCreates(creates, label, runId) {
  const seen = new Map();
  let duplicates = 0;
  for (const c of creates) {
    const p = c && c.properties;
    if (!p) continue;
    const key = `${p.taxidhashed || ''}|${p.date_opened || ''}|${p.account_last_4 || ''}`;
    if (seen.has(key)) duplicates++;
    seen.set(key, c);
  }
  if (duplicates > 0) {
    logger.warn(`${label}: collapsed ${duplicates} duplicate composite-key create(s) before batch call`);
    if (runId) deadLetter.write(runId, `dedupe_${label.toLowerCase()}`, {
      reason: 'duplicate composite keys merged',
      payload: { duplicates, kept: seen.size },
    });
  }
  return Array.from(seen.values());
}

// Generic deal sync — works for DDA, CD, LNA, SDA
async function syncAccountDeals(accountMap, contactIdMap, buildPropsFn, label, opts) {
  const { batchSize, concurrency, searchConcurrency, runId, state, phase } = opts;

  if (!accountMap || accountMap.size === 0) {
    logger.info(`${label}: no rows to process`);
    return { created: 0, updated: 0, failed: 0 };
  }

  const hashes = [...accountMap.keys()];
  logger.info(`${label}: searching existing deals for ${hashes.length} customer hashes (batch)...`);
  const existingDealsMap = await batchSearchDeals(hashes, batchSize, searchConcurrency);
  logger.info(`${label}: found deals for ${existingDealsMap.size} customers`);

  let dealsToUpdate = [];
  let dealsToCreate = [];

  for (const [hash, rows] of accountMap) {
    const contactId = contactIdMap.get(hash);
    if (!contactId) continue;

    const existingDeals = existingDealsMap.get(hash) || [];

    for (const row of rows) {
      if (!row.dateOpened) {
        logger.warn(`${label}: skipping row (hash=${hash}) — dateOpened is missing, cannot form composite key`);
        fileLogger.dealSkipped(runId, { hash, reason: 'missing_date_opened', type: label });
        continue;
      }
      if (!row.accountLast4) {
        logger.warn(`${label}: skipping row (hash=${hash}) — accountLast4 is empty, cannot form composite key`);
        fileLogger.dealSkipped(runId, { hash, reason: 'missing_account_last4', type: label });
        continue;
      }

      const properties = buildPropsFn(row, hash);
      const matching = findDealByCompositeKey(existingDeals, row.dateOpened, row.accountLast4);

      if (matching) {
        dealsToUpdate.push({ id: matching.id, properties });
      } else {
        dealsToCreate.push({ properties, contactId });
      }
    }
  }

  dealsToUpdate = dedupeUpdates(dealsToUpdate, label, runId);
  dealsToCreate = dedupeCreates(dealsToCreate, label, runId);

  let totalFailed = 0;

  if (dealsToUpdate.length > 0) {
    logger.info(`${label}: updating ${dealsToUpdate.length} existing deals (batch)...`);
    const { succeeded, failed } = await resilientBatch({
      items: dealsToUpdate,
      batchSize,
      concurrency,
      doBatch:  (batch) => batchUpdateDeals(batch),
      doSingle: (item)  => updateSingleDeal(item),
      runId,
      kind:  `deal_${label.toLowerCase()}_update`,
      label: `${label} update`,
      state,
      phase,
    });
    for (const d of dealsToUpdate) {
      fileLogger.dealUpdated(runId, { hash: d.properties.taxidhashed, dealId: d.id, type: label });
    }
    totalFailed += failed;
    logger.info(`${label}: updated ${succeeded} deals (${failed} dead-lettered)`);
  }

  if (dealsToCreate.length > 0) {
    logger.info(`${label}: creating ${dealsToCreate.length} new deals (batch)...`);
    const createdDealIdMap = new Map();
    const { results: createResults, succeeded, failed } = await resilientBatch({
      items: dealsToCreate,
      batchSize,
      concurrency,
      doBatch:  (batch) => batchCreateDeals(batch),
      doSingle: (item)  => createSingleDeal(item),
      runId,
      kind:  `deal_${label.toLowerCase()}_create`,
      label: `${label} create`,
      state,
      phase,
    });

    for (const deal of createResults) {
      if (!deal || !deal.properties) continue;
      const key = `${deal.properties.taxidhashed}|${deal.properties.date_opened}|${deal.properties.account_last_4 || ''}`;
      createdDealIdMap.set(key, deal.id);
    }
    for (const d of dealsToCreate) {
      const key = `${d.properties.taxidhashed}|${d.properties.date_opened}|${d.properties.account_last_4 || ''}`;
      const dealId = createdDealIdMap.get(key);
      if (!dealId) continue;
      fileLogger.dealCreated(runId, {
        hash:       d.properties.taxidhashed,
        contactId:  d.contactId,
        dealId,
        dealname:   d.properties.dealname,
        dateOpened: d.properties.date_opened,
        type:       label,
      });
    }
    totalFailed += failed;
    logger.info(`${label}: created ${succeeded} deals (${failed} dead-lettered)`);
  }

  return { created: dealsToCreate.length, updated: dealsToUpdate.length, failed: totalFailed };
}

module.exports = { syncAccountDeals };
