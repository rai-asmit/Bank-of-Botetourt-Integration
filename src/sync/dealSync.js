'use strict';

const {
  batchSearchDeals,
  batchCreateDeals,
  batchUpdateDeals,
  findDealByCompositeKey,
} = require('../services/dealService');
const { chunk, runBatches } = require('../services/hubspotClient');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');

// Generic deal sync — works for DDA, CD, LNA, SDA
async function syncAccountDeals(accountMap, contactIdMap, buildPropsFn, label, { batchSize, concurrency, searchConcurrency, runId }) {
  if (accountMap.size === 0) {
    logger.info(`${label}: no rows to process`);
    return { created: 0, updated: 0 };
  }

  const hashes = [...accountMap.keys()];
  logger.info(`${label}: searching existing deals for ${hashes.length} customer hashes (batch)...`);
  const existingDealsMap = await batchSearchDeals(hashes, batchSize, searchConcurrency);
  logger.info(`${label}: found deals for ${existingDealsMap.size} customers`);

  const dealsToUpdate = [];
  const dealsToCreate = [];

  for (const [hash, rows] of accountMap) {
    const contactId = contactIdMap.get(hash);
    if (!contactId) continue;

    const existingDeals = existingDealsMap.get(hash) || [];

    for (const row of rows) {
      if (!row.dateOpened) {
        logger.warn(`${label}: skipping row (hash=${hash}) — dateOpened is missing, cannot form composite key`);
        continue;
      }
      if (!row.accountLast4) {
        logger.warn(`${label}: skipping row (hash=${hash}) — accountLast4 is empty, cannot form composite key`);
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

  if (dealsToUpdate.length > 0) {
    logger.info(`${label}: updating ${dealsToUpdate.length} existing deals (batch)...`);
    await runBatches(chunk(dealsToUpdate, batchSize), concurrency, (batch) => batchUpdateDeals(batch));
    for (const d of dealsToUpdate) {
      fileLogger.dealUpdated(runId, { hash: d.properties.taxidhashed, dealId: d.id });
    }
    logger.info(`${label}: updated ${dealsToUpdate.length} deals`);
  }

  if (dealsToCreate.length > 0) {
    logger.info(`${label}: creating ${dealsToCreate.length} new deals (batch)...`);
    const createResults = await runBatches(chunk(dealsToCreate, batchSize), concurrency, (batch) => batchCreateDeals(batch));

    const createdDealIdMap = new Map();
    for (const batchResult of createResults) {
      for (const deal of batchResult) {
        const key = `${deal.properties.taxidhashed}|${deal.properties.date_opened}`;
        createdDealIdMap.set(key, deal.id);
      }
    }
    for (const d of dealsToCreate) {
      const key = `${d.properties.taxidhashed}|${d.properties.date_opened}`;
      fileLogger.dealCreated(runId, {
        hash:       d.properties.taxidhashed,
        contactId:  d.contactId,
        dealId:     createdDealIdMap.get(key),
        dealname:   d.properties.dealname,
        dateOpened: d.properties.date_opened,
      });
    }
    logger.info(`${label}: created ${dealsToCreate.length} deals`);
  }

  return { created: dealsToCreate.length, updated: dealsToUpdate.length };
}

module.exports = { syncAccountDeals };
