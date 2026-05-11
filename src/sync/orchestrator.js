'use strict';

const { parseCifFile }  = require('../parsers/cifParser');
const { parseDdaFile }  = require('../parsers/ddaParser');
const { parseCdFile }   = require('../parsers/cdParser');
const { parseLnaFile }  = require('../parsers/lnaParser');
const { parseSdaFile }  = require('../parsers/sdaParser');
const {
  searchContactsByHashes,
  searchContactsByEmails,
  batchCreateContacts,
  batchUpdateContacts,
  buildContactProperties,
} = require('../services/contactService');
const {
  searchDealsByHashes,
  batchCreateDeals,
  batchUpdateDeals,
  findDealByCompositeKey,
  buildDealProperties,
  buildCdDealProperties,
  buildLnaDealProperties,
  buildSdaDealProperties,
} = require('../services/dealService');
const { chunk, runBatches } = require('../services/hubspotClient');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const { config } = require('../config/config');


async function runSync(runId, { cifPath, ddaPath, cdPath, lnaPath, sdaPath }) {
  if (!cifPath) throw new Error('runSync: cifPath is required');
  if (!ddaPath) throw new Error('runSync: ddaPath is required');
  //batch size and concurrency from config.api
  const { batchSize, concurrency, searchConcurrency } = config.api;
  //measure sync time
  const startTimeMs = Date.now();
  logger.info('=== Starting HubSpot sync (batch mode) ===');

  //Parse files 
  //contact mapping
  logger.info(`Parsing CIF file: ${cifPath}`);
  const cifRows = parseCifFile(cifPath);
  logger.info(`CIF rows loaded: ${cifRows.length}`);

  //deals mapping
  logger.info(`Parsing DDA file: ${ddaPath}`);
  const ddaMap = parseDdaFile(ddaPath);
  logger.info(`DDA unique customers loaded: ${ddaMap.size}`);

  // remove duplicate
  const uniqueContacts = deduplicateCifRows(cifRows);
  //no email skip
  const validContacts = uniqueContacts.filter((c) => c.email);
  //skip count
  const skippedCount = uniqueContacts.length - validContacts.length;
  //summary log
  logger.info(`Unique contacts: ${uniqueContacts.length} total, ${skippedCount} skipped (no email), ${validContacts.length} to sync`);

  // log every skipped contact into contacts.log
  for (const c of uniqueContacts) {
    if (!c.email) fileLogger.contactSkipped(runId, { hash: c.taxIdHashed, reason: 'no_email' });
  }

  const stats = {
    contactsCreated: 0,
    contactsUpdated: 0,
    contactsSkipped: skippedCount,
    dealsCreated: 0,
    dealsUpdated: 0,
  };

  if (validContacts.length === 0) {
    logger.warn('No valid contacts to sync.');
    return stats;
  }

  logger.info('Searching HubSpot for existing contacts (batch)...');
  // takeout all hashId
  const allHashes = validContacts.map((c) => c.taxIdHashed);
  // search in hubspot
  const existingContactsMap = await batchSearchContacts(allHashes, batchSize, searchConcurrency);
  logger.info(`Found ${existingContactsMap.size} existing contacts in HubSpot by taxidhashed`);
  // not found through hashedId
  const notFoundByHash = validContacts.filter((c) => !existingContactsMap.has(c.taxIdHashed));
  //search by email
  if (notFoundByHash.length > 0) {
    logger.info(`Searching ${notFoundByHash.length} contacts by email (fallback for missing taxidhashed)...`);
    const emailMap = await batchSearchContactsByEmail(notFoundByHash, batchSize, searchConcurrency);
    for (const contact of notFoundByHash) {
      const found = emailMap.get(contact.email.toLowerCase());
      if (!found) continue;

      const existingHash = found.properties && found.properties.taxidhashed;
      if (!existingHash) {
        // Migration case: contact exists by email but has no taxidhashed yet → update it.
        existingContactsMap.set(contact.taxIdHashed, found);
      }
      // If existingHash is already set (and is a different hash), this is a new Tax ID
      // (Route 3) — do not promote to update; let it fall through to toCreate below.
    }
    logger.info(`Found ${existingContactsMap.size} total existing contacts after email fallback`);
  }

  // create
  const toCreate = validContacts.filter((c) => !existingContactsMap.has(c.taxIdHashed));
  // update
  const toUpdate = validContacts.filter((c) => existingContactsMap.has(c.taxIdHashed));

  // Batch-update existing contacts 
  if (toUpdate.length > 0) {
    logger.info(`Updating ${toUpdate.length} existing contacts (batch)...`);
    //format for api
    const updateInputs = toUpdate.map((c) => ({
      id: existingContactsMap.get(c.taxIdHashed).id,
      properties: buildContactProperties(c),
    }));

    await runBatches(chunk(updateInputs, batchSize), concurrency, (batch) => {
      return batchUpdateContacts(batch);
    });

    stats.contactsUpdated = toUpdate.length;
    logger.info(`Updated ${toUpdate.length} contacts`);

    for (const c of toUpdate) {
      fileLogger.contactUpdated(runId, {
        email:     c.email,
        hash:      c.taxIdHashed,
        hubspotId: existingContactsMap.get(c.taxIdHashed).id,
      });
    }
  }

  //Create new contacts 
  const contactIdMap = new Map();
  for (const [hash, contact] of existingContactsMap) {
    contactIdMap.set(hash, contact.id);
  }

  if (toCreate.length > 0) {
    logger.info(`Creating ${toCreate.length} new contacts (batch)...`);
    const createPropsList = toCreate.map((c) => buildContactProperties(c));
    const emailToHash = new Map();
    for (const c of toCreate) {
      if (c.email) emailToHash.set(c.email.toLowerCase(), c.taxIdHashed);
    }

    const createResults = await runBatches(
      chunk(createPropsList, batchSize),
      concurrency,
      (batch) => batchCreateContacts(batch)
    );

    let trueCreates = 0;
    let conflictUpdates = 0;
    for (const batchResult of createResults) {
      for (const created of batchResult) {
        const email = created.properties && created.properties.email;
        const hash = email && emailToHash.get(email.toLowerCase());
        if (hash) {
          contactIdMap.set(hash, created.id);
          if (created._conflictResolved) {
            conflictUpdates++;
          } else {
            trueCreates++;
          }
        }
      }
    }

    stats.contactsCreated = trueCreates;
    stats.contactsUpdated += conflictUpdates;
    if (trueCreates > 0)     logger.info(`Created ${trueCreates} contacts`);
    if (conflictUpdates > 0) logger.info(`Updated ${conflictUpdates} contacts (email conflict — existing contact updated with new Tax ID hash)`);

    for (const c of toCreate) {
      const hubspotId = contactIdMap.get(c.taxIdHashed);
      if (!hubspotId) continue;
      if (c._conflictResolved) {
        fileLogger.contactUpdated(runId, { email: c.email, hash: c.taxIdHashed, hubspotId });
      } else {
        fileLogger.contactCreated(runId, { email: c.email, hash: c.taxIdHashed, hubspotId });
      }
    }
  }

  // DDA deals
  const ddaResult = await syncAccountDeals(
    ddaMap, contactIdMap, buildDealProperties, 'DDA',
    { batchSize, concurrency, searchConcurrency, runId }
  );
  stats.dealsCreated += ddaResult.created;
  stats.dealsUpdated += ddaResult.updated;

  // CD deals
  let cdMap = new Map();
  if (cdPath) {
    logger.info(`Parsing CD file: ${cdPath}`);
    cdMap = parseCdFile(cdPath);
    logger.info(`CD unique customers loaded: ${cdMap.size}`);
  }

  const cdResult = await syncAccountDeals(
    cdMap, contactIdMap, buildCdDealProperties, 'CD',
    { batchSize, concurrency, searchConcurrency, runId }
  );
  stats.dealsCreated += cdResult.created;
  stats.dealsUpdated += cdResult.updated;

  // LNA deals
  let lnaMap = new Map();
  if (lnaPath) {
    logger.info(`Parsing LNA file: ${lnaPath}`);
    lnaMap = parseLnaFile(lnaPath);
    logger.info(`LNA unique customers loaded: ${lnaMap.size}`);
  }

  const lnaResult = await syncAccountDeals(
    lnaMap, contactIdMap, buildLnaDealProperties, 'LNA',
    { batchSize, concurrency, searchConcurrency, runId }
  );
  stats.dealsCreated += lnaResult.created;
  stats.dealsUpdated += lnaResult.updated;

  // SDA deals
  let sdaMap = new Map();
  if (sdaPath) {
    logger.info(`Parsing SDA file: ${sdaPath}`);
    sdaMap = parseSdaFile(sdaPath);
    logger.info(`SDA unique customers loaded: ${sdaMap.size}`);
  }

  const sdaResult = await syncAccountDeals(
    sdaMap, contactIdMap, buildSdaDealProperties, 'SDA',
    { batchSize, concurrency, searchConcurrency, runId }
  );
  stats.dealsCreated += sdaResult.created;
  stats.dealsUpdated += sdaResult.updated;

  const durationS = Math.round((Date.now() - startTimeMs) / 1000);
  logger.info('=== Sync complete ===');
  logger.info(`Contacts — created: ${stats.contactsCreated}, updated: ${stats.contactsUpdated}, skipped: ${stats.contactsSkipped}`);
  logger.info(`Deals     — created: ${stats.dealsCreated}, updated: ${stats.dealsUpdated}`);

  fileLogger.syncComplete(runId, { ...stats, durationS });

  return stats;
}


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


async function batchSearchContactsByEmail(contacts, batchSize, concurrency) {
  const emails = contacts.map((c) => c.email);
  const results = await runBatches(
    chunk(emails, batchSize),
    concurrency,
    (batch) => searchContactsByEmails(batch)
  );
  const merged = new Map();
  for (const map of results) {
    for (const [k, v] of map) merged.set(k, v);
  }
  return merged;
}


async function batchSearchContacts(hashes, batchSize, concurrency) {
  const results = await runBatches(
    chunk(hashes, batchSize),
    concurrency,
    (batch) => searchContactsByHashes(batch)
  );

  const merged = new Map();
  for (const map of results) {
    for (const [k, v] of map) merged.set(k, v);
  }
  return merged;
}


async function batchSearchDeals(hashes, batchSize, concurrency) {
  let results;
  try {
    results = await runBatches(
      chunk(hashes, batchSize),
      concurrency,
      (batch) => searchDealsByHashes(batch)
    );
  } catch (err) {
    const status = err.code || (err.response && err.response.status);
    if (status === 400) {
      logger.warn(
        'Deal search returned HTTP 400 — the "taxidhashed" property may not ' +
        'exist or may not be searchable on the Deals object in this HubSpot portal. ' +
        'Treating all deals as new for this run.'
      );
      return new Map();
    }
    throw err;
  }

  const merged = new Map();
  for (const map of results) {
    for (const [k, v] of map) {
      if (!merged.has(k)) {
        merged.set(k, v);
      } else {
        merged.get(k).push(...v);
      }
    }
  }
  return merged;
}

function deduplicateCifRows(cifRows) {
  const seen = new Map();
  for (const row of cifRows) {
    const existing = seen.get(row.taxIdHashed);
    // prefer a row that has an email over one that doesn't —
    // avoids silently dropping a customer whose first CIF row has no email but a later row does
    if (!existing || (!existing.email && row.email)) {
      seen.set(row.taxIdHashed, row);
    }
  }
  return Array.from(seen.values());
}

module.exports = { runSync };
