'use strict';

const { parseCifFile }  = require('../parsers/cifParser');
const { parseDdaFile }  = require('../parsers/ddaParser');
const { parseCdFile }   = require('../parsers/cdParser');
const { parseLnaFile }  = require('../parsers/lnaParser');
const { parseSdaFile }  = require('../parsers/sdaParser');
const {
  batchCreateContacts,
  batchUpdateContacts,
  buildContactProperties,
  batchSearchContacts,
  batchSearchContactsByEmail,
} = require('../services/contactService');
const {
  buildDealProperties,
  buildCdDealProperties,
  buildLnaDealProperties,
  buildSdaDealProperties,
} = require('../services/dealService');
const { chunk, runBatches } = require('../services/hubspotClient');
const { syncAccountDeals }  = require('./dealSync');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const { config } = require('../config/config');


async function runSync(runId, { cifPath, ddaPath, cdPath, lnaPath, sdaPath }) {
  if (!cifPath) throw new Error('runSync: cifPath is required');
  if (!ddaPath) throw new Error('runSync: ddaPath is required');

  const { batchSize, concurrency, searchConcurrency } = config.api;
  const startTimeMs = Date.now();
  logger.info('=== Starting HubSpot sync (batch mode) ===');

  // --- Parse input files ---
  logger.info(`Parsing CIF file: ${cifPath}`);
  const cifRows = parseCifFile(cifPath);
  logger.info(`CIF rows loaded: ${cifRows.length}`);

  logger.info(`Parsing DDA file: ${ddaPath}`);
  const ddaMap = parseDdaFile(ddaPath);
  logger.info(`DDA unique customers loaded: ${ddaMap.size}`);

  const uniqueContacts = deduplicateCifRows(cifRows);
  const validContacts  = uniqueContacts.filter((c) => c.email);
  const skippedCount   = uniqueContacts.length - validContacts.length;
  logger.info(`Unique contacts: ${uniqueContacts.length} total, ${skippedCount} skipped (no email), ${validContacts.length} to sync`);

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

  // --- Resolve existing contacts (by hash, then email fallback) ---
  logger.info('Searching HubSpot for existing contacts (batch)...');
  const allHashes = validContacts.map((c) => c.taxIdHashed);
  const existingContactsMap = await batchSearchContacts(allHashes, batchSize, searchConcurrency);
  logger.info(`Found ${existingContactsMap.size} existing contacts in HubSpot by taxidhashed`);

  const notFoundByHash = validContacts.filter((c) => !existingContactsMap.has(c.taxIdHashed));
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
      // If existingHash is set (different hash), this is a new Tax ID — let it fall through to toCreate.
    }
    logger.info(`Found ${existingContactsMap.size} total existing contacts after email fallback`);
  }

  const toCreate = validContacts.filter((c) => !existingContactsMap.has(c.taxIdHashed));
  const toUpdate = validContacts.filter((c) =>  existingContactsMap.has(c.taxIdHashed));

  // --- Update existing contacts ---
  if (toUpdate.length > 0) {
    logger.info(`Updating ${toUpdate.length} existing contacts (batch)...`);
    const updateInputs = toUpdate.map((c) => ({
      id: existingContactsMap.get(c.taxIdHashed).id,
      properties: buildContactProperties(c),
    }));

    await runBatches(chunk(updateInputs, batchSize), concurrency, (batch) => batchUpdateContacts(batch));

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

  // --- Create new contacts ---
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
        const hash  = email && emailToHash.get(email.toLowerCase());
        if (hash) {
          contactIdMap.set(hash, created.id);
          if (created._conflictResolved) conflictUpdates++;
          else                           trueCreates++;
        }
      }
    }

    stats.contactsCreated  = trueCreates;
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

  // --- Sync deals for each account type ---
  const dealOpts = { batchSize, concurrency, searchConcurrency, runId };

  const ddaResult = await syncAccountDeals(ddaMap, contactIdMap, buildDealProperties, 'DDA', dealOpts);
  stats.dealsCreated += ddaResult.created;
  stats.dealsUpdated += ddaResult.updated;

  let cdMap = new Map();
  if (cdPath) {
    logger.info(`Parsing CD file: ${cdPath}`);
    cdMap = parseCdFile(cdPath);
    logger.info(`CD unique customers loaded: ${cdMap.size}`);
  }
  const cdResult = await syncAccountDeals(cdMap, contactIdMap, buildCdDealProperties, 'CD', dealOpts);
  stats.dealsCreated += cdResult.created;
  stats.dealsUpdated += cdResult.updated;

  let lnaMap = new Map();
  if (lnaPath) {
    logger.info(`Parsing LNA file: ${lnaPath}`);
    lnaMap = parseLnaFile(lnaPath);
    logger.info(`LNA unique customers loaded: ${lnaMap.size}`);
  }
  const lnaResult = await syncAccountDeals(lnaMap, contactIdMap, buildLnaDealProperties, 'LNA', dealOpts);
  stats.dealsCreated += lnaResult.created;
  stats.dealsUpdated += lnaResult.updated;

  let sdaMap = new Map();
  if (sdaPath) {
    logger.info(`Parsing SDA file: ${sdaPath}`);
    sdaMap = parseSdaFile(sdaPath);
    logger.info(`SDA unique customers loaded: ${sdaMap.size}`);
  }
  const sdaResult = await syncAccountDeals(sdaMap, contactIdMap, buildSdaDealProperties, 'SDA', dealOpts);
  stats.dealsCreated += sdaResult.created;
  stats.dealsUpdated += sdaResult.updated;

  // --- Summary ---
  const durationS = Math.round((Date.now() - startTimeMs) / 1000);
  logger.info('=== Sync complete ===');
  logger.info(`Contacts — created: ${stats.contactsCreated}, updated: ${stats.contactsUpdated}, skipped: ${stats.contactsSkipped}`);
  logger.info(`Deals     — created: ${stats.dealsCreated}, updated: ${stats.dealsUpdated}`);
  fileLogger.syncComplete(runId, { ...stats, durationS });

  return stats;
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
