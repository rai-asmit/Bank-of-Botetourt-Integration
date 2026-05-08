'use strict';

const { parseCifFile } = require('../parsers/cifParser');
const { parseDdaFile } = require('../parsers/ddaParser');
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
  findDealByDateOpened,
  buildDealProperties,
} = require('../services/dealService');
const { chunk, runBatches } = require('../services/hubspotClient');
const logger = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const { config } = require('../config/config');


async function runSync(runId, { cifPath, ddaPath }) {
  //checking file path
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

  // Batch search existing deals 
  const ddaHashes = [...ddaMap.keys()];
  logger.info(`Searching existing deals for ${ddaHashes.length} customer hashes (batch)...`);
  const existingDealsMap = await batchSearchDeals(ddaHashes, batchSize, searchConcurrency);
  logger.info(`Found deals for ${existingDealsMap.size} customers`);

  //Match DDA rows against existing deals
  const dealsToUpdate = [];
  const dealsToCreate = [];

  for (const [hash, ddaRows] of ddaMap) {
    const contactId = contactIdMap.get(hash);
    if (!contactId) continue; 

    const existingDeals = existingDealsMap.get(hash) || [];

    for (const ddaDeal of ddaRows) {
      const properties = buildDealProperties(ddaDeal, hash);
      const matching = findDealByDateOpened(existingDeals, ddaDeal.dateOpened);

      if (matching) {
        dealsToUpdate.push({ id: matching.id, properties });
      } else {
        dealsToCreate.push({ properties, contactId });
      }
    }
  }

  //  Batch-update matched deals 
  if (dealsToUpdate.length > 0) {
    logger.info(`Updating ${dealsToUpdate.length} existing deals (batch)...`);
    await runBatches(chunk(dealsToUpdate, batchSize), concurrency, (batch) => {
      return batchUpdateDeals(batch);
    });
    stats.dealsUpdated = dealsToUpdate.length;
    logger.info(`Updated ${dealsToUpdate.length} deals`);

    for (const d of dealsToUpdate) {
      fileLogger.dealUpdated(runId, { hash: d.properties.taxidhashed, dealId: d.id });
    }
  }

  // Batch-create new deals
  if (dealsToCreate.length > 0) {
    logger.info(`Creating ${dealsToCreate.length} new deals (batch)...`);

    const dealCreateResults = await runBatches(
      chunk(dealsToCreate, batchSize),
      concurrency,
      (batch) => batchCreateDeals(batch)
    );

    stats.dealsCreated = dealsToCreate.length;
    logger.info(`Created ${dealsToCreate.length} deals`);

  
    const createdDealIdMap = new Map();
    for (const batchResult of dealCreateResults) {
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
        dealname:   d.properties.account_type,
        dateOpened: d.properties.date_opened,
      });
    }
  }

  const durationS = Math.round((Date.now() - startTimeMs) / 1000);
  logger.info('=== Sync complete ===');
  logger.info(`Contacts — created: ${stats.contactsCreated}, updated: ${stats.contactsUpdated}, skipped: ${stats.contactsSkipped}`);
  logger.info(`Deals     — created: ${stats.dealsCreated}, updated: ${stats.dealsUpdated}`);

  fileLogger.syncComplete(runId, { ...stats, durationS });

  return stats;
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
    if (!seen.has(row.taxIdHashed)) {
      seen.set(row.taxIdHashed, row);
    }
  }
  return Array.from(seen.values());
}

module.exports = { runSync };
