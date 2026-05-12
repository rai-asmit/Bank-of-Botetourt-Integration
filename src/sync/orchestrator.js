'use strict';

const { parseCifFile }  = require('../parsers/cifParser');
const { parseDdaFile }  = require('../parsers/ddaParser');
const { parseCdFile }   = require('../parsers/cdParser');
const { parseLnaFile }  = require('../parsers/lnaParser');
const { parseSdaFile }  = require('../parsers/sdaParser');
const {
  batchCreateContacts,
  batchUpdateContacts,
  createSingleContact,
  updateSingleContact,
  buildContactProperties,
  batchSearchContacts,
  batchSearchContactsByEmails,
} = require('../services/contactService');
const {
  buildDealProperties,
  buildCdDealProperties,
  buildLnaDealProperties,
  buildSdaDealProperties,
} = require('../services/dealService');
const { resilientBatch } = require('../services/resilientBatch');
const { syncAccountDeals }  = require('./dealSync');
const logger     = require('../utils/logger');
const fileLogger = require('../utils/fileLogger');
const deadLetter = require('../utils/deadLetter');
const checkpoint = require('../state/checkpoint');
const { config } = require('../config/config');

// Catch obviously-invalid emails before sending them to HubSpot. Pattern:
//   - must have exactly one "@"
//   - local part: at least one non-space, non-@ character
//   - domain: at least one "." and a 2+ letter TLD
//   - reject anything that contains whitespace
// This matches HubSpot's own validator closely enough that real emails go
// through and placeholder rows like "NA@none.none" / "x@gmail" / "foo@bar"
// are skipped upfront.
const EMAIL_RX = /^[^\s@]+@[^\s@]+\.[A-Za-z]{2,}$/;
// Known placeholder domains the bank uses for "no real email" rows.
const PLACEHOLDER_DOMAINS = /@(none\.none|noemail|test\.test)$/i;

function isLikelyValidEmail(raw) {
  if (!raw) return false;
  const email = String(raw).trim();
  if (!EMAIL_RX.test(email)) return false;
  if (PLACEHOLDER_DOMAINS.test(email)) return false;
  return true;
}

// Run one phase with try/catch. A failure marks the phase failed_partial and
// the next phase still runs. Catastrophic errors that propagate up here are
// only those the caller (runner.js) chose to rethrow.
async function runPhase(state, name, fn) {
  if (checkpoint.isCompleted(state, name)) {
    logger.info(`[${name}] already completed in this run — skipping`);
    return null;
  }
  fileLogger.phaseStart(state.runId, name);
  checkpoint.markPhase(state, name, { status: 'in_progress' });
  try {
    const result = await fn();
    checkpoint.markPhase(state, name, { status: 'completed', ...(result && typeof result === 'object' ? result : {}) });
    fileLogger.phaseComplete(state.runId, name, result || {});
    return result;
  } catch (err) {
    logger.error(`[${name}] phase failed but pipeline continues: ${err.message}`);
    fileLogger.phaseFailed(state.runId, name, err.message);
    checkpoint.markPhase(state, name, { status: 'failed_partial', error: err.message });
    return null;
  }
}

function deduplicateCifRows(cifRows) {
  const seen = new Map();
  for (const row of cifRows) {
    const existing = seen.get(row.taxIdHashed);
    if (!existing || (!existing.email && row.email)) {
      seen.set(row.taxIdHashed, row);
    }
  }
  return Array.from(seen.values());
}

function parseAll(paths) {
  const cifRows = paths.cifPath ? parseCifFile(paths.cifPath) : [];
  const ddaMap  = paths.ddaPath ? parseDdaFile(paths.ddaPath) : new Map();
  const cdMap   = paths.cdPath  ? parseCdFile(paths.cdPath)   : new Map();
  const lnaMap  = paths.lnaPath ? parseLnaFile(paths.lnaPath) : new Map();
  const sdaMap  = paths.sdaPath ? parseSdaFile(paths.sdaPath) : new Map();
  return { cifRows, ddaMap, cdMap, lnaMap, sdaMap };
}

async function runSync(state, fetched) {
  const { batchSize, concurrency, searchConcurrency } = config.api;
  const startTimeMs = Date.now();
  logger.info('=== Starting HubSpot sync (resilient, checkpointed) ===');

  // -----  Required inputs  -----
  if (!fetched.cifPath) throw new Error('runSync: cifPath is required');
  if (!fetched.ddaPath) throw new Error('runSync: ddaPath is required');

  // -----  PARSE  -----
  // Always re-parse from disk on resume (cheap, no network). The PARSE phase
  // checkpoint records row counts so the user can see what was loaded.
  let parsed;
  try {
    parsed = parseAll(fetched);
  } catch (err) {
    // A throw from parsing means a header was unrecoverable for one dataset.
    // Mark PARSE failed_partial and abort the run — without parsed data we
    // can't do anything else.
    logger.error(`PARSE phase failed unrecoverably: ${err.message}`);
    fileLogger.phaseFailed(state.runId, 'PARSE', err.message);
    checkpoint.markPhase(state, 'PARSE', { status: 'failed_partial', error: err.message });
    return state.stats;
  }
  checkpoint.markPhase(state, 'PARSE', {
    status: 'completed',
    counts: {
      cif: parsed.cifRows.length,
      dda: parsed.ddaMap.size,
      cd:  parsed.cdMap.size,
      lna: parsed.lnaMap.size,
      sda: parsed.sdaMap.size,
    },
  });
  logger.info(`PARSE: cif=${parsed.cifRows.length} dda=${parsed.ddaMap.size} cd=${parsed.cdMap.size} lna=${parsed.lnaMap.size} sda=${parsed.sdaMap.size}`);

  const uniqueContacts = deduplicateCifRows(parsed.cifRows);

  let noEmail        = 0;
  let badEmail       = 0;
  const validContacts = [];
  for (const c of uniqueContacts) {
    if (!c.email) {
      noEmail++;
      fileLogger.contactSkipped(state.runId, { hash: c.taxIdHashed, reason: 'no_email' });
      continue;
    }
    if (!isLikelyValidEmail(c.email)) {
      badEmail++;
      fileLogger.contactSkipped(state.runId, { hash: c.taxIdHashed, reason: 'invalid_email' });
      deadLetter.write(state.runId, 'contact_invalid_email', {
        reason:  `pre-filter: invalid email "${c.email}"`,
        payload: { hash: c.taxIdHashed, email: c.email },
      });
      continue;
    }
    validContacts.push(c);
  }

  const skippedCount = noEmail + badEmail;
  state.stats.contactsSkipped = skippedCount;
  checkpoint.persist(state);

  logger.info(`Unique contacts: ${uniqueContacts.length} total — ${noEmail} no email, ${badEmail} invalid email, ${validContacts.length} to sync`);

  // -----  CONTACTS_SEARCH  -----
  // Resolves the contactId for every taxIdHashed and persists it to state so
  // the deal phases can resume without re-querying HubSpot.
  let existingContactsMap = new Map();
  await runPhase(state, 'CONTACTS_SEARCH', async () => {
    if (validContacts.length === 0) return { existingIds: 0 };

    const allHashes = validContacts.map((c) => c.taxIdHashed);
    logger.info(`CONTACTS_SEARCH: looking up ${allHashes.length} hashes`);
    existingContactsMap = await batchSearchContacts(allHashes, batchSize, searchConcurrency);
    logger.info(`CONTACTS_SEARCH: found ${existingContactsMap.size} by taxidhashed`);

    // Seed contactIdMap with everyone we know about already.
    const entries = [];
    for (const [hash, contact] of existingContactsMap) {
      entries.push([hash, contact.id]);
    }
    checkpoint.mergeContactIds(state, entries);
    return { existingIds: existingContactsMap.size };
  });

  // If we resumed past CONTACTS_SEARCH but the in-memory map is empty,
  // rebuild it by re-searching — cheaper than re-parsing the whole pipeline.
  if (existingContactsMap.size === 0 && Object.keys(state.contactIdMap).length > 0) {
    // We only have ids in state, not the full contact objects. That's fine —
    // downstream only needs the id, available via state.contactIdMap.
  }

  const knownHashes = new Set(Object.keys(state.contactIdMap));
  const toUpdate = validContacts.filter((c) => knownHashes.has(c.taxIdHashed));
  const toCreate = validContacts.filter((c) => !knownHashes.has(c.taxIdHashed));

  // -----  CONTACTS_UPDATE  -----
  await runPhase(state, 'CONTACTS_UPDATE', async () => {
    if (toUpdate.length === 0) return { totalBatches: 0, doneBatches: 0, succeeded: 0, failed: 0 };

    const updateInputs = toUpdate.map((c) => ({
      id: state.contactIdMap[c.taxIdHashed],
      properties: buildContactProperties(c),
    }));

    const { succeeded, failed } = await resilientBatch({
      items: updateInputs,
      batchSize, concurrency,
      doBatch:  (batch) => batchUpdateContacts(batch),
      doSingle: (item)  => updateSingleContact(item),
      runId: state.runId,
      kind: 'contact_update',
      label: 'contacts update',
      state, phase: 'CONTACTS_UPDATE',
    });

    for (const c of toUpdate) {
      fileLogger.contactUpdated(state.runId, {
        email: c.email, hash: c.taxIdHashed, hubspotId: state.contactIdMap[c.taxIdHashed],
      });
    }
    checkpoint.addStats(state, { contactsUpdated: succeeded });
    logger.info(`CONTACTS_UPDATE: ${succeeded} updated, ${failed} dead-lettered`);
    return { succeeded, failed };
  });

  // -----  CONTACTS_CREATE  -----
  await runPhase(state, 'CONTACTS_CREATE', async () => {
    if (toCreate.length === 0) return { totalBatches: 0, doneBatches: 0, succeeded: 0, failed: 0 };

    // Pre-search by email to avoid 409: contacts that already exist in HubSpot
    // by email (but were not found by taxidhashed) must be updated, not created.
    const allEmails = toCreate.filter((c) => c.email).map((c) => c.email.toLowerCase());
    const emailFoundMap = await batchSearchContactsByEmails(allEmails, batchSize, searchConcurrency);
    logger.info(`CONTACTS_CREATE: email pre-check found ${emailFoundMap.size} already in HubSpot`);

    const reallyToCreate = [];
    const emailFoundUpdates = [];
    const emailFoundEntries = [];
    const seenEmailFoundIds = new Set();
    for (const c of toCreate) {
      const found = c.email && emailFoundMap.get(c.email.toLowerCase());
      if (found) {
        emailFoundEntries.push([c.taxIdHashed, found.id]);
        // Multiple Fiserv rows can share the same email → same HubSpot ID.
        // Track all hash→id mappings but only send one update per HubSpot ID.
        if (!seenEmailFoundIds.has(found.id)) {
          seenEmailFoundIds.add(found.id);
          emailFoundUpdates.push({ id: found.id, properties: buildContactProperties(c) });
        }
      } else {
        reallyToCreate.push(c);
      }
    }

    if (emailFoundEntries.length > 0) {
      checkpoint.mergeContactIds(state, emailFoundEntries);
      await resilientBatch({
        items: emailFoundUpdates,
        batchSize, concurrency,
        doBatch:  (batch) => batchUpdateContacts(batch),
        doSingle: (item)  => updateSingleContact(item),
        runId: state.runId,
        kind: 'contact_email_update',
        label: 'contacts email-pre-update',
        state, phase: 'CONTACTS_CREATE',
      });
      for (const { id } of emailFoundUpdates) {
        fileLogger.contactUpdated(state.runId, { hubspotId: id });
      }
    }

    const createPropsList = reallyToCreate.map((c) => buildContactProperties(c));
    const emailToHash = new Map();
    for (const c of reallyToCreate) {
      if (c.email) emailToHash.set(c.email.toLowerCase(), c.taxIdHashed);
    }

    const { results: createResults, succeeded, failed } = await resilientBatch({
      items: createPropsList,
      batchSize, concurrency,
      doBatch:  (batch) => batchCreateContacts(batch),
      doSingle: (item)  => createSingleContact(item),
      runId: state.runId,
      kind: 'contact_create',
      label: 'contacts create',
      state, phase: 'CONTACTS_CREATE',
    });

    const newEntries = [];
    let trueCreates = 0;
    let conflictUpdates = 0;
    for (const created of createResults) {
      if (!created || !created.properties) continue;
      const email = created.properties.email;
      const hash  = email && emailToHash.get(email.toLowerCase());
      if (!hash) continue;
      newEntries.push([hash, created.id]);
      if (created._conflictResolved) conflictUpdates++;
      else                           trueCreates++;
    }
    checkpoint.mergeContactIds(state, newEntries);

    for (const c of reallyToCreate) {
      const hubspotId = state.contactIdMap[c.taxIdHashed];
      if (!hubspotId) continue;
      fileLogger.contactCreated(state.runId, { email: c.email, hash: c.taxIdHashed, hubspotId });
    }

    const totalUpdated = emailFoundEntries.length + conflictUpdates;
    checkpoint.addStats(state, { contactsCreated: trueCreates, contactsUpdated: totalUpdated });
    logger.info(`CONTACTS_CREATE: ${trueCreates} created, ${emailFoundEntries.length} email-pre-updated, ${conflictUpdates} conflict-resolved, ${failed} dead-lettered`);
    return { succeeded: succeeded + emailFoundEntries.length, failed, trueCreates, conflictUpdates: totalUpdated };
  });

  // Build the in-memory id map deal phases need from the persisted state.
  const contactIdMap = new Map();
  for (const [hash, id] of Object.entries(state.contactIdMap)) {
    contactIdMap.set(hash, id);
  }

  // -----  DEAL PHASES  -----
  // Each dataset is its own phase so a failure in DDA does not skip CD/LNA/SDA.
  const dealOpts = { batchSize, concurrency, searchConcurrency, runId: state.runId, state };

  await runPhase(state, 'DEALS_DDA', async () => {
    const r = await syncAccountDeals(parsed.ddaMap, contactIdMap, buildDealProperties, 'DDA', { ...dealOpts, phase: 'DEALS_DDA' });
    checkpoint.addStats(state, { dealsCreated: r.created, dealsUpdated: r.updated });
    return r;
  });

  await runPhase(state, 'DEALS_CD', async () => {
    const r = await syncAccountDeals(parsed.cdMap, contactIdMap, buildCdDealProperties, 'CD', { ...dealOpts, phase: 'DEALS_CD' });
    checkpoint.addStats(state, { dealsCreated: r.created, dealsUpdated: r.updated });
    return r;
  });

  await runPhase(state, 'DEALS_LNA', async () => {
    const r = await syncAccountDeals(parsed.lnaMap, contactIdMap, buildLnaDealProperties, 'LNA', { ...dealOpts, phase: 'DEALS_LNA' });
    checkpoint.addStats(state, { dealsCreated: r.created, dealsUpdated: r.updated });
    return r;
  });

  await runPhase(state, 'DEALS_SDA', async () => {
    const r = await syncAccountDeals(parsed.sdaMap, contactIdMap, buildSdaDealProperties, 'SDA', { ...dealOpts, phase: 'DEALS_SDA' });
    checkpoint.addStats(state, { dealsCreated: r.created, dealsUpdated: r.updated });
    return r;
  });

  // -----  COMPLETE  -----
  const durationS = Math.round((Date.now() - startTimeMs) / 1000);
  checkpoint.markPhase(state, 'COMPLETE', { status: 'completed' });
  logger.info('=== Sync complete ===');
  logger.info(`Contacts — created: ${state.stats.contactsCreated}, updated: ${state.stats.contactsUpdated}, skipped: ${state.stats.contactsSkipped}`);
  logger.info(`Deals     — created: ${state.stats.dealsCreated}, updated: ${state.stats.dealsUpdated}`);
  fileLogger.syncComplete(state.runId, { ...state.stats, durationS });

  return state.stats;
}

module.exports = { runSync };
