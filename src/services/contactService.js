'use strict';

const { getClient, callWithRetry, chunk, runBatches } = require('./hubspotClient');

// search HubSpot contacts by hashed tax IDs (max 100)
async function searchContactsByHashes(hashes) {
  if (hashes.length === 0) return new Map();

  const response = await callWithRetry(() => {
    return getClient().crm.contacts.searchApi.doSearch({
      filterGroups: [{
        filters: [{
          propertyName: 'taxidhashed',
          operator: 'IN',
          values: hashes,
        }],
      }],
      properties: ['taxidhashed', 'email', 'hs_object_id'],
      limit: 100,
    });
  }, `searchContacts[${hashes.length}]`);

  const map = new Map();
  for (const contact of response.results) {
    const hash = contact.properties.taxidhashed;
    if (hash) map.set(hash, contact);
  }
  return map;
}

// create up to 100 contacts, handle duplicate email conflicts
async function batchCreateContacts(propsList) {
  if (propsList.length === 0) return [];

  try {
    const response = await callWithRetry(() => {
      return getClient().crm.contacts.batchApi.create({
        inputs: propsList.map((properties) => ({ properties })),
      });
    }, `batchCreateContacts[${propsList.length}]`);

    return response.results;
  } catch (err) {
    const status = err.code || (err.response && err.response.status);
    if (status !== 409) throw err;

    // duplicate email — retry one by one to recover each conflict
    const results = [];
    for (const properties of propsList) {
      try {
        const created = await callWithRetry(() => {
          return getClient().crm.contacts.basicApi.create({ properties });
        }, `createContact[${properties.email}]`);
        results.push(created);
      } catch (singleErr) {
        const singleStatus = singleErr.code || (singleErr.response && singleErr.response.status);
        if (singleStatus !== 409) throw singleErr;

        const existingId = extractExistingId(singleErr);
        if (!existingId) throw singleErr; //

        // update existing contact with new properties
        await callWithRetry(() => {
          return getClient().crm.contacts.basicApi.update(existingId, { properties });
        }, `updateConflictContact[${existingId}]`);

        results.push({ id: existingId, properties: { email: properties.email }, _conflictResolved: true });
      }
    }
    return results;
  }
}

// pulls out an id number from error message
function extractExistingId(err) {
  const body = err.response && err.response.body;
  const message = (body && body.message) || err.message || '';
  const match = message.match(/Existing ID:\s*(\d+)/);
  return match ? match[1] : null;
}

// update up to 100 contacts 
async function batchUpdateContacts(updates) {
  if (updates.length === 0) return [];

  const response = await callWithRetry(() => {
    return getClient().crm.contacts.batchApi.update({
      inputs: updates.map(({ id, properties }) => ({ id, properties })),
    });
  }, `batchUpdateContacts[${updates.length}]`);

  return response.results;
}

// build HubSpot property object from a contact
function buildContactProperties(contact) {
  const props = {
    taxidhashed: contact.taxIdHashed,
    email: contact.email,
    firstname: contact.firstname,
    lastname: contact.lastname,
    owner_code: contact.ownerCode,
    br: contact.br,
    address: contact.address,
    address2: contact.address2,
    city: contact.city,
    state: contact.state,
    zip: contact.zip,
    int_bank_one: contact.intBankOne,
    user: contact.user,
    empl: contact.empl,
  };

  // only set numeric/date fields if they have a value
  if (contact.dateOfBirth !== null) props.date_of_birth = contact.dateOfBirth;
  if (contact.dateOpened !== null) props.date_opened = contact.dateOpened;
  if (contact.numberOfDdaAccounts !== null) props.number_of_dda_accounts = contact.numberOfDdaAccounts;
  if (contact.numberOfCdAccounts !== null) props.number_of_cd_accounts = contact.numberOfCdAccounts;
  if (contact.totalDeposits !== null) props.total_deposits = contact.totalDeposits;
  if (contact.numberOfLoanAccounts !== null) props.number_of_loan_accounts = contact.numberOfLoanAccounts;
  if (contact.totalLoans !== null) props.total_number_of_loans = contact.totalLoans;

  return props;
}

// search contacts by email, fallback when hash is missing
async function searchContactsByEmails(emails) {
  if (emails.length === 0) return new Map();

  const response = await callWithRetry(() => {
    return getClient().crm.contacts.searchApi.doSearch({
      filterGroups: [{
        filters: [{
          propertyName: 'email',
          operator: 'IN',
          values: emails,
        }],
      }],
      properties: ['taxidhashed', 'email', 'hs_object_id'],
      limit: 100,
    });
  }, `searchContactsByEmail[${emails.length}]`);

  const map = new Map();
  for (const contact of response.results) {
    const email = contact.properties.email;
    if (email) map.set(email.toLowerCase(), contact);
  }
  return map;
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

module.exports = {
  searchContactsByHashes,
  searchContactsByEmails,
  batchCreateContacts,
  batchUpdateContacts,
  buildContactProperties,
  batchSearchContacts,
  batchSearchContactsByEmail,
};
