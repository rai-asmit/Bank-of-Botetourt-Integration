'use strict';

const { getClient, callWithRetry, chunk, runBatches } = require('./hubspotClient');
const logger = require('../utils/logger');


async function searchDealsByHashes(hashes) {
  if (hashes.length === 0) return new Map();

  const DEAL_PROPERTIES = [
    'taxidhashed',
    'date_opened',
    'date_closed',
    'account_last_4',
    // DDA
    'account_type',
    'account_status',
    'current_balance',
    'delivery_code',
    'last_deposit_amount',
    'last_withdrawal_amount',
    // CD
    'type_code_external_description',
    'openmat_balance',
    // LNA
    'opening_advance',
  ];

  const allDeals = [];
  let after;

  do {
    const body = {
      filterGroups: [{
        filters: [{
          propertyName: 'taxidhashed',
          operator: 'IN',
          values: hashes,
        }],
      }],
      properties: DEAL_PROPERTIES,
      limit: 100,
    };
    if (after) body.after = after;

    const response = await callWithRetry(() => {
      return getClient().crm.deals.searchApi.doSearch(body);
    }, `searchDeals[${hashes.length}]`);

    allDeals.push(...response.results);
    after = response.paging && response.paging.next && response.paging.next.after;
  } while (after);

  const map = new Map();
  for (const deal of allDeals) {
    const hash = deal.properties.taxidhashed;
    if (!hash) continue;
    if (!map.has(hash)) map.set(hash, []);
    map.get(hash).push(deal);
  }
  return map;
}

async function batchCreateDeals(inputs) {
  if (inputs.length === 0) return [];

  const response = await callWithRetry(() => {
    return getClient().crm.deals.batchApi.create({
      inputs: inputs.map(({ properties, contactId }) => ({
        properties,
        associations: contactId ? [{
          to: { id: String(contactId) },
          types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 3 }],
        }] : [],
      })),
    });
  }, `batchCreateDeals[${inputs.length}]`);

  return response.results;
}


async function batchUpdateDeals(updates) {
  if (updates.length === 0) return [];

  const response = await callWithRetry(() => {
    return getClient().crm.deals.batchApi.update({
      inputs: updates.map(({ id, properties }) => ({ id, properties })),
    });
  }, `batchUpdateDeals[${updates.length}]`);

  return response.results;
}


async function createSingleDeal({ properties, contactId }) {
  const response = await callWithRetry(() => {
    return getClient().crm.deals.basicApi.create({
      properties,
      associations: contactId ? [{
        to: { id: String(contactId) },
        types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 3 }],
      }] : [],
    });
  }, `createDeal[${properties.taxidhashed}]`);
  return response;
}

async function updateSingleDeal({ id, properties }) {
  const response = await callWithRetry(() => {
    return getClient().crm.deals.basicApi.update(String(id), { properties });
  }, `updateDeal[${id}]`);
  return response;
}

async function batchAssociateDeals(associations) {
  if (associations.length === 0) return;

  await callWithRetry(() => {
    return getClient().crm.associations.v4.batchApi.create('deals', 'contacts', {
      inputs: associations.map(({ dealId, contactId }) => ({
        from: { id: String(dealId) },
        to: { id: String(contactId) },
        types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId: 3 }],
      })),
    });
  }, `batchAssociateDeals[${associations.length}]`);
}


function toIsoDateString(ts) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}


// Composite key: Tax ID Hash (implicit — deals array is pre-filtered by hash) + Date Opened + Account Last 4
function findDealByCompositeKey(deals, dateOpened, accountLast4) {
  if (!dateOpened) return null;
  const targetDate = toIsoDateString(dateOpened);

  return deals.find((d) => {
    const dealDate = d.properties && d.properties.date_opened;
    if (!dealDate) return false;

    const normalized = /^\d{4}-\d{2}-\d{2}$/.test(dealDate)
      ? dealDate
      : toIsoDateString(Number(dealDate));

    if (normalized !== targetDate) return false;
    if (accountLast4 && d.properties.account_last_4 !== accountLast4) return false;

    return true;
  }) || null;
}

// build HubSpot property object from a deal
function buildDealProperties(ddaDeal, hash) {
  const { deals } = require('../config/config').config;

  const props = {
    dealname: ddaDeal.accountType,
    account_type: ddaDeal.accountType,
    taxidhashed: hash,
    account_status: ddaDeal.accountStatus,
    delivery_code: ddaDeal.deliveryCode,
    pipeline: deals.pipeline,
    dealstage: deals.stage,
  };

  if (ddaDeal.dateOpened !== null) props.date_opened = ddaDeal.dateOpened;
  if (ddaDeal.dateClosed !== null) props.date_closed = ddaDeal.dateClosed;
  if (ddaDeal.currentBalance !== null) {
    props.current_balance = ddaDeal.currentBalance;
    props.amount = String(ddaDeal.currentBalance);
  }
  if (ddaDeal.lastDepositAmount !== null) props.last_deposit_amount = ddaDeal.lastDepositAmount;
  if (ddaDeal.lastWithdrawalAmount !== null) props.last_withdrawal_amount = ddaDeal.lastWithdrawalAmount;
  if (ddaDeal.accountLast4) props.account_last_4 = ddaDeal.accountLast4;

  return props;
}

function buildCdDealProperties(cdDeal, hash) {
  const { deals } = require('../config/config').config;

  const typeDesc = cdDeal.accountLast4
    ? `${cdDeal.typeCodeExternalDescription}-${cdDeal.accountLast4}`
    : cdDeal.typeCodeExternalDescription;

  const props = {
    dealname: typeDesc,
    type_code_external_description: typeDesc,
    taxidhashed: hash,
    account_status: cdDeal.accountStatus,
    delivery_code: cdDeal.deliveryCode,
    pipeline: deals.pipeline,
    dealstage: deals.stage,
  };

  if (cdDeal.dateOpened !== null) props.date_opened = cdDeal.dateOpened;
  if (cdDeal.dateClosed !== null) props.date_closed = cdDeal.dateClosed;
  if (cdDeal.currentBalance !== null) {
    props.current_balance = cdDeal.currentBalance;
    props.amount = String(cdDeal.currentBalance);
  }
  if (cdDeal.openmatBalance !== null) props.openmat_balance = cdDeal.openmatBalance;
  if (cdDeal.accountLast4) props.account_last_4 = cdDeal.accountLast4;

  return props;
}


function buildLnaDealProperties(lnaDeal, hash) {
  const { deals } = require('../config/config').config;

  const typeDesc = lnaDeal.accountLast4
    ? `${lnaDeal.typeCodeExternalDescription}-${lnaDeal.accountLast4}`
    : lnaDeal.typeCodeExternalDescription;

  const props = {
    dealname: typeDesc,
    type_code_external_description: typeDesc,
    taxidhashed: hash,
    account_status: lnaDeal.accountStatus,
    pipeline: deals.pipeline,
    dealstage: deals.stage,
  };

  if (lnaDeal.dateOpened !== null) props.date_opened = lnaDeal.dateOpened;
  if (lnaDeal.dateClosed !== null) props.date_closed = lnaDeal.dateClosed;
  if (lnaDeal.currentBalance !== null) {
    props.current_balance = lnaDeal.currentBalance;
    props.amount = String(lnaDeal.currentBalance);
  }
  if (lnaDeal.openingAdvance !== null) props.opening_advance = lnaDeal.openingAdvance;
  if (lnaDeal.accountLast4) props.account_last_4 = lnaDeal.accountLast4;

  return props;
}


function buildSdaDealProperties(sdaDeal, hash) {
  const { deals } = require('../config/config').config;

  const typeDesc = sdaDeal.accountLast4
    ? `${sdaDeal.typeCodeExternalDescription}-${sdaDeal.accountLast4}`
    : sdaDeal.typeCodeExternalDescription;

  const props = {
    dealname: typeDesc,
    type_code_external_description: typeDesc,
    taxidhashed: hash,
    account_status: sdaDeal.accountStatus,
    pipeline: deals.pipeline,
    dealstage: deals.stage,
  };

  if (sdaDeal.dateOpened !== null) props.date_opened = sdaDeal.dateOpened;
  if (sdaDeal.accountLast4) props.account_last_4 = sdaDeal.accountLast4;

  return props;
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

module.exports = {
  searchDealsByHashes,
  batchSearchDeals,
  batchCreateDeals,
  batchUpdateDeals,
  createSingleDeal,
  updateSingleDeal,
  batchAssociateDeals,
  findDealByCompositeKey,
  buildDealProperties,
  buildCdDealProperties,
  buildLnaDealProperties,
  buildSdaDealProperties,
};
