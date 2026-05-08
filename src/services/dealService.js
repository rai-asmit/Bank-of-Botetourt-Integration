'use strict';

const { getClient, callWithRetry } = require('./hubspotClient');


async function searchDealsByHashes(hashes) {
  if (hashes.length === 0) return new Map();

  const DEAL_PROPERTIES = [
    'taxidhashed',
    'date_opened',
    'account_type',
    'account_status',
    'current_balance',
    'delivery_code',
    'last_deposit_amount',
    'last_withdrawal_amount',
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


function findDealByDateOpened(deals, dateOpened) {
  if (!dateOpened) return null;
  const targetDate = toIsoDateString(dateOpened);

  return deals.find((d) => {
    const dealDate = d.properties && d.properties.date_opened;
    if (!dealDate) return false;

    // normalize date — HubSpot returns either YYYY-MM-DD or ms timestamp
    const normalized = /^\d{4}-\d{2}-\d{2}$/.test(dealDate)
      ? dealDate
      : toIsoDateString(Number(dealDate));

    return normalized === targetDate;
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
  // date_closed: not yet available in DDA file — uncomment when column is added
  // if (ddaDeal.dateClosed !== null) props.date_closed = ddaDeal.dateClosed;
  if (ddaDeal.currentBalance !== null) {
    props.current_balance = ddaDeal.currentBalance;
    props.amount = String(ddaDeal.currentBalance);
  }
  if (ddaDeal.lastDepositAmount !== null) props.last_deposit_amount = ddaDeal.lastDepositAmount;
  if (ddaDeal.lastWithdrawalAmount !== null) props.last_withdrawal_amount = ddaDeal.lastWithdrawalAmount;
  // account_last_4: not yet available in DDA file — uncomment when column is added
  // if (ddaDeal.accountLast4) props.account_last_4 = ddaDeal.accountLast4;

  return props;
}

module.exports = {
  searchDealsByHashes,
  batchCreateDeals,
  batchUpdateDeals,
  batchAssociateDeals,
  findDealByDateOpened,
  buildDealProperties,
};
