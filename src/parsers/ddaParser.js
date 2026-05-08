'use strict';

const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');

// DDA column positions
const COL = {
  TAX_ID: 0,
  ACCOUNT_TYPE: 1,  // External Description 
  DATE_OPENED: 2,
  STATUS: 3,
  CURRENT_BALANCE: 4,
  DELIVERY_CODE: 5,
  LAST_DEPOSIT: 6,
  LAST_WITHDRAWAL: 7,
};

// read DDA CSV and return deals grouped by hashed tax ID
function parseDdaFile(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');

  const allRows = parse(raw, {
    relax_quotes: true,
    skip_empty_lines: true,
    trim: true,
  });

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const ddaMap = new Map();

  for (const row of dataRows) {
    const deal = mapDdaRow(row);
    if (!deal) continue;

    if (!ddaMap.has(deal.taxIdHashed)) {
      ddaMap.set(deal.taxIdHashed, []);
    }
    ddaMap.get(deal.taxIdHashed).push(deal);
  }

  return ddaMap;
}

// map one CSV row to a deal object, null if invalid
function mapDdaRow(row) {
  if (row.length < 7) return null;

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) return null;

  return {
    taxIdRaw: taxIdRaw,
    taxIdHashed: hashTaxId(taxIdRaw),
    accountType: row[COL.ACCOUNT_TYPE] || '',
    dateOpenedRaw: row[COL.DATE_OPENED] || '',
    dateOpened: parseFiservDate(row[COL.DATE_OPENED]),
    accountStatus: row[COL.STATUS] || '',
    currentBalance: parseNumber(row[COL.CURRENT_BALANCE]),
    deliveryCode: row[COL.DELIVERY_CODE] || '',
    lastDepositAmount: parseNumber(row[COL.LAST_DEPOSIT]),
    lastWithdrawalAmount: parseNumber(row[COL.LAST_WITHDRAWAL]),
  };
}

// parse number
function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseDdaFile };
