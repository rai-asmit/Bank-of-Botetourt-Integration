'use strict';

const fs = require('fs');
const { safeParseCsv } = require('../utils/safeCsvParse');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');
const { buildColMap } = require('../utils/colMap');
const { escapeFiservCsv } = require('../utils/csvPreprocess');
const logger = require('../utils/logger');

const EXPECTED_COLUMNS = {
  TAX_ID:               'Tax ID Number',
  ACCOUNT_NUMBER_MASKED: 'Account Number Masked',
  ACCOUNT_TYPE:         'Type Code External Description',
  DATE_OPENED:          'Date Opened',
  DATE_CLOSED:          'Date Closed',
  STATUS:               'Status Desc',
  CURRENT_BALANCE:      'Current Balance',
  DELIVERY_CODE:        'Delivery Code',
  LAST_DEPOSIT:         'Amount Last Deposit',
  LAST_WITHDRAWAL:      'Amount Last Withdrawal',
};

function parseDdaFile(filePath) {
  const raw = escapeFiservCsv(fs.readFileSync(filePath, 'utf8'));

  const allRows = safeParseCsv(raw, { trim: true }, 'DDA');

  const COL = buildColMap(allRows[0], allRows[1], EXPECTED_COLUMNS, 'DDA');
  const minCols = Math.max(...Object.values(COL)) + 1;

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const ddaMap = new Map();

  for (let i = 0; i < dataRows.length; i++) {
    const fileRowNum = i + 3;
    let deal;
    try {
      deal = mapDdaRow(dataRows[i], COL, minCols, fileRowNum);
    } catch (err) {
      logger.error(`[DDA] Row ${fileRowNum}: unexpected error — ${err.message}`);
      continue;
    }
    if (!deal) continue;

    if (!ddaMap.has(deal.taxIdHashed)) {
      ddaMap.set(deal.taxIdHashed, []);
    }
    ddaMap.get(deal.taxIdHashed).push(deal);
  }

  return ddaMap;
}

function mapDdaRow(row, COL, minCols, fileRowNum) {
  if (row.length < minCols) {
    logger.warn(`[DDA] Row ${fileRowNum}: skipping — expected ${minCols} columns, got ${row.length}`);
    return null;
  }

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) {
    logger.warn(`[DDA] Row ${fileRowNum}: skipping — missing Tax ID`);
    return null;
  }

  const accountNumberMasked = (row[COL.ACCOUNT_NUMBER_MASKED] || '').trim();
  const accountLast4 = accountNumberMasked.slice(-4);

  return {
    taxIdRaw,
    taxIdHashed: hashTaxId(taxIdRaw),
    accountLast4,
    accountType: row[COL.ACCOUNT_TYPE] || '',
    dateOpened: parseFiservDate(row[COL.DATE_OPENED]),
    dateClosed: parseFiservDate(row[COL.DATE_CLOSED]),
    accountStatus: row[COL.STATUS] || '',
    currentBalance: parseNumber(row[COL.CURRENT_BALANCE]),
    deliveryCode: row[COL.DELIVERY_CODE] || '',
    lastDepositAmount: parseNumber(row[COL.LAST_DEPOSIT]),
    lastWithdrawalAmount: parseNumber(row[COL.LAST_WITHDRAWAL]),
  };
}

function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseDdaFile };
