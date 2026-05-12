'use strict';

const fs = require('fs');
const { safeParseCsv } = require('../utils/safeCsvParse');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');
const { buildColMap } = require('../utils/colMap');
const { escapeFiservCsv } = require('../utils/csvPreprocess');
const logger = require('../utils/logger');

// The CD file has two columns with the identical merged label "Status Desc"
// (columns 5 and 6 — a duplicate Status Desc Fiserv includes).  buildColMap
// uses indexOf so STATUS always resolves to the FIRST occurrence (col 5).
// Column 6 is never referenced and is effectively skipped.
const EXPECTED_COLUMNS = {
  TAX_ID:                'Tax ID Number',
  ACCOUNT_NUMBER_MASKED: 'Account Number Masked',
  TYPE_CODE:             'Type Code External Description',
  DATE_OPENED:           'Date Opened',
  DATE_CLOSED:           'Date Closed',
  STATUS:                'Status Desc',
  CURRENT_BALANCE:       'Current Balance',
  DELIVERY_CODE:         'Delivery Code',
  OPENMAT_BALANCE:       'Open/Mat Balance',
};

function parseCdFile(filePath) {
  const raw = escapeFiservCsv(fs.readFileSync(filePath, 'utf8'));

  const allRows = safeParseCsv(raw, { trim: true }, 'CD');

  const COL = buildColMap(allRows[0], allRows[1], EXPECTED_COLUMNS, 'CD');
  const minCols = Math.max(...Object.values(COL)) + 1;

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const cdMap = new Map();

  for (let i = 0; i < dataRows.length; i++) {
    const fileRowNum = i + 3;
    let deal;
    try {
      deal = mapCdRow(dataRows[i], COL, minCols, fileRowNum);
    } catch (err) {
      logger.error(`[CD] Row ${fileRowNum}: unexpected error — ${err.message}`);
      continue;
    }
    if (!deal) continue;

    if (!cdMap.has(deal.taxIdHashed)) {
      cdMap.set(deal.taxIdHashed, []);
    }
    cdMap.get(deal.taxIdHashed).push(deal);
  }

  return cdMap;
}

function mapCdRow(row, COL, minCols, fileRowNum) {
  if (row.length < minCols) {
    logger.warn(`[CD] Row ${fileRowNum}: skipping — expected ${minCols} columns, got ${row.length}`);
    return null;
  }

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) {
    logger.warn(`[CD] Row ${fileRowNum}: skipping — missing Tax ID`);
    return null;
  }

  const accountNumberMasked = (row[COL.ACCOUNT_NUMBER_MASKED] || '').trim();
  const accountLast4 = accountNumberMasked.slice(-4);

  return {
    taxIdRaw,
    taxIdHashed: hashTaxId(taxIdRaw),
    accountLast4,
    typeCodeExternalDescription: (row[COL.TYPE_CODE] || '').trim(),
    dateOpened: parseFiservDate(row[COL.DATE_OPENED]),
    dateClosed: parseFiservDate(row[COL.DATE_CLOSED]),
    accountStatus: (row[COL.STATUS] || '').trim(),
    currentBalance: parseNumber(row[COL.CURRENT_BALANCE]),
    deliveryCode: (row[COL.DELIVERY_CODE] || '').trim(),
    openmatBalance: parseNumber(row[COL.OPENMAT_BALANCE]),
  };
}

function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseCdFile };
