'use strict';

const fs = require('fs');
const { safeParseCsv } = require('../utils/safeCsvParse');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');
const { buildColMap } = require('../utils/colMap');
const { escapeFiservCsv } = require('../utils/csvPreprocess');
const logger = require('../utils/logger');

// SDA is a 5-column file — no balance, delivery code, or date closed in source
const EXPECTED_COLUMNS = {
  TAX_ID:                'Tax ID Number',
  ACCOUNT_NUMBER_MASKED: 'Account Number Masked',
  TYPE_CODE:             'Type Code External Description',
  DATE_OPENED:           'Date Opened',
  STATUS:                'Status Desc',
};

function parseSdaFile(filePath) {
  const raw = escapeFiservCsv(fs.readFileSync(filePath, 'utf8'));

  const allRows = safeParseCsv(raw, { trim: true }, 'SDA');

  const COL = buildColMap(allRows[0], allRows[1], EXPECTED_COLUMNS, 'SDA');
  const minCols = Math.max(...Object.values(COL)) + 1;

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const sdaMap = new Map();

  for (let i = 0; i < dataRows.length; i++) {
    const fileRowNum = i + 3;
    let deal;
    try {
      deal = mapSdaRow(dataRows[i], COL, minCols, fileRowNum);
    } catch (err) {
      logger.error(`[SDA] Row ${fileRowNum}: unexpected error — ${err.message}`);
      continue;
    }
    if (!deal) continue;

    if (!sdaMap.has(deal.taxIdHashed)) {
      sdaMap.set(deal.taxIdHashed, []);
    }
    sdaMap.get(deal.taxIdHashed).push(deal);
  }

  return sdaMap;
}

function mapSdaRow(row, COL, minCols, fileRowNum) {
  if (row.length < minCols) {
    logger.warn(`[SDA] Row ${fileRowNum}: skipping — expected ${minCols} columns, got ${row.length}`);
    return null;
  }

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) {
    logger.warn(`[SDA] Row ${fileRowNum}: skipping — missing Tax ID`);
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
    accountStatus: (row[COL.STATUS] || '').trim(),
  };
}

module.exports = { parseSdaFile };
