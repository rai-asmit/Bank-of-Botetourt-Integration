'use strict';

const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');
const { buildColMap } = require('../utils/colMap');

// Note: LNA uses "Status Description" (full word) while DDA, CD, and SDA use
// "Status Desc" (abbreviated).  The labels must match each file's actual header.
const EXPECTED_COLUMNS = {
  TAX_ID:                'Tax ID Number',
  ACCOUNT_NUMBER_MASKED: 'Account Number Masked',
  TYPE_CODE:             'Type Code External Description',
  DATE_OPENED:           'Date Opened',
  DATE_CLOSED:           'Date Closed',
  STATUS:                'Status Description',
  CURRENT_BALANCE:       'Current Balance',
  OPENING_ADVANCE:       'Opening Advance',
};

function parseLnaFile(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');

  const allRows = parse(raw, {
    relax_quotes: true,
    skip_empty_lines: true,
    trim: true,
  });

  const COL = buildColMap(allRows[0], allRows[1], EXPECTED_COLUMNS, 'LNA');
  const minCols = Math.max(...Object.values(COL)) + 1;

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const lnaMap = new Map();

  for (const row of dataRows) {
    const deal = mapLnaRow(row, COL, minCols);
    if (!deal) continue;

    if (!lnaMap.has(deal.taxIdHashed)) {
      lnaMap.set(deal.taxIdHashed, []);
    }
    lnaMap.get(deal.taxIdHashed).push(deal);
  }

  return lnaMap;
}

function mapLnaRow(row, COL, minCols) {
  if (row.length < minCols) return null;

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) return null;

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
    openingAdvance: parseNumber(row[COL.OPENING_ADVANCE]),
  };
}

function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseLnaFile };
