'use strict';

const fs = require('fs');
const { safeParseCsv } = require('../utils/safeCsvParse');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');
const { buildColMap } = require('../utils/colMap');
const { escapeFiservCsv } = require('../utils/csvPreprocess');
const logger = require('../utils/logger');

// Human-readable column labels as they appear when the two Fiserv header rows
// are merged (row0 + " " + row1, trimmed).  buildColMap resolves each to its
// actual column index at parse time, so reordered columns are handled safely.
const EXPECTED_COLUMNS = {
  TAX_ID:         'Tax ID Number',
  EMAIL:          'Primary Email',
  FIRSTNAME:      'First Name/ Name',
  LASTNAME:       'Last Name',
  DATE_OF_BIRTH:  'Date of Birth/Age',
  DATE_OPENED:    'Date Opened',
  OWNER_CODE:     'Own Code',
  BR:             'Br',
  ADDRESS:        'Address Line 2',
  ADDRESS2:       'Address Line 3',
  CITY:           'City',
  STATE:          'State',
  ZIP:            'ZIP Code',
  DDA_ACCTS:      'DDA Accts',
  CD_ACCOUNTS:    'CD Number of Accounts',
  TOTAL_DEPOSITS: 'Total Deposits',
  LOAN_ACCOUNTS:  'Loan Number of Accounts',
  TOTAL_LOANS:    'Total Loans',
  INT_BANK_ONE:   'Int. Bank One',
  USER:           'User Defined Three',
  EMPL:           'Empl Code',
};

function parseCifFile(filePath) {
  const raw = escapeFiservCsv(fs.readFileSync(filePath, 'utf8'));

  const allRows = safeParseCsv(raw, { trim: true }, 'CIF');

  // Resolve column positions dynamically from the two-row Fiserv header
  const COL = buildColMap(allRows[0], allRows[1], EXPECTED_COLUMNS, 'CIF');
  const minCols = Math.max(...Object.values(COL)) + 1;

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  const contacts = [];
  for (let i = 0; i < dataRows.length; i++) {
    const fileRowNum = i + 3; // 2 header rows, 1-indexed
    let contact;
    try {
      contact = mapCifRow(dataRows[i], COL, minCols, fileRowNum);
    } catch (err) {
      logger.error(`[CIF] Row ${fileRowNum}: unexpected error — ${err.message}`);
      continue;
    }
    if (contact) contacts.push(contact);
  }
  return contacts;
}

function mapCifRow(row, COL, minCols, fileRowNum) {
  if (row.length < minCols) {
    logger.warn(`[CIF] Row ${fileRowNum}: skipping — expected ${minCols} columns, got ${row.length}`);
    return null;
  }

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) {
    logger.warn(`[CIF] Row ${fileRowNum}: skipping — missing Tax ID`);
    return null;
  }

  return {
    taxIdRaw,
    taxIdHashed: hashTaxId(taxIdRaw),
    email: row[COL.EMAIL] || '',
    firstname: row[COL.FIRSTNAME] || '',
    lastname: row[COL.LASTNAME] || '',
    dateOfBirth: parseFiservDate(row[COL.DATE_OF_BIRTH]),
    dateOpened: parseFiservDate(row[COL.DATE_OPENED]),
    ownerCode: row[COL.OWNER_CODE] || '',
    br: row[COL.BR] || '',
    address: row[COL.ADDRESS] || '',
    address2: row[COL.ADDRESS2] || '',
    city: row[COL.CITY] || '',
    state: row[COL.STATE] || '',
    zip: row[COL.ZIP] || '',
    numberOfDdaAccounts: parseNumber(row[COL.DDA_ACCTS]),
    numberOfCdAccounts: parseNumber(row[COL.CD_ACCOUNTS]),
    totalDeposits: parseNumber(row[COL.TOTAL_DEPOSITS]),
    numberOfLoanAccounts: parseNumber(row[COL.LOAN_ACCOUNTS]),
    totalLoans: parseNumber(row[COL.TOTAL_LOANS]),
    intBankOne: row[COL.INT_BANK_ONE] || '',
    user: row[COL.USER] || '',
    empl: row[COL.EMPL] || '',
  };
}

function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseCifFile };
