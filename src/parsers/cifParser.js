'use strict';

const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { hashTaxId } = require('../utils/hash');
const { parseFiservDate } = require('../utils/dateUtils');

// CIF column positions
const COL = {
  TAX_ID: 0,
  EMAIL: 1,
  FIRSTNAME: 2,
  LASTNAME: 3,
  DATE_OF_BIRTH: 4,
  DATE_OPENED: 5,
  OWNER_CODE: 6,
  BR: 7,
  ADDRESS: 8,
  ADDRESS2: 9,
  CITY: 10,
  STATE: 11,
  ZIP: 12,
  DDA_ACCTS: 13,
  CD_ACCOUNTS: 14,
  TOTAL_DEPOSITS: 15,
  LOAN_ACCOUNTS: 16,
  TOTAL_LOANS: 17,
  INT_BANK_ONE: 18,
  // USER_DEFINED_THREE: 19,
    // USER: 19,
  EMPL: 20,
};

// read CIF CSV and return array of contact objects
function parseCifFile(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');

  // parse rows and trim whitespace from every cell
  const allRows = parse(raw, {
    relax_quotes: true,
    skip_empty_lines: true,
    trim: true,
  });

  // First 2 rows are the two-line header — skip them
  const dataRows = allRows.slice(2);

  return dataRows
    .map((row) => mapCifRow(row))
    .filter((contact) => contact !== null);
}

// map one CSV row to a contact object, null if invalid
function mapCifRow(row) {
  if (row.length < 18) return null;

  const taxIdRaw = row[COL.TAX_ID] || '';
  if (!taxIdRaw.trim()) return null;

  return {
    taxIdRaw: taxIdRaw,
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
    // userDefinedThree: row[COL.USER_DEFINED_THREE] || '',
    // user: row[COL.USER] || '',
    empl: row[COL.EMPL] || '',
  };
}

// parse padded number string like " 00001234.56" to float
function parseNumber(raw) {
  if (!raw || !raw.trim()) return null;
  const n = parseFloat(raw.trim());
  return isNaN(n) ? null : n;
}

module.exports = { parseCifFile };
