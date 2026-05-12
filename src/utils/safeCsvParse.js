'use strict';

const { parse } = require('csv-parse/sync');
const logger = require('./logger');

function safeParseCsv(raw, opts, tag) {
  const parseOptions = { ...opts, relax_column_count: true };
  const lines = raw.split(/\r?\n/);

  const headerRows = parseHeader(lines, parseOptions, tag);
  const dataRows = parseDataLines(lines, parseOptions, tag);

  return [...headerRows, ...dataRows];
}

function parseHeader(lines, parseOptions, tag) {
  const headerSource = lines.slice(0, 2).join('\n');

  try {
    return parse(headerSource, parseOptions);
  } catch (error) {
    throw new Error(`HEADER_PARSE_FAIL[${tag}]: ${error.message}`);
  }
}

function parseDataLines(lines, parseOptions, tag) {
  const rows = [];

  for (let i = 2; i < lines.length; i++) {
    const line = lines[i];

    if (isEmptyLine(line)) continue;

    const row = tryParseLine(line, parseOptions, tag, i);
    if (row) rows.push(row);
  }

  return rows;
}

function isEmptyLine(line) {
  return !line.trim();
}

function tryParseLine(line, parseOptions, tag, lineNumber) {
  try {
    const parsed = parse(line, parseOptions);
    return parsed.length > 0 ? parsed[0] : null;
  } catch (error) {
    logger.warn(`[${tag}] Line ${lineNumber + 1}: skipping — ${error.message}`);
    return null;
  }
}

module.exports = { safeParseCsv };
