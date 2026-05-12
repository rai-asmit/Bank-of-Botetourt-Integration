'use strict';

const { parse } = require('csv-parse/sync');
const logger = require('./logger');

/**
 * Parses a Fiserv CSV string line by line so a single malformed row never
 * aborts the whole file.  The two-row Fiserv header is parsed together (it
 * must succeed — a broken header means the file is unreadable).  Every data
 * line is parsed independently; failures are logged and skipped.
 *
 * @param {string} raw   - pre-processed CSV text (output of escapeFiservCsv)
 * @param {object} opts  - csv-parse options (trim, etc.) — relax_column_count
 *                         is always added internally
 * @param {string} tag   - parser name used in log messages (e.g. 'CIF')
 * @returns {string[][]} - all rows including the two header rows
 */
function safeParseCsv(raw, opts, tag) {
  const parseOpts = { ...opts, relax_column_count: true };
  const lines = raw.split(/\r?\n/);

  // The two Fiserv header rows must parse together so buildColMap works.
  // We re-throw tagged so the per-phase try/catch in the orchestrator can
  // mark this dataset failed_partial without killing the rest of the run.
  const headerSource = lines.slice(0, 2).join('\n');
  let headerRows;
  try {
    headerRows = parse(headerSource, parseOpts);
  } catch (err) {
    throw new Error(`HEADER_PARSE_FAIL[${tag}]: ${err.message}`);
  }
  const allRows = [...headerRows];

  for (let i = 2; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;

    try {
      const parsed = parse(line, parseOpts);
      if (parsed.length > 0) allRows.push(parsed[0]);
    } catch (err) {
      logger.warn(`[${tag}] Line ${i + 1}: skipping — ${err.message}`);
    }
  }

  return allRows;
}

module.exports = { safeParseCsv };
