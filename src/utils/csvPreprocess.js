'use strict';

/**
 * Fixes Fiserv CSV exports that contain unescaped double-quote characters
 * inside quoted fields (e.g. `"O"Brien"` or `"Ace "Hardware" Co"`).
 *
 * Standard CSV requires internal quotes to be doubled (`""`), but Fiserv
 * does not always do this.  csv-parse throws "Invalid Closing Quote" when it
 * encounters a closing-quote followed by a non-delimiter character.
 *
 * The state machine walks the raw text character by character:
 *   - When a `"` opens a field, it enters quoted-field mode.
 *   - Inside a quoted field, a `"` that is:
 *       • already followed by another `"`  → already escaped, pass through
 *       • followed by `,`, newline, or EOF  → genuine closing quote
 *       • followed by anything else         → unescaped internal quote, doubled
 *   - After the closing quote, it waits for the next field delimiter before
 *     re-entering quoted-field mode so unquoted fields are untouched.
 */
function escapeFiservCsv(raw) {
  let result = '';
  let i = 0;
  const len = raw.length;
  let atFieldStart = true;

  while (i < len) {
    const ch = raw[i];

    if (atFieldStart && ch === '"') {
      atFieldStart = false;
      result += '"';
      i++;

      while (i < len) {
        const c = raw[i];

        if (c === '"') {
          if (i + 1 < len && raw[i + 1] === '"') {
            // Already-escaped pair — pass through as-is
            result += '""';
            i += 2;
          } else if (i + 1 >= len || raw[i + 1] === ',' || raw[i + 1] === '\n' || raw[i + 1] === '\r') {
            // Genuine closing quote
            result += '"';
            i++;
            break;
          } else {
            // Unescaped internal quote — double it
            result += '""';
            i++;
          }
        } else {
          result += c;
          i++;
        }
      }
    } else {
      result += ch;
      if (ch === ',' || ch === '\n') {
        atFieldStart = true;
      } else if (ch !== '\r') {
        atFieldStart = false;
      }
      i++;
    }
  }

  return result;
}

module.exports = { escapeFiservCsv };
