'use strict';

/**
 * Strips everything that is not a letter or digit and lowercases the result.
 * Used to compare Fiserv header labels tolerantly (spaces, slashes, dots, etc.
 * can all vary across file versions without causing a false mismatch).
 */
function normalize(str) {
  return (str || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

/**
 * Builds a column-index map from a Fiserv two-row header.
 *
 * Fiserv files split each column label across two consecutive header rows
 * (row 0 = "Tax ID", row 1 = "Number" → full label "Tax ID Number").
 * This function joins the two rows column-by-column, normalises every merged
 * label, then resolves each key in `expected` to its actual column index.
 *
 * If any expected column is not found the function throws immediately with a
 * clear message naming the missing column and the file — so a format change is
 * caught at parse time, not silently mapped to the wrong field.
 *
 * @param {string[]} row0      - Parsed cells of the first  header row (index 0)
 * @param {string[]} row1      - Parsed cells of the second header row (index 1)
 * @param {Object}   expected  - { KEY: 'Human Readable Label' } for every column you need
 * @param {string}   fileLabel - Short file identifier used in error messages (e.g. 'CIF')
 * @returns {Object}           - { KEY: columnIndex, ... }
 */
function buildColMap(row0, row1, expected, fileLabel) {
  if (!Array.isArray(row0) || !Array.isArray(row1)) {
    throw new Error(`${fileLabel}: file is missing the expected two-row header`);
  }

  // Merge the two header rows into one normalised label per column position
  const len = Math.max(row0.length, row1.length);
  const combined = [];
  for (let i = 0; i < len; i++) {
    const part0 = (row0[i] || '').trim();
    const part1 = (row1[i] || '').trim();
    combined.push(normalize(`${part0} ${part1}`));
  }

  const colMap = {};
  for (const [key, label] of Object.entries(expected)) {
    const target = normalize(label);
    const idx = combined.indexOf(target);
    if (idx === -1) {
      throw new Error(
        `${fileLabel}: expected column "${label}" not found in file header — ` +
        `file format may have changed`
      );
    }
    colMap[key] = idx;
  }
  return colMap;
}

module.exports = { buildColMap };
