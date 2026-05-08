'use strict';

// parse Fiserv date string to Unix timestamp in ms
// input formats: "MM/DD/YYYY" or "MM/DD/YYYY (age)"
function parseFiservDate(raw) {
  if (!raw || !raw.trim()) return null;

  // strip optional age suffix like " (29)"
  const datePart = raw.split('(')[0].trim();
  if (!datePart) return null;

  // split MM/DD/YYYY manually — avoids relying on V8 informal date parsing
  const parts = datePart.split('/');
  if (parts.length !== 3) return null;

  const month = Number(parts[0]);
  const day = Number(parts[1]);
  const year = Number(parts[2]);

  if (!month || !day || !year) return null;

  const ts = Date.UTC(year, month - 1, day);
  return isNaN(ts) ? null : ts;
}

// format timestamp back to M/D/YYYY for comparison
function formatDateForComparison(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return `${d.getUTCMonth() + 1}/${d.getUTCDate()}/${d.getUTCFullYear()}`;
}

module.exports = { parseFiservDate, formatDateForComparison };
