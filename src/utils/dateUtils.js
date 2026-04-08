'use strict';

// parse Fiserv date string to Unix timestamp in ms
function parseFiservDate(raw) {
  if (!raw || !raw.trim()) return null;

  // remove age part like " (29)" from date string
  const datePart = raw.split('(')[0].trim();
  if (!datePart) return null;

  // parse as UTC to avoid timezone shift
  const d = new Date(datePart + ' UTC');
  return isNaN(d.getTime()) ? null : d.getTime();
}

// format timestamp back to M/D/YYYY for comparison
function formatDateForComparison(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return `${d.getUTCMonth() + 1}/${d.getUTCDate()}/${d.getUTCFullYear()}`;
}

module.exports = { parseFiservDate, formatDateForComparison };
