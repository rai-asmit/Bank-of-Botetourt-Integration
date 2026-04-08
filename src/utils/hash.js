'use strict';

const crypto = require('crypto');

// hash tax ID, strip spaces first so format doesn't matter
function hashTaxId(rawTaxId) {
  const normalized = rawTaxId.replace(/\s/g, '');
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

module.exports = { hashTaxId };
