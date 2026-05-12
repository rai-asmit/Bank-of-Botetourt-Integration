'use strict';

const fs   = require('fs');
const path = require('path');
const logger = require('./logger');
const { summarize } = require('./hubspotError');

function cfg() {
  return require('../config/config').config.logging;
}

function deadLetterPath() {
  const conf = cfg();
  const now  = new Date();
  const yyyy = String(now.getFullYear());
  const mm   = String(now.getMonth() + 1).padStart(2, '0');
  const dd   = String(now.getDate()).padStart(2, '0');
  const dir  = path.resolve(conf.dir, yyyy, mm, dd);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'dead-letter.jsonl');
}

function safePayload(payload) {
  if (payload == null) return null;
  try {
    JSON.stringify(payload);
    return payload;
  } catch (_) {
    return { stringified: String(payload) };
  }
}

function write(runId, kind, { error, payload, reason } = {}) {
  const info = error ? summarize(error) : null;
  const entry = {
    ts:      new Date().toISOString(),
    runId,
    kind,
    reason:  reason || (info && info.summary) || (error && error.message) || null,
    status:  info && info.status || null,
    code:    info && info.code   || null,
    property: info && info.propertyName || null,
    value:    info && info.value || null,
    payload: safePayload(payload),
  };
  try {
    fs.appendFileSync(deadLetterPath(), JSON.stringify(entry) + '\n');
  } catch (e) {
    logger.warn(`deadLetter write failed: ${e.message}`);
  }
}

module.exports = { write };
