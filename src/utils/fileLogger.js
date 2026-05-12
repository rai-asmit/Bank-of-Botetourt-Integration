'use strict';

const fs   = require('fs');
const path = require('path');

function fmtTimestamp(d = new Date()) {
  const p2 = n => String(n).padStart(2, '0');
  const p3 = n => String(n).padStart(3, '0');
  const off = -d.getTimezoneOffset();
  const sign = off >= 0 ? '+' : '-';
  const oh = Math.floor(Math.abs(off) / 60);
  const om = Math.abs(off) % 60;
  return (
    `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} ` +
    `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}.${p3(d.getMilliseconds())} ` +
    `${sign}${p2(oh)}:${p2(om)}`
  );
}

// Config is read lazily to avoid a circular-require at module load time
function cfg() {
  return require('../config/config').config.logging;
}

const LEVEL_ORDER = { info: 0, warn: 1, error: 2 };

// generate unique run ID like r-20260408-143012
function startRun() {
  const now = new Date();
  const p = (n) => String(n).padStart(2, '0');
  return (
    `r-${now.getFullYear()}${p(now.getMonth() + 1)}${p(now.getDate())}` +
    `-${p(now.getHours())}${p(now.getMinutes())}${p(now.getSeconds())}`
  );
}

// return path to today's log file, create dirs if needed
function logPath(logDir, category) {
  const now  = new Date();
  const yyyy = String(now.getFullYear());
  const mm   = String(now.getMonth() + 1).padStart(2, '0');
  const dd   = String(now.getDate()).padStart(2, '0');
  const dir  = path.resolve(logDir, yyyy, mm, dd);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${category}.log`);
}

// format log line as plain text
function fmtText(level, category, action, fields) {
  const ts   = fmtTimestamp();
  const lv   = level.toUpperCase().padEnd(5);
  const cat  = category.toUpperCase().padEnd(7);
  const act  = action.padEnd(8);
  const kvs  = Object.entries(fields)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => {
      const s = String(v);
      return s.includes(' ') ? `${k}="${s}"` : `${k}=${s}`;
    })
    .join(' ');
  return `${ts} [${lv}] [${cat}] ${act} ${kvs}\n`;
}

// format log line as JSON (one object per line)
function fmtJson(level, category, action, fields) {
  const entry = {
    ts:       fmtTimestamp(),
    level:    level.toUpperCase(),
    category: category.toUpperCase(),
    action,
  };
  for (const [k, v] of Object.entries(fields)) {
    if (v !== undefined && v !== null && v !== '') entry[k] = v;
  }
  return JSON.stringify(entry) + '\n';
}

function appendLine(dir, category, line) {
  try {
    fs.appendFileSync(logPath(dir, category), line);
  } catch (e) {
    console.error(`[fileLogger] write failed (${category}.log): ${e.message}`);
  }
}

// write one log line. Always writes to <category>.log. Additionally:
//   - mirrors warn+error to <category>-error.log (per-category error file)
//   - mirrors error to global errors.log (aggregate)
//   - writes to any extra `mirrors` files (e.g. deals-sda for per-type split)
function write(level, category, action, fields, { mirrors = [] } = {}) {
  const conf = cfg();

  const minOrder = LEVEL_ORDER[conf.level] ?? 0;
  if ((LEVEL_ORDER[level] ?? 0) < minOrder) return;

  const formatter = conf.format === 'json' ? fmtJson : fmtText;
  const line = formatter(level, category, action, fields);

  appendLine(conf.dir, category, line);

  for (const m of mirrors) {
    if (m) appendLine(conf.dir, m, line);
  }

  // Per-category error file: warn + error from contacts/deals/sync go to
  // <category>-error.log so operators can scan failures without wading through
  // successful events. Skip categories that are themselves error sinks.
  if ((level === 'warn' || level === 'error')
      && category !== 'errors'
      && !category.endsWith('-error')) {
    appendLine(conf.dir, `${category}-error`, line);
  }

  // Global aggregate of every error across categories.
  if (level === 'error' && category !== 'errors') {
    appendLine(conf.dir, 'errors', line);
  }
}

const fileLogger = {
  startRun,

  // sync lifecycle logs → sync.log
  syncStart(runId) {
    write('info', 'sync', 'START', { runId });
  },

  syncComplete(runId, { contactsCreated, contactsUpdated, contactsSkipped, dealsCreated, dealsUpdated, durationS }) {
    write('info', 'sync', 'COMPLETE', {
      runId,
      contacts_created: contactsCreated,
      contacts_updated: contactsUpdated,
      contacts_skipped: contactsSkipped,
      deals_created:    dealsCreated,
      deals_updated:    dealsUpdated,
      duration_s:       durationS,
    });
  },

  syncError(runId, message) {
    write('error', 'sync', 'FAILED', { runId, reason: message });
  },

  syncResume(runId, phase) {
    write('info', 'sync', 'RESUME', { runId, phase });
  },

  phaseStart(runId, phase) {
    write('info', 'sync', 'PHASE_START', { runId, phase });
  },

  phaseComplete(runId, phase, fields = {}) {
    write('info', 'sync', 'PHASE_DONE', { runId, phase, ...fields });
  },

  phaseFailed(runId, phase, message) {
    write('error', 'sync', 'PHASE_FAIL', { runId, phase, reason: message });
  },

  deadLetter(runId, kind, count) {
    write('warn', 'sync', 'DEAD_LTR', { runId, kind, count });
  },

  // contact event logs → contacts.log
  contactCreated(runId, { email, hash, hubspotId }) {
    write('info', 'contacts', 'CREATED', { runId, email, hash, hubspot_id: hubspotId });
  },

  contactUpdated(runId, { email, hash, hubspotId }) {
    write('info', 'contacts', 'UPDATED', { runId, email, hash, hubspot_id: hubspotId });
  },

  contactSkipped(runId, { hash, reason }) {
    write('warn', 'contacts', 'SKIPPED', { runId, hash, reason });
  },

  // deal event logs → deals.log + deals-<type>.log (e.g. deals-sda.log)
  dealCreated(runId, { hash, contactId, dealId, dealname, dateOpened, type }) {
    const typeMirror = type ? `deals-${String(type).toLowerCase()}` : null;
    write('info', 'deals', 'CREATED', {
      runId,
      type,
      hash,
      contact_id:  contactId,
      deal_id:     dealId,
      dealname,
      date_opened: dateOpened,
    }, { mirrors: [typeMirror] });
  },

  dealUpdated(runId, { hash, dealId, type }) {
    const typeMirror = type ? `deals-${String(type).toLowerCase()}` : null;
    write('info', 'deals', 'UPDATED', { runId, type, hash, deal_id: dealId },
      { mirrors: [typeMirror] });
  },

  dealSkipped(runId, { hash, reason, type }) {
    const typeMirror = type ? `deals-${String(type).toLowerCase()}` : null;
    write('warn', 'deals', 'SKIPPED', { runId, type, hash, reason },
      { mirrors: [typeMirror] });
  },

  // general error logs → errors.log
  error(runId, { context, message }) {
    write('error', 'errors', 'ERROR', { runId, context, reason: message });
  },
};

module.exports = fileLogger;
