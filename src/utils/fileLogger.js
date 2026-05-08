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

// write one log line, also mirror errors to errors.log
function write(level, category, action, fields) {
  const conf = cfg();

  // skip if below minimum log level
  const minOrder = LEVEL_ORDER[conf.level] ?? 0;
  if ((LEVEL_ORDER[level] ?? 0) < minOrder) return;

  const formatter = conf.format === 'json' ? fmtJson : fmtText;
  const line = formatter(level, category, action, fields);

  try {
    fs.appendFileSync(logPath(conf.dir, category), line);
  } catch (e) {
    console.error(`[fileLogger] write failed (${category}.log): ${e.message}`);
  }

  // also write errors to errors.log
  if (level === 'error' && category !== 'errors') {
    try {
      fs.appendFileSync(logPath(conf.dir, 'errors'), line);
    } catch (e) {
      console.error(`[fileLogger] write failed (errors.log): ${e.message}`);
    }
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

  // deal event logs → deals.log
  dealCreated(runId, { hash, contactId, dealId, dealname, dateOpened }) {
    write('info', 'deals', 'CREATED', {
      runId,
      hash,
      contact_id:  contactId,
      deal_id:     dealId,
      dealname,
      date_opened: dateOpened,
    });
  },

  dealUpdated(runId, { hash, dealId }) {
    write('info', 'deals', 'UPDATED', { runId, hash, deal_id: dealId });
  },

  // general error logs → errors.log
  error(runId, { context, message }) {
    write('error', 'errors', 'ERROR', { runId, context, reason: message });
  },
};

module.exports = fileLogger;
