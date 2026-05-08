'use strict';

const LEVELS = { info: 'INFO', warn: 'WARN', error: 'ERROR' };

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

function log(level, message) {
  const timestamp = fmtTimestamp();
  console.log(`[${timestamp}] [${LEVELS[level]}] ${message}`);
}

const logger = {
  info: (msg) => log('info', msg),
  warn: (msg) => log('warn', msg),
  error: (msg) => log('error', msg),
};

module.exports = logger;
