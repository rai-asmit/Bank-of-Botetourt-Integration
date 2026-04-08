'use strict';

const LEVELS = { info: 'INFO', warn: 'WARN', error: 'ERROR' };

function log(level, message) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${LEVELS[level]}] ${message}`);
}

const logger = {
  info: (msg) => log('info', msg),
  warn: (msg) => log('warn', msg),
  error: (msg) => log('error', msg),
};

module.exports = logger;
