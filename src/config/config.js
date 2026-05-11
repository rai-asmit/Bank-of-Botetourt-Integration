'use strict';

require('dotenv').config();

const config = {
  hubspot: {
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN,
  },
  logging: {
    dir:    process.env.LOG_DIR    || './logs',
    format: process.env.LOG_FORMAT || 'text',  // 'text' | 'json'
    level:  process.env.LOG_LEVEL  || 'info',  // 'info' | 'warn' | 'error'
  },
  sftp: {
    host:       process.env.SFTP_HOST       || '',
    port:       parseInt(process.env.SFTP_PORT || '22', 10),
    user:       process.env.SFTP_USER       || '',
    password:   process.env.SFTP_PASSWORD   || '',
    privateKey: process.env.SFTP_PRIVATE_KEY || '', // path to private key file (optional)
    remoteDir:  process.env.SFTP_REMOTE_DIR || '/', // remote directory to list/download from
    dataDir:    process.env.DATA_DIR        || './data', // local folder for downloaded files
  },
  api: {
    delayMs: 110,        // ms between HubSpot API calls (rate limit: ~10 req/s)
    maxRetries: 3,       // retry attempts on transient errors
    retryDelayMs: 1000,  // base delay before retry (doubles each attempt)
    batchSize: 100,      // max records per batch API call (HubSpot limit)
    concurrency: 5,      // max parallel batch requests in-flight at once (write APIs)
    searchConcurrency: 1, // CRM Search API has a tighter per-second limit — keep serial
  },
  syncCron: process.env.SYNC_CRON || '0 2 * * *',
  deals: {
    // Pipeline internal ID from HubSpot (Settings → Deals → Pipelines → "..." → Copy ID)
    pipeline: process.env.DEAL_PIPELINE_ID || 'default',
    // Stage internal ID within the pipeline (copy from the same settings page)
    stage: process.env.DEAL_STAGE_ID || 'appointmentscheduled',
  },
};

function validate() {
  if (!config.hubspot.accessToken) {
    throw new Error('Missing HUBSPOT_ACCESS_TOKEN in environment. Copy .env.example → .env and fill it in.');
  }

  if (!config.sftp.host) {
    throw new Error('Missing SFTP_HOST in environment.');
  }
  if (!config.sftp.user) {
    throw new Error('Missing SFTP_USER in environment.');
  }
  if (!config.sftp.password && !config.sftp.privateKey) {
    throw new Error('Missing SFTP_PASSWORD or SFTP_PRIVATE_KEY in environment.');
  }
}

module.exports = { config, validate };
