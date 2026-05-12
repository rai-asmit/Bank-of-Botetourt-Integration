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
    privateKey: process.env.SFTP_PRIVATE_KEY || '',
    remoteDir:  process.env.SFTP_REMOTE_DIR || '/',
    dataDir:    process.env.DATA_DIR        || './data',
  },
  state: {
    dir: process.env.STATE_DIR || './state',
  },
  api: {
    delayMs: 110,        
    maxRetries: 3,       // retry attempts
    retryDelayMs: 1000,  // base delay before retry 
    batchSize: 50,       // max records per batch API call (lower = fewer retries on a poison record)
    concurrency: 5,      // max parallel batch request
    searchConcurrency: 1, // CRM Search API 
  },
  syncCron: process.env.SYNC_CRON || '0 2 * * *',
  deals: {
    pipeline: process.env.DEAL_PIPELINE_ID || '',
    stage: process.env.DEAL_STAGE_ID || '',
  },
};

function validate() {
  if (!config.hubspot.accessToken) {
    throw new Error('Missing HUBSPOT_ACCESS_TOKEN in environment.');
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
  if (!config.deals.pipeline) {
    throw new Error('Missing DEAL_PIPELINE_ID in environment. Copy from HubSpot Settings → Deals → Pipelines → "..." → Copy ID.');
  }
  if (!config.deals.stage) {
    throw new Error('Missing DEAL_STAGE_ID in environment. Copy from HubSpot Settings → Deals → Pipelines (stage ID).');
  }
}

module.exports = { config, validate };
