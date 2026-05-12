'use strict';

const hubspot = require('@hubspot/api-client');
const { config } = require('../config/config');
const logger = require('../utils/logger');

let client = null;

function getClient() {
  if (!client) {
    client = new hubspot.Client({ accessToken: config.hubspot.accessToken });
  }
  return client;
}

// wait between API calls to avoid rate limit
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// call HubSpot API with retry on failure
async function callWithRetry(fn, label) {
  const { maxRetries, retryDelayMs, delayMs } = config.api;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await fn();
      await delay(delayMs);
      return result;
    } catch (err) {
      lastError = err;
      // SDK v11 ApiException: err.code is the numeric HTTP status.
      // FetchError / network errors: err.code is a string (ECONNRESET, etc.) or absent.
      const httpStatus = typeof err.code === 'number' ? err.code
        : (err.response && err.response.status);
      const isRateLimit   = httpStatus === 429;
      const isServerError = typeof httpStatus === 'number' && httpStatus >= 500;
      const isNetworkError = typeof err.code !== 'number' && !err.response; // socket hang up, ECONNRESET, etc.
      const isTransient = isRateLimit || isServerError || isNetworkError;

      if (!isTransient || attempt === maxRetries) break;

      const wait = retryDelayMs * Math.pow(2, attempt - 1);
      logger.warn(`[${label}] attempt ${attempt} failed (${err.message}), retrying in ${wait}ms...`);
      await delay(wait);
    }
  }

  throw lastError;
}

// split array into smaller chunks
function chunk(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

// run batches in parallel with a concurrency limit
async function runBatches(batches, concurrency, batchFn) {
  if (batches.length === 0) return [];
  let nextIdx = 0;
  const results = new Array(batches.length);

  async function worker() {
    while (true) {
      const idx = nextIdx++;
      if (idx >= batches.length) break;
      results[idx] = await batchFn(batches[idx]);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, batches.length) }, worker)
  );
  return results;
}

module.exports = { getClient, callWithRetry, chunk, runBatches };
