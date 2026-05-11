'use strict';

const { Router }                  = require('express');
const { triggerSync, getSyncStatus } = require('../sync/runner');

const router = Router();

router.post('/', async (_req, res) => {
  const result = await triggerSync();

  if (result.alreadyInProgress) {
    return res.status(409).json({ error: 'A sync is already in progress. Try again when it finishes.' });
  }
  if (result.error) {
    return res.status(500).json({ error: result.error, runId: result.runId });
  }
  return res.status(200).json({ runId: result.runId, stats: result.stats });
});

router.get('/status', (_req, res) => {
  res.json(getSyncStatus());
});

module.exports = router;
