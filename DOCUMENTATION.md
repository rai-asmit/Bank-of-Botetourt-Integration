# Bank of Botetourt → HubSpot Sync — Documentation

A single, scan-friendly reference for how this project works end to end: the flow, the data, the functions, and the APIs.

---

## 1. What this project does

Nightly sync from the bank's **Fiserv SFTP** drop into **HubSpot CRM**.

- **CIF** file → HubSpot **Contacts**.
- **DDA / CD / LNA / SDA** files → HubSpot **Deals**, each associated with the customer's Contact.

The pipeline is designed to survive mid-flight crashes, poison records, and Fiserv CSV quirks — without re-doing work that already succeeded.

---

## 2. High-level flow chart

```
                ┌──────────────────┐
   cron 02:00   │   index.js       │   POST /sync (manual)
   ──────────►  │  + Express :3000 │  ◄────────────
                └─────────┬────────┘
                          │ triggerSync()
                          ▼
                ┌──────────────────┐
                │ sync/runner.js   │  ◄── resumes from checkpoint if active run exists
                └─────────┬────────┘
                          │
        ┌─────────────────┴──────────────────┐
        ▼                                    ▼
 ┌──────────────┐                  ┌──────────────────┐
 │   SFTP       │  download 5 CSVs │  data/*.csv      │
 │ sftpFetcher  │ ───────────────► │  (CIF, DDA, CD,  │
 └──────────────┘                  │   LNA, SDA)      │
                                   └────────┬─────────┘
                                            │ parsers/*Parser.js
                                            ▼
                                   ┌──────────────────┐
                                   │ in-memory rows   │
                                   │ contacts[], Maps │
                                   └────────┬─────────┘
                                            │ sync/orchestrator.js
                                            ▼
                          ┌──────── CONTACTS phase ────────┐
                          │ search → update → create        │
                          │ services/contactService.js      │
                          └────────────────┬────────────────┘
                                           │ contactIdMap (hash → HubSpot ID)
                                           ▼
                          ┌──────── DEALS phase ────────────┐
                          │ DDA → CD → LNA → SDA            │
                          │ sync/dealSync.js                │
                          │ services/dealService.js         │
                          └────────────────┬────────────────┘
                                           │
                                           ▼
                          ┌──────── HubSpot CRM API ────────┐
                          │ via @hubspot/api-client          │
                          │ wrapped by resilientBatch        │
                          └──────────────────────────────────┘

  side outputs at every step:
    logs/         ← human + JSON event logs (fileLogger)
    dead-letter/  ← JSONL of permanently failed records
    state/        ← checkpoint JSON (resume after crash)
```

---

## 3. Phase pipeline

Phases are defined in [src/state/checkpoint.js:9-20](src/state/checkpoint.js#L9-L20). Each is independently checkpointed; on restart the run resumes at the last unfinished phase.

| # | Phase | Driver | Input | Output |
|---|---|---|---|---|
| 1 | **DOWNLOAD** | [runner.js](src/sync/runner.js) + [sftpFetcher.js](src/sftp/sftpFetcher.js) | SFTP listing | 5 local CSVs in `data/` |
| 2 | **PARSE** | [orchestrator.js](src/sync/orchestrator.js) + `parsers/*` | CSVs | `cifRows[]`, `ddaMap`, `cdMap`, `lnaMap`, `sdaMap` |
| 3 | **CONTACTS_SEARCH** | orchestrator + [contactService.js](src/services/contactService.js) | hashed tax IDs | `existingContactsMap`; seeds `state.contactIdMap` |
| 4 | **CONTACTS_UPDATE** | orchestrator + [resilientBatch](src/services/resilientBatch.js) | rows with known HubSpot ID | updated contacts |
| 5 | **CONTACTS_CREATE** | orchestrator + resilientBatch | rows without HubSpot ID | created contacts (409 conflicts auto-converted to updates) |
| 6 | **DEALS_DDA** | [dealSync.js](src/sync/dealSync.js) | `ddaMap` + `contactIdMap` | created/updated deals |
| 7 | **DEALS_CD** | dealSync.js | `cdMap` | created/updated deals |
| 8 | **DEALS_LNA** | dealSync.js | `lnaMap` | created/updated deals |
| 9 | **DEALS_SDA** | dealSync.js | `sdaMap` | created/updated deals |
| 10 | **COMPLETE** | runner.js | — | files cleaned up, checkpoint cleared |

Within each phase, `runPhase()` ([src/sync/orchestrator.js:54-72](src/sync/orchestrator.js#L54-L72)) wraps the work in a try/catch: a failure marks the phase `failed_partial` and the next phase still runs.

---

## 4. Data transfer

```
Fiserv server                Local disk (data/)              HubSpot
─────────────                ──────────────────              ───────
HubSpotDownload.*  ──SFTP──► CIF csv ──parseCifFile──► contacts[]
HubSpotDDA*        ──SFTP──► DDA csv ──parseDdaFile──► ddaMap        Contact (taxidhashed)
HubSpotCD*         ──SFTP──► CD  csv ──parseCdFile ──► cdMap            └── associated to ──┐
HubSpotLNA*        ──SFTP──► LNA csv ──parseLnaFile──► lnaMap                                ▼
HubSpotSDA*        ──SFTP──► SDA csv ──parseSdaFile──► sdaMap        Deal (taxidhashed,
                                                                            date_opened,
   join key throughout the pipeline: SHA-256(taxId)                         account_last_4)
```

**Key transforms**

| Input | Transform | Output | Where |
|---|---|---|---|
| Tax ID (plaintext) | SHA-256, first 16 hex chars | `taxidhashed` (HubSpot property) | [src/utils/hash.js](src/utils/hash.js) |
| Fiserv `MMDDYY` strings | `parseFiservDate` | epoch ms | [src/utils/dateUtils.js](src/utils/dateUtils.js) |
| Two-row CSV header | `buildColMap` | column-name → index | [src/utils/colMap.js](src/utils/colMap.js) |
| Unescaped quotes (`"O"Brien"`) | `escapeFiservCsv` (state machine) | properly-escaped CSV | [src/utils/csvPreprocess.js](src/utils/csvPreprocess.js) |
| Whole-file CSV parse | `safeParseCsv` (line-by-line) | rows; one bad line is skipped, not fatal | [src/utils/safeCsvParse.js](src/utils/safeCsvParse.js) |

---

## 5. HTTP API (Express on port 3000)

| Method | Path | Handler | Purpose |
|---|---|---|---|
| `POST` | `/sync` | `routes/sync.js` → `triggerSync()` | Trigger a run. Returns `{ alreadyInProgress: true }` if one is active. |
| `GET` | `/sync/status` | `getSyncStatus()` | Returns `{ syncInProgress, lastSyncResult, activeRun }` (runId, phase, per-phase status, stats). |
| `GET` | `/health` | server.js | Health probe. |

Schedule: `SYNC_CRON` env var, default `0 2 * * *` (02:00 daily). See [src/config/config.js:34](src/config/config.js#L34).

---

## 6. HubSpot CRM API surface

All HubSpot calls go through one shared client + retry wrapper in [src/services/hubspotClient.js](src/services/hubspotClient.js).
`callWithRetry` does exponential backoff on `429` and `5xx` (`retryDelayMs * 2^(attempt-1)`).

| HubSpot endpoint | Wrapper function | Used by |
|---|---|---|
| `crm.contacts.searchApi.doSearch` — filter `taxidhashed IN [...]` | `searchContactsByHashes` | CONTACTS_SEARCH |
| `crm.contacts.searchApi.doSearch` — filter `email IN [...]` | `searchContactsByEmails` | CONTACTS_SEARCH (email fallback) |
| `crm.contacts.batchApi.create` | `batchCreateContacts` | CONTACTS_CREATE |
| `crm.contacts.batchApi.update` | `batchUpdateContacts` | CONTACTS_UPDATE |
| `crm.contacts.basicApi.create` | `createSingleContact` | one-by-one fallback; 409 → existing-ID update |
| `crm.contacts.basicApi.update` | `updateSingleContact` | one-by-one fallback |
| `crm.deals.searchApi.doSearch` — filter `taxidhashed IN [...]`, paginated | `searchDealsByHashes` + `batchSearchDeals` | every DEALS_* phase |
| `crm.deals.batchApi.create` (with `HUBSPOT_DEFINED` association `typeId: 3` to the contact) | `batchCreateDeals` | every DEALS_* phase |
| `crm.deals.batchApi.update` | `batchUpdateDeals` | every DEALS_* phase |
| `crm.deals.basicApi.create` / `.update` | `createSingleDeal` / `updateSingleDeal` | one-by-one fallback |
| `crm.associations.v4.batchApi.create` | `batchAssociateDeals` | **defined but unused** — association is already inline in `batchCreateDeals` |

---

## 7. Identity & deduplication

| What | Rule | Where |
|---|---|---|
| **Contact identity** | `taxidhashed` is primary; `email` is the fallback if hash search misses. | [src/sync/orchestrator.js:163-190](src/sync/orchestrator.js#L163-L190) |
| **409 duplicate email** at create | Regex-parse `Existing ID: <n>` from HubSpot's error message and convert the create into an update. | [src/services/contactService.js:43-79](src/services/contactService.js#L43-L79) |
| **Deal identity** | Composite key `taxidhashed | date_opened | account_last_4`. Search by hash, match in memory. | [src/services/dealService.js:141-158](src/services/dealService.js#L141-L158) |
| **Pre-filter at parse** | Drop rows with no email or placeholder emails (`@none.none`, `@noemail`, `@test.test`, anything not matching the regex). | [src/sync/orchestrator.js:39-49](src/sync/orchestrator.js#L39-L49) |
| **In-batch dedup** | `dedupeUpdates` (by deal ID) and `dedupeCreates` (by composite key) collapse duplicates before sending — HubSpot rejects the whole batch on dup IDs. | [src/sync/dealSync.js:19-58](src/sync/dealSync.js#L19-L58) |
| **CIF dedup** | If the same `taxIdHashed` appears twice, last-wins, preferring rows that have an email. | [src/sync/orchestrator.js:74-83](src/sync/orchestrator.js#L74-L83) |

---

## 8. Resilience layer

Each piece earns its keep — and they compose into one defensive stack.

| Component | File | Job |
|---|---|---|
| `resilientBatch` | [src/services/resilientBatch.js](src/services/resilientBatch.js) | Run an op in batches. If a whole batch fails, retry the items **one-by-one**. Each permanent failure → dead-letter. Heartbeat every 15s or every 5% so silence never looks like a hang. Checkpoints `doneBatches / succeeded / failed` after each batch. |
| `checkpoint` | [src/state/checkpoint.js](src/state/checkpoint.js) | Atomic JSON writes to `state/run-<id>.json` + `state/current.json` pointer. Tracks per-phase status, file metadata, `contactIdMap`, stats. |
| `deadLetter` | [src/utils/deadLetter.js](src/utils/deadLetter.js) | Appends one JSON line per permanently-failed record to `logs/yyyy/mm/dd/dead-letter.jsonl` with reason + payload. |
| `hubspotError.summarize` | [src/utils/hubspotError.js](src/utils/hubspotError.js) | Turns a multi-kB HubSpot SDK error into a one-line human summary (`invalid email "x@y"`, `rate-limited (HTTP 429)`, etc.). |
| `safeCsvParse` | [src/utils/safeCsvParse.js](src/utils/safeCsvParse.js) | Parses each CSV data line independently; a malformed row is logged and skipped, not fatal. |
| `csvPreprocess.escapeFiservCsv` | [src/utils/csvPreprocess.js](src/utils/csvPreprocess.js) | State-machine fixup for Fiserv's unescaped internal double-quotes (e.g. `"O"Brien"`). |
| **File reuse on resume** | [src/sftp/sftpFetcher.js:59-110](src/sftp/sftpFetcher.js#L59-L110) | If checkpointed files still exist on disk with the same size, skip SFTP entirely. |
| **Resume on boot** | [src/index.js:35-39](src/index.js#L35-L39) | At startup, if `state/current.json` points to an unfinished run, fire `triggerSync()` immediately rather than waiting for cron. |

---

## 9. File / directory layout

```
src/
  index.js               entry: validate env, start server, resume pending run
  server.js              Express app + routes
  cron.js                schedules POST /sync via node-cron
  config/config.js       env loading + validate()

  sync/
    runner.js            triggerSync(): DOWNLOAD → runSync → cleanup
    orchestrator.js      runSync(): PARSE + CONTACTS_* + DEALS_*
    dealSync.js          generic deal sync for one account type

  services/
    hubspotClient.js     SDK client, callWithRetry, chunk, runBatches
    contactService.js    contact search/create/update + single fallbacks
    dealService.js       deal search/create/update + 4 property builders
    resilientBatch.js    batch with one-by-one fallback + dead-letter

  parsers/               cifParser, ddaParser, cdParser, lnaParser, sdaParser
                         — one per Fiserv file

  state/checkpoint.js    JSON state with atomic writes

  sftp/sftpFetcher.js    SFTP listing + selective download

  utils/                 logger, fileLogger, hash, dateUtils, colMap,
                         deadLetter, hubspotError, csvPreprocess,
                         safeCsvParse, cleanupFiles

data/                    downloaded CSVs (cleaned up after a successful run)
logs/yyyy/mm/dd/         human log + JSON events + dead-letter.jsonl
state/                   current.json + run-<id>.json
```

---

## 10. Configuration (env vars)

From [src/config/config.js](src/config/config.js):

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HUBSPOT_ACCESS_TOKEN` | yes | — | HubSpot private-app token |
| `SFTP_HOST` / `SFTP_USER` | yes | — | SFTP connection |
| `SFTP_PASSWORD` **or** `SFTP_PRIVATE_KEY` | yes | — | SFTP auth (one of) |
| `SFTP_PORT` | no | `22` | |
| `SFTP_REMOTE_DIR` | no | `/` | where the 5 Fiserv files live |
| `DATA_DIR` | no | `./data` | local CSV cache |
| `STATE_DIR` | no | `./state` | checkpoint files |
| `LOG_DIR` / `LOG_FORMAT` / `LOG_LEVEL` | no | `./logs` / `text` / `info` | log destination + format + level |
| `DEAL_PIPELINE_ID` / `DEAL_STAGE_ID` | yes | — | HubSpot pipeline + stage for newly created deals |
| `SYNC_CRON` | no | `0 2 * * *` | schedule |
| `PORT` | no | `3000` | Express port |

Hard-coded knobs (in `config.api`): `batchSize=50`, `concurrency=5`, `searchConcurrency=1`, `delayMs=110`, `maxRetries=3`, `retryDelayMs=1000` (exponential backoff base).

---

## 11. Where to look when something is wrong

- **Run ID** is `r-YYYYMMDD-HHMMSS`, minted by `fileLogger.startRun()`.
- `logs/yyyy/mm/dd/sync-<runId>.log` — human log.
- `logs/yyyy/mm/dd/events-<runId>.jsonl` — structured events (phase start/complete/failed, contact/deal created/updated/skipped).
- `logs/yyyy/mm/dd/dead-letter.jsonl` — every payload that permanently failed, with the reason.
- `state/run-<runId>.json` — live checkpoint (per-phase status, totals, `contactIdMap`).
- `state/current.json` — pointer to the active run. **Absent when nothing is in flight.**

Common signals in the human log:
- `⚠️  [<label>] batch of 50 rejected — <reason> — retrying one-by-one` — a single record probably poisoned the batch; the one-by-one loop will isolate it.
- `[<label>] dead-letter — <reason>` — a record was given up on and written to the dead-letter file.
- `Found unfinished run r-... at phase ... — resuming on startup` — the previous process crashed mid-run; this boot is picking up where it left off.
- `Deal search returned HTTP 400` — the `taxidhashed` property is not searchable on the **Deals** object in this HubSpot portal; the code treats every deal as new for this run.

---

## 12. Maintenance notes (real footguns)

- `parseNumber` is reimplemented in each of the 5 parsers (cif/dda/cd/lna/sda) — fixes need to land in all five.
- `buildDealProperties`, `buildCdDealProperties`, `buildLnaDealProperties`, `buildSdaDealProperties` are near-duplicates ([src/services/dealService.js:161-254](src/services/dealService.js#L161-L254)).
- `batchAssociateDeals` ([src/services/dealService.js:116-128](src/services/dealService.js#L116-L128)) is exported but **never imported** — association is already inlined in `batchCreateDeals`.
- `fmtTimestamp` is duplicated in `utils/logger.js` and `utils/fileLogger.js`.
- The 409 duplicate-email resolver depends on the literal string `Existing ID: <n>` in HubSpot's error message. If HubSpot ever changes the wording, the regex returns null and the resolver re-throws.
- `taxidhashed` must exist as a property on **both** Contacts and Deals in the portal, and it must be **searchable** on each. Without this, sync degrades silently (deal search returns 400 → all deals treated as new).
