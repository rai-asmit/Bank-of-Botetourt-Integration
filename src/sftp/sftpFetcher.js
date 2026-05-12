'use strict';

const SftpClient = require('ssh2-sftp-client');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { config } = require('../config/config');

// parse timestamp from filename like "HubSpotDownload.04-25-2026-00-01-22"
// returns a Date or null if the name doesn't match the expected format
function parseDateFromName(name) {
  const timePart = name.split('.')[1]; // "04-25-2026-00-01-22"
  if (!timePart) return null;

  const parts = timePart.split('-').map(Number);
  if (parts.length < 3 || parts.some(isNaN)) return null;

  const [MM, DD, YYYY, hh = 0, mm = 0, ss = 0] = parts;
  return new Date(Date.UTC(YYYY, MM - 1, DD, hh, mm, ss));
}

// from a list of SFTP file entries, return the latest CIF, DDA, CD, LNA, and SDA files
function selectLatestFiles(files) {
  let latestCif = null;
  let latestDda = null;
  let latestCd  = null;
  let latestLna = null;
  let latestSda = null;

  for (const file of files) {
    const name = file.name;
    const date = parseDateFromName(name);
    if (!date) continue;

    if (/hubspotdda/i.test(name)) {
      if (!latestDda || date > latestDda.date) latestDda = { file, date };
    } else if (/hubspotlna/i.test(name)) {
      if (!latestLna || date > latestLna.date) latestLna = { file, date };
    } else if (/hubspotsda/i.test(name)) {
      if (!latestSda || date > latestSda.date) latestSda = { file, date };
    } else if (/hubspotcd/i.test(name)) {
      if (!latestCd  || date > latestCd.date)  latestCd  = { file, date };
    } else if (/hubspotdownload/i.test(name)) {
      if (!latestCif || date > latestCif.date) latestCif = { file, date };
    }
  }

  return {
    latestCif: latestCif ? latestCif.file : null,
    latestDda: latestDda ? latestDda.file : null,
    latestCd:  latestCd  ? latestCd.file  : null,
    latestLna: latestLna ? latestLna.file : null,
    latestSda: latestSda ? latestSda.file : null,
  };
}

// returns the local path for a previously-downloaded file if it still matches
// the size recorded in the checkpoint; otherwise null.
function reusableLocalPath(meta) {
  if (!meta || !meta.path) return null;
  try {
    const stat = fs.statSync(meta.path);
    if (stat.size === meta.size) return meta.path;
  } catch (_) { /* missing or unreadable */ }
  return null;
}

/**
 * Fetch the latest CIF/DDA/CD/LNA/SDA files from SFTP. If `state.files`
 * already records files that are still on disk with the same size, those
 * are reused and we don't hit SFTP for them. If everything can be reused,
 * we skip the SFTP connection entirely.
 */
async function fetchFilesFromSFTP(state = null) {
  const { host, port, user, password, privateKey, remoteDir, dataDir } = config.sftp;
  fs.mkdirSync(dataDir, { recursive: true });

  const reuse = {
    cifPath: state ? reusableLocalPath(state.files && state.files.cif) : null,
    ddaPath: state ? reusableLocalPath(state.files && state.files.dda) : null,
    cdPath:  state ? reusableLocalPath(state.files && state.files.cd)  : null,
    lnaPath: state ? reusableLocalPath(state.files && state.files.lna) : null,
    sdaPath: state ? reusableLocalPath(state.files && state.files.sda) : null,
  };

  // If CIF and DDA (the two required files) are both reusable, skip SFTP entirely.
  // Optional files: if state says they existed and disk has them, reuse; if
  // state says they didn't exist (null), keep null without consulting SFTP.
  if (state && reuse.cifPath && reuse.ddaPath) {
    const hasCifMeta = !!(state.files && state.files.cif);
    const hasDdaMeta = !!(state.files && state.files.dda);
    const hasCdMeta  = !!(state.files && state.files.cd);
    const hasLnaMeta = !!(state.files && state.files.lna);
    const hasSdaMeta = !!(state.files && state.files.sda);

    const cdOk  = !hasCdMeta  || !!reuse.cdPath;
    const lnaOk = !hasLnaMeta || !!reuse.lnaPath;
    const sdaOk = !hasSdaMeta || !!reuse.sdaPath;

    if (hasCifMeta && hasDdaMeta && cdOk && lnaOk && sdaOk) {
      logger.info('SFTP: all files reusable from checkpoint, skipping download');
      return {
        cifPath: reuse.cifPath,
        ddaPath: reuse.ddaPath,
        cdPath:  hasCdMeta  ? reuse.cdPath  : null,
        lnaPath: hasLnaMeta ? reuse.lnaPath : null,
        sdaPath: hasSdaMeta ? reuse.sdaPath : null,
      };
    }
  }

  const sftp = new SftpClient();

  const connectOptions = { host, port, username: user, keepaliveInterval: 10000, keepaliveCountMax: 3 };
  if (privateKey) {
    connectOptions.privateKey = fs.readFileSync(privateKey);
  } else {
    connectOptions.password = password;
  }

  logger.info(`SFTP: connecting to ${host}:${port} as ${user}`);
  await sftp.connect(connectOptions);
  logger.info('SFTP: connected');

  const downloaded = {
    cifPath: reuse.cifPath,
    ddaPath: reuse.ddaPath,
    cdPath:  reuse.cdPath,
    lnaPath: reuse.lnaPath,
    sdaPath: reuse.sdaPath,
  };

  try {
    logger.info(`SFTP: listing "${remoteDir}"`);
    const listing = await sftp.list(remoteDir);

    // only regular files, skip directories
    const files = listing.filter((f) => f.type === '-');

    const { latestCif, latestDda, latestCd, latestLna, latestSda } = selectLatestFiles(files);

    if (!downloaded.cifPath && !latestCif) throw new Error(`SFTP: no CIF file found in "${remoteDir}"`);
    if (!downloaded.ddaPath && !latestDda) throw new Error(`SFTP: no DDA file found in "${remoteDir}"`);

    const filesToDownload = [];
    if (!downloaded.cifPath) filesToDownload.push(latestCif);
    else                     logger.info(`SFTP: reusing CIF "${path.basename(downloaded.cifPath)}" from checkpoint`);

    if (!downloaded.ddaPath) filesToDownload.push(latestDda);
    else                     logger.info(`SFTP: reusing DDA "${path.basename(downloaded.ddaPath)}" from checkpoint`);

    if (!downloaded.cdPath && latestCd) {
      filesToDownload.push(latestCd);
    } else if (downloaded.cdPath) {
      logger.info(`SFTP: reusing CD "${path.basename(downloaded.cdPath)}" from checkpoint`);
    } else {
      logger.warn(`SFTP: no CD file found in "${remoteDir}" — CD sync will be skipped`);
    }

    if (!downloaded.lnaPath && latestLna) {
      filesToDownload.push(latestLna);
    } else if (downloaded.lnaPath) {
      logger.info(`SFTP: reusing LNA "${path.basename(downloaded.lnaPath)}" from checkpoint`);
    } else {
      logger.warn(`SFTP: no LNA file found in "${remoteDir}" — LNA sync will be skipped`);
    }

    if (!downloaded.sdaPath && latestSda) {
      filesToDownload.push(latestSda);
    } else if (downloaded.sdaPath) {
      logger.info(`SFTP: reusing SDA "${path.basename(downloaded.sdaPath)}" from checkpoint`);
    } else {
      logger.warn(`SFTP: no SDA file found in "${remoteDir}" — SDA sync will be skipped`);
    }

    for (const file of filesToDownload) {
      const remotePath = path.posix.join(remoteDir, file.name);
      const localPath = path.resolve(dataDir, file.name);
      const tempPath = path.resolve(dataDir, `Temp-${file.name}`);

      try {
        await sftp.fastGet(remotePath, tempPath);
      } catch (err) {
        if (!err.message.includes('ECONNRESET')) throw err;
        logger.warn(`SFTP: connection reset downloading "${file.name}", reconnecting…`);
        try { await sftp.end(); } catch (_) {}
        await sftp.connect(connectOptions);
        await sftp.fastGet(remotePath, tempPath);
      }

      fs.renameSync(tempPath, localPath);
      logger.info(`SFTP: downloaded "${file.name}"`);

      if (/hubspotdda/i.test(file.name)) {
        downloaded.ddaPath = localPath;
      } else if (/hubspotlna/i.test(file.name)) {
        downloaded.lnaPath = localPath;
      } else if (/hubspotsda/i.test(file.name)) {
        downloaded.sdaPath = localPath;
      } else if (/hubspotcd/i.test(file.name)) {
        downloaded.cdPath = localPath;
      } else {
        downloaded.cifPath = localPath;
      }
    }
  } finally {
    await sftp.end();
    logger.info('SFTP: disconnected');
  }

  return downloaded;
}

module.exports = { fetchFilesFromSFTP };
