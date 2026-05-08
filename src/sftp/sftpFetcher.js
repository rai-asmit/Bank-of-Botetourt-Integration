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

// from a list of SFTP file entries, return the latest CIF and DDA files
function selectLatestFiles(files) {
  let latestCif = null;
  let latestDda = null;

  for (const file of files) {
    const name = file.name;
    const date = parseDateFromName(name);
    if (!date) continue;

    if (/hubspotdda/i.test(name)) {
      if (!latestDda || date > latestDda.date) latestDda = { file, date };
    } else if (/hubspotdownload/i.test(name)) {
      if (!latestCif || date > latestCif.date) latestCif = { file, date };
    }
  }

  return {
    latestCif: latestCif ? latestCif.file : null,
    latestDda: latestDda ? latestDda.file : null,
  };
}

async function fetchFilesFromSFTP() {
  const { host, port, user, password, privateKey, remoteDir, dataDir } = config.sftp;

  fs.mkdirSync(dataDir, { recursive: true });

  const sftp = new SftpClient();

  const connectOptions = { host, port, username: user };
  if (privateKey) {
    connectOptions.privateKey = fs.readFileSync(privateKey);
  } else {
    connectOptions.password = password;
  }

  logger.info(`SFTP: connecting to ${host}:${port} as ${user}`);
  await sftp.connect(connectOptions);
  logger.info('SFTP: connected');

  const downloaded = { cifPath: null, ddaPath: null };

  try {
    logger.info(`SFTP: listing "${remoteDir}"`);
    const listing = await sftp.list(remoteDir);

    // only regular files, skip directories
    const files = listing.filter((f) => f.type === '-');

    const { latestCif, latestDda } = selectLatestFiles(files);

    if (!latestCif) throw new Error(`SFTP: no CIF file found in "${remoteDir}"`);
    if (!latestDda) throw new Error(`SFTP: no DDA file found in "${remoteDir}"`);

    logger.info(`SFTP: selected CIF "${latestCif.name}", DDA "${latestDda.name}"`);

    for (const file of [latestCif, latestDda]) {
      const remotePath = path.posix.join(remoteDir, file.name);
      const localPath = path.resolve(dataDir, file.name);
      const tempPath = path.resolve(dataDir, `Temp-${file.name}`);

      await sftp.fastGet(remotePath, tempPath);
      fs.renameSync(tempPath, localPath);
      logger.info(`SFTP: downloaded "${file.name}"`);

      if (/hubspotdda/i.test(file.name)) {
        downloaded.ddaPath = localPath;
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
