'use strict';

const SftpClient = require('ssh2-sftp-client');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { config } = require('../config/config');


function getTodayDateString(){
  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth()+1).padStart(2,"0");
  const dd = String(today.getDate()).padStart(2,"0");
  return `${mm}-${dd}-${yyyy}`;
}
 function fetchLatestTimeRecord(files) {
  let latestDda = null;
  let latestCif = null;

  function parseDateFromName(name) {
    const timePart = name.split(".")[1]; // "04-25-2026-10-30-11"
    if (!timePart) return null;

    const [MM, DD, YYYY, hh, mm, ss] = timePart.split("-").map(Number);

    return new Date(YYYY, MM - 1, DD, hh, mm, ss);
  }

  for (const file of files) {
    const name = file.name || file; // supports both cases
    const fileDate = parseDateFromName(name);

    if (!fileDate) continue;

    // DDA files
    if (name.includes("HubSpotDDADownload")) {
      if (!latestDda || fileDate > latestDda.date) {
        latestDda = { file, date: fileDate };
      }
    }

    // CIF files
    else if (name.includes("HubSpotDownload")) {
      if (!latestCif || fileDate > latestCif.date) {
        latestCif = { file, date: fileDate };
      }
    }
  }

  return {
    latestDda: latestDda?.file || null,
    latestCif: latestCif?.file || null
  };
}

async function fetchFilesFromSFTP() {
  const { host, port, user, password, privateKey, remoteDir, dataDir } = config.sftp;

  // Ensure the local download directory exists
  fs.mkdirSync(dataDir, { recursive: true });

  const sftp = new SftpClient();

  const connectOptions = {
    host,
    port,
    username: user,
  };

  // Prefer private-key auth when provided, fall back to password
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
    logger.info(`SFTP: listing remote directory "${remoteDir}"`);
    const listing = await sftp.list(remoteDir);
    const todayStr = getTodayDateString();
    const csvFiles = listing.filter((f) => f.type === '-' && f.name.includes(todayStr));

    if (csvFiles.length === 0) {
      throw new Error(`SFTP: no CSV files found in "${remoteDir}" — sync cannot proceed`);
    }

    logger.info(`SFTP: found ${csvFiles.length} file(s), selecting latest per type`);

    const { latestDda, latestCif } = fetchLatestTimeRecord(csvFiles);
    if (!latestCif) {
  throw new Error("No latest CIF file found for today");
}

if (!latestDda) {
  throw new Error("No latest DDA file found for today");
}

    const filesToDownload = [latestDda, latestCif].filter(Boolean);

     for (const file of filesToDownload) {
      const remotePath = `${remoteDir}/${file.name}`.replace(/\/\//g, '/');
      const localPath = path.resolve(dataDir, file.name);
      const tempPath = path.resolve(dataDir, `Temp-${file.name}`);

      await sftp.fastGet(remotePath, tempPath);
      fs.renameSync(tempPath, localPath);
      logger.info(`SFTP: downloaded "${file.name}" → ${localPath}`);

      // Map the file to CIF or DDA by filename pattern
      if (/hubspotdda|dda/i.test(file.name)) {
  downloaded.ddaPath = localPath;
} else if (/hubspotdownload|cif|contact/i.test(file.name)) {
  downloaded.cifPath = localPath;
} else {
        logger.warn(`SFTP: "${file.name}" does not match CIF or DDA patterns — saved but not mapped`);
      }
    }

    if (!downloaded.cifPath) {
      throw new Error(`SFTP: no CIF file found in "${remoteDir}" (expected filename matching: cif|contact|hubspotdownload)`);
    }
    if (!downloaded.ddaPath) {
      throw new Error(`SFTP: no DDA file found in "${remoteDir}" (expected filename matching: dda|hubspotdda)`);
    }
  } finally {
    await sftp.end();
    logger.info('SFTP: disconnected');
  }

  return downloaded;
}

module.exports = { fetchFilesFromSFTP };
