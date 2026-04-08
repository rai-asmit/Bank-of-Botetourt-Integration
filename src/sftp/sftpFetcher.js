'use strict';

const SftpClient = require('ssh2-sftp-client');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { config } = require('../config/config');


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
    const csvFiles = listing.filter((f) => f.type === '-' && f.name.toLowerCase().endsWith('.csv'));

    if (csvFiles.length === 0) {
      throw new Error(`SFTP: no CSV files found in "${remoteDir}" — sync cannot proceed`);
    }

    logger.info(`SFTP: found ${csvFiles.length} CSV file(s) — downloading`);

    for (const file of csvFiles) {
      const remotePath = `${remoteDir}/${file.name}`.replace(/\/\//g, '/');
      const localPath = path.resolve(dataDir, file.name);
      const tempPath = path.resolve(dataDir, `Temp-${file.name}`);

      await sftp.fastGet(remotePath, tempPath);
      fs.renameSync(tempPath, localPath);
      logger.info(`SFTP: downloaded "${file.name}" → ${localPath}`);

      // Map the file to CIF or DDA by filename pattern
      if (/cif|contact|hubspotdownload/i.test(file.name)) {
        downloaded.cifPath = localPath;
        logger.info(`SFTP: identified "${file.name}" as CIF file`);
      } else if (/dda|hubspotdda/i.test(file.name)) {
        downloaded.ddaPath = localPath;
        logger.info(`SFTP: identified "${file.name}" as DDA file`);
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
