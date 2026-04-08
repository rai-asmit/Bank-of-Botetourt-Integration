async function cleanupFiles(filePaths) {
  for (const filePath of filePaths) {
    if (!filePath) continue;

    // Delete the final file
    try {
      await fs.promises.unlink(filePath);
      logger.info(`Deleted temporary file: ${filePath}`);
    } catch (e) {
      if (e.code !== 'ENOENT') {
        logger.error(`Failed to delete temporary file ${filePath}: ${e.message}`);
      }
    }

    // Delete any leftover Temp- file
    const tempPath = path.join(path.dirname(filePath), `Temp-${path.basename(filePath)}`);
    try {
      await fs.promises.unlink(tempPath);
      logger.info(`Deleted incomplete temp file: ${tempPath}`);
    } catch (e) {
      if (e.code !== 'ENOENT') {
        logger.error(`Failed to delete temp file ${tempPath}: ${e.message}`);
      }
    }
  }
}

module.exports = {cleanupFiles}