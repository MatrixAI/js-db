#!/usr/bin/env ts-node

import fs from 'fs';
import path from 'path';
import si from 'systeminformation';
import DB1KiB from './db_1KiB.js';
import DB1MiB from './db_1MiB.js';

async function main(): Promise<void> {
  await fs.promises.mkdir(path.join(__dirname, 'results'), { recursive: true });
  await DB1KiB();
  await DB1MiB();
  const resultFilenames = await fs.promises.readdir(
    path.join(__dirname, 'results'),
  );
  const metricsFile = await fs.promises.open(
    path.join(__dirname, 'results', 'metrics.txt'),
    'w',
  );
  let concatenating = false;
  for (const resultFilename of resultFilenames) {
    if (/.+_metrics\.txt$/.test(resultFilename)) {
      const metricsData = await fs.promises.readFile(
        path.join(__dirname, 'results', resultFilename),
      );
      if (concatenating) {
        await metricsFile.write('\n');
      }
      await metricsFile.write(metricsData);
      concatenating = true;
    }
  }
  await metricsFile.close();
  const systemData = await si.get({
    cpu: '*',
    osInfo: 'platform, distro, release, kernel, arch',
    system: 'model, manufacturer',
  });
  await fs.promises.writeFile(
    path.join(__dirname, 'results', 'system.json'),
    JSON.stringify(systemData, null, 2),
  );
}

void main();
