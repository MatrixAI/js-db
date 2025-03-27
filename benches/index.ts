#!/usr/bin/env ts-node

import fs from 'node:fs';
import path from 'node:path';
import url from 'node:url';
import si from 'systeminformation';
import { benchesPath } from './utils/utils.js';
import DB1KiB from './db_1KiB.js';
import DB1MiB from './db_1MiB.js';

async function main(): Promise<void> {
  await fs.promises.mkdir(path.join(benchesPath, 'results'), {
    recursive: true,
  });
  await DB1KiB();
  await DB1MiB();
  const resultFilenames = await fs.promises.readdir(
    path.join(benchesPath, 'results'),
  );
  const metricsFile = await fs.promises.open(
    path.join(benchesPath, 'results', 'metrics.txt'),
    'w',
  );
  let concatenating = false;
  for (const resultFilename of resultFilenames) {
    if (/.+_metrics\.txt$/.test(resultFilename)) {
      const metricsData = await fs.promises.readFile(
        path.join(benchesPath, 'results', resultFilename),
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
    path.join(benchesPath, 'results', 'system.json'),
    JSON.stringify(systemData, null, 2),
  );
}

if (import.meta.url.startsWith('file:')) {
  const modulePath = url.fileURLToPath(import.meta.url);
  if (process.argv[1] === modulePath) {
    void main();
  }
}

export default main;
