import os from 'os';
import crypto from 'crypto';
import path from 'path';
import fs from 'fs';
import b from 'benny';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import DB from '@/DB';
import packageJson from '../package.json';

const logger = new Logger('DB1MiB Bench', LogLevel.WARN, [new StreamHandler()]);

async function main() {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'db-benches-'),
  );
  const dbPath = `${dataDir}/db`;
  const db = await DB.createDB({ dbPath, logger });
  const data0 = crypto.randomBytes(0);
  const data1MiB = crypto.randomBytes(1024 * 1024);
  const summary = await b.suite(
    'DB1MiB',
    b.add('get 1 MiB of data', async () => {
      await db.put('1mib', data1MiB, true);
      return async () => {
        await db.get('1mib', true);
      };
    }),
    b.add('put 1 MiB of data', async () => {
      await db.put('1mib', data1MiB, true);
    }),
    b.add('put zero data', async () => {
      await db.put('0', data0, true);
    }),
    b.add('put zero data then del', async () => {
      await db.put('0', data0, true);
      await db.del('0');
    }),
    b.cycle(),
    b.complete(),
    b.save({
      file: 'DB1MiB',
      folder: 'benches/results',
      version: packageJson.version,
      details: true,
    }),
    b.save({
      file: 'DB1MiB',
      folder: 'benches/results',
      format: 'chart.html',
    }),
  );
  await db.stop();
  await fs.promises.rm(dataDir, {
    force: true,
    recursive: true,
  });
  return summary;
}

if (require.main === module) {
  void main();
}

export default main;
