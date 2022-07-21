import os from 'os';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import b from 'benny';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import DB from '@/DB';
import { suiteCommon } from './utils';

const logger = new Logger('DB1KiB Bench', LogLevel.WARN, [new StreamHandler()]);

async function main() {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'db-benches-'),
  );
  const dbPath = `${dataDir}/db`;
  const db = await DB.createDB({ dbPath, logger });
  const data0 = crypto.randomBytes(0);
  const data1KiB = crypto.randomBytes(1024);
  const summary = await b.suite(
    path.basename(__filename, path.extname(__filename)),
    b.add('get 1 KiB of data', async () => {
      await db.put('1kib', data1KiB, true);
      return async () => {
        await db.get('1kib', true);
      };
    }),
    b.add('put 1 KiB of data', async () => {
      await db.put('1kib', data1KiB, true);
    }),
    b.add('put zero data', async () => {
      await db.put('0', data0, true);
    }),
    b.add('put zero data then del', async () => {
      await db.put('0', data0, true);
      await db.del('0');
    }),
    ...suiteCommon,
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
