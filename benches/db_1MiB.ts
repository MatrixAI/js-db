import os from 'os';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import url from "node:url";
import b from 'benny';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import DB from '#DB.js';
import { suiteCommon } from './utils/utils.js';

const logger = new Logger('DB1MiB Bench', LogLevel.WARN, [new StreamHandler()]);
const filename = url.fileURLToPath(new URL(import.meta.url));

async function main() {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'db-benches-'),
  );
  const dbPath = `${dataDir}/db`;
  const db = await DB.createDB({ dbPath, logger });
  const data0 = crypto.randomBytes(0);
  const data1MiB = crypto.randomBytes(1024 * 1024);
  const summary = await b.suite(
    path.basename(filename, path.extname(filename)),
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
    ...suiteCommon,
  );
  await db.stop();
  await fs.promises.rm(dataDir, {
    force: true,
    recursive: true,
  });
  return summary;
}

if (process.argv[1] === url.fileURLToPath(import.meta.url)) {
  void main();
}

export default main;
