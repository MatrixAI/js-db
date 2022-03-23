import os from 'os';
import path from 'path';
import fs from 'fs';
import b from 'benny';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import DB from '@/DB';
import packageJson from '../package.json';

const logger = new Logger('DBLevel Bench', LogLevel.WARN, [
  new StreamHandler(),
]);

async function main() {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'encryptedfs-benches-'),
  );
  const dbPath = `${dataDir}/db`;
  const db = await DB.createDB({ dbPath, logger });
  const summary = await b.suite(
    'DBLevel',
    b.add('create 1 sublevels', async () => {
      let level;
      for (let i = 0; i < 1; i++) {
        level = await db.level(`level${i}`, level);
      }
    }),
    b.add('create 2 sublevels', async () => {
      let level;
      for (let i = 0; i < 2; i++) {
        level = await db.level(`level${i}`, level);
      }
    }),
    b.add('create 3 sublevels', async () => {
      let level;
      for (let i = 0; i < 3; i++) {
        level = await db.level(`level${i}`, level);
      }
    }),
    b.add('create 4 sublevels', async () => {
      let level;
      for (let i = 0; i < 4; i++) {
        level = await db.level(`level${i}`, level);
      }
    }),
    b.add('get via sublevel', async () => {
      await db.put(['level0'], 'hello', 'world');
      return async () => {
        const level = await db.level('level0');
        await level.get('hello');
      };
    }),
    b.add('get via key path concatenation', async () => {
      await db.put(['level0'], 'hello', 'world');
      return async () => {
        await db.get(['level0'], 'hello');
      };
    }),
    b.cycle(),
    b.complete(),
    b.save({
      file: 'DBLevel',
      folder: 'benches/results',
      version: packageJson.version,
      details: true,
    }),
    b.save({
      file: 'DBLevel',
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
  (async () => {
    await main();
  })();
}

export default main;
