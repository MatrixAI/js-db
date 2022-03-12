import type { DBOp } from '@/types';
import type { DBWorkerModule } from './workers/dbWorkerModule';

import os from 'os';
import path from 'path';
import fs from 'fs';
import nodeCrypto from 'crypto';
import lexi from 'lexicographic-integer';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import { WorkerManager } from '@matrixai/workers';
import { spawn, Worker } from 'threads';
import DB from '@/DB';
import * as utils from './utils';

describe('DB', () => {
  const logger = new Logger('DB Test', LogLevel.WARN, [new StreamHandler()]);
  const crypto = {
    key: utils.generateKeySync(256),
    ops: {
      encrypt: utils.encrypt,
      decrypt: utils.decrypt,
    },
  };
  let dataDir: string;
  beforeEach(async () => {
    dataDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), 'encryptedfs-test-'),
    );
  });
  afterEach(async () => {
    await fs.promises.rm(dataDir, {
      force: true,
      recursive: true,
    });
  });
  test('async construction constructs the filesystem state', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    const dbPathContents = await fs.promises.readdir(dbPath);
    expect(dbPathContents.length).toBeGreaterThan(1);
    await db.stop();
  });
  test('async destruction removes filesystem state', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    await db.stop();
    await db.destroy();
    await expect(fs.promises.readdir(dbPath)).rejects.toThrow(/ENOENT/);
  });
  test('async start and stop preserves state', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    // This is a noop
    await db.start();
    await db.put([], 'a', 'value0');
    await db.stop();
    await db.start();
    expect(await db.get([], 'a')).toBe('value0');
    await db.stop();
  });
  test('async start and stop preserves state without crypto', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    // This is a noop
    await db.start();
    await db.put([], 'a', 'value0');
    await db.stop();
    await db.start();
    expect(await db.get([], 'a')).toBe('value0');
    await db.stop();
  });
  test('async start and stop requires recreation of db levels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    await db.start();
    let level1 = await db.level('level1');
    await db.put(['level1'], 'key', 'value');
    await db.stop();
    await db.start();
    // The `level1` has to be recreated after `await db.stop()`
    await expect(db.level('level2', level1)).rejects.toThrow(
      /Inner database is not open/,
    );
    level1 = await db.level('level1');
    await db.level('level2', level1);
    expect(await db.get(['level1'], 'key')).toBe('value');
    await db.stop();
  });
  test('creating fresh db', async () => {
    const dbPath = `${dataDir}/db`;
    const db1 = await DB.createDB({ dbPath, logger });
    await db1.put([], 'key', 'value');
    await db1.stop();
    const db2 = await DB.createDB({ dbPath, logger, fresh: true });
    expect(await db2.get([], 'key')).toBeUndefined();
    await db2.stop();
  });
  test('get and put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.db.clear();
    await db.put([], 'a', 'value0');
    expect(await db.get([], 'a')).toBe('value0');
    await db.del([], 'a');
    expect(await db.get([], 'a')).toBeUndefined();
    await db.level('level1');
    await db.put(['level1'], 'a', 'value1');
    expect(await db.get(['level1'], 'a')).toBe('value1');
    await db.del(['level1'], 'a');
    expect(await db.get(['level1'], 'a')).toBeUndefined();
    await db.stop();
  });
  test('batch put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.batch([
      {
        type: 'put',
        domain: [],
        key: 'a',
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'b',
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'c',
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        domain: [],
        key: 'a',
      },
      {
        type: 'put',
        domain: [],
        key: 'd',
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get([], 'a')).toBeUndefined();
    expect(await db.get([], 'b')).toBe('value1');
    expect(await db.get([], 'c')).toBe('value2');
    expect(await db.get([], 'd')).toBe('value3');
    await db.stop();
  });
  test('db levels are leveldbs', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.db.put('a', await db.serializeEncrypt('value0', false));
    expect(await db.get([], 'a')).toBe('value0');
    await db.put([], 'b', 'value0');
    expect(await db.deserializeDecrypt(await db.db.get('b'), false)).toBe(
      'value0',
    );
    const level1 = await db.level('level1');
    await level1.put('a', await db.serializeEncrypt('value1', false));
    expect(await db.get(['level1'], 'a')).toBe('value1');
    await db.put(['level1'], 'b', 'value1');
    expect(await db.deserializeDecrypt(await level1.get('b'), false)).toBe(
      'value1',
    );
    await db.stop();
  });
  test('db levels are just ephemeral abstractions', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    // There's no need to actually create a sublevel instance
    // if you are always going to directly use the root
    // however it is useful if you need to iterate over a sublevel
    // plus you do not need to "destroy" a sublevel
    // clearing the entries is sufficient
    await db.put(['level1'], 'a', 'value1');
    expect(await db.get(['level1'], 'a')).toBe('value1');
    await db.del(['level1'], 'a');
    expect(await db.get(['level1'], 'a')).toBeUndefined();
    await db.stop();
  });
  test('db levels are facilitated by key prefixes', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    const level1 = await db.level('level1');
    const level2a = await db.level('100', level1);
    const level2b = await db.level('200', level1);
    let count;
    // Expect level1 to be empty
    count = 0;
    for await (const _ of level1.createKeyStream()) {
      count++;
    }
    expect(count).toBe(0);
    await level2a.put('a', await db.serializeEncrypt('value1', false));
    await level2b.put('b', await db.serializeEncrypt('value2', false));
    // There should be 2 entries at level1
    // because there is 1 entry for each sublevel
    count = 0;
    let keyToTest: string;
    for await (const k of level1.createKeyStream()) {
      // All keys are buffers
      keyToTest = k.toString('utf-8');
      count++;
    }
    expect(count).toBe(2);
    // It is possible to access sublevel entries from the upper level
    const valueToTest = await db.get<string>(['level1'], keyToTest!);
    expect(valueToTest).toBeDefined();
    // The level separator is set to `!`
    expect(keyToTest!).toBe('!200!b');
    await db.stop();
  });
  test('clearing a db level clears all sublevels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    const level1 = await db.level('level1');
    await db.level('level2', level1);
    await db.put([], 'a', 'value0');
    await db.put(['level1'], 'a', 'value1');
    await db.put(['level1', 'level2'], 'a', 'value2');
    expect(await db.get([], 'a')).toBe('value0');
    expect(await db.get(['level1'], 'a')).toBe('value1');
    expect(await db.get(['level1', 'level2'], 'a')).toBe('value2');
    await level1.clear();
    expect(await db.get([], 'a')).toBe('value0');
    expect(await db.get(['level1'], 'a')).toBeUndefined();
    expect(await db.get(['level1', 'level2'], 'a')).toBeUndefined();
    await db.stop();
  });
  test('lexicographic iteration order', async () => {
    // Leveldb stores keys in lexicographic order
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    // Sorted order [ 'AQ', 'L', 'Q', 'fP' ]
    const keys = ['Q', 'fP', 'AQ', 'L'];
    for (const k of keys) {
      await db.put([], k, 'value');
    }
    const keysIterated: Array<string> = [];
    for await (const k of db.db.createKeyStream()) {
      // Keys are buffers due to key encoding
      keysIterated.push(k.toString('utf-8'));
    }
    expect(keys).not.toEqual(keysIterated);
    expect(keys.sort()).toEqual(keysIterated);
    await db.stop();
  });
  test('lexicographic buffer iteration order', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keys: Array<Buffer> = Array.from({ length: 100 }, () =>
      nodeCrypto.randomBytes(3),
    );
    for (const k of keys) {
      await db.put([], k, 'value');
    }
    const keysIterated: Array<Buffer> = [];
    for await (const k of db.db.createKeyStream()) {
      keysIterated.push(k as Buffer);
    }
    expect(keys).not.toStrictEqual(keysIterated);
    expect(keys.sort(Buffer.compare)).toStrictEqual(keysIterated);
    // Buffers can be considered can be considered big-endian numbers
    const keysNumeric = keys.map(utils.bytes2BigInt);
    // Therefore lexicographic ordering of buffers is equal to numeric ordering of bytes
    expect(
      keysNumeric.slice(1).every((item, i) => keysNumeric[i] <= item),
    ).toBe(true);
    await db.stop();
  });
  test('lexicographic integer iteration', async () => {
    // Using the lexicographic-integer encoding
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    // Sorted order should be [3, 4, 42, 100]
    const keys = [100, 3, 4, 42];
    for (const k of keys) {
      await db.put([], Buffer.from(lexi.pack(k)), 'value');
    }
    const keysIterated: Array<number> = [];
    for await (const k of db.db.createKeyStream()) {
      // Keys are buffers due to key encoding
      keysIterated.push(lexi.unpack([...k]));
    }
    expect(keys).not.toEqual(keysIterated);
    // Numeric sort
    expect(keys.sort((a, b) => a - b)).toEqual(keysIterated);
    await db.stop();
  });
  test('db level lexicographic iteration', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    const level1 = await db.level('level1');
    const keys1 = ['Q', 'fP', 'AQ', 'L'];
    for (const k of keys1) {
      await level1.put(k, await db.serializeEncrypt('value1', false));
    }
    const keysIterated1: Array<string> = [];
    for await (const k of level1.createKeyStream()) {
      // Keys are buffers due to key encoding
      keysIterated1.push(k.toString('utf-8'));
    }
    expect(keys1).not.toEqual(keysIterated1);
    expect(keys1.sort()).toEqual(keysIterated1);
    const level2 = await db.level('level2');
    const keys2 = [100, 3, 4, 42];
    for (const k of keys2) {
      await level2.put(
        Buffer.from(lexi.pack(k)),
        await db.serializeEncrypt('value2', false),
      );
    }
    const keysIterated2: Array<number> = [];
    for await (const k of level2.createKeyStream()) {
      // Keys are buffers due to key encoding
      keysIterated2.push(lexi.unpack([...k]));
    }
    expect(keys2).not.toEqual(keysIterated2);
    // Numeric sort
    expect(keys2.sort((a, b) => a - b)).toEqual(keysIterated2);
    await db.stop();
  });
  test('get and put and del on string and buffer keys', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.db.clear();
    // 'string' is the same as Buffer.from('string')
    // even across levels
    await db.put([], 'string', 'value1');
    expect(await db.get([], 'string')).toBe('value1');
    expect(await db.get([], Buffer.from('string'))).toBe('value1');
    await db.del([], 'string');
    expect(await db.get([], 'string')).toBeUndefined();
    expect(await db.get([], Buffer.from('string'))).toBeUndefined();
    // Now using buffer keys across levels that are always strings
    await db.level('level1');
    await db.put(['level1'], 'string', 'value2');
    expect(await db.get(['level1'], 'string')).toBe('value2');
    // Level1 has been typed to use string keys
    // however the reality is that you can always use buffer keys
    // since strings and buffers get turned into buffers
    // so we can use buffer keys starting from root
    // we use this key type to enforce opaque types that are actually strings or buffers
    expect(await db.get(['level1'], Buffer.from('string'))).toBe('value2');
    await db.del(['level1'], Buffer.from('string'));
    expect(await db.get(['level1'], 'string')).toBeUndefined();
    expect(await db.get(['level1'], Buffer.from('string'))).toBeUndefined();
    await db.stop();
  });
  test('streams can be consumed with promises', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.put([], 'a', 'value0');
    await db.put([], 'b', 'value1');
    await db.put([], 'c', 'value2');
    await db.put([], 'd', 'value3');
    const keyStream = db.db.createKeyStream();
    const ops = await new Promise<Array<DBOp>>((resolve, reject) => {
      const ops: Array<DBOp> = [];
      keyStream.on('data', (k) => {
        ops.push({
          type: 'del',
          domain: [],
          key: k,
        });
      });
      keyStream.on('end', () => {
        resolve(ops);
      });
      keyStream.on('error', (e) => {
        reject(e);
      });
    });
    // Here we batch up the deletion
    await db.batch(ops);
    expect(await db.get([], 'a')).toBeUndefined();
    expect(await db.get([], 'b')).toBeUndefined();
    expect(await db.get([], 'c')).toBeUndefined();
    expect(await db.get([], 'd')).toBeUndefined();
    await db.stop();
  });
  test('counting sublevels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.put([], 'a', 'value0');
    await db.put([], 'b', 'value1');
    await db.put([], 'c', 'value2');
    await db.put([], 'd', 'value3');
    await db.put(['level1'], 'a', 'value0');
    await db.put(['level1'], 'b', 'value1');
    await db.put(['level1'], 'c', 'value2');
    await db.put(['level1'], 'd', 'value3');
    await db.put(['level1', 'level11'], 'a', 'value0');
    await db.put(['level1', 'level11'], 'b', 'value1');
    await db.put(['level1', 'level11'], 'c', 'value2');
    await db.put(['level1', 'level11'], 'd', 'value3');
    await db.put(['level2'], 'a', 'value0');
    await db.put(['level2'], 'b', 'value1');
    await db.put(['level2'], 'c', 'value2');
    await db.put(['level2'], 'd', 'value3');
    const level1 = await db.level('level1');
    const level11 = await db.level('level11', level1);
    const level2 = await db.level('level2');
    expect(await db.count(level1)).toBe(8);
    expect(await db.count(level11)).toBe(4);
    expect(await db.count(level2)).toBe(4);
    expect(await db.count()).toBe(16);
    await db.stop();
  });
  test('parallelized get and put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const workerManager =
      await WorkerManager.createWorkerManager<DBWorkerModule>({
        workerFactory: () => spawn(new Worker('./workers/dbWorker')),
        cores: 1,
        logger,
      });
    db.setWorkerManager(workerManager);
    await db.start();
    await db.db.clear();
    await db.put([], 'a', 'value0');
    expect(await db.get([], 'a')).toBe('value0');
    await db.del([], 'a');
    expect(await db.get([], 'a')).toBeUndefined();
    await db.level('level1');
    await db.put(['level1'], 'a', 'value1');
    expect(await db.get(['level1'], 'a')).toBe('value1');
    await db.del(['level1'], 'a');
    expect(await db.get(['level1'], 'a')).toBeUndefined();
    await db.stop();
    await workerManager.destroy();
  });
  test('parallelized batch put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const workerManager =
      await WorkerManager.createWorkerManager<DBWorkerModule>({
        workerFactory: () => spawn(new Worker('./workers/dbWorker')),
        cores: 4,
        logger,
      });
    db.setWorkerManager(workerManager);
    await db.start();
    await db.batch([
      {
        type: 'put',
        domain: [],
        key: 'a',
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'b',
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'c',
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        domain: [],
        key: 'a',
      },
      {
        type: 'put',
        domain: [],
        key: 'd',
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get([], 'a')).toBeUndefined();
    expect(await db.get([], 'b')).toBe('value1');
    expect(await db.get([], 'c')).toBe('value2');
    expect(await db.get([], 'd')).toBe('value3');
    await db.stop();
    await workerManager.destroy();
  });
  test('works without crypto', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    await db.start();
    await db.db.clear();
    await db.put([], 'a', 'value0');
    expect(await db.get([], 'a')).toBe('value0');
    await db.del([], 'a');
    expect(await db.get([], 'a')).toBeUndefined();
    await db.level('level1');
    await db.put(['level1'], 'a', 'value1');
    expect(await db.get(['level1'], 'a')).toBe('value1');
    await db.del(['level1'], 'a');
    expect(await db.get(['level1'], 'a')).toBeUndefined();
    await db.batch([
      {
        type: 'put',
        domain: [],
        key: 'a',
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'b',
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        domain: [],
        key: 'c',
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        domain: [],
        key: 'a',
      },
      {
        type: 'put',
        domain: [],
        key: 'd',
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get([], 'a')).toBeUndefined();
    expect(await db.get([], 'b')).toBe('value1');
    expect(await db.get([], 'c')).toBe('value2');
    expect(await db.get([], 'd')).toBe('value3');
    await db.stop();
  });
});
