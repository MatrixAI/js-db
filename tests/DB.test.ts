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
import * as utils from '@/utils';
import * as testUtils from './utils';

describe(DB.name, () => {
  const logger = new Logger(`${DB.name} Test`, LogLevel.WARN, [
    new StreamHandler(),
  ]);
  const crypto = {
    key: testUtils.generateKeySync(256),
    ops: {
      encrypt: testUtils.encrypt,
      decrypt: testUtils.decrypt,
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
    await db.put('a', 'value0');
    await db.stop();
    await db.start();
    expect(await db.get('a')).toBe('value0');
    await db.stop();
  });
  test('async start and stop preserves state without crypto', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    // This is a noop
    await db.start();
    await db.put('a', 'value0');
    await db.stop();
    await db.start();
    expect(await db.get('a')).toBe('value0');
    await db.stop();
  });
  test('creating fresh db', async () => {
    const dbPath = `${dataDir}/db`;
    const db1 = await DB.createDB({ dbPath, logger });
    await db1.put('key', 'value');
    await db1.stop();
    const db2 = await DB.createDB({ dbPath, logger, fresh: true });
    expect(await db2.get('key')).toBeUndefined();
    await db2.stop();
  });
  test('start wipes dirty transaction state', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const data = await db.serializeEncrypt('bar', false);
    // Put in dirty transaction state
    await db.db.put(utils.keyPathToKey(['transactions', 'foo']), data);
    expect(await db.dump(['transactions'])).toStrictEqual([['foo', 'bar']]);
    await db.stop();
    // Should wipe the transaction state
    await db.start();
    expect(await db.dump(['transactions'])).toStrictEqual([]);
    await db.dump();
    await db.stop();
  });
  test('get and put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put('a', 'value0');
    expect(await db.get('a')).toBe('value0');
    await db.del('a');
    expect(await db.get('a')).toBeUndefined();
    await db.put(['level1', 'a'], 'value1');
    expect(await db.get(['level1', 'a'])).toBe('value1');
    await db.del(['level1', 'a']);
    expect(await db.get(['level1', 'a'])).toBeUndefined();
    await db.stop();
  });
  test('get and put and del on string and buffer keys', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    // 'string' is the same as Buffer.from('string')
    // even across levels
    await db.put('string', 'value1');
    expect(await db.get('string')).toBe('value1');
    expect(await db.get(Buffer.from('string'))).toBe('value1');
    await db.del('string');
    expect(await db.get('string')).toBeUndefined();
    expect(await db.get(Buffer.from('string'))).toBeUndefined();
    // Now using buffer keys across levels that are always strings
    await db.put(['level1', 'string'], 'value2');
    expect(await db.get(['level1', 'string'])).toBe('value2');
    expect(await db.get(['level1', Buffer.from('string')])).toBe('value2');
    await db.del(['level1', Buffer.from('string')]);
    expect(await db.get(['level1', 'string'])).toBeUndefined();
    expect(await db.get(['level1', Buffer.from('string')])).toBeUndefined();
    await db.stop();
  });
  test('levels can contain separator buffer', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put(
      [Buffer.concat([utils.sep, Buffer.from('level')]), 'key'],
      'value',
    );
    await db.get([Buffer.concat([Buffer.from('level'), utils.sep]), 'key']),
      await db.del([
        Buffer.concat([utils.sep, Buffer.from('level'), utils.sep]),
        'key',
      ]);
    const records: Array<[Buffer, Buffer]> = [];
    for await (const [k, v] of db.iterator(undefined, [
      Buffer.concat([utils.sep, Buffer.from('level')]),
    ])) {
      records.push([k, v]);
    }
    expect(records).toStrictEqual([
      [Buffer.from('key'), Buffer.from(JSON.stringify('value'))],
    ]);
    await db.stop();
  });
  test('keys can contain separator buffer', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put(utils.sep, 'value');
    await db.put(
      ['level', Buffer.concat([utils.sep, Buffer.from('key')])],
      'value',
    );
    await db.put(
      ['level', Buffer.concat([Buffer.from('key'), utils.sep])],
      'value',
    );
    await db.put(
      ['level', Buffer.concat([utils.sep, Buffer.from('key'), utils.sep])],
      'value',
    );
    await db.put(['level', utils.sep], 'value');
    await db.put(
      ['level', Buffer.concat([utils.sep, Buffer.from('key')])],
      'value',
    );
    await db.put(
      ['level', Buffer.concat([Buffer.from('key'), utils.sep])],
      'value',
    );
    await db.put(
      ['level', Buffer.concat([utils.sep, Buffer.from('key'), utils.sep])],
      'value',
    );
    expect(await db.get(utils.sep)).toBe('value');
    expect(
      await db.get(['level', Buffer.concat([utils.sep, Buffer.from('key')])]),
    ).toBe('value');
    expect(
      await db.get(['level', Buffer.concat([Buffer.from('key'), utils.sep])]),
    ).toBe('value');
    expect(
      await db.get([
        'level',
        Buffer.concat([utils.sep, Buffer.from('key'), utils.sep]),
      ]),
    ).toBe('value');
    expect(await db.get(['level', utils.sep])).toBe('value');
    expect(
      await db.get(['level', Buffer.concat([utils.sep, Buffer.from('key')])]),
    ).toBe('value');
    expect(
      await db.get(['level', Buffer.concat([Buffer.from('key'), utils.sep])]),
    ).toBe('value');
    expect(
      await db.get([
        'level',
        Buffer.concat([utils.sep, Buffer.from('key'), utils.sep]),
      ]),
    ).toBe('value');
    await db.del(utils.sep);
    await db.del(['level', Buffer.concat([utils.sep, Buffer.from('key')])]);
    await db.del(['level', Buffer.concat([Buffer.from('key'), utils.sep])]);
    await db.del([
      'level',
      Buffer.concat([utils.sep, Buffer.from('key'), utils.sep]),
    ]);
    await db.del(['level', utils.sep]);
    await db.del(['level', Buffer.concat([utils.sep, Buffer.from('key')])]);
    await db.del(['level', Buffer.concat([Buffer.from('key'), utils.sep])]);
    await db.del([
      'level',
      Buffer.concat([utils.sep, Buffer.from('key'), utils.sep]),
    ]);
    await db.stop();
  });
  test('clearing a db level clears all sublevels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put(['a'], 'value0');
    await db.put(['level1', 'a'], 'value1');
    await db.put(['level1', 'level2', 'a'], 'value2');
    expect(await db.get('a')).toBe('value0');
    expect(await db.get(['level1', 'a'])).toBe('value1');
    expect(await db.get(['level1', 'level2', 'a'])).toBe('value2');
    await db.clear(['level1']);
    expect(await db.get(['a'])).toBe('value0');
    expect(await db.get(['level1', 'a'])).toBeUndefined();
    expect(await db.get(['level1', 'level2', 'a'])).toBeUndefined();
    await db.stop();
  });
  test('lexicographic iteration order', async () => {
    // Leveldb stores keys in lexicographic order
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    // Sorted order [ 'AQ', 'L', 'Q', 'fP' ]
    const keys = ['Q', 'fP', 'AQ', 'L'];
    for (const k of keys) {
      await db.put(k, 'value');
    }
    const keysIterated: Array<string> = [];
    for await (const [k] of db.iterator({ values: false })) {
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
      await db.put(k, 'value');
    }
    const keysIterated: Array<Buffer> = [];
    for await (const [k] of db.iterator({ values: false })) {
      keysIterated.push(k);
    }
    expect(keys).not.toStrictEqual(keysIterated);
    expect(keys.sort(Buffer.compare)).toStrictEqual(keysIterated);
    // Buffers can be considered can be considered big-endian numbers
    const keysNumeric = keys.map(testUtils.bytes2BigInt);
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
    // Sorted order should be [3, 4, 42, 100]
    const keys = [100, 3, 4, 42];
    for (const k of keys) {
      await db.put(Buffer.from(lexi.pack(k)), 'value');
    }
    const keysIterated: Array<number> = [];
    for await (const [k] of db.iterator({ values: false })) {
      // Keys are buffers due to key encoding
      keysIterated.push(lexi.unpack([...k]));
    }
    expect(keys).not.toEqual(keysIterated);
    // Numeric sort
    expect(keys.sort((a, b) => a - b)).toEqual(keysIterated);
    await db.stop();
  });
  test('sublevel lexicographic iteration', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    // Const level1 = await db.level('level1');
    const keys1 = ['Q', 'fP', 'AQ', 'L'];
    for (const k of keys1) {
      await db.put(['level1', k], 'value1');
    }
    const keysIterated1: Array<string> = [];
    for await (const [k] of db.iterator({ values: false }, ['level1'])) {
      // Keys are buffers due to key encoding
      keysIterated1.push(k.toString('utf-8'));
    }
    expect(keys1).not.toEqual(keysIterated1);
    expect(keys1.sort()).toEqual(keysIterated1);
    // Const level2 = await db.level('level2');
    const keys2 = [100, 3, 4, 42];
    for (const k of keys2) {
      await db.put(['level2', Buffer.from(lexi.pack(k))], 'value2');
    }
    const keysIterated2: Array<number> = [];
    for await (const [k] of db.iterator({ values: false }, ['level2'])) {
      // Keys are buffers due to key encoding
      keysIterated2.push(lexi.unpack([...k]));
    }
    expect(keys2).not.toEqual(keysIterated2);
    // Numeric sort
    expect(keys2.sort((a, b) => a - b)).toEqual(keysIterated2);
    await db.stop();
  });
  test('iterating sublevels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put('a', 'value0');
    await db.put('b', 'value1');
    await db.put(['level1', 'a'], 'value0');
    await db.put(['level1', 'b'], 'value1');
    await db.put(['level1', 'level2', 'a'], 'value0');
    await db.put(['level1', 'level2', 'b'], 'value1');
    let results: Array<[string, string]>;
    results = [];
    for await (const [k, v] of db.iterator()) {
      results.push([k.toString(), JSON.parse(v.toString())]);
    }
    expect(results).toStrictEqual([
      [
        `${utils.sep.toString('binary')}level1${utils.sep.toString(
          'binary',
        )}${utils.sep.toString('binary')}level2${utils.sep.toString(
          'binary',
        )}a`,
        'value0',
      ],
      [
        `${utils.sep.toString('binary')}level1${utils.sep.toString(
          'binary',
        )}${utils.sep.toString('binary')}level2${utils.sep.toString(
          'binary',
        )}b`,
        'value1',
      ],
      [
        `${utils.sep.toString('binary')}level1${utils.sep.toString('binary')}a`,
        'value0',
      ],
      [
        `${utils.sep.toString('binary')}level1${utils.sep.toString('binary')}b`,
        'value1',
      ],
      ['a', 'value0'],
      ['b', 'value1'],
    ]);
    results = [];
    for await (const [k, v] of db.iterator(undefined, ['level1'])) {
      results.push([k.toString(), JSON.parse(v.toString())]);
    }
    expect(results).toStrictEqual([
      [`${utils.sep}level2${utils.sep}a`, 'value0'],
      [`${utils.sep}level2${utils.sep}b`, 'value1'],
      ['a', 'value0'],
      ['b', 'value1'],
    ]);
    await db.stop();
  });
  test('counting sublevels', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.put('a', 'value0');
    await db.put('b', 'value1');
    await db.put('c', 'value2');
    await db.put('d', 'value3');
    await db.put(['level1', 'a'], 'value0');
    await db.put(['level1', 'b'], 'value1');
    await db.put(['level1', 'c'], 'value2');
    await db.put(['level1', 'd'], 'value3');
    await db.put(['level1', 'level11', 'a'], 'value0');
    await db.put(['level1', 'level11', 'b'], 'value1');
    await db.put(['level1', 'level11', 'c'], 'value2');
    await db.put(['level1', 'level11', 'd'], 'value3');
    await db.put(['level2', 'a'], 'value0');
    await db.put(['level2', 'b'], 'value1');
    await db.put(['level2', 'c'], 'value2');
    await db.put(['level2', 'd'], 'value3');
    expect(await db.count(['level1'])).toBe(8);
    expect(await db.count(['level1', 'level11'])).toBe(4);
    expect(await db.count(['level2'])).toBe(4);
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
    await db.put('a', 'value0');
    expect(await db.get('a')).toBe('value0');
    await db.del('a');
    expect(await db.get('a')).toBeUndefined();
    await db.put(['level1', 'a'], 'value1');
    expect(await db.get(['level1', 'a'])).toBe('value1');
    await db.del(['level1', 'a']);
    expect(await db.get(['level1', 'a'])).toBeUndefined();
    await db.stop();
    await workerManager.destroy();
  });
  test('batch put and del', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.start();
    await db.batch([
      {
        type: 'put',
        keyPath: ['a'],
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        keyPath: 'b',
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        keyPath: ['c'],
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        keyPath: 'a',
      },
      {
        type: 'put',
        keyPath: 'd',
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get('a')).toBeUndefined();
    expect(await db.get('b')).toBe('value1');
    expect(await db.get('c')).toBe('value2');
    expect(await db.get('d')).toBe('value3');
    await db.stop();
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
        keyPath: ['a'],
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        keyPath: 'b',
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        keyPath: ['c'],
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        keyPath: 'a',
      },
      {
        type: 'put',
        keyPath: 'd',
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get('a')).toBeUndefined();
    expect(await db.get('b')).toBe('value1');
    expect(await db.get('c')).toBe('value2');
    expect(await db.get('d')).toBe('value3');
    await db.stop();
    await workerManager.destroy();
  });
  test('works without crypto', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, logger });
    await db.start();
    await db.put('a', 'value0');
    expect(await db.get('a')).toBe('value0');
    await db.del('a');
    expect(await db.get('a')).toBeUndefined();
    await db.put(['level1', 'a'], 'value1');
    expect(await db.get(['level1', 'a'])).toBe('value1');
    await db.del(['level1', 'a']);
    expect(await db.get(['level1', 'a'])).toBeUndefined();
    await db.batch([
      {
        type: 'put',
        keyPath: ['a'],
        value: 'value0',
        raw: false,
      },
      {
        type: 'put',
        keyPath: ['b'],
        value: 'value1',
        raw: false,
      },
      {
        type: 'put',
        keyPath: ['c'],
        value: 'value2',
        raw: false,
      },
      {
        type: 'del',
        keyPath: ['a'],
      },
      {
        type: 'put',
        keyPath: ['d'],
        value: 'value3',
        raw: false,
      },
    ]);
    expect(await db.get('a')).toBeUndefined();
    expect(await db.get('b')).toBe('value1');
    expect(await db.get('c')).toBe('value2');
    expect(await db.get('d')).toBe('value3');
    await db.stop();
  });
});
