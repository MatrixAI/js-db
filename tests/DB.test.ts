import type { KeyPath } from '@/types';
import type { DBWorkerModule } from './workers/dbWorkerModule';
import os from 'os';
import path from 'path';
import fs from 'fs';
import nodeCrypto from 'crypto';
import lexi from 'lexicographic-integer';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import { WorkerManager } from '@matrixai/workers';
import { withF } from '@matrixai/resources';
import { spawn, Worker } from 'threads';
import DB from '@/DB';
import * as errors from '@/errors';
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
    dataDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'db-test-'));
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
    expect(await db.dump(['transactions'], false, true)).toStrictEqual([
      [['foo'], 'bar'],
    ]);
    await db.stop();
    // Should wipe the transaction state
    await db.start();
    expect(await db.dump(['transactions'], false, true)).toStrictEqual([]);
    await db.stop();
  });
  test('start performs canary check to validate key', async () => {
    const dbPath = `${dataDir}/db`;
    let db = await DB.createDB({ dbPath, crypto, logger });
    await db.stop();
    const crypto_ = {
      ...crypto,
      key: testUtils.generateKeySync(256),
    };
    await expect(
      DB.createDB({ dbPath, crypto: crypto_, logger }),
    ).rejects.toThrow(errors.ErrorDBKey);
    // Succeeds with the proper key
    db = await DB.createDB({ dbPath, crypto, logger });
    // Deliberately corrupt the canary
    await db._put(['canary'], 'bad ju ju');
    await db.stop();
    // Start will fail, the DB will still be stopped
    await expect(db.start()).rejects.toThrow(errors.ErrorDBKey);
    // DB is still corrupted at this point
    await expect(DB.createDB({ dbPath, crypto, logger })).rejects.toThrow(
      errors.ErrorDBKey,
    );
    // Must create fresh database
    db = await DB.createDB({ dbPath, crypto, logger, fresh: true });
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
    expect(
      await db.get([
        Buffer.concat([utils.sep, Buffer.from('level'), utils.sep]),
        'key',
      ]),
    ).toBeUndefined();
    await db.del([
      Buffer.concat([utils.sep, Buffer.from('level'), utils.sep]),
      'key',
    ]);
    const records: Array<[KeyPath, Buffer]> = [];
    for await (const [kP, v] of db.iterator(undefined, [
      Buffer.concat([utils.sep, Buffer.from('level')]),
    ])) {
      records.push([kP, v]);
    }
    expect(records).toStrictEqual([
      [[Buffer.from('key')], Buffer.from(JSON.stringify('value'))],
    ]);
    await db.stop();
  });
  test('keys that are empty arrays are converted to empty string', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put([], 'value');
    expect(await db.get([])).toBe('value');
    await db.del([]);
    expect(await db.get([])).toBeUndefined();
    await withF([db.transaction()], async ([tran]) => {
      await tran.put([], 'value');
      expect(await tran.get([])).toBe('value');
      await tran.del([]);
    });
    await withF([db.transaction()], async ([tran]) => {
      await tran.put([], 'value');
    });
    expect(await db.get([])).toBe('value');
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
    await db.put([], 'value0');
    await db.put(['a'], 'value1');
    await db.put(['', 'a'], 'value2');
    await db.put(['a', ''], 'value3');
    await db.put(['', 'a', ''], 'value4');
    await db.put(['level1', ''], 'value5');
    await db.put(['level1', 'a'], 'value6');
    await db.put(['level1', '', 'a'], 'value7');
    await db.put(['level1', 'level2', 'a'], 'value8');
    expect(await db.get([])).toBe('value0');
    expect(await db.get(['a'])).toBe('value1');
    expect(await db.get(['', 'a'])).toBe('value2');
    expect(await db.get(['a', ''])).toBe('value3');
    expect(await db.get(['', 'a', ''])).toBe('value4');
    expect(await db.get(['level1', ''])).toBe('value5');
    expect(await db.get(['level1', 'a'])).toBe('value6');
    expect(await db.get(['level1', '', 'a'])).toBe('value7');
    expect(await db.get(['level1', 'level2', 'a'])).toBe('value8');
    await db.clear(['level1']);
    expect(await db.get([])).toBe('value0');
    expect(await db.get(['a'])).toBe('value1');
    expect(await db.get(['', 'a'])).toBe('value2');
    expect(await db.get(['a', ''])).toBe('value3');
    expect(await db.get(['', 'a', ''])).toBe('value4');
    expect(await db.get(['level1', ''])).toBeUndefined();
    expect(await db.get(['level1', 'a'])).toBeUndefined();
    expect(await db.get(['level1', '', 'a'])).toBeUndefined();
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
    for await (const [kP] of db.iterator({
      keyAsBuffer: false,
      values: false,
    })) {
      keysIterated.push(kP[0] as string);
    }
    expect(keys).not.toEqual(keysIterated);
    expect(keys.sort()).toEqual(keysIterated);
    await db.stop();
  });
  test('lexicographic buffer iteration order', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keys: Array<Buffer> = Array.from({ length: 100 }, () =>
      nodeCrypto.randomBytes(testUtils.getRandomInt(0, 101)),
    );
    for (const k of keys) {
      await db.put(k, 'value');
    }
    const keysIterated: Array<Buffer> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keysIterated.push(kP[0] as Buffer);
    }
    expect(keys).not.toStrictEqual(keysIterated);
    keys.sort(Buffer.compare);
    expect(keys).toStrictEqual(keysIterated);
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
    for await (const [kP] of db.iterator({ values: false })) {
      keysIterated.push(lexi.unpack([...kP[0]]));
    }
    expect(keys).not.toEqual(keysIterated);
    // Numeric sort
    expect(keys.sort((a, b) => a - b)).toEqual(keysIterated);
    await db.stop();
  });
  test('sublevel lexicographic iteration', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keys1 = ['Q', 'fP', 'AQ', 'L'];
    for (const k of keys1) {
      await db.put(['level1', k], 'value1');
    }
    const keysIterated1: Array<string> = [];
    for await (const [kP] of db.iterator(
      { keyAsBuffer: false, values: false },
      ['level1'],
    )) {
      keysIterated1.push(kP[0] as string);
    }
    expect(keys1).not.toEqual(keysIterated1);
    expect(keys1.sort()).toEqual(keysIterated1);
    const keys2 = [100, 3, 4, 42];
    for (const k of keys2) {
      await db.put(['level2', Buffer.from(lexi.pack(k))], 'value2');
    }
    const keysIterated2: Array<number> = [];
    for await (const [kP] of db.iterator({ values: false }, ['level2'])) {
      keysIterated2.push(lexi.unpack([...kP[0]]));
    }
    expect(keys2).not.toEqual(keysIterated2);
    // Numeric sort
    expect(keys2.sort((a, b) => a - b)).toEqual(keysIterated2);
    await db.stop();
  });
  test('sublevel lexicographic buffer iteration', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keys: Array<Array<Buffer>> = Array.from({ length: 100 }, () =>
      Array.from({ length: testUtils.getRandomInt(0, 11) }, () =>
        nodeCrypto.randomBytes(testUtils.getRandomInt(0, 11)),
      ),
    );
    for (const k of keys) {
      await db.put(k, 'value');
    }
    const keysIterated: Array<Array<Buffer>> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keysIterated.push(kP as Array<Buffer>);
    }
    // Combine key parts so that keys can be compared
    const joinedKeysIterated: Array<Buffer> = [];
    for (let kI of keysIterated) {
      if (kI.length < 1) {
        kI = [Buffer.from([])];
      }
      const keyPart = kI.slice(-1)[0];
      const levelPath = kI.slice(0, -1);
      joinedKeysIterated.push(
        Buffer.concat([
          Buffer.concat(
            levelPath.map((p) => Buffer.concat([utils.sep, p, utils.sep])),
          ),
          keyPart,
        ]),
      );
    }
    const joinedKeys: Array<Buffer> = [];
    for (let k of keys) {
      if (k.length < 1) {
        k = [Buffer.from([])];
      }
      const keyPart = k.slice(-1)[0];
      const levelPath = k.slice(0, -1);
      joinedKeys.push(
        Buffer.concat([
          Buffer.concat(
            levelPath.map((p) => Buffer.concat([utils.sep, p, utils.sep])),
          ),
          keyPart,
        ]),
      );
    }
    expect(joinedKeys).not.toStrictEqual(joinedKeysIterated);
    joinedKeys.sort(Buffer.compare);
    expect(joinedKeys).toStrictEqual(joinedKeysIterated);
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
    let results: Array<[KeyPath, string]>;
    results = [];
    for await (const [kP, v] of db.iterator<string>({
      keyAsBuffer: false,
      valueAsBuffer: false,
    })) {
      results.push([kP, v]);
    }
    expect(results).toStrictEqual([
      [['level1', 'level2', 'a'], 'value0'],
      [['level1', 'level2', 'b'], 'value1'],
      [['level1', 'a'], 'value0'],
      [['level1', 'b'], 'value1'],
      [['a'], 'value0'],
      [['b'], 'value1'],
    ]);
    results = [];
    for await (const [kP, v] of db.iterator<string>(
      { keyAsBuffer: false, valueAsBuffer: false },
      ['level1'],
    )) {
      results.push([kP, v]);
    }
    expect(results).toStrictEqual([
      [['level2', 'a'], 'value0'],
      [['level2', 'b'], 'value1'],
      [['a'], 'value0'],
      [['b'], 'value1'],
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
