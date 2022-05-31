import type { LevelDB } from 'level';
import type { KeyPath } from '@/types';
import type { DBWorkerModule } from './workers/dbWorkerModule';
import os from 'os';
import path from 'path';
import fs from 'fs';
import nodeCrypto from 'crypto';
import nodeUtil from 'util';
import lexi from 'lexicographic-integer';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import { WorkerManager } from '@matrixai/workers';
import { withF } from '@matrixai/resources';
import { spawn, Worker } from 'threads';
import level from 'level';
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
  test('get and put on empty buffer and empty string', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put('string', '');
    expect(await db.get('string')).toStrictEqual('');
    await db.put('buffer', Buffer.from([]), true);
    expect(await db.get('buffer', true)).toStrictEqual(Buffer.from([]));
    await db.stop();
  });
  test('get and put fuzzing', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keyPaths: Array<KeyPath> = Array.from({ length: 1000 }, () =>
      Array.from({ length: testUtils.getRandomInt(0, 11) }, () =>
        nodeCrypto.randomBytes(testUtils.getRandomInt(0, 11)),
      ),
    );
    for (const kP of keyPaths) {
      const value = Buffer.concat(kP as Array<Buffer>);
      await db.put(kP, value, true);
      expect(await db.get(kP, true)).toStrictEqual(value);
    }
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
  test('internal db lexicographic iteration order', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await new Promise<LevelDB<string | Buffer, Buffer>>(
      (resolve, reject) => {
        const db = level(
          dbPath,
          {
            keyEncoding: 'binary',
            valueEncoding: 'binary',
          },
          (e) => {
            if (e) {
              reject(e);
            } else {
              resolve(db);
            }
          },
        );
      },
    );
    await db.put(Buffer.from([0x01]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00, 0x00]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00]), Buffer.alloc(0));
    // The empty key is not supported in leveldb
    // However in this DB, empty keys are always put under root level of `data`
    // therefore empty keys are supported
    // await db_.put(Buffer.from([]), Buffer.alloc(0));
    const keys: Array<Buffer> = [];
    // @ts-ignore Outdated types
    for await (const [k] of db.iterator()) {
      keys.push(k);
    }
    expect(keys).toStrictEqual([
      // Therefore `aa` is earlier than `aaa`
      Buffer.from([0x00, 0x00]),
      Buffer.from([0x00, 0x00, 0x00]),
      // Therefore `aa` is earlier than `z`
      Buffer.from([0x01]),
    ]);
    await db.close();
  });
  test('lexicographic iteration order', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    await db.put(Buffer.from([0x01]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00, 0x00]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00]), Buffer.alloc(0));
    await db.put(Buffer.from([]), Buffer.alloc(0));
    const keyPaths: Array<KeyPath> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toStrictEqual([
      // Therefore empty buffer sorts first
      [Buffer.from([])],
      // Therefore `aa` is earlier than `aaa`
      [Buffer.from([0x00, 0x00])],
      [Buffer.from([0x00, 0x00, 0x00])],
      // Therefore `aa` is earlier than `z`
      [Buffer.from([0x01])],
    ]);
    // Check that this matches Buffer.compare order
    const keyPaths_ = [...keyPaths];
    keyPaths_.sort((kP1: Array<Buffer>, kP2: Array<Buffer>) => {
      // Only concatenate the key paths
      const k1 = Buffer.concat(kP1);
      const k2 = Buffer.concat(kP2);
      return Buffer.compare(k1, k2);
    });
    expect(keyPaths_).toStrictEqual(keyPaths);
    await db.stop();
  });
  test('lexicographic iteration order fuzzing', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keys: Array<Buffer> = Array.from({ length: 1000 }, () =>
      nodeCrypto.randomBytes(testUtils.getRandomInt(0, 101)),
    );
    for (const k of keys) {
      await db.put(k, 'value');
    }
    const keyPaths: Array<KeyPath> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keyPaths.push(kP);
    }
    // Check that this matches Buffer.compare order
    const keyPaths_ = [...keyPaths];
    keyPaths_.sort((kP1: Array<Buffer>, kP2: Array<Buffer>) => {
      // Only concatenate the key paths
      const k1 = Buffer.concat(kP1);
      const k2 = Buffer.concat(kP2);
      return Buffer.compare(k1, k2);
    });
    expect(keyPaths_).toStrictEqual(keyPaths);
    await db.stop();
  });
  test('lexicographic integer iteration order', async () => {
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
  test('lexicographic level iteration order', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    // With levels and empty keys, the sorting is more complicated
    await db.put([Buffer.from([0x01])], Buffer.alloc(0));
    await db.put(
      [Buffer.from([0x00, 0x00]), Buffer.from([0x00, 0x00])],
      Buffer.alloc(0),
    );
    await db.put(
      [Buffer.from([0x00, 0x00, 0x00]), Buffer.from([0x00])],
      Buffer.alloc(0),
    );
    await db.put(
      [Buffer.from([0x00, 0x00]), Buffer.from([0x01])],
      Buffer.alloc(0),
    );
    await db.put(
      [Buffer.from([0x00, 0x00, 0x00]), Buffer.from([0x01])],
      Buffer.alloc(0),
    );
    await db.put([Buffer.from([0x01]), Buffer.from([0x00])], Buffer.alloc(0));
    await db.put([Buffer.from([0x00]), Buffer.from([0x00])], Buffer.alloc(0));
    await db.put([Buffer.from([0x00, 0x00])], Buffer.alloc(0));
    await db.put([Buffer.from([0x00, 0x00]), ''], Buffer.alloc(0));
    await db.put([Buffer.from([0xff]), ''], Buffer.alloc(0));
    await db.put([Buffer.from([0x00]), ''], Buffer.alloc(0));
    await db.put([Buffer.from([])], Buffer.alloc(0));
    await db.put([Buffer.from([]), Buffer.from([])], Buffer.alloc(0));
    await db.put([Buffer.from([0x00])], Buffer.alloc(0));
    await db.put(
      [Buffer.from([0x00, 0x00]), Buffer.from([0xff]), Buffer.from([])],
      Buffer.alloc(0),
    );
    await db.put(
      [Buffer.from([0x00, 0x00]), Buffer.from([]), Buffer.from([])],
      Buffer.alloc(0),
    );
    const keyPaths: Array<KeyPath> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keyPaths.push(kP);
    }
    /**
     * Suppose that:
     *
     * * `[]` is a key path of degree 0
     * * `['a']` is a key path of degree 0
     * * `['a', 'b']` is a key path of degree 1
     *
     * The sorting process goes through 3 steps in-order:
     *
     * 1. Level parts at each degree are sorted lexicographically
     * 2. Key parts with the same level path are sorted lexicographically
     * 3. Key parts with degree n are sorted in front of key parts with degree n -1
     */
    expect(keyPaths).toStrictEqual([
      /* Begin degree 1 */
      [Buffer.from([]), Buffer.from([])],
      [Buffer.from([0x00]), Buffer.from([])],
      [Buffer.from([0x00]), Buffer.from([0x00])],
      /* Begin degree 2 */
      [Buffer.from([0x00, 0x00]), Buffer.from([]), Buffer.from([])],
      [Buffer.from([0x00, 0x00]), Buffer.from([0xff]), Buffer.from([])],
      /* End degree 2 */
      [Buffer.from([0x00, 0x00]), Buffer.from([])],
      [Buffer.from([0x00, 0x00]), Buffer.from([0x00, 0x00])],
      [Buffer.from([0x00, 0x00]), Buffer.from([0x01])],
      [Buffer.from([0x00, 0x00, 0x00]), Buffer.from([0x00])],
      [Buffer.from([0x00, 0x00, 0x00]), Buffer.from([0x01])],
      [Buffer.from([0x01]), Buffer.from([0x00])],
      [Buffer.from([0xff]), Buffer.from([])],
      /* End degree 1*/
      /* Begin degree 0 */
      [Buffer.from([])],
      [Buffer.from([0x00])],
      [Buffer.from([0x00, 0x00])],
      [Buffer.from([0x01])],
      /* End degree 0 */
    ]);
    await db.stop();
  });
  test('lexicographic level iteration order fuzzing', async () => {
    const dbPath = `${dataDir}/db`;
    const db = await DB.createDB({ dbPath, crypto, logger });
    const keyPathsInput: Array<KeyPath> = Array.from({ length: 5000 }, () =>
      Array.from({ length: testUtils.getRandomInt(0, 11) }, () =>
        nodeCrypto.randomBytes(testUtils.getRandomInt(0, 11)),
      ),
    );
    for (const kP of keyPathsInput) {
      await db.put(kP, 'value');
    }
    const keyPathsOutput: Array<KeyPath> = [];
    for await (const [kP] of db.iterator({ values: false })) {
      keyPathsOutput.push(kP);
    }
    // Copy the DB sorted key paths
    const keyPathsOutput_ = [...keyPathsOutput];
    // Shuffle the DB sorted key paths
    testUtils.arrayShuffle(keyPathsOutput_);
    keyPathsOutput_.sort((kP1: Array<Buffer>, kP2: Array<Buffer>) => {
      const lP1 = kP1.slice(0, kP1.length - 1);
      const lP2 = kP2.slice(0, kP2.length - 1);
      // Level parts at each degree are sorted lexicographically
      for (let i = 0; i < Math.min(lP1.length, lP2.length); i++) {
        const comp = Buffer.compare(lP1[i], lP2[i]);
        if (comp !== 0) return comp;
        // Continue to the next level part
      }
      // Key parts with the same level path are sorted lexicographically
      if (
        lP1.length === lP2.length &&
        Buffer.concat(lP1).equals(Buffer.concat(lP2))
      ) {
        return Buffer.compare(kP1[kP1.length - 1], kP2[kP2.length - 1]);
      }
      // Key parts with degree n are sorted in front of key parts with degree n -1
      if (kP1.length > kP2.length) {
        return -1;
      } else if (kP2.length > kP1.length) {
        return 1;
      } else {
        // This cannot happen
        throw new Error();
      }
    });
    for (let i = 0; i < keyPathsOutput_.length; i++) {
      try {
        expect(keyPathsOutput_[i]).toStrictEqual(keyPathsOutput[i]);
      } catch (e) {
        // eslint-disable-next-line no-console
        console.error(
          'mismatch: %s vs %s',
          nodeUtil.inspect({
            sort: keyPathsOutput_[i],
            sortBefore: keyPathsOutput_.slice(Math.max(0, i - 5), i),
            sortAfter: keyPathsOutput_.slice(i + 1, i + 1 + 5),
          }),
          nodeUtil.inspect({
            db: keyPathsOutput[i],
            dbBefore: keyPathsOutput.slice(Math.max(0, i - 5), i),
            dbAfter: keyPathsOutput.slice(i + 1, i + 1 + 5),
          }),
        );
        throw e;
      }
    }
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
