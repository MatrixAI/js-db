import type { KeyPath } from '@';
import os from 'os';
import path from 'path';
import fs from 'fs';
import nodeCrypto from 'crypto';
import nodeUtil from 'util';
import lexi from 'lexicographic-integer';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import DB from '@/DB';
import DBIterator from '@/DBIterator';
import rocksdbP from '@/native/rocksdbP';
import * as testsUtils from './utils';

describe(DBIterator.name, () => {
  const logger = new Logger(`${DBIterator.name} test`, LogLevel.WARN, [
    new StreamHandler(),
  ]);
  const crypto = {
    key: testsUtils.generateKeySync(256),
    ops: {
      encrypt: testsUtils.encrypt,
      decrypt: testsUtils.decrypt,
    },
  };
  let dataDir: string;
  let db: DB;
  beforeEach(async () => {
    dataDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), 'db-iter-test-'),
    );
    const dbPath = `${dataDir}/db`;
    db = await DB.createDB({ dbPath, crypto, logger });
  });
  afterEach(async () => {
    await db.stop();
    await fs.promises.rm(dataDir, {
      force: true,
      recursive: true,
    });
  });
  test('internal db lexicographic iteration order', async () => {
    const dbPath = `${dataDir}/leveldb`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    await rocksdbP.dbPut(db, Buffer.from([0x01]), Buffer.alloc(0), {});
    await rocksdbP.dbPut(
      db,
      Buffer.from([0x00, 0x00, 0x00]),
      Buffer.alloc(0),
      {},
    );
    await rocksdbP.dbPut(db, Buffer.from([0x00, 0x00]), Buffer.alloc(0), {});
    await rocksdbP.dbPut(db, Buffer.from([]), Buffer.alloc(0), {});
    const iterator = rocksdbP.iteratorInit(db, {
      keyEncoding: 'buffer',
      valueEncoding: 'buffer',
    });
    const [entries] = await rocksdbP.iteratorNextv(iterator, 4);
    await rocksdbP.iteratorClose(iterator);
    const keys = entries.map((entry) => entry[0]);
    expect(keys).toEqual([
      Buffer.from([]),
      // Therefore `aa` is earlier than `aaa`
      Buffer.from([0x00, 0x00]),
      Buffer.from([0x00, 0x00, 0x00]),
      // Therefore `aa` is earlier than `z`
      Buffer.from([0x01]),
    ]);
    await rocksdbP.dbClose(db);
  });
  test('lexicographic iteration order', async () => {
    await db.put(Buffer.from([0x01]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00, 0x00]), Buffer.alloc(0));
    await db.put(Buffer.from([0x00, 0x00]), Buffer.alloc(0));
    await db.put(Buffer.from([]), Buffer.alloc(0));
    const keyPaths: Array<KeyPath> = [];
    for await (const [kP] of db.iterator([], { values: false })) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toEqual([
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
    expect(keyPaths_).toEqual(keyPaths);
  });
  test('lexicographic iteration order fuzzing', async () => {
    const keys: Array<Buffer> = Array.from({ length: 1000 }, () =>
      nodeCrypto.randomBytes(testsUtils.getRandomInt(0, 101)),
    );
    for (const k of keys) {
      await db.put(k, 'value');
    }
    const keyPaths: Array<KeyPath> = [];
    for await (const [kP] of db.iterator([], { values: false })) {
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
    expect(keyPaths_).toEqual(keyPaths);
  });
  test('lexicographic integer iteration order', async () => {
    // Using the lexicographic-integer encoding
    // Sorted order should be [3, 4, 42, 100]
    const keys = [100, 3, 4, 42];
    for (const k of keys) {
      await db.put(Buffer.from(lexi.pack(k)), 'value');
    }
    const keysIterated: Array<number> = [];
    for await (const [kP] of db.iterator([], { values: false })) {
      keysIterated.push(lexi.unpack([...kP[0]]));
    }
    expect(keys).not.toEqual(keysIterated);
    // Numeric sort
    expect(keys.sort((a, b) => a - b)).toEqual(keysIterated);
  });
  test('lexicographic level iteration order', async () => {
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
    for await (const [kP] of db.iterator([], { values: false })) {
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
     * 3. Key parts with degree n are sorted in front of key parts with degree n - 1
     */
    expect(keyPaths).toEqual([
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
  });
  test('lexicographic level iteration order fuzzing', async () => {
    const keyPathsInput: Array<KeyPath> = Array.from({ length: 5000 }, () =>
      Array.from({ length: testsUtils.getRandomInt(0, 11) }, () =>
        nodeCrypto.randomBytes(testsUtils.getRandomInt(0, 11)),
      ),
    );
    for (const kP of keyPathsInput) {
      await db.put(kP, 'value');
    }
    const keyPathsOutput: Array<KeyPath> = [];
    for await (const [kP] of db.iterator([], { values: false })) {
      keyPathsOutput.push(kP);
    }
    // Copy the DB sorted key paths
    const keyPathsOutput_ = [...keyPathsOutput];
    // Shuffle the DB sorted key paths
    testsUtils.arrayShuffle(keyPathsOutput_);
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
        expect(keyPathsOutput_[i]).toEqual(keyPathsOutput[i]);
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
  });
  test('iterating sublevels', async () => {
    await db.put('a', 'value0');
    await db.put('b', 'value1');
    await db.put(['level1', 'a'], 'value0');
    await db.put(['level1', 'b'], 'value1');
    await db.put(['level1', 'level2', 'a'], 'value0');
    await db.put(['level1', 'level2', 'b'], 'value1');
    let results: Array<[KeyPath, string]>;
    results = [];
    for await (const [kP, v] of db.iterator<string>([], {
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
    for await (const [kP, v] of db.iterator<string>(['level1'], {
      keyAsBuffer: false,
      valueAsBuffer: false,
    })) {
      results.push([kP, v]);
    }
    expect(results).toStrictEqual([
      [['level2', 'a'], 'value0'],
      [['level2', 'b'], 'value1'],
      [['a'], 'value0'],
      [['b'], 'value1'],
    ]);
  });
  test('iterating sublevels with range', async () => {
    // Note that `'a'` is `0x61`
    await db.put(['level', Buffer.from([0x30, 0x34]), 'a'], 'value');
    await db.put(['level', Buffer.from([0x30, 0x35]), 'a', 'b'], 'value');
    // Suppose we only wanted these 2 entries
    await db.put(['level', Buffer.from([0x30, 0x35]), ''], 'value');
    await db.put(['level', Buffer.from([0x30, 0x35]), 'a'], 'value');
    // And none of these entries
    await db.put(['level', Buffer.from([0x30, 0x36]), 'a', 'b'], 'value');
    await db.put(['level', Buffer.from([0x30, 0x36]), 'a'], 'value');
    await db.put(['level', Buffer.from([0x30, 0x34])], 'value');
    let keyPaths: Array<KeyPath> = [];
    // Here we are iterating until the sublevel of `0x30 0x35`
    // We must use a key path for the `lte`
    // It cannot just be `Buffer.from([0x30, 0x35])`
    // Notice that this will not cover the key of `0x30 0x34`
    // That's because of rule 3
    // 3. Key parts with degree n are sorted in front of key parts with degree n - 1
    for await (const [kP] of db.iterator(['level'], {
      lte: [Buffer.from([0x30, 0x35]), ''],
      values: false,
    })) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toStrictEqual([
      [Buffer.from([0x30, 0x34]), Buffer.from([0x61])],
      [Buffer.from([0x30, 0x35]), Buffer.from([0x61]), Buffer.from([0x62])],
      [Buffer.from([0x30, 0x35]), Buffer.from([])],
    ]);
    // If we only wanted entries under the sublevel of `0x30 0x35`
    // this would not work because of rule 3
    // The deeper level is in front
    keyPaths = [];
    for await (const [kP] of db.iterator(['level'], {
      gte: [Buffer.from([0x30, 0x35]), ''],
      lt: [Buffer.from([0x30, 0x36]), ''],
    })) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toStrictEqual([
      [Buffer.from([0x30, 0x35]), Buffer.from([])],
      [Buffer.from([0x30, 0x35]), Buffer.from([0x61])],
      [Buffer.from([0x30, 0x36]), Buffer.from([0x61]), Buffer.from([0x62])],
    ]);
    // To actually do it, we need to specify as part of the level path parameter
    keyPaths = [];
    for await (const [kP] of db.iterator([
      'level',
      Buffer.from([0x30, 0x35]),
    ])) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toStrictEqual([
      [Buffer.from([0x61]), Buffer.from([0x62])],
      [Buffer.from([])],
      [Buffer.from([0x61])],
    ]);
    // However the deeper level is still there
    // But because of rule 3, we can do this instead
    keyPaths = [];
    for await (const [kP] of db.iterator(['level', Buffer.from([0x30, 0x35])], {
      gte: '',
    })) {
      keyPaths.push(kP);
    }
    expect(keyPaths).toStrictEqual([[Buffer.from([])], [Buffer.from([0x61])]]);
  });
});
