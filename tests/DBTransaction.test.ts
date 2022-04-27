import os from 'os';
import path from 'path';
import fs from 'fs';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import { withF } from '@matrixai/resources';
import DB from '@/DB';
import DBTransaction from '@/DBTransaction';
import * as utils from '@/utils';
import * as testUtils from './utils';

describe(DBTransaction.name, () => {
  const logger = new Logger(`${DBTransaction.name} test`, LogLevel.WARN, [
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
  let db: DB;
  beforeEach(async () => {
    dataDir = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), 'encryptedfs-test-'),
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
  test('snapshot state is cleared after releasing transactions', async () => {
    const acquireTran1 = db.transaction();
    const [releaseTran1, tran1] = await acquireTran1();
    await tran1!.put('hello', 'world');
    const acquireTran2 = db.transaction();
    const [releaseTran2, tran2] = await acquireTran2();
    await tran2!.put('hello', 'world');
    expect(await db.dump(['transactions'])).toStrictEqual([
      [`${utils.sep}0${utils.sep}${utils.sep}data${utils.sep}hello`, 'world'],
      [`${utils.sep}1${utils.sep}${utils.sep}data${utils.sep}hello`, 'world'],
    ]);
    await releaseTran1();
    expect(await db.dump(['transactions'])).toStrictEqual([
      [`${utils.sep}1${utils.sep}${utils.sep}data${utils.sep}hello`, 'world'],
    ]);
    await releaseTran2();
    expect(await db.dump(['transactions'])).toStrictEqual([]);
  });
  test('get, put and del', async () => {
    const p = withF([db.transaction()], async ([tran]) => {
      expect(await tran.get('foo')).toBeUndefined();
      // Add foo -> bar to the transaction
      await tran.put('foo', 'bar');
      // Add hello -> world to the transaction
      await tran.put('hello', 'world');
      expect(await tran.get('foo')).toBe('bar');
      expect(await tran.get('hello')).toBe('world');
      expect(await tran.dump()).toStrictEqual([
        [`${utils.sep}data${utils.sep}foo`, 'bar'],
        [`${utils.sep}data${utils.sep}hello`, 'world'],
      ]);
      // Delete hello -> world
      await tran.del('hello');
      // Transaction state should be used
      expect(Object.entries(await db.dump(['transactions'])).length > 0).toBe(
        true,
      );
    });
    // While the transaction is executed, there is no data
    expect(await db.dump(['data'])).toStrictEqual([]);
    await p;
    // Now the state should be applied to the DB
    expect(await db.dump(['data'])).toStrictEqual([['foo', 'bar']]);
    // Transaction state is cleared
    expect(await db.dump(['transactions'])).toStrictEqual([]);
  });
  test('transactional clear', async () => {
    await db.put('1', '1');
    await db.put('2', '2');
    await db.put('3', '3');
    // Transactional clear, clears all values
    await db.withTransactionF(async (tran) => {
      await tran.clear();
    });
    expect(await db.dump(['data'])).toStrictEqual([]);
    // Noop
    await db.clear();
    await db.put('1', '1');
    await db.put(['level1', '2'], '2');
    await db.put(['level1', 'level2', '3'], '3');
    await withF([db.transaction()], async ([tran]) => {
      await tran.clear(['level1']);
    });
    expect(await db.dump(['data'])).toStrictEqual([['1', '1']]);
  });
  test('transactional count', async () => {
    await db.put('1', '1');
    await db.put('2', '2');
    await db.put('3', '3');
    await withF([db.transaction()], async ([tran]) => {
      expect(await tran.count()).toBe(3);
    });
    await db.clear();
    await db.put('1', '1');
    await db.put(['level1', '2'], '2');
    await db.put(['level1', 'level2', '3'], '3');
    await db.withTransactionF(async (tran) => {
      expect(await tran.count(['level1'])).toBe(2);
    });
  });
  test('no dirty reads', async () => {
    await withF([db.transaction()], async ([tran1]) => {
      expect(await tran1.get('hello')).toBeUndefined();
      await withF([db.transaction()], async ([tran2]) => {
        await tran2.put('hello', 'world');
        // `tran2` has not yet committed
        expect(await tran1.get('hello')).toBeUndefined();
      });
    });
    await db.clear();
    await withF([db.transaction()], async ([tran1]) => {
      expect(await tran1.get('hello')).toBeUndefined();
      await tran1.put('hello', 'foo');
      await withF([db.transaction()], async ([tran2]) => {
        // `tran1` has not yet committed
        expect(await tran2.get('hello')).toBeUndefined();
        await tran2.put('hello', 'bar');
        // `tran2` has not yet committed
        expect(await tran1.get('hello')).toBe('foo');
      });
    });
  });
  test('non-repeatable reads', async () => {
    await withF([db.transaction()], async ([tran1]) => {
      expect(await tran1.get('hello')).toBeUndefined();
      await db.withTransactionF(async (tran2) => {
        await tran2.put('hello', 'world');
      });
      // `tran2` is now committed
      expect(await tran1.get('hello')).toBe('world');
    });
    await db.clear();
    await db.withTransactionF(async (tran1) => {
      expect(await tran1.get('hello')).toBeUndefined();
      await tran1.put('hello', 'foo');
      await withF([db.transaction()], async ([tran2]) => {
        // `tran1` has not yet committed
        expect(await tran2.get('hello')).toBeUndefined();
        await tran2.put('hello', 'bar');
      });
      // `tran2` is now committed
      // however because `foo` has been written in tran1, it stays as `foo`
      expect(await tran1.get('hello')).toBe('foo');
    });
  });
  test('phantom reads', async () => {
    await db.put('1', '1');
    await db.put('2', '2');
    await db.put('3', '3');
    let rows: Array<[string, string]>;
    await withF([db.transaction()], async ([tran1]) => {
      rows = [];
      for await (const [k, v] of tran1.iterator()) {
        rows.push([k.toString(), JSON.parse(v.toString())]);
      }
      expect(rows).toStrictEqual([
        ['1', '1'],
        ['2', '2'],
        ['3', '3'],
      ]);
      await withF([db.transaction()], async ([tran2]) => {
        await tran2.del('1');
        await tran2.put('4', '4');
        rows = [];
        for await (const [k, v] of tran1.iterator()) {
          rows.push([k.toString(), JSON.parse(v.toString())]);
        }
        expect(rows).toStrictEqual([
          ['1', '1'],
          ['2', '2'],
          ['3', '3'],
        ]);
      });
      rows = [];
      for await (const [k, v] of tran1.iterator()) {
        rows.push([k.toString(), JSON.parse(v.toString())]);
      }
      expect(rows).toStrictEqual([
        ['2', '2'],
        ['3', '3'],
        ['4', '4'],
      ]);
    });
  });
  test('lost updates', async () => {
    await withF([db.transaction()], async ([tran1]) => {
      await tran1.put('hello', 'foo');
      await withF([db.transaction()], async ([tran2]) => {
        await tran2.put('hello', 'bar');
      });
      expect(await tran1.get('hello')).toBe('foo');
    });
    // `tran2` write is lost because `tran1` committed last
    expect(await db.get('hello')).toBe('foo');
  });
  test('get after delete consistency', async () => {
    await db.put('hello', 'world');
    await withF([db.transaction()], async ([tran]) => {
      expect(await tran.get('hello')).toBe('world');
      await tran.put('hello', 'another');
      expect(await tran.get('hello')).toBe('another');
      await tran.del('hello');
      expect(await tran.dump()).toStrictEqual([
        [`${utils.sep}tombstone${utils.sep}hello`, true],
      ]);
      expect(await tran.get('hello')).toBeUndefined();
      expect(await db.get('hello')).toBe('world');
    });
    expect(await db.get('hello')).toBeUndefined();
  });
  test('iterator get after delete consistency', async () => {
    await db.put('hello', 'world');
    let results: Array<[Buffer, Buffer]> = [];
    await withF([db.transaction()], async ([tran]) => {
      for await (const [k, v] of tran.iterator()) {
        results.push([k, v]);
      }
      expect(results).toStrictEqual([
        [Buffer.from('hello'), Buffer.from('"world"')],
      ]);
      results = [];
      await tran.del('hello');
      for await (const [k, v] of tran.iterator()) {
        results.push([k, v]);
      }
      expect(results).toStrictEqual([]);
      results = [];
      await tran.put('hello', 'another');
      for await (const [k, v] of tran.iterator()) {
        results.push([k, v]);
      }
      expect(results).toStrictEqual([
        [Buffer.from('hello'), Buffer.from('"another"')],
      ]);
    });
  });
  test('iterator get after delete consistency with multiple levels', async () => {
    await db.put(['a', 'b'], 'first');
    await db.put(['a', 'c'], 'second');
    const results: Array<[string, string]> = [];
    await withF([db.transaction()], async ([tran]) => {
      await tran.del(['a', 'b']);
      for await (const [k, v] of tran.iterator(undefined, ['a'])) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual([['c', 'second']]);
  });
  test('iterator with multiple entombed keys', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | X        |        |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | X        |        |
      | g    |       |          |        |
      | h    | h = h | X        |        |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
      | k    | k = k | X        |        |

      Where X means deleted during transaction
    */
    let results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await db.put('k', 'k');
    await withF([db.transaction()], async ([tran]) => {
      await tran.del('a');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.del('f');
      await tran.del('h');
      await tran.put('j', '10');
      await tran.del('k');
      for await (const [k, v] of tran.iterator()) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
      expect(results).toStrictEqual([
        ['b', 'b'],
        ['c', '3'],
        ['d', 'd'],
        ['e', '5'],
        ['j', '10'],
      ]);
      results = [];
      for await (const [k, v] of tran.iterator({ reverse: true })) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
      expect(results).toStrictEqual([
        ['j', '10'],
        ['e', '5'],
        ['d', 'd'],
        ['c', '3'],
        ['b', 'b'],
      ]);
    });
  });
  test('iterator with same largest key', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
      | k    | k = k | k = 11   | k = 11 |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await db.put('k', 'k');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      await tran.put('k', '11');
      for await (const [k, v] of tran.iterator()) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual([
      ['a', '1'],
      ['b', 'b'],
      ['c', '3'],
      ['d', 'd'],
      ['e', '5'],
      ['f', '6'],
      ['h', 'h'],
      ['j', '10'],
      ['k', '11'],
    ]);
  });
  test('iterator with same largest key in reverse', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
      | k    | k = k | k = 11   | k = 11 |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await db.put('k', 'k');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      await tran.put('k', '11');
      for await (const [k, v] of tran.iterator({ reverse: true })) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual(
      [
        ['a', '1'],
        ['b', 'b'],
        ['c', '3'],
        ['d', 'd'],
        ['e', '5'],
        ['f', '6'],
        ['h', 'h'],
        ['j', '10'],
        ['k', '11'],
      ].reverse(),
    );
  });
  test('iterator with snapshot largest key', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      for await (const [k, v] of tran.iterator()) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual([
      ['a', '1'],
      ['b', 'b'],
      ['c', '3'],
      ['d', 'd'],
      ['e', '5'],
      ['f', '6'],
      ['h', 'h'],
      ['j', '10'],
    ]);
  });
  test('iterator with snapshot largest key in reverse', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      for await (const [k, v] of tran.iterator({ reverse: true })) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual(
      [
        ['a', '1'],
        ['b', 'b'],
        ['c', '3'],
        ['d', 'd'],
        ['e', '5'],
        ['f', '6'],
        ['h', 'h'],
        ['j', '10'],
      ].reverse(),
    );
  });
  test('iterator with db largest key', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      for await (const [k, v] of tran.iterator()) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual([
      ['a', '1'],
      ['b', 'b'],
      ['c', '3'],
      ['d', 'd'],
      ['e', '5'],
      ['f', '6'],
      ['h', 'h'],
    ]);
  });
  test('iterator with db largest key in reverse', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
    */
    const results: Array<[string, string]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      for await (const [k, v] of tran.iterator({ reverse: true })) {
        results.push([k.toString(), JSON.parse(v.toString())]);
      }
    });
    expect(results).toStrictEqual(
      [
        ['a', '1'],
        ['b', 'b'],
        ['c', '3'],
        ['d', 'd'],
        ['e', '5'],
        ['f', '6'],
        ['h', 'h'],
      ].reverse(),
    );
  });
  test('iterator with undefined values', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
      | k    | k = k | k = 11   | k = 11 |
    */
    const results: Array<[string, undefined]> = [];
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await db.put('k', 'k');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      await tran.put('k', '11');
      for await (const [k, v] of tran.iterator({ values: false })) {
        results.push([k.toString(), v]);
      }
    });
    expect(results).toStrictEqual([
      ['a', undefined],
      ['b', undefined],
      ['c', undefined],
      ['d', undefined],
      ['e', undefined],
      ['f', undefined],
      ['h', undefined],
      ['j', undefined],
      ['k', undefined],
    ]);
  });
  test('iterator using seek and next', async () => {
    /*
      | KEYS | DB    | SNAPSHOT | RESULT |
      |------|-------|----------|--------|
      | a    | a = a | a = 1    | a = 1  |
      | b    | b = b |          | b = b  |
      | c    |       | c = 3    | c = 3  |
      | d    | d = d |          | d = d  |
      | e    | e = e | e = 5    | e = 5  |
      | f    |       | f = 6    | f = 6  |
      | g    |       |          |        |
      | h    | h = h |          | h = h  |
      | i    |       |          |        |
      | j    |       | j = 10   | j = 10 |
      | k    | k = k | k = 11   | k = 11 |
    */
    await db.put('a', 'a');
    await db.put('b', 'b');
    await db.put('d', 'd');
    await db.put('e', 'e');
    await db.put('h', 'h');
    await db.put('k', 'k');
    await withF([db.transaction()], async ([tran]) => {
      await tran.put('a', '1');
      await tran.put('c', '3');
      await tran.put('e', '5');
      await tran.put('f', '6');
      await tran.put('j', '10');
      await tran.put('k', '11');
      const iterator = tran.iterator();
      iterator.seek('a');
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('a'),
        Buffer.from('"1"'),
      ]);
      iterator.seek('a');
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('a'),
        Buffer.from('"1"'),
      ]);
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('b'),
        Buffer.from('"b"'),
      ]);
      iterator.seek('g');
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('h'),
        Buffer.from('"h"'),
      ]);
      iterator.seek('h');
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('h'),
        Buffer.from('"h"'),
      ]);
      expect(await iterator.next()).toStrictEqual([
        Buffer.from('j'),
        Buffer.from('"10"'),
      ]);
      await iterator.end();
    });
  });
  test('iterator with async generator yield', async () => {
    await db.put('a', 'a');
    await db.put('b', 'b');
    const g = db.withTransactionG(async function* (
      tran: DBTransaction,
    ): AsyncGenerator<[Buffer, Buffer]> {
      for await (const [k, v] of tran.iterator()) {
        yield [k, v];
      }
    });
    const results: Array<[string, string]> = [];
    for await (const [k, v] of g) {
      results.push([k.toString(), JSON.parse(v.toString())]);
    }
    expect(results).toStrictEqual([
      ['a', 'a'],
      ['b', 'b'],
    ]);
  });
  test('queue success hooks', async () => {
    const results: Array<number> = [];
    const mockSuccess1 = jest.fn(() => {
      results.push(1);
    });
    const mockSuccess2 = jest.fn(() => {
      results.push(2);
    });
    const mockFailure = jest.fn();
    await withF([db.transaction()], async ([tran]) => {
      tran.queueSuccess(mockSuccess1);
      tran.queueSuccess(mockSuccess2);
      tran.queueFailure(mockFailure);
    });
    expect(mockSuccess1).toBeCalled();
    expect(mockSuccess2).toBeCalled();
    expect(mockFailure).not.toBeCalled();
    expect(results).toStrictEqual([1, 2]);
  });
  test('queue failure hooks', async () => {
    const results: Array<number> = [];
    const mockSuccess = jest.fn();
    const mockFailure1 = jest.fn(() => {
      results.push(1);
    });
    const mockFailure2 = jest.fn(() => {
      results.push(2);
    });
    await expect(
      withF([db.transaction()], async ([tran]) => {
        tran.queueSuccess(mockSuccess);
        tran.queueFailure(mockFailure1);
        tran.queueFailure(mockFailure2);
        throw new Error('Something bad happened');
      }),
    ).rejects.toThrow('Something bad happened');
    expect(mockSuccess).not.toBeCalled();
    expect(mockFailure1).toBeCalled();
    expect(mockFailure2).toBeCalled();
    expect(results).toStrictEqual([1, 2]);
  });
  test('queue finally hooks', async () => {
    let results: Array<string> = [];
    let mockSuccess = jest.fn(() => {
      results.push('success');
    });
    let mockFailure = jest.fn((e?: Error) => {
      expect(e).toBeUndefined();
      results.push('failure');
    });
    let mockFinally1 = jest.fn((e?: Error) => {
      expect(e).toBeUndefined();
      results.push('finally1');
    });
    let mockFinally2 = jest.fn((e?: Error) => {
      expect(e).toBeUndefined();
      results.push('finally2');
    });
    await withF([db.transaction()], async ([tran]) => {
      tran.queueSuccess(mockSuccess);
      tran.queueFailure(mockFailure);
      tran.queueFinally(mockFinally1);
      tran.queueFinally(mockFinally2);
    });
    expect(mockSuccess).toBeCalled();
    expect(mockFailure).not.toBeCalled();
    expect(mockFinally1).toBeCalled();
    expect(mockFinally2).toBeCalled();
    expect(results).toStrictEqual(['success', 'finally1', 'finally2']);
    mockSuccess = jest.fn(() => {
      results.push('success');
    });
    mockFailure = jest.fn((e?: Error) => {
      expect(e).toBeInstanceOf(Error);
      expect(e!.message).toBe('Something bad happened');
      results.push('failure');
    });
    mockFinally1 = jest.fn((e?: Error) => {
      expect(e).toBeInstanceOf(Error);
      expect(e!.message).toBe('Something bad happened');
      results.push('finally1');
    });
    mockFinally2 = jest.fn((e?: Error) => {
      expect(e).toBeInstanceOf(Error);
      expect(e!.message).toBe('Something bad happened');
      results.push('finally2');
    });
    results = [];
    await expect(
      withF([db.transaction()], async ([tran]) => {
        tran.queueSuccess(mockSuccess);
        tran.queueFailure(mockFailure);
        tran.queueFinally(mockFinally1);
        tran.queueFinally(mockFinally2);
        throw new Error('Something bad happened');
      }),
    ).rejects.toThrow('Something bad happened');
    expect(mockSuccess).not.toBeCalled();
    expect(mockFailure).toBeCalled();
    expect(mockFinally1).toBeCalled();
    expect(mockFinally2).toBeCalled();
    expect(results).toStrictEqual(['failure', 'finally1', 'finally2']);
  });
  test('rollback on error', async () => {
    await db.put('1', 'a');
    await db.put('2', 'b');
    const mockFailure = jest.fn();
    await expect(
      db.withTransactionF(async (tran) => {
        await tran.put('1', '1');
        await tran.put('2', '2');
        tran.queueFailure(mockFailure);
        throw new Error('Oh no!');
      }),
    ).rejects.toThrow('Oh no!');
    expect(mockFailure).toBeCalled();
    expect(await db.get('1')).toBe('a');
    expect(await db.get('2')).toBe('b');
  });
});
