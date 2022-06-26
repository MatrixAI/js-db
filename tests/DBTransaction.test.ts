import type { KeyPath } from '@/types';
import os from 'os';
import path from 'path';
import fs from 'fs';
import Logger, { LogLevel, StreamHandler } from '@matrixai/logger';
import { withF } from '@matrixai/resources';
import { Lock } from '@matrixai/async-locks';
import DB from '@/DB';
import DBTransaction from '@/DBTransaction';
import * as errors from '@/errors';
import * as testsUtils from './utils';

describe(DBTransaction.name, () => {
  const logger = new Logger(`${DBTransaction.name} test`, LogLevel.WARN, [
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
      path.join(os.tmpdir(), 'db-tran-test-'),
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
        [['foo'], 'bar'],
        [['hello'], 'world'],
      ]);
      // Delete hello -> world
      await tran.del('hello');
    });
    // While the transaction is executed, there is no data
    expect(await db.dump(['data'], false, true)).toStrictEqual([]);
    await p;
    // Now the state should be applied to the DB
    expect(await db.dump(['data'], false, true)).toStrictEqual([
      [['foo'], 'bar'],
    ]);
  });
  test('transactional clear', async () => {
    await db.put('1', '1');
    await db.put('2', '2');
    await db.put('3', '3');
    // Transactional clear, clears all values
    await db.withTransactionF(async (tran) => {
      await tran.clear();
    });
    expect(await db.dump(['data'], false, true)).toStrictEqual([]);
    // Noop
    await db.clear();
    await db.put('1', '1');
    await db.put(['level1', '2'], '2');
    await db.put(['level1', 'level2', '3'], '3');
    await withF([db.transaction()], async ([tran]) => {
      await tran.clear(['level1']);
    });
    expect(await db.dump(['data'], false, true)).toStrictEqual([[['1'], '1']]);
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
  test('snapshot is lazily initiated on the first operation', async () => {
    await db.put('foo', 'first');
    expect(await db.get('foo')).toBe('first');
    await withF([db.transaction()], async ([tran]) => {
      await db.put('foo', 'second');
      expect(await tran.get('foo')).toBe('second');
      expect(await db.get('foo')).toBe('second');
      await db.put('foo', 'third');
      // Transaction still sees it as `second`
      expect(await tran.get('foo')).toBe('second');
      // Database sees it as `third`
      expect(await db.get('foo')).toBe('third');
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
    await expect(
      withF([db.transaction()], async ([tran1]) => {
        expect(await tran1.get('hello')).toBeUndefined();
        await tran1.put('hello', 'foo');
        // This transaction commits, but the outside transaction will fail
        await withF([db.transaction()], async ([tran2]) => {
          // `tran1` has not yet committed
          expect(await tran2.get('hello')).toBeUndefined();
          // This will cause a conflict with the external transaction
          await tran2.put('hello', 'bar');
          // `tran2` has not yet committed
          expect(await tran1.get('hello')).toBe('foo');
        });
      }),
    ).rejects.toThrow(errors.ErrorDBTransactionConflict);
  });
  test('repeatable reads', async () => {
    await withF([db.transaction()], async ([tran1]) => {
      expect(await tran1.get('hello')).toBeUndefined();
      await db.put('hello', '?');
      expect(await tran1.get('hello')).toBeUndefined();
      await db.withTransactionF(async (tran2) => {
        await tran2.put('hello', 'world');
      });
      // Even though `tran2` is now committed
      // the snapshot was taken when `hello` was still undefined
      expect(await tran1.get('hello')).toBeUndefined();
    });
    expect(await db.get('hello')).toBe('world');
    await db.clear();
    await expect(
      db.withTransactionF(async (tran1) => {
        expect(await tran1.get('hello')).toBeUndefined();
        await tran1.put('hello', 'foo');
        await expect(
          withF([db.transaction()], async ([tran2]) => {
            // `tran1` has not yet committed
            expect(await tran2.get('hello')).toBeUndefined();
            await tran2.put('hello', 'bar');
          }),
        ).resolves.toBeUndefined();
        // `tran2` is now committed
        // however because `foo` has been written in tran1, it stays as `foo`
        expect(await tran1.get('hello')).toBe('foo');
        // `hello` -> `foo` conflicts with `hello` -> `bar`
      }),
    ).rejects.toThrow(errors.ErrorDBTransactionConflict);
    expect(await db.get('hello')).toBe('bar');
  });
  test('no phantom reads', async () => {
    await db.put('1', '1');
    await db.put('2', '2');
    await db.put('3', '3');
    let rows: Array<[string, string]>;
    await withF([db.transaction()], async ([tran1]) => {
      rows = [];
      for await (const [kP, v] of tran1.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        rows.push([kP.join(), v]);
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
        for await (const [kP, v] of tran1.iterator<string>([], {
          keyAsBuffer: false,
          valueAsBuffer: false,
        })) {
          rows.push([kP.join(), v]);
        }
        expect(rows).toStrictEqual([
          ['1', '1'],
          ['2', '2'],
          ['3', '3'],
        ]);
      });
      rows = [];
      for await (const [kP, v] of tran1.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        rows.push([kP.toString(), v]);
      }
      // This is the same as repeatable read
      // but this applied to different key-values
      expect(rows).toStrictEqual([
        ['1', '1'],
        ['2', '2'],
        ['3', '3'],
      ]);
    });
    // Starting a new iterator, see the new results
    rows = [];
    for await (const [kP, v] of db.iterator<string>([], {
      keyAsBuffer: false,
      valueAsBuffer: false,
    })) {
      rows.push([kP.toString(), v]);
    }
    expect(rows).toStrictEqual([
      ['2', '2'],
      ['3', '3'],
      ['4', '4'],
    ]);
  });
  test('no lost updates', async () => {
    const p = withF([db.transaction()], async ([tran1]) => {
      await tran1.put('hello', 'foo');
      await withF([db.transaction()], async ([tran2]) => {
        await tran2.put('hello', 'bar');
      });
      // `tran1` sees `foo`
      expect(await tran1.get('hello')).toBe('foo');
      // However `db` sees `bar` as that's what is committed
      expect(await db.get('hello')).toBe('bar');
    });
    // Even though `tran1` committed last, the `tran2` write is not lost,
    // instead `tran1` results in a conflict
    await expect(p).rejects.toThrow(errors.ErrorDBTransactionConflict);
    expect(await db.get('hello')).toBe('bar');
  });
  test('get after delete consistency', async () => {
    await db.put('hello', 'world');
    await withF([db.transaction()], async ([tran]) => {
      expect(await tran.get('hello')).toBe('world');
      await tran.put('hello', 'another');
      expect(await tran.get('hello')).toBe('another');
      await tran.del('hello');
      expect(await tran.get('hello')).toBeUndefined();
      expect(await db.get('hello')).toBe('world');
    });
    expect(await db.get('hello')).toBeUndefined();
  });
  test('getForUpdate addresses write-skew by promoting gets into same-value puts', async () => {
    // Snapshot isolation allows write skew anomalies to occur
    // A write skew means that 2 transactions concurrently read from overlapping keys
    // then make disjoint updates to the keys, that breaks a consistency constraint on those keys
    // For example:
    // T1 reads from k1, k2, writes to k1
    // T2 reads from k1, k2, writes to k2
    // Where k1 + k2 >= 0
    await db.put('balance1', '100');
    await db.put('balance2', '100');
    const t1 = withF([db.transaction()], async ([tran]) => {
      let balance1 = parseInt((await tran.getForUpdate('balance1'))!);
      const balance2 = parseInt((await tran.getForUpdate('balance2'))!);
      balance1 -= 100;
      expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
      await tran.put('balance1', balance1.toString());
    });
    const t2 = withF([db.transaction()], async ([tran]) => {
      const balance1 = parseInt((await tran.getForUpdate('balance1'))!);
      let balance2 = parseInt((await tran.getForUpdate('balance2'))!);
      balance2 -= 100;
      expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
      await tran.put('balance2', balance2.toString());
    });
    // By using getForUpdate, we promote the read to a write, where it writes the same value
    // this causes a write-write conflict
    const results = await Promise.allSettled([t1, t2]);
    // One will succeed, one will fail
    expect(results.some((result) => result.status === 'fulfilled')).toBe(true);
    expect(
      results.some((result) => {
        return (
          result.status === 'rejected' &&
          result.reason instanceof errors.ErrorDBTransactionConflict
        );
      }),
    ).toBe(true);
  });
  test('PCC locking to prevent thrashing for racing counters', async () => {
    await db.put('counter', '0');
    let t1 = withF([db.transaction()], async ([tran]) => {
      // Can also use `getForUpdate`, but a conflict exists even for `get`
      let counter = parseInt((await tran.get('counter'))!);
      counter++;
      await tran.put('counter', counter.toString());
    });
    let t2 = withF([db.transaction()], async ([tran]) => {
      // Can also use `getForUpdate`, but a conflict exists even for `get`
      let counter = parseInt((await tran.get('counter'))!);
      counter++;
      await tran.put('counter', counter.toString());
    });
    let results = await Promise.allSettled([t1, t2]);
    expect(results.some((result) => result.status === 'fulfilled')).toBe(true);
    expect(
      results.some((result) => {
        return (
          result.status === 'rejected' &&
          result.reason instanceof errors.ErrorDBTransactionConflict
        );
      }),
    ).toBe(true);
    expect(await db.get('counter')).toBe('1');
    // In OCC, concurrent requests to update an atomic counter would result
    // in race thrashing where only 1 request succeeds, and all other requests
    // keep failing. The only way to prevent this thrashing is to use PCC locking
    await db.put('counter', '0');
    const l = new Lock();
    t1 = l.withF(async () => {
      await withF([db.transaction()], async ([tran]) => {
        // Can also use `get`, no difference here
        let counter = parseInt((await tran.getForUpdate('counter'))!);
        counter++;
        await tran.put('counter', counter.toString());
      });
    });
    t2 = l.withF(async () => {
      await withF([db.transaction()], async ([tran]) => {
        // Can also use `get`, no difference here
        let counter = parseInt((await tran.getForUpdate('counter'))!);
        counter++;
        await tran.put('counter', counter.toString());
      });
    });
    results = await Promise.allSettled([t1, t2]);
    expect(results.every((result) => result.status === 'fulfilled'));
    expect(await db.get('counter')).toBe('2');
    // The PCC locks must be done outside of transaction creation
    // This is because the PCC locks enforce mutual exclusion between commit operations
    // If the locks were done inside the transaction, it's possible for the commit operations
    // to be delayed after all mutually exclusive callbacks are executed
    // resulting in a DBTransactionConflict
    // When this library gains native locking, it must deal with this problem
    // by only releasing the locks when the transaction is committed or rollbacked
  });
  test('iterator get after delete consistency', async () => {
    await db.put('hello', 'world');
    let results: Array<[KeyPath, Buffer]> = [];
    await withF([db.transaction()], async ([tran]) => {
      for await (const [kP, v] of tran.iterator()) {
        results.push([kP, v]);
      }
      expect(results).toStrictEqual([
        [[Buffer.from('hello')], Buffer.from('"world"')],
      ]);
      results = [];
      await tran.del('hello');
      for await (const [kP, v] of tran.iterator()) {
        results.push([kP, v]);
      }
      expect(results).toStrictEqual([]);
      results = [];
      await tran.put('hello', 'another');
      for await (const [kP, v] of tran.iterator()) {
        results.push([kP, v]);
      }
      expect(results).toStrictEqual([
        [[Buffer.from('hello')], Buffer.from('"another"')],
      ]);
    });
  });
  test('iterator get after delete consistency with multiple levels', async () => {
    await db.put(['a', 'b'], 'first');
    await db.put(['a', 'c'], 'second');
    const results: Array<[string, string]> = [];
    await withF([db.transaction()], async ([tran]) => {
      await tran.del(['a', 'b']);
      for await (const [kP, v] of tran.iterator<string>(['a'], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        results.push([kP[0] as string, v]);
      }
    });
    expect(results).toStrictEqual([['c', 'second']]);
  });
  test('iterator with multiple entombed keys', async () => {
    /*
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        results.push([kP[0] as string, v]);
      }
      expect(results).toStrictEqual([
        ['b', 'b'],
        ['c', '3'],
        ['d', 'd'],
        ['e', '5'],
        ['j', '10'],
      ]);
      results = [];
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
        reverse: true,
      })) {
        results.push([kP[0] as string, v]);
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
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        results.push([kP[0] as string, v]);
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
  test('iterator with same largest key reversed', async () => {
    /*
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
        reverse: true,
      })) {
        results.push([kP[0] as string, v]);
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
  test('iterator with largest key in transaction', async () => {
    /*
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        results.push([kP[0] as string, v]);
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
  test('iterator with largest key in transaction reversed', async () => {
    /*
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
        reverse: true,
      })) {
        results.push([kP[0] as string, v]);
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
  test('iterator with largest key in db', async () => {
    /*
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        results.push([kP[0] as string, v]);
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
  test('iterator with largest key in db reversed', async () => {
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
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
        reverse: true,
      })) {
        results.push([kP[0] as string, v]);
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
      | KEYS | DB    | TRAN     | RESULT |
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
      for await (const [kP, v] of tran.iterator([], {
        keyAsBuffer: false,
        values: false,
      })) {
        results.push([kP[0] as string, v]);
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
      | KEYS | DB    | TRAN     | RESULT |
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
        [Buffer.from('a')],
        Buffer.from('"1"'),
      ]);
      iterator.seek('a');
      expect(await iterator.next()).toStrictEqual([
        [Buffer.from('a')],
        Buffer.from('"1"'),
      ]);
      expect(await iterator.next()).toStrictEqual([
        [Buffer.from('b')],
        Buffer.from('"b"'),
      ]);
      iterator.seek('g');
      expect(await iterator.next()).toStrictEqual([
        [Buffer.from('h')],
        Buffer.from('"h"'),
      ]);
      iterator.seek('h');
      expect(await iterator.next()).toStrictEqual([
        [Buffer.from('h')],
        Buffer.from('"h"'),
      ]);
      expect(await iterator.next()).toStrictEqual([
        [Buffer.from('j')],
        Buffer.from('"10"'),
      ]);
      await iterator.destroy();
    });
  });
  test('iterator with async generator yield', async () => {
    await db.put('a', 'a');
    await db.put('b', 'b');
    const g = db.withTransactionG(async function* (
      tran: DBTransaction,
    ): AsyncGenerator<[string, string]> {
      for await (const [kP, v] of tran.iterator<string>([], {
        keyAsBuffer: false,
        valueAsBuffer: false,
      })) {
        yield [kP[0] as string, v];
      }
    });
    const results: Array<[string, string]> = [];
    for await (const [k, v] of g) {
      results.push([k, v]);
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
    const mockFinally = jest.fn();
    const e = new Error('Oh no!');
    await expect(
      db.withTransactionF(async (tran) => {
        await tran.put('1', '1');
        await tran.put('2', '2');
        tran.queueFailure(mockFailure);
        tran.queueFinally(mockFinally);
        throw e;
      }),
    ).rejects.toThrow(e);
    expect(mockFailure).toBeCalledWith(e);
    expect(mockFinally).toBeCalledWith(e);
    expect(await db.get('1')).toBe('a');
    expect(await db.get('2')).toBe('b');
  });
});
