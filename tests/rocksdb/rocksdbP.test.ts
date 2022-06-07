import type { RocksDBDatabase } from '@/rocksdb/types';
import os from 'os';
import path from 'path';
import fs from 'fs';
import rocksdbP from '@/rocksdb/rocksdbP';

describe('rocksdbP', () => {
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
  test('dbOpen invalid log level option', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await expect(
      rocksdbP.dbOpen(db, dbPath, {
        // @ts-ignore use incorrect value
        infoLogLevel: 'incorrect',
      }),
    ).rejects.toHaveProperty('code', 'DB_OPEN');
  });
  test('dbClose is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    await expect(rocksdbP.dbClose(db)).resolves.toBeUndefined();
    await expect(rocksdbP.dbClose(db)).resolves.toBeUndefined();
  });
  test('dbClose auto-closes dangling snapshots, iterators and transactions', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const snap = rocksdbP.snapshotInit(db);
    const iterator = rocksdbP.iteratorInit(db, {});
    const tran = rocksdbP.transactionInit(db, {});
    // This should auto-close them
    await rocksdbP.dbClose(db);
    // We can also attempt to close, which is idempotent
    await rocksdbP.snapshotRelease(snap);
    await rocksdbP.iteratorClose(iterator);
    await rocksdbP.transactionRollback(tran);
  });
  describe('database', () => {
    let dbPath: string;
    let db: RocksDBDatabase;
    beforeEach(async () => {
      dbPath = `${dataDir}/db`;
      db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
    });
    afterEach(async () => {
      await rocksdbP.dbClose(db);
    });
    test('dbMultiGet', async () => {
      await rocksdbP.dbPut(db, 'foo', 'bar', {});
      await rocksdbP.dbPut(db, 'bar', 'foo', {});
      expect(await rocksdbP.dbMultiGet(db, ['foo', 'bar', 'abc'], {})).toEqual([
        'bar',
        'foo',
        undefined,
      ]);
    });
    test('dbGet and dbMultiget with snapshots', async () => {
      await rocksdbP.dbPut(db, 'K1', '100', {});
      await rocksdbP.dbPut(db, 'K2', '100', {});
      const snap = rocksdbP.snapshotInit(db);
      await rocksdbP.dbPut(db, 'K1', '200', {});
      await rocksdbP.dbPut(db, 'K2', '200', {});
      expect(await rocksdbP.dbGet(db, 'K1', { snapshot: snap })).toBe('100');
      expect(await rocksdbP.dbGet(db, 'K2', { snapshot: snap })).toBe('100');
      expect(
        await rocksdbP.dbMultiGet(db, ['K1', 'K2'], {
          snapshot: snap,
        }),
      ).toEqual(['100', '100']);
      expect(await rocksdbP.dbGet(db, 'K1', {})).toBe('200');
      expect(await rocksdbP.dbGet(db, 'K2', {})).toBe('200');
      expect(await rocksdbP.dbMultiGet(db, ['K1', 'K2'], {})).toEqual([
        '200',
        '200',
      ]);
      await rocksdbP.snapshotRelease(snap);
    });
    describe('iterators', () => {
      test('iteratorClose is idempotent', async () => {
        const it = rocksdbP.iteratorInit(db, {});
        await expect(rocksdbP.iteratorClose(it)).resolves.toBeUndefined();
        await expect(rocksdbP.iteratorClose(it)).resolves.toBeUndefined();
      });
      test('iteratorNextv signals when iterator is finished', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        const iter1 = rocksdbP.iteratorInit(db, {});
        expect(await rocksdbP.iteratorNextv(iter1, 2)).toEqual([
          [
            ['K1', '100'],
            ['K2', '100'],
          ],
          false,
        ]);
        await rocksdbP.iteratorClose(iter1);
        const iter2 = rocksdbP.iteratorInit(db, {});
        expect(await rocksdbP.iteratorNextv(iter2, 3)).toEqual([
          [
            ['K1', '100'],
            ['K2', '100'],
          ],
          true,
        ]);
        await rocksdbP.iteratorClose(iter2);
        const iter3 = rocksdbP.iteratorInit(db, {});
        expect(await rocksdbP.iteratorNextv(iter3, 2)).toEqual([
          [
            ['K1', '100'],
            ['K2', '100'],
          ],
          false,
        ]);
        expect(await rocksdbP.iteratorNextv(iter3, 1)).toEqual([[], true]);
        await rocksdbP.iteratorClose(iter3);
      });
      test('iteratorInit with implicit snapshot', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        const iter = rocksdbP.iteratorInit(db, {});
        await rocksdbP.dbPut(db, 'K1', '200', {});
        await rocksdbP.dbPut(db, 'K2', '200', {});
        expect(await rocksdbP.iteratorNextv(iter, 2)).toEqual([
          [
            ['K1', '100'],
            ['K2', '100'],
          ],
          false,
        ]);
        await rocksdbP.iteratorClose(iter);
      });
      test('iteratorInit with explicit snapshot', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        const snap = rocksdbP.snapshotInit(db);
        await rocksdbP.dbPut(db, 'K1', '200', {});
        await rocksdbP.dbPut(db, 'K2', '200', {});
        const iter = rocksdbP.iteratorInit(db, {
          snapshot: snap,
        });
        expect(await rocksdbP.iteratorNextv(iter, 2)).toEqual([
          [
            ['K1', '100'],
            ['K2', '100'],
          ],
          false,
        ]);
        await rocksdbP.iteratorClose(iter);
        await rocksdbP.snapshotRelease(snap);
      });
      test('iterators have consistent iteration', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        const iter = rocksdbP.iteratorInit(db, {});
        expect(await rocksdbP.iteratorNextv(iter, 1)).toEqual([
          [['K1', '100']],
          false,
        ]);
        await rocksdbP.dbPut(db, 'K2', '200', {});
        expect(await rocksdbP.iteratorNextv(iter, 1)).toEqual([
          [['K2', '100']],
          false,
        ]);
        await rocksdbP.iteratorClose(iter);
      });
      test('dbClear with implicit snapshot', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        await rocksdbP.dbClear(db, {});
        await expect(rocksdbP.dbGet(db, 'K1', {})).rejects.toHaveProperty(
          'code',
          'NOT_FOUND',
        );
        await expect(rocksdbP.dbGet(db, 'K2', {})).rejects.toHaveProperty(
          'code',
          'NOT_FOUND',
        );
      });
      test('dbClear with explicit snapshot', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        const snap = rocksdbP.snapshotInit(db);
        await rocksdbP.dbPut(db, 'K1', '200', {});
        await rocksdbP.dbPut(db, 'K2', '200', {});
        await rocksdbP.dbPut(db, 'K3', '200', {});
        await rocksdbP.dbPut(db, 'K4', '200', {});
        await rocksdbP.dbClear(db, {
          snapshot: snap,
        });
        await rocksdbP.snapshotRelease(snap);
        await expect(rocksdbP.dbGet(db, 'K1', {})).rejects.toHaveProperty(
          'code',
          'NOT_FOUND',
        );
        await expect(rocksdbP.dbGet(db, 'K2', {})).rejects.toHaveProperty(
          'code',
          'NOT_FOUND',
        );
        expect(await rocksdbP.dbGet(db, 'K3', {})).toBe('200');
        expect(await rocksdbP.dbGet(db, 'K4', {})).toBe('200');
      });
    });
    describe('transactions', () => {
      test('transactionCommit is idempotent', async () => {
        const tran = rocksdbP.transactionInit(db, {});
        await expect(rocksdbP.transactionCommit(tran)).resolves.toBeUndefined();
        await expect(rocksdbP.transactionCommit(tran)).resolves.toBeUndefined();
      });
      test('transactionRollback is idempotent', async () => {
        const tran = rocksdbP.transactionInit(db, {});
        await expect(
          rocksdbP.transactionRollback(tran),
        ).resolves.toBeUndefined();
        await expect(
          rocksdbP.transactionRollback(tran),
        ).resolves.toBeUndefined();
      });
      test('transactionGet, transactionPut, transactionDel', async () => {
        const tran = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionPut(tran, 'foo', 'bar');
        await rocksdbP.transactionPut(tran, 'bar', 'foo');
        expect(await rocksdbP.transactionGet(tran, 'foo', {})).toBe('bar');
        await rocksdbP.transactionDel(tran, 'bar');
        await rocksdbP.transactionCommit(tran);
        expect(await rocksdbP.dbGet(db, 'foo', {})).toBe('bar');
        await expect(rocksdbP.dbGet(db, 'bar', {})).rejects.toHaveProperty(
          'code',
          'NOT_FOUND',
        );
      });
      test('transactionGetForUpdate addresses write skew by promoting gets into same-value puts', async () => {
        // Snapshot isolation allows write skew anomalies to occur
        // A write skew means that 2 transactions concurrently read from overlapping keys
        // then make disjoint updates to the keys, that breaks a consistency constraint on those keys
        // For example:
        // T1 reads from k1, k2, writes to k1
        // T2 reads from k1, k2, writes to k2
        // Where k1 + k2 >= 0
        await rocksdbP.dbPut(db, 'balance1', '100', {});
        await rocksdbP.dbPut(db, 'balance2', '100', {});
        const t1 = async () => {
          const tran1 = rocksdbP.transactionInit(db, {});
          let balance1 = parseInt(
            await rocksdbP.transactionGetForUpdate(tran1, 'balance1', {}),
          );
          const balance2 = parseInt(
            await rocksdbP.transactionGetForUpdate(tran1, 'balance2', {}),
          );
          balance1 -= 100;
          expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
          await rocksdbP.transactionPut(tran1, 'balance1', balance1.toString());
          await rocksdbP.transactionCommit(tran1);
        };
        const t2 = async () => {
          const tran2 = rocksdbP.transactionInit(db, {});
          const balance1 = parseInt(
            await rocksdbP.transactionGetForUpdate(tran2, 'balance1', {}),
          );
          let balance2 = parseInt(
            await rocksdbP.transactionGetForUpdate(tran2, 'balance2', {}),
          );
          balance2 -= 100;
          expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
          await rocksdbP.transactionPut(tran2, 'balance2', balance2.toString());
          await rocksdbP.transactionCommit(tran2);
        };
        // By using transactionGetForUpdate, we promote the read to a write, where it writes the same value
        // this causes a write-write conflict
        const results = await Promise.allSettled([t1(), t2()]);
        // One will succeed, one will fail
        expect(results.some((result) => result.status === 'fulfilled')).toBe(
          true,
        );
        expect(
          results.some((result) => {
            return (
              result.status === 'rejected' &&
              result.reason.code === 'TRANSACTION_CONFLICT'
            );
          }),
        ).toBe(true);
      });
      test('transactionMultiGetForUpdate addresses write skew by promoting gets into same-value puts', async () => {
        // Snapshot isolation allows write skew anomalies to occur
        // A write skew means that 2 transactions concurrently read from overlapping keys
        // then make disjoint updates to the keys, that breaks a consistency constraint on those keys
        // For example:
        // T1 reads from k1, k2, writes to k1
        // T2 reads from k1, k2, writes to k2
        // Where k1 + k2 >= 0
        await rocksdbP.dbPut(db, 'balance1', '100', {});
        await rocksdbP.dbPut(db, 'balance2', '100', {});
        const t1 = async () => {
          const tran1 = rocksdbP.transactionInit(db, {});
          let balance1 = parseInt(
            (
              await rocksdbP.transactionMultiGetForUpdate(
                tran1,
                ['balance1'],
                {},
              )
            )[0],
          );
          const balance2 = parseInt(
            (
              await rocksdbP.transactionMultiGetForUpdate(
                tran1,
                ['balance2'],
                {},
              )
            )[0],
          );
          balance1 -= 100;
          expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
          await rocksdbP.transactionPut(tran1, 'balance1', balance1.toString());
          await rocksdbP.transactionCommit(tran1);
        };
        const t2 = async () => {
          const tran2 = rocksdbP.transactionInit(db, {});
          const balance1 = parseInt(
            (
              await rocksdbP.transactionMultiGetForUpdate(
                tran2,
                ['balance1'],
                {},
              )
            )[0],
          );
          let balance2 = parseInt(
            (
              await rocksdbP.transactionMultiGetForUpdate(
                tran2,
                ['balance2'],
                {},
              )
            )[0],
          );
          balance2 -= 100;
          expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
          await rocksdbP.transactionPut(tran2, 'balance2', balance2.toString());
          await rocksdbP.transactionCommit(tran2);
        };
        // By using transactionGetForUpdate, we promote the read to a write, where it writes the same value
        // this causes a write-write conflict
        const results = await Promise.allSettled([t1(), t2()]);
        // One will succeed, one will fail
        expect(results.some((result) => result.status === 'fulfilled')).toBe(
          true,
        );
        expect(
          results.some((result) => {
            return (
              result.status === 'rejected' &&
              result.reason.code === 'TRANSACTION_CONFLICT'
            );
          }),
        ).toBe(true);
      });
      test('transactionIteratorInit iterates over overlay defaults to underlay', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        await rocksdbP.dbPut(db, 'K3', '100', {});
        const tran = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionPut(tran, 'K2', '200');
        await rocksdbP.transactionDel(tran, 'K3');
        await rocksdbP.transactionPut(tran, 'K4', '200');
        const iter = rocksdbP.transactionIteratorInit(tran, {});
        expect(await rocksdbP.iteratorNextv(iter, 3)).toEqual([
          [
            ['K1', '100'],
            ['K2', '200'],
            ['K4', '200'],
          ],
          false,
        ]);
        await rocksdbP.iteratorClose(iter);
        await rocksdbP.transactionRollback(tran);
      });
      test('transactionGetForUpdate does not block transactions', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        // T1 locks in K2 and updates K2
        const tran1 = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionGetForUpdate(tran1, 'K2', {});
        await rocksdbP.transactionPut(tran1, 'K2', '200');
        // T2 locks in K2 and updates K2 to the same value
        // if `transactionGetForUpdate` was blocking, then this
        // would result in a deadlock
        const tran2 = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionGetForUpdate(tran2, 'K2', {});
        await rocksdbP.transactionPut(tran2, 'K2', '200');
        await rocksdbP.transactionCommit(tran2);
        // However optimistic transactions never deadlock
        // So T2 commits, but T1 will have conflict exception
        // And therefore the `exclusive` option is not relevant
        // to optimistic transactions
        await expect(rocksdbP.transactionCommit(tran1)).rejects.toHaveProperty(
          'code',
          'TRANSACTION_CONFLICT',
        );
      });
      test('transactionMultiGetForUpdate does not block transactions', async () => {
        await rocksdbP.dbPut(db, 'K1', '100', {});
        await rocksdbP.dbPut(db, 'K2', '100', {});
        // T1 locks in K2 and updates K2
        const tran1 = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionMultiGetForUpdate(tran1, ['K2'], {});
        await rocksdbP.transactionPut(tran1, 'K2', '200');
        // T2 locks in K2 and updates K2 to the same value
        // if `transactionGetForUpdate` was blocking, then this
        // would result in a deadlock
        const tran2 = rocksdbP.transactionInit(db, {});
        await rocksdbP.transactionMultiGetForUpdate(tran2, ['K2'], {});
        await rocksdbP.transactionPut(tran2, 'K2', '200');
        await rocksdbP.transactionCommit(tran2);
        // However optimistic transactions never deadlock
        // So T2 commits, but T1 will have conflict exception
        // And therefore the `exclusive` option is not relevant
        // to optimistic transactions
        await expect(rocksdbP.transactionCommit(tran1)).rejects.toHaveProperty(
          'code',
          'TRANSACTION_CONFLICT',
        );
      });
      describe('transaction without snapshot', () => {
        test('no conflict when db write occurs before transaction write', async () => {
          // No conflict since the write directly to DB occurred before the transaction write occurred
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.transactionPut(tran, 'K1', '200');
          await rocksdbP.transactionCommit(tran);
          expect(await rocksdbP.dbGet(db, 'K1', {})).toBe('200');
        });
        test('conflicts when db write occurs after transaction write', async () => {
          // Conflict because write directly to DB occurred after the transaction write occurred
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.transactionPut(tran, 'K1', '200');
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
            'code',
            'TRANSACTION_CONFLICT',
          );
        });
        test('transactionGet non-repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          expect(await rocksdbP.transactionGet(tran, 'K1', {})).toBe('100');
          await rocksdbP.dbPut(db, 'K1', '200', {});
          expect(await rocksdbP.transactionGet(tran, 'K1', {})).toBe('200');
          await rocksdbP.transactionCommit(tran);
        });
        test('transactionGetForUpdate non-repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          expect(await rocksdbP.transactionGetForUpdate(tran, 'K1', {})).toBe(
            '100',
          );
          await rocksdbP.dbPut(db, 'K1', '200', {});
          expect(await rocksdbP.transactionGetForUpdate(tran, 'K1', {})).toBe(
            '200',
          );
          await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
            'code',
            'TRANSACTION_CONFLICT',
          );
        });
        test('iterator non-repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.dbPut(db, 'K1', '200', {});
          await rocksdbP.dbPut(db, 'K2', '200', {});
          const iter1 = rocksdbP.transactionIteratorInit(tran, {});
          expect(await rocksdbP.iteratorNextv(iter1, 2)).toEqual([
            [
              ['K1', '200'],
              ['K2', '200'],
            ],
            false,
          ]);
          await rocksdbP.iteratorClose(iter1);
          await rocksdbP.dbPut(db, 'K1', '300', {});
          await rocksdbP.dbPut(db, 'K2', '300', {});
          const iter2 = rocksdbP.transactionIteratorInit(tran, {});
          expect(await rocksdbP.iteratorNextv(iter2, 2)).toEqual([
            [
              ['K1', '300'],
              ['K2', '300'],
            ],
            false,
          ]);
          await rocksdbP.iteratorClose(iter2);
          await rocksdbP.transactionRollback(tran);
        });
        test('clear with non-repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          // This will delete K1, K2, K3, K4
          await rocksdbP.transactionClear(tran, {});
          await rocksdbP.transactionCommit(tran);
          await expect(rocksdbP.dbGet(db, 'K1', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          await expect(rocksdbP.dbGet(db, 'K2', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          await expect(rocksdbP.dbGet(db, 'K3', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          await expect(rocksdbP.dbGet(db, 'K4', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
        });
        test('transactionMultiGet with non-repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          expect(
            await rocksdbP.transactionMultiGet(
              tran,
              ['K1', 'K2', 'K3', 'K4', 'K5'],
              {},
            ),
          ).toEqual(['100', '200', '200', '200', undefined]);
          await rocksdbP.transactionCommit(tran);
        });
        test('transactionMultiGetForUpdate with non-repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          expect(
            await rocksdbP.transactionMultiGetForUpdate(
              tran,
              ['K1', 'K2', 'K3', 'K4', 'K5'],
              {},
            ),
          ).toEqual(['100', '200', '200', '200', undefined]);
          // No conflict because K4 write was done prior to `transactionMultiGetForUpdate`
          await rocksdbP.transactionCommit(tran);
        });
      });
      describe('transaction with snapshot', () => {
        test('conflicts when db write occurs after snapshot creation', async () => {
          const tran = rocksdbP.transactionInit(db, {});
          rocksdbP.transactionSnapshot(tran);
          // Conflict because snapshot was set at the beginning of the transaction
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.transactionPut(tran, 'K1', '200');
          await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
            'code',
            'TRANSACTION_CONFLICT',
          );
        });
        test('transactionGet repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          expect(
            await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap }),
          ).toBe('100');
          await rocksdbP.dbPut(db, 'K1', '200', {});
          expect(
            await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap }),
          ).toBe('100');
          await rocksdbP.transactionRollback(tran);
        });
        test('transactionGet repeatable reads use write overlay', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          expect(
            await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap }),
          ).toBe('100');
          await rocksdbP.transactionPut(tran, 'K1', '300');
          await rocksdbP.dbPut(db, 'K1', '200', {});
          // Here even though we're using the snapshot, because the transaction has 300 written
          // it ends up using 300, but it ignores the 200 that's written directly to the DB
          expect(
            await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap }),
          ).toBe('300');
          await rocksdbP.transactionRollback(tran);
        });
        test('transactionGetForUpdate repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          expect(
            await rocksdbP.transactionGetForUpdate(tran, 'K1', {
              snapshot: tranSnap,
            }),
          ).toBe('100');
          await rocksdbP.dbPut(db, 'K1', '200', {});
          expect(
            await rocksdbP.transactionGetForUpdate(tran, 'K1', {
              snapshot: tranSnap,
            }),
          ).toBe('100');
          await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
            'code',
            'TRANSACTION_CONFLICT',
          );
        });
        test('iterator repeatable reads', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          await rocksdbP.transactionPut(tran, 'K3', '100');
          const tranSnap1 = rocksdbP.transactionSnapshot(tran);
          const iter1 = rocksdbP.transactionIteratorInit(tran, {
            snapshot: tranSnap1,
          });
          expect(await rocksdbP.iteratorNextv(iter1, 3)).toEqual([
            [
              ['K1', '100'],
              ['K2', '100'],
              ['K3', '100'],
            ],
            false,
          ]);
          await rocksdbP.iteratorClose(iter1);
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K1', '200', {});
          const iter2 = rocksdbP.transactionIteratorInit(tran, {
            snapshot: tranSnap1,
          });
          // Notice that this iteration uses the new values written
          // to in this transaction, this mean the snapshot only applies
          // to the underlying database, it's not a snapshot on the transaction
          // writes
          expect(await rocksdbP.iteratorNextv(iter2, 3)).toEqual([
            [
              ['K1', '100'],
              ['K2', '200'],
              ['K3', '200'],
            ],
            false,
          ]);
          await rocksdbP.iteratorClose(iter2);
          // Resetting the snapshot for the transaction
          // Now the snapshot takes the current state of the DB,
          // but the transaction writes are overlayed on top
          const tranSnap2 = rocksdbP.transactionSnapshot(tran);
          await rocksdbP.dbPut(db, 'K2', '300', {});
          const iter3 = rocksdbP.transactionIteratorInit(tran, {
            snapshot: tranSnap2,
          });
          expect(await rocksdbP.iteratorNextv(iter3, 3)).toEqual([
            [
              ['K1', '200'],
              ['K2', '200'],
              ['K3', '200'],
            ],
            false,
          ]);
          await rocksdbP.iteratorClose(iter3);
          // Therefore iterators should always use the snapshot taken
          // at the beginning of the transaction
          await rocksdbP.transactionRollback(tran);
        });
        test('clear with repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          // This will delete K1, K2, K3
          await rocksdbP.transactionClear(tran, { snapshot: tranSnap });
          await rocksdbP.transactionCommit(tran);
          await expect(rocksdbP.dbGet(db, 'K1', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          await expect(rocksdbP.dbGet(db, 'K2', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          await expect(rocksdbP.dbGet(db, 'K3', {})).rejects.toHaveProperty(
            'code',
            'NOT_FOUND',
          );
          expect(await rocksdbP.dbGet(db, 'K4', {})).toBe('200');
        });
        test('transactionMultiGet with repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          expect(
            await rocksdbP.transactionMultiGet(
              tran,
              ['K1', 'K2', 'K3', 'K4', 'K5'],
              {
                snapshot: tranSnap,
              },
            ),
          ).toEqual(['100', '200', '200', undefined, undefined]);
          await rocksdbP.transactionCommit(tran);
        });
        test('transactionMultiGetForUpdate with repeatable read', async () => {
          await rocksdbP.dbPut(db, 'K1', '100', {});
          await rocksdbP.dbPut(db, 'K2', '100', {});
          const tran = rocksdbP.transactionInit(db, {});
          const tranSnap = rocksdbP.transactionSnapshot(tran);
          await rocksdbP.transactionPut(tran, 'K2', '200');
          await rocksdbP.transactionPut(tran, 'K3', '200');
          await rocksdbP.dbPut(db, 'K4', '200', {});
          expect(
            await rocksdbP.transactionMultiGetForUpdate(
              tran,
              ['K1', 'K2', 'K3', 'K4', 'K5'],
              {
                snapshot: tranSnap,
              },
            ),
          ).toEqual(['100', '200', '200', undefined, undefined]);
          // Conflict because of K4 write was done after snapshot
          await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
            'code',
            'TRANSACTION_CONFLICT',
          );
        });
      });
    });
  });
});
