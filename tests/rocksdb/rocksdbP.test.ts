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
    await expect(rocksdbP.dbOpen(db, dbPath, {
      // @ts-ignore
      infoLogLevel: 'incorrect'
    })).rejects.toHaveProperty('code', 'DB_OPEN');
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
  test('dbMultiGet', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    await rocksdbP.dbPut(db, 'foo', 'bar', {});
    await rocksdbP.dbPut(db, 'bar', 'foo', {});
    expect(await rocksdbP.dbMultiGet(db, ['foo', 'bar', 'abc'], {})).toEqual(['bar', 'foo', undefined]);
    await rocksdbP.dbClose(db);
  });
  test('dbGet and dbMultiget with snapshots', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    await rocksdbP.dbPut(db, 'K1', '100', {});
    await rocksdbP.dbPut(db, 'K2', '100', {});
    const snap = rocksdbP.snapshotInit(db);
    await rocksdbP.dbPut(db, 'K1', '200', {});
    await rocksdbP.dbPut(db, 'K2', '200', {});
    expect(await rocksdbP.dbGet(db, 'K1', { snapshot: snap })).toBe('100');
    expect(await rocksdbP.dbGet(db, 'K2', { snapshot: snap })).toBe('100');
    expect(await rocksdbP.dbMultiGet(db, ['K1', 'K2'], {
      snapshot: snap
    })).toEqual(['100', '100']);
    expect(await rocksdbP.dbGet(db, 'K1', {})).toBe('200');
    expect(await rocksdbP.dbGet(db, 'K2', {})).toBe('200');
    expect(await rocksdbP.dbMultiGet(db, ['K1', 'K2'], {})).toEqual(['200', '200']);
    await rocksdbP.snapshotRelease(snap);
    await rocksdbP.dbClose(db);
  });
  test('iteratorClose is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const it = rocksdbP.iteratorInit(db, {});
    await expect(rocksdbP.iteratorClose(it)).resolves.toBeUndefined();
    await expect(rocksdbP.iteratorClose(it)).resolves.toBeUndefined();
    await rocksdbP.dbClose(db);
  });
  test('transactionCommit is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const tran = rocksdbP.transactionInit(db, {});
    await expect(rocksdbP.transactionCommit(tran)).resolves.toBeUndefined();
    await expect(rocksdbP.transactionCommit(tran)).resolves.toBeUndefined();
    await rocksdbP.dbClose(db);
  });
  test('transactionRollback is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const tran = rocksdbP.transactionInit(db, {});
    await expect(rocksdbP.transactionRollback(tran)).resolves.toBeUndefined();
    await expect(rocksdbP.transactionRollback(tran)).resolves.toBeUndefined();
    await rocksdbP.dbClose(db);
  });
  test('transactionCommit after rollback', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const tran = rocksdbP.transactionInit(db, {});
    await rocksdbP.transactionRollback(tran);
    await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty(
      'code',
      'TRANSACTION_ROLLBACKED'
    );
    await rocksdbP.dbClose(db);
  });
  test('transactionGet, transactionPut, transactionDel', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
    const tran = rocksdbP.transactionInit(db, {});
    await rocksdbP.transactionPut(tran, 'foo', 'bar');
    await rocksdbP.transactionPut(tran, 'bar', 'foo');
    expect(await rocksdbP.transactionGet(tran, 'foo', {})).toBe('bar');
    await rocksdbP.transactionDel(tran, 'bar');
    await rocksdbP.transactionCommit(tran);
    expect(await rocksdbP.dbGet(db, 'foo', {})).toBe('bar');
    await expect(rocksdbP.dbGet(db, 'bar', {})).rejects.toHaveProperty('code', 'NOT_FOUND');
    await rocksdbP.dbClose(db);
  });
  test('transactionGetForUpdate addresses write skew by promoting gets into same-value puts', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.dbInit();
    await rocksdbP.dbOpen(db, dbPath, {});
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
      let balance1 = parseInt(await rocksdbP.transactionGetForUpdate(tran1, 'balance1', {}));
      const balance2 = parseInt(await rocksdbP.transactionGetForUpdate(tran1, 'balance2', {}));
      balance1 -= 100;
      expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
      await rocksdbP.transactionPut(tran1, 'balance1', balance1.toString());
      await rocksdbP.transactionCommit(tran1);
    };
    const t2 = async () => {
      const tran2 = rocksdbP.transactionInit(db, {});
      const balance1 = parseInt(await rocksdbP.transactionGetForUpdate(tran2, 'balance1', {}));
      let balance2 = parseInt(await rocksdbP.transactionGetForUpdate(tran2, 'balance2', {}));
      balance2 -= 100;
      expect(balance1 + balance2).toBeGreaterThanOrEqual(0);
      await rocksdbP.transactionPut(tran2, 'balance2', balance2.toString());
      await rocksdbP.transactionCommit(tran2);
    };
    // By using transactionGetForUpdate, we promote the read to a write, where it writes the same value
    // this causes a write-write conflict
    const results = await Promise.allSettled([t1(), t2()]);
    // One will succeed, one will fail
    expect(results.some((result) => result.status === 'fulfilled')).toBe(true);
    expect(results.some((result) => {
      return result.status === 'rejected' && result.reason.code === 'TRANSACTION_CONFLICT';
    })).toBe(true);
    await rocksdbP.dbClose(db);
  });
  describe('transaction without snapshot', () => {
    test('no conflict when db write occurs before transaction write', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      // No conflict since the write directly to DB occurred before the transaction write occurred
      const tran = rocksdbP.transactionInit(db, {});
      await rocksdbP.dbPut(db, 'K1', '100', {});
      await rocksdbP.transactionPut(tran, 'K1', '200');
      await rocksdbP.transactionCommit(tran);
      expect(await rocksdbP.dbGet(db, 'K1', {})).toBe('200');
      await rocksdbP.dbClose(db);
    });
    test('conflicts when db write occurs after transaction write', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      // Conflict because write directly to DB occurred after the transaction write occurred
      const tran = rocksdbP.transactionInit(db, {});
      await rocksdbP.transactionPut(tran, 'K1', '200');
      await rocksdbP.dbPut(db, 'K1', '100', {});
      await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty('code', 'TRANSACTION_CONFLICT');
      await rocksdbP.dbClose(db);
    });
    test('non-repeatable reads', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      await rocksdbP.dbPut(db, 'K1', '100', {});
      const tran = rocksdbP.transactionInit(db, {});
      expect(await rocksdbP.transactionGet(tran, 'K1', {})).toBe('100');
      await rocksdbP.dbPut(db, 'K1', '200', {});
      expect(await rocksdbP.transactionGet(tran, 'K1', {})).toBe('200');
      await rocksdbP.transactionCommit(tran);
      await rocksdbP.dbClose(db);
    });
  });
  describe('transaction with snapshot', () => {
    test('conflicts when db write occurs after snapshot creation', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      const tran = rocksdbP.transactionInit(db, {});
      rocksdbP.transactionSnapshot(tran);
      // Conflict because snapshot was set at the beginning of the transaction
      await rocksdbP.dbPut(db, 'K1', '100', {});
      await rocksdbP.transactionPut(tran, 'K1', '200');
      await expect(rocksdbP.transactionCommit(tran)).rejects.toHaveProperty('code', 'TRANSACTION_CONFLICT');
      await rocksdbP.dbClose(db);
    });
    test('repeatable reads', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      await rocksdbP.dbPut(db, 'K1', '100', {});
      const tran = rocksdbP.transactionInit(db, {});
      const tranSnap = rocksdbP.transactionSnapshot(tran);
      expect(await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap })).toBe('100');
      await rocksdbP.dbPut(db, 'K1', '200', {});
      expect(await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap })).toBe('100');
      await rocksdbP.transactionRollback(tran);
      await rocksdbP.dbClose(db);
    });
    test('repeatable reads use write overlay', async () => {
      const dbPath = `${dataDir}/db`;
      const db = rocksdbP.dbInit();
      await rocksdbP.dbOpen(db, dbPath, {});
      await rocksdbP.dbPut(db, 'K1', '100', {});
      const tran = rocksdbP.transactionInit(db, {});
      const tranSnap = rocksdbP.transactionSnapshot(tran);
      expect(await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap })).toBe('100');
      await rocksdbP.transactionPut(tran, 'K1', '300');
      await rocksdbP.dbPut(db, 'K1', '200', {});
      // Here even though we're using the snapshot, because the transaction has 300 written
      // it ends up using 300, but it ignores the 200 that's written directly to the DB
      expect(await rocksdbP.transactionGet(tran, 'K1', { snapshot: tranSnap })).toBe('300');
      await rocksdbP.transactionRollback(tran);
      await rocksdbP.dbClose(db);
    });
  });
});
