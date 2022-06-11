import os from 'os';
import path from 'path';
import fs from 'fs';
import rocksdbP from '@/rocksdb/rocksdbP';
import rocksdb from '@/rocksdb/rocksdb';

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
});
