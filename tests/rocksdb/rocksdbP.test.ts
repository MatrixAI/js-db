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
});
