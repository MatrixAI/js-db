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
  test('db_open invalid log level option', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await expect(rocksdbP.db_open(db, dbPath, {
      // @ts-ignore
      infoLogLevel: 'incorrect'
    })).rejects.toHaveProperty('code', 'DB_OPEN');
  });
  test('db_close is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await rocksdbP.db_open(db, dbPath, {});
    await expect(rocksdbP.db_close(db)).resolves.toBeUndefined();
    await expect(rocksdbP.db_close(db)).resolves.toBeUndefined();
  });
  test('iterator_close is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await rocksdbP.db_open(db, dbPath, {});
    const it = rocksdbP.iterator_init(db, {});
    await expect(rocksdbP.iterator_close(it)).resolves.toBeUndefined();
    await expect(rocksdbP.iterator_close(it)).resolves.toBeUndefined();
    await rocksdbP.db_close(db);
  });
  test('transaction_commit is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await rocksdbP.db_open(db, dbPath, {});
    const tran = rocksdbP.transaction_init(db, {});
    await expect(rocksdbP.transaction_commit(tran)).resolves.toBeUndefined();
    await expect(rocksdbP.transaction_commit(tran)).resolves.toBeUndefined();
    await rocksdbP.db_close(db);
  });
  test('transaction_rollback is idempotent', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await rocksdbP.db_open(db, dbPath, {});
    const tran = rocksdbP.transaction_init(db, {});
    await expect(rocksdbP.transaction_rollback(tran)).resolves.toBeUndefined();
    await expect(rocksdbP.transaction_rollback(tran)).resolves.toBeUndefined();
    await rocksdbP.db_close(db);
  });
  test('transaction_commit after rollback', async () => {
    const dbPath = `${dataDir}/db`;
    const db = rocksdbP.db_init();
    await rocksdbP.db_open(db, dbPath, {});
    const tran = rocksdbP.transaction_init(db, {});
    await rocksdbP.transaction_rollback(tran);
    await expect(rocksdbP.transaction_commit(tran)).rejects.toHaveProperty(
      'code',
      'TRANSACTION_ROLLBACKED'
    );
    await rocksdbP.db_close(db);
  });
});
