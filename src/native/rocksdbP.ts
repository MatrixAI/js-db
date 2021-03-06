import type {
  RocksDBDatabase,
  RocksDBIterator,
  RocksDBTransaction,
  RocksDBSnapshot,
  RocksDBTransactionSnapshot,
  RocksDBBatch,
  RocksDBDatabaseOptions,
  RocksDBGetOptions,
  RocksDBPutOptions,
  RocksDBDelOptions,
  RocksDBClearOptions,
  RocksDBCountOptions,
  RocksDBIteratorOptions,
  RocksDBTransactionOptions,
  RocksDBBatchOptions,
  RocksDBBatchDelOperation,
  RocksDBBatchPutOperation,
} from './types';
import rocksdb from './rocksdb';
import * as utils from '../utils';

interface RocksDBP {
  dbInit(): RocksDBDatabase;
  dbOpen(
    database: RocksDBDatabase,
    location: string,
    options: RocksDBDatabaseOptions,
  ): Promise<void>;
  dbClose(database: RocksDBDatabase): Promise<void>;
  dbGet(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<string>;
  dbGet(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Buffer>;
  dbMultiGet(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<Array<string>>;
  dbMultiGet(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Array<Buffer>>;
  dbPut(
    database: RocksDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: RocksDBPutOptions,
  ): Promise<void>;
  dbDel(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBDelOptions,
  ): Promise<void>;
  dbClear(
    database: RocksDBDatabase,
    options: RocksDBClearOptions,
  ): Promise<void>;
  dbCount(
    database: RocksDBDatabase,
    options: RocksDBCountOptions,
  ): Promise<number>;
  dbApproximateSize(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<number>;
  dbCompactRange(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<void>;
  dbGetProperty(database: RocksDBDatabase, property: string): string;
  snapshotInit(database: RocksDBDatabase): RocksDBSnapshot;
  snapshotRelease(snapshot: RocksDBSnapshot): Promise<void>;
  destroyDb(location: string): Promise<void>;
  repairDb(location: string): Promise<void>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, Buffer>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { keyEncoding: 'buffer' },
  ): RocksDBIterator<Buffer, string>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { valueEncoding: 'buffer' },
  ): RocksDBIterator<string, Buffer>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions,
  ): RocksDBIterator<string, string>;
  iteratorSeek<K extends string | Buffer>(
    iterator: RocksDBIterator<K>,
    target: K,
  ): void;
  iteratorClose(iterator: RocksDBIterator): Promise<void>;
  iteratorNextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: RocksDBIterator<K, V>,
    size: number,
  ): Promise<[Array<[K, V]>, boolean]>;
  batchDo(
    database: RocksDBDatabase,
    operations: Array<RocksDBBatchPutOperation | RocksDBBatchDelOperation>,
    options: RocksDBBatchOptions,
  ): Promise<void>;
  batchInit(database: RocksDBDatabase): RocksDBBatch;
  batchPut(
    batch: RocksDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batchDel(batch: RocksDBBatch, key: string | Buffer): void;
  batchClear(batch: RocksDBBatch): void;
  batchWrite(batch: RocksDBBatch, options: RocksDBBatchOptions): Promise<void>;
  transactionInit(
    database: RocksDBDatabase,
    options: RocksDBTransactionOptions,
  ): RocksDBTransaction;
  transactionId(transaction: RocksDBTransaction): number;
  transactionCommit(transaction: RocksDBTransaction): Promise<void>;
  transactionRollback(transaction: RocksDBTransaction): Promise<void>;
  transactionGet(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
  ): Promise<string>;
  transactionGet(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): Promise<Buffer>;
  transactionGetForUpdate(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
  ): Promise<string>;
  transactionGetForUpdate(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): Promise<Buffer>;
  transactionMultiGet(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
  ): Promise<Array<string>>;
  transactionMultiGet(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): Promise<Array<Buffer>>;
  transactionMultiGetForUpdate(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
  ): Promise<Array<string>>;
  transactionMultiGetForUpdate(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): Promise<Array<Buffer>>;
  transactionPut(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    value: string | Buffer,
  ): Promise<void>;
  transactionDel(
    transaction: RocksDBTransaction,
    key: string | Buffer,
  ): Promise<void>;
  transactionSnapshot(
    transaction: RocksDBTransaction,
  ): RocksDBTransactionSnapshot;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, Buffer>;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      keyEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, string>;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<string, Buffer>;
  transactionIteratorInit(
    database: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot>,
  ): RocksDBIterator<string, string>;
  transactionClear(
    transaction: RocksDBTransaction,
    options: RocksDBClearOptions<RocksDBTransactionSnapshot>,
  ): Promise<void>;
  transactionCount(
    transaction: RocksDBTransaction,
    options: RocksDBCountOptions<RocksDBTransactionSnapshot>,
  ): Promise<number>;
}

/**
 * Promisified version of RocksDB
 */
const rocksdbP: RocksDBP = {
  dbInit: rocksdb.dbInit.bind(rocksdb),
  dbOpen: utils.promisify(rocksdb.dbOpen).bind(rocksdb),
  dbClose: utils.promisify(rocksdb.dbClose).bind(rocksdb),
  dbGet: utils.promisify(rocksdb.dbGet).bind(rocksdb),
  dbMultiGet: utils.promisify(rocksdb.dbMultiGet).bind(rocksdb),
  dbPut: utils.promisify(rocksdb.dbPut).bind(rocksdb),
  dbDel: utils.promisify(rocksdb.dbDel).bind(rocksdb),
  dbClear: utils.promisify(rocksdb.dbClear).bind(rocksdb),
  dbCount: utils.promisify(rocksdb.dbCount).bind(rocksdb),
  dbApproximateSize: utils.promisify(rocksdb.dbApproximateSize).bind(rocksdb),
  dbCompactRange: utils.promisify(rocksdb.dbCompactRange).bind(rocksdb),
  dbGetProperty: rocksdb.dbGetProperty.bind(rocksdb),
  snapshotInit: rocksdb.snapshotInit.bind(rocksdb),
  snapshotRelease: utils.promisify(rocksdb.snapshotRelease).bind(rocksdb),
  destroyDb: utils.promisify(rocksdb.destroyDb).bind(rocksdb),
  repairDb: utils.promisify(rocksdb.repairDb).bind(rocksdb),
  iteratorInit: rocksdb.iteratorInit.bind(rocksdb),
  iteratorSeek: rocksdb.iteratorSeek.bind(rocksdb),
  iteratorClose: utils.promisify(rocksdb.iteratorClose).bind(rocksdb),
  iteratorNextv: utils.promisify(rocksdb.iteratorNextv).bind(rocksdb),
  batchDo: utils.promisify(rocksdb.batchDo).bind(rocksdb),
  batchInit: rocksdb.batchInit.bind(rocksdb),
  batchPut: rocksdb.batchPut.bind(rocksdb),
  batchDel: rocksdb.batchDel.bind(rocksdb),
  batchClear: rocksdb.batchClear.bind(rocksdb),
  batchWrite: rocksdb.batchWrite.bind(rocksdb),
  transactionInit: rocksdb.transactionInit.bind(rocksdb),
  transactionId: rocksdb.transactionId.bind(rocksdb),
  transactionCommit: utils.promisify(rocksdb.transactionCommit).bind(rocksdb),
  transactionRollback: utils
    .promisify(rocksdb.transactionRollback)
    .bind(rocksdb),
  transactionGet: utils.promisify(rocksdb.transactionGet).bind(rocksdb),
  transactionGetForUpdate: utils
    .promisify(rocksdb.transactionGetForUpdate)
    .bind(rocksdb),
  transactionMultiGet: utils
    .promisify(rocksdb.transactionMultiGet)
    .bind(rocksdb),
  transactionMultiGetForUpdate: utils
    .promisify(rocksdb.transactionMultiGetForUpdate)
    .bind(rocksdb),
  transactionPut: utils.promisify(rocksdb.transactionPut).bind(rocksdb),
  transactionDel: utils.promisify(rocksdb.transactionDel).bind(rocksdb),
  transactionSnapshot: rocksdb.transactionSnapshot.bind(rocksdb),
  transactionIteratorInit: rocksdb.transactionIteratorInit.bind(rocksdb),
  transactionClear: utils.promisify(rocksdb.transactionClear).bind(rocksdb),
  transactionCount: utils.promisify(rocksdb.transactionCount).bind(rocksdb),
};

export default rocksdbP;

export type { RocksDBP };
