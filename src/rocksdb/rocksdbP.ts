import type {
  RocksDBDatabase,
  RocksDBIterator,
  RocksDBTransaction,
  RocksDBBatch,
  RocksDBDatabaseOptions,
  RocksDBGetOptions,
  RocksDBPutOptions,
  RocksDBDelOptions,
  RocksDBClearOptions,
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
  dbPut(
    database: RocksDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: RocksDBPutOptions,
  ): Promise<void>;
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
  dbGetMany(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<Array<string>>;
  dbGetMany(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Array<Buffer>>;
  dbDel(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBDelOptions,
  ): Promise<void>;
  dbClear(
    database: RocksDBDatabase,
    options: RocksDBClearOptions,
  ): Promise<void>;
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
    options: RocksDBTransactionOptions
  ): RocksDBTransaction;
  transactionCommit(tran: RocksDBTransaction): Promise<void>;
  transactionRollback(tran: RocksDBTransaction): Promise<void>;
}

/**
 * Promisified version of RocksDB
 */
const rocksdbP: RocksDBP = {
  dbInit: rocksdb.dbInit.bind(rocksdb),
  dbOpen: utils.promisify(rocksdb.dbOpen).bind(rocksdb),
  dbClose: utils.promisify(rocksdb.dbClose).bind(rocksdb),
  dbPut: utils.promisify(rocksdb.dbPut).bind(rocksdb),
  dbGet: utils.promisify(rocksdb.dbGet).bind(rocksdb),
  dbGetMany: utils.promisify(rocksdb.dbGetMany).bind(rocksdb),
  dbDel: utils.promisify(rocksdb.dbDel).bind(rocksdb),
  dbClear: utils.promisify(rocksdb.dbClear).bind(rocksdb),
  dbApproximateSize: utils
    .promisify(rocksdb.dbApproximateSize)
    .bind(rocksdb),
  dbCompactRange: utils.promisify(rocksdb.dbCompactRange).bind(rocksdb),
  dbGetProperty: rocksdb.dbGetProperty.bind(rocksdb),
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
  transactionCommit: utils.promisify(rocksdb.transactionCommit).bind(rocksdb),
  transactionRollback: utils.promisify(rocksdb.transactionRollback).bind(rocksdb),
};

export default rocksdbP;
