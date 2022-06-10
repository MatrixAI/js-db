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

/* eslint-disable @typescript-eslint/naming-convention */
interface RocksDBP {
  db_init(): RocksDBDatabase;
  db_open(
    database: RocksDBDatabase,
    location: string,
    options: RocksDBDatabaseOptions,
  ): Promise<void>;
  db_close(database: RocksDBDatabase): Promise<void>;
  db_put(
    database: RocksDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: RocksDBPutOptions,
  ): Promise<void>;
  db_get(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<string>;
  db_get(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Buffer>;
  db_get_many(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<Array<string>>;
  db_get_many(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Array<Buffer>>;
  db_del(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBDelOptions,
  ): Promise<void>;
  db_clear(
    database: RocksDBDatabase,
    options: RocksDBClearOptions,
  ): Promise<void>;
  db_approximate_size(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<number>;
  db_compact_range(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<void>;
  db_get_property(database: RocksDBDatabase, property: string): string;
  destroy_db(location: string): Promise<void>;
  repair_db(location: string): Promise<void>;
  iterator_init(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, Buffer>;
  iterator_init(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { keyEncoding: 'buffer' },
  ): RocksDBIterator<Buffer, string>;
  iterator_init(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { valueEncoding: 'buffer' },
  ): RocksDBIterator<string, Buffer>;
  iterator_init(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions,
  ): RocksDBIterator<string, string>;
  iterator_seek<K extends string | Buffer>(
    iterator: RocksDBIterator<K>,
    target: K,
  ): void;
  iterator_close(iterator: RocksDBIterator): Promise<void>;
  iterator_nextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: RocksDBIterator<K, V>,
    size: number,
  ): Promise<[Array<[K, V]>, boolean]>;
  batch_do(
    database: RocksDBDatabase,
    operations: Array<RocksDBBatchPutOperation | RocksDBBatchDelOperation>,
    options: RocksDBBatchOptions,
  ): Promise<void>;
  batch_init(database: RocksDBDatabase): RocksDBBatch;
  batch_put(
    batch: RocksDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batch_del(batch: RocksDBBatch, key: string | Buffer): void;
  batch_clear(batch: RocksDBBatch): void;
  batch_write(batch: RocksDBBatch, options: RocksDBBatchOptions): Promise<void>;
  transaction_init(
    database: RocksDBDatabase,
    options: RocksDBTransactionOptions
  ): RocksDBTransaction;
  transaction_commit(tran: RocksDBTransaction): Promise<void>;
  transaction_rollback(tran: RocksDBTransaction): Promise<void>;
}
/* eslint-enable @typescript-eslint/naming-convention */

/**
 * Promisified version of RocksDB
 */
const rocksdbP: RocksDBP = {
  db_init: rocksdb.db_init.bind(rocksdb),
  db_open: utils.promisify(rocksdb.db_open).bind(rocksdb),
  db_close: utils.promisify(rocksdb.db_close).bind(rocksdb),
  db_put: utils.promisify(rocksdb.db_put).bind(rocksdb),
  db_get: utils.promisify(rocksdb.db_get).bind(rocksdb),
  db_get_many: utils.promisify(rocksdb.db_get_many).bind(rocksdb),
  db_del: utils.promisify(rocksdb.db_del).bind(rocksdb),
  db_clear: utils.promisify(rocksdb.db_clear).bind(rocksdb),
  db_approximate_size: utils
    .promisify(rocksdb.db_approximate_size)
    .bind(rocksdb),
  db_compact_range: utils.promisify(rocksdb.db_compact_range).bind(rocksdb),
  db_get_property: rocksdb.db_get_property.bind(rocksdb),
  destroy_db: utils.promisify(rocksdb.destroy_db).bind(rocksdb),
  repair_db: utils.promisify(rocksdb.repair_db).bind(rocksdb),
  iterator_init: rocksdb.iterator_init.bind(rocksdb),
  iterator_seek: rocksdb.iterator_seek.bind(rocksdb),
  iterator_close: utils.promisify(rocksdb.iterator_close).bind(rocksdb),
  iterator_nextv: utils.promisify(rocksdb.iterator_nextv).bind(rocksdb),
  batch_do: utils.promisify(rocksdb.batch_do).bind(rocksdb),
  batch_init: rocksdb.batch_init.bind(rocksdb),
  batch_put: rocksdb.batch_put.bind(rocksdb),
  batch_del: rocksdb.batch_del.bind(rocksdb),
  batch_clear: rocksdb.batch_clear.bind(rocksdb),
  batch_write: rocksdb.batch_write.bind(rocksdb),
  transaction_init: rocksdb.transaction_init.bind(rocksdb),
  transaction_commit: utils.promisify(rocksdb.transaction_commit).bind(rocksdb),
  transaction_rollback: utils.promisify(rocksdb.transaction_rollback).bind(rocksdb),
};

export default rocksdbP;
