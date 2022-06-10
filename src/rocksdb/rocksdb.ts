import type { Callback } from '../types';
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
import path from 'path';
import nodeGypBuild from 'node-gyp-build';

/* eslint-disable @typescript-eslint/naming-convention */
interface RocksDB {
  db_init(): RocksDBDatabase;
  db_open(
    database: RocksDBDatabase,
    location: string,
    options: RocksDBDatabaseOptions,
    callback: Callback<[], void>,
  ): void;
  db_close(database: RocksDBDatabase, callback: Callback<[], void>): void;
  db_put(
    database: RocksDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: RocksDBPutOptions,
    callback: Callback<[], void>,
  ): void;
  db_get(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[string], void>,
  ): void;
  db_get(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Buffer], void>,
  ): void;
  db_get_many(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[Array<string>], void>,
  ): void;
  db_get_many(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Array<Buffer>], void>,
  ): void;
  db_del(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBDelOptions,
    callback: Callback<[], void>,
  ): void;
  db_clear(
    database: RocksDBDatabase,
    options: RocksDBClearOptions,
    callback: Callback<[], void>,
  ): void;
  db_approximate_size(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[number], void>,
  ): void;
  db_compact_range(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[], void>,
  ): void;
  db_get_property(database: RocksDBDatabase, property: string): string;
  destroy_db(location: string, callback: Callback<[], void>): void;
  repair_db(location: string, callback: Callback<[], void>): void;
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
  iterator_close(iterator: RocksDBIterator, callback: Callback<[], void>): void;
  iterator_nextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: RocksDBIterator<K, V>,
    size: number,
    callback: Callback<[Array<[K, V]>, boolean], void>,
  ): void;
  batch_do(
    database: RocksDBDatabase,
    operations: Array<RocksDBBatchPutOperation | RocksDBBatchDelOperation>,
    options: RocksDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
  batch_init(database: RocksDBDatabase): RocksDBBatch;
  batch_put(
    batch: RocksDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batch_del(batch: RocksDBBatch, key: string | Buffer): void;
  batch_clear(batch: RocksDBBatch): void;
  batch_write(
    batch: RocksDBBatch,
    options: RocksDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
  transaction_init(
    database: RocksDBDatabase,
    options: RocksDBTransactionOptions
  ): RocksDBTransaction;
  transaction_commit(
    tran: RocksDBTransaction,
    callback: Callback<[], void>
  ): void;
  transaction_rollback(
    tran: RocksDBTransaction,
    callback: Callback<[], void>
  ): void;
}
/* eslint-enable @typescript-eslint/naming-convention */

const rocksdb: RocksDB = nodeGypBuild(path.join(__dirname, '../../'));

export default rocksdb;
