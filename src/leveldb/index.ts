import type { Opaque, Callback } from '../types';
import path from 'path';
import nodeGypBuild from 'node-gyp-build';
import * as utils from '../utils';

/**
 * LevelDBDatabase object
 * A `napi_external` type
 */
type LevelDBDatabase = Opaque<'LevelDBDatabase', object>;

/**
 * LevelDBIterator object
 * A `napi_external` type
 * If `keys` or `values` is set to `false` then
 * `K` and `V` will be an empty buffer
 * If `keys` and `values` is set to `false`, the iterator will
 * give back empty array as entries
 */
type LevelDBIterator<
  K extends string | Buffer = string | Buffer,
  V extends string | Buffer = string | Buffer,
> = Opaque<'LevelDBIterator', object> & {
  readonly [brandLevelDBIteratorK]: K;
  readonly [brandLevelDBIteratorV]: V;
};
declare const brandLevelDBIteratorK: unique symbol;
declare const brandLevelDBIteratorV: unique symbol;

/**
 * LevelDBBatch object
 * A `napi_external` type
 */
type LevelDBBatch = Opaque<'LevelDBBatch', object>;

/**
 * LevelDB database options
 * Note that `undefined` is not a valid value for these options
 * Make sure that the property either exists and it is a correct type
 * or that it does not exist
 */
type LevelDBDatabaseOptions = {
  createIfMissing?: boolean; // Default true
  errorIfExists?: boolean; // Default false
  compression?: boolean; // Default true
  cacheSize?: number; // Default 8 * 1024 * 1024
  writeBufferSize?: number; // Default 4 * 1024 * 1024
  blockSize?: number; // Default 4096
  maxOpenFiles?: number; // Default 1000
  blockRestartInterval?: number; // Default 16
  maxFileSize?: number; // Default 2 * 1024 * 1024
};

/**
 * Get options
 * Note that `undefined` is not a valid value for these options
 * Make sure that the property either exists and it is a correct type
 * or that it does not exist
 */
type LevelDBGetOptions = {
  valueEncoding?: 'utf8' | 'buffer'; // Default 'utf8';
  fillCache?: boolean; // Default true
};

/**
 * Put options
 * Note that `undefined` is not a valid value for these options
 * Make sure that the property either exists and it is a correct type
 * or that it does not exist
 */
type LevelDBPutOptions = {
  /**
   * If `true`, leveldb will perform `fsync()` before completing operation
   * It is still asynchronous relative to Node.js
   * If the operating system crashes, writes may be lost
   * Prefer to flip this to be true when a transaction batch is written
   * This will amortize the cost of `fsync()` across the entire transaction
   */
  sync?: boolean; // Default false
};

/**
 * Del options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type LevelDBDelOptions = LevelDBPutOptions;

/**
 * Range options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type LevelDBRangeOptions = {
  gt?: string | Buffer;
  gte?: string | Buffer;
  lt?: string | Buffer;
  lte?: string | Buffer;
  reverse?: boolean; // Default false
  limit?: number; // Default -1
};

/**
 * Clear options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type LevelDBClearOptions = LevelDBRangeOptions;

/**
 * Iterator options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type LevelDBIteratorOptions = LevelDBGetOptions &
  LevelDBRangeOptions & {
    keys?: boolean;
    values?: boolean;
    keyEncoding?: 'utf8' | 'buffer'; // Default 'utf8'
    highWaterMarkBytes?: number; // Default is 16 * 1024
  };

/**
 * Batch options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type LevelDBBatchOptions = LevelDBPutOptions;

type LevelDBBatchPutOperation = {
  type: 'put';
  key: string | Buffer;
  value: string | Buffer;
};

type LevelDBBatchDelOperation = {
  type: 'del';
  key: string | Buffer;
};

/* eslint-disable @typescript-eslint/naming-convention */
interface LevelDB {
  db_init(): LevelDBDatabase;
  db_open(
    database: LevelDBDatabase,
    location: string,
    options: LevelDBDatabaseOptions,
    callback: Callback<[], void>,
  ): void;
  db_close(database: LevelDBDatabase, callback: Callback<[], void>): void;
  db_put(
    database: LevelDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: LevelDBPutOptions,
    callback: Callback<[], void>,
  ): void;
  db_get(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[string], void>,
  ): void;
  db_get(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Buffer], void>,
  ): void;
  db_get_many(
    database: LevelDBDatabase,
    keys: Array<string | Buffer>,
    options: LevelDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[Array<string>], void>,
  ): void;
  db_get_many(
    database: LevelDBDatabase,
    keys: Array<string | Buffer>,
    options: LevelDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Array<Buffer>], void>,
  ): void;
  db_del(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBDelOptions,
    callback: Callback<[], void>,
  ): void;
  db_clear(
    database: LevelDBDatabase,
    options: LevelDBClearOptions,
    callback: Callback<[], void>,
  ): void;
  db_approximate_size(
    database: LevelDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[number], void>,
  ): void;
  db_compact_range(
    database: LevelDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[], void>,
  ): void;
  db_get_property(database: LevelDBDatabase, property: string): string;
  destroy_db(location: string, callback: Callback<[], void>): void;
  repair_db(location: string, callback: Callback<[], void>): void;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): LevelDBIterator<Buffer, Buffer>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & { keyEncoding: 'buffer' },
  ): LevelDBIterator<Buffer, string>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & { valueEncoding: 'buffer' },
  ): LevelDBIterator<string, Buffer>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions,
  ): LevelDBIterator<string, string>;
  iterator_seek<K extends string | Buffer>(
    iterator: LevelDBIterator<K>,
    target: K,
  ): void;
  iterator_close(iterator: LevelDBIterator, callback: Callback<[], void>): void;
  iterator_nextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: LevelDBIterator<K, V>,
    size: number,
    callback: Callback<[Array<[K, V]>, boolean], void>,
  ): void;
  batch_do(
    database: LevelDBDatabase,
    operations: Array<LevelDBBatchPutOperation | LevelDBBatchDelOperation>,
    options: LevelDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
  batch_init(database: LevelDBDatabase): LevelDBBatch;
  batch_put(
    batch: LevelDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batch_del(batch: LevelDBBatch, key: string | Buffer): void;
  batch_clear(batch: LevelDBBatch): void;
  batch_write(
    batch: LevelDBBatch,
    options: LevelDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
}

interface LevelDBP {
  db_init(): LevelDBDatabase;
  db_open(
    database: LevelDBDatabase,
    location: string,
    options: LevelDBDatabaseOptions,
  ): Promise<void>;
  db_close(database: LevelDBDatabase): Promise<void>;
  db_put(
    database: LevelDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: LevelDBPutOptions,
  ): Promise<void>;
  db_get(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<string>;
  db_get(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Buffer>;
  db_get_many(
    database: LevelDBDatabase,
    keys: Array<string | Buffer>,
    options: LevelDBGetOptions & { valueEncoding?: 'utf8' },
  ): Promise<Array<string>>;
  db_get_many(
    database: LevelDBDatabase,
    keys: Array<string | Buffer>,
    options: LevelDBGetOptions & { valueEncoding: 'buffer' },
  ): Promise<Array<Buffer>>;
  db_del(
    database: LevelDBDatabase,
    key: string | Buffer,
    options: LevelDBDelOptions,
  ): Promise<void>;
  db_clear(
    database: LevelDBDatabase,
    options: LevelDBClearOptions,
  ): Promise<void>;
  db_approximate_size(
    database: LevelDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<number>;
  db_compact_range(
    database: LevelDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
  ): Promise<void>;
  db_get_property(database: LevelDBDatabase, property: string): string;
  destroy_db(location: string): Promise<void>;
  repair_db(location: string): Promise<void>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): LevelDBIterator<Buffer, Buffer>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & { keyEncoding: 'buffer' },
  ): LevelDBIterator<Buffer, string>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions & { valueEncoding: 'buffer' },
  ): LevelDBIterator<string, Buffer>;
  iterator_init(
    database: LevelDBDatabase,
    options: LevelDBIteratorOptions,
  ): LevelDBIterator<string, string>;
  iterator_seek<K extends string | Buffer>(
    iterator: LevelDBIterator<K>,
    target: K,
  ): void;
  iterator_close(iterator: LevelDBIterator): Promise<void>;
  iterator_nextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: LevelDBIterator<K, V>,
    size: number,
  ): Promise<[Array<[K, V]>, boolean]>;
  batch_do(
    database: LevelDBDatabase,
    operations: Array<LevelDBBatchPutOperation | LevelDBBatchDelOperation>,
    options: LevelDBBatchOptions,
  ): Promise<void>;
  batch_init(database: LevelDBDatabase): LevelDBBatch;
  batch_put(
    batch: LevelDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batch_del(batch: LevelDBBatch, key: string | Buffer): void;
  batch_clear(batch: LevelDBBatch): void;
  batch_write(batch: LevelDBBatch, options: LevelDBBatchOptions): Promise<void>;
}
/* eslint-enable @typescript-eslint/naming-convention */

const leveldb: LevelDB = nodeGypBuild(path.join(__dirname, '../../'));

/**
 * Promisified version of LevelDB
 */
const leveldbP: LevelDBP = {
  db_init: leveldb.db_init.bind(leveldb),
  db_open: utils.promisify(leveldb.db_open).bind(leveldb),
  db_close: utils.promisify(leveldb.db_close).bind(leveldb),
  db_put: utils.promisify(leveldb.db_put).bind(leveldb),
  db_get: utils.promisify(leveldb.db_get).bind(leveldb),
  db_get_many: utils.promisify(leveldb.db_get_many).bind(leveldb),
  db_del: utils.promisify(leveldb.db_del).bind(leveldb),
  db_clear: utils.promisify(leveldb.db_clear).bind(leveldb),
  db_approximate_size: utils
    .promisify(leveldb.db_approximate_size)
    .bind(leveldb),
  db_compact_range: utils.promisify(leveldb.db_compact_range).bind(leveldb),
  db_get_property: leveldb.db_get_property.bind(leveldb),
  destroy_db: utils.promisify(leveldb.destroy_db).bind(leveldb),
  repair_db: utils.promisify(leveldb.repair_db).bind(leveldb),
  iterator_init: leveldb.iterator_init.bind(leveldb),
  iterator_seek: leveldb.iterator_seek.bind(leveldb),
  iterator_close: utils.promisify(leveldb.iterator_close).bind(leveldb),
  iterator_nextv: utils.promisify(leveldb.iterator_nextv).bind(leveldb),
  batch_do: utils.promisify(leveldb.batch_do).bind(leveldb),
  batch_init: leveldb.batch_init.bind(leveldb),
  batch_put: leveldb.batch_put.bind(leveldb),
  batch_del: leveldb.batch_del.bind(leveldb),
  batch_clear: leveldb.batch_clear.bind(leveldb),
  batch_write: leveldb.batch_write.bind(leveldb),
};

export default leveldb;

export { leveldbP };

export type {
  LevelDB,
  LevelDBP,
  LevelDBDatabase,
  LevelDBIterator,
  LevelDBBatch,
  LevelDBDatabaseOptions,
  LevelDBGetOptions,
  LevelDBPutOptions,
  LevelDBDelOptions,
  LevelDBRangeOptions,
  LevelDBClearOptions,
  LevelDBIteratorOptions,
  LevelDBBatchOptions,
  LevelDBBatchPutOperation,
  LevelDBBatchDelOperation,
};
