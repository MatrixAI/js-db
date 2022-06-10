import type { Opaque } from '../types';

/**
 * RocksDBDatabase object
 * A `napi_external` type
 */
type RocksDBDatabase = Opaque<'RocksDBDatabase', object>;

/**
 * RocksDBIterator object
 * A `napi_external` type
 * If `keys` or `values` is set to `false` then
 * `K` and `V` will be an empty buffer
 * If `keys` and `values` is set to `false`, the iterator will
 * give back empty array as entries
 */
type RocksDBIterator<
  K extends string | Buffer = string | Buffer,
  V extends string | Buffer = string | Buffer,
> = Opaque<'RocksDBIterator', object> & {
  readonly [brandRocksDBIteratorK]: K;
  readonly [brandRocksDBIteratorV]: V;
};
declare const brandRocksDBIteratorK: unique symbol;
declare const brandRocksDBIteratorV: unique symbol;

/**
 * RocksDBTransaction object
 * A `napi_external` type
 */
type RocksDBTransaction = Opaque<'RocksDBTransaction', object>;

/**
 * RocksDBBatch object
 * A `napi_external` type
 */
type RocksDBBatch = Opaque<'RocksDBBatch', object>;

/**
 * RocksDB database options
 * Note that `undefined` is not a valid value for these options
 * Make sure that the property either exists and it is a correct type
 * or that it does not exist
 */
type RocksDBDatabaseOptions = {
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
type RocksDBGetOptions = {
  valueEncoding?: 'utf8' | 'buffer'; // Default 'utf8';
  fillCache?: boolean; // Default true
};

/**
 * Put options
 * Note that `undefined` is not a valid value for these options
 * Make sure that the property either exists and it is a correct type
 * or that it does not exist
 */
type RocksDBPutOptions = {
  /**
   * If `true`, rocksdb will perform `fsync()` before completing operation
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
type RocksDBDelOptions = RocksDBPutOptions;

/**
 * Range options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type RocksDBRangeOptions = {
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
type RocksDBClearOptions = RocksDBRangeOptions;

/**
 * Iterator options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type RocksDBIteratorOptions = RocksDBGetOptions &
  RocksDBRangeOptions & {
    keys?: boolean;
    values?: boolean;
    keyEncoding?: 'utf8' | 'buffer'; // Default 'utf8'
    highWaterMarkBytes?: number; // Default is 16 * 1024
  };

/**
 * Transaction options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type RocksDBTransactionOptions = RocksDBPutOptions;

/**
 * Batch options
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */
type RocksDBBatchOptions = RocksDBPutOptions;

type RocksDBBatchPutOperation = {
  type: 'put';
  key: string | Buffer;
  value: string | Buffer;
};

type RocksDBBatchDelOperation = {
  type: 'del';
  key: string | Buffer;
};

export type {
  RocksDBDatabase,
  RocksDBIterator,
  RocksDBTransaction,
  RocksDBBatch,
  RocksDBDatabaseOptions,
  RocksDBGetOptions,
  RocksDBPutOptions,
  RocksDBDelOptions,
  RocksDBRangeOptions,
  RocksDBClearOptions,
  RocksDBIteratorOptions,
  RocksDBTransactionOptions,
  RocksDBBatchOptions,
  RocksDBBatchDelOperation,
  RocksDBBatchPutOperation,
};
