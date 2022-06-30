import type { Opaque } from '../types';

/**
 * Note that `undefined` is not a valid value for these options
 * If properties exist, they must have the correct type
 */

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
 * RocksDBSnapshot object
 * A `napi_external` type
 */
type RocksDBSnapshot = Opaque<'RocksDBSnapshot', object>;

/**
 * RocksDBTransactionSnapshot object
 * A `napi_external` type
 */
type RocksDBTransactionSnapshot = Opaque<'RocksDBTransactionSnapshot', object>;

/**
 * RocksDB database options
 */
type RocksDBDatabaseOptions = {
  createIfMissing?: boolean; // Default true
  errorIfExists?: boolean; // Default false
  compression?: boolean; // Default true
  infoLogLevel?: 'debug' | 'info' | 'warn' | 'error' | 'fatal' | 'header'; // Default undefined
  cacheSize?: number; // Default 8 * 1024 * 1024
  writeBufferSize?: number; // Default 4 * 1024 * 1024
  blockSize?: number; // Default 4096
  maxOpenFiles?: number; // Default 1000
  blockRestartInterval?: number; // Default 16
  maxFileSize?: number; // Default 2 * 1024 * 1024
};

/**
 * Get options
 */
type RocksDBGetOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = {
  valueEncoding?: 'utf8' | 'buffer'; // Default 'utf8';
  fillCache?: boolean; // Default true
  snapshot?: S;
};

/**
 * Put options
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
 */
type RocksDBDelOptions = RocksDBPutOptions;

/**
 * Range options
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
 */
type RocksDBClearOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = Omit<RocksDBRangeOptions, 'reverse'> & {
  snapshot?: S;
  sync?: S extends RocksDBSnapshot ? boolean : void; // Default false
};

/**
 * Count options
 */
type RocksDBCountOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = Omit<RocksDBRangeOptions, 'reverse'> & {
  snapshot?: S;
};

/**
 * Iterator options
 */
type RocksDBIteratorOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = RocksDBGetOptions<S> &
  RocksDBRangeOptions & {
    keys?: boolean;
    values?: boolean;
    keyEncoding?: 'utf8' | 'buffer'; // Default 'utf8'
    highWaterMarkBytes?: number; // Default is 16 * 1024
  };

/**
 * Transaction options
 */
type RocksDBTransactionOptions = RocksDBPutOptions;

/**
 * Batch options
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
  RocksDBSnapshot,
  RocksDBTransactionSnapshot,
  RocksDBDatabaseOptions,
  RocksDBGetOptions,
  RocksDBPutOptions,
  RocksDBDelOptions,
  RocksDBRangeOptions,
  RocksDBClearOptions,
  RocksDBCountOptions,
  RocksDBIteratorOptions,
  RocksDBTransactionOptions,
  RocksDBBatchOptions,
  RocksDBBatchDelOperation,
  RocksDBBatchPutOperation,
};
