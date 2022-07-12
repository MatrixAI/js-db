import type fs from 'fs';
import type { RWLockWriter } from '@matrixai/async-locks';
import type { WorkerManagerInterface } from '@matrixai/workers';
import type {
  RocksDBDatabaseOptions,
  RocksDBIteratorOptions,
  RocksDBBatchPutOperation,
  RocksDBBatchDelOperation,
  RocksDBClearOptions,
  RocksDBCountOptions,
  RocksDBSnapshot,
  RocksDBTransactionSnapshot,
} from './native/types';

/**
 * Plain data dictionary
 */
type POJO = { [key: string]: any };

/**
 * Any type that can be turned into a string
 */
interface ToString {
  toString(): string;
}

/**
 * Opaque types are wrappers of existing types
 * that require smart constructors
 */
type Opaque<K, T> = T & { readonly [brand]: K };
declare const brand: unique symbol;

/**
 * Generic callback
 */
type Callback<P extends Array<any> = [], R = any, E extends Error = Error> = {
  (e: E, ...params: Partial<P>): R;
  (e?: null | undefined, ...params: P): R;
};

/**
 * Merge A property types with B property types
 * while B's property types override A's property types
 */
type Merge<A, B> = {
  [K in keyof (A & B)]: K extends keyof B
    ? B[K]
    : K extends keyof A
    ? A[K]
    : never;
};

interface FileSystem {
  promises: {
    rm: typeof fs.promises.rm;
    mkdir: typeof fs.promises.mkdir;
  };
}

/**
 * Crypto utility object
 * Remember ever Node Buffer is an ArrayBuffer
 */
type Crypto = {
  encrypt(key: ArrayBuffer, plainText: ArrayBuffer): Promise<ArrayBuffer>;
  decrypt(
    key: ArrayBuffer,
    cipherText: ArrayBuffer,
  ): Promise<ArrayBuffer | undefined>;
};

type DBWorkerManagerInterface = WorkerManagerInterface<Crypto>;

/**
 * Path to a key
 * This must be an non-empty array
 */
type KeyPath = Readonly<Array<string | Buffer>>;

/**
 * Path to a DB level
 * Empty level path refers to the root level
 */
type LevelPath = Readonly<Array<string | Buffer>>;

type DBOptions = Omit<
  RocksDBDatabaseOptions,
  'createIfMissing' | 'errorIfExists'
>;

/**
 * Iterator options
 * The `keyAsBuffer` property controls
 * whether DBIterator returns KeyPath as buffers or as strings
 * It should be considered to default to true
 * The `valueAsBuffer` property controls value type
 * It should be considered to default to true
 */
type DBIteratorOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = Merge<
  Omit<RocksDBIteratorOptions<S>, 'keyEncoding' | 'valueEncoding'>,
  {
    gt?: KeyPath | Buffer | string;
    gte?: KeyPath | Buffer | string;
    lt?: KeyPath | Buffer | string;
    lte?: KeyPath | Buffer | string;
    keyAsBuffer?: boolean;
    valueAsBuffer?: boolean;
  }
>;

type DBClearOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = Merge<
  RocksDBClearOptions<S>,
  {
    gt?: KeyPath | Buffer | string;
    gte?: KeyPath | Buffer | string;
    lt?: KeyPath | Buffer | string;
    lte?: KeyPath | Buffer | string;
  }
>;

type DBCountOptions<
  S extends RocksDBSnapshot | RocksDBTransactionSnapshot = RocksDBSnapshot,
> = Merge<
  RocksDBCountOptions<S>,
  {
    gt?: KeyPath | Buffer | string;
    gte?: KeyPath | Buffer | string;
    lt?: KeyPath | Buffer | string;
    lte?: KeyPath | Buffer | string;
  }
>;

type DBBatch = RocksDBBatchPutOperation | RocksDBBatchDelOperation;

type DBOp_ =
  | {
      keyPath: KeyPath | string | Buffer;
      value: any;
      raw?: false;
    }
  | {
      keyPath: KeyPath | string | Buffer;
      value: Buffer;
      raw: true;
    };

type DBOp =
  | ({
      type: 'put';
    } & DBOp_)
  | ({
      type: 'del';
    } & Omit<DBOp_, 'value' | 'raw'>);

type DBOps = Array<DBOp>;

type MultiLockRequest = [
  key: ToString,
  ...lockingParams: Parameters<RWLockWriter['lock']>,
];

export type {
  POJO,
  ToString,
  Opaque,
  Callback,
  Merge,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  KeyPath,
  LevelPath,
  DBOptions,
  DBIteratorOptions,
  DBClearOptions,
  DBCountOptions,
  DBBatch,
  DBOp,
  DBOps,
  MultiLockRequest,
};
