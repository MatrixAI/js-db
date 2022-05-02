import type { AbstractBatch } from 'abstract-leveldown';
import type fs from 'fs';
import type { WorkerManagerInterface } from '@matrixai/workers';

/**
 * Plain data dictionary
 */
type POJO = { [key: string]: any };

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

/**
 * Iterator options
 * The `keyAsBuffer` property controls
 * whether DBIterator returns KeyPath as buffers or as strings
 * It should be considered to default to true
 * The `valueAsBuffer` property controls value type
 * It should be considered to default to true
 */
type DBIteratorOptions = {
  gt?: KeyPath | Buffer | string;
  gte?: KeyPath | Buffer | string;
  lt?: KeyPath | Buffer | string;
  lte?: KeyPath | Buffer | string;
  limit?: number;
  keys?: boolean;
  values?: boolean;
  keyAsBuffer?: boolean;
  valueAsBuffer?: boolean;
  reverse?: boolean;
};

/**
 * Iterator
 */
type DBIterator<K extends KeyPath | undefined, V> = {
  seek: (k: KeyPath | string | Buffer) => void;
  end: () => Promise<void>;
  next: () => Promise<[K, V] | undefined>;
  [Symbol.asyncIterator]: () => AsyncGenerator<[K, V]>;
};

type DBBatch = AbstractBatch;

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

export type {
  POJO,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  KeyPath,
  LevelPath,
  DBIteratorOptions,
  DBIterator,
  DBBatch,
  DBOp,
  DBOps,
};
