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
 * Custom type for our iterator
 * This takes over from the outdated AbstractIterator used in abstract-leveldown
 */
type DBIterator<K = Buffer | undefined, V = Buffer | undefined> = {
  seek: (k: Buffer | string) => void;
  next: () => Promise<[K, V] | undefined>;
  end: () => Promise<void>;
  [Symbol.asyncIterator]: () => AsyncGenerator<[K, V]>;
};

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
  DBIterator,
  DBOp,
  DBOps,
};
