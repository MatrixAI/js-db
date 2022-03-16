import type fs from 'fs';
import type { AbstractLevelDOWN, AbstractIterator } from 'abstract-leveldown';
import type { LevelUp } from 'levelup';
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

type DBDomain = Readonly<Array<string>>;

type DBLevel = LevelUp<
  AbstractLevelDOWN<string | Buffer, Buffer>,
  AbstractIterator<Buffer, Buffer>
>;

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
      domain: DBDomain;
      key: string | Buffer;
      value: any;
      raw?: false;
    }
  | {
      domain: DBDomain;
      key: string | Buffer;
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

type ResourceAcquire<Resource = void> = () => Promise<
  readonly [ResourceRelease, Resource?]
>;

type ResourceRelease = (e?: Error) => Promise<void>;

type Resources<T extends readonly ResourceAcquire<any>[]> = {
  [K in keyof T]: T[K] extends ResourceAcquire<infer R> ? R : never;
};

export type {
  POJO,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBDomain,
  DBLevel,
  DBIterator,
  DBOp,
  DBOps,
  ResourceAcquire,
  ResourceRelease,
  Resources,
};
