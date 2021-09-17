import type fs from 'fs';
import type { AbstractLevelDOWN, AbstractIterator } from 'abstract-leveldown';
import type { LevelUp } from 'levelup';
import type { WorkerManagerInterface } from '@matrixai/workers';

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

interface DBTransaction {
  ops: Readonly<DBOps>;
  snap: ReadonlyMap<string, any>;
  callbacksSuccess: Readonly<Array<() => any>>;
  callbacksFailure: Readonly<Array<() => any>>;
  committed: boolean;

  get<T>(
    domain: DBDomain,
    key: string | Buffer,
    raw?: false,
  ): Promise<T | undefined>;
  get(
    domain: DBDomain,
    key: string | Buffer,
    raw: true,
  ): Promise<Buffer | undefined>;

  put(
    domain: DBDomain,
    key: string | Buffer,
    value: any,
    raw?: false,
  ): Promise<void>;
  put(
    domain: DBDomain,
    key: string | Buffer,
    value: Buffer,
    raw: true,
  ): Promise<void>;

  del(domain: DBDomain, key: string | Buffer): Promise<void>;

  queueSuccess(f: () => any): void;

  queueFailure(f: () => any): void;
}

export type {
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBDomain,
  DBLevel,
  DBOp,
  DBOps,
  DBTransaction,
};
