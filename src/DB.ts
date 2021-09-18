import type { AbstractBatch } from 'abstract-leveldown';
import type { LevelDB } from 'level';
import type { MutexInterface } from 'async-mutex';
import type {
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBDomain,
  DBLevel,
  DBOps,
  DBTransaction,
} from './types';

import level from 'level';
import subleveldown from 'subleveldown';
import { Transfer } from 'threads';
import { Mutex } from 'async-mutex';
import Logger from '@matrixai/logger';
import Transaction from './Transaction';
import * as utils from './utils';
import * as errors from './errors';

class DB {
  public static async createDB({
    dbPath,
    crypto,
    lock = new Mutex(),
    fs = require('fs'),
    logger = new Logger(this.name),
  }: {
    dbPath: string;
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    lock?: MutexInterface;
    fs?: FileSystem;
    logger?: Logger;
  }) {
    const db = new DB({
      dbPath,
      crypto,
      lock,
      fs,
      logger,
    });
    await db.start();
    return db;
  }

  public readonly dbPath: string;

  protected crypto?: {
    key: Buffer;
    ops: Crypto;
  };
  protected lock: MutexInterface;
  protected fs: FileSystem;
  protected logger: Logger;
  protected workerManager?: DBWorkerManagerInterface;
  protected _db: LevelDB<string | Buffer, Buffer>;
  protected _running: boolean = false;
  protected _destroyed: boolean = false;

  protected constructor({
    dbPath,
    crypto,
    lock,
    fs,
    logger,
  }: {
    dbPath: string;
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    lock: MutexInterface;
    fs: FileSystem;
    logger: Logger;
  }) {
    this.logger = logger;
    this.dbPath = dbPath;
    this.crypto = crypto;
    this.lock = lock;
    this.fs = fs;
  }

  get db(): LevelDB<string, Buffer> {
    return this._db;
  }

  get locked(): boolean {
    return this.lock.isLocked();
  }

  get running(): boolean {
    return this._running;
  }

  get destroyed(): boolean {
    return this._destroyed;
  }

  public async start(): Promise<void> {
    return this.withLocks(async () => {
      if (this._running) {
        return;
      }
      if (this._destroyed) {
        throw new errors.ErrorDBDestroyed();
      }
      this.logger.info('Starting DB');
      this.logger.info(`Setting DB path to ${this.dbPath}`);
      try {
        await this.fs.promises.mkdir(this.dbPath, { recursive: true });
      } catch (e) {
        if (e.code !== 'EEXIST') {
          throw e;
        }
      }
      const dbLevel = await new Promise<LevelDB<string | Buffer, Buffer>>(
        (resolve, reject) => {
          const db = level(
            this.dbPath,
            {
              keyEncoding: 'binary',
              valueEncoding: 'binary',
            },
            (e) => {
              if (e) {
                reject(e);
              } else {
                resolve(db);
              }
            },
          );
        },
      );
      this._db = dbLevel;
      this._running = true;
      this.logger.info('Started DB');
    });
  }

  public async stop(): Promise<void> {
    return this.withLocks(async () => {
      if (!this._running) {
        return;
      }
      this.logger.info('Stopping DB');
      await this.db.close();
      this._running = false;
      this.logger.info('Stopped DB');
    });
  }

  public async destroy(): Promise<void> {
    return this.withLocks(async () => {
      if (this._destroyed) {
        return;
      }
      if (this._running) {
        throw new errors.ErrorDBRunning();
      }
      this.logger.info('Destroying DB');
      await this.fs.promises.rm(this.dbPath, { recursive: true });
      this._destroyed = true;
      this.logger.info('Destroyed DB');
    });
  }

  public setWorkerManager(workerManager: DBWorkerManagerInterface) {
    this.workerManager = workerManager;
  }

  public unsetWorkerManager() {
    delete this.workerManager;
  }

  public async withLocks<T>(
    f: () => Promise<T>,
    locks: Array<MutexInterface> = [this.lock],
  ): Promise<T> {
    const releases: Array<MutexInterface.Releaser> = [];
    for (const l of locks) {
      releases.push(await l.acquire());
    }
    try {
      return await f();
    } finally {
      // Release them in the opposite order
      releases.reverse();
      for (const r of releases) {
        r();
      }
    }
  }

  /**
   * Attempts to lock in sequence
   * If you don't pass any
   * Then it will just lock globally
   * Otherwise it tries to run the transaction
   * And commits the operations at the very end
   * This allows one to create a lock to be shared between mutliple transactions
   */
  public async transact<T>(
    f: (t: DBTransaction) => Promise<T>,
    locks: Array<MutexInterface> = [this.lock],
  ): Promise<T> {
    return this.withLocks(async () => {
      if (!this._running) {
        throw new errors.ErrorDBNotRunning();
      }
      const tran = new Transaction({ db: this, logger: this.logger });
      let value: T;
      try {
        value = await f(tran);
        await tran.commit();
      } catch (e) {
        await tran.rollback();
        throw e;
      }
      // Only finalize if commit succeeded
      await tran.finalize();
      return value;
    }, locks);
  }

  public async level(
    domain: string,
    dbLevel: DBLevel = this._db,
  ): Promise<DBLevel> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    try {
      return new Promise<DBLevel>((resolve) => {
        const dbLevelNew = subleveldown(dbLevel, domain, {
          keyEncoding: 'binary',
          valueEncoding: 'binary',
          open: (cb) => {
            cb(undefined);
            resolve(dbLevelNew);
          },
        });
      });
    } catch (e) {
      if (e instanceof RangeError) {
        // Some domain prefixes will conflict with the separator
        throw new errors.ErrorDBLevelPrefix();
      }
      throw e;
    }
  }

  public async count(dbLevel: DBLevel = this._db): Promise<number> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    let count = 0;
    for await (const _ of dbLevel.createKeyStream()) {
      count++;
    }
    return count;
  }

  public async get<T>(
    domain: DBDomain,
    key: string | Buffer,
    raw?: false,
  ): Promise<T | undefined>;
  public async get(
    domain: DBDomain,
    key: string | Buffer,
    raw: true,
  ): Promise<Buffer | undefined>;
  public async get<T>(
    domain: DBDomain,
    key: string | Buffer,
    raw: boolean = false,
  ): Promise<T | undefined> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    let data;
    try {
      data = await this._db.get(utils.domainPath(domain, key));
    } catch (e) {
      if (e.notFound) {
        return undefined;
      }
      throw e;
    }
    return this.deserializeDecrypt<T>(data, raw as any);
  }

  public async put(
    domain: DBDomain,
    key: string | Buffer,
    value: any,
    raw?: false,
  ): Promise<void>;
  public async put(
    domain: DBDomain,
    key: string | Buffer,
    value: Buffer,
    raw: true,
  ): Promise<void>;
  public async put(
    domain: DBDomain,
    key: string | Buffer,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    const data = await this.serializeEncrypt(value, raw as any);
    return this._db.put(utils.domainPath(domain, key), data);
  }

  public async del(domain: DBDomain, key: string | Buffer): Promise<void> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    return this._db.del(utils.domainPath(domain, key));
  }

  public async batch(ops: Readonly<DBOps>): Promise<void> {
    if (!this._running) {
      throw new errors.ErrorDBNotRunning();
    }
    const opsP: Array<Promise<AbstractBatch> | AbstractBatch> = [];
    for (const op of ops) {
      if (op.type === 'del') {
        opsP.push({
          type: op.type,
          key: utils.domainPath(op.domain, op.key),
        });
      } else {
        opsP.push(
          this.serializeEncrypt(op.value, (op.raw === true) as any).then(
            (data) => ({
              type: op.type,
              key: utils.domainPath(op.domain, op.key),
              value: data,
            }),
          ),
        );
      }
    }
    const opsB = await Promise.all(opsP);
    return this._db.batch(opsB);
  }

  public async serializeEncrypt(value: any, raw: false): Promise<Buffer>;
  public async serializeEncrypt(value: Buffer, raw: true): Promise<Buffer>;
  public async serializeEncrypt(
    value: any | Buffer,
    raw: boolean,
  ): Promise<Buffer> {
    const plainTextBuf: Buffer = raw
      ? (value as Buffer)
      : utils.serialize(value);
    if (this.crypto == null) {
      return plainTextBuf;
    } else {
      let cipherText: ArrayBuffer;
      if (this.workerManager != null) {
        // Slice-copy for transferring to worker threads
        const key = utils.toArrayBuffer(this.crypto.key);
        const plainText = utils.toArrayBuffer(plainTextBuf);
        cipherText = await this.workerManager.call(async (w) => {
          return await w.encrypt(
            Transfer(key),
            // @ts-ignore: threads.js types are wrong
            Transfer(plainText),
          );
        });
      } else {
        cipherText = await this.crypto.ops.encrypt(
          this.crypto.key,
          plainTextBuf,
        );
      }
      return utils.fromArrayBuffer(cipherText);
    }
  }

  public async deserializeDecrypt<T>(
    cipherTextBuf: Buffer,
    raw: false,
  ): Promise<T>;
  public async deserializeDecrypt(
    cipherTextBuf: Buffer,
    raw: true,
  ): Promise<Buffer>;
  public async deserializeDecrypt<T>(
    cipherTextBuf: Buffer,
    raw: boolean,
  ): Promise<T | Buffer> {
    if (this.crypto == null) {
      return raw ? cipherTextBuf : utils.deserialize<T>(cipherTextBuf);
    } else {
      let decrypted: ArrayBuffer | undefined;
      if (this.workerManager != null) {
        // Slice-copy for transferring to worker threads
        const key = utils.toArrayBuffer(this.crypto.key);
        const cipherText = utils.toArrayBuffer(cipherTextBuf);
        decrypted = await this.workerManager.call(async (w) => {
          return await w.decrypt(
            Transfer(key),
            // @ts-ignore: threads.js types are wrong
            Transfer(cipherText),
          );
        });
      } else {
        decrypted = await this.crypto.ops.decrypt(
          this.crypto.key,
          cipherTextBuf,
        );
      }
      if (decrypted == null) {
        throw new errors.ErrorDBDecrypt();
      }
      const plainTextBuf = utils.fromArrayBuffer(decrypted);
      return raw ? plainTextBuf : utils.deserialize<T>(plainTextBuf);
    }
  }
}

export default DB;
