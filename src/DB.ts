import type {
  AbstractBatch,
  AbstractIteratorOptions,
} from 'abstract-leveldown';
import type { LevelDB } from 'level';
import type { ResourceAcquire } from '@matrixai/resources';
import type {
  KeyPath,
  LevelPath,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBIterator,
  DBOps,
} from './types';
import level from 'level';
import { Transfer } from 'threads';
import Logger from '@matrixai/logger';
import { withF, withG } from '@matrixai/resources';
import {
  CreateDestroyStartStop,
  ready,
} from '@matrixai/async-init/dist/CreateDestroyStartStop';
import DBTransaction from './DBTransaction';
import * as utils from './utils';
import * as errors from './errors';

interface DB extends CreateDestroyStartStop {}
@CreateDestroyStartStop(
  new errors.ErrorDBRunning(),
  new errors.ErrorDBDestroyed(),
)
class DB {
  public static async createDB({
    dbPath,
    crypto,
    fs = require('fs'),
    logger = new Logger(this.name),
    fresh = false,
  }: {
    dbPath: string;
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    fs?: FileSystem;
    logger?: Logger;
    fresh?: boolean;
  }): Promise<DB> {
    logger.info(`Creating ${this.name}`);
    const db = new DB({
      dbPath,
      crypto,
      fs,
      logger,
    });
    await db.start({ fresh });
    logger.info(`Created ${this.name}`);
    return db;
  }

  public readonly dbPath: string;

  protected crypto?: {
    key: Buffer;
    ops: Crypto;
  };
  protected fs: FileSystem;
  protected logger: Logger;
  protected workerManager?: DBWorkerManagerInterface;
  protected _db: LevelDB<string | Buffer, Buffer>;
  protected transactionCounter: number = 0;

  constructor({
    dbPath,
    crypto,
    fs,
    logger,
  }: {
    dbPath: string;
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    fs: FileSystem;
    logger: Logger;
  }) {
    this.logger = logger;
    this.dbPath = dbPath;
    this.crypto = crypto;
    this.fs = fs;
  }

  get db(): Readonly<LevelDB<string | Buffer, Buffer>> {
    return this._db;
  }

  public async start({
    fresh = false,
  }: {
    fresh?: boolean;
  } = {}) {
    this.logger.info(`Starting ${this.constructor.name}`);
    this.logger.info(`Setting DB path to ${this.dbPath}`);
    if (fresh) {
      try {
        await this.fs.promises.rm(this.dbPath, {
          force: true,
          recursive: true,
        });
      } catch (e) {
        throw new errors.ErrorDBDelete(e.message, { cause: e });
      }
    }
    const db = await this.setupDb(this.dbPath);
    this._db = db;
    try {
      // Only run these after this._db is assigned
      await this.setupRootLevels();
      if (this.crypto != null) {
        await this.canaryCheck();
      }
    } catch (e) {
      // LevelDB must be closed otherwise its lock will persist
      await this._db.close();
      throw e;
    }
    this.logger.info(`Started ${this.constructor.name}`);
  }

  public async stop(): Promise<void> {
    this.logger.info(`Stopping ${this.constructor.name}`);
    await this._db.close();
    this.logger.info(`Stopped ${this.constructor.name}`);
  }

  public async destroy(): Promise<void> {
    this.logger.info(`Destroying ${this.constructor.name}`);
    try {
      await this.fs.promises.rm(this.dbPath, {
        force: true,
        recursive: true,
      });
    } catch (e) {
      throw new errors.ErrorDBDelete(e.message, { cause: e });
    }
    this.logger.info(`Destroyed ${this.constructor.name}`);
  }

  public setWorkerManager(workerManager: DBWorkerManagerInterface) {
    this.workerManager = workerManager;
  }

  public unsetWorkerManager() {
    delete this.workerManager;
  }

  @ready(new errors.ErrorDBNotRunning())
  public transaction(): ResourceAcquire<DBTransaction> {
    return async () => {
      const transactionId = this.transactionCounter++;
      const tran = new DBTransaction({
        db: this,
        transactionId,
        logger: this.logger,
      });
      // const tran = await DBTransaction.createTransaction({
      //   db: this,
      //   transactionId,
      //   logger: this.logger,
      // });
      return [
        async (e?: Error) => {
          try {
            if (e == null) {
              try {
                await tran.commit();
              } catch (e) {
                await tran.rollback(e);
                throw e;
              }
              await tran.finalize();
            } else {
              await tran.rollback(e);
            }
          } finally {
            await tran.destroy();
          }
        },
        tran,
      ];
    };
  }

  public async withTransactionF<T>(
    f: (tran: DBTransaction) => Promise<T>,
  ): Promise<T> {
    return withF([this.transaction()], ([tran]) => f(tran));
  }

  public withTransactionG<T, TReturn, TNext>(
    g: (tran: DBTransaction) => AsyncGenerator<T, TReturn, TNext>,
  ): AsyncGenerator<T, TReturn, TNext> {
    return withG([this.transaction()], ([tran]) => g(tran));
  }

  /**
   * Gets a value from the DB
   * Use raw to return the raw decrypted buffer
   */
  public async get<T>(
    keyPath: KeyPath | string | Buffer,
    raw?: false,
  ): Promise<T | undefined>;
  public async get(
    keyPath: KeyPath | string | Buffer,
    raw: true,
  ): Promise<Buffer | undefined>;
  @ready(new errors.ErrorDBNotRunning())
  public async get<T>(
    keyPath: KeyPath | string | Buffer,
    raw: boolean = false,
  ): Promise<T | Buffer | undefined> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    keyPath = ['data', ...keyPath];
    return this._get<T>(keyPath, raw as any);
  }

  /**
   * Get from root level
   * @internal
   */
  public async _get<T>(keyPath: KeyPath, raw?: false): Promise<T | undefined>;
  /**
   * @internal
   */
  public async _get(keyPath: KeyPath, raw: true): Promise<Buffer | undefined>;
  public async _get<T>(
    keyPath: KeyPath,
    raw: boolean = false,
  ): Promise<T | undefined> {
    let data;
    try {
      const key = utils.keyPathToKey(keyPath);
      data = await this._db.get(key);
    } catch (e) {
      if (e.notFound) {
        return undefined;
      }
      throw e;
    }
    return this.deserializeDecrypt<T>(data, raw as any);
  }

  /**
   * Put a key and value into the DB
   * Use raw to put raw encrypted buffer
   */
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: any,
    raw?: false,
  ): Promise<void>;
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: Buffer,
    raw: true,
  ): Promise<void>;
  @ready(new errors.ErrorDBNotRunning())
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    keyPath = ['data', ...keyPath];
    return this._put(keyPath, value, raw as any);
  }

  /**
   * Put from root level
   * @internal
   */
  public async _put(keyPath: KeyPath, value: any, raw?: false): Promise<void>;
  /**
   * @internal
   */
  public async _put(keyPath: KeyPath, value: Buffer, raw: true): Promise<void>;
  public async _put(
    keyPath: KeyPath,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    const data = await this.serializeEncrypt(value, raw as any);
    return this._db.put(utils.keyPathToKey(keyPath), data);
  }

  /**
   * Deletes a key from the DB
   */
  @ready(new errors.ErrorDBNotRunning())
  public async del(keyPath: KeyPath | string | Buffer): Promise<void> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    keyPath = ['data', ...keyPath];
    return this._del(keyPath);
  }

  /**
   * Delete from root level
   * @internal
   */
  public async _del(keyPath: KeyPath): Promise<void> {
    return this._db.del(utils.keyPathToKey(keyPath));
  }

  /**
   * Batches operations together atomically
   */
  @ready(new errors.ErrorDBNotRunning())
  public async batch(ops: Readonly<DBOps>): Promise<void> {
    const opsP: Array<Promise<AbstractBatch> | AbstractBatch> = [];
    for (const op of ops) {
      if (!Array.isArray(op.keyPath)) {
        op.keyPath = [op.keyPath] as KeyPath;
      }
      if (op.keyPath.length < 1) {
        op.keyPath = [''];
      }
      op.keyPath = ['data', ...op.keyPath];
      if (op.type === 'del') {
        opsP.push({
          type: op.type,
          key: utils.keyPathToKey(op.keyPath),
        });
      } else {
        opsP.push(
          this.serializeEncrypt(op.value, (op.raw === true) as any).then(
            (data) => ({
              type: op.type,
              key: utils.keyPathToKey(op.keyPath as KeyPath),
              value: data,
            }),
          ),
        );
      }
    }
    const opsB = await Promise.all(opsP);
    return this._db.batch(opsB);
  }

  /**
   * Batch from root level
   * @internal
   */
  public async _batch(ops: Readonly<DBOps>): Promise<void> {
    const opsP: Array<Promise<AbstractBatch> | AbstractBatch> = [];
    for (const op of ops) {
      if (!Array.isArray(op.keyPath)) {
        op.keyPath = [op.keyPath] as KeyPath;
      }
      if (op.type === 'del') {
        opsP.push({
          type: op.type,
          key: utils.keyPathToKey(op.keyPath as KeyPath),
        });
      } else {
        opsP.push(
          this.serializeEncrypt(op.value, (op.raw === true) as any).then(
            (data) => ({
              type: op.type,
              key: utils.keyPathToKey(op.keyPath as KeyPath),
              value: data,
            }),
          ),
        );
      }
    }
    const opsB = await Promise.all(opsP);
    return this._db.batch(opsB);
  }

  /**
   * Public iterator that works from the data level
   * If keys and values are both false, this iterator will not run at all
   * You must have at least one of them being true or undefined
   */
  public iterator(
    options: AbstractIteratorOptions & { keys: false; values: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, undefined>;
  public iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  public iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<string, undefined>;
  public iterator(
    options: AbstractIteratorOptions & { keys: false; valueAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  public iterator<V>(
    options: AbstractIteratorOptions & { keys: false; valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, V>;
  public iterator(
    options?: AbstractIteratorOptions & {
      keyAsBuffer?: true;
      valueAsBuffer?: true;
    },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, Buffer>;
  public iterator(
    options?: AbstractIteratorOptions & {
      keyAsBuffer: false;
      valueAsBuffer?: true;
    },
    levelPath?: LevelPath,
  ): DBIterator<string, Buffer>;
  public iterator<V>(
    options?: AbstractIteratorOptions & {
      keyAsBuffer?: true;
      valueAsBuffer: false;
    },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, V>;
  public iterator<V>(
    options?: AbstractIteratorOptions & {
      keyAsBuffer: false;
      valueAsBuffer: false;
    },
    levelPath?: LevelPath,
  ): DBIterator<string, V>;
  @ready(new errors.ErrorDBNotRunning())
  public iterator<V>(
    options?: AbstractIteratorOptions & {
      keyAsBuffer?: any;
      valueAsBuffer?: any;
    },
    levelPath: LevelPath = [],
  ): DBIterator<Buffer | string | undefined, Buffer | V | undefined> {
    levelPath = ['data', ...levelPath];
    return this._iterator(options, levelPath);
  }

  /**
   * Iterator from root level
   * @internal
   */
  public _iterator(
    options: AbstractIteratorOptions & { keys: false; values: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, undefined>;
  /**
   * @internal
   */
  public _iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  /**
   * @internal
   */
  public _iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<string, undefined>;
  /**
   * @internal
   */
  public _iterator(
    options: AbstractIteratorOptions & { keys: false; valueAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  /**
   * @internal
   */
  public _iterator<V>(
    options: AbstractIteratorOptions & { keys: false; valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, V>;
  /**
   * @internal
   */
  public _iterator(
    options?: AbstractIteratorOptions & {
      keyAsBuffer?: true;
      valueAsBuffer?: true;
    },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, Buffer>;
  /**
   * @internal
   */
  public _iterator(
    options?: AbstractIteratorOptions & {
      keyAsBuffer: false;
      valueAsBuffer?: true;
    },
    levelPath?: LevelPath,
  ): DBIterator<string, Buffer>;
  /**
   * @internal
   */
  public _iterator<V>(
    options?: AbstractIteratorOptions & {
      keyAsBuffer?: true;
      valueAsBuffer: false;
    },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, V>;
  /**
   * @internal
   */
  public _iterator<V>(
    options?: AbstractIteratorOptions & {
      keyAsBuffer: false;
      valueAsBuffer: false;
    },
    levelPath?: LevelPath,
  ): DBIterator<string, V>;
  public _iterator(
    options?: AbstractIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator<any, any> {
    const levelKeyStart = utils.levelPathToKey(levelPath);
    options = options ?? {};
    if (options.gt != null) {
      options.gt = Buffer.concat([
        levelKeyStart,
        typeof options.gt === 'string' ? Buffer.from(options.gt) : options.gt,
      ]);
    }
    if (options.gte != null) {
      options.gte = Buffer.concat([
        levelKeyStart,
        typeof options.gte === 'string'
          ? Buffer.from(options.gte)
          : options.gte,
      ]);
    }
    if (options.gt == null && options.gte == null) {
      options.gt = levelKeyStart;
    }
    if (options?.lt != null) {
      options.lt = Buffer.concat([
        levelKeyStart,
        typeof options.lt === 'string' ? Buffer.from(options.lt) : options.lt,
      ]);
    }
    if (options?.lte != null) {
      options.lte = Buffer.concat([
        levelKeyStart,
        typeof options.lte === 'string'
          ? Buffer.from(options.lte)
          : options.lte,
      ]);
    }
    if (options.lt == null && options.lte == null) {
      const levelKeyEnd = Buffer.from(levelKeyStart);
      levelKeyEnd[levelKeyEnd.length - 1] += 1;
      options.lt = levelKeyEnd;
    }
    const iterator = this._db.iterator(options);
    const seek = iterator.seek.bind(iterator);
    const next = iterator.next.bind(iterator);
    // @ts-ignore AbstractIterator type is outdated
    iterator.seek = (k: Buffer | string): void => {
      seek(utils.keyPathToKey([...levelPath, k]));
    };
    // @ts-ignore AbstractIterator type is outdated
    iterator.next = async () => {
      const kv = await next();
      // If kv is undefined, we have reached the end of iteration
      if (kv != null) {
        // Handle keys: false
        if (kv[0] != null) {
          // Truncate level path so the returned key is relative to the level path
          const keyPath = utils.parseKey(kv[0]).slice(levelPath.length);
          kv[0] = utils.keyPathToKey(keyPath);
          if (options?.keyAsBuffer === false) {
            kv[0] = kv[0].toString('utf-8');
          }
        }
        // Handle values: false
        if (kv[1] != null) {
          if (options?.valueAsBuffer === false) {
            kv[1] = await this.deserializeDecrypt(kv[1], false);
          } else {
            kv[1] = await this.deserializeDecrypt(kv[1], true);
          }
        }
      }
      return kv;
    };
    return iterator as unknown as DBIterator<any, any>;
  }

  /**
   * Clear all key values for a specific level
   * This is not atomic, it will iterate over a snapshot of the DB
   */
  @ready(new errors.ErrorDBNotRunning())
  public async clear(levelPath: LevelPath = []): Promise<void> {
    levelPath = ['data', ...levelPath];
    await this._clear(levelPath);
  }

  /**
   * Clear from root level
   * @internal
   */
  public async _clear(levelPath: LevelPath = []): Promise<void> {
    for await (const [k] of this._iterator({ values: false }, levelPath)) {
      await this._del([...levelPath, k]);
    }
  }

  @ready(new errors.ErrorDBNotRunning())
  public async count(levelPath: LevelPath = []): Promise<number> {
    let count = 0;
    for await (const _ of this.iterator({ values: false }, levelPath)) {
      count++;
    }
    return count;
  }

  /**
   * Dump from root level path
   * This will show entries from all levels
   * It is intended for diagnostics
   */
  public async dump<V>(
    levelPath?: LevelPath,
    raw?: false,
  ): Promise<Array<[string, V]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
  ): Promise<Array<[Buffer, Buffer]>>;
  @ready(new errors.ErrorDBNotRunning())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
  ): Promise<Array<[string | Buffer, any]>> {
    const records: Array<[string | Buffer, any]> = [];
    for await (const [k, v] of this._iterator(
      {
        keyAsBuffer: raw as any,
        valueAsBuffer: raw as any,
      },
      levelPath,
    )) {
      records.push([k, v]);
    }
    return records;
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

  protected async setupDb(
    dbPath: string,
  ): Promise<LevelDB<string | Buffer, Buffer>> {
    try {
      await this.fs.promises.mkdir(dbPath);
    } catch (e) {
      if (e.code !== 'EEXIST') {
        throw new errors.ErrorDBCreate(e.message, { cause: e });
      }
    }
    let db: LevelDB<string | Buffer, Buffer>;
    try {
      db = await new Promise<LevelDB<string | Buffer, Buffer>>(
        (resolve, reject) => {
          const db = level(
            dbPath,
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
    } catch (e) {
      throw new errors.ErrorDBCreate(e.message, { cause: e });
    }
    return db;
  }

  protected async setupRootLevels(): Promise<void> {
    // Clear any dirty state in transactions
    await this._clear(['transactions']);
  }

  protected async canaryCheck(): Promise<void> {
    try {
      const deadbeef = await this._get(['canary']);
      if (deadbeef == null) {
        // If the stored value didn't exist, its a new db and so store and proceed
        await this._put(['canary'], 'deadbeef');
      } else if (deadbeef !== 'deadbeef') {
        throw new errors.ErrorDBKey('Incorrect key or DB is corrupted');
      }
    } catch (e) {
      if (e instanceof errors.ErrorDBDecrypt) {
        throw new errors.ErrorDBKey('Incorrect key supplied', { cause: e });
      } else {
        throw e;
      }
    }
  }
}

export default DB;
