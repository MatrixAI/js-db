import type { LevelDB } from 'level';
import type { ResourceAcquire } from '@matrixai/resources';
import type {
  KeyPath,
  LevelPath,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBIteratorOptions,
  DBIterator,
  DBBatch,
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
      const tran = await DBTransaction.createTransaction({
        db: this,
        transactionId,
        logger: this.logger,
      });
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
    keyPath = utils.toKeyPath(keyPath);
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
    keyPath = utils.toKeyPath(keyPath);
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
    keyPath = utils.toKeyPath(keyPath);
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
    const opsP: Array<Promise<DBBatch> | DBBatch> = [];
    for (const op of ops) {
      op.keyPath = utils.toKeyPath(op.keyPath);
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
    const opsP: Array<Promise<DBBatch> | DBBatch> = [];
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
    options: DBIteratorOptions & { keys: false; values: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, undefined>;
  public iterator<V>(
    options: DBIteratorOptions & { keys: false; valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, V>;
  public iterator(
    options: DBIteratorOptions & { keys: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  public iterator(
    options: DBIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, undefined>;
  public iterator<V>(
    options: DBIteratorOptions & { valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, V>;
  public iterator(
    options?: DBIteratorOptions,
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, Buffer>;
  @ready(new errors.ErrorDBNotRunning())
  public iterator(
    options?: DBIteratorOptions & { keyAsBuffer?: any; valueAsBuffer?: any },
    levelPath: LevelPath = [],
  ): DBIterator<any, any> {
    levelPath = ['data', ...levelPath];
    return this._iterator(options, levelPath);
  }

  /**
   * Iterator from root level
   * @internal
   */
  public _iterator(
    options: DBIteratorOptions & { keys: false; values: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, undefined>;
  /**
   * @internal
   */
  public _iterator<V>(
    options: DBIteratorOptions & { keys: false; valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, V>;
  /**
   * @internal
   */
  public _iterator(
    options: DBIteratorOptions & { keys: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  /**
   * @internal
   */
  public _iterator(
    options: DBIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, undefined>;
  /**
   * @internal
   */
  public _iterator<V>(
    options?: DBIteratorOptions & { valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, V>;
  /**
   * @internal
   */
  public _iterator(
    options?: DBIteratorOptions,
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, Buffer>;
  public _iterator<V>(
    options?: DBIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator<KeyPath | undefined, Buffer | V | undefined> {
    const options_ = {
      ...(options ?? {}),
      // Internally we always use the buffer
      keyAsBuffer: true,
      valueAsBuffer: true,
    };
    if (options_.gt != null) {
      options_.gt = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options_.gt)),
      );
    }
    if (options_.gte != null) {
      options_.gte = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options_.gte)),
      );
    }
    if (options_.gt == null && options_.gte == null) {
      options_.gte = utils.levelPathToKey(levelPath);
    }
    if (options_.lt != null) {
      options_.lt = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options_.lt)),
      );
    }
    if (options_.lte != null) {
      options_.lte = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options_.lte)),
      );
    }
    if (options_.lt == null && options_.lte == null) {
      const levelKeyStart = utils.levelPathToKey(levelPath);
      const levelKeyEnd = Buffer.from(levelKeyStart);
      levelKeyEnd[levelKeyEnd.length - 1] += 1;
      options_.lt = levelKeyEnd;
    }
    const iterator_ = this._db.iterator(options_);
    const iterator = {
      seek: (keyPath: KeyPath | Buffer | string): void => {
        iterator_.seek(
          utils.keyPathToKey(levelPath.concat(utils.toKeyPath(keyPath))),
        );
      },
      end: async () => {
        // @ts-ignore AbstractIterator type is outdated
        // eslint-disable-next-line @typescript-eslint/await-thenable
        await iterator_.end();
      },
      next: async () => {
        // @ts-ignore AbstractIterator type is outdated
        // eslint-disable-next-line @typescript-eslint/await-thenable
        const kv = (await iterator_.next()) as any;
        // If kv is undefined, we have reached the end of iteration
        if (kv == null) return kv;
        // Handle keys: false
        if (kv[0] != null) {
          // Truncate level path so the returned key is relative to the level path
          const keyPath = utils.parseKey(kv[0]).slice(levelPath.length);
          if (options?.keyAsBuffer === false) {
            kv[0] = keyPath.map((k) => k.toString('utf-8'));
          } else {
            kv[0] = keyPath;
          }
        }
        // Handle values: false
        if (kv[1] != null) {
          if (options?.valueAsBuffer === false) {
            kv[1] = await this.deserializeDecrypt<V>(kv[1], false);
          } else {
            kv[1] = await this.deserializeDecrypt(kv[1], true);
          }
        }
        return kv;
      },
      [Symbol.asyncIterator]: async function* () {
        try {
          let kv: [KeyPath | undefined, any] | undefined;
          while ((kv = await iterator.next()) !== undefined) {
            yield kv;
          }
        } finally {
          if (!iterator_._ended) await iterator.end();
        }
      },
    };
    return iterator;
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
    for await (const [keyPath] of this._iterator(
      { values: false },
      levelPath,
    )) {
      await this._del(levelPath.concat(keyPath));
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
   * Dump from DB
   * This will show entries from all levels
   * It is intended for diagnostics
   * Use `console.dir` instead of `console.log` to debug the result
   * Set `root` to `true` if you want to dump from root levels
   */
  public async dump<V>(
    levelPath?: LevelPath,
    raw?: false,
    root?: boolean,
  ): Promise<Array<[string, V]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
    root?: boolean,
  ): Promise<Array<[Buffer, Buffer]>>;
  @ready(new errors.ErrorDBNotRunning())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
    root: boolean = false,
  ): Promise<Array<[string | Buffer, any]>> {
    if (!root) {
      levelPath = ['data', ...levelPath];
    }
    const records: Array<[string | Buffer, any]> = [];
    for await (const [keyPath, v] of this._iterator(
      {
        keyAsBuffer: true,
        valueAsBuffer: raw as any,
      },
      levelPath,
    )) {
      let k: Buffer | string = utils.keyPathToKey(keyPath);
      if (!raw) {
        k = k.toString('utf-8');
      }
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
