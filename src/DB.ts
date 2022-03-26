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
        throw new errors.ErrorDBDelete(e.message, undefined, e);
      }
    }
    const db = await this.setupDb(this.dbPath);
    await this.setupRootLevels(db);
    this._db = db;
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
      throw new errors.ErrorDBDelete(e.message, {
        errno: e.errno,
        syscall: e.syscall,
        code: e.code,
        path: e.path,
      });
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
                await tran.rollback();
                throw e;
              }
              await tran.finalize();
            } else {
              await tran.rollback();
            }
          } finally {
            await tran.destroy();
          }
        },
        tran,
      ];
    };
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
  ): Promise<T | undefined> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    keyPath = ['data', ...keyPath];
    if (utils.checkSepKeyPath(keyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    return this._get<T>(keyPath, raw as any);
  }

  /**
   * Get from root level
   * @internal
   */
  public async _get<T>(keyPath: KeyPath, raw?: false): Promise<T | undefined>;
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
    keyPath = ['data', ...keyPath];
    if (utils.checkSepKeyPath(keyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    return this._put(keyPath, value, raw as any);
  }

  /**
   * Put from root level
   * @internal
   */
  public async _put(keyPath: KeyPath, value: any, raw?: false): Promise<void>;
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
    keyPath = ['data', ...keyPath];
    if (utils.checkSepKeyPath(keyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
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
      op.keyPath = ['data', ...op.keyPath];
      if (utils.checkSepKeyPath(op.keyPath)) {
        throw new errors.ErrorDBLevelSep();
      }
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
    options: AbstractIteratorOptions & { keys: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  public iterator(
    options: AbstractIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  public iterator(
    options?: AbstractIteratorOptions,
    levelPath?: LevelPath,
  ): DBIterator<Buffer, Buffer>;
  @ready(new errors.ErrorDBNotRunning())
  public iterator(
    options?: AbstractIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator {
    levelPath = ['data', ...levelPath];
    if (utils.checkSepLevelPath(levelPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    return this._iterator(this._db, options, levelPath);
  }

  /**
   * Iterator from root level
   * @internal
   */
  public _iterator(
    db: LevelDB,
    options: AbstractIteratorOptions & { keys: false; values: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, undefined>;
  public _iterator(
    db: LevelDB,
    options: AbstractIteratorOptions & { keys: false },
    levelPath?: LevelPath,
  ): DBIterator<undefined, Buffer>;
  public _iterator(
    db: LevelDB,
    options: AbstractIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  public _iterator(
    db: LevelDB,
    options?: AbstractIteratorOptions,
    levelPath?: LevelPath,
  ): DBIterator<Buffer, Buffer>;
  public _iterator(
    db: LevelDB,
    options?: AbstractIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator {
    options = options ?? {};
    const levelKeyStart = utils.levelPathToKey(levelPath);
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
    const iterator = db.iterator(options);
    const seek = iterator.seek.bind(iterator);
    const next = iterator.next.bind(iterator);
    // @ts-ignore AbstractIterator type is outdated
    iterator.seek = (k: Buffer | string): void => {
      seek(utils.keyPathToKey([...levelPath, k] as unknown as KeyPath));
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
          kv[0] = utils.keyPathToKey(keyPath as unknown as KeyPath);
        }
        // Handle values: false
        if (kv[1] != null) {
          kv[1] = await this.deserializeDecrypt(kv[1], true);
        }
      }
      return kv;
    };
    return iterator as unknown as DBIterator;
  }

  /**
   * Clear all key values for a specific level
   * This is not atomic, it will iterate over a snapshot of the DB
   */
  @ready(new errors.ErrorDBNotRunning())
  public async clear(levelPath: LevelPath = []): Promise<void> {
    levelPath = ['data', ...levelPath];
    if (utils.checkSepLevelPath(levelPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    await this._clear(this._db, levelPath);
  }

  /**
   * Clear from root level
   * @internal
   */
  public async _clear(db: LevelDB, levelPath: LevelPath = []): Promise<void> {
    for await (const [k] of this._iterator(db, { values: false }, levelPath)) {
      await db.del(utils.keyPathToKey([...levelPath, k] as unknown as KeyPath));
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
   * Dump from root level
   * It is intended for diagnostics
   */
  public async dump(
    levelPath?: LevelPath,
    raw?: false,
  ): Promise<Array<[string, any]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
  ): Promise<Array<[Buffer, Buffer]>>;
  @ready(new errors.ErrorDBNotRunning())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
  ): Promise<Array<[string | Buffer, any]>> {
    if (utils.checkSepLevelPath(levelPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    const records: Array<[string | Buffer, any]> = [];
    for await (const [k, v] of this._iterator(this._db, undefined, levelPath)) {
      let key: string | Buffer, value: any;
      if (raw) {
        key = k;
        value = v;
      } else {
        key = k.toString('utf-8');
        value = utils.deserialize(v);
      }
      records.push([key, value]);
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
        throw new errors.ErrorDBCreate(e.message, undefined, e);
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
      throw new errors.ErrorDBCreate(e.message, undefined, e);
    }
    return db;
  }

  protected async setupRootLevels(db: LevelDB): Promise<void> {
    // Clear any dirty state in transactions
    await this._clear(db, ['transactions']);
  }
}

export default DB;
