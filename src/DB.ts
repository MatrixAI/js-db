import type { ResourceAcquire } from '@matrixai/resources';
import type { RWLockWriter } from '@matrixai/async-locks';
import type {
  KeyPath,
  LevelPath,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBBatch,
  DBOps,
  DBOptions,
  DBIteratorOptions,
  DBClearOptions,
  DBCountOptions,
} from './types';
import type { RocksDBDatabase, RocksDBDatabaseOptions } from './native';
import { Transfer } from 'threads';
import Logger from '@matrixai/logger';
import { withF, withG } from '@matrixai/resources';
import {
  CreateDestroyStartStop,
  ready,
} from '@matrixai/async-init/dist/CreateDestroyStartStop';
import { LockBox } from '@matrixai/async-locks';
import DBIterator from './DBIterator';
import DBTransaction from './DBTransaction';
import { rocksdbP } from './native';
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
    ...dbOptions
  }: {
    dbPath: string;
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    fs?: FileSystem;
    logger?: Logger;
    fresh?: boolean;
  } & DBOptions): Promise<DB> {
    logger.info(`Creating ${this.name}`);
    const db = new this({
      dbPath,
      fs,
      logger,
    });
    await db.start({
      crypto,
      fresh,
      ...dbOptions,
    });
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
  protected _lockBox: LockBox<RWLockWriter> = new LockBox();
  protected _db: RocksDBDatabase;
  /**
   * References to iterators
   */
  protected _iteratorRefs: Set<DBIterator<any, any>> = new Set();
  /**
   * References to transactions
   */
  protected _transactionRefs: Set<DBTransaction> = new Set();

  get db(): Readonly<RocksDBDatabase> {
    return this._db;
  }

  /**
   * @internal
   */
  get iteratorRefs(): Readonly<Set<DBIterator<any, any>>> {
    return this._iteratorRefs;
  }

  /**
   * @internal
   */
  get transactionRefs(): Readonly<Set<DBTransaction>> {
    return this._transactionRefs;
  }

  get lockBox(): Readonly<LockBox<RWLockWriter>> {
    return this._lockBox;
  }

  constructor({
    dbPath,
    fs,
    logger,
  }: {
    dbPath: string;
    fs: FileSystem;
    logger: Logger;
  }) {
    this.logger = logger;
    this.dbPath = dbPath;
    this.fs = fs;
  }

  public async start({
    crypto,
    fresh = false,
    ...dbOptions
  }: {
    crypto?: {
      key: Buffer;
      ops: Crypto;
    };
    fresh?: boolean;
  } & DBOptions = {}) {
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
    this.crypto = crypto;
    const db = await this.setupDb(this.dbPath, {
      ...dbOptions,
      createIfMissing: true,
      errorIfExists: false,
    });
    this._db = db;
    try {
      // Only run these after this._db is assigned
      await this.setupRootLevels();
      if (this.crypto != null) {
        await this.canaryCheck();
      }
    } catch (e) {
      // RocksDB must be closed otherwise its lock will persist
      await rocksdbP.dbClose(db);
      throw e;
    }
    this.logger.info(`Started ${this.constructor.name}`);
  }

  public async stop(): Promise<void> {
    this.logger.info(`Stopping ${this.constructor.name}`);
    for (const iterator of this._iteratorRefs) {
      await iterator.destroy();
    }
    for (const transaction of this._transactionRefs) {
      if (!transaction.committing && !transaction.rollbacking) {
        // If any transactions is still pending at this point
        // then if they try to commit, that will be an error because
        // the transaction is already rollbacked
        await transaction.rollback();
      } else {
        // This will wait for committing or rollbacking to complete
        await transaction.destroy();
      }
    }
    await rocksdbP.dbClose(this._db);
    delete this.crypto;
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
      const tran = new DBTransaction({
        db: this,
        lockBox: this._lockBox,
        logger: this.logger,
      });
      return [
        async (e?: Error) => {
          try {
            if (e == null) {
              await tran.commit();
            } else {
              await tran.rollback(e);
            }
          } finally {
            // If already destroyed, this is a noop
            // this will only have affect if there was an
            // exception during commit or rollback
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
      data = await rocksdbP.dbGet(this._db, key, { valueEncoding: 'buffer' });
    } catch (e) {
      if (e.code === 'NOT_FOUND') {
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
    sync?: boolean,
  ): Promise<void>;
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: Buffer,
    raw: true,
    sync?: boolean,
  ): Promise<void>;
  @ready(new errors.ErrorDBNotRunning())
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: any,
    raw: boolean = false,
    sync: boolean = false,
  ): Promise<void> {
    keyPath = utils.toKeyPath(keyPath);
    keyPath = ['data', ...keyPath];
    return this._put(keyPath, value, raw as any, sync);
  }

  /**
   * Put from root level
   * @internal
   */
  public async _put(
    keyPath: KeyPath,
    value: any,
    raw?: false,
    sync?: boolean,
  ): Promise<void>;
  /**
   * @internal
   */
  public async _put(
    keyPath: KeyPath,
    value: Buffer,
    raw: true,
    sync?: boolean,
  ): Promise<void>;
  public async _put(
    keyPath: KeyPath,
    value: any,
    raw: boolean = false,
    sync: boolean = false,
  ): Promise<void> {
    const data = await this.serializeEncrypt(value, raw as any);
    const key = utils.keyPathToKey(keyPath);
    await rocksdbP.dbPut(this._db, key, data, { sync });
    return;
  }

  /**
   * Deletes a key from the DB
   */
  @ready(new errors.ErrorDBNotRunning())
  public async del(
    keyPath: KeyPath | string | Buffer,
    sync: boolean = false,
  ): Promise<void> {
    keyPath = utils.toKeyPath(keyPath);
    keyPath = ['data', ...keyPath];
    return this._del(keyPath, sync);
  }

  /**
   * Delete from root level
   * @internal
   */
  public async _del(keyPath: KeyPath, sync: boolean = false): Promise<void> {
    const key = utils.keyPathToKey(keyPath);
    await rocksdbP.dbDel(this._db, key, { sync });
    return;
  }

  /**
   * Batches operations together atomically
   */
  @ready(new errors.ErrorDBNotRunning())
  public async batch(
    ops: Readonly<DBOps>,
    sync: boolean = false,
  ): Promise<void> {
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
    await rocksdbP.batchDo(this._db, opsB, { sync });
    return;
  }

  /**
   * Batch from root level
   * @internal
   */
  public async _batch(
    ops: Readonly<DBOps>,
    sync: boolean = false,
  ): Promise<void> {
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
    await rocksdbP.batchDo(this._db, opsB, { sync });
    return;
  }

  /**
   * Public iterator that works from the data level
   * If keys and values are both false, this iterator will not run at all
   * You must have at least one of them being true or undefined
   */
  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false; values: false },
  ): DBIterator<undefined, undefined>;
  public iterator<V>(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false; valueAsBuffer: false },
  ): DBIterator<undefined, V>;
  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false },
  ): DBIterator<undefined, Buffer>;
  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { values: false },
  ): DBIterator<KeyPath, undefined>;
  public iterator<V>(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { valueAsBuffer: false },
  ): DBIterator<KeyPath, V>;
  public iterator(
    levelPath?: LevelPath,
    options?: DBIteratorOptions,
  ): DBIterator<KeyPath, Buffer>;
  @ready(new errors.ErrorDBNotRunning())
  public iterator(
    levelPath: LevelPath = [],
    options: DBIteratorOptions & {
      keyAsBuffer?: any;
      valueAsBuffer?: any;
    } = {},
  ): DBIterator<any, any> {
    levelPath = ['data', ...levelPath];
    return this._iterator(levelPath, options);
  }

  /**
   * Iterator from root level
   * @internal
   */
  public _iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false; values: false },
  ): DBIterator<undefined, undefined>;
  /**
   * @internal
   */
  public _iterator<V>(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false; valueAsBuffer: false },
  ): DBIterator<undefined, V>;
  /**
   * @internal
   */
  public _iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { keys: false },
  ): DBIterator<undefined, Buffer>;
  /**
   * @internal
   */
  public _iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions & { values: false },
  ): DBIterator<KeyPath, undefined>;
  /**
   * @internal
   */
  public _iterator<V>(
    levelPath: LevelPath | undefined,
    options?: DBIteratorOptions & { valueAsBuffer: false },
  ): DBIterator<KeyPath, V>;
  /**
   * @internal
   */
  public _iterator(
    levelPath?: LevelPath | undefined,
    options?: DBIteratorOptions,
  ): DBIterator<KeyPath, Buffer>;
  public _iterator<V>(
    levelPath: LevelPath = [],
    options: DBIteratorOptions = {},
  ): DBIterator<KeyPath | undefined, Buffer | V | undefined> {
    return new DBIterator({
      db: this,
      levelPath,
      logger: this.logger.getChild(DBIterator.name),
      ...options,
    });
  }

  /**
   * Clear all key values for a specific level
   * This is not atomic, it will iterate over a snapshot of the DB
   */
  @ready(new errors.ErrorDBNotRunning())
  public async clear(
    levelPath: LevelPath = [],
    options: DBClearOptions = {},
  ): Promise<void> {
    levelPath = ['data', ...levelPath];
    await this._clear(levelPath, options);
  }

  /**
   * Clear from root level
   * @internal
   */
  public async _clear(
    levelPath: LevelPath = [],
    options: DBClearOptions = {},
  ): Promise<void> {
    const options_ = utils.iterationOptions(options, levelPath);
    return rocksdbP.dbClear(this._db, options_);
  }

  @ready(new errors.ErrorDBNotRunning())
  public async count(
    levelPath: LevelPath = [],
    options: DBCountOptions = {},
  ): Promise<number> {
    levelPath = ['data', ...levelPath];
    return this._count(levelPath, options);
  }

  /**
   * Count from root level
   * @internal
   */
  public async _count(
    levelPath: LevelPath = [],
    options: DBCountOptions = {},
  ): Promise<number> {
    const options_ = utils.iterationOptions(options, levelPath);
    return rocksdbP.dbCount(this._db, options_);
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
  ): Promise<Array<[KeyPath, V]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
    root?: boolean,
  ): Promise<Array<[KeyPath, Buffer]>>;
  @ready(new errors.ErrorDBNotRunning())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
    root: boolean = false,
  ): Promise<Array<[KeyPath, any]>> {
    if (!root) {
      levelPath = ['data', ...levelPath];
    }
    const records: Array<[KeyPath, any]> = [];
    for await (const [keyPath, v] of this._iterator(levelPath, {
      keyAsBuffer: raw,
      valueAsBuffer: raw,
    })) {
      records.push([keyPath, v]);
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
    options: RocksDBDatabaseOptions = {},
  ): Promise<RocksDBDatabase> {
    try {
      await this.fs.promises.mkdir(dbPath);
    } catch (e) {
      if (e.code !== 'EEXIST') {
        throw new errors.ErrorDBCreate(e.message, { cause: e });
      }
    }
    const db = rocksdbP.dbInit();
    // Mutates options object which is copied from this.start
    utils.filterUndefined(options);
    try {
      await rocksdbP.dbOpen(db, dbPath, options);
    } catch (e) {
      throw new errors.ErrorDBCreate(e.message, { cause: e });
    }
    return db;
  }

  protected async setupRootLevels(): Promise<void> {
    // Nothing to do yet
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
