import type {
  RocksDBDatabase,
  RocksDBDatabaseOptions,
} from './rocksdb';
import type { ResourceAcquire } from '@matrixai/resources';
import type {
  KeyPath,
  LevelPath,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBOptions,
  DBIteratorOptions,
  DBBatch,
  DBOps,
} from './types';
import { Transfer } from 'threads';
import Logger from '@matrixai/logger';
import { withF, withG } from '@matrixai/resources';
import {
  CreateDestroyStartStop,
  ready,
} from '@matrixai/async-init/dist/CreateDestroyStartStop';
import DBIterator from './DBIterator';
// Import DBTransaction from './DBTransaction';
import { rocksdbP } from './rocksdb';
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
      crypto,
      fs,
      logger,
    });
    await db.start({ fresh, ...dbOptions });
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
  protected _db: RocksDBDatabase;
  protected transactionCounter: number = 0;

  /**
   * References to iterators
   * This set must be empty when stopping the DB
   */
  protected _iteratorRefs: Set<DBIterator<any, any>> = new Set();

  /**
   * References to transactions
   * This set must be empty when stopping the DB
   */
  // TODO: fix this to DBTransaction
  protected _transactionRefs: Set<any> = new Set();

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

  get db(): Readonly<RocksDBDatabase> {
    return this._db;
  }

  /**
   * @internal
   */
  get iteratorRefs(): Readonly<Set<DBIterator<any, any>>> {
    return this._iteratorRefs;
  }

  // /**
  //  * @internal
  //  */
  // get transactionRefs(): Readonly<Set<DBTransaction>> {
  //   return this._transactionRefs;
  // }

  public async start({
    fresh = false,
    ...dbOptions
  }: {
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
      await rocksdbP.db_close(db);
      throw e;
    }
    this.logger.info(`Started ${this.constructor.name}`);
  }

  public async stop(): Promise<void> {
    this.logger.info(`Stopping ${this.constructor.name}`);
    if (this._iteratorRefs.size > 0 || this._transactionRefs.size > 0) {
      throw new errors.ErrorDBLiveReference();
    }
    await rocksdbP.db_close(this._db);
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

  // @ready(new errors.ErrorDBNotRunning())
  // public transaction(): ResourceAcquire<DBTransaction> {
  //   return async () => {
  //     const transactionId = this.transactionCounter++;
  //     const tran = await DBTransaction.createTransaction({
  //       db: this,
  //       transactionId,
  //       logger: this.logger,
  //     });
  //     return [
  //       async (e?: Error) => {
  //         try {
  //           if (e == null) {
  //             try {
  //               await tran.commit();
  //             } catch (e) {
  //               await tran.rollback(e);
  //               throw e;
  //             }
  //             await tran.finalize();
  //           } else {
  //             await tran.rollback(e);
  //           }
  //         } finally {
  //           await tran.destroy();
  //         }
  //       },
  //       tran,
  //     ];
  //   };
  // }

  // public async withTransactionF<T>(
  //   f: (tran: DBTransaction) => Promise<T>,
  // ): Promise<T> {
  //   return withF([this.transaction()], ([tran]) => f(tran));
  // }

  // public withTransactionG<T, TReturn, TNext>(
  //   g: (tran: DBTransaction) => AsyncGenerator<T, TReturn, TNext>,
  // ): AsyncGenerator<T, TReturn, TNext> {
  //   return withG([this.transaction()], ([tran]) => g(tran));
  // }

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
      data = await rocksdbP.db_get(this._db, key, { valueEncoding: 'buffer' });
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
    await rocksdbP.db_put(this._db, key, data, { sync });
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
    await rocksdbP.db_del(this._db, key, { sync });
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
    await rocksdbP.batch_do(this._db, opsB, { sync });
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
    await rocksdbP.batch_do(this._db, opsB, { sync });
    return;
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
    options: DBIteratorOptions & {
      keyAsBuffer?: any;
      valueAsBuffer?: any;
    } = {},
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
    options: DBIteratorOptions = {},
    levelPath: LevelPath = [],
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
    for await (const [keyPath, v] of this._iterator(
      {
        keyAsBuffer: raw,
        valueAsBuffer: raw,
      },
      levelPath,
    )) {
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
    const db = rocksdbP.db_init();
    // Mutates options object which is copied from this.start
    utils.filterUndefined(options);
    try {
      await rocksdbP.db_open(db, dbPath, options);
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
