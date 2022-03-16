import type {
  AbstractBatch,
  AbstractIteratorOptions,
} from 'abstract-leveldown';
import type { LevelDB } from 'level';
import type {
  POJO,
  FileSystem,
  Crypto,
  DBWorkerManagerInterface,
  DBDomain,
  DBLevel,
  DBIterator,
  DBOps,
  ResourceAcquire,
} from './types';
import level from 'level';
import subleveldown from 'subleveldown';
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
  protected _dataDb: DBLevel;
  protected _transactionsDb: DBLevel;
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

  get db(): LevelDB<string | Buffer, Buffer> {
    return this._db;
  }

  get dataDb(): DBLevel {
    return this._dataDb;
  }

  get transactionsDb(): DBLevel {
    return this._transactionsDb;
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
    const { dataDb, transactionsDb } = await this.setupRootLevels(db);
    this._db = db;
    this._dataDb = dataDb;
    this._transactionsDb = transactionsDb;
    this.logger.info(`Started ${this.constructor.name}`);
  }

  public async stop(): Promise<void> {
    this.logger.info(`Stopping ${this.constructor.name}`);
    await this.db.close();
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
      const transactionDb = await this._level(
        transactionId.toString(),
        this.transactionsDb,
      );
      const tran = await DBTransaction.createTransaction({
        db: this,
        transactionId,
        transactionDb,
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

  @ready(new errors.ErrorDBNotRunning())
  public async level(
    domain: string,
    dbLevel: DBLevel = this._dataDb,
  ): ReturnType<DB['_level']> {
    return this._level(domain, dbLevel);
  }

  public iterator(
    options: AbstractIteratorOptions & { key: false; value: false },
    dbLevel: DBLevel,
  ): DBIterator<undefined, undefined>;
  public iterator(
    options: AbstractIteratorOptions & { key: false },
    dbLevel: DBLevel,
  ): DBIterator<undefined, Buffer>;
  public iterator(
    options: AbstractIteratorOptions & { value: false },
    dbLevel: DBLevel,
  ): DBIterator<Buffer, undefined>;
  public iterator(
    options?: AbstractIteratorOptions,
    dbLevel?: DBLevel,
  ): DBIterator<Buffer, Buffer>;
  @ready(new errors.ErrorDBNotRunning())
  public iterator(
    options?: AbstractIteratorOptions,
    dbLevel: DBLevel = this._dataDb,
  ): DBIterator {
    const iterator = dbLevel.iterator(options);
    const next = iterator.next.bind(iterator);
    // @ts-ignore AbstractIterator type is outdated
    iterator.next = async (cb) => {
      const kv = await next(cb);
      if (kv != null) {
        kv[1] = await this.deserializeDecrypt(kv[1], true);
      }
      return kv;
    };
    return iterator as unknown as DBIterator;
  }

  @ready(new errors.ErrorDBNotRunning())
  public async clear(dbLevel: DBLevel = this._dataDb): Promise<void> {
    await dbLevel.clear();
  }

  @ready(new errors.ErrorDBNotRunning())
  public async count(dbLevel: DBLevel = this._dataDb): Promise<number> {
    let count = 0;
    for await (const _ of dbLevel.createKeyStream()) {
      count++;
    }
    return count;
  }

  @ready(new errors.ErrorDBNotRunning())
  public async dump(dbLevel: DBLevel = this._dataDb): Promise<POJO> {
    const records = {};
    for await (const o of dbLevel.createReadStream()) {
      const key = (o as any).key.toString();
      const data = (o as any).value as Buffer;
      const value = await this.deserializeDecrypt(data, false);
      records[key] = value;
    }
    return records;
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
  @ready(new errors.ErrorDBNotRunning())
  public async get<T>(
    domain: DBDomain,
    key: string | Buffer,
    raw: boolean = false,
  ): Promise<T | undefined> {
    let data;
    try {
      data = await this._dataDb.get(utils.domainPath(domain, key));
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
  @ready(new errors.ErrorDBNotRunning())
  public async put(
    domain: DBDomain,
    key: string | Buffer,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    const data = await this.serializeEncrypt(value, raw as any);
    return this._dataDb.put(utils.domainPath(domain, key), data);
  }

  @ready(new errors.ErrorDBNotRunning())
  public async del(domain: DBDomain, key: string | Buffer): Promise<void> {
    return this._dataDb.del(utils.domainPath(domain, key));
  }

  @ready(new errors.ErrorDBNotRunning())
  public async batch(ops: Readonly<DBOps>): Promise<void> {
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
    return this._dataDb.batch(opsB);
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

  protected async setupRootLevels(
    db: LevelDB<string | Buffer, Buffer>,
  ): Promise<{
    dataDb: DBLevel;
    transactionsDb: DBLevel;
  }> {
    const dataDb = await this._level('data', db);
    const transactionsDb = await this._level('transactions', db);
    return {
      dataDb,
      transactionsDb,
    };
  }

  protected async _level(domain: string, dbLevel: DBLevel): Promise<DBLevel> {
    try {
      return await new Promise<DBLevel>((resolve, reject) => {
        const dbLevelNew = subleveldown(dbLevel, domain, {
          keyEncoding: 'binary',
          valueEncoding: 'binary',
          open: (cb) => {
            // This `cb` is defaulted (hardcoded) to a function that emits an error event
            // When using `level`, we are able to provide a callback that overrides this `cb`
            // However `subleveldown` does not provide a callback parameter
            // It provides this `open` option, which requires us to call `cb` to finish
            // If we provide an exception as a parameter, it will be received by the `error` event handler
            cb(undefined);
            resolve(dbLevelNew);
          },
        });
        // @ts-ignore error event for subleveldown
        dbLevelNew.on('error', (e) => {
          // Errors during construction of the sublevel will be emitted as events
          reject(e);
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
}

export default DB;
