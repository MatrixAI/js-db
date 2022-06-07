import type DB from './DB';
import type {
  KeyPath,
  LevelPath,
  DBIteratorOptions,
  DBClearOptions,
  DBCountOptions,
} from './types';
import type {
  RocksDBTransaction,
  RocksDBTransactionOptions,
  RocksDBTransactionSnapshot,
} from './rocksdb/types';
import Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/dist/CreateDestroy';
import DBIterator from './DBIterator';
import { rocksdbP } from './rocksdb';
import * as utils from './utils';
import * as errors from './errors';

interface DBTransaction extends CreateDestroy {}
@CreateDestroy()
class DBTransaction {
  protected _db: DB;
  protected logger: Logger;

  protected _options: RocksDBTransactionOptions;
  protected _transaction: RocksDBTransaction;
  protected _id: number;
  protected _snapshot: RocksDBTransactionSnapshot;

  protected _callbacksSuccess: Array<() => any> = [];
  protected _callbacksFailure: Array<(e?: Error) => any> = [];
  protected _callbacksFinally: Array<(e?: Error) => any> = [];
  protected _committed: boolean = false;
  protected _rollbacked: boolean = false;

  /**
   * References to iterators
   */
  protected _iteratorRefs: Set<DBIterator<any, any>> = new Set();

  public constructor({
    db,
    logger,
    ...options
  }: {
    db: DB;
    logger?: Logger;
  } & RocksDBTransactionOptions) {
    logger = logger ?? new Logger(this.constructor.name);
    logger.debug(`Constructing ${this.constructor.name}`);
    this.logger = logger;
    this._db = db;
    const options_ = {
      ...options,
      // Transactions should be synchronous
      sync: true,
    };
    utils.filterUndefined(options_);
    this._options = options_;
    this._transaction = rocksdbP.transactionInit(db.db, options_);
    db.transactionRefs.add(this);
    this._id = rocksdbP.transactionId(this._transaction);
    logger.debug(`Constructed ${this.constructor.name} ${this._id}`);
  }

  /**
   * Destroy the transaction
   * This cannot be called until the transaction is committed or rollbacked
   */
  public async destroy() {
    this.logger.debug(`Destroying ${this.constructor.name} ${this._id}`);
    this._db.transactionRefs.delete(this);
    if (!this._committed && !this._rollbacked) {
      throw new errors.ErrorDBTransactionNotCommittedNorRollbacked();
    }
    this.logger.debug(`Destroyed ${this.constructor.name} ${this._id}`);
  }

  get db(): Readonly<DB> {
    return this._db;
  }

  get transaction(): Readonly<RocksDBTransaction> {
    return this._transaction;
  }

  get id(): number {
    return this._id;
  }

  /**
   * @internal
   */
  get iteratorRefs(): Readonly<Set<DBIterator<any, any>>> {
    return this._iteratorRefs;
  }

  get callbacksSuccess(): Readonly<Array<() => any>> {
    return this._callbacksSuccess;
  }

  get callbacksFailure(): Readonly<Array<() => any>> {
    return this._callbacksFailure;
  }

  get callbacksFinally(): Readonly<Array<() => any>> {
    return this._callbacksFinally;
  }

  get committed(): boolean {
    return this._committed;
  }

  get rollbacked(): boolean {
    return this._rollbacked;
  }

  public async get<T>(
    keyPath: KeyPath | string | Buffer,
    raw?: false,
  ): Promise<T | undefined>;
  public async get(
    keyPath: KeyPath | string | Buffer,
    raw: true,
  ): Promise<Buffer | undefined>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public async get<T>(
    keyPath: KeyPath | string | Buffer,
    raw: boolean = false,
  ): Promise<T | Buffer | undefined> {
    keyPath = utils.toKeyPath(keyPath);
    keyPath = ['data', ...keyPath];
    let data: Buffer;
    try {
      const key = utils.keyPathToKey(keyPath);
      data = await rocksdbP.transactionGet(this._transaction, key, {
        valueEncoding: 'buffer',
        snapshot: this.setupSnapshot(),
      });
    } catch (e) {
      if (e.code === 'NOT_FOUND') {
        return undefined;
      }
      throw e;
    }
    return this._db.deserializeDecrypt<T>(data, raw as any);
  }

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
  @ready(new errors.ErrorDBTransactionDestroyed())
  public async put(
    keyPath: KeyPath | string | Buffer,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    this.setupSnapshot();
    keyPath = utils.toKeyPath(keyPath);
    keyPath = ['data', ...keyPath];
    const key = utils.keyPathToKey(keyPath);
    const data = await this._db.serializeEncrypt(value, raw as any);
    return rocksdbP.transactionPut(this._transaction, key, data);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async del(keyPath: KeyPath | string | Buffer): Promise<void> {
    this.setupSnapshot();
    keyPath = utils.toKeyPath(keyPath);
    keyPath = ['data', ...keyPath];
    const key = utils.keyPathToKey(keyPath);
    return rocksdbP.transactionDel(this._transaction, key);
  }

  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions<RocksDBTransactionSnapshot> & {
      keys: false;
      values: false;
    },
  ): DBIterator<undefined, undefined>;
  public iterator<V>(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions<RocksDBTransactionSnapshot> & {
      keys: false;
      valueAsBuffer: false;
    },
  ): DBIterator<undefined, V>;
  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions<RocksDBTransactionSnapshot> & { keys: false },
  ): DBIterator<undefined, Buffer>;
  public iterator(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions<RocksDBTransactionSnapshot> & { values: false },
  ): DBIterator<KeyPath, undefined>;
  public iterator<V>(
    levelPath: LevelPath | undefined,
    options: DBIteratorOptions<RocksDBTransactionSnapshot> & {
      valueAsBuffer: false;
    },
  ): DBIterator<KeyPath, V>;
  public iterator(
    levelPath?: LevelPath | undefined,
    options?: DBIteratorOptions<RocksDBTransactionSnapshot>,
  ): DBIterator<KeyPath, Buffer>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public iterator<V>(
    levelPath: LevelPath = [],
    options: DBIteratorOptions<RocksDBTransactionSnapshot> = {},
  ): DBIterator<KeyPath | undefined, Buffer | V | undefined> {
    levelPath = ['data', ...levelPath];
    return new DBIterator({
      ...options,
      db: this._db,
      transaction: this,
      levelPath,
      logger: this.logger.getChild(DBIterator.name),
      snapshot: this.setupSnapshot(),
    });
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async clear(
    levelPath: LevelPath = [],
    options: DBClearOptions<RocksDBTransactionSnapshot> = {},
  ): Promise<void> {
    levelPath = ['data', ...levelPath];
    const options_ = utils.iterationOptions(options, levelPath);
    return rocksdbP.transactionClear(this._transaction, {
      ...options_,
      snapshot: this.setupSnapshot(),
    });
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async count(
    levelPath: LevelPath = [],
    options: DBCountOptions<RocksDBTransactionSnapshot> = {},
  ): Promise<number> {
    const options_ = {
      ...options,
      keys: true,
      values: false,
    };
    let count = 0;
    for await (const _ of this.iterator(levelPath, options_)) {
      count++;
    }
    return count;
  }

  /**
   * Dump from transaction level path
   * This will only show entries for the current transaction
   * It is intended for diagnostics
   */
  public async dump<V>(
    levelPath?: LevelPath,
    raw?: false,
  ): Promise<Array<[KeyPath, V]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
  ): Promise<Array<[KeyPath, Buffer]>>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
  ): Promise<Array<[KeyPath, any]>> {
    const records: Array<[KeyPath, any]> = [];
    for await (const [keyPath, v] of this.iterator(levelPath, {
      keyAsBuffer: raw,
      valueAsBuffer: raw,
    })) {
      records.push([keyPath, v]);
    }
    return records;
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public queueSuccess(f: () => any): void {
    this._callbacksSuccess.push(f);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public queueFailure(f: (e?: Error) => any): void {
    this._callbacksFailure.push(f);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public queueFinally(f: (e?: Error) => any): void {
    this._callbacksFinally.push(f);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async commit(): Promise<void> {
    if (this._rollbacked) {
      throw new errors.ErrorDBTransactionRollbacked();
    }
    if (this._committed) {
      return;
    }
    this.logger.debug(`Committing ${this.constructor.name} ${this._id}`);
    for (const iterator of this._iteratorRefs) {
      await iterator.destroy();
    }
    this._committed = true;
    try {
      try {
        // If this fails, the `DBTransaction` is still considered committed
        // it must be destroyed, it cannot be reused
        await rocksdbP.transactionCommit(this._transaction);
      } catch (e) {
        if (e.code === 'TRANSACTION_CONFLICT') {
          this.logger.debug(
            `Failed Committing ${this.constructor.name} ${this._id} due to ${errors.ErrorDBTransactionConflict.name}`,
          );
          throw new errors.ErrorDBTransactionConflict(undefined, { cause: e });
        } else {
          this.logger.debug(
            `Failed Committing ${this.constructor.name} ${this._id} due to ${e.message}`,
          );
          throw e;
        }
      }
      for (const f of this._callbacksSuccess) {
        await f();
      }
    } finally {
      for (const f of this._callbacksFinally) {
        await f();
      }
    }
    await this.destroy();
    this.logger.debug(`Committed ${this.constructor.name} ${this._id}`);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async rollback(e?: Error): Promise<void> {
    if (this._committed) {
      throw new errors.ErrorDBTransactionCommitted();
    }
    if (this._rollbacked) {
      return;
    }
    this.logger.debug(`Rollbacking ${this.constructor.name} ${this._id}`);
    for (const iterator of this._iteratorRefs) {
      await iterator.destroy();
    }
    this._rollbacked = true;
    try {
      // If this fails, the `DBTransaction` is still considered rollbacked
      // it must be destroyed, it cannot be reused
      await rocksdbP.transactionRollback(this._transaction);
      for (const f of this._callbacksFailure) {
        await f(e);
      }
    } finally {
      for (const f of this._callbacksFinally) {
        await f(e);
      }
    }
    await this.destroy();
    this.logger.debug(`Rollbacked ${this.constructor.name} ${this._id}`);
  }

  /**
   * Sets up the snapshot
   * This is executed lazily, not at this construction,
   * but at the first transactional operation
   */
  protected setupSnapshot(): RocksDBTransactionSnapshot {
    if (this._snapshot == null) {
      this._snapshot = rocksdbP.transactionSnapshot(this._transaction);
    }
    return this._snapshot;
  }
}

export default DBTransaction;
