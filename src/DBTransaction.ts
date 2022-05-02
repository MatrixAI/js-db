import type { AbstractIteratorOptions } from 'abstract-leveldown';
import type DB from './DB';
import type { KeyPath, LevelPath, DBIterator, DBOps } from './types';
import Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/dist/CreateDestroy';
import { Lock } from '@matrixai/async-locks';
import * as utils from './utils';
import * as errors from './errors';

/**
 * Minimal read-committed transaction system
 *
 * Properties:
 *   - No dirty reads - cannot read uncommitted writes from other transactions
 *   - Non-repeatable reads - multiple reads on the same key may read
 *                            different values due to other committed
 *                            transactions
 *   - Phantom reads - can read entries that are added or deleted by other
 *                     transactions
 *   - Lost updates - can lose writes if 2 transactions commit writes to the
 *                    same key
 *
 * To prevent non-repeatable reads, phantom-reads or lost-updates, it is up to the
 * user to use advisory read/write locking on relevant keys or ranges of keys.
 *
 * This does not use LevelDB snapshots provided by the `iterator` method
 * which would provide "repeatable-read" isolation level by default
 *
 * See: https://en.wikipedia.org/wiki/Isolation_(database_systems)
 */
interface DBTransaction extends CreateDestroy {}
@CreateDestroy()
class DBTransaction {
  public static async createTransaction({
    db,
    transactionId,
    logger = new Logger(this.name),
  }: {
    db: DB;
    transactionId: number;
    logger?: Logger;
  }): Promise<DBTransaction> {
    logger.debug(`Creating ${this.name} ${transactionId}`);
    const tran = new this({
      db,
      transactionId,
      logger,
    });
    logger.debug(`Created ${this.name} ${transactionId}`);
    return tran;
  }

  public readonly transactionId: number;
  public readonly transactionPath: LevelPath;
  public readonly transactionDataPath: LevelPath;
  public readonly transactionTombstonePath: LevelPath;

  protected db: DB;
  protected logger: Logger;
  /**
   * LevelDB snapshots can only be accessed via an iterator
   * This maintains a consistent read-only snapshot of the DB
   * when `DBTransaction` is constructed
   */
  protected snapshot: DBIterator<Buffer, Buffer>;
  /**
   * Reading from the snapshot iterator needs to be an atomic operation
   * involving a synchronus seek and asynchronous next
   */
  protected snapshotLock = new Lock();
  protected _ops: DBOps = [];
  protected _callbacksSuccess: Array<() => any> = [];
  protected _callbacksFailure: Array<(e?: Error) => any> = [];
  protected _callbacksFinally: Array<(e?: Error) => any> = [];
  protected _committed: boolean = false;
  protected _rollbacked: boolean = false;

  public constructor({
    db,
    transactionId,
    logger,
  }: {
    db: DB;
    transactionId: number;
    logger: Logger;
  }) {
    this.logger = logger;
    this.db = db;
    this.snapshot = db._iterator(undefined, ['data']);
    this.transactionId = transactionId;
    this.transactionPath = ['transactions', this.transactionId.toString()];
    // Data path contains the COW overlay
    this.transactionDataPath = [...this.transactionPath, 'data'];
    // Tombstone path tracks whether key has been deleted
    // If `undefined`, it has not been deleted
    // If `true`, then it has been deleted
    // When deleted, the COW overlay entry must also be deleted
    this.transactionTombstonePath = [...this.transactionPath, 'tombstone'];
  }

  public async destroy() {
    this.logger.debug(`Destroying ${this.constructor.name} ${this.transactionId}`);
    await Promise.all([
      this.snapshot.end(),
      this.db._clear(this.transactionPath),
    ]);
    this.logger.debug(`Destroyed ${this.constructor.name} ${this.transactionId}`);
  }

  get ops(): Readonly<DBOps> {
    return this._ops;
  }

  get callbacksSuccess(): Readonly<Array<() => any>> {
    return this._callbacksSuccess;
  }

  get callbacksFailure(): Readonly<Array<() => any>> {
    return this._callbacksFailure;
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
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    let value = await this.db._get<T>(
      [...this.transactionDataPath, ...keyPath],
      raw as any,
    );
    if (value === undefined) {
      if (
        (await this.db._get<boolean>([
          ...this.transactionTombstonePath,
          ...keyPath,
        ])) !== true
      ) {
        value = await this.getSnapshot<T>(keyPath, raw as any);
      }
    }
    return value;
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
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    await this.db._put(
      [...this.transactionDataPath, ...keyPath],
      value,
      raw as any,
    );
    await this.db._del([...this.transactionTombstonePath, ...keyPath]);
    this._ops.push({
      type: 'put',
      keyPath,
      value,
      raw,
    });
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async del(keyPath: KeyPath | string | Buffer): Promise<void> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (keyPath.length < 1) {
      keyPath = [''];
    }
    await this.db._del([...this.transactionDataPath, ...keyPath]);
    await this.db._put([...this.transactionTombstonePath, ...keyPath], true);
    this._ops.push({
      type: 'del',
      keyPath,
    });
  }

  public iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  public iterator(
    options: AbstractIteratorOptions & { values: false; keyAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<string, undefined>;
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
  @ready(new errors.ErrorDBTransactionDestroyed())
  public iterator<V>(
    options?: AbstractIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator<Buffer | string, Buffer | V | undefined> {
    const dataIterator = this.db._iterator(
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
        valueAsBuffer: true,
      },
      ['data', ...levelPath],
    );
    const tranIterator = this.db._iterator(
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
        valueAsBuffer: true,
      },
      [...this.transactionDataPath, ...levelPath],
    );
    const order = options?.reverse ? 'desc' : 'asc';
    const processKV = <V>([k, v]: [Buffer, Buffer | undefined]): [
      Buffer | string,
      Buffer | V | undefined,
    ] => {
      let k_: Buffer | string = k,
        v_: Buffer | V | undefined = v;
      if (options?.keyAsBuffer === false) {
        k_ = k.toString('binary');
      }
      if (v != null && options?.valueAsBuffer === false) {
        v_ = utils.deserialize<V>(v);
      }
      return [k_, v_];
    };
    const iterator = {
      _ended: false,
      _nexting: false,
      seek: (k: Buffer | string): void => {
        if (iterator._ended) {
          throw new Error('cannot call seek() after end()');
        }
        if (iterator._nexting) {
          throw new Error('cannot call seek() before next() has completed');
        }
        if (typeof k === 'string') {
          k = Buffer.from(k, 'utf-8');
        }
        dataIterator.seek(k);
        tranIterator.seek(k);
      },
      end: async (): Promise<void> => {
        if (iterator._ended) {
          throw new Error('end() already called on iterator');
        }
        iterator._ended = true;
        await dataIterator.end();
        await tranIterator.end();
      },
      next: async (): Promise<
        [Buffer | string, Buffer | V | undefined] | undefined
      > => {
        if (iterator._ended) {
          throw new Error('cannot call next() after end()');
        }
        if (iterator._nexting) {
          throw new Error(
            'cannot call next() before previous next() has completed',
          );
        }
        iterator._nexting = true;
        try {
          while (true) {
            const tranKV = (await tranIterator.next()) as
              | [Buffer, Buffer | undefined]
              | undefined;
            const dataKV = (await dataIterator.next()) as
              | [Buffer, Buffer | undefined]
              | undefined;
            // If both are finished, iterator is finished
            if (tranKV == null && dataKV == null) {
              return undefined;
            }
            // If tranIterator is not finished but dataIterator is finished
            // continue with tranIterator
            if (tranKV != null && dataKV == null) {
              return processKV(tranKV);
            }
            // If tranIterator is finished but dataIterator is not finished
            // continue with the dataIterator
            if (tranKV == null && dataKV != null) {
              // If the dataKey is entombed, skip iteration
              if (
                (await this.db._get<boolean>([
                  ...this.transactionTombstonePath,
                  ...levelPath,
                  dataKV[0],
                ])) === true
              ) {
                continue;
              }
              return processKV(dataKV);
            }
            const [tranKey, tranData] = tranKV as [Buffer, Buffer | undefined];
            const [dataKey, dataData] = dataKV as [Buffer, Buffer | undefined];
            const keyCompare = Buffer.compare(tranKey, dataKey);
            if (keyCompare < 0) {
              if (order === 'asc') {
                dataIterator.seek(tranKey);
                return processKV([tranKey, tranData]);
              } else if (order === 'desc') {
                tranIterator.seek(dataKey);
                // If the dataKey is entombed, skip iteration
                if (
                  (await this.db._get<boolean>([
                    ...this.transactionTombstonePath,
                    ...levelPath,
                    dataKey,
                  ])) === true
                ) {
                  continue;
                }
                return processKV([dataKey, dataData]);
              }
            } else if (keyCompare > 0) {
              if (order === 'asc') {
                tranIterator.seek(dataKey);
                // If the dataKey is entombed, skip iteration
                if (
                  (await this.db._get<boolean>([
                    ...this.transactionTombstonePath,
                    ...levelPath,
                    dataKey,
                  ])) === true
                ) {
                  continue;
                }
                return processKV([dataKey, dataData]);
              } else if (order === 'desc') {
                dataIterator.seek(tranKey);
                return processKV([tranKey, tranData]);
              }
            } else {
              return processKV([tranKey, tranData]);
            }
          }
        } finally {
          iterator._nexting = false;
        }
      },
      [Symbol.asyncIterator]: async function* () {
        try {
          let kv;
          while ((kv = await iterator.next()) !== undefined) {
            yield kv;
          }
        } finally {
          if (!iterator._ended) await iterator.end();
        }
      },
    };
    return iterator;
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async clear(levelPath: LevelPath = []): Promise<void> {
    for await (const [k] of this.iterator({ values: false }, levelPath)) {
      await this.del([...levelPath, k]);
    }
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async count(levelPath: LevelPath = []): Promise<number> {
    let count = 0;
    for await (const _ of this.iterator({ values: false }, levelPath)) {
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
  ): Promise<Array<[string, V]>>;
  public async dump(
    levelPath: LevelPath | undefined,
    raw: true,
  ): Promise<Array<[Buffer, Buffer]>>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public async dump(
    levelPath: LevelPath = [],
    raw: boolean = false,
  ): Promise<Array<[string | Buffer, any]>> {
    return await this.db.dump(
      [...this.transactionPath, ...levelPath],
      raw as any,
    );
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
    this.logger.debug(`Committing ${this.constructor.name} ${this.transactionId}`);
    this._committed = true;
    try {
      await this.db.batch(this._ops);
    } catch (e) {
      this._committed = false;
      throw e;
    }
    this.logger.debug(`Committed ${this.constructor.name} ${this.transactionId}`);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async rollback(e?: Error): Promise<void> {
    if (this._committed) {
      throw new errors.ErrorDBTransactionCommitted();
    }
    if (this._rollbacked) {
      return;
    }
    this.logger.debug(`Rollbacking ${this.constructor.name} ${this.transactionId}`);
    this._rollbacked = true;
    for (const f of this._callbacksFailure) {
      await f(e);
    }
    for (const f of this._callbacksFinally) {
      await f(e);
    }
    this.logger.debug(`Rollbacked ${this.constructor.name} ${this.transactionId}`);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async finalize(): Promise<void> {
    if (this._rollbacked) {
      throw new errors.ErrorDBTransactionRollbacked();
    }
    if (!this._committed) {
      throw new errors.ErrorDBTransactionNotCommitted();
    }
    this.logger.debug(`Finalize ${this.constructor.name} ${this.transactionId}`);
    for (const f of this._callbacksSuccess) {
      await f();
    }
    for (const f of this._callbacksFinally) {
      await f();
    }
    this.logger.debug(`Finalized ${this.constructor.name} ${this.transactionId}`);
  }

  /**
   * Get value from the snapshot iterator
   * This is an atomic operation
   * It will seek to the key path and await the next entry
   * If the entry's key equals the desired key, the entry is returned
   */
  protected async getSnapshot<T>(keyPath: KeyPath, raw?: false): Promise<T | undefined>;
  protected async getSnapshot(keyPath: KeyPath, raw: true): Promise<Buffer | undefined>;
  protected async getSnapshot<T>(
    keyPath: KeyPath,
    raw: boolean = false,
  ): Promise<T | Buffer | undefined> {
    return await this.snapshotLock.withF(async () => {
      const key = utils.keyPathToKey(keyPath);
      this.snapshot.seek(utils.keyPathToKey(keyPath));
      const snapKV = await this.snapshot.next();
      if (snapKV == null) {
        return undefined;
      }
      const [snapKey, snapData] = snapKV;
      if (!key.equals(snapKey)) {
        return undefined;
      }
      if (raw) {
        return snapData;
      } else {
        return utils.deserialize<T>(snapData);
      }
    });
  }
}

export default DBTransaction;
