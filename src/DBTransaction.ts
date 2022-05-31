import type DB from './DB';
import type {
  KeyPath,
  LevelPath,
  DBIterator,
  DBOps,
  DBIteratorOptions,
} from './types';
import Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/dist/CreateDestroy';
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
    this.logger.debug(
      `Destroying ${this.constructor.name} ${this.transactionId}`,
    );
    await this.db._clear(this.transactionPath),
      this.logger.debug(
        `Destroyed ${this.constructor.name} ${this.transactionId}`,
      );
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
    keyPath = utils.toKeyPath(keyPath);
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
        value = await this.db.get<T>(keyPath, raw as any);
      }
      // Don't set it in the transaction DB
      // Because this is not a repeatable-read "snapshot"
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
    keyPath = utils.toKeyPath(keyPath);
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
    keyPath = utils.toKeyPath(keyPath);
    await this.db._del([...this.transactionDataPath, ...keyPath]);
    await this.db._put([...this.transactionTombstonePath, ...keyPath], true);
    this._ops.push({
      type: 'del',
      keyPath,
    });
  }

  public iterator(
    options: DBIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, undefined>;
  public iterator(
    options?: DBIteratorOptions & { valueAsBuffer?: true },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, Buffer>;
  public iterator<V>(
    options?: DBIteratorOptions & { valueAsBuffer: false },
    levelPath?: LevelPath,
  ): DBIterator<KeyPath, V>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public iterator<V>(
    options?: DBIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator<KeyPath, Buffer | V | undefined> {
    const dataIterator = this.db._iterator(
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
      },
      ['data', ...levelPath],
    );
    const tranIterator = this.db._iterator(
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
      },
      [...this.transactionDataPath, ...levelPath],
    );
    const order = options?.reverse ? 'desc' : 'asc';
    const processKV = (
      kv: [KeyPath, Buffer | V | undefined],
    ): [KeyPath, Buffer | V | undefined] => {
      if (options?.keyAsBuffer === false) {
        kv[0] = kv[0].map((k) => k.toString('utf-8'));
      }
      return kv;
    };
    const iterator = {
      _ended: false,
      _nexting: false,
      seek: (keyPath: KeyPath | Buffer | string): void => {
        if (iterator._ended) {
          throw new Error('cannot call seek() after end()');
        }
        if (iterator._nexting) {
          throw new Error('cannot call seek() before next() has completed');
        }
        dataIterator.seek(keyPath);
        tranIterator.seek(keyPath);
      },
      end: async () => {
        if (iterator._ended) {
          throw new Error('end() already called on iterator');
        }
        iterator._ended = true;
        await dataIterator.end();
        await tranIterator.end();
      },
      next: async () => {
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
              | [KeyPath, Buffer | undefined]
              | undefined;
            const dataKV = (await dataIterator.next()) as
              | [KeyPath, Buffer | undefined]
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
                (await this.db._get<boolean>(
                  this.transactionTombstonePath.concat(levelPath, dataKV[0]),
                )) === true
              ) {
                continue;
              }
              return processKV(dataKV);
            }
            const [tranKeyPath, tranData] = tranKV as [
              KeyPath,
              Buffer | V | undefined,
            ];
            const [dataKeyPath, dataData] = dataKV as [
              KeyPath,
              Buffer | V | undefined,
            ];
            const keyCompare = Buffer.compare(
              utils.keyPathToKey(tranKeyPath),
              utils.keyPathToKey(dataKeyPath),
            );
            if (keyCompare < 0) {
              if (order === 'asc') {
                dataIterator.seek(tranKeyPath);
                return processKV([tranKeyPath, tranData]);
              } else if (order === 'desc') {
                tranIterator.seek(dataKeyPath);
                // If the dataKey is entombed, skip iteration
                if (
                  (await this.db._get<boolean>(
                    this.transactionTombstonePath.concat(
                      levelPath,
                      dataKeyPath,
                    ),
                  )) === true
                ) {
                  continue;
                }
                return processKV([dataKeyPath, dataData]);
              }
            } else if (keyCompare > 0) {
              if (order === 'asc') {
                tranIterator.seek(dataKeyPath);
                // If the dataKey is entombed, skip iteration
                if (
                  (await this.db._get<boolean>(
                    this.transactionTombstonePath.concat(
                      levelPath,
                      dataKeyPath,
                    ),
                  )) === true
                ) {
                  continue;
                }
                return processKV([dataKeyPath, dataData]);
              } else if (order === 'desc') {
                dataIterator.seek(tranKeyPath);
                return processKV([tranKeyPath, tranData]);
              }
            } else {
              return processKV([tranKeyPath, tranData]);
            }
          }
        } finally {
          iterator._nexting = false;
        }
      },
      [Symbol.asyncIterator]: async function* () {
        try {
          let kv: [KeyPath, any] | undefined;
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
    for await (const [keyPath] of this.iterator({ values: false }, levelPath)) {
      await this.del(levelPath.concat(keyPath));
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
    return await this.db.dump(
      this.transactionPath.concat(levelPath),
      raw as any,
      true,
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
    this.logger.debug(
      `Committing ${this.constructor.name} ${this.transactionId}`,
    );
    this._committed = true;
    try {
      await this.db.batch(this._ops);
    } catch (e) {
      this._committed = false;
      throw e;
    }
    this.logger.debug(
      `Committed ${this.constructor.name} ${this.transactionId}`,
    );
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async rollback(e?: Error): Promise<void> {
    if (this._committed) {
      throw new errors.ErrorDBTransactionCommitted();
    }
    if (this._rollbacked) {
      return;
    }
    this.logger.debug(
      `Rollbacking ${this.constructor.name} ${this.transactionId}`,
    );
    this._rollbacked = true;
    for (const f of this._callbacksFailure) {
      await f(e);
    }
    for (const f of this._callbacksFinally) {
      await f(e);
    }
    this.logger.debug(
      `Rollbacked ${this.constructor.name} ${this.transactionId}`,
    );
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async finalize(): Promise<void> {
    if (this._rollbacked) {
      throw new errors.ErrorDBTransactionRollbacked();
    }
    if (!this._committed) {
      throw new errors.ErrorDBTransactionNotCommitted();
    }
    this.logger.debug(
      `Finalize ${this.constructor.name} ${this.transactionId}`,
    );
    for (const f of this._callbacksSuccess) {
      await f();
    }
    for (const f of this._callbacksFinally) {
      await f();
    }
    this.logger.debug(
      `Finalized ${this.constructor.name} ${this.transactionId}`,
    );
  }
}

export default DBTransaction;
