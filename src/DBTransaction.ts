import type { AbstractIteratorOptions } from 'abstract-leveldown';
import type DB from './DB';
import type { KeyPath, LevelPath, DBIterator, DBOps } from './types';
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
    return new this({
      db,
      transactionId,
      logger,
    });
  }

  public readonly transactionId: number;
  public readonly transactionPath: LevelPath;

  protected db: DB;
  protected logger: Logger;
  protected _ops: DBOps = [];
  protected _callbacksSuccess: Array<() => any> = [];
  protected _callbacksFailure: Array<() => any> = [];
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
  }

  public async destroy() {
    await this.db._clear(this.db.db, this.transactionPath);
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
  ): Promise<T | undefined> {
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (utils.checkSepKeyPath(keyPath as KeyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    let value = await this.db._get<T>(
      [...this.transactionPath, ...keyPath] as unknown as KeyPath,
      raw as any,
    );
    if (value === undefined) {
      value = await this.db.get<T>(keyPath, raw as any);
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
    if (!Array.isArray(keyPath)) {
      keyPath = [keyPath] as KeyPath;
    }
    if (utils.checkSepKeyPath(keyPath as KeyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    await this.db._put(
      [...this.transactionPath, ...keyPath] as unknown as KeyPath,
      value,
      raw as any,
    );
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
    if (utils.checkSepKeyPath(keyPath as KeyPath)) {
      throw new errors.ErrorDBLevelSep();
    }
    await this.db._del([
      ...this.transactionPath,
      ...keyPath,
    ] as unknown as KeyPath);
    this._ops.push({
      type: 'del',
      keyPath,
    });
  }

  public iterator(
    options: AbstractIteratorOptions & { values: false },
    levelPath?: LevelPath,
  ): DBIterator<Buffer, undefined>;
  public iterator(
    options?: AbstractIteratorOptions,
    levelPath?: LevelPath,
  ): DBIterator<Buffer, Buffer>;
  @ready(new errors.ErrorDBTransactionDestroyed())
  public iterator(
    options?: AbstractIteratorOptions,
    levelPath: LevelPath = [],
  ): DBIterator {
    const dataIterator = this.db._iterator(
      this.db.db,
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
        valueAsBuffer: true,
      },
      ['data', ...levelPath],
    );
    const tranIterator = this.db._iterator(
      this.db.db,
      {
        ...options,
        keys: true,
        keyAsBuffer: true,
        valueAsBuffer: true,
      },
      [...this.transactionPath, ...levelPath],
    );
    const order = options?.reverse ? 'desc' : 'asc';
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
      next: async (): Promise<[Buffer, Buffer | undefined] | undefined> => {
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
            return tranKV;
          }
          // If tranIterator is finished but dataIterator is not finished
          // continue with the dataIterator
          if (tranKV == null && dataKV != null) {
            return dataKV;
          }
          const [tranKey, tranData] = tranKV as [Buffer, Buffer | undefined];
          const [dataKey, dataData] = dataKV as [Buffer, Buffer | undefined];
          const keyCompare = Buffer.compare(tranKey, dataKey);
          if (keyCompare < 0) {
            if (order === 'asc') {
              dataIterator.seek(tranKey);
              return [tranKey, tranData];
            } else if (order === 'desc') {
              tranIterator.seek(dataKey);
              return [dataKey, dataData];
            }
          } else if (keyCompare > 0) {
            if (order === 'asc') {
              tranIterator.seek(dataKey);
              return [dataKey, dataData];
            } else if (order === 'desc') {
              dataIterator.seek(tranKey);
              return [tranKey, tranData];
            }
          } else {
            return [tranKey, tranData];
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
    for await (const [k] of await this.iterator({ values: false }, levelPath)) {
      await this.del([...levelPath, k] as unknown as KeyPath);
    }
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async count(levelPath: LevelPath = []): Promise<number> {
    let count = 0;
    for await (const _ of await this.iterator({ values: false }, levelPath)) {
      count++;
    }
    return count;
  }

  /**
   * Dump from transaction level
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
  public queueFailure(f: () => any): void {
    this._callbacksFailure.push(f);
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async commit(): Promise<void> {
    if (this._rollbacked) {
      throw new errors.ErrorDBTransactionRollbacked();
    }
    if (this._committed) {
      return;
    }
    this._committed = true;
    try {
      await this.db.batch(this._ops);
    } catch (e) {
      this._committed = false;
      throw e;
    }
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async rollback(): Promise<void> {
    if (this._committed) {
      throw new errors.ErrorDBTransactionCommitted();
    }
    if (this._rollbacked) {
      return;
    }
    this._rollbacked = true;
    for (const f of this._callbacksFailure) {
      await f();
    }
  }

  @ready(new errors.ErrorDBTransactionDestroyed())
  public async finalize(): Promise<void> {
    if (this._rollbacked) {
      throw new errors.ErrorDBTransactionRollbacked();
    }
    if (!this._committed) {
      throw new errors.ErrorDBTransactionNotCommited();
    }
    for (const f of this._callbacksSuccess) {
      await f();
    }
  }
}

export default DBTransaction;
