import type { AbstractIteratorOptions } from 'abstract-leveldown';
import type DB from './DB';
import type { POJO, DBDomain, DBLevel, DBIterator, DBOps } from './types';
import Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/dist/CreateDestroy';
import * as dbUtils from './utils';
import * as dbErrors from './errors';

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
    transactionDb,
    transactionId,
    logger = new Logger(this.name),
  }: {
    db: DB;
    transactionDb: DBLevel;
    transactionId: number;
    logger?: Logger;
  }): Promise<DBTransaction> {
    return new this({
      db,
      transactionDb,
      transactionId,
      logger,
    });
  }

  public readonly transactionId: number;

  protected db: DB;
  protected transactionDb: DBLevel;
  protected logger: Logger;
  protected _ops: DBOps = [];
  protected _callbacksSuccess: Array<() => any> = [];
  protected _callbacksFailure: Array<() => any> = [];
  protected _committed: boolean = false;
  protected _rollbacked: boolean = false;

  public constructor({
    db,
    transactionDb,
    transactionId,
    logger,
  }: {
    db: DB;
    transactionDb: DBLevel;
    transactionId: number;
    logger: Logger;
  }) {
    this.logger = logger;
    this.db = db;
    this.transactionDb = transactionDb;
    this.transactionId = transactionId;
  }

  public async destroy() {
    await this.transactionDb.clear();
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

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async dump(domain: DBDomain = []): Promise<POJO> {
    let transactionLevel = this.transactionDb;
    for (const d of domain) {
      transactionLevel = await this.db.level(d, transactionLevel);
    }
    const records = {};
    for await (const o of transactionLevel.createReadStream()) {
      const key = (o as any).key.toString();
      const data = (o as any).value as Buffer;
      const value = await this.db.deserializeDecrypt(data, false);
      records[key] = value;
    }
    return records;
  }

  public async iterator(
    options: AbstractIteratorOptions & { values: false },
    domain?: DBDomain,
  ): Promise<DBIterator<Buffer, undefined>>;
  public async iterator(
    options?: AbstractIteratorOptions,
    domain?: DBDomain,
  ): Promise<DBIterator<Buffer, Buffer>>;
  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async iterator(
    options?: AbstractIteratorOptions,
    domain: DBDomain = [],
  ): Promise<DBIterator> {
    let dataLevel = this.db.dataDb;
    for (const d of domain) {
      dataLevel = await this.db.level(d, dataLevel);
    }
    const dataIterator = dataLevel.iterator({
      ...options,
      keys: true,
      keyAsBuffer: true,
      valuesAsBuffer: true,
    });
    let transactionLevel = this.transactionDb;
    for (const d of domain) {
      transactionLevel = await this.db.level(d, transactionLevel);
    }
    const tranIterator = transactionLevel.iterator({
      ...options,
      keys: true,
      keyAsBuffer: true,
      valuesAsBuffer: true,
    });
    const order = options?.reverse ? 'desc' : 'asc';
    const iterator = {
      _ended: false,
      _nexting: false,
      seek: (k: Buffer | string) => {
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
      end: async () => {
        if (iterator._ended) {
          throw new Error('end() already called on iterator');
        }
        iterator._ended = true;
        // @ts-ignore AbstractIterator type is outdated
        await dataIterator.end();
        // @ts-ignore AbstractIterator type is outdated
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
        const decryptKV = async ([key, data]: [
          Buffer,
          Buffer | undefined,
        ]): Promise<[Buffer, Buffer | undefined]> => {
          if (data != null) {
            data = await this.db.deserializeDecrypt(data, true);
          }
          return [key, data];
        };
        try {
          // @ts-ignore AbstractIterator type is outdated
          const tranKV = (await tranIterator.next()) as
            | [Buffer, Buffer | undefined]
            | undefined;
          // @ts-ignore AbstractIterator type is outdated
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
            return decryptKV(tranKV);
          }
          // If tranIterator is finished but dataIterator is not finished
          // continue with the dataIterator
          if (tranKV == null && dataKV != null) {
            return decryptKV(dataKV);
          }
          const [tranKey, tranData] = tranKV as [Buffer, Buffer | undefined];
          const [dataKey, dataData] = dataKV as [Buffer, Buffer | undefined];
          const keyCompare = Buffer.compare(tranKey, dataKey);
          if (keyCompare < 0) {
            if (order === 'asc') {
              dataIterator.seek(tranKey);
              return decryptKV([tranKey, tranData]);
            } else if (order === 'desc') {
              tranIterator.seek(dataKey);
              return decryptKV([dataKey, dataData]);
            }
          } else if (keyCompare > 0) {
            if (order === 'asc') {
              tranIterator.seek(dataKey);
              return decryptKV([dataKey, dataData]);
            } else if (order === 'desc') {
              dataIterator.seek(tranKey);
              return decryptKV([tranKey, tranData]);
            }
          } else {
            return decryptKV([tranKey, tranData]);
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

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async clear(domain: DBDomain = []): Promise<void> {
    for await (const [k] of await this.iterator({ values: false }, domain)) {
      await this.del(domain, k);
    }
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async count(domain: DBDomain = []): Promise<number> {
    let count = 0;
    for await (const _ of await this.iterator({ values: false }, domain)) {
      count++;
    }
    return count;
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
  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async get<T>(
    domain: DBDomain,
    key: string | Buffer,
    raw: boolean = false,
  ): Promise<T | undefined> {
    const path = dbUtils.domainPath(domain, key);
    let value: T | undefined;
    try {
      const data = await this.transactionDb.get(path);
      value = await this.db.deserializeDecrypt<T>(data, raw as any);
    } catch (e) {
      if (e.notFound) {
        value = await this.db.get<T>(domain, key, raw as any);
        // Don't set it in the transaction DB
        // Because this is not a repeatable-read "snapshot"
      } else {
        throw e;
      }
    }
    return value;
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
  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async put(
    domain: DBDomain,
    key: string | Buffer,
    value: any,
    raw: boolean = false,
  ): Promise<void> {
    const path = dbUtils.domainPath(domain, key);
    const data = await this.db.serializeEncrypt(value, raw as any);
    await this.transactionDb.put(path, data);
    this._ops.push({
      type: 'put',
      domain,
      key,
      value,
      raw,
    });
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async del(domain: DBDomain, key: string | Buffer): Promise<void> {
    const path = dbUtils.domainPath(domain, key);
    await this.transactionDb.del(path);
    this._ops.push({
      type: 'del',
      domain,
      key,
    });
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public queueSuccess(f: () => any): void {
    this._callbacksSuccess.push(f);
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public queueFailure(f: () => any): void {
    this._callbacksFailure.push(f);
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async commit(): Promise<void> {
    if (this._rollbacked) {
      throw new dbErrors.ErrorDBTransactionRollbacked();
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

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async rollback(): Promise<void> {
    if (this._committed) {
      throw new dbErrors.ErrorDBTransactionCommitted();
    }
    if (this._rollbacked) {
      return;
    }
    this._rollbacked = true;
    for (const f of this._callbacksFailure) {
      await f();
    }
  }

  @ready(new dbErrors.ErrorDBTransactionDestroyed())
  public async finalize(): Promise<void> {
    if (this._rollbacked) {
      throw new dbErrors.ErrorDBTransactionRollbacked();
    }
    if (!this._committed) {
      throw new dbErrors.ErrorDBTransactionNotCommited();
    }
    for (const f of this._callbacksSuccess) {
      await f();
    }
  }
}

export default DBTransaction;
