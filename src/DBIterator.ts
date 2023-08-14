import type DB from './DB.js';
import type DBTransaction from './DBTransaction.js';
import type { Merge, KeyPath, LevelPath, DBIteratorOptions } from './types.js';
import type {
  RocksDBIterator,
  RocksDBIteratorOptions,
  RocksDBSnapshot,
  RocksDBTransactionSnapshot,
} from './native/index.js';
import Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/CreateDestroy.js';
import { Lock } from '@matrixai/async-locks';
import { rocksdbP } from './native/index.js';
import * as errors from './errors.js';
import * as utils from './utils.js';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
interface DBIterator<K extends KeyPath | undefined, V> extends CreateDestroy {}
@CreateDestroy()
class DBIterator<K extends KeyPath | undefined, V> {
  protected logger: Logger;
  protected levelPath: LevelPath;
  protected _db: DB;
  protected _transaction?: DBTransaction;
  protected _options: Merge<
    DBIteratorOptions<any>,
    {
      gt?: Buffer;
      gte?: Buffer;
      lt?: Buffer;
      lte?: Buffer;
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    }
  >;
  protected _iterator: RocksDBIterator<Buffer, Buffer>;
  protected first: boolean = true;
  protected finished: boolean = false;
  protected cache: Array<[Buffer, Buffer]> = [];
  protected cachePos: number = 0;
  protected lock: Lock = new Lock();

  public constructor(
    options: {
      db: DB;
      levelPath: LevelPath;
      logger?: Logger;
    } & DBIteratorOptions<RocksDBSnapshot>,
  );
  public constructor(
    options: {
      db: DB;
      transaction: DBTransaction;
      levelPath: LevelPath;
      logger?: Logger;
    } & DBIteratorOptions<RocksDBTransactionSnapshot>,
  );
  public constructor({
    db,
    transaction,
    levelPath,
    logger,
    ...options
  }: {
    db: DB;
    transaction?: DBTransaction;
    levelPath: LevelPath;
    logger?: Logger;
  } & DBIteratorOptions<any>) {
    logger = logger ?? new Logger(this.constructor.name);
    logger.debug(`Constructing ${this.constructor.name}`);
    this.logger = logger;
    this.levelPath = levelPath;
    const options_ = utils.iterationOptions<DBIteratorOptions<any>>(
      options,
      levelPath,
    );
    this._options = options_;
    this._db = db;
    if (transaction != null) {
      this._transaction = transaction;
      this._iterator = rocksdbP.transactionIteratorInit(
        transaction.transaction,
        options_ as RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
          keyEncoding: 'buffer';
          valueEncoding: 'buffer';
        },
      );
      transaction.iteratorRefs.add(this);
    } else {
      this._iterator = rocksdbP.iteratorInit(
        db.db,
        options_ as RocksDBIteratorOptions<RocksDBSnapshot> & {
          keyEncoding: 'buffer';
          valueEncoding: 'buffer';
        },
      );
      db.iteratorRefs.add(this);
    }
    logger.debug(`Constructed ${this.constructor.name}`);
  }

  get db(): Readonly<DB> {
    return this._db;
  }

  get transaction(): Readonly<DBTransaction> | undefined {
    return this._transaction;
  }

  get iterator(): Readonly<RocksDBIterator<Buffer, Buffer>> {
    return this._iterator;
  }

  get options(): Readonly<DBIteratorOptions<any>> {
    return this._options;
  }

  public async destroy(): Promise<void> {
    this.logger.debug(`Destroying ${this.constructor.name}`);
    this.cache = [];
    await rocksdbP.iteratorClose(this._iterator);
    if (this._transaction != null) {
      this._transaction.iteratorRefs.delete(this);
    } else {
      this._db.iteratorRefs.delete(this);
    }
    this.logger.debug(`Destroyed ${this.constructor.name}`);
  }

  @ready(new errors.ErrorDBIteratorDestroyed())
  public seek(keyPath: KeyPath | string | Buffer): void {
    if (this.lock.isLocked()) {
      throw new errors.ErrorDBIteratorBusy();
    }
    rocksdbP.iteratorSeek(
      this._iterator,
      utils.keyPathToKey(this.levelPath.concat(utils.toKeyPath(keyPath))),
    );
    this.first = true;
    this.finished = false;
    this.cache = [];
    this.cachePos = 0;
  }

  @ready(new errors.ErrorDBIteratorDestroyed(), true)
  public async next(): Promise<[K, V] | undefined> {
    return this.lock.withF(this._next.bind(this));
  }

  protected async _next(): Promise<[K, V] | undefined> {
    if (this.cachePos < this.cache.length) {
      const entry = this.cache[this.cachePos];
      const result = this.processEntry(entry);
      this.cachePos += 1;
      return result;
    } else if (this.finished) {
      return;
    }
    let entries: Array<[Buffer, Buffer]>, finished: boolean;
    if (this.first) {
      [entries, finished] = await rocksdbP.iteratorNextv(this._iterator, 1);
      this.first = false;
    } else {
      [entries, finished] = await rocksdbP.iteratorNextv(this._iterator, 1000);
    }
    this.cachePos = 0;
    this.cache = entries;
    this.finished = finished;
    // If the entries are empty and finished is false
    // then this will enter a retry loop
    // until entries is filled or finished is true
    return this._next();
  }

  public async *[Symbol.asyncIterator](): AsyncGenerator<[K, V], void, void> {
    try {
      let entry: [K, V] | undefined;
      while ((entry = await this.next()) !== undefined) {
        yield entry;
      }
    } finally {
      // Once entry is undefined, then it is finished
      // therefore we an perform an idempotent destroy
      await this.destroy();
    }
  }

  protected async processEntry(entry: [Buffer, Buffer]): Promise<[K, V]> {
    let keyPath: KeyPath | undefined;
    let value: Buffer | V | undefined;
    // If keys were false, leveldb returns empty buffer
    if (this._options.keys === false) {
      keyPath = undefined;
    } else {
      // Truncate level path so the returned key is relative to the level path
      keyPath = utils.parseKey(entry[0]).slice(this.levelPath.length);
      if (this._options.keyAsBuffer === false) {
        keyPath = keyPath.map((k) => k.toString('utf-8'));
      }
    }
    // If values were false, leveldb returns empty buffer
    if (this._options.values === false) {
      value = undefined;
    } else {
      if (this._options.valueAsBuffer === false) {
        value = await this._db.deserializeDecrypt<V>(entry[1], false);
      } else {
        value = await this._db.deserializeDecrypt(entry[1], true);
      }
    }
    return [keyPath, value] as [K, V];
  }
}

export default DBIterator;
