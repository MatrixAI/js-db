import type { KeyPath, LevelPath, DBIteratorOptions } from './types';
import type { LevelDBIterator, LevelDBIteratorOptions } from './leveldb';
import type DB from './DB';
import type Logger from '@matrixai/logger';
import { CreateDestroy, ready } from '@matrixai/async-init/dist/CreateDestroy';
import { Lock } from '@matrixai/async-locks';
import { leveldbP } from './leveldb';
import * as errors from './errors';
import * as utils from './utils';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
interface DBIterator<K extends KeyPath | undefined, V> extends CreateDestroy {}
@CreateDestroy()
class DBIterator<K extends KeyPath | undefined, V> {
  protected db: DB;
  protected levelPath: LevelPath;
  protected logger: Logger;
  protected first: boolean = true;
  protected finished: boolean = false;
  protected cache: Array<[Buffer, Buffer]> = [];
  protected cachePos: number = 0;
  protected lock: Lock = new Lock();
  protected _options: DBIteratorOptions & LevelDBIteratorOptions;
  protected _iterator: LevelDBIterator<Buffer, Buffer>;

  public constructor({
    db,
    levelPath,
    logger,
    ...options
  }: {
    db: DB;
    levelPath: LevelPath;
    logger: Logger;
  } & DBIteratorOptions) {
    logger.debug(`Constructing ${this.constructor.name}`);
    this.logger = logger;
    this.db = db;
    this.levelPath = levelPath;
    const options_ = {
      ...options,
      // Internally we always use the buffer
      keyEncoding: 'buffer',
      valueEncoding: 'buffer',
    } as DBIteratorOptions &
      LevelDBIteratorOptions & {
        keyEncoding: 'buffer';
        valueEncoding: 'buffer';
      };
    if (options?.gt != null) {
      options_.gt = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options.gt)),
      );
    }
    if (options?.gte != null) {
      options_.gte = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options.gte)),
      );
    }
    if (options?.gt == null && options?.gte == null) {
      options_.gt = utils.levelPathToKey(levelPath);
    }
    if (options?.lt != null) {
      options_.lt = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options.lt)),
      );
    }
    if (options?.lte != null) {
      options_.lte = utils.keyPathToKey(
        levelPath.concat(utils.toKeyPath(options.lte)),
      );
    }
    if (options?.lt == null && options?.lte == null) {
      const levelKeyEnd = utils.levelPathToKey(levelPath);
      levelKeyEnd[levelKeyEnd.length - 1] += 1;
      options_.lt = levelKeyEnd;
    }
    utils.filterUndefined(options_);
    this._options = options_;
    this._iterator = leveldbP.iterator_init(db.db, options_);
    db.iteratorRefs.add(this);
    logger.debug(`Constructed ${this.constructor.name}`);
  }

  get iterator(): Readonly<LevelDBIterator<Buffer, Buffer>> {
    return this._iterator;
  }

  get options(): Readonly<LevelDBIteratorOptions> {
    return this._options;
  }

  public async destroy(): Promise<void> {
    this.logger.debug(`Destroying ${this.constructor.name}`);
    this.cache = [];
    await leveldbP.iterator_close(this._iterator);
    this.db.iteratorRefs.delete(this);
    this.logger.debug(`Destroyed ${this.constructor.name}`);
  }

  @ready(new errors.ErrorDBIteratorDestroyed())
  public seek(keyPath: KeyPath | string | Buffer): void {
    if (this.lock.isLocked()) {
      throw new errors.ErrorDBIteratorBusy();
    }
    leveldbP.iterator_seek(
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
      [entries, finished] = await leveldbP.iterator_nextv(this._iterator, 1);
      this.first = false;
    } else {
      [entries, finished] = await leveldbP.iterator_nextv(this._iterator, 1000);
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
        value = await this.db.deserializeDecrypt<V>(entry[1], false);
      } else {
        value = await this.db.deserializeDecrypt(entry[1], true);
      }
    }
    return [keyPath, value] as [K, V];
  }
}

export default DBIterator;
