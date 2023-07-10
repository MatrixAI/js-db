import type { Callback } from '../types';
import type {
  RocksDBDatabase,
  RocksDBIterator,
  RocksDBTransaction,
  RocksDBSnapshot,
  RocksDBTransactionSnapshot,
  RocksDBBatch,
  RocksDBDatabaseOptions,
  RocksDBGetOptions,
  RocksDBPutOptions,
  RocksDBDelOptions,
  RocksDBClearOptions,
  RocksDBIteratorOptions,
  RocksDBTransactionOptions,
  RocksDBBatchOptions,
  RocksDBBatchDelOperation,
  RocksDBBatchPutOperation,
  RocksDBCountOptions,
} from './types';
import path from 'path';

interface RocksDB {
  dbInit(): RocksDBDatabase;
  dbOpen(
    database: RocksDBDatabase,
    location: string,
    options: RocksDBDatabaseOptions,
    callback: Callback<[], void>,
  ): void;
  dbClose(database: RocksDBDatabase, callback: Callback<[], void>): void;
  dbGet(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[string], void>,
  ): void;
  dbGet(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Buffer], void>,
  ): void;
  dbMultiGet(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding?: 'utf8' },
    callback: Callback<[Array<string>], void>,
  ): void;
  dbMultiGet(
    database: RocksDBDatabase,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions & { valueEncoding: 'buffer' },
    callback: Callback<[Array<Buffer>], void>,
  ): void;
  dbPut(
    database: RocksDBDatabase,
    key: string | Buffer,
    value: string | Buffer,
    options: RocksDBPutOptions,
    callback: Callback<[], void>,
  ): void;
  dbDel(
    database: RocksDBDatabase,
    key: string | Buffer,
    options: RocksDBDelOptions,
    callback: Callback<[], void>,
  ): void;
  dbClear(
    database: RocksDBDatabase,
    options: RocksDBClearOptions,
    callback: Callback<[], void>,
  ): void;
  dbCount(
    database: RocksDBDatabase,
    options: RocksDBCountOptions,
    callback: Callback<[number], void>,
  ): void;
  dbApproximateSize(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[number], void>,
  ): void;
  dbCompactRange(
    database: RocksDBDatabase,
    start: string | Buffer,
    end: string | Buffer,
    callback: Callback<[], void>,
  ): void;
  dbGetProperty(database: RocksDBDatabase, property: string): string;
  snapshotInit(database: RocksDBDatabase): RocksDBSnapshot;
  snapshotRelease(
    snapshot: RocksDBSnapshot,
    callback: Callback<[], void>,
  ): void;
  destroyDb(location: string, callback: Callback<[], void>): void;
  repairDb(location: string, callback: Callback<[], void>): void;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, Buffer>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { keyEncoding: 'buffer' },
  ): RocksDBIterator<Buffer, string>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions & { valueEncoding: 'buffer' },
  ): RocksDBIterator<string, Buffer>;
  iteratorInit(
    database: RocksDBDatabase,
    options: RocksDBIteratorOptions,
  ): RocksDBIterator<string, string>;
  iteratorSeek<K extends string | Buffer>(
    iterator: RocksDBIterator<K>,
    target: K,
  ): void;
  iteratorClose(iterator: RocksDBIterator, callback: Callback<[], void>): void;
  iteratorNextv<K extends string | Buffer, V extends string | Buffer>(
    iterator: RocksDBIterator<K, V>,
    size: number,
    callback: Callback<[Array<[K, V]>, boolean], void>,
  ): void;
  batchDo(
    database: RocksDBDatabase,
    operations: Array<RocksDBBatchPutOperation | RocksDBBatchDelOperation>,
    options: RocksDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
  batchInit(database: RocksDBDatabase): RocksDBBatch;
  batchPut(
    batch: RocksDBBatch,
    key: string | Buffer,
    value: string | Buffer,
  ): void;
  batchDel(batch: RocksDBBatch, key: string | Buffer): void;
  batchClear(batch: RocksDBBatch): void;
  batchWrite(
    batch: RocksDBBatch,
    options: RocksDBBatchOptions,
    callback: Callback<[], void>,
  ): void;
  transactionInit(
    database: RocksDBDatabase,
    options: RocksDBTransactionOptions,
  ): RocksDBTransaction;
  transactionId(transaction: RocksDBTransaction): number;
  transactionCommit(
    transaction: RocksDBTransaction,
    callback: Callback<[], void>,
  ): void;
  transactionRollback(
    transaction: RocksDBTransaction,
    callback: Callback<[], void>,
  ): void;
  transactionGet(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
    callback: Callback<[string], void>,
  ): void;
  transactionGet(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
    callback: Callback<[Buffer], void>,
  ): void;
  transactionGetForUpdate(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
    callback: Callback<[string], void>,
  ): void;
  transactionGetForUpdate(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
    callback: Callback<[Buffer], void>,
  ): void;
  transactionMultiGet(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
    callback: Callback<[Array<string>], void>,
  ): void;
  transactionMultiGet(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
    callback: Callback<[Array<Buffer>], void>,
  ): void;
  transactionMultiGetForUpdate(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding?: 'utf8';
    },
    callback: Callback<[Array<string>], void>,
  ): void;
  transactionMultiGetForUpdate(
    transaction: RocksDBTransaction,
    keys: Array<string | Buffer>,
    options: RocksDBGetOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
    callback: Callback<[Array<Buffer>], void>,
  ): void;
  transactionPut(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    value: string | Buffer,
    callback: Callback<[], void>,
  ): void;
  transactionDel(
    transaction: RocksDBTransaction,
    key: string | Buffer,
    callback: Callback<[], void>,
  ): void;
  transactionSnapshot(
    transaction: RocksDBTransaction,
  ): RocksDBTransactionSnapshot;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      keyEncoding: 'buffer';
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, Buffer>;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      keyEncoding: 'buffer';
    },
  ): RocksDBIterator<Buffer, string>;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot> & {
      valueEncoding: 'buffer';
    },
  ): RocksDBIterator<string, Buffer>;
  transactionIteratorInit(
    transaction: RocksDBTransaction,
    options: RocksDBIteratorOptions<RocksDBTransactionSnapshot>,
  ): RocksDBIterator<string, string>;
  transactionClear(
    transaction: RocksDBTransaction,
    options: RocksDBClearOptions<RocksDBTransactionSnapshot>,
    callback: Callback<[], void>,
  ): void;
  transactionCount(
    transaction: RocksDBTransaction,
    options: RocksDBCountOptions<RocksDBTransactionSnapshot>,
    callback: Callback<[number], void>,
  ): void;
}

const projectRoot = path.join(__dirname, '../../');
const prebuildPath = path.join(projectRoot, 'prebuild');

/**
 * Try require on all prebuild targets first, then
 * try require on all npm targets second.
 */
function requireBinding(targets: Array<string>): RocksDB {
  const prebuildTargets = targets.map((target) =>
    path.join(prebuildPath, `db-${target}.node`),
  );
  for (const prebuildTarget of prebuildTargets) {
    try {
      return require(prebuildTarget);
    } catch (e) {
      if (e.code !== 'MODULE_NOT_FOUND') throw e;
    }
  }
  const npmTargets = targets.map((target) => `@matrixai/db-${target}`);
  for (const npmTarget of npmTargets) {
    try {
      return require(npmTarget);
    } catch (e) {
      if (e.code !== 'MODULE_NOT_FOUND') throw e;
    }
  }
  throw new Error(
    `Failed requiring possible native bindings: ${prebuildTargets.concat(
      npmTargets,
    )}`,
  );
}

let nativeBinding: RocksDB;

/**
 * For desktop we only support win32, darwin and linux.
 * Mobile OS support is pending.
 */
switch (process.platform) {
  case 'win32':
    switch (process.arch) {
      case 'x64':
        nativeBinding = requireBinding(['win32-x64']);
        break;
      case 'ia32':
        nativeBinding = requireBinding(['win32-ia32']);
        break;
      case 'arm64':
        nativeBinding = requireBinding(['win32-arm64']);
        break;
      default:
        throw new Error(`Unsupported architecture on Windows: ${process.arch}`);
    }
    break;
  case 'darwin':
    switch (process.arch) {
      case 'x64':
        nativeBinding = requireBinding([
          'darwin-x64',
          'darwin-x64+arm64',
          'darwin-arm64+x64',
        ]);
        break;
      case 'arm64':
        nativeBinding = requireBinding([
          'darwin-arm64',
          'darwin-arm64+x64',
          'darwin-x64+arm64',
        ]);
        break;
      default:
        throw new Error(`Unsupported architecture on macOS: ${process.arch}`);
    }
    break;
  case 'linux':
    switch (process.arch) {
      case 'x64':
        nativeBinding = requireBinding(['linux-x64']);
        break;
      case 'arm64':
        nativeBinding = requireBinding(['linux-arm64']);
        break;
      case 'arm':
        nativeBinding = requireBinding(['linux-arm']);
        break;
      default:
        throw new Error(`Unsupported architecture on Linux: ${process.arch}`);
    }
    break;
  default:
    throw new Error(
      `Unsupported OS: ${process.platform}, architecture: ${process.arch}`,
    );
}

export default nativeBinding;

export type { RocksDB };
