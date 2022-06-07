#define NAPI_VERSION 3

#include <cassert>
#include <cstdint>
#include <string>
#include <map>
#include <vector>

#include <node/node_api.h>
#include <napi-macros.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/snapshot.h>

#include "debug.h"
#include "database.h"
#include "batch.h"
#include "iterator.h"
#include "transaction.h"
#include "snapshot.h"
#include "utils.h"
#include "workers/database_workers.h"
#include "workers/batch_workers.h"
#include "workers/iterator_workers.h"
#include "workers/transaction_workers.h"
#include "workers/snapshot_workers.h"

/**
 * Hook for when napi environment exits
 * The `napi_env` cannot be accessed at this point in time
 * All napi references are already automatically deleted
 * It is guaranteed that already-scheduled `napi_async_work` items are finished
 * Cleanup operations here have to be synchronous
 * When the napi environment exits, the GC callbacks of live references will
 * run after this hook is called
 */
static void env_cleanup_hook(void* arg) {
  LOG_DEBUG("Cleaning NAPI Environment\n");
  auto database = static_cast<Database*>(arg);
  // Do everything that `dbClose`  does but synchronously
  // This may execute when the database hasn't been opened
  // or when the database hasn't been closed
  // If it hasn't been opened, it means only `dbInit` was caled
  // If it hasn't been closed, then `GCDatabase` did not yet run
  // Therefore this must also check if the `db_` is still set
  if (!database->hasClosed_ && database->db_ != nullptr) {
    std::map<uint32_t, Iterator*> iterators = database->iterators_;
    std::map<uint32_t, Iterator*>::iterator iterator_it;
    for (iterator_it = iterators.begin(); iterator_it != iterators.end();
         ++iterator_it) {
      auto iterator = iterator_it->second;
      iterator->Close();
    }
    std::map<uint32_t, Transaction*> transactions = database->transactions_;
    std::map<uint32_t, Transaction*>::iterator transaction_it;
    for (transaction_it = transactions.begin();
         transaction_it != transactions.end(); ++transaction_it) {
      auto transaction = transaction_it->second;
      // Close transaction iterators too
      std::map<uint32_t, Iterator*> iterators = transaction->iterators_;
      std::map<uint32_t, Iterator*>::iterator iterator_it;
      for (iterator_it = iterators.begin(); iterator_it != iterators.end();
           ++iterator_it) {
        auto iterator = iterator_it->second;
        iterator->Close();
      }
      transaction->Rollback();
    }
    std::map<uint32_t, Snapshot*> snapshots = database->snapshots_;
    std::map<uint32_t, Snapshot*>::iterator snapshot_it;
    for (snapshot_it = snapshots.begin(); snapshot_it != snapshots.end();
         ++snapshot_it) {
      auto snapshot = snapshot_it->second;
      snapshot->Release();
    }
    database->Close();
  }
  LOG_DEBUG("Cleaned NAPI Environment\n");
}

/**
 * Used by:
 *   - `iteratorClose`
 *   - `dbClose`
 *   - `transactionCommit`
 *   - `TransactionRollbackDo`
 */
static void IteratorCloseDo(napi_env env, Iterator* iterator, napi_value cb) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  IteratorCloseWorker* worker = new IteratorCloseWorker(env, iterator, cb);
  iterator->isClosing_ = true;
  // The only pending work for iterator is the `IteratorNextWorker`
  if (!iterator->nexting_) {
    LOG_DEBUG("%s:Queuing IteratorCloseWorker\n", __func__);
    worker->Queue(env);
    LOG_DEBUG("%s:Called %s\n", __func__, __func__);
    return;
  }
  LOG_DEBUG("%s:Delayed IteratorCloseWorker\n", __func__);
  iterator->closeWorker_ = worker;
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Used by `transactionRollback` and `dbClose`
 */
static void TransactionRollbackDo(napi_env env, Transaction* transaction,
                                  napi_value cb) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  TransactionRollbackWorker* worker =
      new TransactionRollbackWorker(env, transaction, cb);
  transaction->isRollbacking_ = true;
  if (!transaction->HasPendingWork()) {
    LOG_DEBUG("%s:Queuing TransactionRollbackWorker\n", __func__);
    worker->Queue(env);
    LOG_DEBUG("%s:Called %s\n", __func__, __func__);
    return;
  }
  LOG_DEBUG("%s:Delayed TransactionRollbackWorker\n", __func__);
  transaction->closeWorker_ = worker;
  napi_value noop;
  napi_create_function(env, NULL, 0, noop_callback, NULL, &noop);
  std::map<uint32_t, Iterator*> iterators = transaction->iterators_;
  std::map<uint32_t, Iterator*>::iterator iterator_it;
  for (iterator_it = iterators.begin(); iterator_it != iterators.end();
       ++iterator_it) {
    Iterator* iterator = iterator_it->second;
    if (iterator->isClosing_ || iterator->hasClosed_) {
      continue;
    }
    LOG_DEBUG("%s:Closing Iterator %d\n", __func__, iterator->id_);
    IteratorCloseDo(env, iterator, noop);
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Used by `snapshotRelease` and `dbClose`
 */
static void SnapshotReleaseDo(napi_env env, Snapshot* snapshot, napi_value cb) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  SnapshotReleaseWorker* worker = new SnapshotReleaseWorker(env, snapshot, cb);
  snapshot->isReleasing_ = true;
  LOG_DEBUG("%s:Queuing SnapshotReleaseWorker\n", __func__);
  worker->Queue(env);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collection `Database`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCDatabase(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data != nullptr) {
    auto database = static_cast<Database*>(data);
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    if (!database->isClosing_ && !database->hasClosed_) {
      database->Close();
      database->Detach(env);
    }
    delete database;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collection `Batch`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCBatch(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data) {
    auto batch = static_cast<Batch*>(data);
    delete batch;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collection `Iterator`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCIterator(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data != nullptr) {
    auto iterator = static_cast<Iterator*>(data);
    if (!iterator->isClosing_ && !iterator->hasClosed_) {
      iterator->Close();
      iterator->Detach(env);
    }
    delete iterator;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collect `Transaction`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCTransaction(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data != nullptr) {
    auto transaction = static_cast<Transaction*>(data);
    if (!transaction->isCommitting_ && !transaction->hasCommitted_ &&
        !transaction->isRollbacking_ && !transaction->hasRollbacked_) {
      transaction->Rollback();
      transaction->Detach(env);
    }
    delete transaction;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collect `Snapshot`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCSnapshot(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data != nullptr) {
    auto snapshot = static_cast<Snapshot*>(data);
    if (!snapshot->isReleasing_ && !snapshot->hasReleased_) {
      snapshot->Release();
      snapshot->Detach(env);
    }
    delete snapshot;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Garbage collect `TransactionSnapshot`
 * Only occurs when the object falls out of scope
 * with no references and no concurrent workers
 */
static void GCTransactionSnapshot(napi_env env, void* data, void* hint) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  if (data) {
    auto snapshot = static_cast<TransactionSnapshot*>(data);
    delete snapshot;
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
}

/**
 * Creates the Database object
 */
NAPI_METHOD(dbInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  Database* database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);
  napi_value database_ref;
  NAPI_STATUS_THROWS(
      napi_create_external(env, database, GCDatabase, nullptr, &database_ref));
  database->Attach(env, database_ref);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return database_ref;
}

/**
 * Open a database
 */
NAPI_METHOD(dbOpen) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  NAPI_ARGV_UTF8_NEW(location, 1);

  napi_value options = argv[2];
  const bool createIfMissing =
      BooleanProperty(env, options, "createIfMissing", true);
  const bool errorIfExists =
      BooleanProperty(env, options, "errorIfExists", false);
  const bool compression = BooleanProperty(env, options, "compression", true);

  const std::string infoLogLevel = StringProperty(env, options, "infoLogLevel");

  const uint32_t cacheSize = Uint32Property(env, options, "cacheSize", 8 << 20);
  const uint32_t writeBufferSize =
      Uint32Property(env, options, "writeBufferSize", 4 << 20);
  const uint32_t blockSize = Uint32Property(env, options, "blockSize", 4096);
  const uint32_t maxOpenFiles =
      Uint32Property(env, options, "maxOpenFiles", 1000);
  const uint32_t blockRestartInterval =
      Uint32Property(env, options, "blockRestartInterval", 16);
  const uint32_t maxFileSize =
      Uint32Property(env, options, "maxFileSize", 2 << 20);

  napi_value callback = argv[3];

  rocksdb::InfoLogLevel log_level;
  rocksdb::Logger* logger;
  if (infoLogLevel.size() > 0) {
    if (infoLogLevel == "debug")
      log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;
    else if (infoLogLevel == "info")
      log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
    else if (infoLogLevel == "warn")
      log_level = rocksdb::InfoLogLevel::WARN_LEVEL;
    else if (infoLogLevel == "error")
      log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
    else if (infoLogLevel == "fatal")
      log_level = rocksdb::InfoLogLevel::FATAL_LEVEL;
    else if (infoLogLevel == "header")
      log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    else {
      napi_value callback_error =
          CreateCodeError(env, "DB_OPEN", "Invalid log level");
      NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
      NAPI_RETURN_UNDEFINED();
    }
    logger = nullptr;
  } else {
    // In some places RocksDB checks this option to see if it should prepare
    // debug information (ahead of logging), so set it to the highest level.
    log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    logger = new NullLogger();
  }

  OpenWorker* worker = new OpenWorker(
      env, database, callback, location, createIfMissing, errorIfExists,
      compression, writeBufferSize, blockSize, maxOpenFiles,
      blockRestartInterval, maxFileSize, cacheSize, log_level, logger);
  LOG_DEBUG("%s:Queuing OpenWorker\n", __func__);
  worker->Queue(env);
  delete[] location;

  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Close a database
 * This is asynchronous
 */
NAPI_METHOD(dbClose) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();
  napi_value callback = argv[1];
  CloseWorker* worker = new CloseWorker(env, database, callback);
  database->isClosing_ = true;
  if (!database->HasPendingWork()) {
    LOG_DEBUG("%s:Queuing CloseWorker\n", __func__);
    worker->Queue(env);
    LOG_DEBUG("%s:Called %s\n", __func__, __func__);
    NAPI_RETURN_UNDEFINED();
  }
  LOG_DEBUG("%s:Delayed CloseWorker\n", __func__);
  database->closeWorker_ = worker;
  napi_value noop;
  napi_create_function(env, NULL, 0, noop_callback, NULL, &noop);
  std::map<uint32_t, Iterator*> iterators = database->iterators_;
  std::map<uint32_t, Iterator*>::iterator iterator_it;
  for (iterator_it = iterators.begin(); iterator_it != iterators.end();
       ++iterator_it) {
    auto iterator = iterator_it->second;
    if (iterator->isClosing_ || iterator->hasClosed_) {
      continue;
    }
    LOG_DEBUG("%s:Closing Iterator %d\n", __func__, iterator->id_);
    IteratorCloseDo(env, iterator, noop);
  }
  std::map<uint32_t, Transaction*> transactions = database->transactions_;
  std::map<uint32_t, Transaction*>::iterator transaction_it;
  for (transaction_it = transactions.begin();
       transaction_it != transactions.end(); ++transaction_it) {
    auto transaction = transaction_it->second;
    if (transaction->isCommitting_ || transaction->hasCommitted_ ||
        transaction->isRollbacking_ || transaction->hasRollbacked_) {
      continue;
    }
    LOG_DEBUG("%s:Rollbacking Transaction %d\n", __func__, transaction->id_);
    TransactionRollbackDo(env, transaction, noop);
  }
  std::map<uint32_t, Snapshot*> snapshots = database->snapshots_;
  std::map<uint32_t, Snapshot*>::iterator snapshot_it;
  for (snapshot_it = snapshots.begin(); snapshot_it != snapshots.end();
       ++snapshot_it) {
    auto snapshot = snapshot_it->second;
    if (snapshot->isReleasing_ || snapshot->hasReleased_) {
      continue;
    }
    LOG_DEBUG("%s:Releasing Snapshot %d\n", __func__, snapshot->id_);
    SnapshotReleaseDo(env, snapshot, noop);
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets a value from a database.
 */
NAPI_METHOD(dbGet) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const Snapshot* snapshot = SnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  GetWorker* worker = new GetWorker(env, database, callback, key, asBuffer,
                                    fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets many values from a database.
 */
NAPI_METHOD(dbMultiGet) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  const std::vector<rocksdb::Slice>* keys = KeyArray(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const Snapshot* snapshot = SnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  MultiGetWorker* worker = new MultiGetWorker(env, database, keys, callback,
                                              asBuffer, fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Puts a key and a value to a database.
 */
NAPI_METHOD(dbPut) {
  NAPI_ARGV(5);
  NAPI_DB_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  rocksdb::Slice value = ToSlice(env, argv[2]);
  bool sync = BooleanProperty(env, argv[3], "sync", false);
  napi_value callback = argv[4];
  PutWorker* worker = new PutWorker(env, database, callback, key, value, sync);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Delete a value from a database.
 */
NAPI_METHOD(dbDel) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  bool sync = BooleanProperty(env, argv[2], "sync", false);
  napi_value callback = argv[3];
  DelWorker* worker = new DelWorker(env, database, callback, key, sync);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Delete a range from a database.
 */
NAPI_METHOD(dbClear) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();
  napi_value options = argv[1];
  napi_value callback = argv[2];
  const int limit = Int32Property(env, options, "limit", -1);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const Snapshot* snapshot = SnapshotProperty(env, options, "snapshot");
  const bool sync = BooleanProperty(env, options, "sync", false);
  IteratorClearWorker* worker = new IteratorClearWorker(
      env, database, callback, limit, lt, lte, gt, gte, sync, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Count a range from a database.
 */
NAPI_METHOD(dbCount) {
  NAPI_ARGV(3);
  NAPI_DB_CONTEXT();
  napi_value options = argv[1];
  napi_value callback = argv[2];
  const int limit = Int32Property(env, options, "limit", -1);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const Snapshot* snapshot = SnapshotProperty(env, options, "snapshot");
  IteratorCountWorker* worker = new IteratorCountWorker(
      env, database, callback, limit, lt, lte, gt, gte, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Calculates the approximate size of a range in a database.
 */
NAPI_METHOD(dbApproximateSize) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  rocksdb::Slice start = ToSlice(env, argv[1]);
  rocksdb::Slice end = ToSlice(env, argv[2]);
  napi_value callback = argv[3];
  ApproximateSizeWorker* worker =
      new ApproximateSizeWorker(env, database, callback, start, end);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Compacts a range in a database.
 */
NAPI_METHOD(dbCompactRange) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  rocksdb::Slice start = ToSlice(env, argv[1]);
  rocksdb::Slice end = ToSlice(env, argv[2]);
  napi_value callback = argv[3];
  CompactRangeWorker* worker =
      new CompactRangeWorker(env, database, callback, start, end);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Get a property from a database.
 */
NAPI_METHOD(dbGetProperty) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();
  rocksdb::Slice property = ToSlice(env, argv[1]);
  std::string value;
  database->GetProperty(property, &value);
  napi_value result;
  napi_create_string_utf8(env, value.data(), value.size(), &result);
  DisposeSliceBuffer(property);
  return result;
}

/**
 * Gets a snapshot from the database
 */
NAPI_METHOD(snapshotInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(1);
  NAPI_DB_CONTEXT();
  const uint32_t id = database->currentSnapshotId_++;
  Snapshot* snapshot = new Snapshot(database, id);
  // Opaque JS value acting as a reference to `rocksdb::Snapshot`
  napi_value snapshot_ref;
  NAPI_STATUS_THROWS(
      napi_create_external(env, snapshot, GCSnapshot, nullptr, &snapshot_ref));
  snapshot->Attach(env, snapshot_ref);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return snapshot_ref;
}

NAPI_METHOD(snapshotRelease) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_SNAPSHOT_CONTEXT();
  napi_value callback = argv[1];
  if (snapshot->isReleasing_ || snapshot->hasReleased_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  SnapshotReleaseDo(env, snapshot, callback);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Destroys a database.
 */
NAPI_METHOD(destroyDb) {
  NAPI_ARGV(2);
  NAPI_ARGV_UTF8_NEW(location, 0);
  napi_value callback = argv[1];
  DestroyWorker* worker = new DestroyWorker(env, location, callback);
  worker->Queue(env);
  delete[] location;
  NAPI_RETURN_UNDEFINED();
}

/**
 * Repairs a database.
 */
NAPI_METHOD(repairDb) {
  NAPI_ARGV(2);
  NAPI_ARGV_UTF8_NEW(location, 0);
  napi_value callback = argv[1];
  RepairWorker* worker = new RepairWorker(env, location, callback);
  worker->Queue(env);
  delete[] location;
  NAPI_RETURN_UNDEFINED();
}

/**
 * Create an iterator.
 */
NAPI_METHOD(iteratorInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();
  napi_value options = argv[1];
  const bool reverse = BooleanProperty(env, options, "reverse", false);
  const bool keys = BooleanProperty(env, options, "keys", true);
  const bool values = BooleanProperty(env, options, "values", true);
  const bool fillCache = BooleanProperty(env, options, "fillCache", false);
  const bool keyAsBuffer = EncodingIsBuffer(env, options, "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const int limit = Int32Property(env, options, "limit", -1);
  const uint32_t highWaterMarkBytes =
      Uint32Property(env, options, "highWaterMarkBytes", 16 * 1024);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const Snapshot* snapshot = SnapshotProperty(env, options, "snapshot");
  const uint32_t id = database->currentIteratorId_++;
  Iterator* iterator = new Iterator(
      database, id, reverse, keys, values, limit, lt, lte, gt, gte, fillCache,
      keyAsBuffer, valueAsBuffer, highWaterMarkBytes, snapshot);
  napi_value iterator_ref;
  NAPI_STATUS_THROWS(
      napi_create_external(env, iterator, GCIterator, NULL, &iterator_ref));
  iterator->Attach(env, iterator_ref);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return iterator_ref;
}

/**
 * Seeks an iterator.
 */
NAPI_METHOD(iteratorSeek) {
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();
  if (iterator->isClosing_ || iterator->hasClosed_) {
    NAPI_RETURN_UNDEFINED();
  }
  rocksdb::Slice target = ToSlice(env, argv[1]);
  iterator->first_ = true;
  iterator->Seek(target);
  DisposeSliceBuffer(target);
  NAPI_RETURN_UNDEFINED();
}

/**
 * CLoses an iterator.
 */
NAPI_METHOD(iteratorClose) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();
  napi_value callback = argv[1];
  if (iterator->isClosing_ || iterator->hasClosed_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  IteratorCloseDo(env, iterator, callback);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Advance repeatedly and get multiple entries at once.
 */
NAPI_METHOD(iteratorNextv) {
  NAPI_ARGV(3);
  NAPI_ITERATOR_CONTEXT();
  uint32_t size;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &size));
  if (size == 0) size = 1;
  napi_value callback = argv[2];
  if (iterator->isClosing_ || iterator->hasClosed_) {
    napi_value argv =
        CreateCodeError(env, "ITERATOR_NOT_OPEN", "Iterator is not open");
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &argv));
    NAPI_RETURN_UNDEFINED();
  }
  IteratorNextWorker* worker =
      new IteratorNextWorker(env, iterator, size, callback);
  iterator->nexting_ = true;
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Does a batch write operation on a database.
 */
NAPI_METHOD(batchDo) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  napi_value array = argv[1];
  const bool sync = BooleanProperty(env, argv[2], "sync", false);
  napi_value callback = argv[3];
  uint32_t length;
  napi_get_array_length(env, array, &length);
  rocksdb::WriteBatch* batch = new rocksdb::WriteBatch();
  bool hasData = false;
  for (uint32_t i = 0; i < length; i++) {
    napi_value element;
    napi_get_element(env, array, i, &element);
    if (!IsObject(env, element)) continue;
    std::string type = StringProperty(env, element, "type");
    if (type == "del") {
      if (!HasProperty(env, element, "key")) continue;
      rocksdb::Slice key = ToSlice(env, GetProperty(env, element, "key"));
      batch->Delete(key);
      if (!hasData) hasData = true;
      DisposeSliceBuffer(key);
    } else if (type == "put") {
      if (!HasProperty(env, element, "key")) continue;
      if (!HasProperty(env, element, "value")) continue;
      rocksdb::Slice key = ToSlice(env, GetProperty(env, element, "key"));
      rocksdb::Slice value = ToSlice(env, GetProperty(env, element, "value"));
      batch->Put(key, value);
      if (!hasData) hasData = true;
      DisposeSliceBuffer(key);
      DisposeSliceBuffer(value);
    }
  }
  BatchWorker* worker =
      new BatchWorker(env, database, callback, batch, sync, hasData);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Return a batch object.
 */
NAPI_METHOD(batchInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(1);
  NAPI_DB_CONTEXT();
  Batch* batch = new Batch(database);
  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch, GCBatch, NULL, &result));
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return result;
}

/**
 * Adds a put instruction to a batch object.
 */
NAPI_METHOD(batchPut) {
  NAPI_ARGV(3);
  NAPI_BATCH_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  rocksdb::Slice value = ToSlice(env, argv[2]);
  batch->Put(key, value);
  DisposeSliceBuffer(key);
  DisposeSliceBuffer(value);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Adds a delete instruction to a batch object.
 */
NAPI_METHOD(batchDel) {
  NAPI_ARGV(2);
  NAPI_BATCH_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  batch->Del(key);
  DisposeSliceBuffer(key);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Clears a batch object.
 */
NAPI_METHOD(batchClear) {
  NAPI_ARGV(1);
  NAPI_BATCH_CONTEXT();
  batch->Clear();
  NAPI_RETURN_UNDEFINED();
}

/**
 * Writes a batch object.
 */
NAPI_METHOD(batchWrite) {
  NAPI_ARGV(3);
  NAPI_BATCH_CONTEXT();
  napi_value options = argv[1];
  const bool sync = BooleanProperty(env, options, "sync", false);
  napi_value callback = argv[2];
  BatchWriteWorker* worker =
      new BatchWriteWorker(env, argv[0], batch, callback, sync);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Creates a transaction
 *
 * @returns {napi_value} A `napi_external` that references `Transaction`
 */
NAPI_METHOD(transactionInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();
  napi_value options = argv[1];
  const bool sync = BooleanProperty(env, options, "sync", false);
  const uint32_t id = database->currentTransactionId_++;
  Transaction* transaction = new Transaction(database, id, sync);
  // Opaque JS value acting as a reference to `Transaction`
  napi_value transaction_ref;
  NAPI_STATUS_THROWS(napi_create_external(env, transaction, GCTransaction, NULL,
                                          &transaction_ref));
  transaction->Attach(env, transaction_ref);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return transaction_ref;
}

NAPI_METHOD(transactionId) {
  NAPI_ARGV(1);
  NAPI_TRANSACTION_CONTEXT();
  ASSERT_TRANSACTION_READY(env, transaction);
  // This uses our own id instead of `Transaction::GetID()` and
  // `Transaction::GetId()`
  const uint32_t id = transaction->id_;
  NAPI_RETURN_UINT32(id);
}

/**
 * Commit transaction
 */
NAPI_METHOD(transactionCommit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_TRANSACTION_CONTEXT();
  assert(!transaction->isRollbacking_ && !transaction->hasRollbacked_);
  napi_value callback = argv[1];
  if (transaction->isCommitting_ || transaction->hasCommitted_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  TransactionCommitWorker* worker =
      new TransactionCommitWorker(env, transaction, callback);
  transaction->isCommitting_ = true;
  if (!transaction->HasPendingWork()) {
    LOG_DEBUG("%s:Queuing TransactionCommitWorker\n", __func__);
    worker->Queue(env);
    LOG_DEBUG("%s:Called %s\n", __func__, __func__);
    NAPI_RETURN_UNDEFINED();
  }
  LOG_DEBUG("%s:Delayed TransactionCommitWorker\n", __func__);
  transaction->closeWorker_ = worker;
  napi_value noop;
  napi_create_function(env, NULL, 0, noop_callback, NULL, &noop);
  // Close transactional iterators
  std::map<uint32_t, Iterator*> iterators = transaction->iterators_;
  std::map<uint32_t, Iterator*>::iterator iterator_it;
  for (iterator_it = iterators.begin(); iterator_it != iterators.end();
       ++iterator_it) {
    Iterator* iterator = iterator_it->second;
    if (iterator->isClosing_ || iterator->hasClosed_) {
      continue;
    }
    LOG_DEBUG("%s:Closing Iterator %d\n", __func__, iterator->id_);
    IteratorCloseDo(env, iterator, noop);
  }
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Rollback transaction
 */
NAPI_METHOD(transactionRollback) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_TRANSACTION_CONTEXT();
  assert(!transaction->isCommitting_ && !transaction->hasCommitted_);
  napi_value callback = argv[1];
  if (transaction->isRollbacking_ || transaction->hasRollbacked_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  TransactionRollbackDo(env, transaction, callback);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets a value from a transaction
 */
NAPI_METHOD(transactionGet) {
  NAPI_ARGV(4);
  NAPI_TRANSACTION_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  ASSERT_TRANSACTION_READY_CB(env, transaction, callback);
  TransactionGetWorker* worker = new TransactionGetWorker(
      env, transaction, callback, key, asBuffer, fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets a value for update from a transaction
 */
NAPI_METHOD(transactionGetForUpdate) {
  NAPI_ARGV(4);
  NAPI_TRANSACTION_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  ASSERT_TRANSACTION_READY_CB(env, transaction, callback);
  TransactionGetForUpdateWorker* worker = new TransactionGetForUpdateWorker(
      env, transaction, callback, key, asBuffer, fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets many values from a transaction
 */
NAPI_METHOD(transactionMultiGet) {
  NAPI_ARGV(4);
  NAPI_TRANSACTION_CONTEXT();
  const std::vector<rocksdb::Slice>* keys = KeyArray(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  TransactionMultiGetWorker* worker = new TransactionMultiGetWorker(
      env, transaction, keys, callback, asBuffer, fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets many values for update from a transaction
 */
NAPI_METHOD(transactionMultiGetForUpdate) {
  NAPI_ARGV(4);
  NAPI_TRANSACTION_CONTEXT();
  const std::vector<rocksdb::Slice>* keys = KeyArray(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  napi_value callback = argv[3];
  TransactionMultiGetForUpdateWorker* worker =
      new TransactionMultiGetForUpdateWorker(env, transaction, keys, callback,
                                             asBuffer, fillCache, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Puts a key and a value to a transaction
 */
NAPI_METHOD(transactionPut) {
  NAPI_ARGV(4);
  NAPI_TRANSACTION_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  rocksdb::Slice value = ToSlice(env, argv[2]);
  napi_value callback = argv[3];
  ASSERT_TRANSACTION_READY_CB(env, transaction, callback);
  TransactionPutWorker* worker =
      new TransactionPutWorker(env, transaction, callback, key, value);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Delete a value from a database.
 */
NAPI_METHOD(transactionDel) {
  NAPI_ARGV(3);
  NAPI_TRANSACTION_CONTEXT();
  rocksdb::Slice key = ToSlice(env, argv[1]);
  napi_value callback = argv[2];
  ASSERT_TRANSACTION_READY_CB(env, transaction, callback);
  TransactionDelWorker* worker =
      new TransactionDelWorker(env, transaction, callback, key);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

NAPI_METHOD(transactionSnapshot) {
  NAPI_ARGV(1);
  NAPI_TRANSACTION_CONTEXT();
  ASSERT_TRANSACTION_READY(env, transaction);
  TransactionSnapshot* snapshot = new TransactionSnapshot(transaction);
  // Opaque JS value acting as a reference to `rocksdb::Snapshot`
  napi_value snapshot_ref;
  NAPI_STATUS_THROWS(napi_create_external(env, snapshot, GCTransactionSnapshot,
                                          nullptr, &snapshot_ref));
  return snapshot_ref;
}

NAPI_METHOD(transactionIteratorInit) {
  LOG_DEBUG("%s:Calling %s\n", __func__, __func__);
  NAPI_ARGV(2);
  NAPI_TRANSACTION_CONTEXT();
  ASSERT_TRANSACTION_READY(env, transaction);
  napi_value options = argv[1];
  const bool reverse = BooleanProperty(env, options, "reverse", false);
  const bool keys = BooleanProperty(env, options, "keys", true);
  const bool values = BooleanProperty(env, options, "values", true);
  const bool fillCache = BooleanProperty(env, options, "fillCache", false);
  const bool keyAsBuffer = EncodingIsBuffer(env, options, "keyEncoding");
  const bool valueAsBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const int limit = Int32Property(env, options, "limit", -1);
  const uint32_t highWaterMarkBytes =
      Uint32Property(env, options, "highWaterMarkBytes", 16 * 1024);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  const uint32_t id = transaction->currentIteratorId_++;
  Iterator* iterator = new Iterator(
      transaction, id, reverse, keys, values, limit, lt, lte, gt, gte,
      fillCache, keyAsBuffer, valueAsBuffer, highWaterMarkBytes, snapshot);
  napi_value iterator_ref;
  NAPI_STATUS_THROWS(
      napi_create_external(env, iterator, GCIterator, NULL, &iterator_ref));
  iterator->Attach(env, iterator_ref);
  LOG_DEBUG("%s:Called %s\n", __func__, __func__);
  return iterator_ref;
}

NAPI_METHOD(transactionClear) {
  NAPI_ARGV(3);
  NAPI_TRANSACTION_CONTEXT();
  ASSERT_TRANSACTION_READY(env, transaction);
  napi_value options = argv[1];
  napi_value callback = argv[2];
  const int limit = Int32Property(env, options, "limit", -1);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  IteratorClearWorker* worker = new IteratorClearWorker(
      env, transaction, callback, limit, lt, lte, gt, gte, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

NAPI_METHOD(transactionCount) {
  NAPI_ARGV(3);
  NAPI_TRANSACTION_CONTEXT();
  ASSERT_TRANSACTION_READY(env, transaction);
  napi_value options = argv[1];
  napi_value callback = argv[2];
  const int limit = Int32Property(env, options, "limit", -1);
  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");
  const TransactionSnapshot* snapshot =
      TransactionSnapshotProperty(env, options, "snapshot");
  IteratorCountWorker* worker = new IteratorCountWorker(
      env, transaction, callback, limit, lt, lte, gt, gte, snapshot);
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * All exported functions.
 */
NAPI_INIT() {
  // Check `NODE_DEBUG_NATIVE` environment variable
  CheckNodeDebugNative();

  NAPI_EXPORT_FUNCTION(dbInit);
  NAPI_EXPORT_FUNCTION(dbOpen);
  NAPI_EXPORT_FUNCTION(dbClose);
  NAPI_EXPORT_FUNCTION(dbGet);
  NAPI_EXPORT_FUNCTION(dbMultiGet);
  NAPI_EXPORT_FUNCTION(dbPut);
  NAPI_EXPORT_FUNCTION(dbDel);
  NAPI_EXPORT_FUNCTION(dbClear);
  NAPI_EXPORT_FUNCTION(dbCount);
  NAPI_EXPORT_FUNCTION(dbApproximateSize);
  NAPI_EXPORT_FUNCTION(dbCompactRange);
  NAPI_EXPORT_FUNCTION(dbGetProperty);

  NAPI_EXPORT_FUNCTION(snapshotInit);
  NAPI_EXPORT_FUNCTION(snapshotRelease);

  NAPI_EXPORT_FUNCTION(destroyDb);
  NAPI_EXPORT_FUNCTION(repairDb);

  NAPI_EXPORT_FUNCTION(iteratorInit);
  NAPI_EXPORT_FUNCTION(iteratorSeek);
  NAPI_EXPORT_FUNCTION(iteratorNextv);
  NAPI_EXPORT_FUNCTION(iteratorClose);

  NAPI_EXPORT_FUNCTION(batchDo);
  NAPI_EXPORT_FUNCTION(batchInit);
  NAPI_EXPORT_FUNCTION(batchPut);
  NAPI_EXPORT_FUNCTION(batchDel);
  NAPI_EXPORT_FUNCTION(batchClear);
  NAPI_EXPORT_FUNCTION(batchWrite);

  NAPI_EXPORT_FUNCTION(transactionInit);
  NAPI_EXPORT_FUNCTION(transactionId);
  NAPI_EXPORT_FUNCTION(transactionCommit);
  NAPI_EXPORT_FUNCTION(transactionRollback);
  NAPI_EXPORT_FUNCTION(transactionGet);
  NAPI_EXPORT_FUNCTION(transactionGetForUpdate);
  NAPI_EXPORT_FUNCTION(transactionMultiGet);
  NAPI_EXPORT_FUNCTION(transactionMultiGetForUpdate);
  NAPI_EXPORT_FUNCTION(transactionPut);
  NAPI_EXPORT_FUNCTION(transactionDel);
  NAPI_EXPORT_FUNCTION(transactionSnapshot);
  NAPI_EXPORT_FUNCTION(transactionIteratorInit);
  NAPI_EXPORT_FUNCTION(transactionClear);
  NAPI_EXPORT_FUNCTION(transactionCount);
}
