#define NAPI_VERSION 3

#include <cstdint>
#include <string>
#include <map>
#include <vector>
#include <node_api.h>
#include <napi-macros.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include "database.h"
#include "batch.h"
#include "iterator.h"
#include "transaction.h"
#include "utils.h"
#include "workers/database_workers.h"
#include "workers/batch_workers.h"
#include "workers/iterator_workers.h"
#include "workers/transaction_workers.h"

/**
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */
static void env_cleanup_hook (void* arg) {
  Database* database = (Database*)arg;

  // Do everything that dbClose() does but synchronously. We're expecting that GC
  // did not (yet) collect the database because that would be a user mistake (not
  // closing their db) made during the lifetime of the environment. That's different
  // from an environment being torn down (like the main process or a worker thread)
  // where it's our responsibility to clean up. Note also, the following code must
  // be a safe noop if called before dbOpen() or after dbClose().
  if (database && database->db_ != NULL) {
    std::map<uint32_t, Iterator*> iterators = database->iterators_;
    std::map<uint32_t, Iterator*>::iterator iterator_it;
    // TODO: does not do `napi_delete_reference(env, iterator->ref_)`. Problem?
    for (iterator_it = iterators.begin(); iterator_it != iterators.end(); ++iterator_it) {
      iterator_it->second->Close();
    }

    std::map<uint32_t, Transaction*> trans = database->transactions_;
    std::map<uint32_t, Transaction*>::iterator tran_it;
    // TODO: does not do `napi_delete_reference(env, iterator->ref_)`. Problem?
    for (tran_it = trans.begin(); tran_it != trans.end(); ++tran_it) {
      tran_it->second->Rollback();
    }

    // Having closed the iterators (and released snapshots) we can safely close.
    database->CloseDatabase();
  }
}

/**
 * Called by NAPI_METHOD(iteratorClose) and also when closing
 * open iterators during NAPI_METHOD(dbClose).
 */
static void iterator_close_do (napi_env env, Iterator* iterator, napi_value cb) {
  CloseIteratorWorker* worker = new CloseIteratorWorker(env, iterator, cb);
  iterator->isClosing_ = true;
  if (iterator->nexting_) {
    iterator->closeWorker_ = worker;
  } else {
    worker->Queue(env);
  }
}

/**
 * Called by NAPI_METHOD(transactionRollback) and also when closing
 * open transactions during NAPI_METHOD(dbClose)
 */
static void transaction_rollback_do (
  napi_env env,
  Transaction* transaction,
  napi_value cb
) {
  TransactionRollbackWorker* worker = new TransactionRollbackWorker(
    env,
    transaction,
    cb
  );
  transaction->isRollbacking_ = true;
  // TODO:
  // if other async ops, delay this operation
  worker->Queue(env);
}

/**
 * Runs when a Database is garbage collected.
 */
static void FinalizeDatabase (napi_env env, void* data, void* hint) {
  if (data) {
    Database* database = (Database*)data;
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    if (database->ref_ != NULL) napi_delete_reference(env, database->ref_);
    delete database;
  }
}

/**
 * Runs when a Batch is garbage collected.
 */
static void FinalizeBatch (napi_env env, void* data, void* hint) {
  if (data) {
    delete (Batch*)data;
  }
}

/**
 * Runs when an Iterator is garbage collected.
 */
static void FinalizeIterator (napi_env env, void* data, void* hint) {
  if (data) {
    delete (Iterator*)data;
  }
}

/**
 * Runs when a Transaction is garbage collected.
 */
static void FinalizeTransaction (napi_env env, void* data, void* hint) {
  if (data) {
    delete (Transaction*)data;
  }
}

/**
 * Returns a context object for a database.
 */
NAPI_METHOD(dbInit) {
  Database* database = new Database();
  napi_add_env_cleanup_hook(env, env_cleanup_hook, database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, database,
                                          FinalizeDatabase,
                                          NULL, &result));

  // Reference counter to prevent GC of database while priority workers are active
  NAPI_STATUS_THROWS(napi_create_reference(env, result, 0, &database->ref_));

  return result;
}

/**
 * Open a database.
 */
NAPI_METHOD(dbOpen) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();
  NAPI_ARGV_UTF8_NEW(location, 1);

  napi_value options = argv[2];
  const bool createIfMissing = BooleanProperty(env, options, "createIfMissing", true);
  const bool errorIfExists = BooleanProperty(env, options, "errorIfExists", false);
  const bool compression = BooleanProperty(env, options, "compression", true);

  const std::string infoLogLevel = StringProperty(env, options, "infoLogLevel");

  const uint32_t cacheSize = Uint32Property(env, options, "cacheSize", 8 << 20);
  const uint32_t writeBufferSize = Uint32Property(env, options , "writeBufferSize" , 4 << 20);
  const uint32_t blockSize = Uint32Property(env, options, "blockSize", 4096);
  const uint32_t maxOpenFiles = Uint32Property(env, options, "maxOpenFiles", 1000);
  const uint32_t blockRestartInterval = Uint32Property(env, options,
                                                 "blockRestartInterval", 16);
  const uint32_t maxFileSize = Uint32Property(env, options, "maxFileSize", 2 << 20);

  napi_value callback = argv[3];

  rocksdb::InfoLogLevel log_level;
  rocksdb::Logger* logger;
  if (infoLogLevel.size() > 0) {
    if (infoLogLevel == "debug") log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;
    else if (infoLogLevel == "info") log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
    else if (infoLogLevel == "warn") log_level = rocksdb::InfoLogLevel::WARN_LEVEL;
    else if (infoLogLevel == "error") log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
    else if (infoLogLevel == "fatal") log_level = rocksdb::InfoLogLevel::FATAL_LEVEL;
    else if (infoLogLevel == "header") log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    else {
      napi_value callback_error = CreateCodeError(
        env,
        "DB_OPEN",
        "Invalid log level"
      );
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

  OpenWorker* worker = new OpenWorker(env, database, callback, location,
                                      createIfMissing, errorIfExists,
                                      compression, writeBufferSize, blockSize,
                                      maxOpenFiles, blockRestartInterval,
                                      maxFileSize, cacheSize,
                                      log_level, logger);
  worker->Queue(env);
  delete [] location;

  NAPI_RETURN_UNDEFINED();
}

/**
 * Close a database.
 */
NAPI_METHOD(dbClose) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  napi_value callback = argv[1];
  CloseWorker* worker = new CloseWorker(env, database, callback);

  if (!database->HasPriorityWork()) {
    worker->Queue(env);
    NAPI_RETURN_UNDEFINED();
  }

  database->pendingCloseWorker_ = worker;

  napi_value noop;
  napi_create_function(env, NULL, 0, noop_callback, NULL, &noop);

  // Close all iterators
  std::map<uint32_t, Iterator*> iterators = database->iterators_;
  std::map<uint32_t, Iterator*>::iterator iterator_it;
  for (iterator_it = iterators.begin(); iterator_it != iterators.end(); ++iterator_it) {
    Iterator* iterator = iterator_it->second;
    if (!iterator->isClosing_ && !iterator->hasClosed_) {
      iterator_close_do(env, iterator, noop);
    }
  }

  // Rollback all transactions
  std::map<uint32_t, Transaction*> trans = database->transactions_;
  std::map<uint32_t, Transaction*>::iterator tran_it;
  for (tran_it = trans.begin(); tran_it != trans.end(); ++tran_it) {
    Transaction* tran = tran_it->second;
    if (
      tran->isCommitting_ ||
      tran->hasCommitted_ ||
      tran->isRollbacking_ ||
      tran->hasRollbacked_
    ) {
      continue;
    }
    transaction_rollback_do(env, tran, noop);
  }

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
  napi_value callback = argv[3];

  GetWorker* worker = new GetWorker(env, database, callback, key, asBuffer,
                                    fillCache);
  worker->Queue(env);

  NAPI_RETURN_UNDEFINED();
}

/**
 * Gets many values from a database.
 */
NAPI_METHOD(dbGetMany) {
  NAPI_ARGV(4);
  NAPI_DB_CONTEXT();

  const std::vector<std::string>* keys = KeyArray(env, argv[1]);
  napi_value options = argv[2];
  const bool asBuffer = EncodingIsBuffer(env, options, "valueEncoding");
  const bool fillCache = BooleanProperty(env, options, "fillCache", true);
  napi_value callback = argv[3];

  GetManyWorker* worker = new GetManyWorker(
    env, database, keys, callback, asBuffer, fillCache
  );

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

  const bool reverse = BooleanProperty(env, options, "reverse", false);
  const int limit = Int32Property(env, options, "limit", -1);

  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");

  ClearWorker* worker = new ClearWorker(env, database, callback, reverse, limit, lt, lte, gt, gte);
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

  ApproximateSizeWorker* worker  = new ApproximateSizeWorker(env, database,
                                                             callback, start,
                                                             end);
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

  CompactRangeWorker* worker  = new CompactRangeWorker(env, database, callback,
                                                       start, end);
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
 * Destroys a database.
 */
NAPI_METHOD(destroyDb) {
  NAPI_ARGV(2);
  NAPI_ARGV_UTF8_NEW(location, 0);
  napi_value callback = argv[1];

  DestroyWorker* worker = new DestroyWorker(env, location, callback);
  worker->Queue(env);

  delete [] location;

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

  delete [] location;

  NAPI_RETURN_UNDEFINED();
}

/**
 * Create an iterator.
 */
NAPI_METHOD(iteratorInit) {
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
  const uint32_t highWaterMarkBytes = Uint32Property(env, options, "highWaterMarkBytes", 16 * 1024);

  std::string* lt = RangeOption(env, options, "lt");
  std::string* lte = RangeOption(env, options, "lte");
  std::string* gt = RangeOption(env, options, "gt");
  std::string* gte = RangeOption(env, options, "gte");

  const uint32_t id = database->currentIteratorId_++;
  Iterator* iterator = new Iterator(database, id, reverse, keys,
                                    values, limit, lt, lte, gt, gte, fillCache,
                                    keyAsBuffer, valueAsBuffer, highWaterMarkBytes);
  napi_value result;

  NAPI_STATUS_THROWS(napi_create_external(env, iterator,
                                          FinalizeIterator,
                                          NULL, &result));

  // Prevent GC of JS object before the iterator is closed (explicitly or on
  // db close) and keep track of non-closed iterators to close them on db close.
  iterator->Attach(env, result);

  return result;
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
  NAPI_ARGV(2);
  NAPI_ITERATOR_CONTEXT();
  napi_value callback = argv[1];
  if (iterator->isClosing_ || iterator->hasClosed_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  iterator_close_do(env, iterator, callback);
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
    napi_value argv = CreateCodeError(env, "ITERATOR_NOT_OPEN", "Iterator is not open");
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &argv));
    NAPI_RETURN_UNDEFINED();
  }

  NextWorker* worker = new NextWorker(env, iterator, size, callback);
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

  BatchWorker* worker = new BatchWorker(env, database, callback, batch, sync, hasData);
  worker->Queue(env);

  NAPI_RETURN_UNDEFINED();
}

/**
 * Return a batch object.
 */
NAPI_METHOD(batchInit) {
  NAPI_ARGV(1);
  NAPI_DB_CONTEXT();

  Batch* batch = new Batch(database);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch,
                                          FinalizeBatch,
                                          NULL, &result));
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

  BatchWriteWorker* worker  = new BatchWriteWorker(env, argv[0], batch, callback, sync);
  worker->Queue(env);

  NAPI_RETURN_UNDEFINED();
}

/**
 * Creates a transaction
 *
 * @returns {napi_value} This is a `napi_external` that references `Transaction`
 */
NAPI_METHOD(transactionInit) {
  NAPI_ARGV(2);
  NAPI_DB_CONTEXT();

  napi_value options = argv[1];
  const bool sync = BooleanProperty(env, options, "sync", false);

  const uint32_t id = database->currentTransactionId_++;
  Transaction* tran = new Transaction(database, id, sync);

  // Opaque JS value acting as a reference to `Transaction`
  napi_value tran_ref;

  NAPI_STATUS_THROWS(napi_create_external(
    env,
    tran,
    FinalizeTransaction,
    NULL,
    &tran_ref
  ));

  tran->Attach(env, tran_ref);

  return tran_ref;
}

/**
 * Commit transaction
 * transactionCommit(transaction, callback)
 */
NAPI_METHOD(transactionCommit) {
  NAPI_ARGV(2);
  NAPI_TRANSACTION_CONTEXT();
  napi_value callback = argv[1];
  if (transaction->isRollbacking_ || transaction->hasRollbacked_) {
    napi_value callback_error = CreateCodeError(
      env,
      "TRANSACTION_ROLLBACKED",
      "Transaction is already rollbacked"
    );
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  if (transaction->isCommitting_ || transaction->hasCommitted_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  TransactionCommitWorker* worker = new TransactionCommitWorker(
    env,
    transaction,
    callback
  );
  transaction->isCommitting_ = true;
  // TODO:
  // if other async ops, delay this operation
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * Rollback transaction
 */
NAPI_METHOD(transactionRollback) {
  NAPI_ARGV(2);
  NAPI_TRANSACTION_CONTEXT();
  napi_value callback = argv[1];
  if (transaction->isCommitting_ || transaction->hasCommitted_) {
    napi_value callback_error = CreateCodeError(
      env,
      "TRANSACTION_COMMITTED",
      "Transaction is already committed"
    );
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  if (transaction->isRollbacking_ || transaction->hasRollbacked_) {
    napi_value callback_error;
    napi_get_null(env, &callback_error);
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));
    NAPI_RETURN_UNDEFINED();
  }
  transaction_rollback_do(env, transaction, callback);
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
  napi_value callback = argv[3];
  TransactionGetWorker* worker = new TransactionGetWorker(
    env,
    transaction,
    callback,
    key,
    asBuffer,
    fillCache
  );
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
  const bool exclusive = BooleanProperty(env, options, "exclusive", true);
  napi_value callback = argv[3];
  TransactionGetForUpdateWorker* worker = new TransactionGetForUpdateWorker(
    env,
    transaction,
    callback,
    key,
    asBuffer,
    fillCache,
    exclusive
  );
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
  TransactionPutWorker* worker = new TransactionPutWorker(
    env,
    transaction,
    callback,
    key,
    value
  );
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
  TransactionDelWorker* worker = new TransactionDelWorker(
    env,
    transaction,
    callback,
    key
  );
  worker->Queue(env);
  NAPI_RETURN_UNDEFINED();
}

/**
 * All exported functions.
 */
NAPI_INIT() {
  NAPI_EXPORT_FUNCTION(dbInit);
  NAPI_EXPORT_FUNCTION(dbOpen);
  NAPI_EXPORT_FUNCTION(dbClose);
  NAPI_EXPORT_FUNCTION(dbGet);
  NAPI_EXPORT_FUNCTION(dbGetMany);
  NAPI_EXPORT_FUNCTION(dbPut);
  NAPI_EXPORT_FUNCTION(dbDel);
  NAPI_EXPORT_FUNCTION(dbClear);
  NAPI_EXPORT_FUNCTION(dbApproximateSize);
  NAPI_EXPORT_FUNCTION(dbCompactRange);
  NAPI_EXPORT_FUNCTION(dbGetProperty);

  NAPI_EXPORT_FUNCTION(destroyDb);
  NAPI_EXPORT_FUNCTION(repairDb);

  NAPI_EXPORT_FUNCTION(iteratorInit);
  NAPI_EXPORT_FUNCTION(iteratorSeek);
  NAPI_EXPORT_FUNCTION(iteratorClose);
  NAPI_EXPORT_FUNCTION(iteratorNextv);

  NAPI_EXPORT_FUNCTION(batchDo);
  NAPI_EXPORT_FUNCTION(batchInit);
  NAPI_EXPORT_FUNCTION(batchPut);
  NAPI_EXPORT_FUNCTION(batchDel);
  NAPI_EXPORT_FUNCTION(batchClear);
  NAPI_EXPORT_FUNCTION(batchWrite);

  NAPI_EXPORT_FUNCTION(transactionInit);
  NAPI_EXPORT_FUNCTION(transactionCommit);
  NAPI_EXPORT_FUNCTION(transactionRollback);
  NAPI_EXPORT_FUNCTION(transactionGet);
  NAPI_EXPORT_FUNCTION(transactionGetForUpdate);
  NAPI_EXPORT_FUNCTION(transactionPut);
  NAPI_EXPORT_FUNCTION(transactionDel);
}
