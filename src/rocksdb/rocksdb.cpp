#define NAPI_VERSION 3

#include <napi-macros.h>
#include <node_api.h>
#include <assert.h>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>

#include <map>
#include <vector>

class NullLogger : public rocksdb::Logger {
public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

/**
 * Forward declarations.
 */
struct Database;
struct Iterator;
struct Transaction;
static void iterator_close_do (napi_env env, Iterator* iterator, napi_value cb);
static void transaction_rollback_do (napi_env env, Transaction* transaction, napi_value cb);

/**
 * Macros.
 */

#define NAPI_DB_CONTEXT() \
  Database* database = NULL; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&database));

#define NAPI_ITERATOR_CONTEXT() \
  Iterator* iterator = NULL; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&iterator));

#define NAPI_TRANSACTION_CONTEXT() \
  Transaction* transaction = NULL; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&transaction));

#define NAPI_BATCH_CONTEXT() \
  Batch* batch = NULL; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&batch));

#define NAPI_RETURN_UNDEFINED() \
  return 0;

#define NAPI_UTF8_NEW(name, val)                \
  size_t name##_size = 0;                                               \
  NAPI_STATUS_THROWS(napi_get_value_string_utf8(env, val, NULL, 0, &name##_size)) \
  char* name = new char[name##_size + 1];                               \
  NAPI_STATUS_THROWS(napi_get_value_string_utf8(env, val, name, name##_size + 1, &name##_size)) \
  name[name##_size] = '\0';

#define NAPI_ARGV_UTF8_NEW(name, i) \
  NAPI_UTF8_NEW(name, argv[i])

#define LD_STRING_OR_BUFFER_TO_COPY(env, from, to)                      \
  char* to##Ch_ = 0;                                                    \
  size_t to##Sz_ = 0;                                                   \
  if (IsString(env, from)) {                                            \
    napi_get_value_string_utf8(env, from, NULL, 0, &to##Sz_);           \
    to##Ch_ = new char[to##Sz_ + 1];                                    \
    napi_get_value_string_utf8(env, from, to##Ch_, to##Sz_ + 1, &to##Sz_); \
    to##Ch_[to##Sz_] = '\0';                                            \
  } else if (IsBuffer(env, from)) {                                     \
    char* buf = 0;                                                      \
    napi_get_buffer_info(env, from, (void **)&buf, &to##Sz_);           \
    to##Ch_ = new char[to##Sz_];                                        \
    memcpy(to##Ch_, buf, to##Sz_);                                      \
  }

/**
 * NAPI_EXPORT_FUNCTION does not export the name of the function
 */
#undef NAPI_EXPORT_FUNCTION
#define NAPI_EXPORT_FUNCTION(name) \
  { \
    napi_value name##_fn; \
    NAPI_STATUS_THROWS_VOID(napi_create_function(env, #name, NAPI_AUTO_LENGTH, name, NULL, &name##_fn)) \
    NAPI_STATUS_THROWS_VOID(napi_set_named_property(env, exports, #name, name##_fn)) \
  }

/*********************************************************************
 * Helpers.
 ********************************************************************/

/**
 * Returns true if 'value' is a string.
 */
static bool IsString (napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_string;
}

/**
 * Returns true if 'value' is a buffer.
 */
static bool IsBuffer (napi_env env, napi_value value) {
  bool isBuffer;
  napi_is_buffer(env, value, &isBuffer);
  return isBuffer;
}

/**
 * Returns true if 'value' is an object.
 */
static bool IsObject (napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_object;
}

/**
 * Create an error object.
 */
static napi_value CreateError (napi_env env, const char* str) {
  napi_value msg;
  napi_create_string_utf8(env, str, strlen(str), &msg);
  napi_value error;
  napi_create_error(env, NULL, msg, &error);
  return error;
}

static napi_value CreateCodeError (napi_env env, const char* code, const char* msg) {
  napi_value codeValue;
  napi_create_string_utf8(env, code, strlen(code), &codeValue);
  napi_value msgValue;
  napi_create_string_utf8(env, msg, strlen(msg), &msgValue);
  napi_value error;
  napi_create_error(env, codeValue, msgValue, &error);
  return error;
}

/**
 * Returns true if 'obj' has a property 'key'.
 */
static bool HasProperty (napi_env env, napi_value obj, const char* key) {
  bool has = false;
  napi_has_named_property(env, obj, key, &has);
  return has;
}

/**
 * Returns a property in napi_value form.
 */
static napi_value GetProperty (napi_env env, napi_value obj, const char* key) {
  napi_value value;
  napi_get_named_property(env, obj, key, &value);
  return value;
}

/**
 * Returns a boolean property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
static bool BooleanProperty (napi_env env, napi_value obj, const char* key,
                             bool DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    bool result;
    napi_get_value_bool(env, value, &result);
    return result;
  }

  return DEFAULT;
}

/**
 * Returns true if the options object contains an encoding option that is "buffer"
 */
static bool EncodingIsBuffer (napi_env env, napi_value options, const char* option) {
  napi_value value;
  size_t size;

  if (napi_get_named_property(env, options, option, &value) == napi_ok &&
    napi_get_value_string_utf8(env, value, NULL, 0, &size) == napi_ok) {
    // Value is either "buffer" or "utf8" so we can tell them apart just by size
    return size == 6;
  }

  return false;
}

/**
 * Returns a uint32 property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
static uint32_t Uint32Property (napi_env env, napi_value obj, const char* key,
                                uint32_t DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    uint32_t result;
    napi_get_value_uint32(env, value, &result);
    return result;
  }

  return DEFAULT;
}

/**
 * Returns a int32 property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
static int Int32Property (napi_env env, napi_value obj, const char* key,
                          int DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    int result;
    napi_get_value_int32(env, value, &result);
    return result;
  }

  return DEFAULT;
}

/**
 * Returns a string property 'key' from 'obj'.
 * Returns empty string if the property doesn't exist.
 */
static std::string StringProperty (napi_env env, napi_value obj, const char* key) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    if (IsString(env, value)) {
      size_t size = 0;
      napi_get_value_string_utf8(env, value, NULL, 0, &size);

      char* buf = new char[size + 1];
      napi_get_value_string_utf8(env, value, buf, size + 1, &size);
      buf[size] = '\0';

      std::string result = buf;
      delete [] buf;
      return result;
    }
  }

  return "";
}

static void DisposeSliceBuffer (rocksdb::Slice slice) {
  if (!slice.empty()) delete [] slice.data();
}

/**
 * Convert a napi_value to a rocksdb::Slice.
 */
static rocksdb::Slice ToSlice (napi_env env, napi_value from) {
  LD_STRING_OR_BUFFER_TO_COPY(env, from, to);
  return rocksdb::Slice(toCh_, toSz_);
}

/**
 * Returns length of string or buffer
 */
static size_t StringOrBufferLength (napi_env env, napi_value value) {
  size_t size = 0;

  if (IsString(env, value)) {
    napi_get_value_string_utf8(env, value, NULL, 0, &size);
  } else if (IsBuffer(env, value)) {
    char* buf;
    napi_get_buffer_info(env, value, (void **)&buf, &size);
  }

  return size;
}

/**
 * Takes a Buffer or string property 'name' from 'opts'.
 * Returns null if the property does not exist or is zero-length.
 */
static std::string* RangeOption (napi_env env, napi_value opts, const char* name) {
  if (HasProperty(env, opts, name)) {
    napi_value value = GetProperty(env, opts, name);

    if (StringOrBufferLength(env, value) >= 0) {
      LD_STRING_OR_BUFFER_TO_COPY(env, value, to);
      std::string* result = new std::string(toCh_, toSz_);
      delete [] toCh_;
      return result;
    }
  }

  return NULL;
}

/**
 * Converts an array containing Buffer or string keys to a vector.
 */
static std::vector<std::string>* KeyArray (napi_env env, napi_value arr) {
  uint32_t length;
  std::vector<std::string>* result = new std::vector<std::string>();

  if (napi_get_array_length(env, arr, &length) == napi_ok) {
    result->reserve(length);

    for (uint32_t i = 0; i < length; i++) {
      napi_value element;

      if (napi_get_element(env, arr, i, &element) == napi_ok &&
          StringOrBufferLength(env, element) >= 0) {
        LD_STRING_OR_BUFFER_TO_COPY(env, element, to);
        result->emplace_back(toCh_, toSz_);
        delete [] toCh_;
      }
    }
  }

  return result;
}

/**
 * Calls a function.
 */
static napi_status CallFunction (napi_env env,
                                 napi_value callback,
                                 const int argc,
                                 napi_value* argv) {
  napi_value global;
  napi_get_global(env, &global);
  return napi_call_function(env, global, callback, argc, argv, NULL);
}

/**
 * Whether to yield entries, keys or values.
 */
enum Mode {
  entries,
  keys,
  values
};

/**
 * Helper struct for caching and converting a key-value pair to napi_values.
 */
struct Entry {
  Entry (const rocksdb::Slice* key, const rocksdb::Slice* value)
    : key_(key->data(), key->size()),
      value_(value->data(), value->size()) {}

  void ConvertByMode (napi_env env, Mode mode, const bool keyAsBuffer, const bool valueAsBuffer, napi_value* result) {
    if (mode == Mode::entries) {
      napi_create_array_with_length(env, 2, result);

      napi_value keyElement;
      napi_value valueElement;

      Convert(env, &key_, keyAsBuffer, &keyElement);
      Convert(env, &value_, valueAsBuffer, &valueElement);

      napi_set_element(env, *result, 0, keyElement);
      napi_set_element(env, *result, 1, valueElement);
    } else if (mode == Mode::keys) {
      Convert(env, &key_, keyAsBuffer, result);
    } else {
      Convert(env, &value_, valueAsBuffer, result);
    }
  }

  static void Convert (napi_env env, const std::string* s, const bool asBuffer, napi_value* result) {
    if (s == NULL) {
      napi_get_undefined(env, result);
    } else if (asBuffer) {
      napi_create_buffer_copy(env, s->size(), s->data(), NULL, result);
    } else {
      napi_create_string_utf8(env, s->data(), s->size(), result);
    }
  }

private:
  std::string key_;
  std::string value_;
};

/**
 * Base worker class. Handles the async work. Derived classes can override the
 * following virtual methods (listed in the order in which they're called):
 *
 * - DoExecute (abstract, worker pool thread): main work
 * - HandleOKCallback (main thread): call JS callback on success
 * - HandleErrorCallback (main thread): call JS callback on error
 * - DoFinally (main thread): do cleanup regardless of success
 */
struct BaseWorker {
  // Note: storing env is discouraged as we'd end up using it in unsafe places.
  BaseWorker (napi_env env,
              Database* database,
              napi_value callback,
              const char* resourceName)
    : database_(database), errMsg_(NULL) {
    NAPI_STATUS_THROWS_VOID(napi_create_reference(env, callback, 1, &callbackRef_));
    napi_value asyncResourceName;
    NAPI_STATUS_THROWS_VOID(napi_create_string_utf8(env, resourceName,
                                               NAPI_AUTO_LENGTH,
                                               &asyncResourceName));
    NAPI_STATUS_THROWS_VOID(napi_create_async_work(env, callback,
                                              asyncResourceName,
                                              BaseWorker::Execute,
                                              BaseWorker::Complete,
                                              this, &asyncWork_));
  }

  virtual ~BaseWorker () {
    delete [] errMsg_;
  }

  static void Execute (napi_env env, void* data) {
    BaseWorker* self = (BaseWorker*)data;

    // Don't pass env to DoExecute() because use of Node-API
    // methods should generally be avoided in async work.
    self->DoExecute();
  }

  bool SetStatus (rocksdb::Status status) {
    status_ = status;
    if (!status.ok()) {
      SetErrorMessage(status.ToString().c_str());
      return false;
    }
    return true;
  }

  void SetErrorMessage(const char *msg) {
    delete [] errMsg_;
    size_t size = strlen(msg) + 1;
    errMsg_ = new char[size];
    memcpy(errMsg_, msg, size);
  }

  virtual void DoExecute () = 0;

  static void Complete (napi_env env, napi_status status, void* data) {
    BaseWorker* self = (BaseWorker*)data;

    self->DoComplete(env);
    self->DoFinally(env);
  }

  void DoComplete (napi_env env) {
    napi_value callback;
    napi_get_reference_value(env, callbackRef_, &callback);

    if (status_.ok()) {
      HandleOKCallback(env, callback);
    } else {
      HandleErrorCallback(env, callback);
    }
  }

  virtual void HandleOKCallback (napi_env env, napi_value callback) {
    napi_value argv;
    napi_get_null(env, &argv);
    CallFunction(env, callback, 1, &argv);
  }

  virtual void HandleErrorCallback (napi_env env, napi_value callback) {
    napi_value argv;

    if (status_.IsNotFound()) {
      argv = CreateCodeError(env, "NOT_FOUND", errMsg_);
    } else if (status_.IsCorruption()) {
      argv = CreateCodeError(env, "CORRUPTION", errMsg_);
    } else if (status_.IsIOError()) {
      if (strlen(errMsg_) > 15 && strncmp("IO error: lock ", errMsg_, 15) == 0) { // fs_posix.cc
        argv = CreateCodeError(env, "LOCKED", errMsg_);
      } else if (strlen(errMsg_) > 32 && strncmp("IO error: Failed to create lock ", errMsg_, 32) == 0) { // env_win.cc
        argv = CreateCodeError(env, "LOCKED", errMsg_);
      } else {
        argv = CreateCodeError(env, "IO_ERROR", errMsg_);
      }
    } else if (status_.IsBusy()) {
      argv = CreateCodeError(env, "TRANSACTION_CONFLICT", errMsg_);
    } else {
      argv = CreateError(env, errMsg_);
    }

    CallFunction(env, callback, 1, &argv);
  }

  virtual void DoFinally (napi_env env) {
    napi_delete_reference(env, callbackRef_);
    napi_delete_async_work(env, asyncWork_);

    delete this;
  }

  void Queue (napi_env env) {
    napi_queue_async_work(env, asyncWork_);
  }

  Database* database_;

private:
  napi_ref callbackRef_;
  napi_async_work asyncWork_;
  rocksdb::Status status_;
  char *errMsg_;
};

/**
 * Owns the RocksDB storage, cache, filter policy and iterators.
 */
struct Database {
  Database ()
    : db_(NULL),
      currentIteratorId_(0),
      currentTransactionId_(0),
      pendingCloseWorker_(NULL),
      ref_(NULL),
      priorityWork_(0) {}

  ~Database () {
    if (db_ != NULL) {
      delete db_;
      db_ = NULL;
    }
  }

  rocksdb::Status Open (const rocksdb::Options& options,
                        const char* location) {
    return rocksdb::OptimisticTransactionDB::Open(options, location, &db_);
  }

  void CloseDatabase () {
    delete db_;
    db_ = NULL;
  }

  rocksdb::Status Put (const rocksdb::WriteOptions& options,
                       rocksdb::Slice key,
                       rocksdb::Slice value) {
    return db_->Put(options, key, value);
  }

  rocksdb::Status Get (const rocksdb::ReadOptions& options,
                       rocksdb::Slice key,
                       std::string& value) {
    return db_->Get(options, key, &value);
  }

  rocksdb::Status Del (const rocksdb::WriteOptions& options,
                       rocksdb::Slice key) {
    return db_->Delete(options, key);
  }

  rocksdb::Status WriteBatch (const rocksdb::WriteOptions& options,
                              rocksdb::WriteBatch* batch) {
    return db_->Write(options, batch);
  }

  uint64_t ApproximateSize (const rocksdb::Range* range) {
    uint64_t size = 0;
    db_->GetApproximateSizes(range, 1, &size);
    return size;
  }

  void CompactRange (const rocksdb::Slice* start,
                     const rocksdb::Slice* end) {
    rocksdb::CompactRangeOptions options;
    db_->CompactRange(options, start, end);
  }

  void GetProperty (const rocksdb::Slice& property, std::string* value) {
    db_->GetProperty(property, value);
  }

  const rocksdb::Snapshot* NewSnapshot () {
    return db_->GetSnapshot();
  }

  rocksdb::Iterator* NewIterator (rocksdb::ReadOptions* options) {
    return db_->NewIterator(*options);
  }

  rocksdb::Transaction* NewTransaction (rocksdb::WriteOptions* options) {
    return db_->BeginTransaction(*options);
  }

  void ReleaseSnapshot (const rocksdb::Snapshot* snapshot) {
    return db_->ReleaseSnapshot(snapshot);
  }

  void AttachIterator (napi_env env, uint32_t id, Iterator* iterator) {
    iterators_[id] = iterator;
    IncrementPriorityWork(env);
  }

  void DetachIterator (napi_env env, uint32_t id) {
    iterators_.erase(id);
    DecrementPriorityWork(env);
  }

  void AttachTransaction (napi_env env, uint32_t id, Transaction* transaction) {
    transactions_[id] = transaction;
    IncrementPriorityWork(env);
  }

  void DetachTransaction (napi_env env, uint32_t id) {
    transactions_.erase(id);
    DecrementPriorityWork(env);
  }

  void IncrementPriorityWork (napi_env env) {
    napi_reference_ref(env, ref_, &priorityWork_);
  }

  void DecrementPriorityWork (napi_env env) {
    napi_reference_unref(env, ref_, &priorityWork_);

    if (priorityWork_ == 0 && pendingCloseWorker_ != NULL) {
      pendingCloseWorker_->Queue(env);
      pendingCloseWorker_ = NULL;
    }
  }

  bool HasPriorityWork () const {
    return priorityWork_ > 0;
  }

  rocksdb::OptimisticTransactionDB* db_;
  uint32_t currentIteratorId_;
  uint32_t currentTransactionId_;
  BaseWorker *pendingCloseWorker_;
  std::map< uint32_t, Iterator * > iterators_;
  std::map< uint32_t, Transaction * > transactions_;
  napi_ref ref_;

private:
  uint32_t priorityWork_;
};

/**
 * Base worker class for doing async work that defers closing the database.
 */
struct PriorityWorker : public BaseWorker {
  PriorityWorker (napi_env env, Database* database, napi_value callback, const char* resourceName)
    : BaseWorker(env, database, callback, resourceName) {
      database_->IncrementPriorityWork(env);
  }

  virtual ~PriorityWorker () {}

  void DoFinally (napi_env env) override {
    database_->DecrementPriorityWork(env);
    BaseWorker::DoFinally(env);
  }
};

/**
 * Owns a rocksdb iterator.
 */
struct BaseIterator {
  BaseIterator(Database* database,
               const bool reverse,
               std::string* lt,
               std::string* lte,
               std::string* gt,
               std::string* gte,
               const int limit,
               const bool fillCache)
    : database_(database),
      hasClosed_(false),
      didSeek_(false),
      reverse_(reverse),
      lt_(lt),
      lte_(lte),
      gt_(gt),
      gte_(gte),
      limit_(limit),
      count_(0) {
    options_ = new rocksdb::ReadOptions();
    options_->fill_cache = fillCache;
    options_->verify_checksums = false;
    options_->snapshot = database->NewSnapshot();
    dbIterator_ = database_->NewIterator(options_);
  }

  virtual ~BaseIterator () {
    assert(hasClosed_);

    if (lt_ != NULL) delete lt_;
    if (gt_ != NULL) delete gt_;
    if (lte_ != NULL) delete lte_;
    if (gte_ != NULL) delete gte_;

    delete options_;
  }

  bool DidSeek () const {
    return didSeek_;
  }

  /**
   * Seek to the first relevant key based on range options.
   */
  void SeekToRange () {
    didSeek_ = true;

    if (!reverse_ && gte_ != NULL) {
      dbIterator_->Seek(*gte_);
    } else if (!reverse_ && gt_ != NULL) {
      dbIterator_->Seek(*gt_);

      if (dbIterator_->Valid() && dbIterator_->key().compare(*gt_) == 0) {
        dbIterator_->Next();
      }
    } else if (reverse_ && lte_ != NULL) {
      dbIterator_->Seek(*lte_);

      if (!dbIterator_->Valid()) {
        dbIterator_->SeekToLast();
      } else if (dbIterator_->key().compare(*lte_) > 0) {
        dbIterator_->Prev();
      }
    } else if (reverse_ && lt_ != NULL) {
      dbIterator_->Seek(*lt_);

      if (!dbIterator_->Valid()) {
        dbIterator_->SeekToLast();
      } else if (dbIterator_->key().compare(*lt_) >= 0) {
        dbIterator_->Prev();
      }
    } else if (reverse_) {
      dbIterator_->SeekToLast();
    } else {
      dbIterator_->SeekToFirst();
    }
  }

  /**
   * Seek manually (during iteration).
   */
  void Seek (rocksdb::Slice& target) {
    didSeek_ = true;

    if (OutOfRange(target)) {
      return SeekToEnd();
    }

    dbIterator_->Seek(target);

    if (dbIterator_->Valid()) {
      int cmp = dbIterator_->key().compare(target);
      if (reverse_ ? cmp > 0 : cmp < 0) {
        Next();
      }
    } else {
      SeekToFirst();
      if (dbIterator_->Valid()) {
        int cmp = dbIterator_->key().compare(target);
        if (reverse_ ? cmp > 0 : cmp < 0) {
          SeekToEnd();
        }
      }
    }
  }

  void Close () {
    if (!hasClosed_) {
      hasClosed_ = true;
      delete dbIterator_;
      dbIterator_ = NULL;
      database_->ReleaseSnapshot(options_->snapshot);
    }
  }

  bool Valid () const {
    return dbIterator_->Valid() && !OutOfRange(dbIterator_->key());
  }

  bool Increment () {
    return limit_ < 0 || ++count_ <= limit_;
  }

  void Next () {
    if (reverse_) dbIterator_->Prev();
    else dbIterator_->Next();
  }

  void SeekToFirst () {
    if (reverse_) dbIterator_->SeekToLast();
    else dbIterator_->SeekToFirst();
  }

  void SeekToLast () {
    if (reverse_) dbIterator_->SeekToFirst();
    else dbIterator_->SeekToLast();
  }

  void SeekToEnd () {
    SeekToLast();
    Next();
  }

  rocksdb::Slice CurrentKey () const {
    return dbIterator_->key();
  }

  rocksdb::Slice CurrentValue () const {
    return dbIterator_->value();
  }

  rocksdb::Status Status () const {
    return dbIterator_->status();
  }

  bool OutOfRange (const rocksdb::Slice& target) const {
    // TODO: benchmark to see if this is worth it
    // if (upperBoundOnly && !reverse_) {
    //   return ((lt_  != NULL && target.compare(*lt_) >= 0) ||
    //           (lte_ != NULL && target.compare(*lte_) > 0));
    // }

    // The lte and gte options take precedence over lt and gt respectively
    if (lte_ != NULL) {
      if (target.compare(*lte_) > 0) return true;
    } else if (lt_ != NULL) {
      if (target.compare(*lt_) >= 0) return true;
    }

    if (gte_ != NULL) {
      if (target.compare(*gte_) < 0) return true;
    } else if (gt_ != NULL) {
      if (target.compare(*gt_) <= 0) return true;
    }

    return false;
  }

  Database* database_;
  bool hasClosed_;

private:
  rocksdb::Iterator* dbIterator_;
  bool didSeek_;
  const bool reverse_;
  std::string* lt_;
  std::string* lte_;
  std::string* gt_;
  std::string* gte_;
  const int limit_;
  int count_;
  rocksdb::ReadOptions* options_;
};

/**
 * Extends BaseIterator for reading it from JS land.
 */
struct Iterator final : public BaseIterator {
  Iterator (Database* database,
            const uint32_t id,
            const bool reverse,
            const bool keys,
            const bool values,
            const int limit,
            std::string* lt,
            std::string* lte,
            std::string* gt,
            std::string* gte,
            const bool fillCache,
            const bool keyAsBuffer,
            const bool valueAsBuffer,
            const uint32_t highWaterMarkBytes)
    : BaseIterator(database, reverse, lt, lte, gt, gte, limit, fillCache),
      id_(id),
      keys_(keys),
      values_(values),
      keyAsBuffer_(keyAsBuffer),
      valueAsBuffer_(valueAsBuffer),
      highWaterMarkBytes_(highWaterMarkBytes),
      first_(true),
      nexting_(false),
      isClosing_(false),
      closeWorker_(NULL),
      ref_(NULL) {
  }

  ~Iterator () {}

  void Attach (napi_env env, napi_value context) {
    napi_create_reference(env, context, 1, &ref_);
    database_->AttachIterator(env, id_, this);
  }

  void Detach (napi_env env) {
    database_->DetachIterator(env, id_);
    if (ref_ != NULL) napi_delete_reference(env, ref_);
  }

  bool ReadMany (uint32_t size) {
    cache_.clear();
    cache_.reserve(size);
    size_t bytesRead = 0;
    rocksdb::Slice empty;

    while (true) {
      if (!first_) Next();
      else first_ = false;

      if (!Valid() || !Increment()) break;

      if (keys_ && values_) {
        rocksdb::Slice k = CurrentKey();
        rocksdb::Slice v = CurrentValue();
        cache_.emplace_back(&k, &v);
        bytesRead += k.size() + v.size();
      } else if (keys_) {
        rocksdb::Slice k = CurrentKey();
        cache_.emplace_back(&k, &empty);
      } else if (values_) {
        rocksdb::Slice v = CurrentValue();
        cache_.emplace_back(&empty, &v);
        bytesRead += v.size();
      }

      if (bytesRead > highWaterMarkBytes_ || cache_.size() >= size) {
        return true;
      }
    }

    return false;
  }

  const uint32_t id_;
  const bool keys_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  const uint32_t highWaterMarkBytes_;
  bool first_;
  bool nexting_;
  bool isClosing_;
  BaseWorker* closeWorker_;
  std::vector<Entry> cache_;

private:
  napi_ref ref_;
};

/**
 * Transaction to be used from JS land.
 */
struct Transaction final {
  Transaction(Database* database,
              const uint32_t id,
              const bool sync)
    : database_(database),
      id_(id),
      isCommitting_(false),
      hasCommitted_(false),
      isRollbacking_(false),
      hasRollbacked_(false),
      ref_(NULL) {
    options_ = new rocksdb::WriteOptions();
    options_->sync = sync;
    dbTransaction_ = database->NewTransaction(options_);
  }

  ~Transaction () {
    assert(hasCommitted_ || hasRollbacked_);
    delete options_;
  }

  /**
   * Creates reference to `napi_external` of this `Transaction`
   * to prevent garbage collection
   * Tracks this `Transaction` in the `Database` for cleanup
   */
  void Attach (napi_env env, napi_value tran_ref) {
    napi_create_reference(env, tran_ref, 1, &ref_);
    database_->AttachTransaction(env, id_, this);
  }

  /**
   * Deletes references to `napi_external` of this `Transaction`
   * to allow garbage collection
   * Untracks this `Transaction` in the `Database` for cleanup
   */
  void Detach (napi_env env) {
    database_->DetachTransaction(env, id_);
    if (ref_ != NULL) napi_delete_reference(env, ref_);
  }

  /**
   * Commit the transaction
   * Synchronous operation
   */
  rocksdb::Status Commit () {
    if (hasCommitted_) {
      return rocksdb::Status::OK();
    }
    hasCommitted_ = true;
    rocksdb::Status status = dbTransaction_->Commit();
    delete dbTransaction_;
    dbTransaction_ = NULL;
    // TODO: release snapshot?
    // database_->ReleaseSnapshot(options_->snapshot);
    return status;
  }

  /**
   * Rollback the transaction
   * Synchronous operation
   */
  rocksdb::Status Rollback () {
    if (hasRollbacked_) {
      return rocksdb::Status::OK();
    }
    hasRollbacked_ = true;
    rocksdb::Status status = dbTransaction_->Rollback();
    delete dbTransaction_;
    dbTransaction_ = NULL;
    // TODO: release snapshot?
    // database_->ReleaseSnapshot(options_->snapshot);
    return status;
  }

  Database* database_;
  const uint32_t id_;
  bool isCommitting_;
  bool hasCommitted_;
  bool isRollbacking_;
  bool hasRollbacked_;

private:
  rocksdb::Transaction* dbTransaction_;
  rocksdb::WriteOptions* options_;
  napi_ref ref_;
};

/**
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */
static void env_cleanup_hook (void* arg) {
  Database* database = (Database*)arg;

  // Do everything that db_close() does but synchronously. We're expecting that GC
  // did not (yet) collect the database because that would be a user mistake (not
  // closing their db) made during the lifetime of the environment. That's different
  // from an environment being torn down (like the main process or a worker thread)
  // where it's our responsibility to clean up. Note also, the following code must
  // be a safe noop if called before db_open() or after db_close().
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
 * Returns a context object for a database.
 */
NAPI_METHOD(db_init) {
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
 * Worker class for opening a database.
 * TODO: shouldn't this be a PriorityWorker?
 */
struct OpenWorker final : public BaseWorker {
  OpenWorker (napi_env env,
              Database* database,
              napi_value callback,
              const std::string& location,
              const bool createIfMissing,
              const bool errorIfExists,
              const bool compression,
              const uint32_t writeBufferSize,
              const uint32_t blockSize,
              const uint32_t maxOpenFiles,
              const uint32_t blockRestartInterval,
              const uint32_t maxFileSize,
              const uint32_t cacheSize,
              const std::string& infoLogLevel)
    : BaseWorker(env, database, callback, "rocksdb.db.open"),
      location_(location) {
    options_.create_if_missing = createIfMissing;
    options_.error_if_exists = errorIfExists;
    options_.compression = compression
      ? rocksdb::kSnappyCompression
      : rocksdb::kNoCompression;
    options_.write_buffer_size = writeBufferSize;
    options_.max_open_files = maxOpenFiles;
    options_.max_log_file_size = maxFileSize;
    options_.paranoid_checks = false;

    if (infoLogLevel.size() > 0) {
      rocksdb::InfoLogLevel lvl;

      if (infoLogLevel == "debug") lvl = rocksdb::InfoLogLevel::DEBUG_LEVEL;
      else if (infoLogLevel == "info") lvl = rocksdb::InfoLogLevel::INFO_LEVEL;
      else if (infoLogLevel == "warn") lvl = rocksdb::InfoLogLevel::WARN_LEVEL;
      else if (infoLogLevel == "error") lvl = rocksdb::InfoLogLevel::ERROR_LEVEL;
      else if (infoLogLevel == "fatal") lvl = rocksdb::InfoLogLevel::FATAL_LEVEL;
      else if (infoLogLevel == "header") lvl = rocksdb::InfoLogLevel::HEADER_LEVEL;
      else napi_throw_error(env, NULL, "invalid log level");

      options_.info_log_level = lvl;
    } else {
      // In some places RocksDB checks this option to see if it should prepare
      // debug information (ahead of logging), so set it to the highest level.
      options_.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
      options_.info_log.reset(new NullLogger());
    }

    rocksdb::BlockBasedTableOptions tableOptions;

    if (cacheSize) {
      tableOptions.block_cache = rocksdb::NewLRUCache(cacheSize);
    } else {
      tableOptions.no_block_cache = true;
    }

    tableOptions.block_size = blockSize;
    tableOptions.block_restart_interval = blockRestartInterval;
    tableOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

    options_.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(tableOptions)
    );
  }

  ~OpenWorker () {}

  void DoExecute () override {
    SetStatus(database_->Open(options_, location_.c_str()));
  }

  rocksdb::Options options_;
  std::string location_;
};

/**
 * Open a database.
 */
NAPI_METHOD(db_open) {
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
  OpenWorker* worker = new OpenWorker(env, database, callback, location,
                                      createIfMissing, errorIfExists,
                                      compression, writeBufferSize, blockSize,
                                      maxOpenFiles, blockRestartInterval,
                                      maxFileSize, cacheSize,
                                      infoLogLevel);
  worker->Queue(env);
  delete [] location;

  NAPI_RETURN_UNDEFINED();
}

/**
 * Worker class for closing a database
 */
struct CloseWorker final : public BaseWorker {
  CloseWorker (napi_env env,
               Database* database,
               napi_value callback)
    : BaseWorker(env, database, callback, "rocksdb.db.close") {}

  ~CloseWorker () {}

  void DoExecute () override {
    database_->CloseDatabase();
  }
};

napi_value noop_callback (napi_env env, napi_callback_info info) {
  return 0;
}

/**
 * Close a database.
 */
NAPI_METHOD(db_close) {
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
 * Worker class for putting key/value to the database
 */
struct PutWorker final : public PriorityWorker {
  PutWorker (napi_env env,
             Database* database,
             napi_value callback,
             rocksdb::Slice key,
             rocksdb::Slice value,
             bool sync)
    : PriorityWorker(env, database, callback, "rocksdb.db.put"),
      key_(key), value_(value) {
    options_.sync = sync;
  }

  ~PutWorker () {
    DisposeSliceBuffer(key_);
    DisposeSliceBuffer(value_);
  }

  void DoExecute () override {
    SetStatus(database_->Put(options_, key_, value_));
  }

  rocksdb::WriteOptions options_;
  rocksdb::Slice key_;
  rocksdb::Slice value_;
};

/**
 * Puts a key and a value to a database.
 */
NAPI_METHOD(db_put) {
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
 * Worker class for getting a value from a database.
 */
struct GetWorker final : public PriorityWorker {
  GetWorker (napi_env env,
             Database* database,
             napi_value callback,
             rocksdb::Slice key,
             const bool asBuffer,
             const bool fillCache)
    : PriorityWorker(env, database, callback, "rocksdb.db.get"),
      key_(key),
      asBuffer_(asBuffer) {
    options_.fill_cache = fillCache;
  }

  ~GetWorker () {
    DisposeSliceBuffer(key_);
  }

  void DoExecute () override {
    SetStatus(database_->Get(options_, key_, value_));
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    Entry::Convert(env, &value_, asBuffer_, &argv[1]);
    CallFunction(env, callback, 2, argv);
  }

private:
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
};

/**
 * Gets a value from a database.
 */
NAPI_METHOD(db_get) {
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
 * Worker class for getting many values.
 */
struct GetManyWorker final : public PriorityWorker {
  GetManyWorker (napi_env env,
                 Database* database,
                 const std::vector<std::string>* keys,
                 napi_value callback,
                 const bool valueAsBuffer,
                 const bool fillCache)
    : PriorityWorker(env, database, callback, "rocksdb.get.many"),
      keys_(keys), valueAsBuffer_(valueAsBuffer) {
      options_.fill_cache = fillCache;
      options_.snapshot = database->NewSnapshot();
    }

  ~GetManyWorker() {
    delete keys_;
  }

  void DoExecute () override {
    cache_.reserve(keys_->size());

    for (const std::string& key: *keys_) {
      std::string* value = new std::string();
      rocksdb::Status status = database_->Get(options_, key, *value);

      if (status.ok()) {
        cache_.push_back(value);
      } else if (status.IsNotFound()) {
        delete value;
        cache_.push_back(NULL);
      } else {
        delete value;
        for (const std::string* value: cache_) {
          if (value != NULL) delete value;
        }
        SetStatus(status);
        break;
      }
    }

    database_->ReleaseSnapshot(options_.snapshot);
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    size_t size = cache_.size();
    napi_value array;
    napi_create_array_with_length(env, size, &array);

    for (size_t idx = 0; idx < size; idx++) {
      std::string* value = cache_[idx];
      napi_value element;
      Entry::Convert(env, value, valueAsBuffer_, &element);
      napi_set_element(env, array, static_cast<uint32_t>(idx), element);
      if (value != NULL) delete value;
    }

    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    argv[1] = array;
    CallFunction(env, callback, 2, argv);
  }

private:
  rocksdb::ReadOptions options_;
  const std::vector<std::string>* keys_;
  const bool valueAsBuffer_;
  std::vector<std::string*> cache_;
};

/**
 * Gets many values from a database.
 */
NAPI_METHOD(db_get_many) {
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
 * Worker class for deleting a value from a database.
 */
struct DelWorker final : public PriorityWorker {
  DelWorker (napi_env env,
             Database* database,
             napi_value callback,
             rocksdb::Slice key,
             bool sync)
    : PriorityWorker(env, database, callback, "rocksdb.db.del"),
      key_(key) {
    options_.sync = sync;
  }

  ~DelWorker () {
    DisposeSliceBuffer(key_);
  }

  void DoExecute () override {
    SetStatus(database_->Del(options_, key_));
  }

  rocksdb::WriteOptions options_;
  rocksdb::Slice key_;
};

/**
 * Delete a value from a database.
 */
NAPI_METHOD(db_del) {
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
 * Worker class for deleting a range from a database.
 */
struct ClearWorker final : public PriorityWorker {
  ClearWorker (napi_env env,
               Database* database,
               napi_value callback,
               const bool reverse,
               const int limit,
               std::string* lt,
               std::string* lte,
               std::string* gt,
               std::string* gte)
    : PriorityWorker(env, database, callback, "rocksdb.db.clear") {
    iterator_ = new BaseIterator(database, reverse, lt, lte, gt, gte, limit, false);
    writeOptions_ = new rocksdb::WriteOptions();
    writeOptions_->sync = false;
  }

  ~ClearWorker () {
    delete iterator_;
    delete writeOptions_;
  }

  void DoExecute () override {
    iterator_->SeekToRange();

    // TODO: add option
    uint32_t hwm = 16 * 1024;
    rocksdb::WriteBatch batch;

    while (true) {
      size_t bytesRead = 0;

      while (bytesRead <= hwm && iterator_->Valid() && iterator_->Increment()) {
        rocksdb::Slice key = iterator_->CurrentKey();
        batch.Delete(key);
        bytesRead += key.size();
        iterator_->Next();
      }

      if (!SetStatus(iterator_->Status()) || bytesRead == 0) {
        break;
      }

      if (!SetStatus(database_->WriteBatch(*writeOptions_, &batch))) {
        break;
      }

      batch.Clear();
    }

    iterator_->Close();
  }

private:
  BaseIterator* iterator_;
  rocksdb::WriteOptions* writeOptions_;
};

/**
 * Delete a range from a database.
 */
NAPI_METHOD(db_clear) {
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
 * Worker class for calculating the size of a range.
 */
struct ApproximateSizeWorker final : public PriorityWorker {
  ApproximateSizeWorker (napi_env env,
                         Database* database,
                         napi_value callback,
                         rocksdb::Slice start,
                         rocksdb::Slice end)
    : PriorityWorker(env, database, callback, "rocksdb.db.approximate_size"),
      start_(start), end_(end) {}

  ~ApproximateSizeWorker () {
    DisposeSliceBuffer(start_);
    DisposeSliceBuffer(end_);
  }

  void DoExecute () override {
    rocksdb::Range range(start_, end_);
    size_ = database_->ApproximateSize(&range);
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    napi_value argv[2];
    napi_get_null(env, &argv[0]);
    napi_create_int64(env, (uint64_t)size_, &argv[1]);
    CallFunction(env, callback, 2, argv);
  }

  rocksdb::Slice start_;
  rocksdb::Slice end_;
  uint64_t size_;
};

/**
 * Calculates the approximate size of a range in a database.
 */
NAPI_METHOD(db_approximate_size) {
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
 * Worker class for compacting a range in a database.
 */
struct CompactRangeWorker final : public PriorityWorker {
  CompactRangeWorker (napi_env env,
                      Database* database,
                      napi_value callback,
                      rocksdb::Slice start,
                      rocksdb::Slice end)
    : PriorityWorker(env, database, callback, "rocksdb.db.compact_range"),
      start_(start), end_(end) {}

  ~CompactRangeWorker () {
    DisposeSliceBuffer(start_);
    DisposeSliceBuffer(end_);
  }

  void DoExecute () override {
    database_->CompactRange(&start_, &end_);
  }

  rocksdb::Slice start_;
  rocksdb::Slice end_;
};

/**
 * Compacts a range in a database.
 */
NAPI_METHOD(db_compact_range) {
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
NAPI_METHOD(db_get_property) {
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
 * Worker class for destroying a database.
 */
struct DestroyWorker final : public BaseWorker {
  DestroyWorker (napi_env env,
                 const std::string& location,
                 napi_value callback)
    : BaseWorker(env, NULL, callback, "rocksdb.destroy_db"),
      location_(location) {}

  ~DestroyWorker () {}

  void DoExecute () override {
    rocksdb::Options options;

    // TODO: support overriding infoLogLevel the same as db.open(options)
    options.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    options.info_log.reset(new NullLogger());

    SetStatus(rocksdb::DestroyDB(location_, options));
  }

  std::string location_;
};

/**
 * Destroys a database.
 */
NAPI_METHOD(destroy_db) {
  NAPI_ARGV(2);
  NAPI_ARGV_UTF8_NEW(location, 0);
  napi_value callback = argv[1];

  DestroyWorker* worker = new DestroyWorker(env, location, callback);
  worker->Queue(env);

  delete [] location;

  NAPI_RETURN_UNDEFINED();
}

/**
 * Worker class for repairing a database.
 */
struct RepairWorker final : public BaseWorker {
  RepairWorker (napi_env env,
                const std::string& location,
                napi_value callback)
    : BaseWorker(env, NULL, callback, "rocksdb.repair_db"),
      location_(location) {}

  ~RepairWorker () {}

  void DoExecute () override {
    rocksdb::Options options;

    // TODO: support overriding infoLogLevel the same as db.open(options)
    options.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
    options.info_log.reset(new NullLogger());

    SetStatus(rocksdb::RepairDB(location_, options));
  }

  std::string location_;
};

/**
 * Repairs a database.
 */
NAPI_METHOD(repair_db) {
  NAPI_ARGV(2);
  NAPI_ARGV_UTF8_NEW(location, 0);
  napi_value callback = argv[1];

  RepairWorker* worker = new RepairWorker(env, location, callback);
  worker->Queue(env);

  delete [] location;

  NAPI_RETURN_UNDEFINED();
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
 * Create an iterator.
 */
NAPI_METHOD(iterator_init) {
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
NAPI_METHOD(iterator_seek) {
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
 * Worker class for closing an iterator
 */
struct CloseIteratorWorker final : public BaseWorker {
  CloseIteratorWorker (napi_env env,
             Iterator* iterator,
             napi_value callback)
    : BaseWorker(env, iterator->database_, callback, "rocksdb.iterator.close"),
      iterator_(iterator) {}

  ~CloseIteratorWorker () {}

  void DoExecute () override {
    iterator_->Close();
  }

  void DoFinally (napi_env env) override {
    iterator_->Detach(env);
    BaseWorker::DoFinally(env);
  }

private:
  Iterator* iterator_;
};

/**
 * Called by NAPI_METHOD(iterator_close) and also when closing
 * open iterators during NAPI_METHOD(db_close).
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
 * CLoses an iterator.
 */
NAPI_METHOD(iterator_close) {
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
 * Worker class for nexting an iterator.
 */
struct NextWorker final : public BaseWorker {
  NextWorker (napi_env env,
              Iterator* iterator,
              uint32_t size,
              napi_value callback)
    : BaseWorker(env, iterator->database_, callback,
                 "rocksdb.iterator.next"),
      iterator_(iterator), size_(size), ok_() {}

  ~NextWorker () {}

  void DoExecute () override {
    if (!iterator_->DidSeek()) {
      iterator_->SeekToRange();
    }

    ok_ = iterator_->ReadMany(size_);

    if (!ok_) {
      SetStatus(iterator_->Status());
    }
  }

  void HandleOKCallback (napi_env env, napi_value callback) override {
    size_t size = iterator_->cache_.size();
    napi_value jsArray;
    napi_create_array_with_length(env, size, &jsArray);

    const bool kab = iterator_->keyAsBuffer_;
    const bool vab = iterator_->valueAsBuffer_;

    for (uint32_t idx = 0; idx < size; idx++) {
      napi_value element;
      iterator_->cache_[idx].ConvertByMode(env, Mode::entries, kab, vab, &element);
      napi_set_element(env, jsArray, idx, element);
    }

    napi_value argv[3];
    napi_get_null(env, &argv[0]);
    argv[1] = jsArray;
    napi_get_boolean(env, !ok_, &argv[2]);
    CallFunction(env, callback, 3, argv);
  }

  void DoFinally (napi_env env) override {
    // clean up & handle the next/close state
    iterator_->nexting_ = false;

    if (iterator_->closeWorker_ != NULL) {
      iterator_->closeWorker_->Queue(env);
      iterator_->closeWorker_ = NULL;
    }

    BaseWorker::DoFinally(env);
  }

private:
  Iterator* iterator_;
  uint32_t size_;
  bool ok_;
};

/**
 * Advance repeatedly and get multiple entries at once.
 */
NAPI_METHOD(iterator_nextv) {
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
 * Worker class for batch write operation.
 */
struct BatchWorker final : public PriorityWorker {
  BatchWorker (napi_env env,
               Database* database,
               napi_value callback,
               rocksdb::WriteBatch* batch,
               const bool sync,
               const bool hasData)
    : PriorityWorker(env, database, callback, "rocksdb.batch.do"),
      batch_(batch), hasData_(hasData) {
    options_.sync = sync;
  }

  ~BatchWorker () {
    delete batch_;
  }

  void DoExecute () override {
    if (hasData_) {
      SetStatus(database_->WriteBatch(options_, batch_));
    }
  }

private:
  rocksdb::WriteOptions options_;
  rocksdb::WriteBatch* batch_;
  const bool hasData_;
};

/**
 * Does a batch write operation on a database.
 */
NAPI_METHOD(batch_do) {
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
 * Owns a WriteBatch.
 */
struct Batch {
  Batch (Database* database)
    : database_(database),
      batch_(new rocksdb::WriteBatch()),
      hasData_(false) {}

  ~Batch () {
    delete batch_;
  }

  void Put (rocksdb::Slice key, rocksdb::Slice value) {
    batch_->Put(key, value);
    hasData_ = true;
  }

  void Del (rocksdb::Slice key) {
    batch_->Delete(key);
    hasData_ = true;
  }

  void Clear () {
    batch_->Clear();
    hasData_ = false;
  }

  rocksdb::Status Write (bool sync) {
    rocksdb::WriteOptions options;
    options.sync = sync;
    return database_->WriteBatch(options, batch_);
  }

  Database* database_;
  rocksdb::WriteBatch* batch_;
  bool hasData_;
};

/**
 * Runs when a Batch is garbage collected.
 */
static void FinalizeBatch (napi_env env, void* data, void* hint) {
  if (data) {
    delete (Batch*)data;
  }
}

/**
 * Return a batch object.
 */
NAPI_METHOD(batch_init) {
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
NAPI_METHOD(batch_put) {
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
NAPI_METHOD(batch_del) {
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
NAPI_METHOD(batch_clear) {
  NAPI_ARGV(1);
  NAPI_BATCH_CONTEXT();

  batch->Clear();

  NAPI_RETURN_UNDEFINED();
}

/**
 * Worker class for batch write operation.
 */
struct BatchWriteWorker final : public PriorityWorker {
  BatchWriteWorker (napi_env env,
                    napi_value context,
                    Batch* batch,
                    napi_value callback,
                    const bool sync)
    : PriorityWorker(env, batch->database_, callback, "rocksdb.batch.write"),
      batch_(batch),
      sync_(sync) {
        // Prevent GC of batch object before we execute
        NAPI_STATUS_THROWS_VOID(napi_create_reference(env, context, 1, &contextRef_));
      }

  ~BatchWriteWorker () {}

  void DoExecute () override {
    if (batch_->hasData_) {
      SetStatus(batch_->Write(sync_));
    }
  }

  void DoFinally (napi_env env) override {
    napi_delete_reference(env, contextRef_);
    PriorityWorker::DoFinally(env);
  }

private:
  Batch* batch_;
  const bool sync_;
  napi_ref contextRef_;
};

/**
 * Writes a batch object.
 */
NAPI_METHOD(batch_write) {
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
 * Runs when a Transaction is garbage collected.
 */
static void FinalizeTransaction (napi_env env, void* data, void* hint) {
  if (data) {
    delete (Transaction*)data;
  }
}

/**
 * Creates a transaction
 *
 * @returns {napi_value} This is a `napi_external` that references `Transaction`
 */
NAPI_METHOD(transaction_init) {
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
 * Transaction commit worker
 * This runs in the background executing the commit
 */
struct CommitTransactionWorker final: public PriorityWorker {

  CommitTransactionWorker (
    napi_env env,
    Transaction* tran,
    napi_value callback
  ) : PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.commit"),
    tran_(tran)
    {}

  ~CommitTransactionWorker() {}

  void DoExecute () override {
    SetStatus(tran_->Commit());
  }

  void DoFinally (napi_env env) override {
    tran_->Detach(env);
    PriorityWorker::DoFinally(env);
  }

private:
  Transaction* tran_;
};

/**
 * Commit transaction
 * transaction_commit(transaction, callback)
 */
NAPI_METHOD(transaction_commit) {
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
  CommitTransactionWorker* worker = new CommitTransactionWorker(
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
 * Rollback commit worker
 * This runs in the background executing the rollback
 */
struct RollbackTransactionWorker final: public PriorityWorker {

  RollbackTransactionWorker (napi_env env,
                             Transaction* tran,
                             napi_value callback)
    : PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.rollback"),
    tran_(tran) {}

  ~RollbackTransactionWorker() {}

  void DoExecute () override {
    SetStatus(tran_->Rollback());
  }

  void DoFinally (napi_env env) override {
    tran_->Detach(env);
    PriorityWorker::DoFinally(env);
  }

private:
  Transaction* tran_;
};

/**
 * Called by NAPI_METHOD(transaction_rollback) and also when closing
 * open transactions during NAPI_METHOD(db_close)
 */
static void transaction_rollback_do (
  napi_env env,
  Transaction* transaction,
  napi_value cb
) {
  RollbackTransactionWorker* worker = new RollbackTransactionWorker(
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
 * Rollback transaction
 */
NAPI_METHOD(transaction_rollback) {
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
 * All exported functions.
 */
NAPI_INIT() {
  NAPI_EXPORT_FUNCTION(db_init);
  NAPI_EXPORT_FUNCTION(db_open);
  NAPI_EXPORT_FUNCTION(db_close);
  NAPI_EXPORT_FUNCTION(db_put);
  NAPI_EXPORT_FUNCTION(db_get);
  NAPI_EXPORT_FUNCTION(db_get_many);
  NAPI_EXPORT_FUNCTION(db_del);
  NAPI_EXPORT_FUNCTION(db_clear);
  NAPI_EXPORT_FUNCTION(db_approximate_size);
  NAPI_EXPORT_FUNCTION(db_compact_range);
  NAPI_EXPORT_FUNCTION(db_get_property);

  NAPI_EXPORT_FUNCTION(destroy_db);
  NAPI_EXPORT_FUNCTION(repair_db);

  NAPI_EXPORT_FUNCTION(iterator_init);
  NAPI_EXPORT_FUNCTION(iterator_seek);
  NAPI_EXPORT_FUNCTION(iterator_close);
  NAPI_EXPORT_FUNCTION(iterator_nextv);

  NAPI_EXPORT_FUNCTION(batch_do);
  NAPI_EXPORT_FUNCTION(batch_init);
  NAPI_EXPORT_FUNCTION(batch_put);
  NAPI_EXPORT_FUNCTION(batch_del);
  NAPI_EXPORT_FUNCTION(batch_clear);
  NAPI_EXPORT_FUNCTION(batch_write);

  NAPI_EXPORT_FUNCTION(transaction_init);
  NAPI_EXPORT_FUNCTION(transaction_commit);
  NAPI_EXPORT_FUNCTION(transaction_rollback);
}