#define NAPI_VERSION 3

#include "database_workers.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include <node_api.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/cache.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/filter_policy.h>

#include "../worker.h"
#include "../database.h"
#include "../snapshot.h"
#include "../utils.h"

OpenWorker::OpenWorker(napi_env env, Database* database, napi_value callback,
                       const std::string& location, const bool createIfMissing,
                       const bool errorIfExists, const bool compression,
                       const uint32_t writeBufferSize, const uint32_t blockSize,
                       const uint32_t maxOpenFiles,
                       const uint32_t blockRestartInterval,
                       const uint32_t maxFileSize, const uint32_t cacheSize,
                       const rocksdb::InfoLogLevel log_level,
                       rocksdb::Logger* logger)
    : BaseWorker(env, database, callback, "rocksdb.db.open"),
      location_(location) {
  options_.create_if_missing = createIfMissing;
  options_.error_if_exists = errorIfExists;
  options_.compression =
      compression ? rocksdb::kSnappyCompression : rocksdb::kNoCompression;
  options_.write_buffer_size = writeBufferSize;
  options_.max_open_files = maxOpenFiles;
  options_.max_log_file_size = maxFileSize;
  options_.paranoid_checks = false;
  options_.info_log_level = log_level;
  if (logger) {
    options_.info_log.reset(logger);
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
      rocksdb::NewBlockBasedTableFactory(tableOptions));
}

OpenWorker::~OpenWorker() {}

void OpenWorker::DoExecute() {
  SetStatus(database_->Open(options_, location_.c_str()));
}

CloseWorker::CloseWorker(napi_env env, Database* database, napi_value callback)
    : BaseWorker(env, database, callback, "rocksdb.db.close") {}

CloseWorker::~CloseWorker() {}

void CloseWorker::DoExecute() { database_->Close(); }

void CloseWorker::DoFinally(napi_env env) {
  database_->Detach(env);
  BaseWorker::DoFinally(env);
}

GetWorker::GetWorker(napi_env env, Database* database, napi_value callback,
                     rocksdb::Slice key, const bool asBuffer,
                     const bool fillCache, const Snapshot* snapshot)
    : PriorityWorker(env, database, callback, "rocksdb.db.get"),
      key_(key),
      asBuffer_(asBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot) options_.snapshot = snapshot->snapshot();
}

GetWorker::~GetWorker() { DisposeSliceBuffer(key_); }

void GetWorker::DoExecute() {
  SetStatus(database_->Get(options_, key_, value_));
}

void GetWorker::HandleOKCallback(napi_env env, napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  Entry::Convert(env, &value_, asBuffer_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

MultiGetWorker::MultiGetWorker(napi_env env, Database* database,
                               const std::vector<rocksdb::Slice>* keys,
                               napi_value callback, const bool valueAsBuffer,
                               const bool fillCache, const Snapshot* snapshot)
    : PriorityWorker(env, database, callback, "rocksdb.db.multiget"),
      keys_(keys),
      valueAsBuffer_(valueAsBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot) options_.snapshot = snapshot->snapshot();
}

MultiGetWorker::~MultiGetWorker() { delete keys_; }

void MultiGetWorker::DoExecute() {
  // NAPI requires a vector of string pointers
  // the nullptr can be used to represent `undefined`
  values_.reserve(keys_->size());
  // RocksDB requires just a vector of strings
  // these will be automatically deallocated
  std::vector<std::string> values(keys_->size());
  std::vector<rocksdb::Status> statuses =
      database_->MultiGet(options_, *keys_, values);
  for (size_t i = 0; i != statuses.size(); i++) {
    if (statuses[i].ok()) {
      std::string* value = new std::string(values[i]);
      values_.push_back(value);
    } else if (statuses[i].IsNotFound()) {
      values_.push_back(nullptr);
    } else {
      for (const std::string* value : values_) {
        if (value != NULL) delete value;
      }
      SetStatus(statuses[i]);
      break;
    }
  }
}

void MultiGetWorker::HandleOKCallback(napi_env env, napi_value callback) {
  size_t size = values_.size();
  napi_value array;
  napi_create_array_with_length(env, size, &array);

  for (size_t idx = 0; idx < size; idx++) {
    std::string* value = values_[idx];
    napi_value element;
    Entry::Convert(env, value, valueAsBuffer_, &element);
    napi_set_element(env, array, static_cast<uint32_t>(idx), element);
    if (value != nullptr) delete value;
  }

  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  argv[1] = array;
  CallFunction(env, callback, 2, argv);
}

PutWorker::PutWorker(napi_env env, Database* database, napi_value callback,
                     rocksdb::Slice key, rocksdb::Slice value, bool sync)
    : PriorityWorker(env, database, callback, "rocksdb.db.put"),
      key_(key),
      value_(value) {
  options_.sync = sync;
}

PutWorker::~PutWorker() {
  DisposeSliceBuffer(key_);
  DisposeSliceBuffer(value_);
}

void PutWorker::DoExecute() {
  SetStatus(database_->Put(options_, key_, value_));
}

DelWorker::DelWorker(napi_env env, Database* database, napi_value callback,
                     rocksdb::Slice key, bool sync)
    : PriorityWorker(env, database, callback, "rocksdb.db.del"), key_(key) {
  options_.sync = sync;
}

DelWorker::~DelWorker() { DisposeSliceBuffer(key_); }

void DelWorker::DoExecute() { SetStatus(database_->Del(options_, key_)); }

ApproximateSizeWorker::ApproximateSizeWorker(napi_env env, Database* database,
                                             napi_value callback,
                                             rocksdb::Slice start,
                                             rocksdb::Slice end)
    : PriorityWorker(env, database, callback, "rocksdb.db.approximate_size"),
      start_(start),
      end_(end) {}

ApproximateSizeWorker::~ApproximateSizeWorker() {
  DisposeSliceBuffer(start_);
  DisposeSliceBuffer(end_);
}

void ApproximateSizeWorker::DoExecute() {
  rocksdb::Range range(start_, end_);
  size_ = database_->ApproximateSize(&range);
}

void ApproximateSizeWorker::HandleOKCallback(napi_env env,
                                             napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  napi_create_int64(env, (uint64_t)size_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

CompactRangeWorker::CompactRangeWorker(napi_env env, Database* database,
                                       napi_value callback,
                                       rocksdb::Slice start, rocksdb::Slice end)
    : PriorityWorker(env, database, callback, "rocksdb.db.compact_range"),
      start_(start),
      end_(end) {}

CompactRangeWorker::~CompactRangeWorker() {
  DisposeSliceBuffer(start_);
  DisposeSliceBuffer(end_);
}

void CompactRangeWorker::DoExecute() {
  database_->CompactRange(&start_, &end_);
}

DestroyWorker::DestroyWorker(napi_env env, const std::string& location,
                             napi_value callback)
    : BaseWorker(env, (Database*)nullptr, callback, "rocksdb.destroyDb"),
      location_(location) {}

DestroyWorker::~DestroyWorker() {}

void DestroyWorker::DoExecute() {
  rocksdb::Options options;

  // TODO: support overriding infoLogLevel the same as db.open(options)
  options.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
  options.info_log.reset(new NullLogger());

  SetStatus(rocksdb::DestroyDB(location_, options));
}

RepairWorker::RepairWorker(napi_env env, const std::string& location,
                           napi_value callback)
    : BaseWorker(env, (Database*)nullptr, callback, "rocksdb.repairDb"),
      location_(location) {}

RepairWorker::~RepairWorker() {}

void RepairWorker::DoExecute() {
  rocksdb::Options options;

  // TODO: support overriding infoLogLevel the same as db.open(options)
  options.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
  options.info_log.reset(new NullLogger());

  SetStatus(rocksdb::RepairDB(location_, options));
}
