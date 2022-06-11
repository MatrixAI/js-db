#define NAPI_VERSION 3

#include "database_workers.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include <node/node_api.h>
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
#include "../iterator.h"
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

void CloseWorker::DoExecute() { database_->CloseDatabase(); }

GetWorker::GetWorker(napi_env env, Database* database, napi_value callback,
                     rocksdb::Slice key, const bool asBuffer,
                     const bool fillCache)
    : PriorityWorker(env, database, callback, "rocksdb.db.get"),
      key_(key),
      asBuffer_(asBuffer) {
  options_.fill_cache = fillCache;
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

GetManyWorker::GetManyWorker(napi_env env, Database* database,
                             const std::vector<std::string>* keys,
                             napi_value callback, const bool valueAsBuffer,
                             const bool fillCache)
    : PriorityWorker(env, database, callback, "rocksdb.get.many"),
      keys_(keys),
      valueAsBuffer_(valueAsBuffer) {
  options_.fill_cache = fillCache;
  options_.snapshot = database->NewSnapshot();
}

GetManyWorker::~GetManyWorker() { delete keys_; }

void GetManyWorker::DoExecute() {
  cache_.reserve(keys_->size());

  for (const std::string& key : *keys_) {
    std::string* value = new std::string();
    rocksdb::Status status = database_->Get(options_, key, *value);

    if (status.ok()) {
      cache_.push_back(value);
    } else if (status.IsNotFound()) {
      delete value;
      cache_.push_back(NULL);
    } else {
      delete value;
      for (const std::string* value : cache_) {
        if (value != NULL) delete value;
      }
      SetStatus(status);
      break;
    }
  }

  database_->ReleaseSnapshot(options_.snapshot);
}

void GetManyWorker::HandleOKCallback(napi_env env, napi_value callback) {
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

ClearWorker::ClearWorker(napi_env env, Database* database, napi_value callback,
                         const bool reverse, const int limit, std::string* lt,
                         std::string* lte, std::string* gt, std::string* gte)
    : PriorityWorker(env, database, callback, "rocksdb.db.clear") {
  iterator_ =
      new BaseIterator(database, reverse, lt, lte, gt, gte, limit, false);
  writeOptions_ = new rocksdb::WriteOptions();
  writeOptions_->sync = false;
}

ClearWorker::~ClearWorker() {
  delete iterator_;
  delete writeOptions_;
}

void ClearWorker::DoExecute() {
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
    : BaseWorker(env, NULL, callback, "rocksdb.destroyDb"),
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
    : BaseWorker(env, NULL, callback, "rocksdb.repairDb"),
      location_(location) {}

RepairWorker::~RepairWorker() {}

void RepairWorker::DoExecute() {
  rocksdb::Options options;

  // TODO: support overriding infoLogLevel the same as db.open(options)
  options.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
  options.info_log.reset(new NullLogger());

  SetStatus(rocksdb::RepairDB(location_, options));
}
