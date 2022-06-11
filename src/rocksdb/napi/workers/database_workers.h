#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>
#include <string>

#include <node/node_api.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include "../worker.h"
#include "../iterator.h"
#include "../database.h"

/**
 * Worker class for opening a database.
 * TODO: shouldn't this be a PriorityWorker?
 */
struct OpenWorker final : public BaseWorker {
  OpenWorker(napi_env env, Database* database, napi_value callback,
             const std::string& location, const bool createIfMissing,
             const bool errorIfExists, const bool compression,
             const uint32_t writeBufferSize, const uint32_t blockSize,
             const uint32_t maxOpenFiles, const uint32_t blockRestartInterval,
             const uint32_t maxFileSize, const uint32_t cacheSize,
             const rocksdb::InfoLogLevel log_level, rocksdb::Logger* logger);

  ~OpenWorker();

  void DoExecute() override;

  rocksdb::Options options_;
  std::string location_;
};

/**
 * Worker class for closing a database
 */
struct CloseWorker final : public BaseWorker {
  CloseWorker(napi_env env, Database* database, napi_value callback);

  ~CloseWorker();

  void DoExecute() override;
};

/**
 * Worker class for getting a value from a database.
 */
struct GetWorker final : public PriorityWorker {
  GetWorker(napi_env env, Database* database, napi_value callback,
            rocksdb::Slice key, const bool asBuffer, const bool fillCache);

  ~GetWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
};

/**
 * Worker class for getting many values.
 */
struct GetManyWorker final : public PriorityWorker {
  GetManyWorker(napi_env env, Database* database,
                const std::vector<std::string>* keys, napi_value callback,
                const bool valueAsBuffer, const bool fillCache);

  ~GetManyWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  const std::vector<std::string>* keys_;
  const bool valueAsBuffer_;
  std::vector<std::string*> cache_;
};

/**
 * Worker class for putting key/value to the database
 */
struct PutWorker final : public PriorityWorker {
  PutWorker(napi_env env, Database* database, napi_value callback,
            rocksdb::Slice key, rocksdb::Slice value, bool sync);

  ~PutWorker();

  void DoExecute() override;

  rocksdb::WriteOptions options_;
  rocksdb::Slice key_;
  rocksdb::Slice value_;
};

/**
 * Worker class for deleting a value from a database.
 */
struct DelWorker final : public PriorityWorker {
  DelWorker(napi_env env, Database* database, napi_value callback,
            rocksdb::Slice key, bool sync);

  ~DelWorker();

  void DoExecute() override;

  rocksdb::WriteOptions options_;
  rocksdb::Slice key_;
};

/**
 * Worker class for deleting a range from a database.
 */
struct ClearWorker final : public PriorityWorker {
  ClearWorker(napi_env env, Database* database, napi_value callback,
              const bool reverse, const int limit, std::string* lt,
              std::string* lte, std::string* gt, std::string* gte);

  ~ClearWorker();

  void DoExecute() override;

 private:
  BaseIterator* iterator_;
  rocksdb::WriteOptions* writeOptions_;
};

/**
 * Worker class for calculating the size of a range.
 */
struct ApproximateSizeWorker final : public PriorityWorker {
  ApproximateSizeWorker(napi_env env, Database* database, napi_value callback,
                        rocksdb::Slice start, rocksdb::Slice end);

  ~ApproximateSizeWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

  rocksdb::Slice start_;
  rocksdb::Slice end_;
  uint64_t size_;
};

/**
 * Worker class for compacting a range in a database.
 */
struct CompactRangeWorker final : public PriorityWorker {
  CompactRangeWorker(napi_env env, Database* database, napi_value callback,
                     rocksdb::Slice start, rocksdb::Slice end);

  ~CompactRangeWorker();

  void DoExecute() override;

  rocksdb::Slice start_;
  rocksdb::Slice end_;
};

/**
 * Worker class for destroying a database.
 */
struct DestroyWorker final : public BaseWorker {
  DestroyWorker(napi_env env, const std::string& location, napi_value callback);

  ~DestroyWorker();

  void DoExecute() override;

  std::string location_;
};

/**
 * Worker class for repairing a database.
 */
struct RepairWorker final : public BaseWorker {
  RepairWorker(napi_env env, const std::string& location, napi_value callback);

  ~RepairWorker();

  void DoExecute() override;

  std::string location_;
};
