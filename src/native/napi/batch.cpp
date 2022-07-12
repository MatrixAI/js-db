#define NAPI_VERSION 3

#include "batch.h"

#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

#include "debug.h"
#include "database.h"

Batch::Batch(Database* database)
    : database_(database), batch_(new rocksdb::WriteBatch()), hasData_(false) {
  LOG_DEBUG("Batch:Constructing Batch\n");
  LOG_DEBUG("Batch:Constructed Batch\n");
}

Batch::~Batch() {
  LOG_DEBUG("Batch:Destroying Batch\n");
  delete batch_;
  LOG_DEBUG("Batch:Destroyed Batch\n");
}

void Batch::Put(rocksdb::Slice key, rocksdb::Slice value) {
  batch_->Put(key, value);
  hasData_ = true;
}

void Batch::Del(rocksdb::Slice key) {
  batch_->Delete(key);
  hasData_ = true;
}

void Batch::Clear() {
  batch_->Clear();
  hasData_ = false;
}

rocksdb::Status Batch::Write(bool sync) {
  rocksdb::WriteOptions options;
  options.sync = sync;
  return database_->WriteBatch(options, batch_);
}
