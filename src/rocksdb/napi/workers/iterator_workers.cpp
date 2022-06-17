#define NAPI_VERSION 3

#include "iterator_workers.h"

#include <cstddef>
#include <cstdint>
#include <cassert>

#include <node/node_api.h>

#include "../worker.h"
#include "../iterator.h"
#include "../utils.h"

CloseIteratorWorker::CloseIteratorWorker(napi_env env, Iterator* iterator,
                                         napi_value callback)
    : BaseWorker(env, iterator->database_, callback, "rocksdb.iterator.close"),
      iterator_(iterator) {}

CloseIteratorWorker::~CloseIteratorWorker() {}

void CloseIteratorWorker::DoExecute() { iterator_->Close(); }

void CloseIteratorWorker::DoFinally(napi_env env) {
  iterator_->Detach(env);
  BaseWorker::DoFinally(env);
}

NextWorker::NextWorker(napi_env env, Iterator* iterator, uint32_t size,
                       napi_value callback)
    : BaseWorker(env, iterator->database_, callback, "rocksdb.iterator.next"),
      iterator_(iterator),
      size_(size),
      ok_() {}

NextWorker::~NextWorker() {}

void NextWorker::DoExecute() {
  if (!iterator_->DidSeek()) {
    iterator_->SeekToRange();
  }

  ok_ = iterator_->ReadMany(size_);

  if (!ok_) {
    SetStatus(iterator_->Status());
  }
}

void NextWorker::HandleOKCallback(napi_env env, napi_value callback) {
  size_t size = iterator_->cache_.size();
  napi_value jsArray;
  napi_create_array_with_length(env, size, &jsArray);

  const bool kab = iterator_->keyAsBuffer_;
  const bool vab = iterator_->valueAsBuffer_;

  for (uint32_t idx = 0; idx < size; idx++) {
    napi_value element;
    iterator_->cache_[idx].ConvertByMode(env, Mode::entries, kab, vab,
                                         &element);
    napi_set_element(env, jsArray, idx, element);
  }

  napi_value argv[3];
  napi_get_null(env, &argv[0]);
  argv[1] = jsArray;
  napi_get_boolean(env, !ok_, &argv[2]);
  CallFunction(env, callback, 3, argv);
}

void NextWorker::DoFinally(napi_env env) {
  // clean up & handle the next/close state
  iterator_->nexting_ = false;

  if (iterator_->closeWorker_ != NULL) {
    iterator_->closeWorker_->Queue(env);
    iterator_->closeWorker_ = NULL;
  }

  BaseWorker::DoFinally(env);
}

ClearWorker::ClearWorker(napi_env env, Database* database, napi_value callback,
                         const bool reverse, const int limit, std::string* lt,
                         std::string* lte, std::string* gt, std::string* gte,
                         const bool sync, const Snapshot* snapshot)
    : PriorityWorker(env, database, callback, "rocksdb.db.clear") {
  iterator_ = new BaseIterator(database, reverse, lt, lte, gt, gte, limit,
                               false, snapshot);
  writeOptions_ = new rocksdb::WriteOptions();
  writeOptions_->sync = sync;
}

ClearWorker::ClearWorker(napi_env env, Transaction* transaction,
                         napi_value callback, const bool reverse,
                         const int limit, std::string* lt, std::string* lte,
                         std::string* gt, std::string* gte,
                         const TransactionSnapshot* snapshot)
    : PriorityWorker(env, transaction, callback, "rocksdb.db.clear") {
  iterator_ = new BaseIterator(transaction, reverse, lt, lte, gt, gte, limit,
                               false, snapshot);
  writeOptions_ = nullptr;
}

ClearWorker::~ClearWorker() {
  delete iterator_;
  delete writeOptions_;
}

void ClearWorker::DoExecute() {
  assert(database_ != nullptr || transaction_ != nullptr);
  iterator_->SeekToRange();
  uint32_t hwm = 16 * 1024;
  if (database_ != nullptr) {
    rocksdb::WriteBatch batch;
    while (true) {
      size_t bytesRead = 0;
      while (bytesRead <= hwm && iterator_->Valid() && iterator_->Increment()) {
        rocksdb::Slice key = iterator_->CurrentKey();
        // If this fails, we return
        if (!SetStatus(batch.Delete(key))) return;
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
  } else if (transaction_ != nullptr) {
    while (true) {
      size_t bytesRead = 0;
      while (bytesRead <= hwm && iterator_->Valid() && iterator_->Increment()) {
        rocksdb::Slice key = iterator_->CurrentKey();
        // If this fails, we return
        if (!SetStatus(transaction_->Del(key))) return;
        bytesRead += key.size();
        iterator_->Next();
      }
      if (!SetStatus(iterator_->Status()) || bytesRead == 0) {
        break;
      }
    }
  }
  iterator_->Close();
}
