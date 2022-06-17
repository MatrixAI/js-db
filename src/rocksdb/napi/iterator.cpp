#define NAPI_VERSION 3

#include "iterator.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include <napi-macros.h>
#include <node/node_api.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include "debug.h"
#include "database.h"
#include "transaction.h"
#include "snapshot.h"

Entry::Entry(const rocksdb::Slice* key, const rocksdb::Slice* value)
    : key_(key->data(), key->size()), value_(value->data(), value->size()) {}

void Entry::ConvertByMode(napi_env env, Mode mode, const bool keyAsBuffer,
                          const bool valueAsBuffer, napi_value* result) {
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

void Entry::Convert(napi_env env, const std::string* s, const bool asBuffer,
                    napi_value* result) {
  if (s == NULL) {
    napi_get_undefined(env, result);
  } else if (asBuffer) {
    napi_create_buffer_copy(env, s->size(), s->data(), NULL, result);
  } else {
    napi_create_string_utf8(env, s->data(), s->size(), result);
  }
}

BaseIterator::BaseIterator(Database* database, const bool reverse,
                           std::string* lt, std::string* lte, std::string* gt,
                           std::string* gte, const int limit,
                           const bool fillCache, const Snapshot* snapshot)
    : database_(database),
      transaction_(nullptr),
      hasClosed_(false),
      didSeek_(false),
      reverse_(reverse),
      lt_(lt),
      lte_(lte),
      gt_(gt),
      gte_(gte),
      limit_(limit),
      count_(0) {
  LOG_DEBUG("BaseIterator:Constructing BaseIterator from Database\n");
  options_ = new rocksdb::ReadOptions();
  options_->fill_cache = fillCache;
  options_->verify_checksums = false;
  if (snapshot != nullptr) options_->snapshot = snapshot->snapshot();
  iter_ = database->NewIterator(*options_);
  LOG_DEBUG("BaseIterator:Constructed BaseIterator from Database\n");
}

BaseIterator::BaseIterator(Transaction* transaction, const bool reverse,
                           std::string* lt, std::string* lte, std::string* gt,
                           std::string* gte, const int limit,
                           const bool fillCache,
                           const TransactionSnapshot* snapshot)
    : database_(nullptr),
      transaction_(transaction),
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
  if (snapshot != nullptr) options_->snapshot = snapshot->snapshot();
  iter_ = transaction->GetIterator(*options_);
}

BaseIterator::~BaseIterator() {
  assert(hasClosed_);
  delete iter_;
  delete options_;
  if (lt_ != nullptr) delete lt_;
  if (gt_ != nullptr) delete gt_;
  if (lte_ != nullptr) delete lte_;
  if (gte_ != nullptr) delete gte_;
}

void BaseIterator::Close() {
  if (hasClosed_) return;
  hasClosed_ = true;
  delete iter_;
  iter_ = nullptr;
  delete options_;
  options_ = nullptr;
  if (lt_ != nullptr) {
    delete lt_;
    lt_ = nullptr;
  }
  if (gt_ != nullptr) {
    delete gt_;
    gt_ = nullptr;
  }
  if (lte_ != nullptr) {
    delete lte_;
    lte_ = nullptr;
  }
  if (gte_ != nullptr) {
    delete gte_;
    gte_ = nullptr;
  }
}

bool BaseIterator::DidSeek() const { return didSeek_; }

void BaseIterator::SeekToRange() {
  assert(!hasClosed_);

  didSeek_ = true;

  if (!reverse_ && gte_ != NULL) {
    iter_->Seek(*gte_);
  } else if (!reverse_ && gt_ != NULL) {
    iter_->Seek(*gt_);

    if (iter_->Valid() && iter_->key().compare(*gt_) == 0) {
      iter_->Next();
    }
  } else if (reverse_ && lte_ != NULL) {
    iter_->Seek(*lte_);

    if (!iter_->Valid()) {
      iter_->SeekToLast();
    } else if (iter_->key().compare(*lte_) > 0) {
      iter_->Prev();
    }
  } else if (reverse_ && lt_ != NULL) {
    iter_->Seek(*lt_);

    if (!iter_->Valid()) {
      iter_->SeekToLast();
    } else if (iter_->key().compare(*lt_) >= 0) {
      iter_->Prev();
    }
  } else if (reverse_) {
    iter_->SeekToLast();
  } else {
    iter_->SeekToFirst();
  }
}

void BaseIterator::Seek(rocksdb::Slice& target) {
  assert(!hasClosed_);
  didSeek_ = true;
  if (OutOfRange(target)) {
    return SeekToEnd();
  }
  iter_->Seek(target);
  if (iter_->Valid()) {
    int cmp = iter_->key().compare(target);
    if (reverse_ ? cmp > 0 : cmp < 0) {
      Next();
    }
  } else {
    SeekToFirst();
    if (iter_->Valid()) {
      int cmp = iter_->key().compare(target);
      if (reverse_ ? cmp > 0 : cmp < 0) {
        SeekToEnd();
      }
    }
  }
}

bool BaseIterator::Valid() const {
  assert(!hasClosed_);
  return iter_->Valid() && !OutOfRange(iter_->key());
}

bool BaseIterator::Increment() {
  assert(!hasClosed_);
  return limit_ < 0 || ++count_ <= limit_;
}

void BaseIterator::Next() {
  assert(!hasClosed_);
  if (reverse_) {
    iter_->Prev();
  } else {
    iter_->Next();
  }
}

void BaseIterator::SeekToFirst() {
  assert(!hasClosed_);
  if (reverse_) {
    iter_->SeekToLast();
  } else {
    iter_->SeekToFirst();
  }
}

void BaseIterator::SeekToLast() {
  assert(!hasClosed_);
  if (reverse_) {
    iter_->SeekToFirst();
  } else {
    iter_->SeekToLast();
  }
}

void BaseIterator::SeekToEnd() {
  SeekToLast();
  Next();
}

rocksdb::Slice BaseIterator::CurrentKey() const { return iter_->key(); }

rocksdb::Slice BaseIterator::CurrentValue() const { return iter_->value(); }

rocksdb::Status BaseIterator::Status() const { return iter_->status(); }

bool BaseIterator::OutOfRange(const rocksdb::Slice& target) const {
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

Iterator::Iterator(Database* database, const uint32_t id, const bool reverse,
                   const bool keys, const bool values, const int limit,
                   std::string* lt, std::string* lte, std::string* gt,
                   std::string* gte, const bool fillCache,
                   const bool keyAsBuffer, const bool valueAsBuffer,
                   const uint32_t highWaterMarkBytes, const Snapshot* snapshot)
    : BaseIterator(database, reverse, lt, lte, gt, gte, limit, fillCache,
                   snapshot),
      id_(id),
      keys_(keys),
      values_(values),
      keyAsBuffer_(keyAsBuffer),
      valueAsBuffer_(valueAsBuffer),
      highWaterMarkBytes_(highWaterMarkBytes),
      first_(true),
      nexting_(false),
      isClosing_(false),
      closeWorker_(nullptr),
      ref_(nullptr) {
  LOG_DEBUG("Iterator %d:Constructing from Database\n", id_);
  LOG_DEBUG("Iterator %d:Constructed from Database\n", id_);
}

Iterator::Iterator(Transaction* transaction, const uint32_t id,
                   const bool reverse, const bool keys, const bool values,
                   const int limit, std::string* lt, std::string* lte,
                   std::string* gt, std::string* gte, const bool fillCache,
                   const bool keyAsBuffer, const bool valueAsBuffer,
                   const uint32_t highWaterMarkBytes,
                   const TransactionSnapshot* snapshot)
    : BaseIterator(transaction, reverse, lt, lte, gt, gte, limit, fillCache,
                   snapshot),
      id_(id),
      keys_(keys),
      values_(values),
      keyAsBuffer_(keyAsBuffer),
      valueAsBuffer_(valueAsBuffer),
      highWaterMarkBytes_(highWaterMarkBytes),
      first_(true),
      nexting_(false),
      isClosing_(false),
      closeWorker_(nullptr),
      ref_(nullptr) {
  LOG_DEBUG("Iterator %d:Constructing from Transaction %d\n", id_,
            transaction->id_);
  LOG_DEBUG("Iterator %d:Constructed from Transaction %d\n", id_,
            transaction->id_);
}

Iterator::~Iterator() {
  LOG_DEBUG("Iterator %d:Destroying\n", id_);
  BaseIterator::~BaseIterator();
  LOG_DEBUG("Iterator %d:Destroyed\n", id_);
};

void Iterator::Attach(napi_env env, napi_value iterator_ref) {
  LOG_DEBUG("Iterator %d:Calling Attach\n", id_);
  assert(database_ != nullptr || transaction_ != nullptr);
  if (ref_ != nullptr) {
    LOG_DEBUG("Iterator %d:Called Attach\n", id_);
    return;
  }
  NAPI_STATUS_THROWS_VOID(napi_create_reference(env, iterator_ref, 1, &ref_));
  if (database_ != nullptr) {
    database_->AttachIterator(env, id_, this);
  } else if (transaction_ != nullptr) {
    transaction_->AttachIterator(env, id_, this);
  }
  LOG_DEBUG("Iterator %d:Called Attach\n", id_);
}

void Iterator::Detach(napi_env env) {
  LOG_DEBUG("Iterator %d:Calling Detach\n", id_);
  assert(database_ != nullptr || transaction_ != nullptr);
  if (ref_ == nullptr) {
    LOG_DEBUG("Iterator %d:Called Detach\n", id_);
    return;
  }
  if (database_ != nullptr) {
    database_->DetachIterator(env, id_);
  } else if (transaction_ != nullptr) {
    transaction_->DetachIterator(env, id_);
  }
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
  LOG_DEBUG("Iterator %d:Called Detach\n", id_);
}

void Iterator::Close() {
  LOG_DEBUG("Iterator %d:Calling Close\n", id_);
  BaseIterator::Close();
  LOG_DEBUG("Iterator %d:Called Close\n", id_);
}

bool Iterator::ReadMany(uint32_t size) {
  assert(!hasClosed_);
  cache_.clear();
  cache_.reserve(size);
  size_t bytesRead = 0;
  rocksdb::Slice empty;
  while (true) {
    if (!first_) {
      Next();
    } else {
      first_ = false;
    }
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
