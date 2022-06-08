#define NAPI_VERSION 3

#include "iterator.h"
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <node_api.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include "database.h"

Entry::Entry(const rocksdb::Slice* key, const rocksdb::Slice* value):
  key_(key->data(), key->size()),
  value_(value->data(), value->size()) {}

void Entry::ConvertByMode(
  napi_env env,
  Mode mode,
  const bool keyAsBuffer,
  const bool valueAsBuffer,
  napi_value* result
) {
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

void Entry::Convert(
  napi_env env,
  const std::string* s,
  const bool asBuffer,
  napi_value* result
) {
  if (s == NULL) {
    napi_get_undefined(env, result);
  } else if (asBuffer) {
    napi_create_buffer_copy(env, s->size(), s->data(), NULL, result);
  } else {
    napi_create_string_utf8(env, s->data(), s->size(), result);
  }
}

BaseIterator::BaseIterator(
  Database* database,
  const bool reverse,
  std::string* lt,
  std::string* lte,
  std::string* gt,
  std::string* gte,
  const int limit,
  const bool fillCache
):
  database_(database),
  hasClosed_(false),
  didSeek_(false),
  reverse_(reverse),
  lt_(lt),
  lte_(lte),
  gt_(gt),
  gte_(gte),
  limit_(limit),
  count_(0)
{
  options_ = new rocksdb::ReadOptions();
  options_->fill_cache = fillCache;
  options_->verify_checksums = false;
  options_->snapshot = database->NewSnapshot();
  dbIterator_ = database_->NewIterator(options_);
}

BaseIterator::~BaseIterator() {
  assert(hasClosed_);

  if (lt_ != NULL) delete lt_;
  if (gt_ != NULL) delete gt_;
  if (lte_ != NULL) delete lte_;
  if (gte_ != NULL) delete gte_;

  delete options_;
}

bool BaseIterator::DidSeek () const {
  return didSeek_;
}

void BaseIterator::SeekToRange () {
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

void BaseIterator::Seek (rocksdb::Slice& target) {
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

void BaseIterator::Close () {
  if (!hasClosed_) {
    hasClosed_ = true;
    delete dbIterator_;
    dbIterator_ = NULL;
    database_->ReleaseSnapshot(options_->snapshot);
  }
}

bool BaseIterator::Valid () const {
  return dbIterator_->Valid() && !OutOfRange(dbIterator_->key());
}

bool BaseIterator::Increment () {
  return limit_ < 0 || ++count_ <= limit_;
}

void BaseIterator::Next () {
  if (reverse_) dbIterator_->Prev();
  else dbIterator_->Next();
}

void BaseIterator::SeekToFirst () {
  if (reverse_) dbIterator_->SeekToLast();
  else dbIterator_->SeekToFirst();
}

void BaseIterator::SeekToLast () {
  if (reverse_) dbIterator_->SeekToFirst();
  else dbIterator_->SeekToLast();
}

void BaseIterator::SeekToEnd () {
  SeekToLast();
  Next();
}

rocksdb::Slice BaseIterator::CurrentKey () const {
  return dbIterator_->key();
}

rocksdb::Slice BaseIterator::CurrentValue () const {
  return dbIterator_->value();
}

rocksdb::Status BaseIterator::Status () const {
  return dbIterator_->status();
}

bool BaseIterator::OutOfRange (const rocksdb::Slice& target) const {
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


/**
 * Extends BaseIterator for reading it from JS land.
 */
Iterator::Iterator (Database* database,
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

Iterator::~Iterator () = default;

void Iterator::Attach (napi_env env, napi_value context) {
  napi_create_reference(env, context, 1, &ref_);
  database_->AttachIterator(env, id_, this);
}

void Iterator::Detach (napi_env env) {
  database_->DetachIterator(env, id_);
  if (ref_ != NULL) napi_delete_reference(env, ref_);
}

bool Iterator::ReadMany (uint32_t size) {
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
