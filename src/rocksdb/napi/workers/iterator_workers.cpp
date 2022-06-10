#define NAPI_VERSION 3

#include "iterator_workers.h"
#include <cstddef>
#include <cstdint>
#include <node_api.h>
#include "../worker.h"
#include "../iterator.h"
#include "../utils.h"

CloseIteratorWorker::CloseIteratorWorker (napi_env env,
            Iterator* iterator,
            napi_value callback)
  : BaseWorker(env, iterator->database_, callback, "rocksdb.iterator.close"),
    iterator_(iterator) {}

CloseIteratorWorker::~CloseIteratorWorker () {}

void CloseIteratorWorker::DoExecute () {
  iterator_->Close();
}

void CloseIteratorWorker::DoFinally (napi_env env) {
  iterator_->Detach(env);
  BaseWorker::DoFinally(env);
}

NextWorker::NextWorker (napi_env env,
            Iterator* iterator,
            uint32_t size,
            napi_value callback)
  : BaseWorker(env, iterator->database_, callback,
                "rocksdb.iterator.next"),
    iterator_(iterator), size_(size), ok_() {}

NextWorker::~NextWorker () {}

void NextWorker::DoExecute () {
  if (!iterator_->DidSeek()) {
    iterator_->SeekToRange();
  }

  ok_ = iterator_->ReadMany(size_);

  if (!ok_) {
    SetStatus(iterator_->Status());
  }
}

void NextWorker::HandleOKCallback (napi_env env, napi_value callback) {
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

void NextWorker::DoFinally (napi_env env) {
  // clean up & handle the next/close state
  iterator_->nexting_ = false;

  if (iterator_->closeWorker_ != NULL) {
    iterator_->closeWorker_->Queue(env);
    iterator_->closeWorker_ = NULL;
  }

  BaseWorker::DoFinally(env);
}
