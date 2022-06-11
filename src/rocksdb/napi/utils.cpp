#define NAPI_VERSION 3

#include "utils.h"

#include <rocksdb/env.h>

void NullLogger::Logv(const char* format, va_list ap) {}

size_t NullLogger::GetLogFileSize() const { return 0; }

bool IsString(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_string;
}

bool IsBuffer(napi_env env, napi_value value) {
  bool isBuffer;
  napi_is_buffer(env, value, &isBuffer);
  return isBuffer;
}

bool IsObject(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_object;
}

bool IsUndefined(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_undefined;
}

bool IsNull(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_null;
}

bool IsExternal(napi_env env, napi_value value) {
  napi_valuetype type;
  napi_typeof(env, value, &type);
  return type == napi_external;
}

napi_value CreateError(napi_env env, const char* str) {
  napi_value msg;
  napi_create_string_utf8(env, str, strlen(str), &msg);
  napi_value error;
  napi_create_error(env, NULL, msg, &error);
  return error;
}

napi_value CreateCodeError(napi_env env, const char* code, const char* msg) {
  napi_value codeValue;
  napi_create_string_utf8(env, code, strlen(code), &codeValue);
  napi_value msgValue;
  napi_create_string_utf8(env, msg, strlen(msg), &msgValue);
  napi_value error;
  napi_create_error(env, codeValue, msgValue, &error);
  return error;
}

bool HasProperty(napi_env env, napi_value obj, const char* key) {
  bool has = false;
  napi_has_named_property(env, obj, key, &has);
  return has;
}

napi_value GetProperty(napi_env env, napi_value obj, const char* key) {
  napi_value value;
  napi_get_named_property(env, obj, key, &value);
  return value;
}

bool BooleanProperty(napi_env env, napi_value obj, const char* key,
                     bool DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    bool result;
    napi_get_value_bool(env, value, &result);
    return result;
  }

  return DEFAULT;
}

bool EncodingIsBuffer(napi_env env, napi_value options, const char* option) {
  napi_value value;
  size_t size;

  if (napi_get_named_property(env, options, option, &value) == napi_ok &&
      napi_get_value_string_utf8(env, value, NULL, 0, &size) == napi_ok) {
    // Value is either "buffer" or "utf8" so we can tell them apart just by size
    return size == 6;
  }

  return false;
}

uint32_t Uint32Property(napi_env env, napi_value obj, const char* key,
                        uint32_t DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    uint32_t result;
    napi_get_value_uint32(env, value, &result);
    return result;
  }

  return DEFAULT;
}

int Int32Property(napi_env env, napi_value obj, const char* key, int DEFAULT) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    int result;
    napi_get_value_int32(env, value, &result);
    return result;
  }

  return DEFAULT;
}

std::string StringProperty(napi_env env, napi_value obj, const char* key) {
  if (HasProperty(env, obj, key)) {
    napi_value value = GetProperty(env, obj, key);
    if (IsString(env, value)) {
      size_t size = 0;
      napi_get_value_string_utf8(env, value, NULL, 0, &size);

      char* buf = new char[size + 1];
      napi_get_value_string_utf8(env, value, buf, size + 1, &size);
      buf[size] = '\0';

      std::string result = buf;
      delete[] buf;
      return result;
    }
  }

  return "";
}

const Snapshot* SnapshotProperty(napi_env env, napi_value obj,
                                 const char* key) {
  if (!HasProperty(env, obj, key)) {
    return nullptr;
  }
  napi_value value = GetProperty(env, obj, key);
  if (!IsExternal(env, value)) {
    return nullptr;
  }
  Snapshot* snapshot = NULL;
  NAPI_STATUS_THROWS(napi_get_value_external(env, value, (void**)&snapshot));
  if (!dynamic_cast<Snapshot*>(snapshot)) {
    return nullptr;
  }
  return snapshot;
}

const TransactionSnapshot* TransactionSnapshotProperty(napi_env env,
                                                       napi_value obj,
                                                       const char* key) {
  if (!HasProperty(env, obj, key)) {
    return nullptr;
  }
  napi_value value = GetProperty(env, obj, key);
  if (!IsExternal(env, value)) {
    return nullptr;
  }
  TransactionSnapshot* snapshot = NULL;
  NAPI_STATUS_THROWS(napi_get_value_external(env, value, (void**)&snapshot));
  if (!dynamic_cast<TransactionSnapshot*>(snapshot)) {
    return nullptr;
  }
  return snapshot;
}

void DisposeSliceBuffer(rocksdb::Slice slice) {
  if (!slice.empty()) delete[] slice.data();
}

rocksdb::Slice ToSlice(napi_env env, napi_value from) {
  LD_STRING_OR_BUFFER_TO_COPY(env, from, to);
  return rocksdb::Slice(toCh_, toSz_);
}

size_t StringOrBufferLength(napi_env env, napi_value value) {
  size_t size = 0;

  if (IsString(env, value)) {
    napi_get_value_string_utf8(env, value, NULL, 0, &size);
  } else if (IsBuffer(env, value)) {
    char* buf;
    napi_get_buffer_info(env, value, (void**)&buf, &size);
  }

  return size;
}

std::string* RangeOption(napi_env env, napi_value opts, const char* name) {
  if (HasProperty(env, opts, name)) {
    napi_value value = GetProperty(env, opts, name);
    LD_STRING_OR_BUFFER_TO_COPY(env, value, to);
    std::string* result = new std::string(toCh_, toSz_);
    delete[] toCh_;
    return result;
  }

  return NULL;
}

std::vector<rocksdb::Slice>* KeyArray(napi_env env, napi_value arr) {
  uint32_t length;
  std::vector<rocksdb::Slice>* result = new std::vector<rocksdb::Slice>();
  if (napi_get_array_length(env, arr, &length) == napi_ok) {
    result->reserve(length);
    for (uint32_t i = 0; i < length; i++) {
      napi_value element;
      if (napi_get_element(env, arr, i, &element) == napi_ok) {
        rocksdb::Slice slice = ToSlice(env, element);
        result->emplace_back(slice);
      }
    }
  }
  return result;
}

napi_status CallFunction(napi_env env, napi_value callback, const int argc,
                         napi_value* argv) {
  napi_value global;
  napi_get_global(env, &global);
  return napi_call_function(env, global, callback, argc, argv, NULL);
}

napi_value noop_callback(napi_env env, napi_callback_info info) { return 0; }
