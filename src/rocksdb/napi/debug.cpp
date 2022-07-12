#include "debug.h"

#include <cstdlib>
#include <string>
#include <sstream>

#include <node_api.h>
#include <napi-macros.h>

bool is_log_debug_enabled = false;

void CheckNodeDebugNative() {
  const char* node_debug_native_env = getenv("NODE_DEBUG_NATIVE");
  if (node_debug_native_env != nullptr) {
    std::string node_debug_native(node_debug_native_env);
    std::stringstream ss(node_debug_native);
    while (ss.good()) {
      std::string module;
      getline(ss, module, ',');
      if (module == "*" || module == "rocksdb") {
        is_log_debug_enabled = true;
        break;
      }
    }
  }
}
