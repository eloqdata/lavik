#pragma once

#include <cstdint>
#include <vector>

#include "keylane/command.h"

namespace keylane {

// Per-connection state, owned by the connection's Serve coroutine frame.
// Everything here must be cleaned up through the single cleanup point at the
// end of RedisService::Serve.
struct ConnectionContext {
  std::uint8_t selected_db = 0;

  // MULTI/EXEC queueing. `multi_db` tracks SELECTs issued while queueing so
  // every queued command records the database it will execute against;
  // `multi_dirty` marks queue-time errors that turn EXEC into EXECABORT.
  bool in_multi = false;
  bool multi_dirty = false;
  std::uint8_t multi_db = 0;
  std::vector<CommandRequest> queued;

  void ResetMulti() {
    in_multi = false;
    multi_dirty = false;
    queued.clear();
  }
};

}  // namespace keylane
