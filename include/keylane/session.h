#pragma once

#include <cstdint>

namespace keylane {

// Per-connection state, owned by the connection's Serve coroutine frame.
// Grows with MULTI/EXEC queueing and WATCH registration in later milestones;
// everything here must be cleaned up through the single cleanup point at the
// end of RedisService::Serve.
struct ConnectionContext {
  std::uint8_t selected_db = 0;
};

}  // namespace keylane
