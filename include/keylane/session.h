#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "keylane/command.h"
#include "keylane/storage/format.h"
#include "keylane/tx/fingerprint.h"

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

  // WATCH registrations: enough to check and unregister on the owning
  // shards. Deduplicated by (db, key) — never by fingerprint, which can
  // collide across distinct keys — and the first registration's liveness
  // snapshot is authoritative (sticky, like Redis).
  struct WatchedKey {
    std::string key;
    storage::Digest digest;
    tx::LockFp fp = 0;
    std::uint16_t owner = 0;
    std::uint8_t db = 0;
    // Liveness observed on the owning shard at WATCH time; EXEC compares it
    // against the key's own current liveness, so fingerprint collisions can
    // only ever cause false aborts, not missed ones.
    bool live = false;
  };
  std::uint64_t conn_id = 0;
  std::vector<WatchedKey> watched;

  void ResetMulti() {
    in_multi = false;
    multi_dirty = false;
    queued.clear();
  }
};

}  // namespace keylane
