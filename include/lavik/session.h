/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "lavik/command.h"
#include "lavik/replication.h"
#include "lavik/resp.h"
#include "lavik/storage/format.h"
#include "lavik/tx/fingerprint.h"

namespace lavik {

class MonitorSession;
class PubSubSession;

// Per-connection state, owned by the connection's Serve coroutine frame.
// Everything here must be cleaned up through the single cleanup point at the
// end of RedisService::Serve.
struct ConnectionContext {
  using HelloHandler = std::string_view (*)(const void*, void*,
                                            ConnectionContext&,
                                            std::span<const std::string>,
                                            ReplyBuilder&);
  // Worker-local shutdown marker: buffered commands stop once cleanup starts.
  bool closing_ = false;
  // Peer EOF can leave complete commands buffered. Only WAIT needs to remember
  // it across suspension before its blocking waiter is registered.
  bool wait_peer_disconnected_ = false;
  std::uint8_t selected_db_ = 0;
  bool authenticated_ = true;
  bool authentication_required_ = false;
  bool counted_as_client_ = true;
  // Redis Cluster replica reads are opt-in per connection. READONLY enables
  // them and READWRITE restores the default MOVED-to-primary behavior.
  bool cluster_readonly_ = false;
  ReplyBuilder reply_builder_;
  const void* hello_authenticator_ = nullptr;
  void* hello_replication_ = nullptr;
  HelloHandler hello_handler_ = nullptr;
  // Set only while DispatchCommand is active. EXEC uses it when flushing the
  // readiness notifications captured from its queued child commands.
  BlockingWakeCascade* blocking_wake_cascade_ = nullptr;
  // Redis WAIT is scoped to writes previously issued by this connection.
  // Source writes mark the cached all-flow cut dirty; WAIT resolves it lazily
  // so the ordinary write path does not fence asynchronous publisher queues.
  std::optional<NativeReplicationWatermark> native_replication_watermark_;
  bool native_replication_watermark_dirty_ = false;

  [[nodiscard]] RespVersion resp_version() const noexcept {
    return reply_builder_.version();
  }
  void SetRespVersion(RespVersion version) noexcept {
    reply_builder_.SetVersion(version);
  }

  // MULTI/EXEC queueing. `multi_db` tracks SELECTs issued while queueing so
  // every queued command records the database it will execute against;
  // `multi_dirty` marks queue-time errors that turn EXEC into EXECABORT.
  bool in_multi_ = false;
  bool multi_dirty_ = false;
  // Internal replica replay turns any child command error into a top-level
  // failure so the flow cannot ACK a partially applied EXEC.
  bool strict_replication_apply_ = false;
  std::uint8_t multi_db_ = 0;
  std::vector<CommandRequest> queued_;

  // WATCH registrations: enough to check and unregister on the owning
  // shards. Deduplicated by (db, key) — never by fingerprint, which can
  // collide across distinct keys — and the first registration's liveness
  // snapshot is authoritative (sticky, like Redis).
  struct WatchedKey {
    std::string key_;
    storage::Digest digest_;
    tx::LockFp fp_ = 0;
    std::uint16_t owner_ = 0;
    std::uint8_t db_ = 0;
    // WATCH is an observation of one logical population, not just one key's
    // liveness. EXEC treats a serving-generation change as a modification even
    // when the replacement happens to contain the same key state.
    std::uint64_t serving_generation_ = 0;
    bool serving_generation_valid_ = false;
    // Liveness observed on the owning shard at WATCH time; EXEC compares it
    // against the key's own current liveness, so fingerprint collisions can
    // only ever cause false aborts, not missed ones.
    bool live_ = false;
  };
  std::uint64_t conn_id_ = 0;
  int socket_fd_ = -1;
  std::string peer_address_;
  std::string client_name_;
  std::vector<WatchedKey> watched_;
  std::shared_ptr<MonitorSession> monitor_session_;
  std::shared_ptr<PubSubSession> pubsub_session_;
  bool close_after_pubsub_ = false;

  void ResetMulti() {
    in_multi_ = false;
    multi_dirty_ = false;
    queued_.clear();
  }
};

}  // namespace lavik
