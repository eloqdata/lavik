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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/task.h"

namespace bycorf {
class TcpStream;
struct Connection;
}  // namespace bycorf

namespace lavik::meta {

class MetaRaft;
class MetaStateMachine;
class MetaDataControlRuntimeStatus;
class MetaAutomaticFailoverDiagnosticsRegistry;

// Process-local Sentinel protocol configuration, independent of Data
// credentials and Meta operator identity. No advertised endpoint or Raft state
// is implied.
struct MetaSentinelServerOptions {
  std::string address_;
  std::string requirepass_;
  std::size_t maxclients_ = 256;
  std::size_t query_limit_ = 64 * 1024;
  std::size_t reply_limit_ = 64 * 1024;
  std::chrono::milliseconds progress_timeout_{10000};
};

// Authority inputs for the five discovery verbs (GET-MASTER-ADDR-BY-NAME,
// MASTER, MASTERS, REPLICAS, SLAVES). Connection and management answers never
// touch these; they exist so discovery can be served from committed state plus
// the two thread-safe observation registries. The state machine pointer is
// non-owning: process assembly keeps it alive until after Shutdown.
struct MetaSentinelDiscoveryDependencies {
  // Read through atomics only (leader triplet); never proposed through.
  std::shared_ptr<MetaRaft> raft_;
  MetaStateMachine* state_machine_ = nullptr;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;
  std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry> diagnostics_;
  std::uint32_t observation_ttl_ms_ = 0;
  // Reused leadership handoff grace: inside this window a node the new leader
  // has never observed reads as unknown instead of down.
  std::uint64_t leader_observation_grace_ms_ = 0;
};

// Worker-owned, fail-closed Redis Sentinel compatibility surface. Only
// completed connection commands are admitted; unsupported queries and
// management commands never reach Data or Admin dispatch. Lifecycle methods are
// called serially by the process main thread while the supplied worker executor
// is alive.
//
// Discovery verbs are answered only by a caught-up leader and read a
// worker-local cache of the committed projection keyed on the state machine's
// change index; a follower or still-catching-up leader closes the connection
// without a reply so client seed lists rotate to another Meta member. That
// silent drop is deliberate: clients must not act on non-authoritative
// topology, and no private redirect exists on this surface.
class MetaSentinelServer {
 public:
  static absl::StatusOr<std::shared_ptr<MetaSentinelServer>> Create(
      bycorf::ForeignExecutor executor,
      MetaSentinelDiscoveryDependencies discovery,
      MetaSentinelServerOptions options);
  ~MetaSentinelServer();
  MetaSentinelServer(const MetaSentinelServer&) = delete;
  MetaSentinelServer& operator=(const MetaSentinelServer&) = delete;

  // Waits on the main thread for the worker's bind result. May be called once.
  absl::Status Start();
  // Stops accepts and joins all sessions before returning; idempotent after
  // drain, including destruction after the runtime has stopped.
  void Shutdown();

 private:
  struct Core;
  class SessionBorrow;
  using CorePtr = std::shared_ptr<Core>;
  explicit MetaSentinelServer(CorePtr core);
  static bycorf::Task<absl::Status> AcceptLoop(CorePtr core);
  static bycorf::Task<absl::Status> SessionLoop(CorePtr core,
                                                bycorf::TcpStream stream,
                                                SessionBorrow borrow);
  CorePtr core_;
  bool started_ = false;
  bool stopped_ = false;
};

}  // namespace lavik::meta
