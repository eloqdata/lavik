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

// Worker-owned, fail-closed Redis Sentinel compatibility surface. Only
// completed connection commands are admitted; unsupported queries and
// management commands never reach Data or Admin dispatch. Lifecycle methods are
// called serially by the process main thread while the supplied worker executor
// is alive.
class MetaSentinelServer {
 public:
  static absl::StatusOr<std::shared_ptr<MetaSentinelServer>> Create(
      bycorf::ForeignExecutor executor, MetaSentinelServerOptions options);
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
