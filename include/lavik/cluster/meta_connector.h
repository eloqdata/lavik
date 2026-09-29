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

#include <atomic>
#include <memory>
#include <span>

#include "bycorf/net/tcp_stream.h"
#include "lavik/cluster/meta_client.h"

namespace lavik::cluster::detail {

// Owns the selected transport and pins its Connection storage across loser
// retirement and the caller's handshake. No application protocol has run yet.
struct ConnectedMetaEndpoint {
  ConnectedMetaEndpoint(std::size_t index, bycorf::TcpStream stream)
      : index_(index),
        stream_(std::move(stream)),
        storage_(stream_.BorrowStorage()) {}
  ~ConnectedMetaEndpoint() { (void)stream_.Close(); }
  ConnectedMetaEndpoint(const ConnectedMetaEndpoint&) = delete;
  ConnectedMetaEndpoint& operator=(const ConnectedMetaEndpoint&) = delete;

  std::size_t index_;
  bycorf::TcpStream stream_;
  bycorf::ConnectionStorageBorrow storage_;
};

// Attempts numeric endpoints in preference order, at most three at once,
// staggered by 100ms. Only TCP establishment is raced; the caller still owns
// the sole authenticated control session. Cancels and joins every loser
// before returning, including on stop. The borrowed candidates/stop flag must
// remain alive through this call. Must run on the control worker.
bycorf::Task<absl::StatusOr<std::unique_ptr<ConnectedMetaEndpoint>>>
ConnectMetaEndpoint(bycorf::Worker& worker,
                    std::span<const MetaControlEndpoint> candidates,
                    const std::atomic<bool>& stopping, bool* attempted);

}  // namespace lavik::cluster::detail
