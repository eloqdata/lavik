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
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

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

// Worker-local discovery state, retained across unsuccessful handshakes. Dial
// order advances even for cancelled losers, so a fast follower cannot keep
// restarting discovery at the same dead prefix. Only the returned winner
// survives Connect; the schedule retains no transport.
class MetaConnectSchedule {
 public:
  // Reconcile an authenticated directory without losing retry progress. A new
  // first choice (leader hint) gets one preferred attempt. Identity changes are
  // new candidates; unresolved seeds remain distinct from learned members.
  void Refresh(std::span<const MetaControlEndpoint> candidates);

  // Resolve an index returned by Connect; valid until the next Refresh.
  const MetaControlEndpoint& Endpoint(std::size_t index) const {
    return candidates_[index].endpoint_;
  }

  // Only a validated non-leader Hello qualifies for the reserved retry slot.
  // Other handshake failures clear that status. Backoff is supplied by the
  // caller, which also controls the pacing between successive handshakes.
  void SessionEnded(std::size_t index, bool follower,
                    std::chrono::steady_clock::time_point retry_at);

  // Race at most three TCP attempts, reserving capacity for a known follower
  // whose retry becomes due. Every attempt keeps its full connect deadline;
  // all losers are cancelled and joined before the sole session may start.
  // The schedule and stop flag must outlive the call and must not be mutated
  // concurrently. Must run on the control worker.
  bycorf::Task<absl::StatusOr<std::unique_ptr<ConnectedMetaEndpoint>>> Connect(
      bycorf::Worker& worker, const std::atomic<bool>& stopping,
      bool* attempted);

 private:
  struct Candidate {
    MetaControlEndpoint endpoint_;
    std::chrono::steady_clock::time_point retry_at_{};
    std::uint64_t last_attempt_ = 0;
    bool follower_ = false;
  };
  std::vector<Candidate> candidates_;
  std::uint64_t next_attempt_ = 0;
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
