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

#include "lavik/cluster/meta_connector.h"

#include <array>
#include <chrono>
#include <optional>

#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_connect.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/metrics.h"

namespace lavik::cluster::detail {
namespace {
using namespace std::chrono_literals;
constexpr auto kConnectTimeout = 10s;
constexpr auto kStagger = 100ms;
constexpr auto kStopPoll = 25ms;
constexpr std::size_t kParallelAttempts = 3;

struct Attempt {
  std::optional<bycorf::TcpConnectCancellation> cancellation_;
  bool active_ = false;
};

struct DialState {
  std::array<Attempt, kParallelAttempts> attempts_;
  bycorf::AsyncNotification changed_;
  std::unique_ptr<ConnectedMetaEndpoint> winner_;
  absl::Status last_error_ = absl::UnavailableError("no Meta control endpoint");
  bool closing_ = false;

  bool Active() const {
    for (const auto& attempt : attempts_)
      if (attempt.active_) return true;
    return false;
  }
  void Cancel() {
    closing_ = true;
    for (auto& attempt : attempts_)
      if (attempt.active_) attempt.cancellation_->Cancel();
  }
};

bycorf::Task<absl::Status> Dial(bycorf::Worker& worker,
                                const MetaControlEndpoint& endpoint,
                                std::size_t index, DialState& state,
                                Attempt& attempt) {
  // No hello, TLS or control-plane side effect occurs in these parallel tasks.
  // The winner pins its Connection before publishing it to the coordinator.
  auto connected = co_await bycorf::ConnectTcpCancellable(
      worker, endpoint.host_, endpoint.port_, kConnectTimeout,
      &*attempt.cancellation_);
  if (connected.ok()) {
    if (!state.closing_ && state.winner_ == nullptr) {
      state.winner_ =
          std::make_unique<ConnectedMetaEndpoint>(index, std::move(*connected));
    } else {
      (void)connected->Close();
    }
  } else if (!absl::IsCancelled(connected.status())) {
    state.last_error_ = connected.status();
  }
  attempt.active_ = false;
  state.changed_.NotifyAll(worker);
  co_return absl::OkStatus();
}
}  // namespace

bycorf::Task<absl::StatusOr<std::unique_ptr<ConnectedMetaEndpoint>>>
ConnectMetaEndpoint(bycorf::Worker& worker,
                    std::span<const MetaControlEndpoint> candidates,
                    const std::atomic<bool>& stopping, bool* attempted) {
  DialState state;
  std::size_t next = 0;
  auto launch_at = std::chrono::steady_clock::now();
  absl::Status result;
  while (state.winner_ == nullptr &&
         (next < candidates.size() || state.Active())) {
    if (stopping.load(std::memory_order_acquire) || worker.stop_requested()) {
      result = absl::CancelledError("Meta connection stopped");
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (next < candidates.size() && (now >= launch_at || !state.Active())) {
      for (auto& attempt : state.attempts_) {
        if (attempt.active_) continue;
        attempt.cancellation_.emplace();
        attempt.active_ = true;
        worker.Spawn(Dial(worker, candidates[next], next, state, attempt));
        if (attempted != nullptr) {
          if (*attempted) RecordClusterControlReconnect();
          *attempted = true;
        }
        ++next;
        launch_at = now + kStagger;
        break;
      }
    }
    // Only active reconnection pays for this bounded stop/stagger polling.
    // Established sessions have no dial coordinator or per-ACK fan-out.
    result = co_await bycorf::SleepFor(worker, kStopPoll);
    if (!result.ok()) break;
  }
  state.Cancel();
  // Stack-owned attempt slots cannot be released until their connect and
  // deadline CQEs have retired. Cancellation never touches the selected stream.
  while (state.Active()) co_await state.changed_.Wait();
  if (!result.ok()) co_return result;
  if (stopping.load(std::memory_order_acquire) || worker.stop_requested())
    co_return absl::CancelledError("Meta connection stopped");
  if (state.winner_ != nullptr) co_return std::move(state.winner_);
  co_return state.last_error_;
}
}  // namespace lavik::cluster::detail
