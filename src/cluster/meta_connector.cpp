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

#include <algorithm>
#include <array>
#include <chrono>
#include <optional>

#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_connect.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/metrics.h"
#include "lavik/status_macros.h"

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

void MetaConnectSchedule::Refresh(
    std::span<const MetaControlEndpoint> candidates) {
  const bool new_preference =
      !candidates.empty() &&
      (candidates_.empty() ||
       candidates.front() != candidates_.front().endpoint_);
  std::vector<Candidate> refreshed;
  refreshed.reserve(candidates.size());
  for (const auto& endpoint : candidates) {
    auto old = std::find_if(
        candidates_.begin(), candidates_.end(),
        [&](const Candidate& entry) { return entry.endpoint_ == endpoint; });
    if (old == candidates_.end()) {
      refreshed.push_back(Candidate{.endpoint_ = endpoint});
    } else {
      refreshed.push_back(std::move(*old));
    }
  }
  candidates_ = std::move(refreshed);
  if (new_preference) candidates_.front().last_attempt_ = 0;
}

void MetaConnectSchedule::SessionEnded(
    std::size_t index, bool follower,
    std::chrono::steady_clock::time_point retry_at) {
  candidates_[index].follower_ = follower;
  candidates_[index].retry_at_ = retry_at;
}

bycorf::Task<absl::StatusOr<std::unique_ptr<ConnectedMetaEndpoint>>>
MetaConnectSchedule::Connect(bycorf::Worker& worker,
                             const std::atomic<bool>& stopping,
                             bool* attempted) {
  DialState state;
  std::vector<bool> started(candidates_.size(), false);
  std::size_t remaining = candidates_.size();
  auto launch_at = std::chrono::steady_clock::now();
  absl::Status result;
  while (state.winner_ == nullptr && (remaining != 0 || state.Active())) {
    if (stopping.load(std::memory_order_acquire) || worker.stop_requested()) {
      result = absl::CancelledError("Meta connection stopped");
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (remaining != 0 && (now >= launch_at || !state.Active())) {
      std::size_t active = 0;
      for (const auto& attempt : state.attempts_) active += attempt.active_;
      bool waiting_follower = false;
      for (std::size_t i = 0; i < candidates_.size(); ++i)
        waiting_follower |= !started[i] && candidates_[i].follower_;
      std::optional<std::size_t> next;
      for (std::size_t i = 0; i < candidates_.size(); ++i) {
        const auto& candidate = candidates_[i];
        if (started[i] || now < candidate.retry_at_) continue;
        // Keep one slot available even before the follower's backoff expires.
        // Otherwise three blackholes can prevent revisiting a newly elected
        // peer for the entire ten-second TCP deadline.
        if (active >= kParallelAttempts ||
            (waiting_follower && !candidate.follower_ &&
             active >= kParallelAttempts - 1))
          continue;
        if (!next || candidate.last_attempt_ < candidates_[*next].last_attempt_)
          next = i;
      }
      if (next) {
        for (auto& attempt : state.attempts_) {
          if (attempt.active_) continue;
          attempt.cancellation_.emplace();
          attempt.active_ = true;
          started[*next] = true;
          --remaining;
          candidates_[*next].last_attempt_ = ++next_attempt_;
          worker.Spawn(Dial(worker, candidates_[*next].endpoint_, *next, state,
                            attempt));
          if (attempted != nullptr) {
            if (*attempted) RecordClusterControlReconnect();
            *attempted = true;
          }
          launch_at = now + kStagger;
          break;
        }
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
  LAVIK_CO_RETURN_IF_ERROR(result);
  if (stopping.load(std::memory_order_acquire) || worker.stop_requested())
    co_return absl::CancelledError("Meta connection stopped");
  if (state.winner_ != nullptr) co_return std::move(state.winner_);
  co_return state.last_error_;
}

bycorf::Task<absl::StatusOr<std::unique_ptr<ConnectedMetaEndpoint>>>
ConnectMetaEndpoint(bycorf::Worker& worker,
                    std::span<const MetaControlEndpoint> candidates,
                    const std::atomic<bool>& stopping, bool* attempted) {
  MetaConnectSchedule schedule;
  schedule.Refresh(candidates);
  co_return co_await schedule.Connect(worker, stopping, attempted);
}
}  // namespace lavik::cluster::detail
