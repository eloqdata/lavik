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

#include "lavik/fault_injection.h"
#include "lavik/status_macros.h"

#if LAVIK_FAULTS_ENABLED
#include <unistd.h>

#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <string>
#include <vector>

#include "bycorf/io/storage.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/storage/detail/grouped/commit.h"
#include "lavik/storage/engine.h"
#include "spdlog/spdlog.h"

namespace lavik::storage {

// One local decision is held per test server. Worker-local state observes the
// real notification awaiter, without introducing a suspension between the
// production state check and waiter registration.
struct GroupedDependencyTestState {
  std::uint64_t txid_ = 0;
  bool probed_ = false;
};
inline thread_local GroupedDependencyTestState grouped_dependency_test_state;

class GroupedDependencyTestWaiter : public bycorf::AsyncNotification::Awaiter {
 public:
  GroupedDependencyTestWaiter(bycorf::AsyncNotification& notification,
                              std::uint64_t txid)
      : Awaiter(&notification), txid_(txid) {}

  bool await_suspend(std::coroutine_handle<> awaiting) {
    const bool suspended = Awaiter::await_suspend(awaiting);
    const auto& test = grouped_dependency_test_state;
    if (test.txid_ == txid_)
      spdlog::info("grouped dependency waiter registered txid={} probed={}",
                   txid_, test.probed_);
    return suspended;
  }

 private:
  std::uint64_t txid_;
};

// Success is held AFTER the last disk flush and BEFORE kDurable, so no I/O
// notification can mask a missing terminal wakeup. Failure is held before the
// commit append and returned through CommitTxWrites' real failure guard. The
// test arms only after seeding is durable; unlink consumes that one-shot gate.
inline bycorf::Task<absl::Status> PauseGroupedDecisionForTest(
    bycorf::Worker& worker, bycorf::AsyncNotification& notification,
    const std::vector<TxShardWrites*>& shards, bool before_append) {
  const char* configured = std::getenv("LAVIK_GROUPED_DEPENDENCY_GATE");
  if (configured == nullptr) co_return absl::OkStatus();
  const std::string base(configured);
  const bool fail = ::access((base + ".fail").c_str(), F_OK) == 0;
  if (fail != before_append) co_return absl::OkStatus();
  std::uint64_t txid = 0;
  for (const auto* shard : shards) {
    if (shard != nullptr && shard->grouped_decision_ != nullptr &&
        shard->grouped_decision_->completion_owner_ == worker.id()) {
      txid = shard->grouped_decision_->txid_;
      break;
    }
  }
  if (txid == 0 || ::unlink((base + ".arm").c_str()) != 0)
    co_return absl::OkStatus();
  auto& test = grouped_dependency_test_state;
  test = {.txid_ = txid};
  struct Reset {
    ~Reset() { grouped_dependency_test_state = {}; }
  } reset;
  spdlog::info("grouped dependency decision held txid={}", txid);
  const auto until =
      std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (::access((base + ".release").c_str(), F_OK) != 0) {
    if (::unlink((base + ".notify").c_str()) == 0) {
      test.probed_ = true;
      notification.NotifyAll(worker);
      spdlog::info("grouped dependency premature notification sent txid={}",
                   txid);
    }
    if (std::chrono::steady_clock::now() >= until)
      co_return absl::DeadlineExceededError(
          "grouped dependency gate timed out");
    LAVIK_CO_RETURN_IF_ERROR(
        co_await bycorf::SleepFor(worker, std::chrono::milliseconds(1)));
  }
  co_return fail ? absl::InternalError("injected grouped commit failure")
                 : absl::OkStatus();
}

}  // namespace lavik::storage
#endif
