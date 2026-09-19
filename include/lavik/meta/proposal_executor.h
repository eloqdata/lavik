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

// Bounded admission and completion waits for Meta mutations/snapshots. Snapshot
// publication can wait for storage, so it stays off the single Bycorf worker.
// That worker only enqueues work and resumes through bycorf::ForeignExecutor.

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "absl/status/status.h"

namespace lavik::meta {

class MetaProposalExecutor {
 public:
  // Work owns its completion state and must translate expected exceptions
  // into that task's failure result. Letting an exception escape is fail-fast:
  // silently abandoning a waiter would wedge control-plane progress.
  using Work = std::function<void()>;

  explicit MetaProposalExecutor(std::size_t capacity = 1024);
  ~MetaProposalExecutor();
  MetaProposalExecutor(const MetaProposalExecutor&) = delete;
  MetaProposalExecutor& operator=(const MetaProposalExecutor&) = delete;

  // Enqueues without waiting. Accepted work is drained during Shutdown;
  // overload is reported to ingress instead of blocking the bycorf worker.
  absl::Status Submit(Work work);
  void Shutdown() noexcept;

 private:
  void Run() noexcept;

  const std::size_t capacity_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Work> queue_;
  bool accepting_ = true;
  std::thread thread_;
};

// Raft accepts only one membership change at a time. Initial cluster creation
// shares this gate so membership cannot race its singleton lifecycle admission.
// Every Admin listener and both workflow reconcilers share one gate. After
// intent submission the background owner holds the lease; the durable Creating
// lifecycle bridges handoff and restart. No mutex is held across suspension.
class MetaMembershipGate
    : public std::enable_shared_from_this<MetaMembershipGate> {
 public:
  class Lease {
   public:
    ~Lease();
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;

   private:
    friend class MetaMembershipGate;
    explicit Lease(std::shared_ptr<MetaMembershipGate> gate)
        : gate_(std::move(gate)) {}
    std::shared_ptr<MetaMembershipGate> gate_;
  };

  // Does not wait for the active workflow: a null lease means the caller must
  // reject the request before making any committed mutation.
  std::unique_ptr<Lease> TryAcquire();

 private:
  void Release() noexcept;
  std::mutex mu_;
  bool held_ = false;
};

}  // namespace lavik::meta
