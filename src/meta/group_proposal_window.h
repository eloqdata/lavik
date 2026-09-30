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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bycorf/io/storage.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/meta/coordinator.h"

namespace lavik::meta::detail {

// Worker-owned bounded proposal overlap. The owner must Drain before releasing
// its leader context. Each coroutine owns its command/result: no references to
// a reconciler's mutable pending map survive a suspension. A Group remains
// occupied until its completion is consumed, including a rejected proposal.
class GroupProposalWindow {
 public:
  static constexpr std::size_t kCapacity = 4;
  GroupProposalWindow() = default;
  GroupProposalWindow(const GroupProposalWindow&) = delete;
  GroupProposalWindow& operator=(const GroupProposalWindow&) = delete;
  ~GroupProposalWindow() {
    if (!entries_.empty())
      std::terminate();  // owner violated the join contract
  }
  struct Entry {
    std::string group_;
    MetaCommand command_;
    std::optional<absl::StatusOr<MetaApplyResult>> result_;
  };

  bool Full() const { return entries_.size() >= kCapacity; }
  bool Contains(std::string_view group) const {
    return entries_.contains(group);
  }

  void Start(MetaLeaderContext& context, std::string group,
             MetaCommand command) {
    if (Full() || Contains(group)) std::terminate();
    auto entry = std::make_shared<Entry>(
        Entry{std::move(group), std::move(command), std::nullopt});
    auto task = Propose(&context, entry);
    entries_.emplace(entry->group_, entry);
    bycorf::ThisWorker().self_->Spawn(std::move(task));
  }

  std::vector<std::shared_ptr<Entry>> TakeCompleted() {
    std::vector<std::shared_ptr<Entry>> completed;
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (it->second->result_) {
        completed.push_back(std::move(it->second));
        it = entries_.erase(it);
      } else {
        ++it;
      }
    }
    return completed;
  }

  // Proposal completion wakes the owner immediately; volatile observation and
  // retry deadlines still get the ordinary bounded poll. The cancellable
  // timer stays in this frame until its CQE arrives, so wakeups never abandon
  // an awaitable or retain a stale coroutine handle.
  bycorf::Task<absl::Status> Wait(std::chrono::milliseconds interval) {
    for (const auto& [group, entry] : entries_)
      if (entry->result_) co_return absl::OkStatus();
    auto timer =
        bycorf::CancellableSleepFor(*bycorf::ThisWorker().self_, interval);
    notified_ = false;
    timer_ = timer.CancelHandle();
    auto status = co_await timer;
    timer_ = {};
    co_return notified_ ? absl::OkStatus() : status;
  }

  bycorf::Task<absl::Status> Drain() {
    for (;;) {
      TakeCompleted();
      if (entries_.empty()) co_return absl::OkStatus();
      co_await completed_.Wait();
    }
  }

 private:
  bycorf::Task<absl::Status> Propose(MetaLeaderContext* context,
                                     std::shared_ptr<Entry> entry) {
    entry->result_.emplace(co_await context->Propose(entry->command_));
    notified_ = true;
    timer_.Cancel();
    completed_.NotifyAll(*bycorf::ThisWorker().self_);
    co_return absl::OkStatus();
  }

  std::map<std::string, std::shared_ptr<Entry>, std::less<>> entries_;
  bycorf::AsyncNotification completed_;
  bycorf::TimerCancelHandle timer_;
  bool notified_ = false;
};

}  // namespace lavik::meta::detail
