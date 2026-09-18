/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/meta/committed_status_view.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/state_apply.h"

namespace lavik::meta {

// Called after deterministic apply under the state mutex. It must only enqueue
// an event, never wait or reenter the state machine. Snapshot installation does
// not synthesize per-command events; consumers reload their committed view.
using MetaCommitEventSink =
    std::function<void(std::uint64_t, const MetaApplyResult&)>;

// The C++ state machine owns the six volatile committed stores. Go owns their
// durable WAL/snapshot recovery root, and replays only its committed prefix.
// Applied is never restored beyond the snapshot's actual state. Apply may run
// again after a crash; command idempotency remains a domain invariant.
//
// One application executor orders commit, Advance, Capture, and Install. The
// state mutex protects atomic views but never covers disk I/O or Raft entry.
// Capture checks its exact applied cut; install validates the complete
// candidate before replacing any store. Corrupt committed commands fail stop,
// while a decoded domain rejection consumes its index and produces an audit
// verdict.
class MetaStateMachine {
 public:
  static absl::StatusOr<std::unique_ptr<MetaStateMachine>> Open(
      const std::string& data_dir);
  static absl::StatusOr<std::shared_ptr<MetaRaftBuffer>> EncodeCommand(
      const MetaCommand& command);
  std::shared_ptr<MetaRaftBuffer> commit(std::uint64_t index,
                                         MetaRaftBuffer& data);
  void Advance(std::uint64_t index);
  absl::StatusOr<std::string> Capture(std::uint64_t index) const;
  absl::Status Install(std::uint64_t index, std::string_view image);
  MetaStores StoresSnapshot() const;
  MetaCommittedStatusView StatusSnapshot() const;
  void SetCommitEventSink(MetaCommitEventSink sink);
  std::uint64_t last_commit_index() const { return last_committed_idx_.load(); }
  std::uint64_t state_change_index() const noexcept {
    return last_state_change_idx_.load(std::memory_order_acquire);
  }
  std::optional<MetaOperationRecord> FindOperation(
      const MetaOperationId& id) const {
    std::lock_guard lock(mutex_);
    return stores_.operation_.FindOperation(id);
  }
  MetaClusterLifecycleState ClusterLifecycle() const {
    std::lock_guard lock(mutex_);
    return stores_.topology_.ClusterLifecycle();
  }
  std::optional<MetaNodeRecord> FindNode(const std::string& id) const {
    std::lock_guard lock(mutex_);
    return stores_.identity_.FindNode(id);
  }
  std::uint64_t consecutive_snapshot_failures() const {
    return consecutive_snapshot_failures_.load();
  }
  void SetSnapshotFailures(std::uint64_t count) {
    consecutive_snapshot_failures_.store(count);
  }
  std::shared_ptr<const std::vector<MetaMemberRecord>> MetaBindings() const {
    return meta_bindings_.load(std::memory_order_acquire);
  }

 private:
  MetaStateMachine() = default;
  mutable std::mutex mutex_;
  MetaStores stores_;
  std::mutex sink_mutex_;
  MetaCommitEventSink commit_event_sink_;
  std::atomic<std::uint64_t> last_committed_idx_{0};
  std::atomic<std::uint64_t> last_state_change_idx_{0};
  std::atomic<std::uint64_t> consecutive_snapshot_failures_{0};
  std::atomic<std::shared_ptr<const std::vector<MetaMemberRecord>>>
      meta_bindings_{std::make_shared<const std::vector<MetaMemberRecord>>()};
};

}  // namespace lavik::meta
