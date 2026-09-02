#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "tests/cluster/reference_model.h"

namespace keylane::test::cluster {

class ManualClock {
 public:
  // Time is an opaque monotonic test tick. AdvanceTo rejects rollback.
  std::uint64_t now() const noexcept { return now_; }
  bool HasExpired(std::uint64_t deadline) const noexcept {
    return now_ >= deadline;
  }
  absl::Status AdvanceTo(std::uint64_t time);

 private:
  std::uint64_t now_ = 0;
};

struct NetworkMessage {
  std::uint64_t id_ = 0;
  NodeId source_;
  NodeId target_;
  std::uint64_t connection_generation_ = 0;
  std::uint64_t deliver_at_ = 0;
  std::string payload_;

  bool operator==(const NetworkMessage&) const = default;
};

// SimNetwork operates above a transport framing seam. It can reorder or
// duplicate logical envelopes. Disconnect and reconnect bracket a closed
// interval with generation changes, so traffic queued before or during the
// disconnect can never be delivered afterward.
class SimNetwork {
 public:
  // Send transfers payload ownership and assigns a stable increasing id.
  // Deliverable is sorted by id; Delay and partitions control membership.
  std::uint64_t Send(NodeId source, NodeId target, std::string payload);
  void Partition(NodeId left, NodeId right);
  void Heal(NodeId left, NodeId right);
  // Disconnect invalidates queued envelopes; Reconnect invalidates messages
  // sent while closed and permits only messages sent after reopening.
  void Disconnect(NodeId left, NodeId right);
  void Reconnect(NodeId left, NodeId right);
  absl::Status Delay(std::uint64_t message_id, std::uint64_t deliver_at);
  std::vector<NetworkMessage> Deliverable(std::uint64_t now = 0) const;
  absl::StatusOr<NetworkMessage> Deliver(std::uint64_t message_id,
                                         std::uint64_t now = 0);
  absl::Status Drop(std::uint64_t message_id);
  // Duplicate retains the original envelope and returns the new envelope id.
  absl::StatusOr<std::uint64_t> Duplicate(std::uint64_t message_id);

 private:
  static std::pair<NodeId, NodeId> Link(NodeId left, NodeId right);

  std::uint64_t next_id_ = 1;
  std::deque<NetworkMessage> pending_;
  std::set<std::pair<NodeId, NodeId>> partitions_;
  std::set<std::pair<NodeId, NodeId>> disconnected_;
  std::map<std::pair<NodeId, NodeId>, std::uint64_t> connection_generations_;
};

class SimStorage {
 public:
  // Writes become visible immediately but become crash-safe only after Flush.
  // FailNext* arms a one-shot error consumed by the next matching operation.
  absl::Status Write(std::string key, std::string value);
  absl::Status Flush(std::string_view key);
  absl::Status PersistPrefix(std::string_view key, std::size_t bytes);
  void CrashAndRecover();
  void FailNextWrite() noexcept { fail_next_write_ = true; }
  void FailNextRead() noexcept { fail_next_read_ = true; }
  void FailNextFlush() noexcept { fail_next_flush_ = true; }

  absl::StatusOr<std::optional<std::string>> Read(std::string_view key);
  absl::StatusOr<std::optional<std::string>> ReadDurable(std::string_view key);

 private:
  std::map<std::string, std::string, std::less<>> volatile_;
  std::map<std::string, std::string, std::less<>> durable_;
  bool fail_next_write_ = false;
  bool fail_next_read_ = false;
  bool fail_next_flush_ = false;
};

struct MetaEntry {
  std::uint64_t index_ = 0;
  GroupTerm term_;
  std::string payload_;

  bool operator==(const MetaEntry&) const = default;
};

// This is a committed-state reference machine, not a Raft implementation. It
// controls replay and snapshot installation while #19 remains the owner of
// consensus, WAL, and versioned production state.
class ControlPlaneReferenceMachine {
 public:
  // Entries must be contiguous with nondecreasing terms. Replay is idempotent
  // for identical committed entries and rejects forks; restore rejects a
  // snapshot older than already-applied state.
  absl::Status AppendCommitted(MetaEntry entry);
  void ChangeLeader(NodeId leader) noexcept { leader_ = leader; }
  std::vector<MetaEntry> Snapshot() const { return applied_; }
  absl::Status RestoreSnapshot(std::vector<MetaEntry> entries);
  absl::Status Replay(const std::vector<MetaEntry>& entries);

  NodeId leader() const noexcept { return leader_; }
  std::uint64_t applied_index() const noexcept;
  const std::vector<MetaEntry>& applied() const noexcept { return applied_; }

 private:
  NodeId leader_;
  std::vector<MetaEntry> applied_;
};

enum class FaultEffect : std::uint8_t {
  kContinue,
  kDelay,
  kFailBefore,
  kFailAfter,
  kPersistPrefix,
  kCrash,
};

struct FaultRule {
  std::string checkpoint_;
  std::uint64_t occurrence_ = 1;
  FaultEffect effect_ = FaultEffect::kContinue;
  std::uint64_t argument_ = 0;
};

struct FaultDecision {
  FaultEffect effect_ = FaultEffect::kContinue;
  std::uint64_t argument_ = 0;
  std::uint64_t occurrence_ = 0;
};

// Stable logical checkpoint names and occurrence counts let both simulated
// and real adapters consume the same fault vocabulary without exposing
// production protocol fields.
class FaultController {
 public:
  explicit FaultController(std::vector<FaultRule> rules);

  // Reach records every stable checkpoint occurrence, including those without
  // a matching rule, so traces can prove where a fault was (or was not) fired.
  FaultDecision Reach(std::string_view checkpoint);
  const std::vector<std::string>& acknowledgments() const noexcept {
    return acknowledgments_;
  }

 private:
  std::vector<FaultRule> rules_;
  std::map<std::string, std::uint64_t, std::less<>> occurrences_;
  std::vector<std::string> acknowledgments_;
};

}  // namespace keylane::test::cluster
