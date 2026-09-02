#include "tests/cluster/fault_controls.h"

#include <algorithm>

#include "absl/strings/str_cat.h"

namespace keylane::test::cluster {

absl::Status ManualClock::AdvanceTo(std::uint64_t time) {
  if (time < now_) {
    return absl::FailedPreconditionError("manual clock cannot move backward");
  }
  now_ = time;
  return absl::OkStatus();
}

std::pair<NodeId, NodeId> SimNetwork::Link(NodeId left, NodeId right) {
  return left < right ? std::pair{left, right} : std::pair{right, left};
}

std::uint64_t SimNetwork::Send(NodeId source, NodeId target,
                               std::string payload) {
  const std::uint64_t id = next_id_++;
  pending_.push_back(NetworkMessage{
      .id_ = id,
      .source_ = source,
      .target_ = target,
      .connection_generation_ = connection_generations_[Link(source, target)],
      .deliver_at_ = 0,
      .payload_ = std::move(payload)});
  return id;
}

void SimNetwork::Partition(NodeId left, NodeId right) {
  partitions_.insert(Link(left, right));
}

void SimNetwork::Heal(NodeId left, NodeId right) {
  partitions_.erase(Link(left, right));
}

void SimNetwork::Disconnect(NodeId left, NodeId right) {
  const auto link = Link(left, right);
  disconnected_.insert(link);
  ++connection_generations_[link];
}

void SimNetwork::Reconnect(NodeId left, NodeId right) {
  const auto link = Link(left, right);
  disconnected_.erase(link);
  // Messages queued while disconnected belong to a connection attempt that
  // never became live. A second generation boundary prevents them from
  // resurfacing when the link is reopened.
  ++connection_generations_[link];
}

absl::Status SimNetwork::Delay(std::uint64_t message_id,
                               std::uint64_t deliver_at) {
  auto found = std::find_if(
      pending_.begin(), pending_.end(),
      [&](const NetworkMessage& message) { return message.id_ == message_id; });
  if (found == pending_.end()) {
    return absl::NotFoundError("network message not found");
  }
  found->deliver_at_ = deliver_at;
  return absl::OkStatus();
}

std::vector<NetworkMessage> SimNetwork::Deliverable(std::uint64_t now) const {
  std::vector<NetworkMessage> result;
  for (const NetworkMessage& message : pending_) {
    const auto link = Link(message.source_, message.target_);
    const auto generation = connection_generations_.find(link);
    const std::uint64_t current_generation =
        generation == connection_generations_.end() ? 0 : generation->second;
    if (message.deliver_at_ <= now && !partitions_.contains(link) &&
        !disconnected_.contains(link) &&
        message.connection_generation_ == current_generation) {
      result.push_back(message);
    }
  }
  std::sort(result.begin(), result.end(),
            [](const NetworkMessage& left, const NetworkMessage& right) {
              return left.id_ < right.id_;
            });
  return result;
}

absl::StatusOr<NetworkMessage> SimNetwork::Deliver(std::uint64_t message_id,
                                                   std::uint64_t now) {
  auto found = std::find_if(
      pending_.begin(), pending_.end(),
      [&](const NetworkMessage& message) { return message.id_ == message_id; });
  if (found == pending_.end()) {
    return absl::NotFoundError("network message not found");
  }
  const auto link = Link(found->source_, found->target_);
  if (disconnected_.contains(link)) {
    return absl::FailedPreconditionError("network connection is closed");
  }
  if (found->connection_generation_ != connection_generations_[link]) {
    return absl::FailedPreconditionError(
        "network message is from a stale connection");
  }
  if (partitions_.contains(link)) {
    return absl::FailedPreconditionError("network link is partitioned");
  }
  if (found->deliver_at_ > now) {
    return absl::FailedPreconditionError("network message is delayed");
  }
  NetworkMessage delivered = std::move(*found);
  pending_.erase(found);
  return delivered;
}

absl::Status SimNetwork::Drop(std::uint64_t message_id) {
  auto found = std::find_if(
      pending_.begin(), pending_.end(),
      [&](const NetworkMessage& message) { return message.id_ == message_id; });
  if (found == pending_.end()) {
    return absl::NotFoundError("network message not found");
  }
  pending_.erase(found);
  return absl::OkStatus();
}

absl::StatusOr<std::uint64_t> SimNetwork::Duplicate(std::uint64_t message_id) {
  auto found = std::find_if(
      pending_.begin(), pending_.end(),
      [&](const NetworkMessage& message) { return message.id_ == message_id; });
  if (found == pending_.end()) {
    return absl::NotFoundError("network message not found");
  }
  const auto link = Link(found->source_, found->target_);
  if (disconnected_.contains(link) ||
      found->connection_generation_ != connection_generations_[link]) {
    return absl::FailedPreconditionError(
        "cannot duplicate a message from an inactive connection");
  }
  NetworkMessage duplicate = *found;
  duplicate.id_ = next_id_++;
  const std::uint64_t duplicate_id = duplicate.id_;
  pending_.push_back(std::move(duplicate));
  return duplicate_id;
}

absl::Status SimStorage::Write(std::string key, std::string value) {
  if (fail_next_write_) {
    fail_next_write_ = false;
    return absl::InternalError("injected storage write failure");
  }
  volatile_.insert_or_assign(std::move(key), std::move(value));
  return absl::OkStatus();
}

absl::Status SimStorage::Flush(std::string_view key) {
  if (fail_next_flush_) {
    fail_next_flush_ = false;
    return absl::InternalError("injected storage flush failure");
  }
  const auto found = volatile_.find(key);
  if (found == volatile_.end()) {
    return absl::NotFoundError("volatile storage key not found");
  }
  durable_.insert_or_assign(found->first, found->second);
  return absl::OkStatus();
}

absl::Status SimStorage::PersistPrefix(std::string_view key,
                                       std::size_t bytes) {
  const auto found = volatile_.find(key);
  if (found == volatile_.end()) {
    return absl::NotFoundError("volatile storage key not found");
  }
  durable_.insert_or_assign(found->first, found->second.substr(0, bytes));
  return absl::OkStatus();
}

void SimStorage::CrashAndRecover() { volatile_ = durable_; }

absl::StatusOr<std::optional<std::string>> SimStorage::Read(
    std::string_view key) {
  if (fail_next_read_) {
    fail_next_read_ = false;
    return absl::InternalError("injected storage read failure");
  }
  const auto found = volatile_.find(key);
  if (found == volatile_.end()) return std::nullopt;
  return found->second;
}

absl::StatusOr<std::optional<std::string>> SimStorage::ReadDurable(
    std::string_view key) {
  if (fail_next_read_) {
    fail_next_read_ = false;
    return absl::InternalError("injected storage read failure");
  }
  const auto found = durable_.find(key);
  if (found == durable_.end()) return std::nullopt;
  return found->second;
}

absl::Status ControlPlaneReferenceMachine::AppendCommitted(MetaEntry entry) {
  const std::uint64_t expected =
      applied_.empty() ? 1 : applied_.back().index_ + 1;
  if (entry.index_ != expected) {
    return absl::FailedPreconditionError("committed Meta index is not next");
  }
  if (!applied_.empty() && entry.term_ < applied_.back().term_) {
    return absl::FailedPreconditionError("committed Meta term regressed");
  }
  applied_.push_back(std::move(entry));
  return absl::OkStatus();
}

absl::Status ControlPlaneReferenceMachine::RestoreSnapshot(
    std::vector<MetaEntry> entries) {
  std::vector<MetaEntry> previous = std::move(applied_);
  applied_.clear();
  for (MetaEntry& entry : entries) {
    absl::Status appended = AppendCommitted(std::move(entry));
    if (!appended.ok()) {
      applied_ = std::move(previous);
      return appended;
    }
  }
  if (!previous.empty() &&
      (applied_.empty() || applied_.back().index_ < previous.back().index_)) {
    applied_ = std::move(previous);
    return absl::FailedPreconditionError("Meta snapshot regressed state");
  }
  return absl::OkStatus();
}

absl::Status ControlPlaneReferenceMachine::Replay(
    const std::vector<MetaEntry>& entries) {
  for (const MetaEntry& entry : entries) {
    if (entry.index_ <= applied_index()) {
      const auto existing = std::find_if(
          applied_.begin(), applied_.end(), [&](const MetaEntry& applied) {
            return applied.index_ == entry.index_;
          });
      if (existing == applied_.end() || *existing != entry) {
        return absl::FailedPreconditionError(
            "Meta replay forked committed log");
      }
      continue;
    }
    absl::Status appended = AppendCommitted(entry);
    if (!appended.ok()) return appended;
  }
  return absl::OkStatus();
}

std::uint64_t ControlPlaneReferenceMachine::applied_index() const noexcept {
  return applied_.empty() ? 0 : applied_.back().index_;
}

FaultController::FaultController(std::vector<FaultRule> rules)
    : rules_(std::move(rules)) {
  std::sort(rules_.begin(), rules_.end(),
            [](const FaultRule& left, const FaultRule& right) {
              if (left.checkpoint_ != right.checkpoint_) {
                return left.checkpoint_ < right.checkpoint_;
              }
              return left.occurrence_ < right.occurrence_;
            });
}

FaultDecision FaultController::Reach(std::string_view checkpoint) {
  const std::uint64_t occurrence = ++occurrences_[std::string(checkpoint)];
  acknowledgments_.push_back(absl::StrCat(checkpoint, "#", occurrence));
  const auto found =
      std::find_if(rules_.begin(), rules_.end(), [&](const FaultRule& rule) {
        return rule.checkpoint_ == checkpoint && rule.occurrence_ == occurrence;
      });
  if (found == rules_.end()) {
    return FaultDecision{.effect_ = FaultEffect::kContinue,
                         .occurrence_ = occurrence};
  }
  return FaultDecision{.effect_ = found->effect_,
                       .argument_ = found->argument_,
                       .occurrence_ = occurrence};
}

}  // namespace keylane::test::cluster
