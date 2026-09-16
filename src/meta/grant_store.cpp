#include "keylane/meta/grant_store.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>

#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

// ValidateActivate/ApplyGrantPart contract violation: the apply layer ran the
// install without a successful validation against the current state. Same
// fail-stop policy as the audit store (spdlog::critical + abort).
[[noreturn]] void FatalGrantContractViolation(std::string_view what,
                                              std::string_view group_id) {
  spdlog::critical(
      "meta grant store: {} for group {}; ApplyGrantPart without a matching "
      "ValidateActivate is an apply-layer bug — aborting per fail-stop "
      "policy",
      what, group_id);
  std::abort();
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

}  // namespace

bool MetaGrantStore::GrantMatches(
    const MetaGroupGrantState& entry, const ActivateAuthority& command,
    const std::optional<MetaFailoverActionId>& activation_action_id) {
  return entry.group_term_ == command.expected_term_ &&
         entry.grant_.has_value() &&
         entry.grant_->owner_ == command.new_owner_ &&
         entry.grant_->activation_action_id_ == activation_action_id;
}

absl::Status MetaGrantStore::AddGroup(std::string_view group_id) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("group id is empty or exceeds its cap");
  }
  if (groups_.contains(std::string(group_id))) {
    return absl::OkStatus();  // idempotent
  }
  if (groups_.size() >= max_groups_) {
    return MetaDomainRejectError("group count cap reached");
  }
  groups_.emplace(group_id, MetaGroupGrantState{});
  return absl::OkStatus();
}

absl::Status MetaGrantStore::RemoveGroup(std::string_view group_id) {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) {
    return absl::OkStatus();  // already gone: idempotent no-op
  }
  if (it->second.grant_.has_value()) {
    // A live grant must be revoked/fenced first; dropping the entry would
    // lose the authority fact used by observation freshness.
    return MetaDomainRejectError("group still has an active grant");
  }
  groups_.erase(it);
  return absl::OkStatus();
}

absl::Status MetaGrantStore::BeginGroupTerm(
    const keylane::meta::BeginGroupTerm& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  MetaGroupGrantState& entry = it->second;
  // Terms advance exactly one step per command: T-1 -> T.
  if (command.expected_term_ == std::numeric_limits<std::uint64_t>::max() ||
      command.new_term_ != command.expected_term_ + 1) {
    return MetaDomainRejectError("new term must be exactly expected term + 1");
  }
  if (command.expected_term_ != entry.group_term_) {
    // Replay: the promotion already happened (term == new, fenced, no grant)
    // — idempotent no-op accept. Any other mismatch is a CAS conflict.
    if (entry.group_term_ == command.new_term_ && !entry.grant_.has_value()) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError("expected term does not match current term");
  }
  entry.group_term_ = command.new_term_;
  entry.grant_.reset();
  return absl::OkStatus();
}

absl::Status MetaGrantStore::ValidateActivate(
    const keylane::meta::ActivateAuthority& command,
    std::optional<MetaFailoverActionId> activation_action_id) const {
  if (activation_action_id.has_value() && IsZero(*activation_action_id)) {
    return MetaDomainRejectError("failover activation action id is zero");
  }
  if (command.expected_term_ == 0) {
    return MetaDomainRejectError(
        "authority activation requires a nonzero group term");
  }
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  const MetaGroupGrantState& entry = it->second;
  // Cross-term activation is rejected: expected_term must equal the current
  // term.
  if (command.expected_term_ != entry.group_term_) {
    return MetaDomainRejectError("expected term does not match current term");
  }
  if (GrantMatches(entry, command, activation_action_id)) {
    return absl::OkStatus();  // replay: already installed, idempotent accept
  }
  if (entry.grant_.has_value()) {
    return MetaDomainRejectError("group term already has an active grant");
  }
  return absl::OkStatus();
}

absl::Status MetaGrantStore::ApplyGrantPart(
    const keylane::meta::ActivateAuthority& command,
    std::optional<MetaFailoverActionId> activation_action_id) {
  if (activation_action_id.has_value() && IsZero(*activation_action_id)) {
    FatalGrantContractViolation("failover activation action id is zero",
                                command.group_id_);
  }
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end() || command.expected_term_ == 0 ||
      command.expected_term_ != it->second.group_term_) {
    FatalGrantContractViolation("term mismatch", command.group_id_);
  }
  MetaGroupGrantState& entry = it->second;
  if (GrantMatches(entry, command, activation_action_id)) {
    return absl::OkStatus();  // replay no-op
  }
  if (entry.grant_.has_value()) {
    FatalGrantContractViolation("group term already has an active grant",
                                command.group_id_);
  }
  MetaGroupGrant grant;
  grant.owner_ = command.new_owner_;
  grant.activation_action_id_ = std::move(activation_action_id);
  entry.grant_ = std::move(grant);
  return absl::OkStatus();
}

std::optional<MetaGroupGrantState> MetaGrantStore::GroupState(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  return it->second;
}

std::optional<std::uint64_t> MetaGrantStore::CurrentGroupTerm(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  return it->second.group_term_;
}

absl::StatusOr<std::string> MetaGrantStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(groups_.size()));
  for (const auto& [group_id, entry] : groups_) {
    if (entry.grant_.has_value()) {
      if (entry.grant_->activation_action_id_.has_value() &&
          IsZero(*entry.grant_->activation_action_id_)) {
        return MetaDomainRejectError(
            "grant store contains a zero failover activation action id");
      }
    }
    w.WriteString(group_id);
    w.WriteU64(entry.group_term_);
    w.WriteOptional(entry.grant_, [](MetaWriter& ww, const MetaGroupGrant& g) {
      ww.WriteString(g.owner_);
      ww.WriteOptional(g.activation_action_id_,
                       [](MetaWriter& www, const MetaFailoverActionId& id) {
                         WriteFixedArray(www, id);
                       });
    });
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaGrantStore> MetaGrantStore::Deserialize(
    std::string_view bytes, std::uint32_t max_groups) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported grant store schema version");
  }
  auto count = r.ReadCount(max_groups);
  if (!count.ok()) return count.status();

  MetaGrantStore store(max_groups);
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto group_id = r.ReadString(kMaxMetaGroupIdBytes);
    if (!group_id.ok()) return group_id.status();
    MetaGroupGrantState entry;
    auto term = r.ReadU64();
    if (!term.ok()) return term.status();
    entry.group_term_ = *term;
    auto grant = r.ReadOptional<MetaGroupGrant>([](MetaReader& rr) {
      MetaGroupGrant g;
      auto owner = rr.ReadString(kMetaNodeIdBytes);
      if (!owner.ok()) return absl::StatusOr<MetaGroupGrant>(owner.status());
      g.owner_ = std::string(*owner);
      auto activation_action_id = rr.ReadOptional<MetaFailoverActionId>(
          [](MetaReader& rrr) { return ReadFixedArray<16>(rrr); });
      if (!activation_action_id.ok()) {
        return absl::StatusOr<MetaGroupGrant>(activation_action_id.status());
      }
      g.activation_action_id_ = std::move(*activation_action_id);
      return absl::StatusOr<MetaGroupGrant>(std::move(g));
    });
    if (!grant.ok()) return grant.status();
    entry.grant_ = std::move(*grant);
    // The store's internal invariants are part of the durable format; a
    // violation is corruption, not a domain condition.
    if (entry.grant_.has_value()) {
      if (entry.group_term_ == 0) {
        return MetaFailStopError("active grant has a zero group term");
      }
      if (entry.grant_->activation_action_id_.has_value() &&
          IsZero(*entry.grant_->activation_action_id_)) {
        return MetaFailStopError("grant failover activation action id is zero");
      }
    }
    if (!store.groups_.emplace(std::string(*group_id), std::move(entry))
             .second) {
      return MetaFailStopError("duplicate group id in grant store snapshot");
    }
  }
  if (absl::Status status = r.Finish(); !status.ok()) return status;
  return store;
}

}  // namespace keylane::meta
