#include "meta/meta_grant_store.h"

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

}  // namespace

bool MetaGrantStore::GrantMatches(const Entry& entry,
                                  const ActivateAuthority& command) {
  return !entry.fenced_ && entry.grant_.has_value() &&
         entry.grant_->owner_ == command.new_owner_ &&
         entry.grant_->term_ == command.expected_term_ &&
         entry.grant_->authority_version_ == command.new_authority_version_ &&
         entry.grant_->spec_ == command.grant_;
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
  groups_.emplace(group_id, Entry{});
  return absl::OkStatus();
}

absl::Status MetaGrantStore::RemoveGroup(std::string_view group_id) {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) {
    return absl::OkStatus();  // already gone: idempotent no-op
  }
  if (it->second.grant_.has_value()) {
    // A live grant must be revoked/fenced first; dropping the entry would
    // lose the authority fact (PolicyInUse, obs freshness).
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
  Entry& entry = it->second;
  // Terms advance exactly one step per command: T-1 -> T.
  if (command.expected_term_ == std::numeric_limits<std::uint64_t>::max() ||
      command.new_term_ != command.expected_term_ + 1) {
    return MetaDomainRejectError("new term must be exactly expected term + 1");
  }
  if (command.expected_term_ != entry.group_term_) {
    // Replay: the promotion already happened (term == new, fenced, no grant)
    // — idempotent no-op accept. Any other mismatch is a CAS conflict.
    if (entry.group_term_ == command.new_term_ && entry.fenced_ &&
        !entry.grant_.has_value()) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError("expected term does not match current term");
  }
  entry.group_term_ = command.new_term_;
  entry.grant_.reset();
  entry.fenced_ = true;
  return absl::OkStatus();
}

absl::Status MetaGrantStore::GrantAuthority(
    const keylane::meta::GrantAuthority& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  Entry& entry = it->second;
  if (!entry.grant_.has_value()) {
    return MetaDomainRejectError("group has no active grant to renew");
  }
  MetaGroupGrant& grant = *entry.grant_;
  if (grant.owner_ != command.node_id_) {
    return MetaDomainRejectError("grant renewal must come from the owner");
  }
  if (command.term_ != grant.term_ ||
      command.authority_version_ != grant.authority_version_) {
    return MetaDomainRejectError(
        "grant CAS token (term/authority_version) mismatch");
  }
  // Same-owner renewal: only the lease parameters and policy reference move.
  // Naturally replay-idempotent: term/authority_version do not change, so a
  // replay installs the same spec again.
  grant.spec_ = command.grant_;
  return absl::OkStatus();
}

absl::Status MetaGrantStore::ValidateActivate(
    const keylane::meta::ActivateAuthority& command) const {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  const Entry& entry = it->second;
  // Cross-term activation is rejected: expected_term must equal the current
  // term.
  if (command.expected_term_ != entry.group_term_) {
    return MetaDomainRejectError("expected term does not match current term");
  }
  if (GrantMatches(entry, command)) {
    return absl::OkStatus();  // replay: already installed, idempotent accept
  }
  if (command.new_authority_version_ <= entry.last_authority_version_) {
    return MetaDomainRejectError("authority version must strictly increase");
  }
  return absl::OkStatus();
}

absl::Status MetaGrantStore::ApplyGrantPart(
    const keylane::meta::ActivateAuthority& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end() || command.expected_term_ != it->second.group_term_) {
    FatalGrantContractViolation("term mismatch", command.group_id_);
  }
  Entry& entry = it->second;
  if (GrantMatches(entry, command)) {
    return absl::OkStatus();  // replay no-op
  }
  MetaGroupGrant grant;
  grant.owner_ = command.new_owner_;
  grant.term_ = entry.group_term_;  // activate never moves the term
  grant.authority_version_ = command.new_authority_version_;
  grant.spec_ = command.grant_;
  entry.last_authority_version_ = command.new_authority_version_;
  entry.grant_ = std::move(grant);
  entry.fenced_ = false;
  return absl::OkStatus();
}

absl::Status MetaGrantStore::RevokeGrant(
    const keylane::meta::RevokeGrant& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  Entry& entry = it->second;
  if (command.expected_term_ != entry.group_term_) {
    return MetaDomainRejectError("expected term does not match current term");
  }
  // Naturally replay-idempotent: a replay sees the same term and re-applies
  // the already-present effect (no grant, fenced).
  entry.grant_.reset();
  entry.fenced_ = true;
  return absl::OkStatus();
}

absl::Status MetaGrantStore::FenceGroup(
    const keylane::meta::FenceGroup& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError("unknown group");
  }
  Entry& entry = it->second;
  if (command.expected_term_ != entry.group_term_) {
    return MetaDomainRejectError("expected term does not match current term");
  }
  entry.grant_.reset();
  entry.fenced_ = true;
  return absl::OkStatus();
}

std::optional<MetaGroupGrantState> MetaGrantStore::GroupState(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  const Entry& entry = it->second;
  MetaGroupGrantState state;
  state.group_term_ = entry.group_term_;
  state.last_authority_version_ = entry.last_authority_version_;
  state.grant_ = entry.grant_;
  state.fenced_ = entry.fenced_;
  return state;
}

std::optional<std::uint64_t> MetaGrantStore::CurrentGroupTerm(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  return it->second.group_term_;
}

bool MetaGrantStore::PolicyInUse(std::string_view policy_id,
                                 std::uint64_t version) const {
  for (const auto& [group_id, entry] : groups_) {
    if (entry.grant_.has_value() &&
        entry.grant_->spec_.policy_id_ == policy_id &&
        entry.grant_->spec_.policy_version_ == version) {
      return true;
    }
  }
  return false;
}

absl::StatusOr<std::string> MetaGrantStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(groups_.size()));
  for (const auto& [group_id, entry] : groups_) {
    w.WriteString(group_id);
    w.WriteU64(entry.group_term_);
    w.WriteU64(entry.last_authority_version_);
    w.WriteBool(entry.fenced_);
    w.WriteOptional(entry.grant_, [](MetaWriter& ww, const MetaGroupGrant& g) {
      ww.WriteString(g.owner_);
      ww.WriteU64(g.term_);
      ww.WriteU64(g.authority_version_);
      ww.WriteU64(g.spec_.lease_duration_ms_);
      ww.WriteString(g.spec_.policy_id_);
      ww.WriteU64(g.spec_.policy_version_);
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
    Entry entry;
    auto term = r.ReadU64();
    if (!term.ok()) return term.status();
    entry.group_term_ = *term;
    auto last_av = r.ReadU64();
    if (!last_av.ok()) return last_av.status();
    entry.last_authority_version_ = *last_av;
    auto fenced = r.ReadBool("fenced tag must be 0 or 1");
    if (!fenced.ok()) return fenced.status();
    entry.fenced_ = *fenced;
    auto grant = r.ReadOptional<MetaGroupGrant>([](MetaReader& rr) {
      MetaGroupGrant g;
      auto owner = rr.ReadString(kMetaNodeIdBytes);
      if (!owner.ok()) return absl::StatusOr<MetaGroupGrant>(owner.status());
      g.owner_ = std::string(*owner);
      auto term = rr.ReadU64();
      if (!term.ok()) return absl::StatusOr<MetaGroupGrant>(term.status());
      g.term_ = *term;
      auto av = rr.ReadU64();
      if (!av.ok()) return absl::StatusOr<MetaGroupGrant>(av.status());
      g.authority_version_ = *av;
      auto lease = rr.ReadU64();
      if (!lease.ok()) return absl::StatusOr<MetaGroupGrant>(lease.status());
      g.spec_.lease_duration_ms_ = *lease;
      auto policy_id = rr.ReadString(kMaxMetaPolicyIdBytes);
      if (!policy_id.ok()) {
        return absl::StatusOr<MetaGroupGrant>(policy_id.status());
      }
      g.spec_.policy_id_ = std::string(*policy_id);
      auto policy_version = rr.ReadU64();
      if (!policy_version.ok()) {
        return absl::StatusOr<MetaGroupGrant>(policy_version.status());
      }
      g.spec_.policy_version_ = *policy_version;
      return absl::StatusOr<MetaGroupGrant>(std::move(g));
    });
    if (!grant.ok()) return grant.status();
    entry.grant_ = std::move(*grant);
    // The store's internal invariants are part of the durable format; a
    // violation is corruption, not a domain condition.
    if (entry.fenced_ == entry.grant_.has_value()) {
      return MetaFailStopError("fenced/no-grant invariant violated");
    }
    if (entry.grant_.has_value() &&
        (entry.grant_->term_ != entry.group_term_ ||
         entry.grant_->authority_version_ != entry.last_authority_version_)) {
      return MetaFailStopError("grant term/version inconsistent with entry");
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
