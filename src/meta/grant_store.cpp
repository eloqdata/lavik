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
    const Entry& entry, const ActivateAuthority& command,
    std::uint64_t committed_index,
    const std::optional<MetaFailoverActionId>& activation_action_id) {
  (void)committed_index;
  return !entry.fenced_ && entry.grant_.has_value() &&
         entry.grant_->owner_ == command.new_owner_ &&
         entry.grant_->term_ == command.expected_term_ &&
         entry.grant_->authority_version_ == command.new_authority_version_ &&
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

absl::Status MetaGrantStore::ValidateActivate(
    const keylane::meta::ActivateAuthority& command,
    std::uint64_t committed_index,
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
  const Entry& entry = it->second;
  // Cross-term activation is rejected: expected_term must equal the current
  // term.
  if (command.expected_term_ != entry.group_term_) {
    return MetaDomainRejectError("expected term does not match current term");
  }
  if (GrantMatches(entry, command, committed_index, activation_action_id)) {
    return absl::OkStatus();  // replay: already installed, idempotent accept
  }
  if (command.new_authority_version_ <= entry.last_authority_version_) {
    return MetaDomainRejectError("authority version must strictly increase");
  }
  if (committed_index == 0 || committed_index <= entry.last_grant_revision_) {
    return MetaDomainRejectError("grant revision must strictly increase");
  }
  return absl::OkStatus();
}

absl::Status MetaGrantStore::ApplyGrantPart(
    const keylane::meta::ActivateAuthority& command,
    std::uint64_t committed_index,
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
  Entry& entry = it->second;
  if (GrantMatches(entry, command, committed_index, activation_action_id)) {
    return absl::OkStatus();  // replay no-op
  }
  if (committed_index == 0 || committed_index <= entry.last_grant_revision_) {
    FatalGrantContractViolation("grant revision mismatch", command.group_id_);
  }
  MetaGroupGrant grant;
  grant.owner_ = command.new_owner_;
  grant.term_ = entry.group_term_;  // activate never moves the term
  grant.authority_version_ = command.new_authority_version_;
  grant.grant_revision_ = committed_index;
  grant.activation_action_id_ = std::move(activation_action_id);
  entry.last_authority_version_ = command.new_authority_version_;
  entry.last_grant_revision_ = committed_index;
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
  state.last_grant_revision_ = entry.last_grant_revision_;
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
    w.WriteU64(entry.last_authority_version_);
    w.WriteU64(entry.last_grant_revision_);
    w.WriteBool(entry.fenced_);
    w.WriteOptional(entry.grant_, [](MetaWriter& ww, const MetaGroupGrant& g) {
      ww.WriteString(g.owner_);
      ww.WriteU64(g.term_);
      ww.WriteU64(g.authority_version_);
      ww.WriteU64(g.grant_revision_);
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
    Entry entry;
    auto term = r.ReadU64();
    if (!term.ok()) return term.status();
    entry.group_term_ = *term;
    auto last_av = r.ReadU64();
    if (!last_av.ok()) return last_av.status();
    entry.last_authority_version_ = *last_av;
    auto last_grant_revision = r.ReadU64();
    if (!last_grant_revision.ok()) return last_grant_revision.status();
    entry.last_grant_revision_ = *last_grant_revision;
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
      auto grant_revision = rr.ReadU64();
      if (!grant_revision.ok()) {
        return absl::StatusOr<MetaGroupGrant>(grant_revision.status());
      }
      g.grant_revision_ = *grant_revision;
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
    if (entry.fenced_ == entry.grant_.has_value()) {
      return MetaFailStopError("fenced/no-grant invariant violated");
    }
    if (entry.grant_.has_value() &&
        (entry.grant_->term_ == 0 || entry.grant_->term_ != entry.group_term_ ||
         entry.grant_->authority_version_ != entry.last_authority_version_)) {
      return MetaFailStopError("grant term/version inconsistent with entry");
    }
    if (entry.grant_.has_value() &&
        (entry.grant_->grant_revision_ == 0 ||
         entry.grant_->grant_revision_ != entry.last_grant_revision_)) {
      return MetaFailStopError("grant revision inconsistent with entry");
    }
    if (entry.grant_.has_value()) {
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
