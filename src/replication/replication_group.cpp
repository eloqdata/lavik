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

#include "lavik/replication_group.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/memory.h"
#include "lavik/population_manifest_format.h"

namespace lavik {
namespace {

using DigestContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

absl::StatusOr<PopulationManifestId> HashManifest(
    std::span<const PopulationManifestEntry> entries) {
  DigestContext context(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
  if (context == nullptr ||
      EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
    return absl::InternalError("failed to initialize population manifest hash");
  }
  std::vector<PopulationManifestDigestEntry> digest_entries;
  digest_entries.reserve(entries.size());
  for (const PopulationManifestEntry& entry : entries) {
    digest_entries.push_back({entry.partition_id_, entry.logical_epoch_});
  }
  const std::string digest_input =
      EncodePopulationManifestDigestInput(digest_entries);
  if (EVP_DigestUpdate(context.get(), digest_input.data(),
                       digest_input.size()) != 1) {
    return absl::InternalError("failed to hash population manifest");
  }

  PopulationManifestId id;
  unsigned int digest_size = 0;
  if (EVP_DigestFinal_ex(context.get(), id.bytes_.data(), &digest_size) != 1 ||
      digest_size != id.bytes_.size()) {
    return absl::InternalError("failed to finish population manifest hash");
  }
  return id;
}

bool IsEmpty(const PopulationManifestId& id) {
  return std::all_of(id.bytes_.begin(), id.bytes_.end(),
                     [](std::uint8_t value) { return value == 0; });
}

absl::Status ValidateIdentity(const RebuildIdentity& identity,
                              bool source_less) {
  if (identity.group_id_.empty() || identity.assignment_id_.empty() ||
      identity.authority_id_.empty() || identity.target_node_id_.empty() ||
      identity.target_boot_id_.empty() || identity.operation_id_.empty() ||
      identity.directive_id_.empty() || identity.attempt_id_.empty()) {
    return absl::InvalidArgumentError(
        "population identity fields must all be nonempty");
  }
  const bool source_fields_empty = identity.source_node_id_.empty() &&
                                   identity.source_assignment_id_.empty() &&
                                   identity.source_boot_id_.empty() &&
                                   identity.source_history_id_.empty();
  if (source_less) {
    if (!source_fields_empty || identity.target_history_id_.empty()) {
      return absl::InvalidArgumentError(
          "empty population must bind target history and have no source");
    }
  } else if (identity.source_node_id_.empty() ||
             identity.source_assignment_id_.empty() ||
             identity.source_boot_id_.empty() ||
             identity.source_history_id_.empty() ||
             !identity.target_history_id_.empty()) {
    return absl::InvalidArgumentError(
        "rebuild must bind every source field and no target history");
  }
  if (identity.term_ == 0) {
    return absl::InvalidArgumentError("rebuild term must be nonzero");
  }
  if (identity.directive_revision_ == 0) {
    return absl::InvalidArgumentError(
        "rebuild directive revision must be nonzero");
  }
  if (identity.manifest_revision_ == 0) {
    return absl::InvalidArgumentError(
        "population manifest revision must be nonzero");
  }
  if (IsEmpty(identity.manifest_id_)) {
    return absl::InvalidArgumentError("population manifest ID must be nonzero");
  }
  return absl::OkStatus();
}

bool SameDirectiveRevisionScope(const RebuildIdentity& left,
                                const RebuildIdentity& right) {
  return left.group_id_ == right.group_id_ &&
         left.assignment_id_ == right.assignment_id_ &&
         left.term_ == right.term_ &&
         left.directive_revision_ == right.directive_revision_ &&
         left.authority_id_ == right.authority_id_ &&
         left.source_node_id_ == right.source_node_id_ &&
         left.source_assignment_id_ == right.source_assignment_id_ &&
         left.source_boot_id_ == right.source_boot_id_ &&
         left.source_history_id_ == right.source_history_id_ &&
         left.target_node_id_ == right.target_node_id_ &&
         left.target_boot_id_ == right.target_boot_id_ &&
         left.target_history_id_ == right.target_history_id_ &&
         left.operation_id_ == right.operation_id_ &&
         left.directive_id_ == right.directive_id_ &&
         left.manifest_revision_ == right.manifest_revision_ &&
         left.manifest_id_ == right.manifest_id_ &&
         left.partition_replication_epoch_ ==
             right.partition_replication_epoch_;
}

bool IsNewerDirectiveVersion(const RebuildIdentity& candidate,
                             const RebuildIdentity& accepted) {
  return candidate.term_ > accepted.term_ ||
         (candidate.term_ == accepted.term_ &&
          candidate.directive_revision_ > accepted.directive_revision_);
}

}  // namespace

std::string PopulationManifestId::Hex() const {
  constexpr char kHex[] = "0123456789abcdef";
  std::string result(bytes_.size() * 2, '0');
  for (std::size_t index = 0; index < bytes_.size(); ++index) {
    result[index * 2] = kHex[bytes_[index] >> 4];
    result[index * 2 + 1] = kHex[bytes_[index] & 0x0f];
  }
  return result;
}

absl::StatusOr<PopulationManifest> PopulationManifest::Create(
    std::vector<PopulationManifestEntry> entries) {
  if (entries.size() > kReplicationPartitionCount) {
    return absl::InvalidArgumentError(
        "population manifest cannot contain more than 16384 partitions");
  }
  for (const PopulationManifestEntry& entry : entries) {
    if (entry.partition_id_ >= kReplicationPartitionCount) {
      return absl::InvalidArgumentError(
          "population manifest partition is out of range");
    }
    if (entry.logical_epoch_ == 0) {
      return absl::InvalidArgumentError(
          "population manifest logical epoch must be nonzero");
    }
  }
  std::sort(entries.begin(), entries.end(),
            [](const PopulationManifestEntry& left,
               const PopulationManifestEntry& right) {
              return left.partition_id_ < right.partition_id_;
            });
  for (std::size_t index = 1; index < entries.size(); ++index) {
    if (entries[index - 1].partition_id_ == entries[index].partition_id_) {
      return absl::InvalidArgumentError(
          "population manifest contains a duplicate partition");
    }
  }

  auto id = HashManifest(entries);
  if (!id.ok()) return id.status();
  std::array<std::uint64_t, kReplicationPartitionCount> logical_epochs{};
  for (const PopulationManifestEntry& entry : entries) {
    logical_epochs[entry.partition_id_] = entry.logical_epoch_;
  }
  return PopulationManifest(std::move(logical_epochs), *id);
}

class ReplicationGroup::Impl {
 public:
  Impl(std::string local_node_id, std::string local_boot_id)
      : local_node_id_(std::move(local_node_id)),
        local_boot_id_(std::move(local_boot_id)) {}

  absl::Status ValidateRebuild(const RebuildDirective& directive,
                               const PopulationManifest& manifest) const {
    return ValidatePopulation(directive, manifest, false);
  }

  absl::StatusOr<std::uint64_t> NextDirectiveRevision(
      std::uint64_t term) const {
    if (term == 0) {
      return absl::InvalidArgumentError("rebuild term must be nonzero");
    }
    if (!last_directive_.has_value() ||
        term > last_directive_->identity_.term_) {
      return 1;
    }
    if (term < last_directive_->identity_.term_) {
      return absl::FailedPreconditionError("stale rebuild term");
    }
    const std::uint64_t accepted_revision =
        last_directive_->identity_.directive_revision_;
    if (accepted_revision == std::numeric_limits<std::uint64_t>::max()) {
      return absl::OutOfRangeError("rebuild directive revision cannot advance");
    }
    return accepted_revision + 1;
  }

  absl::Status ValidateEmptyPopulation(
      const RebuildIdentity& identity,
      const PopulationManifest& manifest) const {
    return ValidatePopulation(RebuildDirective{.identity_ = identity}, manifest,
                              true);
  }

  absl::Status ValidatePopulation(const RebuildDirective& directive,
                                  const PopulationManifest& manifest,
                                  bool source_less) const {
    if (state_ == ReplicationGroupState::kFailedStopped) {
      return absl::FailedPreconditionError(
          "replication group is failure-latched for this boot");
    }
    absl::Status identity_status =
        ValidateIdentity(directive.identity_, source_less);
    if (!identity_status.ok()) return identity_status;
    if (local_node_id_.empty() || local_boot_id_.empty()) {
      return absl::FailedPreconditionError(
          "replication group local node identity is invalid");
    }
    if (directive.identity_.target_node_id_ != local_node_id_ ||
        directive.identity_.target_boot_id_ != local_boot_id_) {
      return absl::FailedPreconditionError(
          "rebuild directive belongs to a different target boot");
    }
    if (directive.identity_.manifest_id_ != manifest.id()) {
      return absl::FailedPreconditionError(
          "rebuild directive does not match the supplied manifest");
    }
    if (source_less) {
      if (directive.flow_count_ != 0 || directive.safe_source_active_) {
        return absl::InvalidArgumentError(
            "empty population cannot carry source-flow authorization");
      }
    } else {
      if (directive.flow_count_ == 0 ||
          directive.flow_count_ > kMaxMemoryWorkers) {
        return absl::InvalidArgumentError(
            "rebuild flow count must be within the supported worker domain");
      }
      if (!directive.safe_source_active_) {
        return absl::FailedPreconditionError(
            "destructive reset requires a safely active source");
      }
    }
    if (last_directive_.has_value() &&
        last_directive_->identity_.group_id_ != directive.identity_.group_id_) {
      return absl::FailedPreconditionError(
          "this node is already assigned to another replication group");
    }

    if (state_ == ReplicationGroupState::kRebuilding) {
      if (current_directive_.has_value() && *current_directive_ == directive) {
        return absl::OkStatus();
      }
      if (!current_directive_.has_value() ||
          !IsNewerDirectiveVersion(directive.identity_,
                                   current_directive_->identity_)) {
        return absl::FailedPreconditionError(
            "another rebuild attempt is already active");
      }
    }
    if (state_ == ReplicationGroupState::kReady &&
        last_directive_.has_value() &&
        !IsNewerDirectiveVersion(directive.identity_,
                                 last_directive_->identity_)) {
      return absl::FailedPreconditionError(
          "the ready population already completed this directive version");
    }

    if (last_directive_.has_value()) {
      const RebuildIdentity& accepted = last_directive_->identity_;
      if (directive.identity_.term_ < accepted.term_) {
        return absl::FailedPreconditionError("stale rebuild term");
      }
      if (directive.identity_.term_ == accepted.term_ &&
          directive.identity_.directive_revision_ <
              accepted.directive_revision_) {
        return absl::FailedPreconditionError(
            "stale rebuild directive revision");
      }
      if (directive.identity_.term_ == accepted.term_ &&
          directive.identity_.directive_revision_ ==
              accepted.directive_revision_ &&
          (!SameDirectiveRevisionScope(directive.identity_, accepted) ||
           directive.flow_count_ != last_directive_->flow_count_ ||
           directive.safe_source_active_ !=
               last_directive_->safe_source_active_)) {
        return absl::FailedPreconditionError(
            "rebuild directive conflicts with the accepted revision");
      }
    }

    const AttemptKey attempt_key{directive.identity_.operation_id_,
                                 directive.identity_.attempt_id_};
    if (used_attempts_.contains(attempt_key)) {
      return absl::FailedPreconditionError(
          "rebuild attempt identity was already used in this boot");
    }

    return absl::OkStatus();
  }

  absl::StatusOr<DestructiveResetAuthorization> BeginRebuild(
      const RebuildDirective& directive, const PopulationManifest& manifest) {
    return BeginPopulation(directive, manifest, false);
  }

  absl::StatusOr<DestructiveResetAuthorization> BeginEmptyPopulation(
      const RebuildIdentity& identity, const PopulationManifest& manifest) {
    return BeginPopulation(RebuildDirective{.identity_ = identity}, manifest,
                           true);
  }

  absl::StatusOr<DestructiveResetAuthorization> BeginPopulation(
      const RebuildDirective& directive, const PopulationManifest& manifest,
      bool source_less) {
    absl::Status validated =
        ValidatePopulation(directive, manifest, source_less);
    if (!validated.ok()) return validated;
    if (state_ == ReplicationGroupState::kRebuilding) {
      if (current_directive_.has_value() && *current_directive_ == directive) {
        return DestructiveResetAuthorization(directive.identity_);
      }
      // Validation intentionally permits a newer directive to be prepared
      // while an attempt is active. Mutating the proof here would revoke its
      // reset capability before the caller has joined its worker tasks.
      return absl::FailedPreconditionError(
          "active rebuild must be retired before supersession");
    }

    if (!last_directive_.has_value() ||
        IsNewerDirectiveVersion(directive.identity_,
                                last_directive_->identity_)) {
      last_directive_ = directive;
    }
    const AttemptKey attempt_key{directive.identity_.operation_id_,
                                 directive.identity_.attempt_id_};
    used_attempts_.insert(attempt_key);
    current_directive_ = directive;
    std::copy(manifest.logical_epochs().begin(),
              manifest.logical_epochs().end(), desired_logical_epochs_.begin());
    target_local_epochs_.fill(0);
    partition_handoffs_.fill(false);
    partition_reset_count_ = 0;
    partition_handoff_count_ = 0;
    flow_cut_vector_.reset();
    function_catalog_complete_ = false;
    storage_promoted_ = false;
    ready_token_.reset();
    state_ = ReplicationGroupState::kRebuilding;
    return DestructiveResetAuthorization(directive.identity_);
  }

  absl::Status ValidateResetAuthorization(
      const RebuildIdentity& authorization_identity) const {
    return ValidateCurrent(authorization_identity);
  }

  absl::Status RecordPartitionReset(const RebuildIdentity& identity,
                                    std::uint32_t partition_id,
                                    std::uint64_t target_local_epoch) {
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    if (partition_id >= kReplicationPartitionCount) {
      return absl::InvalidArgumentError(
          "physical reset partition is out of range");
    }
    if (target_local_epoch == 0) {
      return absl::InvalidArgumentError(
          "target-local reset epoch must be nonzero");
    }
    std::uint64_t& saved_epoch = target_local_epochs_[partition_id];
    if (saved_epoch != 0) {
      if (saved_epoch == target_local_epoch) return absl::OkStatus();
      return absl::FailedPreconditionError(
          "a partition reset epoch cannot be rewritten");
    }
    saved_epoch = target_local_epoch;
    ++partition_reset_count_;
    return absl::OkStatus();
  }

  absl::Status RecordPartitionHandoff(const RebuildIdentity& identity,
                                      std::uint32_t partition_id,
                                      std::uint64_t logical_epoch,
                                      std::uint64_t target_local_epoch) {
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    if (partition_id >= kReplicationPartitionCount) {
      return absl::InvalidArgumentError(
          "physical handoff partition is out of range");
    }
    if (target_local_epoch == 0) {
      return absl::InvalidArgumentError(
          "target-local handoff epoch must be nonzero");
    }
    if (target_local_epochs_[partition_id] == 0) {
      return absl::FailedPreconditionError(
          "partition handoff requires a completed destructive reset");
    }
    if (target_local_epoch != target_local_epochs_[partition_id]) {
      return absl::FailedPreconditionError(
          "partition handoff does not match the saved target-local epoch");
    }
    if (logical_epoch != desired_logical_epochs_[partition_id]) {
      return absl::FailedPreconditionError(
          "partition handoff does not match the saved manifest epoch");
    }
    if (partition_handoffs_[partition_id]) return absl::OkStatus();
    partition_handoffs_[partition_id] = true;
    ++partition_handoff_count_;
    return absl::OkStatus();
  }

  absl::Status RecordFlowCutVector(
      const RebuildIdentity& identity,
      std::span<const std::uint64_t> stable_next_lsns) {
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    if (current_directive_->flow_count_ == 0) {
      return absl::FailedPreconditionError(
          "empty population has no source flow cut");
    }
    if (stable_next_lsns.size() != current_directive_->flow_count_) {
      return absl::InvalidArgumentError(
          "full-sync cut vector does not match the rebuild flow count");
    }
    if (std::any_of(stable_next_lsns.begin(), stable_next_lsns.end(),
                    [](std::uint64_t cut) { return cut == 0; })) {
      return absl::InvalidArgumentError(
          "full-sync stable next-LSNs must be nonzero");
    }
    if (flow_cut_vector_.has_value()) {
      if (std::equal(flow_cut_vector_->begin(), flow_cut_vector_->end(),
                     stable_next_lsns.begin(), stable_next_lsns.end())) {
        return absl::OkStatus();
      }
      return absl::FailedPreconditionError(
          "the full-sync cut vector cannot be rewritten");
    }
    flow_cut_vector_.emplace(stable_next_lsns.begin(), stable_next_lsns.end());
    return absl::OkStatus();
  }

  absl::Status MarkFunctionCatalogComplete(const RebuildIdentity& identity) {
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    function_catalog_complete_ = true;
    return absl::OkStatus();
  }

  absl::Status MarkStoragePromoted(const RebuildIdentity& identity) {
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    if (!CutProofComplete()) {
      return absl::FailedPreconditionError(
          "storage promotion requires complete manifest, catalog, and flow "
          "cuts");
    }
    storage_promoted_ = true;
    return absl::OkStatus();
  }

  absl::StatusOr<ReadyToken> RecoverPopulation(
      RebuildIdentity identity, std::vector<std::uint64_t> frontier) {
    if (state_ != ReplicationGroupState::kNotReady ||
        identity.target_node_id_ != local_node_id_ ||
        identity.target_boot_id_ != local_boot_id_ ||
        identity.group_id_.empty() || identity.assignment_id_.empty() ||
        identity.term_ == 0 || frontier.empty() ||
        std::ranges::any_of(frontier,
                            [](auto cursor) { return cursor == 0; })) {
      return absl::FailedPreconditionError(
          "recovered population is incomplete or already installed");
    }
    last_directive_ = RebuildDirective{
        .identity_ = identity,
        .flow_count_ = static_cast<std::uint32_t>(frontier.size())};
    ready_token_ = ReadyToken(std::move(identity), std::move(frontier));
    state_ = ReplicationGroupState::kReady;
    return *ready_token_;
  }

  absl::StatusOr<ReadyToken> PublishReady(const RebuildIdentity& identity) {
    if (state_ == ReplicationGroupState::kReady && ready_token_.has_value()) {
      if (ready_token_->identity() == identity) return *ready_token_;
      return absl::FailedPreconditionError(
          "ready population belongs to a different rebuild identity");
    }
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    if (!CutProofComplete() || !storage_promoted_) {
      return absl::FailedPreconditionError(
          "rebuild proof is incomplete and cannot publish readiness");
    }
    ready_token_ = ReadyToken(
        identity, flow_cut_vector_.value_or(std::vector<std::uint64_t>{}));
    state_ = ReplicationGroupState::kReady;
    return *ready_token_;
  }

  absl::Status Abort(const RebuildIdentity& identity) {
    if (state_ == ReplicationGroupState::kFailedStopped) {
      return absl::FailedPreconditionError(
          "replication group is failure-latched for this boot");
    }
    absl::Status current = ValidateCurrent(identity);
    if (!current.ok()) return current;
    DiscardCurrentProof();
    return absl::OkStatus();
  }

  absl::Status InvalidateProof(const RebuildIdentity& identity) {
    if (state_ == ReplicationGroupState::kFailedStopped) {
      return absl::FailedPreconditionError(
          "replication group is failure-latched for this boot");
    }
    const bool rebuilding = state_ == ReplicationGroupState::kRebuilding &&
                            current_directive_.has_value() &&
                            current_directive_->identity_ == identity;
    const bool ready = state_ == ReplicationGroupState::kReady &&
                       ready_token_.has_value() &&
                       ready_token_->identity() == identity;
    if (!rebuilding && !ready) {
      return absl::FailedPreconditionError(
          "population proof belongs to a different rebuild identity");
    }
    DiscardCurrentProof();
    return absl::OkStatus();
  }

  absl::Status FailStop(const RebuildIdentity& identity) {
    const bool rebuilding = state_ == ReplicationGroupState::kRebuilding &&
                            current_directive_.has_value() &&
                            current_directive_->identity_ == identity;
    const bool ready = state_ == ReplicationGroupState::kReady &&
                       ready_token_.has_value() &&
                       ready_token_->identity() == identity;
    if (!rebuilding && !ready) {
      return absl::FailedPreconditionError(
          "failure latch belongs to a different rebuild identity");
    }
    ready_token_.reset();
    state_ = ReplicationGroupState::kFailedStopped;
    return absl::OkStatus();
  }

  ReplicationGroupState state() const noexcept { return state_; }

 private:
  using AttemptKey = std::tuple<std::string, std::string>;

  void DiscardCurrentProof() {
    current_directive_.reset();
    desired_logical_epochs_.fill(0);
    target_local_epochs_.fill(0);
    partition_handoffs_.fill(false);
    partition_reset_count_ = 0;
    partition_handoff_count_ = 0;
    flow_cut_vector_.reset();
    function_catalog_complete_ = false;
    storage_promoted_ = false;
    ready_token_.reset();
    state_ = ReplicationGroupState::kNotReady;
    // Directive and attempt history deliberately survive proof loss so stale
    // control input cannot authorize another destructive reset.
  }

  absl::Status ValidateCurrent(const RebuildIdentity& identity) const {
    if (state_ != ReplicationGroupState::kRebuilding ||
        !current_directive_.has_value()) {
      return absl::FailedPreconditionError(
          "no matching rebuild attempt is active");
    }
    if (current_directive_->identity_ != identity) {
      return absl::FailedPreconditionError(
          "stale or mismatched rebuild identity");
    }
    return absl::OkStatus();
  }

  bool CutProofComplete() const {
    return partition_reset_count_ == kReplicationPartitionCount &&
           partition_handoff_count_ == kReplicationPartitionCount &&
           function_catalog_complete_ &&
           (current_directive_->flow_count_ == 0 ||
            flow_cut_vector_.has_value());
  }

  const std::string local_node_id_;
  const std::string local_boot_id_;
  ReplicationGroupState state_ = ReplicationGroupState::kNotReady;
  std::optional<RebuildDirective> last_directive_;
  std::set<AttemptKey> used_attempts_;
  std::optional<RebuildDirective> current_directive_;
  std::array<std::uint64_t, kReplicationPartitionCount>
      desired_logical_epochs_{};
  // Zero is reserved for "not reset", so each nonzero entry is both the reset
  // completion marker and the epoch that a later handoff must prove.
  std::array<std::uint64_t, kReplicationPartitionCount> target_local_epochs_{};
  std::array<bool, kReplicationPartitionCount> partition_handoffs_{};
  std::uint32_t partition_reset_count_ = 0;
  std::uint32_t partition_handoff_count_ = 0;
  std::optional<std::vector<std::uint64_t>> flow_cut_vector_;
  bool function_catalog_complete_ = false;
  bool storage_promoted_ = false;
  std::optional<ReadyToken> ready_token_;
};

ReplicationGroup::ReplicationGroup(std::string local_node_id,
                                   std::string local_boot_id)
    : impl_(std::make_unique<Impl>(std::move(local_node_id),
                                   std::move(local_boot_id))) {}

ReplicationGroup::~ReplicationGroup() = default;

absl::Status ReplicationGroup::ValidateRebuild(
    const RebuildDirective& directive,
    const PopulationManifest& manifest) const {
  return impl_->ValidateRebuild(directive, manifest);
}

absl::StatusOr<std::uint64_t> ReplicationGroup::NextDirectiveRevision(
    std::uint64_t term) const {
  return impl_->NextDirectiveRevision(term);
}

absl::StatusOr<DestructiveResetAuthorization> ReplicationGroup::BeginRebuild(
    const RebuildDirective& directive, const PopulationManifest& manifest) {
  return impl_->BeginRebuild(directive, manifest);
}

absl::Status ReplicationGroup::ValidateEmptyPopulation(
    const RebuildIdentity& identity, const PopulationManifest& manifest) const {
  return impl_->ValidateEmptyPopulation(identity, manifest);
}

absl::StatusOr<DestructiveResetAuthorization>
ReplicationGroup::BeginEmptyPopulation(const RebuildIdentity& identity,
                                       const PopulationManifest& manifest) {
  return impl_->BeginEmptyPopulation(identity, manifest);
}

absl::Status ReplicationGroup::ValidateResetAuthorization(
    const DestructiveResetAuthorization& authorization) const {
  return impl_->ValidateResetAuthorization(authorization.identity_);
}

absl::Status ReplicationGroup::RecordFlowCutVector(
    const RebuildIdentity& identity,
    std::span<const std::uint64_t> stable_next_lsns) {
  return impl_->RecordFlowCutVector(identity, stable_next_lsns);
}

absl::Status ReplicationGroup::RecordPartitionReset(
    const RebuildIdentity& identity, std::uint32_t partition_id,
    std::uint64_t target_local_epoch) {
  return impl_->RecordPartitionReset(identity, partition_id,
                                     target_local_epoch);
}

absl::Status ReplicationGroup::RecordPartitionHandoff(
    const RebuildIdentity& identity, std::uint32_t partition_id,
    std::uint64_t logical_epoch, std::uint64_t target_local_epoch) {
  return impl_->RecordPartitionHandoff(identity, partition_id, logical_epoch,
                                       target_local_epoch);
}

absl::Status ReplicationGroup::MarkFunctionCatalogComplete(
    const RebuildIdentity& identity) {
  return impl_->MarkFunctionCatalogComplete(identity);
}

absl::Status ReplicationGroup::MarkStoragePromoted(
    const RebuildIdentity& identity) {
  return impl_->MarkStoragePromoted(identity);
}

absl::StatusOr<ReadyToken> ReplicationGroup::RecoverPopulation(
    RebuildIdentity identity, std::vector<std::uint64_t> frontier) {
  return impl_->RecoverPopulation(std::move(identity), std::move(frontier));
}

absl::StatusOr<ReadyToken> ReplicationGroup::PublishReady(
    const RebuildIdentity& identity) {
  return impl_->PublishReady(identity);
}

absl::Status ReplicationGroup::Abort(const RebuildIdentity& identity) {
  return impl_->Abort(identity);
}

absl::Status ReplicationGroup::InvalidateProof(
    const RebuildIdentity& identity) {
  return impl_->InvalidateProof(identity);
}

absl::Status ReplicationGroup::FailStop(const RebuildIdentity& identity) {
  return impl_->FailStop(identity);
}

ReplicationGroupState ReplicationGroup::state() const noexcept {
  return impl_->state();
}

}  // namespace lavik
