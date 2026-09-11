#include "keylane/meta/failover_recovery_store.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace keylane::meta {
namespace {

template <typename Array>
bool IsZero(const Array& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool IsCanonicalNodeId(std::string_view value) {
  return value.size() == kMetaNodeIdBytes &&
         std::all_of(value.begin(), value.end(), [](char ch) {
           return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         });
}

absl::Status ValidateCommand(const SetFailoverRecovery& command,
                             std::uint64_t committed_index) {
  if (committed_index == 0) {
    return MetaDomainRejectError("recovery revision must be nonzero");
  }
  if (command.group_id_.empty() ||
      command.group_id_.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("recovery group id is empty or exceeds cap");
  }
  if (command.recovery_generation_ == 0) {
    return MetaDomainRejectError("recovery generation must be nonzero");
  }
  if (!IsCanonicalNodeId(command.old_source_node_id_) ||
      IsZero(command.old_source_assignment_id_) ||
      IsZero(command.old_source_boot_incarnation_) ||
      IsZero(command.old_source_history_id_)) {
    return MetaDomainRejectError("recovery source identity is invalid");
  }
  if (command.excluded_authority_term_ == 0 ||
      command.excluded_authority_version_ == 0 ||
      command.excluded_grant_revision_ == 0) {
    return MetaDomainRejectError("excluded authority anchors must be nonzero");
  }
  if (command.population_manifest_revision_ == 0 ||
      IsZero(command.population_manifest_digest_) ||
      command.partition_replication_epoch_ == 0) {
    return MetaDomainRejectError("recovery population identity is invalid");
  }
  if (command.proof_state_ != MetaFailoverProofState::kPending &&
      command.proof_state_ != MetaFailoverProofState::kExact &&
      command.proof_state_ != MetaFailoverProofState::kUnavailable) {
    return MetaDomainRejectError("recovery proof state is invalid");
  }
  if ((command.proof_state_ == MetaFailoverProofState::kPending &&
       command.frozen_proof_.has_value()) ||
      (command.proof_state_ == MetaFailoverProofState::kExact &&
       !command.frozen_proof_.has_value())) {
    return MetaDomainRejectError(
        "recovery proof state and frozen source proof are inconsistent");
  }
  if (command.recovery_required_ && !command.hold_required_) {
    return MetaDomainRejectError(
        "recovery handoff must retain the source history hold");
  }
  if (command.frozen_proof_.has_value()) {
    const MetaFailoverFrozenProof& proof = *command.frozen_proof_;
    if (proof.final_next_lsns_.empty() ||
        proof.final_next_lsns_.size() > kMaxMetaFailoverRecoveryFlows ||
        std::any_of(proof.final_next_lsns_.begin(),
                    proof.final_next_lsns_.end(),
                    [](std::uint64_t next_lsn) { return next_lsn == 0; }) ||
        IsZero(proof.proof_hash_)) {
      return MetaDomainRejectError("frozen source proof is invalid");
    }
  }
  return absl::OkStatus();
}

bool ImmutableAnchorsMatch(const MetaFailoverRecoveryRecord& record,
                           const SetFailoverRecovery& command) {
  return record.group_id_ == command.group_id_ &&
         record.recovery_generation_ == command.recovery_generation_ &&
         record.old_source_node_id_ == command.old_source_node_id_ &&
         record.old_source_assignment_id_ ==
             command.old_source_assignment_id_ &&
         record.old_source_boot_incarnation_ ==
             command.old_source_boot_incarnation_ &&
         record.old_source_history_id_ == command.old_source_history_id_ &&
         record.excluded_authority_term_ == command.excluded_authority_term_ &&
         record.excluded_authority_version_ ==
             command.excluded_authority_version_ &&
         record.excluded_grant_revision_ == command.excluded_grant_revision_ &&
         record.population_manifest_revision_ ==
             command.population_manifest_revision_ &&
         record.population_manifest_digest_ ==
             command.population_manifest_digest_ &&
         record.partition_replication_epoch_ ==
             command.partition_replication_epoch_;
}

bool DesiredStateMatches(const MetaFailoverRecoveryRecord& record,
                         const SetFailoverRecovery& command,
                         std::uint64_t committed_index) {
  return record.revision_ == committed_index &&
         ImmutableAnchorsMatch(record, command) &&
         record.hold_required_ == command.hold_required_ &&
         record.recovery_required_ == command.recovery_required_ &&
         record.proof_state_ == command.proof_state_ &&
         record.frozen_proof_ == command.frozen_proof_;
}

MetaFailoverRecoveryRecord MakeRecord(const SetFailoverRecovery& command,
                                      std::uint64_t committed_index) {
  return MetaFailoverRecoveryRecord{
      .group_id_ = command.group_id_,
      .revision_ = committed_index,
      .recovery_generation_ = command.recovery_generation_,
      .old_source_node_id_ = command.old_source_node_id_,
      .old_source_assignment_id_ = command.old_source_assignment_id_,
      .old_source_boot_incarnation_ = command.old_source_boot_incarnation_,
      .old_source_history_id_ = command.old_source_history_id_,
      .excluded_authority_term_ = command.excluded_authority_term_,
      .excluded_authority_version_ = command.excluded_authority_version_,
      .excluded_grant_revision_ = command.excluded_grant_revision_,
      .population_manifest_revision_ = command.population_manifest_revision_,
      .population_manifest_digest_ = command.population_manifest_digest_,
      .partition_replication_epoch_ = command.partition_replication_epoch_,
      .hold_required_ = command.hold_required_,
      .recovery_required_ = command.recovery_required_,
      .proof_state_ = command.proof_state_,
      .frozen_proof_ = command.frozen_proof_};
}

void WriteRecord(MetaWriter& writer, const MetaFailoverRecoveryRecord& record) {
  writer.WriteU64(record.revision_);
  writer.WriteU64(record.recovery_generation_);
  writer.WriteString(record.old_source_node_id_);
  WriteFixedArray(writer, record.old_source_assignment_id_);
  WriteFixedArray(writer, record.old_source_boot_incarnation_);
  WriteFixedArray(writer, record.old_source_history_id_);
  writer.WriteU64(record.excluded_authority_term_);
  writer.WriteU64(record.excluded_authority_version_);
  writer.WriteU64(record.excluded_grant_revision_);
  writer.WriteU64(record.population_manifest_revision_);
  WriteFixedArray(writer, record.population_manifest_digest_);
  writer.WriteU64(record.partition_replication_epoch_);
  writer.WriteBool(record.hold_required_);
  writer.WriteBool(record.recovery_required_);
  writer.WriteU8(static_cast<std::uint8_t>(record.proof_state_));
  writer.WriteOptional(
      record.frozen_proof_,
      [](MetaWriter& proof_writer, const MetaFailoverFrozenProof& proof) {
        proof_writer.WriteList(
            proof.final_next_lsns_,
            [](MetaWriter& flow_writer, std::uint64_t next_lsn) {
              flow_writer.WriteU64(next_lsn);
            });
        WriteFixedArray(proof_writer, proof.proof_hash_);
      });
}

absl::StatusOr<MetaFailoverRecoveryRecord> ReadRecord(
    MetaReader& reader, std::string_view group_id) {
  MetaFailoverRecoveryRecord record;
  record.group_id_ = std::string(group_id);
  auto revision = reader.ReadU64();
  if (!revision.ok()) return revision.status();
  record.revision_ = *revision;
  auto generation = reader.ReadU64();
  if (!generation.ok()) return generation.status();
  record.recovery_generation_ = *generation;
  auto source = reader.ReadString(kMetaNodeIdBytes);
  if (!source.ok()) return source.status();
  record.old_source_node_id_ = std::string(*source);
  auto assignment = ReadFixedArray<16>(reader);
  if (!assignment.ok()) return assignment.status();
  record.old_source_assignment_id_ = *assignment;
  auto boot = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!boot.ok()) return boot.status();
  record.old_source_boot_incarnation_ = *boot;
  auto history = ReadFixedArray<kMetaReplicationHistoryIdBytes>(reader);
  if (!history.ok()) return history.status();
  record.old_source_history_id_ = *history;
  auto term = reader.ReadU64();
  if (!term.ok()) return term.status();
  record.excluded_authority_term_ = *term;
  auto authority = reader.ReadU64();
  if (!authority.ok()) return authority.status();
  record.excluded_authority_version_ = *authority;
  auto grant = reader.ReadU64();
  if (!grant.ok()) return grant.status();
  record.excluded_grant_revision_ = *grant;
  auto manifest_revision = reader.ReadU64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  record.population_manifest_revision_ = *manifest_revision;
  auto manifest_digest = ReadFixedArray<32>(reader);
  if (!manifest_digest.ok()) return manifest_digest.status();
  record.population_manifest_digest_ = *manifest_digest;
  auto partition_epoch = reader.ReadU64();
  if (!partition_epoch.ok()) return partition_epoch.status();
  record.partition_replication_epoch_ = *partition_epoch;
  auto hold = reader.ReadBool("recovery hold flag must be 0 or 1");
  if (!hold.ok()) return hold.status();
  record.hold_required_ = *hold;
  auto recovery = reader.ReadBool("recovery-required flag must be 0 or 1");
  if (!recovery.ok()) return recovery.status();
  record.recovery_required_ = *recovery;
  auto proof_state = reader.ReadU8();
  if (!proof_state.ok()) return proof_state.status();
  if (*proof_state <
          static_cast<std::uint8_t>(MetaFailoverProofState::kPending) ||
      *proof_state >
          static_cast<std::uint8_t>(MetaFailoverProofState::kUnavailable)) {
    return MetaFailStopError("invalid failover recovery proof state");
  }
  record.proof_state_ = static_cast<MetaFailoverProofState>(*proof_state);
  auto proof = reader.ReadOptional<MetaFailoverFrozenProof>(
      [](MetaReader& proof_reader) -> absl::StatusOr<MetaFailoverFrozenProof> {
        auto frontier = proof_reader.ReadList<std::uint64_t>(
            kMaxMetaFailoverRecoveryFlows,
            [](MetaReader& flow_reader) { return flow_reader.ReadU64(); });
        if (!frontier.ok()) return frontier.status();
        auto proof_hash = ReadFixedArray<32>(proof_reader);
        if (!proof_hash.ok()) return proof_hash.status();
        return MetaFailoverFrozenProof{.final_next_lsns_ = std::move(*frontier),
                                       .proof_hash_ = *proof_hash};
      });
  if (!proof.ok()) return proof.status();
  record.frozen_proof_ = std::move(*proof);
  return record;
}

SetFailoverRecovery ToCommand(const MetaFailoverRecoveryRecord& record) {
  return SetFailoverRecovery{
      .request_id_ = {},
      .actor_ = {},
      .group_id_ = record.group_id_,
      .expected_revision_ = 0,
      .recovery_generation_ = record.recovery_generation_,
      .old_source_node_id_ = record.old_source_node_id_,
      .old_source_assignment_id_ = record.old_source_assignment_id_,
      .old_source_boot_incarnation_ = record.old_source_boot_incarnation_,
      .old_source_history_id_ = record.old_source_history_id_,
      .excluded_authority_term_ = record.excluded_authority_term_,
      .excluded_authority_version_ = record.excluded_authority_version_,
      .excluded_grant_revision_ = record.excluded_grant_revision_,
      .population_manifest_revision_ = record.population_manifest_revision_,
      .population_manifest_digest_ = record.population_manifest_digest_,
      .partition_replication_epoch_ = record.partition_replication_epoch_,
      .hold_required_ = record.hold_required_,
      .recovery_required_ = record.recovery_required_,
      .proof_state_ = record.proof_state_,
      .frozen_proof_ = record.frozen_proof_};
}

absl::Status ValidateSameGenerationTransition(
    const MetaFailoverRecoveryRecord& current,
    const SetFailoverRecovery& command) {
  if (!ImmutableAnchorsMatch(current, command)) {
    return MetaDomainRejectError(
        "recovery generation immutable anchors cannot change");
  }
  if (!current.hold_required_ && command.hold_required_) {
    return MetaDomainRejectError("released source hold cannot be restored");
  }
  if (current.recovery_required_ && !command.recovery_required_) {
    return MetaDomainRejectError(
        "recovery-required can only be cleared with the whole record");
  }
  switch (current.proof_state_) {
    case MetaFailoverProofState::kPending:
      return absl::OkStatus();
    case MetaFailoverProofState::kExact:
      if ((command.proof_state_ != MetaFailoverProofState::kExact &&
           command.proof_state_ != MetaFailoverProofState::kUnavailable) ||
          command.frozen_proof_ != current.frozen_proof_) {
        return MetaDomainRejectError(
            "exact frozen source proof cannot be changed or discarded");
      }
      return absl::OkStatus();
    case MetaFailoverProofState::kUnavailable:
      if (command.proof_state_ != MetaFailoverProofState::kUnavailable ||
          command.frozen_proof_ != current.frozen_proof_) {
        return MetaDomainRejectError(
            "unavailable frozen source state cannot be upgraded or rewritten");
      }
      return absl::OkStatus();
  }
  return MetaDomainRejectError("recovery proof state is invalid");
}

}  // namespace

absl::Status MetaFailoverRecoveryStore::Set(const SetFailoverRecovery& command,
                                            std::uint64_t committed_index) {
  if (absl::Status status = ValidateCommand(command, committed_index);
      !status.ok()) {
    return status;
  }
  auto it = entries_.find(command.group_id_);
  if (it == entries_.end()) {
    if (command.expected_revision_ != 0) {
      return MetaDomainRejectError("unknown recovery revision");
    }
    if (entries_.size() >= max_groups_) {
      return MetaDomainRejectError("failover recovery group cap reached");
    }
    if (!command.hold_required_ && !command.recovery_required_) {
      return MetaDomainRejectError(
          "new recovery must require a source hold or recovery");
    }
    Entry entry;
    entry.generation_floor_ = command.recovery_generation_;
    entry.revision_ = committed_index;
    entry.record_ = MakeRecord(command, committed_index);
    entries_.emplace(command.group_id_, std::move(entry));
    return absl::OkStatus();
  }

  Entry& entry = it->second;
  if (entry.record_.has_value() &&
      DesiredStateMatches(*entry.record_, command, committed_index)) {
    return absl::OkStatus();
  }
  if (!entry.record_.has_value()) {
    if (command.expected_revision_ != entry.revision_ ||
        command.recovery_generation_ <= entry.generation_floor_ ||
        committed_index <= entry.revision_) {
      return MetaDomainRejectError(
          "successor recovery must CAS and advance its durable cursor");
    }
    if (!command.hold_required_ && !command.recovery_required_) {
      return MetaDomainRejectError(
          "successor recovery must require a source hold or recovery");
    }
    entry.generation_floor_ = command.recovery_generation_;
    entry.revision_ = committed_index;
    entry.cleared_expected_revision_ = 0;
    entry.record_ = MakeRecord(command, committed_index);
    return absl::OkStatus();
  }

  const MetaFailoverRecoveryRecord& current = *entry.record_;
  if (command.expected_revision_ != current.revision_) {
    return MetaDomainRejectError("recovery generation or revision mismatch");
  }
  if (committed_index <= current.revision_) {
    return MetaDomainRejectError("recovery revision must strictly increase");
  }
  if (command.recovery_generation_ < current.recovery_generation_) {
    return MetaDomainRejectError("recovery generation cannot move backwards");
  }
  if (command.recovery_generation_ == current.recovery_generation_) {
    if (absl::Status status =
            ValidateSameGenerationTransition(current, command);
        !status.ok()) {
      return status;
    }
  } else {
    // A post-activation candidate failure needs a new authority term and
    // source identity. Replacing the generation atomically avoids a durable
    // interval with no handoff after the old record is cleared. It may only
    // claim an explicit recovery handoff; otherwise it could steal a source
    // hold from a still-live controlled workflow or bypass its release ack.
    if (!current.hold_required_ || !current.recovery_required_) {
      return MetaDomainRejectError(
          "successor recovery requires an explicit prior handoff");
    }
    if (!command.hold_required_ && !command.recovery_required_) {
      return MetaDomainRejectError(
          "successor recovery must require a source hold or recovery");
    }
    entry.generation_floor_ = command.recovery_generation_;
  }
  entry.revision_ = committed_index;
  entry.cleared_expected_revision_ = 0;
  entry.record_ = MakeRecord(command, committed_index);
  return absl::OkStatus();
}

absl::Status MetaFailoverRecoveryStore::Clear(
    const ClearFailoverRecovery& command, std::uint64_t committed_index) {
  if (command.group_id_.empty() ||
      command.group_id_.size() > kMaxMetaGroupIdBytes ||
      command.expected_revision_ == 0 || command.recovery_generation_ == 0 ||
      committed_index == 0) {
    return MetaDomainRejectError("invalid recovery clear identity");
  }
  const auto it = entries_.find(command.group_id_);
  if (it == entries_.end()) {
    return MetaDomainRejectError("unknown recovery group");
  }
  Entry& entry = it->second;
  if (!entry.record_.has_value()) {
    if (entry.generation_floor_ == command.recovery_generation_ &&
        entry.revision_ == committed_index &&
        entry.cleared_expected_revision_ == command.expected_revision_) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError("recovery clear generation is stale");
  }
  if (entry.record_->recovery_generation_ != command.recovery_generation_ ||
      entry.record_->revision_ != command.expected_revision_) {
    return MetaDomainRejectError("recovery clear CAS mismatch");
  }
  if (entry.record_->hold_required_ || entry.record_->recovery_required_) {
    return MetaDomainRejectError(
        "active recovery desired state must be released before clear");
  }
  if (committed_index <= entry.record_->revision_) {
    return MetaDomainRejectError("recovery clear revision must increase");
  }
  entry.revision_ = committed_index;
  entry.cleared_expected_revision_ = command.expected_revision_;
  entry.record_.reset();
  return absl::OkStatus();
}

std::optional<MetaFailoverRecoveryRecord> MetaFailoverRecoveryStore::Find(
    std::string_view group_id) const {
  const auto it = entries_.find(std::string(group_id));
  if (it == entries_.end()) return std::nullopt;
  return it->second.record_;
}

std::optional<std::uint64_t> MetaFailoverRecoveryStore::LastGeneration(
    std::string_view group_id) const {
  const auto it = entries_.find(std::string(group_id));
  if (it == entries_.end()) return std::nullopt;
  return it->second.generation_floor_;
}

std::optional<std::uint64_t> MetaFailoverRecoveryStore::LastRevision(
    std::string_view group_id) const {
  const auto it = entries_.find(std::string(group_id));
  if (it == entries_.end()) return std::nullopt;
  return it->second.revision_;
}

std::vector<MetaFailoverRecoveryRecord> MetaFailoverRecoveryStore::Records()
    const {
  std::vector<MetaFailoverRecoveryRecord> records;
  records.reserve(Size());
  for (const auto& [group_id, entry] : entries_) {
    (void)group_id;
    if (entry.record_.has_value()) records.push_back(*entry.record_);
  }
  return records;
}

bool MetaFailoverRecoveryStore::PopulationManifestInUse(
    const MetaHash256& digest) const {
  return std::any_of(
      entries_.begin(), entries_.end(), [&digest](const auto& item) {
        return item.second.record_.has_value() &&
               item.second.record_->population_manifest_digest_ == digest;
      });
}

bool MetaFailoverRecoveryStore::SourceAssignmentInUse(
    std::string_view node_id, const MetaAssignmentId& assignment_id) const {
  return std::any_of(
      entries_.begin(), entries_.end(),
      [node_id, &assignment_id](const auto& item) {
        return item.second.record_.has_value() &&
               item.second.record_->old_source_node_id_ == node_id &&
               item.second.record_->old_source_assignment_id_ == assignment_id;
      });
}

std::size_t MetaFailoverRecoveryStore::Size() const {
  return static_cast<std::size_t>(std::count_if(
      entries_.begin(), entries_.end(),
      [](const auto& item) { return item.second.record_.has_value(); }));
}

std::string MetaFailoverRecoveryStore::Serialize() const {
  MetaWriter writer;
  writer.WriteU16(kMetaFormatVersion);
  writer.WriteCount(static_cast<std::uint32_t>(entries_.size()));
  for (const auto& [group_id, entry] : entries_) {
    writer.WriteString(group_id);
    writer.WriteU64(entry.generation_floor_);
    writer.WriteU64(entry.revision_);
    writer.WriteU64(entry.cleared_expected_revision_);
    writer.WriteOptional(entry.record_,
                         [](MetaWriter& record_writer,
                            const MetaFailoverRecoveryRecord& record) {
                           WriteRecord(record_writer, record);
                         });
  }
  return writer.TakeBuffer();
}

absl::StatusOr<MetaFailoverRecoveryStore>
MetaFailoverRecoveryStore::Deserialize(std::string_view bytes,
                                       std::uint32_t max_groups) {
  if (bytes.size() > kMaxMetaSnapshotBytes) {
    return MetaFailStopError(
        "failover recovery store exceeds the snapshot byte cap");
  }
  MetaReader reader(bytes);
  auto version = reader.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unknown failover recovery schema version");
  }
  auto count = reader.ReadCount(max_groups);
  if (!count.ok()) return count.status();
  MetaFailoverRecoveryStore store(max_groups);
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto group_id = reader.ReadString(kMaxMetaGroupIdBytes);
    if (!group_id.ok()) return group_id.status();
    auto generation = reader.ReadU64();
    if (!generation.ok()) return generation.status();
    auto revision = reader.ReadU64();
    if (!revision.ok()) return revision.status();
    auto cleared_expected_revision = reader.ReadU64();
    if (!cleared_expected_revision.ok()) {
      return cleared_expected_revision.status();
    }
    auto record = reader.ReadOptional<MetaFailoverRecoveryRecord>(
        [group_id](MetaReader& record_reader) {
          return ReadRecord(record_reader, *group_id);
        });
    if (!record.ok()) return record.status();
    if (group_id->empty() || *generation == 0 || *revision == 0 ||
        (record->has_value() &&
         ((*record)->revision_ == 0 || (*record)->revision_ != *revision ||
          (*record)->recovery_generation_ != *generation ||
          *cleared_expected_revision != 0)) ||
        (!record->has_value() && (*cleared_expected_revision == 0 ||
                                  *cleared_expected_revision >= *revision))) {
      return MetaFailStopError("invalid failover recovery snapshot entry");
    }
    if (record->has_value()) {
      const SetFailoverRecovery command = ToCommand(**record);
      if (!ValidateCommand(command, (*record)->revision_).ok()) {
        return MetaFailStopError("invalid failover recovery snapshot record");
      }
    }
    if (!store.entries_
             .emplace(
                 std::string(*group_id),
                 Entry{.generation_floor_ = *generation,
                       .revision_ = *revision,
                       .cleared_expected_revision_ = *cleared_expected_revision,
                       .record_ = std::move(*record)})
             .second) {
      return MetaFailStopError("duplicate failover recovery group");
    }
  }
  if (absl::Status status = reader.Finish(); !status.ok()) return status;
  return store;
}

}  // namespace keylane::meta
