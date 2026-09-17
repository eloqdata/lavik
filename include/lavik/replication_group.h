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

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik {

inline constexpr std::uint32_t kReplicationPartitionCount = 16'384;

// Identifies one desired partition and its Meta-owned logical incarnation.
// The logical epoch is distinct from Storage's target-local reset epoch.
struct PopulationManifestEntry {
  std::uint32_t partition_id_ = 0;
  std::uint64_t logical_epoch_ = 0;

  bool operator==(const PopulationManifestEntry&) const = default;
};

// A content identity for a complete, canonical PopulationManifest.
struct PopulationManifestId {
  std::array<std::uint8_t, 32> bytes_{};

  bool operator==(const PopulationManifestId&) const = default;

  // Returns the lowercase hexadecimal SHA-256 digest.
  std::string Hex() const;
};

// The complete logical population expected on a one-group data node.
//
// Create canonicalizes entries by partition and hashes this byte sequence:
//
//   "LAVIK_POPULATION_V1" || u16be(schema_version=1) ||
//   u32be(entry_count) ||
//   repeated(u16be(partition_id) || u64be(logical_epoch))
//
// A manifest may be empty or sparse. Membership is represented internally by
// one epoch slot per physical partition; zero means non-member. This keeps the
// desired transfer set separate from destructive reset, which still covers all
// physical partitions so records outside a sparse manifest cannot survive.
class PopulationManifest {
 public:
  // Validates and canonicalizes a version-1 population of up to 16,384
  // unique, in-range entries with nonzero logical epochs.
  static absl::StatusOr<PopulationManifest> Create(
      std::vector<PopulationManifestEntry> entries);

  // Returns the logical epoch for each physical partition; zero is non-member.
  std::span<const std::uint64_t, kReplicationPartitionCount> logical_epochs()
      const noexcept {
    return logical_epochs_;
  }

  // Returns the SHA-256 identity of the canonical manifest.
  const PopulationManifestId& id() const noexcept { return id_; }

 private:
  PopulationManifest(
      std::array<std::uint64_t, kReplicationPartitionCount> logical_epochs,
      PopulationManifestId id)
      : logical_epochs_(std::move(logical_epochs)), id_(id) {}

  std::array<std::uint64_t, kReplicationPartitionCount> logical_epochs_{};
  PopulationManifestId id_;
};

// Every field that makes a rebuild attempt authoritative for the current boot.
// Work and completion proofs must present an exactly equal identity; matching
// only an attempt ID, cursor, or source history is deliberately insufficient.
struct RebuildIdentity {
  std::string group_id_;
  std::string assignment_id_;
  std::uint64_t term_ = 0;
  // Monotonically orders directive content changes within one authority term.
  std::uint64_t directive_revision_ = 0;
  std::string authority_id_;
  std::string source_node_id_;
  // Membership incarnation of the exporting source. It is distinct from
  // assignment_id_, which always names the rebuild target.
  std::string source_assignment_id_;
  std::string source_boot_id_;
  std::string source_history_id_;
  std::string target_node_id_;
  std::string target_boot_id_;
  // The target's local replication-history incarnation. It is present only
  // for source-less empty initialization, whose destructive reset must remain
  // bound to the exact history advertised by the current control session.
  std::string target_history_id_;
  std::string operation_id_;
  // Stable Meta-owned identity of the directive. Retries may use a fresh
  // attempt ID, but they must not silently rebind work to another directive.
  std::string directive_id_;
  std::string attempt_id_;
  // Monotonic Meta revision pairs with the digest so an A -> B -> A manifest
  // sequence cannot make a later population look identical to an earlier one.
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  // Meta-owned population generation. This is distinct from each target
  // partition's storage-local replication epoch and invalidates a proof even
  // when the immutable manifest is unchanged.
  std::uint64_t partition_replication_epoch_ = 0;

  bool operator==(const RebuildIdentity&) const = default;
};

// A Meta directive that binds safe-source authorization and flow layout to a
// complete rebuild identity.
struct RebuildDirective {
  RebuildIdentity identity_;
  std::uint32_t flow_count_ = 0;
  bool safe_source_active_ = false;

  bool operator==(const RebuildDirective&) const = default;
};

// Runtime-only population lifecycle for this process boot.
enum class ReplicationGroupState : std::uint8_t {
  kNotReady,
  kRebuilding,
  kReady,
  kFailedStopped,
};

// Capability returned only after the directive, local boot, manifest, and
// safe-source assertion have been validated. Storage reset callers should
// require this capability instead of acting directly on an unvalidated
// control-plane directive.
class DestructiveResetAuthorization {
 private:
  friend class ReplicationGroup;
  explicit DestructiveResetAuthorization(RebuildIdentity identity)
      : identity_(std::move(identity)) {}

  RebuildIdentity identity_;
};

// Current-boot proof that the complete group population may be served and may
// participate in candidate selection.
class ReadyToken {
 public:
  // Returns the exact identity whose population was published.
  const RebuildIdentity& identity() const noexcept { return identity_; }

  // Returns the stable next-LSN cut for every source flow, in flow order.
  std::span<const std::uint64_t> cut_vector() const noexcept {
    return cut_vector_;
  }

  // Returns whether the term component permits this completed population to
  // be retained under a committed group term. Callers must still match every
  // content and membership anchor before carrying the proof forward.
  bool CanCarryForwardToTerm(std::uint64_t group_term) const noexcept {
    return group_term >= identity_.term_;
  }

 private:
  friend class ReplicationGroup;
  ReadyToken(RebuildIdentity identity, std::vector<std::uint64_t> cut_vector)
      : identity_(std::move(identity)), cut_vector_(std::move(cut_vector)) {}

  RebuildIdentity identity_;
  std::vector<std::uint64_t> cut_vector_;
};

// Owns the current-boot population proof for this process's single dataset.
// It deliberately exposes attempt-level capabilities and terminal proofs, not
// worker, frame, or storage-index mechanics. This type has no internal
// synchronization: every mutating method must be called by one coordinator
// owner, and callers must externally synchronize cross-thread observations.
class ReplicationGroup {
 public:
  // Creates an initially NOT_READY group owner scoped to one node boot.
  ReplicationGroup(std::string local_node_id, std::string local_boot_id);
  ReplicationGroup(const ReplicationGroup&) = delete;
  ReplicationGroup& operator=(const ReplicationGroup&) = delete;
  ~ReplicationGroup();

  // Rebinds a recovered complete population to this boot without authorizing
  // destructive reset. The caller must have validated and consumed Storage's
  // clean certificate, or hold a committed operator-recovery action. This
  // token grants no lease; the manager keeps serving fenced until promotion.
  absl::StatusOr<ReadyToken> RecoverPopulation(
      RebuildIdentity identity, std::vector<std::uint64_t> frontier);

  // Checks whether a directive could be accepted without consuming its
  // attempt identity or changing the current proof. A newer directive may
  // validate while another attempt is active, but the caller must cancel and
  // retire that attempt before calling BeginRebuild.
  absl::Status ValidateRebuild(const RebuildDirective& directive,
                               const PopulationManifest& manifest) const;

  // Returns a fresh directive revision for the supplied authority term. The
  // accepted-version watermark survives proof invalidation, so callers must
  // derive retries here rather than shadowing it in a transient session.
  absl::StatusOr<std::uint64_t> NextDirectiveRevision(std::uint64_t term) const;

  // Validates a fresh directive and enters REBUILDING. An exact retry while
  // rebuilding is idempotent. The node permanently rejects a different group
  // assignment during this boot.
  absl::StatusOr<DestructiveResetAuthorization> BeginRebuild(
      const RebuildDirective& directive, const PopulationManifest& manifest);

  // Checks a source-less first-population directive without consuming its
  // attempt. Source identity fields must be empty and target_history_id_ must
  // bind the current control session. The resulting storage proof reuses the
  // rebuild lifecycle but deliberately has no replication-flow cut.
  absl::Status ValidateEmptyPopulation(
      const RebuildIdentity& identity,
      const PopulationManifest& manifest) const;

  // Enters REBUILDING for a validated source-less first population. This is
  // the destructive capability seam used by ReplicationManager after it has
  // independently matched session, assignment, history, and authority.
  absl::StatusOr<DestructiveResetAuthorization> BeginEmptyPopulation(
      const RebuildIdentity& identity, const PopulationManifest& manifest);

  // Revalidates a reset capability against the currently active attempt. This
  // must be checked at the destructive storage boundary so an authorization
  // retained from an aborted or superseded attempt cannot be replayed.
  absl::Status ValidateResetAuthorization(
      const DestructiveResetAuthorization& authorization) const;

  // Records the nonzero target-local Storage replication epoch returned by a
  // destructive reset of one physical partition. Repeating the exact event is
  // idempotent; the saved epoch cannot be replaced within an attempt.
  absl::Status RecordPartitionReset(const RebuildIdentity& identity,
                                    std::uint32_t partition_id,
                                    std::uint64_t target_local_epoch);

  // Records one physical partition's post-reset handoff. The supplied logical
  // epoch must exactly match the saved manifest slot, including zero for a
  // non-member. The target-local epoch must be nonzero and match the epoch
  // returned by that partition's reset, proving the handoff is not stale.
  absl::Status RecordPartitionHandoff(const RebuildIdentity& identity,
                                      std::uint32_t partition_id,
                                      std::uint64_t logical_epoch,
                                      std::uint64_t target_local_epoch);

  // Records the immutable stable next-LSN cut for every source flow. The
  // vector must be complete, in flow order, and contain only nonzero values.
  // Repeating the exact vector is idempotent; it cannot be replaced within an
  // attempt.
  absl::Status RecordFlowCutVector(
      const RebuildIdentity& identity,
      std::span<const std::uint64_t> stable_next_lsns);

  // Proves the partitionless Function catalog reached this attempt's cut.
  absl::Status MarkFunctionCatalogComplete(const RebuildIdentity& identity);

  // Records successful storage drain and PromoteReplicaRoot. This is rejected
  // until the manifest and Function catalog are complete, plus every source
  // flow cut for a replicated rebuild.
  absl::Status MarkStoragePromoted(const RebuildIdentity& identity);

  // Atomically publishes current-boot readiness after every proof is complete.
  // Repeating publication for the same ready identity is idempotent.
  absl::StatusOr<ReadyToken> PublishReady(const RebuildIdentity& identity);

  // Adopts an authenticated direct-child history without replacing storage.
  // The caller first drains ingress and proves that actual Applied equals the
  // complete parent boundary advertised by the current Owner. Both layouts
  // are independent; child cursors come from that boundary, never its tail.
  // Publication stays READY and atomically replaces identity and whole cut.
  absl::StatusOr<ReadyToken> SwitchHistory(
      const ReadyToken& parent, const RebuildDirective& child,
      const PopulationManifest& manifest,
      std::span<const std::uint64_t> actual_parent,
      std::span<const std::uint64_t> required_parent,
      std::span<const std::uint64_t> child_origin);

  // Reanchors a complete local Owner population to its own native publisher.
  // The caller must fence writes, drain accepted commands and expiration, and
  // capture the whole source cut before retiring that publisher. This may be
  // the initial source-less population or a rotated boot-local source history;
  // an explicitly authorized operator recovery may instead bind its fresh
  // complete local base to the first publisher in that same term. That origin
  // is not a recovered historical cursor. This is not a peer HistorySwitch
  // and grants no serving or reset authority.
  absl::StatusOr<ReadyToken> BindLocalSourceHistory(
      const ReadyToken& population, const RebuildDirective& source,
      const PopulationManifest& manifest,
      std::span<const std::uint64_t> source_cut);

  // Abandons the matching partial attempt and returns to NOT_READY. A later
  // attempt must use a fresh attempt identity.
  absl::Status Abort(const RebuildIdentity& identity);

  // Invalidates either an in-progress or published runtime proof after a
  // history gap, divergent tail, disconnect that cannot continue exactly, or
  // other loss of evidence. The physical population may remain on disk, but
  // it is neither readable nor candidate-eligible and this attempt cannot be
  // reused; a fresh directive is required.
  absl::Status InvalidateProof(const RebuildIdentity& identity);

  // Latches an unrecoverable reset, promotion, or abort failure. No later
  // attempt is accepted during this process boot. It accepts the matching
  // rebuilding or ready identity because uncertainty may arise immediately
  // after storage promotion.
  absl::Status FailStop(const RebuildIdentity& identity);

  // Returns the current runtime-only population lifecycle state.
  ReplicationGroupState state() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace lavik
