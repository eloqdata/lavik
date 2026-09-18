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

// MetaAuditStore is the metadata control plane's bounded audit window.
//
// One record per privileged command application, keyed by the command's raft
// log index; window order is log-index order. Each record carries the actor
// principal (copied from the trusted-entry-injected ActorContext), an opaque
// command summary, the apply verdict, and the readable propose time the
// trusted entry wrote. THE STORE NEVER READS A CLOCK: readable_time_ is
// command-carried text the store copies verbatim.
//
// Exported records are ordered by Raft log index. An external archive supplies
// its own deployment namespace and deduplicates within it by log index,
// comparing the complete record on duplicate exports. This is an operational
// record, not a cryptographic tamper-evident log.
//
// Replay idempotency: appending an index already in the window with identical
// content is a no-op. Append of an existing index with DIFFERENT content, an
// out-of-order new index, or an index at/below the pruned floor means the
// apply layer lost the log index <-> record correspondence; that is an
// implementation bug and the store FAILS STOP (spdlog::critical + abort, the
// same fail-stop policy as the Raft runtime) rather than corrupt the audit
// trail. The same deterministic input byte stream aborts every node at the
// same index, so this cannot fork the group.
//
// Capacity behavior is a replicated policy. Bounded-rotate (the default)
// drops the oldest record before appending at a
// full window; durable drop watermarks make archival gaps observable.
// Strict-export instead makes NeedsExport() gate privileged proposals until
// an operator archives and prunes a prefix. Disabled suppresses ordinary
// records, but policy changes are always recorded so disabling
// or re-enabling audit is visible. Append beyond capacity in strict mode
// FAILS STOP: a committed command's audit write cannot be refused without
// desynchronizing the state machine, so reaching it means the proposal gate
// was bypassed.
//
// Apply is a pure in-memory function: no IO, no locks (concurrency control
// lives in the state machine above), no clock, no observation access. Domain
// rejections (over-cap fields) return absl::Status of MetaFailureClass
// kDomainReject. Snapshot serialization is the versioned strict encoding of
// encoding.h; decode failures are MetaFailureClass::kFailStop. Decoding checks
// field bounds and strictly increasing indexes above the prune floor.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/encoding.h"

namespace lavik::meta {

// Per-record field caps (deployment-policy values; the encoded schema does
// not depend on them). The principal cap is the identity layer's
// kMaxMetaPrincipalBytes.
inline constexpr std::uint32_t kMaxMetaAuditSummaryBytes = 2048;
inline constexpr std::uint32_t kMaxMetaAuditDetailBytes = 2048;
inline constexpr std::uint32_t kMaxMetaAuditReadableTimeBytes = 128;

// The apply verdict persisted with each record.
enum class MetaAuditVerdict : std::uint8_t {
  kAccepted = 1,  // command applied, including an idempotent no-op accept
  kRejected = 2,  // domain rejection: index consumed, state unchanged
};

// One audit record, keyed by Raft log index.
struct MetaAuditRecord {
  std::uint64_t log_index_ = 0;
  std::string actor_principal_;
  std::string command_summary_;  // opaque, built by the apply layer
  MetaAuditVerdict verdict_ = MetaAuditVerdict::kAccepted;
  std::string verdict_detail_;  // e.g. the rejection message
  std::string readable_time_;   // trusted-entry propose time; copied verbatim
  bool operator==(const MetaAuditRecord&) const = default;
};

class MetaAuditStore {
 public:
  explicit MetaAuditStore(
      std::uint32_t window_capacity = kMaxMetaAuditWindowRecords)
      : window_capacity_(window_capacity) {}

  // Appends the record for its log_index_. Idempotent no-op when the index is
  // already present with identical content. Returns kDomainReject when a field
  // exceeds its cap. FAILS STOP on: same index with different content, an
  // out-of-order new index, an index at/below the pruned floor, or a full
  // strict-export window (see the file header for the rationale of each).
  absl::Status Append(const MetaAuditRecord& record, bool force_record = false);

  // Switching to strict-export while full is rejected because its mandatory
  // policy-change record would have no safe slot.
  absl::Status SetPolicy(MetaAuditPolicy policy);
  MetaAuditPolicy policy() const { return policy_; }

  std::optional<MetaAuditRecord> Find(std::uint64_t log_index) const;
  std::size_t size() const { return window_.size(); }
  std::uint32_t capacity() const { return window_capacity_; }

  // Full-window state gated by the coordinator's Propose layer: privileged
  // proposals return RESOURCE_EXHAUSTED until the operator exports and prunes
  // records.
  bool NeedsExport() const {
    return policy_ == MetaAuditPolicy::kStrictExport &&
           window_.size() >= window_capacity_;
  }

  std::uint64_t dropped_total() const { return dropped_total_; }
  std::uint64_t dropped_through() const { return dropped_through_; }

  // Highest pruned log index (0 = nothing pruned). Appends at or below the
  // floor fail stop (see Append).
  std::uint64_t pruned_floor() const { return pruned_floor_; }

  // Read-only versioned encoding of every window record with
  // log_index <= through for ctl-side external archival. The
  // blob includes drop watermarks and complete records.
  absl::StatusOr<std::string> ExportThrough(std::uint64_t through) const;

  // Removes every record with log_index <= through. `through` must name a
  // record still in the window; re-pruning at/below the floor is an idempotent
  // no-op. The caller must have durably archived the exported bytes first — the
  // store does not track export acknowledgements (that bookkeeping is the ctl
  // layer's).
  absl::Status PruneThrough(std::uint64_t through);

  // Snapshot serialization: versioned strict encoding; decode enforces caps,
  // strictly increasing indexes above the prune floor.
  absl::StatusOr<std::string> Serialize() const;
  // Exact durable size without allocating or copying snapshot bytes.
  std::uint64_t SerializedSize() const;
  static absl::StatusOr<MetaAuditStore> Deserialize(
      std::string_view bytes,
      std::uint32_t window_capacity = kMaxMetaAuditWindowRecords);

 private:
  void WriteSnapshot(MetaWriter& writer) const;
  std::uint32_t window_capacity_;
  MetaAuditPolicy policy_ = MetaAuditPolicy::kBoundedRotate;
  std::map<std::uint64_t, MetaAuditRecord> window_;  // keyed by log index
  std::uint64_t pruned_floor_ = 0;  // highest pruned log index (0 = none)
  // Automatic bounded-rotate loss is distinct from an operator-confirmed
  // prune. These fields let status/export surface an archival gap.
  std::uint64_t dropped_total_ = 0;
  std::uint64_t dropped_through_ = 0;
};

// Decoded export with drop watermarks and records in strictly increasing
// log-index order. No content authenticity is implied by this format.
struct MetaAuditExport {
  std::uint64_t dropped_total_ = 0;
  std::uint64_t dropped_through_ = 0;
  std::vector<MetaAuditRecord> records_;
  bool operator==(const MetaAuditExport&) const = default;
};

absl::StatusOr<MetaAuditExport> DecodeMetaAuditExport(std::string_view bytes);

}  // namespace lavik::meta
