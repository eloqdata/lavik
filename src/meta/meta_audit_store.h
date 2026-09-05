#pragma once

// MetaAuditStore is the metadata control plane's bounded, hash-chained audit
// window.
//
// One record per privileged command application, keyed by the command's raft
// log index; window order is log-index order. Each record carries the actor
// principal (copied from the trusted-entry-injected ActorContext), an opaque
// command summary, the apply verdict, and the readable propose time the
// trusted entry wrote. THE STORE NEVER READS A CLOCK: readable_time_ is
// command-carried text the store copies verbatim.
//
// Rolling hash chain: every record's chain hash is
//   SHA-256(previous record's chain hash || canonical record encoding)
// over the all-zero genesis hash for the first record after a prune anchor.
// Pruning a prefix does not break verification of what remains: the store
// keeps the hash of the last pruned record as the new anchor, and exported
// bytes carry the anchor they chain from, so an external archive can verify
// continuity across exports. External archives deduplicate by
// (cluster_id, raft_log_index, record_hash).
//
// Replay idempotency: appending an index already in the window with identical
// content is a no-op. Append of an existing index with DIFFERENT content, an
// out-of-order new index, or an index at/below the pruned floor means the
// apply layer lost the log index <-> record correspondence; that is an
// implementation bug and the store FAILS STOP (spdlog::critical + abort, the
// same policy as NuraftStateMgr::system_exit) rather than corrupt the audit
// trail. The same deterministic input byte stream aborts every node at the
// same index, so this cannot fork the group.
//
// Capacity is fail-safe: never silently drop unexported records. The window
// holds at most capacity() records (default
// kMaxMetaAuditWindowRecords). NeedsExport() reports a full window; the
// coordinator gates privileged proposals on it (RESOURCE_EXHAUSTED until the
// operator exports). Append beyond capacity FAILS STOP: a committed command's
// audit write cannot be refused without desynchronizing the state machine, so
// reaching it means the propose gate was bypassed (it must account for
// in-flight proposals). Exports (ExportThrough) are pure reads; PruneThrough
// removes an exported prefix and advances the anchor/floor. The store does
// not track export acknowledgements — pruning discipline (export durably
// first, prune deterministically) belongs to the apply/ctl orchestration.
//
// Apply is a pure in-memory function: no IO, no locks (concurrency control
// lives in the state machine above), no clock, no observation access. Domain
// rejections (over-cap fields) return absl::Status of MetaFailureClass
// kDomainReject. Snapshot serialization is the versioned strict encoding of
// meta_encoding.h; decode failures are MetaFailureClass::kFailStop, and a
// loaded window whose chain does not recompute is corruption and fails
// decoding the same way.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "meta/meta_commands.h"
#include "meta/meta_encoding.h"

namespace keylane::meta {

// Per-record field caps (v1 policy values; the encoded schema does not depend
// on them). The principal cap is the identity layer's kMaxMetaPrincipalBytes.
inline constexpr std::uint32_t kMaxMetaAuditSummaryBytes = 2048;
inline constexpr std::uint32_t kMaxMetaAuditDetailBytes = 2048;
inline constexpr std::uint32_t kMaxMetaAuditReadableTimeBytes = 128;

// The apply verdict persisted with each record.
enum class MetaAuditVerdict : std::uint8_t {
  kAccepted = 1,  // command applied, including an idempotent no-op accept
  kRejected = 2,  // domain rejection: index consumed, state unchanged
};

// One audit record, keyed by raft log index. The chain hash is derived by the
// store and exposed via MetaAuditChainEntry; it is not part of the input.
struct MetaAuditRecord {
  std::uint64_t log_index_ = 0;
  std::string actor_principal_;
  std::string command_summary_;  // opaque, built by the apply layer
  MetaAuditVerdict verdict_ = MetaAuditVerdict::kAccepted;
  std::string verdict_detail_;  // e.g. the rejection message
  std::string readable_time_;   // trusted-entry propose time; copied verbatim
  bool operator==(const MetaAuditRecord&) const = default;
};

// A window entry: the record plus its store-computed rolling chain hash.
struct MetaAuditChainEntry {
  MetaAuditRecord record_;
  MetaHash256 chain_hash_{};
  bool operator==(const MetaAuditChainEntry&) const = default;
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
  // window (see the file header for the rationale of each).
  absl::Status Append(const MetaAuditRecord& record);

  std::optional<MetaAuditChainEntry> Find(std::uint64_t log_index) const;
  std::size_t size() const { return window_.size(); }
  std::uint32_t capacity() const { return window_capacity_; }

  // Full-window state gated by the coordinator's Propose layer: privileged
  // proposals return RESOURCE_EXHAUSTED until the operator exports records.
  bool NeedsExport() const { return window_.size() >= window_capacity_; }

  // Hash of the newest record, or the prune anchor when the window is empty
  // (all-zero before any append).
  const MetaHash256& chain_head() const { return chain_head_; }

  // Recomputes the chain from the prune anchor through the window; false on
  // mismatch (never happens through the public API; a corruption tripwire for
  // tests and diagnostics).
  bool VerifyChain() const;

  // Highest pruned log index (0 = nothing pruned). Appends at or below the
  // floor fail stop (see Append).
  std::uint64_t pruned_floor() const { return pruned_floor_; }

  // Versioned byte drain of every window record with log_index <= through,
  // for ctl-side external archival. A pure read; the
  // blob carries the anchor it chains from plus per-record chain hashes.
  absl::StatusOr<std::string> ExportThrough(std::uint64_t through) const;

  // Removes every record with log_index <= through and advances the anchor
  // to that record's chain hash. `through` must name a record still in the
  // window; re-pruning at/below the floor is an idempotent no-op. The caller
  // must have durably archived the exported bytes first — the store does not
  // track export acknowledgements (that bookkeeping is the ctl layer's).
  absl::Status PruneThrough(std::uint64_t through);

  // Snapshot serialization: versioned strict encoding; decode enforces caps,
  // strictly increasing indexes, and recomputes the chain (a break is
  // corruption and fails with MetaFailureClass::kFailStop).
  absl::StatusOr<std::string> Serialize() const;
  static absl::StatusOr<MetaAuditStore> Deserialize(
      std::string_view bytes,
      std::uint32_t window_capacity = kMaxMetaAuditWindowRecords);

 private:
  std::uint32_t window_capacity_;
  std::map<std::uint64_t, MetaAuditChainEntry> window_;  // keyed by log index
  // Hash the first window record chains from: all-zero genesis, or the chain
  // hash of the last pruned record (prefix truncation keeps the remaining
  // chain verifiable from this anchor).
  MetaHash256 anchor_{};
  MetaHash256 chain_head_{};        // == anchor_ when the window is empty
  std::uint64_t pruned_floor_ = 0;  // highest pruned log index (0 = none)
};

// Decoded export blob (see MetaAuditStore::ExportThrough): the chain anchor
// the first exported record chains from, plus the exported entries in log
// index order. Decode verifies the chain inside the blob.
struct MetaAuditExport {
  MetaHash256 anchor_before_{};
  std::vector<MetaAuditChainEntry> records_;
  bool operator==(const MetaAuditExport&) const = default;
};

absl::StatusOr<MetaAuditExport> DecodeMetaAuditExport(std::string_view bytes);

}  // namespace keylane::meta
