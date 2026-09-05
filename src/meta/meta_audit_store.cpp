#include "meta/meta_audit_store.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include "meta/meta_hash.h"
#include "spdlog/spdlog.h"

namespace keylane::meta {

// The shared SHA-256 lives in meta/meta_hash.h (header-only; the meta plane
// links no crypto library — see that header).

namespace {

// Canonical encoding of one record for chain-hashing: the strict
// meta_encoding.h field layout without a schema-version envelope (the hash
// input is internal, not a wire format).
std::string CanonicalRecordBytes(const MetaAuditRecord& record) {
  MetaWriter w;
  w.WriteU64(record.log_index_);
  w.WriteString(record.actor_principal_);
  w.WriteString(record.command_summary_);
  w.WriteU8(static_cast<std::uint8_t>(record.verdict_));
  w.WriteString(record.verdict_detail_);
  w.WriteString(record.readable_time_);
  return w.TakeBuffer();
}

// Chain hash of one record: SHA-256(previous hash || canonical encoding).
MetaHash256 ComputeChainHash(const MetaHash256& previous,
                             const MetaAuditRecord& record) {
  const std::string canonical = CanonicalRecordBytes(record);
  std::string input;
  input.reserve(previous.size() + canonical.size());
  input.append(reinterpret_cast<const char*>(previous.data()), previous.size());
  input.append(canonical);
  return MetaSha256(input);
}

// Wire layout of one window entry (snapshot and export blobs):
//   log_index u64 | principal str | summary str | verdict u8 |
//   detail str | readable_time str | chain_hash 32B
void WriteChainEntry(MetaWriter& w, const MetaAuditChainEntry& entry) {
  w.WriteU64(entry.record_.log_index_);
  w.WriteString(entry.record_.actor_principal_);
  w.WriteString(entry.record_.command_summary_);
  w.WriteU8(static_cast<std::uint8_t>(entry.record_.verdict_));
  w.WriteString(entry.record_.verdict_detail_);
  w.WriteString(entry.record_.readable_time_);
  WriteFixedArray(w, entry.chain_hash_);
}

absl::StatusOr<MetaAuditChainEntry> ReadChainEntry(MetaReader& r) {
  MetaAuditChainEntry entry;
  auto index = r.ReadU64();
  if (!index.ok()) return index.status();
  entry.record_.log_index_ = *index;
  auto principal = r.ReadString(kMaxMetaPrincipalBytes);
  if (!principal.ok()) return principal.status();
  entry.record_.actor_principal_ = std::string(*principal);
  auto summary = r.ReadString(kMaxMetaAuditSummaryBytes);
  if (!summary.ok()) return summary.status();
  entry.record_.command_summary_ = std::string(*summary);
  auto verdict = r.ReadU8();
  if (!verdict.ok()) return verdict.status();
  if (*verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kAccepted) &&
      *verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kRejected)) {
    return MetaFailStopError("unknown audit verdict tag");
  }
  entry.record_.verdict_ = static_cast<MetaAuditVerdict>(*verdict);
  auto detail = r.ReadString(kMaxMetaAuditDetailBytes);
  if (!detail.ok()) return detail.status();
  entry.record_.verdict_detail_ = std::string(*detail);
  auto time = r.ReadString(kMaxMetaAuditReadableTimeBytes);
  if (!time.ok()) return time.status();
  entry.record_.readable_time_ = std::string(*time);
  auto hash = ReadFixedArray<32>(r);
  if (!hash.ok()) return hash.status();
  entry.chain_hash_ = *hash;
  return entry;
}

// Reads the u16 schema-version envelope shared by the snapshot and export
// blobs.
absl::Status ReadSchemaVersion(MetaReader& r) {
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version < kMetaMinReadableSchemaVersion ||
      *version > kMetaCurrentSchemaVersion) {
    return MetaFailStopError("unsupported audit blob schema version");
  }
  return absl::OkStatus();
}

// Apply-layer correspondence bug: the log index <-> record mapping broke.
// Fail stop rather than dropping or overwriting: never silently lose or
// rewrite an unexported record. The same input stream aborts every node at the
// same index, so this cannot fork the group.
[[noreturn]] void FatalAuditCorruption(std::string_view what,
                                       std::uint64_t log_index) {
  spdlog::critical(
      "meta audit store: {} at raft log index {}; the apply layer violated "
      "the log-index/audit-record correspondence — aborting per fail-stop "
      "policy",
      what, log_index);
  std::abort();
}

}  // namespace

absl::Status MetaAuditStore::Append(const MetaAuditRecord& record) {
  if (record.actor_principal_.size() > kMaxMetaPrincipalBytes ||
      record.command_summary_.size() > kMaxMetaAuditSummaryBytes ||
      record.verdict_detail_.size() > kMaxMetaAuditDetailBytes ||
      record.readable_time_.size() > kMaxMetaAuditReadableTimeBytes) {
    return MetaDomainRejectError("audit record field exceeds its cap");
  }
  const auto existing = window_.find(record.log_index_);
  if (existing != window_.end()) {
    if (existing->second.record_ == record) {
      return absl::OkStatus();  // replay of the same log entry: no-op
    }
    FatalAuditCorruption("same index with different content",
                         record.log_index_);
  }
  if (!window_.empty() && record.log_index_ <= window_.rbegin()->first) {
    FatalAuditCorruption("out-of-order new index", record.log_index_);
  }
  if (record.log_index_ <= pruned_floor_) {
    FatalAuditCorruption("index at or below the pruned floor",
                         record.log_index_);
  }
  if (window_.size() >= window_capacity_) {
    // The coordinator's proposal reservation makes this unreachable for
    // correctly orchestrated proposals; reaching it means the gate was
    // bypassed.
    FatalAuditCorruption("window capacity exceeded", record.log_index_);
  }
  const MetaHash256 hash = ComputeChainHash(chain_head_, record);
  window_.emplace(record.log_index_, MetaAuditChainEntry{record, hash});
  chain_head_ = hash;
  return absl::OkStatus();
}

std::optional<MetaAuditChainEntry> MetaAuditStore::Find(
    std::uint64_t log_index) const {
  const auto it = window_.find(log_index);
  if (it == window_.end()) return std::nullopt;
  return it->second;
}

bool MetaAuditStore::VerifyChain() const {
  MetaHash256 previous = anchor_;
  for (const auto& [index, entry] : window_) {
    const MetaHash256 recomputed = ComputeChainHash(previous, entry.record_);
    if (recomputed != entry.chain_hash_) return false;
    previous = recomputed;
  }
  return previous == chain_head_;
}

absl::StatusOr<std::string> MetaAuditStore::ExportThrough(
    std::uint64_t through) const {
  MetaWriter w;
  w.WriteU16(kMetaCurrentSchemaVersion);
  WriteFixedArray(w, anchor_);
  // Count the records at/below the watermark first (the writer is
  // append-only, so the count precedes the entries).
  std::uint32_t count = 0;
  for (const auto& [index, entry] : window_) {
    if (index > through) break;
    ++count;
  }
  w.WriteCount(count);
  for (const auto& [index, entry] : window_) {
    if (index > through) break;
    WriteChainEntry(w, entry);
  }
  return w.TakeBuffer();
}

absl::Status MetaAuditStore::PruneThrough(std::uint64_t through) {
  if (through <= pruned_floor_) {
    return absl::OkStatus();  // already pruned: idempotent no-op
  }
  const auto it = window_.find(through);
  if (it == window_.end()) {
    // The anchor advances to the pruned record's hash, so the watermark must
    // name a live record (gaps between audited indexes are normal).
    return MetaDomainRejectError(
        "prune watermark must name a record in the window");
  }
  anchor_ = it->second.chain_hash_;
  window_.erase(window_.begin(), std::next(it));
  pruned_floor_ = through;
  if (window_.empty()) chain_head_ = anchor_;
  return absl::OkStatus();
}

absl::StatusOr<std::string> MetaAuditStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaCurrentSchemaVersion);
  WriteFixedArray(w, anchor_);
  w.WriteU64(pruned_floor_);
  w.WriteCount(static_cast<std::uint32_t>(window_.size()));
  for (const auto& [index, entry] : window_) {
    WriteChainEntry(w, entry);
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaAuditStore> MetaAuditStore::Deserialize(
    std::string_view bytes, std::uint32_t window_capacity) {
  MetaReader r(bytes);
  if (absl::Status status = ReadSchemaVersion(r); !status.ok()) {
    return status;
  }
  auto anchor = ReadFixedArray<32>(r);
  if (!anchor.ok()) return anchor.status();
  auto floor = r.ReadU64();
  if (!floor.ok()) return floor.status();
  auto entries = r.ReadList<MetaAuditChainEntry>(
      window_capacity, [](MetaReader& rr) { return ReadChainEntry(rr); });
  if (!entries.ok()) return entries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaAuditStore store(window_capacity);
  store.anchor_ = *anchor;
  store.pruned_floor_ = *floor;
  std::uint64_t previous_index = 0;
  for (const auto& entry : *entries) {
    // Strictly increasing indexes above the floor; the map insert would
    // silently drop a duplicate, so check before emplacing.
    if (entry.record_.log_index_ <= previous_index ||
        entry.record_.log_index_ <= store.pruned_floor_) {
      return MetaFailStopError("audit window indexes are not increasing");
    }
    previous_index = entry.record_.log_index_;
    store.chain_head_ = entry.chain_hash_;
    store.window_.emplace(entry.record_.log_index_, entry);
  }
  if (store.window_.empty()) store.chain_head_ = store.anchor_;
  // A snapshot whose chain does not recompute is corruption: fail-stop,
  // identical on every node.
  if (!store.VerifyChain()) {
    return MetaFailStopError("audit window hash chain does not recompute");
  }
  return store;
}

absl::StatusOr<MetaAuditExport> DecodeMetaAuditExport(std::string_view bytes) {
  MetaReader r(bytes);
  if (absl::Status status = ReadSchemaVersion(r); !status.ok()) {
    return status;
  }
  auto anchor = ReadFixedArray<32>(r);
  if (!anchor.ok()) return anchor.status();
  auto entries = r.ReadList<MetaAuditChainEntry>(
      kMaxMetaAuditWindowRecords,
      [](MetaReader& rr) { return ReadChainEntry(rr); });
  if (!entries.ok()) return entries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaAuditExport out;
  out.anchor_before_ = *anchor;
  // Verify continuity inside the blob: each entry must chain from the
  // previous one, starting at the anchor.
  MetaHash256 previous = *anchor;
  std::uint64_t previous_index = 0;
  for (const auto& entry : *entries) {
    if (ComputeChainHash(previous, entry.record_) != entry.chain_hash_) {
      return MetaFailStopError("audit export hash chain does not recompute");
    }
    if (!out.records_.empty() && entry.record_.log_index_ <= previous_index) {
      return MetaFailStopError("audit export indexes are not increasing");
    }
    previous_index = entry.record_.log_index_;
    previous = entry.chain_hash_;
    out.records_.push_back(entry);
  }
  return out;
}

}  // namespace keylane::meta
