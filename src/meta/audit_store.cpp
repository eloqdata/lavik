#include "keylane/meta/audit_store.h"

#include <cstdlib>
#include <string>

#include "spdlog/spdlog.h"

namespace keylane::meta {

namespace {

// Canonical record encoding shared by snapshots and exports.
void WriteRecord(MetaWriter& w, const MetaAuditRecord& entry) {
  w.WriteU64(entry.log_index_);
  w.WriteString(entry.actor_principal_);
  w.WriteString(entry.command_summary_);
  w.WriteU8(static_cast<std::uint8_t>(entry.verdict_));
  w.WriteString(entry.verdict_detail_);
  w.WriteString(entry.readable_time_);
}

absl::StatusOr<MetaAuditRecord> ReadRecord(MetaReader& r) {
  MetaAuditRecord entry;
  auto index = r.ReadU64();
  if (!index.ok()) return index.status();
  entry.log_index_ = *index;
  auto principal = r.ReadString(kMaxMetaPrincipalBytes);
  if (!principal.ok()) return principal.status();
  entry.actor_principal_ = std::string(*principal);
  auto summary = r.ReadString(kMaxMetaAuditSummaryBytes);
  if (!summary.ok()) return summary.status();
  entry.command_summary_ = std::string(*summary);
  auto verdict = r.ReadU8();
  if (!verdict.ok()) return verdict.status();
  if (*verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kAccepted) &&
      *verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kRejected)) {
    return MetaFailStopError("unknown audit verdict tag");
  }
  entry.verdict_ = static_cast<MetaAuditVerdict>(*verdict);
  auto detail = r.ReadString(kMaxMetaAuditDetailBytes);
  if (!detail.ok()) return detail.status();
  entry.verdict_detail_ = std::string(*detail);
  auto time = r.ReadString(kMaxMetaAuditReadableTimeBytes);
  if (!time.ok()) return time.status();
  entry.readable_time_ = std::string(*time);
  return entry;
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

absl::Status MetaAuditStore::Append(const MetaAuditRecord& record,
                                    bool force_record) {
  if (record.actor_principal_.size() > kMaxMetaPrincipalBytes ||
      record.command_summary_.size() > kMaxMetaAuditSummaryBytes ||
      record.verdict_detail_.size() > kMaxMetaAuditDetailBytes ||
      record.readable_time_.size() > kMaxMetaAuditReadableTimeBytes) {
    return MetaDomainRejectError("audit record field exceeds its cap");
  }
  const auto existing = window_.find(record.log_index_);
  if (existing != window_.end()) {
    if (existing->second == record) {
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
  if (policy_ == MetaAuditPolicy::kDisabled && !force_record) {
    return absl::OkStatus();
  }
  if (window_.size() >= window_capacity_) {
    if (policy_ == MetaAuditPolicy::kStrictExport) {
      // The coordinator's proposal reservation makes this unreachable for
      // correctly orchestrated proposals; reaching it means the gate was
      // bypassed.
      FatalAuditCorruption("strict-export window capacity exceeded",
                           record.log_index_);
    }
    // A forced policy-change record while disabled follows bounded rotation
    // so the transition itself cannot disappear.
    const auto oldest = window_.begin();
    dropped_through_ = oldest->first;
    ++dropped_total_;
    window_.erase(oldest);
  }
  window_.emplace(record.log_index_, record);
  return absl::OkStatus();
}

absl::Status MetaAuditStore::SetPolicy(MetaAuditPolicy policy) {
  if (policy != MetaAuditPolicy::kDisabled &&
      policy != MetaAuditPolicy::kBoundedRotate &&
      policy != MetaAuditPolicy::kStrictExport) {
    return MetaDomainRejectError("unknown audit policy");
  }
  if (policy == MetaAuditPolicy::kStrictExport &&
      policy_ != MetaAuditPolicy::kStrictExport &&
      window_.size() >= window_capacity_) {
    return MetaDomainRejectError(
        "strict-export requires one free slot for its policy-change record");
  }
  policy_ = policy;
  return absl::OkStatus();
}

std::optional<MetaAuditRecord> MetaAuditStore::Find(
    std::uint64_t log_index) const {
  const auto it = window_.find(log_index);
  if (it == window_.end()) return std::nullopt;
  return it->second;
}

absl::StatusOr<std::string> MetaAuditStore::ExportThrough(
    std::uint64_t through) const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteU64(dropped_total_);
  w.WriteU64(dropped_through_);
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
    WriteRecord(w, entry);
  }
  return w.TakeBuffer();
}

absl::Status MetaAuditStore::PruneThrough(std::uint64_t through) {
  if (through <= pruned_floor_) {
    return absl::OkStatus();  // already pruned: idempotent no-op
  }
  const auto it = window_.find(through);
  if (it == window_.end()) {
    // Require a live record so an operator cannot prune beyond this window.
    return MetaDomainRejectError(
        "prune watermark must name a record in the window");
  }
  window_.erase(window_.begin(), std::next(it));
  pruned_floor_ = through;
  return absl::OkStatus();
}

void MetaAuditStore::WriteSnapshot(MetaWriter& w) const {
  w.WriteU16(kMetaFormatVersion);
  w.WriteU64(pruned_floor_);
  w.WriteU8(static_cast<std::uint8_t>(policy_));
  w.WriteU64(dropped_total_);
  w.WriteU64(dropped_through_);
  w.WriteCount(static_cast<std::uint32_t>(window_.size()));
  for (const auto& [index, entry] : window_) {
    WriteRecord(w, entry);
  }
}

absl::StatusOr<std::string> MetaAuditStore::Serialize() const {
  MetaWriter writer;
  WriteSnapshot(writer);
  return writer.TakeBuffer();
}

std::uint64_t MetaAuditStore::SerializedSize() const {
  MetaWriter counter(false);
  WriteSnapshot(counter);
  return counter.size();
}

absl::StatusOr<MetaAuditStore> MetaAuditStore::Deserialize(
    std::string_view bytes, std::uint32_t window_capacity) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported audit blob schema version");
  }
  auto floor = r.ReadU64();
  if (!floor.ok()) return floor.status();
  MetaAuditPolicy policy = MetaAuditPolicy::kBoundedRotate;
  std::uint64_t dropped_total = 0;
  std::uint64_t dropped_through = 0;
  auto policy_tag = r.ReadU8();
  if (!policy_tag.ok()) return policy_tag.status();
  if (*policy_tag > static_cast<std::uint8_t>(MetaAuditPolicy::kStrictExport)) {
    return MetaFailStopError("unknown audit policy tag");
  }
  policy = static_cast<MetaAuditPolicy>(*policy_tag);
  auto dropped_count = r.ReadU64();
  if (!dropped_count.ok()) return dropped_count.status();
  dropped_total = *dropped_count;
  auto dropped_floor = r.ReadU64();
  if (!dropped_floor.ok()) return dropped_floor.status();
  dropped_through = *dropped_floor;
  auto entries = r.ReadList<MetaAuditRecord>(
      window_capacity, [](MetaReader& rr) { return ReadRecord(rr); });
  if (!entries.ok()) return entries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaAuditStore store(window_capacity);
  store.pruned_floor_ = *floor;
  store.policy_ = policy;
  store.dropped_total_ = dropped_total;
  store.dropped_through_ = dropped_through;
  std::uint64_t previous_index = 0;
  for (const auto& entry : *entries) {
    // Strictly increasing indexes above the floor; the map insert would
    // silently drop a duplicate, so check before emplacing.
    if (entry.log_index_ <= previous_index ||
        entry.log_index_ <= store.pruned_floor_) {
      return MetaFailStopError("audit window indexes are not increasing");
    }
    previous_index = entry.log_index_;
    store.window_.emplace(entry.log_index_, entry);
  }
  return store;
}

absl::StatusOr<MetaAuditExport> DecodeMetaAuditExport(std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported audit export schema version");
  }
  std::uint64_t dropped_total = 0;
  std::uint64_t dropped_through = 0;
  auto count = r.ReadU64();
  if (!count.ok()) return count.status();
  dropped_total = *count;
  auto through = r.ReadU64();
  if (!through.ok()) return through.status();
  dropped_through = *through;
  auto entries = r.ReadList<MetaAuditRecord>(
      kMaxMetaAuditWindowRecords,
      [](MetaReader& rr) { return ReadRecord(rr); });
  if (!entries.ok()) return entries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaAuditExport out;
  out.dropped_total_ = dropped_total;
  out.dropped_through_ = dropped_through;
  std::uint64_t previous_index = 0;
  for (const auto& entry : *entries) {
    if (entry.log_index_ <= previous_index) {
      return MetaFailStopError("audit export indexes are not increasing");
    }
    previous_index = entry.log_index_;
    out.records_.push_back(entry);
  }
  return out;
}

}  // namespace keylane::meta
