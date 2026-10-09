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

#include "lavik/meta/audit_store.h"

#include <algorithm>
#include <cstdlib>
#include <string>
#include <utility>

#include "lavik/status_macros.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {

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
  LAVIK_ASSIGN_OR_RETURN(entry.log_index_, r.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(entry.actor_principal_,
                         r.ReadString(kMaxMetaPrincipalBytes));
  LAVIK_ASSIGN_OR_RETURN(entry.command_summary_,
                         r.ReadString(kMaxMetaAuditSummaryBytes));
  auto verdict = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(verdict);
  if (*verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kAccepted) &&
      *verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kRejected)) {
    return MetaFailStopError("unknown audit verdict tag");
  }
  entry.verdict_ = static_cast<MetaAuditVerdict>(*verdict);
  LAVIK_ASSIGN_OR_RETURN(entry.verdict_detail_,
                         r.ReadString(kMaxMetaAuditDetailBytes));
  LAVIK_ASSIGN_OR_RETURN(entry.readable_time_,
                         r.ReadString(kMaxMetaAuditReadableTimeBytes));
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

MetaAuditStore::MetaAuditStore(const MetaAuditStore& other)
    : pages_(other.pages_),
      record_count_(other.record_count_),
      window_capacity_(other.window_capacity_),
      policy_(other.policy_),
      pruned_floor_(other.pruned_floor_),
      dropped_total_(other.dropped_total_),
      dropped_through_(other.dropped_through_) {
  // Copying a store and mutating that same store require external exclusion,
  // provided by the state-machine lock. Concurrent copies of a const store
  // may seal the same page; no copy is published until all its pages are
  // sealed. The flag never becomes false. It does not wait for or synchronize
  // readers.
  for (const auto& page : pages_)
    if (!page->sealed_.load(std::memory_order_relaxed))
      page->sealed_.store(true, std::memory_order_relaxed);
}

MetaAuditStore& MetaAuditStore::operator=(const MetaAuditStore& other) {
  if (this != &other) {
    MetaAuditStore copy(other);
    Swap(copy);
  }
  return *this;
}

MetaAuditStore::MetaAuditStore(MetaAuditStore&& other) noexcept
    : MetaAuditStore(other.window_capacity_) {
  Swap(other);
}

MetaAuditStore& MetaAuditStore::operator=(MetaAuditStore&& other) noexcept {
  if (this != &other) {
    MetaAuditStore moved(std::move(other));
    Swap(moved);
  }
  return *this;
}

void MetaAuditStore::Swap(MetaAuditStore& other) noexcept {
  pages_.swap(other.pages_);
  std::swap(record_count_, other.record_count_);
  std::swap(window_capacity_, other.window_capacity_);
  std::swap(policy_, other.policy_);
  std::swap(pruned_floor_, other.pruned_floor_);
  std::swap(dropped_total_, other.dropped_total_);
  std::swap(dropped_through_, other.dropped_through_);
}

const MetaAuditRecord* MetaAuditStore::FindRecord(std::uint64_t index) const {
  const auto page = std::lower_bound(
      pages_.begin(), pages_.end(), index,
      [](const auto& p, auto i) { return p->records_.back().log_index_ < i; });
  if (page == pages_.end()) return nullptr;
  const auto record = std::lower_bound(
      (*page)->records_.begin(), (*page)->records_.end(), index,
      [](const auto& r, auto i) { return r.log_index_ < i; });
  return record != (*page)->records_.end() && record->log_index_ == index
             ? &*record
             : nullptr;
}

MetaAuditStore::Records& MetaAuditStore::WritablePage(std::size_t index) {
  auto& page = pages_[index];
  if (page->sealed_.load(std::memory_order_relaxed))
    page = std::make_shared<Page>(page->records_);
  return page->records_;
}

void MetaAuditStore::AppendUnchecked(const MetaAuditRecord& record) {
  if (pages_.empty() || pages_.back()->records_.size() == kRecordsPerPage) {
    auto page = std::make_shared<Page>();
    page->records_.push_back(record);
    pages_.push_back(std::move(page));
  } else {
    WritablePage(pages_.size() - 1).push_back(record);
  }
  ++record_count_;
}

void MetaAuditStore::DropFront() {
  if (pages_.front()->records_.size() == 1) {
    pages_.erase(pages_.begin());
  } else {
    WritablePage(0).pop_front();
  }
  --record_count_;
}

absl::Status MetaAuditStore::Append(const MetaAuditRecord& record,
                                    bool force_record) {
  if (record.actor_principal_.size() > kMaxMetaPrincipalBytes ||
      record.command_summary_.size() > kMaxMetaAuditSummaryBytes ||
      record.verdict_detail_.size() > kMaxMetaAuditDetailBytes ||
      record.readable_time_.size() > kMaxMetaAuditReadableTimeBytes) {
    return MetaDomainRejectError("audit record field exceeds its cap");
  }
  const auto* existing = FindRecord(record.log_index_);
  if (existing != nullptr) {
    if (*existing == record) {
      return absl::OkStatus();  // replay of the same log entry: no-op
    }
    FatalAuditCorruption("same index with different content",
                         record.log_index_);
  }
  if (!pages_.empty() &&
      record.log_index_ <= pages_.back()->records_.back().log_index_) {
    FatalAuditCorruption("out-of-order new index", record.log_index_);
  }
  if (record.log_index_ <= pruned_floor_) {
    FatalAuditCorruption("index at or below the pruned floor",
                         record.log_index_);
  }
  if (policy_ == MetaAuditPolicy::kDisabled && !force_record) {
    return absl::OkStatus();
  }
  if (record_count_ >= window_capacity_) {
    if (policy_ == MetaAuditPolicy::kStrictExport) {
      // The coordinator's proposal reservation makes this unreachable for
      // correctly orchestrated proposals; reaching it means the gate was
      // bypassed.
      FatalAuditCorruption("strict-export window capacity exceeded",
                           record.log_index_);
    }
    // A forced policy-change record while disabled follows bounded rotation
    // so the transition itself cannot disappear.
    const auto oldest = pages_.front()->records_.front().log_index_;
    DropFront();
    dropped_through_ = oldest;
    ++dropped_total_;
  }
  AppendUnchecked(record);
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
      record_count_ >= window_capacity_) {
    return MetaDomainRejectError(
        "strict-export requires one free slot for its policy-change record");
  }
  policy_ = policy;
  return absl::OkStatus();
}

std::optional<MetaAuditRecord> MetaAuditStore::Find(
    std::uint64_t log_index) const {
  const auto* record = FindRecord(log_index);
  if (record == nullptr) return std::nullopt;
  return *record;
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
  for (const auto& page : pages_) {
    if (page->records_.front().log_index_ > through) break;
    for (const auto& entry : page->records_) {
      if (entry.log_index_ > through) break;
      ++count;
    }
  }
  w.WriteCount(count);
  for (const auto& page : pages_) {
    if (page->records_.front().log_index_ > through) break;
    for (const auto& entry : page->records_) {
      if (entry.log_index_ > through) break;
      WriteRecord(w, entry);
    }
  }
  return w.TakeBuffer();
}

absl::Status MetaAuditStore::PruneThrough(std::uint64_t through) {
  if (through <= pruned_floor_) {
    return absl::OkStatus();  // already pruned: idempotent no-op
  }
  if (FindRecord(through) == nullptr) {
    // Require a live record so an operator cannot prune beyond this window.
    return MetaDomainRejectError(
        "prune watermark must name a record in the window");
  }
  // Whole pages can be released without cloning their records. Only a
  // surviving boundary page needs detaching from retained snapshots.
  std::size_t removed_pages = 0;
  std::size_t removed_records = 0;
  while (removed_pages < pages_.size() &&
         pages_[removed_pages]->records_.back().log_index_ <= through) {
    removed_records += pages_[removed_pages]->records_.size();
    ++removed_pages;
  }
  // Finish the only possible allocation before changing the live window.
  if (removed_pages < pages_.size() &&
      pages_[removed_pages]->records_.front().log_index_ <= through)
    (void)WritablePage(removed_pages);
  record_count_ -= removed_records;
  pages_.erase(pages_.begin(), pages_.begin() + removed_pages);
  while (!pages_.empty() &&
         pages_.front()->records_.front().log_index_ <= through)
    DropFront();
  pruned_floor_ = through;
  return absl::OkStatus();
}

void MetaAuditStore::WriteSnapshot(MetaWriter& w) const {
  w.WriteU16(kMetaFormatVersion);
  w.WriteU64(pruned_floor_);
  w.WriteU8(static_cast<std::uint8_t>(policy_));
  w.WriteU64(dropped_total_);
  w.WriteU64(dropped_through_);
  w.WriteCount(static_cast<std::uint32_t>(record_count_));
  for (const auto& page : pages_)
    for (const auto& entry : page->records_) WriteRecord(w, entry);
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
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported audit blob schema version");
  }
  auto floor = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(floor);
  MetaAuditPolicy policy = MetaAuditPolicy::kBoundedRotate;
  std::uint64_t dropped_total = 0;
  std::uint64_t dropped_through = 0;
  auto policy_tag = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(policy_tag);
  if (*policy_tag > static_cast<std::uint8_t>(MetaAuditPolicy::kStrictExport)) {
    return MetaFailStopError("unknown audit policy tag");
  }
  policy = static_cast<MetaAuditPolicy>(*policy_tag);
  LAVIK_ASSIGN_OR_RETURN(dropped_total, r.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(dropped_through, r.ReadU64());
  auto entries = r.ReadList<MetaAuditRecord>(
      window_capacity, [](MetaReader& rr) { return ReadRecord(rr); });
  LAVIK_RETURN_IF_ERROR(entries);
  LAVIK_RETURN_IF_ERROR(r.Finish());

  MetaAuditStore store(window_capacity);
  store.pruned_floor_ = *floor;
  store.policy_ = policy;
  store.dropped_total_ = dropped_total;
  store.dropped_through_ = dropped_through;
  std::uint64_t previous_index = 0;
  for (const auto& entry : *entries) {
    // Strictly increasing indexes above the floor, including across pages.
    if (entry.log_index_ <= previous_index ||
        entry.log_index_ <= store.pruned_floor_) {
      return MetaFailStopError("audit window indexes are not increasing");
    }
    previous_index = entry.log_index_;
    store.AppendUnchecked(entry);
  }
  return store;
}

absl::StatusOr<MetaAuditExport> DecodeMetaAuditExport(std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported audit export schema version");
  }
  std::uint64_t dropped_total = 0;
  std::uint64_t dropped_through = 0;
  LAVIK_ASSIGN_OR_RETURN(dropped_total, r.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(dropped_through, r.ReadU64());
  auto entries = r.ReadList<MetaAuditRecord>(
      kMaxMetaAuditWindowRecords,
      [](MetaReader& rr) { return ReadRecord(rr); });
  LAVIK_RETURN_IF_ERROR(entries);
  LAVIK_RETURN_IF_ERROR(r.Finish());

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

}  // namespace lavik::meta
