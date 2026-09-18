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

#include "native_recovery.h"

#include <algorithm>
#include <limits>

namespace lavik::detail {
namespace {
void Put(std::string& out, std::uint64_t value, unsigned bytes) {
  for (unsigned i = 0; i < bytes; ++i)
    out.push_back(static_cast<char>(value >> (8 * i)));
}
bool Get(std::string_view& in, std::uint64_t& value, unsigned bytes) {
  if (in.size() < bytes) return false;
  value = 0;
  for (unsigned i = 0; i < bytes; ++i)
    value |= std::uint64_t(static_cast<unsigned char>(in[i])) << (8 * i);
  in.remove_prefix(bytes);
  return true;
}
absl::Status Validate(const NativeRecoveryAdvertisement& report) {
  if (report.boot_id_.empty() || report.boot_id_.size() > 128 ||
      report.applied_.empty() || report.applied_.size() > 1024 ||
      report.coverage_.size() != report.applied_.size()) {
    return absl::InvalidArgumentError(
        "invalid recovery report identity or layout");
  }
  for (std::size_t flow = 0; flow < report.applied_.size(); ++flow) {
    if (report.applied_[flow] == 0 || report.coverage_[flow].size() > 8)
      return absl::InvalidArgumentError(
          "invalid recovery report cut or range count");
    std::uint64_t previous = 0;
    for (const auto& range : report.coverage_[flow]) {
      if (range.first_lsn_ == 0 || range.first_lsn_ >= range.end_lsn_ ||
          range.first_lsn_ < previous ||
          range.end_lsn_ > report.applied_[flow]) {
        return absl::InvalidArgumentError(
            "recovery coverage is not a retained Applied prefix range");
      }
      previous = range.end_lsn_;
    }
  }
  return absl::OkStatus();
}
}  // namespace

std::vector<std::vector<NativeHistoryRange>> MergeWorkerHistoryCoverage(
    std::vector<std::vector<NativeHistoryRange>> ranges) {
  for (auto& flow : ranges) {
    std::ranges::sort(flow, {}, &NativeHistoryRange::first_lsn_);
    std::size_t count = 0;
    for (const auto range : flow) {
      if (count != 0 && flow[count - 1].end_lsn_ >= range.first_lsn_) {
        flow[count - 1].end_lsn_ =
            std::max(flow[count - 1].end_lsn_, range.end_lsn_);
      } else {
        flow[count++] = range;
      }
    }
    // The sampled input can contain many more ranges than the wire result.
    // Release its allocation before the caller drops the temporary merge
    // charge; resize/erase alone would retain that unbounded capacity.
    flow = std::vector<NativeHistoryRange>(
        flow.begin() + (count > 8 ? count - 8 : 0), flow.begin() + count);
  }
  return ranges;
}

absl::StatusOr<std::string> EncodeRecoveryAdvertisement(
    const NativeRecoveryAdvertisement& report) {
  auto status = Validate(report);
  if (!status.ok()) return status;
  std::string out = "LVA1";
  Put(out, report.boot_id_.size(), 2);
  out.append(report.boot_id_);
  Put(out, report.applied_.size(), 2);
  for (std::size_t flow = 0; flow < report.applied_.size(); ++flow) {
    Put(out, report.applied_[flow], 8);
    Put(out, report.coverage_[flow].size(), 1);
    for (const auto& range : report.coverage_[flow]) {
      Put(out, range.first_lsn_, 8);
      Put(out, range.end_lsn_, 8);
    }
  }
  return out;
}
absl::StatusOr<NativeRecoveryAdvertisement> DecodeRecoveryAdvertisement(
    std::string_view wire) {
  const auto invalid =
      absl::InvalidArgumentError("malformed recovery advertisement");
  if (wire.size() > kRecoveryMetadataBytes || !wire.starts_with("LVA1"))
    return invalid;
  wire.remove_prefix(4);
  std::uint64_t count = 0;
  if (!Get(wire, count, 2) || count == 0 || count > 128 || wire.size() < count)
    return invalid;
  NativeRecoveryAdvertisement report;
  report.boot_id_ = wire.substr(0, count);
  wire.remove_prefix(count);
  if (!Get(wire, count, 2) || count == 0 || count > 1024) return invalid;
  report.applied_.resize(count);
  report.coverage_.resize(count);
  for (std::size_t flow = 0; flow < report.applied_.size(); ++flow) {
    if (!Get(wire, report.applied_[flow], 8) || !Get(wire, count, 1) ||
        count > 8)
      return invalid;
    for (std::uint64_t i = 0; i < count; ++i) {
      NativeHistoryRange range;
      if (!Get(wire, range.first_lsn_, 8) || !Get(wire, range.end_lsn_, 8))
        return invalid;
      report.coverage_[flow].push_back(range);
    }
  }
  if (!wire.empty()) return invalid;
  auto status = Validate(report);
  if (!status.ok()) return status;
  return report;
}
absl::StatusOr<std::string> EncodeRecoveryEffectManifest(
    std::span<const NativeHistoryRecordInfo> records) {
  if (records.empty() || records.size() > 1024)
    return absl::InvalidArgumentError("invalid recovery effect size");
  std::string out = "LVE1";
  Put(out, records.size(), 2);
  std::uint64_t total = 0;
  for (std::size_t i = 0; i < records.size(); ++i) {
    const auto& record = records[i];
    if (record.flow_id_ >= 1024 || record.lsn_ == 0 ||
        record.lsn_ == std::numeric_limits<std::uint64_t>::max() ||
        record.bytes_ == 0 || record.bytes_ > kRecoveryReceiveBytes - total ||
        (i != 0 && records[i - 1].flow_id_ >= record.flow_id_))
      return absl::InvalidArgumentError("invalid recovery effect record");
    total += record.bytes_;
    Put(out, record.flow_id_, 2);
    Put(out, record.lsn_, 8);
    Put(out, record.bytes_, 8);
  }
  return out;
}
absl::StatusOr<std::vector<NativeHistoryRecordInfo>>
DecodeRecoveryEffectManifest(std::string_view wire) {
  const auto invalid =
      absl::InvalidArgumentError("malformed recovery effect manifest");
  if (!wire.starts_with("LVE1") || wire.size() > kRecoveryMetadataBytes)
    return invalid;
  wire.remove_prefix(4);
  std::uint64_t count = 0;
  if (!Get(wire, count, 2) || count == 0 || count > 1024 ||
      wire.size() != count * 18)
    return invalid;
  std::vector<NativeHistoryRecordInfo> records;
  for (std::uint64_t i = 0; i < count; ++i) {
    std::uint64_t flow = 0;
    NativeHistoryRecordInfo record;
    if (!Get(wire, flow, 2) || !Get(wire, record.lsn_, 8) ||
        !Get(wire, record.bytes_, 8))
      return invalid;
    record.flow_id_ = flow;
    records.push_back(record);
  }
  if (!EncodeRecoveryEffectManifest(records).ok()) return invalid;
  return records;
}
bool RecoveryCovers(const NativeRecoveryAdvertisement& report, unsigned flow,
                    std::uint64_t lsn) {
  return flow < report.coverage_.size() &&
         std::ranges::any_of(
             report.coverage_[flow],
             [lsn](const auto& range) {
               return range.first_lsn_ <= lsn && lsn < range.end_lsn_;
             });
}
absl::StatusOr<std::vector<std::uint64_t>> RecoveryTarget(
    std::span<const std::uint64_t> local,
    std::span<const NativeRecoveryAdvertisement> donors) {
  if (local.empty() || local.size() > 1024 ||
      std::ranges::find(local, 0) != local.end())
    return absl::InvalidArgumentError("invalid local recovery Applied cut");
  std::vector<std::uint64_t> target(local.begin(), local.end());
  for (const auto& donor : donors) {
    auto status = Validate(donor);
    if (!status.ok()) return status;
    if (donor.applied_.size() != target.size())
      return absl::InvalidArgumentError(
          "recovery report layout differs from its origin domain");
    for (std::size_t flow = 0; flow < target.size(); ++flow)
      target[flow] = std::max(target[flow], donor.applied_[flow]);
  }
  return target;
}
}  // namespace lavik::detail
