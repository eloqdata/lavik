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

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "lavik/replication_history.h"

namespace lavik::detail {

// Each report is one complete, same-domain Applied cut. Coverage is optional
// availability evidence and never substitutes for that cut.
struct NativeRecoveryAdvertisement {
  std::string boot_id_;
  std::vector<std::uint64_t> applied_;
  std::vector<std::vector<NativeHistoryRange>> coverage_;
};

inline constexpr std::size_t kRecoveryMetadataBytes = 256 * 1024;
inline constexpr std::size_t kRecoveryReceiveBytes = 8 * 1024 * 1024;

// Coalesces copied worker-local intervals before retaining the newest eight
// ranges per origin flow. Truncating individual workers first would lose a
// continuous suffix whose ordinary and transactional events have different
// apply owners. This is availability evidence; readers still revalidate bytes.
std::vector<std::vector<NativeHistoryRange>> MergeWorkerHistoryCoverage(
    std::vector<std::vector<NativeHistoryRange>> ranges);

absl::StatusOr<std::string> EncodeRecoveryAdvertisement(
    const NativeRecoveryAdvertisement& report);
absl::StatusOr<NativeRecoveryAdvertisement> DecodeRecoveryAdvertisement(
    std::string_view wire);
absl::StatusOr<std::string> EncodeRecoveryEffectManifest(
    std::span<const NativeHistoryRecordInfo> records);
absl::StatusOr<std::vector<NativeHistoryRecordInfo>>
DecodeRecoveryEffectManifest(std::string_view wire);
bool RecoveryCovers(const NativeRecoveryAdvertisement& report, unsigned flow,
                    std::uint64_t lsn);
// Freeze the envelope only after discovery. Later donor loss changes coverage,
// not this target and not the receiver's actual Applied frontier.
absl::StatusOr<std::vector<std::uint64_t>> RecoveryTarget(
    std::span<const std::uint64_t> local,
    std::span<const NativeRecoveryAdvertisement> donors);

}  // namespace lavik::detail
