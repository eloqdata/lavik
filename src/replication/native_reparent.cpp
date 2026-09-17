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

#include "native_reparent.h"

#include <algorithm>

namespace lavik::detail {

NativeReparentPlan PlanNativeReparent(
    const NativeHistoryBridge& bridge,
    const ClusterFailoverCompatibilityDomain& parent,
    std::span<const std::uint64_t> cursor,
    const std::vector<std::vector<NativeHistoryRange>>& retained,
    bool child_origin_available) {
  const auto& boundary = bridge.promotion_.frozen_applied_next_lsns_;
  if (!child_origin_available || parent != bridge.parent_ || cursor.empty() ||
      cursor.size() != parent.flow_count_ || cursor.size() != boundary.size() ||
      bridge.child_origin_.size() != bridge.child_.flow_count_ ||
      bridge.child_origin_.empty())
    return NativeReparentPlan::kFull;
  bool exact = true;
  for (std::size_t flow = 0; flow < cursor.size(); ++flow) {
    if (cursor[flow] == 0 || cursor[flow] > boundary[flow])
      return NativeReparentPlan::kFull;
    exact &= cursor[flow] == boundary[flow];
  }
  if (exact) return NativeReparentPlan::kExact;
  if (retained.size() != cursor.size()) return NativeReparentPlan::kFull;
  for (std::size_t flow = 0; flow < cursor.size(); ++flow) {
    auto covered = cursor[flow];
    for (const auto& range : retained[flow]) {
      if (range.first_lsn_ > covered) break;
      covered = std::max(covered, range.end_lsn_);
      if (covered >= boundary[flow]) break;
    }
    if (covered < boundary[flow]) return NativeReparentPlan::kFull;
  }
  return NativeReparentPlan::kReplay;
}

}  // namespace lavik::detail
