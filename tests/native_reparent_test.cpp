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

#include "gtest/gtest.h"

namespace lavik::detail {
namespace {
NativeHistoryBridge Bridge() {
  NativeHistoryBridge bridge;
  bridge.parent_ = {1, "old-owner", "old-assignment", "old-boot", "parent", 2};
  bridge.child_ = {2, "new-owner", "new-assignment", "new-boot", "child", 1};
  bridge.promotion_.frozen_applied_next_lsns_ = {11, 21};
  bridge.child_origin_ = {1};
  return bridge;
}
TEST(NativeReparentTest, ExactParentNeedsNoPayloadButStillNeedsChildOrigin) {
  const auto bridge = Bridge();
  const std::vector<std::uint64_t> cursor{11, 21};
  EXPECT_EQ(PlanNativeReparent(bridge, bridge.parent_, cursor, {}, true),
            NativeReparentPlan::kExact);
  EXPECT_EQ(PlanNativeReparent(bridge, bridge.parent_, cursor, {}, false),
            NativeReparentPlan::kFull);
}
TEST(NativeReparentTest,
     LaggingRequiresEveryParentSuffixAndNoDivergentComponent) {
  const auto bridge = Bridge();
  std::vector<std::vector<NativeHistoryRange>> retained{{{5, 11}}, {{10, 21}}};
  EXPECT_EQ(
      PlanNativeReparent(bridge, bridge.parent_,
                         std::vector<std::uint64_t>{8, 20}, retained, true),
      NativeReparentPlan::kReplay);
  EXPECT_EQ(
      PlanNativeReparent(bridge, bridge.parent_,
                         std::vector<std::uint64_t>{12, 20}, retained, true),
      NativeReparentPlan::kFull);
  retained[1] = {{10, 19}, {20, 21}};
  EXPECT_EQ(
      PlanNativeReparent(bridge, bridge.parent_,
                         std::vector<std::uint64_t>{8, 18}, retained, true),
      NativeReparentPlan::kFull);
  auto changed = bridge.parent_;
  changed.source_boot_id_ = "restarted";
  EXPECT_EQ(
      PlanNativeReparent(bridge, changed, std::vector<std::uint64_t>{11, 21},
                         retained, true),
      NativeReparentPlan::kFull);
  EXPECT_EQ(PlanNativeReparent(bridge, bridge.parent_,
                               std::vector<std::uint64_t>{11}, retained, true),
            NativeReparentPlan::kFull);
}
}  // namespace
}  // namespace lavik::detail
