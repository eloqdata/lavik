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
#include <vector>

#include "lavik/replication.h"
#include "lavik/replication_history.h"

namespace lavik::detail {

// One boot-local direct edge, installed before child activation. Payloads
// remain in the shared optional cache; losing every payload keeps this exact
// boundary proof usable while the child origin can still continue.
struct NativeHistoryBridge {
  std::string id_;
  std::string group_id_;
  ClusterFailoverCompatibilityDomain parent_;
  ClusterFailoverCompatibilityDomain child_;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;
  ClusterPromotionPrepared promotion_;
  std::vector<std::uint64_t> child_origin_;
};

enum class NativeReparentPlan { kFull, kExact, kReplay };

// Classifies only comparable parent cursors. The host separately proves the
// current Owner/FDS/lease and independently checks all child-origin coverage.
NativeReparentPlan PlanNativeReparent(
    const NativeHistoryBridge& bridge,
    const ClusterFailoverCompatibilityDomain& parent,
    std::span<const std::uint64_t> cursor,
    const std::vector<std::vector<NativeHistoryRange>>& retained,
    bool child_origin_available);

}  // namespace lavik::detail
