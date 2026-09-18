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

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "lavik/meta/cluster_create.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/membership_reconciler.h"
#include "lavik/meta/raft.h"

namespace lavik::meta {

// Read-only adapter projection from Raft. `last_response_age_us_` is elapsed
// time since a transport-verified response, not a wall-clock timestamp.
struct MetaClusterCreatePeerProgress {
  std::uint32_t server_id_ = 0;
  std::uint64_t last_sm_committed_index_ = 0;
  std::uint64_t last_response_age_us_ = 0;
  bool operator==(const MetaClusterCreatePeerProgress&) const = default;
};

struct MetaClusterCreateRaftView {
  std::uint32_t local_server_id_ = 0;
  std::vector<MetaMembershipPeer> members_;
  std::vector<MetaClusterCreatePeerProgress> peer_progress_;
  // Maximum acceptable age of a transport-verified response. Zero means the
  // runtime cannot provide a safe recency window, so no remote peer can satisfy
  // the fixed creation barrier.
  std::uint64_t max_response_age_us_ = 0;
};

namespace detail {
// Stable child identity used by recovery and the Admin outcome. It is derived
// from the durable root operation and normalized Group id, so leadership
// changes never mint a competing population operation.
MetaOperationId ClusterCreateV1GroupOperationId(const MetaOperationId& root,
                                                std::string_view group_id);

// Exact desired/actual Meta validation shared by Admin admission and every
// recovered planner step. It is read-only and never repairs membership.
absl::Status ValidateClusterCreateMetaSet(
    const MetaCommittedView& view, const ClusterCreateManifestV1& manifest,
    const MetaClusterCreateRaftView& raft);

// Plans at most one committed effect from an atomic recovered view. A missing
// command means wait for Data; incompatible state requires operator recovery,
// never another destructive initialization. No I/O or in-memory phase cursor.
absl::StatusOr<std::optional<MetaCommand>> PlanClusterCreateStep(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaClusterCreateRaftView& raft);
}  // namespace detail

// Leader-scoped owner of durable cluster-create operations. Admin submits the
// complete intent before changing topology and merely waits for this owner.
// Every Start rescans the recovered operation journal. Demotion/shutdown joins
// local proposals, not remote Data completion, leaving durable work resumable.
class MetaClusterCreateReconciler final : public MetaReconciler {
 public:
  MetaClusterCreateReconciler(
      bycorf::ForeignExecutor executor,
      std::shared_ptr<MetaMembershipGate> membership_gate,
      std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status,
      std::shared_ptr<MetaRaft> server, std::uint64_t max_peer_response_age_us);
  ~MetaClusterCreateReconciler() override;
  void Start(MetaLeaderContext& context) override;
  void CancelAndWait() override;
  // Permanent process stop; call before draining Admin/Data listeners while
  // the worker and proposal executor can still complete accepted proposals.
  void Shutdown();
  // Thread-safe ingress check; shutdown closes it before joining proposals.
  bool accepting() const;

 private:
  struct Core;
  static bycorf::Task<absl::Status> Run(std::shared_ptr<Core> core,
                                        MetaLeaderContext* context);
  void Stop(bool permanent);
  std::shared_ptr<Core> core_;
};

}  // namespace lavik::meta
