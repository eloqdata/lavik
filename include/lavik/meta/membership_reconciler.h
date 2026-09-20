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
#include <string>
#include <variant>
#include <vector>

#include "lavik/meta/coordinator.h"
#include "lavik/meta/raft.h"

namespace lavik::meta {

// Semantic configuration identity: election-time config log indices are not
// membership changes. Preserve every peer attribute so recovery never turns a
// changed endpoint, principal or voter role back into the old requested one.
struct MetaMembershipPeer {
  std::uint32_t id_ = 0;
  std::string endpoint_;
  std::string principal_;
  std::string data_control_endpoint_;
  std::string ctl_endpoint_;
  std::int32_t dc_id_ = 0;
  std::int32_t priority_ = 1;
  bool learner_ = false;
  bool new_joiner_ = false;
  bool operator==(const MetaMembershipPeer&) const = default;
};

struct MetaMembershipIntent {
  bool add_ = true;
  MetaMembershipPeer target_;
  MetaMemberRecord binding_;
  std::vector<MetaMembershipPeer> before_;
  std::vector<MetaMemberRecord> bindings_;
  bool operator==(const MetaMembershipIntent&) const = default;
};

// Capture the committed Raft peer set, sorted by id. Unsupported global
// configuration modes fail closed instead of being omitted from the intent.
absl::StatusOr<std::vector<MetaMembershipPeer>> CaptureMembershipConfig(
    const std::shared_ptr<MetaRaftConfig>& config);
// Reconciles the identity-store projection of a loaded config. Missing
// bindings are legal only while the state manager's durable genesis marker is
// active; one deterministic BindMetaMember effect is returned at a time.
absl::StatusOr<std::optional<BindMetaMember>> PlanInitialMetaBindings(
    const MetaCommittedView& view,
    const std::vector<MetaMembershipPeer>& config, bool initial_config);
// Bounded, versioned operation-intent codec; no changes to the generic journal
// format. The complete precondition and target survive snapshot/WAL recovery.
absl::StatusOr<std::string> EncodeMembershipIntent(const MetaMembershipIntent&);
absl::StatusOr<MetaMembershipIntent> DecodeMembershipIntent(std::string_view);

enum class MetaMembershipRaftAction { kAdd, kRemove, kYieldLeadership };
using MetaMembershipStep = std::variant<MetaCommand, MetaMembershipRaftAction>;
// Pure recovery planner. A missing step waits; an incompatible committed
// prefix requires operator recovery, never automatic rollback or replacement.
absl::StatusOr<std::optional<MetaMembershipStep>> PlanMembershipStep(
    const MetaCommittedView&, const MetaOperationRecord&,
    const std::vector<MetaMembershipPeer>& config, std::uint32_t local_id);

// Leader-owned membership workflow. Local Raft API entry runs on the
// proposal executor. Demotion joins that entry, not remote invite/leave
// completion; callbacks retain only their own inert result storage.
class MetaMembershipReconciler final : public MetaReconciler {
 public:
  MetaMembershipReconciler(bycorf::ForeignExecutor executor,
                           MetaProposalExecutor& proposals,
                           std::shared_ptr<MetaRaft> server,
                           std::shared_ptr<MetaStateMachine> state_machine,

                           std::shared_ptr<MetaMembershipGate> gate);
  ~MetaMembershipReconciler() override;
  void Start(MetaLeaderContext&) override;
  void CancelAndWait() override;
  // Close new admission and join local work before stopping Admin/worker/Raft.
  void Shutdown();
  bool accepting() const;

 private:
  struct Core;
  static bycorf::Task<absl::Status> Run(std::shared_ptr<Core>,
                                        MetaLeaderContext*);
  void Stop(bool permanent);
  std::shared_ptr<Core> core_;
};
}  // namespace lavik::meta
