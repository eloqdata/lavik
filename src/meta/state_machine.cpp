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

#include "lavik/meta/state_machine.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/str_cat.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {
template <std::size_t N>
std::string HexId(const std::array<std::uint8_t, N>& id) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(N * 2);
  for (const std::uint8_t byte : id) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

bool IsSafeLogTokenByte(std::uint8_t byte) {
  return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
         (byte >= '0' && byte <= '9') || byte == '-' || byte == '_' ||
         byte == '.' || byte == ':';
}

// Failover logs use whitespace-delimited key=value tokens. Percent-encode
// every byte outside a deliberately small ASCII alphabet so opaque committed
// identifiers cannot inject fields or record boundaries. '%' is encoded too,
// making the representation canonical and reversible.
std::string LogToken(std::string_view value) {
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    const auto byte = static_cast<std::uint8_t>(character);
    if (IsSafeLogTokenByte(byte)) {
      result.push_back(character);
      continue;
    }
    result.push_back('%');
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

struct FailoverCommitLog {
  std::string message_;
  bool data_loss_possible_ = false;
};

// Build the event from the pre-apply aggregate so candidate replacement and
// domain fallback remain distinguishable after the command overwrites the
// transition. The line is emitted only if deterministic apply accepts the
// entry; its fields deliberately mirror the durable audit identifiers.
std::optional<FailoverCommitLog> DescribeFailoverCommit(
    const MetaCommand& command, const MetaStores& before,
    std::uint64_t commit_index) {
  return std::visit(
      [&]<typename Command>(
          const Command& cmd) -> std::optional<FailoverCommitLog> {
        std::string event;
        std::string mode;
        std::string group;
        std::string transition = "none";
        std::string action = "none";
        std::string loss = "pending";
        std::string detail;
        bool data_loss_possible = false;

        if constexpr (std::is_same_v<Command, BeginControlledFailover>) {
          event = "begin";
          mode = "controlled";
          group = cmd.group_id_;
          transition = HexId(cmd.transition_id_);
          action = HexId(cmd.candidate_action_.action_id_);
          loss = "none";
          detail = " candidate=" +
                   LogToken(cmd.candidate_action_.candidate_.node_id_);
        } else if constexpr (std::is_same_v<Command,
                                            BeginUncontrolledFailover>) {
          event = "begin";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.transition_id_);
          if (cmd.candidate_action_.has_value()) {
            action = HexId(cmd.candidate_action_->action_id_);
          }
          loss = "unknown";
          data_loss_possible = true;
          if (cmd.trigger_reason_ != MetaAutomaticFailoverReason::kManual) {
            detail = absl::StrCat(
                " suspect_ms=", cmd.suspect_duration_ms_, " reason=",
                LogToken(MetaAutomaticFailoverReasonName(cmd.trigger_reason_)));
          }
        } else if constexpr (std::is_same_v<Command, StartCandidateRecovery>) {
          event = "recovery-start";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          detail = absl::StrCat(" recovery_deadline_ms=",
                                cmd.recovery_deadline_unix_ms_);
        } else if constexpr (std::is_same_v<Command,
                                            SetUncontrolledCandidate>) {
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          const auto group_before = before.topology_.FindGroup(cmd.group_id_);
          const MetaFailoverCandidateAction* previous = nullptr;
          if (group_before.has_value() &&
              group_before->failover_transition_.has_value() &&
              group_before->failover_transition_->candidate_action_
                  .has_value()) {
            previous = &*group_before->failover_transition_->candidate_action_;
          }
          const bool exact_post_effect =
              cmd.candidate_action_.has_value() && previous != nullptr &&
              group_before->failover_transition_->transition_id_ ==
                  cmd.expected_transition_.transition_id_ &&
              group_before->failover_transition_->revision_ == commit_index &&
              *previous == *cmd.candidate_action_;
          if (exact_post_effect) {
            // Apply accepts the same index against its exact post-state. The
            // original event depended on the overwritten previous action, so
            // replay cannot reconstruct it and must not emit a conflicting
            // selected/fallback/replaced classification at the same index.
            return std::nullopt;
          }
          if (!cmd.candidate_action_.has_value()) {
            // A fresh clear requires an installed candidate. If none is
            // visible, this can only become an accepted command through the
            // exact post-effect replay path; suppress that duplicate event
            // because the cleared action identity is no longer reconstructible.
            if (previous == nullptr) return std::nullopt;
            event = "candidate-cleared";
            action = HexId(previous->action_id_);
          } else {
            action = HexId(cmd.candidate_action_->action_id_);
            if (previous == nullptr) {
              event = "candidate-selected";
            } else if (previous->domain_ != cmd.candidate_action_->domain_) {
              event = "domain-fallback";
            } else {
              event = "candidate-replaced";
            }
            detail = absl::StrCat(
                " source_group_term=",
                cmd.candidate_action_->domain_.source_group_term_,
                " candidate=",
                LogToken(cmd.candidate_action_->candidate_.node_id_));
          }
          loss = "unknown";
          data_loss_possible = true;
        } else if constexpr (std::is_same_v<Command,
                                            AuthorizeFailoverPrepare>) {
          event = "authorize";
          mode = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone
                     ? "controlled"
                     : "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone ? "none"
                                                                 : "unknown";
          data_loss_possible = cmd.loss_if_cutover_ != MetaFailoverLoss::kNone;
        } else if constexpr (std::is_same_v<Command, AbortControlledFailover>) {
          event = "abort";
          mode = "controlled";
          group = cmd.group_id_;
          if (cmd.expected_transition_.has_value()) {
            transition = HexId(cmd.expected_transition_->transition_id_);
            const auto group_before = before.topology_.FindGroup(cmd.group_id_);
            if (!group_before.has_value() ||
                !group_before->failover_transition_.has_value() ||
                group_before->failover_transition_->transition_id_ !=
                    cmd.expected_transition_->transition_id_ ||
                group_before->failover_transition_->revision_ !=
                    cmd.expected_transition_->revision_ ||
                group_before->failover_transition_->mode_ !=
                    MetaFailoverMode::kControlled ||
                !group_before->failover_transition_->candidate_action_
                     .has_value()) {
              // A post-Begin Abort clears the transition that supplied its
              // action id. Exact replay is accepted against that post-state,
              // but emitting action=none would conflict with the original
              // event at the same commit index.
              return std::nullopt;
            }
            action = HexId(group_before->failover_transition_->candidate_action_
                               ->action_id_);
          }
          loss = "none";
          detail = " reason=" + LogToken(cmd.reason_);
        } else if constexpr (std::is_same_v<Command,
                                            DegradeControlledFailover>) {
          event = "degrade";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          if (cmd.expected_candidate_action_.has_value()) {
            action = HexId(cmd.expected_candidate_action_->action_id_);
          }
          loss = cmd.retain_candidate_action_ ? "none" : "unknown";
          data_loss_possible = !cmd.retain_candidate_action_;
          detail = " reason=" + LogToken(cmd.reason_);
        } else if constexpr (std::is_same_v<Command,
                                            CommitControlledFailover>) {
          event = "cutover";
          mode = "controlled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = "none";
          detail = " candidate=" + LogToken(cmd.expected_candidate_.node_id_);
        } else if constexpr (std::is_same_v<Command,
                                            CommitUncontrolledFailover>) {
          event = "cutover";
          mode = "uncontrolled";
          group = cmd.group_id_;
          transition = HexId(cmd.expected_transition_.transition_id_);
          action = HexId(cmd.action_id_);
          loss = cmd.loss_if_cutover_ == MetaFailoverLoss::kNone ? "none"
                                                                 : "unknown";
          data_loss_possible = cmd.loss_if_cutover_ != MetaFailoverLoss::kNone;
          detail = " candidate=" + LogToken(cmd.expected_candidate_.node_id_);
        } else {
          return std::nullopt;
        }

        return FailoverCommitLog{
            .message_ = absl::StrCat("failover event=", event, " mode=", mode,
                                     " group=", LogToken(group),
                                     " transition=", transition,
                                     " action=", action, " loss=", loss,
                                     " commit_index=", commit_index, detail),
            .data_loss_possible_ = data_loss_possible,
        };
      },
      command);
}

}  // namespace

absl::StatusOr<std::unique_ptr<MetaStateMachine>> MetaStateMachine::Open(
    const std::string& /*data_dir*/) {
  // Go validates the directory and restores the durable cut before transport
  // starts. Constructing a volatile state machine does not establish genesis.
  return std::unique_ptr<MetaStateMachine>(new MetaStateMachine());
}

MetaStores MetaStateMachine::StoresSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stores_;
}

MetaCommittedStoresSnapshot MetaStateMachine::CaptureStores() const {
  std::lock_guard lock(mutex_);
  return {stores_,
          {last_committed_idx_.load(std::memory_order_relaxed),
           last_state_change_idx_.load(std::memory_order_relaxed)}};
}

MetaDataPublicationView MetaStateMachine::CaptureDataPublication() const {
  MetaDataPublicationView::CaptureData data;
  {
    std::lock_guard lock(mutex_);
    data = MetaDataPublicationView::Extract(
        stores_, {last_committed_idx_.load(std::memory_order_relaxed),
                  last_state_change_idx_.load(std::memory_order_relaxed)});
  }
  return MetaDataPublicationView(std::move(data));
}

MetaDirectiveResultView MetaStateMachine::CaptureDirectiveResult(
    const MetaTerminalReceiptKey& key) const {
  MetaDirectiveResultView view;
  {
    std::lock_guard lock(mutex_);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.receipt_ = stores_.operation_.FindTerminalReceipt(key);
    if (!view.receipt_)
      view.operation_ = stores_.operation_.FindOperation(key.operation_id_);
  }
  return view;
}

std::optional<MetaTerminalReceipt> MetaStateMachine::FindTerminalReceipt(
    const MetaTerminalReceiptKey& key) const {
  std::lock_guard lock(mutex_);
  return stores_.operation_.FindTerminalReceipt(key);
}

MetaProposalView MetaStateMachine::CaptureProposal(
    const MetaCommand& command) const {
  MetaProposalView::CaptureData data;
  {
    std::lock_guard lock(mutex_);
    data = MetaProposalView::Extract(
        command, stores_,
        {last_committed_idx_.load(std::memory_order_relaxed),
         last_state_change_idx_.load(std::memory_order_relaxed)});
  }
  return MetaProposalView(std::move(data));
}

MetaObservationFactsView MetaStateMachine::CaptureObservationFacts() const {
  std::vector<std::string> active_nodes;
  std::vector<MetaObservationGroupFacts> groups;
  MetaCommittedCursor cursor;
  {
    std::lock_guard lock(mutex_);
    active_nodes = stores_.identity_.ActiveNodeIds();
    groups = stores_.topology_.ObservationFacts();
    cursor = {last_committed_idx_.load(std::memory_order_relaxed),
              last_state_change_idx_.load(std::memory_order_relaxed)};
  }
  return MetaObservationFactsView(std::move(active_nodes), std::move(groups),
                                  cursor);
}

MetaCommittedCursor MetaStateMachine::CaptureCommittedCursor() const {
  std::lock_guard lock(mutex_);
  return {last_committed_idx_.load(std::memory_order_relaxed),
          last_state_change_idx_.load(std::memory_order_relaxed)};
}

namespace {
bool SameWorkflow(const MetaOperationRecord& current,
                  const MetaOperationRecord& expected) {
  return current.operation_id_ == expected.operation_id_ &&
         current.operation_seq_ == expected.operation_seq_ &&
         current.kind_ == expected.kind_ &&
         current.revision_ == expected.revision_ &&
         current.lifecycle_ == expected.lifecycle_ &&
         current.intent_hash_ == expected.intent_hash_ &&
         current.intent_ == expected.intent_;
}
}  // namespace

MetaClusterCreateDiscovery MetaStateMachine::CaptureClusterCreateDiscovery()
    const {
  MetaClusterCreateDiscovery view;
  {
    std::lock_guard lock(mutex_);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.lifecycle_ = stores_.topology_.ClusterLifecycle();
    if (view.lifecycle_.state_ == MetaClusterLifecycle::kCreating)
      view.root_ =
          stores_.operation_.FindOperation(view.lifecycle_.root_operation_id_);
  }
  return view;
}

std::optional<MetaClusterCreateView> MetaStateMachine::CaptureClusterCreateView(
    const MetaClusterCreateDiscovery& expected,
    std::span<const MetaOperationId> children,
    std::span<const MetaHash256> manifests) const {
  if (!expected.root_) return std::nullopt;
  MetaClusterCreateView view;
  std::optional<MetaOperationRecord> root;
  {
    std::lock_guard lock(mutex_);
    if (stores_.topology_.ClusterLifecycle() != expected.lifecycle_)
      return std::nullopt;
    root = stores_.operation_.FindOperation(expected.root_->operation_id_);
    if (!root || !SameWorkflow(*root, *expected.root_)) return std::nullopt;
    view.root_ = std::move(*root);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.meta_members_ = stores_.identity_.MetaMembers();
    view.nodes_ = stores_.identity_.Nodes();
    view.groups_ = stores_.topology_.Groups();
    view.authorities_.reserve(view.groups_.size());
    for (const auto& group : view.groups_)
      view.authorities_.push_back(
          *stores_.topology_.AuthorityFor(group.group_id_));
    view.topology_epoch_ = stores_.topology_.TopologyEpoch();
    view.slots_ = stores_.topology_.SlotRanges();
    // Registered policies are validated on apply/restore. Their presence does
    // not require copying or decoding the retained document again.
    view.automatic_failover_policy_ =
        stores_.policy_
            .LatestVersion(std::string(kAutomaticUncontrolledFailoverPolicyId))
            .has_value();
    view.authority_lease_policy_ =
        stores_.policy_.LatestVersion(std::string(kAuthorityLeasePolicyId))
            .has_value();
    view.candidate_recovery_policy_ =
        stores_.policy_.LatestVersion(std::string(kCandidateRecoveryPolicyId))
            .has_value();
    for (const auto& digest : manifests)
      if (stores_.population_manifest_.Contains(digest))
        view.present_manifests_.push_back(digest);
    for (const auto& id : children)
      view.children_.emplace(
          id, MetaClusterCreateChild{stores_.operation_.OperationKnown(id),
                                     stores_.operation_.FindOperation(id)});
  }
  return view;
}

MetaMembershipDiscovery MetaStateMachine::CaptureMembershipDiscovery() const {
  MetaMembershipDiscovery view;
  {
    std::lock_guard lock(mutex_);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    // The borrowed range is confined to the state lock. Scan headers only and
    // copy the same first active operation previously selected by
    // LiveOperations.
    for (const auto& operation : stores_.operation_.LiveOperationsView()) {
      if (operation.kind_ == kMetaMembershipOperationKind &&
          operation.lifecycle_ != MetaOperationLifecycle::kCompleted &&
          operation.lifecycle_ != MetaOperationLifecycle::kAborted) {
        view.operation_ = operation;
        break;
      }
    }
    if (!view.operation_)
      view.initial_bindings_ = stores_.identity_.MetaMembers();
  }
  return view;
}

std::optional<MetaMembershipView> MetaStateMachine::CaptureMembershipView(
    const MetaOperationRecord& expected,
    std::span<const std::uint32_t> member_ids) const {
  // Stable ordering and deduplication are caller-independent, but require no
  // state exclusion. Unknown requested bindings remain absent in the result.
  std::vector<std::uint32_t> ids(member_ids.begin(), member_ids.end());
  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
  MetaMembershipView view;
  std::optional<MetaOperationRecord> operation;
  {
    std::lock_guard lock(mutex_);
    operation = stores_.operation_.FindOperation(expected.operation_id_);
    if (!operation || !SameWorkflow(*operation, expected)) return std::nullopt;
    view.operation_ = std::move(*operation);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.bindings_.reserve(ids.size());
    for (const auto id : ids)
      if (auto binding = stores_.identity_.FindMetaMember(id))
        view.bindings_.push_back(std::move(*binding));
  }
  return view;
}

std::vector<MetaOperationSummary> MetaStateMachine::OperationSummaries(
    std::uint64_t after, std::size_t limit) const {
  std::lock_guard lock(mutex_);
  limit = std::min<std::size_t>(limit, 100);
  std::vector<const MetaOperationRecord*> ordered;
  for (const auto& operation : stores_.operation_.LiveOperationsView()) {
    if (operation.operation_seq_ > after) ordered.push_back(&operation);
  }
  const auto count = std::min(limit, ordered.size());
  std::partial_sort(ordered.begin(), ordered.begin() + count, ordered.end(),
                    [](const auto* a, const auto* b) {
                      return a->operation_seq_ < b->operation_seq_;
                    });
  std::vector<MetaOperationSummary> result;
  result.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    const auto& operation = *ordered[i];
    result.push_back({operation.operation_id_, operation.operation_seq_,
                      operation.lifecycle_, operation.kind_.substr(0, 512),
                      operation.kind_phase_blob_.substr(0, 512),
                      operation.terminal_result_.substr(0, 512)});
  }
  return result;
}

MetaCommittedStatusView MetaStateMachine::StatusSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  MetaCommittedStatusView view;
  view.applied_index_ = last_committed_idx_.load(std::memory_order_relaxed);
  view.topology_epoch_ = stores_.topology_.TopologyEpoch();
  view.cluster_lifecycle_ = stores_.topology_.ClusterLifecycle();
  view.cluster_non_pristine_ =
      view.cluster_lifecycle_.state_ == MetaClusterLifecycle::kUninitialized &&
      HasDataClusterArtifacts(stores_);
  view.active_cluster_create_operation_ =
      view.cluster_lifecycle_.state_ == MetaClusterLifecycle::kCreating;
  if (view.active_cluster_create_operation_) {
    const auto operation = stores_.operation_.FindOperation(
        view.cluster_lifecycle_.root_operation_id_);
    if (operation.has_value()) {
      view.active_cluster_create_phase_ = operation->kind_phase_blob_;
      MetaOperationId intent_root{};
      auto manifest =
          DecodeClusterCreateRequest(operation->intent_, &intent_root);
      if (manifest.ok()) {
        view.active_cluster_create_data_nodes_.reserve(
            manifest->data_nodes_.size());
        for (const auto& node : manifest->data_nodes_) {
          view.active_cluster_create_data_nodes_.push_back(node.node_id_);
        }
      }
    }
  }
  view.meta_members_ = stores_.identity_.MetaMembers();
  view.data_nodes_ = stores_.identity_.Nodes();
  const auto automatic_policy =
      stores_.policy_.CurrentAutomaticUncontrolledFailover();
  const auto authority_lease_policy = stores_.policy_.CurrentAuthorityLease();
  if (automatic_policy.has_value()) {
    view.automatic_failover_threshold_ms_ = automatic_policy->suspect_after_ms_;
  }
  for (MetaTopologyGroupView topology : stores_.topology_.Groups()) {
    auto grant = stores_.topology_.AuthorityFor(topology.group_id_);
    // Cross-store validation guarantees the grant half exists for every
    // topology group; retain a defensive fenced value if corrupted in memory
    // so status reports NOT READY instead of inventing authority.
    MetaCommittedStatusGroup group;
    group.topology_ = std::move(topology);
    if (grant.has_value()) group.grant_ = std::move(*grant);
    const auto& record = group.topology_.record_;
    group.manifest_present_ = record.population_manifest_revision_ != 0 &&
                              stores_.population_manifest_.Contains(
                                  record.population_manifest_digest_);
    group.policy_active_ =
        automatic_policy.has_value() && authority_lease_policy.has_value();
    view.groups_.push_back(std::move(group));
  }
  for (std::uint32_t slot = 0; slot < kMetaSlotCount;) {
    auto owner = stores_.topology_.SlotOwner(slot);
    if (!owner.has_value()) {
      ++slot;
      continue;
    }
    std::uint32_t last = slot;
    while (last + 1 < kMetaSlotCount &&
           stores_.topology_.SlotOwner(last + 1) == owner) {
      ++last;
    }
    view.slot_ranges_.push_back(
        {.first_ = slot, .last_ = last, .group_id_ = std::move(*owner)});
    slot = last + 1;
  }
  return view;
}

void MetaStateMachine::SetCommitEventSink(MetaCommitEventSink sink) {
  std::lock_guard<std::mutex> lock(sink_mutex_);
  commit_event_sink_ = std::move(sink);
}

absl::StatusOr<std::shared_ptr<MetaRaftBuffer>> MetaStateMachine::EncodeCommand(
    const MetaCommand& command) {
  absl::StatusOr<std::string> encoded = EncodeMetaCommand(command);
  if (!encoded.ok()) return encoded.status();
  std::shared_ptr<MetaRaftBuffer> out = MetaRaftBuffer::alloc(encoded->size());
  std::memcpy(out->data_begin(), encoded->data(), encoded->size());
  return out;
}

std::shared_ptr<MetaRaftBuffer> MetaStateMachine::commit(std::uint64_t log_idx,
                                                         MetaRaftBuffer& data) {
  const std::string_view bytes(
      data.size() > 0 ? reinterpret_cast<const char*>(data.data_begin()) : "",
      data.size());
  absl::StatusOr<MetaCommand> decoded = DecodeMetaCommand(bytes);
  if (!decoded.ok()) {
    // A decode failure is fail-stop, strictly
    // separated from a domain rejection (cleanly decoded, refused by
    // ApplyCommitted, index consumed). The same byte sequence fails
    // identically on every node, so aborting here cannot fork the group —
    // this is also how an old binary loudly refuses a newer encoding after an
    // upgrade.
    spdlog::critical(
        "meta state machine: undecodable committed command at {}: {}", log_idx,
        decoded.status().message());
    std::abort();
  }

  // The trusted entry's injected ActorContext rides the command struct and
  // the raft-log encoding (commands.h), so the decoded command carries
  // the same actor on every node; apply only copies it into audit/journal.
  // Unforgeability is enforced at the ctl/coordinator entry layer, not here.
  const ActorContext actor =
      std::visit([](const auto& cmd) { return cmd.actor_; }, *decoded);
  MetaApplyResult applied;
  std::optional<FailoverCommitLog> failover_log;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    failover_log = DescribeFailoverCommit(*decoded, stores_, log_idx);
    applied = ApplyCommitted(stores_, log_idx, *decoded, actor.principal_,
                             actor.readable_time_);
    // Publish the cursor while the same state lock still protects the effects
    // it names. Readers cannot copy stores_ until the cursor and sink event
    // for this commit are both visible.
    last_committed_idx_ = log_idx;
    last_state_change_idx_ = log_idx;
    if (std::holds_alternative<BindMetaMember>(*decoded) ||
        std::holds_alternative<RetireMetaMember>(*decoded)) {
      meta_bindings_.store(
          std::make_shared<const std::vector<MetaMemberRecord>>(
              stores_.identity_.MetaMembers()),
          std::memory_order_release);
    }
    // Coordinator commit-event sink: still under the state mutex, so consumers
    // observe the event atomically with the apply. The sink contract
    // (state_machine.h) keeps this O(1) and non-blocking.
    std::lock_guard<std::mutex> sink_lock(sink_mutex_);
    if (commit_event_sink_) {
      commit_event_sink_(log_idx, applied);
    }
  }
  if (failover_log.has_value() &&
      applied.verdict_ == MetaAuditVerdict::kAccepted) {
    if (failover_log->data_loss_possible_) {
      spdlog::warn("{}", failover_log->message_);
    } else {
      spdlog::info("{}", failover_log->message_);
    }
  }
  const std::string completion = EncodeMetaApplyResult(applied);
  std::shared_ptr<MetaRaftBuffer> result =
      MetaRaftBuffer::alloc(completion.size());
  if (!completion.empty()) {
    std::memcpy(result->data_begin(), completion.data(), completion.size());
  }
  return result;
}

void MetaStateMachine::Advance(std::uint64_t index) {
  std::lock_guard lock(mutex_);
  if (index < last_committed_idx_.load()) std::terminate();
  last_committed_idx_.store(index, std::memory_order_release);
}

absl::StatusOr<std::string> MetaStateMachine::Capture(
    std::uint64_t index) const {
  std::lock_guard lock(mutex_);
  if (index != last_committed_idx_.load()) {
    return absl::FailedPreconditionError(
        "snapshot cut differs from applied state");
  }
  return stores_.Serialize();
}

absl::Status MetaStateMachine::Install(std::uint64_t index,
                                       std::string_view image) {
  auto stores = MetaStores::Deserialize(image);
  if (!stores.ok()) return stores.status();
  std::lock_guard lock(mutex_);
  if (index < last_committed_idx_.load()) {
    return absl::FailedPreconditionError(
        "snapshot would regress applied state");
  }
  LAVIK_MAYBE_CRASH_AT("meta-snapshot-before-membership");
  stores_ = std::move(*stores);
  meta_bindings_.store(std::make_shared<const std::vector<MetaMemberRecord>>(
                           stores_.identity_.MetaMembers()),
                       std::memory_order_release);
  last_committed_idx_.store(index, std::memory_order_release);
  last_state_change_idx_.store(index, std::memory_order_release);
  return absl::OkStatus();
}

}  // namespace lavik::meta
