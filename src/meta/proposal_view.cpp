/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include "lavik/meta/proposal_view.h"

#include <cstdlib>
#include <type_traits>

#include "lavik/meta/state_apply.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {

template <typename T, typename... Alternatives>
constexpr bool kOneOf = (std::is_same_v<T, Alternatives> || ...);

void RequireCaptured(bool captured) {
  if (captured) return;
  spdlog::critical("meta proposal view: dependency was not captured");
  std::abort();
}

}  // namespace

MetaProposalView::CaptureData MetaProposalView::Extract(
    const MetaCommand& command, const MetaStores& stores,
    MetaCommittedCursor cursor) {
  CaptureData captured;
  Data& data = captured.selected_;
  data.audit_ = stores.audit_;
  data.cursor_ = cursor;
  // This is a closed dependency map for the production hooks, not a default
  // "unknown means absent" projection. Every new command must choose a branch.
  std::visit(
      [&](const auto& cmd) {
        using T = std::decay_t<decltype(cmd)>;
        if constexpr (kOneOf<T, RegisterNode, UpdateNode, RetireNode,
                             CreateGroup, AssignNodeToGroup,
                             RemoveNodeFromGroup, SetSlotMap, BeginGroupTerm,
                             ActivateAuthority, FenceGroup, PutPolicy,
                             SubmitOperation, ArchiveOperations, PruneAudit,
                             PruneOperationArchive, BindMetaMember,
                             RetireMetaMember, SetGroupReplicationState,
                             SetAuditPolicy, PutPopulationManifest,
                             PrunePopulationManifest, CommitDirectiveResult,
                             PruneTerminalReceipts, AbortControlledFailover>) {
          // These commands have no committed business reads in admission hooks.
          // AbortControlledFailover's full recovery simulation is separate.
        } else if constexpr (kOneOf<T, TransitionOperationPhase,
                                    CompleteOperation, AbortOperation>) {
          data.operation_id_ = cmd.operation_id_;
          data.operation_ =
              stores.operation_.FindOperationHeader(cmd.operation_id_);
        } else if constexpr (
            kOneOf<T, BeginControlledFailover, BeginUncontrolledFailover,
                   SetUncontrolledCandidate, StartCandidateRecovery,
                   AuthorizeFailoverPrepare, DegradeControlledFailover,
                   CommitControlledFailover, CommitUncontrolledFailover>) {
          data.group_id_ = cmd.group_id_;
          data.group_ = stores.topology_.FindGroup(cmd.group_id_);
          data.authority_ = stores.topology_.AuthorityFor(cmd.group_id_);
          // Absence proofs and LatestForNode may inspect other Groups. Keep the
          // complete compact fact domain, excluding slots and unrelated
          // payloads.
          captured.active_nodes_ = stores.identity_.ActiveNodeIds();
          captured.groups_ = stores.topology_.ObservationFacts();
          if constexpr (std::is_same_v<T, BeginUncontrolledFailover>) {
            if (cmd.trigger_reason_ != MetaAutomaticFailoverReason::kManual) {
              data.automatic_.emplace();
              data.automatic_->topology_epoch_ =
                  stores.topology_.TopologyEpoch();
              // Typed getters parse JSON. Extract raw current documents so
              // both decoding and its temporary allocations stay outside the
              // state lock; retained Policy history never enters this cut.
              captured.automatic_policy_ = stores.policy_.CurrentVersion(
                  std::string(kAutomaticUncontrolledFailoverPolicyId));
              captured.lease_policy_ = stores.policy_.CurrentVersion(
                  std::string(kAuthorityLeasePolicyId));
            }
          }
        } else {
          static_assert(kOneOf<T>,
                        "MetaCommand needs explicit proposal dependencies");
        }
      },
      command);
  return captured;
}

MetaProposalView::MetaProposalView(CaptureData data)
    : data_(std::move(data.selected_)) {
  if (data_.group_id_.has_value()) {
    facts_.emplace(MetaObservationFactsView(
        std::move(data.active_nodes_), std::move(data.groups_), data_.cursor_));
  }
  if (data_.automatic_.has_value()) {
    if (data.automatic_policy_.has_value()) {
      auto decoded = DecodeAutomaticUncontrolledFailoverPolicy(
          data.automatic_policy_->content_);
      if (decoded.ok()) {
        decoded->version_ = data.automatic_policy_->version_;
        data_.automatic_->automatic_ = *decoded;
      }
    }
    if (data.lease_policy_.has_value()) {
      auto decoded = DecodeAuthorityLeasePolicy(data.lease_policy_->content_);
      if (decoded.ok()) {
        decoded->version_ = data.lease_policy_->version_;
        data_.automatic_->lease_ = *decoded;
      }
    }
  }
}

MetaProposalView MetaProposalView::FromStores(const MetaCommand& command,
                                              const MetaStores& stores,
                                              MetaCommittedCursor cursor) {
  return MetaProposalView(Extract(command, stores, cursor));
}

const std::optional<MetaOperationHeader>& MetaProposalView::operation_header(
    const MetaOperationId& id) const {
  RequireCaptured(data_.operation_id_ == std::optional(id));
  return data_.operation_;
}

const std::optional<MetaTopologyGroupView>& MetaProposalView::group(
    std::string_view group_id) const {
  RequireCaptured(data_.group_id_.has_value() && *data_.group_id_ == group_id);
  return data_.group_;
}

const std::optional<MetaGroupAuthorityView>& MetaProposalView::authority(
    std::string_view group_id) const {
  RequireCaptured(data_.group_id_.has_value() && *data_.group_id_ == group_id);
  return data_.authority_;
}

const MetaCommittedFacts& MetaProposalView::facts() const {
  RequireCaptured(facts_.has_value());
  return *facts_;
}

const MetaProposalAutomaticPolicies& MetaProposalView::automatic_policies()
    const {
  RequireCaptured(data_.automatic_.has_value());
  return *data_.automatic_;
}

}  // namespace lavik::meta
