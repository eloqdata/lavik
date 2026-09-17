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

#include "lavik/meta/automatic_failover_reconciler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <ranges>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/policy_store.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {

constexpr std::array<std::uint64_t, 5> kRetryBackoffMs = {100, 200, 400, 800,
                                                          1'000};

#if LAVIK_FAULTS_ENABLED
std::chrono::milliseconds TestPauseDelay(const char* variable) {
  const char* configured = std::getenv(variable);
  std::uint64_t delay_ms = 0;
  if (configured == nullptr) return std::chrono::milliseconds::zero();
  const char* end = configured + std::strlen(configured);
  const auto parsed = std::from_chars(configured, end, delay_ms);
  if (parsed.ec != std::errc{} || parsed.ptr != end || delay_ms == 0 ||
      delay_ms > 60'000) {
    return std::chrono::milliseconds::zero();
  }
  return std::chrono::milliseconds(delay_ms);
}

#endif

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::ranges::all_of(value,
                             [](std::uint8_t byte) { return byte == 0; });
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(N * 2);
  for (std::uint8_t byte : value) {
    result.push_back(kDigits[byte >> 4]);
    result.push_back(kDigits[byte & 0x0f]);
  }
  return result;
}

std::optional<MetaAssignmentId> AssignmentFor(
    const MetaTopologyGroupView& group, std::string_view node_id) {
  const auto found =
      std::lower_bound(group.members_.begin(), group.members_.end(), node_id,
                       [](const MetaGroupMember& member, std::string_view id) {
                         return member.node_id_ < id;
                       });
  if (found == group.members_.end() || found->node_id_ != node_id) {
    return std::nullopt;
  }
  return found->assignment_id_;
}

bool ActiveGrantMatches(const MetaTopologyGroupView& group,
                        const MetaGroupAuthorityView& grant) {
  return grant.grant_.has_value() &&
         grant.group_term_ == group.record_.group_term_ &&
         grant.grant_->owner_ == group.record_.owner_;
}

const MetaDataControlRuntimeNode* RuntimeNodeFor(
    const MetaDataControlRuntimeSnapshot& runtime, std::string_view node_id) {
  const auto found =
      std::lower_bound(runtime.nodes_.begin(), runtime.nodes_.end(), node_id,
                       [](const MetaDataControlRuntimeNode& node,
                          std::string_view id) { return node.node_id_ < id; });
  return found == runtime.nodes_.end() || found->node_id_ != node_id ? nullptr
                                                                     : &*found;
}

const MetaDataControlRuntimeGroup* RuntimeGroupFor(
    const MetaDataControlRuntimeNode& runtime, std::string_view group_id) {
  const auto found = std::lower_bound(
      runtime.groups_.begin(), runtime.groups_.end(), group_id,
      [](const MetaDataControlRuntimeGroup& group, std::string_view id) {
        return group.group_id_ < id;
      });
  return found == runtime.groups_.end() || found->group_id_ != group_id
             ? nullptr
             : &*found;
}

bool RuntimeProjectionIsCurrent(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaAssignmentId& assignment,
    const MetaDataControlRuntimeNode& runtime_node) {
  const MetaDataControlRuntimeGroup* runtime_group =
      RuntimeGroupFor(runtime_node, group.group_id_);
  return runtime_node.leadership_generation_ != 0 &&
         runtime_node.validated_committed_high_water_ >= view.applied_index() &&
         runtime_node.topology_epoch_ == view.topology().TopologyEpoch() &&
         runtime_group != nullptr &&
         runtime_group->assignment_id_ == assignment &&
         runtime_group->group_term_ == group.record_.group_term_ &&
         runtime_group->manifest_revision_ ==
             group.record_.population_manifest_revision_ &&
         runtime_group->manifest_digest_ ==
             group.record_.population_manifest_digest_ &&
         runtime_group->partition_replication_epoch_ ==
             group.record_.partition_replication_epoch_;
}

MetaOwnerAuthorityAnchor OwnerAnchor(
    const MetaTopologyGroupView& group, const MetaAssignmentId& assignment,
    const MetaDataControlRuntimeNode* runtime_node, bool projection_current) {
  MetaOwnerAuthorityAnchor result{
      .group_id_ = group.group_id_,
      .owner_node_id_ = group.record_.owner_,
      .owner_assignment_id_ = assignment,
      .group_term_ = group.record_.group_term_,
  };
  if (runtime_node != nullptr && projection_current) {
    result.control_revision_ = runtime_node->control_revision_;
  }
  return result;
}

MetaOwnerAuthorityAnchor ObservedAnchor(
    const MetaObservedOwnerProjection& observed) {
  return {
      .group_id_ = observed.group_id_,
      .owner_node_id_ = observed.owner_node_id_,
      .owner_assignment_id_ = observed.owner_assignment_id_,
      .group_term_ = observed.group_term_,
      .control_revision_ = observed.control_revision_,
  };
}

bool SameOwnerAuthority(const MetaObservedOwnerProjection& possible,
                        const MetaOwnerAuthorityAnchor& committed) {
  return possible.group_id_ == committed.group_id_ &&
         possible.owner_node_id_ == committed.owner_node_id_ &&
         possible.owner_assignment_id_ == committed.owner_assignment_id_ &&
         possible.group_term_ == committed.group_term_;
}

bool ConservativelyFreshAt(std::optional<std::uint64_t> received_steady_ms,
                           std::uint64_t now_steady_ms, std::uint32_t ttl_ms) {
  if (!received_steady_ms.has_value()) return false;
  // Production's steady clock cannot move backwards. Treat an injected or
  // wrapping clock conservatively so time anomalies cannot manufacture Owner
  // failure evidence.
  if (now_steady_ms <= *received_steady_ms) return true;
  const std::uint64_t age = now_steady_ms - *received_steady_ms;
  return age <= ttl_ms;
}

MetaCausalProgressFreshness CausalProgressFreshnessAt(
    std::optional<std::uint64_t> received_steady_ms,
    std::optional<std::uint32_t> effective_duration_ms,
    std::uint64_t now_steady_ms) {
  if (!received_steady_ms.has_value() || !effective_duration_ms.has_value() ||
      *effective_duration_ms == 0) {
    return MetaCausalProgressFreshness::kUnknown;
  }
  return ConservativelyFreshAt(received_steady_ms, now_steady_ms,
                               *effective_duration_ms)
             ? MetaCausalProgressFreshness::kFresh
             : MetaCausalProgressFreshness::kExpired;
}

MetaCausalProgressFreshness PossibleCausalProgressFreshness(
    MetaCausalProgressFreshness observed,
    std::optional<MetaCausalProgressFreshness> lease_window) {
  if (!lease_window.has_value()) return observed;
  if (observed == MetaCausalProgressFreshness::kFresh ||
      *lease_window == MetaCausalProgressFreshness::kFresh) {
    return MetaCausalProgressFreshness::kFresh;
  }
  if (observed == MetaCausalProgressFreshness::kUnknown ||
      *lease_window == MetaCausalProgressFreshness::kUnknown) {
    return MetaCausalProgressFreshness::kUnknown;
  }
  return MetaCausalProgressFreshness::kExpired;
}

std::optional<MetaCausalProgressFreshness> OwnerLeaseWindowFreshness(
    const std::optional<MetaObservedOwnerState::LeaseWindow>& lease,
    const MetaOwnerAuthorityAnchor& committed, std::uint64_t now_steady_ms) {
  if (!lease.has_value() ||
      !SameOwnerAuthority(lease->projection_, committed)) {
    return std::nullopt;
  }
  return CausalProgressFreshnessAt(
      lease->heartbeat_received_steady_ms_,
      lease->projection_.authority_lease_duration_ms_, now_steady_ms);
}

bool HandoffComplete(const MetaDataControlRuntimeNode* runtime_node,
                     bool projection_current,
                     const std::optional<MetaObservedOwnerState>& observed,
                     const MetaOwnerAuthorityAnchor& committed,
                     std::uint64_t leadership_generation) {
  const std::uint64_t* pending_sequence = nullptr;
  if (observed.has_value() && observed->connected_ &&
      observed->authority_handoff_pending_sequence_.has_value() &&
      observed->owner_projection_.has_value() &&
      SameOwnerAuthority(*observed->owner_projection_, committed) &&
      runtime_node != nullptr &&
      runtime_node->session_generation_ ==
          observed->identity_.session_generation_ &&
      runtime_node->leadership_generation_ == leadership_generation &&
      runtime_node->boot_id_ == Hex(observed->identity_.boot_incarnation_)) {
    pending_sequence = &*observed->authority_handoff_pending_sequence_;
  }
  if (runtime_node != nullptr && projection_current &&
      runtime_node->last_lease_decision_.has_value()) {
    const auto* denied = std::get_if<cluster::control::LeaseDenied>(
        &*runtime_node->last_lease_decision_);
    if (pending_sequence == nullptr) {
      return denied == nullptr ||
             denied->reason !=
                 cluster::control::LeaseDenialReason::kAuthorityHandoffPending;
    }
    if (runtime_node->lease_decision_heartbeat_sequence_ >= *pending_sequence) {
      // Grant proves that the suspend-aware handoff deadline elapsed. The
      // only non-Grant proof is NodeNotReady: the server's handoff guard runs
      // before publishing that health denial and keeps returning Pending
      // until the same deadline. Other denial kinds bypass the guard and
      // cannot supersede the marker.
      if (std::holds_alternative<cluster::control::LeaseGranted>(
              *runtime_node->last_lease_decision_) ||
          (denied != nullptr &&
           denied->reason ==
               cluster::control::LeaseDenialReason::kNodeNotReady)) {
        return true;
      }
    }
  }
  if (pending_sequence != nullptr) {
    // FDS publication clears runtime's projection-local latest decision. The
    // pre-send marker closes that replacement window until this session
    // writes a later decision or the handoff guard produces a Grant.
    return false;
  }
  // An absent Owner must be diagnosable after warmup; handoff quarantine is
  // relevant only once an exact current session has actually received its
  // explicit pending denial.
  return true;
}

absl::StatusOr<MetaAutomaticFailoverStateMachine::Input> BuildInput(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaAutomaticUncontrolledFailoverPolicy& automatic,
    const MetaAuthorityLeasePolicy& lease,
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaObservationStore& observations, std::uint64_t now_steady_ms,
    std::uint32_t observation_ttl_ms, bool warmup_complete) {
  const auto grant = view.topology().AuthorityFor(group.group_id_);
  if (!grant.has_value()) {
    return absl::FailedPreconditionError(
        "automatic failover group has no grant state");
  }
  const auto assignment = AssignmentFor(group, group.record_.owner_);
  if (group.record_.owner_.empty() || !assignment.has_value()) {
    return absl::FailedPreconditionError(
        "automatic failover Owner assignment is absent");
  }

  const MetaDataControlRuntimeNode* runtime_node =
      RuntimeNodeFor(runtime, group.record_.owner_);
  const bool projection_current =
      runtime_node != nullptr &&
      RuntimeProjectionIsCurrent(view, group, *assignment, *runtime_node);
  const MetaOwnerAuthorityAnchor committed =
      OwnerAnchor(group, *assignment, runtime_node, projection_current);
  const auto observed = observations.OwnerObservationFor(group.record_.owner_);

  MetaOwnerServiceabilityCut cut{
      .leader_authority_eligible_ = runtime.leader_authority_eligible_,
      .leadership_warmup_complete_ = warmup_complete,
      .authority_handoff_complete_ =
          ActiveGrantMatches(group, *grant) &&
          HandoffComplete(runtime_node, projection_current, observed, committed,
                          runtime.leadership_generation_),
      .failover_transition_active_ = group.failover_transition_.has_value(),
      .committed_anchor_ = committed,
      .session_ = std::nullopt,
  };

  if (observed.has_value() && observed->connected_) {
    MetaOwnerServiceabilityCut::Session session;
    const bool runtime_identity_current =
        runtime_node != nullptr &&
        runtime_node->session_generation_ ==
            observed->identity_.session_generation_ &&
        runtime_node->leadership_generation_ ==
            runtime.leadership_generation_ &&
        runtime_node->boot_id_ == Hex(observed->identity_.boot_incarnation_);
    // Keep the cross-source join result, not a second copy of each identity
    // field. A torn or superseded runtime/session join remains explicit and
    // cannot be interpreted as Owner failure.
    session.current_ = runtime_identity_current;
    if (observed->health_.has_value() &&
        observed->heartbeat_received_steady_ms_.has_value()) {
      MetaOwnerServiceabilityCut::Session::Heartbeat heartbeat{
          .installed_anchor_ = {},
          .sequence_ = observed->heartbeat_sequence_,
          .fresh_ =
              ConservativelyFreshAt(observed->heartbeat_received_steady_ms_,
                                    now_steady_ms, observation_ttl_ms),
          .draining_ = observed->health_->draining_,
          .storage_ready_ = observed->health_->storage_ready_,
          .population_ready_ = observed->health_->population_ready_,
      };
      if (observed->owner_projection_.has_value()) {
        heartbeat.installed_anchor_ =
            ObservedAnchor(*observed->owner_projection_);
      }
      session.heartbeat_ = std::move(heartbeat);
    }
    std::optional<std::uint32_t> observed_effective_lease_duration_ms;
    if (observed->owner_projection_.has_value() &&
        observed->owner_projection_->authority_lease_duration_ms_ != 0) {
      // Before the first causal confirmation, the trusted projection marker
      // still carries the effective duration used for its pending interval.
      observed_effective_lease_duration_ms =
          observed->owner_projection_->authority_lease_duration_ms_;
    }
    MetaCausalProgressFreshness causal_progress_freshness =
        CausalProgressFreshnessAt(observed->causal_progress_received_steady_ms_,
                                  observed_effective_lease_duration_ms,
                                  now_steady_ms);
    // Handoff denials can span 2D, so the projection's initial causal-progress
    // timestamp may already be older than D when the first Grant is attempted.
    // A Grant may also remain active across a same-authority FDS replacement.
    // The observation session therefore retains both the maximum unconfirmed
    // attempt and the exact causally installed Grant. Their finite windows are
    // independent of runtime's current projection and duration.
    causal_progress_freshness = PossibleCausalProgressFreshness(
        causal_progress_freshness,
        OwnerLeaseWindowFreshness(observed->possible_owner_lease_, committed,
                                  now_steady_ms));
    session.causal_progress_freshness_ = PossibleCausalProgressFreshness(
        causal_progress_freshness,
        OwnerLeaseWindowFreshness(observed->installed_owner_lease_, committed,
                                  now_steady_ms));
    session.confirmed_grant_sequence_ = observed->confirmed_grant_sequence_;
    cut.session_ = std::move(session);
  }

  return MetaAutomaticFailoverStateMachine::Input{
      .anchor_ =
          MetaAutomaticFailoverAnchor{
              .group_id_ = group.group_id_,
              .leadership_generation_ = runtime.leadership_generation_,
              .leader_authority_eligibility_revision_ =
                  runtime.leader_authority_eligibility_revision_,
              .owner_node_id_ = group.record_.owner_,
              .owner_assignment_id_ = *assignment,
              .group_term_ = group.record_.group_term_,
              .automatic_failover_policy_version_ = automatic.version_,
              .authority_lease_policy_version_ = lease.version_,
          },
      .leader_authority_eligible_ = runtime.leader_authority_eligible_,
      .suspect_after_ms_ = automatic.suspect_after_ms_,
      .owner_serviceability_ = EvaluateOwnerServiceability(cut),
  };
}

MetaAutomaticFailoverReason CommandReason(
    MetaOwnerServiceabilityReason reason) {
  switch (reason) {
    case MetaOwnerServiceabilityReason::kSessionMissing:
      return MetaAutomaticFailoverReason::kSessionMissing;
    case MetaOwnerServiceabilityReason::kHeartbeatExpired:
      return MetaAutomaticFailoverReason::kHeartbeatExpired;
    case MetaOwnerServiceabilityReason::kDraining:
      return MetaAutomaticFailoverReason::kDraining;
    case MetaOwnerServiceabilityReason::kStorageUnready:
      return MetaAutomaticFailoverReason::kStorageUnready;
    case MetaOwnerServiceabilityReason::kPopulationUnready:
      return MetaAutomaticFailoverReason::kPopulationUnready;
    case MetaOwnerServiceabilityReason::kNone:
    case MetaOwnerServiceabilityReason::kStaleOwnerAnchor:
    case MetaOwnerServiceabilityReason::kCausalLeasePending:
      return MetaAutomaticFailoverReason::kManual;
  }
  return MetaAutomaticFailoverReason::kManual;
}

absl::StatusOr<MetaRequestId> NextId(
    const MetaAutomaticFailoverReconcilerOptions& options) {
  auto id = options.next_id_();
  if (!id.ok()) return id.status();
  if (IsZero(*id)) {
    return absl::FailedPreconditionError(
        "automatic failover generated a zero identity");
  }
  return *id;
}

void SetGroupAnchors(BeginUncontrolledFailover& command,
                     const MetaTopologyGroupView& group,
                     const MetaAssignmentId& owner_assignment) {
  command.expected_owner_node_id_ = group.record_.owner_;
  command.expected_owner_assignment_id_ = owner_assignment;
  command.expected_membership_revision_ = group.revision_;
  command.expected_group_term_ = group.record_.group_term_;
  command.expected_population_manifest_revision_ =
      group.record_.population_manifest_revision_;
  command.expected_population_manifest_digest_ =
      group.record_.population_manifest_digest_;
  command.expected_partition_replication_epoch_ =
      group.record_.partition_replication_epoch_;
}

std::optional<MetaOperationRecord> PreemptableControlledRequest(
    const MetaCommittedView& view, std::string_view group_id) {
  std::vector<MetaOperationRecord> operations =
      view.operation().LiveOperations();
  std::ranges::sort(operations, {}, &MetaOperationRecord::operation_seq_);
  for (const MetaOperationRecord& operation : operations) {
    if (operation.kind_ != kFailoverOperationKind ||
        operation.lifecycle_ != MetaOperationLifecycle::kSubmitted ||
        operation.revision_ != 0 || !operation.kind_phase_blob_.empty() ||
        !operation.current_directives_.empty() ||
        !operation.terminal_receipts_.empty() ||
        !std::ranges::all_of(operation.replication_history_id_,
                             [](std::uint8_t byte) { return byte == 0; }) ||
        operation.intent_hash_ != MetaSha256(operation.intent_)) {
      continue;
    }
    const auto intent = DecodeFailoverOperationIntent(operation.intent_);
    if (intent.ok() && intent->group_id_ == group_id) return operation;
  }
  return std::nullopt;
}

std::optional<std::uint64_t> AddDelay(std::uint64_t now, std::uint64_t delay) {
  if (delay > std::numeric_limits<std::uint64_t>::max() - now) {
    return std::nullopt;
  }
  return now + delay;
}

bool WarmupComplete(std::optional<std::uint64_t> started_ms,
                    std::uint64_t now_ms, std::uint64_t grace_ms) {
  if (!started_ms.has_value() || now_ms < *started_ms) return false;
  const std::optional<std::uint64_t> deadline = AddDelay(*started_ms, grace_ms);
  return deadline.has_value() && now_ms >= *deadline;
}

}  // namespace

struct MetaAutomaticFailoverReconciler::Core {
  struct Admission {
    MetaAutomaticFailoverAnchor anchor_;
    MetaAutomaticFailoverReason trigger_reason_ =
        MetaAutomaticFailoverReason::kManual;
    std::uint64_t suspect_duration_ms_ = 0;
    bool uncertain_append_ = false;
  };

  struct Pending {
    BeginUncontrolledFailover command_;
    MetaAutomaticFailoverAnchor anchor_;
    std::uint64_t next_attempt_steady_ms_ = 0;
    std::size_t backoff_index_ = 0;
    bool uncertain_append_ = false;
  };

  bycorf::ForeignExecutor executor_;
  MetaAutomaticFailoverReconcilerOptions options_;

  // Worker-owned lifecycle/detector state.
  bool running_ = false;
  bool cancelled_ = true;
  bool shutdown_ = false;
  std::uint64_t leadership_generation_ = 0;
  bool last_eligible_ = false;
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  std::optional<std::uint64_t> eligible_since_steady_ms_;
  MetaAutomaticFailoverStateMachine detector_;
  std::map<std::string, Pending, std::less<>> pending_;
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;

#if LAVIK_FAULTS_ENABLED
  // Deterministic process-test cuts. They are reset for each leadership run
  // and compiled out of ordinary Release binaries.
  bool test_pause_before_propose_applied_ = false;
#endif

  // Validation hooks may be entered independently of the reconciler loop in
  // component tests. Keep their small, bounded admission set synchronized.
  std::mutex admission_mu_;
  std::map<MetaFailoverTransitionId, Admission> admissions_;

  std::atomic<bool> stopped_{false};
};

namespace {

#if LAVIK_FAULTS_ENABLED
bycorf::Task<absl::Status> TestPause(
    const std::shared_ptr<MetaAutomaticFailoverReconciler::Core>& core,
    std::chrono::milliseconds delay) {
  constexpr auto kSlice = std::chrono::milliseconds(25);
  while (!core->cancelled_ && delay > std::chrono::milliseconds::zero()) {
    const auto slice = std::min(delay, kSlice);
    const auto slept =
        co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, slice);
    if (!slept.ok()) co_return slept;
    delay -= slice;
  }
  co_return absl::OkStatus();
}
#endif

void ClearAdmissions(
    const std::shared_ptr<MetaAutomaticFailoverReconciler::Core>& core) {
  std::lock_guard<std::mutex> lock(core->admission_mu_);
  core->admissions_.clear();
}

void ArmAdmission(
    const std::shared_ptr<MetaAutomaticFailoverReconciler::Core>& core,
    const MetaAutomaticFailoverReconciler::Core::Pending& pending) {
  std::lock_guard<std::mutex> lock(core->admission_mu_);
  core->admissions_[pending.command_.transition_id_] = {
      .anchor_ = pending.anchor_,
      .trigger_reason_ = pending.command_.trigger_reason_,
      .suspect_duration_ms_ = pending.command_.suspect_duration_ms_,
      .uncertain_append_ = pending.uncertain_append_,
  };
}

void RemoveAdmission(
    const std::shared_ptr<MetaAutomaticFailoverReconciler::Core>& core,
    const MetaFailoverTransitionId& transition_id) {
  std::lock_guard<std::mutex> lock(core->admission_mu_);
  core->admissions_.erase(transition_id);
}

absl::Status ValidateAutomaticProposal(
    const std::shared_ptr<MetaAutomaticFailoverReconciler::Core>& core,
    const MetaCommand& proposed, const MetaCommittedView& view,
    const MetaObservationStore& observations, std::int64_t now_unix_ms) {
  const auto* command = std::get_if<BeginUncontrolledFailover>(&proposed);
  if (command == nullptr ||
      command->trigger_reason_ == MetaAutomaticFailoverReason::kManual) {
    return absl::OkStatus();
  }

  MetaAutomaticFailoverReconciler::Core::Admission admission;
  {
    std::lock_guard<std::mutex> lock(core->admission_mu_);
    const auto found = core->admissions_.find(command->transition_id_);
    if (found == core->admissions_.end()) {
      return absl::FailedPreconditionError(
          "automatic failover Begin has no live detector admission");
    }
    admission = found->second;
  }
  if (admission.suspect_duration_ms_ != command->suspect_duration_ms_ ||
      admission.trigger_reason_ != command->trigger_reason_) {
    return absl::FailedPreconditionError(
        "automatic failover Begin disagrees with detector admission");
  }
  // Once an append outcome becomes uncertain, health or Policy changes must
  // not make a retry use a new identity or abandon reconciliation. Durable
  // Group/authority CAS still rejects an obsolete command at apply.
  if (admission.uncertain_append_) return absl::OkStatus();

  if (now_unix_ms < 0) {
    return absl::FailedPreconditionError(
        "automatic failover proposal time is invalid");
  }
  const MetaDataControlRuntimeSnapshot runtime =
      core->options_.data_control_runtime_status_->Snapshot();
  if (!runtime.leader_authority_eligible_ ||
      runtime.leadership_generation_ !=
          admission.anchor_.leadership_generation_) {
    return absl::FailedPreconditionError(
        "automatic failover leader authority is no longer eligible");
  }
  const auto group = view.topology().FindGroup(command->group_id_);
  if (!group.has_value()) {
    return absl::FailedPreconditionError(
        "automatic failover group disappeared before append");
  }
  const auto automatic = view.policy().CurrentAutomaticUncontrolledFailover();
  const auto lease = view.policy().CurrentAuthorityLease();
  if (!automatic.has_value() || !lease.has_value()) {
    return absl::FailedPreconditionError(
        "automatic failover requires both current global Policies");
  }
  auto input = BuildInput(view, *group, *automatic, *lease, runtime,
                          observations, core->options_.now_steady_ms_(),
                          core->options_.observation_ttl_ms_,
                          /*warmup_complete=*/true);
  if (!input.ok()) return input.status();
  // The command records the exact reason observed at the threshold edge, but
  // moving between exact failure reasons does not interrupt unserviceability.
  // Requiring equality here would discard a completed debounce interval just
  // because, for example, a disconnected Owner reconnects storage-unready.
  if (input->anchor_ != admission.anchor_ ||
      !input->leader_authority_eligible_ ||
      input->owner_serviceability_.state_ !=
          MetaOwnerServiceabilityState::kUnserviceable ||
      CommandReason(input->owner_serviceability_.reason_) ==
          MetaAutomaticFailoverReason::kManual ||
      command->suspect_duration_ms_ < input->suspect_after_ms_) {
    return absl::FailedPreconditionError(
        "automatic failover recovered or changed before append");
  }
  return absl::OkStatus();
}

bool CommandTransitionCommitted(
    const MetaCommittedView& view,
    const MetaAutomaticFailoverReconciler::Core::Pending& pending,
    std::uint64_t* committed_index) {
  const auto group = view.topology().FindGroup(pending.command_.group_id_);
  if (!group.has_value() || !group->failover_transition_.has_value()) {
    return false;
  }
  if (group->failover_transition_->transition_id_ !=
      pending.command_.transition_id_) {
    return false;
  }
  *committed_index = group->failover_transition_->revision_;
  return true;
}

bool DefiniteNonAppend(const absl::Status& status) {
  return status.code() == absl::StatusCode::kInvalidArgument ||
         status.code() == absl::StatusCode::kFailedPrecondition ||
         status.code() == absl::StatusCode::kPermissionDenied;
}

void LogStatusEdge(const std::optional<MetaAutomaticFailoverStatus>& before,
                   const MetaAutomaticFailoverStatus& after) {
  // SUSPECT elapsed time changes every poll. Logging that scalar would turn
  // one quiet detector into a per-Group 40 Hz log stream; only semantic edges
  // and anchor/threshold resets belong in the transition log.
  if (before.has_value() && before->anchor_ == after.anchor_ &&
      before->state_ == after.state_ &&
      before->current_reason_ == after.current_reason_ &&
      before->blocker_ == after.blocker_ &&
      before->effective_threshold_ms_ == after.effective_threshold_ms_) {
    return;
  }
  spdlog::info(
      "automatic failover detector group={} owner={} term={} "
      "state={} reason={} blocker={} suspect_ms={} threshold_ms={}",
      after.anchor_.group_id_, after.anchor_.owner_node_id_,
      after.anchor_.group_term_, MetaAutomaticFailoverStateName(after.state_),
      MetaOwnerServiceabilityReasonName(after.current_reason_),
      MetaAutomaticFailoverBlockerName(after.blocker_),
      after.accumulated_suspect_ms_, after.effective_threshold_ms_);
}

std::optional<MetaAutomaticFailoverStatus> FindStatus(
    const std::vector<MetaAutomaticFailoverStatus>& statuses,
    std::string_view group_id) {
  const auto found = std::lower_bound(
      statuses.begin(), statuses.end(), group_id,
      [](const MetaAutomaticFailoverStatus& status, std::string_view id) {
        return status.anchor_.group_id_ < id;
      });
  return found == statuses.end() || found->anchor_.group_id_ != group_id
             ? std::nullopt
             : std::optional(*found);
}

}  // namespace

MetaAutomaticFailoverReconciler::MetaAutomaticFailoverReconciler(
    bycorf::ForeignExecutor executor,
    MetaAutomaticFailoverReconcilerOptions options)
    : core_(std::make_shared<Core>()) {
  if (options.data_control_runtime_status_ == nullptr ||
      options.diagnostics_ == nullptr || options.observation_ttl_ms_ == 0 ||
      options.poll_interval_.count() <= 0) {
    throw std::invalid_argument(
        "invalid automatic failover reconciler options");
  }
  if (!options.now_steady_ms_) {
    options.now_steady_ms_ = [] {
      return static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count());
    };
  }
  if (!options.next_id_) {
    options.next_id_ = [] { return cluster::control::GenerateId128(); };
  }
  core_->executor_ = std::move(executor);
  core_->options_ = std::move(options);
}

MetaAutomaticFailoverReconciler::~MetaAutomaticFailoverReconciler() {
  Shutdown();
}

MetaValidateHook MetaAutomaticFailoverReconciler::validation_hook() const {
  const std::shared_ptr<Core> core = core_;
  return [core](const MetaCommand& command, const MetaCommittedView& view,
                const MetaObservationStore& observations,
                std::int64_t proposal_now_unix_ms) {
    return ValidateAutomaticProposal(core, command, view, observations,
                                     proposal_now_unix_ms);
  };
}

void MetaAutomaticFailoverReconciler::Start(MetaLeaderContext& context) {
  const std::shared_ptr<Core> core = core_;
  if (!core->executor_.Notify([core, context = &context]() noexcept {
        if (core->shutdown_) return;
        if (core->running_) std::terminate();
        core->cancelled_ = false;
        core->running_ = true;
        core->leadership_generation_ = 0;
        core->last_eligible_ = false;
        core->leader_authority_eligibility_revision_ = 0;
        core->eligible_since_steady_ms_.reset();
        core->detector_.Clear();
        core->pending_.clear();
#if LAVIK_FAULTS_ENABLED
        core->test_pause_before_propose_applied_ = false;
#endif
        ClearAdmissions(core);
        bycorf::ThisWorker().self_->Spawn(Run(core, context));
      })) {
    std::terminate();
  }
}

void MetaAutomaticFailoverReconciler::Stop(bool permanent) {
  const std::shared_ptr<Core> core = core_;
  if (core->stopped_.load(std::memory_order_acquire)) return;
  auto waiter = std::make_shared<std::promise<void>>();
  std::future<void> done = waiter->get_future();
  if (!core->executor_.Notify([core, waiter, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        if (core->running_) {
          core->waiters_.push_back(waiter);
        } else {
          ClearAdmissions(core);
          if (core->leadership_generation_ != 0) {
            core->options_.diagnostics_->EndLeadership(
                core->leadership_generation_);
          }
          waiter->set_value();
        }
      })) {
    if (core->stopped_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent) core->stopped_.store(true, std::memory_order_release);
}

void MetaAutomaticFailoverReconciler::CancelAndWait() { Stop(false); }

void MetaAutomaticFailoverReconciler::Shutdown() { Stop(true); }

bycorf::Task<absl::Status> MetaAutomaticFailoverReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context) {
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  std::string last_error;

  while (!core->cancelled_) {
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel)) {
      subscribed.view_ = context->CommittedView();
    }
    const std::uint64_t now_steady = core->options_.now_steady_ms_();
    const MetaDataControlRuntimeSnapshot runtime =
        core->options_.data_control_runtime_status_->Snapshot();

    if (runtime.leadership_generation_ != core->leadership_generation_) {
      if (core->leadership_generation_ != 0) {
        core->options_.diagnostics_->EndLeadership(
            core->leadership_generation_);
      }
      core->leadership_generation_ = runtime.leadership_generation_;
      core->last_eligible_ = false;
      core->leader_authority_eligibility_revision_ =
          runtime.leader_authority_eligibility_revision_;
      core->eligible_since_steady_ms_.reset();
      core->detector_.Clear();
      core->pending_.clear();
      ClearAdmissions(core);
      if (core->leadership_generation_ != 0) {
        core->options_.diagnostics_->BeginLeadership(
            core->leadership_generation_);
      }
    }

    if (runtime.leader_authority_eligible_ != core->last_eligible_ ||
        runtime.leader_authority_eligibility_revision_ !=
            core->leader_authority_eligibility_revision_) {
      core->last_eligible_ = runtime.leader_authority_eligible_;
      core->leader_authority_eligibility_revision_ =
          runtime.leader_authority_eligibility_revision_;
      core->detector_.Clear();
      if (runtime.leader_authority_eligible_) {
        core->eligible_since_steady_ms_ = now_steady;
      } else {
        core->eligible_since_steady_ms_.reset();
      }
      // A completed false -> true ABA is still an authority discontinuity.
      // Definite non-appends must reacquire warmup/debounce; uncertain appends
      // retain their stable identity and reconcile against durable state.
      for (auto it = core->pending_.begin(); it != core->pending_.end();) {
        if (!it->second.uncertain_append_) {
          RemoveAdmission(core, it->second.command_.transition_id_);
          it = core->pending_.erase(it);
        } else {
          ++it;
        }
      }
      spdlog::info(
          "automatic failover leadership generation={} eligible={} "
          "eligibility_revision={}",
          runtime.leadership_generation_, runtime.leader_authority_eligible_,
          runtime.leader_authority_eligibility_revision_);
    }
    const bool warmup_complete =
        runtime.leader_authority_eligible_ &&
        WarmupComplete(core->eligible_since_steady_ms_, now_steady,
                       core->options_.observation_grace_ms_);

    // Reconcile stable identities against durable state before deriving any
    // new edge. A matching transition is definitive even when the proposal
    // reply was lost.
    for (auto it = core->pending_.begin(); it != core->pending_.end();) {
      std::uint64_t committed_index = 0;
      if (CommandTransitionCommitted(subscribed.view_, it->second,
                                     &committed_index)) {
        spdlog::info(
            "automatic failover Begin committed group={} transition={} "
            "commit_index={}",
            it->first, Hex(it->second.command_.transition_id_),
            committed_index);
        RemoveAdmission(core, it->second.command_.transition_id_);
        core->detector_.EraseGroup(it->first);
        it = core->pending_.erase(it);
        continue;
      }
      const auto group = subscribed.view_.topology().FindGroup(it->first);
      if (group.has_value() && group->failover_transition_.has_value()) {
        spdlog::info(
            "automatic failover proposal superseded group={} transition={}",
            it->first, Hex(it->second.command_.transition_id_));
        RemoveAdmission(core, it->second.command_.transition_id_);
        core->detector_.EraseGroup(it->first);
        it = core->pending_.erase(it);
        continue;
      }
      ++it;
    }

    const auto before_status = core->detector_.Snapshot();
    std::set<std::string, std::less<>> live_groups;
    // Policy accessors validate/parse the retained raw documents. Resolve one
    // typed pair for this committed cut instead of reparsing both documents
    // for every Group in the 25ms detector loop.
    const auto automatic =
        subscribed.view_.policy().CurrentAutomaticUncontrolledFailover();
    const auto lease = subscribed.view_.policy().CurrentAuthorityLease();
    if (subscribed.view_.topology().ClusterLifecycle().state_ ==
            MetaClusterLifecycle::kCreated &&
        runtime.leadership_generation_ != 0) {
      for (const MetaTopologyGroupView& group :
           subscribed.view_.topology().Groups()) {
        live_groups.insert(group.group_id_);
        if (core->pending_.contains(group.group_id_)) continue;
        if (!automatic.has_value() || !lease.has_value()) {
          if (last_error !=
              "automatic failover requires both current global Policies") {
            spdlog::warn(
                "automatic failover evaluation blocked: automatic failover "
                "requires both current global Policies");
            last_error =
                "automatic failover requires both current global Policies";
          }
          continue;
        }
        auto input =
            BuildInput(subscribed.view_, group, *automatic, *lease, runtime,
                       context->Observations(), now_steady,
                       core->options_.observation_ttl_ms_, warmup_complete);
        if (!input.ok()) {
          if (last_error != input.status().message()) {
            spdlog::warn("automatic failover evaluation blocked: {}",
                         input.status().message());
            last_error = std::string(input.status().message());
          }
          continue;
        }
        auto update = core->detector_.Advance(*input, now_steady);
        if (!update.ok()) {
          if (last_error != update.status().message()) {
            spdlog::warn("automatic failover detector rejected input: {}",
                         update.status().message());
            last_error = std::string(update.status().message());
          }
          continue;
        }
        last_error.clear();
        LogStatusEdge(FindStatus(before_status, group.group_id_),
                      update->status_);
        if (!update->trigger_now_) continue;

        const auto grant =
            subscribed.view_.topology().AuthorityFor(group.group_id_);
        const auto assignment = AssignmentFor(group, group.record_.owner_);
        if (!grant.has_value() || !assignment.has_value() ||
            group.record_.group_term_ ==
                std::numeric_limits<std::uint64_t>::max()) {
          core->detector_.EraseGroup(group.group_id_);
          continue;
        }
        auto request_id = NextId(core->options_);
        auto transition_id = NextId(core->options_);
        if (!request_id.ok() || !transition_id.ok()) {
          const absl::Status& status =
              !request_id.ok() ? request_id.status() : transition_id.status();
          spdlog::warn("automatic failover identity generation failed: {}",
                       status.message());
          core->detector_.EraseGroup(group.group_id_);
          continue;
        }
        BeginUncontrolledFailover command;
        command.request_id_ = *request_id;
        command.group_id_ = group.group_id_;
        command.transition_id_ = *transition_id;
        command.target_term_ = group.record_.group_term_ + 1;
        command.candidate_action_ = std::nullopt;
        command.trigger_reason_ =
            CommandReason(update->status_.current_reason_);
        command.suspect_duration_ms_ = update->status_.accumulated_suspect_ms_;
        if (const auto preempted =
                PreemptableControlledRequest(subscribed.view_, group.group_id_);
            preempted.has_value()) {
          command.preempted_operation_id_ = preempted->operation_id_;
          command.expected_preempted_operation_revision_ = preempted->revision_;
        }
        SetGroupAnchors(command, group, *assignment);

        Core::Pending pending{
            .command_ = std::move(command),
            .anchor_ = update->status_.anchor_,
            .next_attempt_steady_ms_ = now_steady,
        };
        auto [inserted, fresh] =
            core->pending_.emplace(group.group_id_, std::move(pending));
        if (!fresh) std::terminate();
        ArmAdmission(core, inserted->second);
        spdlog::warn(
            "automatic failover threshold reached group={} owner={} reason={} "
            "suspect_ms={} transition={}",
            group.group_id_, group.record_.owner_,
            MetaAutomaticFailoverReasonName(
                inserted->second.command_.trigger_reason_),
            inserted->second.command_.suspect_duration_ms_,
            Hex(inserted->second.command_.transition_id_));
      }
    }

    // Remove diagnostics for deleted Groups without touching an in-flight
    // uncertain proposal that still needs deterministic reconciliation.
    for (const MetaAutomaticFailoverStatus& status :
         core->detector_.Snapshot()) {
      if (!live_groups.contains(status.anchor_.group_id_) &&
          !core->pending_.contains(status.anchor_.group_id_)) {
        core->detector_.EraseGroup(status.anchor_.group_id_);
      }
    }
    if (core->leadership_generation_ != 0) {
      core->options_.diagnostics_->Publish(
          core->leadership_generation_,
          runtime.leader_authority_eligibility_revision_,
          subscribed.view_.applied_index(), core->detector_.Snapshot());
    }

    auto ready = std::ranges::find_if(core->pending_, [&](const auto& entry) {
      return runtime.leader_authority_eligible_ &&
             now_steady >= entry.second.next_attempt_steady_ms_;
    });
    if (ready != core->pending_.end() && !core->cancelled_) {
      Core::Pending& pending = ready->second;
      ArmAdmission(core, pending);
#if LAVIK_FAULTS_ENABLED
      if (!core->test_pause_before_propose_applied_) {
        const auto delay =
            TestPauseDelay("LAVIK_TEST_PAUSE_AUTOMATIC_BEFORE_PROPOSE_MS");
        if (delay != std::chrono::milliseconds::zero()) {
          core->test_pause_before_propose_applied_ = true;
          spdlog::info(
              "automatic failover paused before Begin proposal group={} "
              "transition={} delay_ms={}",
              ready->first, Hex(pending.command_.transition_id_),
              delay.count());
          const absl::Status slept = co_await TestPause(core, delay);
          if (!slept.ok() || core->cancelled_) break;
        }
      }
#endif
      spdlog::info(
          "automatic failover proposing Begin group={} transition={} "
          "attempt={}",
          ready->first, Hex(pending.command_.transition_id_),
          pending.backoff_index_ + 1);
      const auto applied =
          co_await context->Propose(MetaCommand{pending.command_});
      const std::uint64_t propose_returned_steady =
          core->options_.now_steady_ms_();
      if (core->cancelled_) break;
      changed->store(true, std::memory_order_release);

      if (applied.ok()) {
        if (applied->verdict_ == MetaAuditVerdict::kAccepted) {
          spdlog::info(
              "automatic failover Begin accepted group={} transition={} "
              "commit_index={}",
              ready->first, Hex(pending.command_.transition_id_),
              applied->log_index_);
          // Propose returns only after this state machine has applied the
          // command. Refresh immediately so the next iteration reconciles the
          // accepted transition even if its subscription callback has not yet
          // reached this worker.
          subscribed.view_ = context->CommittedView();
          continue;
        }
        spdlog::warn(
            "automatic failover Begin domain-rejected group={} transition={} "
            "detail={}",
            ready->first, Hex(pending.command_.transition_id_),
            applied->detail_);
        RemoveAdmission(core, pending.command_.transition_id_);
        core->detector_.EraseGroup(ready->first);
        core->pending_.erase(ready);
        continue;
      }

      const bool definite = DefiniteNonAppend(applied.status());
      if (definite) {
        spdlog::warn(
            "automatic failover Begin suppressed before append group={} "
            "transition={} detail={}",
            ready->first, Hex(pending.command_.transition_id_),
            applied.status().message());
        RemoveAdmission(core, pending.command_.transition_id_);
        core->detector_.EraseGroup(ready->first);
        core->pending_.erase(ready);
        continue;
      }

      if (applied.status().code() == absl::StatusCode::kDeadlineExceeded ||
          applied.status().code() == absl::StatusCode::kCancelled ||
          applied.status().code() == absl::StatusCode::kInternal) {
        pending.uncertain_append_ = true;
      }
      const std::uint64_t delay = kRetryBackoffMs[std::min(
          pending.backoff_index_, kRetryBackoffMs.size() - 1)];
      ++pending.backoff_index_;
      pending.next_attempt_steady_ms_ =
          AddDelay(propose_returned_steady, delay)
              .value_or(std::numeric_limits<std::uint64_t>::max());
      ArmAdmission(core, pending);
      spdlog::warn(
          "automatic failover Begin retry scheduled group={} transition={} "
          "delay_ms={} uncertain={} detail={}",
          ready->first, Hex(pending.command_.transition_id_), delay,
          pending.uncertain_append_, applied.status().message());
    }

    const auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                 core->options_.poll_interval_);
    if (!slept.ok()) break;
  }

  ClearAdmissions(core);
  if (core->leadership_generation_ != 0) {
    core->options_.diagnostics_->EndLeadership(core->leadership_generation_);
  }
  core->leadership_generation_ = 0;
  core->detector_.Clear();
  core->pending_.clear();
  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}

}  // namespace lavik::meta
