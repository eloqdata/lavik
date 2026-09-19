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

#include "lavik/meta/coordinator.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <ranges>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

#include "lavik/meta/raft.h"
#include "lavik/meta/state_machine.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {

// Shared because a Raft completion may arrive after the proposing coroutine
// timed out or was dropped. Reservations therefore never call back through a
// potentially destroyed coordinator.
class MetaProposalGate {
 public:
  std::mutex mu_;
  std::uint64_t regular_reservations_ = 0;
  bool prune_reserved_ = false;
  bool fail_safe_recovery_reserved_ = false;
};

// ---------------------------------------------------------------------------
// Subscription machinery (the opaque MetaSubscriptionCore of the header).
// ---------------------------------------------------------------------------

namespace {

class AuditReservation {
 public:
  AuditReservation(std::shared_ptr<MetaProposalGate> gate, bool prune)
      : gate_(std::move(gate)), prune_(prune) {}
  ~AuditReservation() {
    if (gate_ == nullptr) return;
    std::lock_guard<std::mutex> lock(gate_->mu_);
    if (prune_) {
      gate_->prune_reserved_ = false;
    } else if (gate_->regular_reservations_ > 0) {
      --gate_->regular_reservations_;
    }
  }

 private:
  std::shared_ptr<MetaProposalGate> gate_;
  bool prune_;
};

// Exactly one durability-recovery proposal may be unresolved at a time. The
// lease follows Raft's completion rather than the caller coroutine, so a
// timeout cannot admit a duplicate against the same committed view.
class FailSafeRecoveryReservation {
 public:
  explicit FailSafeRecoveryReservation(std::shared_ptr<MetaProposalGate> gate)
      : gate_(std::move(gate)) {}
  ~FailSafeRecoveryReservation() {
    if (gate_ == nullptr) return;
    std::lock_guard<std::mutex> lock(gate_->mu_);
    gate_->fail_safe_recovery_reserved_ = false;
  }

 private:
  std::shared_ptr<MetaProposalGate> gate_;
};

struct Subscriber {
  std::uint64_t id_ = 0;
  MetaCommitCallback callback_;
  std::size_t capacity_ = 0;
  std::deque<MetaCommitEvent> queue_;  // FIFO, bounded by capacity_
  bool cancelled_ = false;
  bool needs_resync_ = false;
  // Pinned while the dispatch thread runs the callback; the handle's
  // destructor waits for this before erasing (no callback-after-free).
  bool callback_in_flight_ = false;
};

}  // namespace

class MetaSubscriptionCore {
 public:
  std::mutex mu_;
  // Dispatch wakeup, and the condition for unsubscribe/in-flight waits.
  std::condition_variable cv_;
  std::map<std::uint64_t, std::shared_ptr<Subscriber>> subs_;
  std::uint64_t next_id_ = 1;
  // Highest log index whose committed effects are known to be reflected in
  // the state machine's stores: the sink's high-water mark, seeded from the
  // SM's durable commit watermark at coordinator construction. Snapshot
  // installs advance the stores past it without events — the documented
  // resync case (coordinator.h, MetaSubscriptionStart).
  std::uint64_t high_water_ = 0;
  // Set by the O(1) commit sink and consumed by the dispatch worker. The
  // worker snapshots committed stores only after apply releases the state
  // machine lock, then purges observations made stale by that commit.
  bool observations_dirty_ = false;
  bool dispatch_stop_ = false;
};

MetaCommitSubscription::MetaCommitSubscription(
    std::shared_ptr<MetaSubscriptionCore> core, std::uint64_t id)
    : core_(std::move(core)), id_(id) {}

MetaCommitSubscription::~MetaCommitSubscription() {
  const std::shared_ptr<MetaSubscriptionCore> core = core_;
  if (core == nullptr) return;
  std::unique_lock<std::mutex> lock(core->mu_);
  const auto it = core->subs_.find(id_);
  if (it == core->subs_.end()) return;  // already torn down by the coordinator
  it->second->cancelled_ = true;        // the dispatcher skips it from now on
  core->cv_.wait(lock, [&] { return !it->second->callback_in_flight_; });
  core->subs_.erase(it);
}

bool MetaCommitSubscription::cancelled() const {
  if (core_ == nullptr) return true;
  std::lock_guard<std::mutex> lock(core_->mu_);
  const auto it = core_->subs_.find(id_);
  return it == core_->subs_.end() || it->second->cancelled_;
}

bool MetaCommitSubscription::needs_resync() const {
  if (core_ == nullptr) return true;
  std::lock_guard<std::mutex> lock(core_->mu_);
  const auto it = core_->subs_.find(id_);
  return it == core_->subs_.end() || it->second->needs_resync_;
}

// ---------------------------------------------------------------------------
// MetaStoresFacts
// ---------------------------------------------------------------------------

bool MetaStoresFacts::IsActiveNode(std::string_view node_id) const {
  return stores_.identity_.IsActiveNode(std::string(node_id));
}

uint64_t MetaStoresFacts::CurrentGroupTerm(std::string_view group_id) const {
  // Conservative "unknown" per the MetaCommittedFacts contract: 0.
  return stores_.topology_.CurrentGroupTerm(group_id).value_or(0);
}

uint64_t MetaStoresFacts::CurrentPopulationManifestRevision(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.population_manifest_revision_ : 0;
}

MetaHash256 MetaStoresFacts::CurrentPopulationManifestDigest(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.population_manifest_digest_
                           : MetaHash256{};
}

uint64_t MetaStoresFacts::CurrentPartitionReplicationEpoch(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.partition_replication_epoch_ : 0;
}

bool MetaStoresFacts::AssignmentMatches(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

bool MetaStoresFacts::IsOwnerAssignment(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() && group->record_.owner_ == node_id &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

bool MetaStoresFacts::MayReportFencedOwnerCandidate(
    const MetaCandidateProgressObs& candidate) const {
  const auto group =
      stores_.topology_.FindGroup(std::string(candidate.group_id_));
  const auto grant = stores_.topology_.AuthorityFor(candidate.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value() ||
      group->failover_transition_->mode_ != MetaFailoverMode::kUncontrolled ||
      group->failover_transition_->target_term_ != group->record_.group_term_ ||
      candidate.group_term_ != group->record_.group_term_ ||
      candidate.source_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      candidate.source_group_term_ + 1 != candidate.group_term_ ||
      group->record_.owner_ != candidate.node_id_ ||
      grant->group_term_ != group->record_.group_term_ ||
      grant->grant_.has_value()) {
    return false;
  }
  return std::ranges::any_of(
      group->members_, [&](const MetaGroupMember& member) {
        return member.node_id_ == candidate.node_id_ &&
               member.assignment_id_ == candidate.assignment_id_;
      });
}

bool MetaStoresFacts::IsCurrentFailoverCandidate(
    std::string_view node_id, const MetaBootIncarnation& boot_id) const {
  return std::ranges::any_of(
      stores_.topology_.Groups(), [&](const MetaTopologyGroupView& group) {
        return group.failover_transition_.has_value() &&
               group.failover_transition_->candidate_action_.has_value() &&
               group.failover_transition_->candidate_action_->candidate_
                       .node_id_ == node_id &&
               group.failover_transition_->candidate_action_->candidate_
                       .boot_id_ == boot_id;
      });
}

std::optional<MetaCommittedFacts::FailoverTransitionView>
MetaStoresFacts::FailoverTransitionById(
    const MetaFailoverTransitionId& transition_id) const {
  for (const MetaTopologyGroupView& group : stores_.topology_.Groups()) {
    if (group.failover_transition_.has_value() &&
        group.failover_transition_->transition_id_ == transition_id) {
      return FailoverTransitionView{group.group_id_,
                                    *group.failover_transition_};
    }
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Propose internals
// ---------------------------------------------------------------------------

namespace {

using CmdResult = MetaRaftResult;

// Shared wait state for one Propose round trip. The Raft completion handler
// (any Raft thread, or inline on the caller for an already-completed
// result) fills the raw outcome and resumes the suspended coroutine through
// the foreign executor. If the Propose task was destroyed while suspended,
// the awaiter detached the handle and the completion just drops. Shared
// ownership is what makes the drop-safe path possible (same pattern as the
// ctl server's AsyncReply).
struct ProposeWaiter {
  std::mutex mu_;
  std::coroutine_handle<> awaiting_{};
  bool ready_ = false;
  bool detached_ = false;
  MetaRaftResultCode code_ = MetaRaftResultCode::CANCELLED;
  std::optional<MetaApplyResult> apply_result_;
  bool has_exception_ = false;
  bycorf::ForeignExecutor foreign_executor_;
  bool inline_resume_ = false;
  // Released only when Raft resolves the append, not when the caller's
  // local deadline wins. That distinction closes the uncertain-tail audit
  // overflow race.
  std::unique_ptr<AuditReservation> audit_reservation_;
  // Same ownership rule for durability-gate recovery. Serializing effective
  // recovery proposals prevents two callers from appending the same logical
  // shrink after both inspected one stale committed view.
  std::unique_ptr<FailSafeRecoveryReservation> recovery_reservation_;
};

// Awaiter for the Raft round trip. The destructor runs on every exit from
// the co_await expression — including destruction of a suspended frame — and
// detaches the waiter so a late completion can never resume a dead coroutine.
class ProposeAwaiter {
 public:
  explicit ProposeAwaiter(std::shared_ptr<ProposeWaiter> waiter) noexcept
      : waiter_(std::move(waiter)) {}

  bool await_ready() noexcept {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    return waiter_->ready_;
  }
  bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    if (waiter_->ready_) return false;  // completed before we parked
    waiter_->awaiting_ = awaiting;
    return true;
  }
  // The waiter fields are read by the coroutine body after resumption; both
  // completion paths (inline during when_ready, or through ForeignExecutor)
  // establish the happens-before edge, so no relock is needed there.
  void await_resume() noexcept {}

  ~ProposeAwaiter() {
    std::lock_guard<std::mutex> lock(waiter_->mu_);
    waiter_->detached_ = true;
    waiter_->awaiting_ = {};
  }

 private:
  std::shared_ptr<ProposeWaiter> waiter_;
};

// Shared scheduling step for both completion paths (raft callback, timeout).
// Construction guarantees one explicit policy for every waiter, so neither
// foreign thread has an accidental inline-resume path.
void ScheduleProposeResume(const ProposeWaiter& waiter,
                           std::coroutine_handle<> to_resume) {
  if (!to_resume) return;
  if (waiter.inline_resume_) {
    to_resume.resume();
    return;
  }
  // A rejected wake (teardown violation or mailbox allocation failure) cannot
  // be recovered without stranding the coordinator's in-flight drain.
  if (!waiter.foreign_executor_.Resume(to_resume)) std::terminate();
}

void CompletePropose(const std::shared_ptr<ProposeWaiter>& waiter,
                     CmdResult& result, std::shared_ptr<std::exception>& err) {
  std::coroutine_handle<> to_resume;
  {
    std::lock_guard<std::mutex> lock(waiter->mu_);
    // FIRST-WINS for the caller-visible result. A late Raft completion still
    // releases its audit reservation: the unresolved append, not the local
    // coroutine lifetime, owns that headroom.
    if (waiter->ready_) {
      waiter->audit_reservation_.reset();
      waiter->recovery_reservation_.reset();
      return;
    }
    waiter->code_ = result.get_result_code();
    // Shutdown delivers CANCELLED; a missing exception does not turn an
    // unresolved proposal into a successful application outcome.
    waiter->has_exception_ = (err != nullptr);
    if (waiter->code_ == MetaRaftResultCode::OK) {
      // The state machine's commit() return carries the exact apply outcome;
      // do not re-read the rotating/prunable audit window after resumption.
      std::shared_ptr<MetaRaftBuffer>& payload = result.get();
      if (payload != nullptr) {
        const std::string_view bytes(
            reinterpret_cast<const char*>(payload->data_begin()),
            payload->size());
        auto decoded = DecodeMetaApplyResult(bytes);
        if (decoded.ok()) waiter->apply_result_ = std::move(*decoded);
      }
    }
    waiter->ready_ = true;
    if (!waiter->detached_ && waiter->awaiting_) {
      to_resume = waiter->awaiting_;
    }
    waiter->audit_reservation_.reset();
    waiter->recovery_reservation_.reset();
  }
  ScheduleProposeResume(*waiter, to_resume);
}

void FailProposeDispatch(const std::shared_ptr<ProposeWaiter>& waiter) {
  std::coroutine_handle<> to_resume;
  {
    std::lock_guard<std::mutex> lock(waiter->mu_);
    if (waiter->ready_) {
      waiter->audit_reservation_.reset();
      waiter->recovery_reservation_.reset();
      return;
    }
    waiter->code_ = MetaRaftResultCode::FAILED;
    waiter->ready_ = true;
    if (!waiter->detached_ && waiter->awaiting_) {
      to_resume = waiter->awaiting_;
    }
    waiter->audit_reservation_.reset();
    waiter->recovery_reservation_.reset();
  }
  ScheduleProposeResume(*waiter, to_resume);
}

// UTC "YYYY-MM-DDTHH:MM:SS.mmmZ" — far under kMaxMetaActorReadableTimeBytes.
// Twin of the ctl server's FormatReadableTime (ctl_server.cpp): both are
// trusted-entry propose stamps; keep the format identical for audit
// readability. If one ever changes, change both.
std::string FormatReadableTime() {
  const auto now = std::chrono::system_clock::now();
  const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch())
                          .count();
  const std::time_t secs = static_cast<std::time_t>(millis / 1000);
  std::tm tm{};
  ::gmtime_r(&secs, &tm);
  // Sized for the theoretical worst case of the %0Nd ints (~75 bytes).
  char buf[96];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                tm.tm_min, tm.tm_sec, static_cast<int>(millis % 1000));
  return buf;
}

absl::Status IneffectiveFailSafeRecovery(std::string_view detail) {
  return absl::ResourceExhaustedError(
      "meta: durability fail-safe admits only one effective recovery "
      "mutation at a time: " +
      std::string(detail));
}

bool IsNonTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kSubmitted ||
         lifecycle == MetaOperationLifecycle::kRunning;
}

// The WAL/snapshot fail-safe cannot use a variant-name whitelist: most prune
// commands are intentionally replay-idempotent, so an absent target would let
// fresh request ids append no-op records forever. Evaluate the command against
// the exact committed view and require a one-way, bounded recovery effect.
// Terminalization is the sole non-shrinking prerequisite and permits no new
// variable-length payload while the guard is active; it can happen once per
// live operation and unlocks archive -> prune.
absl::Status ValidateFailSafeRecovery(const MetaCommand& command,
                                      const MetaCommittedView& view,
                                      std::uint64_t applied_index,
                                      std::string_view actor_principal,
                                      std::string_view readable_time) {
  const MetaStores& stores = view.stores();
  if (applied_index == std::numeric_limits<std::uint64_t>::max()) {
    return IneffectiveFailSafeRecovery("the applied index is exhausted");
  }
  if (const auto* complete = std::get_if<CompleteOperation>(&command)) {
    const auto before =
        stores.operation_.FindOperation(complete->operation_id_);
    const bool creation_root =
        before.has_value() &&
        before->kind_ == kMetaClusterCreateOperationKind &&
        stores.topology_.ClusterLifecycle().root_operation_id_ ==
            complete->operation_id_;
    if ((!creation_root && !complete->result_.empty()) ||
        (creation_root && complete->result_ != "cluster-created")) {
      return IneffectiveFailSafeRecovery(
          "CompleteOperation result is not the bounded workflow outcome");
    }
    MetaStores candidate = stores;
    const MetaApplyResult applied = ApplyCommitted(
        candidate, applied_index + 1, command, actor_principal, readable_time);
    const auto after =
        candidate.operation_.FindOperation(complete->operation_id_);
    if (applied.verdict_ != MetaAuditVerdict::kAccepted ||
        !before.has_value() || !after.has_value() ||
        !IsNonTerminal(before->lifecycle_) ||
        after->lifecycle_ != MetaOperationLifecycle::kCompleted ||
        after->revision_ != before->revision_ + 1 ||
        (creation_root && candidate.topology_.ClusterLifecycle().state_ !=
                              MetaClusterLifecycle::kCreated)) {
      return IneffectiveFailSafeRecovery(
          "CompleteOperation does not terminalize the full aggregate");
    }
    return absl::OkStatus();
  }
  if (const auto* abort = std::get_if<AbortOperation>(&command)) {
    const auto before = stores.operation_.FindOperation(abort->operation_id_);
    const bool creation_root =
        before.has_value() &&
        before->kind_ == kMetaClusterCreateOperationKind &&
        stores.topology_.ClusterLifecycle().root_operation_id_ ==
            abort->operation_id_;
    if (!creation_root && !abort->reason_.empty()) {
      return IneffectiveFailSafeRecovery(
          "AbortOperation must use an empty reason while recovery is gated");
    }
    MetaStores candidate = stores;
    const MetaApplyResult applied = ApplyCommitted(
        candidate, applied_index + 1, command, actor_principal, readable_time);
    const auto after = candidate.operation_.FindOperation(abort->operation_id_);
    if (applied.verdict_ != MetaAuditVerdict::kAccepted ||
        !before.has_value() || !after.has_value() ||
        !IsNonTerminal(before->lifecycle_) ||
        after->lifecycle_ != MetaOperationLifecycle::kAborted ||
        after->revision_ != before->revision_ + 1 ||
        (creation_root && candidate.topology_.ClusterLifecycle().state_ !=
                              MetaClusterLifecycle::kProvisioningFailed)) {
      return IneffectiveFailSafeRecovery(
          "AbortOperation does not terminalize the full aggregate");
    }
    return absl::OkStatus();
  }
  if (const auto* abort = std::get_if<AbortControlledFailover>(&command)) {
    const auto before_operation =
        stores.operation_.FindOperation(abort->operation_id_);
    if (!before_operation.has_value() ||
        !IsNonTerminal(before_operation->lifecycle_)) {
      return IneffectiveFailSafeRecovery(
          "AbortControlledFailover does not target a live operation");
    }

    // Model the only topology effect this recovery command may have. The
    // pre-Begin form must leave topology byte-for-byte unchanged; the
    // post-Begin form may remove only the exact transition that belongs to
    // the operation it terminalizes.
    MetaTopologyStore expected_topology = stores.topology_;
    if (abort->expected_transition_.has_value()) {
      const auto before_group = stores.topology_.FindGroup(abort->group_id_);
      if (!before_group.has_value() ||
          !before_group->failover_transition_.has_value() ||
          before_group->failover_transition_->transition_id_ !=
              abort->expected_transition_->transition_id_ ||
          before_group->failover_transition_->revision_ !=
              abort->expected_transition_->revision_ ||
          before_group->failover_transition_->mode_ !=
              MetaFailoverMode::kControlled ||
          !before_group->failover_transition_->controlled_.has_value() ||
          before_group->failover_transition_->controlled_->operation_id_ !=
              abort->operation_id_) {
        return IneffectiveFailSafeRecovery(
            "AbortControlledFailover does not name the exact controlled "
            "transition");
      }
      if (absl::Status cleared = expected_topology.ClearFailoverTransition(
              abort->group_id_, *abort->expected_transition_);
          !cleared.ok()) {
        return IneffectiveFailSafeRecovery(
            "AbortControlledFailover cannot clear its transition");
      }
    }

    MetaStores candidate = stores;
    const MetaApplyResult applied = ApplyCommitted(
        candidate, applied_index + 1, command, actor_principal, readable_time);
    const auto after_operation =
        candidate.operation_.FindOperation(abort->operation_id_);
    if (applied.verdict_ != MetaAuditVerdict::kAccepted ||
        !after_operation.has_value() ||
        after_operation->lifecycle_ != MetaOperationLifecycle::kAborted ||
        after_operation->revision_ != before_operation->revision_ + 1 ||
        after_operation->terminal_result_ != abort->reason_ ||
        after_operation->data_loss_possible_ ||
        candidate.topology_.Serialize() != expected_topology.Serialize()) {
      return IneffectiveFailSafeRecovery(
          "AbortControlledFailover does not exclusively terminalize its "
          "operation and optional transition");
    }
    return absl::OkStatus();
  }
  if (const auto* archive = std::get_if<ArchiveOperations>(&command)) {
    MetaOperationStore candidate = stores.operation_;
    const std::size_t live_before = candidate.LiveCount();
    const std::size_t archived_before = candidate.ArchivedCount();
    const absl::Status applied = candidate.ArchiveOperations(*archive);
    if (!applied.ok() || candidate.LiveCount() >= live_before ||
        candidate.ArchivedCount() <= archived_before) {
      return IneffectiveFailSafeRecovery(
          "ArchiveOperations moves no terminal live operation");
    }
    return absl::OkStatus();
  }
  if (const auto* prune = std::get_if<PruneOperationArchive>(&command)) {
    MetaOperationStore candidate = stores.operation_;
    const std::size_t before = candidate.ArchivedCount();
    const absl::Status applied = candidate.PruneArchive(*prune);
    if (!applied.ok() || candidate.ArchivedCount() >= before) {
      return IneffectiveFailSafeRecovery(
          "PruneOperationArchive removes no retained summary");
    }
    return absl::OkStatus();
  }
  if (const auto* prune = std::get_if<PruneTerminalReceipts>(&command)) {
    std::size_t before = 0;
    for (const MetaTerminalReceiptKey& key : prune->receipts_) {
      before += stores.operation_.FindTerminalReceipt(key).has_value() ? 1 : 0;
    }
    MetaOperationStore candidate = stores.operation_;
    const absl::Status applied = candidate.PruneTerminalReceipts(*prune);
    std::size_t after = 0;
    for (const MetaTerminalReceiptKey& key : prune->receipts_) {
      after += candidate.FindTerminalReceipt(key).has_value() ? 1 : 0;
    }
    if (!applied.ok() || after >= before) {
      return IneffectiveFailSafeRecovery(
          "PruneTerminalReceipts removes no retained receipt");
    }
    return absl::OkStatus();
  }
  if (const auto* prune = std::get_if<PrunePopulationManifest>(&command)) {
    if (!stores.population_manifest_.Contains(prune->manifest_digest_) ||
        stores.topology_.PopulationManifestInUse(prune->manifest_digest_) ||
        stores.operation_.PopulationManifestInUse(prune->manifest_digest_)) {
      return IneffectiveFailSafeRecovery(
          "PrunePopulationManifest removes no unreferenced document");
    }
    MetaPopulationManifestStore candidate = stores.population_manifest_;
    const std::size_t before = candidate.Size();
    const absl::Status applied = candidate.Prune(*prune);
    if (!applied.ok() || candidate.Size() >= before) {
      return IneffectiveFailSafeRecovery(
          "PrunePopulationManifest removes no unreferenced document");
    }
    return absl::OkStatus();
  }
  if (std::holds_alternative<PruneAudit>(command)) {
    const auto before = stores.audit_.Serialize();
    if (!before.ok()) {
      return absl::InternalError(
          "meta: cannot evaluate fail-safe audit recovery state");
    }
    // Apply against a minimal aggregate carrying the exact audit store. This
    // reuses the real prune + mandatory audit-append semantics without copying
    // unrelated potentially-large snapshot stores onto the ingress thread.
    MetaStores candidate;
    candidate.audit_ = stores.audit_;
    const MetaApplyResult applied = ApplyCommitted(
        candidate, applied_index + 1, command, actor_principal, readable_time);
    const auto after = candidate.audit_.Serialize();
    if (applied.verdict_ != MetaAuditVerdict::kAccepted || !after.ok() ||
        after->size() >= before->size()) {
      return IneffectiveFailSafeRecovery(
          "PruneAudit does not reduce the serialized audit window after its "
          "own audit record");
    }
    return absl::OkStatus();
  }
  return IneffectiveFailSafeRecovery(
      "the command is not part of terminalize, archive, or prune recovery");
}

std::int64_t NowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

absl::Status UncertainOutcome(absl::StatusCode code, const std::string& what) {
  return absl::Status(
      code,
      "meta: propose " + what +
          "; outcome uncertain — the command may still have committed; "
          "reconcile via CommittedView using the command's idempotency key "
          "(commands are replay/idempotency-safe by design)");
}

}  // namespace

// ---------------------------------------------------------------------------
// MetaProposeTimer: one thread walking a deadline queue of proposal waiters.
// Raft resolves results on application, demotion or shutdown; the coordinator
// owns client deadlines and bounds the wait
// itself. Expiry resolves the waiter as TIMEOUT (first-wins against a late
// raft completion). The thread holds only weak waiter references and never
// touches coordinator state, so destruction just stops and joins.
// ---------------------------------------------------------------------------

class MetaProposeTimer {
 public:
  MetaProposeTimer() : thread_([this] { Main(); }) {}
  ~MetaProposeTimer() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
  }
  MetaProposeTimer(const MetaProposeTimer&) = delete;
  MetaProposeTimer& operator=(const MetaProposeTimer&) = delete;

  void Arm(std::chrono::steady_clock::time_point deadline,
           std::weak_ptr<ProposeWaiter> waiter) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      queue_.emplace(std::make_pair(deadline, seq_++), std::move(waiter));
    }
    cv_.notify_all();
  }

 private:
  void Main() {
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
      if (stop_) return;
      if (queue_.empty()) {
        cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
        continue;
      }
      const auto earliest = queue_.begin();
      const auto now = std::chrono::steady_clock::now();
      if (earliest->first.first > now) {
        // Wake at the deadline, or early when Arm notifies (the new entry may
        // be earlier than the current earliest).
        cv_.wait_until(lock, earliest->first.first);
        continue;
      }
      std::shared_ptr<ProposeWaiter> waiter = earliest->second.lock();
      queue_.erase(earliest);
      // The resume runs arbitrary continuations — never under the queue lock.
      lock.unlock();
      ResolveTimeout(std::move(waiter));
      lock.lock();
    }
  }

  static void ResolveTimeout(std::shared_ptr<ProposeWaiter> waiter) {
    if (waiter == nullptr) return;
    std::coroutine_handle<> to_resume;
    {
      std::lock_guard<std::mutex> lock(waiter->mu_);
      if (waiter->ready_) return;  // the raft completion won
      waiter->code_ = MetaRaftResultCode::TIMEOUT;
      waiter->ready_ = true;
      if (!waiter->detached_ && waiter->awaiting_) {
        to_resume = waiter->awaiting_;
      }
    }
    ScheduleProposeResume(*waiter, to_resume);
  }

  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::map<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>,
           std::weak_ptr<ProposeWaiter>>
      queue_;
  std::uint64_t seq_ = 0;
  std::thread thread_;
};

// Decrements the coordinator's in-flight count on every coroutine exit,
// normal or by frame destruction mid-flight (a dropped Propose task). That
// second path is what keeps the coordinator destructor's drain wait from
// hanging on abandoned tasks.
class MetaCoordinator::InFlightGuard {
 public:
  explicit InFlightGuard(MetaCoordinator& coordinator)
      : coordinator_(coordinator) {
    coordinator_.InFlightEnter();
  }
  ~InFlightGuard() { coordinator_.InFlightLeave(); }

 private:
  MetaCoordinator& coordinator_;
};

// ---------------------------------------------------------------------------
// MetaCoordinator
// ---------------------------------------------------------------------------

MetaCoordinator::MetaCoordinator(std::shared_ptr<MetaRaft> server,
                                 MetaStateMachine& state_machine,
                                 MetaObservationStore& observations,
                                 MetaCoordinatorOptions options)
    : server_(std::move(server)),
      state_machine_(state_machine),
      observations_(observations),
      options_(std::move(options)),
      owned_proposal_executor_(options_.proposal_executor_ == nullptr
                                   ? std::make_unique<MetaProposalExecutor>()
                                   : nullptr),
      proposal_executor_(options_.proposal_executor_ != nullptr
                             ? options_.proposal_executor_
                             : owned_proposal_executor_.get()),
      proposal_gate_(std::make_shared<MetaProposalGate>()),
      propose_timer_(std::make_unique<MetaProposeTimer>()),
      sub_core_(std::make_shared<MetaSubscriptionCore>()),
      leader_context_(*this,
                      AuthenticatedPrincipal(options_.coordinator_principal_,
                                             MetaPrincipalPasskey{})) {
  if (server_ != nullptr && !options_.foreign_executor_.valid() &&
      !options_.inline_resume_for_testing_) {
    throw std::invalid_argument(
        "MetaCoordinator requires a foreign executor when attached to Raft");
  }
  // A coordinator assembled onto already-committed state (recovery,
  // assembly ordering) starts its coverage mark at the durable watermark:
  // everything at or below it is covered by the initial view, never by
  // events this process will deliver.
  sub_core_->high_water_ = state_machine_.last_commit_index();

  // The sink must be O(1) and non-blocking — it runs on the commit thread
  // under the SM state mutex (state_machine.h). It captures the core by
  // weak_ptr so a sink call racing the destructor can never touch a dead
  // coordinator; the destructor detaches the sink before tearing down.
  std::weak_ptr<MetaSubscriptionCore> weak_core = sub_core_;
  state_machine_.SetCommitEventSink(
      [weak_core](std::uint64_t log_index, const MetaApplyResult& result) {
        const std::shared_ptr<MetaSubscriptionCore> core = weak_core.lock();
        if (core == nullptr) return;
        std::lock_guard<std::mutex> lock(core->mu_);
        if (log_index > core->high_water_) {
          core->high_water_ = log_index;
        }
        core->observations_dirty_ = true;
        bool any_queued = false;
        for (const auto& entry : core->subs_) {
          const std::shared_ptr<Subscriber>& sub = entry.second;
          if (sub->cancelled_) continue;
          if (sub->queue_.size() >= sub->capacity_) {
            // Backpressure contract: overflow cancels the subscription and
            // flags mandatory resync. The event is NOT delivered — the
            // consumer must rebuild from a fresh view, never guess.
            sub->cancelled_ = true;
            sub->needs_resync_ = true;
            sub->queue_.clear();
            continue;
          }
          sub->queue_.push_back(MetaCommitEvent{log_index, result});
          any_queued = true;
        }
        // Revalidation needs a wake even when there are no subscriptions.
        if (any_queued || core->observations_dirty_) core->cv_.notify_all();
      });

  try {
    dispatch_thread_ = std::thread(&MetaCoordinator::DispatchMain, this);
    leadership_thread_ = std::thread(&MetaCoordinator::LeadershipMain, this);
  } catch (const std::system_error&) {
    // Half-started assembly: stop what exists before propagating.
    stopping_.store(true, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(sub_core_->mu_);
      sub_core_->dispatch_stop_ = true;
    }
    sub_core_->cv_.notify_all();
    leadership_cv_.notify_all();
    if (dispatch_thread_.joinable()) dispatch_thread_.join();
    if (leadership_thread_.joinable()) leadership_thread_.join();
    state_machine_.SetCommitEventSink({});
    throw;
  }
}

MetaCoordinator::~MetaCoordinator() {
  stopping_.store(true, std::memory_order_release);
  // Detach first: no new sink calls after this returns (an in-flight call
  // holds its own core reference and finishes on its own).
  state_machine_.SetCommitEventSink({});

  // Cancel and join reconcilers on the leadership thread (it owns every
  // Start/CancelAndWait call).
  leadership_cv_.notify_all();
  if (leadership_thread_.joinable()) leadership_thread_.join();

  // Cancel every subscription, stop the dispatcher, join it. Handles that
  // outlive the coordinator observe cancelled() through the shared core.
  {
    std::lock_guard<std::mutex> lock(sub_core_->mu_);
    for (const auto& entry : sub_core_->subs_) {
      entry.second->cancelled_ = true;
      entry.second->queue_.clear();
    }
    sub_core_->dispatch_stop_ = true;
  }
  sub_core_->cv_.notify_all();
  if (dispatch_thread_.joinable()) dispatch_thread_.join();

  // Drain in-flight proposals. raft_server::shutdown() (teardown contract,
  // file header) resolves their cmd_results; wedged ones are resolved by the
  // propose timer (still alive here); dropped Propose tasks decrement via
  // their InFlightGuard. Either way this terminates.
  {
    std::unique_lock<std::mutex> lock(gate_mu_);
    gate_cv_.wait(lock, [&] { return in_flight_ == 0; });
  }

  // Only now stop the timer: in-flight proposals needed it for their
  // timeout resolution. Any waiter entries left in its queue are weak and
  // simply expire.
  propose_timer_.reset();
}

void MetaCoordinator::InFlightEnter() {
  std::lock_guard<std::mutex> lock(gate_mu_);
  ++in_flight_;
}

void MetaCoordinator::InFlightLeave() {
  std::lock_guard<std::mutex> lock(gate_mu_);
  if (in_flight_ > 0) --in_flight_;
  if (in_flight_ == 0) gate_cv_.notify_all();
}

absl::Status MetaCoordinator::NotLeaderStatus() const {
  std::string hint = "none";
  const std::int32_t leader = server_->get_leader();
  if (leader >= 0) {
    hint = "id=" + std::to_string(leader);
    const std::shared_ptr<MetaRaftMember> config =
        server_->get_srv_config(leader);
    if (config != nullptr) {
      hint += " endpoint=" + config->get_endpoint();
    }
  }
  return absl::Status(absl::StatusCode::kFailedPrecondition,
                      "meta: not leader; known leader: " + hint);
}

MetaStores MetaCoordinator::AtomicStoresSnapshot(std::uint64_t& applied_index,
                                                 std::uint64_t& high_water) {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  // Two-phase bracketed read, NO core lock held across SM calls (lock order).
  // Accept when neither watermark moved across the capture:
  //   - high_water_ (sink, set inside the SM's apply critical section) stable
  //     => no command commit reflected in the captured stores is newer than
  //        it;
  //   - last_commit_index() (set atomically with the stores for snapshot
  //     installs, right after them for command commits) stable => no install
  //     landed mid-capture.
  // Together the accepted stores reflect exactly the committed prefix through
  // li1. Retries are rare: control-plane commit rates are low.
  for (;;) {
    std::uint64_t hw1;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw1 = core->high_water_;
    }
    const std::uint64_t li1 = state_machine_.last_commit_index();
    MetaStores stores = state_machine_.StoresSnapshot();
    const std::uint64_t li2 = state_machine_.last_commit_index();
    std::uint64_t hw2;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw2 = core->high_water_;
    }
    if (hw1 == hw2 && li1 == li2) {
      applied_index = li1;
      high_water = hw1;
      return stores;
    }
  }
}

MetaCommittedView MetaCoordinator::CommittedView() {
  std::uint64_t applied_index = 0;
  std::uint64_t high_water = 0;
  MetaStores stores = AtomicStoresSnapshot(applied_index, high_water);
  return MetaCommittedView(std::move(stores), applied_index);
}

std::uint64_t MetaCoordinator::AppliedIndex() const {
  return state_machine_.last_commit_index();
}

std::uint64_t MetaCoordinator::CommittedHighWater() const {
  return state_machine_.state_change_index();
}

MetaSubscriptionStart MetaCoordinator::SubscribeCommitted(
    MetaCommitCallback callback, std::size_t queue_capacity) {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  const std::size_t capacity = queue_capacity != 0
                                   ? queue_capacity
                                   : options_.default_subscription_capacity_;
  // Same atomic capture as AtomicStoresSnapshot, with registration folded
  // into the accepting critical section: the subscriber's queue starts empty
  // and the sink appends every later commit, so events are exactly the
  // commits with log_index > cursor, in order.
  for (;;) {
    std::uint64_t hw1;
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      hw1 = core->high_water_;
    }
    const std::uint64_t li1 = state_machine_.last_commit_index();
    MetaStores stores = state_machine_.StoresSnapshot();
    const std::uint64_t li2 = state_machine_.last_commit_index();
    {
      std::lock_guard<std::mutex> lock(core->mu_);
      if (core->high_water_ == hw1 && li1 == li2) {
        auto sub = std::make_shared<Subscriber>();
        sub->id_ = core->next_id_++;
        sub->callback_ = std::move(callback);
        sub->capacity_ = capacity;
        core->subs_[sub->id_] = sub;
        return MetaSubscriptionStart{
            MetaCommittedView(std::move(stores), li1), hw1,
            std::unique_ptr<MetaCommitSubscription>(
                new MetaCommitSubscription(core, sub->id_))};
      }
    }
  }
}

void MetaCoordinator::AddValidateHook(MetaValidateHook hook) {
  hooks_.push_back(std::move(hook));
}

void MetaCoordinator::DispatchMain() {
  const std::shared_ptr<MetaSubscriptionCore> core = sub_core_;
  std::unique_lock<std::mutex> lock(core->mu_);
  while (!core->dispatch_stop_) {
    core->cv_.wait(lock, [&] {
      if (core->dispatch_stop_) return true;
      if (core->observations_dirty_) return true;
      for (const auto& entry : core->subs_) {
        const std::shared_ptr<Subscriber>& sub = entry.second;
        if (!sub->cancelled_ && !sub->callback_in_flight_ &&
            !sub->queue_.empty()) {
          return true;
        }
      }
      return false;
    });
    if (core->dispatch_stop_) break;
    if (core->observations_dirty_) {
      core->observations_dirty_ = false;
      lock.unlock();
      // Do not hold the subscription mutex across state-machine or
      // observation-store calls. A commit racing this snapshot sets the dirty
      // bit again, guaranteeing another pass after this one.
      const MetaStores stores = state_machine_.StoresSnapshot();
      const MetaStoresFacts facts(stores);
      observations_.RevalidateAll(facts, NowUnixMs());
      lock.lock();
      continue;
    }
    for (const auto& entry : core->subs_) {
      const std::shared_ptr<Subscriber>& sub = entry.second;
      if (sub->cancelled_ || sub->callback_in_flight_ || sub->queue_.empty()) {
        continue;
      }
      MetaCommitEvent event = std::move(sub->queue_.front());
      sub->queue_.pop_front();
      sub->callback_in_flight_ = true;
      MetaCommitCallback callback = sub->callback_;
      lock.unlock();
      // Subscriber code on the dispatch thread. Per-subscription delivery is
      // strictly FIFO; one event per round keeps cancellation responsive.
      callback(event);
      lock.lock();
      sub->callback_in_flight_ = false;
      core->cv_.notify_all();  // wake a handle destructor waiting on in-flight
      break;
    }
  }
}

void MetaCoordinator::RunAsLeader(std::shared_ptr<MetaReconciler> reconciler) {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    leadership_events_.push_back(
        LeadershipEvent{LeadershipEventKind::kRegister, std::move(reconciler)});
  }
  leadership_cv_.notify_one();
}

void MetaCoordinator::BecomeLeader() {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    leadership_events_.push_back(
        LeadershipEvent{LeadershipEventKind::kBecomeLeader, nullptr});
  }
  leadership_cv_.notify_one();
}

void MetaCoordinator::BecomeFollower() {
  {
    std::lock_guard<std::mutex> lock(leadership_mu_);
    if (stopping_.load(std::memory_order_acquire)) return;
    leadership_events_.push_back(
        LeadershipEvent{LeadershipEventKind::kBecomeFollower, nullptr});
  }
  leadership_cv_.notify_one();
}

void MetaCoordinator::LeadershipMain() {
  std::unique_lock<std::mutex> lock(leadership_mu_);
  for (;;) {
    leadership_cv_.wait(lock, [&] {
      return stopping_.load(std::memory_order_acquire) ||
             !leadership_events_.empty();
    });
    if (stopping_.load(std::memory_order_acquire)) break;
    LeadershipEvent event = std::move(leadership_events_.front());
    leadership_events_.pop_front();
    // External lifecycle calls can wait for their own workers. Leave the
    // queue mutex free so Raft callbacks keep recording later edges while the
    // current edge is still serving as their ordering barrier.
    lock.unlock();

    if (event.kind_ == LeadershipEventKind::kRegister) {
      reconcilers_.push_back(
          ReconcilerEntry{std::move(event.reconciler_), false});
      ReconcilerEntry& entry = reconcilers_.back();
      if (applied_leader_) {
        entry.started_ = true;
        entry.reconciler_->Start(leader_context_);
      }
    } else if (event.kind_ == LeadershipEventKind::kBecomeLeader) {
      if (!applied_leader_) {
        // Soft evidence is scoped to one leadership epoch. Reset before any
        // reconciler starts so it can act only on freshly authenticated data.
        observations_.ResetForLeadershipChange();
        applied_leader_ = true;
        for (ReconcilerEntry& entry : reconcilers_) {
          if (!entry.started_) {
            entry.started_ = true;
            entry.reconciler_->Start(leader_context_);
          }
        }
      }
    } else {
      // Every observed follower edge is processed, including a redundant one:
      // it is the invalidation barrier for all leader-local authority.
      applied_leader_ = false;
      // Cancel in reverse registration order (stack discipline for
      // reconcilers that depend on earlier ones).
      for (auto it = reconcilers_.rbegin(); it != reconcilers_.rend(); ++it) {
        if (it->started_) {
          it->reconciler_->CancelAndWait();
          it->started_ = false;
        }
      }
      observations_.ResetForLeadershipChange();
    }
    lock.lock();
  }
  lock.unlock();
  // Teardown: cancel and join whatever is still running.
  for (auto it = reconcilers_.rbegin(); it != reconcilers_.rend(); ++it) {
    if (it->started_) {
      it->reconciler_->CancelAndWait();
      it->started_ = false;
    }
  }
  applied_leader_ = false;
}

void MetaLeadershipRelay::RecordLeaderEdge() noexcept { Record(Role::kLeader); }

void MetaLeadershipRelay::RecordFollowerEdge() noexcept {
  Record(Role::kFollower);
}

void MetaLeadershipRelay::Record(Role role) noexcept {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopped_) return;
  // Allocation failure is fail-stop: dropping a role edge could preserve an
  // obsolete authority session, which is less safe than terminating.
  pending_.push_back(role);
}

void MetaLeadershipRelay::Drain() noexcept {
  std::deque<Role> batch;
  MetaCoordinator* target = nullptr;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (stopped_ || target_ == nullptr || draining_) return;
    draining_ = true;
    target = target_;
    batch.swap(pending_);
  }

  for (;;) {
    for (Role role : batch) Forward(*target, role);
    batch.clear();

    std::lock_guard<std::mutex> lock(mu_);
    if (stopped_ || pending_.empty()) {
      draining_ = false;
      cv_.notify_all();
      return;
    }
    // Record() only appends and this is the sole drainer, so swapping the next
    // prefix outside the lock preserves the global callback order.
    batch.swap(pending_);
  }
}

void MetaLeadershipRelay::Attach(MetaCoordinator& coordinator) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (attached_once_ || stopped_) {
      throw std::logic_error(
          "MetaLeadershipRelay may be attached exactly once");
    }
    attached_once_ = true;
    target_ = &coordinator;
  }
  Drain();
  // If the Bycorf worker won the single-drainer race, wait for it to finish the
  // retained pre-attach prefix before process assembly proceeds.
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [&] { return !draining_; });
}

void MetaLeadershipRelay::DetachAndStop() noexcept {
  std::unique_lock<std::mutex> lock(mu_);
  stopped_ = true;
  pending_.clear();
  // Drain forwards outside mu_; wait out its captured target before releasing
  // the caller's lifetime ownership of the coordinator.
  cv_.wait(lock, [&] { return !draining_; });
  target_ = nullptr;
}

void MetaLeadershipRelay::Forward(MetaCoordinator& coordinator, Role role) {
  if (role == Role::kLeader) {
    coordinator.BecomeLeader();
  } else {
    coordinator.BecomeFollower();
  }
}

bycorf::Task<absl::StatusOr<MetaApplyResult>> MetaCoordinator::Propose(
    MetaCommand command, AuthenticatedPrincipal principal) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return absl::Status(absl::StatusCode::kCancelled,
                           "meta: coordinator is stopping");
  }
  if (server_ == nullptr) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "meta: no raft server attached");
  }
  if (!server_->is_leader()) {
    co_return NotLeaderStatus();
  }

  // Counted from here to every exit (normal or frame destruction) so the
  // destructor can drain caller coroutines. Audit headroom follows the Raft
  // completion separately and can outlive a timed-out caller.
  InFlightGuard in_flight(*this);

  // One atomic committed view serves the fail-safe gates AND the validate
  // hooks. This bounded aggregate copy may be large and contributes to
  // proposal latency, but copying once is cheaper than letting each hook
  // snapshot independently and keeps every validation on one exact cut.
  std::uint64_t applied_index = 0;
  std::uint64_t high_water = 0;
  MetaStores stores = AtomicStoresSnapshot(applied_index, high_water);
  MetaCommittedView view(std::move(stores), applied_index);
  // All semantic hooks evaluate volatile observations against one proposal
  // instant. Re-reading wall time in individual hooks could otherwise make
  // command admission depend on hook order around the same TTL boundary.
  const std::int64_t proposal_now_unix_ms = NowUnixMs();
  // Reuse one stamp for emergency candidate evaluation and the real command;
  // audit-size admission must model exactly the record that would be appended.
  const std::string readable_time = FormatReadableTime();

  // Strict-export reserves audit headroom before validation/encoding. The
  // default bounded-rotate policy needs no reservation because deterministic
  // apply evicts exactly one oldest record when required; disabled creates no
  // ordinary records. A strict reservation moves to the Raft waiter so an
  // uncertain client timeout cannot free space while its append is unresolved.
  std::unique_ptr<AuditReservation> audit_reservation;
  if (view.stores().audit_.policy() == MetaAuditPolicy::kStrictExport) {
    std::lock_guard<std::mutex> lock(proposal_gate_->mu_);
    const MetaAuditStore& audit = view.stores().audit_;
    const auto* prune = std::get_if<PruneAudit>(&command);
    const bool valid_prune =
        prune != nullptr && audit.Find(prune->through_log_index_).has_value();
    const bool prune_busy = proposal_gate_->prune_reserved_ ||
                            proposal_gate_->regular_reservations_ != 0;
    const bool regular_overflow =
        proposal_gate_->prune_reserved_ || audit.NeedsExport() ||
        audit.size() + proposal_gate_->regular_reservations_ + 1 >
            audit.capacity();
    const bool prune_overflow =
        !valid_prune &&
        (audit.NeedsExport() || audit.size() + 1 > audit.capacity());
    if ((prune != nullptr && (prune_busy || prune_overflow)) ||
        (prune == nullptr && regular_overflow)) {
      co_return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "meta: audit headroom unavailable; wait for pending proposals or "
          "export and prune the full window (fail-safe)");
    }
    if (prune != nullptr) {
      proposal_gate_->prune_reserved_ = true;
      audit_reservation =
          std::make_unique<AuditReservation>(proposal_gate_, /*prune=*/true);
    } else {
      ++proposal_gate_->regular_reservations_;
      audit_reservation =
          std::make_unique<AuditReservation>(proposal_gate_, /*prune=*/false);
    }
  }

  const std::uint64_t uncompacted = server_->UncompactedBytes();
  const std::uint64_t snapshot_failures =
      state_machine_.consecutive_snapshot_failures();
  const bool wal_fail_safe = uncompacted > options_.max_uncompacted_wal_bytes_;
  const bool snapshot_fail_safe =
      snapshot_failures >= options_.max_consecutive_snapshot_failures_;
  std::unique_ptr<FailSafeRecoveryReservation> recovery_reservation;
  if (wal_fail_safe || snapshot_fail_safe) {
    // Serialize recovery through the actual Raft outcome. A type whitelist is
    // insufficient because idempotent prune commands can legally be no-ops;
    // repeated fresh request ids would then grow the WAL without bound.
    std::lock_guard<std::mutex> lock(proposal_gate_->mu_);
    if (proposal_gate_->fail_safe_recovery_reserved_) {
      co_return IneffectiveFailSafeRecovery(
          "another recovery proposal still has an uncertain Raft outcome");
    }
    if (absl::Status recovery = ValidateFailSafeRecovery(
            command, view, applied_index, principal.principal(), readable_time);
        !recovery.ok()) {
      std::string trigger;
      if (wal_fail_safe) {
        trigger = "uncompacted WAL bytes " + std::to_string(uncompacted) +
                  " exceed limit " +
                  std::to_string(options_.max_uncompacted_wal_bytes_);
      }
      if (snapshot_fail_safe) {
        if (!trigger.empty()) trigger += "; ";
        trigger += std::to_string(snapshot_failures) +
                   " consecutive snapshot failures reached the limit " +
                   std::to_string(options_.max_consecutive_snapshot_failures_);
      }
      co_return absl::Status(absl::StatusCode::kResourceExhausted,
                             "meta: " + trigger + " (fail-safe); " +
                                 std::string(recovery.message()));
    }
    proposal_gate_->fail_safe_recovery_reserved_ = true;
    recovery_reservation =
        std::make_unique<FailSafeRecoveryReservation>(proposal_gate_);
  }

  // ValidateProposal plugins run leader-locally. The first rejection aborts
  // the proposal before anything is encoded or appended.
  for (const MetaValidateHook& hook : hooks_) {
    const absl::Status status =
        hook(command, view, observations_, proposal_now_unix_ms);
    if (!status.ok()) co_return status;
  }

  // Actor injection: the trusted entry's principal plus a
  // propose-time readable timestamp. The clock read is legal HERE — the
  // proposal entry point; apply only copies the text into the audit record.
  std::visit(
      [&](auto& cmd) {
        cmd.actor_.principal_ = principal.principal();
        cmd.actor_.readable_time_ = readable_time;
      },
      command);

  auto encoded = MetaStateMachine::EncodeCommand(command);
  if (!encoded.ok()) co_return encoded.status();

  auto waiter = std::make_shared<ProposeWaiter>();
  waiter->foreign_executor_ = options_.foreign_executor_;
  waiter->inline_resume_ = options_.inline_resume_for_testing_;
  waiter->audit_reservation_ = std::move(audit_reservation);
  waiter->recovery_reservation_ = std::move(recovery_reservation);
  std::vector<std::shared_ptr<MetaRaftBuffer>> logs;
  logs.push_back(*encoded);
  const absl::Status submitted = proposal_executor_->Submit(
      [server = server_, logs = std::move(logs), waiter]() mutable {
        try {
          std::shared_ptr<CmdResult> result = server->append_entries(logs);
          if (result == nullptr) {
            FailProposeDispatch(waiter);
            return;
          }
          // Registration belongs inside the task's exception boundary too:
          // without a handler, neither Raft nor the executor can resolve
          // the waiter for this dispatch.
          result->when_ready([waiter](CmdResult& completed,
                                      std::shared_ptr<std::exception>& err) {
            CompletePropose(waiter, completed, err);
          });
        } catch (...) {
          FailProposeDispatch(waiter);
        }
      });
  if (!submitted.ok()) co_return submitted;
  // The seam's own round-trip bound (Raft results have no
  // client-side timeout). It includes executor queueing time and first-wins
  // against the raft completion.
  propose_timer_->Arm(
      std::chrono::steady_clock::now() +
          std::chrono::milliseconds(options_.propose_timeout_ms_),
      waiter);
  co_await ProposeAwaiter(waiter);

  // The waiter is filled (see ProposeAwaiter for the happens-before).
  switch (waiter->code_) {
    case MetaRaftResultCode::OK:
      break;
    case MetaRaftResultCode::TIMEOUT:
      co_return UncertainOutcome(absl::StatusCode::kDeadlineExceeded,
                                 "timed out");
    case MetaRaftResultCode::CANCELLED:
      co_return UncertainOutcome(absl::StatusCode::kCancelled,
                                 "was cancelled (shutdown or leadership loss)");
    case MetaRaftResultCode::NOT_LEADER:
      co_return NotLeaderStatus();
    default:
      co_return UncertainOutcome(
          absl::StatusCode::kInternal,
          std::string("failed with raft code ") +
              std::to_string(static_cast<int>(waiter->code_)) +
              (waiter->has_exception_ ? " (exception attached)" : ""));
  }
  if (!waiter->apply_result_.has_value()) {
    co_return UncertainOutcome(absl::StatusCode::kInternal,
                               "committed without an apply result");
  }
  if (waiter->apply_result_->command_tag_ != MetaCommandTagOf(command)) {
    co_return absl::Status(
        absl::StatusCode::kInternal,
        "meta: committed apply result has a mismatched command tag");
  }
  co_return *waiter->apply_result_;
}

bycorf::Task<absl::StatusOr<MetaApplyResult>> MetaLeaderContext::Propose(
    MetaCommand command) {
  return coordinator_->Propose(std::move(command), actor_);
}

MetaCommittedView MetaLeaderContext::CommittedView() {
  return coordinator_->CommittedView();
}

std::uint64_t MetaLeaderContext::AppliedIndex() const {
  return coordinator_->AppliedIndex();
}

MetaSubscriptionStart MetaLeaderContext::SubscribeCommitted(
    MetaCommitCallback callback, std::size_t queue_capacity) {
  return coordinator_->SubscribeCommitted(std::move(callback), queue_capacity);
}

const MetaObservationStore& MetaLeaderContext::Observations() const {
  return coordinator_->Observations();
}

}  // namespace lavik::meta
