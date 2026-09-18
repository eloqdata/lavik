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

#include "lavik/cluster/authority.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "lavik/metrics.h"

namespace lavik::cluster {

namespace {

// Storage recovery and initial population both gate serving. Grant state is
// deliberately not part of readiness: a fenced group is handled by the
// ownership step below (no safe owner), not reported as still loading.
bool GroupReady(const GroupView& group) {
  return group.storage_ready_ && group.population_ready_;
}

// Keyed requests consult only the groups their slots map to, so one group's
// recovery does not stall traffic owned by healthy groups. Unbound slots have
// no group to consult; the unbound branch below outranks readiness for them
// in the committed decision order.
bool InvolvedGroupsReady(const ServingState& state,
                         std::span<const std::uint16_t> slots) {
  for (const std::uint16_t slot : slots) {
    const GroupView* group = state.GroupForSlot(slot);
    if (group != nullptr && !GroupReady(*group)) {
      return false;
    }
  }
  return true;
}

}  // namespace

AuthorityAdmission::AuthorityAdmission(AuthorityAdmission&& other) noexcept
    : decision_(std::exchange(other.decision_, Decision{})),
      state_(std::move(other.state_)),
      slots_(std::move(other.slots_)),
      gate_generation_(other.gate_generation_),
      lease_checked_(other.lease_checked_),
      mutation_started_(
          other.mutation_started_.load(std::memory_order_relaxed)),
      final_recheck_failed_(
          other.final_recheck_failed_.load(std::memory_order_relaxed)) {}

AuthorityAdmission& AuthorityAdmission::operator=(
    AuthorityAdmission&& other) noexcept {
  if (this == &other) return *this;
  // The moved-from admission no longer owns the snapshot behind its host.
  decision_ = std::exchange(other.decision_, Decision{});
  state_ = std::move(other.state_);
  slots_ = std::move(other.slots_);
  gate_generation_ = other.gate_generation_;
  lease_checked_ = other.lease_checked_;
  mutation_started_.store(
      other.mutation_started_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
  final_recheck_failed_.store(
      other.final_recheck_failed_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
  return *this;
}

Decision Admit(const ServingState* state, const RequestView& request) {
  Decision decision;

  // Loading gate, first in Redis's order too (processCommand runs its loading
  // check before getNodeByQuery): loading, then first-key unbound, then
  // cross-slot, then ownership. No-key commands consult the aggregate; keyed
  // commands only their involved groups. Allowlisted commands serve
  // unconditionally so clients can probe health and discovery while storage
  // recovers — the Redis layer mirrors the existing is_loading allowlist.
  const bool ready =
      state != nullptr &&
      (request.slots_.empty() ? state->FullyReady()
                              : InvolvedGroupsReady(*state, request.slots_));
  if (!ready) {
    decision.kind_ = request.loading_allowed_ ? Decision::Kind::kServe
                                              : Decision::Kind::kLoading;
    return decision;
  }

  // No-key commands admit locally; readiness above is the only generic gate.
  // The Redis adapter rejects persistent global mutations before this point,
  // because an empty slot set cannot name authority or a drain cell. Callers
  // also report key-extraction failure as empty so malformed commands reach
  // their own argument error, matching Redis getNodeByQuery.
  if (request.slots_.empty()) {
    decision.kind_ = Decision::Kind::kServe;
    return decision;
  }

  // First-key unbound outranks cross-slot (Redis checks coverage before the
  // single-slot rule): [unbound-slot key, other-slot key] yields CLUSTERDOWN,
  // not CROSSSLOT.
  const std::uint16_t slot = request.slots_.front();
  const GroupView* group = state->GroupForSlot(slot);
  if (group == nullptr) {
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
    return decision;
  }
  for (const std::uint16_t other : request.slots_.subspan(1)) {
    if (other != slot) {
      decision.kind_ = Decision::Kind::kCrossSlot;
      return decision;
    }
  }

  // Single-slot request from here on; the loading gate already established
  // the owning group's readiness.
  const NodeIndex self_index = state->SelfNodeIndex();
  if (self_index == group->primary_node_index_) {
    // A fenced group has no safe owner: the primary must not serve and there
    // is no other authority to redirect to.
    if (!group->granted_) {
      decision.kind_ = Decision::Kind::kClusterDownUnbound;
    } else if (request.is_write_ && group->mutations_paused_) {
      decision.kind_ = Decision::Kind::kTryAgain;
    } else {
      decision.kind_ = Decision::Kind::kServe;
    }
    return decision;
  }

  // A READONLY connection on a replica of the owning group serves reads
  // locally; staleness is the client's explicit choice. Writes and
  // non-READONLY reads redirect to the primary.
  if (self_index != kNoNodeIndex && !request.is_write_ &&
      request.connection_readonly_) {
    for (NodeIndex replica_index : group->replica_node_indices_) {
      if (replica_index == self_index) {
        decision.kind_ = Decision::Kind::kServeStaleRead;
        return decision;
      }
    }
  }

  const NodeDescriptor* primary = state->NodeAt(group->primary_node_index_);
  if (primary == nullptr) {
    // Build() rejects a group whose primary node is missing, so no committed
    // state reaches this; fail closed rather than redirect to nowhere.
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
    return decision;
  }
  // MOVED carries the concrete advertised address — the empty-host
  // startup-node convention is a discovery-only concern — and both ports;
  // the Redis layer picks between them by the connection's TLS state.
  decision.kind_ = Decision::Kind::kMoved;
  decision.moved_slot_ = slot;
  decision.moved_host_ = primary->host();
  decision.moved_port_ = primary->port_;
  decision.moved_tls_port_ = primary->tls_port_;
  return decision;
}

bool AuthorityUnchanged(const ServingState& admitted,
                        const ServingState* current,
                        std::span<const std::uint16_t> slots) {
  // The gate and owner normally observe the same immutable snapshot. Return
  // before walking slot tables or comparing group ids/tokens; pointer identity
  // is a complete authority proof because ServingState cannot change in place.
  if (&admitted == current) return true;
  if (current == nullptr) {
    // Losing the committed state entirely revokes every slotted admission;
    // a no-key request captured no authority and stays valid.
    return slots.empty();
  }
  for (const std::uint16_t slot : slots) {
    const GroupView* before = admitted.GroupForSlot(slot);
    const GroupView* now = current->GroupForSlot(slot);
    // Coverage transitions are authority changes: an admission made while
    // the slot was unbound (or bound) must not survive its binding.
    if ((before == nullptr) != (now == nullptr)) {
      return false;
    }
    if (before == nullptr) {
      continue;  // Unbound on both sides: no authority to compare.
    }
    // Distinct group ids always count as a change even if token contents
    // happened to match: ownership moved between groups. Tokens are
    // precomputed at Build, so the comparison is a table lookup per side.
    if (before->group_id_ != now->group_id_ ||
        admitted.AuthorityTokenForSlot(slot) !=
            current->AuthorityTokenForSlot(slot)) {
      return false;
    }
  }
  return true;
}

namespace {

// A pause is an admission barrier, not an authority change: work that already
// registered its GroupInFlight guard must finish so NodeControl can drain to a
// stable replication frontier. A request that merely captured the old state
// but has not registered yet must observe the pause and retry instead.
bool MutationAdmissionUnchanged(const ServingState& admitted,
                                const ServingState* current,
                                std::span<const std::uint16_t> slots) {
  if (&admitted == current) return true;
  if (current == nullptr) return slots.empty();
  for (const std::uint16_t slot : slots) {
    const GroupView* before = admitted.GroupForSlot(slot);
    const GroupView* now = current->GroupForSlot(slot);
    if (before == nullptr || now == nullptr ||
        before->group_id_ != now->group_id_ ||
        before->mutations_paused_ != now->mutations_paused_) {
      return false;
    }
  }
  return true;
}

}  // namespace

AuthorityGuard::AuthorityGuard(TopologyCache& topology) : topology_(topology) {}

std::optional<AuthorityAnchor> AuthorityGuard::LocalPrimaryAnchor(
    const ServingState& state, std::string_view group_id) {
  const GroupView* group = state.FindGroup(group_id);
  if (group == nullptr || state.SelfNodeIndex() == kNoNodeIndex ||
      group->primary_node_index_ != state.SelfNodeIndex() || !group->granted_ ||
      !group->population_ready_ || !group->storage_ready_) {
    return std::nullopt;
  }
  return AuthorityAnchor{
      .group_id_ = group->group_id_,
      .assignment_id_ = group->assignment_id_,
      .group_term_ = group->group_term_,
  };
}

bool AuthorityGuard::LeaseCoversLocked(const ServingState& state,
                                       std::span<const std::uint16_t> slots,
                                       MonotonicTime now) const {
  if (!session_.has_value()) return false;

  // Redis admits only same-slot requests, but keeping this loop general makes
  // the lease proof fail closed if a future caller reaches the seam before
  // applying the cross-slot verdict.
  std::string_view checked_group;
  for (const std::uint16_t slot : slots) {
    const GroupView* group = state.GroupForSlot(slot);
    if (group == nullptr || group->group_id_ == checked_group) continue;
    checked_group = group->group_id_;
    if (group->primary_node_index_ != state.SelfNodeIndex()) continue;
    const auto lease = leases_.find(group->group_id_);
    if (lease == leases_.end() || lease->second.session_ != *session_) {
      return false;
    }
    if (lease->second.deadline_ <= now) {
      if (!lease->second.expiration_recorded_) {
        lease->second.expiration_recorded_ = true;
        RecordClusterControlLeaseExpiration();
      }
      return false;
    }
    const std::optional<AuthorityAnchor> current =
        LocalPrimaryAnchor(state, group->group_id_);
    if (!current.has_value() || lease->second.anchor_ != *current) return false;
  }
  return true;
}

AuthorityAdmission AuthorityGuard::CaptureAndAdmit(const RequestView& request,
                                                   MonotonicTime now) const {
  AuthorityAdmission admission;
  admission.state_ = topology_.Current();
  admission.slots_.assign(request.slots_.begin(), request.slots_.end());
  admission.decision_ = Admit(admission.state_.get(), request);

  if (admission.decision_.kind_ != Decision::Kind::kServe ||
      admission.state_ == nullptr || admission.slots_.empty()) {
    return admission;
  }

  // Only an owner serving its own group consumes Meta authority. Replica
  // READONLY decisions use kServeStaleRead and redirects carry no admission.
  const GroupView* group =
      admission.state_->GroupForSlot(admission.slots_.front());
  if (group == nullptr ||
      group->primary_node_index_ != admission.state_->SelfNodeIndex()) {
    return admission;
  }

  const std::lock_guard lock(mutex_);
  admission.gate_generation_ = generation_;
  admission.lease_checked_ = true;
  if (!LeaseCoversLocked(*admission.state_, admission.slots_, now)) {
    admission.decision_.kind_ = Decision::Kind::kClusterDownUnbound;
  }
  return admission;
}

RecheckResult AuthorityGuard::Recheck(const AuthorityAdmission& admission,
                                      MonotonicTime now) const {
  if (admission.decision_.kind_ != Decision::Kind::kServe &&
      admission.decision_.kind_ != Decision::Kind::kServeStaleRead) {
    return RecheckResult::kReject;
  }
  if (admission.state_ == nullptr) {
    // Loading-allowlisted no-key commands carry no authority and are safe to
    // complete even before the first control snapshot arrives.
    return admission.slots_.empty() ? RecheckResult::kOk
                                    : RecheckResult::kReject;
  }

  if (admission.lease_checked_) {
    const std::lock_guard lock(mutex_);
    if (admission.gate_generation_ != generation_ ||
        !LeaseCoversLocked(*admission.state_, admission.slots_, now)) {
      return RecheckResult::kReject;
    }
  }
  return AuthorityUnchanged(*admission.state_, topology_.Current().get(),
                            admission.slots_)
             ? RecheckResult::kOk
             : RecheckResult::kReject;
}

RecheckResult AuthorityGuard::RecheckAtMutation(
    const AuthorityAdmission& admission, MonotonicTime now) const {
  const RecheckResult result = Recheck(admission, now);
  if (result == RecheckResult::kOk) {
    admission.mutation_started_.store(true, std::memory_order_release);
  } else {
    admission.final_recheck_failed_.store(true, std::memory_order_release);
  }
  return result;
}

RecheckResult AuthorityGuard::RegisterAndRecheck(
    const AuthorityAdmission& admission, std::size_t worker_stripe,
    MonotonicTime now, AuthorityInFlightGuards* guards) const {
  guards->clear();

  // CurrentCachedWithVersion spins through an odd sequence and returns only
  // after observing one completed publication. The snapshot itself is not
  // used here: Recheck is the sole authority comparator, while the sequence
  // brackets registration against a concurrent publisher's drain.
  std::uint64_t unused_version = 0;
  std::uint64_t publication_before = 0;
  const std::shared_ptr<const ServingState> registration_state =
      CurrentCachedWithVersion(topology_, &unused_version, &publication_before);

  const std::shared_ptr<const ServingState>& admitted_state = admission.state();
  if (admitted_state == nullptr) return RecheckResult::kReject;
  for (const std::uint16_t slot : admission.slots()) {
    GroupInFlight* cell = admitted_state->InFlightCellForSlot(slot);
    if (cell == nullptr) continue;
    const bool already_registered = std::any_of(
        guards->begin(), guards->end(),
        [cell](const InFlightGuard& guard) { return guard.cell() == cell; });
    if (!already_registered) guards->emplace_back(*cell, worker_stripe);
  }

  if (topology_.publication_sequence() == publication_before &&
      MutationAdmissionUnchanged(*admitted_state, registration_state.get(),
                                 admission.slots()) &&
      Recheck(admission, now) == RecheckResult::kOk) {
    return RecheckResult::kOk;
  }
  guards->clear();
  return RecheckResult::kReject;
}

absl::Status AuthorityGuard::RenewLease(const SessionIdentity& session,
                                        const AuthorityAnchor& anchor,
                                        MonotonicTime deadline,
                                        MonotonicTime now) {
  if (!session.complete()) {
    return absl::InvalidArgumentError("lease session identity is incomplete");
  }
  if (deadline <= now) {
    return absl::DeadlineExceededError(
        "lease grant expired before authority installation");
  }
  const std::lock_guard lock(mutex_);
  if (!session_.has_value() || *session_ != session) {
    leases_.clear();
    session_ = session;
    ++generation_;
  }

  const auto existing = leases_.find(anchor.group_id_);
  if (existing != leases_.end() && existing->second.session_ == session &&
      existing->second.anchor_ == anchor) {
    if (existing->second.deadline_ <= now) {
      // Extending this object would preserve generation_ and retroactively
      // validate work admitted before expiry. NodeControl must first run the
      // exact expiration cleanup transition, which removes this lease and
      // advances the generation before a later grant can be installed.
      return absl::FailedPreconditionError(
          "expired lease requires cleanup before renewal");
    }
    // Deadline-only renewal is deliberately invisible to already admitted
    // work. Replacing generation here would turn a healthy heartbeat into a
    // spurious write abort.
    existing->second.deadline_ = deadline;
    existing->second.expiration_recorded_ = false;
    return absl::OkStatus();
  }
  leases_.insert_or_assign(anchor.group_id_,
                           Lease{session, anchor, deadline, false});
  ++generation_;
  return absl::OkStatus();
}

bool AuthorityGuard::HasExactLease(const SessionIdentity& session,
                                   const AuthorityAnchor& anchor,
                                   MonotonicTime deadline,
                                   MonotonicTime now) const {
  const std::lock_guard lock(mutex_);
  if (!session_.has_value() || *session_ != session) return false;
  const auto lease = leases_.find(anchor.group_id_);
  return lease != leases_.end() && lease->second.session_ == session &&
         lease->second.anchor_ == anchor &&
         lease->second.deadline_ == deadline && deadline > now;
}

bool AuthorityGuard::ExpireLease(const SessionIdentity& session,
                                 const AuthorityAnchor& anchor,
                                 MonotonicTime deadline, MonotonicTime now) {
  if (now < deadline) return false;
  const std::lock_guard lock(mutex_);
  if (!session_.has_value() || *session_ != session) return false;
  const auto lease = leases_.find(anchor.group_id_);
  if (lease == leases_.end() || lease->second.session_ != session ||
      lease->second.anchor_ != anchor || lease->second.deadline_ != deadline) {
    return false;
  }
  const bool already_recorded = lease->second.expiration_recorded_;
  leases_.erase(lease);
  ++generation_;
  if (!already_recorded) RecordClusterControlLeaseExpiration();
  return true;
}

void AuthorityGuard::InvalidateSession(const SessionIdentity& session) {
  const std::lock_guard lock(mutex_);
  if (!session_.has_value() || *session_ != session) return;
  session_.reset();
  leases_.clear();
  ++generation_;
}

void AuthorityGuard::InvalidateAnchorsChanged(const ServingState* before,
                                              const ServingState& after) {
  const std::lock_guard lock(mutex_);
  bool invalidated = false;
  for (auto it = leases_.begin(); it != leases_.end();) {
    const std::optional<AuthorityAnchor> current =
        LocalPrimaryAnchor(after, it->first);
    if (!current.has_value() || *current != it->second.anchor_) {
      it = leases_.erase(it);
      invalidated = true;
    } else {
      ++it;
    }
  }
  // `before` documents the sequencing contract: callers invoke this before
  // publishing `after`. Existing leases are enough to identify admissions
  // that can be live, so no generation churn is needed without one.
  (void)before;
  if (invalidated) ++generation_;
}

void AuthorityGuard::Fence(const AuthorityAnchor& anchor) {
  const std::lock_guard lock(mutex_);
  const auto lease = leases_.find(anchor.group_id_);
  if (lease == leases_.end()) return;
  leases_.erase(lease);
  ++generation_;
}

void AuthorityGuard::InvalidateLeases() {
  const std::lock_guard lock(mutex_);
  if (leases_.empty()) return;
  leases_.clear();
  ++generation_;
}

void AuthorityGuard::InvalidateAll() {
  const std::lock_guard lock(mutex_);
  session_.reset();
  leases_.clear();
  ++generation_;
}

}  // namespace lavik::cluster
