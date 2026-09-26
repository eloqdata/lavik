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

// TLS entries must not survive destruction followed by address reuse.
std::atomic<std::uint64_t> next_authority_cache_identity{1};

// Dataset-scoped access needs one Group rather than client hash-slot routing.
// A representative slot reuses the existing lease/drain/token machinery.
std::span<const std::uint16_t> AuthoritySlots(const ServingState* state,
                                              const RequestView& request) {
  if (request.local_group_) {
    // Resolve from the same immutable snapshot that admission will retain.
    // The catalog belongs to the local dataset, never to an arbitrary slot
    // zero or a remote Group. Existing slot proofs then supply lease/drain.
    const GroupView* local = nullptr;
    if (state != nullptr && state->SelfNodeIndex() != kNoNodeIndex) {
      for (const GroupView& group : state->Groups()) {
        if (group.primary_node_index_ != state->SelfNodeIndex() &&
            std::find(group.replica_node_indices_.begin(),
                      group.replica_node_indices_.end(),
                      state->SelfNodeIndex()) ==
                group.replica_node_indices_.end()) {
          continue;
        }
        if (local != nullptr) {
          local = nullptr;
          break;
        }
        local = &group;
      }
    }
    if (local != nullptr && !local->slot_ranges_.empty()) {
      return {&local->slot_ranges_.front().first_, 1};
    }
    // An unbound sentinel fails through the ordinary admission decision;
    // returning empty would accidentally grant the diagnostic bypass.
    static constexpr std::uint16_t unbound = kSlotCount;
    return {&unbound, 1};
  }
  if (request.client_mode_ != ClientMode::kSingle) return request.slots_;
  if (!request.slots_.empty()) return request.slots_.first(1);
  // Keyless data still belongs to the one Group; only diagnostics may bypass
  // authority. This also closes the empty-key-set escape hatch for new paths.
  static constexpr std::uint16_t representative = 0;
  return request.loading_allowed_
             ? request.slots_
             : std::span<const std::uint16_t>(&representative, 1);
}

bool HasSingleFullGroup(const ServingState* state) {
  return state != nullptr && state->Groups().size() == 1 &&
         state->CoverageComplete();
}

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
      lease_(std::move(other.lease_)),
      lease_checked_(other.lease_checked_),
      single_group_(other.single_group_),
      topology_sequence_(other.topology_sequence_),
      authority_version_(other.authority_version_),
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
  lease_ = std::move(other.lease_);
  lease_checked_ = other.lease_checked_;
  single_group_ = other.single_group_;
  topology_sequence_ = other.topology_sequence_;
  authority_version_ = other.authority_version_;
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
  const auto slots = AuthoritySlots(state, request);

  // Loading gate, first in Redis's order too (processCommand runs its loading
  // check before getNodeByQuery): loading, then first-key unbound, then
  // cross-slot, then ownership. No-key commands consult the aggregate; keyed
  // commands only their involved groups. Allowlisted commands serve
  // unconditionally so clients can probe health and discovery while storage
  // recovers — the Redis layer mirrors the existing is_loading allowlist.
  const bool ready =
      state != nullptr && (slots.empty() ? state->FullyReady()
                                         : InvolvedGroupsReady(*state, slots));
  if (!ready) {
    decision.kind_ = request.loading_allowed_ ? Decision::Kind::kServe
                                              : Decision::Kind::kLoading;
    return decision;
  }

  // No-key commands admit locally; readiness above is the only generic gate.
  // The Redis adapter rejects unscoped persistent mutations before this point,
  // because an empty slot set cannot name authority or a drain cell. Callers
  // also report key-extraction failure as empty so malformed commands reach
  // their own argument error, matching Redis getNodeByQuery.
  if (slots.empty()) {
    decision.kind_ = Decision::Kind::kServe;
    return decision;
  }

  if (request.client_mode_ == ClientMode::kSingle &&
      !HasSingleFullGroup(state)) {
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
    return decision;
  }

  // First-key unbound outranks cross-slot (Redis checks coverage before the
  // single-slot rule): [unbound-slot key, other-slot key] yields CLUSTERDOWN,
  // not CROSSSLOT.
  const std::uint16_t slot = slots.front();
  const GroupView* group = state->GroupForSlot(slot);
  if (group == nullptr) {
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
    return decision;
  }
  for (const std::uint16_t other : slots.subspan(1)) {
    if (other != slot) {
      decision.kind_ = Decision::Kind::kCrossSlot;
      return decision;
    }
  }

  // One authority Group from here on; the loading gate already established
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

  // Local dataset requests and Single replicas admit ordinary reads and
  // reject writes with READONLY.
  // Slot-routed Cluster reads require a READONLY connection; other accesses
  // redirect to the primary. Population/link policy is checked by replication.
  if (self_index != kNoNodeIndex &&
      (request.local_group_ || request.client_mode_ == ClientMode::kSingle ||
       (!request.is_write_ && request.connection_readonly_))) {
    for (NodeIndex replica_index : group->replica_node_indices_) {
      if (replica_index == self_index) {
        decision.kind_ = request.is_write_ ? Decision::Kind::kReadOnly
                                           : Decision::Kind::kServeStaleRead;
        return decision;
      }
    }
  }

  if (request.client_mode_ == ClientMode::kSingle) {
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
    return decision;
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

AuthorityGuard::AuthorityGuard(TopologyCache& topology,
                               RetirementCallback retirement_callback)
    : retirement_callback_(retirement_callback),
      topology_(topology),
      published_authority_(std::make_shared<const AuthorityState>()),
      cache_identity_(next_authority_cache_identity.fetch_add(
          1, std::memory_order_relaxed)) {}

const AuthorityGuard::AuthorityState& AuthorityGuard::CurrentAuthority(
    std::uint64_t* publication_version) const {
  struct Cached {
    std::uint64_t identity = 0;
    std::uint64_t version = 0;
    std::shared_ptr<const AuthorityState> state;
  };
  thread_local Cached cached;
  std::uint64_t version = authority_version_.load(std::memory_order_acquire);
  if (cached.identity == cache_identity_ && cached.version == version) {
    if (publication_version != nullptr) *publication_version = cached.version;
    return *cached.state;
  }
  for (;;) {
    auto state = published_authority_.load(std::memory_order_acquire);
    const std::uint64_t after =
        authority_version_.load(std::memory_order_acquire);
    if (version == after) {
      cached.identity = cache_identity_;
      cached.version = version;
      cached.state = std::move(state);
      if (publication_version != nullptr) *publication_version = version;
      return *cached.state;
    }
    version = after;
  }
}

void AuthorityGuard::PublishAuthorityLocked() {
  bool retired = false;
  if (retirement_callback_ != nullptr) {
    const auto previous = published_authority_.load(std::memory_order_acquire);
    for (const auto& [group, lease] : previous->leases_) {
      const auto next = writer_state_.leases_.find(group);
      if (next == writer_state_.leases_.end() ||
          next->second.session_ != lease.session_ ||
          next->second.anchor_ != lease.anchor_) {
        retired = true;
        break;
      }
    }
  }
  auto state = std::make_shared<const AuthorityState>(writer_state_);
  published_authority_.store(std::move(state), std::memory_order_release);
  // This version is only a cache invalidation hint, not part of the lease
  // proof. A refresh racing the store may see the next complete snapshot
  // before this increment; that is safe and the increment refreshes it again.
  // Once publication returns, acquire-version readers cannot reuse old state.
  authority_version_.fetch_add(1, std::memory_order_release);
  // Close request authority before asking socket-owning workers to clean up.
  // Each worker sweeps its current clients, including recent reconnects.
  if (retired) retirement_callback_();
}

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

bool AuthorityGuard::LeaseCovers(
    const AuthorityState& authority, const ServingState& state,
    std::span<const std::uint16_t> slots, MonotonicTime now,
    std::shared_ptr<LeaseDeadline>* single_lease) const {
  if (!authority.session_.has_value()) return false;
  if (single_lease != nullptr) single_lease->reset();
  bool multiple_leases = false;

  // Single carries one representative slot for its full-keyspace Group.
  // Cluster admission enforces same-slot requests; keep this helper general
  // so callers cannot accidentally omit a Group from the lease proof.
  std::string_view checked_group;
  for (const std::uint16_t slot : slots) {
    const GroupView* group = state.GroupForSlot(slot);
    if (group == nullptr || group->group_id_ == checked_group) continue;
    checked_group = group->group_id_;
    if (group->primary_node_index_ != state.SelfNodeIndex()) continue;
    const auto lease = authority.leases_.find(group->group_id_);
    if (lease == authority.leases_.end() ||
        lease->second.session_ != *authority.session_) {
      return false;
    }
    if (!lease->second.deadline_->valid_at(now.time_since_epoch())) {
      if (lease->second.deadline_->deadline() ==
          std::chrono::nanoseconds::zero())
        return false;
      // Request admission/rechecks can discover expiry on several workers
      // before control-plane cleanup runs. An atomic metric increment alone
      // would count each observer, not each lease expiry. Claim the shared
      // receipt atomically; separate load/store operations could both win.
      if (!lease->second.expiration_recorded_->exchange(
              true, std::memory_order_relaxed)) {
        RecordClusterControlLeaseExpiration();
      }
      return false;
    }
    // The slot lookup already resolved this Group and the lease lookup used
    // its id. Compare the remaining anchor fields in place: constructing an
    // owning AuthorityAnchor here repeats the Group lookup and string copies
    // at every admission and mutation recheck.
    if (state.SelfNodeIndex() == kNoNodeIndex || !group->granted_ ||
        !GroupReady(*group) ||
        lease->second.anchor_.assignment_id_ != group->assignment_id_ ||
        lease->second.anchor_.group_term_ != group->group_term_) {
      return false;
    }
    if (single_lease != nullptr && !multiple_leases) {
      // The fast path is valid for one capability only. A future caller that
      // spans distinct local Groups must recheck every lease in the slow path.
      if (*single_lease == nullptr ||
          *single_lease == lease->second.deadline_) {
        // Admissions copy this worker's control block. Keep one global owner
        // only when the capability changes, just like the topology cache;
        // atomic deadline renewal does not refresh ownership or allocate.
        thread_local std::shared_ptr<LeaseDeadline> local_lease;
        if (local_lease.get() != lease->second.deadline_.get()) {
          auto owner = std::make_shared<std::shared_ptr<LeaseDeadline>>(
              lease->second.deadline_);
          local_lease = std::shared_ptr<LeaseDeadline>(
              std::move(owner), lease->second.deadline_.get());
        }
        *single_lease = local_lease;
      } else {
        single_lease->reset();
        multiple_leases = true;
      }
    }
  }
  return true;
}

AuthorityAdmission AuthorityGuard::CaptureAndAdmit(const RequestView& request,
                                                   MonotonicTime now) const {
  AuthorityAdmission admission;
  std::uint64_t version = 0;
  admission.state_ = CurrentCachedWithVersion(topology_, &version,
                                              &admission.topology_sequence_);
  const auto slots = AuthoritySlots(admission.state_.get(), request);
  admission.slots_.assign(slots.begin(), slots.end());
  admission.single_group_ = request.client_mode_ == ClientMode::kSingle;
  admission.decision_ =
      DecideWithLease(admission.state_.get(), request, now, &admission);
  return admission;
}

Decision AuthorityGuard::DecideNow(const RequestView& request,
                                   std::optional<MonotonicTime> now) const {
  // Single's complete Group authorizes every read through the same predicate.
  // Keep only successful verdicts in worker-local memory; exact publication
  // identity and lease expiry remain checked on every call. This avoids both
  // repeated Group/lease lookups and per-request ownership, without adding a
  // second authority, shared atomics, or a command-specific permission rule.
  const bool single_read =
      request.client_mode_ == ClientMode::kSingle && !request.is_write_ &&
      !request.loading_allowed_ &&
      (request.slots_.empty() || request.slots_.front() < kSlotCount);
  struct CachedRead {
    std::uint64_t identity = 0;
    std::uint64_t sequence = 0;
    Decision::Kind kind = Decision::Kind::kLoading;
    LeaseCheck lease;
  };
  thread_local CachedRead cached;
  std::uint64_t sequence = topology_.publication_sequence();
  // A successful verdict needs neither borrowed snapshot when their exact
  // publication identities are unchanged. An odd topology sequence always
  // takes the coherent slow path. The authority version is captured together
  // with the immutable lease identity, not sampled after its validation.
  // The shared capability itself is checked even when no snapshot changed:
  // ordinary renewal, shortening and revocation update its atomic deadline.
  if (single_read && (sequence & 1U) == 0 &&
      cached.identity == cache_identity_ && cached.sequence == sequence &&
      (cached.kind == Decision::Kind::kServeStaleRead ||
       (cached.lease.publication_version ==
            authority_version_.load(std::memory_order_acquire) &&
        cached.lease.lease != nullptr &&
        cached.lease.lease->valid_at(
            (now.has_value() ? *now : LeaseClockNow()).time_since_epoch())))) {
    Decision decision;
    decision.kind_ = cached.kind;
    return decision;
  }
  std::uint64_t version = 0;
  const auto& state = CurrentCachedWithVersion(topology_, &version, &sequence);
  const MonotonicTime checked_at = now.has_value() ? *now : LeaseClockNow();
  if (!single_read) {
    return DecideWithLease(state.get(), request, checked_at, nullptr);
  }
  LeaseCheck lease;
  const Decision decision =
      DecideWithLease(state.get(), request, checked_at, nullptr, &lease);
  if (decision.kind_ == Decision::Kind::kServe ||
      decision.kind_ == Decision::Kind::kServeStaleRead) {
    cached = {.identity = cache_identity_,
              .sequence = sequence,
              .kind = decision.kind_,
              .lease = lease};
  } else {
    cached.identity = 0;
  }
  return decision;
}

Decision AuthorityGuard::DecideWithLease(const ServingState* state,
                                         const RequestView& request,
                                         MonotonicTime now,
                                         AuthorityAdmission* proof,
                                         LeaseCheck* lease_check) const {
  Decision decision = Admit(state, request);
  const auto slots = AuthoritySlots(state, request);
  if (decision.kind_ != Decision::Kind::kServe || state == nullptr ||
      slots.empty()) {
    return decision;
  }

  // Only an Owner consumes Meta authority. A synchronous read need not retain
  // shared ownership or construct the write proof, but uses exactly the same
  // lease/session checks as an admission that survives storage preparation.
  const GroupView* group = state->GroupForSlot(slots.front());
  if (group == nullptr ||
      group->primary_node_index_ != state->SelfNodeIndex()) {
    return decision;
  }
  std::uint64_t proof_version = 0;
  const AuthorityState& authority = CurrentAuthority(
      lease_check != nullptr ? &lease_check->publication_version
      : proof != nullptr     ? &proof_version
                             : nullptr);
  if (proof != nullptr) {
    proof->authority_version_ = proof_version;
    proof->lease_checked_ = true;
  }
  auto* lease = proof != nullptr         ? &proof->lease_
                : lease_check != nullptr ? &lease_check->lease
                                         : nullptr;
  if (!LeaseCovers(authority, *state, slots, now, lease)) {
    decision.kind_ = Decision::Kind::kClusterDownUnbound;
  }
  return decision;
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
    // An unchanged publication preserves the capability's identity, not its
    // validity. The deadline may be shortened or revoked without publication.
    // On expiry, use LeaseCovers to claim the one-time expiration metric.
    if (admission.authority_version_ !=
            authority_version_.load(std::memory_order_acquire) ||
        admission.lease_ == nullptr ||
        !admission.lease_->valid_at(now.time_since_epoch())) {
      const AuthorityState& authority = CurrentAuthority();
      // A replacement live lease may cover the same committed authority.
      // Local expiry or session loss alone does not permanently invalidate
      // this admission; the topology check below still fences changed Terms,
      // Owners and assignments. Never reuse the expired capability itself.
      if (!LeaseCovers(authority, *admission.state_, admission.slots_, now)) {
        return RecheckResult::kReject;
      }
    }
  }
  const std::uint64_t sequence = topology_.publication_sequence();
  if ((sequence & 1U) == 0 && sequence == admission.topology_sequence_) {
    // No publication since admission: the current snapshot is the admitted
    // one, so the stale-read pointer, single-group and token comparisons
    // would all trivially pass. Skip the thread-local snapshot cache walk.
    return RecheckResult::kOk;
  }
  std::uint64_t version = 0;
  // This synchronous comparison cannot suspend or refresh this thread's cache
  // again. Borrow its pinned snapshot to avoid unnecessary atomic shared
  // ownership updates on every mutation.
  const auto& current = CurrentCachedWithVersion(topology_, &version);
  // Owner tokens deliberately exclude replica membership. A retained replica
  // read is reusable only on its exact snapshot; a fresh admission must prove
  // membership again after publication, even if the Owner/term did not change.
  if (admission.decision_.kind_ == Decision::Kind::kServeStaleRead &&
      current.get() != admission.state_.get()) {
    return RecheckResult::kReject;
  }
  // A representative slot proves the whole dataset only while its one-Group
  // topology remains intact, including across a publication before mutation.
  if (admission.single_group_ && !admission.slots_.empty() &&
      !HasSingleFullGroup(current.get())) {
    return RecheckResult::kReject;
  }
  return AuthorityUnchanged(*admission.state_, current.get(), admission.slots_)
             ? RecheckResult::kOk
             : RecheckResult::kReject;
}

RecheckResult AuthorityGuard::RecheckForExecution(
    const AuthorityAdmission& admission, MonotonicTime now) const {
  const RecheckResult result = Recheck(admission, now);
  if (result != RecheckResult::kOk) {
    admission.final_recheck_failed_.store(true, std::memory_order_release);
  }
  return result;
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

  const std::shared_ptr<const ServingState>& admitted_state = admission.state();
  if (admitted_state == nullptr) return RecheckResult::kReject;

  // Fast path: the admitted topology is still the current publication. Its
  // pointer identity makes the mutation-admission comparison below trivial,
  // and the sequence alone brackets registration against a concurrent
  // publisher's drain. An odd or advanced sequence falls back to borrowing a
  // coherent snapshot: CurrentCachedWithVersion spins through an odd sequence
  // and returns only after observing one completed publication.
  std::uint64_t unused_version = 0;
  std::uint64_t publication_before = topology_.publication_sequence();
  const ServingState* registration_state = nullptr;
  if ((publication_before & 1U) == 0 &&
      publication_before == admission.topology_sequence_) {
    registration_state = admitted_state.get();
  } else {
    // The snapshot itself is not used beyond the comparison: Recheck is the
    // sole authority comparator, while the sequence brackets registration
    // against a concurrent publisher's drain. Registration cannot suspend.
    // This cache entry is consumed before Recheck can refresh it, so
    // retaining another shared owner would only add atomic reference-count
    // traffic to every write.
    registration_state = CurrentCachedWithVersion(topology_, &unused_version,
                                                  &publication_before)
                             .get();
  }

  for (const std::uint16_t slot : admission.slots()) {
    GroupInFlight* cell = admitted_state->InFlightCellForSlot(slot);
    if (cell == nullptr) continue;
    const bool already_registered = std::any_of(
        guards->begin(), guards->end(),
        [cell](const InFlightGuard& guard) { return guard.cell() == cell; });
    if (!already_registered) guards->emplace_back(*cell, worker_stripe);
  }

  if (topology_.publication_sequence() == publication_before &&
      MutationAdmissionUnchanged(*admitted_state, registration_state,
                                 admission.slots()) &&
      Recheck(admission, now) == RecheckResult::kOk) {
    return RecheckResult::kOk;
  }
  guards->clear();
  return RecheckResult::kReject;
}

absl::Status AuthorityGuard::RenewLease(
    const SessionIdentity& session, const AuthorityAnchor& anchor,
    MonotonicTime deadline, MonotonicTime now,
    std::shared_ptr<LeaseDeadline> shared_lease) {
  if (!session.complete()) {
    return absl::InvalidArgumentError("lease session identity is incomplete");
  }
  if (deadline <= now) {
    return absl::DeadlineExceededError(
        "lease grant expired before authority installation");
  }
  const std::lock_guard lock(mutex_);
  if (!writer_state_.session_.has_value() ||
      *writer_state_.session_ != session) {
    for (const auto& [id, lease] : writer_state_.leases_)
      lease.deadline_->Revoke();
    writer_state_.leases_.clear();
    writer_state_.session_ = session;
  }

  const auto existing = writer_state_.leases_.find(anchor.group_id_);
  if (existing != writer_state_.leases_.end() &&
      existing->second.session_ == session &&
      existing->second.anchor_ == anchor) {
    if (!existing->second.deadline_->valid_at(now.time_since_epoch())) {
      // Expired capabilities are terminal. NodeControl must close admission,
      // retire clients and finish the exact expiration drain before it can
      // install a replacement lease, even for the same committed Term.
      return absl::FailedPreconditionError(
          "expired lease requires cleanup before renewal");
    }
    // Healthy renewal preserves admitted work; readers of a replaced
    // capability fall back to the current lease for the same authority.
    if (shared_lease != nullptr) {
      existing->second.deadline_->Revoke();
      existing->second.deadline_ = std::move(shared_lease);
      PublishAuthorityLocked();
    } else if (!existing->second.deadline_->Renew(
                   now.time_since_epoch(), deadline.time_since_epoch())) {
      return absl::FailedPreconditionError("lease was revoked during renewal");
    }
    return absl::OkStatus();
  }
  if (existing != writer_state_.leases_.end())
    existing->second.deadline_->Revoke();
  if (shared_lease == nullptr) {
    shared_lease = std::make_shared<LeaseDeadline>(deadline.time_since_epoch());
  }
  writer_state_.leases_.insert_or_assign(
      anchor.group_id_, Lease{session, anchor, std::move(shared_lease)});
  PublishAuthorityLocked();
  return absl::OkStatus();
}

bool AuthorityGuard::HasExactLease(const SessionIdentity& session,
                                   const AuthorityAnchor& anchor,
                                   MonotonicTime deadline,
                                   MonotonicTime now) const {
  const AuthorityState& authority = CurrentAuthority();
  if (!authority.session_.has_value() || *authority.session_ != session)
    return false;
  const auto lease = authority.leases_.find(anchor.group_id_);
  // deadline() preserves the cut after observed expiry. A caller with an
  // earlier clock sample must still reject that terminal capability state.
  return lease != authority.leases_.end() &&
         lease->second.session_ == session && lease->second.anchor_ == anchor &&
         lease->second.deadline_->deadline() == deadline.time_since_epoch() &&
         lease->second.deadline_->valid_at(now.time_since_epoch());
}

bool AuthorityGuard::ExpireLease(const SessionIdentity& session,
                                 const AuthorityAnchor& anchor,
                                 MonotonicTime deadline, MonotonicTime now) {
  if (now < deadline) return false;
  const std::lock_guard lock(mutex_);
  if (!writer_state_.session_.has_value() || *writer_state_.session_ != session)
    return false;
  const auto lease = writer_state_.leases_.find(anchor.group_id_);
  if (lease == writer_state_.leases_.end() ||
      lease->second.session_ != session || lease->second.anchor_ != anchor ||
      lease->second.deadline_->deadline() != deadline.time_since_epoch()) {
    return false;
  }
  // Called by the expiry timer, or by lease-grant handling that first cleans
  // up an overdue lease. mutex_ serializes writers but does not exclude
  // snapshot readers in LeaseCovers(), so this cleanup must claim the same
  // receipt before incrementing the metric. An unconditional increment would
  // double-count an expiry already observed by a reader.
  const bool already_recorded = lease->second.expiration_recorded_->exchange(
      true, std::memory_order_relaxed);
  lease->second.deadline_->Revoke();
  writer_state_.leases_.erase(lease);
  PublishAuthorityLocked();
  if (!already_recorded) RecordClusterControlLeaseExpiration();
  return true;
}

void AuthorityGuard::InvalidateSession(const SessionIdentity& session) {
  const std::lock_guard lock(mutex_);
  if (!writer_state_.session_.has_value() || *writer_state_.session_ != session)
    return;
  writer_state_.session_.reset();
  for (const auto& [id, lease] : writer_state_.leases_)
    lease.deadline_->Revoke();
  writer_state_.leases_.clear();
  PublishAuthorityLocked();
}

void AuthorityGuard::InvalidateAnchorsChanged(const ServingState* before,
                                              const ServingState& after) {
  const std::lock_guard lock(mutex_);
  bool invalidated = false;
  for (auto it = writer_state_.leases_.begin();
       it != writer_state_.leases_.end();) {
    const std::optional<AuthorityAnchor> current =
        LocalPrimaryAnchor(after, it->first);
    if (!current.has_value() || *current != it->second.anchor_) {
      it->second.deadline_->Revoke();
      writer_state_.leases_.erase(it++);
      invalidated = true;
    } else {
      ++it;
    }
  }
  // `before` documents the sequencing contract: callers invoke this before
  // publishing `after`. Existing leases are enough to identify admissions
  // that can be live, so no authority publication is needed without one.
  (void)before;
  if (invalidated) {
    PublishAuthorityLocked();
  }
}

void AuthorityGuard::Fence(const AuthorityAnchor& anchor) {
  const std::lock_guard lock(mutex_);
  const auto lease = writer_state_.leases_.find(anchor.group_id_);
  if (lease == writer_state_.leases_.end()) return;
  lease->second.deadline_->Revoke();
  writer_state_.leases_.erase(lease);
  PublishAuthorityLocked();
}

void AuthorityGuard::InvalidateLeases() {
  const std::lock_guard lock(mutex_);
  if (writer_state_.leases_.empty()) return;
  for (const auto& [id, lease] : writer_state_.leases_)
    lease.deadline_->Revoke();
  writer_state_.leases_.clear();
  PublishAuthorityLocked();
}

void AuthorityGuard::InvalidateAll() {
  const std::lock_guard lock(mutex_);
  writer_state_.session_.reset();
  for (const auto& [id, lease] : writer_state_.leases_)
    lease.deadline_->Revoke();
  writer_state_.leases_.clear();
  PublishAuthorityLocked();
}

}  // namespace lavik::cluster
