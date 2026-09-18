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

// AuthorityGuard: the single request-path authority boundary for the cluster
// data plane. CaptureAndAdmit records routing plus finite-lease state;
// RegisterAndRecheck closes the publication race and keeps a stale admission
// from overtaking a fence, and RecheckAtMutation enforces the same proof at
// the storage publication cut after intervening suspension.
//
// Admit() and AuthorityUnchanged() remain pure routing/test helpers over
// committed ServingState values; they do not carry a lease generation,
// deadline, or in-flight registration and therefore are not safe substitutes
// for AuthorityGuard on a request path. Wire mapping
// (MOVED/CLUSTERDOWN/CROSSSLOT/LOADING text) lives in the Redis layer.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/cluster/topology.h"

namespace lavik::cluster {

// What the admission gate knows about one command.
struct RequestView {
  // Distinct hash slots of the command's keys. Empty means the command takes
  // no keys OR key extraction failed: both admit locally (readiness still
  // applies), mirroring Redis getNodeByQuery returning myself for zero keys
  // and letting the command produce its own argument error. The Redis adapter
  // rejects persistent zero-key mutations because no group-scoped authority
  // can prove ownership for them.
  std::span<const std::uint16_t> slots_;
  // Includes runtime-only mutations such as PUBLISH that advance the
  // replication frontier even though they do not change the keyspace.
  bool is_write_ = false;
  bool connection_readonly_ = false;  // READONLY issued on this connection
  // Whitelisted during recovery (PING/INFO/CLUSTER/CONFIG/... — the Redis
  // layer mirrors the existing is_loading allowlist verbatim).
  bool loading_allowed_ = false;
};

struct Decision {
  enum class Kind : std::uint8_t {
    kServe,               // execute locally
    kServeStaleRead,      // replica read admitted under READONLY
    kMoved,               // another node owns the slot; endpoint filled below
    kClusterDownUnbound,  // first key's slot has no owner
    kCrossSlot,           // keys span multiple slots
    kLoading,             // no ready ServingState / storage not ready
    kTryAgain,            // controlled failover paused local mutations
    kCloseConnection,     // execution outcome undeterminable (never from Admit)
  };
  Kind kind_ = Kind::kServe;
  std::uint16_t moved_slot_ = 0;
  std::uint16_t moved_port_ = 0;
  std::uint16_t moved_tls_port_ = 0;
  // Only kMoved uses this nonempty address. Borrows the node's host from the
  // admitted ServingState, which must outlive every use of this view.
  std::string_view moved_host_;
};

// Single admission decision, evaluated against one committed snapshot.
// `state` may be null (nothing published yet → kLoading for everything except
// loading_allowed_ commands). Evaluation order mirrors Redis getNodeByQuery:
// loading gate, then first-key unbound (kClusterDownUnbound), then cross-slot
// (kCrossSlot), then ownership (kServe / kServeStaleRead / kMoved). A group
// whose grant is fenced has no safe owner: when self is that fenced primary,
// keyed requests get kClusterDownUnbound. A fenced remote primary still gets
// kMoved — the redirect target applies its own grant gate and answers
// CLUSTERDOWN, so the client never reaches a writable fenced node; the grant
// bit is only consumed by the node holding it.
// The returned Decision borrows its MOVED host from `state`; callers must keep
// that snapshot alive until the address has been consumed.
Decision Admit(const ServingState* state, const RequestView& request);

// Owner-side authority re-check result. The request path first registers its
// in-flight cells and rechecks, holds those guards across execution, then
// rechecks the captured proof once more at the storage mutation cut.
enum class RecheckResult : std::uint8_t {
  kOk,         // authority unchanged; proceed
  kReject,     // nothing executed yet; safe to answer with redirect/error
  kUncertain,  // a revoking change raced an irreversible step; close connection
};

// Compares per-group authority (owner identity, term, grant, readiness —
// ServingState::AuthorityToken) for `slots` between two snapshots. Slot
// coverage changes count as authority changes. `admitted` must not be null;
// `current` may be null (counts as changed for any slotted request).
bool AuthorityUnchanged(const ServingState& admitted,
                        const ServingState* current,
                        std::span<const std::uint16_t> slots);

using MonotonicTime = LeaseTime;
using MonotonicDuration = LeaseDuration;

// Committed source of the installed node projection. Leases and directives
// must name this exact index; lower snapshot indexes cannot roll state back.
// Meta retains this basis when unrelated commits leave desired content equal.
struct ProjectionBasis {
  std::uint64_t control_revision_ = 0;

  friend bool operator==(const ProjectionBasis&,
                         const ProjectionBasis&) = default;
};

// Process-session incarnation. The Data boot identity prevents a frame from a
// previous process boot from acquiring authority after all in-memory floors
// and leases have intentionally disappeared.
struct SessionIdentity {
  SessionId session_id_;
  std::uint64_t generation_ = 0;
  NodeId data_boot_id_;

  bool complete() const noexcept {
    return !session_id_.empty() && generation_ != 0 && !data_boot_id_.empty();
  }
  friend bool operator==(const SessionIdentity&,
                         const SessionIdentity&) = default;
};

// The committed fields that make one group's authority unique. Owner
// identity is resolved through the ServingState's primary node; group id plus
// assignment prevent a removed-and-readded group from inheriting counters.
struct AuthorityAnchor {
  std::string group_id_;
  AssignmentId assignment_id_;
  std::uint64_t group_term_ = 0;

  friend bool operator==(const AuthorityAnchor&,
                         const AuthorityAnchor&) = default;
};

// Captures both the routing verdict and everything needed for the mandatory
// side-effect recheck. Callers do not reconstruct an admission from a bare
// ServingState; that would omit the lease generation and deadline proof.
class AuthorityAdmission {
 public:
  AuthorityAdmission() = default;
  AuthorityAdmission(const AuthorityAdmission&) = delete;
  AuthorityAdmission& operator=(const AuthorityAdmission&) = delete;
  AuthorityAdmission(AuthorityAdmission&& other) noexcept;
  AuthorityAdmission& operator=(AuthorityAdmission&& other) noexcept;

  // state_ owns the borrowed MOVED host. Copying this decision does not retain
  // that ownership; consume its address before releasing or replacing state_.
  const Decision& decision() const noexcept { return decision_; }
  const std::shared_ptr<const ServingState>& state() const noexcept {
    return state_;
  }
  std::span<const std::uint16_t> slots() const noexcept { return slots_; }
  // A final storage check records whether any mutation in this admission has
  // linearized and whether a later one was rejected. Callers use the pair to
  // distinguish a safe fresh redirect from an indeterminate partial result.
  bool mutation_started() const noexcept {
    return mutation_started_.load(std::memory_order_acquire);
  }
  bool final_recheck_failed() const noexcept {
    return final_recheck_failed_.load(std::memory_order_acquire);
  }

 private:
  friend class AuthorityGuard;
  Decision decision_;
  std::shared_ptr<const ServingState> state_;
  absl::InlinedVector<std::uint16_t, 4> slots_;
  std::uint64_t gate_generation_ = 0;
  bool lease_checked_ = false;
  mutable std::atomic<bool> mutation_started_{false};
  mutable std::atomic<bool> final_recheck_failed_{false};
};

// Guards held from the final authority check until the admitted mutation has
// finished. The inline capacity covers the normal single-group Redis command
// without a request-path allocation.
using AuthorityInFlightGuards = absl::InlinedVector<InFlightGuard, 4>;

class NodeControlInstaller;

// Unique request-path authority interface. Serving combines committed topology
// with a session-scoped process-memory lease; topology alone never grants
// authority.
class AuthorityGuard {
 public:
  explicit AuthorityGuard(TopologyCache& topology);
  AuthorityGuard(const AuthorityGuard&) = delete;
  AuthorityGuard& operator=(const AuthorityGuard&) = delete;

  // Captures a coherent serving verdict and lease generation at `now`.
  // Meta-managed local-primary requests fail closed when no exact unexpired
  // lease exists. A mutating request must retain the returned record and pass
  // it through RegisterAndRecheck while holding the resulting guards across
  // execution. Storage writes additionally carry it to RecheckAtMutation at
  // their publication seam.
  AuthorityAdmission CaptureAndAdmit(const RequestView& request,
                                     MonotonicTime now) const;

  // Lower-level verification of topology, session generation, and lease
  // deadline captured at admission. This call does not enter an in-flight cell
  // and is not by itself a safe request-mutation boundary; request paths use
  // RegisterAndRecheck plus RecheckAtMutation. kReject means the caller must
  // not begin a new side effect.
  RecheckResult Recheck(const AuthorityAdmission& admission,
                        MonotonicTime now) const;

  // Final non-suspending check at the storage publication seam. On success it
  // records that this shared admission has begun a mutation; on rejection it
  // records the failure so aggregate commands never return a falsely certain
  // result after an earlier participant already changed state.
  RecheckResult RecheckAtMutation(const AuthorityAdmission& admission,
                                  MonotonicTime now) const;

  // Atomically closes the publication/fence race around the owner-side
  // recheck: enter every distinct admitted group's in-flight cell, then prove
  // that no topology publication crossed the registration and that the
  // captured lease is still valid. On any rejection `guards` is empty; on
  // success the caller must retain it until the mutation finishes.
  RecheckResult RegisterAndRecheck(const AuthorityAdmission& admission,
                                   std::size_t worker_stripe, MonotonicTime now,
                                   AuthorityInFlightGuards* guards) const;

 private:
  struct Lease {
    SessionIdentity session_;
    AuthorityAnchor anchor_;
    MonotonicTime deadline_;
    // Copies of a published lease share one expiration receipt, so concurrent
    // readers and the expiry timer count it once. Renewal installs a fresh
    // receipt; ordinary admission never writes this atomic. Relaxed ordering
    // suffices for metric deduplication; this flag publishes no authority
    // state.
    std::shared_ptr<std::atomic<bool>> expiration_recorded_ =
        std::make_shared<std::atomic<bool>>(false);
  };

  struct AuthorityState {
    std::optional<SessionIdentity> session_;
    absl::flat_hash_map<std::string, Lease> leases_;
    std::uint64_t generation_ = 1;
  };

  static std::optional<AuthorityAnchor> LocalPrimaryAnchor(
      const ServingState& state, std::string_view group_id);
  bool LeaseCovers(const AuthorityState& authority, const ServingState& state,
                   std::span<const std::uint16_t> slots,
                   MonotonicTime now) const;
  // The returned reference is valid until this thread's next CurrentAuthority
  // call. Callers must not suspend while borrowing it.
  const AuthorityState& CurrentAuthority() const;
  // Writer-only: allocate the immutable copy before publishing; no reader
  // drain, request gate closure, or wait for old snapshot owners is needed.
  void PublishAuthorityLocked();
  absl::Status RenewLease(const SessionIdentity& session,
                          const AuthorityAnchor& anchor, MonotonicTime deadline,
                          MonotonicTime now);
  // NodeControl uses this after an awaited dependent activation to prove that
  // the exact lease it installed still exists and remains live.
  bool HasExactLease(const SessionIdentity& session,
                     const AuthorityAnchor& anchor, MonotonicTime deadline,
                     MonotonicTime now) const;
  // Removes only the exact lease instance named by its original deadline.
  // A renewal changes that deadline, so a stale timer cannot revoke the
  // replacement lease. Returns true exactly once for a due lease.
  bool ExpireLease(const SessionIdentity& session,
                   const AuthorityAnchor& anchor, MonotonicTime deadline,
                   MonotonicTime now);
  void InvalidateSession(const SessionIdentity& session);
  void InvalidateAnchorsChanged(const ServingState* before,
                                const ServingState& after);
  void Fence(const AuthorityAnchor& anchor);
  void InvalidateLeases();
  void InvalidateAll();

  TopologyCache& topology_;
  // Only control-plane writers acquire this mutex. Request admission and
  // mutation rechecks read immutable, independently versioned snapshots.
  mutable std::mutex mutex_;
  AuthorityState writer_state_;
  std::atomic<std::shared_ptr<const AuthorityState>> published_authority_;
  std::atomic<std::uint64_t> authority_version_{0};
  const std::uint64_t cache_identity_;

  friend class NodeControlInstaller;
};

}  // namespace lavik::cluster
