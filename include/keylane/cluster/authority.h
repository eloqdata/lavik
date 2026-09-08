#pragma once

// AuthorityGuard: the single admission decision point for the
// cluster data plane, plus the owner-side re-check that keeps a stale
// admission from mutating after a fence.
//
// Admit() is a pure function over one committed ServingState — no Redis wire
// concerns, no globals — so the full decision matrix is testable offline.
// Wire mapping (MOVED/CLUSTERDOWN/CROSSSLOT/LOADING text) lives in the Redis
// layer. Internal state (term, grant, fence reason) never crosses into RESP.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "keylane/cluster/topology.h"

namespace keylane::cluster {

// What the admission gate knows about one command.
struct RequestView {
  // Distinct hash slots of the command's keys. Empty means the command takes
  // no keys OR key extraction failed: both admit locally (readiness still
  // applies), mirroring Redis getNodeByQuery returning myself for zero keys
  // and letting the command produce its own argument error. The Redis adapter
  // separately binds eligible static global mutations to one representative
  // owner slot and rejects every persistent global mutation without that
  // group proof.
  std::span<const std::uint16_t> slots_;
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
    kCloseConnection,     // execution outcome undeterminable (never from Admit)
  };
  Kind kind_ = Kind::kServe;
  std::uint16_t moved_slot_ = 0;
  std::string moved_host_;  // concrete advertised address, never empty
  std::uint16_t moved_port_ = 0;
  std::uint16_t moved_tls_port_ = 0;
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
Decision Admit(const ServingState* state, const RequestView& request);

// Owner-side authority re-check. The admission side captures the snapshot it
// admitted against; immediately before mutation (after every suspending
// admission), the executing worker compares per-group authority tokens for
// the request's slots between the admitted and the current snapshot.
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

using MonotonicTime = std::chrono::steady_clock::time_point;
using MonotonicDuration = std::chrono::steady_clock::duration;

// Node-specific semantic basis of a projected Meta state. The Raft applied
// index orders observations and detects rollback; the SHA-256 projection hash
// is the actual dependency of lease grants and directives, so unrelated Meta
// commits do not revoke valid work.
struct ProjectionBasis {
  std::uint64_t source_meta_applied_index_ = 0;
  Sha256Digest projection_hash_{};

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
  std::uint64_t authority_version_ = 0;
  std::uint64_t grant_revision_ = 0;

  friend bool operator==(const AuthorityAnchor&,
                         const AuthorityAnchor&) = default;
};

// Captures both the routing verdict and everything needed for the mandatory
// side-effect recheck. Callers do not reconstruct an admission from a bare
// ServingState; that would omit the lease generation and deadline proof.
class AuthorityAdmission {
 public:
  const Decision& decision() const noexcept { return decision_; }
  const std::shared_ptr<const ServingState>& state() const noexcept {
    return state_;
  }
  std::span<const std::uint16_t> slots() const noexcept { return slots_; }

 private:
  friend class AuthorityGuard;
  Decision decision_;
  std::shared_ptr<const ServingState> state_;
  absl::InlinedVector<std::uint16_t, 4> slots_;
  std::uint64_t gate_generation_ = 0;
  bool lease_checked_ = false;
};

// Guards held from the final authority check until the admitted mutation has
// finished. The inline capacity covers the normal single-group Redis command
// without a request-path allocation.
using AuthorityInFlightGuards = absl::InlinedVector<InFlightGuard, 4>;

class NodeControlInstaller;

// Unique request-path authority interface. Dynamic mode combines committed
// topology with a process-memory lease; static mode supplies a permanent
// lease while retaining identical topology recheck semantics.
class AuthorityGuard {
 public:
  enum class LeaseMode : std::uint8_t { kFinite, kPermanent };

  AuthorityGuard(TopologyCache& topology, LeaseMode lease_mode);
  AuthorityGuard(const AuthorityGuard&) = delete;
  AuthorityGuard& operator=(const AuthorityGuard&) = delete;

  // Identifies the control model for policy decisions that cannot carry a
  // keyed group proof (for example, process-wide catalog mutations). It does
  // not grant authority by itself; callers must still inspect the committed
  // ServingState and local role.
  LeaseMode lease_mode() const noexcept { return lease_mode_; }

  // Captures a coherent serving verdict and lease generation at `now`.
  // Meta-managed local-primary requests fail closed when no exact unexpired
  // lease exists. The returned record must be passed to Recheck immediately
  // before a side effect.
  AuthorityAdmission CaptureAndAdmit(const RequestView& request,
                                     MonotonicTime now) const;

  // Verifies topology, session generation, and lease deadline captured at
  // admission. kReject means no side effect may begin; the caller alone knows
  // whether an already-started irreversible operation instead requires
  // kUncertain/connection close handling.
  RecheckResult Recheck(const AuthorityAdmission& admission,
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
    // Mutable because admission is logically read-only. It suppresses a hot
    // request stream from counting the same locally observed expiry more than
    // once; a later renewal clears it.
    mutable bool expiration_recorded_ = false;
  };

  static std::optional<AuthorityAnchor> LocalPrimaryAnchor(
      const ServingState& state, std::string_view group_id);
  bool LeaseCoversLocked(const ServingState& state,
                         std::span<const std::uint16_t> slots,
                         MonotonicTime now) const;
  absl::Status RenewLease(const SessionIdentity& session,
                          const AuthorityAnchor& anchor,
                          MonotonicTime deadline);
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
  const LeaseMode lease_mode_;
  mutable std::mutex mutex_;
  std::optional<SessionIdentity> session_;
  std::unordered_map<std::string, Lease> leases_;
  std::uint64_t generation_ = 1;

  friend class NodeControlInstaller;
};

}  // namespace keylane::cluster
