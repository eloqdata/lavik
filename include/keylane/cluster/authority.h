#pragma once

// AuthorityGuard: the single admission decision point for the
// cluster data plane, plus the owner-side re-check that keeps a stale
// admission from mutating after a fence.
//
// Admit() is a pure function over one committed ServingState — no Redis wire
// concerns, no globals — so the full decision matrix is testable offline.
// Wire mapping (MOVED/CLUSTERDOWN/CROSSSLOT/LOADING text) lives in the Redis
// layer. Internal state (term, grant, fence reason) never crosses into RESP.

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "keylane/cluster/topology.h"

namespace keylane::cluster {

// What the admission gate knows about one command.
struct RequestView {
  // Distinct hash slots of the command's keys. Empty means the command takes
  // no keys OR key extraction failed: both admit locally (readiness still
  // applies), mirroring Redis getNodeByQuery returning myself for zero keys
  // and letting the command produce its own argument error.
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

}  // namespace keylane::cluster
