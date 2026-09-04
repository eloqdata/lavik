#include "keylane/cluster/authority.h"

#include <utility>

namespace keylane::cluster {

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

  // No-key commands admit locally; readiness above is the only gate. Callers
  // report key-extraction failure as an empty slot set, so such commands land
  // here as well and produce their own argument error — the same treatment
  // Redis gives zero-key commands in getNodeByQuery.
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
  const NodeDescriptor* self = state->Self();
  if (self != nullptr && self->node_id_ == group->primary_node_id_) {
    // A fenced group has no safe owner: the primary must not serve and there
    // is no other authority to redirect to.
    decision.kind_ = group->granted_ ? Decision::Kind::kServe
                                     : Decision::Kind::kClusterDownUnbound;
    return decision;
  }

  // A READONLY connection on a replica of the owning group serves reads
  // locally; staleness is the client's explicit choice. Writes and
  // non-READONLY reads redirect to the primary.
  if (self != nullptr && !request.is_write_ && request.connection_readonly_) {
    for (const std::string& replica_id : group->replica_node_ids_) {
      if (replica_id == self->node_id_) {
        decision.kind_ = Decision::Kind::kServeStaleRead;
        return decision;
      }
    }
  }

  const NodeDescriptor* primary = state->FindNode(group->primary_node_id_);
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
  decision.moved_host_ = primary->host_;
  decision.moved_port_ = primary->port_;
  decision.moved_tls_port_ = primary->tls_port_;
  return decision;
}

bool AuthorityUnchanged(const ServingState& admitted,
                        const ServingState* current,
                        std::span<const std::uint16_t> slots) {
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

}  // namespace keylane::cluster
