#pragma once

// Compact volatile status registry published by the Data-control worker.
// This is observational only: lease authorization never reads it back.

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

struct MetaDataControlRuntimeGroup {
  std::string group_id_;
  cluster::control::WireId128 assignment_id_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  cluster::control::WireHash256 manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
};

struct MetaDataControlRuntimeNode {
  std::string node_id_;
  std::string boot_id_;
  cluster::control::WireId128 session_id_{};
  // Authenticated by the Hello handshake and scoped to this exact live
  // session. Source-less population initialization binds its durable intent
  // to this value before any destructive reset is projected to Data.
  MetaReplicationHistoryId replication_history_id_{};
  // Native source layout from the same authenticated Hello as boot/history.
  std::uint32_t replication_flow_count_ = 0;
  std::uint64_t session_generation_ = 0;
  std::uint64_t leadership_generation_ = 0;
  std::uint64_t source_meta_applied_index_ = 0;
  std::uint64_t validated_committed_high_water_ = 0;
  std::uint64_t topology_epoch_ = 0;
  cluster::control::WireHash256 projection_hash_{};
  // Lexicographically sorted by group_id_ in every published snapshot.
  std::vector<MetaDataControlRuntimeGroup> groups_;
  std::optional<cluster::control::HeartbeatHealth> health_;
  std::int64_t health_received_unix_ms_ = 0;
  std::optional<cluster::control::LeaseDecision> last_lease_decision_;
  // Sequence of the heartbeat whose Ack carried last_lease_decision_. Keeping
  // the pair lets causal consumers bind a written Grant to the exact request
  // receive time without introducing a second clock sample.
  std::uint64_t lease_decision_heartbeat_sequence_ = 0;
  std::int64_t lease_decision_written_unix_ms_ = 0;
};

struct MetaDataControlRuntimeSnapshot {
  std::uint64_t leadership_generation_ = 0;
  bool leader_authority_eligible_ = false;
  // Changes on every observed eligibility edge within one leadership
  // generation. A reader that misses false -> true between snapshots can still
  // detect that authority continuity was broken and repeat its warmup.
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  // Lexicographically sorted by node_id_; detector/status readers may use
  // binary lookup without rebuilding an index on every poll.
  std::vector<MetaDataControlRuntimeNode> nodes_;
  // Authenticated/parsed Hellos rejected because cluster-create has not yet
  // committed the corresponding identity. This distinguishes a reconnecting
  // Data process from a declared node that has never contacted this leader.
  std::vector<std::string> unregistered_retries_;
  // Nodes that established an accepted session in this leadership generation.
  // Entries outlive session removal so diagnostics can distinguish a missing
  // session from a node this leader has never observed. The registry is
  // bounded by the protocol's maximum projected node count.
  std::vector<std::string> observed_nodes_;
};

struct MetaDataControlLeadershipState {
  std::uint64_t leadership_generation_ = 0;
  bool leader_authority_eligible_ = false;
  std::uint64_t leader_authority_eligibility_revision_ = 0;
};

class MetaDataControlRuntimeStatus {
 public:
  // Starts a new leader-owned observation epoch. Status capture uses this
  // generation to reject a response assembled across a leadership change and
  // resets its eligibility-continuity revision.
  void BeginLeadership(std::uint64_t leadership_generation);
  // Changes eligibility only for the current generation and returns the
  // effective result. Stale worker notifications and revision exhaustion
  // return false; each accepted edge advances the snapshot's revision so
  // false -> true cannot disappear between detector polls. Becoming
  // ineligible preserves observations so recovery within the same generation
  // can reuse still-current sessions.
  bool SetLeaderAuthorityEligible(std::uint64_t leadership_generation,
                                  bool eligible);
  // Clears observations only when ending the current generation. A delayed
  // demotion for an older generation cannot erase a newer leader's state.
  void EndLeadership(std::uint64_t leadership_generation);
  // Records an authenticated, active-create-declared node id only in the
  // current leadership generation. The caller enforces those predicates and
  // this store independently caps retained evidence; diagnostics never
  // participate in authorization.
  void NoteUnregisteredRetry(std::string node_id,
                             std::uint64_t leadership_generation);
  // Publishes a fully validated Hello/FDS session for the current eligible
  // leader generation. Replacing a node session atomically discards all
  // heartbeat and lease observations belonging to its predecessor.
  void PublishCurrent(std::string node_id, std::string boot_id,
                      const cluster::control::WireId128& session_id,
                      const MetaReplicationHistoryId& replication_history_id,
                      std::uint32_t replication_flow_count,
                      std::uint64_t session_generation,
                      std::uint64_t leadership_generation,
                      std::uint64_t validated_committed_high_water,
                      const cluster::control::FullDesiredState& projection);
  // Advances only the named current session's validated committed high-water;
  // stale sessions are ignored and the value never moves backwards.
  void MarkValidated(std::string_view node_id,
                     const cluster::control::WireId128& session_id,
                     std::uint64_t validated_committed_high_water);
  // Replaces health and its Meta receive time only for the named current
  // session; status freshness is evaluated later from this receive time.
  void RecordHealth(std::string_view node_id,
                    const cluster::control::WireId128& session_id,
                    const cluster::control::HeartbeatHealth& health,
                    std::int64_t received_unix_ms);
  // Records a lease decision only after its Ack was written successfully. The
  // heartbeat sequence is atomically bound to that decision in the same
  // runtime update.
  void RecordLeaseDecisionWritten(
      std::string_view node_id, const cluster::control::WireId128& session_id,
      std::uint64_t heartbeat_sequence,
      const cluster::control::LeaseDecision& written_decision,
      std::int64_t written_unix_ms);
  // Removes only the matching session. A null session_id is a no-op: a
  // rejected handshake never owned a runtime incarnation and must not erase
  // an incumbent. Leadership teardown clears all nodes through EndLeadership.
  void Remove(std::string_view node_id,
              const cluster::control::WireId128* session_id);
  // Copies one mutex-consistent observational snapshot.
  MetaDataControlRuntimeSnapshot Snapshot() const;
  // Copies only the leadership bracket used after off-worker status encoding.
  MetaDataControlLeadershipState LeadershipState() const;

 private:
  mutable std::mutex mutex_;
  std::uint64_t leadership_generation_ = 0;
  bool leader_authority_eligible_ = false;
  // Saturation permanently leaves this generation ineligible instead of
  // allowing the revision to wrap and make an authority interruption
  // invisible.
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  std::map<std::string, MetaDataControlRuntimeNode> nodes_;
  std::map<std::string, std::uint64_t> unregistered_retries_;
  std::set<std::string> observed_nodes_;
};

}  // namespace keylane::meta
