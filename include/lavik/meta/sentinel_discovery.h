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

// Pure mapping from Meta authority snapshots to Redis Sentinel discovery
// answers. This layer owns no sockets, no locks, and no timers: every decision
// is a function of one MetaDiscoveryCut, so the complete publication matrix is
// unit-testable without a runtime. The Sentinel server supplies the cut from
// the committed state machine projection plus the two thread-safe observation
// registries; it never reads MetaObservationStore here.
//
// Core invariants:
//   - Publication is gated on committed state only: lifecycle Created, client
//     mode Single, exactly one committed Group, an active authority grant, and
//     a non-retired Owner with a publishable plaintext client endpoint.
//     Observation health never gates publication; it only sets flags.
//   - o_down is never emitted. Objective withdrawal of a Primary is expressed
//     by retracting publication (null address / omitted entry), because a
//     fenced or replaced Owner must never be announced as a master again.
//   - Only tcp:// (or legacy untagged) numeric client endpoints with a nonzero
//     port and a non-wildcard host are published. TLS-only deployments yield
//     null until TLS discovery is designed; Admin/Raft/Data-control endpoints
//     never appear here.
//   - Sentinel flags stay parseable by stock redis-py 8.1.0 / go-redis: the
//     tokens master/slave/s_down/disconnected/master_down in the same
//     comma-separated order real Redis 7.2 uses (s_down first, then role).

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "lavik/meta/automatic_failover_detector.h"
#include "lavik/meta/committed_status_view.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/resp.h"

namespace lavik::meta {

// One consistent read of every input a discovery answer may use. `committed_`
// is the compact committed projection; `runtime_` and `diagnostics_` are the
// volatile leader-local registries; `observation_grace_active_` marks the
// post-election window in which a node this leader has never observed is
// unverified rather than down: absence of observation is not failure evidence.
// The Owner keeps its publication and flags then (committed state alone gates
// the Primary), while an unverified member stays out of the replica listing so
// read pools cannot select it.
struct MetaDiscoveryCut {
  MetaCommittedStatusView committed_;
  MetaDataControlRuntimeSnapshot runtime_;
  MetaAutomaticFailoverDiagnosticsSnapshot diagnostics_;
  std::int64_t now_unix_ms_ = 0;
  std::uint32_t observation_ttl_ms_ = 0;
  bool observation_grace_active_ = false;
  std::int64_t raft_term_ = -1;
  std::uint32_t local_meta_id_ = 0;
  // Raft's effective committed configuration excludes staged identity binds.
  std::vector<std::uint32_t> effective_meta_ids_;
};

// A Primary that passed every committed-state publication gate.
struct MetaDiscoveryPrimary {
  std::string group_id_;
  std::string owner_node_id_;  // also the published runid
  NumericEndpoint endpoint_;   // plaintext client endpoint
  std::uint64_t group_term_ = 0;
  // Committed non-retired non-owner member count, independent of health.
  std::size_t replica_count_ = 0;
  std::size_t other_sentinel_count_ = 0;
};

// Registered peers, not a liveness claim. Both SENTINELS and MASTER counts
// use this set, excluding the responder and identities not yet in Raft config.
std::vector<MetaMemberRecord> DiscoverySentinels(const MetaDiscoveryCut& cut);
void EncodeDiscoverySentinelsReply(ReplyBuilder& reply,
                                   const MetaDiscoveryCut& cut,
                                   std::string_view name);

struct MetaDiscoveryMasterFlags {
  bool s_down_ = false;
  bool disconnected_ = false;
};

struct MetaDiscoveryReplica {
  std::string node_id_;
  NumericEndpoint endpoint_;
  bool s_down_ = false;
  bool disconnected_ = false;
  // True when the service currently has no publishable Primary.
  bool master_down_ = false;
};

struct MetaDiscoveryEvent {
  std::string channel_;
  std::string payload_;
};

// Proof of actual source adoption, independent of replica read admission.
bool ReplicaReconfigurationComplete(const MetaDiscoveryCut& cut,
                                    const MetaCommittedStatusGroup& group,
                                    const MetaGroupMember& member);

// Worker-owned, volatile transition observer. Reset at any lost continuity;
// its first cut is a baseline, never a reconstruction of missed history.
class MetaDiscoveryEvents {
 public:
  std::vector<MetaDiscoveryEvent> Observe(const MetaDiscoveryCut& cut);
  void Reset();

 private:
  struct ReplicaState {
    std::string boot_;
    MetaAssignmentId assignment_{};
    bool pending_ = false;
    bool completed_ = false;
  };
  std::optional<MetaDiscoveryPrimary> primary_;
  std::uint64_t term_ = 0;
  std::map<std::string, ReplicaState> replicas_;
};

// The committed state declares at most one discoverable service: the single
// Group of a Created Single-mode cluster, and only while that Group owns the
// complete 0..16383 slot space. Non-Single clusters, any multi/zero-Group
// shape, and a partial slot assignment deliberately expose no service —
// clients of such a deployment have no Sentinel contract here;
// the coverage gate restates a committed invariant so publication stays
// locally auditable.
const MetaCommittedStatusGroup* DiscoveryServiceGroup(
    const MetaDiscoveryCut& cut);

// Applies the committed publication gates to one service Group. `group` must
// come from DiscoveryServiceGroup so the lifecycle/mode/unique-Group conditions
// hold. Fenced authority, an owner that is not a current member, a retired
// owner, or an owner without a publishable plaintext endpoint all retract
// publication instead of emitting a down-marked master.
std::optional<MetaDiscoveryPrimary> PublishablePrimary(
    const MetaDiscoveryCut& cut, const MetaCommittedStatusGroup& group);

// Master flags from observation truth only. s_down adopts the Automatic
// Failover Detector's kSuspect/kTriggering classification, and only when the
// diagnostics snapshot proves it describes this exact cut: leadership
// generation and eligibility revision must equal the runtime snapshot's (a
// leadership edge must not splice stale diagnostics into a new epoch), and the
// status anchor's owner/term must equal the committed grant. Any mismatch reads
// as "no diagnostics" and never raises s_down. disconnected means the owner
// has no live Data-control session, with the observation-grace rule applied to
// never-observed nodes.
MetaDiscoveryMasterFlags MasterFlags(const MetaDiscoveryCut& cut,
                                     const MetaDiscoveryPrimary& primary);

// Truthful replica listing: every committed non-owner member with an active
// identity record and a publishable endpoint. Group membership's role_ field
// is a creation-time hint that failover does not rewrite, so the committed
// owner identity (record_.owner_) is the sole role authority and is excluded.
// A replica reads as not s_down only when all three hold: a live session, an
// all-green heartbeat health report received within the observation TTL, and a
// projected anchor (group term, the member's own assignment, manifest
// revision/digest, partition replication epoch) equal to the committed anchor
// — the runtime registry's per-group anchors describe the last projection the
// node acknowledged applying, never a self-report. Members with an
// unpublishable endpoint are omitted rather than announced with a fabricated
// address. So are members the current leader has never observed while its
// post-election grace window is open: their state is unverified rather than
// failed, and client read pools must not select an unproven node merely
// because its entry lacks down flags. Once the window closes, that same
// absence is evidence and the member lists as s_down,disconnected.
std::vector<MetaDiscoveryReplica> ListReplicas(
    const MetaDiscoveryCut& cut, const MetaCommittedStatusGroup& group,
    bool primary_publishable);

// Verbatim Redis 7.2 Sentinel error for name-addressed discovery verbs on an
// unknown (or currently masterless, for MASTER) service.
inline constexpr std::string_view kDiscoveryNoSuchMasterError =
    "ERR No such master with that name";

// Reply encoders. RESP2/RESP3 shape selection is ReplyBuilder's: map headers
// degrade to flat key/value arrays under RESP2, and the null address reply is
// *-1 under RESP2 and _ under RESP3, matching real Redis 7.2.
void EncodeDiscoveryAddressReply(ReplyBuilder& reply,
                                 const MetaDiscoveryCut& cut,
                                 std::string_view name);
void EncodeDiscoveryMastersReply(ReplyBuilder& reply,
                                 const MetaDiscoveryCut& cut);
void EncodeDiscoveryMasterReply(ReplyBuilder& reply,
                                const MetaDiscoveryCut& cut,
                                std::string_view name);
void EncodeDiscoveryReplicasReply(ReplyBuilder& reply,
                                  const MetaDiscoveryCut& cut,
                                  std::string_view name);

}  // namespace lavik::meta
