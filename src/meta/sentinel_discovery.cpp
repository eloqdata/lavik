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

#include "lavik/meta/sentinel_discovery.h"

#include <algorithm>
#include <string>
#include <utility>

namespace lavik::meta {
namespace {

// Selects the one client endpoint a Sentinel answer may publish. Only the
// plaintext tcp:// (or legacy untagged) address qualifies: TLS discovery is
// not part of this surface, and a wildcard bind is a listen address, never a
// routable client address. ParseNumericEndpoint already rejects non-numeric
// hosts and zero ports; the wildcard strings are its inet_ntop-normalized
// spellings of the unspecified addresses.
std::optional<NumericEndpoint> PublishableClientEndpoint(
    const std::vector<std::string>& endpoints) {
  for (const std::string& raw : endpoints) {
    std::string_view text(raw);
    if (text.starts_with("tls://")) continue;
    constexpr std::string_view kTcpTag = "tcp://";
    if (text.starts_with(kTcpTag)) text.remove_prefix(kTcpTag.size());
    std::optional<NumericEndpoint> endpoint = ParseNumericEndpoint(text);
    if (!endpoint.has_value()) continue;
    if (endpoint->host_ == "0.0.0.0" || endpoint->host_ == "::" ||
        endpoint->host_ == "::ffff:0.0.0.0") {
      continue;
    }
    return endpoint;
  }
  return std::nullopt;
}

// data_nodes_ is identity-store ordered (sorted by node_id_).
const MetaNodeRecord* FindNodeRecord(const MetaDiscoveryCut& cut,
                                     const std::string& node_id) {
  const auto& nodes = cut.committed_.data_nodes_;
  const auto it =
      std::lower_bound(nodes.begin(), nodes.end(), node_id,
                       [](const MetaNodeRecord& node, const std::string& id) {
                         return node.node_id_ < id;
                       });
  return it != nodes.end() && it->node_id_ == node_id ? &*it : nullptr;
}

// runtime_.nodes_ is lexicographically sorted by node_id_ by contract.
const MetaDataControlRuntimeNode* FindRuntimeNode(const MetaDiscoveryCut& cut,
                                                  const std::string& node_id) {
  const auto& nodes = cut.runtime_.nodes_;
  const auto it = std::lower_bound(
      nodes.begin(), nodes.end(), node_id,
      [](const MetaDataControlRuntimeNode& node, const std::string& id) {
        return node.node_id_ < id;
      });
  return it != nodes.end() && it->node_id_ == node_id ? &*it : nullptr;
}

// observed_nodes_ outlives session removal precisely so that "never contacted
// this leader" is distinguishable from "had a session that is gone". During
// the post-election grace window the former reads as unknown and sets no
// down flags; the latter counts as down at any time, because a lost session
// is positive evidence while a node this leader has never seen proves nothing.
bool EverObserved(const MetaDiscoveryCut& cut, const std::string& node_id) {
  const auto& observed = cut.runtime_.observed_nodes_;
  return std::binary_search(observed.begin(), observed.end(), node_id);
}

bool HealthGreen(const MetaDataControlRuntimeNode& node,
                 const MetaDiscoveryCut& cut) {
  return node.health_.has_value() &&
         node.health_received_unix_ms_ <= cut.now_unix_ms_ &&
         cut.now_unix_ms_ - node.health_received_unix_ms_ <=
             static_cast<std::int64_t>(cut.observation_ttl_ms_) &&
         node.health_->storage_ready && node.health_->population_ready &&
         !node.health_->draining;
}

// The runtime registry's per-group anchors are the term/assignment of the last
// desired-state projection this node acknowledged applying (they are published
// only after the node's FullStateApplied), so equality with the committed
// anchor proves the node carries the current term and membership incarnation.
bool AnchorCurrent(const MetaDataControlRuntimeNode& node,
                   const MetaCommittedStatusGroup& group,
                   const MetaGroupMember& member) {
  const auto projected = std::find_if(
      node.groups_.begin(), node.groups_.end(), [&](const auto& item) {
        return item.group_id_ == group.topology_.group_id_;
      });
  return projected != node.groups_.end() &&
         projected->group_term_ == group.grant_.group_term_ &&
         projected->assignment_id_ == member.assignment_id_;
}

// Committed non-owner members whose identity record is still active.
std::size_t CommittedReplicaCount(const MetaDiscoveryCut& cut,
                                  const MetaCommittedStatusGroup& group,
                                  const std::string& owner) {
  std::size_t count = 0;
  for (const MetaGroupMember& member : group.topology_.members_) {
    if (member.node_id_ == owner) continue;
    const MetaNodeRecord* record = FindNodeRecord(cut, member.node_id_);
    if (record != nullptr && !record->retired_) ++count;
  }
  return count;
}

// Token order matches real Redis 7.2 (subjective-down first, then role, then
// link state) so stock client flag parsers see a familiar shape. o_down has no
// spelling here by design: an objectively down Primary has its publication
// retracted instead.
std::string MasterFlagsString(const MetaDiscoveryMasterFlags& flags) {
  std::string result;
  if (flags.s_down_) result += "s_down,";
  result += "master";
  if (flags.disconnected_) result += ",disconnected";
  return result;
}

std::string ReplicaFlagsString(const MetaDiscoveryReplica& replica) {
  std::string result;
  if (replica.s_down_) result += "s_down,";
  result += "slave";
  if (replica.disconnected_) result += ",disconnected";
  if (replica.master_down_) result += ",master_down";
  return result;
}

void AppendEntryField(ReplyBuilder& reply, std::string_view key,
                      std::string_view value) {
  reply.AppendBulkString(key);
  reply.AppendBulkString(value);
}

// Real Redis 7.2 Sentinel entries encode every value — including numeric
// fields such as port, config-epoch, and the counters — as a bulk string, in
// both RESP2 flat arrays and RESP3 maps. Keep that byte shape verbatim.
void AppendEntryCounter(ReplyBuilder& reply, std::string_view key,
                        long long value) {
  reply.AppendBulkString(key);
  reply.AppendBulkString(std::to_string(value));
}

// Field list and order follow the Redis 7.2 addReplySentinelRedisInstance
// master shape. Counters this surface cannot observe truthfully are documented
// constants rather than omitted, because stock clients parse them by name;
// num-other-sentinels=0 is the truth until the Sentinel directory exists.
void AppendMasterEntry(ReplyBuilder& reply, const MetaDiscoveryPrimary& primary,
                       const MetaDiscoveryMasterFlags& flags) {
  reply.AppendMapHeader(20);
  AppendEntryField(reply, "name", primary.group_id_);
  AppendEntryField(reply, "ip", primary.endpoint_.host_);
  AppendEntryCounter(reply, "port", primary.endpoint_.port_);
  AppendEntryField(reply, "runid", primary.owner_node_id_);
  AppendEntryField(reply, "flags", MasterFlagsString(flags));
  AppendEntryCounter(reply, "link-pending-commands", 0);
  AppendEntryCounter(reply, "link-refcount", 0);
  AppendEntryCounter(reply, "last-ping-sent", 0);
  AppendEntryCounter(reply, "last-ok-ping-reply", 0);
  AppendEntryCounter(reply, "last-ping-reply", 0);
  AppendEntryCounter(reply, "down-after-milliseconds", 30000);
  AppendEntryCounter(reply, "info-refresh", 0);
  AppendEntryField(reply, "role-reported", "master");
  AppendEntryCounter(reply, "role-reported-time", 0);
  AppendEntryCounter(reply, "config-epoch",
                     static_cast<long long>(primary.group_term_));
  AppendEntryCounter(reply, "num-slaves",
                     static_cast<long long>(primary.replica_count_));
  AppendEntryCounter(reply, "num-other-sentinels", 0);
  AppendEntryCounter(reply, "quorum", 1);
  AppendEntryCounter(reply, "failover-timeout", 180000);
  AppendEntryCounter(reply, "parallel-syncs", 1);
}

// The Redis 7.2 replica shape minus the master-link-status /
// master-link-down-time pair: replication link health is not published through
// this interface (its absence is a documented contract, not a stale zero).
void AppendReplicaEntry(ReplyBuilder& reply, const MetaDiscoveryCut& cut,
                        const MetaCommittedStatusGroup& group,
                        const MetaDiscoveryReplica& replica) {
  // master-host/master-port carry the committed owner's endpoint. When it
  // cannot be resolved to a publishable address the pair is omitted; a
  // placeholder address would route replica clients nowhere real.
  std::optional<NumericEndpoint> owner_endpoint;
  if (!group.topology_.record_.owner_.empty()) {
    if (const MetaNodeRecord* owner =
            FindNodeRecord(cut, group.topology_.record_.owner_);
        owner != nullptr) {
      owner_endpoint = PublishableClientEndpoint(owner->endpoints_);
    }
  }
  reply.AppendMapHeader(owner_endpoint.has_value() ? 19 : 17);
  AppendEntryField(reply, "name", FormatNumericEndpoint(replica.endpoint_));
  AppendEntryField(reply, "ip", replica.endpoint_.host_);
  AppendEntryCounter(reply, "port", replica.endpoint_.port_);
  AppendEntryField(reply, "runid", replica.node_id_);
  AppendEntryField(reply, "flags", ReplicaFlagsString(replica));
  AppendEntryCounter(reply, "link-pending-commands", 0);
  AppendEntryCounter(reply, "link-refcount", 0);
  AppendEntryCounter(reply, "last-ping-sent", 0);
  AppendEntryCounter(reply, "last-ok-ping-reply", 0);
  AppendEntryCounter(reply, "last-ping-reply", 0);
  AppendEntryCounter(reply, "down-after-milliseconds", 30000);
  AppendEntryCounter(reply, "info-refresh", 0);
  AppendEntryField(reply, "role-reported", "slave");
  AppendEntryCounter(reply, "role-reported-time", 0);
  if (owner_endpoint.has_value()) {
    AppendEntryField(reply, "master-host", owner_endpoint->host_);
    AppendEntryCounter(reply, "master-port", owner_endpoint->port_);
  }
  AppendEntryCounter(reply, "slave-priority", 100);
  AppendEntryCounter(reply, "slave-repl-offset", 0);
  AppendEntryCounter(reply, "replica-announced", 1);
}

// The complete-Single-Group publication condition includes owning the whole
// slot space. Creation, SetSlotMap, and snapshot decode already enforce this
// for a committed Single cluster; checking it here makes the publication
// condition locally auditable instead of relying on that invariant chain. The
// store keeps ranges as sorted runs, so a contiguity walk from slot 0 that
// ends exactly at kMetaSlotCount proves full single-group coverage.
bool CoversFullSlotSpace(const MetaCommittedStatusView& view,
                         const std::string& group_id) {
  std::uint32_t expected_first = 0;
  for (const MetaCommittedStatusSlotRange& range : view.slot_ranges_) {
    if (range.group_id_ != group_id || range.first_ != expected_first) {
      return false;
    }
    expected_first = range.last_ + 1;
  }
  return expected_first == kMetaSlotCount;
}

}  // namespace

const MetaCommittedStatusGroup* DiscoveryServiceGroup(
    const MetaDiscoveryCut& cut) {
  const MetaClusterLifecycleState& lifecycle =
      cut.committed_.cluster_lifecycle_;
  if (lifecycle.state_ != MetaClusterLifecycle::kCreated ||
      lifecycle.client_mode_ != std::optional(ClientMode::kSingle) ||
      cut.committed_.groups_.size() != 1) {
    return nullptr;
  }
  const MetaCommittedStatusGroup& group = cut.committed_.groups_.front();
  if (!CoversFullSlotSpace(cut.committed_, group.topology_.group_id_)) {
    return nullptr;
  }
  return &group;
}

std::optional<MetaDiscoveryPrimary> PublishablePrimary(
    const MetaDiscoveryCut& cut, const MetaCommittedStatusGroup& group) {
  // A fenced term publishes no Primary at all: the retained record owner has
  // no authority, and announcing it (even flagged down) would route clients to
  // a node that cannot serve.
  if (!group.grant_.grant_.has_value()) return std::nullopt;
  const std::string& owner = group.grant_.grant_->owner_;
  if (owner.empty()) return std::nullopt;
  const auto member = std::find_if(
      group.topology_.members_.begin(), group.topology_.members_.end(),
      [&](const MetaGroupMember& item) { return item.node_id_ == owner; });
  if (member == group.topology_.members_.end()) return std::nullopt;
  const MetaNodeRecord* record = FindNodeRecord(cut, owner);
  if (record == nullptr || record->retired_) return std::nullopt;
  std::optional<NumericEndpoint> endpoint =
      PublishableClientEndpoint(record->endpoints_);
  if (!endpoint.has_value()) return std::nullopt;
  return MetaDiscoveryPrimary{
      .group_id_ = group.topology_.group_id_,
      .owner_node_id_ = owner,
      .endpoint_ = *endpoint,
      .group_term_ = group.grant_.group_term_,
      .replica_count_ = CommittedReplicaCount(cut, group, owner)};
}

MetaDiscoveryMasterFlags MasterFlags(const MetaDiscoveryCut& cut,
                                     const MetaDiscoveryPrimary& primary) {
  MetaDiscoveryMasterFlags flags;
  const bool unknown = cut.observation_grace_active_ &&
                       !EverObserved(cut, primary.owner_node_id_);
  flags.disconnected_ =
      FindRuntimeNode(cut, primary.owner_node_id_) == nullptr && !unknown;
  const MetaAutomaticFailoverDiagnosticsSnapshot& diagnostics =
      cut.diagnostics_;
  if (diagnostics.leadership_generation_ != 0 &&
      diagnostics.leadership_generation_ ==
          cut.runtime_.leadership_generation_ &&
      diagnostics.leader_authority_eligibility_revision_ ==
          cut.runtime_.leader_authority_eligibility_revision_) {
    const auto status =
        std::find_if(diagnostics.statuses_.begin(), diagnostics.statuses_.end(),
                     [&](const MetaAutomaticFailoverStatus& item) {
                       return item.anchor_.group_id_ == primary.group_id_;
                     });
    if (status != diagnostics.statuses_.end() &&
        status->anchor_.owner_node_id_ == primary.owner_node_id_ &&
        status->anchor_.group_term_ == primary.group_term_ &&
        (status->state_ == MetaAutomaticFailoverState::kSuspect ||
         status->state_ == MetaAutomaticFailoverState::kTriggering)) {
      flags.s_down_ = true;
    }
  }
  return flags;
}

std::vector<MetaDiscoveryReplica> ListReplicas(
    const MetaDiscoveryCut& cut, const MetaCommittedStatusGroup& group,
    bool primary_publishable) {
  std::vector<MetaDiscoveryReplica> replicas;
  const std::string& owner = group.topology_.record_.owner_;
  for (const MetaGroupMember& member : group.topology_.members_) {
    if (member.node_id_ == owner) continue;
    const MetaNodeRecord* record = FindNodeRecord(cut, member.node_id_);
    if (record == nullptr || record->retired_) continue;
    std::optional<NumericEndpoint> endpoint =
        PublishableClientEndpoint(record->endpoints_);
    if (!endpoint.has_value()) continue;
    const MetaDataControlRuntimeNode* runtime =
        FindRuntimeNode(cut, member.node_id_);
    const bool unknown =
        cut.observation_grace_active_ && !EverObserved(cut, member.node_id_);
    const bool readable = runtime != nullptr && HealthGreen(*runtime, cut) &&
                          AnchorCurrent(*runtime, group, member);
    replicas.push_back(
        MetaDiscoveryReplica{.node_id_ = member.node_id_,
                             .endpoint_ = *endpoint,
                             .s_down_ = !readable && !unknown,
                             .disconnected_ = runtime == nullptr && !unknown,
                             .master_down_ = !primary_publishable});
  }
  return replicas;
}

void EncodeDiscoveryAddressReply(ReplyBuilder& reply,
                                 const MetaDiscoveryCut& cut,
                                 std::string_view name) {
  std::optional<MetaDiscoveryPrimary> primary;
  if (const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
      group != nullptr && group->topology_.group_id_ == name) {
    primary = PublishablePrimary(cut, *group);
  }
  if (!primary.has_value()) {
    reply.AppendNullArray();
    return;
  }
  // Real Redis 7.2 answers both elements as bulk strings; redis-py accepts a
  // falsy reply or a pair whose port coerces to int.
  reply.AppendArrayHeader(2);
  reply.AppendBulkString(primary->endpoint_.host_);
  reply.AppendBulkString(std::to_string(primary->endpoint_.port_));
}

void EncodeDiscoveryMastersReply(ReplyBuilder& reply,
                                 const MetaDiscoveryCut& cut) {
  std::optional<MetaDiscoveryPrimary> primary;
  if (const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
      group != nullptr) {
    primary = PublishablePrimary(cut, *group);
  }
  reply.AppendArrayHeader(primary.has_value() ? 1 : 0);
  if (primary.has_value()) {
    AppendMasterEntry(reply, *primary, MasterFlags(cut, *primary));
  }
}

void EncodeDiscoveryMasterReply(ReplyBuilder& reply,
                                const MetaDiscoveryCut& cut,
                                std::string_view name) {
  std::optional<MetaDiscoveryPrimary> primary;
  if (const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
      group != nullptr && group->topology_.group_id_ == name) {
    primary = PublishablePrimary(cut, *group);
  }
  if (!primary.has_value()) {
    // A known but currently masterless service has no truthful MASTER entry:
    // publication is already retracted, and real clients treat this error as
    // "nothing to connect to" on either reading.
    reply.AppendError(kDiscoveryNoSuchMasterError);
    return;
  }
  AppendMasterEntry(reply, *primary, MasterFlags(cut, *primary));
}

void EncodeDiscoveryReplicasReply(ReplyBuilder& reply,
                                  const MetaDiscoveryCut& cut,
                                  std::string_view name) {
  const MetaCommittedStatusGroup* group = DiscoveryServiceGroup(cut);
  if (group == nullptr || group->topology_.group_id_ != name) {
    reply.AppendError(kDiscoveryNoSuchMasterError);
    return;
  }
  const std::vector<MetaDiscoveryReplica> replicas =
      ListReplicas(cut, *group, PublishablePrimary(cut, *group).has_value());
  reply.AppendArrayHeader(replicas.size());
  for (const MetaDiscoveryReplica& replica : replicas) {
    AppendReplicaEntry(reply, cut, *group, replica);
  }
}

}  // namespace lavik::meta
