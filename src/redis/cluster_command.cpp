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

#include "cluster_command.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "lavik/cluster/runtime.h"
#include "lavik/cluster/topology.h"
#include "lavik/resp.h"
#include "lavik/storage/format.h"

namespace lavik {
namespace {

// Same case-insensitive compare as the other Redis command translation
// units; the subcommand literals below are passed lowercase.
bool EqualsIgnoreCase(std::string_view text, std::string_view lower) {
  if (text.size() != lower.size()) return false;
  for (std::size_t i = 0; i < text.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(text[i]);
    if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    if (c != static_cast<unsigned char>(lower[i])) return false;
  }
  return true;
}

// Redis addReplySubcommandSyntaxError semantics: an unknown subcommand and a
// known subcommand with a wrong argument count produce the same error. The
// command name is pinned to the canonical CLUSTER rather than echoed as
// typed, per the cluster-mode wire contract.
std::string_view AppendClusterSubcommandError(ReplyBuilder& reply_builder,
                                              std::string_view subcommand) {
  return reply_builder.AppendError(
      absl::StrCat("ERR Unknown CLUSTER subcommand or wrong number of "
                   "arguments for '",
                   subcommand, "'"));
}

// Same "host:port@0" shape as the standalone-mode CLUSTER shim in
// command.cpp, bracketing bare IPv6 literals. Duplicated because that helper
// is file-local to command.cpp and command.h cannot export it.
std::string ClusterNodeAddress(std::string_view host, std::uint16_t port) {
  if (host.find(':') != std::string_view::npos &&
      !(host.starts_with('[') && host.ends_with(']'))) {
    return absl::StrCat("[", host, "]:", port, "@0");
  }
  return absl::StrCat(host, ":", port, "@0");
}

// Host a discovery reply advertises for `node`. The self entry uses the
// configured announce address, which stays empty under a wildcard bind so
// clients dial the startup node's address (Redis's empty-host convention);
// every other node advertises its concrete address from the topology source.
std::string_view DiscoveryHost(const cluster::ServingState& state,
                               const cluster::ClusterRuntime& runtime,
                               const cluster::NodeDescriptor& node) {
  const cluster::NodeDescriptor* self = state.Self();
  if (self == &node) {
    return runtime.announce_ip_;
  }
  return node.host();
}

// Client port a discovery reply advertises for `node`. The self entry honors
// the announce-port/-tls-port overrides (NAT deployments), which the
// startup wiring resolves to the listening ports when unset; every other node
// advertises its file port, selected by the connection's TLS state.
std::uint16_t DiscoveryPort(const cluster::ServingState& state,
                            const cluster::ClusterRuntime& runtime,
                            const cluster::NodeDescriptor& node,
                            bool connection_tls) {
  const cluster::NodeDescriptor* self = state.Self();
  if (self == &node) {
    const std::uint16_t announced =
        connection_tls && runtime.announce_tls_port_ != 0
            ? runtime.announce_tls_port_
            : runtime.announce_port_;
    if (announced != 0) return announced;
  }
  return cluster::router::ClientPort(node, connection_tls);
}

// Sorts ranges and merges adjacent spans so every emitted span is maximal.
// ServingStateBuilder::Build already rejected overlaps, so only adjacency
// needs merging. SLOTS and NODES share this compaction (Redis emits the
// compact form in both).
std::vector<cluster::SlotRange> CompactSlotRanges(
    std::vector<cluster::SlotRange> ranges) {
  std::sort(ranges.begin(), ranges.end(),
            [](const cluster::SlotRange& a, const cluster::SlotRange& b) {
              return a.first_ < b.first_;
            });
  std::vector<cluster::SlotRange> compact;
  for (const cluster::SlotRange& range : ranges) {
    if (!compact.empty() &&
        static_cast<std::uint32_t>(range.first_) <=
            static_cast<std::uint32_t>(compact.back().last_) + 1) {
      compact.back().last_ = std::max(compact.back().last_, range.last_);
    } else {
      compact.push_back(range);
    }
  }
  return compact;
}

void AppendClusterSlotsNode(ReplyBuilder& reply_builder, std::string_view host,
                            std::uint16_t port,
                            const cluster::NodeId& node_id) {
  reply_builder.AppendArrayHeader(3);
  reply_builder.AppendBulkString(host);
  reply_builder.AppendInteger(port);
  const cluster::NodeId::Hex hex = node_id.ToHex();
  reply_builder.AppendBulkString(std::string_view(hex.data(), hex.size()));
}

// CLUSTER SLOTS: one [start, end, primary, replica...] entry per contiguous
// slot range of every group (Redis clusterReplyMultiBulkSlots). The reply is
// built per request because port selection depends on the connection's TLS
// state; the wire shape is identical under RESP2 and RESP3.
void BuildClusterSlotsReply(const cluster::ServingState& state,
                            const cluster::ClusterRuntime& runtime,
                            bool connection_tls, ReplyBuilder& reply_builder) {
  // Resolve every group up front so the announced array lengths can never
  // disagree with the entries actually written.
  struct GroupSlots {
    const cluster::NodeDescriptor* primary_;
    std::vector<const cluster::NodeDescriptor*> replicas_;
    std::vector<cluster::SlotRange> ranges_;
  };
  std::vector<GroupSlots> groups;
  std::uint64_t entry_count = 0;
  for (const cluster::GroupView& group : state.Groups()) {
    const cluster::NodeDescriptor* primary =
        state.NodeAt(group.primary_node_index_);
    if (primary == nullptr) continue;  // Build() validates; defensive
    GroupSlots view;
    view.primary_ = primary;
    for (cluster::NodeIndex replica_index : group.replica_node_indices_) {
      if (const cluster::NodeDescriptor* replica =
              state.NodeAt(replica_index)) {
        view.replicas_.push_back(replica);
      }
    }
    view.ranges_ = CompactSlotRanges(group.slot_ranges_);
    entry_count += view.ranges_.size();
    groups.push_back(std::move(view));
  }

  reply_builder.AppendArrayHeader(entry_count);
  for (const GroupSlots& group : groups) {
    for (const cluster::SlotRange& range : group.ranges_) {
      reply_builder.AppendArrayHeader(2 + 1 + group.replicas_.size());
      reply_builder.AppendInteger(range.first_);
      reply_builder.AppendInteger(range.last_);
      AppendClusterSlotsNode(
          reply_builder, DiscoveryHost(state, runtime, *group.primary_),
          DiscoveryPort(state, runtime, *group.primary_, connection_tls),
          group.primary_->node_id_);
      for (const cluster::NodeDescriptor* replica : group.replicas_) {
        AppendClusterSlotsNode(
            reply_builder, DiscoveryHost(state, runtime, *replica),
            DiscoveryPort(state, runtime, *replica, connection_tls),
            replica->node_id_);
      }
    }
  }
}

// CLUSTER NODES: one nodes.conf-format line per node —
//   <id> <host>:<port>@0 <flags> <master-id> 0 0 <config_epoch> <link> [slots]
// config_epoch is derived from the member Group's term, including fenced
// groups. It is a compatibility display, not Redis's cross-Group election or
// slot-conflict clock. The ping/pong fields stay 0 (no gossip); the link state
// is whatever the topology source recorded. Primaries carry their group's
// compact slot ranges; replicas name their primary and carry no slots.
std::string BuildClusterNodes(const cluster::ServingState& state,
                              const cluster::ClusterRuntime& runtime,
                              bool connection_tls) {
  std::string nodes;
  const cluster::NodeDescriptor* self = state.Self();
  for (std::size_t node_index = 0; node_index < state.Nodes().size();
       ++node_index) {
    const cluster::NodeDescriptor& node = state.Nodes()[node_index];
    node.node_id_.AppendHexTo(&nodes);
    absl::StrAppend(
        &nodes, " ",
        ClusterNodeAddress(DiscoveryHost(state, runtime, node),
                           DiscoveryPort(state, runtime, node, connection_tls)),
        " ");
    if (self == &node) {
      absl::StrAppend(&nodes, "myself,");
    }
    absl::StrAppend(&nodes, node.is_primary() ? "master" : "slave", " ");
    const cluster::NodeDescriptor* primary =
        state.NodeAt(node.primary_node_index_);
    if (primary != nullptr) {
      primary->node_id_.AppendHexTo(&nodes);
    } else {
      nodes.push_back('-');
    }
    absl::StrAppend(&nodes, " 0 0 ", node.group_term_, " ",
                    node.link_connected_ ? "connected" : "disconnected");
    if (node.is_primary()) {
      for (const cluster::GroupView& group : state.Groups()) {
        if (group.primary_node_index_ != node_index) continue;
        for (const cluster::SlotRange& range :
             CompactSlotRanges(group.slot_ranges_)) {
          if (range.first_ == range.last_) {
            absl::StrAppend(&nodes, " ", range.first_);
          } else {
            absl::StrAppend(&nodes, " ", range.first_, "-", range.last_);
          }
        }
      }
    }
    nodes.push_back('\n');
  }
  return nodes;
}

// CLUSTER INFO with the field set pinned for v1. v1 has no
// gossip: pfail/fail and the message counters are always 0, cluster_state
// only reflects slot coverage, and cluster_size counts primaries that
// actually serve slots. Epochs are derived from Group Terms: my_epoch is the
// local membership's term and current_epoch is the view's maximum. Neither
// replaces topology_epoch as the ordering for whole-cluster topology.
// A null state (nothing published yet) reports
// cluster_state:fail with every counter at 0.
std::string BuildClusterInfo(const cluster::ServingState* state) {
  std::uint32_t slots_assigned = 0;
  std::uint64_t known_nodes = 0;
  std::uint64_t cluster_size = 0;
  std::uint64_t current_epoch = 0;
  std::uint64_t my_epoch = 0;
  bool coverage_complete = false;
  if (state != nullptr) {
    coverage_complete = state->CoverageComplete();
    slots_assigned = state->CoveredSlotCount();
    known_nodes = state->Nodes().size();
    current_epoch = state->max_group_term();
    for (const cluster::GroupView& group : state->Groups()) {
      if (!group.slot_ranges_.empty() &&
          state->NodeAt(group.primary_node_index_) != nullptr) {
        ++cluster_size;
      }
    }
    if (const cluster::NodeDescriptor* self = state->Self()) {
      my_epoch = self->group_term_;
    }
  }
  return absl::StrCat(
      "cluster_state:", coverage_complete ? "ok" : "fail",
      "\r\ncluster_slots_assigned:", slots_assigned,
      "\r\ncluster_slots_ok:", slots_assigned, "\r\ncluster_slots_pfail:0",
      "\r\ncluster_slots_fail:0", "\r\ncluster_known_nodes:", known_nodes,
      "\r\ncluster_size:", cluster_size,
      "\r\ncluster_current_epoch:", current_epoch,
      "\r\ncluster_my_epoch:", my_epoch, "\r\ncluster_stats_messages_sent:0",
      "\r\ncluster_stats_messages_received:0\r\n");
}

}  // namespace

Task<CommandReply> ExecuteClusterModeCommand(const CommandRequest& request,
                                             ReplyBuilder& reply_builder) {
  CommandReply reply;
  const auto& args = request.args_;
  // The command table pins CLUSTER's min-arity at 2; this guard only fires
  // for direct callers that bypass the table and mirrors its error text.
  if (args.size() < 2) {
    reply.encoded_ = reply_builder.AppendError(
        "ERR wrong number of arguments for 'cluster' command");
    co_return reply;
  }
  const std::string_view subcommand = args[1];

  // Every subcommand observes one consistent snapshot (cluster invariant 2);
  // it is null until the control port publishes the first ServingState.
  const cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
  const std::shared_ptr<const cluster::ServingState> state =
      runtime != nullptr ? runtime->topology_cache_.Current() : nullptr;

  // KEYSLOT is a pure function of the key and answers even with no snapshot.
  if (EqualsIgnoreCase(subcommand, "keyslot")) {
    if (args.size() != 3) {
      reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
      co_return reply;
    }
    reply.encoded_ = reply_builder.AppendInteger(storage::RedisSlot(args[2]));
    co_return reply;
  }

  if (EqualsIgnoreCase(subcommand, "myid")) {
    if (args.size() != 2) {
      reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
      co_return reply;
    }
    cluster::NodeId::Hex self_id{};
    std::size_t self_id_size = 0;
    if (state != nullptr && state->Self() != nullptr) {
      self_id = state->Self()->node_id_.ToHex();
      self_id_size = self_id.size();
    }
    reply.encoded_ = reply_builder.AppendBulkString(
        std::string_view(self_id.data(), self_id_size));
    co_return reply;
  }

  if (EqualsIgnoreCase(subcommand, "info")) {
    if (args.size() != 2) {
      reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
      co_return reply;
    }
    reply.encoded_ =
        reply_builder.AppendBulkString(BuildClusterInfo(state.get()));
    co_return reply;
  }

  if (EqualsIgnoreCase(subcommand, "slots")) {
    if (args.size() != 2) {
      reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
      co_return reply;
    }
    if (state == nullptr) {
      reply.encoded_ = reply_builder.AppendArrayHeader(0);
      co_return reply;
    }
    BuildClusterSlotsReply(*state, *runtime, request.connection_tls_,
                           reply_builder);
    reply.encoded_ = reply_builder.View();
    co_return reply;
  }

  if (EqualsIgnoreCase(subcommand, "nodes")) {
    if (args.size() != 2) {
      reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
      co_return reply;
    }
    reply.encoded_ = reply_builder.AppendBulkString(
        state == nullptr
            ? std::string()
            : BuildClusterNodes(*state, *runtime, request.connection_tls_));
    co_return reply;
  }

  // Everything else — SHARDS/SETSLOT/MEET/FORGET/REPLICATE/ADDSLOTS/
  // DELSLOTS/FAILOVER/RESET/BUMPEPOCH/SAVECONFIG/SET-CONFIG-EPOCH/
  // COUNTKEYSINSLOT/GETKEYSINSLOT/ASKING/HELP — is out of v1 scope.
  reply.encoded_ = AppendClusterSubcommandError(reply_builder, subcommand);
  co_return reply;
}

}  // namespace lavik
