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

#include "lavik/cluster/topology.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "lavik/storage/format.h"

namespace lavik::cluster {

std::optional<NodeId> NodeId::Parse(std::string_view hex) noexcept {
  if (hex.size() != kHexSize) return std::nullopt;

  Bytes bytes;
  for (std::size_t i = 0; i < kByteSize; ++i) {
    const auto nibble = [](char c) -> std::optional<std::uint8_t> {
      if (c >= '0' && c <= '9') return static_cast<std::uint8_t>(c - '0');
      if (c >= 'a' && c <= 'f') {
        return static_cast<std::uint8_t>(c - 'a' + 10);
      }
      return std::nullopt;
    };
    const std::optional<std::uint8_t> high = nibble(hex[2 * i]);
    const std::optional<std::uint8_t> low = nibble(hex[2 * i + 1]);
    if (!high.has_value() || !low.has_value()) return std::nullopt;
    bytes[i] = static_cast<std::uint8_t>((*high << 4) | *low);
  }
  return NodeId(bytes);
}

NodeId::Hex NodeId::ToHex() const noexcept {
  assert(!empty());
  Hex hex{};
  constexpr char kHexDigits[] = "0123456789abcdef";
  for (std::size_t i = 0; i < bytes_.size(); ++i) {
    hex[2 * i] = kHexDigits[bytes_[i] >> 4];
    hex[2 * i + 1] = kHexDigits[bytes_[i] & 0x0f];
  }
  return hex;
}

std::string NodeId::ToHexString() const {
  if (empty()) return {};
  const Hex hex = ToHex();
  return std::string(hex.data(), hex.size());
}

void NodeId::AppendHexTo(std::string* output) const {
  if (empty()) return;
  const Hex hex = ToHex();
  output->append(hex.data(), hex.size());
}

// The data plane hashes keys with storage::RedisSlot and routes by hash slot;
// the two slot spaces must stay identical or routing and storage would
// disagree on where a key lives.
static_assert(kSlotCount == storage::kLogicalStorageShards,
              "cluster hash slot count must match the storage shard count");

namespace {

// FNV-1a (64-bit) over a canonical field serialization. Hashes are only ever
// compared against values produced by this same process, but integers are
// serialized little-endian regardless so hashes stay stable across builds.
constexpr std::uint64_t kFnv1aOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnv1aPrime = 1099511628211ULL;

void HashByte(std::uint64_t& hash, std::uint8_t byte) {
  hash ^= byte;
  hash *= kFnv1aPrime;
}

void HashU64(std::uint64_t& hash, std::uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    HashByte(hash, static_cast<std::uint8_t>(value & 0xFF));
    value >>= 8;
  }
}

void HashBool(std::uint64_t& hash, bool value) {
  HashByte(hash, value ? 1 : 0);
}

void HashString(std::uint64_t& hash, std::string_view value) {
  HashU64(hash, value.size());
  for (char c : value) {
    HashByte(hash, static_cast<std::uint8_t>(c));
  }
}

void HashNodeId(std::uint64_t& hash, const NodeId& node_id) {
  HashBool(hash, !node_id.empty());
  if (node_id.empty()) return;
  for (std::uint8_t byte : node_id.bytes()) HashByte(hash, byte);
}

void HashAssignmentId(std::uint64_t& hash, const AssignmentId& assignment_id) {
  HashBool(hash, !assignment_id.empty());
  if (assignment_id.empty()) return;
  for (std::uint8_t byte : assignment_id.bytes()) HashByte(hash, byte);
}

const NodeDescriptor* NodeAt(const std::vector<NodeDescriptor>& nodes,
                             NodeIndex node_index) {
  if (node_index == kNoNodeIndex || node_index >= nodes.size()) return nullptr;
  return &nodes[node_index];
}

void HashNodeReference(std::uint64_t& hash,
                       const std::vector<NodeDescriptor>& nodes,
                       NodeIndex node_index) {
  const NodeDescriptor* node = NodeAt(nodes, node_index);
  if (node == nullptr) {
    HashBool(hash, false);
    return;
  }
  HashNodeId(hash, node->node_id_);
}

// Composite authority token for one group: owner identity, term, grant, and
// readiness. Computed once per group at Build; the request path only compares
// the precomputed values. 0 is reserved for "unknown group" so a dangling
// group id can never compare equal to a real authority during the owner-side
// re-check.
std::uint64_t ComputeGroupToken(const GroupView& group,
                                const std::vector<NodeDescriptor>& nodes) {
  std::uint64_t hash = kFnv1aOffsetBasis;
  HashNodeReference(hash, nodes, group.primary_node_index_);
  HashAssignmentId(hash, group.assignment_id_);
  HashU64(hash, group.group_term_);
  HashU64(hash, group.manifest_revision_);
  HashBool(hash, group.granted_);
  HashBool(hash, group.population_ready_);
  HashBool(hash, group.storage_ready_);
  return hash == 0 ? 1 : hash;
}

// Serializes the semantic content of a snapshot: nodes, per-group authority
// and readiness, slot ownership, topology epoch, and self id. Two
// normalizations keep the hash semantic rather than literal: nodes, groups,
// and replica lists are hashed in id order (insertion order is not content),
// and slot ownership is hashed through the computed slot map instead of
// GroupView::slot_ranges_ (differently partitioned but equivalent ranges —
// e.g. [0,9] versus [0,4]+[5,9] — describe the same state).
std::uint64_t ComputeContentHash(
    std::uint64_t topology_epoch, std::uint64_t max_group_term,
    NodeIndex self_node_index, const std::vector<NodeDescriptor>& nodes,
    const std::vector<GroupView>& groups,
    const std::array<GroupIndex, kSlotCount>& slot_to_group) {
  std::uint64_t hash = kFnv1aOffsetBasis;
  HashU64(hash, topology_epoch);
  HashU64(hash, max_group_term);
  HashNodeReference(hash, nodes, self_node_index);

  std::vector<const NodeDescriptor*> sorted_nodes;
  sorted_nodes.reserve(nodes.size());
  for (const NodeDescriptor& node : nodes) sorted_nodes.push_back(&node);
  std::sort(sorted_nodes.begin(), sorted_nodes.end(),
            [](const NodeDescriptor* a, const NodeDescriptor* b) {
              return a->node_id_ < b->node_id_;
            });
  HashU64(hash, sorted_nodes.size());
  for (const NodeDescriptor* node : sorted_nodes) {
    HashNodeId(hash, node->node_id_);
    HashString(hash, node->host());
    HashU64(hash, node->port_);
    HashU64(hash, node->tls_port_);
    HashBool(hash, node->is_primary());
    HashNodeReference(hash, nodes, node->primary_node_index_);
    HashU64(hash, node->group_term_);
    HashBool(hash, node->link_connected_);
  }

  std::vector<const GroupView*> sorted_groups;
  sorted_groups.reserve(groups.size());
  for (const GroupView& group : groups) sorted_groups.push_back(&group);
  std::sort(sorted_groups.begin(), sorted_groups.end(),
            [](const GroupView* a, const GroupView* b) {
              return a->group_id_ < b->group_id_;
            });
  HashU64(hash, sorted_groups.size());
  for (const GroupView* group : sorted_groups) {
    HashString(hash, group->group_id_);
    HashNodeReference(hash, nodes, group->primary_node_index_);
    HashAssignmentId(hash, group->assignment_id_);
    std::vector<NodeId> replicas;
    replicas.reserve(group->replica_node_indices_.size());
    for (NodeIndex replica_index : group->replica_node_indices_) {
      replicas.push_back(nodes[replica_index].node_id_);
    }
    std::sort(replicas.begin(), replicas.end());
    HashU64(hash, replicas.size());
    for (const NodeId& replica : replicas) HashNodeId(hash, replica);
    HashU64(hash, group->group_term_);
    HashU64(hash, group->manifest_revision_);
    HashBool(hash, group->granted_);
    HashBool(hash, group->population_ready_);
    HashBool(hash, group->storage_ready_);
    HashBool(hash, group->mutations_paused_);
  }

  for (int slot = 0; slot < kSlotCount; ++slot) {
    const GroupIndex group_index = slot_to_group[slot];
    if (group_index == kNoGroupIndex) continue;
    HashU64(hash, static_cast<std::uint64_t>(slot));
    HashString(hash, groups[static_cast<std::size_t>(group_index)].group_id_);
  }
  return hash;
}

}  // namespace

const NodeDescriptor* ServingState::Self() const {
  return NodeAt(self_node_index_);
}

const NodeDescriptor* ServingState::NodeAt(NodeIndex node_index) const {
  if (node_index == kNoNodeIndex || node_index >= nodes_.size()) return nullptr;
  return &nodes_[node_index];
}

const NodeDescriptor* ServingState::FindNode(const NodeId& node_id) const {
  for (const NodeDescriptor& node : nodes_) {
    if (node.node_id_ == node_id) return &node;
  }
  return nullptr;
}

const GroupView* ServingState::FindGroup(std::string_view group_id) const {
  for (const GroupView& group : groups_) {
    if (group.group_id_ == group_id) return &group;
  }
  return nullptr;
}

const GroupView* ServingState::GroupForSlot(std::uint16_t slot) const {
  if (slot >= kSlotCount) return nullptr;
  const GroupIndex group_index = slot_to_group_[slot];
  if (group_index == kNoGroupIndex) return nullptr;
  return &groups_[static_cast<std::size_t>(group_index)];
}

bool ServingState::FullyReady() const {
  return std::all_of(groups_.begin(), groups_.end(), [](const GroupView& g) {
    return g.storage_ready_ && g.population_ready_;
  });
}

std::uint64_t ServingState::AuthorityToken(std::string_view group_id) const {
  const GroupView* group = FindGroup(group_id);
  if (group == nullptr) return 0;
  return group_tokens_[static_cast<std::size_t>(group - groups_.data())];
}

std::uint64_t ServingState::AuthorityTokenForSlot(std::uint16_t slot) const {
  if (slot >= kSlotCount) return 0;
  const GroupIndex group_index = slot_to_group_[slot];
  if (group_index == kNoGroupIndex) return 0;
  return group_tokens_[static_cast<std::size_t>(group_index)];
}

GroupInFlight* ServingState::InFlightCellForSlot(std::uint16_t slot) const {
  const GroupIndex index = slot_to_group_[slot];
  if (index == kNoGroupIndex) return nullptr;
  return &in_flight_cells_[static_cast<std::size_t>(index)];
}

std::uint64_t ServingState::GroupInFlightCount(
    std::string_view group_id) const {
  const GroupView* group = FindGroup(group_id);
  if (group == nullptr) return 0;
  const std::size_t index = static_cast<std::size_t>(group - groups_.data());
  return in_flight_cells_[index].Total();
}

std::uint64_t ServingState::TotalInFlightCount() const {
  std::uint64_t total = 0;
  for (const GroupInFlight& cell : in_flight_cells_) {
    total += cell.Total();
  }
  return total;
}

void ServingState::ShareInFlightCellsFrom(const ServingState& previous) const {
  for (std::size_t i = 0; i < groups_.size(); ++i) {
    const std::uint64_t token = AuthorityToken(groups_[i].group_id_);
    const GroupView* before = previous.FindGroup(groups_[i].group_id_);
    if (before == nullptr ||
        token != previous.AuthorityToken(groups_[i].group_id_)) {
      continue;
    }
    const std::size_t previous_index =
        static_cast<std::size_t>(before - previous.groups_.data());
    in_flight_cells_[i] = previous.in_flight_cells_[previous_index];
  }
}

ServingStateBuilder& ServingStateBuilder::SetTopologyEpoch(
    std::uint64_t epoch) {
  topology_epoch_ = epoch;
  return *this;
}

ServingStateBuilder& ServingStateBuilder::SetSelfNodeIndex(
    NodeIndex node_index) {
  self_node_index_ = node_index;
  return *this;
}

ServingStateBuilder& ServingStateBuilder::IncludeGroupTerm(std::uint64_t term) {
  max_group_term_ = std::max(max_group_term_, term);
  return *this;
}

ServingStateBuilder& ServingStateBuilder::SetInFlightStripeCount(
    std::size_t stripe_count) {
  in_flight_stripe_count_ = stripe_count;
  return *this;
}

ServingStateBuilder& ServingStateBuilder::AddNode(NodeDescriptor node) {
  IncludeGroupTerm(node.group_term_);
  nodes_.push_back(std::move(node));
  return *this;
}

ServingStateBuilder& ServingStateBuilder::AddGroup(GroupView group) {
  IncludeGroupTerm(group.group_term_);
  groups_.push_back(std::move(group));
  return *this;
}

ServingStateBuilder& ServingStateBuilder::AddSlotRange(
    std::string_view group_id, SlotRange range) {
  // The frozen signature cannot report failure. Ranges addressed to a group
  // that was never added are dropped: silently serving them would be worse,
  // and Build() still validates every range that did attach.
  for (GroupView& group : groups_) {
    if (group.group_id_ == group_id) {
      group.slot_ranges_.push_back(range);
      break;
    }
  }
  return *this;
}

absl::StatusOr<std::shared_ptr<const ServingState>> ServingStateBuilder::Build()
    const {
  if (in_flight_stripe_count_ == 0) {
    return absl::InvalidArgumentError(
        "in-flight stripe count must be greater than zero");
  }
  std::vector<NodeId> node_ids;
  node_ids.reserve(nodes_.size());
  for (const NodeDescriptor& node : nodes_) {
    if (node.node_id_.empty()) {
      return absl::InvalidArgumentError("node id must not be empty");
    }
    node_ids.push_back(node.node_id_);
  }
  std::sort(node_ids.begin(), node_ids.end());
  for (std::size_t i = 1; i < node_ids.size(); ++i) {
    if (node_ids[i] == node_ids[i - 1]) {
      return absl::InvalidArgumentError(
          absl::StrCat("duplicate node id '", node_ids[i].ToHexString(), "'"));
    }
  }
  if (self_node_index_ != kNoNodeIndex && self_node_index_ >= nodes_.size()) {
    return absl::InvalidArgumentError("self node index is out of range");
  }
  for (const NodeDescriptor& node : nodes_) {
    if (node.primary_node_index_ == kNoNodeIndex) continue;
    if (node.primary_node_index_ >= nodes_.size()) {
      return absl::InvalidArgumentError(
          absl::StrCat("node '", node.node_id_.ToHexString(),
                       "' primary node index is out of range"));
    }
    if (!nodes_[node.primary_node_index_].is_primary()) {
      return absl::InvalidArgumentError(
          absl::StrCat("node '", node.node_id_.ToHexString(),
                       "' points at a non-primary node"));
    }
  }

  if (groups_.size() > kNoGroupIndex) {
    return absl::ResourceExhaustedError(
        "topology has too many groups for 16-bit group indices");
  }
  std::vector<std::string_view> group_ids;
  group_ids.reserve(groups_.size());
  for (const GroupView& group : groups_) {
    if (group.group_id_.empty()) {
      return absl::InvalidArgumentError("group id must not be empty");
    }
    group_ids.push_back(group.group_id_);
  }
  std::sort(group_ids.begin(), group_ids.end());
  for (std::size_t i = 1; i < group_ids.size(); ++i) {
    if (group_ids[i] == group_ids[i - 1]) {
      return absl::InvalidArgumentError(
          absl::StrCat("duplicate group id '", group_ids[i], "'"));
    }
  }

  for (const GroupView& group : groups_) {
    if (group.primary_node_index_ >= nodes_.size()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "group '", group.group_id_, "' primary node index is out of range"));
    }
    if (!nodes_[group.primary_node_index_].is_primary()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "group '", group.group_id_, "' primary index names a replica"));
    }
    for (NodeIndex replica_index : group.replica_node_indices_) {
      if (replica_index >= nodes_.size()) {
        return absl::InvalidArgumentError(
            absl::StrCat("group '", group.group_id_,
                         "' replica node index is out of range"));
      }
      const NodeDescriptor& replica = nodes_[replica_index];
      if (replica.is_primary() ||
          replica.primary_node_index_ != group.primary_node_index_) {
        return absl::InvalidArgumentError(
            absl::StrCat("group '", group.group_id_,
                         "' replica index does not name its replica"));
      }
    }
  }

  std::array<GroupIndex, kSlotCount> slot_to_group;
  slot_to_group.fill(kNoGroupIndex);
  std::uint32_t covered_slots = 0;
  for (std::size_t gi = 0; gi < groups_.size(); ++gi) {
    const GroupView& group = groups_[gi];
    for (const SlotRange& range : group.slot_ranges_) {
      if (range.first_ > range.last_) {
        return absl::InvalidArgumentError(
            absl::StrCat("group '", group.group_id_,
                         "' has inverted slot "
                         "range ",
                         range.first_, "-", range.last_));
      }
      if (range.last_ >= kSlotCount) {
        return absl::InvalidArgumentError(absl::StrCat(
            "group '", group.group_id_, "' slot range ", range.first_, "-",
            range.last_, " exceeds the slot count"));
      }
      for (int slot = range.first_; slot <= range.last_; ++slot) {
        const GroupIndex existing = slot_to_group[slot];
        if (existing != kNoGroupIndex) {
          // Any double assignment is rejected, including within one group:
          // no committed Meta projection can legitimately produce it.
          const GroupView& owner = groups_[static_cast<std::size_t>(existing)];
          return absl::InvalidArgumentError(absl::StrCat(
              "slot ", slot, " is covered by both group '", owner.group_id_,
              "' and group '", group.group_id_, "'"));
        }
        slot_to_group[slot] = static_cast<GroupIndex>(gi);
        ++covered_slots;
      }
    }
  }

  auto state = std::make_shared<ServingState>();
  state->topology_epoch_ = topology_epoch_;
  state->max_group_term_ = max_group_term_;
  state->self_node_index_ = self_node_index_;
  state->nodes_ = nodes_;
  state->groups_ = groups_;
  state->slot_to_group_ = slot_to_group;
  state->covered_slots_ = covered_slots;
  state->in_flight_stripe_count_ = in_flight_stripe_count_;
  state->group_tokens_.reserve(state->groups_.size());
  for (const GroupView& group : state->groups_) {
    state->group_tokens_.push_back(ComputeGroupToken(group, state->nodes_));
  }
  // Every group starts with a fresh cell; Publish may swap in the replaced
  // snapshot's cell for token-unchanged groups.
  state->in_flight_cells_.reserve(state->groups_.size());
  for (std::size_t i = 0; i < state->groups_.size(); ++i) {
    state->in_flight_cells_.emplace_back(in_flight_stripe_count_);
  }
  state->content_hash_ =
      ComputeContentHash(topology_epoch_, max_group_term_, self_node_index_,
                         nodes_, groups_, slot_to_group);
  return state;
}

namespace {
std::atomic<std::uint64_t> next_topology_cache_identity{1};
}

TopologyCache::TopologyCache()
    : cache_identity_(next_topology_cache_identity.fetch_add(
          1, std::memory_order_relaxed)) {}

std::shared_ptr<const ServingState> TopologyCache::Current() const {
  return current_.load();
}

std::uint64_t TopologyCache::Publish(
    std::shared_ptr<const ServingState> state) {
  const std::lock_guard publish_lock(publish_mutex_);
  const std::shared_ptr<const ServingState> current = current_.load();
  if (current != nullptr && state != nullptr &&
      current->content_hash() == state->content_hash()) {
    // Content-identical republish: keep the older snapshot and the version,
    // so projection replay is invisible to observers (invariant 2). Note the
    // check-then-store is deliberately not serialized: two publishers racing
    // with different content simply produce two bumps with the last store
    // winning.
    return version_.load(std::memory_order_relaxed);
  }
  // Share the replaced snapshot's cells into token-unchanged groups before
  // the state can become visible to readers. The state store must precede
  // the version bump: registrants bracket their GroupInFlight::Enter with
  // version loads, so a drain that follows the bump either observes the
  // registration or the registrant observes the bump and rolls back.
  if (state != nullptr && current != nullptr) {
    state->ShareInFlightCellsFrom(*current);
  }
  // Mark the pair unstable before making the new state visible. A reader that
  // lands between the state store and version bump observes the odd sequence
  // and retries instead of accepting the new state with the old version.
  publication_sequence_.fetch_add(1, std::memory_order_acq_rel);
  current_.store(std::move(state));
  const std::uint64_t published = version_.fetch_add(1) + 1;
  publication_sequence_.fetch_add(1, std::memory_order_release);
  return published;
}

std::uint64_t TopologyCache::version() const { return version_.load(); }

std::uint64_t TopologyCache::publication_sequence() const {
  return publication_sequence_.load(std::memory_order_acquire);
}

const std::shared_ptr<const ServingState>& CurrentCachedWithVersion(
    TopologyCache& cache, std::uint64_t* version_out,
    std::uint64_t* publication_sequence_out) {
  thread_local std::uint64_t entry_identity = 0;
  thread_local std::shared_ptr<const ServingState> entry;
  thread_local std::uint64_t entry_version = 0;
  thread_local std::uint64_t entry_sequence = 0;
  const std::uint64_t sequence = cache.publication_sequence();
  if ((sequence & 1U) == 0 && entry_identity == cache.cache_identity() &&
      entry != nullptr && entry_sequence == sequence) {
    *version_out = entry_version;
    if (publication_sequence_out != nullptr) {
      *publication_sequence_out = sequence;
    }
    return entry;
  }
  // Miss: an odd sequence means a writer is between the state store and
  // version bump. Matching even sequence reads prove both values came from
  // one completed publication. Include the cache identity in the TLS entry:
  // tests and embedders may reuse an address for caches whose versions
  // coincide.
  for (;;) {
    const std::uint64_t before = cache.publication_sequence();
    if ((before & 1U) != 0) continue;
    std::shared_ptr<const ServingState> state = cache.Current();
    const std::uint64_t version = cache.version();
    const std::uint64_t after = cache.publication_sequence();
    if (before == after) {
      if (state != nullptr) {
        // Give this thread a separate control block that owns one reference
        // to the published snapshot. Aliasing preserves the ServingState
        // address, but per-request copies update this block instead of the
        // global snapshot's count. Allocate only on cache refresh. Retained
        // admissions may cross workers or outlive this TLS entry, so the
        // local block must still use thread-safe shared ownership.
        const ServingState* snapshot = state.get();
        auto local_owner =
            std::make_shared<std::shared_ptr<const ServingState>>(
                std::move(state));
        state = std::shared_ptr<const ServingState>(std::move(local_owner),
                                                    snapshot);
      }
      entry_identity = cache.cache_identity();
      entry = std::move(state);
      entry_version = version;
      entry_sequence = before;
      *version_out = version;
      if (publication_sequence_out != nullptr) {
        *publication_sequence_out = before;
      }
      return entry;
    }
  }
}

namespace router {

const NodeDescriptor* PrimaryForSlot(const ServingState& state,
                                     std::uint16_t slot) {
  const GroupView* group = state.GroupForSlot(slot);
  if (group == nullptr) return nullptr;
  return state.NodeAt(group->primary_node_index_);
}

std::uint16_t ClientPort(const NodeDescriptor& node, bool connection_tls) {
  if (connection_tls && node.tls_port_ != 0) return node.tls_port_;
  return node.port_;
}

std::string Endpoint(const NodeDescriptor& node, bool connection_tls) {
  const std::uint16_t port = ClientPort(node, connection_tls);
  const std::string_view host = node.host();
  // Bracket IPv6 literals so the "host:port" shape stays unambiguous.
  if (host.find(':') != std::string_view::npos) {
    return absl::StrCat("[", host, "]:", port);
  }
  return absl::StrCat(host, ":", port);
}

}  // namespace router

}  // namespace lavik::cluster
