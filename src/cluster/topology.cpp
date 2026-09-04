#include "keylane/cluster/topology.h"

#include <algorithm>
#include <cstddef>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "keylane/storage/format.h"

namespace keylane::cluster {

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

// Composite authority token for one group: owner identity, term, grant, and
// readiness. Computed once per group at Build; the request path only compares
// the precomputed values. 0 is reserved for "unknown group" so a dangling
// group id can never compare equal to a real authority during the owner-side
// re-check.
std::uint64_t ComputeGroupToken(const GroupView& group) {
  std::uint64_t hash = kFnv1aOffsetBasis;
  HashString(hash, group.primary_node_id_);
  HashU64(hash, group.group_term_);
  HashBool(hash, group.granted_);
  HashBool(hash, group.population_ready_);
  HashBool(hash, group.storage_ready_);
  HashU64(hash, group.config_epoch_);
  return hash == 0 ? 1 : hash;
}

// Node ids come from the shared static topology file; Redis writes them as 40
// lowercase hex characters and the data plane keeps them verbatim so MYID and
// CLUSTER NODES stay stable across restarts.
bool IsValidNodeId(std::string_view node_id) {
  if (node_id.size() != 40) return false;
  return std::all_of(node_id.begin(), node_id.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

// Serializes the semantic content of a snapshot: nodes, per-group authority
// and readiness, slot ownership, topology epoch, and self id. Two
// normalizations keep the hash semantic rather than literal: nodes, groups,
// and replica lists are hashed in id order (insertion order is not content),
// and slot ownership is hashed through the computed slot map instead of
// GroupView::slot_ranges_ (differently partitioned but equivalent ranges —
// e.g. [0,9] versus [0,4]+[5,9] — describe the same state).
std::uint64_t ComputeContentHash(
    std::uint64_t topology_epoch, std::string_view self_node_id,
    const std::vector<NodeDescriptor>& nodes,
    const std::vector<GroupView>& groups,
    const std::array<std::int32_t, kSlotCount>& slot_to_group) {
  std::uint64_t hash = kFnv1aOffsetBasis;
  HashU64(hash, topology_epoch);
  HashString(hash, self_node_id);

  std::vector<const NodeDescriptor*> sorted_nodes;
  sorted_nodes.reserve(nodes.size());
  for (const NodeDescriptor& node : nodes) sorted_nodes.push_back(&node);
  std::sort(sorted_nodes.begin(), sorted_nodes.end(),
            [](const NodeDescriptor* a, const NodeDescriptor* b) {
              return a->node_id_ < b->node_id_;
            });
  HashU64(hash, sorted_nodes.size());
  for (const NodeDescriptor* node : sorted_nodes) {
    HashString(hash, node->node_id_);
    HashString(hash, node->host_);
    HashU64(hash, node->port_);
    HashU64(hash, node->tls_port_);
    HashBool(hash, node->is_primary_);
    HashString(hash, node->primary_id_);
    HashU64(hash, node->config_epoch_);
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
    HashString(hash, group->primary_node_id_);
    std::vector<std::string_view> replicas(group->replica_node_ids_.begin(),
                                           group->replica_node_ids_.end());
    std::sort(replicas.begin(), replicas.end());
    HashU64(hash, replicas.size());
    for (std::string_view replica : replicas) HashString(hash, replica);
    HashU64(hash, group->group_term_);
    HashBool(hash, group->granted_);
    HashBool(hash, group->population_ready_);
    HashBool(hash, group->storage_ready_);
    HashU64(hash, group->config_epoch_);
  }

  for (int slot = 0; slot < kSlotCount; ++slot) {
    const std::int32_t group_index = slot_to_group[slot];
    if (group_index < 0) continue;
    HashU64(hash, static_cast<std::uint64_t>(slot));
    HashString(hash, groups[static_cast<std::size_t>(group_index)].group_id_);
  }
  return hash;
}

}  // namespace

const NodeDescriptor* ServingState::Self() const {
  if (self_node_id_.empty()) return nullptr;
  return FindNode(self_node_id_);
}

const NodeDescriptor* ServingState::FindNode(std::string_view node_id) const {
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
  const std::int32_t group_index = slot_to_group_[slot];
  if (group_index < 0) return nullptr;
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
  const std::int32_t group_index = slot_to_group_[slot];
  if (group_index < 0) return 0;
  return group_tokens_[static_cast<std::size_t>(group_index)];
}

std::size_t InFlightStripe() noexcept {
  static std::atomic<std::size_t> next{0};
  thread_local const std::size_t stripe =
      next.fetch_add(1, std::memory_order_relaxed);
  return stripe;
}

GroupInFlight* ServingState::InFlightCellForSlot(std::uint16_t slot) const {
  const std::int32_t index = slot_to_group_[slot];
  if (index < 0) return nullptr;
  return in_flight_cells_[static_cast<std::size_t>(index)].get();
}

std::uint64_t ServingState::GroupInFlightCount(std::string_view group_id) const {
  const GroupView* group = FindGroup(group_id);
  if (group == nullptr) return 0;
  const std::size_t index =
      static_cast<std::size_t>(group - groups_.data());
  return in_flight_cells_[index]->Total();
}

std::uint64_t ServingState::TotalInFlightCount() const {
  std::uint64_t total = 0;
  for (const std::shared_ptr<GroupInFlight>& cell : in_flight_cells_) {
    total += cell->Total();
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

ServingStateBuilder& ServingStateBuilder::SetSelfNodeId(
    std::string_view node_id) {
  self_node_id_ = node_id;
  return *this;
}

ServingStateBuilder& ServingStateBuilder::AddNode(NodeDescriptor node) {
  nodes_.push_back(std::move(node));
  return *this;
}

ServingStateBuilder& ServingStateBuilder::AddGroup(GroupView group) {
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
  std::vector<std::string_view> node_ids;
  node_ids.reserve(nodes_.size());
  for (const NodeDescriptor& node : nodes_) {
    if (!IsValidNodeId(node.node_id_)) {
      return absl::InvalidArgumentError(
          absl::StrCat("invalid node id '", node.node_id_,
                       "': want 40 lowercase hex characters"));
    }
    node_ids.push_back(node.node_id_);
  }
  std::sort(node_ids.begin(), node_ids.end());
  for (std::size_t i = 1; i < node_ids.size(); ++i) {
    if (node_ids[i] == node_ids[i - 1]) {
      return absl::InvalidArgumentError(
          absl::StrCat("duplicate node id '", node_ids[i], "'"));
    }
  }
  auto node_exists = [&node_ids](std::string_view id) {
    return std::binary_search(node_ids.begin(), node_ids.end(), id);
  };

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
    if (!node_exists(group.primary_node_id_)) {
      return absl::InvalidArgumentError(
          absl::StrCat("group '", group.group_id_, "' primary '",
                       group.primary_node_id_, "' is not a known node"));
    }
    for (const std::string& replica : group.replica_node_ids_) {
      if (!node_exists(replica)) {
        return absl::InvalidArgumentError(
            absl::StrCat("group '", group.group_id_, "' replica '", replica,
                         "' is not a known node"));
      }
    }
  }

  std::array<std::int32_t, kSlotCount> slot_to_group;
  slot_to_group.fill(-1);
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
        const std::int32_t existing = slot_to_group[slot];
        if (existing >= 0) {
          // Any double assignment is rejected, including within one group:
          // neither the static file nor Meta can legitimately produce it.
          const GroupView& owner = groups_[static_cast<std::size_t>(existing)];
          return absl::InvalidArgumentError(absl::StrCat(
              "slot ", slot, " is covered by both group '", owner.group_id_,
              "' and group '", group.group_id_, "'"));
        }
        slot_to_group[slot] = static_cast<std::int32_t>(gi);
        ++covered_slots;
      }
    }
  }

  auto state = std::make_shared<ServingState>();
  state->topology_epoch_ = topology_epoch_;
  state->self_node_id_ = self_node_id_;
  state->nodes_ = nodes_;
  state->groups_ = groups_;
  state->slot_to_group_ = slot_to_group;
  state->covered_slots_ = covered_slots;
  state->group_tokens_.reserve(state->groups_.size());
  for (const GroupView& group : state->groups_) {
    state->group_tokens_.push_back(ComputeGroupToken(group));
  }
  // Every group starts with a fresh cell; Publish may swap in the replaced
  // snapshot's cell for token-unchanged groups.
  state->in_flight_cells_.resize(state->groups_.size());
  for (std::shared_ptr<GroupInFlight>& cell : state->in_flight_cells_) {
    cell = std::make_shared<GroupInFlight>();
  }
  state->content_hash_ = ComputeContentHash(topology_epoch_, self_node_id_,
                                            nodes_, groups_, slot_to_group);
  return state;
}

std::shared_ptr<const ServingState> TopologyCache::Current() const {
  return current_.load();
}

std::uint64_t TopologyCache::Publish(
    std::shared_ptr<const ServingState> state) {
  const std::shared_ptr<const ServingState> current = current_.load();
  if (current != nullptr && state != nullptr &&
      current->content_hash() == state->content_hash()) {
    // Content-identical republish: keep the older snapshot and the version,
    // so reload churn is invisible to observers (invariant 2). Note the
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
  current_.store(std::move(state));
  return version_.fetch_add(1) + 1;
}

std::uint64_t TopologyCache::version() const { return version_.load(); }

const std::shared_ptr<const ServingState>& CurrentCachedWithVersion(
    TopologyCache& cache, std::uint64_t* version_out) {
  thread_local std::shared_ptr<const ServingState> entry;
  thread_local std::uint64_t entry_version = 0;
  const std::uint64_t v = cache.version();
  if (entry != nullptr && entry_version == v) {
    *version_out = v;
    return entry;
  }
  // Miss: pair the snapshot with the version that actually covers it. A
  // publish landing between the two version reads is retried rather than
  // cached, so a hit always means "this state was current at this version".
  for (;;) {
    const std::uint64_t before = cache.version();
    std::shared_ptr<const ServingState> state = cache.Current();
    const std::uint64_t after = cache.version();
    if (before == after) {
      entry = std::move(state);
      entry_version = before;
      *version_out = before;
      return entry;
    }
  }
}

namespace router {

const NodeDescriptor* PrimaryForSlot(const ServingState& state,
                                     std::uint16_t slot) {
  const GroupView* group = state.GroupForSlot(slot);
  if (group == nullptr) return nullptr;
  return state.FindNode(group->primary_node_id_);
}

std::uint16_t ClientPort(const NodeDescriptor& node, bool connection_tls) {
  if (connection_tls && node.tls_port_ != 0) return node.tls_port_;
  return node.port_;
}

std::string Endpoint(const NodeDescriptor& node, bool connection_tls) {
  const std::uint16_t port = ClientPort(node, connection_tls);
  // Bracket IPv6 literals so the "host:port" shape stays unambiguous.
  if (node.host_.find(':') != std::string::npos) {
    return absl::StrCat("[", node.host_, "]:", port);
  }
  return absl::StrCat(node.host_, ":", port);
}

}  // namespace router

}  // namespace keylane::cluster
