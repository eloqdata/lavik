#pragma once

// Redis Cluster data-plane routing model.
//
// A ServingState is the atomically published unit of serving truth: slot
// ownership, per-group authority (term/grant), and readiness are built and
// validated together and never mutated in place (invariant 2 of the cluster
// design). Readers hold a shared_ptr and therefore observe one consistent
// snapshot; the TopologyCache swaps snapshots atomically.
//
// This module is deliberately free of Redis wire concerns, Raft phases, and
// transport details: it answers "which group/node owns slot S, and is that
// authority currently safe to serve" and nothing more.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"

namespace keylane::cluster {

// Redis Cluster hash-slot count. Identical to storage::kLogicalStorageShards;
// kept local so this module does not depend on storage internals. A
// static_assert in topology.cpp ties the two together.
inline constexpr std::uint16_t kSlotCount = 16384;

// Redis Cluster node identity in its compact binary form. The wire and
// nodes.conf representation remains exactly 40 lowercase hexadecimal
// characters; parsing at the topology boundary avoids keeping a separately
// allocated string in every immutable descriptor and every node reference.
// A default-constructed value is empty so optional relationships such as a
// primary node's absent upstream do not need a sentinel from the valid
// 160-bit identity space.
class NodeId {
 public:
  static constexpr std::size_t kByteSize = 20;
  static constexpr std::size_t kHexSize = 2 * kByteSize;
  using Bytes = std::array<std::uint8_t, kByteSize>;
  using Hex = std::array<char, kHexSize>;

  NodeId() = default;

  // Parses the canonical Redis spelling. Uppercase is rejected rather than
  // normalized so configuration continues to have one spelling per id.
  static std::optional<NodeId> Parse(std::string_view hex) noexcept;

  // Empty represents an absent relationship, not the valid all-zero id.
  bool empty() const noexcept { return !present_; }
  // Returns the binary identity; callers must check empty() when absence is
  // meaningful because an empty id also carries zero-initialized storage.
  const Bytes& bytes() const noexcept { return bytes_; }
  // Formats the canonical lowercase wire spelling. ToHex() requires a
  // present id; an empty id produces no text from ToHexString()/AppendHexTo.
  Hex ToHex() const noexcept;
  std::string ToHexString() const;
  void AppendHexTo(std::string* output) const;

  friend bool operator==(const NodeId&, const NodeId&) = default;
  friend bool operator<(const NodeId& left, const NodeId& right) noexcept {
    if (left.present_ != right.present_) return !left.present_;
    return left.bytes_ < right.bytes_;
  }

 private:
  explicit NodeId(Bytes bytes) : bytes_(bytes), present_(true) {}

  Bytes bytes_{};
  bool present_ = false;
};

// Stable index into one ServingState's contiguous node table. Indices never
// cross snapshot boundaries; authority hashes resolve them back to NodeId so
// reordering an equivalent table does not change semantic identity.
using NodeIndex = std::uint32_t;
inline constexpr NodeIndex kNoNodeIndex = std::numeric_limits<NodeIndex>::max();

// One cluster node as the data plane sees it. In v1 the static topology file
// is the only source; `tls_port_` is the configured cluster-wide TLS port
// (uniform-port assumption, see control_port.h).
struct NodeDescriptor {
  NodeId node_id_;              // stable across restarts
  bool link_connected_ = true;  // parsed from the file; not consulted in v1
  std::uint16_t port_ = 0;
  std::uint16_t tls_port_ = 0;  // 0 = TLS not offered
  // kNoNodeIndex identifies a primary; replicas point at their primary in
  // the same ServingState::Nodes() table.
  NodeIndex primary_node_index_ = kNoNodeIndex;
  std::uint64_t config_epoch_ = 0;
  std::string host_;

  bool is_primary() const noexcept {
    return primary_node_index_ == kNoNodeIndex;
  }
};

// Slot range, both ends inclusive, as written in a nodes.conf node line.
struct SlotRange {
  std::uint16_t first_ = 0;
  std::uint16_t last_ = 0;
};

// Striped in-flight mutation counter for one group, owned by the published
// ServingState. Registration on the request path is one atomic increment on
// the calling thread's stripe: no lock, no allocation, and no cross-thread
// cacheline contention as long as threads stay on distinct stripes. The drain
// side (control plane, tests) sums the stripes off the request path.
class GroupInFlight {
 public:
  static constexpr std::size_t kStripeCount = 64;

  // Enter is seq_cst on purpose: paired with the registrant's subsequent
  // TopologyCache::version() load and the publisher's store-then-bump order it
  // forms a Dekker handshake — a drain that starts after a revoking publish
  // either observes this registration, or the registrant observes the
  // publication and rolls back (see TopologyCache::Publish).
  void Enter(std::size_t stripe) noexcept {
    stripes_[stripe % kStripeCount].value_.fetch_add(1);
  }
  // Release is sufficient for Exit: an Exit not yet visible to the drain only
  // makes the drain wait longer, never miss an execution.
  void Exit(std::size_t stripe) noexcept {
    stripes_[stripe % kStripeCount].value_.fetch_sub(1,
                                                     std::memory_order_release);
  }
  // Drain-side aggregate; sums every stripe. Never on the request path.
  std::uint64_t Total() const noexcept {
    std::uint64_t total = 0;
    for (const Stripe& stripe : stripes_) {
      // seq_cst so the drain participates in the Dekker handshake described
      // at Enter().
      total += stripe.value_.load();
    }
    return total;
  }

 private:
  struct alignas(64) Stripe {
    std::atomic<std::int64_t> value_{0};
  };
  std::array<Stripe, kStripeCount> stripes_;
};

// One stable stripe per thread, assigned lazily from a process-wide counter.
// Keeps request-path registrations contention-free without tying this module
// to a worker model; stripe sharing only costs cacheline contention, never
// correctness.
std::size_t InFlightStripe() noexcept;

// RAII marker for one admitted in-flight mutation: Enter on construction,
// Exit on destruction. The request path keeps a small inlined vector of
// these, one per distinct group touched by the command, so registration never
// allocates.
class [[nodiscard]] InFlightGuard {
 public:
  InFlightGuard(GroupInFlight& cell, std::size_t stripe) noexcept
      : cell_(&cell), stripe_(stripe) {
    cell_->Enter(stripe_);
  }
  ~InFlightGuard() {
    if (cell_ != nullptr) cell_->Exit(stripe_);
  }
  InFlightGuard(InFlightGuard&& other) noexcept
      : cell_(std::exchange(other.cell_, nullptr)), stripe_(other.stripe_) {}
  InFlightGuard(const InFlightGuard&) = delete;
  InFlightGuard& operator=(const InFlightGuard&) = delete;

  GroupInFlight* cell() const noexcept { return cell_; }

 private:
  GroupInFlight* cell_;
  std::size_t stripe_;
};

// Authority and readiness for one shard group. `group_id_` is opaque to the
// data plane; the static adapter uses the primary's node id (a future Meta
// adapter will assign Meta-scoped ids over the same seam).
struct GroupView {
  std::string group_id_;
  NodeIndex primary_node_index_ = kNoNodeIndex;
  bool granted_ = true;  // false = fenced: this group must not serve
  bool population_ready_ = true;
  bool storage_ready_ = true;
  std::uint64_t group_term_ = 0;
  std::uint64_t config_epoch_ = 0;
  std::vector<NodeIndex> replica_node_indices_;
  std::vector<SlotRange> slot_ranges_;  // owned slots, validated at Build
};

// Immutable committed routing + authority + readiness snapshot.
class ServingState {
 public:
  std::uint64_t topology_epoch() const { return topology_epoch_; }
  // Content identity: two states with equal hashes are interchangeable, and
  // TopologyCache::Publish drops the newer one without bumping the version.
  std::uint64_t content_hash() const { return content_hash_; }

  const NodeDescriptor* Self() const;  // nullptr when self is not in the file
  NodeIndex SelfNodeIndex() const { return self_node_index_; }
  // Direct node-table lookup; returns nullptr for kNoNodeIndex/out of range.
  const NodeDescriptor* NodeAt(NodeIndex node_index) const;
  const NodeDescriptor* FindNode(const NodeId& node_id) const;
  const GroupView* FindGroup(std::string_view group_id) const;
  const std::vector<NodeDescriptor>& Nodes() const { return nodes_; }
  const std::vector<GroupView>& Groups() const { return groups_; }

  // Owning group of a slot, or nullptr when the slot is not covered.
  const GroupView* GroupForSlot(std::uint16_t slot) const;
  bool CoverageComplete() const { return covered_slots_ == kSlotCount; }
  std::uint32_t CoveredSlotCount() const { return covered_slots_; }
  // True when every group's storage and population are ready. Global/admin
  // commands without keys consult this aggregate.
  bool FullyReady() const;

  // Composite authority token for one group (owner identity, term, grant,
  // readiness). The admission side captures it per involved slot; the
  // owner-side re-check compares it against the current snapshot. Returns 0
  // for an unknown group, which compares unequal to any real token.
  std::uint64_t AuthorityToken(std::string_view group_id) const;
  // Hot-path form: the precomputed token of the group owning `slot`, or 0
  // when the slot is unbound. Pure table lookup — no scanning or hashing.
  std::uint64_t AuthorityTokenForSlot(std::uint16_t slot) const;

  // Striped in-flight cell of the group owning `slot`, or nullptr when the
  // slot is unbound. Request-path registration is one atomic Enter on the
  // calling thread's stripe; the cell outlives this snapshot whenever a later
  // snapshot shares it (token-unchanged groups, see TopologyCache::Publish),
  // so a guard is safe to hold for the whole execution.
  GroupInFlight* InFlightCellForSlot(std::uint16_t slot) const;
  // Drain-side aggregates over the cells, for the control plane and tests —
  // never the request path. To fence a group, publish the revoking state and
  // then drain the *replaced* snapshot's cell: guards admitted under
  // token-equal earlier snapshots share it.
  std::uint64_t GroupInFlightCount(std::string_view group_id) const;
  std::uint64_t TotalInFlightCount() const;

 private:
  friend class ServingStateBuilder;
  std::uint64_t topology_epoch_ = 0;
  std::uint64_t content_hash_ = 0;
  std::vector<NodeDescriptor> nodes_;
  std::vector<GroupView> groups_;
  NodeIndex self_node_index_ = kNoNodeIndex;
  // slot -> index into groups_, -1 when unbound.
  std::array<std::int32_t, kSlotCount> slot_to_group_;
  std::uint32_t covered_slots_ = 0;
  // Precomputed per-group authority tokens, parallel to groups_. Computed
  // once at Build so the request path never hashes or scans for them.
  std::vector<std::uint64_t> group_tokens_;
  // One cell per entry in groups_. Build installs a fresh cell per group;
  // TopologyCache::Publish then swaps in the replaced snapshot's cell for
  // every group whose authority token is unchanged, so executions admitted
  // under either snapshot drain together. Mutable because Publication
  // installs the sharing after the state is built but before it becomes
  // visible to readers.
  mutable std::vector<std::shared_ptr<GroupInFlight>> in_flight_cells_;

  // Implements the cell sharing described on in_flight_cells_; called by
  // TopologyCache::Publish before the state is stored.
  void ShareInFlightCellsFrom(const ServingState& previous) const;
  friend class TopologyCache;
};

class ServingStateBuilder {
 public:
  ServingStateBuilder& SetTopologyEpoch(std::uint64_t epoch);
  ServingStateBuilder& SetSelfNodeIndex(NodeIndex node_index);
  ServingStateBuilder& AddNode(NodeDescriptor node);
  // Takes ownership of the group's slot ranges. Slots may also be attached
  // later via AddSlotRange to an existing group id.
  ServingStateBuilder& AddGroup(GroupView group);
  ServingStateBuilder& AddSlotRange(std::string_view group_id, SlotRange range);

  // Validates: node ids are present and unique; group ids are unique; every
  // node index is in range and describes a consistent primary/replica
  // relationship; slot ranges are in [0, kSlotCount) and non-overlapping.
  // Computes the slot map and content hash. Self may legitimately be absent
  // (validation of self-match is the control adapter's job, since only it
  // knows the match rule).
  absl::StatusOr<std::shared_ptr<const ServingState>> Build() const;

 private:
  std::uint64_t topology_epoch_ = 0;
  NodeIndex self_node_index_ = kNoNodeIndex;
  std::vector<NodeDescriptor> nodes_;
  std::vector<GroupView> groups_;
};

// Committed local routing snapshot. Publication
// is a single atomic swap, so topology, grants, and readiness always appear
// together. The version serves two purposes: publication ordering/tests, and
// the publication-race handshake that keeps in-flight registration honest —
// registrants bracket their Enter() with version loads, so a publisher that
// stores the new state and then bumps the version either has its drain
// observe the registration or has the registrant observe the bump and roll
// back. Authority decisions use ServingState::AuthorityToken, never this
// global version, so unrelated groups republishing does not disturb
// in-flight writes.
class TopologyCache {
 public:
  // nullptr until the first publish; callers treat that as "not ready".
  std::shared_ptr<const ServingState> Current() const;
  // Publishes `state`; a content-identical state is a no-op (same version).
  // Installs the in-flight cell sharing (ServingState::in_flight_cells_)
  // before the store, so the state must not be visible to readers yet. The
  // state store precedes the version bump; the registration handshake relies
  // on that order. Returns the current version after the call.
  std::uint64_t Publish(std::shared_ptr<const ServingState> state);
  std::uint64_t version() const;

 private:
  std::atomic<std::shared_ptr<const ServingState>> current_;
  std::atomic<std::uint64_t> version_{0};
};

// Request-path reader for the cache: keeps the last observed snapshot in a
// thread_local and re-reads only the version per call, so the steady-state
// cost is one atomic load on a read-shared cacheline. This avoids
// atomic<shared_ptr>::load on the hot path, which this toolchain's libstdc++
// implements with an internal packed spin bit — a process-wide serialization
// point when every request on every worker takes it.
//
// The returned pair is consistent: the snapshot was the current state at the
// returned version. Callers registering in-flight work bracket the
// registration between this version and a fresh version() read afterwards;
// Publish stores the state before bumping the version, so an unchanged final
// read proves no publication — and therefore no drain — raced the
// registration.
// Returns a reference to the calling thread's cached snapshot — no refcount
// traffic on the shared control block. The reference stays valid until the
// calling thread's next CurrentCachedWithVersion call; callers that need the
// snapshot across suspension points (the admission record on the request)
// copy it deliberately.
const std::shared_ptr<const ServingState>& CurrentCachedWithVersion(
    TopologyCache& cache, std::uint64_t* version_out);

// Pure routing decisions over a committed ServingState. All functions are
// stateless.
namespace router {

// Primary serving `slot`, or nullptr when the slot is unbound.
const NodeDescriptor* PrimaryForSlot(const ServingState& state,
                                     std::uint16_t slot);

// Client-facing port of `node` chosen by the requesting connection's TLS
// state, mirroring Redis getNodeClientPort/shouldReturnTlsInfo: TLS
// connections get tls_port (falling back to port when the node offers no
// TLS), plaintext connections get port.
std::uint16_t ClientPort(const NodeDescriptor& node, bool connection_tls);

// "host:port" for MOVED targets and discovery entries. MOVED hosts are
// always the concrete advertised address; the empty-host startup-node
// convention is a discovery-only concern handled by the reply builder for
// the self entry.
std::string Endpoint(const NodeDescriptor& node, bool connection_tls);

}  // namespace router

}  // namespace keylane::cluster
