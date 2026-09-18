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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "lavik/cluster/control_types.h"

namespace lavik::cluster {

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

// Compact index stored in the fixed 16,384-entry slot map. The sentinel
// consumes the largest value, leaving 65,535 representable groups per
// snapshot, well above the number that can own at least one Redis slot.
using GroupIndex = std::uint16_t;
inline constexpr GroupIndex kNoGroupIndex =
    std::numeric_limits<GroupIndex>::max();
static_assert(kSlotCount < kNoGroupIndex);

// One cluster node as the data plane sees it, built from an authenticated
// complete desired state. Each node retains its independently committed
// plaintext and TLS client endpoints.
struct alignas(64) NodeDescriptor {
  NodeId node_id_;              // stable across restarts
  bool link_connected_ = true;  // projected diagnostic; not consulted in v1
  std::uint16_t port_ = 0;
  std::uint16_t tls_port_ = 0;  // 0 = TLS not offered
  // kNoNodeIndex identifies a primary; replicas point at their primary in
  // the same ServingState::Nodes() table.
  NodeIndex primary_node_index_ = kNoNodeIndex;
  // Read-only projection of the member Group's term for Redis discovery.
  // Retained even when fencing removes the Group from the routing table;
  // this is neither an independent counter nor proof of serving authority.
  std::uint64_t group_term_ = 0;

  // Most advertised addresses (including every IPv4 literal) stay inside the
  // descriptor. Longer hostnames and IPv6 literals retain their full value by
  // using InlinedVector's overflow allocation.
  absl::InlinedVector<char, 16> host_;

  // Replaces the advertised host without requiring callers to depend on its
  // compact storage representation.
  void SetHost(std::string_view host) {
    host_.assign(host.begin(), host.end());
  }
  // Returns the advertised host for hashing, comparison, and wire formatting.
  std::string_view host() const noexcept {
    if (host_.empty()) return {};
    return std::string_view(host_.data(), host_.size());
  }

  bool is_primary() const noexcept {
    return primary_node_index_ == kNoNodeIndex;
  }
};

// Node tables are traversed on routing and discovery paths. Keeping each
// descriptor in one aligned cache line prevents adjacent entries from sharing
// a line while preserving support for arbitrarily long advertised hosts.
static_assert(sizeof(NodeDescriptor) == 64);
static_assert(alignof(NodeDescriptor) == 64);

// Redis Cluster slot range, both ends inclusive.
struct SlotRange {
  std::uint16_t first_ = 0;
  std::uint16_t last_ = 0;
};

// Striped in-flight mutation counter for one group, owned by the published
// ServingState. One stripe is allocated for every request worker, so
// registration is one directly indexed atomic increment: no lock, request-
// path allocation, modulo, or cross-thread cacheline contention. Copies share
// the same stripe allocation so token-equivalent snapshots drain together.
class GroupInFlight {
 public:
  explicit GroupInFlight(std::size_t stripe_count)
      : stripes_(std::make_shared_for_overwrite<Stripe[]>(stripe_count)),
        stripe_count_(stripe_count) {
    assert(stripe_count_ != 0);
  }

  // Number of configured request-worker stripes in this cell.
  std::size_t StripeCount() const noexcept { return stripe_count_; }

  // Enter is seq_cst on purpose: paired with the registrant's subsequent
  // TopologyCache::version() load and the publisher's store-then-bump order it
  // forms a Dekker handshake — a drain that starts after a revoking publish
  // either observes this registration, or the registrant observes the
  // publication and rolls back (see TopologyCache::Publish). `stripe` is the
  // configured Bycorf worker id and must be less than StripeCount().
  void Enter(std::size_t stripe) noexcept {
    assert(stripe < stripe_count_);
    stripes_[stripe].value_.fetch_add(1);
  }
  // Release is sufficient for Exit: an Exit not yet visible to the drain only
  // makes the drain wait longer, never miss an execution.
  void Exit(std::size_t stripe) noexcept {
    assert(stripe < stripe_count_);
    stripes_[stripe].value_.fetch_sub(1, std::memory_order_release);
  }
  // Drain-side aggregate; sums every stripe. Never on the request path.
  std::uint64_t Total() const noexcept {
    std::uint64_t total = 0;
    for (std::size_t i = 0; i < stripe_count_; ++i) {
      // seq_cst so the drain participates in the Dekker handshake described
      // at Enter().
      total += stripes_[i].value_.load();
    }
    return total;
  }

 private:
  struct alignas(64) Stripe {
    std::atomic<std::int64_t> value_{0};
  };
  static_assert(sizeof(Stripe) == 64);

  // make_shared_for_overwrite<T[]> keeps the control block and variable-length
  // stripe array in one allocation; Stripe's member initializer still zeros
  // every counter. GroupInFlight itself stays a small, copyable handle in
  // ServingState's cell vector, so dynamic sizing adds no request-path pointer
  // indirection compared with shared_ptr<GroupInFlight> plus an inline array.
  std::shared_ptr<Stripe[]> stripes_;
  std::size_t stripe_count_;
};

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
// data plane and supplied by Meta through the node controller.
struct GroupView {
  std::string group_id_;
  NodeIndex primary_node_index_ = kNoNodeIndex;
  // Meta creates a fresh assignment incarnation on remove/re-add. Authority
  // terms are compared only while this identity is unchanged.
  AssignmentId assignment_id_;
  // This is the committed desired grant, not a live lease. A Meta-managed
  // primary serves only while AuthorityGuard also holds an unexpired lease.
  bool granted_ = true;  // false = fenced: this group must not serve
  bool population_ready_ = true;
  bool storage_ready_ = true;
  // A controlled failover pause closes only new client-side mutations. It is
  // deliberately separate from `granted_`: the old owner keeps its finite
  // lease, reads, and established replication exports while NodeControl
  // drains requests admitted before the pause publication.
  bool mutations_paused_ = false;
  std::uint64_t group_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  std::vector<NodeIndex> replica_node_indices_;
  std::vector<SlotRange> slot_ranges_;  // owned slots, validated at Build
};

// Immutable committed routing + authority + readiness snapshot.
class ServingState {
 public:
  std::uint64_t topology_epoch() const { return topology_epoch_; }
  // Derived Redis discovery value over every projected Group, including
  // grantless Groups with no members/routing entry. Not an authority clock.
  std::uint64_t max_group_term() const { return max_group_term_; }
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
  // Builder setting retained so installer-driven readiness/fence republishes
  // preserve the request-worker stripe layout.
  std::size_t InFlightStripeCount() const { return in_flight_stripe_count_; }
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
  // calling thread's stripe. The admitted snapshot keeps this handle alive
  // for every guard, while token-unchanged later snapshots share its
  // underlying stripe allocation (see TopologyCache::Publish).
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
  std::uint64_t max_group_term_ = 0;
  std::uint64_t content_hash_ = 0;
  std::vector<NodeDescriptor> nodes_;
  std::vector<GroupView> groups_;
  NodeIndex self_node_index_ = kNoNodeIndex;
  // slot -> index into groups_, kNoGroupIndex when unbound. A 16-bit entry
  // keeps the hot, fixed-size routing table at 32 KiB.
  std::array<GroupIndex, kSlotCount> slot_to_group_;
  std::uint32_t covered_slots_ = 0;
  std::size_t in_flight_stripe_count_ = 1;
  // Precomputed per-group authority tokens, parallel to groups_. Computed
  // once at Build so the request path never hashes or scans for them.
  std::vector<std::uint64_t> group_tokens_;
  // One cell handle per entry in groups_. Build gives each handle a fresh
  // stripe allocation; TopologyCache::Publish then copies the replaced
  // snapshot's handle for every group whose authority token is unchanged, so
  // executions admitted under either snapshot drain together. Mutable because
  // publication installs the sharing after the state is built but before it
  // becomes visible to readers.
  mutable std::vector<GroupInFlight> in_flight_cells_;

  // Implements the cell sharing described on in_flight_cells_; called by
  // TopologyCache::Publish before the state is stored.
  void ShareInFlightCellsFrom(const ServingState& previous) const;
  friend class TopologyCache;
};

class ServingStateBuilder {
 public:
  ServingStateBuilder& SetTopologyEpoch(std::uint64_t epoch);
  // Includes non-routing/empty Groups in the derived maximum. AddNode and
  // AddGroup also include their terms; callers never increment this value.
  ServingStateBuilder& IncludeGroupTerm(std::uint64_t term);
  ServingStateBuilder& SetSelfNodeIndex(NodeIndex node_index);
  // Configures one in-flight stripe per request worker. The default keeps
  // standalone model construction convenient; production adapters must pass
  // the server's configured worker count.
  ServingStateBuilder& SetInFlightStripeCount(std::size_t stripe_count);
  ServingStateBuilder& AddNode(NodeDescriptor node);
  // Takes ownership of the group's slot ranges. Slots may also be attached
  // later via AddSlotRange to an existing group id.
  ServingStateBuilder& AddGroup(GroupView group);
  ServingStateBuilder& AddSlotRange(std::string_view group_id, SlotRange range);

  // Validates: node ids are present and unique; group ids are unique; the
  // snapshot fits the compact group-index space; every node index is in range
  // and describes a consistent primary/replica relationship; slot ranges are
  // in [0, kSlotCount) and non-overlapping. Computes the slot map and content
  // hash. Self may legitimately be absent (validation of self-match is the
  // control adapter's job, since only it knows the match rule).
  absl::StatusOr<std::shared_ptr<const ServingState>> Build() const;

 private:
  std::uint64_t topology_epoch_ = 0;
  std::uint64_t max_group_term_ = 0;
  NodeIndex self_node_index_ = kNoNodeIndex;
  std::size_t in_flight_stripe_count_ = 1;
  std::vector<NodeDescriptor> nodes_;
  std::vector<GroupView> groups_;
};

// Committed local routing snapshot. Publication
// is a single atomic swap, so topology, grants, and readiness always appear
// together. A logical version orders completed publications for observers and
// tests. A separate odd/even publication sequence keeps snapshot/version reads
// and in-flight registration honest: registrants accept only equal even
// sequence reads around their Enter, so a publisher either observes the
// registration while draining or makes the registrant roll it back. Authority
// decisions use ServingState::AuthorityToken, never the global version, so
// unrelated groups republishing does not disturb in-flight writes.
class TopologyCache {
 public:
  TopologyCache();
  // Distinguishes lifetimes even when a new cache reuses the same address.
  std::uint64_t cache_identity() const { return cache_identity_; }
  // nullptr until the first publish; callers treat that as "not ready".
  std::shared_ptr<const ServingState> Current() const;
  // Publishes `state`; a content-identical state is a no-op (same version).
  // Installs the in-flight cell sharing (ServingState::in_flight_cells_)
  // before the store, so the state must not be visible to readers yet. The
  // publication sequence is odd across the state/version update. Returns the
  // current logical version after the call.
  std::uint64_t Publish(std::shared_ptr<const ServingState> state);
  std::uint64_t version() const;
  // Odd while a publication is changing the snapshot/version pair and even
  // otherwise. Request-side registration brackets its in-flight Enter with
  // this token so it cannot accept a snapshot from a half-published pair.
  std::uint64_t publication_sequence() const;

 private:
  // Publications are control-plane events, so serializing writers has no
  // request-path cost and lets publication_sequence_ provide seqlock
  // semantics even if multiple control sources publish concurrently.
  std::mutex publish_mutex_;
  std::atomic<std::shared_ptr<const ServingState>> current_;
  std::atomic<std::uint64_t> version_{0};
  std::atomic<std::uint64_t> publication_sequence_{0};
  const std::uint64_t cache_identity_;
};

// Request-path reader for the cache: keeps the last observed snapshot in a
// thread_local and re-reads only the publication sequence per call, so the
// cost is one atomic load on a read-shared cacheline. This avoids
// atomic<shared_ptr>::load on the hot path, which this toolchain's libstdc++
// implements with an internal packed spin bit — a process-wide serialization
// point when every request on every worker takes it.
//
// The returned pair is consistent: the snapshot was the current state at the
// returned version. Callers registering in-flight work request the optional
// publication sequence and compare it with a fresh publication_sequence()
// afterwards; an unchanged even token proves no publication — and therefore
// no drain — raced the registration.
// Returns a reference to the calling thread's cached snapshot. Each refresh
// creates an independent shared_ptr control block owning one reference to
// the published snapshot; it aliases the same ServingState, without copying
// topology data. Per-request copies update this local block, not the global
// snapshot's count. The count remains atomic for cross-worker admissions.
// The returned reference stays valid until this thread's next call; copying
// it retains the snapshot across suspension, cache refresh, and thread exit.
const std::shared_ptr<const ServingState>& CurrentCachedWithVersion(
    TopologyCache& cache, std::uint64_t* version_out,
    std::uint64_t* publication_sequence_out = nullptr);

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

}  // namespace lavik::cluster
