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

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lavik/cluster/topology.h"

namespace lavik::cluster {
namespace {

// 40 lowercase hex chars, as Redis Cluster discovery writes them. TestNodeId(0)
// stays all-zero; tests number real nodes from 1.
NodeId TestNodeId(unsigned n) {
  std::string id(40, '0');
  for (int i = 39; n != 0; n >>= 4, --i) {
    id[static_cast<std::size_t>(i)] = "0123456789abcdef"[n & 0xF];
  }
  return *NodeId::Parse(id);
}

TEST(NodeIdTest, ParsesAndFormatsCanonicalRedisIdentity) {
  constexpr std::string_view kHex = "0123456789abcdef0123456789abcdef01234567";
  const std::optional<NodeId> id = NodeId::Parse(kHex);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(id->ToHexString(), kHex);
  EXPECT_EQ(id->bytes().front(), 0x01);
  EXPECT_EQ(id->bytes().back(), 0x67);

  std::string appended = "id=";
  id->AppendHexTo(&appended);
  EXPECT_EQ(appended, std::string("id=") + std::string(kHex));
}

TEST(NodeIdTest, DistinguishesAbsentFromAnAllZeroIdentity) {
  const NodeId absent;
  const std::optional<NodeId> zero = NodeId::Parse(std::string(40, '0'));
  ASSERT_TRUE(zero.has_value());
  EXPECT_TRUE(absent.empty());
  EXPECT_FALSE(zero->empty());
  EXPECT_NE(absent, *zero);
  EXPECT_TRUE(absent.ToHexString().empty());
  EXPECT_EQ(zero->ToHexString(), std::string(40, '0'));
}

NodeDescriptor MakeNode(unsigned n, std::uint16_t port = 7000) {
  NodeDescriptor node;
  node.node_id_ = TestNodeId(n);
  node.SetHost("127.0.0.1");
  node.port_ = port;
  return node;
}

TEST(NodeDescriptorTest, PreservesInlineAndOverflowHosts) {
  NodeDescriptor node;
  node.SetHost("255.255.255.255");
  EXPECT_EQ(node.host(), "255.255.255.255");

  constexpr std::string_view kLongHost = "redis-primary.example.internal";
  node.SetHost(kLongHost);
  EXPECT_EQ(node.host(), kLongHost);
}

GroupView MakeGroup(std::string group_id, NodeIndex primary_node_index,
                    std::vector<SlotRange> slots) {
  GroupView group;
  group.group_id_ = std::move(group_id);
  group.primary_node_index_ = primary_node_index;
  group.slot_ranges_ = std::move(slots);
  return group;
}

// Single group owning every slot; `mutate` customizes the group before
// building (readiness/grant/term variants).
template <typename Mutate>
std::shared_ptr<const ServingState> MakeState(std::uint64_t epoch,
                                              Mutate&& mutate) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(epoch).SetSelfNodeIndex(0).AddNode(MakeNode(1));
  GroupView group = MakeGroup("g1", 0, {{0, kSlotCount - 1}});
  mutate(group);
  builder.AddGroup(std::move(group));
  auto result = builder.Build();
  EXPECT_TRUE(result.ok()) << result.status().message();
  return *std::move(result);
}

std::shared_ptr<const ServingState> MakeState(std::uint64_t epoch = 1) {
  return MakeState(epoch, [](GroupView&) {});
}

// Two primaries split the slot space at 5460/5461, the classic Redis Cluster
// test topology.
ServingStateBuilder MakeTwoGroupBuilder() {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(7)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 0, {{0, 5460}}))
      .AddGroup(MakeGroup("g2", 1, {{5461, 16383}}));
  return builder;
}

TEST(ServingStateBuilderTest, RejectsMalformedNodeIds) {
  for (std::string bad :
       {std::string(39, 'a'), std::string(41, 'a'), std::string(40, 'A'),
        std::string(40, 'g'), std::string()}) {
    EXPECT_FALSE(NodeId::Parse(bad).has_value())
        << "accepted malformed node id '" << bad << "'";
  }
  ServingStateBuilder builder;
  builder.AddNode(NodeDescriptor{});
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsZeroInFlightStripes) {
  ServingStateBuilder builder;
  builder.SetInFlightStripeCount(0);
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsDuplicateNodeId) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddNode(MakeNode(1));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsEmptyAndDuplicateGroupId) {
  {
    ServingStateBuilder builder;
    builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("", 0, {}));
    EXPECT_FALSE(builder.Build().ok());
  }
  {
    ServingStateBuilder builder;
    builder.AddNode(MakeNode(1))
        .AddGroup(MakeGroup("g1", 0, {}))
        .AddGroup(MakeGroup("g1", 0, {}));
    EXPECT_FALSE(builder.Build().ok());
  }
}

TEST(ServingStateBuilderTest, RejectsUnknownPrimaryNode) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1));
  GroupView group = MakeGroup("g1", 9, {});  // never added as a node
  builder.AddGroup(std::move(group));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsUnknownReplicaNode) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1));
  GroupView group = MakeGroup("g1", 0, {});
  group.replica_node_indices_.push_back(9);
  builder.AddGroup(std::move(group));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsOutOfRangeSelfAndReplicaPrimaryIndices) {
  ServingStateBuilder bad_self;
  bad_self.SetSelfNodeIndex(1)
      .AddNode(MakeNode(1))
      .AddGroup(MakeGroup("g1", 0, {}));
  EXPECT_FALSE(bad_self.Build().ok());

  NodeDescriptor replica = MakeNode(2);
  replica.primary_node_index_ = 9;
  ServingStateBuilder bad_replica;
  bad_replica.AddNode(MakeNode(1))
      .AddNode(std::move(replica))
      .AddGroup(MakeGroup("g1", 0, {}));
  EXPECT_FALSE(bad_replica.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsInconsistentGroupNodeRoles) {
  NodeDescriptor replica = MakeNode(2);
  replica.primary_node_index_ = 0;

  ServingStateBuilder replica_as_primary;
  replica_as_primary.AddNode(MakeNode(1))
      .AddNode(replica)
      .AddGroup(MakeGroup("g1", 1, {}));
  EXPECT_FALSE(replica_as_primary.Build().ok());

  GroupView wrong_group = MakeGroup("g1", 1, {});
  wrong_group.replica_node_indices_.push_back(2);
  ServingStateBuilder mismatched_replica;
  mismatched_replica.AddNode(MakeNode(1))
      .AddNode(MakeNode(3))
      .AddNode(std::move(replica))
      .AddGroup(std::move(wrong_group));
  EXPECT_FALSE(mismatched_replica.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsInvertedSlotRange) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {{100, 99}}));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsSlotRangeBeyondSlotCount) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {{16000, 20000}}));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsOverlappingSlotsAcrossGroups) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 0, {{0, 100}}))
      .AddGroup(MakeGroup("g2", 1, {{100, 200}}));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, RejectsOverlappingRangesWithinGroup) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1))
      .AddGroup(MakeGroup("g1", 0, {{0, 100}, {50, 150}}));
  EXPECT_FALSE(builder.Build().ok());
}

TEST(ServingStateBuilderTest, AddSlotRangeAttachesToExistingGroup) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {}));
  builder.AddSlotRange("g1", {10, 20});
  auto result = builder.Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const std::shared_ptr<const ServingState>& state = *result;
  ASSERT_NE(state->GroupForSlot(10), nullptr);
  EXPECT_EQ(state->GroupForSlot(10)->group_id_, "g1");
  EXPECT_EQ(state->GroupForSlot(20)->group_id_, "g1");
  EXPECT_EQ(state->GroupForSlot(21), nullptr);
  EXPECT_EQ(state->CoveredSlotCount(), 11);
}

TEST(ServingStateBuilderTest, AddSlotRangeToUnknownGroupIsDropped) {
  // The frozen AddSlotRange signature cannot report failure, so the range is
  // dropped and Build stays valid with the slot left uncovered.
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {}));
  builder.AddSlotRange("no-such-group", {0, 100});
  auto result = builder.Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ((*result)->CoveredSlotCount(), 0);
  EXPECT_EQ((*result)->GroupForSlot(0), nullptr);
}

TEST(ServingStateBuilderTest, SelfMayBeAbsent) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {}));
  auto result = builder.Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ((*result)->Self(), nullptr);
}

TEST(ServingStateTest, AccessorsRoundTrip) {
  ServingStateBuilder builder = MakeTwoGroupBuilder();
  NodeDescriptor replica = MakeNode(3, 7003);
  replica.primary_node_index_ = 0;
  builder.AddNode(std::move(replica));
  auto result = builder.Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const std::shared_ptr<const ServingState>& state = *result;

  EXPECT_EQ(state->topology_epoch(), 7);
  EXPECT_EQ(state->SelfNodeIndex(), 0);
  ASSERT_NE(state->Self(), nullptr);
  EXPECT_EQ(state->Self()->node_id_, TestNodeId(1));
  EXPECT_EQ(state->NodeAt(0), state->Self());
  EXPECT_EQ(state->NodeAt(kNoNodeIndex), nullptr);
  EXPECT_EQ(state->NodeAt(99), nullptr);

  ASSERT_NE(state->FindNode(TestNodeId(3)), nullptr);
  EXPECT_EQ(state->FindNode(TestNodeId(3))->port_, 7003);
  EXPECT_FALSE(state->FindNode(TestNodeId(3))->is_primary());
  EXPECT_EQ(state->FindNode(TestNodeId(9)), nullptr);

  ASSERT_NE(state->FindGroup("g2"), nullptr);
  EXPECT_EQ(state->FindGroup("g2")->primary_node_index_, 1);
  EXPECT_EQ(state->FindGroup("nope"), nullptr);

  EXPECT_EQ(state->Nodes().size(), 3);
  EXPECT_EQ(state->Groups().size(), 2);

  ASSERT_NE(state->GroupForSlot(0), nullptr);
  EXPECT_EQ(state->GroupForSlot(0)->group_id_, "g1");
  EXPECT_EQ(state->GroupForSlot(5460)->group_id_, "g1");
  EXPECT_EQ(state->GroupForSlot(5461)->group_id_, "g2");
  EXPECT_EQ(state->GroupForSlot(16383)->group_id_, "g2");
}

TEST(ServingStateTest, CoverageCompleteWhenAllSlotsBound) {
  auto result = MakeTwoGroupBuilder().Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_TRUE((*result)->CoverageComplete());
  EXPECT_EQ((*result)->CoveredSlotCount(), kSlotCount);
}

TEST(ServingStateTest, CoverageGapIsReported) {
  ServingStateBuilder builder;
  builder.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {{0, 5460}}));
  auto result = builder.Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const std::shared_ptr<const ServingState>& state = *result;
  EXPECT_FALSE(state->CoverageComplete());
  EXPECT_EQ(state->CoveredSlotCount(), 5461);
  EXPECT_EQ(state->GroupForSlot(5461), nullptr);
  EXPECT_EQ(state->GroupForSlot(16383), nullptr);
}

TEST(ServingStateTest, FullyReadyTracksStorageAndPopulation) {
  EXPECT_TRUE(MakeState()->FullyReady());
  EXPECT_FALSE(MakeState(1, [](GroupView& g) {
                 g.storage_ready_ = false;
               })->FullyReady());
  EXPECT_FALSE(MakeState(1, [](GroupView& g) {
                 g.population_ready_ = false;
               })->FullyReady());
  // Fencing is an authority concern, not a readiness one.
  EXPECT_TRUE(
      MakeState(1, [](GroupView& g) { g.granted_ = false; })->FullyReady());
}

TEST(ServingStateTest, ContentHashIsOrderIndependent) {
  // Same semantic content, built with nodes, groups, and slot ranges in
  // different orders — including differently partitioned but equivalent
  // ranges.
  NodeDescriptor replica_a = MakeNode(3);
  replica_a.primary_node_index_ = 0;
  GroupView group_a = MakeGroup("g1", 0, {{0, 5460}});
  group_a.replica_node_indices_.push_back(2);
  ServingStateBuilder a;
  a.SetTopologyEpoch(7)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddNode(std::move(replica_a))
      .AddGroup(std::move(group_a))
      .AddGroup(MakeGroup("g2", 1, {{5461, 16383}}));

  NodeDescriptor replica_b = MakeNode(3);
  replica_b.primary_node_index_ = 2;
  GroupView reordered_group_a = MakeGroup("g1", 2, {{0, 2000}, {2001, 5460}});
  reordered_group_a.replica_node_indices_.push_back(0);
  ServingStateBuilder b;
  b.SetTopologyEpoch(7)
      .SetSelfNodeIndex(2)
      .AddNode(std::move(replica_b))
      .AddNode(MakeNode(2))
      .AddNode(MakeNode(1))
      .AddGroup(MakeGroup("g2", 1, {{10000, 16383}, {5461, 9999}}))
      .AddGroup(std::move(reordered_group_a));
  auto result_a = a.Build();
  auto result_b = b.Build();
  ASSERT_TRUE(result_a.ok()) << result_a.status().message();
  ASSERT_TRUE(result_b.ok()) << result_b.status().message();
  EXPECT_EQ((*result_a)->content_hash(), (*result_b)->content_hash());
}

TEST(ServingStateTest, ContentHashCoversEverySemanticField) {
  const std::uint64_t base = MakeState()->content_hash();
  EXPECT_NE(MakeState(2)->content_hash(), base);  // topology epoch

  ServingStateBuilder other_self;
  other_self.SetTopologyEpoch(1)
      .SetSelfNodeIndex(1)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 0, {{0, kSlotCount - 1}}));
  auto other = other_self.Build();
  ASSERT_TRUE(other.ok()) << other.status().message();
  EXPECT_NE((*other)->content_hash(), base);  // self id

  EXPECT_NE(
      MakeState(1, [](GroupView& g) { g.group_term_ = 9; })->content_hash(),
      base);
  EXPECT_NE(
      MakeState(1, [](GroupView& g) { g.granted_ = false; })->content_hash(),
      base);
  EXPECT_NE(MakeState(1, [](GroupView& g) { g.population_ready_ = false; })
                ->content_hash(),
            base);
  EXPECT_NE(MakeState(1, [](GroupView& g) { g.storage_ready_ = false; })
                ->content_hash(),
            base);

  // Slot ownership: move slot 100 from g1 to g2.
  ServingStateBuilder moved;
  moved.SetTopologyEpoch(1)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 0, {{0, 99}, {101, 16383}}))
      .AddGroup(MakeGroup("g2", 1, {{100, 100}}));
  auto moved_result = moved.Build();
  ASSERT_TRUE(moved_result.ok()) << moved_result.status().message();
  EXPECT_NE((*moved_result)->content_hash(), base);
}

TEST(ServingStateTest, AuthorityToken) {
  const std::shared_ptr<const ServingState> base = MakeState();
  const std::uint64_t token = base->AuthorityToken("g1");
  EXPECT_NE(token, 0);
  EXPECT_EQ(base->AuthorityToken("unknown-group"), 0);

  // Stable for identical content.
  EXPECT_EQ(MakeState()->AuthorityToken("g1"), token);

  // Changes in every covered dimension: owner identity, term, grant, and both
  // readiness flags.
  ServingStateBuilder other_primary;
  other_primary.SetTopologyEpoch(1)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 1, {{0, kSlotCount - 1}}));
  auto other_result = other_primary.Build();
  ASSERT_TRUE(other_result.ok()) << other_result.status().message();
  EXPECT_NE((*other_result)->AuthorityToken("g1"), token);

  EXPECT_NE(MakeState(1, [](GroupView& g) { g.group_term_ = 2; })
                ->AuthorityToken("g1"),
            token);
  EXPECT_NE(MakeState(1, [](GroupView& g) { g.granted_ = false; })
                ->AuthorityToken("g1"),
            token);
  EXPECT_NE(MakeState(1, [](GroupView& g) { g.population_ready_ = false; })
                ->AuthorityToken("g1"),
            token);
  EXPECT_NE(MakeState(1, [](GroupView& g) { g.storage_ready_ = false; })
                ->AuthorityToken("g1"),
            token);
}

TEST(TopologyCacheTest, PublishSharesCellsOnlyForTokenUnchangedGroups) {
  TopologyCache cache;
  auto first = MakeTwoGroupBuilder().Build();
  ASSERT_TRUE(first.ok()) << first.status().message();
  cache.Publish(*first);
  const std::shared_ptr<const ServingState> replaced = cache.Current();

  // Hold one in-flight guard per group against the published snapshot.
  GroupInFlight* cell_g1 = replaced->InFlightCellForSlot(5);
  GroupInFlight* cell_g2 = replaced->InFlightCellForSlot(5461);
  ASSERT_NE(cell_g1, nullptr);
  ASSERT_NE(cell_g2, nullptr);
  constexpr std::size_t stripe = 0;
  const InFlightGuard guard_g1(*cell_g1, stripe);
  const InFlightGuard guard_g2(*cell_g2, stripe);

  // Fence g2 (higher epoch, grant dropped); g1's authority is untouched.
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(8)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(1))
      .AddNode(MakeNode(2))
      .AddGroup(MakeGroup("g1", 0, {{0, 5460}}));
  GroupView fenced_g2 = MakeGroup("g2", 1, {{5461, 16383}});
  fenced_g2.granted_ = false;
  builder.AddGroup(std::move(fenced_g2));
  auto second = builder.Build();
  ASSERT_TRUE(second.ok()) << second.status().message();
  cache.Publish(*second);
  const std::shared_ptr<const ServingState> current = cache.Current();
  ASSERT_NE(current, replaced);

  // g1 keeps the shared cell: the in-flight guard is visible from both
  // snapshots, so draining either covers it.
  EXPECT_EQ(current->GroupInFlightCount("g1"), 1);
  EXPECT_EQ(replaced->GroupInFlightCount("g1"), 1);
  // g2's authority changed: the new snapshot starts a fresh cell, while the
  // still-running guard remains visible on the replaced snapshot a fence
  // publisher drains.
  EXPECT_EQ(current->GroupInFlightCount("g2"), 0);
  EXPECT_EQ(replaced->GroupInFlightCount("g2"), 1);
}

TEST(TopologyCacheTest, EmptyGroupTermChangesDiscoveryButNotServingAuthority) {
  ServingStateBuilder builder = MakeTwoGroupBuilder();
  builder.IncludeGroupTerm(11);
  auto first = builder.Build();
  ASSERT_TRUE(first.ok()) << first.status();
  TopologyCache cache;
  EXPECT_EQ(cache.Publish(*first), 1U);

  // Only an unrepresented Group's term advances; every routable Group stays
  // identical. Publication must still refresh Redis's cluster_current_epoch.
  builder.IncludeGroupTerm(13).IncludeGroupTerm(2);
  auto second = builder.Build();
  ASSERT_TRUE(second.ok()) << second.status();
  EXPECT_EQ((*second)->max_group_term(), 13U);
  EXPECT_NE((*first)->content_hash(), (*second)->content_hash());
  EXPECT_EQ((*first)->AuthorityToken("g1"), (*second)->AuthorityToken("g1"));
  EXPECT_EQ(cache.Publish(*second), 2U);
  EXPECT_EQ(cache.Current()->max_group_term(), 13U);
}

TEST(TopologyCacheTest, ContentIdenticalRepublishKeepsInFlightCells) {
  TopologyCache cache;
  cache.Publish(MakeState(1));
  const std::shared_ptr<const ServingState> state = cache.Current();
  GroupInFlight* cell = state->InFlightCellForSlot(5);
  ASSERT_NE(cell, nullptr);
  const InFlightGuard guard(*cell, /*stripe=*/0);

  // A no-op republish must not disturb in-flight accounting.
  cache.Publish(MakeState(1));
  EXPECT_EQ(cache.Current()->GroupInFlightCount("g1"), 1);
  EXPECT_EQ(cache.version(), 1);
}

TEST(TopologyCacheTest, PublishSwapsAndBumpsVersion) {
  TopologyCache cache;
  EXPECT_EQ(cache.Current(), nullptr);
  EXPECT_EQ(cache.version(), 0);

  const std::shared_ptr<const ServingState> first = MakeState(1);
  EXPECT_EQ(cache.Publish(first), 1);
  EXPECT_EQ(cache.Current(), first);
  EXPECT_EQ(cache.version(), 1);

  const std::shared_ptr<const ServingState> second = MakeState(2);
  EXPECT_EQ(cache.Publish(second), 2);
  EXPECT_EQ(cache.Current(), second);
  EXPECT_EQ(cache.version(), 2);
}

TEST(TopologyCacheTest, ContentIdenticalRepublishKeepsVersionAndSnapshot) {
  TopologyCache cache;
  const std::shared_ptr<const ServingState> first = MakeState(1);
  const std::shared_ptr<const ServingState> same_content = MakeState(1);
  ASSERT_EQ(first->content_hash(), same_content->content_hash());

  EXPECT_EQ(cache.Publish(first), 1);
  EXPECT_EQ(cache.Publish(same_content), 1);
  // The older snapshot is kept, not swapped.
  EXPECT_EQ(cache.Current(), first);
  EXPECT_EQ(cache.version(), 1);
}

TEST(TopologyCacheTest, VersionsIncreaseMonotonicallyUnderOnePublisher) {
  TopologyCache cache;
  for (std::uint64_t i = 1; i <= 10; ++i) {
    EXPECT_EQ(cache.Publish(MakeState(i)), i);
  }
  EXPECT_EQ(cache.version(), 10);
}

TEST(TopologyCacheTest, ConcurrentPublishAndCurrentSmoke) {
  TopologyCache cache;
  constexpr int kThreads = 4;
  constexpr int kPublishesPerThread = 50;
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([t, &start, &cache] {
      while (!start.load(std::memory_order_acquire)) {
      }
      for (int i = 0; i < kPublishesPerThread; ++i) {
        // Distinct epochs make every published snapshot unique, so every
        // Publish must bump the version exactly once.
        cache.Publish(MakeState(
            static_cast<std::uint64_t>(t * kPublishesPerThread + i + 1)));
        EXPECT_NE(cache.Current(), nullptr);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (std::thread& thread : threads) thread.join();

  EXPECT_NE(cache.Current(), nullptr);
  EXPECT_EQ(cache.version(), kThreads * kPublishesPerThread);
}

TEST(ClusterRouterTest, PrimaryForSlot) {
  auto result = MakeTwoGroupBuilder().Build();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const std::shared_ptr<const ServingState>& state = *result;

  const NodeDescriptor* owner = router::PrimaryForSlot(*state, 0);
  ASSERT_NE(owner, nullptr);
  EXPECT_EQ(owner->node_id_, TestNodeId(1));
  EXPECT_EQ(router::PrimaryForSlot(*state, 16383)->node_id_, TestNodeId(2));

  // Unbound slot: no group, hence no primary.
  ServingStateBuilder partial;
  partial.AddNode(MakeNode(1)).AddGroup(MakeGroup("g1", 0, {{0, 100}}));
  auto partial_result = partial.Build();
  ASSERT_TRUE(partial_result.ok()) << partial_result.status().message();
  EXPECT_EQ(router::PrimaryForSlot(**partial_result, 101), nullptr);
}

TEST(ClusterRouterTest, ClientPortFollowsConnectionTlsState) {
  NodeDescriptor node = MakeNode(1, 7000);
  node.tls_port_ = 7443;
  EXPECT_EQ(router::ClientPort(node, /*connection_tls=*/true), 7443);
  EXPECT_EQ(router::ClientPort(node, /*connection_tls=*/false), 7000);

  // TLS connection to a node that offers no TLS port falls back to port.
  node.tls_port_ = 0;
  EXPECT_EQ(router::ClientPort(node, /*connection_tls=*/true), 7000);
}

TEST(ClusterRouterTest, EndpointFormatsHostAndPort) {
  NodeDescriptor node = MakeNode(1, 7000);
  node.tls_port_ = 7443;
  EXPECT_EQ(router::Endpoint(node, /*connection_tls=*/false), "127.0.0.1:7000");
  EXPECT_EQ(router::Endpoint(node, /*connection_tls=*/true), "127.0.0.1:7443");

  node.SetHost("::1");
  node.tls_port_ = 0;
  EXPECT_EQ(router::Endpoint(node, /*connection_tls=*/false), "[::1]:7000");
}

TEST(TopologyCacheTest, CachedReaderTracksVersionAndSnapshot) {
  TopologyCache cache;
  {
    std::uint64_t version = 1;
    const auto& state = CurrentCachedWithVersion(cache, &version);
    EXPECT_EQ(state, nullptr);
    EXPECT_EQ(version, 0);
  }
  const std::shared_ptr<const ServingState> first = MakeState(1);
  cache.Publish(first);
  {
    std::uint64_t version = 0;
    const auto& state = CurrentCachedWithVersion(cache, &version);
    EXPECT_EQ(state, first);
    EXPECT_EQ(version, 1);
    // A second read on the same thread hits the cached entry with the same
    // consistent pair.
    std::uint64_t version2 = 0;
    const auto& again = CurrentCachedWithVersion(cache, &version2);
    EXPECT_EQ(again, first);
    EXPECT_EQ(version2, 1);
  }
  const std::shared_ptr<const ServingState> second = MakeState(2);
  cache.Publish(second);
  {
    std::uint64_t version = 0;
    const auto& state = CurrentCachedWithVersion(cache, &version);
    EXPECT_EQ(state, second);
    EXPECT_EQ(version, 2);
  }
}

TEST(TopologyCacheTest, CachedReaderIsScopedToCacheInstance) {
  TopologyCache first_cache;
  TopologyCache second_cache;
  const std::shared_ptr<const ServingState> first = MakeState(11);
  const std::shared_ptr<const ServingState> second = MakeState(22);
  first_cache.Publish(first);
  second_cache.Publish(second);

  std::uint64_t version = 0;
  EXPECT_EQ(CurrentCachedWithVersion(first_cache, &version), first);
  EXPECT_EQ(version, 1);
  EXPECT_EQ(CurrentCachedWithVersion(second_cache, &version), second);
  EXPECT_EQ(version, 1);
}

TEST(TopologyCacheTest, CachedOwnersIsolateRequestCopiesAndOutliveWorkers) {
  TopologyCache cache;
  auto published = MakeState(1);
  std::weak_ptr<const ServingState> retired = published;
  cache.Publish(published);
  std::array<std::shared_ptr<const ServingState>, 2> owners;
  std::vector<std::thread> workers;
  for (std::size_t i = 0; i < owners.size(); ++i) {
    workers.emplace_back([&, i] {
      std::uint64_t version = 0;
      owners[i] = CurrentCachedWithVersion(cache, &version);
      const auto& hit = CurrentCachedWithVersion(cache, &version);
      EXPECT_FALSE(owners[i].owner_before(hit));
      EXPECT_FALSE(hit.owner_before(owners[i]));
    });
  }
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(owners[0].get(), published.get());
  EXPECT_EQ(owners[1].get(), published.get());
  EXPECT_TRUE(owners[0].owner_before(owners[1]) ||
              owners[1].owner_before(owners[0]));
  const auto global_references = published.use_count();
  {
    std::vector<std::shared_ptr<const ServingState>> requests(100, owners[0]);
    EXPECT_EQ(published.use_count(), global_references);
  }

  // Both originating threads (and their TLS caches) have exited. Retained
  // requests must still own the old snapshot after publication replaces it,
  // and releasing them on another thread must release that snapshot too.
  cache.Publish(MakeState(2));
  published.reset();
  ASSERT_FALSE(retired.expired());
  EXPECT_EQ(owners[0]->topology_epoch(), 1);
  owners[0].reset();
  EXPECT_FALSE(retired.expired());
  owners[1].reset();
  EXPECT_TRUE(retired.expired());
}

TEST(TopologyCacheTest, PublicationSequenceBracketsCompletedPairs) {
  TopologyCache cache;
  EXPECT_EQ(cache.publication_sequence(), 0);
  EXPECT_EQ(cache.Publish(MakeState(1)), 1);
  EXPECT_EQ(cache.publication_sequence(), 2);
  EXPECT_EQ(cache.Publish(MakeState(2)), 2);
  EXPECT_EQ(cache.publication_sequence(), 4);
}

TEST(TopologyCacheTest, CachedReaderConsistentUnderConcurrentPublish) {
  TopologyCache cache;
  std::atomic<bool> stop{false};
  std::atomic<bool> mismatch{false};
  std::thread publisher([&] {
    for (std::uint64_t i = 1; i <= 2000 && !mismatch.load(); ++i) {
      cache.Publish(MakeState(i));
    }
    stop.store(true, std::memory_order_release);
  });
  // The reader runs on a fresh thread so its thread_local entry starts empty
  // (the cache is per-thread and not scoped to a TopologyCache instance).
  // This publish sequence yields version == topology epoch, so any
  // inconsistent (state, version) pairing is directly visible.
  std::thread reader([&] {
    while (!stop.load(std::memory_order_acquire)) {
      std::uint64_t version = 0;
      const auto& state = CurrentCachedWithVersion(cache, &version);
      if (state != nullptr && state->topology_epoch() != version) {
        mismatch.store(true);
        break;
      }
    }
  });
  publisher.join();
  reader.join();
  EXPECT_FALSE(mismatch.load());
}

}  // namespace
}  // namespace lavik::cluster
