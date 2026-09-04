#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "keylane/cluster/authority.h"
#include "keylane/cluster/topology.h"

namespace {

using keylane::cluster::Admit;
using keylane::cluster::AuthorityUnchanged;
using keylane::cluster::Decision;
using keylane::cluster::GroupInFlight;
using keylane::cluster::GroupView;
using keylane::cluster::InFlightGuard;
using keylane::cluster::InFlightStripe;
using keylane::cluster::NodeDescriptor;
using keylane::cluster::NodeId;
using keylane::cluster::RequestView;
using keylane::cluster::ServingState;
using keylane::cluster::ServingStateBuilder;
using keylane::cluster::SlotRange;

// 40-hex node ids, as the builder validates.
constexpr std::string_view kNodeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kNodeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kNodeR = "cccccccccccccccccccccccccccccccccccccccc";
constexpr std::string_view kGroupA = "group-a";
constexpr std::string_view kGroupB = "group-b";

// Default topology: group-a (primary A, replica R) owns [0, 9999], group-b
// (primary B) owns [10000, 16383].
constexpr std::uint16_t kSlotInA = 5;
constexpr std::uint16_t kOtherSlotInA = 7;
constexpr std::uint16_t kSlotInB = 10005;
constexpr std::uint16_t kUnboundSlot = 16000;  // only in gap topologies

NodeId ParseNodeId(std::string_view id) {
  const std::optional<NodeId> parsed = NodeId::Parse(id);
  EXPECT_TRUE(parsed.has_value());
  return parsed.value_or(NodeId{});
}

NodeDescriptor MakeNode(std::string_view id, std::string_view host,
                        std::uint16_t port, std::uint16_t tls_port) {
  NodeDescriptor node;
  node.node_id_ = ParseNodeId(id);
  node.host_ = std::string(host);
  node.port_ = port;
  node.tls_port_ = tls_port;
  return node;
}

GroupView MakeGroup(std::string_view id, std::string_view primary,
                    std::uint16_t first_slot, std::uint16_t last_slot) {
  GroupView group;
  group.group_id_ = std::string(id);
  group.primary_node_id_ = ParseNodeId(primary);
  group.slot_ranges_.push_back(SlotRange{first_slot, last_slot});
  return group;
}

GroupView GroupA() {
  GroupView group = MakeGroup(kGroupA, kNodeA, 0, 9999);
  group.replica_node_ids_.push_back(ParseNodeId(kNodeR));
  return group;
}

GroupView GroupB() { return MakeGroup(kGroupB, kNodeB, 10000, 16383); }

// Group-b covers only [10000, 15000], leaving [15001, 16383] unbound.
GroupView GroupBWithGap() { return MakeGroup(kGroupB, kNodeB, 10000, 15000); }

std::shared_ptr<const ServingState> BuildState(std::string_view self,
                                               GroupView group_a,
                                               GroupView group_b) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(1);
  builder.SetSelfNodeId(ParseNodeId(self));
  builder.AddNode(MakeNode(kNodeA, "10.0.0.1", 7000, 17000));
  builder.AddNode(MakeNode(kNodeB, "10.0.0.2", 7001, 17001));
  NodeDescriptor replica = MakeNode(kNodeR, "10.0.0.3", 7002, 17002);
  replica.is_primary_ = false;
  replica.primary_id_ = ParseNodeId(kNodeA);
  builder.AddNode(std::move(replica));
  builder.AddGroup(std::move(group_a));
  builder.AddGroup(std::move(group_b));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

std::shared_ptr<const ServingState> BuildState(std::string_view self) {
  return BuildState(self, GroupA(), GroupB());
}

RequestView MakeRequest(std::span<const std::uint16_t> slots, bool is_write,
                        bool connection_readonly = false,
                        bool loading_allowed = false) {
  RequestView request;
  request.slots_ = slots;
  request.is_write_ = is_write;
  request.connection_readonly_ = connection_readonly;
  request.loading_allowed_ = loading_allowed;
  return request;
}

// MOVED targets always carry the concrete advertised host plus both ports;
// the Redis layer picks the port by the connection's TLS state.
void ExpectMovedToNodeA(const Decision& decision, std::uint16_t slot) {
  EXPECT_EQ(decision.kind_, Decision::Kind::kMoved);
  EXPECT_EQ(decision.moved_slot_, slot);
  EXPECT_EQ(decision.moved_host_, "10.0.0.1");
  EXPECT_EQ(decision.moved_port_, 7000);
  EXPECT_EQ(decision.moved_tls_port_, 17000);
}

TEST(ClusterAuthorityTest, PrimaryServesKeyedReadAndWriteWhenReady) {
  const auto state = BuildState(kNodeA);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, /*is_write=*/false)).kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, /*is_write=*/true)).kind_,
            Decision::Kind::kServe);
}

TEST(ClusterAuthorityTest, NullStateLoadsEverythingExceptAllowlist) {
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_EQ(Admit(nullptr, MakeRequest(slots, true)).kind_,
            Decision::Kind::kLoading);
  EXPECT_EQ(Admit(nullptr, MakeRequest({}, false)).kind_,
            Decision::Kind::kLoading);
  EXPECT_EQ(Admit(nullptr, MakeRequest(slots, true, false,
                                       /*loading_allowed=*/true))
                .kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(Admit(nullptr, MakeRequest({}, false, false,
                                       /*loading_allowed=*/true))
                .kind_,
            Decision::Kind::kServe);
}

TEST(ClusterAuthorityTest, UnreadyOwningGroupLoadsKeyedRequests) {
  for (const bool break_storage : {true, false}) {
    GroupView group_a = GroupA();
    if (break_storage) {
      group_a.storage_ready_ = false;
    } else {
      group_a.population_ready_ = false;
    }
    const auto state = BuildState(kNodeA, group_a, GroupB());
    const std::array<std::uint16_t, 1> slots{kSlotInA};
    EXPECT_EQ(Admit(state.get(), MakeRequest(slots, false)).kind_,
              Decision::Kind::kLoading);
    EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
              Decision::Kind::kLoading);
    // Allowlisted commands still serve.
    EXPECT_EQ(Admit(state.get(),
                    MakeRequest(slots, true, false, /*loading_allowed=*/true))
                  .kind_,
              Decision::Kind::kServe);
  }
}

TEST(ClusterAuthorityTest, KeyedReadinessIsPerGroup) {
  // group-b recovering does not stall traffic owned by the healthy group-a.
  GroupView group_b = GroupB();
  group_b.storage_ready_ = false;
  const auto state = BuildState(kNodeA, GroupA(), group_b);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kServe);
  // ... but no-key commands consult the aggregate and load.
  EXPECT_EQ(Admit(state.get(), MakeRequest({}, false)).kind_,
            Decision::Kind::kLoading);
  EXPECT_EQ(Admit(state.get(),
                  MakeRequest({}, false, false, /*loading_allowed=*/true))
                .kind_,
            Decision::Kind::kServe);
}

TEST(ClusterAuthorityTest, NoKeyCommandsServeWhenReady) {
  const auto state = BuildState(kNodeA);
  // Empty slots also covers key-extraction failure (EVAL/LMPOP with invalid
  // numkeys): the caller reports it as an empty slot set, the command itself
  // produces the native argument error.
  EXPECT_EQ(Admit(state.get(), MakeRequest({}, true)).kind_,
            Decision::Kind::kServe);
}

TEST(ClusterAuthorityTest, UnboundFirstSlotYieldsClusterDown) {
  const auto state = BuildState(kNodeA, GroupA(), GroupBWithGap());
  const std::array<std::uint16_t, 1> slots{kUnboundSlot};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, false)).kind_,
            Decision::Kind::kClusterDownUnbound);
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kClusterDownUnbound);
}

TEST(ClusterAuthorityTest, UnboundFirstSlotOutranksCrossSlot) {
  // Redis getNodeByQuery order: first-key coverage is checked before the
  // single-slot rule, so this is CLUSTERDOWN, not CROSSSLOT.
  const auto state = BuildState(kNodeA, GroupA(), GroupBWithGap());
  const std::array<std::uint16_t, 2> slots{kUnboundSlot, kSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kClusterDownUnbound);
}

TEST(ClusterAuthorityTest, UnboundNonFirstSlotIsCrossSlot) {
  const auto state = BuildState(kNodeA, GroupA(), GroupBWithGap());
  const std::array<std::uint16_t, 2> slots{kSlotInA, kUnboundSlot};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kCrossSlot);
}

TEST(ClusterAuthorityTest, CrossSlotRejected) {
  const auto state = BuildState(kNodeA);
  const std::array<std::uint16_t, 2> slots{kSlotInA, kSlotInB};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kCrossSlot);
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, false)).kind_,
            Decision::Kind::kCrossSlot);
}

TEST(ClusterAuthorityTest, SameSlotKeysServe) {
  // Duplicate slot entries are what a same-slot (hashtag) multi-key command
  // looks like; the request may carry them before dedup.
  const auto state = BuildState(kNodeA);
  const std::array<std::uint16_t, 2> slots{kSlotInA, kSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kServe);
}

TEST(ClusterAuthorityTest, DifferentSlotsInSameGroupAreCrossSlot) {
  // Redis checks slot equality, not shared ownership: two slots owned by the
  // same group still cross slots.
  const auto state = BuildState(kNodeA);
  const std::array<std::uint16_t, 2> slots{kSlotInA, kOtherSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kCrossSlot);
}

TEST(ClusterAuthorityTest, UnreadyNonFirstGroupLoadsBeforeCrossSlot) {
  // The loading gate outranks the cross-slot check, and a keyed request
  // consults every involved group.
  GroupView group_b = GroupB();
  group_b.population_ready_ = false;
  const auto state = BuildState(kNodeA, GroupA(), group_b);
  const std::array<std::uint16_t, 2> slots{kSlotInA, kSlotInB};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kLoading);
}

TEST(ClusterAuthorityTest, FencedPrimaryReportsClusterDown) {
  // A fenced group has no safe owner; there is nowhere to redirect to.
  GroupView group_a = GroupA();
  group_a.granted_ = false;
  const auto state = BuildState(kNodeA, group_a, GroupB());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, false)).kind_,
            Decision::Kind::kClusterDownUnbound);
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, true)).kind_,
            Decision::Kind::kClusterDownUnbound);
}

TEST(ClusterAuthorityTest, ReplicaReadWithReadonlyServesStale) {
  const auto state = BuildState(kNodeR);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_EQ(Admit(state.get(),
                  MakeRequest(slots, false, /*connection_readonly=*/true))
                .kind_,
            Decision::Kind::kServeStaleRead);
}

TEST(ClusterAuthorityTest, ReplicaReadWithoutReadonlyMoves) {
  const auto state = BuildState(kNodeR);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  ExpectMovedToNodeA(Admit(state.get(), MakeRequest(slots, false)), kSlotInA);
}

TEST(ClusterAuthorityTest, ReplicaWriteMovesRegardlessOfReadonly) {
  const auto state = BuildState(kNodeR);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  ExpectMovedToNodeA(Admit(state.get(), MakeRequest(slots, true)), kSlotInA);
  ExpectMovedToNodeA(
      Admit(state.get(),
            MakeRequest(slots, true, /*connection_readonly=*/true)),
      kSlotInA);
}

TEST(ClusterAuthorityTest, NonMemberMovesToPrimary) {
  const auto state = BuildState(kNodeB);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  ExpectMovedToNodeA(Admit(state.get(), MakeRequest(slots, false)), kSlotInA);
  ExpectMovedToNodeA(Admit(state.get(), MakeRequest(slots, true)), kSlotInA);
}

TEST(ClusterAuthorityTest, AuthorityUnchangedAcrossEqualStates) {
  const auto admitted = BuildState(kNodeA);
  const auto current = BuildState(kNodeA);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_TRUE(AuthorityUnchanged(*admitted, admitted.get(), slots));
  EXPECT_TRUE(AuthorityUnchanged(*admitted, current.get(), slots));
}

TEST(ClusterAuthorityTest, AuthorityUnchangedWithNullCurrent) {
  const auto admitted = BuildState(kNodeA);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_FALSE(AuthorityUnchanged(*admitted, nullptr, slots));
  // A no-key request captured no authority and survives any republish.
  EXPECT_TRUE(AuthorityUnchanged(*admitted, nullptr, {}));
}

TEST(ClusterAuthorityTest, AuthorityUnchangedIgnoresUnrelatedGroupChanges) {
  // Per-group tokens: republishing an unrelated group must not disturb
  // in-flight writes against this group.
  const auto admitted = BuildState(kNodeA);
  GroupView group_b = GroupB();
  group_b.group_term_ = 42;
  const auto current = BuildState(kNodeA, GroupA(), group_b);
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  EXPECT_TRUE(AuthorityUnchanged(*admitted, current.get(), slots));
}

TEST(ClusterAuthorityTest, AuthorityUnchangedDetectsInvolvedGroupChanges) {
  const auto admitted = BuildState(kNodeA);
  const std::array<std::uint16_t, 1> slots{kSlotInA};

  GroupView term_bumped = GroupA();
  term_bumped.group_term_ = 2;
  EXPECT_FALSE(AuthorityUnchanged(
      *admitted, BuildState(kNodeA, term_bumped, GroupB()).get(), slots));

  GroupView fenced = GroupA();
  fenced.granted_ = false;
  EXPECT_FALSE(AuthorityUnchanged(
      *admitted, BuildState(kNodeA, fenced, GroupB()).get(), slots));

  GroupView unready = GroupA();
  unready.population_ready_ = false;
  EXPECT_FALSE(AuthorityUnchanged(
      *admitted, BuildState(kNodeA, unready, GroupB()).get(), slots));

  GroupView new_owner = GroupA();
  new_owner.primary_node_id_ = ParseNodeId(kNodeR);
  new_owner.replica_node_ids_.clear();
  EXPECT_FALSE(AuthorityUnchanged(
      *admitted, BuildState(kNodeA, new_owner, GroupB()).get(), slots));
}

TEST(ClusterAuthorityTest, AuthorityUnchangedDetectsCoverageTransitions) {
  const auto with_gap = BuildState(kNodeA, GroupA(), GroupBWithGap());
  const auto full = BuildState(kNodeA, GroupA(), GroupB());
  const std::array<std::uint16_t, 1> gap_slot{kUnboundSlot};
  // Unbound -> bound and bound -> unbound both count as authority changes.
  EXPECT_FALSE(AuthorityUnchanged(*with_gap, full.get(), gap_slot));
  EXPECT_FALSE(AuthorityUnchanged(*full, with_gap.get(), gap_slot));
  // Unbound on both sides: no authority to compare, unchanged.
  const auto another_gap = BuildState(kNodeA, GroupA(), GroupBWithGap());
  EXPECT_TRUE(AuthorityUnchanged(*with_gap, another_gap.get(), gap_slot));
}

TEST(ClusterAuthorityTest, AuthorityUnchangedChecksEverySlot) {
  const auto admitted = BuildState(kNodeA);
  const std::array<std::uint16_t, 2> slots{kSlotInA, kSlotInB};

  const auto identical = BuildState(kNodeA);
  EXPECT_TRUE(AuthorityUnchanged(*admitted, identical.get(), slots));

  // A change in any one involved group invalidates the whole admission.
  GroupView fenced_b = GroupB();
  fenced_b.granted_ = false;
  EXPECT_FALSE(AuthorityUnchanged(
      *admitted, BuildState(kNodeA, GroupA(), fenced_b).get(), slots));
}

TEST(GroupInFlightTest, CountsAndDrains) {
  const auto state = BuildState(kNodeA);
  EXPECT_EQ(state->GroupInFlightCount(kGroupA), 0);
  EXPECT_EQ(state->TotalInFlightCount(), 0);
  // Unbound slots have no cell.
  const auto gap_state = BuildState(kNodeA, GroupA(), GroupBWithGap());
  EXPECT_EQ(gap_state->InFlightCellForSlot(kUnboundSlot), nullptr);
  {
    GroupInFlight* cell_a = state->InFlightCellForSlot(kSlotInA);
    GroupInFlight* cell_b = state->InFlightCellForSlot(kSlotInB);
    ASSERT_NE(cell_a, nullptr);
    ASSERT_NE(cell_b, nullptr);
    const std::size_t stripe = InFlightStripe();
    const InFlightGuard first(*cell_a, stripe);
    const InFlightGuard second(*cell_a, stripe);
    const InFlightGuard other(*cell_b, stripe);
    EXPECT_EQ(state->GroupInFlightCount(kGroupA), 2);
    EXPECT_EQ(state->GroupInFlightCount(kGroupB), 1);
    EXPECT_EQ(state->TotalInFlightCount(), 3);
  }
  EXPECT_EQ(state->GroupInFlightCount(kGroupA), 0);
  EXPECT_EQ(state->GroupInFlightCount(kGroupB), 0);
  EXPECT_EQ(state->TotalInFlightCount(), 0);
}

TEST(GroupInFlightTest, MoveTransfersOwnership) {
  const auto state = BuildState(kNodeA);
  GroupInFlight* cell = state->InFlightCellForSlot(kSlotInA);
  ASSERT_NE(cell, nullptr);
  const std::size_t stripe = InFlightStripe();
  {
    InFlightGuard first(*cell, stripe);
    {
      InFlightGuard second(std::move(first));
      EXPECT_EQ(state->GroupInFlightCount(kGroupA), 1);
    }
    // `second` exited; the moved-from `first` must not exit again.
    EXPECT_EQ(state->GroupInFlightCount(kGroupA), 0);
  }
  EXPECT_EQ(state->TotalInFlightCount(), 0);
}

TEST(GroupInFlightTest, ConcurrentEnterExit) {
  const auto state = BuildState(kNodeA);
  const std::array<std::uint16_t, 3> slots{kSlotInA, kOtherSlotInA, kSlotInB};
  constexpr int kThreads = 4;
  constexpr int kIterations = 20000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&state, &slots, t] {
      // Each thread gets its own stripe, so the hot path never contends.
      const std::size_t stripe = InFlightStripe();
      for (int i = 0; i < kIterations; ++i) {
        GroupInFlight* cell = state->InFlightCellForSlot(slots[(t + i) % 3]);
        const InFlightGuard guard(*cell, stripe);
        // While this thread's guard is alive the counts must include it.
        EXPECT_GE(state->TotalInFlightCount(), 1);
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(state->GroupInFlightCount(kGroupA), 0);
  EXPECT_EQ(state->GroupInFlightCount(kGroupB), 0);
  EXPECT_EQ(state->TotalInFlightCount(), 0);
}

}  // namespace
