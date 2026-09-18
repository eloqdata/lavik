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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/test_topology_installer.h"
#include "lavik/cluster/authority.h"
#include "lavik/cluster/node_control.h"
#include "lavik/cluster/topology.h"
#include "lavik/resp.h"

namespace {

using lavik::cluster::Admit;
using lavik::cluster::AuthorityGuard;
using lavik::cluster::AuthorityInFlightGuards;
using lavik::cluster::AuthorityUnchanged;
using lavik::cluster::Decision;
using lavik::cluster::GroupInFlight;
using lavik::cluster::GroupView;
using lavik::cluster::InFlightGuard;
using lavik::cluster::NodeDescriptor;
using lavik::cluster::NodeId;
using lavik::cluster::NodeIndex;
using lavik::cluster::RecheckResult;
using lavik::cluster::RequestView;
using lavik::cluster::ServingState;
using lavik::cluster::ServingStateBuilder;
using lavik::cluster::SlotRange;

// 40-hex node ids, as the builder validates.
constexpr std::string_view kNodeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kNodeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kNodeR = "cccccccccccccccccccccccccccccccccccccccc";
constexpr std::string_view kGroupA = "group-a";
constexpr std::string_view kGroupB = "group-b";
constexpr NodeIndex kNodeAIndex = 0;
constexpr NodeIndex kNodeBIndex = 1;
constexpr NodeIndex kNodeRIndex = 2;

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
  node.SetHost(host);
  node.port_ = port;
  node.tls_port_ = tls_port;
  return node;
}

GroupView MakeGroup(std::string_view id, NodeIndex primary_node_index,
                    std::uint16_t first_slot, std::uint16_t last_slot) {
  GroupView group;
  group.group_id_ = std::string(id);
  group.primary_node_index_ = primary_node_index;
  group.slot_ranges_.push_back(SlotRange{first_slot, last_slot});
  return group;
}

GroupView GroupA() {
  GroupView group = MakeGroup(kGroupA, kNodeAIndex, 0, 9999);
  group.replica_node_indices_.push_back(kNodeRIndex);
  return group;
}

GroupView GroupB() { return MakeGroup(kGroupB, kNodeBIndex, 10000, 16383); }

// Group-b covers only [10000, 15000], leaving [15001, 16383] unbound.
GroupView GroupBWithGap() {
  return MakeGroup(kGroupB, kNodeBIndex, 10000, 15000);
}

std::shared_ptr<const ServingState> BuildState(std::string_view self,
                                               GroupView group_a,
                                               GroupView group_b) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(1).SetInFlightStripeCount(4);
  if (self == kNodeA) {
    builder.SetSelfNodeIndex(kNodeAIndex);
  } else if (self == kNodeB) {
    builder.SetSelfNodeIndex(kNodeBIndex);
  } else if (self == kNodeR) {
    builder.SetSelfNodeIndex(kNodeRIndex);
  }
  builder.AddNode(MakeNode(kNodeA, "10.0.0.1", 7000, 17000));
  builder.AddNode(MakeNode(kNodeB, "10.0.0.2", 7001, 17001));
  NodeDescriptor replica = MakeNode(kNodeR, "10.0.0.3", 7002, 17002);
  replica.primary_node_index_ = kNodeAIndex;
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

struct TestAuthorityControl {
  TestAuthorityControl()
      : authority(cache),
        installer(cache, authority, actions),
        topology(installer, cache) {}

  lavik::cluster::TopologyCache cache;
  AuthorityGuard authority;
  lavik::cluster::NullNodeControlActions actions;
  lavik::cluster::NodeControlInstaller installer;
  lavik::cluster::testing::TestTopologyInstaller topology;
};

TEST(ClusterAuthoritySnapshotTest, CachedReadExpiresWithoutAnyPublication) {
  using namespace std::chrono_literals;
  TestAuthorityControl control;
  const auto start = lavik::cluster::MonotonicTime{};
  ASSERT_TRUE(control.topology.Install(BuildState(kNodeA), start, 10ms).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto read = MakeRequest(slots, false);
  EXPECT_EQ(
      control.authority.CaptureAndAdmit(read, start + 9ms).decision().kind_,
      Decision::Kind::kServe);
  const auto write =
      control.authority.CaptureAndAdmit(MakeRequest(slots, true), start + 9ms);
  EXPECT_EQ(
      control.authority.CaptureAndAdmit(read, start + 10ms).decision().kind_,
      Decision::Kind::kClusterDownUnbound);
  EXPECT_EQ(control.authority.RecheckAtMutation(write, start + 10ms),
            RecheckResult::kReject);
}

TEST(ClusterAuthoritySnapshotTest,
     RenewalRefreshesDeadlineWithoutAbortingWrite) {
  using namespace std::chrono_literals;
  TestAuthorityControl control;
  const auto start = lavik::cluster::MonotonicTime{};
  ASSERT_TRUE(control.topology.Install(BuildState(kNodeA), start, 10ms).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto read = MakeRequest(slots, false);
  ASSERT_EQ(
      control.authority.CaptureAndAdmit(read, start + 4ms).decision().kind_,
      Decision::Kind::kServe);
  const auto write =
      control.authority.CaptureAndAdmit(MakeRequest(slots, true), start + 4ms);
  ASSERT_TRUE(
      control.topology.Install(BuildState(kNodeA), start + 5ms, 10ms).ok());
  EXPECT_EQ(
      control.authority.CaptureAndAdmit(read, start + 11ms).decision().kind_,
      Decision::Kind::kServe);
  EXPECT_EQ(control.authority.RecheckAtMutation(write, start + 11ms),
            RecheckResult::kOk);
  EXPECT_EQ(
      control.authority.CaptureAndAdmit(read, start + 15ms).decision().kind_,
      Decision::Kind::kClusterDownUnbound);
}

TEST(ClusterAuthoritySnapshotTest,
     ConcurrentRenewalsThenFenceInvalidateReaders) {
  using namespace std::chrono_literals;
  TestAuthorityControl control;
  const auto now = lavik::cluster::MonotonicTime{};
  const auto state = BuildState(kNodeA);
  ASSERT_TRUE(control.topology.Install(state, now).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto write =
      control.authority.CaptureAndAdmit(MakeRequest(slots, true), now);
  std::atomic<unsigned> started{0};
  std::atomic<unsigned> phase{0};
  std::atomic<unsigned> failures{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      const auto request = MakeRequest(slots, false);
      if (control.authority.CaptureAndAdmit(request, now).decision().kind_ !=
          Decision::Kind::kServe)
        ++failures;
      started.fetch_add(1, std::memory_order_release);
      while (phase.load(std::memory_order_acquire) == 0) {
        const auto decision =
            control.authority.CaptureAndAdmit(request, now).decision();
        if (phase.load(std::memory_order_acquire) == 0 &&
            decision.kind_ != Decision::Kind::kServe)
          ++failures;
      }
      while (phase.load(std::memory_order_acquire) != 2)
        std::this_thread::yield();
      for (int n = 0; n < 100; ++n) {
        if (control.authority.CaptureAndAdmit(request, now).decision().kind_ !=
            Decision::Kind::kClusterDownUnbound)
          ++failures;
        if (control.authority.Recheck(write, now) != RecheckResult::kReject)
          ++failures;
      }
    });
  }
  while (started.load(std::memory_order_acquire) != 4)
    std::this_thread::yield();
  for (int i = 0; i < 100; ++i) {
    if (!control.topology.Install(state, now, 1h + std::chrono::seconds(i))
             .ok())
      ++failures;
  }
  phase.store(1, std::memory_order_release);
  auto group = GroupA();
  group.granted_ = false;
  if (!control.topology.Install(BuildState(kNodeA, group, GroupB()), now).ok())
    ++failures;
  phase.store(2, std::memory_order_release);
  for (auto& reader : readers) reader.join();
  EXPECT_EQ(failures.load(), 0u);
  EXPECT_EQ(control.authority.RecheckAtMutation(write, now),
            RecheckResult::kReject);
}

TEST(ClusterAuthoritySnapshotTest, ReusedAddressDoesNotReuseCachedAuthority) {
  using namespace std::chrono_literals;
  std::optional<TestAuthorityControl> control;
  const auto now = lavik::cluster::MonotonicTime{};
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto request = MakeRequest(slots, false);
  control.emplace();
  ASSERT_TRUE(control->topology.Install(BuildState(kNodeA), now, 10ms).ok());
  EXPECT_EQ(control->authority.CaptureAndAdmit(request, now).decision().kind_,
            Decision::Kind::kServe);
  control.reset();
  control.emplace();
  ASSERT_TRUE(control->topology.Install(BuildState(kNodeA), now, 20ms).ok());
  EXPECT_EQ(
      control->authority.CaptureAndAdmit(request, now + 15ms).decision().kind_,
      Decision::Kind::kServe);
  control.reset();
  control.emplace();
  ASSERT_TRUE(control->topology.Install(BuildState(kNodeB), now, 20ms).ok());
  EXPECT_EQ(control->authority.CaptureAndAdmit(request, now).decision().kind_,
            Decision::Kind::kMoved);
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

TEST(ClusterAuthorityTest,
     ControlledPauseKeepsReadsButReturnsTryAgainForWrites) {
  GroupView group_a = GroupA();
  group_a.mutations_paused_ = true;
  const auto state = BuildState(kNodeA, group_a, GroupB());
  const std::array<std::uint16_t, 1> slots{kSlotInA};

  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, /*is_write=*/false)).kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(Admit(state.get(), MakeRequest(slots, /*is_write=*/true)).kind_,
            Decision::Kind::kTryAgain);
}

TEST(GroupInFlightTest,
     PauseRejectsUnregisteredCaptureButDrainsAlreadyRegisteredMutation) {
  TestAuthorityControl control;
  const auto unpaused = BuildState(kNodeA);
  ASSERT_TRUE(control.topology.Install(unpaused, {}).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};

  auto stale = control.authority.CaptureAndAdmit(MakeRequest(slots, true), {});
  ASSERT_EQ(stale.decision().kind_, Decision::Kind::kServe);

  auto registered =
      control.authority.CaptureAndAdmit(MakeRequest(slots, true), {});
  AuthorityInFlightGuards in_flights;
  ASSERT_EQ(
      control.authority.RegisterAndRecheck(registered, 0, {}, &in_flights),
      RecheckResult::kOk);
  ASSERT_EQ(in_flights.size(), 1u);

  GroupView paused_group = GroupA();
  paused_group.mutations_paused_ = true;
  const auto paused = BuildState(kNodeA, paused_group, GroupB());
  // This is the lower-level publication/registration handshake itself. The
  // initial adapter install supplied a real finite session lease; publishing
  // the pause directly avoids asking NodeControl to drain the very guard this
  // test intentionally keeps alive.
  control.cache.Publish(paused);

  AuthorityInFlightGuards rejected;
  EXPECT_EQ(control.authority.RegisterAndRecheck(stale, 0, {}, &rejected),
            RecheckResult::kReject);
  EXPECT_TRUE(rejected.empty());
  // The pause-only publication preserves authority for work that registered
  // before it, and the shared cell remains visible to the drain side.
  EXPECT_EQ(control.authority.RecheckAtMutation(registered, {}),
            RecheckResult::kOk);
  EXPECT_EQ(paused->GroupInFlightCount(kGroupA), 1u);
  in_flights.clear();
  EXPECT_EQ(paused->GroupInFlightCount(kGroupA), 0u);
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

TEST(ClusterAuthorityTest,
     MovedHostSurvivesSnapshotReplacementAndAdmissionMoves) {
  const std::array<std::string, 3> hosts{
      "10.0.0.1", "2001:db8:85a3:0000:0000:8a2e:0370:7334",
      std::string(180, 'x') + ".example.org"};
  for (const std::string& host : hosts) {
    SCOPED_TRACE(host);
    lavik::cluster::TopologyCache cache;
    AuthorityGuard authority(cache);
    std::weak_ptr<const ServingState> retired;
    {
      ServingStateBuilder builder;
      builder.AddNode(MakeNode(kNodeA, host, 7000, 17000));
      builder.AddGroup(MakeGroup(kGroupA, kNodeAIndex, 0, 9999));
      auto state = builder.Build();
      ASSERT_TRUE(state.ok()) << state.status();
      retired = *state;
      cache.Publish(std::move(*state));
    }
    const std::array<std::uint16_t, 1> slots{kSlotInA};
    auto admission = authority.CaptureAndAdmit(MakeRequest(slots, false), {});
    ASSERT_EQ(admission.decision().kind_, Decision::Kind::kMoved);
    cache.Publish(BuildState(kNodeB));
    ASSERT_FALSE(retired.expired());

    lavik::cluster::AuthorityAdmission moved(std::move(admission));
    EXPECT_TRUE(admission.decision().moved_host_.empty());
    // Replacing an existing admission must transfer the new view and its
    // snapshot together, even when that retires the destination's old owner.
    auto assigned = authority.CaptureAndAdmit(MakeRequest(slots, false), {});
    std::weak_ptr<const ServingState> replaced = assigned.state();
    cache.Publish(nullptr);
    // Refresh the request-thread cache so the lifetime assertions below
    // measure admission ownership without an extra cached snapshot owner.
    EXPECT_EQ(authority.CaptureAndAdmit(MakeRequest(slots, false), {})
                  .decision()
                  .kind_,
              Decision::Kind::kLoading);
    assigned = std::move(moved);
    EXPECT_TRUE(replaced.expired());
    EXPECT_TRUE(moved.decision().moved_host_.empty());
    EXPECT_EQ(assigned.decision().moved_host_, host);

    auto shared = std::make_shared<const lavik::cluster::AuthorityAdmission>(
        std::move(assigned));
    std::string reply;
    std::thread worker([owned = std::move(shared), &host, &reply] {
      EXPECT_EQ(owned->decision().moved_host_, host);
      lavik::ReplyBuilder builder;
      reply = lavik::AppendMovedError(builder, owned->decision().moved_slot_,
                                      owned->decision().moved_host_,
                                      owned->decision().moved_port_);
    });
    worker.join();
    EXPECT_TRUE(retired.expired());
    // Network output owns the encoded bytes after the last admission dies.
    EXPECT_EQ(reply, "-MOVED 5 " + host + ":7000\r\n");
  }
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
  new_owner.primary_node_index_ = kNodeBIndex;
  new_owner.replica_node_indices_.clear();
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
    constexpr std::size_t stripe = 0;
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

TEST(GroupInFlightTest, AllocatesOneStripePerConfiguredWorker) {
  const auto state = BuildState(kNodeA);
  const GroupInFlight* cell = state->InFlightCellForSlot(kSlotInA);
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->StripeCount(), 4);
}

TEST(GroupInFlightTest, MoveTransfersOwnership) {
  const auto state = BuildState(kNodeA);
  GroupInFlight* cell = state->InFlightCellForSlot(kSlotInA);
  ASSERT_NE(cell, nullptr);
  constexpr std::size_t stripe = 0;
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

TEST(AuthorityGuardTest, RegistrationHandshakeOwnsOneGuardPerGroup) {
  TestAuthorityControl control;
  ASSERT_TRUE(control.topology.Install(BuildState(kNodeA), {}).ok());
  const std::array<std::uint16_t, 2> duplicate_slots{kSlotInA, kSlotInA};
  const auto admission = control.authority.CaptureAndAdmit(
      MakeRequest(duplicate_slots, /*is_write=*/true),
      lavik::cluster::MonotonicTime{});

  AuthorityInFlightGuards guards;
  EXPECT_EQ(control.authority.RegisterAndRecheck(
                admission, /*worker_stripe=*/2, lavik::cluster::MonotonicTime{},
                &guards),
            RecheckResult::kOk);
  EXPECT_EQ(guards.size(), 1U);
  EXPECT_EQ(admission.state()->GroupInFlightCount(kGroupA), 1U);

  guards.clear();
  EXPECT_EQ(admission.state()->GroupInFlightCount(kGroupA), 0U);
}

TEST(AuthorityGuardTest,
     TopologyTransitionRejectsRegistrationAndReleasesItsGuard) {
  TestAuthorityControl control;
  ASSERT_TRUE(control.topology.Install(BuildState(kNodeA), {}).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto admission = control.authority.CaptureAndAdmit(
      MakeRequest(slots, /*is_write=*/true), lavik::cluster::MonotonicTime{});

  GroupView fenced = GroupA();
  fenced.granted_ = false;
  ASSERT_TRUE(control.topology
                  .Install(BuildState(kNodeA, std::move(fenced), GroupB()), {})
                  .ok());
  AuthorityInFlightGuards guards;
  EXPECT_EQ(control.authority.RegisterAndRecheck(
                admission, /*worker_stripe=*/0, lavik::cluster::MonotonicTime{},
                &guards),
            RecheckResult::kReject);
  EXPECT_TRUE(guards.empty());
  EXPECT_EQ(admission.state()->GroupInFlightCount(kGroupA), 0U);
}

TEST(AuthorityGuardTest, FinalMutationRecheckTracksAggregateOutcome) {
  TestAuthorityControl control;
  ASSERT_TRUE(control.topology.Install(BuildState(kNodeA), {}).ok());
  const std::array<std::uint16_t, 1> slots{kSlotInA};
  const auto started = control.authority.CaptureAndAdmit(
      MakeRequest(slots, /*is_write=*/true), lavik::cluster::MonotonicTime{});
  const auto rejected = control.authority.CaptureAndAdmit(
      MakeRequest(slots, /*is_write=*/true), lavik::cluster::MonotonicTime{});

  EXPECT_EQ(control.authority.RecheckAtMutation(
                started, lavik::cluster::MonotonicTime{}),
            RecheckResult::kOk);
  EXPECT_TRUE(started.mutation_started());
  EXPECT_FALSE(started.final_recheck_failed());

  GroupView fenced = GroupA();
  fenced.granted_ = false;
  ASSERT_TRUE(control.topology
                  .Install(BuildState(kNodeA, std::move(fenced), GroupB()), {})
                  .ok());
  EXPECT_EQ(control.authority.RecheckAtMutation(
                started, lavik::cluster::MonotonicTime{}),
            RecheckResult::kReject);
  EXPECT_TRUE(started.mutation_started());
  EXPECT_TRUE(started.final_recheck_failed());

  EXPECT_EQ(control.authority.RecheckAtMutation(
                rejected, lavik::cluster::MonotonicTime{}),
            RecheckResult::kReject);
  EXPECT_FALSE(rejected.mutation_started());
  EXPECT_TRUE(rejected.final_recheck_failed());
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
      const std::size_t stripe = static_cast<std::size_t>(t);
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
