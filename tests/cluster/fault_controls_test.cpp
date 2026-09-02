#include "tests/cluster/fault_controls.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace keylane::test::cluster {
namespace {

TEST(ManualClockTest, MovesOnlyForward) {
  ManualClock clock;
  EXPECT_FALSE(clock.HasExpired(9));
  EXPECT_TRUE(clock.AdvanceTo(9).ok());
  EXPECT_EQ(clock.now(), 9U);
  EXPECT_TRUE(clock.HasExpired(9));
  EXPECT_FALSE(clock.AdvanceTo(8).ok());
}

TEST(SimNetworkTest, ControlsPartitionOrderDuplicationAndDrop) {
  SimNetwork network;
  const std::uint64_t first = network.Send(NodeId{1}, NodeId{2}, "first");
  const std::uint64_t second = network.Send(NodeId{1}, NodeId{2}, "second");
  ASSERT_TRUE(network.Delay(first, 10).ok());
  EXPECT_EQ(network.Deliverable().size(), 1U);
  EXPECT_FALSE(network.Deliver(first).ok());
  EXPECT_EQ(network.Deliverable(10).size(), 2U);
  auto duplicated = network.Duplicate(first);
  ASSERT_TRUE(duplicated.ok()) << duplicated.status();
  network.Partition(NodeId{2}, NodeId{1});
  EXPECT_TRUE(network.Deliverable().empty());
  EXPECT_FALSE(network.Deliver(second).ok());
  network.Heal(NodeId{1}, NodeId{2});
  auto delivered_second = network.Deliver(second);
  ASSERT_TRUE(delivered_second.ok()) << delivered_second.status();
  EXPECT_EQ(delivered_second->payload_, "second");
  ASSERT_TRUE(network.Drop(first).ok());
  ASSERT_EQ(network.Deliverable(10).size(), 1U);
  EXPECT_EQ(network.Deliverable(10).front().id_, *duplicated);
  EXPECT_EQ(network.Deliverable(10).front().payload_, "first");

  network.Disconnect(NodeId{1}, NodeId{2});
  const std::uint64_t while_disconnected =
      network.Send(NodeId{1}, NodeId{2}, "while-disconnected");
  network.Reconnect(NodeId{1}, NodeId{2});
  EXPECT_TRUE(network.Deliverable(10).empty());
  EXPECT_FALSE(network.Deliver(*duplicated, 10).ok());
  EXPECT_FALSE(network.Deliver(while_disconnected, 10).ok());
  const std::uint64_t fresh =
      network.Send(NodeId{1}, NodeId{2}, "after-reconnect");
  EXPECT_TRUE(network.Deliver(fresh, 10).ok());
  EXPECT_TRUE(network.Drop(*duplicated).ok());
  EXPECT_TRUE(network.Drop(while_disconnected).ok());
}

TEST(SimStorageTest, SeparatesCompletionDurabilityAndRecovery) {
  SimStorage storage;
  ASSERT_TRUE(storage.Write("key", "abcdef").ok());
  ASSERT_TRUE(storage.Read("key").ok());
  EXPECT_EQ(*storage.Read("key"), "abcdef");
  ASSERT_TRUE(storage.ReadDurable("key").ok());
  EXPECT_FALSE(storage.ReadDurable("key")->has_value());
  ASSERT_TRUE(storage.PersistPrefix("key", 3).ok());
  storage.CrashAndRecover();
  EXPECT_EQ(*storage.Read("key"), "abc");

  storage.FailNextWrite();
  EXPECT_FALSE(storage.Write("key", "lost").ok());
  ASSERT_TRUE(storage.Write("key", "complete").ok());
  storage.FailNextRead();
  EXPECT_FALSE(storage.Read("key").ok());
  storage.FailNextFlush();
  EXPECT_FALSE(storage.Flush("key").ok());
  ASSERT_TRUE(storage.Flush("key").ok());
  storage.CrashAndRecover();
  EXPECT_EQ(*storage.Read("key"), "complete");
}

TEST(ControlPlaneReferenceMachineTest, ReplaysIdempotentlyAndRejectsForks) {
  ControlPlaneReferenceMachine machine;
  machine.ChangeLeader(NodeId{2});
  const MetaEntry first{
      .index_ = 1, .term_ = GroupTerm{1}, .payload_ = "topology-1"};
  const MetaEntry second{
      .index_ = 2, .term_ = GroupTerm{2}, .payload_ = "grant-2"};
  ASSERT_TRUE(machine.AppendCommitted(first).ok());
  ASSERT_TRUE(machine.Replay({first, second}).ok());
  EXPECT_EQ(machine.applied_index(), 2U);
  EXPECT_EQ(machine.leader(), NodeId{2});

  MetaEntry fork = second;
  fork.payload_ = "different";
  EXPECT_FALSE(machine.Replay({fork}).ok());
  EXPECT_FALSE(machine.RestoreSnapshot({first}).ok());
}

TEST(FaultControllerTest, MatchesStableCheckpointOccurrence) {
  FaultController controller({FaultRule{.checkpoint_ = "storage.flush",
                                        .occurrence_ = 2,
                                        .effect_ = FaultEffect::kCrash,
                                        .argument_ = 17}});
  EXPECT_EQ(controller.Reach("storage.flush").effect_, FaultEffect::kContinue);
  const FaultDecision decision = controller.Reach("storage.flush");
  EXPECT_EQ(decision.effect_, FaultEffect::kCrash);
  EXPECT_EQ(decision.argument_, 17U);
  EXPECT_EQ(controller.acknowledgments(),
            std::vector<std::string>({"storage.flush#1", "storage.flush#2"}));
}

}  // namespace
}  // namespace keylane::test::cluster
