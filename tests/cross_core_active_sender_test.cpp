#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "bycorf/runtime/cross_core.h"
#include "gtest/gtest.h"

namespace bycorf {
namespace {

TEST(RemoteNotificationTest, AggregateInitializationZeroesOmittedFields) {
  const RemoteNotification empty{};
  EXPECT_EQ(empty.context_, nullptr);
  EXPECT_EQ(empty.value_, 0U);
  EXPECT_EQ(empty.run_fn_, nullptr);
  const RemoteNotification partial{.value_ = 42};
  EXPECT_EQ(partial.context_, nullptr);
  EXPECT_EQ(partial.value_, 42U);
  EXPECT_EQ(partial.run_fn_, nullptr);
}

TEST(RemoteNotificationTest, BothDequeuePathsOverwriteAllScratchFields) {
  std::uint64_t result = 0;
  const RemoteNotification message{
      .context_ = &result,
      .value_ = 42,
      .run_fn_ = +[](void* context, std::uint64_t value) noexcept {
        *static_cast<std::uint64_t*>(context) += value;
      }};
  SpscRing<RemoteNotification, 4> ring;
  moodycamel::ConcurrentQueue<RemoteNotification> overflow;
  RemoteNotification scratch[2];
  ASSERT_TRUE(ring.try_enqueue(message));
  ASSERT_EQ(ring.try_dequeue_bulk(scratch, 2), 1U);
  ASSERT_EQ(scratch[0].context_, message.context_);
  ASSERT_EQ(scratch[0].run_fn_, message.run_fn_);
  scratch[0].run_fn_(scratch[0].context_, scratch[0].value_);
  EXPECT_EQ(result, 42U);
  ASSERT_TRUE(overflow.enqueue(message));
  // A different, uninitialized slot also exercises the overflow assignment.
  ASSERT_EQ(overflow.try_dequeue_bulk(scratch + 1, 1), 1U);
  ASSERT_EQ(scratch[1].context_, message.context_);
  ASSERT_EQ(scratch[1].run_fn_, message.run_fn_);
  scratch[1].run_fn_(scratch[1].context_, scratch[1].value_);
  EXPECT_EQ(result, 84U);
  EXPECT_EQ(ring.try_dequeue_bulk(scratch, 2), 0U);
  EXPECT_EQ(overflow.try_dequeue_bulk(scratch, 2), 0U);
}

TEST(SpscRingTest, RefreshesCachedHeadAfterConsumerProgress) {
  SpscRing<int, 4> ring;

  EXPECT_TRUE(ring.try_enqueue(1));
  EXPECT_TRUE(ring.try_enqueue(2));
  EXPECT_TRUE(ring.try_enqueue(3));
  EXPECT_FALSE(ring.try_enqueue(4));

  int drained[3]{};
  EXPECT_EQ(ring.try_dequeue_bulk(drained, 2), 2U);
  EXPECT_EQ(drained[0], 1);
  EXPECT_EQ(drained[1], 2);

  EXPECT_TRUE(ring.try_enqueue(4));
  EXPECT_TRUE(ring.try_enqueue(5));
  EXPECT_FALSE(ring.try_enqueue(6));

  EXPECT_EQ(ring.try_dequeue_bulk(drained, 3), 3U);
  EXPECT_EQ(drained[0], 3);
  EXPECT_EQ(drained[1], 4);
  EXPECT_EQ(drained[2], 5);
  EXPECT_TRUE(ring.empty());
}

TEST(CrossCoreActiveSenderTest, BitmapAddressesAndClearsSenderGroups) {
  CrossCore cross_core(130);

  cross_core.ActivateSender(129, 0);
  cross_core.ActivateSender(129, 63);
  cross_core.ActivateSender(129, 64);
  cross_core.ActivateSender(129, 129);

  EXPECT_EQ(cross_core.active_sender_word_count(), 3U);
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 0),
            (std::uint64_t{1} << 0) | (std::uint64_t{1} << 63));
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 1), std::uint64_t{1});
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 2), std::uint64_t{1} << 1);
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 0), 0U);
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 1), 0U);
  EXPECT_EQ(cross_core.TakeActiveSenders(129, 2), 0U);
}

TEST(CrossCoreActiveSenderTest, RepeatedPostsBatchLaterLanePublications) {
  CrossCore cross_core(4);
  SetThisWorker(2, &cross_core, nullptr);
  RemoteWork first;
  RemoteWork second;

  PostRequest(&cross_core, 1, &first);
  EXPECT_EQ(cross_core.TakeActiveSenders(1, 0), std::uint64_t{1} << 2);
  ASSERT_EQ(ThisWorker().wake_list_.size(), 1U);
  EXPECT_EQ(ThisWorker().wake_list_.front(), 1U);

  // Model the receiver completing the first lane visit before the next post.
  cross_core.lane(1, 2).active_.store(false, std::memory_order_release);
  RemoteWork* drained[2]{};
  EXPECT_EQ(cross_core.lane(1, 2).requests_.try_dequeue_bulk(drained, 1), 1U);
  EXPECT_EQ(drained[0], &first);

  PostRequest(&cross_core, 1, &second);
  EXPECT_EQ(cross_core.TakeActiveSenders(1, 0), 0U);
  EXPECT_EQ(ThisWorker().wake_pending_[1], 2U);

  // FlushWakes performs this one final publication for all later posts.
  PublishCrossCoreLane(1);
  EXPECT_EQ(cross_core.TakeActiveSenders(1, 0), std::uint64_t{1} << 2);
  EXPECT_EQ(cross_core.TakeActiveSenders(1, 0), 0U);
  EXPECT_TRUE(cross_core.lane(1, 2).active_.load(std::memory_order_acquire));

  EXPECT_EQ(cross_core.lane(1, 2).requests_.try_dequeue_bulk(drained, 2), 1U);
  EXPECT_EQ(drained[0], &second);
}

TEST(CrossCoreActiveSenderTest, SelfPostActivatesImmediately) {
  CrossCore cross_core(2);
  SetThisWorker(1, &cross_core, nullptr);
  RemoteWork work;

  PostReply(&cross_core, 1, &work);

  EXPECT_TRUE(ThisWorker().wake_list_.empty());
  EXPECT_EQ(cross_core.TakeActiveSenders(1, 0), std::uint64_t{1} << 1);
}

TEST(CrossCoreActiveSenderTest, ConcurrentDrainReactivatesNonemptyLanes) {
  constexpr unsigned kSenderCount = 8;
  constexpr unsigned kReceiver = kSenderCount;
  constexpr std::uint64_t kMessagesPerSender = 20'000;
  constexpr std::size_t kDrainBatch = 17;
  constexpr std::uint64_t kTotal = kSenderCount * kMessagesPerSender;

  CrossCore cross_core(kSenderCount + 1);
  std::atomic<bool> start{false};
  std::vector<std::thread> producers;
  producers.reserve(kSenderCount);
  for (unsigned sender = 0; sender < kSenderCount; ++sender) {
    producers.emplace_back([&, sender] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      CrossCoreLane& lane = cross_core.lane(kReceiver, sender);
      for (std::uint64_t sequence = 0; sequence < kMessagesPerSender;
           ++sequence) {
        const std::uint64_t value = sender * kMessagesPerSender + sequence;
        RemoteNotification notification{.value_ = value};
        while (!lane.notifications_.try_enqueue(notification)) {
          std::this_thread::yield();
        }
        ActivateCrossCoreLane(&cross_core, kReceiver, sender, lane);
      }
    });
  }

  std::vector<bool> seen(kTotal, false);
  std::uint64_t consumed = 0;
  bool invalid = false;
  start.store(true, std::memory_order_release);
  while (consumed != kTotal) {
    bool active_found = false;
    for (unsigned word = 0; word < cross_core.active_sender_word_count();
         ++word) {
      std::uint64_t active = cross_core.TakeActiveSenders(kReceiver, word);
      active_found |= active != 0;
      while (active != 0) {
        const unsigned bit = std::countr_zero(active);
        active &= active - 1;
        const unsigned sender = word * 64U + bit;
        CrossCoreLane& lane = cross_core.lane(kReceiver, sender);
        RemoteNotification batch[kDrainBatch];
        const std::size_t count =
            lane.notifications_.try_dequeue_bulk(batch, kDrainBatch);
        for (std::size_t i = 0; i < count; ++i) {
          const std::uint64_t value = batch[i].value_;
          if (value >= kTotal || seen[value]) {
            invalid = true;
          } else {
            seen[value] = true;
          }
        }
        consumed += count;

        // Match Worker::DrainCrossCore: acquire a producer that coalesced its
        // post while the lane was still active before checking for leftovers.
        (void)lane.active_.exchange(false, std::memory_order_acq_rel);
        if (!lane.empty() &&
            !lane.active_.exchange(true, std::memory_order_acq_rel)) {
          cross_core.ActivateSender(kReceiver, sender);
        }
      }
    }
    if (!active_found) {
      std::this_thread::yield();
    }
  }

  for (std::thread& producer : producers) {
    producer.join();
  }
  EXPECT_FALSE(invalid);
  for (bool value_seen : seen) {
    EXPECT_TRUE(value_seen);
  }
  for (unsigned sender = 0; sender < kSenderCount; ++sender) {
    EXPECT_TRUE(cross_core.lane(kReceiver, sender).empty());
  }
  EXPECT_EQ(cross_core.TakeActiveSenders(kReceiver, 0), 0U);
}

}  // namespace
}  // namespace bycorf
