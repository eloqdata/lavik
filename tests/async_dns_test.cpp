// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#include "lavik/async_dns.h"

#include <gtest/gtest.h>

#include <vector>

TEST(AsyncDns, SlowFailedLookupDoesNotBlockCallerOrNumericLookup) {
  const auto begin = std::chrono::steady_clock::now();
  auto query = lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1234);
  ASSERT_TRUE(query);
  EXPECT_LT(std::chrono::steady_clock::now() - begin,
            std::chrono::milliseconds(100));
  EXPECT_FALSE(query->done_.load(std::memory_order_acquire));
  auto numeric = lavik::AsyncDnsQuery::Start("127.0.0.1", 1234);
  ASSERT_TRUE(numeric);
  EXPECT_TRUE(numeric->done_.load(std::memory_order_acquire));
  const auto deadline = begin + std::chrono::seconds(5);
  while (!query->done_.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(query->done_.load(std::memory_order_acquire));
  EXPECT_EQ(query->result_, EAI_AGAIN);
  EXPECT_EQ(query->addresses_, nullptr);
}

TEST(AsyncDns, SaturationBoundsUncancelableLookupsAndKeepsNumericFastPath) {
  std::vector<std::shared_ptr<lavik::AsyncDnsQuery>> queries;
  for (unsigned i = 0; i < 32; ++i)
    queries.push_back(lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1));
  EXPECT_FALSE(lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1));
  EXPECT_TRUE(lavik::AsyncDnsQuery::Start("::1", 1));
  queries
      .clear();  // Abandoning callers must leave only resolver-owned storage.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (lavik::AsyncDnsQuery::in_flight_.load() &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(lavik::AsyncDnsQuery::in_flight_.load(), 0u);
}
