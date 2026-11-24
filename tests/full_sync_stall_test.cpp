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

#include "full_sync_stall.h"

#include <gtest/gtest.h>

namespace lavik::detail {
namespace {
using namespace std::chrono_literals;
using Clock = FullSyncStallWatchdog::Clock;

TEST(FullSyncStallWatchdogTest, BarrierWaiterInheritsProgressFromLaterFlow) {
  const Clock::time_point start{};
  FullSyncStallWatchdog watchdog(2, start, 10s);
  watchdog.Observe(0, 1, start);
  watchdog.Observe(1, 1, start);
  watchdog.Observe(1, 2, start + 9s);
  EXPECT_FALSE(watchdog.Stalled(0, true, start + 11s));
  EXPECT_FALSE(watchdog.Stalled(1, false, start + 11s));
  // An active but stuck transport must not borrow the same peer progress.
  EXPECT_TRUE(watchdog.Stalled(0, false, start + 11s));
}

TEST(FullSyncStallWatchdogTest, WholeSessionStallStillExpiresBarrierWaiters) {
  const Clock::time_point start{};
  FullSyncStallWatchdog watchdog(2, start, 10s);
  watchdog.Observe(0, 1, start);
  watchdog.Observe(1, 1, start);
  watchdog.Observe(1, 2, start + 9s);
  watchdog.Observe(1, 2, start + 18s);
  EXPECT_FALSE(watchdog.Stalled(0, true, start + 18s));
  EXPECT_TRUE(watchdog.Stalled(0, true, start + 19s));
  EXPECT_TRUE(watchdog.Stalled(1, false, start + 19s));
}

TEST(FullSyncStallWatchdogTest, LeavingBarrierStartsNewIndependentDeadline) {
  const Clock::time_point start{};
  FullSyncStallWatchdog watchdog(2, start, 10s);
  watchdog.Observe(0, 1, start);
  watchdog.Observe(1, 1, start);
  watchdog.Observe(1, 2, start + 9s);
  // PeerFlowWait publishes a new generation before clearing its wait flag.
  watchdog.Observe(0, 2, start + 11s);
  EXPECT_FALSE(watchdog.Stalled(0, false, start + 20s));
  watchdog.Observe(1, 3, start + 20s);
  EXPECT_TRUE(watchdog.Stalled(0, false, start + 21s));
}
}  // namespace
}  // namespace lavik::detail
