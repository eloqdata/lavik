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

#include "gtest/gtest.h"
#include "lavik/latency_clock.h"

namespace lavik {
namespace {
TEST(LatencyIntervalsTest, MergesChildrenThatFinishOutOfOrder) {
  LatencyIntervals parent, child;
  parent.Add(100, 200);
  child.Add(10, 30);
  parent.Merge(child);
  EXPECT_EQ(parent.first_ns, 10);
  EXPECT_EQ(parent.last_ns, 200);
  EXPECT_EQ(parent.total_ns, 120);
  EXPECT_EQ(parent.count, 2);
  EXPECT_EQ(parent.intervals[0].start_ns, 10);
  EXPECT_EQ(parent.intervals[1].start_ns, 100);
}

TEST(LatencyIntervalsTest,
     PreservesAllResourceTimeWhenBothDetailsAreTruncated) {
  LatencyIntervals parent, child;
  for (unsigned i = 0; i < 8; ++i) parent.Add(100 + i * 10, 102 + i * 10);
  for (unsigned i = 1; i <= 21; ++i) child.Add(i, i + 3);
  parent.Merge(child);
  EXPECT_EQ(parent.first_ns, 1);
  EXPECT_EQ(parent.last_ns, 172);
  EXPECT_EQ(parent.total_ns, 79);
  EXPECT_EQ(parent.count, 29);
  for (unsigned i = 0; i < 8; ++i)
    EXPECT_EQ(parent.intervals[i].start_ns, i + 1);
}
}  // namespace
}  // namespace lavik
