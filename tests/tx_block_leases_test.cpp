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

#include "lavik/storage/detail/tx_block_leases.h"

#include "gtest/gtest.h"

namespace lavik::storage {
namespace {

TEST(TxBlockLeases, ProbeDoesNotKeepWriterAliveOrDiscardDecisionMembership) {
  TxBlockLeases leases;
  auto writer = std::make_shared<int>(1);
  leases.Record(7, writer);
  EXPECT_TRUE(leases.HasLiveWriter());
  EXPECT_TRUE(leases.HasLiveWriter());
  EXPECT_EQ(writer.use_count(), 1);
  writer.reset();
  EXPECT_FALSE(leases.HasLiveWriter());
  EXPECT_FALSE(leases.HasLiveWriter());
  ASSERT_EQ(leases.entries().size(), 1);
  EXPECT_TRUE(leases.entries().at(7).expired());
}

TEST(TxBlockLeases, NewMembershipInvalidatesSettledProofAndReplacementWitness) {
  TxBlockLeases leases;
  leases.Record(1, {});
  EXPECT_FALSE(leases.HasLiveWriter());
  auto first = std::make_shared<int>(1);
  leases.Record(2, first);
  EXPECT_TRUE(leases.HasLiveWriter());
  // A recovery-style replacement must not keep reporting an old writer that
  // no longer belongs to this block, even if that writer remains alive.
  leases.Record(2, {});
  EXPECT_FALSE(leases.HasLiveWriter());
  auto second = std::make_shared<int>(2);
  leases.Record(3, second);
  EXPECT_TRUE(leases.HasLiveWriter());
  leases = {};
  EXPECT_FALSE(leases.HasLiveWriter());
  EXPECT_TRUE(leases.entries().empty());
}

TEST(TxBlockLeases, ExpiredWitnessFindsOtherLiveWriter) {
  TxBlockLeases leases;
  auto first = std::make_shared<int>(1);
  auto second = std::make_shared<int>(2);
  leases.Record(1, first);
  EXPECT_TRUE(leases.HasLiveWriter());
  leases.Record(2, second);
  EXPECT_TRUE(leases.HasLiveWriter());
  first.reset();
  EXPECT_TRUE(leases.HasLiveWriter());
  second.reset();
  EXPECT_FALSE(leases.HasLiveWriter());
  EXPECT_EQ(leases.entries().size(), 2);
}

}  // namespace
}  // namespace lavik::storage
