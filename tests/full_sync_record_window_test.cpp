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

#include "full_sync_record_window.h"

#include <limits>

#include "gtest/gtest.h"

namespace lavik::detail {
namespace {

TEST(FullSyncRecordWindowTest, TinyFramesCannotExceedTheFrameCeiling) {
  FullSyncRecordWindow window;
  for (std::uint64_t sequence = 1; sequence <= window.kMaxFrames; ++sequence) {
    ASSERT_TRUE(window.Begin(7, sequence, 26).ok());
  }
  EXPECT_EQ(window.inflight_frames(), window.kMaxFrames);
  EXPECT_EQ(window.inflight_bytes(), 26 * window.kMaxFrames);
  EXPECT_FALSE(window.CanSend(1));
  EXPECT_EQ(window.Begin(7, 9, 26).code(), absl::StatusCode::kUnavailable);
  ASSERT_TRUE(window.Acknowledge(7, 4).value());
  ASSERT_TRUE(window.Begin(7, 9, 26).ok());
  EXPECT_EQ(window.inflight_frames(), window.kMaxFrames);
  EXPECT_EQ(window.inflight_bytes(), 26 * window.kMaxFrames);
}

TEST(FullSyncRecordWindowTest,
     ByteCreditIncludesPartialFramesAndCannotOverflow) {
  FullSyncRecordWindow window;
  ASSERT_TRUE(window.Begin(0, 1, window.kMaxBytes - 1).ok());
  EXPECT_TRUE(window.CanSend(1));
  EXPECT_FALSE(window.CanSend(2));
  EXPECT_EQ(window.Begin(0, 2, 2).code(), absl::StatusCode::kUnavailable);
  ASSERT_TRUE(window.Begin(0, 2, 1).ok());
  EXPECT_EQ(window.inflight_bytes(), window.kMaxBytes);
  EXPECT_FALSE(window.CanSend(1));
  EXPECT_FALSE(window.CanSend(std::numeric_limits<std::size_t>::max()));
  EXPECT_EQ(window.Begin(0, 3, std::numeric_limits<std::size_t>::max()).code(),
            absl::StatusCode::kResourceExhausted);
  ASSERT_TRUE(window.Acknowledge(0, 1).value());
  EXPECT_EQ(window.inflight_bytes(), 1);
  EXPECT_EQ(window.inflight_frames(), 1);
}

TEST(FullSyncRecordWindowTest, AcknowledgingCommitDoesNotReleaseEarlierChunks) {
  FullSyncRecordWindow window;
  ASSERT_TRUE(window.Begin(3, 1, 64).ok());
  ASSERT_TRUE(window.Begin(3, 2, 4096).ok());
  ASSERT_TRUE(window.Begin(3, 3, 64).ok());
  ASSERT_TRUE(window.Acknowledge(3, 3).value());
  EXPECT_EQ(window.inflight_frames(), 2);
  EXPECT_EQ(window.inflight_bytes(), 4160);
  ASSERT_TRUE(window.Acknowledge(3, 1).value());
  EXPECT_EQ(window.inflight_frames(), 1);
  EXPECT_EQ(window.inflight_bytes(), 4096);
  ASSERT_TRUE(window.Acknowledge(3, 2).value());
  EXPECT_EQ(window.inflight_frames(), 0);
  EXPECT_EQ(window.inflight_bytes(), 0);
}

TEST(FullSyncRecordWindowTest, ForeignAndDuplicateAcksNeverReleaseCredit) {
  FullSyncRecordWindow window;
  ASSERT_TRUE(window.Begin(5, 12, 512).ok());
  EXPECT_FALSE(window.Acknowledge(5, 0).value());
  EXPECT_FALSE(window.Acknowledge(5, 13).value());
  EXPECT_EQ(window.Acknowledge(6, 12).status().code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(window.inflight_frames(), 1);
  EXPECT_EQ(window.inflight_bytes(), 512);
  ASSERT_TRUE(window.Acknowledge(5, 12).value());
  // The shared reader rejects this false result unless another FULL ledger
  // owns the identity; the record ledger must not decrement a second time.
  EXPECT_FALSE(window.Acknowledge(5, 12).value());
  EXPECT_EQ(window.inflight_frames(), 0);
  EXPECT_EQ(window.inflight_bytes(), 0);
}

TEST(FullSyncRecordWindowTest, RegistrationAllowsAckBeforeWriterResumes) {
  FullSyncRecordWindow window;
  ASSERT_TRUE(window.Begin(2, 1, 128).ok());
  // A socket write may suspend, deliver its ACK, then resume the sender. No
  // subsequent "sent" bookkeeping is needed to make this completion visible.
  ASSERT_TRUE(window.Acknowledge(2, 1).value());
  EXPECT_EQ(window.inflight_frames(), 0);
  ASSERT_TRUE(window.Begin(2, 2, window.kMaxBytes).ok());
  EXPECT_FALSE(window.CanSend(1));
  ASSERT_TRUE(window.Acknowledge(2, 2).value());
  EXPECT_EQ(window.inflight_bytes(), 0);
}

TEST(FullSyncRecordWindowTest, SnapshotReceiptWaitsForItsOwnAck) {
  FullSyncRecordWindow window;
  EXPECT_FALSE(window.Contains(0));
  ASSERT_TRUE(window.Begin(2, 11, 1024).ok());
  ASSERT_TRUE(window.Begin(2, 12, 1024).ok());
  EXPECT_TRUE(window.Contains(11));
  EXPECT_TRUE(window.Contains(12));
  ASSERT_TRUE(window.Acknowledge(2, 12).value());
  EXPECT_TRUE(window.Contains(11));
  EXPECT_FALSE(window.Contains(12));
  ASSERT_TRUE(window.Begin(2, 13, 1024).ok());
  EXPECT_FALSE(window.Contains(12));
  EXPECT_TRUE(window.Contains(13));
  ASSERT_TRUE(window.Acknowledge(2, 11).value());
  EXPECT_FALSE(window.Contains(11));
  EXPECT_TRUE(window.Contains(13));
}

TEST(FullSyncRecordWindowTest, RejectedRegistrationDoesNotConsumeCapacity) {
  FullSyncRecordWindow window;
  EXPECT_EQ(window.Begin(0, 0, 64).code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(window.Begin(0, 1, 0).code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(window.Begin(0, 1, window.kMaxBytes + 1).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(window.inflight_frames(), 0);
  ASSERT_TRUE(window.Begin(0, 1, 64).ok());
  EXPECT_EQ(window.Begin(1, 1, 128).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(window.inflight_bytes(), 64);
  FullSyncRecordWindow replacement;
  EXPECT_FALSE(replacement.Acknowledge(0, 1).value());
  EXPECT_EQ(replacement.inflight_frames(), 0);
}

}  // namespace
}  // namespace lavik::detail
