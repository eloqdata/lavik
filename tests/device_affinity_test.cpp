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

#include "../src/storage/engine/device_affinity.h"

#include <gtest/gtest.h>

namespace lavik::storage {
namespace {

TEST(DeviceAffinityTest, RejectsInsufficientQpairCapacity) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0000:01:00.0", .foreground_weight_ = 10, .io_qpair_count_ = 2},
      {.id_ = "0000:02:00.0", .foreground_weight_ = 10, .io_qpair_count_ = 1},
  };
  auto plan = PlanControllerAffinity(controllers, 4);
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), absl::StatusCode::kResourceExhausted);
}

TEST(DeviceAffinityTest, SingleControllerCoversEveryWorker) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0000:01:00.0", .foreground_weight_ = 10, .io_qpair_count_ = 4},
  };
  auto plan = PlanControllerAffinity(controllers, 4);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->controller_quotas_, (std::vector<unsigned>{4}));
  for (const auto& owned : plan->worker_controllers_) {
    EXPECT_EQ(owned, (std::vector<std::size_t>{0}));
  }
}

TEST(DeviceAffinityTest, AllocatesWorkersByCapacityWeightAndQpairCap) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0000:01:00.0", .foreground_weight_ = 1, .io_qpair_count_ = 4},
      {.id_ = "0000:02:00.0", .foreground_weight_ = 3, .io_qpair_count_ = 4},
  };
  auto plan = PlanControllerAffinity(controllers, 4);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->controller_quotas_, (std::vector<unsigned>{1, 3}));
  for (const auto& owned : plan->worker_controllers_) {
    EXPECT_EQ(owned.size(), 1);
  }

  const std::vector<ControllerAffinityInput> capped{
      {.id_ = "0000:01:00.0", .foreground_weight_ = 100, .io_qpair_count_ = 1},
      {.id_ = "0000:02:00.0", .foreground_weight_ = 1, .io_qpair_count_ = 3},
  };
  plan = PlanControllerAffinity(capped, 4);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->controller_quotas_, (std::vector<unsigned>{1, 3}));
}

TEST(DeviceAffinityTest, AssignsEveryControllerWhenControllersExceedWorkers) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0000:01:00.0", .foreground_weight_ = 10, .io_qpair_count_ = 1},
      {.id_ = "0000:02:00.0", .foreground_weight_ = 5, .io_qpair_count_ = 1},
      {.id_ = "0000:03:00.0", .foreground_weight_ = 1, .io_qpair_count_ = 1},
  };
  auto plan = PlanControllerAffinity(controllers, 2);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->worker_controllers_[0], (std::vector<std::size_t>{0}));
  EXPECT_EQ(plan->worker_controllers_[1], (std::vector<std::size_t>{1, 2}));
}

TEST(DeviceAffinityTest, SixControllersGiveEveryWorkerDirectAccess) {
  std::vector<ControllerAffinityInput> controllers;
  for (unsigned controller = 0; controller < 6; ++controller) {
    controllers.push_back({.id_ = std::to_string(controller),
                           .foreground_weight_ = 10,
                           .io_qpair_count_ = 16});
  }
  auto plan = PlanControllerAffinity(controllers, 16);
  ASSERT_TRUE(plan.ok()) << plan.status();
  // More read access must preserve the existing weighted write distribution.
  EXPECT_EQ(plan->controller_quotas_,
            (std::vector<unsigned>{3, 3, 3, 3, 2, 2}));
  for (const auto& home : plan->worker_controllers_) {
    EXPECT_EQ(home.size(), 1);
  }
  ASSERT_EQ(plan->controller_qpair_workers_.size(), 6);
  for (const auto& workers : plan->controller_qpair_workers_) {
    ASSERT_EQ(workers.size(), 16);
    for (unsigned worker = 0; worker < 16; ++worker) {
      EXPECT_EQ(workers[worker], worker);
    }
  }
}

TEST(DeviceAffinityTest, SpareQpairsRespectCapacityAndRetainHomeAccess) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0", .foreground_weight_ = 10, .io_qpair_count_ = 2},
      {.id_ = "1", .foreground_weight_ = 10, .io_qpair_count_ = 3},
      {.id_ = "2", .foreground_weight_ = 10, .io_qpair_count_ = 4},
  };
  auto plan = PlanControllerAffinity(controllers, 6);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->controller_quotas_, (std::vector<unsigned>{2, 2, 2}));
  std::vector<unsigned> qpair_counts(6, 0);
  for (std::size_t controller = 0; controller < controllers.size();
       ++controller) {
    const auto& workers = plan->controller_qpair_workers_[controller];
    EXPECT_EQ(workers.size(), controllers[controller].io_qpair_count_);
    EXPECT_TRUE(std::is_sorted(workers.begin(), workers.end()));
    EXPECT_EQ(std::adjacent_find(workers.begin(), workers.end()),
              workers.end());
    for (const auto worker : workers) {
      ASSERT_LT(worker, qpair_counts.size());
      ++qpair_counts[worker];
    }
  }
  for (unsigned worker = 0; worker < 6; ++worker) {
    EXPECT_GE(qpair_counts[worker], 1);
    EXPECT_LE(qpair_counts[worker], 2);
    for (const auto controller : plan->worker_controllers_[worker]) {
      const auto& workers = plan->controller_qpair_workers_[controller];
      EXPECT_TRUE(std::binary_search(workers.begin(), workers.end(), worker));
    }
  }
}

TEST(DeviceAffinityTest, MoreControllersThanWorkersCapAccessAtWorkerCount) {
  const std::vector<ControllerAffinityInput> controllers{
      {.id_ = "0", .foreground_weight_ = 10, .io_qpair_count_ = 16},
      {.id_ = "1", .foreground_weight_ = 5, .io_qpair_count_ = 16},
      {.id_ = "2", .foreground_weight_ = 1, .io_qpair_count_ = 16},
  };
  auto plan = PlanControllerAffinity(controllers, 2);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->worker_controllers_[0], (std::vector<std::size_t>{0}));
  EXPECT_EQ(plan->worker_controllers_[1], (std::vector<std::size_t>{1, 2}));
  for (const auto& workers : plan->controller_qpair_workers_) {
    EXPECT_EQ(workers, (std::vector<std::uint16_t>{0, 1}));
  }
}

}  // namespace
}  // namespace lavik::storage
