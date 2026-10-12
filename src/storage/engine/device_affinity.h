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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::storage {

struct ControllerAffinityInput {
  std::string id_;
  std::uint64_t foreground_weight_ = 0;
  unsigned io_qpair_count_ = 0;
};

struct ControllerAffinityPlan {
  // Home assignments control write placement independently of I/O access.
  std::vector<unsigned> controller_quotas_;
  std::vector<std::vector<std::size_t>> worker_controllers_;
  // Sorted worker IDs with a qpair, including every home worker. Namespaces
  // on the same controller share this assignment and one qpair per worker.
  std::vector<std::vector<std::uint16_t>> controller_qpair_workers_;
};

// Inputs must be in stable controller-id order. The returned indexes refer to
// that order, making the same topology deterministic across restarts. Preserve
// weighted home placement, then use spare qpairs to widen direct I/O access up
// to min(controller capacity, worker count), without changing write placement.
inline absl::StatusOr<ControllerAffinityPlan> PlanControllerAffinity(
    std::span<const ControllerAffinityInput> controllers,
    unsigned worker_count) {
  if (controllers.empty() || worker_count == 0) {
    return absl::InvalidArgumentError(
        "controller affinity requires controllers and workers");
  }
  std::uint64_t total_capacity = 0;
  for (const ControllerAffinityInput& controller : controllers) {
    if (controller.id_.empty() || controller.foreground_weight_ == 0 ||
        controller.io_qpair_count_ == 0) {
      return absl::InvalidArgumentError(
          "controller affinity input has zero capacity or weight");
    }
    total_capacity +=
        std::min<unsigned>(controller.io_qpair_count_, worker_count);
  }
  const std::uint64_t required_relations =
      std::max<std::uint64_t>(worker_count, controllers.size());
  if (total_capacity < required_relations) {
    return absl::ResourceExhaustedError(
        "SPDK I/O qpair capacity is insufficient: controllers provide " +
        std::to_string(total_capacity) + " worker-controller qpairs, need " +
        std::to_string(required_relations) + " for " +
        std::to_string(worker_count) + " workers");
  }

  ControllerAffinityPlan plan;
  plan.controller_quotas_.assign(controllers.size(), 1);
  plan.worker_controllers_.resize(worker_count);
  std::uint64_t assigned = controllers.size();
  while (assigned < required_relations) {
    std::size_t selected = 0;
    bool valid = false;
    for (std::size_t i = 0; i < controllers.size(); ++i) {
      const unsigned cap =
          std::min<unsigned>(controllers[i].io_qpair_count_, worker_count);
      if (plan.controller_quotas_[i] >= cap) continue;
      if (!valid) {
        selected = i;
        valid = true;
        continue;
      }
      const long double candidate_score =
          static_cast<long double>(plan.controller_quotas_[i] + 1) /
          static_cast<long double>(controllers[i].foreground_weight_);
      const long double selected_score =
          static_cast<long double>(plan.controller_quotas_[selected] + 1) /
          static_cast<long double>(controllers[selected].foreground_weight_);
      if (candidate_score < selected_score ||
          (candidate_score == selected_score &&
           controllers[i].id_ < controllers[selected].id_)) {
        selected = i;
      }
    }
    if (!valid) {
      return absl::InternalError(
          "controller affinity capacity accounting is inconsistent");
    }
    ++plan.controller_quotas_[selected];
    ++assigned;
  }

  if (controllers.size() <= worker_count) {
    std::vector<unsigned> remaining = plan.controller_quotas_;
    std::vector<std::int64_t> current(controllers.size(), 0);
    for (unsigned worker = 0; worker < worker_count; ++worker) {
      std::size_t selected = 0;
      bool valid = false;
      for (std::size_t i = 0; i < controllers.size(); ++i) {
        current[i] += plan.controller_quotas_[i];
        if (remaining[i] != 0 && (!valid || current[i] > current[selected])) {
          selected = i;
          valid = true;
        }
      }
      if (!valid) {
        return absl::InternalError(
            "controller affinity quota assignment is inconsistent");
      }
      current[selected] -= worker_count;
      --remaining[selected];
      plan.worker_controllers_[worker].push_back(selected);
    }
  } else {
    std::vector<std::size_t> placement(controllers.size());
    for (std::size_t i = 0; i < placement.size(); ++i) placement[i] = i;
    std::sort(placement.begin(), placement.end(),
              [&controllers](std::size_t left, std::size_t right) {
                if (controllers[left].foreground_weight_ !=
                    controllers[right].foreground_weight_) {
                  return controllers[left].foreground_weight_ >
                         controllers[right].foreground_weight_;
                }
                return controllers[left].id_ < controllers[right].id_;
              });
    std::vector<std::uint64_t> worker_weights(worker_count, 0);
    for (const std::size_t controller : placement) {
      const auto lightest =
          std::min_element(worker_weights.begin(), worker_weights.end());
      const std::size_t worker = lightest - worker_weights.begin();
      plan.worker_controllers_[worker].push_back(controller);
      *lightest += controllers[controller].foreground_weight_;
    }
  }
  plan.controller_qpair_workers_.resize(controllers.size());
  std::vector<unsigned> worker_qpair_counts(worker_count, 0);
  for (unsigned worker = 0; worker < worker_count; ++worker) {
    for (const std::size_t controller : plan.worker_controllers_[worker]) {
      plan.controller_qpair_workers_[controller].push_back(worker);
      ++worker_qpair_counts[worker];
    }
  }
  for (std::size_t controller = 0; controller < controllers.size();
       ++controller) {
    auto& workers = plan.controller_qpair_workers_[controller];
    const unsigned cap =
        std::min(controllers[controller].io_qpair_count_, worker_count);
    if (workers.size() == cap) continue;

    std::vector<unsigned> candidates;
    for (unsigned worker = 0; worker < worker_count; ++worker) {
      if (!std::binary_search(workers.begin(), workers.end(), worker)) {
        candidates.push_back(worker);
      }
    }
    // Home qpairs are reserved first, so additional access cannot starve a
    // writer. Spread limited spare capacity across workers deterministically.
    std::sort(candidates.begin(), candidates.end(),
              [&worker_qpair_counts](unsigned left, unsigned right) {
                if (worker_qpair_counts[left] != worker_qpair_counts[right]) {
                  return worker_qpair_counts[left] < worker_qpair_counts[right];
                }
                return left < right;
              });
    const std::size_t extra = cap - workers.size();
    for (std::size_t i = 0; i < extra; ++i) {
      workers.push_back(candidates[i]);
      ++worker_qpair_counts[candidates[i]];
    }
    std::sort(workers.begin(), workers.end());
  }
  return plan;
}

}  // namespace lavik::storage
