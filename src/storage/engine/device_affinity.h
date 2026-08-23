#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace keylane::storage {

struct ControllerAffinityInput {
  std::string id_;
  std::uint64_t foreground_weight_ = 0;
  unsigned io_qpair_count_ = 0;
};

struct ControllerAffinityPlan {
  std::vector<unsigned> controller_quotas_;
  std::vector<std::vector<std::size_t>> worker_controllers_;
};

// Inputs must be in stable controller-id order. The returned indexes refer to
// that order, making the same topology deterministic across restarts.
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
  return plan;
}

}  // namespace keylane::storage
