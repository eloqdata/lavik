/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include "lavik/meta/failover_reconciler.h"
#include "lavik/meta/state_machine.h"

namespace lavik::test {
// Existing behavior fixtures own a complete committed cut. Restore it through
// the public state-machine boundary so every planner test exercises the real
// discovery and per-target captures instead of a second test projection.
inline absl::StatusOr<std::optional<meta::MetaCommand>> PlanFailoverFixture(
    const meta::MetaStores& stores, std::uint64_t applied_index,
    const meta::MetaObservationStore& observations,
    const meta::MetaFailoverPlannerContext& context) {
  auto machine = meta::MetaStateMachine::Open("");
  if (!machine.ok()) return machine.status();
  auto image = stores.Serialize();
  if (!image.ok()) return image.status();
  auto installed = (*machine)->Install(applied_index, *image);
  if (!installed.ok()) return installed;
  const auto discovery = (*machine)->CaptureFailoverDiscovery();
  return meta::PlanFailoverStep(
      discovery,
      [&](const auto& group, auto operation) {
        return (*machine)->CaptureFailoverPlanningView(discovery.cursor_, group,
                                                       operation);
      },
      observations, context);
}
}  // namespace lavik::test
