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

#include "lavik/fault_injection.h"

#if LAVIK_FAULTS_ENABLED
#include <unistd.h>

#include <chrono>

#include "absl/status/status.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace lavik::fault_injection {

// A process fixture creates the configured file only after setup, waits for
// this log acknowledgement, changes external state, then unlinks to release.
// Bound the wait so a failed fixture cannot strand an admitted operation.
inline bycorf::Task<absl::Status> PauseWhileFileExists(const char* variable) {
  const char* path = std::getenv(variable);
  if (path == nullptr || ::access(path, F_OK) != 0) co_return absl::OkStatus();
  spdlog::warn("fault pause reached: {}", variable);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (::access(path, F_OK) == 0) {
    if (std::chrono::steady_clock::now() >= deadline) {
      co_return absl::DeadlineExceededError("fault pause was not released");
    }
    auto status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                            std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::fault_injection
#endif
