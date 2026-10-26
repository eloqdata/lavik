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

#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>

#include "lavik/fault_injection.h"

namespace lavik::test {

// Fail speculative full-range probes; chunk/exact waiting paths still work.
// Arm before starting worker threads, or while a fixture drives all workers
// synchronously. Restore the environment after every assertion exit.
class ScopedReadBufferAllocationFailure {
 public:
  ScopedReadBufferAllocationFailure() {
    if (const auto* previous = std::getenv(kVariable)) previous_ = previous;
    if (::setenv(kVariable, "1", 1) != 0)
      throw std::runtime_error("cannot inject read buffer allocation failure");
  }
  ScopedReadBufferAllocationFailure(const ScopedReadBufferAllocationFailure&) =
      delete;
  ScopedReadBufferAllocationFailure& operator=(
      const ScopedReadBufferAllocationFailure&) = delete;
  ~ScopedReadBufferAllocationFailure() {
    if (previous_)
      ::setenv(kVariable, previous_->c_str(), 1);
    else
      ::unsetenv(kVariable);
  }

 private:
  static constexpr const char* kVariable = "LAVIK_FAIL_READ_BUFFER_ALLOCATION";
  std::optional<std::string> previous_;
};

}  // namespace lavik::test
