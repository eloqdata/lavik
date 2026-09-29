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

#include <signal.h>

#include "absl/status/status.h"

namespace lavik::meta {

// Installs the process-wide SIGINT/SIGTERM handlers used by lavik-meta.
// The handler must be async-signal-safe. Any alternate stack belongs to the
// receiving thread (Go supplies one for its threads), not to this installer.
inline absl::Status InstallShutdownSignalHandlers(void (*handler)(int)) {
  struct sigaction action{};
  sigemptyset(&action.sa_mask);
  action.sa_handler = handler;
  // Go's c-archive initializes signals before main. Replacing a handler here
  // must keep alternate-stack delivery: an asynchronous shutdown signal may
  // land on a small goroutine stack rather than the native main thread.
  action.sa_flags = SA_ONSTACK;
  if (::sigaction(SIGINT, &action, nullptr) != 0 ||
      ::sigaction(SIGTERM, &action, nullptr) != 0) {
    return absl::InternalError("sigaction setup failed");
  }
  return absl::OkStatus();
}

}  // namespace lavik::meta
