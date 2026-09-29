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

#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "gtest/gtest.h"
#include "lavik/meta/shutdown_signals.h"

namespace {
alignas(16) std::array<std::byte, 64 * 1024> alternate_stack;
volatile sig_atomic_t observed_signal = 0;
volatile sig_atomic_t observed_alternate_stack = 0;

void ProbeSignal(int signal) {
  const char marker = 0;
  const auto address = reinterpret_cast<std::uintptr_t>(&marker);
  const auto begin = reinterpret_cast<std::uintptr_t>(alternate_stack.data());
  observed_signal = signal;
  observed_alternate_stack =
      address >= begin && address < begin + alternate_stack.size();
}

// Fork isolates signal dispositions and alternate stacks from gtest and other
// cases. Test actual handler execution, not merely the sigaction flag value.
void CheckDelivery(bool enable_alternate_stack) {
  const auto child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    stack_t stack{};
    stack.ss_sp = alternate_stack.data();
    stack.ss_size = alternate_stack.size();
    stack.ss_flags = enable_alternate_stack ? 0 : SS_DISABLE;
    if (::sigaltstack(&stack, nullptr) != 0) ::_exit(10);
    if (!lavik::meta::InstallShutdownSignalHandlers(ProbeSignal).ok())
      ::_exit(11);
    for (const auto signal : {SIGINT, SIGTERM}) {
      observed_signal = 0;
      if (::raise(signal) != 0) ::_exit(12);
      if (observed_signal != signal) ::_exit(13);
      if (observed_alternate_stack != enable_alternate_stack) ::_exit(14);
    }
    ::_exit(0);
  }
  int status = 0;
  pid_t waited;
  do {
    waited = ::waitpid(child, &status, 0);
  } while (waited == -1 && errno == EINTR);
  ASSERT_EQ(waited, child);
  ASSERT_TRUE(WIFEXITED(status)) << status;
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(MetaShutdownSignals, UsesReceivingThreadsAlternateStack) {
  CheckDelivery(true);
}

TEST(MetaShutdownSignals, AllowsNativeThreadWithoutAlternateStack) {
  CheckDelivery(false);
}
}  // namespace
