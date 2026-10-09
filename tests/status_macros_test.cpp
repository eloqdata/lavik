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

#include "lavik/status_macros.h"

#include <coroutine>
#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "bycorf/runtime/task.h"
#include "gtest/gtest.h"

namespace {

absl::Status Error() {
  auto error = absl::DataLossError("original error");
  error.SetPayload("lavik.test/detail", absl::Cord("preserve payload"));
  return error;
}

struct Cleanup {
  int& count;
  ~Cleanup() { ++count; }
};

TEST(StatusMacrosTest, ReturnEvaluatesOnceAndPreservesErrorAndCleanup) {
  for (bool fail : {false, true}) {
    int calls = 0, cleanups = 0;
    bool continued = false;
    const auto status = fail ? Error() : absl::OkStatus();
    const auto run = [&]() -> absl::StatusOr<int> {
      Cleanup cleanup{cleanups};
      // Also exercise use as one unbraced if/else statement.
      if (true)
        LAVIK_RETURN_IF_ERROR((++calls, status));
      else
        return -1;
      continued = true;
      return 7;
    };
    auto result = run();
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(cleanups, 1);
    EXPECT_EQ(continued, !fail);
    EXPECT_EQ(result.status(), status);
    EXPECT_EQ(status, fail ? Error() : absl::OkStatus());
    if (result.ok()) EXPECT_EQ(*result, 7);
  }
}

TEST(StatusMacrosTest, AssignEvaluatesLhsOnlyOnSuccessAndMovesUniqueValues) {
  for (bool fail : {false, true}) {
    int calls = 0, assignments = 0;
    auto target = std::make_unique<int>(9);
    const auto produce = [&]() -> absl::StatusOr<std::unique_ptr<int>> {
      ++calls;
      if (fail) return Error();
      return std::make_unique<int>(42);
    };
    const auto run = [&]() -> absl::Status {
      if (true)
        LAVIK_ASSIGN_OR_RETURN((++assignments, target), produce());
      else
        return absl::InternalError("unexpected else");
      return absl::OkStatus();
    };
    EXPECT_EQ(run(), fail ? Error() : absl::OkStatus());
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(assignments, fail ? 0 : 1);
    EXPECT_EQ(*target, fail ? 9 : 42);
  }
}

// Suspend the child for real, without requiring an I/O worker. Tests resume
// only the handle published by the awaiter, preserving Bycorf's continuation
// transfer back into the parent macro expression.
struct Pause {
  std::coroutine_handle<>& pending;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) const { pending = handle; }
  void await_resume() const noexcept {}
};

template <typename T>
bycorf::Task<T> Delayed(T value, std::coroutine_handle<>& pending) {
  co_await Pause{pending};
  co_return std::move(value);
}

bycorf::Task<absl::StatusOr<int>> CoroutineSequence(
    bool status_failure, bool value_failure, int& calls, int& assignments,
    int& cleanups, std::unique_ptr<int>& target,
    std::coroutine_handle<>& pending) {
  Cleanup cleanup{cleanups};
  if (true)
    LAVIK_CO_RETURN_IF_ERROR(co_await Delayed(
        (++calls, status_failure ? Error() : absl::OkStatus()), pending));
  else
    co_return -1;
  if (true)
    LAVIK_ASSIGN_OR_CO_RETURN(
        (++assignments, target),
        co_await Delayed(
            (++calls, value_failure
                          ? absl::StatusOr<std::unique_ptr<int>>(Error())
                          : absl::StatusOr<std::unique_ptr<int>>(
                                std::make_unique<int>(42))),
            pending));
  else
    co_return -1;
  co_return *target;
}

TEST(StatusMacrosTest, CoroutinePropagationSurvivesSuspensionAndCleansUp) {
  for (const auto [status_failure, value_failure] :
       {std::pair{false, false}, std::pair{true, false},
        std::pair{false, true}}) {
    SCOPED_TRACE(status_failure  ? "status failure"
                 : value_failure ? "value failure"
                                 : "success");
    int calls = 0, assignments = 0, cleanups = 0, suspensions = 0;
    auto target = std::make_unique<int>(9);
    std::coroutine_handle<> pending;
    auto task = CoroutineSequence(status_failure, value_failure, calls,
                                  assignments, cleanups, target, pending);
    auto handle = std::move(task).ReleaseHandle();
    handle.resume();
    while (!handle.done() && pending && suspensions < 2) {
      EXPECT_EQ(cleanups, 0);
      auto suspended = std::exchange(pending, {});
      ++suspensions;
      suspended.resume();
    }
    EXPECT_TRUE(handle.done());
    if (handle.done()) {
      const auto& result = handle.promise().value_;
      const bool fail = status_failure || value_failure;
      EXPECT_EQ(result.status(), fail ? Error() : absl::OkStatus());
      EXPECT_EQ(assignments, fail ? 0 : 1);
      EXPECT_EQ(*target, fail ? 9 : 42);
      if (result.ok()) EXPECT_EQ(*result, 42);
    }
    handle.destroy();
    EXPECT_EQ(cleanups, 1);
    EXPECT_EQ(calls, status_failure ? 1 : 2);
    EXPECT_EQ(suspensions, calls);
  }
}

}  // namespace
