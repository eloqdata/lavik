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

#include <type_traits>
#include <utility>

#include "absl/status/status.h"

namespace lavik::detail {

// Borrow named results without copying their Status or value. Materialize
// rvalues before the initializer ends: co_await on a temporary Bycorf Task
// returns a reference into the child frame, which the Task then destroys.
template <typename T>
auto BorrowOrOwnStatusResult(T&& result)
    -> std::conditional_t<std::is_lvalue_reference_v<T>, T,
                          std::remove_cvref_t<T>> {
  return std::forward<T>(result);
}

template <typename T>
decltype(auto) PropagationStatus(T&& result) {
  if constexpr (std::is_same_v<std::remove_cvref_t<T>, absl::Status>) {
    return std::forward<T>(result);
  } else {
    return std::forward<T>(result).status();
  }
}

}  // namespace lavik::detail

// Propagate an absl::Status or StatusOr error from an ordinary function.
// Evaluate once, borrow lvalues, and own rvalues; extract Status only on error.
// A named StatusOr keeps its value available after a successful check.
#define LAVIK_RETURN_IF_ERROR(...)                               \
  do {                                                           \
    auto&& lavik_internal_status_result =                        \
        ::lavik::detail::BorrowOrOwnStatusResult((__VA_ARGS__)); \
    if (!lavik_internal_status_result.ok()) {                    \
      return ::lavik::detail::PropagationStatus(                 \
          std::forward<decltype(lavik_internal_status_result)>(  \
              lavik_internal_status_result));                    \
    }                                                            \
  } while (false)

// Coroutine counterpart of LAVIK_RETURN_IF_ERROR; the promise must accept
// the propagated absl::Status. The expression may contain co_await.
#define LAVIK_CO_RETURN_IF_ERROR(...)                            \
  do {                                                           \
    auto&& lavik_internal_status_result =                        \
        ::lavik::detail::BorrowOrOwnStatusResult((__VA_ARGS__)); \
    if (!lavik_internal_status_result.ok()) {                    \
      co_return ::lavik::detail::PropagationStatus(              \
          std::forward<decltype(lavik_internal_status_result)>(  \
              lavik_internal_status_result));                    \
    }                                                            \
  } while (false)

// Propagate a StatusOr error or move its value into an existing lhs. lhs is
// evaluated only on success; declarations are not supported. A borrowed view
// assigned to lhs must not outlive its owner. Parenthesize lhs if it has
// commas. Evaluate the result expression once and own its result; passing an
// existing result copies it, so use std::move to consume it instead.
#define LAVIK_ASSIGN_OR_RETURN(lhs, ...)                       \
  do {                                                         \
    auto lavik_internal_status_result = (__VA_ARGS__);         \
    if (!lavik_internal_status_result.ok()) {                  \
      return std::move(lavik_internal_status_result).status(); \
    }                                                          \
    (lhs) = *std::move(lavik_internal_status_result);          \
  } while (false)

// Coroutine counterpart of LAVIK_ASSIGN_OR_RETURN; the promise must accept
// the propagated absl::Status. The expression may contain co_await.
#define LAVIK_ASSIGN_OR_CO_RETURN(lhs, ...)                       \
  do {                                                            \
    auto lavik_internal_status_result = (__VA_ARGS__);            \
    if (!lavik_internal_status_result.ok()) {                     \
      co_return std::move(lavik_internal_status_result).status(); \
    }                                                             \
    (lhs) = *std::move(lavik_internal_status_result);             \
  } while (false)
