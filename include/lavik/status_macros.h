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

#include <utility>

// Evaluate an absl::Status expression once and return its error unchanged.
// The containing function must accept absl::Status as its return value.
#define LAVIK_RETURN_IF_ERROR(...)                                          \
  LAVIK_INTERNAL_STATUS_RETURN(                                             \
      LAVIK_INTERNAL_STATUS_CONCAT(lavik_status_result_, __LINE__), return, \
      auto&&, __VA_ARGS__)

// Coroutine counterpart of LAVIK_RETURN_IF_ERROR. The promise must accept
// absl::Status through co_return; expr may itself contain co_await. Own the
// result: Bycorf await_resume returns a reference into a temporary Task frame,
// which is destroyed at the end of the initializer's full expression.
#define LAVIK_CO_RETURN_IF_ERROR(...)                                          \
  LAVIK_INTERNAL_STATUS_RETURN(                                                \
      LAVIK_INTERNAL_STATUS_CONCAT(lavik_status_result_, __LINE__), co_return, \
      auto, __VA_ARGS__)

// Evaluate an absl::StatusOr expression once, returning its error or assigning
// its value to an existing lhs. Declarations are not supported: the macro is
// one scoped statement. lhs is evaluated only on success. Rvalue results move
// their value; lvalues retain their value category (use std::move explicitly
// to consume them). Parenthesize lhs if it contains a preprocessor comma.
// A reference/view in lhs must not outlive the result object it borrows from.
#define LAVIK_ASSIGN_OR_RETURN(lhs, ...)                                    \
  LAVIK_INTERNAL_STATUS_ASSIGN(                                             \
      LAVIK_INTERNAL_STATUS_CONCAT(lavik_status_result_, __LINE__), return, \
      auto&&, lhs, __VA_ARGS__)

// Coroutine counterpart of LAVIK_ASSIGN_OR_RETURN. Own the StatusOr result
// before a temporary Task can destroy its frame, just like `auto r = co_await
// task`. Explicitly move an existing move-only StatusOr lvalue to consume it.
// The promise must accept the propagated absl::Status.
#define LAVIK_ASSIGN_OR_CO_RETURN(lhs, ...)                                    \
  LAVIK_INTERNAL_STATUS_ASSIGN(                                                \
      LAVIK_INTERNAL_STATUS_CONCAT(lavik_status_result_, __LINE__), co_return, \
      auto, lhs, __VA_ARGS__)

#define LAVIK_INTERNAL_STATUS_CONCAT_INNER(a, b) a##b
#define LAVIK_INTERNAL_STATUS_CONCAT(a, b) \
  LAVIK_INTERNAL_STATUS_CONCAT_INNER(a, b)

// Ordinary expressions borrow lvalues and extend prvalue lifetimes; reference
// results must have an owner that outlives the statement. Coroutine expressions
// instead materialize the result before their awaitable owner is destroyed.
// Forwarding preserves the selected ownership and the original error payload.
#define LAVIK_INTERNAL_STATUS_RETURN(result, return_keyword, binding, ...) \
  do {                                                                     \
    binding result = (__VA_ARGS__);                                        \
    if (!result.ok()) {                                                    \
      return_keyword std::forward<decltype(result)>(result);               \
    }                                                                      \
  } while (false)

#define LAVIK_INTERNAL_STATUS_ASSIGN(result, return_keyword, binding, lhs, \
                                     ...)                                  \
  do {                                                                     \
    binding result = (__VA_ARGS__);                                        \
    if (!result.ok()) {                                                    \
      return_keyword std::forward<decltype(result)>(result).status();      \
    }                                                                      \
    (lhs) = *std::forward<decltype(result)>(result);                       \
  } while (false)
