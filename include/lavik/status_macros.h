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

// These macros expand the usual `auto result = expr; check; return/assign`
// sequence. Each expression runs once. Owning the result also keeps co_await
// safe when a temporary Bycorf Task releases its frame after the initializer.
// Passing an existing result copies it; use std::move to consume it instead.

// Propagate an absl::Status error from an ordinary function.
#define LAVIK_RETURN_IF_ERROR(...)                     \
  do {                                                 \
    auto lavik_internal_status_result = (__VA_ARGS__); \
    if (!lavik_internal_status_result.ok()) {          \
      return lavik_internal_status_result;             \
    }                                                  \
  } while (false)

// Propagate an absl::Status error from a coroutine whose promise accepts it.
#define LAVIK_CO_RETURN_IF_ERROR(...)                  \
  do {                                                 \
    auto lavik_internal_status_result = (__VA_ARGS__); \
    if (!lavik_internal_status_result.ok()) {          \
      co_return lavik_internal_status_result;          \
    }                                                  \
  } while (false)

// Propagate a StatusOr error or move its value into an existing lhs. lhs is
// evaluated only on success; declarations are not supported. A borrowed view
// assigned to lhs must not outlive its owner. Parenthesize lhs if it has
// commas.
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
