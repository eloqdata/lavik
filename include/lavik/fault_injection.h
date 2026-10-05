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

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>

// All fault sites use this single build policy. The explicit test-server option
// also enables faults in optimized sanitizer builds; package builds disable it.
#if !defined(NDEBUG) || \
    (defined(LAVIK_ENABLE_TEST_FAULTS) && LAVIK_ENABLE_TEST_FAULTS)
#define LAVIK_FAULTS_ENABLED 1
#else
#define LAVIK_FAULTS_ENABLED 0
#endif

namespace lavik::fault_injection {

#if LAVIK_FAULTS_ENABLED
// Exact, allocation-free match, including empty keys. Environment variables
// cannot represent embedded NUL bytes; such keys must not match a prefix.
// Configure the environment before starting workers, never concurrently with
// fault execution. Unlike the process crash selector, key selectors are not
// cached, so sequential in-process fixtures may re-arm them.
inline bool Matches(const char* variable, std::string_view key) noexcept {
  const char* armed = std::getenv(variable);
  return armed != nullptr && key == armed;
}

// Match a one-based position supplied by the caller, not a process-global hit
// counter. Each command therefore fails at the same selected auxiliary, even
// when other commands or workers are concurrently exercising the hook.
inline bool MatchesNth(const char* variable, std::string_view key,
                       const char* ordinal_variable,
                       std::uint64_t ordinal) noexcept {
  if (!Matches(variable, key)) return false;
  const char* configured = std::getenv(ordinal_variable);
  if (configured == nullptr) return false;
  std::uint64_t selected = 0;
  const char* end = configured + std::strlen(configured);
  const auto parsed = std::from_chars(configured, end, selected);
  return parsed.ec == std::errc{} && parsed.ptr == end && selected != 0 &&
         ordinal == selected;
}

// Cache the process-wide selector on first use. Exit 86 distinguishes an armed
// power-loss boundary from an accidental crash: no destructors or stdio flush.
inline void CrashAt(const char* point) noexcept {
  static const char* const armed = std::getenv("LAVIK_CRASH_POINT");
  if (armed != nullptr && std::strcmp(armed, point) == 0) std::_Exit(86);
}
#endif

}  // namespace lavik::fault_injection

// These macros, rather than disabled inline functions, erase argument
// evaluation, environment lookups and coroutine suspension points in Release.
// INJECT stays in the caller's coroutine: co_await/co_return preserve ownership
// and exception handling. Its block is a do/while scope; use the central
// LAVIK_FAULTS_ENABLED guard for cross-scope declarations or an outer-loop
// break/continue instead. Do not put production side effects in fault
// arguments.
#if LAVIK_FAULTS_ENABLED
#define LAVIK_FAULT_INJECT(...) \
  do {                          \
    __VA_ARGS__                 \
  } while (false)
#define LAVIK_FAULT_MATCHES(variable, key) \
  ::lavik::fault_injection::Matches((variable), (key))
#define LAVIK_FAULT_MATCHES_NTH(variable, key, ordinal_variable, ordinal)     \
  ::lavik::fault_injection::MatchesNth((variable), (key), (ordinal_variable), \
                                       (ordinal))
#define LAVIK_MAYBE_CRASH_AT(point) ::lavik::fault_injection::CrashAt((point))
#else
#define LAVIK_FAULT_INJECT(...) ((void)0)
#define LAVIK_FAULT_MATCHES(variable, key) false
#define LAVIK_FAULT_MATCHES_NTH(variable, key, ordinal_variable, ordinal) false
#define LAVIK_MAYBE_CRASH_AT(point) ((void)0)
#endif
