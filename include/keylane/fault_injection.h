#pragma once

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>

// All fault sites use this single build policy. The explicit test-server option
// also enables faults in optimized sanitizer builds; package builds disable it.
#if !defined(NDEBUG) || \
    (defined(KEYLANE_ENABLE_TEST_FAULTS) && KEYLANE_ENABLE_TEST_FAULTS)
#define KEYLANE_FAULTS_ENABLED 1
#else
#define KEYLANE_FAULTS_ENABLED 0
#endif

namespace keylane::fault_injection {

#if KEYLANE_FAULTS_ENABLED
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

// Fail inside the caller's existing exception/rollback boundary, without an
// allocation that could obscure which deterministic fault was exercised.
inline void BadAlloc(const char* variable, std::string_view key) {
  if (Matches(variable, key)) throw std::bad_alloc();
}

// Cache the process-wide selector on first use. Exit 86 distinguishes an armed
// power-loss boundary from an accidental crash: no destructors or stdio flush.
inline void CrashAt(const char* point) noexcept {
  static const char* const armed = std::getenv("KEYLANE_CRASH_POINT");
  if (armed != nullptr && std::strcmp(armed, point) == 0) std::_Exit(86);
}
#endif

}  // namespace keylane::fault_injection

// These macros, rather than disabled inline functions, erase argument
// evaluation, environment lookups and coroutine suspension points in Release.
// INJECT stays in the caller's coroutine: co_await/co_return preserve ownership
// and exception handling. Its block is a do/while scope; use the central
// KEYLANE_FAULTS_ENABLED guard for cross-scope declarations or an outer-loop
// break/continue instead. Do not put production side effects in fault
// arguments.
#if KEYLANE_FAULTS_ENABLED
#define KEYLANE_FAULT_INJECT(...) \
  do {                            \
    __VA_ARGS__                   \
  } while (false)
#define KEYLANE_FAULT_MATCHES(variable, key) \
  ::keylane::fault_injection::Matches((variable), (key))
#define KEYLANE_FAULT_MATCHES_NTH(variable, key, ordinal_variable, ordinal) \
  ::keylane::fault_injection::MatchesNth((variable), (key),                 \
                                         (ordinal_variable), (ordinal))
#define KEYLANE_FAULT_BAD_ALLOC(variable, key) \
  ::keylane::fault_injection::BadAlloc((variable), (key))
#define KEYLANE_MAYBE_CRASH_AT(point) \
  ::keylane::fault_injection::CrashAt((point))
#else
#define KEYLANE_FAULT_INJECT(...) ((void)0)
#define KEYLANE_FAULT_MATCHES(variable, key) false
#define KEYLANE_FAULT_MATCHES_NTH(variable, key, ordinal_variable, ordinal) \
  false
#define KEYLANE_FAULT_BAD_ALLOC(variable, key) ((void)0)
#define KEYLANE_MAYBE_CRASH_AT(point) ((void)0)
#endif
