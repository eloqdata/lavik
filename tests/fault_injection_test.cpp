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

#include "lavik/fault_injection.h"

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace {

static_assert(LAVIK_FAULTS_ENABLED == LAVIK_FAULT_TEST_EXPECTED_ENABLED);
constexpr bool kEnabled = LAVIK_FAULT_TEST_EXPECTED_ENABLED;
constexpr char kKeyVariable[] = "LAVIK_FAULT_HELPER_TEST_KEY";
constexpr char kOrdinalVariable[] = "LAVIK_FAULT_HELPER_TEST_ORDINAL";

// Tests mutate the environment only on this single test thread. Each target
// is a separate process, so its independently compiled fault policy cannot
// share inline definitions or cached selectors with another build mode.
class ScopedEnvironment {
 public:
  explicit ScopedEnvironment(const char* name) : name_(name) {
    if (const char* value = std::getenv(name)) previous_.emplace(value);
    EXPECT_EQ(::unsetenv(name), 0);
  }
  ~ScopedEnvironment() {
    if (previous_)
      (void)::setenv(name_, previous_->c_str(), 1);
    else
      (void)::unsetenv(name_);
  }
  void Set(const char* value) { ASSERT_EQ(::setenv(name_, value, 1), 0); }

 private:
  const char* name_;
  std::optional<std::string> previous_;
};

template <typename Body>
int ChildExit(Body body) {
  const pid_t child = ::fork();
  if (child < 0) return -1;
  if (child == 0) {
    body();
    std::_Exit(0);
  }
  int status = 0;
  pid_t waited;
  do {
    waited = ::waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  if (waited != child) return -1;
  return WIFEXITED(status) ? WEXITSTATUS(status) : -128;
}

TEST(FaultInjectionTest, ExactDynamicKeysIncludeEmptyAndBinaryBoundaries) {
  ScopedEnvironment key(kKeyVariable);
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, ""));
  key.Set("alpha");
  EXPECT_EQ(LAVIK_FAULT_MATCHES(kKeyVariable, "alpha"), kEnabled);
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, "alph"));
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, "alpha-more"));
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, "ALPHA"));
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES(kKeyVariable, std::string_view("alpha\0suffix", 12)));
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES(kKeyVariable, std::string_view("alpha\0", 6)));
  key.Set("");
  EXPECT_EQ(LAVIK_FAULT_MATCHES(kKeyVariable, ""), kEnabled);
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, "alpha"));
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(kKeyVariable, std::string_view("\0", 1)));
  key.Set("rearmed");
  EXPECT_EQ(LAVIK_FAULT_MATCHES(kKeyVariable, "rearmed"), kEnabled);
}

TEST(FaultInjectionTest, InjectIsASingleStatementAndAcceptsCommaBodies) {
  int count = 0;
  if (true)
    LAVIK_FAULT_INJECT(int first = 2, second = 3; count += first + second;);
  else
    count = -1;
  EXPECT_EQ(count, kEnabled ? 5 : 0);
  LAVIK_FAULT_INJECT(++count; ++count;);
  EXPECT_EQ(count, kEnabled ? 7 : 0);
}

TEST(FaultInjectionTest, MatchArgumentsAreErasedInRelease) {
  ScopedEnvironment key(kKeyVariable);
  key.Set("armed");
  int variable_calls = 0;
  int key_calls = 0;
  EXPECT_EQ(LAVIK_FAULT_MATCHES((++variable_calls, kKeyVariable),
                                (++key_calls, "armed")),
            kEnabled);
  EXPECT_EQ(variable_calls, kEnabled ? 1 : 0);
  EXPECT_EQ(key_calls, kEnabled ? 1 : 0);
}

TEST(FaultInjectionTest, NthMatchIsStrictOneBasedAndCallerLocal) {
  ScopedEnvironment key(kKeyVariable);
  ScopedEnvironment ordinal(kOrdinalVariable);
  key.Set("armed");
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  for (const char* invalid :
       {"", "0", "-1", "+1", " 1", "1 ", "1x", "18446744073709551616"}) {
    SCOPED_TRACE(invalid);
    ordinal.Set(invalid);
    EXPECT_FALSE(
        LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  }
  ordinal.Set("2");
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 0));
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  EXPECT_FALSE(
      LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "other", kOrdinalVariable, 2));
  for (int command = 0; command < 3; ++command) {
    // Matching does not consume a shared hit counter between commands.
    EXPECT_EQ(
        LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 2),
        kEnabled);
  }
  ordinal.Set("18446744073709551615");
  EXPECT_EQ(LAVIK_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable,
                                    std::numeric_limits<std::uint64_t>::max()),
            kEnabled);
  int evaluations[4]{};
  (void)LAVIK_FAULT_MATCHES_NTH(
      (++evaluations[0], kKeyVariable), (++evaluations[1], "armed"),
      (++evaluations[2], kOrdinalVariable), (++evaluations[3], 2));
  for (const int evaluated : evaluations)
    EXPECT_EQ(evaluated, kEnabled ? 1 : 0);
}

TEST(FaultInjectionTest, ReleaseErasesEvenUnavailableArgumentNames) {
#if !LAVIK_FAULT_TEST_EXPECTED_ENABLED
  // An inline no-op function would still parse/evaluate these arguments. The
  // production macros must remove the entire fault-only expression instead.
  LAVIK_FAULT_INJECT(UnavailableFaultType value = MissingFaultFactory(););
  EXPECT_FALSE(LAVIK_FAULT_MATCHES(missing_variable, missing_key));
  EXPECT_FALSE(LAVIK_FAULT_MATCHES_NTH(missing_variable, missing_key,
                                       missing_ordinal_variable,
                                       missing_ordinal));
  LAVIK_MAYBE_CRASH_AT(missing_crash_point);
#else
  SUCCEED();
#endif
}

TEST(FaultInjectionTest, MatchedCrashExits86AndNonmatchesReturn) {
  EXPECT_EQ(ChildExit([] {
              if (::setenv("LAVIK_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              LAVIK_MAYBE_CRASH_AT("other");
              LAVIK_MAYBE_CRASH_AT("selected");
            }),
            kEnabled ? 86 : 0);
  EXPECT_EQ(ChildExit([] {
              if (::setenv("LAVIK_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              LAVIK_MAYBE_CRASH_AT("selected-suffix");
            }),
            0);
}

TEST(FaultInjectionTest, CrashCachesInitiallyUnsetSelectorAndErasesArguments) {
  // The parent never calls CrashAt: every fork starts with an uninitialized
  // selector. Cache null before rearming, avoiding any assumption about the
  // lifetime of an old getenv pointer after replacing that variable.
  EXPECT_EQ(ChildExit([] {
              if (::unsetenv("LAVIK_CRASH_POINT") != 0) std::_Exit(31);
              int evaluations = 0;
              LAVIK_MAYBE_CRASH_AT((++evaluations, "selected"));
              if (evaluations != (kEnabled ? 1 : 0)) std::_Exit(32);
              if (::setenv("LAVIK_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              LAVIK_MAYBE_CRASH_AT("selected");
            }),
            0);
}

}  // namespace
