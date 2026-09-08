#include "keylane/fault_injection.h"

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <new>
#include <optional>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace {

static_assert(KEYLANE_FAULTS_ENABLED == KEYLANE_FAULT_TEST_EXPECTED_ENABLED);
constexpr bool kEnabled = KEYLANE_FAULT_TEST_EXPECTED_ENABLED;
constexpr char kKeyVariable[] = "KEYLANE_FAULT_HELPER_TEST_KEY";
constexpr char kOrdinalVariable[] = "KEYLANE_FAULT_HELPER_TEST_ORDINAL";

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
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, ""));
  key.Set("alpha");
  EXPECT_EQ(KEYLANE_FAULT_MATCHES(kKeyVariable, "alpha"), kEnabled);
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, "alph"));
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, "alpha-more"));
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, "ALPHA"));
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable,
                                     std::string_view("alpha\0suffix", 12)));
  EXPECT_FALSE(
      KEYLANE_FAULT_MATCHES(kKeyVariable, std::string_view("alpha\0", 6)));
  key.Set("");
  EXPECT_EQ(KEYLANE_FAULT_MATCHES(kKeyVariable, ""), kEnabled);
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, "alpha"));
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(kKeyVariable, std::string_view("\0", 1)));
  key.Set("rearmed");
  EXPECT_EQ(KEYLANE_FAULT_MATCHES(kKeyVariable, "rearmed"), kEnabled);
}

TEST(FaultInjectionTest, InjectIsASingleStatementAndAcceptsCommaBodies) {
  int count = 0;
  if (true)
    KEYLANE_FAULT_INJECT(int first = 2, second = 3; count += first + second;);
  else
    count = -1;
  EXPECT_EQ(count, kEnabled ? 5 : 0);
  KEYLANE_FAULT_INJECT(++count; ++count;);
  EXPECT_EQ(count, kEnabled ? 7 : 0);
}

TEST(FaultInjectionTest, MatchAndThrowArgumentsAreErasedInRelease) {
  ScopedEnvironment key(kKeyVariable);
  key.Set("armed");
  int variable_calls = 0;
  int key_calls = 0;
  EXPECT_EQ(KEYLANE_FAULT_MATCHES((++variable_calls, kKeyVariable),
                                  (++key_calls, "armed")),
            kEnabled);
  EXPECT_EQ(variable_calls, kEnabled ? 1 : 0);
  EXPECT_EQ(key_calls, kEnabled ? 1 : 0);

  bool thrown = false;
  try {
    KEYLANE_FAULT_BAD_ALLOC((++variable_calls, kKeyVariable),
                            (++key_calls, "armed"));
  } catch (const std::bad_alloc&) {
    thrown = true;
  }
  EXPECT_EQ(thrown, kEnabled);
  EXPECT_EQ(variable_calls, kEnabled ? 2 : 0);
  EXPECT_EQ(key_calls, kEnabled ? 2 : 0);
  EXPECT_NO_THROW(KEYLANE_FAULT_BAD_ALLOC(kKeyVariable, "not-armed"));
  key.Set("");
  if constexpr (kEnabled) {
    EXPECT_THROW(KEYLANE_FAULT_BAD_ALLOC(kKeyVariable, ""), std::bad_alloc);
  } else {
    EXPECT_NO_THROW(KEYLANE_FAULT_BAD_ALLOC(kKeyVariable, ""));
  }
}

TEST(FaultInjectionTest, NthMatchIsStrictOneBasedAndCallerLocal) {
  ScopedEnvironment key(kKeyVariable);
  ScopedEnvironment ordinal(kOrdinalVariable);
  key.Set("armed");
  EXPECT_FALSE(
      KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  for (const char* invalid :
       {"", "0", "-1", "+1", " 1", "1 ", "1x", "18446744073709551616"}) {
    SCOPED_TRACE(invalid);
    ordinal.Set(invalid);
    EXPECT_FALSE(
        KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  }
  ordinal.Set("2");
  EXPECT_FALSE(
      KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 0));
  EXPECT_FALSE(
      KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 1));
  EXPECT_FALSE(
      KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "other", kOrdinalVariable, 2));
  for (int command = 0; command < 3; ++command) {
    // Matching does not consume a shared hit counter between commands.
    EXPECT_EQ(
        KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable, 2),
        kEnabled);
  }
  ordinal.Set("18446744073709551615");
  EXPECT_EQ(
      KEYLANE_FAULT_MATCHES_NTH(kKeyVariable, "armed", kOrdinalVariable,
                                std::numeric_limits<std::uint64_t>::max()),
      kEnabled);
  int evaluations[4]{};
  (void)KEYLANE_FAULT_MATCHES_NTH(
      (++evaluations[0], kKeyVariable), (++evaluations[1], "armed"),
      (++evaluations[2], kOrdinalVariable), (++evaluations[3], 2));
  for (const int evaluated : evaluations)
    EXPECT_EQ(evaluated, kEnabled ? 1 : 0);
}

TEST(FaultInjectionTest, ReleaseErasesEvenUnavailableArgumentNames) {
#if !KEYLANE_FAULT_TEST_EXPECTED_ENABLED
  // An inline no-op function would still parse/evaluate these arguments. The
  // production macros must remove the entire fault-only expression instead.
  KEYLANE_FAULT_INJECT(UnavailableFaultType value = MissingFaultFactory(););
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES(missing_variable, missing_key));
  EXPECT_FALSE(KEYLANE_FAULT_MATCHES_NTH(missing_variable, missing_key,
                                         missing_ordinal_variable,
                                         missing_ordinal));
  KEYLANE_FAULT_BAD_ALLOC(missing_variable, missing_key);
  KEYLANE_MAYBE_CRASH_AT(missing_crash_point);
#else
  SUCCEED();
#endif
}

TEST(FaultInjectionTest, MatchedCrashExits86AndNonmatchesReturn) {
  EXPECT_EQ(ChildExit([] {
              if (::setenv("KEYLANE_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              KEYLANE_MAYBE_CRASH_AT("other");
              KEYLANE_MAYBE_CRASH_AT("selected");
            }),
            kEnabled ? 86 : 0);
  EXPECT_EQ(ChildExit([] {
              if (::setenv("KEYLANE_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              KEYLANE_MAYBE_CRASH_AT("selected-suffix");
            }),
            0);
}

TEST(FaultInjectionTest, CrashCachesInitiallyUnsetSelectorAndErasesArguments) {
  // The parent never calls CrashAt: every fork starts with an uninitialized
  // selector. Cache null before rearming, avoiding any assumption about the
  // lifetime of an old getenv pointer after replacing that variable.
  EXPECT_EQ(ChildExit([] {
              if (::unsetenv("KEYLANE_CRASH_POINT") != 0) std::_Exit(31);
              int evaluations = 0;
              KEYLANE_MAYBE_CRASH_AT((++evaluations, "selected"));
              if (evaluations != (kEnabled ? 1 : 0)) std::_Exit(32);
              if (::setenv("KEYLANE_CRASH_POINT", "selected", 1) != 0)
                std::_Exit(31);
              KEYLANE_MAYBE_CRASH_AT("selected");
            }),
            0);
}

}  // namespace
