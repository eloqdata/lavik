#pragma once

#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>

namespace keylane::test {

// Keeps large test devices and diagnostic logs relocatable without changing
// the host's general-purpose temporary-file policy.
inline std::filesystem::path TestDataDirectory() {
  const char* configured = std::getenv("KEYLANE_TEST_DATA_DIR");
  return configured != nullptr && configured[0] != '\0'
             ? std::filesystem::path(configured)
             : std::filesystem::path("/tmp");
}

// Resolves one test artifact beneath TestDataDirectory.
inline std::string TestDataPath(std::string_view filename) {
  return (TestDataDirectory() / filename).string();
}

// Supports suites that derive several sibling artifact names from one prefix.
inline std::string TestDataPathPrefix() {
  std::string prefix = TestDataDirectory().string();
  if (!prefix.ends_with(std::filesystem::path::preferred_separator)) {
    prefix.push_back(std::filesystem::path::preferred_separator);
  }
  return prefix;
}

}  // namespace keylane::test
