#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "keylane/storage/engine.h"

namespace {

constexpr std::uint64_t kMiB = 1024 * 1024;

#define ASSERT_CHECK(condition, message) ASSERT_TRUE(condition) << message

bool CreateFile(const std::string& path, std::uint64_t bytes) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    return false;
  }
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  return ::close(fd) == 0 && allocated == 0;
}

bool GrowFile(const std::string& path, std::uint64_t bytes) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  return allocated == 0 && close_error == 0;
}

std::uint64_t FileSize(const std::string& path) {
  struct stat info {};
  return ::stat(path.c_str(), &info) == 0
             ? static_cast<std::uint64_t>(info.st_size)
             : 0;
}

absl::Status Prepare(const std::vector<std::string>& paths,
                     bool reset = false) {
  keylane::storage::StorageEngineOptions options;
  options.data_files_ = paths;
  options.reset_data_files_ = reset;
  keylane::storage::StorageEngine engine(std::move(options));
  return engine.Prepare(1);
}

struct Cleanup {
  std::vector<std::string> paths_;
  ~Cleanup() {
    for (const std::string& path : paths_) {
      (void)::unlink(path.c_str());
    }
  }
};

}  // namespace

TEST(StorageCapacityTest, ValidatesAndPreservesDeviceCapacities) {
  const std::string prefix =
      "/tmp/keylane-storage-capacity-" + std::to_string(::getpid());
  Cleanup cleanup;

  const std::string unequal_a = prefix + "-unequal-a.data";
  const std::string unequal_b = prefix + "-unequal-b.data";
  cleanup.paths_.push_back(unequal_a);
  cleanup.paths_.push_back(unequal_b);
  ASSERT_CHECK(
      CreateFile(unequal_a, 88 * kMiB) && CreateFile(unequal_b, 96 * kMiB),
      "failed to create unequal-capacity files");
  ASSERT_CHECK(Prepare({unequal_a, unequal_b}).ok(),
               "unequal fresh device capacities were rejected");
  ASSERT_CHECK(
      FileSize(unequal_a) == 88 * kMiB && FileSize(unequal_b) == 96 * kMiB,
      "storage prepare changed regular-file sizes");

  ASSERT_CHECK(GrowFile(unequal_b, 104 * kMiB),
               "failed to grow initialized test file");
  ASSERT_CHECK(Prepare({unequal_a, unequal_b}).ok(),
               "larger backing file did not preserve labeled capacity");
  ASSERT_CHECK(
      ::truncate(unequal_a.c_str(), static_cast<off_t>(80 * kMiB)) == 0 &&
          !Prepare({unequal_a, unequal_b}).ok(),
      "backing file smaller than its label was accepted");

  const std::string too_small = prefix + "-small.data";
  cleanup.paths_.push_back(too_small);
  ASSERT_CHECK(CreateFile(too_small, 72 * kMiB) && !Prepare({too_small}).ok(),
               "single-device file with no foreground block was accepted");

  const std::string minimum = prefix + "-minimum.data";
  cleanup.paths_.push_back(minimum);
  ASSERT_CHECK(CreateFile(minimum, 80 * kMiB) && Prepare({minimum}).ok() &&
                   Prepare({minimum}, true).ok(),
               "80 MiB single-device minimum was rejected");

  const std::string unaligned = prefix + "-unaligned.data";
  cleanup.paths_.push_back(unaligned);
  ASSERT_CHECK(
      CreateFile(unaligned, 80 * kMiB + 4096) && !Prepare({unaligned}).ok(),
      "unaligned fresh regular file was accepted");

  const std::string missing = prefix + "-missing.data";
  ASSERT_CHECK(!Prepare({missing}).ok(), "missing storage path was created");
}

TEST(StorageCapacityTest, ExpandsAnInitializedStorageSet) {
  const std::string prefix =
      "/tmp/keylane-storage-expansion-" + std::to_string(::getpid());
  Cleanup cleanup;
  const std::string original = prefix + "-original.data";
  const std::string added = prefix + "-added.data";
  cleanup.paths_ = {original, added};

  ASSERT_CHECK(CreateFile(original, 88 * kMiB),
               "failed to create original storage file");
  ASSERT_CHECK(Prepare({original}).ok(),
               "failed to initialize original storage set");
  ASSERT_CHECK(CreateFile(added, 88 * kMiB),
               "failed to create added storage file");
  ASSERT_CHECK(Prepare({added, original}).ok(),
               "failed to expand initialized storage set");
  ASSERT_CHECK(Prepare({original, added}).ok(),
               "expanded storage set did not reopen in a new argument order");
  ASSERT_CHECK(!Prepare({original}).ok(),
               "expanded storage set reopened with a missing member");
}

TEST(StorageCapacityTest, RejectsForeignDeviceDuringExpansion) {
  const std::string prefix =
      "/tmp/keylane-storage-foreign-" + std::to_string(::getpid());
  Cleanup cleanup;
  const std::string first = prefix + "-first.data";
  const std::string foreign = prefix + "-foreign.data";
  cleanup.paths_ = {first, foreign};

  ASSERT_CHECK(CreateFile(first, 88 * kMiB) && CreateFile(foreign, 88 * kMiB),
               "failed to create foreign-device test files");
  ASSERT_CHECK(Prepare({first}).ok() && Prepare({foreign}).ok(),
               "failed to initialize independent storage sets");
  ASSERT_CHECK(!Prepare({first, foreign}).ok(),
               "foreign initialized device was accepted as an expansion");
  ASSERT_CHECK(Prepare({first, foreign}, true).ok(),
               "explicit storage reset did not replace foreign device sets");
  ASSERT_CHECK(Prepare({foreign, first}).ok(),
               "reset storage set could not be reopened");
}
