#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "keylane/storage/engine.h"

namespace {

constexpr std::uint64_t kMiB = 1024 * 1024;

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
  }
  return condition;
}

bool CreateFile(const std::string& path, std::uint64_t bytes) {
  const int fd = ::open(path.c_str(),
                        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    return false;
  }
  const int allocated =
      ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  return ::close(fd) == 0 && allocated == 0;
}

bool GrowFile(const std::string& path, std::uint64_t bytes) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  const int allocated =
      ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  return allocated == 0 && close_error == 0;
}

std::uint64_t FileSize(const std::string& path) {
  struct stat info {};
  return ::stat(path.c_str(), &info) == 0
             ? static_cast<std::uint64_t>(info.st_size)
             : 0;
}

celer::Status Prepare(const std::vector<std::string>& paths) {
  keylane::storage::StorageEngineOptions options;
  options.data_files = paths;
  keylane::storage::StorageEngine engine(std::move(options));
  return engine.Prepare(1);
}

struct Cleanup {
  std::vector<std::string> paths;
  ~Cleanup() {
    for (const std::string& path : paths) {
      (void)::unlink(path.c_str());
    }
  }
};

}  // namespace

int main() {
  const std::string prefix =
      "/tmp/keylane-storage-capacity-" + std::to_string(::getpid());
  Cleanup cleanup;

  const std::string unequal_a = prefix + "-unequal-a.data";
  const std::string unequal_b = prefix + "-unequal-b.data";
  cleanup.paths.push_back(unequal_a);
  cleanup.paths.push_back(unequal_b);
  if (!Check(CreateFile(unequal_a, 80 * kMiB) &&
                 CreateFile(unequal_b, 88 * kMiB),
             "failed to create unequal-capacity files")) {
    return 1;
  }
  if (!Check(Prepare({unequal_a, unequal_b}).ok(),
             "unequal fresh device capacities were rejected")) {
    return 1;
  }
  if (!Check(FileSize(unequal_a) == 80 * kMiB &&
                 FileSize(unequal_b) == 88 * kMiB,
             "storage prepare changed regular-file sizes")) {
    return 1;
  }

  if (!Check(GrowFile(unequal_b, 96 * kMiB),
             "failed to grow initialized test file")) {
    return 1;
  }
  if (!Check(Prepare({unequal_a, unequal_b}).ok(),
             "larger backing file did not preserve labeled capacity")) {
    return 1;
  }
  if (!Check(::truncate(unequal_a.c_str(),
                        static_cast<off_t>(72 * kMiB)) == 0 &&
                 !Prepare({unequal_a, unequal_b}).ok(),
             "backing file smaller than its label was accepted")) {
    return 1;
  }

  const std::string too_small = prefix + "-small.data";
  cleanup.paths.push_back(too_small);
  if (!Check(CreateFile(too_small, 72 * kMiB) &&
                 !Prepare({too_small}).ok(),
             "single-device file with no foreground block was accepted")) {
    return 1;
  }

  const std::string minimum = prefix + "-minimum.data";
  cleanup.paths.push_back(minimum);
  if (!Check(CreateFile(minimum, 80 * kMiB) && Prepare({minimum}).ok(),
             "80 MiB single-device minimum was rejected")) {
    return 1;
  }

  const std::string unaligned = prefix + "-unaligned.data";
  cleanup.paths.push_back(unaligned);
  if (!Check(CreateFile(unaligned, 80 * kMiB + 4096) &&
                 !Prepare({unaligned}).ok(),
             "unaligned fresh regular file was accepted")) {
    return 1;
  }

  const std::string missing = prefix + "-missing.data";
  if (!Check(!Prepare({missing}).ok(), "missing storage path was created")) {
    return 1;
  }
  return 0;
}
