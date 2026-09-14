// Destructive benchmark-only reset. Never use this on a database to preserve.
// Clearing every block header prevents stale records from becoming visible if
// a fresh allocation bitmap is persisted before the first new record is flushed.
// Payload regions are not securely erased. The host must verify mounts/users
// before invocation; O_EXCL additionally refuses mounted/claimed block devices.
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

constexpr size_t block = 8 * 1024 * 1024;
constexpr size_t header = 8192;
constexpr unsigned long long expected_capacity = 1919850381312ULL;

void check(bool ok, const std::string& message) {
  if (!ok) { perror(message.c_str()); std::exit(1); }
}
void clear(const std::string path) {
  int fd = open(path.c_str(), O_RDWR | O_DIRECT | O_EXCL);
  check(fd >= 0, "exclusive open " + path);
  unsigned long long size = 0;
  check(ioctl(fd, BLKGETSIZE64, &size) == 0 && size == expected_capacity,
        "unexpected device capacity " + path);
  void* zero = nullptr;
  void* readback = nullptr;
  check(posix_memalign(&zero, 4096, block) == 0, "aligned zero buffer");
  check(posix_memalign(&readback, 4096, block) == 0, "aligned read buffer");
  memset(zero, 0, block);
  check(pwrite(fd, zero, block, 0) == block, "metadata write " + path);
  for (unsigned long long off = block; off + block <= size; off += block)
    check(pwrite(fd, zero, header, off) == header, "block header write " + path);
  check(fdatasync(fd) == 0, "sync " + path);
  check(pread(fd, readback, block, 0) == block && memcmp(zero, readback, block) == 0,
        "metadata verify " + path);
  for (unsigned long long off = block; off + block <= size; off += block)
    check(pread(fd, readback, header, off) == header &&
          memcmp(zero, readback, header) == 0, "block header verify " + path);
  close(fd);
  free(zero);
  free(readback);
  std::cout << path << " verified metadata and " << size / block - 1
            << " data-block headers zero" << std::endl;
}
int main(int argc, char** argv) {
  check(argc == 2 && std::string(argv[1]) == "--erase-six-benchmark-disks",
        "explicit destructive selector required");
  std::vector<std::thread> jobs;
  for (const char* path : {"/dev/nvme1n1", "/dev/nvme2n1", "/dev/nvme3n1",
                           "/dev/nvme4n1", "/dev/nvme5n1", "/dev/nvme6n1"})
    jobs.emplace_back(clear, path);
  for (auto& job : jobs) job.join();
}
