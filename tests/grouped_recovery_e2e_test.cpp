// Build authoritative record images independently of the foreground adapter.
// This prevents a writer and its recovery decoder from hiding the same bug,
// and exercises incomplete transactions without timing a process crash.
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <fstream>
#include <future>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "grouped_write_e2e_support.h"
#include "gtest/gtest.h"
#include "keylane/rdb.h"
#include "keylane/storage/detail/grouped_collection.h"
#include "keylane/storage/detail/grouped_hash.h"
#include "keylane/storage/detail/ordered_compact_codec.h"
#include "keylane/storage/format.h"
#include "support/test_data_path.h"

namespace {
using namespace keylane::storage;
using namespace std::chrono_literals;
std::string server_binary;

void Check(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

void WriteAt(int fd, std::span<const std::byte> bytes, std::uint64_t offset) {
  while (!bytes.empty()) {
    const ssize_t written = ::pwrite(fd, bytes.data(), bytes.size(), offset);
    if (written < 0 && errno == EINTR) continue;
    Check(written > 0, "writing private recovery image failed");
    bytes = bytes.subspan(written);
    offset += written;
  }
}

class RecordImage {
 public:
  RecordImage() {
    std::string pattern =
        keylane::test::TestDataPath("keylane-grouped-recovery-XXXXXX");
    fd_ = ::mkstemp(pattern.data());
    Check(fd_ >= 0, "mkstemp failed");
    path_ = std::move(pattern);
    Check(::ftruncate(fd_, kBlocks * kStorageBlockBytes) == 0,
          "sizing private recovery image failed");
    std::array<std::byte, kDirectIoAlignment> page{};
    EncodeDeviceLabel(DeviceLabel{.storage_set_id_ = 101,
                                  .device_id_ = 0,
                                  .capacity_blocks_ = kBlocks,
                                  .device_count_ = 1},
                      page);
    WriteAt(fd_, page, 0);
  }
  ~RecordImage() {
    if (fd_ >= 0) ::close(fd_);
    if (!path_.empty()) ::unlink(path_.c_str());
  }
  const std::string& path() const { return path_; }

  void Root(std::string_view key, const GroupedHashRoot& root,
            std::uint64_t sequence, std::uint64_t txid = 0) {
    auto persisted = root;
    if (persisted.revision_ == 0) persisted.revision_ = sequence;
    auto bytes = EncodeGroupedHashRoot(persisted);
    Check(bytes.ok(), "root fixture encoding failed");
    Append(key, *bytes,
           RecordHeader{
               .value_type_ = ValueType::kHash,
               .grouped_ = true,
               .logical_size_ = static_cast<std::uint32_t>(root.field_count_),
               .txid_ = txid,
               .mutation_sequence_ = sequence});
  }

  std::vector<ExtentRef> Group(std::string_view key,
                               const HashGroupSnapshot& group,
                               std::uint64_t sequence, std::uint64_t txid = 0,
                               bool external = false,
                               std::uint64_t batch_txid = 0) {
    auto encoded = EncodeHashGroup(group);
    Check(encoded.ok(), "group fixture encoding failed");
    return GroupPayload(
        key, std::move(*encoded),
        RecordHeader{.value_type_ = ValueType::kHash,
                     .external_ = external,
                     .auxiliary_group_ = true,
                     .group_retired_ = group.retired_,
                     .group_incarnation_ = group.incarnation_,
                     .group_prefix_ = group.id_.prefix_,
                     .group_prefix_bits_ = group.id_.bits_,
                     .group_batch_txid_ = batch_txid,
                     .logical_size_ = static_cast<std::uint32_t>(
                         group.value_.entries_.size()),
                     .txid_ = txid,
                     .mutation_sequence_ = sequence});
  }

  void OrderedRoot(std::string_view key, const OrderedCollectionRoot& root,
                   std::uint64_t sequence, std::uint64_t txid = 0) {
    auto encoded = EncodeOrderedCollectionRoot(root);
    Check(encoded.ok(), "ordered root fixture encoding failed");
    Append(key, *encoded,
           RecordHeader{
               .value_type_ = root.kind_ == OrderedCollectionKind::kList
                                  ? ValueType::kList
                                  : ValueType::kSortedSet,
               .grouped_ = true,
               .logical_size_ = static_cast<std::uint32_t>(root.item_count_),
               .txid_ = txid,
               .mutation_sequence_ = sequence});
  }

  std::vector<ExtentRef> OrderedGroup(std::string_view key,
                                      const OrderedGroupSnapshot& group,
                                      std::uint64_t revision,
                                      std::uint64_t txid = 0,
                                      bool external = false,
                                      std::uint64_t batch_txid = 0) {
    auto encoded = EncodeOrderedGroup(group);
    Check(encoded.ok(), "ordered group fixture encoding failed");
    return GroupPayload(
        key, std::move(*encoded),
        RecordHeader{
            .value_type_ = group.kind_ == OrderedCollectionKind::kList
                               ? ValueType::kList
                               : ValueType::kSortedSet,
            .external_ = external,
            .auxiliary_group_ = true,
            .group_retired_ = group.retired_,
            .group_incarnation_ = group.incarnation_,
            .group_prefix_ = group.id_,
            .group_batch_txid_ = batch_txid,
            .logical_size_ = static_cast<std::uint32_t>(group.entries_.size()),
            .txid_ = txid,
            .mutation_sequence_ = revision});
  }

  std::vector<ExtentRef> GroupPayload(std::string_view key, std::string payload,
                                      const RecordHeader& record) {
    std::vector<ExtentRef> extents;
    if (record.external_) {
      std::size_t offset = 0;
      while (offset < payload.size()) {
        const std::uint64_t id = Allocate();
        const std::size_t count =
            std::min(kExtentPayloadBytes, payload.size() - offset);
        const auto bytes =
            std::as_bytes(std::span(payload.data() + offset, count));
        ExtentRef ref{.block_id_ = id,
                      .allocation_epoch_ = id + 100,
                      .payload_bytes_ = static_cast<std::uint32_t>(count),
                      .payload_checksum_ = Crc32c(bytes)};
        BlockHeader header{
            .block_id_ = id,
            .allocation_epoch_ = ref.allocation_epoch_,
            .committed_bytes_ =
                static_cast<std::uint32_t>(kBlockHeaderBytes + count),
            .header_sequence_ = 1,
            .layout_worker_count_ = 1,
            .kind_ = BlockKind::kPayloadExtent,
            .extent_index_ = static_cast<std::uint32_t>(extents.size()),
            .extent_payload_bytes_ = ref.payload_bytes_,
            .extent_payload_checksum_ = ref.payload_checksum_,
        };
        std::array<std::byte, kDirectIoAlignment> page{};
        EncodeBlockHeader(header, page);
        WriteAt(fd_, page, LocalBlockOffset(id));
        WriteAt(fd_, bytes, LocalBlockOffset(id) + kBlockHeaderBytes);
        extents.push_back(ref);
        offset += count;
      }
      ExtentManifestHeader manifest{
          .extent_count_ = static_cast<std::uint32_t>(extents.size())};
      payload.resize(sizeof(manifest) + extents.size() * sizeof(ExtentRef));
      std::memcpy(payload.data(), &manifest, sizeof(manifest));
      std::memcpy(payload.data() + sizeof(manifest), extents.data(),
                  extents.size() * sizeof(ExtentRef));
    }
    Append(key, payload, record);
    return extents;
  }

  void Commit(std::uint64_t txid) {
    Append({}, {}, RecordHeader{.kind_ = RecordKind::kTxCommit, .txid_ = txid});
  }

  void Finish() {
    for (auto& [tx, block] : records_) {
      std::array<std::byte, kDirectIoAlignment> page{};
      EncodeBlockHeader(block.header_, page);
      WriteAt(fd_, page, LocalBlockOffset(block.header_.block_id_));
      WriteAt(fd_, block.payload_,
              LocalBlockOffset(block.header_.block_id_) + kBlockHeaderBytes);
    }
    std::array<std::byte, ScanBitmapBytes(kBlocks)> bitmap{};
    for (const std::uint64_t id : allocated_) {
      bitmap[id / 8] |= static_cast<std::byte>(1U << (id % 8));
    }
    std::array<std::byte, kDirectIoAlignment> page{};
    EncodeMetadataPage(MetadataPageKind::kScanBitmap, 0, 1, bitmap, page);
    WriteAt(fd_, page, kScanBitmapMetadataOffset);
    Check(::fdatasync(fd_) == 0, "syncing private recovery image failed");
  }

  void CorruptExtentBody(const ExtentRef& extent) {
    // Corrupt a non-prefix extent without changing either copy of its CRC.
    // Startup must read all extents, not stop after obtaining the envelope.
    const std::array<std::byte, 1> bad{std::byte{0xff}};
    WriteAt(fd_, bad,
            LocalBlockOffset(extent.block_id_) + kBlockHeaderBytes + 37);
    Check(::fdatasync(fd_) == 0, "syncing extent corruption failed");
  }

  void FreeExtents(std::span<const ExtentRef> extents) {
    std::array<std::byte, kBlockHeaderBytes> empty{};
    for (const ExtentRef& extent : extents) {
      allocated_.erase(
          std::find(allocated_.begin(), allocated_.end(), extent.block_id_));
      WriteAt(fd_, empty, LocalBlockOffset(extent.block_id_));
    }
    std::array<std::byte, ScanBitmapBytes(kBlocks)> bitmap{};
    for (const auto id : allocated_) {
      bitmap[id / 8] |= static_cast<std::byte>(1U << (id % 8));
    }
    std::array<std::byte, kDirectIoAlignment> page{};
    EncodeMetadataPage(MetadataPageKind::kScanBitmap, 0, 2, bitmap, page);
    WriteAt(fd_, page, kScanBitmapMetadataOffset);
    Check(::fdatasync(fd_) == 0, "syncing reclaimed fixture extents failed");
  }

 private:
  struct RecordsBlock {
    BlockHeader header_;
    std::vector<std::byte> payload_;
  };
  std::uint64_t Allocate() {
    const std::uint64_t id = DataBlockBegin(kBlocks) + allocated_.size();
    Check(id < kBlocks - 8, "fixture exhausted foreground blocks");
    allocated_.push_back(id);
    return id;
  }
  void Append(std::string_view key, std::string_view payload,
              RecordHeader record) {
    const bool tagged = record.txid_ != 0;
    auto [position, inserted] = records_.try_emplace(tagged);
    auto& block = position->second;
    if (inserted) {
      const auto id = Allocate();
      block.header_ = BlockHeader{
          .block_id_ = id,
          .allocation_epoch_ = id + 100,
          .committed_bytes_ = kBlockHeaderBytes,
          .header_sequence_ = 1,
          .layout_worker_count_ = 1,
          .kind_ = tagged ? BlockKind::kTransaction : BlockKind::kRecords,
          .tx_generation_ = tagged ? 1U : 0U,
      };
    }
    record.key_bytes_ = key.size();
    record.header_bytes_ = RecordHeaderBytes(key.size(), false, tagged, false,
                                             record.auxiliary_group_);
    record.payload_bytes_ = payload.size();
    record.total_disk_bytes_ =
        AlignRecord(record.header_bytes_ + payload.size());
    record.allocation_epoch_ = block.header_.allocation_epoch_;
    record.lsn_ = ++lsn_;
    record.payload_checksum_ = Crc32c(std::as_bytes(std::span(payload)));
    const auto offset = block.payload_.size();
    Check(offset + record.total_disk_bytes_ <= kExtentPayloadBytes,
          "fixture record block is full");
    block.payload_.resize(offset + record.total_disk_bytes_);
    auto output = std::span(block.payload_).subspan(offset);
    Check(EncodeRecordHeader(record, key, output.first(record.header_bytes_)),
          "fixture record header encoding failed");
    if (!payload.empty()) {
      std::memcpy(output.data() + record.header_bytes_, payload.data(),
                  payload.size());
    }
    block.header_.committed_bytes_ += record.total_disk_bytes_;
    ++block.header_.record_count_;
    block.header_.max_lsn_ = record.lsn_;
  }
  static constexpr std::uint64_t kBlocks = 16;
  int fd_ = -1;
  std::string path_;
  std::uint64_t lsn_ = 0;
  std::vector<std::uint64_t> allocated_;
  std::map<bool, RecordsBlock> records_;
};

class ChildServer {
 public:
  explicit ChildServer(const RecordImage& image, bool checkpoint = false,
                       std::string_view crash_point = {}, bool defrag = false,
                       bool pause_snapshot = false,
                       std::string_view fail_group_batch = {},
                       std::string_view fail_transaction_write = {},
                       std::string_view pause_record_write = {},
                       std::string_view pause_root_pin = {}) {
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Check(listener >= 0, "port socket failed");
    sockaddr_in address{.sin_family = AF_INET,
                        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
    Check(::bind(listener, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == 0,
          "port bind failed");
    socklen_t size = sizeof(address);
    Check(::getsockname(listener, reinterpret_cast<sockaddr*>(&address),
                        &size) == 0,
          "getsockname failed");
    port_ = ntohs(address.sin_port);
    ::close(listener);
    log_ = image.path() + ".log";
    dump_ = image.path() + ".rdb";
    pid_ = ::fork();
    Check(pid_ >= 0, "fork failed");
    if (pid_ == 0) {
      if (!crash_point.empty()) {
        ::setenv("KEYLANE_CRASH_POINT", std::string(crash_point).c_str(), 1);
      }
      if (pause_snapshot) ::setenv("KEYLANE_RDB_SCAN_PAUSE_MS", "1000", 1);
      if (!fail_group_batch.empty()) {
        ::setenv("KEYLANE_FAIL_GROUP_BATCH_KEY",
                 std::string(fail_group_batch).c_str(), 1);
      }
      if (!fail_transaction_write.empty()) {
        ::setenv("KEYLANE_FAIL_TX_WRITE",
                 std::string(fail_transaction_write).c_str(), 1);
      }
      if (!pause_record_write.empty()) {
        ::setenv("KEYLANE_RECORD_WRITE_PAUSE_KEY",
                 std::string(pause_record_write).c_str(), 1);
      }
      if (!pause_root_pin.empty()) {
        ::setenv("KEYLANE_GROUP_ROOT_PIN_PAUSE_KEY",
                 std::string(pause_root_pin).c_str(), 1);
      }
      const int log = ::open(log_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
      if (log < 0) ::_exit(127);
      ::dup2(log, STDOUT_FILENO);
      ::dup2(log, STDERR_FILENO);
      ::close(log);
      std::vector<std::string> args{server_binary,
                                    "--bind",
                                    "127.0.0.1",
                                    "--port",
                                    std::to_string(port_),
                                    "--threads",
                                    "2",
                                    "--recv-buffers-per-worker",
                                    "8",
                                    "--max-memory",
                                    "1G",
                                    "--flush-max-ms",
                                    "20",
                                    "--logtostderr",
                                    "--data-file",
                                    image.path()};
      args.insert(args.end(),
                  {"--rdb-dir", keylane::test::TestDataDirectory().string(),
                   "--dbfilename", dump_.substr(dump_.rfind('/') + 1)});
      if (!defrag) args.emplace_back("--defrag-paused");
      if (checkpoint) args.emplace_back("--shutdown-checkpoint");
      std::vector<char*> argv;
      for (auto& arg : args) argv.push_back(arg.data());
      argv.push_back(nullptr);
      ::execv(argv.front(), argv.data());
      ::_exit(127);
    }
  }
  ~ChildServer() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
    }
    ::unlink(log_.c_str());
    ::unlink(dump_.c_str());
  }
  const std::string& DumpPath() const { return dump_; }
  std::uint16_t port() const { return port_; }
  std::string Log() const {
    std::ifstream stream(log_);
    return {std::istreambuf_iterator<char>(stream), {}};
  }
  bool WaitForLog(std::string_view message) const {
    const auto until = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < until) {
      if (Log().find(message) != std::string::npos) return true;
      std::this_thread::sleep_for(10ms);
    }
    return false;
  }
  std::string Command(std::initializer_list<std::string_view> args) {
    const auto until = std::chrono::steady_clock::now() + 20s;
    int fd = -1;
    while (std::chrono::steady_clock::now() < until) {
      fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      Check(fd >= 0, "client socket failed");
      sockaddr_in address{.sin_family = AF_INET,
                          .sin_port = htons(port_),
                          .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
      if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) == 0)
        break;
      ::close(fd);
      fd = -1;
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        throw std::runtime_error("server failed startup: " + Log());
      }
      std::this_thread::sleep_for(10ms);
    }
    Check(fd >= 0, "server did not become ready");
    timeval timeout{.tv_sec = 10};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    std::string command = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto arg : args) {
      command += "$" + std::to_string(arg.size()) + "\r\n";
      command.append(arg).append("\r\n");
    }
    std::string_view remaining(command);
    while (!remaining.empty()) {
      const auto written =
          ::send(fd, remaining.data(), remaining.size(), MSG_NOSIGNAL);
      if (written < 0 && errno == EINTR) continue;
      Check(written > 0, "client send failed");
      remaining.remove_prefix(written);
    }
    std::string line;
    while (!line.ends_with("\r\n")) {
      char byte;
      const auto read = ::recv(fd, &byte, 1, 0);
      if (read < 0 && errno == EINTR) continue;
      if (read != 1) {
        const auto error = errno;
        ::close(fd);
        throw std::runtime_error(
            "client response ended early for " + std::string(*args.begin()) +
            " errno=" + std::to_string(error) + "\n" + Log());
      }
      line.push_back(byte);
    }
    if (line.starts_with('$') && line != "$-1\r\n") {
      const std::size_t length = std::stoull(line.substr(1));
      std::string bulk(length + 2, '\0');
      std::size_t read_bytes = 0;
      while (read_bytes < bulk.size()) {
        const auto read =
            ::recv(fd, bulk.data() + read_bytes, bulk.size() - read_bytes, 0);
        if (read < 0 && errno == EINTR) continue;
        Check(read > 0, "client bulk response ended early");
        read_bytes += read;
      }
      Check(bulk.ends_with("\r\n"), "client bulk response is malformed");
      bulk.resize(length);
      ::close(fd);
      return bulk;
    }
    ::close(fd);
    return line.substr(0, line.size() - 2);
  }
  int Wait(bool stop = false) {
    if (stop) Check(::kill(pid_, SIGINT) == 0, "stopping server failed");
    const auto until = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < until) {
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        Check(WIFEXITED(status), "server did not exit normally");
        return WEXITSTATUS(status);
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("server did not exit: " + Log());
  }

 private:
  pid_t pid_ = -1;
  std::uint16_t port_ = 0;
  std::string log_;
  std::string dump_;
};

HashGroupSnapshot SingleGroup(std::size_t value_bytes = 128) {
  HashGroupSnapshot group{.incarnation_ = 17};
  group.value_.entries_.push_back(
      HashEntry{.digest_ = ComputeDigest("field"),
                .field_ = "field",
                .value_ = std::string(value_bytes, 'v')});
  return group;
}

TEST(GroupedRecoveryE2e, RestoresRootAndAuxiliaryWithoutExposingExtraKeys) {
  RecordImage image;
  auto group = SingleGroup();
  image.Group("hash", group, 1, 41);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1, 41);
  image.Commit(41);
  image.Finish();
  ChildServer server(image);
  EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(server.Command({"DBSIZE"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}), std::string(128, 'v'));
}

TEST(GroupedRecoveryE2e, IgnoresUncommittedAndFutureGroupVersions) {
  RecordImage image;
  auto group = SingleGroup();
  image.Group("hash", group, 1);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  group.value_.entries_.push_back(HashEntry{
      .digest_ = ComputeDigest("second"), .field_ = "second", .value_ = "x"});
  image.Group("hash", group, 1, 99);   // Same sequence, no decision.
  image.Group("hash", group, 2, 100);  // Committed but newer than winning root.
  image.Commit(100);
  image.Finish();
  ChildServer server(image);
  EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(server.Command({"DBSIZE"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}), std::string(128, 'v'));
}

TEST(GroupedRecoveryE2e,
     FailedCommandBatchDoesNotCommitWithItsOuterTransaction) {
  RecordImage image;
  auto old = SingleGroup();
  old.id_.bits_ = 1;
  old.id_.prefix_ =
      ComputeDigest("field", DigestSeed{}).value_ & (std::uint64_t{1} << 63);
  image.Group("hash", old, 1);
  auto sibling = old;
  sibling.id_.prefix_ ^= std::uint64_t{1} << 63;
  sibling.value_.entries_.clear();
  image.Group("hash", sibling, 1);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 2},
      1);
  auto failed = old;
  failed.value_.entries_[0].value_.assign(128, 'x');
  image.Group("hash", failed, 2, 77, false, 100);
  // A later successful command in the same EXEC changes another group and
  // commits a newer root. The failed command's lower seq is not sufficient
  // authority: its separate batch decision is deliberately absent.
  image.Group("hash", sibling, 3, 77, false, 101);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 2},
      3, 77);
  image.Commit(101);
  image.Commit(77);
  image.Finish();
  {
    ChildServer server(image, true);
    EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
    EXPECT_EQ(server.Command({"HGET", "hash", "field"}), std::string(128, 'v'));
    // Shutdown transaction cleaning must clear both decision dependencies on
    // promoted groups before freeing the generation containing those commits.
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}),
            std::string(128, 'v'));
}

TEST(GroupedRecoveryE2e, RejectsCommittedRootWithMissingGroup) {
  RecordImage image;
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  ChildServer server(image);
  EXPECT_NE(server.Wait(), 0) << server.Log();
  EXPECT_NE(server.Log().find("Hash"), std::string::npos);
}

TEST(GroupedRecoveryE2e, LegacySortedSetCanStillMutateAndRecover) {
  RecordImage image;
  OrderedGroupSnapshot page{.kind_ = OrderedCollectionKind::kSortedSet,
                            .incarnation_ = 17,
                            .id_ = 1,
                            .entries_ = {{"original", 3.5}}};
  image.OrderedGroup("legacy", page, 1);
  image.OrderedRoot(
      "legacy",
      OrderedCollectionRoot{.kind_ = OrderedCollectionKind::kSortedSet,
                            .incarnation_ = 17,
                            .item_count_ = 1,
                            .first_group_ = 1,
                            .last_group_ = 1,
                            .next_group_id_ = 2,
                            .group_count_ = 1,
                            .revision_ = 1},
      1);
  image.Finish();
  {
    ChildServer server(image);
    EXPECT_EQ(server.Command({"ZSCORE", "legacy", "original"}), "3.5");
    EXPECT_EQ(server.Command({"ZINCRBY", "legacy", "2", "original"}), "5.5");
    EXPECT_EQ(server.Command({"ZADD", "legacy", "-1", "new"}), ":1");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"ZCARD", "legacy"}), ":2");
  EXPECT_EQ(recovered.Command({"ZSCORE", "legacy", "original"}), "5.5");
  EXPECT_EQ(recovered.Command({"ZSCORE", "legacy", "new"}), "-1");
  EXPECT_EQ(recovered.Command({"ZREM", "legacy", "original"}), ":1");
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRecoveryE2e, IndexedSortedSetRequiresAndChecksMemberGraph) {
  for (const int mode : {0, 1, 2}) {
    SCOPED_TRACE(mode);
    RecordImage image;
    const std::string member(9 * 1024 * 1024, 'D');
    OrderedGroupSnapshot page{.kind_ = OrderedCollectionKind::kSortedSet,
                              .incarnation_ = 17,
                              .id_ = 1,
                              .entries_ = {{member, 3.5}}};
    image.OrderedGroup("indexed", page, 1, 0, true);
    OrderedCollectionRoot root{
        .kind_ = OrderedCollectionKind::kSortedSet,
        .incarnation_ = 17,
        .item_count_ = 1,
        .first_group_ = 1,
        .last_group_ = 1,
        .next_group_id_ = 2,
        .group_count_ = 1,
        .revision_ = 1,
        .member_index_ = GroupedHashRoot{.incarnation_ = 17,
                                         .field_count_ = 1,
                                         .group_count_ = 1,
                                         .revision_ = 1}};
    image.OrderedRoot("indexed", root, 1);
    std::vector<ExtentRef> member_extents;
    if (mode != 1) {
      HashGroupSnapshot prefix{.incarnation_ = 17};
      prefix.value_.entries_.push_back(
          {.digest_ = ComputeDigest(member),
           .field_ = member,
           .value_ = EncodeSortedSetMemberScore(3.5)});
      auto encoded = EncodeHashGroup(prefix);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      RecordHeader header{.value_type_ = ValueType::kSortedSet,
                          .external_ = true,
                          .auxiliary_group_ = true,
                          .group_incarnation_ = 17,
                          .logical_size_ = 1,
                          .mutation_sequence_ = 1};
      member_extents = image.GroupPayload("indexed", *encoded, header);
      // A newer, committed but root-unreachable member version must not win.
      prefix.value_.entries_[0].value_ = EncodeSortedSetMemberScore(99);
      encoded = EncodeHashGroup(prefix);
      ASSERT_TRUE(encoded.ok());
      header.mutation_sequence_ = 2;
      image.GroupPayload("indexed", *encoded, header);
    }
    image.Finish();
    if (mode == 2) {
      ASSERT_GE(member_extents.size(), 2);
      image.CorruptExtentBody(member_extents.back());
    }
    ChildServer server(image);
    if (mode != 0) {
      EXPECT_NE(server.Wait(), 0) << server.Log();
      continue;
    }
    EXPECT_EQ(server.Command({"ZCARD", "indexed"}), ":1");
    EXPECT_EQ(server.Command({"ZSCORE", "indexed", member}), "3.5");
    EXPECT_EQ(server.Command({"DBSIZE"}), ":1");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
    ChildServer recovered(image);
    EXPECT_EQ(recovered.Command({"ZSCORE", "indexed", member}), "3.5");
    EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(GroupedRecoveryE2e, RetainsSplitParentRetirementEvidence) {
  RecordImage image;
  auto group = SingleGroup();
  image.Group("hash", group, 1);
  auto retired = group;
  retired.retired_ = true;
  retired.value_.entries_.clear();
  image.Group("hash", retired, 2, 42);
  group.id_.bits_ = 1;
  group.id_.prefix_ =
      ComputeDigest("field", DigestSeed{}).value_ & (std::uint64_t{1} << 63);
  // Empty siblings remain part of the routing partition after a split.
  image.Group("hash", group, 2, 42);
  group.id_.prefix_ ^= std::uint64_t{1} << 63;
  group.value_.entries_.clear();
  image.Group("hash", group, 2, 42);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 2},
      2, 42);
  image.Commit(42);
  image.Finish();
  ChildServer server(image);
  EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(server.Command({"DBSIZE"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}), std::string(128, 'v'));
}

TEST(GroupedRecoveryE2e, ValidatesOversizedGroupBeyondItsEnvelopeExtent) {
  RecordImage image;
  const auto extents =
      image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  ASSERT_EQ(extents.size(), 2);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  image.CorruptExtentBody(extents.back());
  ChildServer server(image);
  EXPECT_NE(server.Wait(), 0) << server.Log();
  EXPECT_NE(server.Log().find("checksum mismatch"), std::string::npos)
      << server.Log();
}

TEST(GroupedRecoveryE2e, RestoresOversizedGroupAcrossWorkerTopologyChange) {
  RecordImage image;
  const auto extents =
      image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  ASSERT_EQ(extents.size(), 2);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  ChildServer server(image);
  EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(server.Command({"DBSIZE"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}),
            std::string(9 * 1024 * 1024, 'v'));
}

TEST(GroupedRecoveryE2e, ObsoleteGroupDoesNotReadAlreadyReclaimedValueExtents) {
  RecordImage image;
  const auto obsolete =
      image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  image.Group("hash", SingleGroup(), 2);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      2);
  image.Finish();
  // Keep the obsolete group's checksummed manifest in the same still-live
  // records block, but make its old value extents truly absent from the scan
  // bitmap. Recovery must select the live group before reading value bodies.
  image.FreeExtents(obsolete);
  ChildServer server(image);
  EXPECT_EQ(server.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}), std::string(128, 'v'));
}

TEST(GroupedRecoveryE2e, CheckpointDeclinesSafelyAndColdRecoveryRetainsGraph) {
  RecordImage image;
  image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  {
    ChildServer server(image, true);
    ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
    EXPECT_NE(server.Log().find("shutdown checkpoint was not published"),
              std::string::npos)
        << server.Log();
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}),
            std::string(9 * 1024 * 1024, 'v'));
}

TEST(GroupedRecoveryE2e, FlushDbDetachesAndRetiresTheCompleteGraph) {
  for (const auto mode : {"SYNC", "ASYNC"}) {
    SCOPED_TRACE(mode);
    RecordImage image;
    image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
    image.Root("hash",
               GroupedHashRoot{
                   .incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
               1);
    image.Finish();
    {
      ChildServer server(image);
      ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
      EXPECT_EQ(server.Command({"FLUSHDB", mode}), "+OK");
      EXPECT_EQ(server.Command({"DBSIZE"}), ":0");
      EXPECT_EQ(server.Wait(true), 0) << server.Log();
    }
    ChildServer recovered(image);
    EXPECT_EQ(recovered.Command({"DBSIZE"}), ":0");
    EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}), "$-1");
  }
}

TEST(GroupedRecoveryE2e, AuxiliaryAndRootGcCrashKeepTheDurableGraph) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug or the explicitly test-instrumented module";
#else
  for (const auto point :
       {"hash-group-defrag-copy-staged", "hash-defrag-copy-staged",
        "defrag-source-retired"}) {
    SCOPED_TRACE(point);
    RecordImage image;
    // Obsolete inline snapshots make this ordinary records block less than
    // half live. Its surviving group is extent-backed and its root shares
    // the same source, so both independent relocation paths must run.
    for (std::uint64_t sequence = 1; sequence < 4; ++sequence) {
      image.Group("hash", SingleGroup(), sequence);
    }
    auto retired = SingleGroup();
    retired.retired_ = true;
    retired.value_.entries_.clear();
    image.Group("hash", retired, 4);
    auto leaf = SingleGroup(9 * 1024 * 1024);
    leaf.id_.bits_ = 1;
    leaf.id_.prefix_ =
        ComputeDigest("field", DigestSeed{}).value_ & (std::uint64_t{1} << 63);
    image.Group("hash", leaf, 4, 0, true);
    leaf.id_.prefix_ ^= std::uint64_t{1} << 63;
    leaf.value_.entries_.clear();
    image.Group("hash", leaf, 4);
    image.Root("hash",
               GroupedHashRoot{
                   .incarnation_ = 17, .field_count_ = 1, .group_count_ = 2},
               4);
    image.Finish();
    {
      ChildServer server(image, false, point, true);
      EXPECT_EQ(server.Wait(), 86) << server.Log();
    }
    ChildServer recovered(image);
    EXPECT_EQ(recovered.Command({"HLEN", "hash"}), ":1");
    EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}),
              std::string(9 * 1024 * 1024, 'v'));
  }
#endif
}
TEST(GroupedRecoveryE2e, CommandBatchCrashNeverPublishesWithoutOuterDecision) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped command decision crash points";
#else
  for (const auto point :
       {"group-batch-before-root", "group-root-staged-before-batch-decision",
        "group-batch-durable-before-outer-decision"}) {
    SCOPED_TRACE(point);
    RecordImage image;
    image.Group("hash", SingleGroup(), 1);
    image.Root("hash",
               GroupedHashRoot{
                   .incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
               1);
    image.Finish();
    {
      ChildServer server(image, false, point);
      ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
      // EVAL supplies an outer atomic transaction; the command's independent
      // decision alone must never authorize its data after restart.
      EXPECT_THROW(server.Command({"EVAL",
                                   "return redis.call('HSET',KEYS[1],"
                                   "'field','changed','added','new')",
                                   "1", "hash"}),
                   std::runtime_error);
      EXPECT_EQ(server.Wait(), 86) << server.Log();
    }
    ChildServer recovered(image);
    EXPECT_EQ(recovered.Command({"HLEN", "hash"}), ":1");
    EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}),
              std::string(128, 'v'));
    EXPECT_EQ(recovered.Command({"HGET", "hash", "added"}), "$-1");
  }
#endif
}

TEST(GroupedRecoveryE2e, FailedPostRootBatchPoisonsOuterCommit) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped command decision failure injection";
#else
  RecordImage image;
  image.Group("hash", SingleGroup(), 1);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  {
    ChildServer server(image, false, {}, false, false, "hash");
    ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
    // Catch the command error deliberately: script success cannot rescue a
    // failed batch after its root has been staged. The guard participates in
    // the same outer transaction and must not become durable either.
    const auto response = server.Command(
        {"EVAL",
         "redis.call('SET',KEYS[2],'attempted'); "
         "redis.pcall('HSET',KEYS[1],'field','changed','added','new'); "
         "return 1",
         "2", "hash", "guard"});
    EXPECT_TRUE(response.starts_with('-')) << response;
    EXPECT_EQ(server.Command({"SET", "hash", "must-fail"}).front(), '-');
    // Failed roots remain installed until restart/rollback, but even metadata
    // must not expose their tentative type, expiry or existence. Exercise all
    // command dispatch forms, not just the storage loader's direct API.
    grouped_e2e::Client metadata(server.port());
    for (const auto* command : {"TYPE", "TTL", "PTTL", "EXPIRETIME",
                                "PEXPIRETIME", "EXISTS", "TOUCH"}) {
      auto reply = metadata.Command({command, "hash"});
      EXPECT_EQ(reply.kind_, '-') << command << ": " << reply.text_;
    }
    auto duplicate = metadata.Command({"EXISTS", "absent", "hash", "hash"});
    EXPECT_EQ(duplicate.kind_, '-') << duplicate.text_;
    ASSERT_EQ(metadata.Command({"MULTI"}).text_, "OK");
    for (const auto* command : {"TYPE", "TTL", "PTTL", "EXISTS"})
      ASSERT_EQ(metadata.Command({command, "hash"}).text_, "QUEUED");
    auto executed = metadata.Command({"EXEC"});
    ASSERT_EQ(executed.kind_, '*') << executed.text_;
    ASSERT_EQ(executed.items_.size(), 4);
    for (const auto& reply : executed.items_)
      EXPECT_EQ(reply.kind_, '-') << reply.text_;
    auto scripted = metadata.Command(
        {"EVAL",
         "local n=0; for _,cmd in ipairs({'TYPE','TTL','PTTL','EXISTS'}) do "
         "local r=redis.pcall(cmd,KEYS[1]); "
         "if type(r)=='table' and r.err then n=n+1 end end; return n",
         "1", "hash"});
    EXPECT_EQ(scripted.kind_, ':') << scripted.text_;
    EXPECT_EQ(scripted.text_, "4");
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"HLEN", "hash"}), ":1");
  EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}),
            std::string(128, 'v'));
  EXPECT_EQ(recovered.Command({"EXISTS", "guard"}), ":0");
#endif
}

TEST(GroupedRecoveryE2e, FailedMultiKeyOverwriteRestoresTheGroupedGraph) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires transaction write failure injection";
#else
  for (const bool script : {false, true}) {
    SCOPED_TRACE(script);
    RecordImage image;
    image.Group("hash{undo}", SingleGroup(9 * 1024 * 1024), 1, 0, true);
    image.Root("hash{undo}",
               GroupedHashRoot{
                   .incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
               1);
    image.Finish();
    {
      ChildServer server(image, true, {}, false, false, {}, "fail{undo}");
      ASSERT_EQ(server.Command({"HLEN", "hash{undo}"}), ":1");
      if (script) {
        // The script catches MSET's failure and commits a subsequent command.
        // Recovery must see the durable compensation, not the staged String
        // that briefly replaced the grouped root inside the failed MSET.
        EXPECT_EQ(server.Command(
                      {"EVAL",
                       "redis.pcall('MSET',KEYS[1],'replacement',KEYS[2],'x'); "
                       "redis.call('SET',KEYS[3],'committed'); return 1",
                       "3", "hash{undo}", "fail{undo}", "guard{undo}"}),
                  ":1");
      } else {
        EXPECT_TRUE(server
                        .Command({"MSET", "hash{undo}", "replacement",
                                  "fail{undo}", "x"})
                        .starts_with('-'));
      }
      EXPECT_EQ(server.Command({"HLEN", "hash{undo}"}), ":1");
      EXPECT_TRUE(server.Command({"HGET", "hash{undo}", "field"}) ==
                  std::string(9 * 1024 * 1024, 'v'));
      EXPECT_EQ(server.Wait(true), 0) << server.Log();
    }
    ChildServer recovered(image);
    EXPECT_TRUE(recovered.Command({"HGET", "hash{undo}", "field"}) ==
                std::string(9 * 1024 * 1024, 'v'));
    EXPECT_EQ(recovered.Command({"EXISTS", "fail{undo}"}), ":0");
    EXPECT_EQ(recovered.Command({"EXISTS", "guard{undo}"}),
              script ? ":1" : ":0");
  }
#endif
}

TEST(GroupedRecoveryE2e, SameCommandRootsUseRevisionBeforePhysicalLsn) {
  RecordImage image;
  auto group = SingleGroup();
  image.Group("hash", group, 10);
  group.value_.entries_[0].value_ = "new-revision";
  image.Group("hash", group, 20);
  image.Root("hash",
             GroupedHashRoot{.incarnation_ = 17,
                             .field_count_ = 1,
                             .group_count_ = 1,
                             .revision_ = 20},
             10);
  // This physical copy of the older root has a larger LSN. Both roots came
  // from one replay command envelope, but only R20 authorizes its new group.
  image.Root("hash",
             GroupedHashRoot{.incarnation_ = 17,
                             .field_count_ = 1,
                             .group_count_ = 1,
                             .revision_ = 10},
             10);
  image.Finish();
  {
    ChildServer server(image, true);
    EXPECT_EQ(server.Command({"HGET", "hash", "field"}), "new-revision");
    // No transaction tags survived in this image. A subsequent real mutation
    // still needs a globally fresh R; startup must seed its id watermark from
    // the selected root and auxiliary metadata rather than tags alone.
    EXPECT_EQ(server.Command({"HSET", "hash", "field", "after-restart"}), ":0");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"HGET", "hash", "field"}), "after-restart");
}

TEST(GroupedRecoveryE2e,
     PublicationLsnFollowsGcDuringForegroundAllocationWait) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires deterministic record publication pause";
#else
  RecordImage image;
  for (std::uint64_t sequence = 1; sequence <= 7; ++sequence) {
    image.Group("hash", SingleGroup(), sequence);
  }
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      7);
  image.Finish();
  ChildServer server(image, false, {}, false, false, {}, {}, "hash");
  ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
  auto foreground = std::async(std::launch::async, [&] {
    return server.Command({"SET", "hash", "compact-after-gc"});
  });
  ASSERT_TRUE(server.WaitForLog("record write publication pause armed"));
  ASSERT_EQ(server.Command({"CONFIG", "SET", "defrag-paused", "no"}), "+OK");
  EXPECT_EQ(foreground.get(), "+OK");
  EXPECT_EQ(server.Command({"GET", "hash"}), "compact-after-gc");
  const auto log = server.Log();
  constexpr std::string_view hash_prefix =
      "record publication test type=5 lsn=";
  constexpr std::string_view string_prefix =
      "record publication test type=1 lsn=";
  const auto hash_position = log.find(hash_prefix);
  const auto string_position = log.find(string_prefix);
  ASSERT_NE(hash_position, std::string::npos) << log;
  ASSERT_NE(string_position, std::string::npos) << log;
  const auto hash_lsn =
      std::stoull(log.substr(hash_position + hash_prefix.size()));
  const auto string_lsn =
      std::stoull(log.substr(string_position + string_prefix.size()));
  EXPECT_LT(hash_position, string_position);
  EXPECT_LT(hash_lsn, string_lsn);
  EXPECT_EQ(server.Wait(true), 0) << server.Log();
#endif
}

TEST(GroupedRecoveryE2e, TaggedRootDependencyPinsCrossRecoveredPhysicalOwners) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires deterministic root dependency pause";
#else
  for (const bool metadata_only : {true, false}) {
    SCOPED_TRACE(metadata_only);
    RecordImage image;
    const std::string key = "hash{bar}";
    ASSERT_EQ(RedisSlot(key) % 2, 1);
    image.Group(key, SingleGroup(), 1, 41);
    image.Root(key,
               GroupedHashRoot{
                   .incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
               1, 41);
    image.Commit(41);
    image.Finish();
    {
      ChildServer server(image, false, {}, false, false, {}, {}, {}, key);
      ASSERT_EQ(
          server.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
          "+OK");
      auto mutation = std::async(std::launch::async, [&] {
        return metadata_only ? server.Command({"PEXPIRE", key, "120000"})
                             : server.Command({"MSET", key, "replacement",
                                               "guard{bar}", "value"});
      });
      // The fixture's old-layout record block is redistributed to physical
      // owner 0, while this key is served by worker 1. The server hook checks
      // the exact root identity in the admitted pin guard, not merely whether
      // another changed group happened to pin the same source block.
      ASSERT_TRUE(server.WaitForLog(
          "group root dependency pause owner=0 key-owner=1 pinned=1"))
          << server.Log();
      ASSERT_EQ(
          server.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "1"}),
          "+OK");
      EXPECT_EQ(mutation.get(), metadata_only ? ":1" : "+OK");
      bool cleaner_ran = false;
      for (unsigned attempt = 0; attempt < 100; ++attempt) {
        const auto stats = server.Command({"INFO", "STATS"});
        constexpr std::string_view prefix = "tx_cleaner_rounds:";
        const auto position = stats.find(prefix);
        if (position != std::string::npos &&
            std::stoull(stats.substr(position + prefix.size())) != 0) {
          cleaner_ran = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      EXPECT_TRUE(cleaner_ran);
      EXPECT_EQ(server.Wait(true), 0) << server.Log();
    }
    ChildServer recovered(image);
    EXPECT_EQ(metadata_only ? recovered.Command({"HGET", key, "field"})
                            : recovered.Command({"GET", key}),
              metadata_only ? std::string(128, 'v') : "replacement");
    if (metadata_only)
      EXPECT_GT(std::stoll(recovered.Command({"PTTL", key}).substr(1)), 0);
  }
#endif
}

TEST(GroupedRecoveryE2e, FailedOverwriteReleasesTaggedRootOnItsPhysicalOwner) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires transaction failure and exact root pin hook";
#else
  RecordImage image;
  const std::string key = "hash{bar}";
  image.Group(key, SingleGroup(), 1, 41);
  image.Root(
      key,
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1, 41);
  image.Commit(41);
  image.Finish();
  {
    ChildServer server(image, false, {}, false, false, {}, "guard{bar}", {},
                       key);
    ASSERT_EQ(server.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}),
              "+OK");
    auto mutation = std::async(std::launch::async, [&] {
      return server.Command(
          {"MSET", key, "replacement", "guard{bar}", "value"});
    });
    ASSERT_TRUE(server.WaitForLog(
        "group root dependency pause owner=0 key-owner=1 pinned=1"))
        << server.Log();
    // Keep cleaner stopped until rollback has released the actual old-owner
    // pin. Otherwise promotion during the pause could turn the predecessor
    // into an untagged local copy and conceal a wrong-owner unpin bug.
    EXPECT_TRUE(mutation.get().starts_with("-ERR"));
    EXPECT_EQ(server.Command({"HGET", key, "field"}), std::string(128, 'v'));
    ASSERT_EQ(server.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "1"}),
              "+OK");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
    EXPECT_EQ(server.Log().find("transaction generations did not quiesce"),
              std::string::npos)
        << server.Log();
  }
  ChildServer recovered(image);
  EXPECT_EQ(recovered.Command({"HGET", key, "field"}), std::string(128, 'v'));
  EXPECT_EQ(recovered.Command({"EXISTS", "guard{bar}"}), ":0");
#endif
}

TEST(GroupedRecoveryE2e, OrderedRetirementSkipsFreedObsoletePageExtents) {
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    SCOPED_TRACE(static_cast<unsigned>(kind));
    RecordImage image;
    OrderedGroupSnapshot page{
        .kind_ = kind,
        .incarnation_ = 17,
        .id_ = 1,
        .entries_ = {
            {.value_ = std::string(9 * 1024 * 1024, 'v'),
             .score_ = kind == OrderedCollectionKind::kList ? 0.0 : 1.0}}};
    const auto obsolete = image.OrderedGroup("ordered", page, 1, 0, true);
    page.retired_ = true;
    page.entries_.clear();
    image.OrderedGroup("ordered", page, 2, 41, false, 91);
    page.id_ = 2;
    page.retired_ = false;
    page.entries_.push_back(
        {.value_ = "new",
         .score_ = kind == OrderedCollectionKind::kList ? 0.0 : 1.0});
    image.OrderedGroup("ordered", page, 2, 41, false, 91);
    // Its outer transaction commits, but this command's batch does not.
    // The later root must inherit R2, not the failed R3 page contents.
    page.entries_[0].value_ = "failed";
    image.OrderedGroup("ordered", page, 3, 42, false, 92);
    image.OrderedRoot("ordered",
                      OrderedCollectionRoot{.kind_ = kind,
                                            .incarnation_ = 17,
                                            .item_count_ = 1,
                                            .first_group_ = 2,
                                            .last_group_ = 2,
                                            .next_group_id_ = 3,
                                            .group_count_ = 1,
                                            .revision_ = 4},
                      4, 42);
    image.Commit(41);
    image.Commit(91);
    image.Commit(42);
    image.Finish();
    image.FreeExtents(obsolete);
    for (const bool checkpoint : {true, false}) {
      ChildServer server(image, checkpoint);
      if (kind == OrderedCollectionKind::kList) {
        EXPECT_EQ(server.Command({"LLEN", "ordered"}), ":1");
        EXPECT_EQ(server.Command({"LINDEX", "ordered", "0"}), "new");
      } else {
        EXPECT_EQ(server.Command({"ZCARD", "ordered"}), ":1");
        EXPECT_EQ(server.Command({"ZSCORE", "ordered", "new"}), "1");
      }
      if (checkpoint) EXPECT_EQ(server.Wait(true), 0) << server.Log();
    }
  }
}

TEST(GroupedRecoveryE2e, OrderedOversizedItemChecksEveryLiveExtent) {
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    for (const bool corrupt : {false, true}) {
      SCOPED_TRACE(static_cast<unsigned>(kind));
      SCOPED_TRACE(corrupt);
      RecordImage image;
      const std::string item(9 * 1024 * 1024, 'v');
      OrderedGroupSnapshot page{
          .kind_ = kind,
          .incarnation_ = 17,
          .id_ = 1,
          .entries_ = {
              {.value_ = item,
               .score_ = kind == OrderedCollectionKind::kList ? 0.0 : 1.0}}};
      const auto extents = image.OrderedGroup("ordered", page, 1, 0, true);
      ASSERT_EQ(extents.size(), 2);
      image.OrderedRoot("ordered",
                        OrderedCollectionRoot{.kind_ = kind,
                                              .incarnation_ = 17,
                                              .item_count_ = 1,
                                              .first_group_ = 1,
                                              .last_group_ = 1,
                                              .next_group_id_ = 2,
                                              .group_count_ = 1,
                                              .revision_ = 1},
                        1);
      image.Finish();
      if (corrupt) image.CorruptExtentBody(extents.back());
      ChildServer server(image);
      if (corrupt) {
        EXPECT_NE(server.Wait(), 0);
        EXPECT_NE(server.Log().find("checksum"), std::string::npos);
      } else if (kind == OrderedCollectionKind::kList) {
        EXPECT_TRUE(server.Command({"LINDEX", "ordered", "0"}) == item);
      } else {
        EXPECT_EQ(server.Command({"ZSCORE", "ordered", item}), "1");
      }
    }
  }
}

TEST(GroupedRecoveryE2e, OrderedGcCrashesPreservePagesMarkersAndRoot) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped GC crash points";
#else
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    for (const auto point :
         {"hash-group-defrag-copy-staged", "grouped-root-defrag-copy-staged",
          "defrag-source-retired"}) {
      SCOPED_TRACE(static_cast<unsigned>(kind));
      SCOPED_TRACE(point);
      RecordImage image;
      OrderedGroupSnapshot page{
          .kind_ = kind,
          .incarnation_ = 17,
          .id_ = 1,
          .entries_ = {
              {.value_ = std::string(128, 'v'),
               .score_ = kind == OrderedCollectionKind::kList ? 0.0 : 1.0}}};
      for (std::uint64_t revision = 1; revision <= 6; ++revision)
        image.OrderedGroup("ordered", page, revision);
      const std::string item(9 * 1024 * 1024, 'v');
      page.id_ = 2;
      page.entries_[0].value_ = item;
      // Put the live extent-backed page before its retirement marker, so the
      // first auxiliary crash hook exercises live-page physical relocation.
      image.OrderedGroup("ordered", page, 7, 0, true);
      page.id_ = 1;
      page.retired_ = true;
      page.entries_.clear();
      image.OrderedGroup("ordered", page, 7);
      image.OrderedRoot("ordered",
                        OrderedCollectionRoot{.kind_ = kind,
                                              .incarnation_ = 17,
                                              .item_count_ = 1,
                                              .first_group_ = 2,
                                              .last_group_ = 2,
                                              .next_group_id_ = 3,
                                              .group_count_ = 1,
                                              .revision_ = 7},
                        7);
      image.Finish();
      {
        ChildServer server(image, false, point, true);
        EXPECT_EQ(server.Wait(), 86) << server.Log();
      }
      ChildServer recovered(image);
      if (kind == OrderedCollectionKind::kList)
        EXPECT_TRUE(recovered.Command({"LINDEX", "ordered", "0"}) == item);
      else
        EXPECT_EQ(recovered.Command({"ZSCORE", "ordered", item}), "1");
    }
  }
#endif
}

TEST(GroupedRecoveryE2e, OrderedBgSavePinsExactPreCutPagesAndExpiration) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires deterministic snapshot scan pause";
#else
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    SCOPED_TRACE(static_cast<unsigned>(kind));
    RecordImage image;
    const std::string key = "ordered{bar}";
    ASSERT_EQ(RedisSlot(key) % 2, 1);
    const std::string item(9 * 1024 * 1024, 'v');
    OrderedGroupSnapshot page{
        .kind_ = kind,
        .incarnation_ = 17,
        .id_ = 1,
        .entries_ = {
            {.value_ = item,
             .score_ = kind == OrderedCollectionKind::kList ? 0.0 : 1.0}}};
    // Retain real transaction tags across the old single-worker image and
    // new two-worker runtime. Root/group dependency pins cannot assume that
    // the current key owner also owns their physical source block.
    image.OrderedGroup(key, page, 1, 41, true);
    image.OrderedRoot(key,
                      OrderedCollectionRoot{.kind_ = kind,
                                            .incarnation_ = 17,
                                            .item_count_ = 1,
                                            .first_group_ = 1,
                                            .last_group_ = 1,
                                            .next_group_id_ = 2,
                                            .group_count_ = 1,
                                            .revision_ = 1},
                      1, 41);
    image.Commit(41);
    image.Finish();
    ChildServer server(image, false, {}, false, true);
    const auto deadline =
        static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count()) +
        120000;
    const auto deadline_text = std::to_string(deadline);
    ASSERT_EQ(server.Command({"PEXPIREAT", key, deadline_text}), ":1");
    ASSERT_EQ(server.Command({"BGSAVE"}), "+Background saving started");
    ASSERT_EQ(server.Command({"PERSIST", key}), ":1");
    if (kind == OrderedCollectionKind::kList) {
      ASSERT_EQ(server.Command({"LSET", key, "0", "new"}), "+OK");
    } else {
      ASSERT_EQ(server.Command({"ZREM", key, item}), ":1");
      ASSERT_EQ(server.Command({"ZADD", key, "2", "new"}), ":1");
    }
    ASSERT_TRUE(server.WaitForLog("RDB backup completed:")) << server.Log();
    auto reader = keylane::rdb::FileReader::Open(server.DumpPath());
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto next = reader->Next();
    ASSERT_TRUE(next.ok()) << next.status();
    ASSERT_TRUE(next->has_value());
    EXPECT_EQ((**next).key_, key);
    EXPECT_EQ((**next).value_.expire_at_ms_, deadline);
    auto restored = DecodeOrderedCompactValue(kind, (**next).value_.encoded_,
                                              (**next).value_.logical_size_);
    ASSERT_TRUE(restored.ok()) << restored.status();
    ASSERT_EQ(restored->size(), 1);
    EXPECT_TRUE(restored->front().value_ == item);
    EXPECT_EQ(restored->front().score_,
              kind == OrderedCollectionKind::kList ? 0.0 : 1.0);
    EXPECT_EQ(server.Command({"FLUSHDB", "SYNC"}), "+OK");
    EXPECT_EQ(server.Wait(true), 0) << server.Log();
  }
#endif
}

TEST(GroupedRecoveryE2e, BgSaveRetainsPreCutGroupedValueAcrossReplacement) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires deterministic snapshot scan pause";
#else
  RecordImage image;
  image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  ChildServer server(image, false, {}, true, true);
  ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
  ASSERT_EQ(server.Command({"BGSAVE"}), "+Background saving started");
  // The first scan is held after the cut. This command must capture and pin
  // the exact old grouped graph before publishing its new grouped revision;
  // no current-by-key lookup may substitute the new value during export.
  ASSERT_EQ(server.Command({"HSET", "hash", "after-cut", "new"}), ":1");
  ASSERT_EQ(server.Command({"HDEL", "hash", "field"}), ":1");
  EXPECT_EQ(server.Command({"HGET", "hash", "field"}), "$-1");
  ASSERT_TRUE(server.WaitForLog("RDB backup completed:")) << server.Log();
  auto reader = keylane::rdb::FileReader::Open(server.DumpPath());
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto next = reader->Next();
  ASSERT_TRUE(next.ok()) << next.status();
  ASSERT_TRUE(next->has_value());
  EXPECT_EQ((**next).key_, "hash");
  EXPECT_EQ((**next).value_.value_type_, ValueType::kHash);
  auto hash = DecodeHashValue((**next).value_.encoded_);
  ASSERT_TRUE(hash.ok()) << hash.status();
  ASSERT_EQ(hash->entries_.size(), 1);
  EXPECT_EQ(hash->entries_[0].field_, "field");
  EXPECT_EQ(hash->entries_[0].value_, std::string(9 * 1024 * 1024, 'v'));
  next = reader->Next();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());
  EXPECT_EQ(server.Command({"FLUSHDB", "SYNC"}), "+OK");
  EXPECT_EQ(server.Wait(true), 0) << server.Log();
#endif
}

TEST(GroupedRecoveryE2e,
     FlushDbCancelsPausedGroupedSnapshotWithoutLeakingPins) {
#if defined(NDEBUG) && !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires deterministic snapshot scan pause";
#else
  RecordImage image;
  image.Group("hash", SingleGroup(9 * 1024 * 1024), 1, 0, true);
  image.Root(
      "hash",
      GroupedHashRoot{.incarnation_ = 17, .field_count_ = 1, .group_count_ = 1},
      1);
  image.Finish();
  ChildServer server(image, false, {}, true, true);
  ASSERT_EQ(server.Command({"HLEN", "hash"}), ":1");
  ASSERT_EQ(server.Command({"BGSAVE"}), "+Background saving started");
  // First capture the complete old auxiliary/extent pin list, then invalidate
  // the cut while its physical reader is still deliberately paused.
  ASSERT_EQ(server.Command({"HSET", "hash", "after-cut", "new"}), ":1");
  EXPECT_EQ(server.Command({"FLUSHDB", "ASYNC"}), "+OK");
  ASSERT_TRUE(server.WaitForLog("RDB backup failed:")) << server.Log();
  EXPECT_EQ(server.Command({"DBSIZE"}), ":0");
  // Draining the same bounded device proves cancellation did not strand pins
  // while detached group reclamation waited for its old snapshot readers.
  EXPECT_EQ(server.Command({"FLUSHDB", "SYNC"}), "+OK");
  EXPECT_EQ(server.Wait(true), 0) << server.Log();
#endif
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  server_binary = argv[1];
  return RUN_ALL_TESTS();
}
