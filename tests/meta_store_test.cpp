// Tests for the persistence glue under src/meta/: the WAL v1
// segmented NuraftLogStore and NuraftStateMgr.
//
// Component contracts are exercised by closing and reopening the same data
// directory (close + reopen stands in for process restart). A narrow fault
// injector covers pwrite/ftruncate/fdatasync/unlink policy without depending
// on a particular filesystem. The state machine tests — component and
// core-driven raft_server integration — live in meta_state_machine_test.cpp
// on MetaStateMachine with real commands. The snapshot-stream pruning
// regression is covered by MetaStateMachineTest.MidStreamPrune* there.

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/meta/nuraft_log_store.h"
#include "keylane/meta/nuraft_state_mgr.h"
#include "libnuraft/nuraft.hxx"

namespace {

using keylane::meta::MetaMemberIdentity;
using keylane::meta::NuraftLogFaultInjector;
using keylane::meta::NuraftLogFaultPoint;
using keylane::meta::NuraftLogStore;
using keylane::meta::NuraftMemberConfig;
using keylane::meta::NuraftStartupMode;
using keylane::meta::NuraftStateMgr;
using keylane::meta::NuraftStateMgrOpenOptions;

class OneShotLogFault final : public NuraftLogFaultInjector {
 public:
  explicit OneShotLogFault(NuraftLogFaultPoint point) : point_(point) {}

  absl::Status Before(NuraftLogFaultPoint point) override {
    if (!fired_ && point == point_) {
      fired_ = true;
      return absl::InternalError("injected log-store fault");
    }
    return absl::OkStatus();
  }

 private:
  NuraftLogFaultPoint point_;
  bool fired_ = false;
};

std::filesystem::path MakeTestDir(const char* suite, const char* name) {
  const ::testing::TestInfo* info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::filesystem::path dir = std::filesystem::temp_directory_path() /
                              ("keylane_meta_test_" + std::string(suite) + "_" +
                               name + "_" + info->test_suite_name() + "_" +
                               info->name() + "_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  return dir;
}

void RemoveTestDir(const std::filesystem::path& dir) {
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

nuraft::ptr<nuraft::log_entry> MakeEntry(
    uint64_t term, const std::string& payload,
    nuraft::log_val_type type = nuraft::log_val_type::app_log) {
  nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(payload.size());
  if (!payload.empty()) {
    std::memcpy(buf->data_begin(), payload.data(), payload.size());
  }
  return nuraft::cs_new<nuraft::log_entry>(term, buf, type);
}

std::string EntryPayload(const nuraft::ptr<nuraft::log_entry>& entry) {
  nuraft::buffer& buf = entry->get_buf();
  return std::string(reinterpret_cast<const char*>(buf.data_begin()),
                     buf.size());
}

std::string ReadFileBytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFileBytes(const std::filesystem::path& path,
                    const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(bytes.data(), bytes.size());
  ASSERT_TRUE(out.good());
}

// Keep the header checksum valid so recovery must distinguish an unsupported
// format from an interrupted header write.
void SetSegmentVersion(std::string& bytes, uint32_t version) {
  ASSERT_GE(bytes.size(), 20u);
  for (unsigned ii = 0; ii < 4; ++ii) {
    bytes[4 + ii] = static_cast<char>(version >> (8 * ii));
  }
  uint32_t checksum = 2166136261u;
  for (unsigned ii = 0; ii < 16; ++ii) {
    checksum ^= static_cast<unsigned char>(bytes[ii]);
    checksum *= 16777619u;
  }
  for (unsigned ii = 0; ii < 4; ++ii) {
    bytes[16 + ii] = static_cast<char>(checksum >> (8 * ii));
  }
}

// ---------------------------------------------------------------------------
// NuraftLogStore
// ---------------------------------------------------------------------------

class LogStoreTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("store", "log"); }
  void TearDown() override { RemoveTestDir(dir_); }

  absl::StatusOr<std::unique_ptr<NuraftLogStore>> Open() {
    return NuraftLogStore::Open(dir_);
  }

  // WAL v1: tiny segment cap forces rolling so tests exercise multi-segment
  // behavior without writing megabytes.
  absl::StatusOr<std::unique_ptr<NuraftLogStore>> OpenWithCap(
      uint64_t max_segment_bytes) {
    return NuraftLogStore::Open(dir_, max_segment_bytes);
  }

  // Names of the log-*.seg files currently on disk, sorted.
  std::vector<std::string> SegmentFiles() {
    std::vector<std::string> names;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      const std::string name = entry.path().filename().string();
      if (name.rfind("log-", 0) == 0 && name.size() >= 4 &&
          name.compare(name.size() - 4, 4, ".seg") == 0) {
        names.push_back(name);
      }
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  uint64_t SegmentBytesOnDisk() {
    uint64_t total = 0;
    for (const std::string& name : SegmentFiles()) {
      total += std::filesystem::file_size(dir_ / name);
    }
    return total;
  }

  std::map<std::string, std::string> DirectoryBytes() {
    std::map<std::string, std::string> result;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
      result.emplace(entry.path().filename().string(),
                     ReadFileBytes(entry.path()));
    }
    return result;
  }

  std::filesystem::path dir_;
};

TEST_F(LogStoreTest, FreshStoreIsEmpty) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);

  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(store->next_slot(), 1u);
  EXPECT_EQ(store->last_entry()->get_term(), 0u);
  EXPECT_EQ(store->term_at(1), 0u);
  EXPECT_EQ(store->entry_at(1), nullptr);
}

TEST_F(LogStoreTest, SegmentHeaderUsesFormatV1) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    auto entry = MakeEntry(1, "payload");
    (*opened)->append(entry);
    ASSERT_TRUE((*opened)->flush());
  }
  const std::string bytes = ReadFileBytes(dir_ / "log-1.seg");
  ASSERT_GE(bytes.size(), 20u);
  EXPECT_EQ(bytes.substr(4, 4), std::string("\x01\x00\x00\x00", 4));
}

TEST_F(LogStoreTest, UnsupportedSegmentVersionLeavesAllFilesUntouched) {
  for (const uint32_t version : {2u, 3u}) {
    for (const bool incompatible_first : {true, false}) {
      SCOPED_TRACE(version);
      SCOPED_TRACE(incompatible_first);
      RemoveTestDir(dir_);
      {
        auto opened = OpenWithCap(64);
        ASSERT_TRUE(opened.ok()) << opened.status();
        for (unsigned ii = 0; ii < 5; ++ii) {
          auto entry = MakeEntry(1, "x");
          (*opened)->append(entry);
        }
        ASSERT_TRUE((*opened)->flush());
      }
      const auto segments = SegmentFiles();
      ASSERT_GT(segments.size(), 1u);
      const auto incompatible =
          dir_ / (incompatible_first ? segments.front() : segments.back());
      std::string bytes = ReadFileBytes(incompatible);
      SetSegmentVersion(bytes, version);
      WriteFileBytes(incompatible, bytes);
      if (!incompatible_first) {
        // A later incompatible header must be found before an earlier torn
        // record could trigger destructive prefix recovery.
        const auto first = dir_ / segments.front();
        WriteFileBytes(first, ReadFileBytes(first) + "torn");
      }
      const auto before = DirectoryBytes();
      auto opened = Open();
      ASSERT_FALSE(opened.ok());
      EXPECT_EQ(opened.status().code(), absl::StatusCode::kFailedPrecondition);
      EXPECT_NE(opened.status().message().find("format version"),
                std::string_view::npos);
      EXPECT_EQ(DirectoryBytes(), before);
    }
  }
}

TEST_F(LogStoreTest, UnsupportedCompactIntentLeavesAllFilesUntouched) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    auto entry = MakeEntry(1, "payload");
    (*opened)->append(entry);
    ASSERT_TRUE((*opened)->flush());
  }
  for (const uint32_t version : {2u, 3u}) {
    SCOPED_TRACE(version);
    std::string intent = ReadFileBytes(dir_ / "log-1.seg");
    SetSegmentVersion(intent, version);
    WriteFileBytes(dir_ / "compact-1.ready", intent);
    const auto before = DirectoryBytes();
    auto opened = Open();
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), absl::StatusCode::kFailedPrecondition);
    EXPECT_EQ(DirectoryBytes(), before);
  }
}

TEST_F(LogStoreTest, AppendSurvivesReopen) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);

    nuraft::ptr<nuraft::log_entry> e1 = MakeEntry(1, "alpha");
    nuraft::ptr<nuraft::log_entry> e2 = MakeEntry(1, "beta");
    nuraft::ptr<nuraft::log_entry> e3 = MakeEntry(2, "gamma");
    EXPECT_EQ(store->append(e1), 1u);
    EXPECT_EQ(store->append(e2), 2u);
    EXPECT_EQ(store->append(e3), 3u);
    ASSERT_TRUE(store->flush());

    EXPECT_EQ(store->next_slot(), 4u);
    EXPECT_EQ(store->last_entry()->get_term(), 2u);
    EXPECT_EQ(store->term_at(1), 1u);
    EXPECT_EQ(store->term_at(3), 2u);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);

  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(store->next_slot(), 4u);
  EXPECT_EQ(store->term_at(1), 1u);
  EXPECT_EQ(store->term_at(2), 1u);
  EXPECT_EQ(store->term_at(3), 2u);
  ASSERT_NE(store->entry_at(2), nullptr);
  EXPECT_EQ(EntryPayload(store->entry_at(2)), "beta");
  EXPECT_EQ(EntryPayload(store->last_entry()), "gamma");
}

TEST_F(LogStoreTest, WriteAtTruncatesTailDurably) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 5; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(1, "v" + std::to_string(ii));
      store->append(entry);
    }

    // A conflicting leader overwrites index 3: entries 3..5 must vanish.
    nuraft::ptr<nuraft::log_entry> replacement = MakeEntry(9, "new");
    store->write_at(3, replacement);
    ASSERT_TRUE(store->flush());

    EXPECT_EQ(store->next_slot(), 4u);
    EXPECT_EQ(store->term_at(3), 9u);
    EXPECT_EQ(store->term_at(4), 0u);
    EXPECT_EQ(store->entry_at(4), nullptr);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->next_slot(), 4u);
  EXPECT_EQ(store->term_at(3), 9u);
  EXPECT_EQ(EntryPayload(store->entry_at(3)), "new");

  // Appending after the overwrite keeps the sequence contiguous.
  nuraft::ptr<nuraft::log_entry> next = MakeEntry(9, "after");
  EXPECT_EQ(store->append(next), 4u);
}

TEST_F(LogStoreTest, WriteAtEmptyStoreAppends) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);

  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(4, "first");
  store->write_at(1, entry);
  EXPECT_EQ(store->next_slot(), 2u);
  EXPECT_EQ(store->term_at(1), 4u);
}

TEST_F(LogStoreTest, LogEntriesRange) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  for (uint64_t ii = 1; ii <= 4; ++ii) {
    nuraft::ptr<nuraft::log_entry> entry =
        MakeEntry(ii, "p" + std::to_string(ii));
    store->append(entry);
  }

  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> entries =
      store->log_entries(2, 5);
  ASSERT_NE(entries, nullptr);
  ASSERT_EQ(entries->size(), 3u);
  EXPECT_EQ(EntryPayload(entries->at(0)), "p2");
  EXPECT_EQ(entries->at(2)->get_term(), 4u);

  // The contract requires nullptr when any index in the range is missing.
  EXPECT_EQ(store->log_entries(3, 6), nullptr);
  EXPECT_EQ(store->log_entries(0, 2), nullptr);
}

TEST_F(LogStoreTest, CompactAdvancesStartIndexDurably) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 10; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(ii, "c" + std::to_string(ii));
      store->append(entry);
    }
    ASSERT_TRUE(store->compact(6));

    EXPECT_EQ(store->start_index(), 7u);
    EXPECT_EQ(store->next_slot(), 11u);
    EXPECT_EQ(store->term_at(6), 0u);
    EXPECT_EQ(store->entry_at(6), nullptr);
    EXPECT_EQ(store->term_at(7), 7u);
    EXPECT_EQ(store->term_at(10), 10u);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->start_index(), 7u);
  EXPECT_EQ(store->next_slot(), 11u);
  EXPECT_EQ(store->term_at(7), 7u);

  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(11, "post");
  EXPECT_EQ(store->append(entry), 11u);
}

TEST_F(LogStoreTest, CompactBeyondEndEmptiesStore) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 3; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "x");
      store->append(entry);
    }
    ASSERT_TRUE(store->compact(100));
    EXPECT_EQ(store->start_index(), 101u);
    EXPECT_EQ(store->next_slot(), 101u);
    EXPECT_EQ(store->last_entry()->get_term(), 0u);

    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(2, "y");
    EXPECT_EQ(store->append(entry), 101u);
    ASSERT_TRUE(store->flush());
  }

  // The header-only floor segment pinned the compacted start index on disk.
  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->start_index(), 101u);
  EXPECT_EQ(store->next_slot(), 102u);
  EXPECT_EQ(EntryPayload(store->entry_at(101)), "y");
}

TEST_F(LogStoreTest, PackApplyPackRoundTrip) {
  auto source_opened = Open();
  ASSERT_TRUE(source_opened.ok()) << source_opened.status();
  std::unique_ptr<NuraftLogStore> source = std::move(*source_opened);
  for (uint64_t ii = 1; ii <= 5; ++ii) {
    nuraft::ptr<nuraft::log_entry> entry =
        MakeEntry(ii, "k" + std::to_string(ii));
    source->append(entry);
  }

  nuraft::ptr<nuraft::buffer> pack = source->pack(2, 3);
  ASSERT_NE(pack, nullptr);

  std::filesystem::path other_dir = MakeTestDir("store", "log_pack_dst");
  auto target_opened = NuraftLogStore::Open(other_dir);
  ASSERT_TRUE(target_opened.ok()) << target_opened.status();
  std::unique_ptr<NuraftLogStore> target = std::move(*target_opened);

  // A joiner already holding conflicting entries 2..4 loses them.
  for (uint64_t ii = 1; ii <= 4; ++ii) {
    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(99, "stale");
    target->append(entry);
  }
  target->apply_pack(2, *pack);
  ASSERT_TRUE(target->flush());

  EXPECT_EQ(target->next_slot(), 5u);
  EXPECT_EQ(EntryPayload(target->entry_at(1)), "stale");
  EXPECT_EQ(EntryPayload(target->entry_at(2)), "k2");
  EXPECT_EQ(target->term_at(4), 4u);
  EXPECT_EQ(target->entry_at(5), nullptr);

  target.reset();
  auto reopened = NuraftLogStore::Open(other_dir);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  target = std::move(*reopened);
  EXPECT_EQ(target->next_slot(), 5u);
  EXPECT_EQ(EntryPayload(target->entry_at(3)), "k3");
  RemoveTestDir(other_dir);
}

TEST_F(LogStoreTest, TornTailIsTruncatedOnOpen) {
  uint64_t size_before_garbage = 0;
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 3; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(1, "d" + std::to_string(ii));
      store->append(entry);
    }
    ASSERT_TRUE(store->flush());
    store.reset();
    size_before_garbage = std::filesystem::file_size(dir_ / "log-1.seg");
  }

  // Simulate a torn pwrite: a partial record landed after the last good one.
  {
    std::ofstream out(dir_ / "log-1.seg", std::ios::binary | std::ios::app);
    const char garbage[] = {'\x4c', '\x52', '\x41', '\x31', '\x07', '\x00'};
    out.write(garbage, sizeof(garbage));
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->next_slot(), 4u);
  EXPECT_EQ(store->term_at(3), 1u);
  EXPECT_EQ(std::filesystem::file_size(dir_ / "log-1.seg"),
            size_before_garbage);

  // The store stays writable after truncating the torn tail.
  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "d4");
  EXPECT_EQ(store->append(entry), 4u);
}

TEST_F(LogStoreTest, RejectsPrototypeSingleFileLayoutDirectory) {
  // The segmented WAL is incompatible with the prototype single-file layout:
  // refuse that directory instead of silently starting an empty segment set.
  std::filesystem::create_directories(dir_);
  {
    std::ofstream out(dir_ / "raft_log.dat", std::ios::binary);
    out << "LRAH";
  }
  auto opened = Open();
  ASSERT_FALSE(opened.ok());
  EXPECT_NE(opened.status().message().find("raft_log.dat"), std::string::npos)
      << opened.status();
}

TEST_F(LogStoreTest, SegmentRollsAtSizeCap) {
  {
    // One record is 42 + payload bytes; the cap lets a handful of records
    // share a segment before rolling.
    auto opened = OpenWithCap(160);
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 12; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(ii, "r" + std::to_string(ii));
      EXPECT_EQ(store->append(entry), ii);
    }
    ASSERT_TRUE(store->flush());
    EXPECT_EQ(store->UncompactedBytes(), SegmentBytesOnDisk());
  }
  const std::vector<std::string> segments = SegmentFiles();
  ASSERT_GT(segments.size(), 1u) << "expected the tiny cap to force rolling";

  auto reopened = OpenWithCap(160);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(store->next_slot(), 13u);
  for (uint64_t ii = 1; ii <= 12; ++ii) {
    EXPECT_EQ(EntryPayload(store->entry_at(ii)), "r" + std::to_string(ii));
    EXPECT_EQ(store->term_at(ii), ii);
  }
  EXPECT_EQ(store->UncompactedBytes(), SegmentBytesOnDisk());
}

TEST_F(LogStoreTest, WriteAtTruncatesAcrossSegments) {
  {
    auto opened = OpenWithCap(160);
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 10; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(1, "w" + std::to_string(ii));
      store->append(entry);
    }
    ASSERT_TRUE(store->flush());
    ASSERT_GT(SegmentFiles().size(), 1u);

    // A conflicting leader overwrites index 4: every segment past the one
    // holding index 4 must disappear, and the tail must not come back.
    nuraft::ptr<nuraft::log_entry> replacement = MakeEntry(9, "new");
    store->write_at(4, replacement);
    ASSERT_TRUE(store->flush());
    EXPECT_EQ(store->next_slot(), 5u);
  }

  EXPECT_EQ(SegmentFiles().size(), 1u);
  auto reopened = OpenWithCap(160);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->next_slot(), 5u);
  EXPECT_EQ(store->term_at(4), 9u);
  EXPECT_EQ(EntryPayload(store->entry_at(4)), "new");
  EXPECT_EQ(store->entry_at(5), nullptr);

  // Appending after the cross-segment overwrite keeps rolling cleanly.
  for (uint64_t ii = 5; ii <= 10; ++ii) {
    nuraft::ptr<nuraft::log_entry> entry =
        MakeEntry(9, "w" + std::to_string(ii));
    EXPECT_EQ(store->append(entry), ii);
  }
  ASSERT_TRUE(store->flush());
  EXPECT_EQ(store->next_slot(), 11u);
}

TEST_F(LogStoreTest, CompactDropsWholeSegmentsAndRewritesBoundary) {
  {
    auto opened = OpenWithCap(160);
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    for (uint64_t ii = 1; ii <= 12; ++ii) {
      nuraft::ptr<nuraft::log_entry> entry =
          MakeEntry(ii, "c" + std::to_string(ii));
      store->append(entry);
    }
    ASSERT_TRUE(store->flush());
    ASSERT_GT(SegmentFiles().size(), 1u);

    // Compact at 6: whole segments below the boundary are unlinked, the
    // segment straddling it is rewritten so a segment starts exactly at the
    // snapshot compact boundary.
    ASSERT_TRUE(store->compact(6));
    EXPECT_EQ(store->start_index(), 7u);
    EXPECT_EQ(store->next_slot(), 13u);
    EXPECT_EQ(store->UncompactedBytes(), SegmentBytesOnDisk());
  }

  // The boundary segment was renamed to start exactly at 7; nothing below
  // the boundary remains on disk.
  const std::vector<std::string> segments = SegmentFiles();
  ASSERT_FALSE(segments.empty());
  EXPECT_EQ(segments.front(), "log-7.seg");

  auto reopened = OpenWithCap(160);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*reopened);
  EXPECT_EQ(store->start_index(), 7u);
  EXPECT_EQ(store->next_slot(), 13u);
  EXPECT_EQ(store->term_at(6), 0u);
  for (uint64_t ii = 7; ii <= 12; ++ii) {
    EXPECT_EQ(EntryPayload(store->entry_at(ii)), "c" + std::to_string(ii));
  }

  // A second compaction past every segment leaves one header-only floor
  // segment pinning the new start index.
  ASSERT_TRUE(store->compact(12));
  EXPECT_EQ(store->start_index(), 13u);
  EXPECT_EQ(store->next_slot(), 13u);
  EXPECT_EQ(SegmentFiles(), std::vector<std::string>{"log-13.seg"});
  EXPECT_EQ(store->UncompactedBytes(), SegmentBytesOnDisk());

  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(13, "post");
  EXPECT_EQ(store->append(entry), 13u);
  ASSERT_TRUE(store->flush());
}

TEST_F(LogStoreTest, AppendPwriteFailureFailsStop) {
  auto fault = std::make_shared<OneShotLogFault>(NuraftLogFaultPoint::kPwrite);
  auto opened = NuraftLogStore::Open(
      dir_, NuraftLogStore::kDefaultMaxSegmentBytes, fault);
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "lost");
  EXPECT_DEATH(store->append(entry), "");
}

TEST_F(LogStoreTest, WriteAtFtruncateFailureFailsStop) {
  auto fault =
      std::make_shared<OneShotLogFault>(NuraftLogFaultPoint::kFtruncate);
  auto opened = NuraftLogStore::Open(
      dir_, NuraftLogStore::kDefaultMaxSegmentBytes, fault);
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  for (int i = 0; i < 3; ++i) {
    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "tail");
    store->append(entry);
  }
  nuraft::ptr<nuraft::log_entry> replacement = MakeEntry(2, "fork");
  EXPECT_DEATH(store->write_at(2, replacement), "");
}

TEST_F(LogStoreTest, FlushFailureUsesNuRaftErrorChannel) {
  auto fault =
      std::make_shared<OneShotLogFault>(NuraftLogFaultPoint::kFdatasync);
  auto opened = NuraftLogStore::Open(
      dir_, NuraftLogStore::kDefaultMaxSegmentBytes, fault);
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "pending");
  store->append(entry);
  EXPECT_FALSE(store->flush());
  EXPECT_TRUE(store->flush());
}

TEST_F(LogStoreTest, CompactUnlinkFailureReturnsFalse) {
  auto fault = std::make_shared<OneShotLogFault>(NuraftLogFaultPoint::kUnlink);
  auto opened = NuraftLogStore::Open(
      dir_, NuraftLogStore::kDefaultMaxSegmentBytes, fault);
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "committed");
  store->append(entry);
  ASSERT_TRUE(store->flush());
  EXPECT_FALSE(store->compact(1));
  // A reported failure is atomic to both the live object and recovery: the
  // old floor and record remain authoritative and a retry can succeed.
  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(store->next_slot(), 2u);
  EXPECT_EQ(EntryPayload(store->entry_at(1)), "committed");
  store.reset();
  auto reopened = OpenWithCap(NuraftLogStore::kDefaultMaxSegmentBytes);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  store = std::move(*reopened);
  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(EntryPayload(store->entry_at(1)), "committed");
  EXPECT_TRUE(store->compact(1));
}

TEST_F(LogStoreTest, CompactRenameFailureLeavesStateUnchanged) {
  {
    auto opened = OpenWithCap(NuraftLogStore::kDefaultMaxSegmentBytes);
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftLogStore> store = std::move(*opened);
    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "committed");
    store->append(entry);
    ASSERT_TRUE(store->flush());
  }
  auto fault = std::make_shared<OneShotLogFault>(NuraftLogFaultPoint::kRename);
  auto opened = NuraftLogStore::Open(
      dir_, NuraftLogStore::kDefaultMaxSegmentBytes, fault);
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftLogStore> store = std::move(*opened);
  EXPECT_FALSE(store->compact(1));
  EXPECT_EQ(store->start_index(), 1u);
  EXPECT_EQ(store->next_slot(), 2u);
  EXPECT_EQ(EntryPayload(store->entry_at(1)), "committed");
}

// ---------------------------------------------------------------------------
// NuraftStateMgr
// ---------------------------------------------------------------------------

class StateMgrTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("store", "mgr"); }
  void TearDown() override { RemoveTestDir(dir_); }

  static NuraftMemberConfig Member(std::uint32_t id) {
    return NuraftMemberConfig{
        .server_id_ = static_cast<std::int32_t>(id),
        .raft_endpoint_ = "127.0.0.1:" + std::to_string(9700 + id),
        .principal_ = "keylane://meta/" + std::to_string(id),
        .data_control_endpoint_ = "127.0.0.1:" + std::to_string(9800 + id),
        .ctl_endpoint_ = "127.0.0.1:" + std::to_string(9900 + id),
    };
  }

  absl::StatusOr<std::unique_ptr<NuraftStateMgr>> OpenInitial(
      std::vector<NuraftMemberConfig> members = {Member(7)}) {
    return NuraftStateMgr::Open({.data_dir_ = dir_.string(),
                                 .local_member_ = Member(7),
                                 .initial_cluster_ = std::move(members)});
  }

  absl::StatusOr<std::unique_ptr<NuraftStateMgr>> OpenRestart() {
    return NuraftStateMgr::Open(
        {.data_dir_ = dir_.string(), .local_member_ = Member(7)});
  }

  absl::StatusOr<std::unique_ptr<NuraftStateMgr>> Open() {
    return std::filesystem::exists(dir_ / "cluster_config.dat") ? OpenRestart()
                                                                : OpenInitial();
  }

  static void PersistInitializedRaftEvidence(NuraftStateMgr& manager) {
    nuraft::srv_state state;
    state.set_term(1);
    state.set_voted_for(manager.server_id());
    manager.save_state(state);
    auto log = manager.load_log_store();
    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(1, "initialized");
    ASSERT_EQ(log->append(entry), 1U);
    ASSERT_TRUE(log->flush());
  }

  std::filesystem::path dir_;
};

TEST_F(StateMgrTest, FreshDirYieldsInitialConfigAndNoState) {
  auto opened = OpenInitial();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);

  EXPECT_EQ(mgr->server_id(), 7);
  EXPECT_EQ(mgr->startup_mode(), NuraftStartupMode::kInitialCluster);
  EXPECT_TRUE(mgr->initial_bindings_pending());
  EXPECT_EQ(mgr->read_state(), nullptr);
  EXPECT_TRUE(std::filesystem::exists(dir_ / "cluster_config.dat"));
  EXPECT_TRUE(std::filesystem::exists(dir_ / "initial_bindings.dat"));

  nuraft::ptr<nuraft::cluster_config> config = mgr->load_config();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(config->get_servers().size(), 1u);
  const nuraft::ptr<nuraft::srv_config>& self = config->get_servers().front();
  EXPECT_EQ(self->get_id(), 7);
  EXPECT_EQ(self->get_endpoint(), "127.0.0.1:9707");
}

TEST_F(StateMgrTest, InitialAdvertisedControlRoutesMayDifferFromLocalBinds) {
  NuraftMemberConfig local = Member(7);
  NuraftMemberConfig advertised = local;
  advertised.data_control_endpoint_ = "127.0.0.1:19707";
  advertised.ctl_endpoint_ = "127.0.0.1:29707";

  auto opened = NuraftStateMgr::Open({
      .data_dir_ = dir_.string(),
      .local_member_ = std::move(local),
      .initial_cluster_ = std::vector<NuraftMemberConfig>{advertised},
  });

  ASSERT_TRUE(opened.ok()) << opened.status();
  const auto config = (*opened)->load_config();
  ASSERT_NE(config, nullptr);
  auto identity =
      MetaMemberIdentity::DecodeAux(config->get_servers().front()->get_aux());
  ASSERT_TRUE(identity.ok()) << identity.status();
  EXPECT_EQ(identity->data_control_endpoint_, "127.0.0.1:19707");
  EXPECT_EQ(identity->ctl_endpoint_, "127.0.0.1:29707");
}

TEST_F(StateMgrTest, InitialConfigTreatsOneThreeAndFiveMembersIdentically) {
  for (const std::size_t count : {1U, 3U, 5U}) {
    RemoveTestDir(dir_);
    std::vector<NuraftMemberConfig> members;
    for (std::size_t id = 1; id <= count; ++id) members.push_back(Member(id));
    if (count == 1) members.front() = Member(7);
    if (count > 1) members[0] = Member(7);

    auto opened = OpenInitial(std::move(members));

    ASSERT_TRUE(opened.ok()) << opened.status();
    EXPECT_EQ((*opened)->startup_mode(), NuraftStartupMode::kInitialCluster);
    EXPECT_EQ((*opened)->load_config()->get_servers().size(), count);
    EXPECT_EQ((*opened)->load_config()->get_log_idx(), 0U);
  }
}

TEST_F(StateMgrTest, PristineWithoutManifestIsDurableWaitingJoiner) {
  auto opened = OpenRestart();

  ASSERT_TRUE(opened.ok()) << opened.status();
  EXPECT_EQ((*opened)->startup_mode(), NuraftStartupMode::kWaitingJoiner);
  EXPECT_TRUE(std::filesystem::exists(dir_ / "cluster_config.dat"));
  EXPECT_TRUE(std::filesystem::exists(dir_ / "waiting_joiner.dat"));
  opened->reset();

  auto restarted = OpenRestart();
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_EQ((*restarted)->startup_mode(), NuraftStartupMode::kWaitingJoiner);

  restarted->reset();
  std::filesystem::remove(dir_ / "cluster_config.dat");
  auto recovered_prefix = OpenRestart();
  ASSERT_TRUE(recovered_prefix.ok()) << recovered_prefix.status();
  EXPECT_EQ((*recovered_prefix)->startup_mode(),
            NuraftStartupMode::kWaitingJoiner);
  EXPECT_TRUE(std::filesystem::exists(dir_ / "cluster_config.dat"));
}

TEST_F(StateMgrTest, WaitingJoinerSurvivesConfigBeforeWalCatchup) {
  auto opened = OpenRestart();
  ASSERT_TRUE(opened.ok()) << opened.status();
  auto config = nuraft::cs_new<nuraft::cluster_config>(/*log_idx=*/9,
                                                       /*prev_log_idx=*/4);
  for (const std::uint32_t id : {1U, 7U}) {
    const NuraftMemberConfig member = Member(id);
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
        MetaMemberIdentity{
            .server_id_ = member.server_id_,
            .principal_ = member.principal_,
            .data_control_endpoint_ = member.data_control_endpoint_,
            .ctl_endpoint_ = member.ctl_endpoint_,
        }
            .EncodeAux(),
        /*learner=*/false, /*priority=*/1));
  }
  (*opened)->save_config(*config);
  opened->reset();

  auto restarted = OpenRestart();
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_EQ((*restarted)->startup_mode(), NuraftStartupMode::kWaitingJoiner);
  EXPECT_TRUE((*restarted)->waiting_joiner_catchup_pending());
  EXPECT_EQ((*restarted)->read_state(), nullptr);
}

TEST_F(StateMgrTest,
       InitialBindingMarkerSurvivesConfigCopiesAndClearsAtExactBoundary) {
  auto opened = OpenInitial();
  ASSERT_TRUE(opened.ok()) << opened.status();
  PersistInitializedRaftEvidence(**opened);

  auto election_copy = nuraft::cs_new<nuraft::cluster_config>(
      /*log_idx=*/5, /*prev_log_idx=*/1);
  const NuraftMemberConfig member = Member(7);
  election_copy->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
      member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
      MetaMemberIdentity{
          .server_id_ = member.server_id_,
          .principal_ = member.principal_,
          .data_control_endpoint_ = member.data_control_endpoint_,
          .ctl_endpoint_ = member.ctl_endpoint_,
      }
          .EncodeAux(),
      /*learner=*/false, /*priority=*/1));
  (*opened)->save_config(*election_copy);
  EXPECT_TRUE((*opened)->initial_bindings_pending());
  EXPECT_TRUE(std::filesystem::exists(dir_ / "initial_bindings.dat"));

  ASSERT_TRUE((*opened)->CompleteInitialBindings(/*applied_index=*/1).ok());
  EXPECT_FALSE((*opened)->initial_bindings_pending());
  EXPECT_FALSE(std::filesystem::exists(dir_ / "initial_bindings.dat"));
  opened->reset();

  NuraftMemberConfig rebound = Member(7);
  rebound.raft_endpoint_ = "127.0.0.1:19707";
  rebound.data_control_endpoint_ = "127.0.0.1:29707";
  rebound.ctl_endpoint_ = "127.0.0.1:39707";
  auto restarted = NuraftStateMgr::Open(
      {.data_dir_ = dir_.string(), .local_member_ = std::move(rebound)});
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_FALSE((*restarted)->initial_bindings_pending());
  EXPECT_TRUE((*restarted)->transport_binding_replay_pending(
      /*applied_index=*/0));
  EXPECT_FALSE((*restarted)->transport_binding_replay_pending(
      /*applied_index=*/1));
}

TEST_F(StateMgrTest, CompletedZeroIndexGenesisDoesNotResurrectGrace) {
  auto opened = OpenInitial();
  ASSERT_TRUE(opened.ok()) << opened.status();
  PersistInitializedRaftEvidence(**opened);
  ASSERT_TRUE((*opened)->CompleteInitialBindings(/*applied_index=*/1).ok());
  EXPECT_FALSE(std::filesystem::exists(dir_ / "initial_bindings.dat"));
  EXPECT_TRUE(
      std::filesystem::exists(dir_ / "initial_bindings_complete.dat"));
  opened->reset();

  auto restarted = OpenRestart();
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_FALSE((*restarted)->initial_bindings_pending());
  EXPECT_FALSE(std::filesystem::exists(dir_ / "initial_bindings.dat"));
}

TEST_F(StateMgrTest,
       InitialBindingMarkerRecoversPublicationPrefixBeforeMembershipChange) {
  auto opened = OpenInitial();
  ASSERT_TRUE(opened.ok()) << opened.status();
  opened->reset();

  // The config file is the first atomic publication. Reconstruct the marker
  // if a process dies before the second publication reaches disk.
  std::filesystem::remove(dir_ / "initial_bindings.dat");
  auto recovered = OpenRestart();
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE((*recovered)->initial_bindings_pending());
  EXPECT_TRUE(std::filesystem::exists(dir_ / "initial_bindings.dat"));
  PersistInitializedRaftEvidence(**recovered);
  ASSERT_TRUE(
      (*recovered)->CompleteInitialBindings(/*applied_index=*/1).ok());

  auto changed = nuraft::cs_new<nuraft::cluster_config>(
      /*log_idx=*/9, /*prev_log_idx=*/5);
  for (const std::uint32_t id : {7U, 8U}) {
    const NuraftMemberConfig member = Member(id);
    changed->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
        MetaMemberIdentity{
            .server_id_ = member.server_id_,
            .principal_ = member.principal_,
            .data_control_endpoint_ = member.data_control_endpoint_,
            .ctl_endpoint_ = member.ctl_endpoint_,
        }
            .EncodeAux(),
        /*learner=*/false, /*priority=*/1));
  }
  (*recovered)->save_config(*changed);
  EXPECT_FALSE((*recovered)->initial_bindings_pending());
  EXPECT_FALSE(std::filesystem::exists(dir_ / "initial_bindings.dat"));
  EXPECT_TRUE(std::filesystem::exists(dir_ / "transport_bindings.dat"));
}

TEST_F(StateMgrTest, JoinedWaitingNodeRestartsAsOrdinaryMember) {
  auto opened = OpenRestart();
  ASSERT_TRUE(opened.ok()) << opened.status();
  PersistInitializedRaftEvidence(**opened);

  auto config = nuraft::cs_new<nuraft::cluster_config>(/*log_idx=*/9,
                                                       /*prev_log_idx=*/4);
  for (const std::uint32_t id : {1U, 7U}) {
    const NuraftMemberConfig member = Member(id);
    config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
        MetaMemberIdentity{
            .server_id_ = member.server_id_,
            .principal_ = member.principal_,
            .data_control_endpoint_ = member.data_control_endpoint_,
            .ctl_endpoint_ = member.ctl_endpoint_,
        }
            .EncodeAux(),
        /*learner=*/false, /*priority=*/1));
  }
  (*opened)->save_config(*config);
  EXPECT_TRUE((*opened)->waiting_joiner_catchup_pending());
  EXPECT_TRUE(std::filesystem::exists(dir_ / "waiting_joiner.dat"));
  ASSERT_TRUE(
      (*opened)->CompleteWaitingJoinerCatchup(/*applied_index=*/9).ok());
  EXPECT_FALSE((*opened)->waiting_joiner_catchup_pending());
  EXPECT_FALSE(std::filesystem::exists(dir_ / "waiting_joiner.dat"));
  opened->reset();

  auto restarted = OpenRestart();
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_EQ((*restarted)->startup_mode(), NuraftStartupMode::kRestart);
}

TEST_F(StateMgrTest, WaitingJoinerDoesNotCompleteAgainstPreAddConfig) {
  auto opened = OpenRestart();
  ASSERT_TRUE(opened.ok()) << opened.status();
  PersistInitializedRaftEvidence(**opened);

  auto pre_add = nuraft::cs_new<nuraft::cluster_config>(/*log_idx=*/9,
                                                        /*prev_log_idx=*/4);
  const NuraftMemberConfig inviter = Member(1);
  pre_add->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
      inviter.server_id_, /*dc_id=*/0, inviter.raft_endpoint_,
      MetaMemberIdentity{
          .server_id_ = inviter.server_id_,
          .principal_ = inviter.principal_,
          .data_control_endpoint_ = inviter.data_control_endpoint_,
          .ctl_endpoint_ = inviter.ctl_endpoint_,
      }
          .EncodeAux(),
      /*learner=*/false, /*priority=*/1));
  (*opened)->save_config(*pre_add);
  EXPECT_EQ((*opened)->CompleteWaitingJoinerCatchup(/*applied_index=*/9).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE((*opened)->waiting_joiner_catchup_pending());
  opened->reset();

  auto restarted = OpenRestart();
  ASSERT_TRUE(restarted.ok()) << restarted.status();
  EXPECT_EQ((*restarted)->startup_mode(), NuraftStartupMode::kWaitingJoiner);
  EXPECT_EQ((*restarted)->load_config()->get_server(7), nullptr);
  EXPECT_TRUE((*restarted)->waiting_joiner_catchup_pending());
}

TEST_F(StateMgrTest, RejectsManifestReplayAndPartialOrCorruptState) {
  auto initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok());
  initialized->reset();
  EXPECT_EQ(OpenInitial().status().code(),
            absl::StatusCode::kFailedPrecondition);

  WriteFileBytes(dir_ / "initial_bindings.dat", "truncated");
  EXPECT_EQ(OpenRestart().status().code(), absl::StatusCode::kDataLoss);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  WriteFileBytes(dir_ / "srv_state.dat", "partial");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  WriteFileBytes(dir_ / "cluster_config.dat", "truncated");
  EXPECT_FALSE(OpenRestart().ok());

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  WriteFileBytes(dir_ / "waiting_joiner.dat", "truncated");
  EXPECT_FALSE(OpenRestart().ok());

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  initialized->reset();
  std::filesystem::remove(dir_ / "initial_bindings.dat");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  initialized->reset();
  std::filesystem::remove(dir_ / "raft_started.dat");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  ASSERT_TRUE(
      (*initialized)->CompleteInitialBindings(/*applied_index=*/1).ok());
  initialized->reset();
  WriteFileBytes(dir_ / "transport_bindings.dat", "truncated");
  EXPECT_EQ(OpenRestart().status().code(), absl::StatusCode::kDataLoss);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  ASSERT_TRUE(
      (*initialized)->CompleteInitialBindings(/*applied_index=*/1).ok());
  initialized->reset();
  std::filesystem::remove(dir_ / "transport_bindings.dat");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  ASSERT_TRUE(
      (*initialized)->CompleteInitialBindings(/*applied_index=*/1).ok());
  initialized->reset();
  std::filesystem::remove(dir_ / "initial_bindings_complete.dat");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  initialized->reset();
  std::filesystem::remove(dir_ / "srv_state.dat");
  for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
    const std::string name = entry.path().filename().string();
    if (name.starts_with("log-") && name.ends_with(".seg")) {
      std::filesystem::remove(entry.path());
    }
  }
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  ASSERT_TRUE(
      (*initialized)->CompleteInitialBindings(/*applied_index=*/1).ok());
  initialized->reset();
  std::filesystem::remove(dir_ / "srv_state.dat");
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);

  RemoveTestDir(dir_);
  std::filesystem::create_directories(dir_);
  initialized = OpenInitial();
  ASSERT_TRUE(initialized.ok()) << initialized.status();
  PersistInitializedRaftEvidence(**initialized);
  ASSERT_TRUE(
      (*initialized)->CompleteInitialBindings(/*applied_index=*/1).ok());
  initialized->reset();
  for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
    const std::string name = entry.path().filename().string();
    if (name.starts_with("log-") && name.ends_with(".seg")) {
      std::filesystem::remove(entry.path());
    }
  }
  EXPECT_EQ(OpenRestart().status().code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST_F(StateMgrTest, SaveStateSurvivesReopen) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);

    nuraft::srv_state state;
    state.set_term(41);
    state.set_voted_for(3);
    mgr->save_state(state);
    auto log = mgr->load_log_store();
    nuraft::ptr<nuraft::log_entry> entry = MakeEntry(41, "initialized");
    ASSERT_EQ(log->append(entry), 1U);
    ASSERT_TRUE(log->flush());
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*reopened);
  nuraft::ptr<nuraft::srv_state> state = mgr->read_state();
  ASSERT_NE(state, nullptr);
  EXPECT_EQ(state->get_term(), 41u);
  EXPECT_EQ(state->get_voted_for(), 3);
}

TEST_F(StateMgrTest, SaveConfigSurvivesReopen) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);
    PersistInitializedRaftEvidence(*mgr);
    ASSERT_TRUE(mgr->CompleteInitialBindings(/*applied_index=*/1).ok());

    nuraft::ptr<nuraft::cluster_config> config =
        nuraft::cs_new<nuraft::cluster_config>(/*log_idx=*/9,
                                               /*prev_log_idx=*/4);
    for (const std::uint32_t id : {1U, 7U, 9U}) {
      const NuraftMemberConfig member = Member(id);
      config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
          member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
          MetaMemberIdentity{
              .server_id_ = member.server_id_,
              .principal_ = member.principal_,
              .data_control_endpoint_ = member.data_control_endpoint_,
              .ctl_endpoint_ = member.ctl_endpoint_,
          }
              .EncodeAux(),
          /*learner=*/false, /*priority=*/1));
    }
    mgr->save_config(*config);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*reopened);
  nuraft::ptr<nuraft::cluster_config> config = mgr->load_config();
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->get_log_idx(), 9u);
  EXPECT_EQ(config->get_prev_log_idx(), 4u);
  ASSERT_EQ(config->get_servers().size(), 3u);
  EXPECT_NE(config->get_server(9), nullptr);
  EXPECT_EQ(config->get_server(9)->get_endpoint(), "127.0.0.1:9709");
}

TEST_F(StateMgrTest, RecoversBothTransportBaselineTransactionPrefixes) {
  auto opened = OpenInitial();
  ASSERT_TRUE(opened.ok()) << opened.status();
  PersistInitializedRaftEvidence(**opened);
  ASSERT_TRUE(
      (*opened)->CompleteInitialBindings(/*applied_index=*/1).ok());
  const std::string old_config = ReadFileBytes(dir_ / "cluster_config.dat");
  const std::string old_baseline =
      ReadFileBytes(dir_ / "transport_bindings.dat");

  auto changed = nuraft::cs_new<nuraft::cluster_config>(
      /*log_idx=*/9, /*prev_log_idx=*/4);
  for (const std::uint32_t id : {7U, 8U}) {
    const NuraftMemberConfig member = Member(id);
    changed->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(
        member.server_id_, /*dc_id=*/0, member.raft_endpoint_,
        MetaMemberIdentity{
            .server_id_ = member.server_id_,
            .principal_ = member.principal_,
            .data_control_endpoint_ = member.data_control_endpoint_,
            .ctl_endpoint_ = member.ctl_endpoint_,
        }
            .EncodeAux(),
        /*learner=*/false, /*priority=*/1));
  }
  (*opened)->save_config(*changed);
  const std::string new_config = ReadFileBytes(dir_ / "cluster_config.dat");
  const std::string new_baseline =
      ReadFileBytes(dir_ / "transport_bindings.dat");
  opened->reset();

  // Crash before publishing config: retain the old pair and discard next.
  WriteFileBytes(dir_ / "cluster_config.dat", old_config);
  WriteFileBytes(dir_ / "transport_bindings.dat", old_baseline);
  WriteFileBytes(dir_ / "transport_bindings.next", new_baseline);
  auto before_config = OpenRestart();
  ASSERT_TRUE(before_config.ok()) << before_config.status();
  EXPECT_EQ((*before_config)->load_config()->get_servers().size(), 1U);
  EXPECT_FALSE(std::filesystem::exists(dir_ / "transport_bindings.next"));
  before_config->reset();

  // Crash after config but before promotion: next completes the new pair.
  WriteFileBytes(dir_ / "cluster_config.dat", new_config);
  WriteFileBytes(dir_ / "transport_bindings.dat", old_baseline);
  WriteFileBytes(dir_ / "transport_bindings.next", new_baseline);
  auto after_config = OpenRestart();
  ASSERT_TRUE(after_config.ok()) << after_config.status();
  EXPECT_EQ((*after_config)->load_config()->get_servers().size(), 2U);
  EXPECT_EQ(ReadFileBytes(dir_ / "transport_bindings.dat"), new_baseline);
  EXPECT_FALSE(std::filesystem::exists(dir_ / "transport_bindings.next"));
}

TEST_F(StateMgrTest, LoadLogStoreReturnsUsableSharedStore) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);

  nuraft::ptr<nuraft::log_store> first = mgr->load_log_store();
  nuraft::ptr<nuraft::log_store> second = mgr->load_log_store();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first.get(), second.get());

  nuraft::ptr<nuraft::log_entry> entry = MakeEntry(2, "via-mgr");
  EXPECT_EQ(first->append(entry), 1u);
  EXPECT_EQ(second->term_at(1), 2u);
}

TEST_F(StateMgrTest, SystemExitAbortsProcess) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);
  // The meta plane treats Raft-reported unrecoverable errors as fatal.
  EXPECT_DEATH(mgr->system_exit(nuraft::raft_err::N21_log_flush_failed), "");
}

}  // namespace
