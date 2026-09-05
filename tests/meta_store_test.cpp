// Tests for the issue-#19 persistence glue under src/meta/: the WAL v2
// segmented NuraftLogStore and NuraftStateMgr.
//
// Component contracts are exercised by closing and reopening the same data
// directory (close + reopen stands in for process restart). A narrow fault
// injector covers pwrite/ftruncate/fdatasync/unlink policy without depending
// on a particular filesystem. The state machine tests — component and
// core-driven raft_server integration — live in meta_state_machine_test.cpp
// on the formal MetaStateMachine with real commands (the spike state machine
// and its component tests were removed when the formal state machine landed;
// the MidStreamPrune livelock
// regression is covered by MetaStateMachineTest.MidStreamPrune* there).

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "libnuraft/nuraft.hxx"
#include "meta/nuraft_log_store.h"
#include "meta/nuraft_state_mgr.h"

namespace {

using keylane::meta::NuraftLogFaultInjector;
using keylane::meta::NuraftLogFaultPoint;
using keylane::meta::NuraftLogStore;
using keylane::meta::NuraftStateMgr;

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

  // WAL v2: tiny segment cap forces rolling so tests exercise multi-segment
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

TEST_F(LogStoreTest, RejectsV1LayoutDirectory) {
  // WAL v2 is incompatible with the v1 spike layout: a directory holding the
  // v1 single-file log must fail loudly instead of silently starting a fresh
  // v2 log next to it.
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
    // snapshot compact boundary (plan §3).
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
  EXPECT_FALSE(store->compact(1));
}

// ---------------------------------------------------------------------------
// NuraftStateMgr
// ---------------------------------------------------------------------------

class StateMgrTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("store", "mgr"); }
  void TearDown() override { RemoveTestDir(dir_); }

  absl::StatusOr<std::unique_ptr<NuraftStateMgr>> Open() {
    return NuraftStateMgr::Open(dir_, /*server_id=*/7, "127.0.0.1:9707");
  }

  std::filesystem::path dir_;
};

TEST_F(StateMgrTest, FreshDirYieldsInitialConfigAndNoState) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<NuraftStateMgr> mgr = std::move(*opened);

  EXPECT_EQ(mgr->server_id(), 7);
  EXPECT_EQ(mgr->read_state(), nullptr);

  nuraft::ptr<nuraft::cluster_config> config = mgr->load_config();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(config->get_servers().size(), 1u);
  const nuraft::ptr<nuraft::srv_config>& self = config->get_servers().front();
  EXPECT_EQ(self->get_id(), 7);
  EXPECT_EQ(self->get_endpoint(), "127.0.0.1:9707");
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

    nuraft::ptr<nuraft::cluster_config> config =
        nuraft::cs_new<nuraft::cluster_config>(/*log_idx=*/9,
                                               /*prev_log_idx=*/4);
    config->get_servers().push_back(
        nuraft::cs_new<nuraft::srv_config>(1, "127.0.0.1:9701"));
    config->get_servers().push_back(
        nuraft::cs_new<nuraft::srv_config>(7, "127.0.0.1:9707"));
    config->get_servers().push_back(
        nuraft::cs_new<nuraft::srv_config>(9, "127.0.0.1:9709"));
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
