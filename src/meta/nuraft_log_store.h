#pragma once

// NuraftLogStore: NuRaft `log_store` backed by the WAL v2 segmented log, one
// log per server (issue #19; plan
// docs/plans/issue-19-metadata-raft-implementation.md §0/§3 "WAL v2: 按快照
// 边界+大小上限分段,compact 删段").
//
// WAL v2 layout (`data_dir`):
//   log-<first_idx>.seg   one segment per index range; <first_idx> is the
//                         first log index the segment can hold, pinned again
//                         in the segment header
//   log-<first_idx>.seg.tmp
//                         in-flight compact rewrite, never valid data
// Segments roll when the active segment reaches the size cap (default
// 64 MiB, configurable via Open); a segment may exceed the cap by one record
// (the cap is a roll trigger checked before placing each record, never a
// hard record-size limit). compact(last_log_index) — always called by the
// core once the state machine made the snapshot at that index durable —
// unlinks every segment whose records all lie at or below the boundary and
// rewrites a partially overlapping segment into a fresh segment named
// log-<last_log_index+1>.seg, so post-compact segment boundaries coincide
// with snapshot compact boundaries. A full compaction leaves one header-only
// floor segment pinning start_index.
//
// INCOMPATIBLE WITH THE v1 SPIKE LAYOUT: Open fails with a clear error when
// `data_dir` holds a v1 `raft_log.dat`. Spike directories carry no production
// data; the remedy is to wipe the directory, not to migrate.
//
// Durability contract with the Raft core (verified against the pinned NuRaft
// source, third_party/nuraft @ 0b01b18) — unchanged from v1, extended to
// segment-file metadata:
//   - append()/write_at() pwrite records but never fsync. NuRaft always
//     follows a batch of writes with exactly one sync point before the
//     append result becomes visible: end_of_append_batch() after every
//     append_entries batch on both leader (handle_client_request.cxx) and
//     follower (handle_append_entries.cxx), and flush() inside
//     store_log_entry() for conf entries (raft_server.cxx, split-brain
//     guard). Those two hooks fdatasync the active segment here, plus an
//     fsync of the data directory when segment files were created, renamed,
//     or unlinked since the last sync — record bytes, ftruncate size changes,
//     and directory metadata are all pinned to those two hooks, so a crash
//     can only resurrect state from an append batch that was never acked
//     (Raft reconverges such tails through the normal consistency check).
//   - The segment rolled away from is fdatasynced before its descriptor
//     closes, so an intact non-tail segment on disk is always complete.
//   - write_at(index) ftruncates the segment holding `index` at the record
//     boundary and unlinks every later segment before writing, so a reopened
//     store never sees the overwritten tail past the last sync point as
//     anything but a torn tail (see below).
//   - compact() performs its own durability eagerly (tmp file + fdatasync +
//     rename + directory fsync) and reports failure as `false`, the error
//     channel NuRaft designed for it. NuRaft only compacts after the state
//     machine has made the snapshot durable (on_snapshot_completed runs
//     after create_snapshot's callback), so a crash cannot leave compacted
//     logs without a durable snapshot.
//
// On-disk format (all integers little-endian); the record format is
// byte-identical to v1, the segment header adds the format version:
//   segment  := segment_header record*
//   segment_header := magic u32 ("LSEG") | format_version u32 (=2) |
//                     first_index u64 | checksum u32
//   record   := magic u32 ("LRA1") | index u64 | term u64 | type u8 |
//               has_crc32 u8 | crc32 u32 | timestamp_us u64 |
//               payload_len u32 | payload bytes | checksum u32
//   checksum := FNV-1a32 over every preceding byte of the header/record.
// The segment header pins first_index so a fully compacted (record-less)
// floor segment still reopens at the right index.
//
// Recovery scan: Open scans segments in first_index order and rebuilds the
// in-memory index as an exact mirror of the intact on-disk prefix. Records
// must be contiguous within and across segments. The first anomaly — a short
// or checksum-mismatched record, a sequence gap, a bad segment header — is
// treated as a torn tail: the intact prefix is kept, the current segment is
// ftruncated at the anomaly offset (a torn header-only segment is recreated
// header-only so the compaction floor survives), and every later segment
// file is unlinked. Bytes past the anomaly were never acked in their current
// form: the anomaly is either a crash mid-pwrite or the remnant of an
// interrupted write_at/apply_pack truncation, both of which cut at or above
// the uncommitted tail (committed prefixes are never overwritten in Raft).
// This is v1's torn-tail rule lifted to a segment list.
//
// Threading and IO model: every method takes the internal mutex and performs
// synchronous pwrite/fdatasync on the calling NuRaft thread. NuRaft drives
// log writes from its append/commit background threads and request handlers,
// which are allowed to block on storage (the asio path behaves the same);
// the meta plane deliberately keeps durability IO out of celer coroutines.
//
// Failure behavior: Open() reports errors via absl::Status. pwrite failures
// inside append/write_at/apply_pack cannot be propagated through NuRaft's
// interface, and continuing would ack non-durable data, so they log a fatal
// message and abort (same policy as NuraftStateMgr::system_exit). flush()
// and compact() instead return `false`, the error channel NuRaft designed
// for them.
//
// Ownership: the store owns the active segment's file descriptor and closes
// it on destruction. Entries are cloned in and out (NuRaft buffers embed a
// cursor, so sharing instances across threads is unsafe); the segment files
// are the source of truth, the in-memory index an exact mirror of them.

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "absl/status/statusor.h"
#include "libnuraft/log_entry.hxx"
#include "libnuraft/log_store.hxx"

namespace keylane::meta {

enum class NuraftLogFaultPoint {
  kPwrite,
  kFtruncate,
  kFdatasync,
  kUnlink,
};

// Test seam for exercising the real NuRaft error policy without depending on
// a particular filesystem. Production passes no injector.
class NuraftLogFaultInjector {
 public:
  virtual ~NuraftLogFaultInjector() = default;
  virtual absl::Status Before(NuraftLogFaultPoint point) = 0;
};

class NuraftLogStore : public nuraft::log_store {
 public:
  // Default roll trigger for the active segment (plan §3 "单段大小上限").
  static constexpr uint64_t kDefaultMaxSegmentBytes = 64ull << 20;  // 64 MiB

  // Opens (creating if absent) the v2 segment set inside `data_dir`; the
  // directory itself is created when missing. Fails when the directory holds
  // a v1 spike log (`raft_log.dat`) — the layouts are incompatible.
  // `max_segment_bytes` is the segment roll trigger; tests pass a tiny value
  // to exercise rolling.
  static absl::StatusOr<std::unique_ptr<NuraftLogStore>> Open(
      const std::string& data_dir,
      uint64_t max_segment_bytes = kDefaultMaxSegmentBytes,
      std::shared_ptr<NuraftLogFaultInjector> fault_injector = nullptr);

  ~NuraftLogStore() override;

  nuraft::ulong next_slot() const override;
  nuraft::ulong start_index() const override;
  nuraft::ptr<nuraft::log_entry> last_entry() const override;
  nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;
  void write_at(nuraft::ulong index,
                nuraft::ptr<nuraft::log_entry>& entry) override;
  void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;
  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(
      nuraft::ulong start, nuraft::ulong end) override;
  nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;
  nuraft::ulong term_at(nuraft::ulong index) override;
  nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index,
                                   nuraft::int32 cnt) override;
  void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;
  bool compact(nuraft::ulong last_log_index) override;
  bool flush() override;

  // On-disk bytes covering [start_index(), next_slot()) — segment headers
  // plus live records. The §3 max_uncompacted_wal_bytes fail-safe gates
  // Propose on this while a snapshot is outstanding.
  uint64_t UncompactedBytes() const;

 private:
  struct Slot {
    uint64_t offset_ = 0;        // byte offset of the record in its segment
    uint32_t record_bytes_ = 0;  // total encoded record size
    uint64_t segment_ = 0;       // first index of the owning segment
    nuraft::ptr<nuraft::log_entry> entry_;
  };

  struct Segment {
    uint64_t first_index_ = 0;  // from the filename; the header pins it too
    uint64_t file_bytes_ = 0;   // header + records currently on disk
  };

  NuraftLogStore(std::string data_dir, uint64_t max_segment_bytes,
                 uint64_t start_index, std::map<uint64_t, Slot> entries,
                 std::map<uint64_t, Segment> segments, int active_fd,
                 std::shared_ptr<NuraftLogFaultInjector> fault_injector);

  static std::string SegmentFileName(uint64_t first_index);
  std::string SegmentPath(uint64_t first_index) const;

  // Callers must hold mutex_.
  // Appends the record at `index`, rolling to a fresh segment first when the
  // active one is at/past the size cap (or materializing the very first
  // segment). Segment creation marks the directory dirty; the next sync
  // point fsyncs it before any ack.
  absl::Status WriteRecordLocked(uint64_t index,
                                 const nuraft::log_entry& entry);
  // Truncates the log at `index`: ftruncates the segment holding `index` at
  // the record boundary, unlinks every later segment, and makes the
  // truncated segment the active one. No-op when `index == next_slot()`.
  absl::Status TruncateAtLocked(uint64_t index);
  // fdatasync of the active segment plus a directory fsync when segment
  // metadata changed since the last sync. This is the sync point every
  // mutation is pinned to (see the header contract).
  absl::Status SyncLocked();
  absl::Status MaybeFailLocked(NuraftLogFaultPoint point);
  void RecomputeLiveBytesLocked();

  const std::string data_dir_;
  const uint64_t max_segment_bytes_;

  mutable std::mutex mutex_;
  uint64_t start_index_ = 1;
  std::map<uint64_t, Slot> entries_;
  std::map<uint64_t, Segment> segments_;  // by first index, ascending
  // Descriptor of the last (append-target) segment; -1 when no segment
  // exists yet. Earlier segments stay closed between Open and any
  // truncation/rewrite that touches them.
  int active_fd_ = -1;
  // Set when a segment file was created/renamed/unlinked since the last
  // directory fsync; SyncLocked clears it.
  bool dir_dirty_ = false;
  uint64_t live_bytes_ = 0;  // sum of segment file_bytes_; see header
  std::shared_ptr<NuraftLogFaultInjector> fault_injector_;
};

}  // namespace keylane::meta
