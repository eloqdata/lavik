#include "keylane/meta/nuraft_log_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "libnuraft/buffer.hxx"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

constexpr uint32_t kSegmentMagic = 0x4C534547;  // "LSEG"
constexpr uint32_t kSegmentFormatVersion = 2;
constexpr size_t kSegmentHeaderSize =
    20;  // magic | version | first_index | crc
constexpr uint32_t kRecordMagic = 0x4C524131;  // "LRA1"
constexpr size_t kRecordHeaderSize = 38;       // magic..payload_len
constexpr size_t kRecordChecksumSize = 4;
// Guards the scan against absurd payload_len values from a corrupt header;
// far above anything the meta plane replicates.
constexpr uint32_t kMaxPayloadSize = 256 * 1024 * 1024;

void PutLe32(uint8_t* out, uint32_t value) {
  out[0] = static_cast<uint8_t>(value);
  out[1] = static_cast<uint8_t>(value >> 8);
  out[2] = static_cast<uint8_t>(value >> 16);
  out[3] = static_cast<uint8_t>(value >> 24);
}

void PutLe64(uint8_t* out, uint64_t value) {
  for (size_t ii = 0; ii < 8; ++ii) {
    out[ii] = static_cast<uint8_t>(value >> (8 * ii));
  }
}

uint32_t LoadLe32(const uint8_t* in) {
  return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) |
         (static_cast<uint32_t>(in[2]) << 16) |
         (static_cast<uint32_t>(in[3]) << 24);
}

uint64_t LoadLe64(const uint8_t* in) {
  uint64_t value = 0;
  for (size_t ii = 0; ii < 8; ++ii) {
    value |= static_cast<uint64_t>(in[ii]) << (8 * ii);
  }
  return value;
}

uint32_t Fnv1a32(const uint8_t* data, size_t len) {
  uint32_t hash = 2166136261u;
  for (size_t ii = 0; ii < len; ++ii) {
    hash ^= data[ii];
    hash *= 16777619u;
  }
  return hash;
}

absl::Status ErrnoStatus(const char* op, const std::string& path) {
  return absl::ErrnoToStatus(errno, std::string(op) + " failed on " + path);
}

// pwrite(2) may return short; loop until the full record is out.
absl::Status PwriteAll(int fd, const uint8_t* data, size_t len,
                       uint64_t offset) {
  size_t done = 0;
  while (done < len) {
    ssize_t written = ::pwrite(fd, data + done, len - done,
                               static_cast<off_t>(offset + done));
    if (written < 0) {
      if (errno == EINTR) continue;
      return absl::ErrnoToStatus(errno, "pwrite");
    }
    done += static_cast<size_t>(written);
  }
  return absl::OkStatus();
}

// Returns the number of bytes read; short reads signal EOF (clean or torn).
absl::StatusOr<size_t> PreadUpTo(int fd, uint8_t* data, size_t len,
                                 uint64_t offset) {
  size_t done = 0;
  while (done < len) {
    ssize_t got =
        ::pread(fd, data + done, len - done, static_cast<off_t>(offset + done));
    if (got < 0) {
      if (errno == EINTR) continue;
      return absl::ErrnoToStatus(errno, "pread");
    }
    if (got == 0) break;
    done += static_cast<size_t>(got);
  }
  return done;
}

absl::Status FsyncDirectory(const std::string& dir) {
  int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_fd < 0) return ErrnoStatus("open(dir)", dir);
  int rc = ::fsync(dir_fd);
  int saved_errno = errno;
  ::close(dir_fd);
  if (rc < 0) {
    errno = saved_errno;
    return ErrnoStatus("fsync(dir)", dir);
  }
  return absl::OkStatus();
}

// NuRaft's log-mutation interface has no error channel; a failed write means
// the entry might be acked without being durable, so the store stops the
// process rather than risk silent data loss (same policy as system_exit).
[[noreturn]] void FatalStoreError(const char* op, const std::string& path,
                                  const absl::Status& status) {
  spdlog::critical("nuraft log store: {} on {} failed unrecoverably: {}", op,
                   path, status.message());
  std::abort();
}

nuraft::ptr<nuraft::log_entry> CloneEntry(
    const nuraft::ptr<nuraft::log_entry>& entry) {
  nuraft::ptr<nuraft::buffer> buf;
  if (!entry->is_buf_null()) {
    buf = nuraft::buffer::clone(entry->get_buf());
  }
  return nuraft::cs_new<nuraft::log_entry>(
      entry->get_term(), buf, entry->get_val_type(), entry->get_timestamp(),
      entry->has_crc32(), entry->get_crc32(), /*compute_crc=*/false);
}

// Serializes one record (see the header for the layout). The buffer cursor of
// `entry` is left untouched: payload is taken via data_begin()/size().
std::vector<uint8_t> EncodeRecord(uint64_t index,
                                  const nuraft::log_entry& entry) {
  const uint8_t* payload = nullptr;
  size_t payload_len = 0;
  if (!entry.is_buf_null()) {
    payload = entry.get_buf().data_begin();
    payload_len = entry.get_buf().size();
  }

  const size_t total = kRecordHeaderSize + payload_len + kRecordChecksumSize;
  std::vector<uint8_t> out(total);
  uint8_t* cursor = out.data();
  PutLe32(cursor, kRecordMagic);
  PutLe64(cursor + 4, index);
  PutLe64(cursor + 12, entry.get_term());
  cursor[20] = static_cast<uint8_t>(entry.get_val_type());
  cursor[21] = entry.has_crc32() ? 1 : 0;
  PutLe32(cursor + 22, entry.get_crc32());
  PutLe64(cursor + 26, entry.get_timestamp());
  PutLe32(cursor + 34, static_cast<uint32_t>(payload_len));
  if (payload_len > 0) {
    std::memcpy(cursor + kRecordHeaderSize, payload, payload_len);
  }
  PutLe32(cursor + kRecordHeaderSize + payload_len,
          Fnv1a32(out.data(), kRecordHeaderSize + payload_len));
  return out;
}

// Serializes the fixed-size segment header pinning the segment's first index
// and the WAL format version.
std::vector<uint8_t> EncodeSegmentHeader(uint64_t first_index) {
  std::vector<uint8_t> out(kSegmentHeaderSize);
  PutLe32(out.data(), kSegmentMagic);
  PutLe32(out.data() + 4, kSegmentFormatVersion);
  PutLe64(out.data() + 8, first_index);
  PutLe32(out.data() + 16, Fnv1a32(out.data(), 16));
  return out;
}

}  // namespace

std::string NuraftLogStore::SegmentFileName(uint64_t first_index) {
  return "log-" + std::to_string(first_index) + ".seg";
}

std::string NuraftLogStore::SegmentPath(uint64_t first_index) const {
  return data_dir_ + "/" + SegmentFileName(first_index);
}

absl::StatusOr<std::unique_ptr<NuraftLogStore>> NuraftLogStore::Open(
    const std::string& data_dir, uint64_t max_segment_bytes,
    std::shared_ptr<NuraftLogFaultInjector> fault_injector) {
  if (max_segment_bytes <
      kSegmentHeaderSize + kRecordHeaderSize + kRecordChecksumSize + 1) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "max_segment_bytes is below one minimal record");
  }
  if (::mkdir(data_dir.c_str(), 0755) < 0 && errno != EEXIST) {
    return ErrnoStatus("mkdir", data_dir);
  }

  // v2 is deliberately incompatible with the legacy single-file layout (see
  // the class header): refuse to open a directory that still holds a v1 log.
  const std::string v1_path = data_dir + "/raft_log.dat";
  std::error_code ec;
  if (std::filesystem::exists(v1_path, ec)) {
    return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "WAL v2 refuses the legacy single-file layout: " + v1_path +
            " exists; wipe the legacy data directory (it holds no production "
            "data) instead of migrating it");
  }

  // A compact replacement is published under a non-WAL name before any old
  // segment is removed. If the process died during the cleanup/rename phase,
  // the fsynced ready file is the authoritative intent and contains the full
  // surviving suffix (or just its floor header). Finish that transaction
  // before the normal contiguous-prefix scan.
  std::optional<std::pair<uint64_t, std::string>> compact_ready;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind("compact-", 0) == 0 && name.size() > 14 &&
        name.ends_with(".ready")) {
      const std::string digits = name.substr(8, name.size() - 8 - 6);
      if (digits.empty() ||
          digits.find_first_not_of("0123456789") != std::string::npos) {
        continue;
      }
      if (compact_ready.has_value()) {
        return absl::DataLossError("multiple compact ready intents in " +
                                   data_dir);
      }
      try {
        compact_ready = {std::stoull(digits), entry.path().string()};
      } catch (...) {
        continue;
      }
    } else if (name.rfind("compact-", 0) == 0 && name.ends_with(".tmp")) {
      if (::unlink(entry.path().c_str()) < 0) {
        return ErrnoStatus("unlink(stale compact tmp)", entry.path().string());
      }
    }
  }
  if (compact_ready.has_value()) {
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
      const std::string name = entry.path().filename().string();
      if (name.rfind("log-", 0) == 0 && name.ends_with(".seg") &&
          ::unlink(entry.path().c_str()) < 0) {
        return ErrnoStatus("unlink(compact recovery)", entry.path().string());
      }
    }
    const std::string target =
        data_dir + "/" + SegmentFileName(compact_ready->first);
    if (::rename(compact_ready->second.c_str(), target.c_str()) < 0) {
      return ErrnoStatus("rename(compact recovery)", target);
    }
    absl::Status status = FsyncDirectory(data_dir);
    if (!status.ok()) return status;
  }

  // Collect segment files by first index; also tolerate and discard the
  // obsolete log-*.seg.tmp spelling from pre-intent builds.
  std::vector<std::pair<uint64_t, std::string>> files;
  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind("log-", 0) != 0) continue;
    if (name.size() >= 9 && name.compare(name.size() - 8, 8, ".seg.tmp") == 0) {
      if (::unlink(entry.path().c_str()) < 0) {
        return ErrnoStatus("unlink(stale tmp)", entry.path().string());
      }
      continue;
    }
    if (name.size() < 9 || name.compare(name.size() - 4, 4, ".seg") != 0) {
      continue;
    }
    const std::string digits = name.substr(4, name.size() - 4 - 4);
    if (digits.empty() ||
        digits.find_first_not_of("0123456789") != std::string::npos) {
      continue;  // not one of ours
    }
    uint64_t first_index = 0;
    try {
      first_index = std::stoull(digits);
    } catch (...) {
      continue;  // not one of ours
    }
    files.emplace_back(first_index, entry.path().string());
  }
  std::sort(files.begin(), files.end());

  // Scan the segment list, rebuilding the in-memory index as an exact mirror
  // of the intact on-disk prefix. `keep` counts the leading segment files
  // that survive; everything from the first anomaly on is torn tail (see the
  // class header for why bytes past it were never acked in their current
  // form) and is truncated/unlinked below.
  std::map<uint64_t, Slot> entries;
  std::map<uint64_t, Segment> segments;
  uint64_t expected = 0;  // next record index; 0 = pinned by the first segment
  size_t keep = 0;
  for (size_t ii = 0; ii < files.size(); ++ii, ++keep) {
    const uint64_t first_index = files[ii].first;
    const std::string& path = files[ii].second;
    if (expected != 0 && first_index != expected) {
      break;  // sequence gap/overlap across segment files
    }

    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) return ErrnoStatus("open(segment)", path);

    // Segment header. A missing/torn/foreign header means the crash tore the
    // segment creation before its first batch sync, so nothing in it was
    // acked: the very first segment is recreated header-only to preserve the
    // compaction floor its filename pins; any later one is dropped with the
    // rest of the tail.
    uint8_t header[kSegmentHeaderSize];
    absl::StatusOr<size_t> header_read =
        PreadUpTo(fd, header, sizeof(header), 0);
    if (!header_read.ok()) {
      ::close(fd);
      return header_read.status();
    }
    const bool header_ok = *header_read == sizeof(header) &&
                           LoadLe32(header) == kSegmentMagic &&
                           LoadLe32(header + 4) == kSegmentFormatVersion &&
                           LoadLe32(header + 16) == Fnv1a32(header, 16) &&
                           LoadLe64(header + 8) == first_index;
    if (!header_ok) {
      if (!segments.empty()) {
        ::close(fd);
        break;
      }
      spdlog::warn(
          "nuraft log store: recreating torn segment header of {} "
          "(first_index {})",
          path, first_index);
      std::vector<uint8_t> fresh = EncodeSegmentHeader(first_index);
      absl::Status status = PwriteAll(fd, fresh.data(), fresh.size(), 0);
      if (status.ok() && ::ftruncate(fd, kSegmentHeaderSize) < 0) {
        status = ErrnoStatus("ftruncate", path);
      }
      if (status.ok() && ::fdatasync(fd) < 0) {
        status = ErrnoStatus("fdatasync", path);
      }
      ::close(fd);
      if (!status.ok()) return status;
      segments[first_index] = Segment{first_index, kSegmentHeaderSize};
      expected = first_index;
      continue;
    }

    // Records: contiguous from first_index; the first anomaly is the torn
    // tail (crash mid-pwrite or an interrupted write_at/apply_pack
    // truncation).
    uint64_t offset = kSegmentHeaderSize;
    uint64_t index = first_index;
    uint64_t torn_offset = UINT64_MAX;
    while (true) {
      uint8_t record_header[kRecordHeaderSize];
      absl::StatusOr<size_t> record_header_read =
          PreadUpTo(fd, record_header, sizeof(record_header), offset);
      if (!record_header_read.ok()) {
        ::close(fd);
        return record_header_read.status();
      }
      if (*record_header_read == 0) break;  // clean EOF
      if (*record_header_read < sizeof(record_header)) {
        torn_offset = offset;
        break;
      }

      const uint32_t magic = LoadLe32(record_header);
      const uint64_t record_index = LoadLe64(record_header + 4);
      const uint32_t payload_len = LoadLe32(record_header + 34);
      if (magic != kRecordMagic || payload_len > kMaxPayloadSize ||
          record_index != index) {
        torn_offset = offset;
        break;
      }

      std::vector<uint8_t> rest(payload_len + kRecordChecksumSize);
      absl::StatusOr<size_t> rest_read =
          PreadUpTo(fd, rest.data(), rest.size(), offset + kRecordHeaderSize);
      if (!rest_read.ok()) {
        ::close(fd);
        return rest_read.status();
      }
      if (*rest_read < rest.size()) {
        torn_offset = offset;
        break;
      }
      {
        std::vector<uint8_t> full(kRecordHeaderSize + payload_len);
        std::memcpy(full.data(), record_header, sizeof(record_header));
        if (payload_len > 0) {
          std::memcpy(full.data() + kRecordHeaderSize, rest.data(),
                      payload_len);
        }
        if (LoadLe32(rest.data() + payload_len) !=
            Fnv1a32(full.data(), full.size())) {
          torn_offset = offset;
          break;
        }
      }

      nuraft::ptr<nuraft::buffer> payload = nuraft::buffer::alloc(payload_len);
      if (payload_len > 0) {
        std::memcpy(payload->data_begin(), rest.data(), payload_len);
      }
      Slot slot;
      slot.offset_ = offset;
      slot.record_bytes_ = static_cast<uint32_t>(
          kRecordHeaderSize + payload_len + kRecordChecksumSize);
      slot.segment_ = first_index;
      slot.entry_ = nuraft::cs_new<nuraft::log_entry>(
          LoadLe64(record_header + 12), payload,
          static_cast<nuraft::log_val_type>(record_header[20]),
          LoadLe64(record_header + 26), record_header[21] == 1,
          LoadLe32(record_header + 22),
          /*compute_crc=*/false);
      entries[index] = std::move(slot);
      ++index;
      offset += kRecordHeaderSize + payload_len + kRecordChecksumSize;
    }

    if (torn_offset != UINT64_MAX) {
      spdlog::warn("nuraft log store: truncating {} at torn offset {}", path,
                   torn_offset);
      if (::ftruncate(fd, static_cast<off_t>(torn_offset)) < 0) {
        absl::Status status = ErrnoStatus("ftruncate", path);
        ::close(fd);
        return status;
      }
      if (::fdatasync(fd) < 0) {
        absl::Status status = ErrnoStatus("fdatasync", path);
        ::close(fd);
        return status;
      }
      ::close(fd);
      segments[first_index] = Segment{first_index, torn_offset};
      // The torn segment is the tail by definition; nothing later survives.
      ++keep;
      break;
    }
    ::close(fd);
    segments[first_index] = Segment{first_index, offset};
    // A header-only segment must be the tail: the next file's first index
    // cannot equal `expected`, so the loop breaks on the next iteration.
    expected = index;
  }

  // Drop every segment file past the intact prefix.
  bool unlinked = false;
  for (size_t ii = keep; ii < files.size(); ++ii) {
    if (::unlink(files[ii].second.c_str()) < 0) {
      return ErrnoStatus("unlink(torn tail)", files[ii].second);
    }
    unlinked = true;
  }
  if (unlinked) {
    absl::Status status = FsyncDirectory(data_dir);
    if (!status.ok()) return status;
  }

  const uint64_t start_index = segments.empty() ? 1 : segments.begin()->first;
  int active_fd = -1;
  if (!segments.empty()) {
    const std::string active_path =
        data_dir + "/" + SegmentFileName(segments.rbegin()->first);
    active_fd = ::open(active_path.c_str(), O_RDWR | O_CLOEXEC);
    if (active_fd < 0) return ErrnoStatus("open(active segment)", active_path);
  }

  std::unique_ptr<NuraftLogStore> store(new NuraftLogStore(
      data_dir, max_segment_bytes, start_index, std::move(entries),
      std::move(segments), active_fd, std::move(fault_injector)));
  store->RecomputeLiveBytesLocked();
  return store;
}

NuraftLogStore::NuraftLogStore(
    std::string data_dir, uint64_t max_segment_bytes, uint64_t start_index,
    std::map<uint64_t, Slot> entries, std::map<uint64_t, Segment> segments,
    int active_fd, std::shared_ptr<NuraftLogFaultInjector> fault_injector)
    : data_dir_(std::move(data_dir)),
      max_segment_bytes_(max_segment_bytes),
      start_index_(start_index),
      entries_(std::move(entries)),
      segments_(std::move(segments)),
      active_fd_(active_fd),
      fault_injector_(std::move(fault_injector)) {}

NuraftLogStore::~NuraftLogStore() {
  if (active_fd_ >= 0) ::close(active_fd_);
}

nuraft::ulong NuraftLogStore::next_slot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.empty() ? start_index_ : entries_.rbegin()->first + 1;
}

nuraft::ulong NuraftLogStore::start_index() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return start_index_;
}

nuraft::ptr<nuraft::log_entry> NuraftLogStore::last_entry() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (entries_.empty()) {
    // NuRaft requires a dummy entry with term 0 when the store is empty.
    return nuraft::cs_new<nuraft::log_entry>(
        0, nuraft::buffer::alloc(sizeof(nuraft::ulong)));
  }
  return CloneEntry(entries_.rbegin()->second.entry_);
}

nuraft::ulong NuraftLogStore::append(nuraft::ptr<nuraft::log_entry>& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t index =
      entries_.empty() ? start_index_ : entries_.rbegin()->first + 1;
  absl::Status status = WriteRecordLocked(index, *entry);
  if (!status.ok()) FatalStoreError("append", SegmentPath(index), status);
  return index;
}

void NuraftLogStore::write_at(nuraft::ulong index,
                              nuraft::ptr<nuraft::log_entry>& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t current_next =
      entries_.empty() ? start_index_ : entries_.rbegin()->first + 1;
  if (index < start_index_ || index > current_next) {
    // The core never writes into the compacted prefix or past the tail;
    // refusing keeps the on-disk sequence contiguous.
    spdlog::error(
        "nuraft log store: write_at({}) outside [{}, {}], ignored in {}", index,
        start_index_, current_next, data_dir_);
    return;
  }
  absl::Status status = TruncateAtLocked(index);
  if (!status.ok()) FatalStoreError("write_at(truncate)", data_dir_, status);
  status = WriteRecordLocked(index, *entry);
  if (!status.ok()) FatalStoreError("write_at", SegmentPath(index), status);
}

void NuraftLogStore::end_of_append_batch(nuraft::ulong /*start*/,
                                         nuraft::ulong /*cnt*/) {
  std::lock_guard<std::mutex> lock(mutex_);
  // The batch sync hook: after this returns the core may ack the entries.
  absl::Status status = SyncLocked();
  if (!status.ok()) FatalStoreError("end_of_append_batch", data_dir_, status);
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
NuraftLogStore::log_entries(nuraft::ulong start, nuraft::ulong end) {
  std::lock_guard<std::mutex> lock(mutex_);
  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> result =
      nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
  if (end <= start) return result;
  result->reserve(end - start);
  for (uint64_t ii = start; ii < end; ++ii) {
    auto found = entries_.find(ii);
    if (found == entries_.end()) return nullptr;
    result->push_back(CloneEntry(found->second.entry_));
  }
  return result;
}

nuraft::ptr<nuraft::log_entry> NuraftLogStore::entry_at(nuraft::ulong index) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = entries_.find(index);
  if (found == entries_.end()) return nullptr;
  return CloneEntry(found->second.entry_);
}

nuraft::ulong NuraftLogStore::term_at(nuraft::ulong index) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = entries_.find(index);
  if (found == entries_.end()) return 0;
  return found->second.entry_->get_term();
}

nuraft::ptr<nuraft::buffer> NuraftLogStore::pack(nuraft::ulong index,
                                                 nuraft::int32 cnt) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (cnt < 0) return nullptr;
  std::vector<nuraft::ptr<nuraft::buffer>> serialized;
  size_t total = sizeof(nuraft::int32);
  for (uint64_t ii = index; ii < index + static_cast<uint64_t>(cnt); ++ii) {
    auto found = entries_.find(ii);
    if (found == entries_.end()) return nullptr;
    // log_entry::serialize() encodes term|type|payload; the entry timestamp
    // and crc are not carried by this format (same loss as NuRaft's own
    // in-memory store's pack).
    nuraft::ptr<nuraft::log_entry> clone = CloneEntry(found->second.entry_);
    nuraft::ptr<nuraft::buffer> buf = clone->serialize();
    serialized.push_back(buf);
    total += sizeof(nuraft::int32) + buf->size();
  }

  nuraft::ptr<nuraft::buffer> out = nuraft::buffer::alloc(total);
  out->put(cnt);
  for (const nuraft::ptr<nuraft::buffer>& buf : serialized) {
    out->put(static_cast<nuraft::int32>(buf->size()));
    out->put(*buf);
  }
  out->pos(0);
  return out;
}

void NuraftLogStore::apply_pack(nuraft::ulong index, nuraft::buffer& pack) {
  // Decode outside the lock; a malformed pack is peer input, so it is
  // rejected without touching local state rather than aborting the process.
  pack.pos(0);
  if (pack.size() < sizeof(nuraft::int32)) {
    spdlog::error("nuraft log store: malformed pack ({} bytes) in {}",
                  pack.size(), data_dir_);
    return;
  }
  const nuraft::int32 cnt = pack.get_int();
  std::vector<nuraft::ptr<nuraft::log_entry>> decoded;
  for (nuraft::int32 ii = 0; ii < cnt; ++ii) {
    if (pack.pos() + sizeof(nuraft::int32) > pack.size()) {
      spdlog::error("nuraft log store: truncated pack header in {}", data_dir_);
      return;
    }
    const nuraft::int32 entry_size = pack.get_int();
    if (entry_size < 0 ||
        pack.pos() + static_cast<size_t>(entry_size) > pack.size()) {
      spdlog::error("nuraft log store: truncated pack entry in {}", data_dir_);
      return;
    }
    nuraft::ptr<nuraft::buffer> entry_buf =
        nuraft::buffer::alloc(static_cast<size_t>(entry_size));
    pack.get(entry_buf);
    decoded.push_back(nuraft::log_entry::deserialize(*entry_buf));
  }

  std::lock_guard<std::mutex> lock(mutex_);
  absl::Status status = TruncateAtLocked(index);
  if (!status.ok()) FatalStoreError("apply_pack(truncate)", data_dir_, status);
  for (size_t ii = 0; ii < decoded.size(); ++ii) {
    status = WriteRecordLocked(index + ii, *decoded[ii]);
    if (!status.ok()) FatalStoreError("apply_pack", data_dir_, status);
  }
  // Snapshot installation is a rare bulk write; sync immediately so the
  // catch-up state is durable before the core moves on.
  status = SyncLocked();
  if (!status.ok()) FatalStoreError("apply_pack(sync)", data_dir_, status);
  start_index_ = entries_.empty() ? 1 : entries_.begin()->first;
}

bool NuraftLogStore::compact(nuraft::ulong last_log_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t new_first =
      start_index_ <= last_log_index ? last_log_index + 1 : start_index_;
  std::map<uint64_t, Slot> replacement_entries;
  auto survivor = entries_.upper_bound(last_log_index);
  const std::string ready_path =
      data_dir_ + "/compact-" + std::to_string(new_first) + ".ready";
  const std::string tmp_path = ready_path + ".tmp";

  // Preflight the only destructive operation while the old segment set is
  // still completely authoritative. Once the durable intent is published,
  // cleanup failures cannot truthfully be returned as a non-mutating false.
  if (!segments_.empty()) {
    if (auto injected = MaybeFailLocked(NuraftLogFaultPoint::kUnlink);
        !injected.ok()) {
      spdlog::error("nuraft log store: compact unlink injected failure: {}",
                    injected.message());
      return false;
    }
  }

  int tmp_fd =
      ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (tmp_fd < 0) {
    spdlog::error("nuraft log store: compact open intent failed on {}: {}",
                  tmp_path, std::strerror(errno));
    return false;
  }
  uint64_t offset = 0;
  absl::Status status = absl::OkStatus();
  {
    std::vector<uint8_t> header = EncodeSegmentHeader(new_first);
    status = MaybeFailLocked(NuraftLogFaultPoint::kPwrite);
    if (status.ok()) {
      status = PwriteAll(tmp_fd, header.data(), header.size(), 0);
    }
    offset = header.size();
  }
  while (status.ok() && survivor != entries_.end()) {
    std::vector<uint8_t> record =
        EncodeRecord(survivor->first, *survivor->second.entry_);
    status = MaybeFailLocked(NuraftLogFaultPoint::kPwrite);
    if (status.ok()) {
      status = PwriteAll(tmp_fd, record.data(), record.size(), offset);
    }
    if (!status.ok()) break;
    Slot slot = survivor->second;
    slot.offset_ = offset;
    slot.segment_ = new_first;
    replacement_entries.emplace(survivor->first, std::move(slot));
    offset += record.size();
    ++survivor;
  }
  if (status.ok()) {
    status = MaybeFailLocked(NuraftLogFaultPoint::kFdatasync);
  }
  if (status.ok() && ::fdatasync(tmp_fd) < 0) {
    status = ErrnoStatus("fdatasync(compact intent)", tmp_path);
  }
  if (::close(tmp_fd) < 0 && status.ok()) {
    status = ErrnoStatus("close(tmp)", tmp_path);
  }
  if (!status.ok()) {
    (void)::unlink(tmp_path.c_str());
    spdlog::error("nuraft log store: compact intent write failed on {}: {}",
                  tmp_path, status.message());
    return false;
  }
  status = MaybeFailLocked(NuraftLogFaultPoint::kRename);
  if (status.ok()) {
    status = MaybeFailLocked(NuraftLogFaultPoint::kDirectorySync);
  }
  if (!status.ok() || ::rename(tmp_path.c_str(), ready_path.c_str()) < 0) {
    if (status.ok()) status = ErrnoStatus("rename(compact intent)", ready_path);
    (void)::unlink(tmp_path.c_str());
    spdlog::error("nuraft log store: compact publish failed on {}: {}",
                  ready_path, status.message());
    return false;
  }
  status = FsyncDirectory(data_dir_);
  if (!status.ok()) {
    FatalStoreError("compact(sync intent)", data_dir_, status);
  }

  // The durable ready file is now a recovery transaction. Complete it
  // fail-stop: returning false after this point would promise NuRaft that the
  // old log remained authoritative when a restart would finish compaction.
  if (active_fd_ >= 0) {
    ::close(active_fd_);
    active_fd_ = -1;
  }
  for (const auto& [first, segment] : segments_) {
    (void)segment;
    if (::unlink(SegmentPath(first).c_str()) < 0) {
      FatalStoreError("compact(unlink committed intent)", SegmentPath(first),
                      ErrnoStatus("unlink", SegmentPath(first)));
    }
  }
  status = FsyncDirectory(data_dir_);
  if (!status.ok())
    FatalStoreError("compact(sync removals)", data_dir_, status);
  const std::string new_path = SegmentPath(new_first);
  if (::rename(ready_path.c_str(), new_path.c_str()) < 0) {
    FatalStoreError("compact(install intent)", new_path,
                    ErrnoStatus("rename", new_path));
  }
  status = FsyncDirectory(data_dir_);
  if (!status.ok()) FatalStoreError("compact(sync install)", data_dir_, status);
  active_fd_ = ::open(new_path.c_str(), O_RDWR | O_CLOEXEC);
  if (active_fd_ < 0) {
    FatalStoreError("compact(reopen)", new_path, ErrnoStatus("open", new_path));
  }
  start_index_ = new_first;
  entries_ = std::move(replacement_entries);
  segments_.clear();
  segments_[new_first] = Segment{new_first, offset};
  dir_dirty_ = false;
  RecomputeLiveBytesLocked();
  return true;
}

bool NuraftLogStore::flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  // Returning false routes the core into state_mgr::system_exit(N21).
  return SyncLocked().ok();
}

uint64_t NuraftLogStore::UncompactedBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return live_bytes_;
}

absl::Status NuraftLogStore::WriteRecordLocked(uint64_t index,
                                               const nuraft::log_entry& entry) {
  if (auto injected = MaybeFailLocked(NuraftLogFaultPoint::kPwrite);
      !injected.ok()) {
    return injected;
  }
  auto active =
      segments_.empty() ? segments_.end() : std::prev(segments_.end());
  // Roll when the active segment holds at least one record and is at/past the
  // size cap; the cap is a roll trigger, never a hard record-size limit, so a
  // single oversized record simply lands in its own segment.
  const bool roll = active != segments_.end() &&
                    active->second.file_bytes_ > kSegmentHeaderSize &&
                    active->second.file_bytes_ >= max_segment_bytes_;
  if (active == segments_.end() || roll) {
    if (active_fd_ >= 0) {
      // fdatasync the segment rolled away from before its descriptor closes,
      // so an intact non-tail segment on disk is always complete.
      if (::fdatasync(active_fd_) < 0) {
        return ErrnoStatus("fdatasync(roll)",
                           SegmentPath(active->second.first_index_));
      }
      ::close(active_fd_);
      active_fd_ = -1;
    }
    const std::string path = SegmentPath(index);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return ErrnoStatus("open(segment)", path);
    std::vector<uint8_t> header = EncodeSegmentHeader(index);
    absl::Status status = PwriteAll(fd, header.data(), header.size(), 0);
    if (!status.ok()) {
      ::close(fd);
      return status;
    }
    segments_[index] = Segment{index, static_cast<uint64_t>(header.size())};
    active_fd_ = fd;
    live_bytes_ += header.size();
    // The new segment file becomes durable at the next sync point's
    // directory fsync, before any record in it can be acked.
    dir_dirty_ = true;
    active = std::prev(segments_.end());
  }

  Segment& segment = active->second;
  std::vector<uint8_t> record = EncodeRecord(index, entry);
  absl::Status status =
      PwriteAll(active_fd_, record.data(), record.size(), segment.file_bytes_);
  if (!status.ok()) return status;

  Slot slot;
  slot.offset_ = segment.file_bytes_;
  slot.record_bytes_ = static_cast<uint32_t>(record.size());
  slot.segment_ = segment.first_index_;
  slot.entry_ = nuraft::cs_new<nuraft::log_entry>(
      entry.get_term(),
      entry.is_buf_null() ? nuraft::ptr<nuraft::buffer>()
                          : nuraft::buffer::clone(entry.get_buf()),
      entry.get_val_type(), entry.get_timestamp(), entry.has_crc32(),
      entry.get_crc32(), /*compute_crc=*/false);
  entries_[index] = std::move(slot);
  segment.file_bytes_ += record.size();
  live_bytes_ += record.size();
  return absl::OkStatus();
}

absl::Status NuraftLogStore::TruncateAtLocked(uint64_t index) {
  auto first_replaced = entries_.lower_bound(index);
  if (first_replaced == entries_.end()) {
    return absl::OkStatus();  // truncating at the tail: nothing to do
  }
  const uint64_t cut_offset = first_replaced->second.offset_;
  const uint64_t cut_segment = first_replaced->second.segment_;

  // The truncated segment becomes the append target; every later segment is
  // dead. ftruncate precedes the unlinks so no crash ordering can resurrect
  // overwritten records past the truncation point as anything but a torn
  // tail (see the class header).
  if (active_fd_ >= 0) {
    ::close(active_fd_);
    active_fd_ = -1;
  }
  const std::string path = SegmentPath(cut_segment);
  int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) return ErrnoStatus("open(truncate)", path);
  active_fd_ = fd;
  if (auto injected = MaybeFailLocked(NuraftLogFaultPoint::kFtruncate);
      !injected.ok()) {
    return injected;
  }
  if (::ftruncate(active_fd_, static_cast<off_t>(cut_offset)) < 0) {
    return ErrnoStatus("ftruncate", path);
  }
  segments_.at(cut_segment).file_bytes_ = cut_offset;

  for (auto it = segments_.upper_bound(cut_segment); it != segments_.end();) {
    if (::unlink(SegmentPath(it->first).c_str()) < 0) {
      return ErrnoStatus("unlink(truncate)", SegmentPath(it->first));
    }
    it = segments_.erase(it);
  }
  dir_dirty_ = true;
  entries_.erase(first_replaced, entries_.end());
  RecomputeLiveBytesLocked();
  return absl::OkStatus();
}

absl::Status NuraftLogStore::SyncLocked() {
  if (auto injected = MaybeFailLocked(NuraftLogFaultPoint::kFdatasync);
      !injected.ok()) {
    return injected;
  }
  if (active_fd_ >= 0 && ::fdatasync(active_fd_) < 0) {
    return ErrnoStatus("fdatasync", data_dir_);
  }
  if (dir_dirty_) {
    absl::Status status = FsyncDirectory(data_dir_);
    if (!status.ok()) return status;
    dir_dirty_ = false;
  }
  return absl::OkStatus();
}

absl::Status NuraftLogStore::MaybeFailLocked(NuraftLogFaultPoint point) {
  if (fault_injector_ == nullptr) return absl::OkStatus();
  return fault_injector_->Before(point);
}

void NuraftLogStore::RecomputeLiveBytesLocked() {
  uint64_t total = 0;
  for (const auto& [first, segment] : segments_) {
    total += segment.file_bytes_;
  }
  live_bytes_ = total;
}

}  // namespace keylane::meta
