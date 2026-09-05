#pragma once

// MetaEncoding provides primitives for the metadata control plane's
// hand-written, versioned, little-endian binary encoding.
//
// Wire conventions:
//   - Fixed-width integers (u8/u16/u32/u64) are little-endian.
//   - Every committed blob (command or record) starts with a u16
//     schema_version; readers reject versions outside
//     [kMetaMinReadableSchemaVersion, kMetaCurrentSchemaVersion].
//   - Variable-length byte strings carry a u32 length prefix; readers always
//     enforce a caller-supplied cap, and the cap check precedes the bounds
//     check so an over-cap prefix fails even on a truncated buffer.
//   - Lists carry a u32 element count, likewise capped by the reader.
//   - Optionals carry a u8 presence tag (0 = absent, 1 = present); any other
//     tag value is a decode failure.
//   - Readers are strict: truncation, cap violations, unknown versions/tags,
//     and trailing bytes all fail safely. There is no silent truncation.
//
// Failure classification:
//   - Decode failures (unknown schema version, corrupt or over-cap encoding)
//     are FAIL-STOP: the same byte sequence must fail identically on every
//     node, and the state machine treats one as system_exit territory. These
//     statuses carry MetaFailureClass::kFailStop (absl kInvalidArgument).
//   - Domain rejections (CAS conflicts, invariant violations on a cleanly
//     decoded command) are produced by the apply layer: the log index is
//     consumed and an audit record written, state unchanged.
//     These carry MetaFailureClass::kDomainReject (absl kFailedPrecondition).
//   Encode-side cap violations (a caller trying to build an out-of-spec
//   command) use kDomainReject: nothing was ever put on the wire.
//
// No exceptions are thrown; all fallible operations return absl::Status or
// absl::StatusOr.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace keylane::meta {

// ---------------------------------------------------------------------------
// Hard caps keep all state bounded. Exceeding a cap fails safely and never
// silently truncates. Values are v1 policy choices; the
// encoded schema does not depend on them.
// ---------------------------------------------------------------------------

// Single committed command, total encoded bytes.
inline constexpr std::uint32_t kMaxMetaCommandBytes = 1u << 20;  // 1 MiB
// One bounded payload blob: policy content, intent, kind_phase_blob, terminal
// result.
inline constexpr std::uint32_t kMaxMetaPayloadBytes = 256u * 1024u;  // 256 KiB
// Registered data nodes.
inline constexpr std::uint32_t kMaxMetaNodes = 4096;
// Shard groups.
inline constexpr std::uint32_t kMaxMetaGroups = 512;
// Non-terminal operations.
inline constexpr std::uint32_t kMaxMetaActiveOperations = 4096;
// Terminal-operation archive summaries kept as the tombstone index; at the
// cap ArchiveOperations is rejected until the operator exports.
inline constexpr std::uint32_t kMaxMetaArchivedOperationSummaries = 65536;
// Retained versions per policy_id.
inline constexpr std::uint32_t kMaxMetaPolicyVersionsPerPolicy = 32;
// Total policy content bytes across all policies.
inline constexpr std::uint32_t kMaxMetaPolicyTotalBytes = 16u << 20;  // 16 MiB
// Audit window records; when full, privileged Propose fails with
// RESOURCE_EXHAUSTED until the operator exports; records are never silently
// dropped.
inline constexpr std::uint32_t kMaxMetaAuditWindowRecords = 65536;
// Total snapshot bytes; create_snapshot fails and alerts beyond this.
inline constexpr std::uint64_t kMaxMetaSnapshotBytes = 512ull << 20;  // 512 MiB
// Uncompacted WAL bytes; Propose fails with RESOURCE_EXHAUSTED beyond this
// while a snapshot is outstanding.
inline constexpr std::uint64_t kMaxMetaUncompactedWalBytes = 1ull
                                                             << 30;  // 1 GiB

// ---------------------------------------------------------------------------
// Schema versioning.
// ---------------------------------------------------------------------------

inline constexpr std::uint16_t kMetaSchemaVersionV1 = 1;
inline constexpr std::uint16_t kMetaSchemaVersionV2 = 2;
#ifndef KEYLANE_META_BINARY_SCHEMA_MAX
#define KEYLANE_META_BINARY_SCHEMA_MAX 2
#endif
static_assert(KEYLANE_META_BINARY_SCHEMA_MAX == 1 ||
                  KEYLANE_META_BINARY_SCHEMA_MAX == 2,
              "test fixture binary schema must be v1 or v2");
// Oldest format this binary can read. The state machine reads N and N-1.
// V2 deliberately keeps the v1 field layout so negotiation and rolling
// compatibility can be exercised independently of structural migration.
inline constexpr std::uint16_t kMetaMinReadableSchemaVersion =
    kMetaSchemaVersionV1;
// Format this binary writes, except where a command's layout belongs to the
// permanently frozen oldest-readable subset (see SetSchemaVersion).
inline constexpr std::uint16_t kMetaCurrentSchemaVersion =
    KEYLANE_META_BINARY_SCHEMA_MAX;

// ---------------------------------------------------------------------------
// Failure classification. See the file header for the two classes.
// ---------------------------------------------------------------------------

enum class MetaFailureClass {
  kFailStop,      // decode failure: same bytes fail identically everywhere
  kDomainReject,  // cleanly decoded; rejected by validation (apply/propose)
};

absl::Status MetaFailStopError(std::string_view message);
absl::Status MetaDomainRejectError(std::string_view message);
MetaFailureClass MetaFailureClassOf(const absl::Status& status);

// ---------------------------------------------------------------------------
// Writer: append-only buffer. Writes cannot fail; out-of-spec values are
// rejected by the command layer at encode time (see meta_commands.cpp).
// ---------------------------------------------------------------------------

class MetaWriter {
 public:
  MetaWriter() = default;

  void WriteU8(std::uint8_t v);
  void WriteU16(std::uint16_t v);
  void WriteU32(std::uint32_t v);
  void WriteU64(std::uint64_t v);

  // Fixed-length bytes, no length prefix; the reader must know the length.
  void WriteRaw(std::string_view bytes);
  // u32 length prefix + bytes.
  void WriteString(std::string_view bytes);
  // List element count (u32). Prefer WriteList below.
  void WriteCount(std::uint32_t count);

  const std::string& buffer() const { return buffer_; }
  std::string&& TakeBuffer() { return std::move(buffer_); }

  // Element codecs are lambdas: WriteList(v, [](MetaWriter& w, const T&
  // x){...}).
  template <typename T, typename WriteElem>
  void WriteList(const std::vector<T>& items, WriteElem write_elem) {
    WriteCount(static_cast<std::uint32_t>(items.size()));
    for (const T& item : items) write_elem(*this, item);
  }

  // Presence tag 0/1 + element when present.
  template <typename T, typename WriteElem>
  void WriteOptional(const std::optional<T>& opt, WriteElem write_elem) {
    if (!opt.has_value()) {
      WriteU8(0);
      return;
    }
    WriteU8(1);
    write_elem(*this, *opt);
  }

 private:
  std::string buffer_;
};

// ---------------------------------------------------------------------------
// Reader: strict bounds checking; every failure is MetaFailureClass::
// kFailStop. Views returned by ReadRaw/ReadString alias the input buffer,
// which must outlive them.
// ---------------------------------------------------------------------------

class MetaReader {
 public:
  explicit MetaReader(std::string_view data) : data_(data) {}

  absl::StatusOr<std::uint8_t> ReadU8();
  absl::StatusOr<std::uint16_t> ReadU16();
  absl::StatusOr<std::uint32_t> ReadU32();
  absl::StatusOr<std::uint64_t> ReadU64();

  absl::StatusOr<std::string_view> ReadRaw(std::size_t bytes);
  // Cap check precedes the bounds check (over-cap prefixes fail even on
  // truncated input, so the two corruptions are indistinguishable by result).
  absl::StatusOr<std::string_view> ReadString(std::uint32_t max_bytes);
  absl::StatusOr<std::uint32_t> ReadCount(std::uint32_t max_count);

  // Fails when unread bytes remain; call once at the end of every decode.
  absl::Status Finish() const;

  std::size_t remaining() const { return data_.size() - pos_; }

  // Element codecs are lambdas returning StatusOr<T>.
  template <typename T, typename ReadElem>
  absl::StatusOr<std::vector<T>> ReadList(std::uint32_t max_count,
                                          ReadElem read_elem) {
    auto count = ReadCount(max_count);
    if (!count.ok()) return count.status();
    std::vector<T> items;
    items.reserve(*count);
    for (std::uint32_t i = 0; i < *count; ++i) {
      auto item = read_elem(*this);
      if (!item.ok()) return item.status();
      items.push_back(std::move(*item));
    }
    return items;
  }

  template <typename T, typename ReadElem>
  absl::StatusOr<std::optional<T>> ReadOptional(ReadElem read_elem) {
    auto present = ReadU8();
    if (!present.ok()) return present.status();
    if (*present == 0) return std::optional<T>{};
    if (*present != 1) {
      return MetaFailStopError("optional presence tag must be 0 or 1");
    }
    auto value = read_elem(*this);
    if (!value.ok()) return value.status();
    return std::optional<T>{std::move(*value)};
  }

 private:
  std::string_view data_;
  std::size_t pos_ = 0;
};

// Fixed-size byte arrays (request ids, hashes, ...): raw, no length prefix.
template <std::size_t N>
void WriteFixedArray(MetaWriter& w, const std::array<std::uint8_t, N>& a) {
  w.WriteRaw(std::string_view(reinterpret_cast<const char*>(a.data()), N));
}

template <std::size_t N>
absl::StatusOr<std::array<std::uint8_t, N>> ReadFixedArray(MetaReader& r) {
  auto raw = r.ReadRaw(N);
  if (!raw.ok()) return raw.status();
  std::array<std::uint8_t, N> out{};
  std::copy_n(raw->data(), N, reinterpret_cast<char*>(out.data()));
  return out;
}

}  // namespace keylane::meta
