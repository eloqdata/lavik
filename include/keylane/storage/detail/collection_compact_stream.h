#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/storage/collection_page.h"

namespace keylane::storage {

inline constexpr std::size_t kCollectionStreamPageBytes = 8 * 1024;

// Streams the existing compact wire image, not the durable grouped layout.
// Aggregate lengths are uint64_t, while the unchanged wire count is uint32_t.
// No serialized whole-value or whole-page buffer is allocated. Pages must be
// supplied in logical order, including empty Hash/Set routing pages if desired.
class CollectionCompactEncoder {
 public:
  static absl::StatusOr<CollectionCompactEncoder> Create(
      ValueType type, std::uint64_t total_count,
      std::uint64_t total_encoded_bytes);

  // Validates a page and measures only its entries, excluding the single
  // collection header. Add 32 bytes for Hash/Set, or 8 for List/Sorted Set.
  static absl::StatusOr<std::uint64_t> MeasurePage(const CollectionPage& page);

  // Borrows the page; keep it immutable and alive until page_done() and until
  // the last returned borrowed span has been consumed. Rejects a second page
  // while the previous one is active. Cursor/done DTO fields are not wire data.
  absl::Status StartPage(const CollectionPage& page);

  // A nullopt means there is currently no more output; StartPage may resume it.
  // Empty spans are valid. Framing spans survive only until the next call or a
  // move/destruction; string spans borrow the supplied page. The header may be
  // drained before supplying the first page. Call Finish at the complete EOF.
  std::optional<std::string_view> Next() noexcept;
  bool page_done() const noexcept { return page_ == nullptr; }
  absl::Status Finish() const;

 private:
  CollectionCompactEncoder() = default;
  ValueType type_ = ValueType::kNone;
  std::uint64_t total_count_ = 0;
  std::uint64_t total_bytes_ = 0;
  std::uint64_t supplied_count_ = 0;
  std::uint64_t supplied_bytes_ = 0;
  std::uint64_t emitted_bytes_ = 0;
  std::array<char, 32> header_{};
  std::array<char, 12> framing_{};
  std::size_t header_bytes_ = 0;
  bool header_pending_ = true;
  const CollectionPage* page_ = nullptr;
  std::size_t entry_ = 0;
  unsigned phase_ = 0;
};

// Incremental, fail-closed compact reader. Expected count and byte length come
// from the enclosing transport metadata; the compact header must agree. At
// most one roughly 8 KiB page plus one indivisible large entry is assembled.
// No allocation is sized from the aggregate count/length. The caller may await
// admission/storage between TakePage and the next Consume call.
class CollectionCompactDecoder {
 public:
  CollectionCompactDecoder(const CollectionCompactDecoder&) = delete;
  CollectionCompactDecoder& operator=(const CollectionCompactDecoder&) = delete;
  CollectionCompactDecoder(CollectionCompactDecoder&&) noexcept = default;
  CollectionCompactDecoder& operator=(CollectionCompactDecoder&&) noexcept =
      default;
  using Admission = std::function<absl::Status(std::size_t)>;
  static absl::StatusOr<CollectionCompactDecoder> Create(
      ValueType type, std::uint64_t total_count,
      std::uint64_t total_encoded_bytes, Admission admission = {});

  // Stops at a ready page, possibly leaving an unconsumed input suffix. While
  // page_ready() is true this consumes zero bytes. After TakePage, pass exactly
  // that suffix again. All malformed-input/OOM failures poison this decoder.
  absl::StatusOr<std::size_t> Consume(std::string_view input);
  bool page_ready() const noexcept { return ready_; }
  // Optional admission runs BEFORE every string allocation/vector growth. Its
  // successful reservations belong to the caller, not this codec. TakePage
  // transfers that page's cumulative reservation (including conservative
  // vector growth peaks); release it only after destroying the returned page.
  // On error/destruction release pending_admitted_bytes() after the decoder's
  // buffers die. An admission failure must leave its external charge unchanged.
  // Supplying admission requires consuming the non-null transfer output.
  absl::StatusOr<CollectionPage> TakePage(
      std::size_t* transferred_admission = nullptr);
  std::size_t pending_admitted_bytes() const noexcept {
    return admission_count_.bytes_;
  }

  // Validates exact bytes/count and no partial header/entry at transport EOF.
  // A final ready page may still be taken after successful Finish. No trailing
  // bytes may subsequently be supplied. A truncated EOF poisons the decoder.
  absl::Status Finish();

 private:
  CollectionCompactDecoder() = default;
  // Moving transfers the external admission receipt, never copies it. Before
  // overwriting a decoder, its owner releases the old pending receipt after
  // its buffers die, just as it must when destroying that decoder.
  struct AdmissionCount {
    AdmissionCount() = default;
    AdmissionCount(AdmissionCount&& other) noexcept
        : bytes_(std::exchange(other.bytes_, 0)) {}
    AdmissionCount& operator=(AdmissionCount&& other) noexcept {
      bytes_ = std::exchange(other.bytes_, 0);
      return *this;
    }
    std::size_t bytes_ = 0;
  };
  enum class Stage { kHeader, kEntryHeader, kFirst, kSecond, kDone };
  absl::Status ReadHeader();
  absl::Status ReadEntryHeader();
  absl::Status CompleteEntry();
  absl::Status Admit(std::size_t bytes);
  absl::Status Fail(absl::Status status);

  ValueType type_ = ValueType::kNone;
  std::uint64_t total_count_ = 0;
  std::uint64_t total_bytes_ = 0;
  std::uint64_t consumed_bytes_ = 0;
  std::uint64_t parsed_count_ = 0;
  std::uint64_t page_bytes_ = 0;
  std::uint64_t next_cursor_ = 0;
  std::uint64_t entry_bytes_ = 0;
  std::size_t first_bytes_ = 0;
  std::size_t second_bytes_ = 0;
  std::size_t first_used_ = 0;
  std::size_t second_used_ = 0;
  double score_ = 0;
  std::array<char, 32> framing_{};
  std::size_t framing_used_ = 0;
  std::string first_;
  std::string second_;
  CollectionPage page_;
  Stage stage_ = Stage::kHeader;
  bool ready_ = false;
  absl::Status status_;
  Admission admission_;
  AdmissionCount admission_count_;
};

}  // namespace keylane::storage
