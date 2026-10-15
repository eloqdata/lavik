/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/resp_version.h"

namespace lavik {

inline constexpr std::size_t kDefaultClientQueryBufferLimit =
    1ULL * 1024 * 1024 * 1024;
inline constexpr std::size_t kMinimumClientQueryBufferLimit =
    1ULL * 1024 * 1024;

struct RespCommand {
  std::vector<std::string> args_;
};

enum class RespParseState {
  kOk,
  kNeedMoreData,
  kError,
};

struct RespParseResult {
  RespParseState state_ = RespParseState::kNeedMoreData;
  std::size_t consumed_ = 0;
  absl::Status status_ = absl::OkStatus();
  RespCommand command_;
};

// Incremental RESP command parser. Bytes reported in consumed_ are retained in
// parser-owned state when a command is incomplete and may be discarded by the
// caller. One parser belongs to one connection.
class RespCommandParser {
 public:
  // The limit applies to the wire bytes retained while assembling one command.
  // Completed commands transfer ownership to the caller and no longer count
  // toward this parser-local guard.
  explicit RespCommandParser(
      std::size_t query_buffer_limit = kDefaultClientQueryBufferLimit) noexcept
      : query_buffer_limit_(query_buffer_limit) {}

  RespParseResult Parse(std::string_view input);

  // Suggests bounded additional body storage for a partially received large
  // bulk argument. Returns zero until payload arrives, while prepared storage
  // remains, or when the current query limit leaves no allowance. The caller
  // must admit this many request-buffer bytes before allocating them.
  [[nodiscard]] std::size_t BulkReadReservationSize() const noexcept;
  // Preallocates 1..BulkReadReservationSize() bytes in the final argument.
  // This does not consume input or account wire bytes; unused space stays
  // owned by the parser until filled or Reset. Invalid sizes leave it intact.
  absl::Status PrepareBulkRead(std::size_t bytes);
  // Returns the next bounded writable body window, excluding the terminator.
  // Commit bytes written here instead of passing them to Parse. The span is
  // invalidated by Parse, Reset, or another preparation; none may run while a
  // transport read owns it. The caller retains its admission across the read.
  // A lower query limit does not revoke this admitted span; Commit rechecks it.
  [[nodiscard]] std::span<char> BulkReadBuffer() noexcept;
  // Commits at most the exposed window's bytes and rechecks the latest query
  // limit. Partial/zero commits are allowed while a prepared window remains;
  // failure leaves logical parser state intact. Parse still consumes CRLF and
  // returns completed commands. It can also fill a partially used window.
  absl::Status CommitBulkRead(std::size_t bytes);

  void Reset();
  // Changes the guard without disturbing partially parsed state. If the new
  // limit is already below the retained command size, the next input byte is
  // rejected rather than abandoning bytes that the connection already owns.
  void SetQueryBufferLimit(std::size_t value) noexcept {
    query_buffer_limit_ = value;
  }
  [[nodiscard]] bool idle() const noexcept {
    return state_ == State::kArrayStart;
  }

 private:
  enum class State : std::uint8_t {
    kArrayStart,
    kInline,
    kArrayLength,
    kArgumentStart,
    kBulkLength,
    kBulkData,
    kBulkTerminator,
    kLineArgument,
  };

  RespParseResult Error(absl::Status status, std::size_t consumed);
  bool Account(std::size_t bytes);
  void ResetCommand();

  State state_ = State::kArrayStart;
  std::string length_text_;
  RespCommand command_;
  std::string current_argument_;
  std::size_t arguments_remaining_ = 0;
  std::size_t bulk_remaining_ = 0;
  // String size includes this prepared but unwritten tail. Received payload
  // ends before it; bulk_remaining_ includes it. Parse-only callers keep zero.
  std::size_t bulk_prepared_ = 0;
  std::size_t terminator_bytes_ = 0;
  std::size_t command_bytes_ = 0;
  std::size_t query_buffer_limit_ = kDefaultClientQueryBufferLimit;
  char argument_type_ = '$';
};

// Convenience wrapper for callers that already hold one complete contiguous
// request. Incremental network paths should retain a RespCommandParser.
RespParseResult ParseRespCommand(std::string_view input);

// Reuses one contiguous response buffer for the lifetime of a connection.
// A reply remains valid until Reset() is called for the next request.
class ReplyBuilder {
 public:
  explicit ReplyBuilder(RespVersion version = RespVersion::k2)
      : version_(version) {}

  void Reset();
  void Reserve(std::size_t capacity);
  void SetVersion(RespVersion version) noexcept { version_ = version; }
  [[nodiscard]] RespVersion version() const noexcept { return version_; }

  std::string_view AppendSimpleString(std::string_view value);
  std::string_view AppendBulkString(std::string_view value);
  std::string_view AppendNullBulkString();
  std::string_view AppendNullArray();
  // A protocol-semantic null. RESP2 represents it as a null bulk string,
  // while RESP3 has a dedicated null type.
  std::string_view AppendNull();
  std::string_view AppendInteger(long long value);
  std::string_view AppendBoolean(bool value);
  std::string_view AppendDouble(double value);
  // Emits an already formatted finite/inf/nan Redis double without parsing it
  // again. RESP2 represents the same semantic value as a bulk string.
  std::string_view AppendDoubleText(std::string_view value);
  std::string_view AppendBigNumber(std::string_view value);
  std::string_view AppendVerbatimString(std::string_view format,
                                        std::string_view value);
  std::string_view AppendError(std::string_view message);
  std::string_view AppendError(std::string_view prefix,
                               std::string_view message);
  std::string_view AppendArrayHeader(std::uint64_t count);
  // Map/set/push degrade to their RESP2 array representation. Map count is
  // the number of key-value pairs, not the number of encoded elements.
  std::string_view AppendMapHeader(std::uint64_t count);
  std::string_view AppendSetHeader(std::uint64_t count);
  std::string_view AppendPushHeader(std::uint64_t count);
  std::string_view AppendRaw(std::string_view encoded);

  [[nodiscard]] std::string_view View() const noexcept { return buffer_; }
  [[nodiscard]] std::size_t Capacity() const noexcept {
    return buffer_.capacity();
  }
  [[nodiscard]] std::string Release() && { return std::move(buffer_); }

 private:
  std::string buffer_;
  RespVersion version_ = RespVersion::k2;
};

std::string EncodeSimpleString(std::string_view value);
std::string EncodeBulkString(std::string_view value);
std::string EncodeNullBulkString();
std::string EncodeInteger(long long value);
std::string EncodeError(std::string_view message);

// Redis Cluster wire errors. The texts are verbatim Redis 7.2:
// cluster clients dispatch on the first token (MOVED/CROSSSLOT/CLUSTERDOWN/
// TRYAGAIN), and MOVED carries the slot plus the owning node's concrete
// "host:port". The TLS-vs-plain port choice belongs to the caller, which knows
// the requesting connection's TLS state (mirroring Redis getNodeClientPort).
std::string ClusterMovedMessage(std::uint16_t slot, std::string_view host,
                                std::uint16_t port);
inline constexpr std::string_view kClusterCrossSlotMessage =
    "CROSSSLOT Keys in request don't hash to the same slot";
// Redis's CLUSTERDOWN has several message variants; v1 has no global
// cluster-down state, so only the per-slot coverage-gap form is emitted
// (Redis's CLUSTER_DOWN_UNBOUND).
inline constexpr std::string_view kClusterDownUnboundMessage =
    "CLUSTERDOWN Hash slot not served";
std::string ClusterTryAgainMessage(std::string_view message);

std::string_view AppendMovedError(ReplyBuilder& builder, std::uint16_t slot,
                                  std::string_view host, std::uint16_t port);
std::string_view AppendCrossSlotError(ReplyBuilder& builder);
std::string_view AppendClusterDownUnboundError(ReplyBuilder& builder);
std::string_view AppendTryAgainError(ReplyBuilder& builder,
                                     std::string_view message);
std::string_view EncodeScanReply(ReplyBuilder& builder, std::uint64_t cursor,
                                 const std::vector<std::string>& keys);
std::string EncodeScanReply(std::uint64_t cursor,
                            const std::vector<std::string>& keys);

}  // namespace lavik
