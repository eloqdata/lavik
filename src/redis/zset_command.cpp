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

#include "zset_command.h"

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <map>
#include <numbers>
#include <numeric>
#include <optional>
#include <random>

#include "absl/strings/str_cat.h"
#include "blocking_wait.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "cluster_gate.h"
#include "lavik/command_table.h"
#include "lavik/glob.h"
#include "lavik/memory.h"
#include "lavik/random_sample.h"
#include "lavik/redis_parse.h"
#include "lavik/resp.h"
#include "lavik/storage/detail/ordered_compact_codec.h"
#include "lavik/tx/transaction.h"

namespace lavik {
namespace {

constexpr std::string_view kMagic = "LZS1";
constexpr double kGeoMinLat = -85.05112878;
constexpr double kGeoMaxLat = 85.05112878;
constexpr double kEarthRadiusMeters = 6372797.560856;

storage::StorageEngine* g_storage = nullptr;

struct Element {
  std::string member_;
  double score_ = 0;
};

using ZSet = std::vector<Element>;

CommandReply Built(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

bool EqualCi(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(a[i]);
    if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    if (c != static_cast<unsigned char>(b[i])) return false;
  }
  return true;
}

template <class T>
bool ParseInt(std::string_view input, T* value) {
  const auto parsed =
      std::from_chars(input.data(), input.data() + input.size(), *value);
  return parsed.ec == std::errc{} && parsed.ptr == input.data() + input.size();
}

std::string FormatDouble(double value) {
  if (value == 0) return std::signbit(value) ? "-0" : "0";
  if (std::isnan(value)) return "nan";
  if (std::isinf(value)) return value < 0 ? "-inf" : "inf";
  char buffer[128];
  constexpr double kSafeIntegerLimit =
      static_cast<double>(std::numeric_limits<std::int64_t>::max() / 2);
  if (std::isfinite(value) && value >= -kSafeIntegerLimit &&
      value <= kSafeIntegerLimit && std::trunc(value) == value) {
    const auto formatted = std::to_chars(buffer, buffer + sizeof(buffer),
                                         static_cast<std::int64_t>(value));
    return formatted.ec == std::errc{} ? std::string(buffer, formatted.ptr)
                                       : std::string("0");
  }

  // Redis 7.2's fpconv_dtoa and std::to_chars agree on the shortest
  // significant digits, but not on when to select fixed notation or how to
  // spell the exponent. Normalize the shortest representation through
  // fpconv's emit_digits rules so replies are byte-for-byte compatible.
  const auto formatted = std::to_chars(buffer, buffer + sizeof(buffer), value);
  if (formatted.ec != std::errc{}) return "0";
  std::string_view shortest(buffer, formatted.ptr);
  const bool negative = shortest.starts_with('-');
  if (negative) shortest.remove_prefix(1);

  const std::size_t exponent_at = shortest.find_first_of("eE");
  const std::string_view mantissa = shortest.substr(0, exponent_at);
  int explicit_exponent = 0;
  if (exponent_at != std::string_view::npos) {
    std::string_view exponent = shortest.substr(exponent_at + 1);
    bool exponent_negative = false;
    if (!exponent.empty() &&
        (exponent.front() == '+' || exponent.front() == '-')) {
      exponent_negative = exponent.front() == '-';
      exponent.remove_prefix(1);
    }
    const auto parsed = std::from_chars(
        exponent.data(), exponent.data() + exponent.size(), explicit_exponent);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != exponent.data() + exponent.size()) {
      return "0";
    }
    if (exponent_negative) explicit_exponent = -explicit_exponent;
  }

  const std::size_t decimal_at = mantissa.find('.');
  const std::size_t decimal_position =
      decimal_at == std::string_view::npos ? mantissa.size() : decimal_at;
  std::string digits;
  digits.reserve(mantissa.size());
  for (char digit : mantissa) {
    if (digit != '.') digits.push_back(digit);
  }
  const std::size_t leading = digits.find_first_not_of('0');
  if (leading == std::string::npos) return negative ? "-0" : "0";
  digits.erase(0, leading);
  const std::int64_t significant_decimal_position =
      static_cast<std::int64_t>(decimal_position) -
      static_cast<std::int64_t>(leading);
  int k = explicit_exponent + static_cast<int>(significant_decimal_position) -
          static_cast<int>(digits.size());
  while (digits.size() > 1 && digits.back() == '0') {
    digits.pop_back();
    ++k;
  }

  const int ndigits = static_cast<int>(digits.size());
  const int scientific_exponent = k + ndigits - 1;
  const int exponent_magnitude = std::abs(scientific_exponent);
  std::string output;
  output.reserve(32);
  if (negative) output.push_back('-');
  if (k >= 0 && exponent_magnitude < ndigits + 7) {
    output.append(digits);
    output.append(static_cast<std::size_t>(k), '0');
    return output;
  }
  if (k < 0 && (k > -7 || exponent_magnitude < 4)) {
    const int offset = ndigits - std::abs(k);
    if (offset <= 0) {
      output.append("0.");
      output.append(static_cast<std::size_t>(-offset), '0');
      output.append(digits);
    } else {
      output.append(digits.substr(0, static_cast<std::size_t>(offset)));
      output.push_back('.');
      output.append(digits.substr(static_cast<std::size_t>(offset)));
    }
    return output;
  }
  output.push_back(digits.front());
  if (digits.size() > 1) {
    output.push_back('.');
    output.append(digits.substr(1));
  }
  output.push_back('e');
  output.push_back(scientific_exponent < 0 ? '-' : '+');
  output.append(std::to_string(exponent_magnitude));
  return output;
}

void AppendScore(ReplyBuilder& builder, double score) {
  builder.AppendDoubleText(FormatDouble(score));
}

void AppendScore(ReplyBuilder& builder, std::string_view score) {
  builder.AppendDoubleText(score);
}

void AppendMemberScore(ReplyBuilder& builder, std::string_view member,
                       double score, bool nested_in_resp3 = true) {
  if (nested_in_resp3 && builder.version() == RespVersion::k3)
    builder.AppendArrayHeader(2);
  builder.AppendBulkString(member);
  AppendScore(builder, score);
}

std::string FormatGeoCoordinate(double value) {
  char buffer[128];
  const int formatted = std::snprintf(buffer, sizeof(buffer), "%.17Lf",
                                      static_cast<long double>(value));
  if (formatted <= 0 || static_cast<std::size_t>(formatted) >= sizeof(buffer)) {
    return "0";
  }
  std::size_t length = static_cast<std::size_t>(formatted);
  while (length > 0 && buffer[length - 1] == '0') --length;
  if (length > 0 && buffer[length - 1] == '.') --length;
  if (length == 2 && buffer[0] == '-' && buffer[1] == '0') return "0";
  return std::string(buffer, length);
}

std::string FormatGeoDistance(double value) {
  if (value == 0) value = 0;
  char buffer[128];
  const int length = std::snprintf(buffer, sizeof(buffer), "%.4f", value);
  return length > 0 && static_cast<std::size_t>(length) < sizeof(buffer)
             ? std::string(buffer, static_cast<std::size_t>(length))
             : std::string("0.0000");
}

bool ValidGeoCoordinates(double lon, double lat) {
  return std::isfinite(lon) && std::isfinite(lat) && lon >= -180 &&
         lon <= 180 && lat >= kGeoMinLat && lat <= kGeoMaxLat;
}

std::optional<std::uint64_t> DecodeGeoScore(double score) {
  if (!std::isfinite(score)) return std::nullopt;
  // Redis converts any finite Sorted Set score to uint64_t and consumes the
  // low 52 geohash bits. Define the wrap explicitly so negative and
  // fractional scores created through ZADD remain decodable without relying
  // on implementation-defined floating-to-integer overflow behavior.
  constexpr long double kGeoModulus =
      static_cast<long double>(std::uint64_t{1} << 52);
  long double wrapped =
      std::fmod(std::trunc(static_cast<long double>(score)), kGeoModulus);
  if (wrapped < 0) wrapped += kGeoModulus;
  return static_cast<std::uint64_t>(wrapped);
}

void Put32(std::string* out, std::uint32_t value) {
  for (unsigned i = 0; i != 4; ++i)
    out->push_back(static_cast<char>(value >> (i * 8)));
}

void Put64(std::string* out, std::uint64_t value) {
  for (unsigned i = 0; i != 8; ++i)
    out->push_back(static_cast<char>(value >> (i * 8)));
}

bool Get32(std::string_view in, std::size_t* offset, std::uint32_t* value) {
  if (*offset > in.size() || in.size() - *offset < 4) return false;
  *value = 0;
  for (unsigned i = 0; i != 4; ++i) {
    *value |=
        static_cast<std::uint32_t>(static_cast<unsigned char>(in[*offset + i]))
        << (i * 8);
  }
  *offset += 4;
  return true;
}

bool Get64(std::string_view in, std::size_t* offset, std::uint64_t* value) {
  if (*offset > in.size() || in.size() - *offset < 8) return false;
  *value = 0;
  for (unsigned i = 0; i != 8; ++i) {
    *value |=
        static_cast<std::uint64_t>(static_cast<unsigned char>(in[*offset + i]))
        << (i * 8);
  }
  *offset += 8;
  return true;
}

void Sort(ZSet* set) {
  std::sort(set->begin(), set->end(), [](const Element& a, const Element& b) {
    return a.score_ < b.score_ ||
           (a.score_ == b.score_ && a.member_ < b.member_);
  });
}

absl::StatusOr<ZSet> Decode(
    const std::optional<storage::CompactValueView>& value) {
  if (!value.has_value()) return ZSet{};
  const std::string_view in = value->encoded_;
  if (!in.starts_with(kMagic))
    return absl::InternalError("invalid persisted Sorted Set");
  std::size_t offset = kMagic.size();
  std::uint32_t count = 0;
  if (!Get32(in, &offset, &count) || count != value->logical_size_ ||
      count > (in.size() - offset) / 12) {
    return absl::InternalError("invalid persisted Sorted Set cardinality");
  }
  ZSet set;
  set.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint64_t bits = 0;
    std::uint32_t size = 0;
    if (!Get64(in, &offset, &bits) || !Get32(in, &offset, &size) ||
        offset > in.size() || size > storage::kMaxStringBytes ||
        size > in.size() - offset) {
      return absl::InternalError("truncated persisted Sorted Set");
    }
    const double score = std::bit_cast<double>(bits);
    if (std::isnan(score))
      return absl::InternalError("NaN persisted Sorted Set score");
    set.push_back({std::string(in.substr(offset, size)), score});
    offset += size;
  }
  if (offset != in.size())
    return absl::InternalError("trailing persisted Sorted Set bytes");
  Sort(&set);
  return set;
}

absl::StatusOr<std::string> Encode(const ZSet& set) {
  if (set.size() > std::numeric_limits<std::uint32_t>::max())
    return absl::OutOfRangeError("Sorted Set cardinality is too large");
  std::string out;
  std::size_t bytes = 8;
  for (const auto& element : set) {
    auto next = storage::AppendOrderedEntrySize(
        storage::OrderedCollectionKind::kSortedSet, bytes,
        element.member_.size(), out.max_size());
    if (!next.ok()) return next.status();
    bytes = *next;
  }
  out.reserve(bytes);
  out.append(kMagic);
  Put32(&out, static_cast<std::uint32_t>(set.size()));
  for (const auto& element : set) {
    Put64(&out, std::bit_cast<std::uint64_t>(element.score_));
    Put32(&out, static_cast<std::uint32_t>(element.member_.size()));
    out.append(element.member_);
  }
  return out;
}

Element* Find(ZSet* set, std::string_view member) {
  auto found = std::find_if(set->begin(), set->end(), [&](const Element& e) {
    return e.member_ == member;
  });
  return found == set->end() ? nullptr : &*found;
}

const Element* Find(const ZSet& set, std::string_view member) {
  return Find(const_cast<ZSet*>(&set), member);
}

struct ScoreBound {
  double value_ = 0;
  bool exclusive_ = false;
};

absl::StatusOr<ScoreBound> ParseScoreBound(std::string_view text) {
  ScoreBound bound;
  if (!text.empty() && text.front() == '(') {
    bound.exclusive_ = true;
    text.remove_prefix(1);
  }
  if (EqualCi(text, "+inf"))
    bound.value_ = INFINITY;
  else if (EqualCi(text, "-inf"))
    bound.value_ = -INFINITY;
  else if (!ParseRedisDouble(text, &bound.value_, true))
    return absl::InvalidArgumentError("min or max is not a float");
  return bound;
}

bool AboveMin(double score, ScoreBound bound) {
  return bound.exclusive_ ? score > bound.value_ : score >= bound.value_;
}
bool BelowMax(double score, ScoreBound bound) {
  return bound.exclusive_ ? score < bound.value_ : score <= bound.value_;
}

struct LexBound {
  std::string_view value_;
  int infinity_ = 0;
  bool exclusive_ = false;
};

absl::StatusOr<LexBound> ParseLexBound(std::string_view text) {
  if (text == "-") return LexBound{.value_ = {}, .infinity_ = -1};
  if (text == "+") return LexBound{.value_ = {}, .infinity_ = 1};
  if (text.size() < 1 || (text.front() != '(' && text.front() != '['))
    return absl::InvalidArgumentError("min or max not valid string range item");
  return LexBound{.value_ = text.substr(1), .exclusive_ = text.front() == '('};
}

bool AboveMin(std::string_view value, LexBound bound) {
  if (bound.infinity_ < 0) return true;
  if (bound.infinity_ > 0) return false;
  return bound.exclusive_ ? value > bound.value_ : value >= bound.value_;
}
bool BelowMax(std::string_view value, LexBound bound) {
  if (bound.infinity_ > 0) return true;
  if (bound.infinity_ < 0) return false;
  return bound.exclusive_ ? value < bound.value_ : value <= bound.value_;
}

storage::CompactValueUpdate NoChange() { return {}; }

absl::StatusOr<storage::CompactValueUpdate> ChangedSorted(ZSet set) {
  if (set.empty())
    return storage::CompactValueUpdate{.changed_ = true,
                                       .erase_ = true,
                                       .encoded_ = {},
                                       .logical_size_ = 0,
                                       .expire_at_ms_ = std::nullopt};
  auto encoded = Encode(set);
  if (!encoded.ok()) return encoded.status();
  return storage::CompactValueUpdate{.changed_ = true,
                                     .encoded_ = std::move(*encoded),
                                     .logical_size_ = set.size(),
                                     .expire_at_ms_ = std::nullopt};
}

absl::StatusOr<storage::CompactValueUpdate> Changed(ZSet set) {
  Sort(&set);
  return ChangedSorted(std::move(set));
}

std::string_view StorageError(ReplyBuilder& builder,
                              const absl::Status& status) {
  if (status.code() == absl::StatusCode::kResourceExhausted &&
      status.message().starts_with("OOM ")) {
    return builder.AppendError(status.message());
  }
  return status.message().starts_with("WRONGTYPE ")
             ? builder.AppendError(status.message())
             : builder.AppendError("ERR " + std::string(status.message()));
}

Task<absl::Status> RunCompact(const CommandRequest& request,
                              const storage::Digest* digest,
                              storage::TxShardWrites* tx, bool read_only,
                              const storage::CompactValueCallback& callback) {
  const std::string_view key = request.args_[1];
  auto replication = tx == nullptr && !read_only
                         ? PrepareReplicationCommand(request)
                         : std::nullopt;
  if (digest == nullptr) {
    const storage::MutationPrecondition mutation_precondition =
        ClusterMutationPrecondition(request);
    co_return co_await g_storage->ExecuteCompact(
        request.db_id_, key, storage::ValueType::kSortedSet, read_only,
        callback, 0, replication ? &*replication : nullptr,
        &mutation_precondition);
  }
  co_return co_await g_storage->ExecuteCompactLocked(
      request.db_id_, key, *digest, storage::ValueType::kSortedSet, read_only,
      callback, tx, 0, replication ? &*replication : nullptr);
}

Task<absl::StatusOr<storage::SortedSetResult>> RunSortedSet(
    const CommandRequest& request, const storage::Digest* digest,
    storage::TxShardWrites* tx, const storage::SortedSetOperation& operation) {
  const bool read_only =
      operation.kind_ != storage::SortedSetOperationKind::kAdd &&
      operation.kind_ != storage::SortedSetOperationKind::kRemove &&
      operation.kind_ != storage::SortedSetOperationKind::kPop;
  auto replication = tx == nullptr && !read_only
                         ? PrepareReplicationCommand(request)
                         : std::nullopt;
  if (digest == nullptr) {
    const storage::MutationPrecondition mutation_precondition =
        ClusterMutationPrecondition(request);
    co_return co_await g_storage->ExecuteSortedSet(
        request.db_id_, request.args_[1], operation,
        replication ? &*replication : nullptr, &mutation_precondition);
  }
  co_return co_await g_storage->ExecuteSortedSetLocked(
      request.db_id_, request.args_[1], *digest, operation, tx,
      replication ? &*replication : nullptr);
}

struct MultiPopShape {
  std::vector<std::size_t> key_args_;
  bool maximum_ = false;
  bool flat_reply_ = false;
  std::uint64_t count_ = 1;
};

std::vector<std::string> CanonicalSelectedZSetPop(std::string_view key,
                                                  bool maximum,
                                                  std::size_t count) {
  return {maximum ? "ZPOPMAX" : "ZPOPMIN", std::string(key),
          std::to_string(count)};
}

absl::StatusOr<MultiPopShape> ParseMultiPopShape(
    const CommandRequest& request) {
  const auto& args = request.args_;
  MultiPopShape shape;
  if (request.kind_ == CommandKind::kBZPopMin ||
      request.kind_ == CommandKind::kBZPopMax) {
    if (args.size() < 3) return absl::InvalidArgumentError("syntax error");
    shape.maximum_ = request.kind_ == CommandKind::kBZPopMax;
    shape.flat_reply_ = true;
    for (std::size_t i = 1; i + 1 < args.size(); ++i) {
      shape.key_args_.push_back(i);
    }
    return shape;
  }

  const std::size_t count_arg = request.kind_ == CommandKind::kBZMPop ? 2 : 1;
  const std::size_t first_key = count_arg + 1;
  std::int64_t parsed_keys = 0;
  if (count_arg >= args.size() ||
      !ParseRedisInt64(args[count_arg], &parsed_keys) || parsed_keys <= 0) {
    return absl::InvalidArgumentError("numkeys should be greater than 0");
  }
  const std::uint64_t key_count = static_cast<std::uint64_t>(parsed_keys);
  if (key_count > args.size() - std::min(first_key, args.size()) ||
      first_key + key_count >= args.size()) {
    return absl::InvalidArgumentError("syntax error");
  }
  for (std::uint64_t i = 0; i < key_count; ++i) {
    shape.key_args_.push_back(first_key + static_cast<std::size_t>(i));
  }
  const std::size_t direction = first_key + static_cast<std::size_t>(key_count);
  if (EqualCi(args[direction], "min")) {
    shape.maximum_ = false;
  } else if (EqualCi(args[direction], "max")) {
    shape.maximum_ = true;
  } else {
    return absl::InvalidArgumentError("syntax error");
  }
  const std::size_t trailing = args.size() - direction - 1;
  if (trailing != 0) {
    if (trailing != 2 || !EqualCi(args[direction + 1], "count")) {
      return absl::InvalidArgumentError("syntax error");
    }
    std::int64_t parsed_count = 0;
    if (!ParseRedisInt64(args[direction + 2], &parsed_count) ||
        parsed_count <= 0) {
      return absl::InvalidArgumentError("count should be greater than 0");
    }
    shape.count_ = static_cast<std::uint64_t>(parsed_count);
  }
  return shape;
}

absl::StatusOr<std::optional<std::chrono::steady_clock::time_point>>
ParseBlockingZSetDeadline(const CommandRequest& request) {
  const std::size_t timeout_arg =
      request.kind_ == CommandKind::kBZMPop ? 1 : request.args_.size() - 1;
  double timeout_seconds = 0;
  if (!ParseRedisDouble(request.args_[timeout_arg], &timeout_seconds)) {
    return absl::InvalidArgumentError("timeout is not a float or out of range");
  }
  if (timeout_seconds < 0) {
    return absl::InvalidArgumentError("timeout is negative");
  }
  return BlockingDeadlineFromSeconds(timeout_seconds);
}

Task<absl::StatusOr<storage::SortedSetResult>> PopZSetLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest,
    bool maximum, std::uint64_t count, storage::TxShardWrites* tx = nullptr,
    const CommandRequest* request = nullptr) {
  const storage::MutationPrecondition mutation_precondition =
      tx == nullptr && request != nullptr
          ? ClusterMutationPrecondition(*request)
          : storage::MutationPrecondition{};
  const storage::MutationPrecondition* mutation_precondition_ptr =
      tx == nullptr && request != nullptr ? &mutation_precondition : nullptr;
  auto popped = co_await g_storage->ExecuteSortedSetLocked(
      db_id, key, digest,
      storage::SortedSetOperation{
          .kind_ = storage::SortedSetOperationKind::kPop,
          .reverse_ = maximum,
          .pop_count_ = count},
      tx, nullptr, mutation_precondition_ptr);
  if (!popped.ok()) co_return popped.status();
  if (!popped->members_.empty() && popped->length_ != 0) {
    if (request != nullptr) {
      NotifyZSetBlockingKey(*request, key);
    } else {
      NotifyZSetBlockingKey(db_id, key);
    }
  }
  co_return popped;
}

Task<absl::Status> ZSetHoldCallback(void*, const tx::ShardSlice&) {
  co_return absl::OkStatus();
}

struct SingleShardPopContext {
  const CommandRequest* request_ = nullptr;
  const MultiPopShape* shape_ = nullptr;
  std::size_t selected_arg_ = 0;
  storage::SortedSetResult popped_;
};

Task<absl::Status> SingleShardPopCallback(void* opaque,
                                          const tx::ShardSlice& slice) {
  auto* context = static_cast<SingleShardPopContext*>(opaque);
  for (std::size_t argument : context->shape_->key_args_) {
    const tx::TxKey* locked = nullptr;
    for (const tx::TxKey& key : slice.keys_) {
      if (key.arg_index_ == argument) {
        locked = &key;
        break;
      }
    }
    if (locked == nullptr) {
      co_return absl::InternalError("Sorted Set pop key routing is incomplete");
    }
    auto popped = co_await PopZSetLocked(
        context->request_->db_id_, context->request_->args_[argument],
        locked->digest_, context->shape_->maximum_, context->shape_->count_,
        nullptr, context->request_);
    if (!popped.ok()) co_return popped.status();
    if (!popped->members_.empty()) {
      context->selected_arg_ = argument;
      context->popped_ = std::move(*popped);
      break;
    }
  }
  co_return absl::OkStatus();
}

void AppendMultiPopReply(ReplyBuilder& builder, std::string_view key,
                         const storage::SortedSetResult& result,
                         bool flat_reply) {
  const auto& popped = result.members_;
  if (flat_reply) {
    builder.AppendArrayHeader(3);
    builder.AppendBulkString(key);
    builder.AppendBulkString(popped.front().member_);
    AppendScore(builder, popped.front().score_);
    return;
  }
  builder.AppendArrayHeader(2);
  builder.AppendBulkString(key);
  builder.AppendArrayHeader(popped.size());
  for (const auto& element : popped) {
    builder.AppendArrayHeader(2);
    builder.AppendBulkString(element.member_);
    AppendScore(builder, element.score_);
  }
}

Task<CommandReply> ExecuteZSetMultiPopAttempt(const CommandRequest& request,
                                              ReplyBuilder& builder,
                                              bool* empty = nullptr) {
  if (empty != nullptr) *empty = false;
  auto parsed = ParseMultiPopShape(request);
  if (!parsed.ok()) {
    co_return Built(
        builder.AppendError(absl::StrCat("ERR ", parsed.status().message())));
  }
  const MultiPopShape shape = std::move(*parsed);
  struct SnapshotAttemptGuard {
    bool snapshot_active_ = false;
    bool order_active_ = false;
    ~SnapshotAttemptGuard() {
      if (snapshot_active_) EndSnapshotTransaction();
      if (order_active_) EndReplicationTransactionOrder();
    }
  } snapshot_attempt;
  if (!request.replication_origin_ && request.spec_ != nullptr &&
      (request.spec_->flags_ & kCmdMayBlock) != 0 &&
      g_storage->ReplicationLogActive()) {
    while (!TryBeginReplicationTransactionOrder()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return Built(StorageError(builder, waited));
    }
    snapshot_attempt.order_active_ = true;
    while (!TryBeginSnapshotTransaction()) {
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return Built(StorageError(builder, waited));
    }
    snapshot_attempt.snapshot_active_ = true;
  }
  tx::Transaction transaction;
  for (std::size_t argument : shape.key_args_) {
    transaction.AddKey(
        g_storage->OwnerForKey(request.args_[argument]), request.db_id_,
        storage::ComputeDigest(request.args_[argument]),
        static_cast<std::uint32_t>(argument), tx::LockMode::kExclusive);
  }
  transaction.Seal();
  // Cluster owner-side re-check: pops mutate, so the validator
  // gates the single-shard pop callback and the multi-shard hold hop.
  ClusterShardValidatorContext cluster_validator;
  InstallClusterShardValidator(transaction, request, cluster_validator);
  ReplicationTransactionGuard replication(request, &transaction);
  if (!replication.status().ok()) {
    co_return Built(StorageError(builder, replication.status()));
  }
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) co_return Built(StorageError(builder, status));
  if (transaction.single_shard()) {
    SingleShardPopContext context{
        .request_ = &request,
        .shape_ = &shape,
        .selected_arg_ = 0,
        .popped_ = {},
    };
    status =
        co_await transaction.Execute(&SingleShardPopCallback, &context, true);
    if (!status.ok()) {
      if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
        co_return ClusterValidatorFailureReply(
            transaction, cluster_validator, request.connection_tls_, builder);
      }
      co_return Built(StorageError(builder, status));
    }
    if (context.popped_.members_.empty()) {
      if (empty != nullptr) *empty = true;
      co_return Built(builder.AppendNullArray());
    }
    replication.SetCommandArgs(CanonicalSelectedZSetPop(
        request.args_[context.selected_arg_], shape.maximum_,
        context.popped_.members_.size()));
    replication.Commit();
    AppendMultiPopReply(builder, request.args_[context.selected_arg_],
                        context.popped_, shape.flat_reply_);
    co_return Built(builder.View());
  }
  status = co_await transaction.Execute(&ZSetHoldCallback, nullptr, false);
  if (!status.ok()) {
    if (cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      // The hold hop retains its locks on failure; drop them before
      // answering. Nothing mutated: the callback never ran.
      (void)co_await transaction.Release();
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_, builder);
    }
    co_return Built(StorageError(builder, status));
  }
  // The mutations below run in bare SubmitTaskTo hops the transaction hook
  // cannot see; the hook's job here was to gate entry into the mutation
  // phase. Clear it so the settle hops cannot be fenced off retroactively.
  // (Unreachable in cluster mode: admission allows one slot, hence one shard.)
  transaction.SetShardValidator(nullptr, nullptr);

  for (std::size_t argument : shape.key_args_) {
    const std::string& key = request.args_[argument];
    const storage::Digest digest = storage::ComputeDigest(key);
    auto popped = co_await bycorf::SubmitTaskTo(
        g_storage->OwnerForKey(key),
        [db = request.db_id_, key = std::string(key), digest,
         maximum = shape.maximum_, count = shape.count_,
         request_ptr = &request]() {
          return PopZSetLocked(db, key, digest, maximum, count, nullptr,
                               request_ptr);
        });
    if (!popped.ok()) {
      (void)co_await transaction.Execute(&ZSetHoldCallback, nullptr, true);
      co_return Built(StorageError(builder, popped.status()));
    }
    if (!popped->members_.empty()) {
      status = co_await transaction.Execute(&ZSetHoldCallback, nullptr, true);
      if (!status.ok()) co_return Built(StorageError(builder, status));
      replication.SetCommandArgs(CanonicalSelectedZSetPop(
          key, shape.maximum_, popped->members_.size()));
      replication.Commit();
      AppendMultiPopReply(builder, key, *popped, shape.flat_reply_);
      co_return Built(builder.View());
    }
  }
  (void)co_await transaction.Execute(&ZSetHoldCallback, nullptr, true);
  if (empty != nullptr) *empty = true;
  co_return Built(builder.AppendNullArray());
}

std::uint64_t Interleave26(std::uint32_t lon, std::uint32_t lat) {
  std::uint64_t result = 0;
  for (int bit = 25; bit >= 0; --bit) {
    result = (result << 1) | ((lon >> bit) & 1);
    result = (result << 1) | ((lat >> bit) & 1);
  }
  return result;
}

std::uint64_t GeoScore(double lon, double lat) {
  constexpr double scale = static_cast<double>(std::uint64_t{1} << 26);
  auto normalized = [scale](double value, double minimum, double maximum) {
    double n = (value - minimum) / (maximum - minimum);
    n = std::clamp(n, 0.0, std::nextafter(1.0, 0.0));
    return static_cast<std::uint32_t>(n * scale);
  };
  return Interleave26(normalized(lon, -180, 180),
                      normalized(lat, kGeoMinLat, kGeoMaxLat));
}

std::pair<double, double> GeoDecode(std::uint64_t hash) {
  std::uint32_t lon_bits = 0;
  std::uint32_t lat_bits = 0;
  for (unsigned i = 0; i < 26; ++i) {
    lon_bits = (lon_bits << 1) | ((hash >> (51 - i * 2)) & 1);
    lat_bits = (lat_bits << 1) | ((hash >> (50 - i * 2)) & 1);
  }
  constexpr double cells = static_cast<double>(std::uint64_t{1} << 26);
  return {-180 + (static_cast<double>(lon_bits) + .5) * 360 / cells,
          kGeoMinLat + (static_cast<double>(lat_bits) + .5) *
                           (kGeoMaxLat - kGeoMinLat) / cells};
}

std::string GeoHashString(double lon, double lat) {
  static constexpr char alphabet[] = "0123456789bcdefghjkmnpqrstuvwxyz";
  constexpr double scale = static_cast<double>(std::uint64_t{1} << 26);
  auto normalized = [scale](double value, double minimum, double maximum) {
    double n = (value - minimum) / (maximum - minimum);
    n = std::clamp(n, 0.0, std::nextafter(1.0, 0.0));
    return static_cast<std::uint32_t>(n * scale);
  };
  // GEO scores use the Mercator latitude interval. GEOHASH replies are
  // standard geohashes and therefore use the full [-90, 90] latitude range.
  const std::uint64_t hash =
      Interleave26(normalized(lon, -180, 180), normalized(lat, -90, 90));
  // Redis emits the 52 interleaved bits as an 11-character geohash, padded
  // with three zero low bits.
  std::uint64_t bits = hash << 3;
  std::string out(11, '0');
  for (int i = 10; i >= 0; --i) {
    out[i] = alphabet[bits & 31];
    bits >>= 5;
  }
  // Redis exposes ten complete base32 digits from the 52-bit GEO score and
  // pads the eleventh character instead of exposing the final two score bits.
  out.back() = '0';
  return out;
}

double GeoDistance(double lon1, double lat1, double lon2, double lat2) {
  constexpr double radians = std::numbers::pi / 180.0;
  const double dlat = (lat2 - lat1) * radians;
  const double dlon = (lon2 - lon1) * radians;
  const double a = std::sin(dlat / 2) * std::sin(dlat / 2) +
                   std::cos(lat1 * radians) * std::cos(lat2 * radians) *
                       std::sin(dlon / 2) * std::sin(dlon / 2);
  return kEarthRadiusMeters * 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
}

absl::StatusOr<double> UnitMeters(std::string_view unit) {
  if (EqualCi(unit, "m")) return 1.0;
  if (EqualCi(unit, "km")) return 1000.0;
  if (EqualCi(unit, "mi")) return 1609.344;
  if (EqualCi(unit, "ft")) return 0.3048;
  return absl::InvalidArgumentError(
      "unsupported unit provided. please use m, km, ft, mi");
}

struct RangeOptions {
  enum class Mode { kRank, kScore, kLex } mode_ = Mode::kRank;
  bool reverse_ = false;
  bool with_scores_ = false;
  bool limit_ = false;
  std::int64_t offset_ = 0;
  std::int64_t count_ = -1;
};

absl::Status ParseRangeLimit(const std::vector<std::string>& args,
                             std::size_t option, RangeOptions* output) {
  if (option + 2 >= args.size()) {
    return absl::InvalidArgumentError("syntax error");
  }
  if (!ParseInt(args[option + 1], &output->offset_) ||
      !ParseInt(args[option + 2], &output->count_)) {
    return absl::InvalidArgumentError(
        "value is not an integer or out of range");
  }
  output->limit_ = true;
  return absl::OkStatus();
}

std::pair<std::size_t, std::size_t> RankSlice(std::int64_t start,
                                              std::int64_t stop,
                                              std::size_t size) {
  auto normalize = [size](std::int64_t x) -> std::int64_t {
    if (x < 0 && size <= static_cast<std::size_t>(INT64_MAX))
      x += static_cast<std::int64_t>(size);
    return x;
  };
  start = normalize(start);
  stop = normalize(stop);
  if (start < 0) start = 0;
  if (stop < 0 || start > stop || static_cast<std::uint64_t>(start) >= size)
    return {size, size};
  return {static_cast<std::size_t>(start),
          std::min(size, static_cast<std::size_t>(stop) + 1)};
}

bool IsRangeCommand(CommandKind kind) {
  switch (kind) {
    case CommandKind::kZRange:
    case CommandKind::kZRangeByLex:
    case CommandKind::kZRangeByScore:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
    case CommandKind::kZRevRange:
    case CommandKind::kZRevRangeByLex:
    case CommandKind::kZRevRangeByScore:
      return true;
    default:
      return false;
  }
}

// The bounded storage path consumes the same parser as syntax validation;
// do not let legacy aliases, endpoint reversal or LIMIT semantics drift.
absl::Status ValidateRangeSyntax(
    const CommandRequest& request,
    storage::SortedSetOperation* normalized = nullptr,
    bool* with_scores = nullptr) {
  const auto& args = request.args_;
  RangeOptions options;
  std::string_view min_text = args[2], max_text = args[3];
  if (request.kind_ == CommandKind::kZRevRange ||
      request.kind_ == CommandKind::kZRevRangeByLex ||
      request.kind_ == CommandKind::kZRevRangeByScore)
    options.reverse_ = true;
  if (request.kind_ == CommandKind::kZRangeByScore ||
      request.kind_ == CommandKind::kZRevRangeByScore ||
      request.kind_ == CommandKind::kZRemRangeByScore)
    options.mode_ = RangeOptions::Mode::kScore;
  if (request.kind_ == CommandKind::kZRangeByLex ||
      request.kind_ == CommandKind::kZRevRangeByLex ||
      request.kind_ == CommandKind::kZRemRangeByLex)
    options.mode_ = RangeOptions::Mode::kLex;
  std::size_t i = 4;
  if (request.kind_ == CommandKind::kZRange) {
    while (i < args.size()) {
      if (EqualCi(args[i], "byscore"))
        options.mode_ = RangeOptions::Mode::kScore;
      else if (EqualCi(args[i], "bylex"))
        options.mode_ = RangeOptions::Mode::kLex;
      else if (EqualCi(args[i], "rev"))
        options.reverse_ = true;
      else if (EqualCi(args[i], "withscores"))
        options.with_scores_ = true;
      else if (EqualCi(args[i], "limit")) {
        absl::Status parsed = ParseRangeLimit(args, i, &options);
        if (!parsed.ok()) return parsed;
        i += 3;
        continue;
      } else {
        return absl::InvalidArgumentError("syntax error");
      }
      ++i;
    }
    if (options.limit_ && options.mode_ == RangeOptions::Mode::kRank)
      return absl::InvalidArgumentError(
          "syntax error, LIMIT is only supported in combination with either "
          "BYSCORE or BYLEX");
    if (options.with_scores_ && options.mode_ == RangeOptions::Mode::kLex)
      return absl::InvalidArgumentError(
          "syntax error, WITHSCORES not supported in combination with BYLEX");
  } else if (request.kind_ == CommandKind::kZRangeByScore ||
             request.kind_ == CommandKind::kZRevRangeByScore ||
             request.kind_ == CommandKind::kZRangeByLex ||
             request.kind_ == CommandKind::kZRevRangeByLex) {
    while (i < args.size()) {
      if (EqualCi(args[i], "withscores")) {
        if (options.mode_ == RangeOptions::Mode::kLex)
          return absl::InvalidArgumentError(
              "syntax error, WITHSCORES not supported in combination with "
              "BYLEX");
        options.with_scores_ = true;
        ++i;
      } else if (EqualCi(args[i], "limit")) {
        absl::Status parsed = ParseRangeLimit(args, i, &options);
        if (!parsed.ok()) return parsed;
        i += 3;
      } else {
        return absl::InvalidArgumentError("syntax error");
      }
    }
  } else if (request.kind_ == CommandKind::kZRevRange) {
    if (args.size() > 5 ||
        (args.size() == 5 && !EqualCi(args[4], "withscores"))) {
      return absl::InvalidArgumentError("syntax error");
    }
    options.with_scores_ = args.size() == 5;
  }
  if (normalized) {
    normalized->kind_ = storage::SortedSetOperationKind::kRange;
    normalized->reverse_ = options.reverse_;
    normalized->limit_ = options.limit_;
    normalized->offset_ = options.offset_;
    normalized->count_ = options.count_;
  }
  if (with_scores) *with_scores = options.with_scores_;
  if (options.reverse_ && options.mode_ != RangeOptions::Mode::kRank)
    std::swap(min_text, max_text);
  if (options.mode_ == RangeOptions::Mode::kRank) {
    std::int64_t start = 0, stop = 0;
    if (!ParseInt(min_text, &start) || !ParseInt(max_text, &stop))
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    if (normalized) {
      normalized->range_mode_ = storage::SortedSetRangeMode::kRank;
      normalized->first_ = start;
      normalized->last_ = stop;
    }
  } else if (options.mode_ == RangeOptions::Mode::kScore) {
    auto min = ParseScoreBound(min_text), max = ParseScoreBound(max_text);
    if (!min.ok()) return min.status();
    if (!max.ok()) return max.status();
    if (normalized) {
      normalized->range_mode_ = storage::SortedSetRangeMode::kScore;
      normalized->minimum_score_ = {min->value_, min->exclusive_};
      normalized->maximum_score_ = {max->value_, max->exclusive_};
    }
  } else {
    auto min = ParseLexBound(min_text), max = ParseLexBound(max_text);
    if (!min.ok()) return min.status();
    if (!max.ok()) return max.status();
    if (normalized) {
      normalized->range_mode_ = storage::SortedSetRangeMode::kLex;
      normalized->minimum_lex_ = {min->value_, min->infinity_, min->exclusive_};
      normalized->maximum_lex_ = {max->value_, max->infinity_, max->exclusive_};
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateZSetSyntax(const CommandRequest& request) {
  const auto& args = request.args_;
  if (IsRangeCommand(request.kind_)) return ValidateRangeSyntax(request);
  if (request.kind_ == CommandKind::kZCount) {
    auto min = ParseScoreBound(args[2]), max = ParseScoreBound(args[3]);
    if (!min.ok()) return min.status();
    return max.ok() ? absl::OkStatus() : max.status();
  }
  if (request.kind_ == CommandKind::kZLexCount) {
    auto min = ParseLexBound(args[2]), max = ParseLexBound(args[3]);
    if (!min.ok()) return min.status();
    return max.ok() ? absl::OkStatus() : max.status();
  }
  if (request.kind_ == CommandKind::kZPopMin ||
      request.kind_ == CommandKind::kZPopMax) {
    std::int64_t count = 1;
    if (args.size() == 3 && (!ParseInt(args[2], &count) || count < 0))
      return absl::InvalidArgumentError(
          "value is out of range, must be positive");
  } else if (request.kind_ == CommandKind::kZRandMember) {
    std::int64_t count = 1;
    if (args.size() >= 3 && !ParseInt(args[2], &count))
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    if (args.size() == 4 && !EqualCi(args[3], "withscores"))
      return absl::InvalidArgumentError("syntax error");
    if (count == std::numeric_limits<std::int64_t>::min())
      return absl::InvalidArgumentError("value is out of range");
    const bool with_scores = args.size() == 4;
    const std::uint64_t magnitude =
        static_cast<std::uint64_t>(count < 0 ? -count : count);
    if (with_scores &&
        magnitude > static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max()) /
                        2) {
      return absl::InvalidArgumentError("value is out of range");
    }
  } else if (request.kind_ == CommandKind::kZScan) {
    std::uint64_t cursor = 0, count = 10;
    if (!ParseInt(args[2], &cursor))
      return absl::InvalidArgumentError("invalid cursor");
    for (std::size_t i = 3; i < args.size();) {
      if (EqualCi(args[i], "match") && i + 1 < args.size())
        i += 2;
      else if (EqualCi(args[i], "count") && i + 1 < args.size() &&
               ParseInt(args[i + 1], &count) && count != 0)
        i += 2;
      else
        return absl::InvalidArgumentError("syntax error");
    }
  } else if ((request.kind_ == CommandKind::kZRank ||
              request.kind_ == CommandKind::kZRevRank) &&
             (args.size() > 4 ||
              (args.size() == 4 && !EqualCi(args[3], "withscore")))) {
    return absl::InvalidArgumentError("syntax error");
  } else if (request.kind_ == CommandKind::kGeoDist && args.size() == 5) {
    auto unit = UnitMeters(args[4]);
    if (!unit.ok()) return unit.status();
  }
  return absl::OkStatus();
}

Task<CommandReply> ExecuteImpl(const CommandRequest& request,
                               const storage::Digest* digest,
                               storage::TxShardWrites* tx,
                               ReplyBuilder& builder) {
  const auto& a = request.args_;

  absl::Status syntax = ValidateZSetSyntax(request);
  if (!syntax.ok())
    co_return Built(
        builder.AppendError(absl::StrCat("ERR ", syntax.message())));

  if (request.kind_ == CommandKind::kZAdd ||
      request.kind_ == CommandKind::kZIncrBy ||
      request.kind_ == CommandKind::kGeoAdd) {
    bool nx = false, xx = false, gt = false, lt = false, ch = false;
    bool incr = request.kind_ == CommandKind::kZIncrBy;
    std::size_t index = request.kind_ == CommandKind::kGeoAdd ? 2 : 2;
    if (request.kind_ == CommandKind::kZAdd) {
      while (index < a.size()) {
        if (EqualCi(a[index], "nx"))
          nx = true;
        else if (EqualCi(a[index], "xx"))
          xx = true;
        else if (EqualCi(a[index], "gt"))
          gt = true;
        else if (EqualCi(a[index], "lt"))
          lt = true;
        else if (EqualCi(a[index], "ch"))
          ch = true;
        else if (EqualCi(a[index], "incr"))
          incr = true;
        else
          break;
        ++index;
      }
    } else if (request.kind_ == CommandKind::kGeoAdd) {
      while (index < a.size()) {
        if (EqualCi(a[index], "nx"))
          nx = true;
        else if (EqualCi(a[index], "xx"))
          xx = true;
        else if (EqualCi(a[index], "ch"))
          ch = true;
        else
          break;
        ++index;
      }
    }
    const std::size_t tuple = request.kind_ == CommandKind::kGeoAdd ? 3 : 2;
    if (index >= a.size() || (a.size() - index) % tuple != 0) {
      co_return Built(builder.AppendError("ERR syntax error"));
    }
    if (request.kind_ == CommandKind::kZAdd && nx && xx) {
      co_return Built(builder.AppendError(
          "ERR XX and NX options at the same time are not compatible"));
    }
    if (request.kind_ == CommandKind::kZAdd &&
        ((gt && lt) || (nx && (gt || lt)))) {
      co_return Built(builder.AppendError(
          "ERR GT, LT, and/or NX options at the same time are not compatible"));
    }
    if (request.kind_ == CommandKind::kZAdd && incr && index < a.size() &&
        (a.size() - index) % tuple == 0 && (a.size() - index) != tuple) {
      co_return Built(builder.AppendError(
          "ERR INCR option supports a single increment-element pair"));
    }
    if ((nx && xx) || (incr && (a.size() - index) != tuple)) {
      co_return Built(builder.AppendError("ERR syntax error"));
    }
    const auto input_count = (a.size() - index) / tuple;
    auto input_admission =
        TryReserveMemory(input_count * sizeof(storage::ScoredMemberView));
    if (!input_admission) {
      RecordMemoryRejection();
      co_return Built(builder.AppendError("OOM Sorted Set input admission"));
    }
    std::vector<storage::ScoredMemberView> inputs;
    inputs.reserve(input_count);
    for (; index < a.size(); index += tuple) {
      double score = 0;
      std::string_view member;
      if (request.kind_ == CommandKind::kGeoAdd) {
        double lon = 0, lat = 0;
        if (!ParseRedisDouble(a[index], &lon) ||
            !ParseRedisDouble(a[index + 1], &lat) ||
            !ValidGeoCoordinates(lon, lat)) {
          co_return Built(
              builder.AppendError("ERR invalid longitude,latitude pair"));
        }
        score = static_cast<double>(GeoScore(lon, lat));
        member = a[index + 2];
      } else {
        if (!ParseRedisDouble(a[index], &score, true)) {
          co_return Built(
              builder.AppendError("ERR value is not a valid float"));
        }
        member = a[index + 1];
      }
      inputs.push_back({member, score});
    }
    storage::SortedSetOperation operation{
        .kind_ = storage::SortedSetOperationKind::kAdd,
        .entries_ = inputs,
        .members_ = {},
        .nx_ = nx,
        .xx_ = xx,
        .gt_ = gt,
        .lt_ = lt,
        .increment_ = incr,
        .prepare_unlocked_ = request.kind_ == CommandKind::kZAdd ||
                             request.kind_ == CommandKind::kZIncrBy};
    auto result = co_await RunSortedSet(request, digest, tx, operation);
    if (!result.ok()) co_return Built(StorageError(builder, result.status()));
    if (result->changed_ != 0) NotifyZSetBlockingKey(request, a[1]);
    if (incr) {
      co_return Built(
          result->incremented_.has_value()
              ? builder.AppendDoubleText(FormatDouble(*result->incremented_))
              : builder.AppendNull());
    }
    co_return Built(
        builder.AppendInteger(ch ? result->changed_ : result->added_));
  }

  if (request.kind_ == CommandKind::kGeoDist ||
      request.kind_ == CommandKind::kGeoHash ||
      request.kind_ == CommandKind::kGeoPos ||
      request.kind_ == CommandKind::kGeoRadius ||
      request.kind_ == CommandKind::kGeoRadiusRo ||
      request.kind_ == CommandKind::kGeoRadiusByMember ||
      request.kind_ == CommandKind::kGeoRadiusByMemberRo ||
      request.kind_ == CommandKind::kGeoSearch) {
    struct GeoResult {
      std::string member_;
      double lon_ = 0;
      double lat_ = 0;
      double distance_m_ = 0;
      std::uint64_t hash_ = 0;
    };
    std::vector<std::optional<GeoResult>> results;
    double unit_meters = 1;
    bool with_coord = false, with_dist = false, with_hash = false;
    bool ascending = false, descending = false;
    bool count_any = false;
    std::optional<std::uint64_t> count;
    double center_lon = 0, center_lat = 0;
    std::string_view center_member;
    bool center_by_member = false;
    bool center_missing = false;
    double radius_m = 0, box_width_m = 0, box_height_m = 0;
    bool box = false;

    if (request.kind_ == CommandKind::kGeoRadius ||
        request.kind_ == CommandKind::kGeoRadiusRo) {
      double radius = 0;
      auto unit = UnitMeters(a[5]);
      if (!ParseRedisDouble(a[2], &center_lon) ||
          !ParseRedisDouble(a[3], &center_lat) ||
          !ParseRedisDouble(a[4], &radius) ||
          !ValidGeoCoordinates(center_lon, center_lat) || radius < 0 ||
          !unit.ok()) {
        co_return Built(builder.AppendError(
            "ERR invalid longitude,latitude pair or radius"));
      }
      unit_meters = *unit;
      radius_m = radius * unit_meters;
    } else if (request.kind_ == CommandKind::kGeoRadiusByMember ||
               request.kind_ == CommandKind::kGeoRadiusByMemberRo) {
      double radius = 0;
      auto unit = UnitMeters(a[4]);
      if (!ParseRedisDouble(a[3], &radius) || radius < 0 || !unit.ok())
        co_return Built(builder.AppendError("ERR need numeric radius"));
      center_member = a[2];
      center_by_member = true;
      unit_meters = *unit;
      radius_m = radius * unit_meters;
    }

    std::size_t option =
        (request.kind_ == CommandKind::kGeoRadius ||
         request.kind_ == CommandKind::kGeoRadiusRo)
            ? 6
            : ((request.kind_ == CommandKind::kGeoRadiusByMember ||
                request.kind_ == CommandKind::kGeoRadiusByMemberRo)
                   ? 5
                   : a.size());
    bool center_specified = request.kind_ != CommandKind::kGeoSearch;
    bool shape_specified = request.kind_ != CommandKind::kGeoSearch;
    if (request.kind_ == CommandKind::kGeoSearch) option = 2;
    for (; option < a.size();) {
      if (EqualCi(a[option], "withcoord")) {
        with_coord = true;
        ++option;
      } else if (EqualCi(a[option], "withdist")) {
        with_dist = true;
        ++option;
      } else if (EqualCi(a[option], "withhash")) {
        with_hash = true;
        ++option;
      } else if (EqualCi(a[option], "asc")) {
        ascending = true;
        ++option;
      } else if (EqualCi(a[option], "desc")) {
        descending = true;
        ++option;
      } else if (EqualCi(a[option], "any")) {
        count_any = true;
        ++option;
      } else if (EqualCi(a[option], "count") && option + 1 < a.size()) {
        std::uint64_t parsed = 0;
        if (!ParseInt(a[option + 1], &parsed) || parsed == 0)
          co_return Built(builder.AppendError("ERR COUNT must be > 0"));
        count = parsed;
        option += 2;
      } else if (request.kind_ == CommandKind::kGeoSearch &&
                 EqualCi(a[option], "frommember") && !center_specified &&
                 option + 1 < a.size()) {
        center_member = a[option + 1];
        center_by_member = true;
        center_specified = true;
        option += 2;
      } else if (request.kind_ == CommandKind::kGeoSearch &&
                 EqualCi(a[option], "fromlonlat") && !center_specified &&
                 option + 2 < a.size() &&
                 ParseRedisDouble(a[option + 1], &center_lon) &&
                 ParseRedisDouble(a[option + 2], &center_lat)) {
        if (!ValidGeoCoordinates(center_lon, center_lat)) {
          co_return Built(
              builder.AppendError("ERR invalid longitude,latitude pair"));
        }
        center_specified = true;
        option += 3;
      } else if (request.kind_ == CommandKind::kGeoSearch &&
                 EqualCi(a[option], "byradius") && !shape_specified &&
                 option + 2 < a.size()) {
        double radius = 0;
        auto unit = UnitMeters(a[option + 2]);
        if (!ParseRedisDouble(a[option + 1], &radius) || radius < 0 ||
            !unit.ok())
          co_return Built(builder.AppendError("ERR syntax error"));
        unit_meters = *unit;
        radius_m = radius * unit_meters;
        shape_specified = true;
        option += 3;
      } else if (request.kind_ == CommandKind::kGeoSearch &&
                 EqualCi(a[option], "bybox") && !shape_specified &&
                 option + 3 < a.size()) {
        double width = 0, height = 0;
        auto unit = UnitMeters(a[option + 3]);
        if (!ParseRedisDouble(a[option + 1], &width) ||
            !ParseRedisDouble(a[option + 2], &height) || width < 0 ||
            height < 0 || !unit.ok())
          co_return Built(builder.AppendError("ERR syntax error"));
        unit_meters = *unit;
        box_width_m = width * unit_meters;
        box_height_m = height * unit_meters;
        box = true;
        shape_specified = true;
        option += 4;
      } else {
        co_return Built(builder.AppendError("ERR syntax error"));
      }
    }
    if (ascending && descending)
      co_return Built(builder.AppendError("ERR syntax error"));
    if (!center_specified || !shape_specified)
      co_return Built(builder.AppendError("ERR syntax error"));
    if (count_any && !count.has_value())
      co_return Built(
          builder.AppendError("ERR the ANY argument requires COUNT argument"));

    auto callback = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto decoded = Decode(value);
      if (!decoded.ok()) return decoded.status();
      const ZSet& set = *decoded;
      if (request.kind_ == CommandKind::kGeoHash ||
          request.kind_ == CommandKind::kGeoPos) {
        for (std::size_t i = 2; i < a.size(); ++i) {
          const Element* element = Find(set, a[i]);
          if (!element) {
            results.push_back(std::nullopt);
          } else {
            const auto hash = DecodeGeoScore(element->score_);
            if (!hash) {
              results.push_back(std::nullopt);
            } else {
              const auto [lon, lat] = GeoDecode(*hash);
              results.push_back(GeoResult{a[i], lon, lat, 0, *hash});
            }
          }
        }
        return NoChange();
      }
      if (request.kind_ == CommandKind::kGeoDist) {
        const Element* first = Find(set, a[2]);
        const Element* second = Find(set, a[3]);
        if (!first || !second) return NoChange();
        const auto first_hash = DecodeGeoScore(first->score_);
        const auto second_hash = DecodeGeoScore(second->score_);
        if (!first_hash || !second_hash) return NoChange();
        auto [lon1, lat1] = GeoDecode(*first_hash);
        auto [lon2, lat2] = GeoDecode(*second_hash);
        results.push_back(
            GeoResult{.member_ = {},
                      .lon_ = 0,
                      .lat_ = 0,
                      .distance_m_ = GeoDistance(lon1, lat1, lon2, lat2),
                      .hash_ = 0});
        return NoChange();
      }
      if (center_by_member) {
        const Element* center = Find(set, center_member);
        if (!center) {
          center_missing = value.has_value();
          return NoChange();
        }
        const auto center_hash = DecodeGeoScore(center->score_);
        if (!center_hash) {
          center_missing = true;
          return NoChange();
        }
        std::tie(center_lon, center_lat) = GeoDecode(*center_hash);
      }
      for (const Element& element : set) {
        const auto decoded_hash = DecodeGeoScore(element.score_);
        if (!decoded_hash) continue;
        const std::uint64_t hash = *decoded_hash;
        auto [lon, lat] = GeoDecode(hash);
        const double distance = GeoDistance(center_lon, center_lat, lon, lat);
        bool inside = distance <= radius_m;
        if (box) {
          const double north =
              GeoDistance(center_lon, center_lat, center_lon, lat);
          // Redis evaluates longitude width on the candidate's latitude,
          // rather than projecting every point at the center latitude.
          const double east = GeoDistance(center_lon, lat, lon, lat);
          inside = east <= box_width_m / 2 && north <= box_height_m / 2;
        }
        if (inside)
          results.push_back(
              GeoResult{element.member_, lon, lat, distance, hash});
      }
      if (ascending || descending || (count.has_value() && !count_any)) {
        std::sort(results.begin(), results.end(),
                  [&](const auto& x, const auto& y) {
                    if (x.has_value() != y.has_value()) {
                      return x.has_value();
                    }
                    if (!x.has_value()) return false;
                    if (descending) return x->distance_m_ > y->distance_m_;
                    return x->distance_m_ < y->distance_m_;
                  });
      }
      if (count && results.size() > *count) results.resize(*count);
      return NoChange();
    };
    absl::Status status =
        co_await RunCompact(request, digest, tx, true, callback);
    if (!status.ok()) co_return Built(StorageError(builder, status));
    if (center_missing) {
      co_return Built(
          builder.AppendError("ERR could not decode requested zset member"));
    }

    if (request.kind_ == CommandKind::kGeoDist) {
      if (a.size() == 5) {
        auto unit = UnitMeters(a[4]);
        if (!unit.ok()) co_return Built(StorageError(builder, unit.status()));
        unit_meters = *unit;
      }
      co_return Built(results.empty()
                          ? builder.AppendNull()
                          : builder.AppendBulkString(FormatGeoDistance(
                                results[0]->distance_m_ / unit_meters)));
    }
    builder.AppendArrayHeader(results.size());
    for (const auto& result : results) {
      if (!result) {
        if (request.kind_ == CommandKind::kGeoPos)
          builder.AppendNullArray();
        else
          builder.AppendNull();
        continue;
      }
      if (request.kind_ == CommandKind::kGeoHash) {
        builder.AppendBulkString(GeoHashString(result->lon_, result->lat_));
      } else if (request.kind_ == CommandKind::kGeoPos) {
        builder.AppendArrayHeader(2);
        builder.AppendDoubleText(FormatGeoCoordinate(result->lon_));
        builder.AppendDoubleText(FormatGeoCoordinate(result->lat_));
      } else if (!with_coord && !with_dist && !with_hash) {
        builder.AppendBulkString(result->member_);
      } else {
        builder.AppendArrayHeader(1 + with_dist + with_hash + with_coord);
        builder.AppendBulkString(result->member_);
        if (with_dist)
          builder.AppendBulkString(
              FormatGeoDistance(result->distance_m_ / unit_meters));
        if (with_hash) builder.AppendInteger(result->hash_);
        if (with_coord) {
          builder.AppendArrayHeader(2);
          builder.AppendDoubleText(FormatGeoCoordinate(result->lon_));
          builder.AppendDoubleText(FormatGeoCoordinate(result->lat_));
        }
      }
    }
    co_return Built(builder.View());
  }

  if (request.kind_ == CommandKind::kZCard ||
      request.kind_ == CommandKind::kZScore ||
      request.kind_ == CommandKind::kZMScore ||
      request.kind_ == CommandKind::kZRem) {
    auto member_admission =
        TryReserveMemory((a.size() - 2) * sizeof(std::string_view));
    if (!member_admission) {
      RecordMemoryRejection();
      co_return Built(builder.AppendError("OOM Sorted Set input admission"));
    }
    std::vector<std::string_view> members;
    members.reserve(a.size() - 2);
    for (std::size_t i = 2; i < a.size(); ++i) members.push_back(a[i]);
    storage::SortedSetOperation operation{
        .kind_ = request.kind_ == CommandKind::kZCard
                     ? storage::SortedSetOperationKind::kLength
                 : request.kind_ == CommandKind::kZRem
                     ? storage::SortedSetOperationKind::kRemove
                     : storage::SortedSetOperationKind::kScores,
        .entries_ = {},
        .members_ = members,
        .prepare_unlocked_ = request.kind_ == CommandKind::kZRem};
    auto result = co_await RunSortedSet(request, digest, tx, operation);
    if (!result.ok()) co_return Built(StorageError(builder, result.status()));
    if (request.kind_ == CommandKind::kZCard)
      co_return Built(builder.AppendInteger(result->length_));
    if (request.kind_ == CommandKind::kZRem)
      co_return Built(builder.AppendInteger(result->changed_));
    if (request.kind_ == CommandKind::kZMScore)
      builder.AppendArrayHeader(result->scores_.size());
    for (const auto score : result->scores_) {
      if (score)
        builder.AppendDoubleText(FormatDouble(*score));
      else
        builder.AppendNull();
    }
    co_return Built(builder.View());
  }

  if (request.kind_ == CommandKind::kZPopMin ||
      request.kind_ == CommandKind::kZPopMax ||
      request.kind_ == CommandKind::kZScan) {
    storage::SortedSetOperation operation;
    if (request.kind_ == CommandKind::kZScan) {
      operation.kind_ = storage::SortedSetOperationKind::kScan;
      if (!ParseInt(a[2], &operation.scan_cursor_))
        co_return Built(builder.AppendError("ERR invalid cursor"));
      for (std::size_t i = 3; i < a.size();) {
        if (EqualCi(a[i], "match") && i + 1 < a.size()) {
          operation.scan_pattern_ = a[i + 1];
          i += 2;
        } else if (EqualCi(a[i], "count") && i + 1 < a.size() &&
                   ParseInt(a[i + 1], &operation.scan_count_) &&
                   operation.scan_count_ != 0) {
          i += 2;
        } else {
          co_return Built(builder.AppendError("ERR syntax error"));
        }
      }
    } else {
      std::int64_t count = 1;
      if (a.size() == 3 && (!ParseInt(a[2], &count) || count < 0))
        co_return Built(
            builder.AppendError("ERR value is out of range, must be positive"));
      operation.kind_ = storage::SortedSetOperationKind::kPop;
      operation.reverse_ = request.kind_ == CommandKind::kZPopMax;
      operation.pop_count_ = count;
    }
    auto result = co_await RunSortedSet(request, digest, tx, operation);
    if (!result.ok()) co_return Built(StorageError(builder, result.status()));
    const bool scan = operation.kind_ == storage::SortedSetOperationKind::kScan;
    if (!scan && result->changed_ != 0 && result->length_ != 0)
      NotifyZSetBlockingKey(request, a[1]);
    const bool nested =
        !scan && builder.version() == RespVersion::k3 && a.size() == 3;
    if (scan) {
      builder.AppendArrayHeader(2);
      builder.AppendBulkString(std::to_string(result->next_cursor_));
    }
    builder.AppendArrayHeader(result->members_.size() * (nested ? 1 : 2));
    for (const auto& member : result->members_) {
      if (nested) builder.AppendArrayHeader(2);
      builder.AppendBulkString(member.member_);
      if (scan)
        builder.AppendBulkString(FormatDouble(member.score_));
      else
        AppendScore(builder, member.score_);
    }
    co_return Built(builder.View());
  }

  const bool range_read = IsRangeCommand(request.kind_) &&
                          request.kind_ != CommandKind::kZRemRangeByRank &&
                          request.kind_ != CommandKind::kZRemRangeByScore &&
                          request.kind_ != CommandKind::kZRemRangeByLex;
  if (range_read || request.kind_ == CommandKind::kZRank ||
      request.kind_ == CommandKind::kZRevRank ||
      request.kind_ == CommandKind::kZCount ||
      request.kind_ == CommandKind::kZLexCount) {
    storage::SortedSetOperation operation;
    bool with_scores = false;
    std::array<std::string_view, 1> wanted{a[2]};
    if (range_read) {
      auto normalized = ValidateRangeSyntax(request, &operation, &with_scores);
      if (!normalized.ok()) co_return Built(StorageError(builder, normalized));
    } else if (request.kind_ == CommandKind::kZRank ||
               request.kind_ == CommandKind::kZRevRank) {
      operation.kind_ = storage::SortedSetOperationKind::kRank;
      operation.members_ = wanted;
      operation.reverse_ = request.kind_ == CommandKind::kZRevRank;
    } else {
      operation.kind_ = storage::SortedSetOperationKind::kCount;
      if (request.kind_ == CommandKind::kZCount) {
        auto low = ParseScoreBound(a[2]), high = ParseScoreBound(a[3]);
        if (!low.ok()) co_return Built(StorageError(builder, low.status()));
        if (!high.ok()) co_return Built(StorageError(builder, high.status()));
        operation.range_mode_ = storage::SortedSetRangeMode::kScore;
        operation.minimum_score_ = {low->value_, low->exclusive_};
        operation.maximum_score_ = {high->value_, high->exclusive_};
      } else {
        auto low = ParseLexBound(a[2]), high = ParseLexBound(a[3]);
        if (!low.ok()) co_return Built(StorageError(builder, low.status()));
        if (!high.ok()) co_return Built(StorageError(builder, high.status()));
        operation.range_mode_ = storage::SortedSetRangeMode::kLex;
        operation.minimum_lex_ = {low->value_, low->infinity_, low->exclusive_};
        operation.maximum_lex_ = {high->value_, high->infinity_,
                                  high->exclusive_};
      }
    }
    auto result = co_await RunSortedSet(request, digest, tx, operation);
    if (!result.ok()) co_return Built(StorageError(builder, result.status()));
    if (operation.kind_ == storage::SortedSetOperationKind::kCount)
      co_return Built(builder.AppendInteger(result->count_));
    if (operation.kind_ == storage::SortedSetOperationKind::kRank) {
      if (!result->rank_)
        co_return Built(a.size() == 4 ? builder.AppendNullArray()
                                      : builder.AppendNull());
      if (a.size() == 4) {
        builder.AppendArrayHeader(2);
        builder.AppendInteger(*result->rank_);
        AppendScore(builder, *result->rank_score_);
        co_return Built(builder.View());
      }
      co_return Built(builder.AppendInteger(*result->rank_));
    }
    const bool nested = with_scores && builder.version() == RespVersion::k3;
    builder.AppendArrayHeader(result->members_.size() *
                              (with_scores && !nested ? 2 : 1));
    for (const auto& member : result->members_) {
      if (nested) builder.AppendArrayHeader(2);
      builder.AppendBulkString(member.member_);
      if (with_scores) AppendScore(builder, member.score_);
    }
    co_return Built(builder.View());
  }

  bool read_only = true;
  switch (request.kind_) {
    case CommandKind::kZPopMax:
    case CommandKind::kZPopMin:
    case CommandKind::kZRem:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
      read_only = false;
      break;
    default:
      break;
  }

  long long integer = 0;
  std::optional<std::string> scalar;
  // Callback output outlives the engine's full-image scratch reservation.
  // Keep repeated random replies charged until ReplyBuilder has copied them.
  RetainedMemoryCharge random_reply_charge;
  std::vector<std::optional<std::string>> output;
  std::uint64_t next_cursor = 0;
  const bool reply_with_scores = std::any_of(
      a.begin() + std::min<std::size_t>(2, a.size()), a.end(),
      [](std::string_view arg) { return EqualCi(arg, "withscores"); });
  if ((request.kind_ == CommandKind::kZRank ||
       request.kind_ == CommandKind::kZRevRank) &&
      a.size() == 4 && !EqualCi(a[3], "withscore")) {
    co_return Built(builder.AppendError("ERR syntax error"));
  }
  auto callback = [&](std::optional<storage::CompactValueView> value)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    auto decoded = Decode(value);
    if (!decoded.ok()) return decoded.status();
    ZSet set = std::move(*decoded);

    switch (request.kind_) {
      case CommandKind::kZCard:
        integer = set.size();
        return NoChange();
      case CommandKind::kZScore:
      case CommandKind::kZMScore: {
        const std::size_t first = request.kind_ == CommandKind::kZScore ? 2 : 2;
        for (std::size_t i = first; i < a.size(); ++i) {
          const Element* element = Find(set, a[i]);
          output.push_back(element
                               ? std::optional(FormatDouble(element->score_))
                               : std::nullopt);
        }
        return NoChange();
      }
      case CommandKind::kZRank:
      case CommandKind::kZRevRank: {
        const bool reverse = request.kind_ == CommandKind::kZRevRank;
        auto found =
            std::find_if(set.begin(), set.end(),
                         [&](const Element& e) { return e.member_ == a[2]; });
        if (found != set.end()) {
          const std::size_t rank = found - set.begin();
          integer = reverse ? set.size() - rank - 1 : rank;
          scalar = FormatDouble(found->score_);
        } else {
          scalar.reset();
          integer = -1;
        }
        return NoChange();
      }
      case CommandKind::kZCount: {
        auto min = ParseScoreBound(a[2]);
        auto max = ParseScoreBound(a[3]);
        if (!min.ok()) return min.status();
        if (!max.ok()) return max.status();
        for (const auto& e : set)
          if (AboveMin(e.score_, *min) && BelowMax(e.score_, *max)) ++integer;
        return NoChange();
      }
      case CommandKind::kZLexCount: {
        auto min = ParseLexBound(a[2]);
        auto max = ParseLexBound(a[3]);
        if (!min.ok()) return min.status();
        if (!max.ok()) return max.status();
        for (const auto& e : set)
          if (AboveMin(e.member_, *min) && BelowMax(e.member_, *max)) ++integer;
        return NoChange();
      }
      case CommandKind::kZRem: {
        const std::size_t old = set.size();
        for (std::size_t i = 2; i < a.size(); ++i) {
          std::erase_if(set,
                        [&](const Element& e) { return e.member_ == a[i]; });
        }
        integer = old - set.size();
        return integer == 0
                   ? absl::StatusOr<storage::CompactValueUpdate>(NoChange())
                   : Changed(std::move(set));
      }
      case CommandKind::kZPopMin:
      case CommandKind::kZPopMax: {
        std::int64_t parsed_count = 1;
        if (a.size() == 3 &&
            (!ParseInt(a[2], &parsed_count) || parsed_count < 0))
          return absl::InvalidArgumentError(
              "value is out of range, must be positive");
        std::uint64_t count = static_cast<std::uint64_t>(parsed_count);
        const bool maximum = request.kind_ == CommandKind::kZPopMax;
        count = std::min<std::uint64_t>(count, set.size());
        for (std::uint64_t i = 0; i < count; ++i) {
          std::size_t at = maximum ? set.size() - 1 : 0;
          output.push_back(set[at].member_);
          output.push_back(FormatDouble(set[at].score_));
          set.erase(set.begin() + at);
        }
        return count == 0
                   ? absl::StatusOr<storage::CompactValueUpdate>(NoChange())
                   : Changed(std::move(set));
      }
      case CommandKind::kZRandMember: {
        std::int64_t count = 1;
        bool count_given = a.size() >= 3;
        if (count_given && !ParseInt(a[2], &count))
          return absl::InvalidArgumentError(
              "value is not an integer or out of range");
        const bool with_scores = a.size() == 4 && EqualCi(a[3], "withscores");
        if (a.size() == 4 && !with_scores)
          return absl::InvalidArgumentError("syntax error");
        if (count == std::numeric_limits<std::int64_t>::min())
          return absl::InvalidArgumentError("value is out of range");
        if (count < 0 && with_scores &&
            static_cast<std::uint64_t>(-count) >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) /
                    2) {
          return absl::InvalidArgumentError("value is out of range");
        }
        if (set.empty()) return NoChange();
        const std::uint64_t requested =
            count < 0 ? static_cast<std::uint64_t>(-count)
                      : std::min<std::uint64_t>(count, set.size());
        constexpr auto limit = std::numeric_limits<std::size_t>::max();
        constexpr auto slot_bytes = sizeof(decltype(output)::value_type);
        const std::uint64_t multiplier = with_scores ? 2 : 1;
        if (requested > limit / sizeof(std::uint64_t) ||
            requested > output.max_size() / multiplier ||
            requested > limit / (multiplier * slot_bytes * 2))
          return absl::ResourceExhaustedError(
              "OOM Sorted Set random reply size overflow");
        const auto index_bytes = requested * sizeof(std::uint64_t);
        auto index_admission = TryReserveMemory(index_bytes);
        if (!index_admission) {
          RecordMemoryRejection();
          return absl::ResourceExhaustedError(
              "OOM Sorted Set random rank admission");
        }
        // Draw first, then admit the actual chosen strings. A single huge
        // member must not charge every small draw as if it selected that item.
        std::vector<std::uint64_t> indexes;
        if (count >= 0) {
          indexes = SampleUniqueRandomRanks(set.size(), requested, true,
                                            RandomSampleGenerator());
        } else {
          indexes.reserve(requested);
          for (std::uint64_t i = 0; i < requested; ++i)
            indexes.push_back(RandomRank(set.size(), RandomSampleGenerator()));
        }
        const auto slots = requested * multiplier;
        std::size_t output_bytes = slots * slot_bytes * 2;
        for (const auto at : indexes) {
          // Include SSO capacity and the bounded score text, even when no
          // separate heap allocation happens for those strings.
          const auto bytes = std::max<std::size_t>(set[at].member_.size() + 1,
                                                   sizeof(std::string)) +
                             (with_scores ? 64 : 0);
          if (bytes > limit - output_bytes)
            return absl::ResourceExhaustedError(
                "OOM Sorted Set random reply size overflow");
          output_bytes += bytes;
        }
        auto output_admission = TryReserveMemory(output_bytes);
        if (!output_admission) {
          RecordMemoryRejection();
          return absl::ResourceExhaustedError(
              "OOM Sorted Set random reply admission");
        }
        output.reserve(slots);
        random_reply_charge.Adopt(&*output_admission, output_bytes);
        for (std::uint64_t at : indexes) {
          output.push_back(set[at].member_);
          if (with_scores) output.push_back(FormatDouble(set[at].score_));
        }
        integer = count_given ? 1 : 0;
        return NoChange();
      }
      case CommandKind::kZScan: {
        std::uint64_t cursor = 0, count = 10;
        std::string_view pattern = "*";
        if (!ParseInt(a[2], &cursor))
          return absl::InvalidArgumentError("invalid cursor");
        for (std::size_t i = 3; i < a.size();) {
          if (EqualCi(a[i], "match") && i + 1 < a.size()) {
            pattern = a[i + 1];
            i += 2;
          } else if (EqualCi(a[i], "count") && i + 1 < a.size() &&
                     ParseInt(a[i + 1], &count) && count != 0) {
            i += 2;
          } else
            return absl::InvalidArgumentError("syntax error");
        }
        struct ScanElement {
          const Element* element_ = nullptr;
          std::uint64_t prefix_ = 0;
        };
        std::vector<ScanElement> scan;
        scan.reserve(set.size());
        for (const Element& element : set) {
          scan.push_back(ScanElement{
              .element_ = &element,
              .prefix_ = storage::ScanCursorPrefix(
                  storage::ComputeDigest(element.member_)),
          });
        }
        std::sort(scan.begin(), scan.end(),
                  [](const auto& left, const auto& right) {
                    return left.prefix_ < right.prefix_ ||
                           (left.prefix_ == right.prefix_ &&
                            left.element_->member_ < right.element_->member_);
                  });
        const auto begin_it = std::lower_bound(
            scan.begin(), scan.end(), cursor,
            [](const ScanElement& element, std::uint64_t wanted) {
              return element.prefix_ < wanted;
            });
        const std::size_t begin = begin_it - scan.begin();
        const std::size_t examined = static_cast<std::size_t>(
            std::min<std::uint64_t>(count, scan.size() - begin));
        std::size_t end = begin + examined;
        while (end < scan.size() && end != begin &&
               scan[end].prefix_ == scan[end - 1].prefix_) {
          ++end;
        }
        for (std::size_t at = begin; at < end; ++at) {
          const Element& element = *scan[at].element_;
          if (pattern == "*" || RedisGlobMatch(pattern, element.member_)) {
            output.push_back(element.member_);
            output.push_back(FormatDouble(element.score_));
          }
        }
        next_cursor = end == scan.size() ? 0 : scan[end].prefix_;
        return NoChange();
      }
      default:
        break;
    }

    // Range and range-removal family.
    RangeOptions options;
    std::string_view min_text = a[2], max_text = a[3];
    if (request.kind_ == CommandKind::kZRevRange ||
        request.kind_ == CommandKind::kZRevRangeByLex ||
        request.kind_ == CommandKind::kZRevRangeByScore)
      options.reverse_ = true;
    if (request.kind_ == CommandKind::kZRangeByScore ||
        request.kind_ == CommandKind::kZRevRangeByScore ||
        request.kind_ == CommandKind::kZRemRangeByScore)
      options.mode_ = RangeOptions::Mode::kScore;
    if (request.kind_ == CommandKind::kZRangeByLex ||
        request.kind_ == CommandKind::kZRevRangeByLex ||
        request.kind_ == CommandKind::kZRemRangeByLex)
      options.mode_ = RangeOptions::Mode::kLex;
    std::size_t option_index = 4;
    if (request.kind_ == CommandKind::kZRange) {
      while (option_index < a.size()) {
        if (EqualCi(a[option_index], "byscore"))
          options.mode_ = RangeOptions::Mode::kScore;
        else if (EqualCi(a[option_index], "bylex"))
          options.mode_ = RangeOptions::Mode::kLex;
        else if (EqualCi(a[option_index], "rev"))
          options.reverse_ = true;
        else if (EqualCi(a[option_index], "withscores"))
          options.with_scores_ = true;
        else if (EqualCi(a[option_index], "limit")) {
          absl::Status parsed = ParseRangeLimit(a, option_index, &options);
          if (!parsed.ok()) return parsed;
          option_index += 3;
          continue;
        } else
          return absl::InvalidArgumentError("syntax error");
        ++option_index;
      }
      if (options.limit_ && options.mode_ == RangeOptions::Mode::kRank)
        return absl::InvalidArgumentError(
            "syntax error, LIMIT is only supported in combination with "
            "either BYSCORE or BYLEX");
      if (options.with_scores_ && options.mode_ == RangeOptions::Mode::kLex)
        return absl::InvalidArgumentError(
            "syntax error, WITHSCORES not supported in combination with "
            "BYLEX");
    } else if (request.kind_ == CommandKind::kZRangeByScore ||
               request.kind_ == CommandKind::kZRevRangeByScore ||
               request.kind_ == CommandKind::kZRangeByLex ||
               request.kind_ == CommandKind::kZRevRangeByLex) {
      while (option_index < a.size()) {
        if (EqualCi(a[option_index], "withscores")) {
          if (request.kind_ == CommandKind::kZRangeByLex ||
              request.kind_ == CommandKind::kZRevRangeByLex)
            return absl::InvalidArgumentError(
                "syntax error, WITHSCORES not supported in combination with "
                "BYLEX");
          options.with_scores_ = true;
          ++option_index;
        } else if (EqualCi(a[option_index], "limit")) {
          absl::Status parsed = ParseRangeLimit(a, option_index, &options);
          if (!parsed.ok()) return parsed;
          option_index += 3;
        } else
          return absl::InvalidArgumentError("syntax error");
      }
    } else if ((request.kind_ == CommandKind::kZRevRange) && a.size() == 5) {
      if (!EqualCi(a[4], "withscores"))
        return absl::InvalidArgumentError("syntax error");
      options.with_scores_ = true;
    }
    if (options.reverse_ && options.mode_ != RangeOptions::Mode::kRank)
      std::swap(min_text, max_text);

    std::vector<std::size_t> selected;
    if (options.mode_ == RangeOptions::Mode::kRank) {
      std::int64_t start = 0, stop = 0;
      if (!ParseInt(min_text, &start) || !ParseInt(max_text, &stop))
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      auto [begin, end] = RankSlice(start, stop, set.size());
      for (std::size_t i = begin; i < end; ++i)
        selected.push_back(options.reverse_ ? set.size() - 1 - i : i);
    } else if (options.mode_ == RangeOptions::Mode::kScore) {
      auto min = ParseScoreBound(min_text), max = ParseScoreBound(max_text);
      if (!min.ok()) return min.status();
      if (!max.ok()) return max.status();
      for (std::size_t i = 0; i < set.size(); ++i)
        if (AboveMin(set[i].score_, *min) && BelowMax(set[i].score_, *max))
          selected.push_back(i);
      if (options.reverse_) std::reverse(selected.begin(), selected.end());
    } else {
      auto min = ParseLexBound(min_text), max = ParseLexBound(max_text);
      if (!min.ok()) return min.status();
      if (!max.ok()) return max.status();
      std::vector<std::size_t> lex(set.size());
      std::iota(lex.begin(), lex.end(), 0);
      std::sort(lex.begin(), lex.end(), [&](std::size_t x, std::size_t y) {
        return set[x].member_ < set[y].member_;
      });
      for (std::size_t i : lex)
        if (AboveMin(set[i].member_, *min) && BelowMax(set[i].member_, *max))
          selected.push_back(i);
      if (options.reverse_) std::reverse(selected.begin(), selected.end());
    }
    if (options.limit_) {
      const std::size_t offset =
          options.offset_ < 0
              ? selected.size()
              : std::min<std::uint64_t>(options.offset_, selected.size());
      const std::size_t count =
          options.count_ < 0 ? selected.size() - offset
                             : std::min<std::uint64_t>(
                                   options.count_, selected.size() - offset);
      selected = std::vector<std::size_t>(selected.begin() + offset,
                                          selected.begin() + offset + count);
    }
    const bool remove = request.kind_ == CommandKind::kZRemRangeByRank ||
                        request.kind_ == CommandKind::kZRemRangeByScore ||
                        request.kind_ == CommandKind::kZRemRangeByLex;
    if (remove) {
      integer = selected.size();
      std::sort(selected.rbegin(), selected.rend());
      for (std::size_t i : selected) set.erase(set.begin() + i);
      return integer == 0
                 ? absl::StatusOr<storage::CompactValueUpdate>(NoChange())
                 : Changed(std::move(set));
    }
    for (std::size_t i : selected) {
      output.push_back(set[i].member_);
      if (options.with_scores_) output.push_back(FormatDouble(set[i].score_));
    }
    return NoChange();
  };

  absl::Status status =
      co_await RunCompact(request, digest, tx, read_only, callback);
  if (!status.ok()) co_return Built(StorageError(builder, status));

  switch (request.kind_) {
    case CommandKind::kZCard:
    case CommandKind::kZCount:
    case CommandKind::kZLexCount:
    case CommandKind::kZRem:
    case CommandKind::kZRemRangeByLex:
    case CommandKind::kZRemRangeByRank:
    case CommandKind::kZRemRangeByScore:
      co_return Built(builder.AppendInteger(integer));
    case CommandKind::kZScore:
      co_return Built(output.empty() || !output[0].has_value()
                          ? builder.AppendNull()
                          : builder.AppendDoubleText(*output[0]));
    case CommandKind::kZMScore:
      builder.AppendArrayHeader(output.size());
      for (const auto& item : output) {
        if (item)
          AppendScore(builder, *item);
        else
          builder.AppendNull();
      }
      co_return Built(builder.View());
    case CommandKind::kZRank:
    case CommandKind::kZRevRank:
      if (integer < 0) {
        co_return Built(a.size() == 4 ? builder.AppendNullArray()
                                      : builder.AppendNull());
      }
      if (a.size() == 4 && EqualCi(a[3], "withscore")) {
        builder.AppendArrayHeader(2);
        builder.AppendInteger(integer);
        AppendScore(builder, *scalar);
        co_return Built(builder.View());
      }
      co_return Built(builder.AppendInteger(integer));
    case CommandKind::kZRandMember:
      if (a.size() == 2)
        co_return Built(output.empty() ? builder.AppendNull()
                                       : builder.AppendBulkString(*output[0]));
      if (reply_with_scores) {
        const std::size_t pairs = output.size() / 2;
        builder.AppendArrayHeader(
            builder.version() == RespVersion::k3 ? pairs : output.size());
        for (std::size_t i = 0; i < pairs; ++i) {
          if (builder.version() == RespVersion::k3)
            builder.AppendArrayHeader(2);
          builder.AppendBulkString(*output[i * 2]);
          AppendScore(builder, *output[i * 2 + 1]);
        }
        co_return Built(builder.View());
      }
      break;
    case CommandKind::kZScan:
      builder.AppendArrayHeader(2);
      builder.AppendBulkString(std::to_string(next_cursor));
      builder.AppendArrayHeader(output.size());
      for (const auto& item : output) builder.AppendBulkString(*item);
      co_return Built(builder.View());
    case CommandKind::kZPopMin:
    case CommandKind::kZPopMax: {
      const std::size_t pairs = output.size() / 2;
      const bool nested = builder.version() == RespVersion::k3 && a.size() == 3;
      builder.AppendArrayHeader(nested ? pairs : output.size());
      for (std::size_t i = 0; i < pairs; ++i) {
        if (nested) builder.AppendArrayHeader(2);
        builder.AppendBulkString(*output[i * 2]);
        AppendScore(builder, *output[i * 2 + 1]);
      }
      co_return Built(builder.View());
    }
    default:
      break;
  }
  if (reply_with_scores) {
    const std::size_t pairs = output.size() / 2;
    builder.AppendArrayHeader(
        builder.version() == RespVersion::k3 ? pairs : output.size());
    for (std::size_t i = 0; i < pairs; ++i) {
      if (builder.version() == RespVersion::k3) builder.AppendArrayHeader(2);
      builder.AppendBulkString(*output[i * 2]);
      AppendScore(builder, *output[i * 2 + 1]);
    }
    co_return Built(builder.View());
  }
  builder.AppendArrayHeader(output.size());
  for (auto& item : output) {
    if (item)
      builder.AppendBulkString(*item);
    else
      builder.AppendNull();
  }
  co_return Built(builder.View());
}

enum class MultiAggregate { kDifference, kIntersection, kUnion };
enum class ScoreAggregate { kSum, kMin, kMax };

struct StoreShape {
  std::size_t destination_arg_ = 1;
  std::size_t source_arg_ = 2;
  bool distance_scores_ = false;
};

absl::StatusOr<std::optional<StoreShape>> ParseStoreShape(
    const CommandRequest& request) {
  if (request.kind_ == CommandKind::kZRangeStore ||
      request.kind_ == CommandKind::kGeoSearchStore) {
    return std::optional(StoreShape{});
  }
  if (request.kind_ != CommandKind::kGeoRadius &&
      request.kind_ != CommandKind::kGeoRadiusByMember) {
    return std::optional<StoreShape>{};
  }
  const auto& args = request.args_;
  const std::size_t begin = request.kind_ == CommandKind::kGeoRadius ? 6 : 5;
  std::optional<StoreShape> result;
  for (std::size_t i = begin; i < args.size(); ++i) {
    if (!EqualCi(args[i], "store") && !EqualCi(args[i], "storedist")) continue;
    if (result.has_value() || i + 1 >= args.size())
      return absl::InvalidArgumentError("syntax error");
    result = StoreShape{.destination_arg_ = i + 1,
                        .source_arg_ = 1,
                        .distance_scores_ = EqualCi(args[i], "storedist")};
    ++i;
  }
  return result;
}

absl::Status ComputeRangeStore(const CommandRequest& request,
                               const ZSet& source, ZSet* output) {
  const auto& args = request.args_;
  RangeOptions options;
  std::string_view min_text = args[3];
  std::string_view max_text = args[4];
  for (std::size_t i = 5; i < args.size();) {
    if (EqualCi(args[i], "byscore")) {
      options.mode_ = RangeOptions::Mode::kScore;
      ++i;
    } else if (EqualCi(args[i], "bylex")) {
      options.mode_ = RangeOptions::Mode::kLex;
      ++i;
    } else if (EqualCi(args[i], "rev")) {
      options.reverse_ = true;
      ++i;
    } else if (EqualCi(args[i], "limit")) {
      absl::Status parsed = ParseRangeLimit(args, i, &options);
      if (!parsed.ok()) return parsed;
      i += 3;
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }
  if (options.limit_ && options.mode_ == RangeOptions::Mode::kRank)
    return absl::InvalidArgumentError(
        "syntax error, LIMIT is only supported in combination with either "
        "BYSCORE or BYLEX");
  if (options.reverse_ && options.mode_ != RangeOptions::Mode::kRank)
    std::swap(min_text, max_text);

  std::vector<std::size_t> selected;
  if (options.mode_ == RangeOptions::Mode::kRank) {
    std::int64_t start = 0, stop = 0;
    if (!ParseInt(min_text, &start) || !ParseInt(max_text, &stop))
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    auto [begin, end] = RankSlice(start, stop, source.size());
    for (std::size_t i = begin; i < end; ++i)
      selected.push_back(options.reverse_ ? source.size() - 1 - i : i);
  } else if (options.mode_ == RangeOptions::Mode::kScore) {
    auto min = ParseScoreBound(min_text), max = ParseScoreBound(max_text);
    if (!min.ok()) return min.status();
    if (!max.ok()) return max.status();
    for (std::size_t i = 0; i < source.size(); ++i)
      if (AboveMin(source[i].score_, *min) && BelowMax(source[i].score_, *max))
        selected.push_back(i);
    if (options.reverse_) std::reverse(selected.begin(), selected.end());
  } else {
    auto min = ParseLexBound(min_text), max = ParseLexBound(max_text);
    if (!min.ok()) return min.status();
    if (!max.ok()) return max.status();
    selected.resize(source.size());
    std::iota(selected.begin(), selected.end(), 0);
    std::sort(selected.begin(), selected.end(),
              [&](std::size_t x, std::size_t y) {
                return source[x].member_ < source[y].member_;
              });
    std::erase_if(selected, [&](std::size_t i) {
      return !AboveMin(source[i].member_, *min) ||
             !BelowMax(source[i].member_, *max);
    });
    if (options.reverse_) std::reverse(selected.begin(), selected.end());
  }
  if (options.limit_) {
    const std::size_t offset =
        options.offset_ < 0
            ? selected.size()
            : std::min<std::uint64_t>(options.offset_, selected.size());
    const std::size_t count =
        options.count_ < 0
            ? selected.size() - offset
            : std::min<std::uint64_t>(options.count_, selected.size() - offset);
    selected = std::vector<std::size_t>(selected.begin() + offset,
                                        selected.begin() + offset + count);
  }
  output->clear();
  output->reserve(selected.size());
  for (std::size_t i : selected) output->push_back(source[i]);
  Sort(output);
  return absl::OkStatus();
}

struct GeoStoreQuery {
  bool center_by_member_ = false;
  std::string_view center_member_;
  double center_lon_ = 0;
  double center_lat_ = 0;
  bool box_ = false;
  double radius_m_ = 0;
  double box_width_m_ = 0;
  double box_height_m_ = 0;
  double unit_meters_ = 1;
  bool ascending_ = false;
  bool descending_ = false;
  bool any_ = false;
  std::optional<std::uint64_t> count_;
  bool distance_scores_ = false;
};

absl::StatusOr<GeoStoreQuery> ParseGeoStoreQuery(const CommandRequest& request,
                                                 const StoreShape& shape) {
  const auto& args = request.args_;
  GeoStoreQuery query;
  query.distance_scores_ = shape.distance_scores_;
  bool center = false, area = false;
  std::size_t i = 0;
  if (request.kind_ == CommandKind::kGeoRadius) {
    double radius = 0;
    auto unit = UnitMeters(args[5]);
    if (!ParseRedisDouble(args[2], &query.center_lon_) ||
        !ParseRedisDouble(args[3], &query.center_lat_) ||
        !ParseRedisDouble(args[4], &radius) ||
        !ValidGeoCoordinates(query.center_lon_, query.center_lat_) ||
        radius < 0 || !unit.ok())
      return absl::InvalidArgumentError(
          "invalid longitude,latitude pair or radius");
    query.unit_meters_ = *unit;
    query.radius_m_ = radius * *unit;
    center = area = true;
    i = 6;
  } else if (request.kind_ == CommandKind::kGeoRadiusByMember) {
    double radius = 0;
    auto unit = UnitMeters(args[4]);
    if (!ParseRedisDouble(args[3], &radius) || radius < 0 || !unit.ok())
      return absl::InvalidArgumentError("need numeric radius");
    query.center_by_member_ = true;
    query.center_member_ = args[2];
    query.unit_meters_ = *unit;
    query.radius_m_ = radius * *unit;
    center = area = true;
    i = 5;
  } else {
    i = 3;
  }
  for (; i < args.size();) {
    if ((EqualCi(args[i], "store") || EqualCi(args[i], "storedist")) &&
        request.kind_ != CommandKind::kGeoSearchStore && i + 1 < args.size()) {
      query.distance_scores_ = EqualCi(args[i], "storedist");
      i += 2;
    } else if (request.kind_ == CommandKind::kGeoSearchStore &&
               EqualCi(args[i], "storedist")) {
      query.distance_scores_ = true;
      ++i;
    } else if (EqualCi(args[i], "frommember") && !center &&
               i + 1 < args.size()) {
      query.center_by_member_ = true;
      query.center_member_ = args[i + 1];
      center = true;
      i += 2;
    } else if (EqualCi(args[i], "fromlonlat") && !center &&
               i + 2 < args.size() &&
               ParseRedisDouble(args[i + 1], &query.center_lon_) &&
               ParseRedisDouble(args[i + 2], &query.center_lat_)) {
      if (!ValidGeoCoordinates(query.center_lon_, query.center_lat_))
        return absl::InvalidArgumentError("invalid longitude,latitude pair");
      center = true;
      i += 3;
    } else if (EqualCi(args[i], "byradius") && !area && i + 2 < args.size()) {
      double radius = 0;
      auto unit = UnitMeters(args[i + 2]);
      if (!ParseRedisDouble(args[i + 1], &radius) || radius < 0 || !unit.ok())
        return absl::InvalidArgumentError("syntax error");
      query.unit_meters_ = *unit;
      query.radius_m_ = radius * *unit;
      area = true;
      i += 3;
    } else if (EqualCi(args[i], "bybox") && !area && i + 3 < args.size()) {
      double width = 0, height = 0;
      auto unit = UnitMeters(args[i + 3]);
      if (!ParseRedisDouble(args[i + 1], &width) ||
          !ParseRedisDouble(args[i + 2], &height) || width < 0 || height < 0 ||
          !unit.ok())
        return absl::InvalidArgumentError("syntax error");
      query.unit_meters_ = *unit;
      query.box_width_m_ = width * *unit;
      query.box_height_m_ = height * *unit;
      query.box_ = true;
      area = true;
      i += 4;
    } else if (EqualCi(args[i], "count") && i + 1 < args.size()) {
      std::uint64_t count = 0;
      if (!ParseInt(args[i + 1], &count) || count == 0)
        return absl::InvalidArgumentError("COUNT must be > 0");
      query.count_ = count;
      i += 2;
    } else if (EqualCi(args[i], "any")) {
      query.any_ = true;
      ++i;
    } else if (EqualCi(args[i], "asc")) {
      query.ascending_ = true;
      ++i;
    } else if (EqualCi(args[i], "desc")) {
      query.descending_ = true;
      ++i;
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }
  if (!center || !area || (query.ascending_ && query.descending_))
    return absl::InvalidArgumentError("syntax error");
  if (query.any_ && !query.count_)
    return absl::InvalidArgumentError(
        "the ANY argument requires COUNT argument");
  return query;
}

absl::Status ComputeGeoStore(const ZSet& source, GeoStoreQuery query,
                             ZSet* output) {
  output->clear();
  if (source.empty()) return absl::OkStatus();
  if (query.center_by_member_) {
    const Element* center = Find(source, query.center_member_);
    if (!center)
      return absl::InvalidArgumentError(
          "could not decode requested zset member");
    const auto hash = DecodeGeoScore(center->score_);
    if (!hash)
      return absl::InvalidArgumentError(
          "could not decode requested zset member");
    std::tie(query.center_lon_, query.center_lat_) = GeoDecode(*hash);
  }
  struct Match {
    const Element* element_;
    double distance_;
  };
  std::vector<Match> matches;
  for (const Element& element : source) {
    const auto hash = DecodeGeoScore(element.score_);
    if (!hash) continue;
    const auto [lon, lat] = GeoDecode(*hash);
    const double distance =
        GeoDistance(query.center_lon_, query.center_lat_, lon, lat);
    bool inside = distance <= query.radius_m_;
    if (query.box_) {
      const double north = GeoDistance(query.center_lon_, query.center_lat_,
                                       query.center_lon_, lat);
      const double east = GeoDistance(query.center_lon_, lat, lon, lat);
      inside =
          east <= query.box_width_m_ / 2 && north <= query.box_height_m_ / 2;
    }
    if (inside) matches.push_back(Match{&element, distance});
  }
  if (query.ascending_ || query.descending_ || (query.count_ && !query.any_)) {
    std::sort(matches.begin(), matches.end(),
              [&](const Match& x, const Match& y) {
                return query.descending_ ? x.distance_ > y.distance_
                                         : x.distance_ < y.distance_;
              });
  }
  if (query.count_ && matches.size() > *query.count_)
    matches.resize(*query.count_);
  output->reserve(matches.size());
  for (const Match& match : matches) {
    output->push_back(Element{match.element_->member_,
                              query.distance_scores_
                                  ? match.distance_ / query.unit_meters_
                                  : match.element_->score_});
  }
  Sort(output);
  return absl::OkStatus();
}

struct ZSetInputCharge {
  RetainedMemoryCharge decoded_;
  RetainedMemoryCharge inherited_;
};

absl::StatusOr<MemoryReservation> ReserveZSetScratch(std::size_t bytes,
                                                     std::size_t count,
                                                     std::size_t copies = 1) {
  constexpr auto limit = std::numeric_limits<std::size_t>::max();
  if (copies == 0 || bytes > limit - 4096 ||
      count > (limit - bytes - 4096) / 512 ||
      bytes + 4096 + count * 512 > limit / copies)
    return absl::ResourceExhaustedError(
        "OOM Sorted Set aggregate size overflow");
  auto admission = TryReserveMemory((bytes + 4096 + count * 512) * copies);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM Sorted Set aggregate admission");
  }
  return std::move(*admission);
}

absl::StatusOr<std::size_t> ZSetRetainedBytes(const ZSet& value) {
  constexpr auto limit = std::numeric_limits<std::size_t>::max();
  if (value.capacity() > limit / sizeof(Element))
    return absl::ResourceExhaustedError("OOM Sorted Set vector size overflow");
  std::size_t bytes = value.capacity() * sizeof(Element);
  for (const auto& element : value) {
    if (element.member_.capacity() >= limit - bytes)
      return absl::ResourceExhaustedError(
          "OOM Sorted Set string size overflow");
    bytes += element.member_.capacity() + 1;
  }
  return bytes;
}

struct MultiContext {
  // These owners can be populated on source/destination workers and destroyed
  // by the coordinator. A local MemoryReservation cannot cross that boundary.
  RetainedMemoryCharge metadata_charge_;
  RetainedMemoryCharge output_charge_;
  std::vector<ZSetInputCharge> input_charges_;
  const CommandRequest* request_ = nullptr;
  std::vector<ZSet> inputs_;
  ZSet output_;
  std::vector<CapturedReplicationCommand> replacement_effects_;
  std::vector<std::string> replacement_args_;
  std::vector<double> weights_;
  std::vector<storage::TxShardWrites> writes_;
  MultiAggregate aggregate_ = MultiAggregate::kUnion;
  ScoreAggregate score_aggregate_ = ScoreAggregate::kSum;
  std::size_t first_source_ = 0;
  std::size_t last_source_ = 0;
  bool store_ = false;
  bool single_shard_ = false;
  bool rollback_ = false;
  bool rollback_failed_ = false;
  bool borrowed_tx_ = false;
  std::optional<StoreShape> store_shape_;
};

absl::Status PrepareMultiContext(MultiContext* context, std::size_t arguments) {
  try {
    constexpr auto slot_bytes =
        sizeof(ZSet) + sizeof(ZSetInputCharge) + sizeof(double);
    const auto workers = g_storage->worker_count();
    constexpr auto limit = std::numeric_limits<std::size_t>::max();
    if (arguments > limit / slot_bytes ||
        workers >
            (limit - arguments * slot_bytes) / sizeof(storage::TxShardWrites))
      return absl::ResourceExhaustedError(
          "OOM Sorted Set context size overflow");
    auto admission = ReserveZSetScratch(
        arguments * slot_bytes + workers * sizeof(storage::TxShardWrites), 0,
        2);
    if (!admission.ok()) return admission.status();
    context->inputs_.resize(arguments);
    context->input_charges_.resize(arguments);
    context->weights_.reserve(arguments);
    context->writes_.reserve(workers);
    context->metadata_charge_.Adopt(
        &*admission,
        context->inputs_.capacity() * sizeof(ZSet) +
            context->input_charges_.capacity() * sizeof(ZSetInputCharge) +
            context->weights_.capacity() * sizeof(double) +
            context->writes_.capacity() * sizeof(storage::TxShardWrites));
    return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError("OOM Sorted Set context allocation");
  }
}

void ClearMultiPayloads(MultiContext* context) noexcept {
  // Called only after shard work joins, never concurrently by finish hops.
  // Undo may need admission on the same worker that held these large copies.
  for (auto& input : context->inputs_) ZSet{}.swap(input);
  for (auto& charge : context->input_charges_) {
    charge.decoded_.Reset();
    charge.inherited_.Reset();
  }
  ZSet{}.swap(context->output_);
  std::vector<CapturedReplicationCommand>{}.swap(context->replacement_effects_);
  std::vector<std::string>{}.swap(context->replacement_args_);
  context->output_charge_.Reset();
}

absl::Status PrepareMultiCapture(const MultiContext& context) {
  const auto& capture = context.request_->replication_capture_;
  if (!capture) return absl::OkStatus();
  std::size_t bytes = 0;
  constexpr auto limit = std::numeric_limits<std::size_t>::max();
  for (const auto& effect : context.replacement_effects_) {
    if (effect.args_.capacity() > (limit - bytes) / sizeof(std::string))
      return absl::ResourceExhaustedError(
          "OOM Sorted Set capture size overflow");
    bytes += effect.args_.capacity() * sizeof(std::string);
    for (const auto& argument : effect.args_) {
      if (argument.capacity() >= limit - bytes)
        return absl::ResourceExhaustedError(
            "OOM Sorted Set capture size overflow");
      bytes += argument.capacity() + 1;
    }
  }
  return capture->ReserveAdditionalCommands(context.replacement_effects_.size(),
                                            bytes);
}

std::vector<CapturedReplicationCommand> BuildZSetReplacement(
    const CommandRequest& request, std::size_t destination_arg,
    const ZSet& output) {
  std::vector<CapturedReplicationCommand> effects;
  effects.reserve(output.empty() ? 1 : 2);
  effects.push_back(CapturedReplicationCommand{
      request.db_id_, {"DEL", request.args_[destination_arg]}});
  if (!output.empty()) {
    std::vector<std::string> add{"ZADD", request.args_[destination_arg]};
    add.reserve(2 + output.size() * 2);
    for (const Element& element : output) {
      add.push_back(FormatDouble(element.score_));
      add.push_back(element.member_);
    }
    effects.push_back(
        CapturedReplicationCommand{request.db_id_, std::move(add)});
  }
  return effects;
}

storage::TxShardWrites* LocalWrites(MultiContext& context) {
  return context.writes_.empty() ? nullptr
                                 : &context.writes_[bycorf::ThisWorker().id_];
}

absl::Status ComputeMultiUnchecked(MultiContext* context) {
  context->output_.clear();
  if (context->request_->kind_ == CommandKind::kZRangeStore) {
    return ComputeRangeStore(*context->request_,
                             context->inputs_[context->first_source_],
                             &context->output_);
  }
  if (context->request_->kind_ == CommandKind::kGeoSearchStore ||
      context->request_->kind_ == CommandKind::kGeoRadius ||
      context->request_->kind_ == CommandKind::kGeoRadiusByMember) {
    if (!context->store_shape_)
      return absl::InternalError("missing GEO STORE destination");
    auto query = ParseGeoStoreQuery(*context->request_, *context->store_shape_);
    if (!query.ok()) return query.status();
    return ComputeGeoStore(context->inputs_[context->first_source_], *query,
                           &context->output_);
  }
  if (context->first_source_ > context->last_source_) return absl::OkStatus();
  const auto source_offset = [&](std::size_t arg) {
    return arg - context->first_source_;
  };
  const auto redis_aggregate_score = [](double score) {
    return std::isnan(score) ? 0.0 : score;
  };
  std::map<std::string, double> scores;
  const ZSet& first = context->inputs_[context->first_source_];
  for (const Element& element : first) {
    scores[element.member_] = redis_aggregate_score(
        element.score_ *
        context->weights_[source_offset(context->first_source_)]);
  }
  if (context->aggregate_ == MultiAggregate::kDifference) {
    for (std::size_t arg = context->first_source_ + 1;
         arg <= context->last_source_; ++arg) {
      for (const Element& element : context->inputs_[arg]) {
        scores.erase(element.member_);
      }
    }
  } else if (context->aggregate_ == MultiAggregate::kIntersection) {
    for (std::size_t arg = context->first_source_ + 1;
         arg <= context->last_source_; ++arg) {
      const ZSet& input = context->inputs_[arg];
      for (auto it = scores.begin(); it != scores.end();) {
        const Element* element = Find(input, it->first);
        if (!element) {
          it = scores.erase(it);
          continue;
        }
        // Redis 7.2 applies its historical NaN handling after combining
        // subsequent intersection sources, unlike the union path below.
        const double weighted =
            element->score_ * context->weights_[source_offset(arg)];
        if (context->score_aggregate_ == ScoreAggregate::kSum)
          it->second += weighted;
        else if (context->score_aggregate_ == ScoreAggregate::kMin)
          it->second = std::min(it->second, weighted);
        else
          it->second = std::max(it->second, weighted);
        it->second = redis_aggregate_score(it->second);
        ++it;
      }
    }
  } else {
    for (std::size_t arg = context->first_source_ + 1;
         arg <= context->last_source_; ++arg) {
      for (const Element& element : context->inputs_[arg]) {
        const double weighted = redis_aggregate_score(
            element.score_ * context->weights_[source_offset(arg)]);
        auto [it, inserted] = scores.try_emplace(
            element.member_, redis_aggregate_score(weighted));
        if (!inserted) {
          if (context->score_aggregate_ == ScoreAggregate::kSum)
            it->second += weighted;
          else if (context->score_aggregate_ == ScoreAggregate::kMin)
            it->second = std::min(it->second, weighted);
          else
            it->second = std::max(it->second, weighted);
          it->second = redis_aggregate_score(it->second);
        }
      }
    }
  }
  for (auto& [member, score] : scores) {
    context->output_.push_back(Element{std::move(member), score});
  }
  Sort(&context->output_);
  return absl::OkStatus();
}

absl::Status ComputeMulti(MultiContext* context) {
  try {
    std::size_t bytes = 0, count = 0;
    constexpr auto limit = std::numeric_limits<std::size_t>::max();
    for (const auto& input : context->inputs_) {
      const auto retained = ZSetRetainedBytes(input);
      if (!retained.ok()) return retained.status();
      if (*retained > limit - bytes || input.size() > limit - count)
        return absl::ResourceExhaustedError(
            "OOM Sorted Set aggregate size overflow");
      bytes += *retained;
      count += input.size();
    }
    // Covers score-map keys/nodes, selection indexes, output copies and
    // canonical replication effects before any destination is deleted.
    auto admission = ReserveZSetScratch(bytes, count, 4);
    if (!admission.ok()) return admission.status();
    struct ClearOnFailure {
      MultiContext* context_;
      bool published_ = false;
      ~ClearOnFailure() {
        if (published_) return;
        ZSet{}.swap(context_->output_);
        std::vector<CapturedReplicationCommand>{}.swap(
            context_->replacement_effects_);
        std::vector<std::string>{}.swap(context_->replacement_args_);
      }
    } cleanup{context};
    auto computed = ComputeMultiUnchecked(context);
    if (!computed.ok()) return computed;
    if (context->store_) {
      auto effects = BuildZSetReplacement(
          *context->request_, context->store_shape_->destination_arg_,
          context->output_);
      if (context->borrowed_tx_)
        context->replacement_effects_ = std::move(effects);
      else
        context->replacement_args_ =
            EncodeReplicationCommandEffects(std::move(effects));
    }
    auto retained = ZSetRetainedBytes(context->output_);
    if (!retained.ok()) return retained.status();
    auto add_strings = [&](const std::vector<std::string>& strings) {
      if (strings.capacity() > (limit - *retained) / sizeof(std::string))
        return false;
      *retained += strings.capacity() * sizeof(std::string);
      for (const auto& string : strings) {
        if (string.capacity() >= limit - *retained) return false;
        *retained += string.capacity() + 1;
      }
      return true;
    };
    if (!add_strings(context->replacement_args_))
      return absl::ResourceExhaustedError(
          "OOM Sorted Set replication size overflow");
    if (context->replacement_effects_.capacity() >
        (limit - *retained) / sizeof(CapturedReplicationCommand))
      return absl::ResourceExhaustedError(
          "OOM Sorted Set replication size overflow");
    *retained += context->replacement_effects_.capacity() *
                 sizeof(CapturedReplicationCommand);
    for (const auto& effect : context->replacement_effects_)
      if (!add_strings(effect.args_))
        return absl::ResourceExhaustedError(
            "OOM Sorted Set replication size overflow");
    if (admission->bytes() && *retained > admission->bytes())
      return absl::ResourceExhaustedError(
          "OOM Sorted Set output exceeds admission");
    context->output_charge_.Adopt(&*admission, *retained);
    cleanup.published_ = true;
    return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError("OOM Sorted Set aggregate allocation");
  }
}

Task<absl::Status> ReplaceMultiDestination(
    MultiContext* context, const storage::Digest& digest,
    storage::TxShardWrites* writes = nullptr) {
  try {
    const auto& request = *context->request_;
    const std::size_t destination_arg =
        context->store_shape_ ? context->store_shape_->destination_arg_ : 1;
    const std::string& destination = request.args_[destination_arg];
    if (writes == nullptr) writes = LocalWrites(*context);
    const auto bytes = ZSetRetainedBytes(context->output_);
    if (!bytes.ok()) co_return bytes.status();
    auto admission = ReserveZSetScratch(*bytes, context->output_.size(), 6);
    if (!admission.ok()) co_return admission.status();
    // Encode and admit destination-local decode/planner headroom before DEL.
    // Errors after DEL are returned to the existing transaction undo owner.
    auto encoded = Encode(context->output_);
    if (!encoded.ok()) co_return encoded.status();
    auto deleted = co_await g_storage->DeleteLocked(request.db_id_, destination,
                                                    digest, writes);
    if (!deleted.ok()) co_return deleted.status();
    if (context->output_.empty()) co_return absl::OkStatus();
    auto callback = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      if (value) {
        return absl::InternalError(
            "sorted-set STORE destination was not replaced");
      }
      return storage::CompactValueUpdate{
          .changed_ = true,
          .encoded_ = std::move(*encoded),
          .logical_size_ = context->output_.size(),
          .expire_at_ms_ = std::nullopt};
    };
    co_return co_await g_storage->ExecuteCompactLocked(
        request.db_id_, destination, digest, storage::ValueType::kSortedSet,
        false, callback, writes);
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError("OOM Sorted Set STORE allocation");
  }
}

absl::Status DecodeRetainedInput(std::optional<storage::CompactValueView> value,
                                 ZSet* input, ZSetInputCharge* charge) {
  auto admission = ReserveZSetScratch(value ? value->encoded_.size() : 0,
                                      value ? value->logical_size_ : 0);
  if (!admission.ok()) return admission.status();
  auto decoded = Decode(value);
  if (!decoded.ok()) return decoded.status();
  const auto retained = ZSetRetainedBytes(*decoded);
  if (!retained.ok()) return retained.status();
  if (admission->bytes() && *retained > admission->bytes())
    return absl::ResourceExhaustedError(
        "OOM Sorted Set input exceeds admission");
  *input = std::move(*decoded);
  charge->decoded_.Adopt(&*admission, *retained);
  return absl::OkStatus();
}

Task<absl::StatusOr<ZSet>> ReadAggregateInputLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest,
    ZSetInputCharge* charge) {
  try {
    ZSet input;
    auto callback = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto decoded = DecodeRetainedInput(value, &input, charge);
      if (!decoded.ok()) return decoded;
      return NoChange();
    };
    absl::Status status = co_await g_storage->ExecuteCompactLocked(
        db_id, key, digest, storage::ValueType::kSortedSet, true, callback);
    if (status.ok()) co_return input;
    if (!status.message().starts_with("WRONGTYPE ")) co_return status;

    // Redis ZUNION/ZINTER/ZDIFF accept Set inputs and assign every Set member
    // the implicit score 1.0.
    storage::HashOperation operation;
    operation.kind_ = storage::HashOperationKind::kKeys;
    auto members =
        co_await g_storage->ExecuteSetLocked(db_id, key, digest, operation);
    if (!members.ok()) co_return members.status();
    std::size_t string_bytes = 0;
    for (const auto& member : members->values_) {
      if (!member)
        co_return absl::InternalError(
            "Set aggregate source has missing member");
      if (member->capacity() >=
          std::numeric_limits<std::size_t>::max() - string_bytes)
        co_return absl::ResourceExhaustedError(
            "OOM Set aggregate size overflow");
      string_bytes += member->capacity() + 1;
    }
    const bool transfer = members->retained_charge_.bytes() >= string_bytes;
    auto admission = ReserveZSetScratch(transfer ? 0 : string_bytes,
                                        members->values_.size());
    if (!admission.ok()) co_return admission.status();
    input.reserve(members->values_.size());
    for (auto& member : members->values_) {
      if (!member.has_value())
        co_return absl::InternalError(
            "Set aggregate source has missing member");
      input.push_back(Element{std::move(*member), 1.0});
    }
    if (transfer) charge->inherited_ = std::move(members->retained_charge_);
    charge->decoded_.Adopt(&*admission, input.capacity() * sizeof(Element) +
                                            (transfer ? 0 : string_bytes));
    co_return input;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM Sorted Set aggregate input allocation");
  }
}

Task<absl::StatusOr<ZSet>> ReadZSetOnlyLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest,
    ZSetInputCharge* retained = nullptr) {
  try {
    ZSetInputCharge local_charge;
    auto* charge = retained ? retained : &local_charge;
    ZSet input;
    auto callback = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto decoded = DecodeRetainedInput(value, &input, charge);
      if (!decoded.ok()) return decoded;
      return NoChange();
    };
    absl::Status status = co_await g_storage->ExecuteCompactLocked(
        db_id, key, digest, storage::ValueType::kSortedSet, true, callback);
    if (!status.ok()) co_return status;
    co_return input;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM Sorted Set aggregate input allocation");
  }
}

Task<absl::Status> MultiReadShard(void* opaque, const tx::ShardSlice& slice) {
  auto* context = static_cast<MultiContext*>(opaque);
  const auto& request = *context->request_;
  for (const tx::TxKey& key : slice.keys_) {
    if (context->store_ && context->store_shape_ &&
        key.arg_index_ == context->store_shape_->destination_arg_)
      continue;
    const bool zset_only = request.kind_ == CommandKind::kZRangeStore ||
                           request.kind_ == CommandKind::kGeoSearchStore ||
                           request.kind_ == CommandKind::kGeoRadius ||
                           request.kind_ == CommandKind::kGeoRadiusByMember;
    absl::StatusOr<ZSet> input;
    if (zset_only) {
      input = co_await ReadZSetOnlyLocked(
          request.db_id_, request.args_[key.arg_index_], key.digest_,
          &context->input_charges_[key.arg_index_]);
    } else {
      input = co_await ReadAggregateInputLocked(
          request.db_id_, request.args_[key.arg_index_], key.digest_,
          &context->input_charges_[key.arg_index_]);
    }
    if (!input.ok()) co_return input.status();
    context->inputs_[key.arg_index_] = std::move(*input);
  }
  if (context->single_shard_ && context->store_) {
    absl::Status computed = ComputeMulti(context);
    if (!computed.ok()) co_return computed;
    const storage::Digest destination = storage::ComputeDigest(
        request.args_[context->store_shape_->destination_arg_]);
    absl::Status replaced =
        co_await ReplaceMultiDestination(context, destination);
    if (!replaced.ok()) {
      ClearMultiPayloads(context);
      if (!context->writes_.empty()) {
        const auto rolled_back =
            co_await g_storage->RollbackTxLocal(context->writes_.front().txid_);
        if (!rolled_back.ok()) {
          context->rollback_failed_ = true;
          co_return rolled_back;
        }
      }
      co_return replaced;
    }
    if (!context->writes_.empty())
      co_return co_await g_storage->DiscardTxUndoLocal(
          context->writes_.front().txid_);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> MultiWriteShard(void* opaque, const tx::ShardSlice& slice) {
  auto* context = static_cast<MultiContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    if (context->store_shape_ &&
        key.arg_index_ == context->store_shape_->destination_arg_)
      co_return co_await ReplaceMultiDestination(context, key.digest_);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> MultiFinishShard(void* opaque, const tx::ShardSlice&) {
  auto* context = static_cast<MultiContext*>(opaque);
  if (context->writes_.empty()) co_return absl::OkStatus();
  const std::uint64_t txid = context->writes_.front().txid_;
  if (context->rollback_) co_return co_await g_storage->RollbackTxLocal(txid);
  co_return co_await g_storage->DiscardTxUndoLocal(txid);
}

Task<absl::Status> CommitMulti(std::uint64_t txid,
                               std::vector<storage::TxShardWrites> writes) {
  struct Done {
    ~Done() { g_storage->NoteTxCommitFinished(); }
  } done;
  std::vector<storage::TxShardWrites*> shards;
  for (auto& shard : writes)
    if (!shard.fences_.empty() || !shard.retirements_.empty())
      shards.push_back(&shard);
  if (shards.empty()) co_return absl::OkStatus();
  co_return co_await g_storage->CommitTxWrites(txid, std::move(shards));
}

}  // namespace

void InitZSetCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<absl::StatusOr<storage::HashResult>> ZSetRandomSnapshotLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest,
    bool with_scores, storage::TxShardWrites* tx, std::uint64_t now_ms) {
  try {
    storage::HashResult result;
    auto callback = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto admission = ReserveZSetScratch(value ? value->encoded_.size() : 0,
                                          value ? value->logical_size_ : 0, 2);
      if (!admission.ok()) return admission.status();
      auto decoded = Decode(value);
      if (!decoded.ok()) return decoded.status();
      result.key_exists_ = value.has_value();
      result.length_ = decoded->size();
      result.values_.reserve(decoded->size() * (with_scores ? 2 : 1));
      for (const Element& element : *decoded) {
        result.values_.emplace_back(element.member_);
        if (with_scores) {
          result.values_.emplace_back(FormatDouble(element.score_));
        }
      }
      std::size_t retained =
          result.values_.capacity() * sizeof(result.values_[0]);
      for (const auto& item : result.values_) {
        if (!item) continue;
        if (item->capacity() >=
            std::numeric_limits<std::size_t>::max() - retained)
          return absl::ResourceExhaustedError(
              "OOM Sorted Set random snapshot size overflow");
        retained += item->capacity() + 1;
      }
      if (admission->bytes() && retained > admission->bytes())
        return absl::ResourceExhaustedError(
            "OOM Sorted Set random snapshot exceeds admission");
      result.retained_charge_.Adopt(&*admission, retained);
      return NoChange();
    };
    absl::Status status = co_await g_storage->ExecuteCompactLocked(
        db_id, key, digest, storage::ValueType::kSortedSet, true, callback, tx,
        now_ms);
    if (!status.ok()) co_return status;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM Sorted Set random snapshot allocation");
  }
}

Task<CommandReply> ExecuteZSetCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder) {
  return ExecuteImpl(request, nullptr, nullptr, reply_builder);
}

Task<absl::StatusOr<std::vector<std::string>>> ZSetMembersSnapshotLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest) {
  auto elements = co_await ReadZSetOnlyLocked(db_id, key, digest);
  if (!elements.ok()) co_return elements.status();
  Sort(&*elements);
  std::vector<std::string> members;
  members.reserve(elements->size());
  for (auto& element : *elements) {
    members.push_back(std::move(element.member_));
  }
  co_return members;
}

Task<CommandReply> ExecuteZSetCommandLocked(const CommandRequest& request,
                                            const storage::Digest& digest,
                                            storage::TxShardWrites* tx,
                                            ReplyBuilder& reply_builder) {
  return ExecuteImpl(request, &digest, tx, reply_builder);
}

Task<CommandReply> ExecuteZSetMultiKey(const CommandRequest& request,
                                       ReplyBuilder& builder) {
  if (request.kind_ == CommandKind::kZMPop) {
    co_return co_await ExecuteZSetMultiPopAttempt(request, builder);
  }
  const auto& args = request.args_;
  auto parsed_store = ParseStoreShape(request);
  if (!parsed_store.ok())
    co_return Built(builder.AppendError(
        absl::StrCat("ERR ", parsed_store.status().message())));
  if ((request.kind_ == CommandKind::kGeoRadius ||
       request.kind_ == CommandKind::kGeoRadiusByMember) &&
      !parsed_store->has_value()) {
    const unsigned owner = g_storage->OwnerForKey(args[1]);
    if (owner == bycorf::ThisWorker().id_)
      co_return co_await ExecuteZSetCommand(request, builder);
    co_return co_await bycorf::SubmitTaskTo(
        owner, [&request, &builder]() -> Task<CommandReply> {
          co_return co_await ExecuteZSetCommand(request, builder);
        });
  }
  if (request.kind_ == CommandKind::kZRangeStore) {
    ZSet ignored;
    absl::Status syntax = ComputeRangeStore(request, ZSet{}, &ignored);
    if (!syntax.ok())
      co_return Built(
          builder.AppendError(absl::StrCat("ERR ", syntax.message())));
  } else if (request.kind_ == CommandKind::kGeoSearchStore ||
             request.kind_ == CommandKind::kGeoRadius ||
             request.kind_ == CommandKind::kGeoRadiusByMember) {
    auto syntax = ParseGeoStoreQuery(request, **parsed_store);
    if (!syntax.ok())
      co_return Built(
          builder.AppendError(absl::StrCat("ERR ", syntax.status().message())));
  }
  auto keys = DetermineKeys(*request.spec_, args);
  if (!keys.ok())
    co_return Built(
        builder.AppendError(absl::StrCat("ERR ", keys.status().message())));

  MultiContext context;
  context.request_ = &request;
  const auto prepared_context = PrepareMultiContext(&context, args.size());
  if (!prepared_context.ok())
    co_return Built(StorageError(builder, prepared_context));
  context.store_shape_ = std::move(*parsed_store);
  const bool aggregate_store = request.kind_ == CommandKind::kZDiffStore ||
                               request.kind_ == CommandKind::kZInterStore ||
                               request.kind_ == CommandKind::kZUnionStore;
  if (aggregate_store) context.store_shape_ = StoreShape{};
  if (aggregate_store) {
    context.first_source_ = keys->first_;
    context.last_source_ = keys->last_;
  } else if (context.store_shape_) {
    context.first_source_ = context.store_shape_->source_arg_;
    context.last_source_ = context.store_shape_->source_arg_;
  } else {
    context.first_source_ = keys->first_;
    context.last_source_ = keys->last_;
  }
  context.store_ = request.kind_ == CommandKind::kZDiffStore ||
                   request.kind_ == CommandKind::kZInterStore ||
                   request.kind_ == CommandKind::kZUnionStore ||
                   context.store_shape_.has_value();
  if (request.kind_ == CommandKind::kZDiff ||
      request.kind_ == CommandKind::kZDiffStore)
    context.aggregate_ = MultiAggregate::kDifference;
  else if (request.kind_ == CommandKind::kZInter ||
           request.kind_ == CommandKind::kZInterCard ||
           request.kind_ == CommandKind::kZInterStore)
    context.aggregate_ = MultiAggregate::kIntersection;

  const bool specialized_store =
      request.kind_ == CommandKind::kZRangeStore ||
      request.kind_ == CommandKind::kGeoSearchStore ||
      request.kind_ == CommandKind::kGeoRadius ||
      request.kind_ == CommandKind::kGeoRadiusByMember;
  const std::size_t key_count = specialized_store ? 1 : keys->count();
  context.weights_.assign(key_count, 1.0);
  bool with_scores = false;
  std::uint64_t limit = 0;
  for (std::size_t i = aggregate_store || !context.store_ ? keys->last_ + 1
                                                          : args.size();
       i < args.size();) {
    if (EqualCi(args[i], "withscores") && !context.store_ &&
        request.kind_ != CommandKind::kZInterCard) {
      with_scores = true;
      ++i;
    } else if (EqualCi(args[i], "weights") &&
               context.aggregate_ != MultiAggregate::kDifference &&
               request.kind_ != CommandKind::kZInterCard &&
               i + key_count < args.size()) {
      for (std::size_t w = 0; w < key_count; ++w) {
        if (!ParseRedisDouble(args[i + 1 + w], &context.weights_[w], true))
          co_return Built(
              builder.AppendError("ERR weight value is not a float"));
      }
      i += 1 + key_count;
    } else if (EqualCi(args[i], "aggregate") && i + 1 < args.size() &&
               context.aggregate_ != MultiAggregate::kDifference &&
               request.kind_ != CommandKind::kZInterCard) {
      if (EqualCi(args[i + 1], "sum"))
        context.score_aggregate_ = ScoreAggregate::kSum;
      else if (EqualCi(args[i + 1], "min"))
        context.score_aggregate_ = ScoreAggregate::kMin;
      else if (EqualCi(args[i + 1], "max"))
        context.score_aggregate_ = ScoreAggregate::kMax;
      else
        co_return Built(builder.AppendError("ERR syntax error"));
      i += 2;
    } else if (request.kind_ == CommandKind::kZInterCard &&
               EqualCi(args[i], "limit") && i + 1 < args.size()) {
      std::int64_t parsed_limit = 0;
      if (!ParseInt(args[i + 1], &parsed_limit)) {
        co_return Built(builder.AppendError("ERR LIMIT can't be negative"));
      }
      if (parsed_limit < 0) {
        co_return Built(builder.AppendError("ERR LIMIT can't be negative"));
      }
      limit = static_cast<std::uint64_t>(parsed_limit);
      i += 2;
    } else {
      co_return Built(builder.AppendError("ERR syntax error"));
    }
  }

  tx::Transaction transaction;
  if (context.store_) {
    const std::size_t destination = context.store_shape_->destination_arg_;
    transaction.AddKey(g_storage->OwnerForKey(args[destination]),
                       request.db_id_,
                       storage::ComputeDigest(args[destination]), destination,
                       tx::LockMode::kExclusive);
  }
  for (std::size_t i = context.first_source_; i <= context.last_source_; ++i) {
    transaction.AddKey(g_storage->OwnerForKey(args[i]), request.db_id_,
                       storage::ComputeDigest(args[i]), i,
                       tx::LockMode::kShared);
  }
  transaction.Seal();
  // Cluster owner-side re-check: only STORE forms mutate.
  // Single-shard stores write inside the read callback hop; multi-shard
  // stores validate again before the write hop and clear before the settle.
  ClusterShardValidatorContext cluster_validator;
  if (context.store_) {
    InstallClusterShardValidator(transaction, request, cluster_validator);
  }
  ReplicationTransactionGuard replication(request, &transaction);
  if (!replication.status().ok()) {
    co_return Built(StorageError(builder, replication.status()));
  }
  context.single_shard_ = transaction.single_shard();
  std::uint64_t txid = 0;
  if (context.store_) {
    txid = storage::StorageEngine::AllocateWriteTxid();
    context.writes_.resize(g_storage->worker_count());
    g_storage->InitializeTxWrites(txid, context.writes_,
                                  ClusterMutationPrecondition(request));
    for (auto& write : context.writes_) {
      write.collect_undo_ = true;
    }
  }
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) co_return Built(StorageError(builder, status));
  status = co_await transaction.Execute(
      &MultiReadShard, &context, !context.store_ || context.single_shard_);
  if (status.ok() && !context.store_) status = ComputeMulti(&context);
  if (status.ok() && context.store_ && !context.single_shard_) {
    status = ComputeMulti(&context);
    if (status.ok())
      status = co_await transaction.Execute(&MultiWriteShard, &context, false);
    context.rollback_ = !status.ok();
    if (context.rollback_) ClearMultiPayloads(&context);
    // The finish hop settles (or rolls back) what the write hop did; it must
    // not be fenced off by an authority change the write hop already beat.
    transaction.SetShardValidator(nullptr, nullptr);
    absl::Status finished =
        co_await transaction.Execute(&MultiFinishShard, &context, true);
    if (!finished.ok()) {
      context.rollback_failed_ = context.rollback_;
      status = finished;
    }
  }
  if (!status.ok()) {
    ClearMultiPayloads(&context);
    if (!context.rollback_failed_ &&
        cluster_validator.tripped_.load(std::memory_order_relaxed)) {
      if (context.store_ && !context.single_shard_ &&
          !transaction.releasing()) {
        (void)co_await transaction.Release();
      }
      co_return ClusterValidatorFailureReply(transaction, cluster_validator,
                                             request.connection_tls_, builder);
    }
    if (context.store_ && !context.single_shard_ && !transaction.releasing())
      (void)co_await transaction.Release();
    co_return Built(StorageError(builder, status));
  }
  if (context.store_) {
    replication.SetCommandArgs(std::move(context.replacement_args_));
    replication.SetFinalExpirations(context.writes_);
    replication.Commit();
    g_storage->NoteTxCommitStarted();
    bycorf::SpawnOnCurrentWorker(CommitMulti(txid, std::move(context.writes_)));
    if (!context.output_.empty()) {
      NotifyZSetBlockingKey(
          request,
          args[context.store_shape_ ? context.store_shape_->destination_arg_
                                    : 1]);
    }
    co_return Built(builder.AppendInteger(context.output_.size()));
  }
  if (request.kind_ == CommandKind::kZInterCard) {
    const std::uint64_t size = context.output_.size();
    co_return Built(
        builder.AppendInteger(limit == 0 ? size : std::min(size, limit)));
  }
  builder.AppendArrayHeader(with_scores && builder.version() == RespVersion::k3
                                ? context.output_.size()
                                : context.output_.size() *
                                      (with_scores ? 2 : 1));
  for (const Element& element : context.output_) {
    if (with_scores)
      AppendMemberScore(builder, element.member_, element.score_);
    else
      builder.AppendBulkString(element.member_);
  }
  co_return Built(builder.View());
}

Task<std::string> ExecuteZSetMultiKeyLocked(
    const CommandRequest& request, std::span<const ZSetExecKey> locked_keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  ReplyBuilder builder(request.resp_version_);
  const auto& args = request.args_;
  auto parsed_store = ParseStoreShape(request);
  if (!parsed_store.ok()) {
    co_return std::string(builder.AppendError(
        absl::StrCat("ERR ", parsed_store.status().message())));
  }
  auto keys = DetermineKeys(*request.spec_, args);
  if (!keys.ok()) {
    co_return std::string(
        builder.AppendError(absl::StrCat("ERR ", keys.status().message())));
  }

  auto find_key = [&](std::size_t argument) -> const ZSetExecKey* {
    for (const ZSetExecKey& key : locked_keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  if ((request.kind_ == CommandKind::kGeoRadius ||
       request.kind_ == CommandKind::kGeoRadiusByMember) &&
      !parsed_store->has_value()) {
    const ZSetExecKey* source = find_key(1);
    if (!source)
      co_return std::string(
          builder.AppendError("ERR GEO source key is missing"));
    CommandReply reply = co_await ExecuteZSetCommandLocked(
        request, source->digest_, &tx_writes[source->owner_], builder);
    co_return std::string(reply.encoded_);
  }
  if (request.kind_ == CommandKind::kZRangeStore) {
    ZSet ignored;
    absl::Status syntax = ComputeRangeStore(request, ZSet{}, &ignored);
    if (!syntax.ok())
      co_return std::string(
          builder.AppendError(absl::StrCat("ERR ", syntax.message())));
  } else if (request.kind_ == CommandKind::kGeoSearchStore ||
             request.kind_ == CommandKind::kGeoRadius ||
             request.kind_ == CommandKind::kGeoRadiusByMember) {
    auto syntax = ParseGeoStoreQuery(request, **parsed_store);
    if (!syntax.ok())
      co_return std::string(
          builder.AppendError(absl::StrCat("ERR ", syntax.status().message())));
  }

  MultiContext context;
  context.request_ = &request;
  context.borrowed_tx_ = true;
  const auto prepared_context = PrepareMultiContext(&context, args.size());
  if (!prepared_context.ok())
    co_return std::string(StorageError(builder, prepared_context));
  context.store_shape_ = std::move(*parsed_store);
  const bool aggregate_store = request.kind_ == CommandKind::kZDiffStore ||
                               request.kind_ == CommandKind::kZInterStore ||
                               request.kind_ == CommandKind::kZUnionStore;
  if (aggregate_store) context.store_shape_ = StoreShape{};
  if (aggregate_store) {
    context.first_source_ = keys->first_;
    context.last_source_ = keys->last_;
  } else if (context.store_shape_) {
    context.first_source_ = context.store_shape_->source_arg_;
    context.last_source_ = context.store_shape_->source_arg_;
  } else {
    context.first_source_ = keys->first_;
    context.last_source_ = keys->last_;
  }
  context.store_ = request.kind_ == CommandKind::kZDiffStore ||
                   request.kind_ == CommandKind::kZInterStore ||
                   request.kind_ == CommandKind::kZUnionStore ||
                   context.store_shape_.has_value();
  if (context.store_) MarkReplicationCommandHandled(request);
  if (request.kind_ == CommandKind::kZDiff ||
      request.kind_ == CommandKind::kZDiffStore) {
    context.aggregate_ = MultiAggregate::kDifference;
  } else if (request.kind_ == CommandKind::kZInter ||
             request.kind_ == CommandKind::kZInterCard ||
             request.kind_ == CommandKind::kZInterStore) {
    context.aggregate_ = MultiAggregate::kIntersection;
  }

  const bool specialized_store =
      request.kind_ == CommandKind::kZRangeStore ||
      request.kind_ == CommandKind::kGeoSearchStore ||
      request.kind_ == CommandKind::kGeoRadius ||
      request.kind_ == CommandKind::kGeoRadiusByMember;
  const std::size_t key_count = specialized_store ? 1 : keys->count();
  context.weights_.assign(key_count, 1.0);
  bool with_scores = false;
  std::uint64_t limit = 0;
  for (std::size_t i = aggregate_store || !context.store_ ? keys->last_ + 1
                                                          : args.size();
       i < args.size();) {
    if (EqualCi(args[i], "withscores") && !context.store_ &&
        request.kind_ != CommandKind::kZInterCard) {
      with_scores = true;
      ++i;
    } else if (EqualCi(args[i], "weights") &&
               context.aggregate_ != MultiAggregate::kDifference &&
               request.kind_ != CommandKind::kZInterCard &&
               i + key_count < args.size()) {
      for (std::size_t w = 0; w < key_count; ++w) {
        if (!ParseRedisDouble(args[i + 1 + w], &context.weights_[w], true)) {
          co_return std::string(
              builder.AppendError("ERR weight value is not a float"));
        }
      }
      i += 1 + key_count;
    } else if (EqualCi(args[i], "aggregate") && i + 1 < args.size() &&
               context.aggregate_ != MultiAggregate::kDifference &&
               request.kind_ != CommandKind::kZInterCard) {
      if (EqualCi(args[i + 1], "sum"))
        context.score_aggregate_ = ScoreAggregate::kSum;
      else if (EqualCi(args[i + 1], "min"))
        context.score_aggregate_ = ScoreAggregate::kMin;
      else if (EqualCi(args[i + 1], "max"))
        context.score_aggregate_ = ScoreAggregate::kMax;
      else
        co_return std::string(builder.AppendError("ERR syntax error"));
      i += 2;
    } else if (request.kind_ == CommandKind::kZInterCard &&
               EqualCi(args[i], "limit") && i + 1 < args.size()) {
      std::int64_t parsed_limit = 0;
      if (!ParseInt(args[i + 1], &parsed_limit)) {
        co_return std::string(
            builder.AppendError("ERR LIMIT can't be negative"));
      }
      if (parsed_limit < 0) {
        co_return std::string(
            builder.AppendError("ERR LIMIT can't be negative"));
      }
      limit = static_cast<std::uint64_t>(parsed_limit);
      i += 2;
    } else {
      co_return std::string(builder.AppendError("ERR syntax error"));
    }
  }

  for (std::size_t argument = context.first_source_;
       argument <= context.last_source_; ++argument) {
    const ZSetExecKey* key = find_key(argument);
    if (key == nullptr) {
      co_return std::string(
          builder.AppendError("ERR Sorted Set source key is missing"));
    }
    auto read = [&]() -> Task<absl::Status> {
      const bool zset_only = request.kind_ == CommandKind::kZRangeStore ||
                             request.kind_ == CommandKind::kGeoSearchStore ||
                             request.kind_ == CommandKind::kGeoRadius ||
                             request.kind_ == CommandKind::kGeoRadiusByMember;
      absl::StatusOr<ZSet> input;
      if (zset_only) {
        input = co_await ReadZSetOnlyLocked(request.db_id_, args[argument],
                                            key->digest_,
                                            &context.input_charges_[argument]);
      } else {
        input = co_await ReadAggregateInputLocked(
            request.db_id_, args[argument], key->digest_,
            &context.input_charges_[argument]);
      }
      if (!input.ok()) co_return input.status();
      context.inputs_[argument] = std::move(*input);
      co_return absl::OkStatus();
    };
    absl::Status status;
    if (key->owner_ == bycorf::ThisWorker().id_) {
      status = co_await read();
    } else {
      status = co_await bycorf::SubmitTaskTo(key->owner_, read);
    }
    if (!status.ok()) {
      co_return std::string(StorageError(builder, status));
    }
  }

  absl::Status computed = ComputeMulti(&context);
  if (!computed.ok()) co_return std::string(StorageError(builder, computed));
  if (context.store_) {
    const auto capture_ready = PrepareMultiCapture(context);
    if (!capture_ready.ok())
      co_return std::string(StorageError(builder, capture_ready));
    const std::size_t destination_arg = context.store_shape_->destination_arg_;
    const ZSetExecKey* destination = find_key(destination_arg);
    if (destination == nullptr) {
      co_return std::string(
          builder.AppendError("ERR Sorted Set destination key is missing"));
    }
    storage::TxShardWrites& destination_writes = tx_writes[destination->owner_];
    absl::Status undo_ready = co_await bycorf::SubmitTaskTo(
        destination->owner_, [txid = destination_writes.txid_] {
          return g_storage->DiscardTxUndoLocal(txid);
        });
    if (!undo_ready.ok())
      co_return std::string(StorageError(builder, undo_ready));
    destination_writes.collect_undo_ = true;
    auto write = [&]() -> Task<absl::Status> {
      co_return co_await ReplaceMultiDestination(
          &context, destination->digest_, &tx_writes[destination->owner_]);
    };
    absl::Status status;
    if (destination->owner_ == bycorf::ThisWorker().id_) {
      status = co_await write();
    } else {
      status = co_await bycorf::SubmitTaskTo(destination->owner_, write);
    }
    destination_writes.collect_undo_ = false;
    if (!status.ok()) ClearMultiPayloads(&context);
    absl::Status undo_finished = co_await bycorf::SubmitTaskTo(
        destination->owner_,
        [txid = destination_writes.txid_, rollback = !status.ok(),
         writes = &destination_writes] {
          return rollback ? g_storage->RollbackTxLocal(txid, writes)
                          : g_storage->DiscardTxUndoLocal(txid);
        });
    if (!status.ok()) {
      co_return std::string(
          StorageError(builder, undo_finished.ok() ? status : undo_finished));
    }
    if (!undo_finished.ok())
      co_return std::string(StorageError(builder, undo_finished));
    for (auto& effect : context.replacement_effects_) {
      CaptureReplicationCommand(request, effect.db_id_,
                                std::move(effect.args_));
    }
    if (!context.output_.empty()) {
      NotifyZSetBlockingKey(request, args[destination_arg]);
    }
    co_return EncodeInteger(static_cast<long long>(context.output_.size()));
  }
  if (request.kind_ == CommandKind::kZInterCard) {
    const std::uint64_t size = context.output_.size();
    co_return EncodeInteger(
        static_cast<long long>(limit == 0 ? size : std::min(size, limit)));
  }
  builder.AppendArrayHeader(with_scores && builder.version() == RespVersion::k3
                                ? context.output_.size()
                                : context.output_.size() *
                                      (with_scores ? 2 : 1));
  for (const Element& element : context.output_) {
    if (with_scores)
      AppendMemberScore(builder, element.member_, element.score_);
    else
      builder.AppendBulkString(element.member_);
  }
  co_return std::string(builder.View());
}

Task<std::string> ExecuteZSetMultiPopLocked(
    const CommandRequest& request, std::span<const ZSetExecKey> locked_keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  MarkReplicationCommandHandled(request);
  ReplyBuilder builder(request.resp_version_);
  if (request.kind_ != CommandKind::kZMPop) {
    auto timeout = ParseBlockingZSetDeadline(request);
    if (!timeout.ok()) {
      co_return std::string(builder.AppendError(
          absl::StrCat("ERR ", timeout.status().message())));
    }
  }
  auto parsed = ParseMultiPopShape(request);
  if (!parsed.ok()) {
    co_return std::string(
        builder.AppendError(absl::StrCat("ERR ", parsed.status().message())));
  }
  const MultiPopShape shape = std::move(*parsed);
  for (std::size_t argument : shape.key_args_) {
    const ZSetExecKey* key = nullptr;
    for (const ZSetExecKey& candidate : locked_keys) {
      if (candidate.arg_ == argument) {
        key = &candidate;
        break;
      }
    }
    if (key == nullptr) {
      co_return std::string(
          builder.AppendError("ERR Sorted Set key is missing"));
    }
    auto pop = [&]() {
      return PopZSetLocked(request.db_id_, request.args_[argument],
                           key->digest_, shape.maximum_, shape.count_,
                           &tx_writes[key->owner_], &request);
    };
    // Keep worker selection outside a conditional expression containing two
    // co_await operands. GCC 13 can alias their coroutine-frame slots and
    // resume the local pop on the caller after selecting the remote branch,
    // violating PartitionFor's worker-affinity invariant.
    absl::StatusOr<storage::SortedSetResult> popped;
    if (key->owner_ == bycorf::ThisWorker().id_) {
      popped = co_await pop();
    } else {
      popped = co_await bycorf::SubmitTaskTo(key->owner_, pop);
    }
    if (!popped.ok()) {
      co_return std::string(StorageError(builder, popped.status()));
    }
    if (!popped->members_.empty()) {
      CaptureReplicationCommand(
          request,
          CanonicalSelectedZSetPop(request.args_[argument], shape.maximum_,
                                   popped->members_.size()));
      AppendMultiPopReply(builder, request.args_[argument], *popped,
                          shape.flat_reply_);
      co_return std::string(builder.View());
    }
  }
  co_return std::string(builder.AppendNullArray());
}

Task<CommandReply> ExecuteBlockingZSetCommand(const CommandRequest& request,
                                              ReplyBuilder& builder,
                                              std::uint64_t client_id) {
  const auto& args = request.args_;
  auto deadline = ParseBlockingZSetDeadline(request);
  if (!deadline.ok()) {
    co_return Built(
        builder.AppendError(absl::StrCat("ERR ", deadline.status().message())));
  }
  auto shape = ParseMultiPopShape(request);
  if (!shape.ok()) {
    co_return Built(
        builder.AppendError(absl::StrCat("ERR ", shape.status().message())));
  }

  std::vector<BlockingWaitSpec> specs;
  specs.reserve(shape->key_args_.size());
  for (std::size_t argument : shape->key_args_) {
    specs.push_back(BlockingWaitSpec{
        .key_ = args[argument],
        .lane_ = {},
        .value_type_ = BlockingValueType::kSortedSet,
        .policy_ = BlockingQueuePolicy::kFifo,
        .stream_after_ = std::nullopt,
    });
  }
  CommandRequest nonblocking = request;
  auto attempt =
      [&](BlockingWakeCascade* cascade) -> Task<BlockingAttemptResult> {
    // The wait loop re-admits the original request for every attempt. Keep
    // this rewritten request on the same proof so its shard validators do not
    // reject a legitimately refreshed authority generation.
    nonblocking.cluster_authority_admission_ =
        request.cluster_authority_admission_;
    nonblocking.blocking_wake_cascade_ = cascade;
    ReplyBuilder attempt_builder(nonblocking.resp_version_);
    bool empty = false;
    CommandReply result = co_await ExecuteZSetMultiPopAttempt(
        nonblocking, attempt_builder, &empty);
    // Keep the result named for the same Clang 18 coroutine codegen constraint
    // as the blocking List path; the wait loop takes ownership after return.
    if (empty) {
      BlockingAttemptResult retry;
      co_return retry;
    }
    BlockingAttemptResult completed;
    completed.state_ = BlockingAttemptState::kComplete;
    completed.reply_ = Built(builder.AppendRaw(result.encoded_));
    co_return completed;
  };
  auto timeout_reply = [&] { return Built(builder.AppendNullArray()); };
  auto status_reply = [&](const absl::Status& status) {
    if (absl::IsAborted(status)) {
      return Built(
          builder.AppendError("TRYAGAIN " + std::string(status.message())));
    }
    return Built(StorageError(builder, status));
  };
  auto unblock_error_reply = [&] {
    return Built(
        builder.AppendError("UNBLOCKED client unblocked via CLIENT UNBLOCK"));
  };
  co_return co_await ExecuteBlockingWaitLoop(
      request, builder, client_id, std::move(specs), *deadline,
      "blocking Sorted Set wait cancelled", std::move(attempt), timeout_reply,
      unblock_error_reply, status_reply);
}

}  // namespace lavik
