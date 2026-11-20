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

#include "http_response.h"

#include <array>
#include <charconv>
#include <optional>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"

namespace object_store_probe {
namespace {
constexpr std::size_t kMaxHeaders = 8192;
constexpr std::size_t kMaxBody = 2 * 1024 * 1024;

bool Number(std::string_view field, int base, std::size_t& value) {
  if (field.empty()) return false;
  auto [end, ec] =
      std::from_chars(field.data(), field.data() + field.size(), value, base);
  return ec == std::errc{} && end == field.data() + field.size();
}

// Only unconsumed bytes survive a refill. Views returned by Line remain valid
// until the next refill; fields retained in the response must be copied.
class Reader {
 public:
  explicit Reader(bycorf::TcpStream& stream) : stream_(stream) {}
  bycorf::Task<absl::StatusOr<std::string_view>> Line() {
    for (;;) {
      auto end = pending_.find("\r\n", position_);
      if (end != std::string::npos) {
        auto line = Remaining().substr(0, end - position_);
        if (line.size() > kMaxHeaders)
          co_return absl::DataLossError("HTTP line too long");
        position_ = end + 2;
        co_return line;
      }
      if (Remaining().size() > kMaxHeaders)
        co_return absl::DataLossError("HTTP line too long");
      auto n = co_await Refill();
      if (!n.ok()) co_return n.status();
      if (!*n) co_return absl::DataLossError("truncated HTTP line");
    }
  }

  // A missing length denotes a close-delimited response.
  bycorf::Task<absl::Status> Append(std::string& out,
                                    std::optional<std::size_t> length) {
    if (length && *length > kMaxBody - out.size())
      co_return absl::ResourceExhaustedError("HTTP body too large");
    while (!length || *length != 0) {
      if (Remaining().empty()) {
        auto n = co_await Refill();
        if (!n.ok()) co_return n.status();
        if (!*n)
          co_return length ? absl::DataLossError("truncated HTTP body")
                           : absl::OkStatus();
      }
      auto data =
          Remaining().substr(0, length.value_or(std::string_view::npos));
      if (data.size() > kMaxBody - out.size())
        co_return absl::ResourceExhaustedError("HTTP body too large");
      out.append(data);
      position_ += data.size();
      if (length) *length -= data.size();
    }
    co_return absl::OkStatus();
  }

 private:
  std::string_view Remaining() const {
    return std::string_view(pending_).substr(position_);
  }
  bycorf::Task<absl::StatusOr<std::size_t>> Refill() {
    pending_.erase(0, position_);
    position_ = 0;
    auto n =
        co_await stream_.ReadSome(std::as_writable_bytes(std::span(buffer_)));
    if (n.ok()) pending_.append(buffer_.data(), *n);
    co_return n;
  }
  bycorf::TcpStream& stream_;
  std::array<char, 16 * 1024> buffer_;
  std::string pending_;
  std::size_t position_ = 0;
};
}  // namespace

bycorf::Task<absl::StatusOr<Response>> ReadResponse(bycorf::TcpStream& stream) {
  Reader reader(stream);
  Response response;
  std::optional<std::size_t> length;
  bool chunked = false;
  std::size_t header_bytes = 0;
  do {
    auto line = co_await reader.Line();
    if (!line.ok()) co_return line.status();
    std::size_t status;
    if ((!line->starts_with("HTTP/1.1 ") && !line->starts_with("HTTP/1.0 ")) ||
        line->size() < 12 || (line->size() > 12 && (*line)[12] != ' ') ||
        !Number(line->substr(9, 3), 10, status) || status < 100 ||
        status > 599 || status == 101)
      co_return absl::DataLossError("invalid HTTP status");
    response.status = status;
    length.reset();
    chunked = false;
    response.etag.clear();
    header_bytes += line->size() + 2;
    for (;;) {
      line = co_await reader.Line();
      if (!line.ok()) co_return line.status();
      header_bytes += line->size() + 2;
      if (header_bytes > kMaxHeaders)
        co_return absl::DataLossError("HTTP headers too large");
      if (line->empty()) break;
      const auto colon = line->find(':');
      if (colon == line->npos || colon == 0)
        co_return absl::DataLossError("invalid HTTP field");
      const auto name = line->substr(0, colon);
      const auto value = absl::StripAsciiWhitespace(line->substr(colon + 1));
      if (absl::EqualsIgnoreCase(name, "Content-Length")) {
        std::size_t size;
        if (!Number(value, 10, size) || (length && *length != size))
          co_return absl::DataLossError("invalid Content-Length");
        length = size;
      } else if (absl::EqualsIgnoreCase(name, "Transfer-Encoding")) {
        if (chunked || !absl::EqualsIgnoreCase(value, "chunked"))
          co_return absl::DataLossError("unsupported Transfer-Encoding");
        chunked = true;
      } else if (absl::EqualsIgnoreCase(name, "ETag")) {
        response.etag = value;
      }
    }
  } while (response.status < 200);

  if (chunked && length)
    co_return absl::DataLossError("ambiguous HTTP framing");
  if (response.status == 204 || response.status == 304) co_return response;
  if (!chunked) {
    auto status = co_await reader.Append(response.body, length);
    if (!status.ok()) co_return status;
    co_return response;
  }
  for (;;) {
    auto line = co_await reader.Line();
    if (!line.ok()) co_return line.status();
    std::size_t size;
    if (!Number(line->substr(0, line->find(';')), 16, size))
      co_return absl::DataLossError("invalid HTTP chunk size");
    if (size == 0) break;
    auto status = co_await reader.Append(response.body, size);
    if (!status.ok()) co_return status;
    line = co_await reader.Line();
    if (!line.ok()) co_return line.status();
    if (!line->empty())
      co_return absl::DataLossError("invalid HTTP chunk ending");
  }
  for (;;) {
    auto line = co_await reader.Line();
    if (!line.ok()) co_return line.status();
    header_bytes += line->size() + 2;
    if (header_bytes > kMaxHeaders)
      co_return absl::DataLossError("HTTP trailers too large");
    if (line->empty()) co_return response;
  }
}
}  // namespace object_store_probe
