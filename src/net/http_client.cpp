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

#include "lavik/net/http_client.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <optional>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/worker.h"

namespace lavik::net {
namespace {
constexpr std::size_t kMaxHeaders = 8192;
bool IsToken(std::string_view s) {
  return !s.empty() && std::ranges::all_of(s, [](unsigned char c) {
    return absl::ascii_isalnum(c) ||
           std::string_view("!#$%&'*+-.^_`|~").find(c) !=
               std::string_view::npos;
  });
}
bool IsFieldValue(std::string_view s) {
  return std::ranges::all_of(
      s, [](unsigned char c) { return c == '\t' || (c >= 32 && c != 127); });
}
bool IsHost(const std::string& host) {
  if (host.find(':') != host.npos) {
    in6_addr ip{};
    return host.find('\0') == host.npos &&
           inet_pton(AF_INET6, host.c_str(), &ip) == 1;
  }
  return !host.empty() && std::ranges::all_of(host, [](unsigned char c) {
    return absl::ascii_isalnum(c) || c == '-' || c == '.';
  });
}

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
  Reader(bycorf::TcpStream& stream, std::size_t limit)
      : stream_(stream), limit_(limit) {}
  bycorf::Task<absl::StatusOr<std::string_view>> Line() {
    for (;;) {
      auto end = Remaining().find("\r\n");
      if (end != std::string::npos) {
        auto line = Remaining().substr(0, end);
        if (line.size() > kMaxHeaders)
          co_return absl::DataLossError("HTTP line too long");
        position_ += end + 2;
        co_return line;
      }
      // A CR at the end of a read can still become the line delimiter.
      auto partial = Remaining();
      if (partial.ends_with('\r')) partial.remove_suffix(1);
      if (partial.size() > kMaxHeaders)
        co_return absl::DataLossError("HTTP line too long");
      auto n = co_await Refill();
      if (!n.ok()) co_return n.status();
      if (!*n) co_return absl::DataLossError("truncated HTTP line");
    }
  }

  // A missing length denotes a close-delimited response.
  bycorf::Task<absl::Status> Append(std::string& out,
                                    std::optional<std::size_t> length) {
    if (length && *length > limit_ - out.size())
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
      if (data.size() > limit_ - out.size())
        co_return absl::ResourceExhaustedError("HTTP body too large");
      out.append(data);
      position_ += data.size();
      if (length) *length -= data.size();
    }
    co_return absl::OkStatus();
  }

 private:
  std::string_view Remaining() const {
    return {buffer_.data() + position_, end_ - position_};
  }
  bycorf::Task<absl::StatusOr<std::size_t>> Refill() {
    const auto tail = Remaining();
    if (position_ != 0 && !tail.empty())
      std::memmove(buffer_.data(), tail.data(), tail.size());
    position_ = 0;
    end_ = tail.size();
    auto n = co_await stream_.ReadSome(
        std::as_writable_bytes(std::span(buffer_).subspan(end_)));
    if (n.ok()) end_ += *n;
    co_return n;
  }
  bycorf::TcpStream& stream_;
  std::size_t limit_;
  static constexpr std::size_t kReadBytes = 16 * 1024;
  static_assert(kMaxHeaders + 2 <= kReadBytes);
  std::array<char, kReadBytes> buffer_;
  std::size_t end_ = 0;
  std::size_t position_ = 0;
};
bycorf::Task<absl::StatusOr<HttpResponse>> ReadResponse(
    bycorf::TcpStream& stream, HttpMethod method, HttpRequestOptions options) {
  Reader reader(stream, options.max_response_body_bytes_);
  HttpResponse response;
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
    response.status_ = status;
    response.headers_.clear();
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
      auto value = line->substr(colon + 1);
      if (!IsToken(name) || !IsFieldValue(value))
        co_return absl::DataLossError("invalid HTTP header");
      value = absl::StripAsciiWhitespace(value);
      response.headers_.push_back({std::string(name), std::string(value)});
    }
  } while (response.status_ < 200);

  // Framing fields on HEAD and 304 can describe a hypothetical GET.
  if (method == HttpMethod::kHead || response.status_ == 204 ||
      response.status_ == 304 ||
      (options.skip_error_body_ && response.status_ >= 400))
    co_return response;

  std::optional<std::size_t> length;
  bool chunked = false;
  for (const auto& [name, value] : response.headers_) {
    if (absl::EqualsIgnoreCase(name, "Content-Length")) {
      std::size_t size;
      if (!Number(value, 10, size) || (length && *length != size))
        co_return absl::DataLossError("invalid Content-Length");
      length = size;
    } else if (absl::EqualsIgnoreCase(name, "Transfer-Encoding")) {
      if (chunked || !absl::EqualsIgnoreCase(value, "chunked"))
        co_return absl::DataLossError("unsupported Transfer-Encoding");
      chunked = true;
    }
  }
  if (chunked && length)
    co_return absl::DataLossError("ambiguous HTTP framing");
  if (!chunked) {
    auto status = co_await reader.Append(response.body_, length);
    if (!status.ok()) co_return status;
    co_return response;
  }
  for (;;) {
    auto line = co_await reader.Line();
    if (!line.ok()) co_return line.status();
    std::size_t size;
    auto digits = *line;
    if (const auto ext = digits.find(';'); ext != digits.npos) {
      digits = digits.substr(0, ext);
      while (digits.ends_with(' ') || digits.ends_with('\t'))
        digits.remove_suffix(1);
    }
    if (!Number(digits, 16, size))
      co_return absl::DataLossError("invalid HTTP chunk size");
    if (size == 0) break;
    auto status = co_await reader.Append(response.body_, size);
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
    const auto colon = line->find(':');
    if (colon == line->npos || !IsToken(line->substr(0, colon)) ||
        !IsFieldValue(line->substr(colon + 1)))
      co_return absl::DataLossError("invalid HTTP trailer");
  }
}

absl::StatusOr<std::string> RequestHead(const HttpEndpoint& endpoint,
                                        const HttpRequest& request) {
  const auto& host =
      endpoint.host_.empty() ? endpoint.address_ : endpoint.host_;
  const auto method = MethodName(request.method_);
  if (endpoint.port_ == 0 || !IsHost(host) ||
      endpoint.address_.find('\0') != std::string::npos || method.empty() ||
      !request.target_.starts_with('/') ||
      !std::ranges::all_of(request.target_, [](unsigned char c) {
        return c > 32 && c < 127 && c != '#';
      }))
    return absl::InvalidArgumentError(
        "invalid HTTP endpoint, method or request target");
  std::string head = absl::StrCat(method, " ", request.target_,
                                  " HTTP/1.1\r\nHost: ", endpoint.Authority(),
                                  "\r\nConnection: close\r\n");
  for (const auto& [name, value] : request.headers_) {
    if (!IsToken(name) || !IsFieldValue(value))
      return absl::InvalidArgumentError("invalid HTTP request field");
    for (auto reserved : {"Host", "Content-Length", "Transfer-Encoding",
                          "Connection", "Expect", "Upgrade", "Trailer"})
      if (absl::EqualsIgnoreCase(name, reserved))
        return absl::InvalidArgumentError("HTTP client owns request framing");
    absl::StrAppend(&head, name, ": ", value, "\r\n");
  }
  if (!request.body_.empty() || method == "PUT" || method == "POST" ||
      method == "PATCH")
    absl::StrAppend(&head, "Content-Length: ", request.body_.size(), "\r\n");
  head += "\r\n";
  return head;
}

// The timer and exchange run on the same worker. Clear the stream pointer
// before releasing its storage borrow, even during worker teardown.
struct ExchangeDeadline {
  bycorf::TcpStream* stream = nullptr;
  bycorf::TimerCancelHandle cancel;
  absl::Status failure;
};

bycorf::Task<absl::Status> Expire(
    bycorf::Worker& worker, std::shared_ptr<ExchangeDeadline> state,
    std::chrono::steady_clock::time_point deadline) {
  if (!state->stream) co_return absl::OkStatus();
  const auto remaining = deadline - std::chrono::steady_clock::now();
  if (remaining > remaining.zero()) {
    auto timer = bycorf::CancellableSleepFor(worker, remaining);
    state->cancel = timer.CancelHandle();
    auto waited = co_await timer;
    if (state->stream && !waited.ok()) state->failure = std::move(waited);
  }
  if (state->stream) {
    if (state->failure.ok())
      state->failure = absl::DeadlineExceededError("HTTP request timed out");
    (void)state->stream->Close();
  }
  co_return absl::OkStatus();
}

bycorf::Task<absl::StatusOr<HttpResponse>> Exchange(
    bycorf::TcpStream& stream, const HttpEndpoint& endpoint,
    const HttpRequest& request, const std::string& head,
    HttpRequestOptions options,
    std::chrono::steady_clock::time_point deadline) {
  if (endpoint.tls_) {
    auto status = co_await stream.StartTls(
        endpoint.tls_, false,
        endpoint.host_.empty() ? endpoint.address_ : endpoint.host_);
    if (!status.ok()) co_return status;
  }
  if (std::chrono::steady_clock::now() >= deadline)
    co_return absl::DeadlineExceededError(
        "HTTP request timed out before sending headers");
  auto status = co_await stream.WriteAll(std::as_bytes(std::span(head)));
  if (!status.ok()) co_return status;
  if (!request.body_.empty()) {
    if (std::chrono::steady_clock::now() >= deadline)
      co_return absl::DeadlineExceededError(
          "HTTP request timed out before sending body");
    status = co_await stream.WriteAll(std::as_bytes(std::span(request.body_)));
    if (!status.ok()) co_return status;
  }
  co_return co_await ReadResponse(stream, request.method_, options);
}
}  // namespace

std::string_view MethodName(HttpMethod method) {
  switch (method) {
    case HttpMethod::kGet:
      return "GET";
    case HttpMethod::kHead:
      return "HEAD";
    case HttpMethod::kPut:
      return "PUT";
    case HttpMethod::kPost:
      return "POST";
    case HttpMethod::kPatch:
      return "PATCH";
    case HttpMethod::kDelete:
      return "DELETE";
  }
  return {};
}

std::string HttpEndpoint::Authority() const {
  const auto& host = host_.empty() ? address_ : host_;
  std::string authority =
      host.contains(':') ? absl::StrCat("[", host, "]") : host;
  if (port_ != (tls_ ? 443 : 80)) absl::StrAppend(&authority, ":", port_);
  return authority;
}

std::optional<std::string_view> HttpResponse::Header(
    std::string_view name) const {
  for (const auto& field : headers_)
    if (absl::EqualsIgnoreCase(field.name_, name)) return field.value_;
  return std::nullopt;
}

HttpClient::HttpClient(HttpEndpoint endpoint)
    : endpoint_(std::move(endpoint)) {}

bycorf::Task<absl::StatusOr<HttpResponse>> HttpClient::Send(
    HttpRequest request, HttpRequestOptions options) const {
  auto* worker = bycorf::ThisWorker().self_;
  if (!worker)
    co_return absl::FailedPreconditionError(
        "HTTP client requires a Bycorf worker");
  const auto now = std::chrono::steady_clock::now();
  if (options.timeout_.count() <= 0 ||
      options.timeout_ >=
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::time_point::max() - now))
    co_return absl::InvalidArgumentError("invalid HTTP request timeout");
  const auto deadline = now + options.timeout_;
  auto head = RequestHead(endpoint_, request);
  if (!head.ok()) co_return head.status();
  const auto remaining = deadline - std::chrono::steady_clock::now();
  if (remaining <= remaining.zero())
    co_return absl::DeadlineExceededError(
        "HTTP request timed out before connecting");
  auto stream = co_await bycorf::ConnectTcp(*worker, endpoint_.address_,
                                            endpoint_.port_, remaining);
  if (!stream.ok()) co_return stream.status();
  auto borrow = stream->BorrowStorage();
  auto state = std::make_shared<ExchangeDeadline>();
  state->stream = &*stream;
  absl::Cleanup close = [&] {
    state->stream = nullptr;
    state->cancel.Cancel();
    (void)stream->Close();
  };
  // Connect may have succeeded while this worker was descheduled. The timer
  // runs on this worker too, so check before submitting TLS or HTTP bytes.
  if (std::chrono::steady_clock::now() >= deadline)
    co_return absl::DeadlineExceededError(
        "HTTP request timed out before sending headers");
  worker->Spawn(Expire(*worker, state, deadline));
  auto result =
      co_await Exchange(*stream, endpoint_, request, *head, options, deadline);
  if (!state->failure.ok()) co_return state->failure;
  if (std::chrono::steady_clock::now() >= deadline)
    co_return absl::DeadlineExceededError("HTTP request timed out");
  co_return result;
}

}  // namespace lavik::net
