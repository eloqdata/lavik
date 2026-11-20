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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/task.h"

namespace lavik::net {

struct HttpEndpoint {
  // Resolve the numeric dial address before entering the worker. host_ is the
  // unbracketed HTTP host and TLS peer name, defaulting to address_ if empty.
  std::string address_;
  std::uint16_t port_ = 0;
  std::string host_;
  // A client TLS context, prepared off-worker. Empty selects plaintext.
  std::shared_ptr<bycorf::TlsContext> tls_;
};

enum class HttpMethod { kGet, kHead, kPut, kPost, kPatch, kDelete };

struct HttpHeader {
  std::string name_;
  std::string value_;
};

struct HttpRequest {
  HttpMethod method_ = HttpMethod::kGet;
  // Already encoded origin-form path and query, preserved byte for byte.
  std::string target_;
  std::vector<HttpHeader> headers_;
  // Borrowed binary payload must stay valid and immutable until completion.
  std::string_view body_;
};

struct HttpResponse {
  unsigned status_ = 0;
  // Final response fields in wire order, including repeated names.
  std::vector<HttpHeader> headers_;
  std::string body_;

  // First matching field, compared case-insensitively. Borrows from headers_.
  [[nodiscard]] std::optional<std::string_view> Header(
      std::string_view name) const;
};

struct HttpRequestOptions {
  std::chrono::milliseconds timeout_{30000};
  std::size_t max_response_body_bytes_ = 16 * 1024 * 1024;
};

// One connection per HTTP/1.1 exchange. The immutable client may serve
// concurrent callers and must outlive their tasks. Each Send executes entirely
// on the calling Bycorf worker. It returns HTTP statuses (including 4xx/5xx)
// verbatim; transport, protocol, deadline and size failures return an error.
class HttpClient {
 public:
  explicit HttpClient(HttpEndpoint endpoint);

  // The deadline covers connect, TLS, send and response. The client manages
  // Host, framing and connection headers and performs no automatic retries.
  [[nodiscard]] bycorf::Task<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request, HttpRequestOptions options = {}) const;

 private:
  HttpEndpoint endpoint_;
};

}  // namespace lavik::net
