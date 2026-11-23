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

#include <charconv>
#include <cstdint>
#include <iostream>
#include <string>

#include "absl/strings/str_cat.h"
#include "bycorf/runtime/runtime.h"
#include "lavik/net/http_client.h"
#include "lavik/object_store/s3_client.h"

namespace {
using lavik::net::HttpClient;
using lavik::net::HttpMethod;
using lavik::net::HttpRequest;
using lavik::net::HttpResponse;

struct Endpoint {
  bool azure;
  std::uint16_t port;
  std::string put, get, remove, list;
  std::string bucket;
  lavik::object_store::S3Credentials credentials;
};

bycorf::Task<absl::StatusOr<HttpResponse>> Request(
    const HttpClient& client, HttpMethod method, std::string_view target,
    std::string_view data = {}, std::string_view if_match = {}) {
  HttpRequest request{.method_ = method,
                      .target_ = std::string(target),
                      .headers_ = {},
                      .body_ = data};
  if (method == HttpMethod::kPut)
    request.headers_.push_back({"x-ms-blob-type", "BlockBlob"});
  if (!if_match.empty())
    request.headers_.push_back({"If-Match", std::string(if_match)});
  co_return co_await client.Send(std::move(request));
}

absl::Status Check(const absl::StatusOr<HttpResponse>& result,
                   unsigned expected, std::string_view operation) {
  if (!result.ok()) return result.status();
  if (result->status_ != expected)
    return absl::FailedPreconditionError(
        absl::StrCat(operation, " returned HTTP ", result->status_,
                     ", expected ", expected));
  return absl::OkStatus();
}

std::string TestPayload() {
  std::string data(1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < data.size(); ++i)
    data[i] = static_cast<char>(i % 256);
  return data;
}

bycorf::Task<absl::Status> AzureRoundTrip(const Endpoint& endpoint) {
  HttpClient client({.address_ = "127.0.0.1", .port_ = endpoint.port});
  const auto data = TestPayload();
  auto put = co_await Request(client, HttpMethod::kPut, endpoint.put, data);
  auto status = Check(put, 201, "PUT");
  if (!status.ok()) co_return status;
  const std::string original_tag(put->Header("etag").value_or(""));
  if (original_tag.empty()) co_return absl::DataLossError("PUT omitted ETag");
  auto get = co_await Request(client, HttpMethod::kGet, endpoint.get);
  status = Check(get, 200, "GET");
  if (!status.ok()) co_return status;
  if (get->body_ != data) co_return absl::DataLossError("GET payload mismatch");

  const std::string replacement("new\0bytes", 9);
  put = co_await Request(client, HttpMethod::kPut, endpoint.put, replacement,
                         original_tag);
  status = Check(put, 201, "conditional overwrite");
  if (!status.ok()) co_return status;
  put = co_await Request(client, HttpMethod::kPut, endpoint.put, data,
                         original_tag);
  status = Check(put, 412, "stale If-Match");
  if (!status.ok()) co_return status;
  get = co_await Request(client, HttpMethod::kGet, endpoint.get);
  status = Check(get, 200, "GET after overwrite");
  if (!status.ok()) co_return status;
  if (get->body_ != replacement)
    co_return absl::DataLossError("overwrite mismatch");

  const std::string list_entry = "<Name>blocks/1001_8192</Name>";
  auto list = co_await Request(client, HttpMethod::kGet, endpoint.list);
  status = Check(list, 200, "list prefix");
  if (!status.ok()) co_return status;
  if (!list->body_.contains(list_entry))
    co_return absl::DataLossError("key missing from prefix listing");

  auto removed = co_await Request(client, HttpMethod::kDelete, endpoint.remove);
  status = Check(removed, 202, "DELETE");
  if (!status.ok()) co_return status;
  get = co_await Request(client, HttpMethod::kGet, endpoint.get);
  co_return Check(get, 404, "GET after delete");
}

bycorf::Task<absl::Status> S3RoundTrip(const Endpoint& endpoint) {
  using lavik::object_store::S3Client;
  lavik::net::HttpEndpoint host{.address_ = "127.0.0.1",
                                .port_ = endpoint.port};
  S3Client client(host, "us-east-1", endpoint.bucket, endpoint.credentials);
  const std::string prefix = "blocks/空 格+%&/";
  const std::string first = prefix + "1001_0";
  const std::string second = prefix + "1001_8192";
  const auto bytes = TestPayload();
  auto tag = co_await client.Put(first, bytes);
  if (!tag.ok()) co_return tag.status();
  auto get = co_await client.Get(first);
  if (!get.ok()) co_return get.status();
  if (*get != bytes) co_return absl::DataLossError("S3 binary GET mismatch");
  const std::string replacement("new\0bytes", 9);
  auto overwrite = co_await client.Put(first, replacement, *tag);
  if (!overwrite.ok()) co_return overwrite.status();
  S3Client limited(host, "us-east-1", endpoint.bucket, endpoint.credentials,
                   {.max_response_body_bytes_ = 0});
  auto too_large = co_await limited.Get(first);
  if (!absl::IsResourceExhausted(too_large.status()))
    co_return absl::DataLossError("S3 GET ignored its response body limit");
  auto stale = co_await limited.Put(first, bytes, *tag);
  if (!absl::IsFailedPrecondition(stale.status()))
    co_return absl::DataLossError(
        "stale S3 If-Match did not return FailedPrecondition");
  get = co_await client.Get(first);
  if (!get.ok()) co_return get.status();
  if (*get != replacement)
    co_return absl::DataLossError("S3 conditional overwrite mismatch");

  auto other = co_await client.Put("another-prefix", "");
  if (!other.ok()) co_return other.status();
  auto empty = co_await client.Put(second, "");
  if (!empty.ok()) co_return empty.status();
  auto page = co_await client.List(prefix, "", 1);
  if (!page.ok()) co_return page.status();
  if (page->keys_.size() != 1 || page->keys_[0] != first ||
      page->next_token_.empty())
    co_return absl::DataLossError("unexpected first S3 page");
  page = co_await client.List(prefix, page->next_token_, 1);
  if (!page.ok()) co_return page.status();
  if (page->keys_.size() != 1 || page->keys_[0] != second ||
      !page->next_token_.empty())
    co_return absl::DataLossError("unexpected final S3 page");

  auto invalid_credentials = endpoint.credentials;
  invalid_credentials.secret_key_ += "invalid";
  S3Client invalid(host, "us-east-1", endpoint.bucket,
                   std::move(invalid_credentials));
  auto denied = co_await invalid.Get(first);
  if (!absl::IsPermissionDenied(denied.status()))
    co_return absl::DataLossError(
        "invalid S3 signature did not return PermissionDenied");
  auto removed = co_await client.Delete(first);
  if (!removed.ok()) co_return removed;
  get = co_await limited.Get(first);
  if (!absl::IsNotFound(get.status()))
    co_return absl::DataLossError(
        "S3 GET after delete did not return NotFound");
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> Run(bycorf::Worker& worker, const Endpoint& endpoint,
                               absl::Status& result) {
  if (endpoint.azure)
    result = co_await AzureRoundTrip(endpoint);
  else
    result = co_await S3RoundTrip(endpoint);
  worker.RequestStop();
  co_return absl::OkStatus();
}
}  // namespace

int main(int argc, char** argv) {
  Endpoint endpoint{};
  if (argc != 3) return 2;
  const std::string_view provider(argv[1]), port(argv[2]);
  if (provider != "azure" && provider != "s3") return 2;
  endpoint.azure = provider == "azure";
  const auto [end, ec] =
      std::from_chars(port.begin(), port.end(), endpoint.port);
  if (ec != std::errc{} || end != port.end() || !endpoint.port) return 2;
  // Receive fixture credentials over stdin before starting the worker.
  if (endpoint.azure) {
    for (auto* target :
         {&endpoint.put, &endpoint.get, &endpoint.remove, &endpoint.list})
      if (!std::getline(std::cin, *target) || !target->starts_with('/') ||
          target->find_first_of("\r\n ") != std::string::npos)
        return 2;
  } else {
    for (auto* field : {&endpoint.bucket, &endpoint.credentials.access_key_,
                        &endpoint.credentials.secret_key_})
      if (!std::getline(std::cin, *field) || field->empty()) return 2;
  }
  bycorf::Runtime runtime;
  absl::Status result;
  runtime.Start(
      1,
      [&](unsigned, bycorf::Worker& worker) {
        bycorf::WorkerOptions options;
        options.recv_buffer_count_ = 0;
        result = worker.Init(options);
        if (!result.ok()) return 1;
        worker.Spawn(Run(worker, endpoint, result));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return result.ok() ? 0 : 1;
      },
      false);
  runtime.WaitUntilStopped();
  if (!result.ok()) std::cerr << result << '\n';
  return runtime.exit_code();
}
