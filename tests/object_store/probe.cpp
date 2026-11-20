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

namespace {
using lavik::net::HttpClient;
using lavik::net::HttpMethod;
using lavik::net::HttpRequest;
using lavik::net::HttpResponse;

struct Endpoint {
  bool azure;
  std::uint16_t port;
  std::string put, get, remove, list;
};

bycorf::Task<absl::StatusOr<HttpResponse>> Request(
    const HttpClient& client, const Endpoint& endpoint, HttpMethod method,
    std::string_view target, std::string_view data = {},
    std::string_view if_match = {}) {
  HttpRequest request{.method_ = method,
                      .target_ = std::string(target),
                      .headers_ = {},
                      .body_ = data};
  if (endpoint.azure && method == HttpMethod::kPut)
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

bycorf::Task<absl::Status> RoundTrip(const Endpoint& endpoint) {
  HttpClient client({.address_ = "127.0.0.1", .port_ = endpoint.port});
  std::string data(1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < data.size(); ++i)
    data[i] = static_cast<char>(i % 256);
  const auto put_status = endpoint.azure ? 201 : 200;
  auto put =
      co_await Request(client, endpoint, HttpMethod::kPut, endpoint.put, data);
  auto status = Check(put, put_status, "PUT");
  if (!status.ok()) co_return status;
  const std::string original_tag(put->Header("etag").value_or(""));
  if (original_tag.empty()) co_return absl::DataLossError("PUT omitted ETag");
  auto get = co_await Request(client, endpoint, HttpMethod::kGet, endpoint.get);
  status = Check(get, 200, "GET");
  if (!status.ok()) co_return status;
  if (get->body_ != data) co_return absl::DataLossError("GET payload mismatch");

  const std::string replacement("new\0bytes", 9);
  put = co_await Request(client, endpoint, HttpMethod::kPut, endpoint.put,
                         replacement, original_tag);
  status = Check(put, put_status, "conditional overwrite");
  if (!status.ok()) co_return status;
  put = co_await Request(client, endpoint, HttpMethod::kPut, endpoint.put, data,
                         original_tag);
  status = Check(put, 412, "stale If-Match");
  if (!status.ok()) co_return status;
  get = co_await Request(client, endpoint, HttpMethod::kGet, endpoint.get);
  status = Check(get, 200, "GET after overwrite");
  if (!status.ok()) co_return status;
  if (get->body_ != replacement)
    co_return absl::DataLossError("overwrite mismatch");

  // The fixture uses one ASCII key with no XML escaping in either service.
  const std::string list_entry = endpoint.azure
                                     ? "<Name>blocks/1001_8192</Name>"
                                     : "<Key>blocks/1001_8192</Key>";
  auto list =
      co_await Request(client, endpoint, HttpMethod::kGet, endpoint.list);
  status = Check(list, 200, "list prefix");
  if (!status.ok()) co_return status;
  if (!list->body_.contains(list_entry))
    co_return absl::DataLossError("key missing from prefix listing");

  auto removed =
      co_await Request(client, endpoint, HttpMethod::kDelete, endpoint.remove);
  status = Check(removed, endpoint.azure ? 202 : 204, "DELETE");
  if (!status.ok()) co_return status;
  get = co_await Request(client, endpoint, HttpMethod::kGet, endpoint.get);
  co_return Check(get, 404, "GET after delete");
}

bycorf::Task<absl::Status> Run(bycorf::Worker& worker, const Endpoint& endpoint,
                               absl::Status& result) {
  result = co_await RoundTrip(endpoint);
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
  // Signed request targets are credentials. Read them over stdin so they do
  // not appear in the process list or failure output.
  for (auto* target :
       {&endpoint.put, &endpoint.get, &endpoint.remove, &endpoint.list})
    if (!std::getline(std::cin, *target) || !target->starts_with('/') ||
        target->find_first_of("\r\n ") != std::string::npos)
      return 2;
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
