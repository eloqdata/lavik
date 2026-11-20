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

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <span>
#include <sstream>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/runtime.h"

namespace {
namespace http = boost::beast::http;
using namespace std::chrono_literals;
using Response = http::response<http::string_body>;

struct Endpoint {
  bool azure;
  std::uint16_t port;
  std::string put, get, remove, list;
};

bycorf::Task<absl::StatusOr<Response>> Request(
    bycorf::Worker& worker, const Endpoint& endpoint, http::verb method,
    const std::string& target, const std::string& data = {},
    const std::string& if_match = {}) {
  auto stream =
      co_await bycorf::ConnectTcp(worker, "127.0.0.1", endpoint.port, 5s);
  if (!stream.ok()) co_return stream.status();
  auto borrow = stream->BorrowStorage();
  absl::Cleanup close = [&] { (void)stream->Close(); };
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, absl::StrCat("127.0.0.1:", endpoint.port));
  request.keep_alive(false);
  if (method == http::verb::put) {
    if (endpoint.azure) request.set("x-ms-blob-type", "BlockBlob");
    if (!if_match.empty()) request.set(http::field::if_match, if_match);
    request.body() = data;
    request.prepare_payload();
  }
  std::ostringstream output;
  output << request;
  auto wire = output.view();
  auto status = co_await stream->WriteAll(std::as_bytes(std::span(wire)));
  if (!status.ok()) co_return status;

  http::response_parser<http::string_body> parser;
  parser.body_limit(2 * 1024 * 1024);
  parser.eager(true);
  boost::beast::flat_buffer buffer(64 * 1024);
  boost::beast::error_code error;
  while (!parser.is_done()) {
    auto chunk = buffer.prepare(16 * 1024);
    auto count = co_await stream->ReadSome(
        {static_cast<std::byte*>(chunk.data()), chunk.size()});
    if (!count.ok()) co_return count.status();
    if (*count == 0) {
      if (!parser.got_some())
        co_return absl::DataLossError("empty HTTP response");
      parser.put_eof(error);
      if (error) co_return absl::DataLossError(error.message());
      break;
    }
    buffer.commit(*count);
    buffer.consume(parser.put(buffer.data(), error));
    if (error && error != http::error::need_more)
      co_return absl::DataLossError(error.message());
  }
  co_return parser.release();
}

absl::Status Check(const absl::StatusOr<Response>& result, unsigned expected,
                   std::string_view operation) {
  if (!result.ok()) return result.status();
  if (result->result_int() != expected)
    return absl::FailedPreconditionError(
        absl::StrCat(operation, " returned HTTP ", result->result_int(),
                     ", expected ", expected));
  return absl::OkStatus();
}

bycorf::Task<absl::Status> RoundTrip(bycorf::Worker& worker,
                                     const Endpoint& endpoint) {
  std::string data(1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < data.size(); ++i)
    data[i] = static_cast<char>(i % 256);
  const auto put_status = endpoint.azure ? 201 : 200;
  auto put =
      co_await Request(worker, endpoint, http::verb::put, endpoint.put, data);
  auto status = Check(put, put_status, "PUT");
  if (!status.ok()) co_return status;
  const auto tag = (*put)[http::field::etag];
  const std::string original_tag(tag.data(), tag.size());
  if (original_tag.empty()) co_return absl::DataLossError("PUT omitted ETag");
  auto get = co_await Request(worker, endpoint, http::verb::get, endpoint.get);
  status = Check(get, 200, "GET");
  if (!status.ok()) co_return status;
  if (get->body() != data)
    co_return absl::DataLossError("GET payload mismatch");

  const std::string replacement("new\0bytes", 9);
  put = co_await Request(worker, endpoint, http::verb::put, endpoint.put,
                         replacement, original_tag);
  status = Check(put, put_status, "conditional overwrite");
  if (!status.ok()) co_return status;
  put = co_await Request(worker, endpoint, http::verb::put, endpoint.put, data,
                         original_tag);
  status = Check(put, 412, "stale If-Match");
  if (!status.ok()) co_return status;
  get = co_await Request(worker, endpoint, http::verb::get, endpoint.get);
  status = Check(get, 200, "GET after overwrite");
  if (!status.ok()) co_return status;
  if (get->body() != replacement)
    co_return absl::DataLossError("overwrite mismatch");

  // The fixture uses one ASCII key with no XML escaping in either service.
  const std::string list_entry = endpoint.azure
                                     ? "<Name>blocks/1001_8192</Name>"
                                     : "<Key>blocks/1001_8192</Key>";
  auto list =
      co_await Request(worker, endpoint, http::verb::get, endpoint.list);
  status = Check(list, 200, "list prefix");
  if (!status.ok()) co_return status;
  if (!list->body().contains(list_entry))
    co_return absl::DataLossError("key missing from prefix listing");

  auto removed =
      co_await Request(worker, endpoint, http::verb::delete_, endpoint.remove);
  status = Check(removed, endpoint.azure ? 202 : 204, "DELETE");
  if (!status.ok()) co_return status;
  get = co_await Request(worker, endpoint, http::verb::get, endpoint.get);
  co_return Check(get, 404, "GET after delete");
}

bycorf::Task<absl::Status> Run(bycorf::Worker& worker, const Endpoint& endpoint,
                               absl::Status& result) {
  result = co_await RoundTrip(worker, endpoint);
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
