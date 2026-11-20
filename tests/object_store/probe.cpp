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

#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/runtime/runtime.h"
#include "http_response.h"

namespace {
using object_store_probe::Response;
using namespace std::chrono_literals;

struct Endpoint {
  bool azure;
  std::uint16_t port;
  std::string put, get, remove, list;
};

bycorf::Task<absl::StatusOr<Response>> Request(bycorf::Worker& worker,
                                               const Endpoint& endpoint,
                                               std::string_view method,
                                               std::string_view target,
                                               std::string_view data = {},
                                               std::string_view if_match = {}) {
  auto stream =
      co_await bycorf::ConnectTcp(worker, "127.0.0.1", endpoint.port, 5s);
  if (!stream.ok()) co_return stream.status();
  auto borrow = stream->BorrowStorage();
  absl::Cleanup close = [&] { (void)stream->Close(); };
  std::string headers = absl::StrCat(
      method, " ", target, " HTTP/1.1\r\nHost: 127.0.0.1:", endpoint.port,
      "\r\nConnection: close\r\n");
  if (method == "PUT") {
    absl::StrAppend(&headers, "Content-Length: ", data.size(), "\r\n");
    if (endpoint.azure) headers += "x-ms-blob-type: BlockBlob\r\n";
    if (!if_match.empty())
      absl::StrAppend(&headers, "If-Match: ", if_match, "\r\n");
  }
  headers += "\r\n";
  auto status = co_await stream->WriteAll(std::as_bytes(std::span(headers)));
  if (!status.ok()) co_return status;
  if (!data.empty()) {
    status = co_await stream->WriteAll(std::as_bytes(std::span(data)));
    if (!status.ok()) co_return status;
  }
  co_return co_await object_store_probe::ReadResponse(*stream);
}

absl::Status Check(const absl::StatusOr<Response>& result, unsigned expected,
                   std::string_view operation) {
  if (!result.ok()) return result.status();
  if (result->status != expected)
    return absl::FailedPreconditionError(absl::StrCat(
        operation, " returned HTTP ", result->status, ", expected ", expected));
  return absl::OkStatus();
}

bycorf::Task<absl::Status> RoundTrip(bycorf::Worker& worker,
                                     const Endpoint& endpoint) {
  std::string data(1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < data.size(); ++i)
    data[i] = static_cast<char>(i % 256);
  const auto put_status = endpoint.azure ? 201 : 200;
  auto put = co_await Request(worker, endpoint, "PUT", endpoint.put, data);
  auto status = Check(put, put_status, "PUT");
  if (!status.ok()) co_return status;
  const std::string original_tag = put->etag;
  if (original_tag.empty()) co_return absl::DataLossError("PUT omitted ETag");
  auto get = co_await Request(worker, endpoint, "GET", endpoint.get);
  status = Check(get, 200, "GET");
  if (!status.ok()) co_return status;
  if (get->body != data) co_return absl::DataLossError("GET payload mismatch");

  const std::string replacement("new\0bytes", 9);
  put = co_await Request(worker, endpoint, "PUT", endpoint.put, replacement,
                         original_tag);
  status = Check(put, put_status, "conditional overwrite");
  if (!status.ok()) co_return status;
  put = co_await Request(worker, endpoint, "PUT", endpoint.put, data,
                         original_tag);
  status = Check(put, 412, "stale If-Match");
  if (!status.ok()) co_return status;
  get = co_await Request(worker, endpoint, "GET", endpoint.get);
  status = Check(get, 200, "GET after overwrite");
  if (!status.ok()) co_return status;
  if (get->body != replacement)
    co_return absl::DataLossError("overwrite mismatch");

  // The fixture uses one ASCII key with no XML escaping in either service.
  const std::string list_entry = endpoint.azure
                                     ? "<Name>blocks/1001_8192</Name>"
                                     : "<Key>blocks/1001_8192</Key>";
  auto list = co_await Request(worker, endpoint, "GET", endpoint.list);
  status = Check(list, 200, "list prefix");
  if (!status.ok()) co_return status;
  if (!list->body.contains(list_entry))
    co_return absl::DataLossError("key missing from prefix listing");

  auto removed = co_await Request(worker, endpoint, "DELETE", endpoint.remove);
  status = Check(removed, endpoint.azure ? 202 : 204, "DELETE");
  if (!status.ok()) co_return status;
  get = co_await Request(worker, endpoint, "GET", endpoint.get);
  co_return Check(get, 404, "GET after delete");
}

// Both ends run on one worker. A blocking receive prevents the peer's timer
// from resuming and sending the final chunk; the driver's timeout catches it.
bycorf::Task<absl::Status> DelayedReply(
    bycorf::Worker& worker, std::unique_ptr<bycorf::TcpListener> listener) {
  auto accepted = co_await listener->Accept();
  (void)listener->Close();
  if (!accepted.ok()) co_return accepted.status();
  bycorf::TcpStream stream(*accepted);
  auto borrow = stream.BorrowStorage();
  absl::Cleanup close = [&] { (void)stream.Close(); };
  std::array<char, 512> request;
  std::size_t used = 0;
  while (!std::string_view(request.data(), used).contains("\r\n\r\n")) {
    if (used == request.size())
      co_return absl::DataLossError("unexpected probe request");
    auto count = co_await stream.ReadSome(
        std::as_writable_bytes(std::span(request).subspan(used)));
    if (!count.ok()) co_return count.status();
    if (!*count) co_return absl::DataLossError("incomplete probe request");
    used += *count;
  }
  constexpr std::string_view first =
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\no";
  constexpr std::string_view last = "k\r\n0\r\n\r\n";
  auto status = co_await stream.WriteAll(std::as_bytes(std::span(first)));
  if (!status.ok()) co_return status;
  status = co_await bycorf::SleepFor(worker, 25ms);
  if (!status.ok()) co_return status;
  co_return co_await stream.WriteAll(std::as_bytes(std::span(last)));
}

bycorf::Task<absl::Status> CheckWorkerProgress(bycorf::Worker& worker) {
  auto listener = std::make_unique<bycorf::TcpListener>();
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bycorf::ResolvedTcpAddress loopback{};
  std::memcpy(&loopback.address_, &address, sizeof(address));
  loopback.length_ = sizeof(address);
  auto status = listener->Bind(&worker, loopback);
  if (!status.ok()) co_return status;
  socklen_t size = sizeof(address);
  if (::getsockname(listener->NativeFd(), reinterpret_cast<sockaddr*>(&address),
                    &size) != 0) {
    (void)listener->Close();
    co_return absl::InternalError("cannot read loopback listener port");
  }
  Endpoint local{};
  local.port = ntohs(address.sin_port);
  worker.Spawn(DelayedReply(worker, std::move(listener)));
  auto reply = co_await Request(worker, local, "GET", "/slow");
  status = Check(reply, 200, "delayed GET");
  if (!status.ok()) co_return status;
  if (reply->body != "ok")
    co_return absl::DataLossError("incomplete delayed reply");
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> Run(bycorf::Worker& worker, const Endpoint& endpoint,
                               absl::Status& result) {
  result = co_await CheckWorkerProgress(worker);
  if (result.ok()) result = co_await RoundTrip(worker, endpoint);
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
