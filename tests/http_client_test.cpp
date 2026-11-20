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

#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/runtime.h"

namespace {
using namespace lavik::net;
using namespace std::chrono_literals;
// Both ends run on one worker. A blocking receive prevents the peer's timer
// from resuming and sending the final chunk, and CTest times out.
struct Peer {
  std::string_view first;
  std::string_view last{};
  std::chrono::milliseconds delay{};
  std::shared_ptr<bycorf::TlsContext> tls{};
};

bycorf::Task<absl::Status> Reply(bycorf::Worker& worker,
                                 std::unique_ptr<bycorf::TcpListener> listener,
                                 Peer peer) {
  auto accepted = co_await listener->Accept();
  (void)listener->Close();
  if (!accepted.ok()) co_return accepted.status();
  bycorf::TcpStream stream(*accepted);
  auto borrow = stream.BorrowStorage();
  absl::Cleanup close = [&] { (void)stream.Close(); };
  if (peer.tls) {
    auto tls = co_await stream.StartTls(peer.tls, true);
    if (!tls.ok()) co_return tls;
  }
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
  auto status = co_await stream.WriteAll(std::as_bytes(std::span(peer.first)));
  if (!status.ok()) co_return status;
  if (peer.delay.count() > 0) {
    status = co_await bycorf::SleepFor(worker, peer.delay);
    if (!status.ok()) co_return status;
  }
  co_return co_await stream.WriteAll(std::as_bytes(std::span(peer.last)));
}

bycorf::Task<absl::StatusOr<HttpResponse>> SendToPeer(
    bycorf::Worker& worker, Peer peer, HttpRequest request,
    HttpRequestOptions options = {},
    const std::shared_ptr<bycorf::TlsContext>& tls = {},
    std::string host = "localhost") {
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
  HttpClient client({.address_ = "127.0.0.1",
                     .port_ = ntohs(address.sin_port),
                     .host_ = std::move(host),
                     .tls_ = tls});
  worker.Spawn(Reply(worker, std::move(listener), std::move(peer)));
  co_return co_await client.Send(std::move(request), options);
}

bycorf::Task<absl::Status> CheckClient(
    bycorf::Worker& worker,
    const std::shared_ptr<bycorf::TlsContext>& server_tls,
    const std::shared_ptr<bycorf::TlsContext>& client_tls) {
  HttpRequest get{.target_ = "/object"};
  Peer chunked{.first =
                   "HTTP/1.1 103 Early Hints\r\nLink: hint\r\n\r\nHTTP/1.1 200 "
                   "OK\r\nTransfer-Encoding: chunked\r\nETag: "
                   "opaque\r\nX-Value: one\r\nX-Value: two\r\n\r\n2\r\no",
               .last = "k\r\n0\r\n\r\n",
               .delay = 25ms,
               .tls = server_tls};
  auto reply = co_await SendToPeer(worker, chunked, get, {}, client_tls);
  if (!reply.ok()) co_return reply.status();
  if (reply->status_ != 200 || reply->body_ != "ok" ||
      reply->Header("etag") != "opaque" || reply->Header("Link") ||
      reply->headers_.size() != 4 || reply->headers_[3].value_ != "two")
    co_return absl::DataLossError("unexpected delayed HTTPS response");

  reply = co_await SendToPeer(worker, chunked, get, {}, client_tls,
                              "wrong.invalid");
  if (reply.ok()) co_return absl::InternalError("TLS accepted the wrong peer");

  HttpRequest head{.method_ = HttpMethod::kHead, .target_ = "/object"};
  Peer headers{.first = "HTTP/1.1 200 OK\r\nContent-Length: 8192\r\n\r\n"};
  reply = co_await SendToPeer(worker, headers, head);
  if (!reply.ok() || !reply->body_.empty())
    co_return absl::DataLossError("HEAD expected headers only");

  reply = co_await SendToPeer(worker, headers, get,
                              {.max_response_body_bytes_ = 16});
  if (!absl::IsResourceExhausted(reply.status()))
    co_return absl::InternalError("missing body limit");
  reply = co_await SendToPeer(worker, headers, get);
  if (!absl::IsDataLoss(reply.status()))
    co_return absl::InternalError("accepted truncated body");
  Peer ambiguous{.first =
                     "HTTP/1.1 200 OK\r\nContent-Length: "
                     "1\r\nTransfer-Encoding: chunked\r\n\r\n"};
  reply = co_await SendToPeer(worker, ambiguous, get);
  if (!absl::IsDataLoss(reply.status()))
    co_return absl::InternalError("accepted ambiguous framing");

  HttpClient client({.address_ = "127.0.0.1", .port_ = 1});
  auto invalid = get;
  invalid.headers_.push_back({"Content-Length", "0"});
  reply = co_await client.Send(std::move(invalid));
  if (!absl::IsInvalidArgument(reply.status()))
    co_return absl::InternalError("accepted caller-supplied framing");

  Peer slow{.first = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\no",
            .last = "k",
            .delay = 300ms};
  reply = co_await SendToPeer(worker, slow, get, {.timeout_ = 80ms});
  if (!absl::IsDeadlineExceeded(reply.status()))
    co_return absl::InternalError("missing read deadline");

  // The connection and timer for the previous request cannot affect a new
  // exchange.
  Peer closed{.first = "HTTP/1.0 404 Missing\r\n\r\ngone"};
  reply = co_await SendToPeer(worker, closed, get);
  if (!reply.ok() || reply->status_ != 404 || reply->body_ != "gone")
    co_return absl::InternalError(
        "close-delimited response after timeout failed");
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> Run(
    bycorf::Worker& worker,
    const std::shared_ptr<bycorf::TlsContext>& server_tls,
    const std::shared_ptr<bycorf::TlsContext>& client_tls,
    absl::Status& result) {
  result = co_await CheckClient(worker, server_tls, client_tls);
  worker.RequestStop();
  co_return absl::OkStatus();
}
}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string dir(argv[1]);
  auto server = bycorf::TlsContext::CreateServer(
      {.cert_file_ = dir + "/server.crt", .key_file_ = dir + "/server.key"});
  auto client =
      bycorf::TlsContext::CreateClient({.ca_cert_file_ = dir + "/ca.crt"});
  if (!server.ok() || !client.ok()) return 1;
  bycorf::Runtime runtime;
  absl::Status result;
  runtime.Start(
      1,
      [&](unsigned, bycorf::Worker& worker) {
        bycorf::WorkerOptions options;
        options.recv_buffer_count_ = 0;
        result = worker.Init(options);
        if (!result.ok()) return 1;
        worker.Spawn(Run(worker, *server, *client, result));
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
