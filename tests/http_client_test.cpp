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
#include <thread>

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
  std::string first;
  std::string last{};
  std::chrono::milliseconds delay{};
  std::shared_ptr<bycorf::TlsContext> tls{};
  std::chrono::milliseconds before_reply{};
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
  if (peer.before_reply.count()) {
    auto wait = co_await bycorf::SleepFor(worker, peer.before_reply);
    if (!wait.ok()) co_return wait;
  }
  auto status = co_await stream.WriteAll(std::as_bytes(std::span(peer.first)));
  if (!status.ok()) co_return status;
  if (peer.delay.count() > 0) {
    status = co_await bycorf::SleepFor(worker, peer.delay);
    if (!status.ok()) co_return status;
  }
  co_return co_await stream.WriteAll(std::as_bytes(std::span(peer.last)));
}

absl::StatusOr<std::uint16_t> BindLoopback(bycorf::Worker& worker,
                                           bycorf::TcpListener& listener) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bycorf::ResolvedTcpAddress loopback{};
  std::memcpy(&loopback.address_, &address, sizeof(address));
  loopback.length_ = sizeof(address);
  auto status = listener.Bind(&worker, loopback);
  if (!status.ok()) return status;
  socklen_t size = sizeof(address);
  if (::getsockname(listener.NativeFd(), reinterpret_cast<sockaddr*>(&address),
                    &size) != 0) {
    (void)listener.Close();
    return absl::InternalError("cannot read loopback listener port");
  }
  return ntohs(address.sin_port);
}

bycorf::Task<absl::StatusOr<HttpResponse>> SendToPeer(
    bycorf::Worker& worker, Peer peer, HttpRequest request,
    HttpRequestOptions options = {},
    const std::shared_ptr<bycorf::TlsContext>& tls = {},
    std::string host = "localhost") {
  auto listener = std::make_unique<bycorf::TcpListener>();
  auto port = BindLoopback(worker, *listener);
  if (!port.ok()) co_return port.status();
  HttpClient client({.address_ = "127.0.0.1",
                     .port_ = *port,
                     .host_ = std::move(host),
                     .tls_ = tls});
  worker.Spawn(Reply(worker, std::move(listener), std::move(peer)));
  co_return co_await client.Send(std::move(request), options);
}

// Simulate a descheduled worker. Send submits its connect and suspends before
// this queued task runs. The peer can accept a connection even though the
// client can no longer start the PUT when it resumes.
bycorf::Task<absl::Status> DelayWorkerResume() {
  std::this_thread::sleep_for(100ms);
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> CheckExpiredConnect(bycorf::Worker& worker) {
  bycorf::TcpListener listener;
  auto port = BindLoopback(worker, listener);
  if (!port.ok()) co_return port.status();
  absl::Cleanup close_listener = [&] { (void)listener.Close(); };
  HttpClient client({.address_ = "127.0.0.1", .port_ = *port});
  HttpRequest put{
      .method_ = HttpMethod::kPut, .target_ = "/object", .body_ = "value"};
  worker.Spawn(DelayWorkerResume());
  auto reply = co_await client.Send(std::move(put), {.timeout_ = 50ms});
  // A deschedule before connect is submitted leaves the accept queue empty.
  if (absl::IsDeadlineExceeded(reply.status()) &&
      reply.status().message() == "HTTP request timed out before connecting")
    co_return absl::OkStatus();
  if (!absl::IsDeadlineExceeded(reply.status()) ||
      !reply.status().message().contains("before sending headers"))
    co_return absl::InternalError(
        "PUT should expire after connecting and before writing: " +
        reply.status().ToString());

  auto accepted = co_await listener.Accept();
  if (!accepted.ok()) co_return accepted.status();
  bycorf::TcpStream stream(*accepted);
  auto borrow = stream.BorrowStorage();
  absl::Cleanup close_stream = [&] { (void)stream.Close(); };
  std::array<std::byte, 1> byte;
  auto count = co_await stream.ReadSome(byte);
  if (!count.ok()) co_return count.status();
  if (*count != 0)
    co_return absl::InternalError("expired PUT sent request bytes");
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> CheckClient(
    bycorf::Worker& worker,
    const std::shared_ptr<bycorf::TlsContext>& server_tls,
    const std::shared_ptr<bycorf::TlsContext>& client_tls) {
  auto expired = co_await CheckExpiredConnect(worker);
  if (!expired.ok()) co_return expired;

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
  // The same certificate and trust root just succeeded for localhost. Require
  // a verification error from TLS itself when only the requested name changes.
  const auto& tls_error = reply.status();
  if (!((absl::IsInternal(tls_error) &&
         tls_error.message().starts_with("TLS handshake failed:") &&
         tls_error.message().contains("certificate verify failed")) ||
        (absl::IsPermissionDenied(tls_error) &&
         tls_error.message().contains("hostname mismatch"))))
    co_return absl::InternalError(
        "expected certificate verification failure for wrong.invalid: " +
        tls_error.ToString());

  // Withhold window updates for the rest of the request. A sequential
  // uploader cannot observe the rejection before its deadline. The delayed
  // response also makes the TLS writer contend on real socket backpressure.
  std::string large_body(16 * 1024 * 1024, 'x');
  HttpRequest put{
      .method_ = HttpMethod::kPut, .target_ = "/object", .body_ = large_body};
  for (bool https : {false, true}) {
    Peer reject{
        .first =
            "HTTP/1.1 412 Precondition Failed\r\nContent-Length: 3\r\n\r\nold",
        .delay = 4s,
        .tls = https ? server_tls : nullptr,
        .before_reply = 100ms};
    reply = co_await SendToPeer(worker, reject, put, {.timeout_ = 2s},
                                https ? client_tls : nullptr);
    if (!reply.ok()) co_return reply.status();
    if (reply->status_ != 412 || reply->body_ != "old")
      co_return absl::DataLossError(
          "lost final response during backpressured upload");
  }

  Peer silent{.first = "", .before_reply = 4s};
  reply = co_await SendToPeer(worker, silent, put, {.timeout_ = 100ms});
  if (!absl::IsDeadlineExceeded(reply.status()))
    co_return absl::InternalError("missing stalled upload deadline");

  HttpRequest head{.method_ = HttpMethod::kHead, .target_ = "/object"};
  Peer headers{.first = "HTTP/1.1 200 OK\r\nContent-Length: 8192\r\n\r\n"};
  reply = co_await SendToPeer(worker, headers, head);
  if (!reply.ok() || !reply->body_.empty())
    co_return absl::DataLossError("HEAD expected headers only");

  Peer metadata{
      .first = "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"};
  reply = co_await SendToPeer(worker, metadata, head);
  if (!reply.ok() || !reply->body_.empty() ||
      reply->Header("Transfer-Encoding") != "gzip, chunked")
    co_return absl::InternalError("HEAD transfer coding metadata lost");

  // Deliver CR and LF in different reads on a maximum-length chunk-size line.
  std::string first = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
  std::string size_line = "1 \t;x=";
  size_line.resize(8192, 'a');
  first += size_line + "\r";
  Peer split{.first = first, .last = "\nx\r\n0\r\n\r\n", .delay = 10ms};
  reply = co_await SendToPeer(worker, split, get);
  if (!reply.ok() || reply->body_ != "x")
    co_return absl::InternalError("split chunk-size delimiter rejected");
  first.insert(first.size() - 1, 1, 'a');
  split.first = first;
  reply = co_await SendToPeer(worker, split, get);
  if (!absl::IsDataLoss(reply.status()))
    co_return absl::InternalError("accepted oversized chunk-size line");

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
