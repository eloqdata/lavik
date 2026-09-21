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

#include "lavik/cluster/bootstrap.h"

#include <poll.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <string>
#include <utility>

#include "lavik/cluster/meta_client.h"
#include "lavik/numeric_endpoint.h"
#include "spdlog/spdlog.h"

namespace lavik::cluster {
namespace {
using namespace std::chrono_literals;

absl::StatusOr<control::BootstrapReply> QueryMode(
    const DataBootstrapOptions& options, const MetaControlEndpoint& endpoint) {
  net::SyncTarget target;
  target.transport_ = options.tls ? net::SyncTarget::Transport::kTcpMtls
                                  : net::SyncTarget::Transport::kTcpPlaintext;
  target.endpoint_ = FormatNumericEndpoint({endpoint.host_, endpoint.port_});
  if (options.tls) target.tls_ = *options.tls;
  auto stream = net::SyncStream::Connect(
      target, std::chrono::steady_clock::now() + 2s, options.cancel_fd);
  if (!stream.ok()) return stream.status();
  auto payload = control::EncodeMessage(control::BootstrapHello{
      .node_id = options.node_id, .capabilities = options.capabilities});
  if (!payload.ok()) return payload.status();
  control::FrameEncoder encoder;
  auto request =
      encoder.Encode(control::MessageType::kBootstrapHello, payload->payload);
  if (!request.ok()) return request.status();
  if (auto status = (*stream)->WriteAll(*request); !status.ok()) return status;
  auto header_bytes = (*stream)->ReadExact(control::kFrameHeaderBytes);
  if (!header_bytes.ok())
    return absl::IsDataLoss(header_bytes.status())
               ? absl::UnavailableError(header_bytes.status().message())
               : header_bytes.status();
  auto header = control::ParseFrameHeader(*header_bytes);
  if (!header.ok()) return header.status();
  if (header->type != control::MessageType::kBootstrapReply) {
    return absl::InvalidArgumentError("expected Meta BootstrapReply");
  }
  auto body = (*stream)->ReadExact(header->payload_length);
  if (!body.ok())
    return absl::IsDataLoss(body.status())
               ? absl::UnavailableError(body.status().message())
               : body.status();
  control::FrameDecoder decoder;
  auto frame = decoder.Decode(*header_bytes + *body);
  if (!frame.ok()) return frame.status();
  auto decoded = control::DecodeMessage(frame->type, frame->payload);
  if (!decoded.ok()) return decoded.status();
  auto reply = std::get<control::BootstrapReply>(std::move(*decoded));
  const auto& hello = reply.server;
  if (hello.negotiated_version != control::kProtocolVersion ||
      hello.meta_server_id == 0 ||
      (endpoint.server_id_ != 0 &&
       endpoint.server_id_ != hello.meta_server_id)) {
    return absl::PermissionDeniedError(
        "bootstrap Meta identity or protocol mismatch");
  }
  const auto member = std::find_if(
      hello.directory.begin(), hello.directory.end(), [&](const auto& value) {
        return value.server_id == hello.meta_server_id;
      });
  if (member != hello.directory.end()) {
    if (auto status = ValidateDialedMetaIdentity(endpoint, *member);
        !status.ok())
      return status;
  } else if (!hello.directory.empty() ||
             reply.disposition == control::BootstrapDisposition::kReady) {
    return absl::PermissionDeniedError(
        "bootstrap Meta identity absent from committed directory");
  }
  if (options.tls) {
    auto sans = (*stream)->PeerUriSans();
    if (!sans.ok()) return sans.status();
    // Before Genesis, an unresolved configured seed has no committed
    // directory to return. Authenticate its canonical Meta role/IP identity,
    // but learn no endpoint or mode from that retry-only response.
    const std::string principal = endpoint.principal_.value_or(
        member != hello.directory.end() && member->principal
            ? *member->principal
            : "lavik://meta/" + std::to_string(hello.meta_server_id));
    if (auto status = ValidateUniqueControlPrincipal(*sans, principal);
        !status.ok())
      return status;
  }
  return reply;
}

absl::Status Backoff(int cancel_fd, std::chrono::milliseconds delay) {
  const auto deadline = std::chrono::steady_clock::now() + delay;
  while (true) {
    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
                               deadline - std::chrono::steady_clock::now())
                               .count();
    if (remaining <= 0) return absl::OkStatus();
    pollfd descriptor{.fd = cancel_fd, .events = POLLIN, .revents = 0};
    const int result = ::poll(&descriptor, 1, static_cast<int>(remaining));
    if (result > 0) return absl::CancelledError("Data bootstrap interrupted");
    if (result == 0) return absl::OkStatus();
    if (errno != EINTR)
      return absl::UnavailableError("bootstrap backoff poll failed");
  }
}
}  // namespace

control::ClientServiceCapabilities SupportedClientServiceCapabilities() {
  return {.supported_modes =
              control::kSingleServiceMode | control::kClusterServiceMode,
          .services =
              control::kDb0GroupAuthority | control::kReplicaPopulationRead};
}

absl::StatusOr<control::ServiceDeclaration> BootstrapClientService(
    const DataBootstrapOptions& options) {
  if (options.seeds.empty() ||
      !control::IsCanonicalIdentity160(options.node_id)) {
    return absl::InvalidArgumentError(
        "bootstrap needs Meta seeds and a canonical Data identity");
  }
  std::vector<MetaControlEndpoint> seeds;
  for (const auto& seed : options.seeds) {
    auto endpoint = ParseNumericControlEndpoint(seed);
    if (!endpoint.ok()) return endpoint.status();
    seeds.push_back(std::move(*endpoint));
  }
  MetaEndpointDirectory directory(std::move(seeds));
  std::string last_reason;
  while (true) {
    for (const auto& endpoint : directory.Candidates()) {
      auto reply = QueryMode(options, endpoint);
      if (!reply.ok()) {
        const auto& status = reply.status();
        if (!absl::IsUnavailable(status) && !absl::IsDeadlineExceeded(status))
          return status;
        if (last_reason != status.message()) {
          last_reason = std::string(status.message());
          spdlog::info("waiting for Meta bootstrap: {}", last_reason);
        }
        continue;
      }
      if (!reply->server.directory.empty()) {
        if (auto status = directory.Update(reply->server.directory,
                                           reply->server.leader_id);
            !status.ok())
          return status;
      }
      switch (reply->disposition) {
        case control::BootstrapDisposition::kUnauthorized:
          return absl::PermissionDeniedError(reply->server.rejection_reason);
        case control::BootstrapDisposition::kIncompatible:
          return absl::FailedPreconditionError(reply->server.rejection_reason);
        case control::BootstrapDisposition::kReady:
          if (reply->server.disposition !=
                  control::ServerHelloDisposition::kAccepted ||
              reply->server.raft_term == 0) {
            return absl::FailedPreconditionError(
                "bootstrap mode did not come from a ready Meta leader");
          }
          if (auto status = control::ValidateClientService(
                  reply->server.service, options.capabilities, false);
              !status.ok())
            return status;
          return reply->server.service;
        case control::BootstrapDisposition::kRetry:
          if (last_reason != reply->server.rejection_reason) {
            last_reason = reply->server.rejection_reason;
            spdlog::info("waiting for Meta bootstrap: {}", last_reason);
          }
          break;
      }
    }
    // One fixed 100 ms wait between full candidate rounds. Startup failures
    // must not accumulate additional delay, matching MetaReconnectPolicy's
    // no-failure-count rule; simultaneous Data boots are few enough that the
    // session-reconnect jitter is not needed here.
    if (auto status =
            Backoff(options.cancel_fd, std::chrono::milliseconds(100));
        !status.ok())
      return status;
  }
}

}  // namespace lavik::cluster
