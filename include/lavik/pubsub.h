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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/task.h"
#include "lavik/command_args.h"
#include "lavik/resp.h"

namespace bycorf {
class TcpStream;
}

namespace lavik {

class PubSubSession;
class CapturedPubSubPublication;

// Initializes one worker-local subscription registry per runtime worker.
void PreparePubSub(unsigned worker_count);

std::shared_ptr<PubSubSession> RegisterPubSubSession(int fd,
                                                     RespVersion version);
void UnregisterPubSubSession(const std::shared_ptr<PubSubSession>& session);
void SetPubSubRespVersion(const std::shared_ptr<PubSubSession>& session,
                          RespVersion version);

std::size_t PubSubSubscriptionCount(
    const std::shared_ptr<PubSubSession>& session) noexcept;
std::size_t PubSubPatternSubscriptionCount(
    const std::shared_ptr<PubSubSession>& session) noexcept;

// These return one or more complete push-style frames in the session's
// negotiated RESP version.
std::string SubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                              CommandArgsView channels);
std::string UnsubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                                CommandArgsView channels);
std::string PSubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                               CommandArgsView patterns);
std::string PUnsubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                                 CommandArgsView patterns);
void ResetPubSubSubscriptions(const std::shared_ptr<PubSubSession>& session);

// PUBSUB introspection reports this node's aggregate worker-local state.
bycorf::Task<std::vector<std::string>> PubSubChannels(
    std::optional<std::string> pattern);
bycorf::Task<std::vector<std::uint64_t>> PubSubNumSub(CommandArgsView channels);
bycorf::Task<std::uint64_t> PubSubNumPat();

// Delivers to every worker-local registry and returns the number of matching
// live subscriptions on this node. The encoded message body is shared across
// all recipients.
bycorf::Task<std::uint64_t> PublishChannel(std::string_view channel,
                                           std::string_view payload);

// Captures the live subscription matches and their reply protocol at the
// command's logical execution point without making the message visible. The
// frozen receiver count is independent of later SUBSCRIBE/UNSUBSCRIBE changes;
// delivery still uses each session's owner worker and bounded output queue.
bycorf::Task<absl::StatusOr<std::shared_ptr<CapturedPubSubPublication>>>
CapturePubSubPublication(std::string_view channel, std::string_view payload);
std::uint64_t CapturedPubSubReceiverCount(
    const std::shared_ptr<CapturedPubSubPublication>& publication) noexcept;
bycorf::Task<absl::Status> DeliverCapturedPubSubPublication(
    std::shared_ptr<CapturedPubSubPublication> publication);

void EnqueuePubSubReply(const std::shared_ptr<PubSubSession>& session,
                        std::string encoded);
void ExitPubSubMode(const std::shared_ptr<PubSubSession>& session);
void ClosePubSubSession(const std::shared_ptr<PubSubSession>& session);
void MarkPubSubReaderStarted(const std::shared_ptr<PubSubSession>& session);
void MarkPubSubReaderDone(const std::shared_ptr<PubSubSession>& session);
bycorf::Task<absl::Status> WaitPubSubReaderDone(
    const std::shared_ptr<PubSubSession>& session);

bycorf::Task<absl::Status> StreamPubSubMessages(
    bycorf::TcpStream& stream, const std::shared_ptr<PubSubSession>& session);

}  // namespace lavik
