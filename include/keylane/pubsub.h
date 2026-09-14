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
#include "celer/runtime/task.h"
#include "keylane/resp.h"

namespace celer {
class TcpStream;
}

namespace keylane {

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
                              std::span<const std::string> channels);
std::string UnsubscribeChannels(const std::shared_ptr<PubSubSession>& session,
                                std::span<const std::string> channels);
std::string PSubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                               std::span<const std::string> patterns);
std::string PUnsubscribePatterns(const std::shared_ptr<PubSubSession>& session,
                                 std::span<const std::string> patterns);
void ResetPubSubSubscriptions(const std::shared_ptr<PubSubSession>& session);

// PUBSUB introspection reports this node's aggregate worker-local state.
celer::Task<std::vector<std::string>> PubSubChannels(
    std::optional<std::string> pattern);
celer::Task<std::vector<std::uint64_t>> PubSubNumSub(
    std::span<const std::string> channels);
celer::Task<std::uint64_t> PubSubNumPat();

// Delivers to every worker-local registry and returns the number of matching
// live subscriptions on this node. The encoded message body is shared across
// all recipients.
celer::Task<std::uint64_t> PublishChannel(std::string_view channel,
                                          std::string_view payload);

// Captures the live subscription matches and their reply protocol at the
// command's logical execution point without making the message visible. The
// frozen receiver count is independent of later SUBSCRIBE/UNSUBSCRIBE changes;
// delivery still uses each session's owner worker and bounded output queue.
celer::Task<absl::StatusOr<std::shared_ptr<CapturedPubSubPublication>>>
CapturePubSubPublication(std::string_view channel, std::string_view payload);
std::uint64_t CapturedPubSubReceiverCount(
    const std::shared_ptr<CapturedPubSubPublication>& publication) noexcept;
celer::Task<absl::Status> DeliverCapturedPubSubPublication(
    std::shared_ptr<CapturedPubSubPublication> publication);

void EnqueuePubSubReply(const std::shared_ptr<PubSubSession>& session,
                        std::string encoded);
void ExitPubSubMode(const std::shared_ptr<PubSubSession>& session);
void ClosePubSubSession(const std::shared_ptr<PubSubSession>& session);
void MarkPubSubReaderStarted(const std::shared_ptr<PubSubSession>& session);
void MarkPubSubReaderDone(const std::shared_ptr<PubSubSession>& session);
celer::Task<absl::Status> WaitPubSubReaderDone(
    const std::shared_ptr<PubSubSession>& session);

celer::Task<absl::Status> StreamPubSubMessages(
    celer::TcpStream& stream, const std::shared_ptr<PubSubSession>& session);

}  // namespace keylane
