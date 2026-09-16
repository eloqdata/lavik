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

// Versioned Meta <-> Data control protocol primitives.
//
// This module owns only transport-facing values and codecs.  Wire types live
// in the nested `control` namespace on purpose: the data-plane installer maps
// them into its domain types only after authentication and validation.  That
// keeps an untrusted frame from becoming serving authority merely because it
// decoded successfully.

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::cluster::control {

inline constexpr std::uint32_t kFrameMagic = 0x4c564350;  // "LVCP"
// Control v1 is unreleased; schema changes replace its layout in place.
inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kFrameHeaderBytes = 28;
inline constexpr std::size_t kMaxFrameBytes = 16u * 1024u;
inline constexpr std::size_t kMaxFramePayloadBytes =
    kMaxFrameBytes - kFrameHeaderBytes;
// This is a cap for each opaque schema field, not for an entire message.
inline constexpr std::size_t kMaxOpaqueFieldBytes = 256u * 1024u;
// This caps canonical wire bytes, not the decoded object graph. Validation
// must not allocate another projection-sized serialized buffer.
inline constexpr std::uint64_t kMaxFullDesiredStateBytes = 512ull << 20;
// A streamed Directive contains one bounded opaque payload plus
// fixed and identifier envelopes that themselves fit in one frame.
inline constexpr std::uint64_t kMaxDirectiveTransferBytes =
    kMaxOpaqueFieldBytes + kMaxFrameBytes;
// DirectiveResult has one opaque result plus its fixed/identifier envelope.
inline constexpr std::uint64_t kMaxDirectiveResultTransferBytes =
    kMaxOpaqueFieldBytes + kMaxFrameBytes;
inline constexpr std::size_t kMaxDirectoryEntries = 4096;
inline constexpr std::size_t kMaxIdentifierBytes = 1024;
inline constexpr std::size_t kMaxProjectedNodes = 4096;
inline constexpr std::size_t kMaxProjectedGroups = 512;
inline constexpr std::size_t kMaxManifestEntries = 16384;
inline constexpr std::size_t kMaxProjectedDirectives = 4096;
inline constexpr std::size_t kMaxCandidateFlows = 1024;
inline constexpr std::size_t kMaxFailoverFailureClassBytes = 64;
// Heartbeats are single-frame messages. Keeping failure detail below 4 KiB
// leaves room for a maximal CandidateProgress plus one ActionFailed report.
inline constexpr std::size_t kMaxFailoverFailureDetailBytes = 4096;

using WireId128 = std::array<std::uint8_t, 16>;
using WireHash256 = std::array<std::uint8_t, 32>;

// Generates an incarnation identity directly from the operating system CSPRNG
// and formats it as the canonical 40-character lowercase spelling.
absl::StatusOr<std::string> GenerateIdentity160();
// Generates a binary 128-bit protocol identity directly from the OS CSPRNG.
absl::StatusOr<WireId128> GenerateId128();
bool IsCanonicalIdentity160(std::string_view identity) noexcept;

enum class MessageType : std::uint16_t {
  kClientHello = 1,
  kServerHello = 2,
  kTransferStart = 3,
  kTransferChunk = 4,
  kTransferEnd = 5,
  kTransferAbort = 6,
  kFullStateApplied = 7,
  kHeartbeat = 8,
  kHeartbeatAck = 9,
  kFence = 10,
  kFenceAck = 11,
  kDirective = 12,
  kDirectiveResponse = 13,
  kDirectiveResult = 14,
  kResultCommitted = 15,
  kResultNoLongerTracked = 16,
  // Frame-sized canonical FullDesiredState. Larger projections use the
  // existing TransferKind::kFullDesiredState Start/Chunk/End form.
  kFullDesiredState = 18,
  kNodeControlUpdate = 19,
};

struct Frame {
  MessageType type = MessageType::kClientHello;
  std::uint16_t flags = 0;
  std::uint64_t sequence = 0;
  std::string payload;

  friend bool operator==(const Frame&, const Frame&) = default;
};

struct FrameHeader {
  MessageType type = MessageType::kClientHello;
  std::uint16_t flags = 0;
  std::uint32_t payload_length = 0;
  std::uint64_t sequence = 0;
  std::uint32_t crc32c = 0;

  friend bool operator==(const FrameHeader&, const FrameHeader&) = default;
};

// Parses exactly one header for a read-header/read-payload transport. CRC
// validation remains in FrameDecoder because it covers both pieces.
absl::StatusOr<FrameHeader> ParseFrameHeader(std::string_view encoded_header);

// Per-direction encoder.  A frame is emitted only after all size checks pass,
// so a rejected local message does not create a sequence gap.
class FrameEncoder {
 public:
  absl::StatusOr<std::string> Encode(MessageType type, std::string_view payload,
                                     std::uint16_t flags = 0);
  std::uint64_t next_sequence() const noexcept { return next_sequence_; }

 private:
  std::uint64_t next_sequence_ = 1;
};

// Per-direction decoder.  CRC and structural validation happen before the
// strict sequence check, and a rejected frame never advances the sequence.
class FrameDecoder {
 public:
  absl::StatusOr<Frame> Decode(std::string_view encoded);
  std::uint64_t expected_sequence() const noexcept {
    return expected_sequence_;
  }

 private:
  std::uint64_t expected_sequence_ = 1;
};

struct WireMetaEndpoint {
  std::uint32_t server_id = 0;
  std::string host;
  std::uint16_t port = 0;
  std::optional<std::string> principal;

  friend bool operator==(const WireMetaEndpoint&,
                         const WireMetaEndpoint&) = default;
};

struct ClientHello {
  std::uint16_t minimum_version = kProtocolVersion;
  std::uint16_t maximum_version = kProtocolVersion;
  std::string node_id;
  std::string boot_id;
  std::string replication_history_id;
  // Native source layout for this boot/history, independent of any upstream
  // source layout when this node is a replica. Zero is not a valid layout.
  std::uint32_t replication_flow_count = 0;

  friend bool operator==(const ClientHello&, const ClientHello&) = default;
};

enum class ServerHelloDisposition : std::uint8_t {
  kAccepted = 1,
  kNotLeader = 2,
  kLeaderUnknown = 3,
};

struct ServerHello {
  ServerHelloDisposition disposition = ServerHelloDisposition::kAccepted;
  std::uint16_t negotiated_version = kProtocolVersion;
  std::uint32_t meta_server_id = 0;
  std::uint64_t raft_term = 0;
  WireId128 session_id{};
  std::uint64_t session_generation = 0;
  std::optional<std::uint32_t> leader_id;
  std::vector<WireMetaEndpoint> directory;
  std::uint32_t observation_ttl_ms = 0;
  std::uint32_t session_progress_timeout_ms = 0;

  friend bool operator==(const ServerHello&, const ServerHello&) = default;
};

enum class TransferKind : std::uint16_t {
  kFullDesiredState = 1,
  // Bytes are the canonical EncodeMessage payload for the named message
  // type; receivers feed the committed object back through DecodeMessage.
  kDirectivePayload = 3,
  kDirectiveResult = 4,
  kNodeControlUpdate = 5,
};

struct TransferStart {
  TransferKind kind = TransferKind::kFullDesiredState;
  WireId128 object_id{};
  std::uint64_t total_length = 0;

  friend bool operator==(const TransferStart&, const TransferStart&) = default;
};

struct TransferChunk {
  WireId128 object_id{};
  std::uint64_t offset = 0;
  std::string bytes;

  friend bool operator==(const TransferChunk&, const TransferChunk&) = default;
};

struct TransferEnd {
  WireId128 object_id{};

  friend bool operator==(const TransferEnd&, const TransferEnd&) = default;
};

enum class TransferAbortReason : std::uint16_t {
  kUnspecified = 0,
  // The sender found newer committed control state before this object became
  // installable. The receiver may discard the matching active control object
  // and keep the authenticated session for its replacement.
  kFullDesiredStateSuperseded = 1,
};

struct TransferAbort {
  WireId128 object_id{};
  TransferAbortReason reason = TransferAbortReason::kUnspecified;

  friend bool operator==(const TransferAbort&, const TransferAbort&) = default;
};

// The reassembler itself retains only transfer identity and byte counters.
// The sink chooses how bytes are staged (file, bounded buffer, decoder, ...),
// so an attacker-controlled total_length never causes eager allocation here.
class LargeObjectSink {
 public:
  virtual ~LargeObjectSink() = default;
  virtual absl::Status Begin(const TransferStart& start) = 0;
  virtual absl::Status Write(std::uint64_t offset, std::string_view bytes) = 0;
  virtual absl::Status Commit() = 0;
  virtual void Abort() noexcept = 0;
};

// One instance belongs to one transport direction and permits one in-progress
// object. Small control frames can still be decoded and handled around it. The
// referenced sink must outlive the reassembler; destruction aborts any active
// object through that sink.
class LargeObjectReassembler {
 public:
  explicit LargeObjectReassembler(LargeObjectSink& sink);
  ~LargeObjectReassembler();
  LargeObjectReassembler(const LargeObjectReassembler&) = delete;
  LargeObjectReassembler& operator=(const LargeObjectReassembler&) = delete;

  absl::Status Accept(const TransferStart& start);
  absl::Status Accept(const TransferChunk& chunk);
  absl::Status Accept(const TransferEnd& end);
  absl::Status Accept(const TransferAbort& abort);
  bool active() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

struct FullStateApplied {
  WireId128 request_id{};
  std::uint64_t control_revision = 0;

  friend bool operator==(const FullStateApplied&,
                         const FullStateApplied&) = default;
};

struct WireProjectionBasis {
  std::uint64_t control_revision = 0;

  friend bool operator==(const WireProjectionBasis&,
                         const WireProjectionBasis&) = default;
};

struct WireAuthorityAnchor {
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;

  friend bool operator==(const WireAuthorityAnchor&,
                         const WireAuthorityAnchor&) = default;
};

struct HeartbeatHealth {
  bool storage_ready = false;
  bool population_ready = false;
  bool draining = false;
  std::uint32_t active_groups = 0;
  std::string summary;

  friend bool operator==(const HeartbeatHealth&,
                         const HeartbeatHealth&) = default;
};

// Latest boot-scoped candidate proof piggybacked on a replica heartbeat. Meta
// derives the reporter boot and local history from the authenticated session;
// source_* names the completed rebuild lineage and must not be conflated with
// that reporter-local history.
struct CandidateProgress {
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;
  // Term of the live source lineage that produced this population. It may be
  // older than group_term after an uncontrolled fence, but never newer.
  std::uint64_t source_group_term = 0;
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  std::uint64_t partition_replication_epoch = 0;
  std::string source_node_id;
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_history_id;
  std::vector<std::uint64_t> applied_next_lsns;
  // A consumed clean-shutdown proof reports historical progress while the
  // current boot is still fenced. Operator recovery instead has no source
  // progress and is never an automatic candidate. The flags are exclusive.
  bool recovered = false;
  bool operator_recovery = false;

  friend bool operator==(const CandidateProgress&,
                         const CandidateProgress&) = default;
};

struct LeaseChallenge {
  WireId128 nonce{};
  std::uint64_t control_revision = 0;
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;

  friend bool operator==(const LeaseChallenge&,
                         const LeaseChallenge&) = default;
};

struct NoRoleInformation {
  friend bool operator==(const NoRoleInformation&,
                         const NoRoleInformation&) = default;
};

struct AuthorityLeaseRequest {
  LeaseChallenge challenge;

  friend bool operator==(const AuthorityLeaseRequest&,
                         const AuthorityLeaseRequest&) = default;
};

struct ReplicaCandidate {
  CandidateProgress progress;

  friend bool operator==(const ReplicaCandidate&,
                         const ReplicaCandidate&) = default;
};

// Exactly one role-specific payload follows common heartbeat health. The
// installed FDS remains authoritative: Meta validates this tag against the
// committed owner/member assignment before using either payload.
using HeartbeatRoleInformation =
    std::variant<NoRoleInformation, AuthorityLeaseRequest, ReplicaCandidate>;

// Failover progress is orthogonal to the steady-state role payload: a
// controlled source still requests lease renewal while reporting its stable
// pause. Candidate action progress can coexist with ReplicaCandidate, but
// history rotation or terminal failure may suppress that ordinary role while
// the independent failover observation reports Prepared or Failed. All values
// are boot-local observations; only the transition projected in
// FullDesiredState is durable.
struct SourcePaused {
  WireId128 transition_id{};
  std::string source_node_id;
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_history_id;
  std::uint64_t source_group_term = 0;
  std::vector<std::uint64_t> stable_next_lsns;

  friend bool operator==(const SourcePaused&, const SourcePaused&) = default;
};

struct CandidatePrepared {
  WireId128 transition_id{};
  WireId128 action_id{};
  std::string candidate_node_id;
  WireId128 candidate_assignment_id{};
  std::string candidate_boot_id;
  WireId128 prepared_context_id{};

  friend bool operator==(const CandidatePrepared&,
                         const CandidatePrepared&) = default;
};

struct ActionFailed {
  WireId128 transition_id{};
  WireId128 action_id{};
  std::string candidate_node_id;
  WireId128 candidate_assignment_id{};
  std::string candidate_boot_id;
  std::uint64_t population_manifest_revision = 0;
  WireHash256 population_manifest_digest{};
  std::uint64_t partition_replication_epoch = 0;
  std::string failure_class;
  std::string failure_detail;

  friend bool operator==(const ActionFailed&, const ActionFailed&) = default;
};

using FailoverObservation =
    std::variant<SourcePaused, CandidatePrepared, ActionFailed>;

struct Heartbeat {
  WireId128 session_id{};
  std::uint64_t heartbeat_sequence = 0;
  HeartbeatHealth health;
  HeartbeatRoleInformation role_information = NoRoleInformation{};
  std::optional<FailoverObservation> failover_observation;

  friend bool operator==(const Heartbeat&, const Heartbeat&) = default;
};

enum class ObservationStatus : std::uint8_t {
  kNotIncluded = 0,
  kAccepted = 1,
  kRejected = 2,
  kStale = 3,
};

struct NoChallenge {
  friend bool operator==(const NoChallenge&, const NoChallenge&) = default;
};

struct LeaseGranted {
  WireId128 nonce{};
  std::uint32_t leader_id = 0;
  std::uint64_t raft_term = 0;
  std::uint64_t leadership_generation = 0;
  std::string data_boot_id;
  std::uint64_t control_revision = 0;
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;
  std::uint32_t granted_duration_ms = 0;

  friend bool operator==(const LeaseGranted&, const LeaseGranted&) = default;
};

enum class LeaseDenialReason : std::uint16_t {
  kNotLeader = 1,
  kProjectionNotApplied = 2,
  kAuthorityMismatch = 3,
  kGrantInactive = 4,
  kNodeNotReady = 5,
  kAuthorityHandoffPending = 6,
};

struct LeaseDenied {
  WireId128 nonce{};
  LeaseDenialReason reason = LeaseDenialReason::kAuthorityMismatch;
  std::uint64_t current_control_revision = 0;

  friend bool operator==(const LeaseDenied&, const LeaseDenied&) = default;
};

struct LeaseStateOutOfDate {
  WireId128 nonce{};
  std::uint64_t current_control_revision = 0;

  friend bool operator==(const LeaseStateOutOfDate&,
                         const LeaseStateOutOfDate&) = default;
};

using LeaseDecision =
    std::variant<NoChallenge, LeaseGranted, LeaseDenied, LeaseStateOutOfDate>;

struct HeartbeatAck {
  WireId128 session_id{};
  std::uint64_t heartbeat_sequence = 0;
  ObservationStatus observation_status = ObservationStatus::kNotIncluded;
  std::string observation_detail;
  LeaseDecision lease_decision = NoChallenge{};

  friend bool operator==(const HeartbeatAck&, const HeartbeatAck&) = default;
};

// Meta-side stop-and-wait business sequence. Data never retries a heartbeat
// within a session: a lost Ack closes the session. Reject duplicates as well
// as gaps so a sequence always identifies one request and its causal Ack.
class HeartbeatSequenceWindow {
 public:
  absl::Status Observe(std::uint64_t sequence);
  void Reset() noexcept;

 private:
  std::uint64_t last_sequence_ = 0;
};

// Pure client-side challenge state. MarkWritten must be called immediately
// before the first WriteAll. Its timestamp and AcceptGrant's receive cut must
// come from the same suspend-aware lease-clock domain (production uses
// LeaseClockMillis/CLOCK_BOOTTIME). A grant's deadline is derived from that
// original send timestamp, never from arrival time.
class LeaseChallengeTracker {
 public:
  absl::Status Begin(WireId128 session_id, std::string data_boot_id,
                     LeaseChallenge challenge);
  absl::Status MarkWritten(const WireId128& nonce, std::int64_t lease_now_ms);
  absl::StatusOr<std::int64_t> AcceptGrant(const WireId128& session_id,
                                           const LeaseGranted& grant,
                                           std::int64_t lease_now_ms);
  void Cancel() noexcept;
  bool pending() const noexcept { return pending_.has_value(); }

 private:
  struct Pending {
    WireId128 session_id{};
    std::string data_boot_id;
    LeaseChallenge challenge;
    std::optional<std::int64_t> lease_sent_at_ms;
  };
  std::optional<Pending> pending_;
  std::optional<WireId128> last_nonce_;
};

struct Fence {
  WireId128 session_id{};
  std::string target_boot_id;
  WireProjectionBasis basis;
  WireAuthorityAnchor reject_through;

  friend bool operator==(const Fence&, const Fence&) = default;
};

struct FenceAck {
  WireId128 session_id{};
  std::string target_boot_id;
  WireAuthorityAnchor reject_through;

  friend bool operator==(const FenceAck&, const FenceAck&) = default;
};

struct WireDirectiveIdentity {
  WireId128 operation_id{};
  WireId128 directive_id{};
  WireId128 attempt_id{};
  std::uint64_t directive_revision = 0;

  friend bool operator==(const WireDirectiveIdentity&,
                         const WireDirectiveIdentity&) = default;
};

enum class WireDirectiveKind : std::uint8_t {
  kRebuild = 1,
  kAuthorizeSource = 2,
  kRevokeSources = 3,
  // Source-less destructive initialization of the target's first committed
  // population. Existing values are wire-stable; new kinds append only.
  kInitializeEmptyPopulation = 4,
};

// Versioned payload shared by rebuild and authorize-source. The envelope binds
// this layout to its exact source assignment, boot, and history; the target's
// local worker count never participates in that identity.
struct RebuildRequest {
  std::uint32_t source_flow_count = 0;

  friend bool operator==(const RebuildRequest&,
                         const RebuildRequest&) = default;
};

// Encodes/decodes a bounded, nonzero source layout. Unknown schemas, missing
// counts, and trailing bytes are rejected rather than inferred locally.
absl::StatusOr<std::string> EncodeRebuildRequest(const RebuildRequest& request);
absl::StatusOr<RebuildRequest> DecodeRebuildRequest(std::string_view encoded);

struct Directive {
  WireId128 session_id{};
  WireProjectionBasis basis;
  WireAuthorityAnchor authority;
  WireDirectiveIdentity identity;
  std::string recipient_node_id;
  std::string recipient_boot_id;
  std::string target_node_id;
  std::string target_boot_id;
  std::string source_node_id;
  // Independent from authority.assignment_id, which is target-scoped.
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_replication_history_id;
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  std::uint64_t partition_replication_epoch = 0;
  WireDirectiveKind kind = WireDirectiveKind::kRebuild;
  // V1 uses typed payloads for rebuild/authorize-source source layouts and a
  // target history id for initialize-empty-population.
  std::string payload;

  friend bool operator==(const Directive&, const Directive&) = default;
};

// Request response emitted after local execution admission. Final results
// are independent messages and do not require this response to be retained.
struct DirectiveResponse {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireDirectiveIdentity identity;
  bool started = false;

  friend bool operator==(const DirectiveResponse&,
                         const DirectiveResponse&) = default;
};

enum class DirectiveResultStatus : std::uint8_t {
  kSucceeded = 1,
  kFailed = 2,
  kRejected = 3,
};

// One immutable terminal outcome per directive attempt. Meta rejects a retry
// whose status or result bytes differ from its committed receipt.
struct DirectiveResult {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireId128 assignment_id{};
  WireDirectiveIdentity identity;
  DirectiveResultStatus status = DirectiveResultStatus::kSucceeded;
  std::string result;

  friend bool operator==(const DirectiveResult&,
                         const DirectiveResult&) = default;
};

// Acknowledges the exact attempt, after comparing the complete result on Meta.
// No content digest is needed because an attempt cannot change its outcome.
struct ResultCommitted {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireDirectiveIdentity identity;
  std::uint64_t committed_index = 0;

  friend bool operator==(const ResultCommitted&,
                         const ResultCommitted&) = default;
};

struct ResultNoLongerTracked {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireDirectiveIdentity identity;

  friend bool operator==(const ResultNoLongerTracked&,
                         const ResultNoLongerTracked&) = default;
};

struct WireDataEndpoint {
  std::string node_id;
  std::string host;
  std::uint16_t port = 0;
  std::uint16_t tls_port = 0;

  friend bool operator==(const WireDataEndpoint&,
                         const WireDataEndpoint&) = default;
};

struct WireDesiredMember {
  // Membership incarnation only. WireDesiredGroup's committed owner identity
  // classifies heartbeat role; serving additionally requires grant_active. A
  // redundant member-role byte could contradict those facts.
  std::string node_id;
  WireId128 assignment_id{};

  friend bool operator==(const WireDesiredMember&,
                         const WireDesiredMember&) = default;
};

struct WireSlotRange {
  std::uint16_t first = 0;
  std::uint16_t last = 0;

  friend bool operator==(const WireSlotRange&, const WireSlotRange&) = default;
};

// Control-protocol projection of committed transition semantics. Controlled
// mode preserves the current owner's authority until cutover; Uncontrolled
// mode requires that authority to be fenced first.
enum class WireFailoverMode : std::uint8_t {
  kControlled = 1,
  kUncontrolled = 2,
};

// Wire form of the authorized cutover's durability claim. None means the
// accepted evidence proves no acknowledged write is lost; Unknown carries no
// such guarantee and does not itself report observed loss.
enum class WireFailoverLoss : std::uint8_t {
  kNone = 1,
  kUnknown = 2,
};

// Exact candidate process incarnation projected by Meta. Receivers compare
// the node, membership assignment, and boot identities as one identity.
struct WireFailoverCandidate {
  std::string node_id;
  WireId128 assignment_id{};
  std::string boot_id;

  friend bool operator==(const WireFailoverCandidate&,
                         const WireFailoverCandidate&) = default;
};

// Projected source lineage within which candidate progress was compared. It
// fixes the expected frontier shape but deliberately carries no live frontier.
struct WireFailoverCompatibilityDomain {
  std::uint64_t source_group_term = 0;
  std::string source_node_id;
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_history_id;
  std::uint32_t flow_count = 0;

  friend bool operator==(const WireFailoverCompatibilityDomain&,
                         const WireFailoverCompatibilityDomain&) = default;
};

// Action-scoped, revision-stamped permission to prepare promotion, including
// the durability claim that any cutover through it must retain.
struct WireFailoverAuthorization {
  std::uint64_t authorized_revision = 0;
  WireFailoverLoss loss_if_cutover = WireFailoverLoss::kUnknown;

  friend bool operator==(const WireFailoverAuthorization&,
                         const WireFailoverAuthorization&) = default;
};

// One replaceable candidate attempt projected from the durable transition.
// Absence of authorization keeps the candidate selected but not executable.
struct WireFailoverCandidateAction {
  WireId128 action_id{};
  WireFailoverCandidate candidate;
  WireFailoverCompatibilityDomain domain;
  std::optional<WireFailoverAuthorization> authorization;

  bool operator_recovery = false;

  friend bool operator==(const WireFailoverCandidateAction&,
                         const WireFailoverCandidateAction&) = default;
};

// Data execution subset of the committed transition. This is a replaceable
// FDS projection, not Data-owned durable state, and is resent after reconnect
// or Meta leadership change. Meta-only workflow data such as the Controlled
// operation/deadline stays out of this protocol; after cutover the
// failover-installed Grant is represented by the ordinary current Grant and
// its optional activation action. Volatile source/candidate progress is
// likewise deliberately absent.
struct WireFailoverTransition {
  WireId128 transition_id{};
  std::uint64_t revision = 0;
  WireFailoverMode mode = WireFailoverMode::kUncontrolled;
  std::uint64_t target_term = 0;
  std::optional<WireFailoverCandidateAction> candidate_action;

  friend bool operator==(const WireFailoverTransition&,
                         const WireFailoverTransition&) = default;
};

struct WireDesiredGroup {
  std::string group_id;
  std::vector<WireDesiredMember> members;
  // A fenced group may retain both owner fields for role classification even
  // though grant_active is false. Presence must match; there is no all-zero
  // identity sentinel in the 128-bit id space.
  std::optional<std::string> owner_node_id;
  std::optional<WireId128> owner_assignment_id;
  std::uint64_t group_term = 0;
  bool grant_active = false;
  // Present only on a failover-installed current grant. Data may activate a
  // prepared promotion only when this matches its boot-local action context.
  std::optional<WireId128> activation_action_id;
  std::vector<WireSlotRange> slot_ranges;
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  // Independent Meta freshness generation for a partition population. It is
  // part of population identity even when immutable manifest content stays
  // unchanged.
  std::uint64_t partition_replication_epoch = 0;
  // Genesis population directives exclusively own replication ingress.
  // Meta enables this only after the committed cluster lifecycle is Created;
  // failover and population actions may still temporarily supersede it.
  bool steady_replication_enabled = false;
  std::optional<WireFailoverTransition> failover_transition;

  friend bool operator==(const WireDesiredGroup&,
                         const WireDesiredGroup&) = default;
};

struct WireManifestEntry {
  std::uint16_t partition_id = 0;
  std::uint64_t logical_epoch = 0;

  friend bool operator==(const WireManifestEntry&,
                         const WireManifestEntry&) = default;
};

// Entries are canonical only when strictly sorted by partition_id. This
// gives one digestable representation and rejects duplicate partitions.
struct WireManifestDocument {
  std::uint64_t revision = 0;
  WireHash256 digest{};
  std::vector<WireManifestEntry> entries;

  friend bool operator==(const WireManifestDocument&,
                         const WireManifestDocument&) = default;
};

// Task body carried once in bootstrap or a task upsert. Data adds its current
// session and local control version when admitting the work.
struct WireProjectedDirective {
  WireAuthorityAnchor authority;
  WireDirectiveIdentity identity;
  std::string recipient_node_id;
  std::string recipient_boot_id;
  std::string target_node_id;
  std::string target_boot_id;
  std::string source_node_id;
  // Independent from authority.assignment_id, which is target-scoped.
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_replication_history_id;
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  std::uint64_t partition_replication_epoch = 0;
  WireDirectiveKind kind = WireDirectiveKind::kRebuild;
  // V1 uses typed payloads for rebuild/authorize-source source layouts and a
  // target history id for initialize-empty-population.
  std::string payload;

  friend bool operator==(const WireProjectedDirective&,
                         const WireProjectedDirective&) = default;
};

// Complete node-specific semantic projection sent either as one typed frame
// or as a FullDesiredState large object after each accepted session.
struct FullDesiredState {
  // Bootstrap seed for local control and routing revisions. Later updates
  // advance each object independently; Data does not follow a Meta log index.
  std::uint64_t control_revision = 0;
  std::uint64_t topology_epoch = 0;
  // Resolved by the current Meta Leader from one global Policy and its local
  // leadership-validity limit. Data never interprets Policy identity or
  // documents; it derives heartbeat cadence from this effective duration.
  std::uint32_t authority_lease_duration_ms = 0;
  std::vector<WireMetaEndpoint> meta_directory;
  std::vector<WireDataEndpoint> nodes;
  std::vector<WireDesiredGroup> groups;
  std::vector<WireManifestDocument> manifests;
  std::vector<WireProjectedDirective> current_directives;

  friend bool operator==(const FullDesiredState&,
                         const FullDesiredState&) = default;
};

// Routing retains only discovery and serving information for remote Groups.
// Member assignments, population and failover details belong to local control;
// routing keeps only the owner's assignment as part of its serving identity.
struct WireRoutingGroup {
  std::string group_id;
  std::uint64_t term = 0;
  std::optional<std::string> owner;
  WireId128 owner_assignment{};
  bool available = false;
  std::vector<std::string> members;
  std::vector<WireSlotRange> slots;
  bool operator==(const WireRoutingGroup&) const = default;
};

struct RoutingState {
  std::uint64_t revision = 0;
  std::vector<WireDataEndpoint> nodes;
  std::vector<WireRoutingGroup> groups;
  bool operator==(const RoutingState&) const = default;
};

struct LocalGroupState {
  std::uint64_t revision = 0;
  std::uint32_t lease_duration_ms = 0;
  // Zero or one Group for the current single-population Data process.
  std::vector<WireDesiredGroup> groups;
  std::vector<WireManifestDocument> manifests;
  bool operator==(const LocalGroupState&) const = default;
};

struct MetaDirectoryState {
  std::uint64_t revision = 0;
  std::vector<WireMetaEndpoint> endpoints;
  bool operator==(const MetaDirectoryState&) const = default;
};

struct TaskChanges {
  std::uint64_t base_revision = 0;
  std::uint64_t revision = 0;
  std::vector<WireProjectedDirective> upserts;
  std::vector<WireDirectiveIdentity> removed;
  bool operator==(const TaskChanges&) const = default;
};

// A request may atomically change related objects, but each object owns its
// version. request_id correlates the acknowledgement; it is not a state
// version.
struct NodeControlUpdate {
  WireId128 request_id{};
  std::optional<RoutingState> routing;
  std::optional<LocalGroupState> local;
  std::optional<MetaDirectoryState> directory;
  std::optional<TaskChanges> tasks;
  bool operator==(const NodeControlUpdate&) const = default;
};

// Data's retained control state. FullDesiredState is a bootstrap input only.
struct NodeControlState {
  RoutingState routing;
  LocalGroupState local;
  MetaDirectoryState directory;
  std::uint64_t tasks_revision = 0;
  std::vector<WireProjectedDirective> tasks;
};

// Extracts node-owned control and lean global routes from a bootstrap input.
NodeControlState SelectNodeControlState(const FullDesiredState& bootstrap,
                                        std::string_view node_id);
// Sets only changed object versions in next. A semantic no-op yields an empty
// request and retains every installed version, even after unrelated Raft logs.
NodeControlUpdate DiffNodeControlState(const NodeControlState& previous,
                                       NodeControlState& next);
// Validates all object versions and task dependencies before replacing any
// retained state. Older objects are ignored, exact replay is idempotent, and
// same-version conflicting content or a task delta gap is rejected.
absl::Status ApplyNodeControlUpdate(NodeControlState& state,
                                    const NodeControlUpdate& update);
// Encodes the bounded object update; oversized frames use object transfer.
absl::StatusOr<std::string> EncodeNodeControlUpdate(
    const NodeControlUpdate& update);
// Decodes one complete update without changing installed state.
absl::StatusOr<NodeControlUpdate> DecodeNodeControlUpdate(
    std::string_view bytes);

// Data has no independent heartbeat setting. Keeping this derivation at the
// protocol seam prevents Meta and Data from carrying two values that must
// always agree.
constexpr std::uint32_t DataHeartbeatIntervalMs(
    std::uint32_t authority_lease_duration_ms) noexcept {
  const std::uint32_t divided = authority_lease_duration_ms / 3;
  return divided == 0 ? 1 : divided;
}

// Checks schema, relationships encoded by the wire types, and size bounds
// without materializing bytes. This also admits constructed values at the
// Data adapter seam; it performs no content-integrity hashing.
absl::Status ValidateFullDesiredState(const FullDesiredState& state);

// Canonical semantic body used as the FullDesiredState transfer payload.
absl::StatusOr<std::string> EncodeFullDesiredState(
    const FullDesiredState& state);
absl::StatusOr<FullDesiredState> DecodeFullDesiredState(
    std::string_view encoded);
// Consuming transfer overload. It releases the contiguous wire allocation on
// every return path so a decoded owning projection does not keep a second
// complete representation alive during installation.
absl::StatusOr<FullDesiredState> DecodeFullDesiredState(std::string&& encoded);
// Compares executable/routing content without the source applied index.
// Unrelated Raft commits therefore do not cause FDS replacement; messages still
// reference the index actually installed.
bool SameDesiredState(const FullDesiredState& left,
                      const FullDesiredState& right);

using WireMessage =
    std::variant<ClientHello, ServerHello, TransferStart, TransferChunk,
                 TransferEnd, TransferAbort, FullStateApplied, Heartbeat,
                 HeartbeatAck, Fence, FenceAck, Directive, DirectiveResponse,
                 DirectiveResult, ResultCommitted, ResultNoLongerTracked,
                 FullDesiredState, NodeControlUpdate>;

MessageType MessageTypeOf(const WireMessage& message) noexcept;

// Encodes/decodes a message payload.  The frame header is deliberately a
// separate operation so transport queues can select priority before assigning
// the per-direction frame sequence number.
absl::StatusOr<std::string> EncodeMessage(const WireMessage& message);
absl::StatusOr<WireMessage> DecodeMessage(MessageType type,
                                          std::string_view payload);

// Heartbeat and HeartbeatAck (which carries a lease grant) are never eligible
// for Start/Chunk/End fragmentation. The typed FullDesiredState alternative
// is likewise frame-only; its sender selects the transfer form before creating
// a message when the canonical bytes are larger. All v1 frames still obey the
// global 16-KiB frame cap.
bool RequiresSingleFrame(MessageType type) noexcept;

}  // namespace lavik::cluster::control
