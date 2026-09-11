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

namespace keylane::cluster::control {

inline constexpr std::uint32_t kFrameMagic = 0x4b4c4350;  // "KLCP"
// Control v1 is unreleased; schema changes replace its layout in place.
inline constexpr std::uint16_t kProtocolVersion = 1;
inline constexpr std::size_t kFrameHeaderBytes = 28;
inline constexpr std::size_t kMaxFrameBytes = 16u * 1024u;
inline constexpr std::size_t kMaxFramePayloadBytes =
    kMaxFrameBytes - kFrameHeaderBytes;
// This is a cap for each opaque schema field, not for an entire message.
inline constexpr std::size_t kMaxOpaqueFieldBytes = 256u * 1024u;
// This caps canonical wire bytes, not the decoded object graph. Decode must
// temporarily retain the input plus its owning fields; digest/projection
// validation is therefore required to use only O(directive-count) references
// and constant-size hashing state, never another projection-sized buffer.
inline constexpr std::uint64_t kMaxFullDesiredStateBytes = 512ull << 20;
// A streamed Directive contains two independently bounded opaque fields plus
// fixed and identifier envelopes that themselves fit in one frame.
inline constexpr std::uint64_t kMaxDirectiveTransferBytes =
    2ull * kMaxOpaqueFieldBytes + kMaxFrameBytes;
// DirectiveResult has one opaque result plus its fixed/identifier envelope.
inline constexpr std::uint64_t kMaxDirectiveResultTransferBytes =
    kMaxOpaqueFieldBytes + kMaxFrameBytes;
// OperationEvidence has one opaque evidence body; its fixed identities and
// bounded phase/group labels fit inside the remaining frame-sized envelope.
inline constexpr std::uint64_t kMaxOperationEvidenceTransferBytes =
    kMaxOpaqueFieldBytes + kMaxFrameBytes;
inline constexpr std::size_t kMaxDirectoryEntries = 4096;
inline constexpr std::size_t kMaxIdentifierBytes = 1024;
inline constexpr std::size_t kMaxProjectedNodes = 4096;
inline constexpr std::size_t kMaxProjectedGroups = 512;
inline constexpr std::size_t kMaxManifestEntries = 16384;
inline constexpr std::size_t kMaxProjectedPolicies = 4096;
inline constexpr std::size_t kMaxProjectedDirectives = 4096;
inline constexpr std::size_t kMaxCandidateFlows = 1024;

using WireId128 = std::array<std::uint8_t, 16>;
using WireHash256 = std::array<std::uint8_t, 32>;

// Generates an incarnation identity directly from the operating system CSPRNG
// and formats it as the canonical 40-character lowercase spelling.
absl::StatusOr<std::string> GenerateIdentity160();
// Generates a binary 128-bit protocol identity directly from the OS CSPRNG.
absl::StatusOr<WireId128> GenerateId128();
bool IsCanonicalIdentity160(std::string_view identity) noexcept;

// SHA-256 is the object-integrity algorithm fixed by this protocol family.
WireHash256 ComputeSha256(std::string_view bytes) noexcept;

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
  kDirectiveReceipt = 13,
  kDirectiveResult = 14,
  kResultCommitted = 15,
  kResultNoLongerTracked = 16,
  kOperationEvidence = 17,
  // Frame-sized canonical FullDesiredState. Larger projections use the
  // existing TransferKind::kFullDesiredState Start/Chunk/End form.
  kFullDesiredState = 18,
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
  std::uint32_t heartbeat_interval_ms = 0;
  std::uint32_t observation_ttl_ms = 0;
  std::uint32_t session_progress_timeout_ms = 0;

  friend bool operator==(const ServerHello&, const ServerHello&) = default;
};

enum class TransferKind : std::uint16_t {
  kFullDesiredState = 1,
  kObservationEvidence = 2,
  // Bytes are the canonical EncodeMessage payload for the named message
  // type; receivers feed the committed object back through DecodeMessage.
  kDirectivePayload = 3,
  kDirectiveResult = 4,
};

struct TransferStart {
  TransferKind kind = TransferKind::kFullDesiredState;
  WireId128 object_id{};
  std::uint64_t total_length = 0;
  WireHash256 sha256{};

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

struct TransferAbort {
  WireId128 object_id{};
  std::uint16_t reason = 0;

  friend bool operator==(const TransferAbort&, const TransferAbort&) = default;
};

// The reassembler itself retains only counters and incremental SHA-256 state.
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
  std::uint64_t source_meta_applied_index = 0;
  WireHash256 projection_hash{};
  WireHash256 object_hash{};

  friend bool operator==(const FullStateApplied&,
                         const FullStateApplied&) = default;
};

struct WireProjectionBasis {
  std::uint64_t source_meta_applied_index = 0;
  WireHash256 projection_hash{};

  friend bool operator==(const WireProjectionBasis&,
                         const WireProjectionBasis&) = default;
};

struct WireAuthorityAnchor {
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;
  std::uint64_t authority_version = 0;
  std::uint64_t grant_revision = 0;

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
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  std::uint64_t partition_replication_epoch = 0;
  std::string source_node_id;
  WireId128 source_assignment_id{};
  std::string source_boot_id;
  std::string source_history_id;
  std::vector<std::uint64_t> applied_next_lsns;

  friend bool operator==(const CandidateProgress&,
                         const CandidateProgress&) = default;
};

struct LeaseChallenge {
  WireId128 nonce{};
  WireHash256 projection_hash{};
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;
  std::uint64_t authority_version = 0;
  std::uint64_t grant_revision = 0;

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

struct Heartbeat {
  WireId128 session_id{};
  std::uint64_t heartbeat_sequence = 0;
  HeartbeatHealth health;
  HeartbeatRoleInformation role_information = NoRoleInformation{};

  friend bool operator==(const Heartbeat&, const Heartbeat&) = default;
};

// Boot/session-scoped soft evidence for one committed operation. Meta derives
// the reporter node from the authenticated connection; the explicit member
// assignment, population counters, and history prevent reuse after any
// reassignment, rebuild generation, operation, or process incarnation change.
struct OperationEvidence {
  WireId128 session_id{};
  std::string reporter_boot_id;
  WireId128 assignment_id{};
  WireId128 operation_id{};
  std::string kind_phase;
  WireHash256 evidence_hash{};
  std::string evidence;
  std::string group_id;
  std::uint64_t group_term = 0;
  std::uint64_t manifest_revision = 0;
  std::uint64_t partition_replication_epoch = 0;
  std::string replication_history_id;

  friend bool operator==(const OperationEvidence&,
                         const OperationEvidence&) = default;
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
  WireHash256 projection_hash{};
  std::string group_id;
  WireId128 assignment_id{};
  std::uint64_t group_term = 0;
  std::uint64_t authority_version = 0;
  std::uint64_t grant_revision = 0;
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
  WireHash256 current_projection_hash{};

  friend bool operator==(const LeaseDenied&, const LeaseDenied&) = default;
};

struct LeaseStateOutOfDate {
  WireId128 nonce{};
  WireHash256 current_projection_hash{};

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

enum class HeartbeatSequenceDisposition : std::uint8_t {
  kAcceptNew,
  kReplayCachedAck,
};

// Meta-side business-sequence guard. Frame sequence numbers are transport
// scoped and never replay; this separate sequence permits only an exact
// duplicate heartbeat to retrieve its cached application Ack.
class HeartbeatSequenceWindow {
 public:
  absl::StatusOr<HeartbeatSequenceDisposition> Observe(
      std::uint64_t sequence, const WireHash256& message_hash);
  void Reset() noexcept;

 private:
  std::uint64_t last_sequence_ = 0;
  WireHash256 last_hash_{};
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
  absl::Status MarkWritten(const WireId128& nonce,
                           std::int64_t lease_now_ms);
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
  // V1 uses payload only for initialize-empty-population's target history id;
  // every other kind requires it empty. Preconditions remain reserved.
  std::string payload;
  std::string preconditions;
  // Active V1 classification: population mutations set it; source
  // authorization and revocation do not. Data admission enforces the split.
  bool storage_mutating = false;
  // Reserved execution override; Data admission requires false in V1.
  bool force = false;

  friend bool operator==(const Directive&, const Directive&) = default;
};

enum class DirectiveReceiptStage : std::uint8_t {
  kAccepted = 1,
  kStarted = 2,
  kCompleted = 3,
};

struct DirectiveReceipt {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireDirectiveIdentity identity;
  DirectiveReceiptStage stage = DirectiveReceiptStage::kAccepted;

  friend bool operator==(const DirectiveReceipt&,
                         const DirectiveReceipt&) = default;
};

enum class DirectiveResultStatus : std::uint8_t {
  kSucceeded = 1,
  kFailed = 2,
  kRejected = 3,
};

struct DirectiveResult {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireId128 assignment_id{};
  WireDirectiveIdentity identity;
  DirectiveResultStatus status = DirectiveResultStatus::kSucceeded;
  WireHash256 result_hash{};
  std::string result;

  friend bool operator==(const DirectiveResult&,
                         const DirectiveResult&) = default;
};

struct ResultCommitted {
  WireId128 session_id{};
  std::string recipient_boot_id;
  WireDirectiveIdentity identity;
  WireHash256 result_hash{};
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

struct WireDesiredGroup {
  std::string group_id;
  std::vector<WireDesiredMember> members;
  // A fenced group may retain both owner fields for role classification even
  // though grant_active is false. Presence must match; there is no all-zero
  // identity sentinel in the 128-bit id space.
  std::optional<std::string> owner_node_id;
  std::optional<WireId128> owner_assignment_id;
  std::uint64_t group_term = 0;
  std::uint64_t authority_version = 0;
  std::uint64_t grant_revision = 0;
  std::uint32_t grant_duration_ms = 0;
  bool grant_active = false;
  std::uint64_t config_epoch = 0;
  std::vector<WireSlotRange> slot_ranges;
  std::uint64_t manifest_revision = 0;
  WireHash256 manifest_digest{};
  // Independent Meta freshness generation for a partition population. It is
  // part of population identity even when immutable manifest content stays
  // unchanged.
  std::uint64_t partition_replication_epoch = 0;
  std::string grant_policy_id;
  std::uint64_t grant_policy_version = 0;

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

struct WirePolicy {
  std::string policy_id;
  std::uint64_t version = 0;
  WireHash256 content_hash{};
  std::string content;

  friend bool operator==(const WirePolicy&, const WirePolicy&) = default;
};

// Session-independent form used inside a projection. The live Directive
// envelope adds the current session id only when it is sent.
struct WireProjectedDirective {
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
  // V1 uses payload only for initialize-empty-population's target history id;
  // every other kind requires it empty. Preconditions remain reserved.
  std::string payload;
  std::string preconditions;
  // Active V1 classification: population mutations set it; source
  // authorization and revocation do not. Data admission enforces the split.
  bool storage_mutating = false;
  // Reserved execution override; Data admission requires false in V1.
  bool force = false;

  friend bool operator==(const WireProjectedDirective&,
                         const WireProjectedDirective&) = default;
};

// Complete node-specific semantic projection sent either as one typed frame
// or as a FullDesiredState large object after each accepted session.
// `object_hash` is derived from the canonical bytes: it is not encoded (which
// would be self-referential), is ignored when encoding, and is populated by
// DecodeFullDesiredState.
struct FullDesiredState {
  std::uint64_t source_meta_applied_index = 0;
  std::uint64_t topology_epoch = 0;
  WireHash256 projection_hash{};
  WireHash256 object_hash{};
  std::vector<WireMetaEndpoint> meta_directory;
  std::vector<WireDataEndpoint> nodes;
  std::vector<WireDesiredGroup> groups;
  std::vector<WireManifestDocument> manifests;
  std::vector<WirePolicy> policies;
  std::vector<WireProjectedDirective> current_directives;
  WireHash256 directive_set_digest{};

  friend bool operator==(const FullDesiredState&,
                         const FullDesiredState&) = default;
};

// Canonical semantic body used as the FullDesiredState transfer payload.
// Decode derives object_hash as SHA-256 over these exact bytes.
absl::StatusOr<std::string> EncodeFullDesiredState(
    const FullDesiredState& state);
absl::StatusOr<FullDesiredState> DecodeFullDesiredState(
    std::string_view encoded);
// Consuming transfer overload. It releases the contiguous wire allocation on
// every return path so a decoded owning projection does not keep a second
// complete representation alive during installation.
absl::StatusOr<FullDesiredState> DecodeFullDesiredState(std::string&& encoded);
// Hashes only node-specific semantic content. Diagnostic applied indices,
// derived hashes, and directive projection-basis copies are normalized out,
// so an unrelated Raft commit cannot invalidate an installed projection.
absl::StatusOr<WireHash256> ComputeProjectionHash(
    const FullDesiredState& state);
// Returns a stable digest for the semantic directive set. Input order and the
// enclosing projection-basis copies do not affect the result; all other wire
// fields do. Duplicate directives remain observable through the encoded count.
absl::StatusOr<WireHash256> ComputeDirectiveSetDigest(
    const std::vector<WireProjectedDirective>& directives);

using WireMessage =
    std::variant<ClientHello, ServerHello, TransferStart, TransferChunk,
                 TransferEnd, TransferAbort, FullStateApplied, Heartbeat,
                 HeartbeatAck, OperationEvidence, Fence, FenceAck, Directive,
                 DirectiveReceipt, DirectiveResult, ResultCommitted,
                 ResultNoLongerTracked, FullDesiredState>;

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

}  // namespace keylane::cluster::control
