// Model-layer tests for encoding
// primitives (src/meta/encoding.cpp) and the committed command schema
// (src/meta/commands.cpp).
//
// The tests exercise only the public surface: encode/decode round-trips and
// rejection behavior (truncation, corruption, unknown version/command,
// over-cap fields, trailing bytes). Decode failures are the fail-stop
// class; domain validation rejections are the other class — the two must stay
// distinguishable.

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/state_apply.h"

namespace {

using keylane::meta::MetaReader;
using keylane::meta::MetaWriter;

// ---------------------------------------------------------------------------
// Encoding primitives: fixed-width little-endian integers, fixed bytes,
// length-prefixed strings, lists, optionals, strict bounds.
// ---------------------------------------------------------------------------

TEST(MetaModelEncoding, FixedWidthIntegersAreLittleEndian) {
  MetaWriter w;
  w.WriteU8(0x01);
  w.WriteU16(0x0203);
  w.WriteU32(0x04050607);
  w.WriteU64(0x08090A0B0C0D0E0F);

  const std::string expected = {
      '\x01',                                                          // u8
      '\x03', '\x02',                                                  // u16 LE
      '\x07', '\x06', '\x05', '\x04',                                  // u32 LE
      '\x0F', '\x0E', '\x0D', '\x0C', '\x0B', '\x0A', '\x09', '\x08',  // u64 LE
  };
  EXPECT_EQ(w.buffer(), expected);

  MetaReader r(w.buffer());
  auto u8 = r.ReadU8();
  auto u16 = r.ReadU16();
  auto u32 = r.ReadU32();
  auto u64 = r.ReadU64();
  ASSERT_TRUE(u8.ok() && u16.ok() && u32.ok() && u64.ok());
  EXPECT_EQ(*u8, 0x01);
  EXPECT_EQ(*u16, 0x0203);
  EXPECT_EQ(*u32, 0x04050607u);
  EXPECT_EQ(*u64, 0x08090A0B0C0D0E0F);
  EXPECT_TRUE(r.Finish().ok());
}

TEST(MetaModelEncoding, RawAndStringRoundTrip) {
  MetaWriter w;
  w.WriteRaw(std::string_view("\x00\x01\x02", 3));
  w.WriteString("hello");
  w.WriteString("");  // empty string is legal

  MetaReader r(w.buffer());
  auto raw = r.ReadRaw(3);
  ASSERT_TRUE(raw.ok());
  EXPECT_EQ(*raw, std::string_view("\x00\x01\x02", 3));
  auto s = r.ReadString(/*max_bytes=*/16);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(*s, "hello");
  auto empty = r.ReadString(/*max_bytes=*/16);
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty->empty());
  EXPECT_TRUE(r.Finish().ok());
}

TEST(MetaModelEncoding, ListRoundTrip) {
  MetaWriter w;
  w.WriteList(std::vector<std::uint32_t>{10, 20, 30},
              [](MetaWriter& ww, std::uint32_t v) { ww.WriteU32(v); });

  MetaReader r(w.buffer());
  auto items = r.ReadList<std::uint32_t>(
      /*max_count=*/8, [](MetaReader& rr) { return rr.ReadU32(); });
  ASSERT_TRUE(items.ok()) << items.status();
  EXPECT_EQ(*items, (std::vector<std::uint32_t>{10, 20, 30}));
  EXPECT_TRUE(r.Finish().ok());
}

TEST(MetaModelEncoding, NestedListRoundTrip) {
  using Inner = std::vector<std::uint32_t>;
  MetaWriter w;
  w.WriteList(std::vector<Inner>{{1, 2}, {}, {3}},
              [](MetaWriter& ww, const Inner& inner) {
                ww.WriteList(inner, [](MetaWriter& www, std::uint32_t v) {
                  www.WriteU32(v);
                });
              });

  MetaReader r(w.buffer());
  auto outer = r.ReadList<Inner>(/*max_count=*/4, [](MetaReader& rr) {
    return rr.ReadList<std::uint32_t>(
        /*max_count=*/4, [](MetaReader& rrr) { return rrr.ReadU32(); });
  });
  ASSERT_TRUE(outer.ok()) << outer.status();
  ASSERT_EQ(outer->size(), 3);
  EXPECT_EQ((*outer)[0], (Inner{1, 2}));
  EXPECT_TRUE((*outer)[1].empty());
  EXPECT_EQ((*outer)[2], (Inner{3}));
  EXPECT_TRUE(r.Finish().ok());
}

TEST(MetaModelEncoding, OptionalRoundTrip) {
  MetaWriter w;
  w.WriteOptional(std::optional<std::uint64_t>{42},
                  [](MetaWriter& ww, std::uint64_t v) { ww.WriteU64(v); });
  w.WriteOptional(std::optional<std::uint64_t>{},
                  [](MetaWriter& ww, std::uint64_t v) { ww.WriteU64(v); });

  MetaReader r(w.buffer());
  auto present = r.ReadOptional<std::uint64_t>(
      [](MetaReader& rr) { return rr.ReadU64(); });
  ASSERT_TRUE(present.ok()) << present.status();
  ASSERT_TRUE(present->has_value());
  EXPECT_EQ(**present, 42);
  auto absent = r.ReadOptional<std::uint64_t>(
      [](MetaReader& rr) { return rr.ReadU64(); });
  ASSERT_TRUE(absent.ok()) << absent.status();
  EXPECT_FALSE(absent->has_value());
  EXPECT_TRUE(r.Finish().ok());
}

TEST(MetaModelEncoding, TruncationFails) {
  MetaWriter w;
  w.WriteU64(0x0102030405060708);
  // Every proper prefix of an 8-byte integer fails to decode.
  for (std::size_t len = 0; len < 8; ++len) {
    MetaReader r(std::string_view(w.buffer()).substr(0, len));
    EXPECT_FALSE(r.ReadU64().ok()) << "len=" << len;
  }
}

TEST(MetaModelEncoding, TruncatedStringBodyFails) {
  MetaWriter w;
  w.WriteString("abcdef");
  // Length prefix says 6 but fewer bytes remain.
  for (std::size_t len = 0; len < w.buffer().size(); ++len) {
    MetaReader r(std::string_view(w.buffer()).substr(0, len));
    EXPECT_FALSE(r.ReadString(/*max_bytes=*/16).ok()) << "len=" << len;
  }
}

TEST(MetaModelEncoding, StringLengthOverCapFails) {
  MetaWriter w;
  w.WriteString("abc");
  MetaReader r(w.buffer());
  // Cap below the encoded length fails even though bytes are present.
  EXPECT_FALSE(r.ReadString(/*max_bytes=*/2).ok());
}

TEST(MetaModelEncoding, StringLengthPrefixOverCapFailsWithoutBody) {
  MetaWriter w;
  w.WriteU32(1000);  // length prefix far above the cap; no body at all
  MetaReader r(w.buffer());
  // The cap check must fire before the bounds check.
  EXPECT_FALSE(r.ReadString(/*max_bytes=*/16).ok());
}

TEST(MetaModelEncoding, ListCountOverCapFails) {
  MetaWriter w;
  w.WriteU32(9);  // count above the reader's cap
  MetaReader r(w.buffer());
  auto items = r.ReadList<std::uint32_t>(
      /*max_count=*/8, [](MetaReader& rr) { return rr.ReadU32(); });
  EXPECT_FALSE(items.ok());
}

TEST(MetaModelEncoding, TruncatedListElementFails) {
  MetaWriter w;
  w.WriteList(std::vector<std::uint32_t>{1, 2},
              [](MetaWriter& ww, std::uint32_t v) { ww.WriteU32(v); });
  // Drop the last byte: count is fine, the second element is truncated.
  MetaReader r(std::string_view(w.buffer()).substr(0, w.buffer().size() - 1));
  auto items = r.ReadList<std::uint32_t>(
      /*max_count=*/8, [](MetaReader& rr) { return rr.ReadU32(); });
  EXPECT_FALSE(items.ok());
}

TEST(MetaModelEncoding, BoolRoundTripAndInvalidTagFails) {
  MetaWriter w;
  w.WriteBool(false);
  w.WriteBool(true);

  MetaReader r(w.buffer());
  auto false_value = r.ReadBool("invalid bool");
  ASSERT_TRUE(false_value.ok()) << false_value.status();
  EXPECT_FALSE(*false_value);
  auto true_value = r.ReadBool("invalid bool");
  ASSERT_TRUE(true_value.ok()) << true_value.status();
  EXPECT_TRUE(*true_value);
  EXPECT_TRUE(r.Finish().ok());

  MetaReader invalid(std::string_view("\x02", 1));
  auto invalid_value = invalid.ReadBool("invalid bool");
  ASSERT_FALSE(invalid_value.ok());
  EXPECT_EQ(invalid_value.status().message(), "invalid bool");
}

TEST(MetaModelEncoding, BadOptionalPresenceTagFails) {
  MetaWriter w;
  w.WriteU8(2);  // presence tag must be 0 or 1
  MetaReader r(w.buffer());
  auto opt = r.ReadOptional<std::uint64_t>(
      [](MetaReader& rr) { return rr.ReadU64(); });
  EXPECT_FALSE(opt.ok());
}

TEST(MetaModelEncoding, TrailingBytesFailFinish) {
  MetaWriter w;
  w.WriteU8(1);
  w.WriteU8(2);
  MetaReader r(w.buffer());
  ASSERT_TRUE(r.ReadU8().ok());
  EXPECT_FALSE(r.Finish().ok());  // one unread byte remains
}

TEST(MetaModelEncoding, FixedArrayRoundTrip) {
  std::array<std::uint8_t, 16> id{};
  for (std::size_t i = 0; i < id.size(); ++i)
    id[i] = static_cast<std::uint8_t>(i);
  MetaWriter w;
  keylane::meta::WriteFixedArray(w, id);

  MetaReader r(w.buffer());
  auto back = keylane::meta::ReadFixedArray<16>(r);
  ASSERT_TRUE(back.ok()) << back.status();
  EXPECT_EQ(*back, id);
  EXPECT_TRUE(r.Finish().ok());
  MetaReader short_r(std::string_view("\x00\x01", 2));
  EXPECT_FALSE(keylane::meta::ReadFixedArray<16>(short_r).ok());
}

// ---------------------------------------------------------------------------
// Failure classification: fail-stop decode failures vs domain
// rejections must be distinguishable at the type/enum level.
// ---------------------------------------------------------------------------

TEST(MetaModelEncoding, FailureClassesAreDistinguishable) {
  using keylane::meta::MetaFailureClass;
  using keylane::meta::MetaFailureClassOf;

  // Decode-path failures (produced by MetaReader) classify as fail-stop.
  MetaReader r(std::string_view("\x00", 1));
  auto truncated = r.ReadU64();
  ASSERT_FALSE(truncated.ok());
  EXPECT_EQ(MetaFailureClassOf(truncated.status()),
            MetaFailureClass::kFailStop);
  EXPECT_EQ(truncated.status().code(), absl::StatusCode::kInvalidArgument);

  EXPECT_EQ(MetaFailureClassOf(keylane::meta::MetaFailStopError("x")),
            MetaFailureClass::kFailStop);

  // Domain rejections (apply/propose validation) classify
  // separately: log index consumed, audit record written, state unchanged.
  const absl::Status domain = keylane::meta::MetaDomainRejectError("cas");
  EXPECT_EQ(MetaFailureClassOf(domain), MetaFailureClass::kDomainReject);
  EXPECT_EQ(domain.code(), absl::StatusCode::kFailedPrecondition);
}

// ---------------------------------------------------------------------------
// Command envelope: u16 schema_version | u16 command tag | request_id | body.
// ---------------------------------------------------------------------------

using keylane::meta::DecodeMetaCommand;
using keylane::meta::EncodeMetaCommand;
using keylane::meta::MetaCommand;
using keylane::meta::MetaRequestId;

MetaRequestId MakeRequestId(std::uint8_t seed) {
  MetaRequestId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed + i);
  }
  return id;
}

// Encodes `cmd`, asserts success, and returns the bytes.
std::string MustEncode(const MetaCommand& cmd) {
  const auto encoded = EncodeMetaCommand(cmd);
  EXPECT_TRUE(encoded.ok()) << encoded.status();
  return encoded.value_or("");
}

// Decode must fail with the fail-stop class because the same bytes must fail
// identically on every node.
void ExpectDecodeFailStop(std::string_view bytes) {
  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_FALSE(decoded.ok())
      << "decoded unexpectedly: " << bytes.size() << " bytes";
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(decoded.status()),
            keylane::meta::MetaFailureClass::kFailStop);
}

template <typename T>
void ExpectRoundTrip(const T& cmd) {
  const std::string bytes = MustEncode(cmd);
  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<T>(*decoded));
  EXPECT_EQ(std::get<T>(*decoded), cmd);
}

void ExpectRecordDecodeFailStop(std::string_view bytes) {
  const auto decoded = keylane::meta::DecodeMetaGroupRecord(bytes);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(decoded.status()),
            keylane::meta::MetaFailureClass::kFailStop);
}

// Encode-side cap violations are proposal-validation failures (domain
// reject): nothing out-of-spec ever reaches the wire.
void ExpectEncodeDomainReject(const MetaCommand& cmd) {
  const auto encoded = EncodeMetaCommand(cmd);
  ASSERT_FALSE(encoded.ok()) << "encoded unexpectedly";
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(encoded.status()),
            keylane::meta::MetaFailureClass::kDomainReject);
}

keylane::meta::RegisterNode MakeRegisterNode() {
  keylane::meta::RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(0x10);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";  // 40 hex
  cmd.principal_ = "keylane://node/0123456789abcdef0123456789abcdef01234567";
  cmd.endpoints_ = {"10.0.0.1:7000", "10.0.0.1:17000"};
  cmd.capability_mask_ = 0x5;
  cmd.role_ = keylane::meta::MetaNodeRole::kReplica;
  return cmd;
}

TEST(MetaModelCommands, RegisterNodeRoundTrip) {
  const keylane::meta::RegisterNode cmd = MakeRegisterNode();
  const std::string bytes = MustEncode(cmd);

  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<keylane::meta::RegisterNode>(*decoded));
  EXPECT_EQ(std::get<keylane::meta::RegisterNode>(*decoded), cmd);
}

TEST(MetaModelCommands, EnvelopeStartsWithFormatVersionThenTag) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  ASSERT_GE(bytes.size(), 4u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  const std::uint16_t version = static_cast<std::uint16_t>(p[0] | (p[1] << 8));
  const std::uint16_t tag = static_cast<std::uint16_t>(p[2] | (p[3] << 8));
  EXPECT_EQ(version, keylane::meta::kMetaFormatVersion);
  EXPECT_EQ(tag, static_cast<std::uint16_t>(
                     keylane::meta::MetaCommandTag::kRegisterNode));
}

TEST(MetaModelCommands, UnknownFormatVersionFails) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  for (const std::uint16_t bad_version : {0, 0x7FFF, 0xFFFF}) {
    std::string corrupt = bytes;
    corrupt[0] = static_cast<char>(bad_version & 0xFF);
    corrupt[1] = static_cast<char>((bad_version >> 8) & 0xFF);
    ExpectDecodeFailStop(corrupt);
  }
}

TEST(MetaModelCommands, UnknownCommandTagFails) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  std::string corrupt = bytes;
  corrupt[2] = '\xFF';  // tag u16 = 0xFFFF
  corrupt[3] = '\xFF';
  ExpectDecodeFailStop(corrupt);
}

TEST(MetaModelCommands, TruncatedCommandFails) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  // Every proper prefix must fail to decode.
  for (std::size_t len = 0; len < bytes.size(); ++len) {
    ExpectDecodeFailStop(std::string_view(bytes).substr(0, len));
  }
}

TEST(MetaModelCommands, ActorContextRoundTripsOnTheWire) {
  // The raft-log encoding carries the trusted-entry-injected ActorContext as
  // ordinary bounded fields, so a follower's apply can persist the real actor
  // into audit/journal. Unforgeability is the entry layer's property: only
  // trusted ctl/coordinator entries construct commands, and the ctl protocol
  // never accepts actor fields from external callers.
  keylane::meta::RegisterNode cmd = MakeRegisterNode();
  cmd.actor_.principal_ = "keylane://operator/alice";
  cmd.actor_.readable_time_ = "2026-09-04T01:02:03Z";

  const std::string bytes = MustEncode(cmd);
  EXPECT_NE(bytes.find("keylane://operator/alice"), std::string::npos);
  EXPECT_NE(bytes.find("2026-09-04T01:02:03Z"), std::string::npos);

  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  // Equality covers the actor fields: they survive the round trip verbatim.
  EXPECT_EQ(std::get<keylane::meta::RegisterNode>(*decoded), cmd);
}

TEST(MetaModelCommands, ActorFieldCapsEnforced) {
  // Encode side: an over-cap actor field is a proposal-validation failure.
  keylane::meta::RegisterNode cmd = MakeRegisterNode();
  cmd.actor_.principal_ =
      std::string(keylane::meta::kMaxMetaPrincipalBytes + 1, 'p');
  ExpectEncodeDomainReject(cmd);
  cmd.actor_.principal_ = "keylane://operator/alice";
  cmd.actor_.readable_time_ =
      std::string(keylane::meta::kMaxMetaActorReadableTimeBytes + 1, 't');
  ExpectEncodeDomainReject(cmd);

  // Decode side: an over-cap actor length prefix on the wire is fail-stop.
  // Hand-built RegisterNode header: version | tag | request_id | actor...
  {
    keylane::meta::MetaWriter w;
    w.WriteU16(keylane::meta::kMetaFormatVersion);
    w.WriteU16(static_cast<std::uint16_t>(
        keylane::meta::MetaCommandTag::kRegisterNode));
    w.WriteRaw(std::string(16, '\0'));                      // request_id
    w.WriteU32(keylane::meta::kMaxMetaPrincipalBytes + 1);  // actor prefix
    ExpectDecodeFailStop(w.buffer());
  }
  {
    keylane::meta::MetaWriter w;
    w.WriteU16(keylane::meta::kMetaFormatVersion);
    w.WriteU16(static_cast<std::uint16_t>(
        keylane::meta::MetaCommandTag::kRegisterNode));
    w.WriteRaw(std::string(16, '\0'));
    w.WriteString("keylane://operator/alice");
    // readable_time prefix over its cap.
    w.WriteU32(keylane::meta::kMaxMetaActorReadableTimeBytes + 1);
    ExpectDecodeFailStop(w.buffer());
  }
}

TEST(MetaModelCommands, DecodeRejectsMissingActorFields) {
  // Bytes shaped like the obsolete pre-actor layout (request_id immediately
  // followed by node_id) are truncated input under the current layout: the
  // node_id length prefix is consumed as the actor principal prefix and the
  // decode runs out of bytes. (Every proper prefix already fails via
  // TruncatedCommandFails; this names the actor-position case explicitly.)
  keylane::meta::MetaWriter w;
  w.WriteU16(keylane::meta::kMetaFormatVersion);
  w.WriteU16(
      static_cast<std::uint16_t>(keylane::meta::MetaCommandTag::kRegisterNode));
  w.WriteRaw(std::string(16, '\0'));  // request_id
  w.WriteString(MakeRegisterNode().node_id_);
  w.WriteString("keylane://node/x");
  w.WriteU32(0);  // empty endpoints
  w.WriteU64(0x5);
  w.WriteU8(1);
  ExpectDecodeFailStop(w.buffer());
}

// ---------------------------------------------------------------------------
// identity/enrollment.
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, UpdateNodeRoundTrip) {
  keylane::meta::UpdateNode cmd;
  cmd.request_id_ = MakeRequestId(0x20);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_revision_ = 41;
  cmd.endpoints_ = {"10.0.0.9:7000"};
  cmd.capability_mask_ = 0x3;
  ExpectRoundTrip(cmd);
}

// UpdateNode must not be able to modify the principal binding because rotation
// is unimplemented. This is a schema-level guarantee, so assert it
// structurally. (The indirection through a template makes the member access
// dependent, so the requires-expression can fail softly.)
template <typename T>
concept HasPrincipalField = requires(T t) { t.principal_; };
static_assert(!HasPrincipalField<keylane::meta::UpdateNode>);

TEST(MetaModelCommands, RetireNodeRoundTrip) {
  keylane::meta::RetireNode cmd;
  cmd.request_id_ = MakeRequestId(0x21);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_revision_ = 42;
  ExpectRoundTrip(cmd);
}

// ---------------------------------------------------------------------------
// topology.
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, CreateGroupRoundTrip) {
  keylane::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.new_topology_epoch_ = 100;  // absolute value
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, AssignNodeToGroupRoundTrip) {
  keylane::meta::AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.assignment_id_.fill(0x31);
  cmd.role_ = keylane::meta::MetaNodeRole::kReplica;
  cmd.expected_revision_ = 7;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, RemoveNodeFromGroupRoundTrip) {
  keylane::meta::RemoveNodeFromGroup cmd;
  cmd.request_id_ = MakeRequestId(0x32);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.expected_revision_ = 8;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, SetSlotMapRoundTrip) {
  keylane::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x33);
  cmd.ranges_ = {
      {0, 5460, "0123456789abcdef0123456789abcdef01234567"},
      {5461, 10922, "89abcdef0123456789abcdef0123456789abcdef"},
      {10923, 16383, "456789abcdef0123456789abcdef0123456789ab"},
  };
  cmd.new_topology_epoch_ = 101;  // absolute value
  cmd.config_epochs_ = {
      {"0123456789abcdef0123456789abcdef01234567", 11},
      {"89abcdef0123456789abcdef0123456789abcdef", 22},
  };
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, GroupRecordRoundTrip) {
  // The per-group committed record contains owner, group_term,
  // authority_version, population_manifest_revision, and
  // partition_replication_epoch.
  // replication_history_id is deliberately absent because it is scoped to a
  // data-plane boot. The record codec is defined here.
  keylane::meta::MetaGroupRecord record;
  record.owner_ = "0123456789abcdef0123456789abcdef01234567";
  record.group_term_ = 9;
  record.authority_version_ = 4;
  record.population_manifest_revision_ = 777;
  record.population_manifest_digest_.fill(0x77);
  record.partition_replication_epoch_ = 3;

  const auto encoded = keylane::meta::EncodeMetaGroupRecord(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  // Records carry the same u16 format-version envelope convention.
  ASSERT_GE(encoded->size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(encoded->data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            keylane::meta::kMetaFormatVersion);

  const auto decoded = keylane::meta::DecodeMetaGroupRecord(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, record);

  // Trailing bytes and truncation fail.
  ExpectRecordDecodeFailStop(*encoded + '\0');
  ExpectRecordDecodeFailStop(encoded->substr(0, encoded->size() - 1));
}

// ---------------------------------------------------------------------------
// Term and grant commands.
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, BeginGroupTermRoundTrip) {
  keylane::meta::BeginGroupTerm cmd;
  cmd.request_id_ = MakeRequestId(0x40);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 41;  // T-1
  cmd.new_term_ = 42;       // T
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, GrantAuthorityRoundTrip) {
  keylane::meta::GrantAuthority cmd;
  cmd.request_id_ = MakeRequestId(0x41);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.term_ = 42;
  cmd.authority_version_ = 4;
  cmd.grant_.lease_duration_ms_ = 5000;
  cmd.grant_.policy_id_ = "migration-policy";
  cmd.grant_.policy_version_ = 3;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, ActivateAuthorityRoundTrip) {
  keylane::meta::ActivateAuthority cmd;
  cmd.request_id_ = MakeRequestId(0x42);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 42;
  cmd.new_owner_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.grant_.lease_duration_ms_ = 5000;
  cmd.grant_.policy_id_ = "failover-policy";
  cmd.grant_.policy_version_ = 1;
  cmd.new_authority_version_ = 5;
  cmd.new_topology_epoch_ = 102;
  cmd.new_config_epoch_ = 12;
  ExpectRoundTrip(cmd);
}

// ActivateAuthority is the failover/migration atomic commit point and must
// NOT move the term; it validates
// expected_term but carries no new term. Schema-level guarantee, asserted
// structurally.
template <typename T>
concept HasBareTermField = requires(T t) { t.term_; };
template <typename T>
concept HasNewTermField = requires(T t) { t.new_term_; };
static_assert(!HasBareTermField<keylane::meta::ActivateAuthority>);
static_assert(!HasNewTermField<keylane::meta::ActivateAuthority>);
static_assert(requires(keylane::meta::ActivateAuthority t) {
  t.expected_term_;
});

TEST(MetaModelCommands, RevokeGrantRoundTrip) {
  keylane::meta::RevokeGrant cmd;
  cmd.request_id_ = MakeRequestId(0x43);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 42;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, FenceGroupRoundTrip) {
  keylane::meta::FenceGroup cmd;
  cmd.request_id_ = MakeRequestId(0x44);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 42;
  ExpectRoundTrip(cmd);
}

// ---------------------------------------------------------------------------
// Policy documents are versioned and content-hash addressed.
// ---------------------------------------------------------------------------

keylane::meta::MetaHash256 MakeHash(std::uint8_t seed) {
  keylane::meta::MetaHash256 h{};
  for (std::size_t i = 0; i < h.size(); ++i) {
    h[i] = static_cast<std::uint8_t>(seed ^ i);
  }
  return h;
}

TEST(MetaModelCommands, PutPolicyRoundTrip) {
  keylane::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0x50);
  cmd.policy_id_ = "migration-policy";
  cmd.version_ = 3;
  cmd.content_ = "{\"phases\":[\"prepare\",\"move\",\"cutover\"]}";
  cmd.content_hash_ = MakeHash(0x5A);
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, RetirePolicyRoundTrip) {
  // Apply refuses to retire a version still referenced by an active
  // grant or a non-terminal operation — domain validation, not the codec.
  keylane::meta::RetirePolicy cmd;
  cmd.request_id_ = MakeRequestId(0x51);
  cmd.policy_id_ = "migration-policy";
  cmd.version_ = 2;
  ExpectRoundTrip(cmd);
}

// ---------------------------------------------------------------------------
// operation journal. operation_id is the client-provided
// stable UUID and permanent idempotency key; operation_seq = the raft log
// index of the SubmitOperation command, assigned by apply, and appears
// in commands only as an archive reference.
// ---------------------------------------------------------------------------

keylane::meta::MetaOperationId MakeOperationId(std::uint8_t seed) {
  keylane::meta::MetaOperationId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed * 3 + i);
  }
  return id;
}

keylane::meta::MetaEvidenceSummary MakeEvidence(std::uint8_t seed) {
  keylane::meta::MetaEvidenceSummary ev;
  ev.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  ev.group_id_ = "group-a";
  ev.assignment_id_.fill(static_cast<std::uint8_t>(seed + 1));
  for (std::size_t i = 0; i < ev.boot_incarnation_.size(); ++i) {
    ev.boot_incarnation_[i] = static_cast<std::uint8_t>(seed + 7 * i);
  }
  ev.group_term_ = 42;
  ev.population_manifest_revision_ = 777;
  ev.population_manifest_digest_ = MakeHash(seed + 1);
  ev.partition_replication_epoch_ = 778;
  ev.replication_history_id_.fill(seed);
  ev.operation_id_ = MakeOperationId(seed);
  ev.kind_hash_ = MakeHash(seed);
  return ev;
}

TEST(MetaModelCommands, SubmitOperationRoundTrip) {
  keylane::meta::SubmitOperation cmd;
  cmd.request_id_ = MakeRequestId(0x60);
  cmd.operation_id_ = MakeOperationId(0x01);
  cmd.kind_ = "migration";
  cmd.intent_ = "{\"slot\":42,\"to\":\"group-b\"}";
  cmd.intent_hash_ = MakeHash(0x11);
  cmd.replication_history_id_.fill(0x12);
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, TransitionOperationPhaseRoundTrip) {
  keylane::meta::TransitionOperationPhase cmd;
  cmd.request_id_ = MakeRequestId(0x61);
  cmd.operation_id_ = MakeOperationId(0x02);
  cmd.expected_revision_ = 3;
  cmd.kind_phase_blob_ = "{\"phase\":\"cutover\"}";
  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(0x10);
  directive.attempt_id_.fill(0x11);
  directive.recipient_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  directive.target_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  directive.target_boot_id_.fill(0x12);
  directive.assignment_id_.fill(0x13);
  directive.source_node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  directive.source_assignment_id_.fill(0x17);
  directive.source_boot_id_.fill(0x14);
  directive.source_replication_history_id_.fill(0x15);
  directive.group_id_ = "group-a";
  directive.group_term_ = 42;
  directive.authority_version_ = 7;
  directive.grant_revision_ = 101;
  directive.population_manifest_revision_ = 777;
  directive.population_manifest_digest_ = MakeHash(0x16);
  directive.partition_replication_epoch_ = 778;
  directive.kind_ = "rebuild";
  directive.payload_ = "{\"partition\":5}";
  directive.preconditions_ = "{\"source_caught_up\":true}";
  directive.storage_mutating_ = true;
  cmd.current_directives_ = {directive};
  cmd.evidence_ = {MakeEvidence(0x01), MakeEvidence(0x02)};
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, CompleteOperationRoundTrip) {
  keylane::meta::CompleteOperation cmd;
  cmd.request_id_ = MakeRequestId(0x62);
  cmd.operation_id_ = MakeOperationId(0x03);
  cmd.expected_revision_ = 4;
  cmd.result_ = "{\"moved_slots\":100}";
  cmd.data_loss_possible_ = true;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, AbortOperationRoundTrip) {
  keylane::meta::AbortOperation cmd;
  cmd.request_id_ = MakeRequestId(0x63);
  cmd.operation_id_ = MakeOperationId(0x04);
  cmd.expected_revision_ = 2;
  cmd.reason_ = "target group fenced";
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, ArchiveOperationsRoundTrip) {
  // Non-contiguous archival of terminal operations; apply
  // rejects references to non-terminal or unknown operations.
  keylane::meta::ArchiveOperations cmd;
  cmd.request_id_ = MakeRequestId(0x64);
  cmd.operation_seqs_ = {100, 137, 4096};
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, DirectiveResultReceiptCommandsRoundTrip) {
  keylane::meta::CommitDirectiveResult commit;
  commit.request_id_ = MakeRequestId(0x65);
  commit.operation_id_ = MakeOperationId(0x05);
  commit.directive_id_.fill(0x20);
  commit.attempt_id_.fill(0x21);
  commit.directive_revision_ = 88;
  commit.recipient_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  commit.recipient_boot_id_.fill(0x22);
  commit.assignment_id_.fill(0x23);
  commit.status_ = keylane::meta::MetaDirectiveResultStatus::kFailed;
  commit.result_ = "source rejected the replication handshake";
  commit.result_hash_ = keylane::meta::MetaSha256(commit.result_);
  ExpectRoundTrip(commit);

  keylane::meta::PruneTerminalReceipts prune;
  prune.request_id_ = MakeRequestId(0x66);
  prune.receipts_ = {{commit.operation_id_, commit.directive_id_,
                      commit.attempt_id_, commit.directive_revision_}};
  ExpectRoundTrip(prune);
}

TEST(MetaModelCommands, AdministrativeCommandsRoundTrip) {
  keylane::meta::PruneAudit audit;
  audit.through_log_index_ = 42;
  ExpectRoundTrip(audit);

  keylane::meta::PruneOperationArchive operations;
  operations.operation_seqs_ = {3, 8, 13};
  ExpectRoundTrip(operations);

  keylane::meta::BindMetaMember bind;
  bind.server_id_ = 7;
  bind.principal_ = "keylane://meta/7";
  bind.data_control_endpoint_ = "10.0.0.7:7100";
  ExpectRoundTrip(bind);

  keylane::meta::RetireMetaMember retire;
  retire.server_id_ = 7;
  ExpectRoundTrip(retire);
}

TEST(MetaModelCommands, SetAuditPolicyRoundTrips) {
  keylane::meta::SetAuditPolicy command;
  command.request_id_ = MakeRequestId(0x74);
  command.policy_ = keylane::meta::MetaAuditPolicy::kStrictExport;
  command.attestation_ = "ticket OPS-4321";
  ExpectRoundTrip(command);
}

TEST(MetaModelCommands, SetGroupReplicationStateRoundTrip) {
  keylane::meta::SetGroupReplicationState command;
  command.request_id_ = MakeRequestId(0x75);
  command.group_id_ = "g1";
  command.expected_population_manifest_revision_ = 7;
  command.new_population_manifest_revision_ = 8;
  command.expected_population_manifest_digest_.fill(0x70);
  command.new_population_manifest_digest_.fill(0x80);
  command.expected_partition_replication_epoch_ = 10;
  command.new_partition_replication_epoch_ = 11;
  command.new_topology_epoch_ = 12;
  ExpectRoundTrip(command);
}

// ---------------------------------------------------------------------------
// Cap enforcement fails safely without truncation. Encode-side violations
// are proposal-validation failures (kDomainReject); over-cap bytes on the
// wire are decode failures (kFailStop).
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, EncodeRejectsOverCapPayload) {
  keylane::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0x80);
  cmd.policy_id_ = "migration-policy";
  cmd.version_ = 1;
  cmd.content_ = std::string(keylane::meta::kMaxMetaPayloadBytes + 1, 'x');
  cmd.content_hash_ = MakeHash(0x01);
  ExpectEncodeDomainReject(cmd);
  // Exactly at the cap it still encodes (bounds are inclusive).
  cmd.content_.resize(keylane::meta::kMaxMetaPayloadBytes);
  EXPECT_TRUE(EncodeMetaCommand(cmd).ok());
}

TEST(MetaModelCommands, EncodeRejectsOverCapListsAndFields) {
  keylane::meta::RegisterNode reg = MakeRegisterNode();
  reg.endpoints_.resize(keylane::meta::kMaxMetaEndpointsPerNode + 1, "e");
  ExpectEncodeDomainReject(reg);

  keylane::meta::RegisterNode bad_id = MakeRegisterNode();
  bad_id.node_id_ = std::string(keylane::meta::kMetaNodeIdBytes + 1, 'a');
  ExpectEncodeDomainReject(bad_id);

  keylane::meta::ArchiveOperations arch;
  arch.request_id_ = MakeRequestId(0x81);
  arch.operation_seqs_.resize(keylane::meta::kMaxMetaActiveOperations + 1, 1);
  ExpectEncodeDomainReject(arch);
}

TEST(MetaModelCommands, EncodeRejectsOutOfRangeSlotRange) {
  keylane::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x82);
  cmd.ranges_ = {{100, 99, "g"}};  // first > last
  ExpectEncodeDomainReject(cmd);
  cmd.ranges_ = {{0, keylane::meta::kMetaSlotCount, "g"}};  // slot out of range
  ExpectEncodeDomainReject(cmd);
}

TEST(MetaModelCommands, EncodeRejectsOversizedCommand) {
  // 16384 maximally-sized slot assignments exceed the 1 MiB command cap.
  keylane::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x83);
  const std::string group_id(keylane::meta::kMaxMetaGroupIdBytes, 'g');
  for (std::uint32_t i = 0; i < keylane::meta::kMaxMetaSlotRangeCount; ++i) {
    cmd.ranges_.push_back({0, 0, group_id});
  }
  ExpectEncodeDomainReject(cmd);
}

TEST(MetaModelCommands, DecodeRejectsOverCapLengthPrefix) {
  // Hand-build a PutPolicy whose content length prefix exceeds the payload
  // cap; the reader must reject on the cap before even looking for the body.
  keylane::meta::MetaWriter w;
  w.WriteU16(keylane::meta::kMetaFormatVersion);
  w.WriteU16(
      static_cast<std::uint16_t>(keylane::meta::MetaCommandTag::kPutPolicy));
  w.WriteRaw(std::string(16, '\0'));  // request_id
  w.WriteString("migration-policy");
  w.WriteU64(1);
  w.WriteU32(keylane::meta::kMaxMetaPayloadBytes + 1);  // content prefix
  ExpectDecodeFailStop(w.buffer());
}

TEST(MetaModelCommands, DecodeRejectsCorruptEnumAndBool) {
  {
    const std::string bytes = MustEncode(MakeRegisterNode());
    std::string corrupt = bytes;
    corrupt.back() = '\x09';  // role is the last byte; 9 is not a role
    ExpectDecodeFailStop(corrupt);
  }
  {
    keylane::meta::CompleteOperation cmd;
    cmd.request_id_ = MakeRequestId(0x84);
    cmd.operation_id_ = MakeOperationId(0x05);
    cmd.expected_revision_ = 1;
    cmd.result_ = "{}";
    const std::string bytes = MustEncode(cmd);
    std::string corrupt = bytes;
    corrupt.back() = '\x07';  // data_loss_possible is the last byte (0/1)
    ExpectDecodeFailStop(corrupt);
  }
}

TEST(MetaModelCommands, DecodeRejectsTrailingBytes) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  ExpectDecodeFailStop(bytes + '\0');
}

TEST(MetaModelCommands, DecodeRejectsBufferOverCommandCap) {
  std::string bytes = MustEncode(MakeRegisterNode());
  bytes.resize(keylane::meta::kMaxMetaCommandBytes + 1, '\0');
  ExpectDecodeFailStop(bytes);
}

// ---------------------------------------------------------------------------
// Apply layer (src/meta/state_apply.cpp): the ApplyCommitted dispatcher.
// Tests drive the public surface only: MetaStores + ApplyCommitted with
// caller-injected actor fields, plus the whole-aggregate snapshot codec.
// ---------------------------------------------------------------------------

using keylane::meta::ApplyCommitted;
using keylane::meta::MetaApplyResult;
using keylane::meta::MetaAuditVerdict;
using keylane::meta::MetaStores;

constexpr std::string_view kActorPrincipal = "keylane://operator/alice";
constexpr std::string_view kReadableTime = "2026-09-04T00:00:00Z";

// 40 lowercase hex chars, distinct per n (the data-plane node_id convention).
std::string MakeNodeId(std::uint32_t n) {
  std::string id(40, '0');
  for (int i = 39; n > 0 && i >= 0; --i, n >>= 4) {
    id[i] = "0123456789abcdef"[n & 0xF];
  }
  return id;
}

keylane::meta::RegisterNode MakeRegisterFor(std::uint32_t n) {
  keylane::meta::RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(static_cast<std::uint8_t>(n));
  cmd.node_id_ = MakeNodeId(n);
  cmd.principal_ = "keylane://node/" + MakeNodeId(n);
  cmd.endpoints_ = {"10.0.0.1:7000"};
  cmd.capability_mask_ = 0x5;
  cmd.role_ = keylane::meta::MetaNodeRole::kPrimary;
  return cmd;
}

MetaApplyResult ApplyOk(MetaStores& stores, std::uint64_t log_index,
                        const MetaCommand& cmd) {
  MetaApplyResult result =
      ApplyCommitted(stores, log_index, cmd, kActorPrincipal, kReadableTime);
  EXPECT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
  EXPECT_EQ(result.log_index_, log_index);
  return result;
}

MetaApplyResult ApplyRejected(MetaStores& stores, std::uint64_t log_index,
                              const MetaCommand& cmd) {
  MetaApplyResult result =
      ApplyCommitted(stores, log_index, cmd, kActorPrincipal, kReadableTime);
  EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_FALSE(result.detail_.empty());
  return result;
}

// Whole-state byte comparison: equal states serialize to equal bytes (the
// per-store codecs are deterministic), so this is the strongest cheap
// "state unchanged / states identical" probe.
std::string MustSerialize(const MetaStores& stores) {
  const auto bytes = stores.Serialize();
  EXPECT_TRUE(bytes.ok()) << bytes.status();
  return bytes.value_or("");
}

void ExpectAggregateSnapshotFailStop(const MetaStores& stores) {
  const auto bytes = stores.Serialize();
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  const auto restored = MetaStores::Deserialize(*bytes);
  ASSERT_FALSE(restored.ok());
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(restored.status()),
            keylane::meta::MetaFailureClass::kFailStop);
}

// The committed DOMAIN state (everything but the audit window): a rejected
// command must leave this unchanged, while the audit trail still grows by the
// rejection record; the index is consumed and state otherwise remains
// unchanged.
std::string DomainStateBytes(const MetaStores& stores) {
  std::string out = stores.identity_.Serialize();
  out += stores.topology_.Serialize();
  out += stores.policy_.Serialize();
  out += stores.grant_.Serialize().value_or("!");
  out += stores.operation_.Serialize().value_or("!");
  out += stores.population_manifest_.Serialize();
  return out;
}

TEST(MetaStateApply, RegisterNodeAcceptedAndAudited) {
  MetaStores stores;
  const keylane::meta::RegisterNode cmd = MakeRegisterFor(1);
  const MetaApplyResult result = ApplyOk(stores, 1, cmd);
  EXPECT_EQ(result.command_tag_, keylane::meta::MetaCommandTag::kRegisterNode);
  EXPECT_TRUE(stores.identity_.IsActiveNode(cmd.node_id_));

  // Every privileged command appends exactly one audit record keyed by its
  // raft log index; the injected actor fields are copied verbatim.
  ASSERT_EQ(stores.audit_.size(), 1u);
  const auto entry = stores.audit_.Find(1);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->record_.log_index_, 1u);
  EXPECT_EQ(entry->record_.actor_principal_, kActorPrincipal);
  EXPECT_EQ(entry->record_.readable_time_, kReadableTime);
  EXPECT_EQ(entry->record_.verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_TRUE(entry->record_.verdict_detail_.empty());
  EXPECT_NE(entry->record_.command_summary_.find(cmd.node_id_),
            std::string::npos);
}

TEST(MetaStateApply, ReplaySameIndexProducesSameVerdictStateAndAudit) {
  MetaStores stores;
  const keylane::meta::RegisterNode cmd = MakeRegisterFor(1);
  const MetaApplyResult first = ApplyOk(stores, 1, cmd);
  const std::string state_after_first = MustSerialize(stores);
  const auto record_after_first = stores.audit_.Find(1);
  ASSERT_TRUE(record_after_first.has_value());

  // Replay contract: same (index, command) -> same
  // verdict, same state, same audit record (the window does not grow).
  const MetaApplyResult second = ApplyOk(stores, 1, cmd);
  EXPECT_EQ(second, first);
  EXPECT_EQ(MustSerialize(stores), state_after_first);
  ASSERT_EQ(stores.audit_.size(), 1u);
  EXPECT_EQ(stores.audit_.Find(1)->record_, record_after_first->record_);
}

TEST(MetaStateApply, RejectedCommandIsAuditedAndLeavesStateUnchanged) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  // Same principal bound to a second node_id: domain rejection because the
  // binding is globally one-to-one.
  keylane::meta::RegisterNode conflict = MakeRegisterFor(2);
  conflict.principal_ = "keylane://node/" + MakeNodeId(1);
  const std::string domain_before = DomainStateBytes(stores);
  const MetaApplyResult result = ApplyRejected(stores, 2, conflict);
  EXPECT_EQ(result.command_tag_, keylane::meta::MetaCommandTag::kRegisterNode);

  // Domain state is unchanged; the audit window still grew by the rejection
  // record (the index is consumed).
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  ASSERT_EQ(stores.audit_.size(), 2u);
  const auto entry = stores.audit_.Find(2);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->record_.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(entry->record_.verdict_detail_, result.detail_);
  EXPECT_EQ(entry->record_.actor_principal_, kActorPrincipal);
  // The rejection consumes the log index: the chain advanced over both
  // records.
  EXPECT_TRUE(stores.audit_.VerifyChain());
}

TEST(MetaStateApply, UpdateAndRetireNodeThroughDispatcher) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  const std::string node_id = MakeNodeId(1);

  keylane::meta::UpdateNode update;
  update.request_id_ = MakeRequestId(0x21);
  update.node_id_ = node_id;
  update.expected_revision_ = 1;
  update.endpoints_ = {"10.0.0.9:7000"};
  update.capability_mask_ = 0x3;
  update.new_topology_epoch_ = 1;
  ApplyOk(stores, 2, update);
  EXPECT_EQ(stores.identity_.FindNode(node_id)->revision_, 2u);

  // Re-applying the identical command is a replay: idempotent accept.
  ApplyOk(stores, 2, update);

  // CAS conflict: a DIFFERENT update carrying the stale expected_revision is
  // a domain rejection.
  keylane::meta::UpdateNode stale = update;
  stale.request_id_ = MakeRequestId(0x23);
  stale.endpoints_ = {"10.0.0.10:7000"};
  ApplyRejected(stores, 3, stale);
  EXPECT_EQ(stores.identity_.FindNode(node_id)->revision_, 2u);

  keylane::meta::RetireNode retire;
  retire.request_id_ = MakeRequestId(0x22);
  retire.node_id_ = node_id;
  retire.expected_revision_ = 2;
  ApplyOk(stores, 4, retire);
  EXPECT_FALSE(stores.identity_.IsActiveNode(node_id));

  // Retired is terminal: re-registering the same node_id is rejected.
  ApplyRejected(stores, 5, MakeRegisterFor(1));
  ASSERT_EQ(stores.audit_.size(), 5u);
}

TEST(MetaStateApply, CreateGroupMirrorsIntoGrantStore) {
  MetaStores stores;
  keylane::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = "g1";
  cmd.new_topology_epoch_ = 1;
  ApplyOk(stores, 1, cmd);

  EXPECT_TRUE(stores.topology_.GroupExists("g1"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 1u);
  // The grant half of the group exists too (fenced, grantless): grant
  // commands operate on groups known to both stores.
  const auto grant_state = stores.grant_.GroupState("g1");
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_TRUE(grant_state->fenced_);
  EXPECT_FALSE(grant_state->grant_.has_value());
  EXPECT_EQ(grant_state->group_term_, 0u);

  // Replay: idempotent accept, both halves unchanged, no new audit record.
  ApplyOk(stores, 1, cmd);
  EXPECT_EQ(stores.topology_.GroupCount(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);
}

TEST(MetaStateApply, CreateGroupEpochMustBeExactlyNext) {
  MetaStores stores;
  keylane::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = "g1";
  cmd.new_topology_epoch_ = 7;  // not current(0)+1
  ApplyRejected(stores, 1, cmd);
  EXPECT_FALSE(stores.topology_.GroupExists("g1"));
  EXPECT_FALSE(stores.grant_.GroupState("g1").has_value());
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 0u);
}

TEST(MetaStateApply, MetaStoresSnapshotRoundTrip) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  keylane::meta::CreateGroup group;
  group.request_id_ = MakeRequestId(0x32);
  group.group_id_ = "g1";
  group.new_topology_epoch_ = 1;
  ApplyOk(stores, 2, group);

  const std::string bytes = MustSerialize(stores);
  const auto restored = MetaStores::Deserialize(bytes);
  ASSERT_TRUE(restored.ok()) << restored.status();
  // Equal states serialize to equal bytes.
  EXPECT_EQ(MustSerialize(*restored), bytes);
  EXPECT_TRUE(restored->identity_.IsActiveNode(MakeNodeId(1)));
  EXPECT_TRUE(restored->topology_.GroupExists("g1"));
  EXPECT_TRUE(restored->grant_.GroupState("g1").has_value());
  EXPECT_EQ(restored->audit_.size(), 2u);
  EXPECT_TRUE(restored->audit_.VerifyChain());
  const auto* envelope = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(envelope[0] | (envelope[1] << 8)),
            keylane::meta::kMetaFormatVersion);
}

TEST(MetaStateApply, MetaStoresDeserializeRejectsCorruption) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  const std::string bytes = MustSerialize(stores);

  for (std::size_t len : {std::size_t{0}, std::size_t{1}, bytes.size() - 1}) {
    const auto decoded =
        MetaStores::Deserialize(std::string_view(bytes).substr(0, len));
    ASSERT_FALSE(decoded.ok()) << "len=" << len;
    EXPECT_EQ(keylane::meta::MetaFailureClassOf(decoded.status()),
              keylane::meta::MetaFailureClass::kFailStop);
  }
  const auto trailing = MetaStores::Deserialize(bytes + '\0');
  ASSERT_FALSE(trailing.ok());
  EXPECT_EQ(keylane::meta::MetaFailureClassOf(trailing.status()),
            keylane::meta::MetaFailureClass::kFailStop);
}

TEST(MetaStateApply, LogIndexZeroRejectedWithoutDispatchOrAudit) {
  MetaStores stores;
  // Raft log indexes start at 1; 0 means the caller lost the index
  // correspondence. Rejected deterministically, nothing dispatched, and no
  // audit write attempted (the audit store would fail-stop on index 0).
  const MetaApplyResult result = ApplyRejected(stores, 0, MakeRegisterFor(1));
  EXPECT_EQ(stores.identity_.NodeCount(), 0u);
  EXPECT_EQ(stores.audit_.size(), 0u);
  EXPECT_EQ(result.log_index_, 0u);
}

TEST(MetaStateApply, OverCapActorContextRejectedBeforeDispatch) {
  MetaStores stores;
  // The trusted entry guarantees bounded actor fields; an over-cap field is
  // an entry-contract violation. The command is rejected before dispatch so
  // committed state stays unchanged and identical on every node.
  const keylane::meta::RegisterNode cmd = MakeRegisterFor(1);
  const std::string huge_principal(keylane::meta::kMaxMetaPrincipalBytes + 1,
                                   'p');
  const MetaApplyResult result =
      ApplyCommitted(stores, 1, cmd, huge_principal, kReadableTime);
  EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(stores.identity_.NodeCount(), 0u);
  EXPECT_EQ(stores.audit_.size(), 0u);
}

// ---------------------------------------------------------------------------
// Cross-store fixtures: registers/creates/assigns through ApplyCommitted, so
// every fixture step is itself exercised through the dispatcher.
// ---------------------------------------------------------------------------

keylane::meta::PutPolicy MakePutPolicy(const std::string& policy_id,
                                       std::uint64_t version,
                                       const std::string& content) {
  keylane::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(static_cast<std::uint8_t>(0x50 + version));
  cmd.policy_id_ = policy_id;
  cmd.version_ = version;
  cmd.content_ = content;
  cmd.content_hash_ = keylane::meta::MetaPolicyStore::ContentHash(content);
  return cmd;
}

keylane::meta::CreateGroup MakeCreateGroup(const std::string& group_id,
                                           std::uint64_t topology_epoch) {
  keylane::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = group_id;
  cmd.new_topology_epoch_ = topology_epoch;
  return cmd;
}

keylane::meta::AssignNodeToGroup MakeAssign(const std::string& group_id,
                                            std::uint32_t node,
                                            std::uint64_t expected_revision,
                                            std::uint64_t topology_epoch = 0) {
  keylane::meta::AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = group_id;
  cmd.node_id_ = MakeNodeId(node);
  cmd.assignment_id_.fill(static_cast<std::uint8_t>(node));
  cmd.role_ = keylane::meta::MetaNodeRole::kPrimary;
  cmd.expected_revision_ = expected_revision;
  cmd.new_topology_epoch_ =
      topology_epoch == 0 ? expected_revision + 1 : topology_epoch;
  return cmd;
}

// A fully valid ActivateAuthority against the state built by the fixture
// helpers: group with term 1 begun, policy committed, owner a member.
keylane::meta::ActivateAuthority MakeActivate(
    const std::string& group_id, std::uint64_t expected_term,
    std::uint32_t owner_node, std::uint64_t authority_version,
    std::uint64_t topology_epoch, std::uint64_t config_epoch,
    const std::string& policy_id = "p", std::uint64_t policy_version = 1) {
  keylane::meta::ActivateAuthority cmd;
  cmd.request_id_ = MakeRequestId(0x42);
  cmd.group_id_ = group_id;
  cmd.expected_term_ = expected_term;
  cmd.new_owner_ = MakeNodeId(owner_node);
  cmd.grant_.lease_duration_ms_ = 5000;
  cmd.grant_.policy_id_ = policy_id;
  cmd.grant_.policy_version_ = policy_version;
  cmd.new_authority_version_ = authority_version;
  cmd.new_topology_epoch_ = topology_epoch;
  cmd.new_config_epoch_ = config_epoch;
  return cmd;
}

// Registers node, creates group (topology_epoch 1), assigns the node
// (membership revision 1 -> 2, topology_epoch 2), commits policy p@1, and
// begins group term 1.
// Consumes log indexes 1..5.
void SetupActivatedGroupPrerequisites(MetaStores& stores, std::uint32_t node,
                                      const std::string& group_id) {
  ApplyOk(stores, 1, MakeRegisterFor(node));
  ApplyOk(stores, 2, MakeCreateGroup(group_id, 1));
  ApplyOk(stores, 3, MakeAssign(group_id, node, 1));
  ApplyOk(stores, 4, MakePutPolicy("p", 1, "{\"lease_ms\":5000}"));
  keylane::meta::BeginGroupTerm begin;
  begin.request_id_ = MakeRequestId(0x40);
  begin.group_id_ = group_id;
  begin.expected_term_ = 0;
  begin.new_term_ = 1;
  ApplyOk(stores, 5, begin);
}

TEST(MetaStateApply, AssignNodeToGroupRequiresRegisteredActiveNode) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  ApplyOk(stores, 2, MakeRegisterFor(2));
  ApplyOk(stores, 3, MakeCreateGroup("g1", 1));

  // Unregistered node: the identity fact is cross-store for the topology
  // store (its header delegates), so the apply layer rejects.
  ApplyRejected(stores, 4, MakeAssign("g1", 9, 1));
  EXPECT_FALSE(stores.topology_.FindGroupOfNode(MakeNodeId(9)).has_value());

  // Retired node (terminal): rejected as well. Retire is legal here because
  // the node holds no membership.
  keylane::meta::RetireNode retire;
  retire.request_id_ = MakeRequestId(0x22);
  retire.node_id_ = MakeNodeId(2);
  retire.expected_revision_ = 1;
  ApplyOk(stores, 5, retire);
  ApplyRejected(stores, 6, MakeAssign("g1", 2, 1));

  // Registered active node: accepted.
  ApplyOk(stores, 7, MakeAssign("g1", 1, 1));
  EXPECT_EQ(stores.topology_.FindGroupOfNode(MakeNodeId(1)),
            std::optional<std::string>("g1"));
}

TEST(MetaStateApply, AssignNodeToOtherGroupRejectedByOneNodeOneGroup) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  ApplyOk(stores, 2, MakeCreateGroup("g1", 1));
  ApplyOk(stores, 3, MakeCreateGroup("g2", 2));
  ApplyOk(stores, 4, MakeAssign("g1", 1, 1, 3));

  // Moving to a different group without an explicit RemoveNodeFromGroup
  // first: rejected; membership unchanged.
  const std::string domain_before = DomainStateBytes(stores);
  ApplyRejected(stores, 5, MakeAssign("g2", 1, 1));
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  EXPECT_EQ(stores.topology_.FindGroupOfNode(MakeNodeId(1)),
            std::optional<std::string>("g1"));

  // Same-group replay with the same role and the produced revision is the
  // idempotent-accept path, even though the membership fact now exists.
  ApplyOk(stores, 4, MakeAssign("g1", 1, 1, 3));
}

TEST(MetaStateApply, RetireNodeWithMembershipRejected) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");

  keylane::meta::RetireNode retire;
  retire.request_id_ = MakeRequestId(0x22);
  retire.node_id_ = MakeNodeId(1);
  retire.expected_revision_ = 1;
  // The node still holds group membership: retire is a cross-store rejection.
  ApplyRejected(stores, 6, retire);
  EXPECT_TRUE(stores.identity_.IsActiveNode(MakeNodeId(1)));

  // Remove the membership first, then retire succeeds.
  keylane::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = MakeRequestId(0x32);
  remove.group_id_ = "g1";
  remove.node_id_ = MakeNodeId(1);
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 3;
  ApplyOk(stores, 7, remove);
  ApplyOk(stores, 8, retire);
  EXPECT_FALSE(stores.identity_.IsActiveNode(MakeNodeId(1)));
}

TEST(MetaStateApply, RemoveNodeFromGroupOfGrantOwnerRejected) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});

  keylane::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = MakeRequestId(0x32);
  remove.group_id_ = "g1";
  remove.node_id_ = MakeNodeId(1);
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;

  // The node owns the group's active grant: removing it would strand the
  // authority fact. Reject; revoke the grant first.
  ApplyRejected(stores, 7, remove);
  EXPECT_TRUE(stores.topology_.FindGroup("g1")->members_.size() == 1u);

  keylane::meta::RevokeGrant revoke;
  revoke.request_id_ = MakeRequestId(0x43);
  revoke.group_id_ = "g1";
  revoke.expected_term_ = 1;
  ApplyOk(stores, 8, revoke);
  ApplyOk(stores, 9, remove);
  EXPECT_TRUE(stores.topology_.FindGroup("g1")->members_.empty());
}

TEST(MetaStateApply, BeginGroupTermRaisesTermInBothStores) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});

  keylane::meta::BeginGroupTerm begin;
  begin.request_id_ = MakeRequestId(0x41);
  begin.group_id_ = "g1";
  begin.expected_term_ = 1;
  begin.new_term_ = 2;
  ApplyOk(stores, 7, begin);

  // Grant half: term 2, fenced, grantless. Topology half: the committed
  // GroupRecord carries the same term.
  const auto grant_state = stores.grant_.GroupState("g1");
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_EQ(grant_state->group_term_, 2u);
  EXPECT_TRUE(grant_state->fenced_);
  EXPECT_FALSE(grant_state->grant_.has_value());
  EXPECT_EQ(stores.topology_.FindGroup("g1")->record_.group_term_, 2u);

  // Replay at the same index: idempotent accept, no state movement, audit
  // window unchanged.
  const std::string domain_before = DomainStateBytes(stores);
  ApplyOk(stores, 7, begin);
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  EXPECT_EQ(stores.audit_.size(), 7u);

  // A stale-term activation is now rejected on both paths: the term moved on.
  ApplyRejected(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 2, 4, 2)});
}

TEST(MetaStateApply, GrantAuthorityRenewsLeaseWithCommittedPolicy) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});

  keylane::meta::GrantAuthority renew;
  renew.request_id_ = MakeRequestId(0x44);
  renew.group_id_ = "g1";
  renew.node_id_ = MakeNodeId(1);
  renew.term_ = 1;
  renew.authority_version_ = 1;
  renew.grant_.lease_duration_ms_ = 9000;
  renew.grant_.policy_id_ = "p";
  renew.grant_.policy_version_ = 1;
  ApplyOk(stores, 7, renew);
  EXPECT_EQ(stores.grant_.GroupState("g1")->grant_->spec_.lease_duration_ms_,
            9000u);

  // Renewal referencing an uncommitted policy version is rejected.
  renew.grant_.policy_version_ = 99;
  ApplyRejected(stores, 8, renew);
  renew.grant_.policy_version_ = 1;

  // Renewal naming an unregistered node: rejected (principal-vs-grant, the
  // node must be registered and active).
  renew.node_id_ = MakeNodeId(9);
  ApplyRejected(stores, 9, renew);

  // Replay of the accepted renewal at its own index: idempotent accept.
  renew.node_id_ = MakeNodeId(1);
  const std::string domain_before = DomainStateBytes(stores);
  ApplyOk(stores, 7, renew);
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
}

TEST(MetaStateApply, RetirePolicyRejectedWhileReferencedByActiveGrant) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});

  keylane::meta::RetirePolicy retire;
  retire.request_id_ = MakeRequestId(0x51);
  retire.policy_id_ = "p";
  retire.version_ = 1;

  // A version referenced by an active grant cannot retire. Non-terminal
  // operation references are covered separately through their structured
  // dependency list.
  ApplyRejected(stores, 7, retire);
  EXPECT_TRUE(stores.policy_.IsVersionActive("p", 1));

  keylane::meta::RevokeGrant revoke;
  revoke.request_id_ = MakeRequestId(0x43);
  revoke.group_id_ = "g1";
  revoke.expected_term_ = 1;
  ApplyOk(stores, 8, revoke);

  ApplyOk(stores, 9, retire);
  EXPECT_FALSE(stores.policy_.IsVersionActive("p", 1));
  EXPECT_TRUE(stores.policy_.IsVersionPresent("p", 1));  // content-retaining

  // Replay of the retire at its own index: idempotent accept, no new audit.
  ApplyOk(stores, 9, retire);
  EXPECT_EQ(stores.audit_.size(), 9u);
}

TEST(MetaStateApply, RetirePolicyRejectedWhileLiveOperationReferencesIt) {
  MetaStores stores;
  ApplyOk(stores, 1, MakePutPolicy("p", 1, "policy"));
  keylane::meta::SubmitOperation submit;
  submit.request_id_ = MakeRequestId(0x71);
  submit.operation_id_.fill(0x41);
  submit.kind_ = "migration";
  submit.intent_ = "policy-bound";
  submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
  submit.policy_references_ = {{"p", 1}};
  ApplyOk(stores, 2, submit);

  keylane::meta::RetirePolicy retire;
  retire.request_id_ = MakeRequestId(0x72);
  retire.policy_id_ = "p";
  retire.version_ = 1;
  ApplyRejected(stores, 3, retire);

  keylane::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x73);
  complete.operation_id_ = submit.operation_id_;
  complete.result_ = "done";
  ApplyOk(stores, 4, complete);
  ApplyOk(stores, 5, retire);
  EXPECT_FALSE(stores.policy_.IsVersionActive("p", 1));

  // The original submit remains an accepted replay after its dependency can
  // legally retire; the duplicate cannot mutate the terminal record.
  ApplyOk(stores, 2, submit);
  EXPECT_EQ(stores.audit_.size(), 5u);
}

TEST(MetaStateApply, TransitionEvidenceMustMatchCommittedAnchors) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  keylane::meta::SubmitOperation submit;
  submit.request_id_ = MakeRequestId(0x74);
  submit.operation_id_.fill(0x42);
  submit.kind_ = "migration";
  submit.intent_ = "history-bound";
  submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
  submit.replication_history_id_.fill(55);
  ApplyOk(stores, 6, submit);

  keylane::meta::TransitionOperationPhase transition;
  transition.request_id_ = MakeRequestId(0x75);
  transition.operation_id_ = submit.operation_id_;
  transition.kind_phase_blob_ = "prepare";
  keylane::meta::MetaEvidenceSummary evidence;
  evidence.node_id_ = MakeNodeId(1);
  evidence.group_id_ = "g1";
  evidence.assignment_id_.fill(1);
  evidence.boot_incarnation_.fill(1);
  evidence.group_term_ = 1;
  evidence.population_manifest_revision_ = 0;
  evidence.replication_history_id_.fill(55);
  evidence.operation_id_ = submit.operation_id_;
  evidence.kind_hash_ = keylane::meta::MetaSha256("proof");
  transition.evidence_ = {evidence};
  ApplyOk(stores, 7, transition);

  keylane::meta::TransitionOperationPhase stale = transition;
  stale.request_id_ = MakeRequestId(0x76);
  stale.expected_revision_ = 1;
  stale.kind_phase_blob_ = "commit";
  stale.evidence_[0].group_term_ = 2;
  ApplyRejected(stores, 8, stale);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            1u);

  keylane::meta::PutPopulationManifest put_manifest;
  put_manifest.request_id_ = MakeRequestId(0x77);
  put_manifest.entries_ = {{1, 11}};
  put_manifest.manifest_digest_ =
      keylane::meta::MetaPopulationManifestStore::CanonicalDigest(
          put_manifest.entries_);
  ApplyOk(stores, 9, put_manifest);
  keylane::meta::SetGroupReplicationState manifest;
  manifest.request_id_ = MakeRequestId(0x77);
  manifest.group_id_ = "g1";
  manifest.new_population_manifest_revision_ = 1;
  manifest.new_population_manifest_digest_ = put_manifest.manifest_digest_;
  manifest.new_partition_replication_epoch_ = 1;
  manifest.new_topology_epoch_ = 3;
  ApplyOk(stores, 10, manifest);

  keylane::meta::TransitionOperationPhase wrong_partition_epoch = transition;
  wrong_partition_epoch.request_id_ = MakeRequestId(0x7a);
  wrong_partition_epoch.expected_revision_ = 1;
  wrong_partition_epoch.kind_phase_blob_ = "commit";
  wrong_partition_epoch.evidence_[0].population_manifest_revision_ = 1;
  wrong_partition_epoch.evidence_[0].population_manifest_digest_ =
      put_manifest.manifest_digest_;
  ApplyRejected(stores, 11, wrong_partition_epoch);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            1u);

  keylane::meta::TransitionOperationPhase wrong_manifest = transition;
  wrong_manifest.request_id_ = MakeRequestId(0x79);
  wrong_manifest.expected_revision_ = 1;
  wrong_manifest.kind_phase_blob_ = "commit";
  wrong_manifest.evidence_[0].population_manifest_revision_ = 1;
  wrong_manifest.evidence_[0].population_manifest_digest_.fill(0x7f);
  ApplyRejected(stores, 12, wrong_manifest);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            1u);

  keylane::meta::BeginGroupTerm next_term;
  next_term.request_id_ = MakeRequestId(0x78);
  next_term.group_id_ = "g1";
  next_term.expected_term_ = 1;
  next_term.new_term_ = 2;
  ApplyOk(stores, 13, next_term);

  // Evidence is checked against committed anchors on first application, but
  // an exact replay stays accepted after those anchors legitimately advance.
  ApplyOk(stores, 7, transition);
  EXPECT_EQ(stores.audit_.size(), 13u);
}

TEST(MetaStateApply,
     TransitionEvidenceRejectsProposalToApplyMembershipIncarnationRaces) {
  const auto make_submit = [] {
    keylane::meta::SubmitOperation submit;
    submit.operation_id_.fill(0x43);
    submit.kind_ = "migration";
    submit.intent_ = "membership-bound";
    submit.intent_hash_ = keylane::meta::MetaSha256(submit.intent_);
    submit.replication_history_id_.fill(55);
    return submit;
  };
  const auto make_transition =
      [](const keylane::meta::MetaOperationId& operation_id) {
        keylane::meta::TransitionOperationPhase transition;
        transition.operation_id_ = operation_id;
        transition.kind_phase_blob_ = "prepare";
        keylane::meta::MetaEvidenceSummary evidence;
        evidence.node_id_ = MakeNodeId(1);
        evidence.group_id_ = "g1";
        evidence.assignment_id_.fill(1);
        evidence.boot_incarnation_.fill(2);
        evidence.group_term_ = 1;
        evidence.replication_history_id_.fill(55);
        evidence.operation_id_ = operation_id;
        evidence.kind_hash_ = keylane::meta::MetaSha256("proof");
        transition.evidence_ = {evidence};
        return transition;
      };

  {
    MetaStores stores;
    SetupActivatedGroupPrerequisites(stores, 1, "g1");
    const keylane::meta::SubmitOperation submit = make_submit();
    ApplyOk(stores, 6, submit);
    // Model a leader that validated and baked evidence before the membership
    // commands below committed, but whose transition reaches apply afterward.
    const keylane::meta::TransitionOperationPhase proposed =
        make_transition(submit.operation_id_);

    keylane::meta::RemoveNodeFromGroup remove;
    remove.group_id_ = "g1";
    remove.node_id_ = MakeNodeId(1);
    remove.expected_revision_ = 2;
    remove.new_topology_epoch_ = 3;
    ApplyOk(stores, 7, remove);
    keylane::meta::AssignNodeToGroup readd = MakeAssign("g1", 1, 3, 4);
    readd.assignment_id_.fill(0x21);
    ApplyOk(stores, 8, readd);

    ApplyRejected(stores, 9, proposed);
    EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
              0u);
  }

  {
    MetaStores stores;
    SetupActivatedGroupPrerequisites(stores, 1, "g1");
    ApplyOk(stores, 6, MakeCreateGroup("g2", 3));
    keylane::meta::BeginGroupTerm begin_g2;
    begin_g2.group_id_ = "g2";
    begin_g2.new_term_ = 1;
    ApplyOk(stores, 7, begin_g2);
    const keylane::meta::SubmitOperation submit = make_submit();
    ApplyOk(stores, 8, submit);
    const keylane::meta::TransitionOperationPhase proposed =
        make_transition(submit.operation_id_);

    keylane::meta::RemoveNodeFromGroup remove;
    remove.group_id_ = "g1";
    remove.node_id_ = MakeNodeId(1);
    remove.expected_revision_ = 2;
    remove.new_topology_epoch_ = 4;
    ApplyOk(stores, 9, remove);
    keylane::meta::AssignNodeToGroup move = MakeAssign("g2", 1, 1, 5);
    move.assignment_id_.fill(0x31);
    ApplyOk(stores, 10, move);

    ApplyRejected(stores, 11, proposed);
    EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
              0u);
  }
}

TEST(MetaStateApply, GroupReplicationStateAdvancesWithTopologyEpoch) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
  keylane::meta::PutPopulationManifest put;
  put.entries_ = {{1, 1}};
  put.manifest_digest_ =
      keylane::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
  ApplyOk(stores, 2, put);
  keylane::meta::SetGroupReplicationState update;
  update.request_id_ = MakeRequestId(0x79);
  update.group_id_ = "g1";
  update.new_population_manifest_revision_ = 1;
  update.new_population_manifest_digest_ = put.manifest_digest_;
  update.new_partition_replication_epoch_ = 1;
  update.new_topology_epoch_ = 2;
  ApplyOk(stores, 3, update);
  const auto group = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.population_manifest_revision_, 1u);
  EXPECT_EQ(group->record_.partition_replication_epoch_, 1u);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 2u);

  keylane::meta::SetGroupReplicationState stale = update;
  stale.request_id_ = MakeRequestId(0x7a);
  stale.new_population_manifest_revision_ = 2;
  stale.new_topology_epoch_ = 3;
  ApplyRejected(stores, 4, stale);
}

TEST(MetaStateApply, ManifestRevisionDistinguishesAtoBtoAAndGuardsPrune) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeCreateGroup("g1", 1));

  keylane::meta::PutPopulationManifest a;
  a.entries_ = {{1, 10}};
  a.manifest_digest_ =
      keylane::meta::MetaPopulationManifestStore::CanonicalDigest(a.entries_);
  keylane::meta::PutPopulationManifest b;
  b.entries_ = {{1, 11}};
  b.manifest_digest_ =
      keylane::meta::MetaPopulationManifestStore::CanonicalDigest(b.entries_);
  ApplyOk(stores, 2, a);
  ApplyOk(stores, 3, b);

  keylane::meta::SetGroupReplicationState set;
  set.group_id_ = "g1";
  set.new_population_manifest_revision_ = 1;
  set.new_population_manifest_digest_ = a.manifest_digest_;
  set.new_topology_epoch_ = 2;
  ApplyOk(stores, 4, set);
  set.expected_population_manifest_revision_ = 1;
  set.expected_population_manifest_digest_ = a.manifest_digest_;
  set.new_population_manifest_revision_ = 2;
  set.new_population_manifest_digest_ = b.manifest_digest_;
  set.new_topology_epoch_ = 3;
  ApplyOk(stores, 5, set);
  set.expected_population_manifest_revision_ = 2;
  set.expected_population_manifest_digest_ = b.manifest_digest_;
  set.new_population_manifest_revision_ = 3;
  set.new_population_manifest_digest_ = a.manifest_digest_;
  set.new_topology_epoch_ = 4;
  ApplyOk(stores, 6, set);

  const auto group = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(group.has_value());
  EXPECT_EQ(group->record_.population_manifest_revision_, 3u);
  EXPECT_EQ(group->record_.population_manifest_digest_, a.manifest_digest_);

  keylane::meta::PrunePopulationManifest prune;
  prune.manifest_digest_ = a.manifest_digest_;
  ApplyRejected(stores, 7, prune);
  prune.manifest_digest_ = b.manifest_digest_;
  ApplyOk(stores, 8, prune);
  EXPECT_FALSE(stores.population_manifest_.Contains(b.manifest_digest_));
}

TEST(MetaStateApply, SetSlotMapThroughDispatcher) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeCreateGroup("g2", 3));

  keylane::meta::SetSlotMap slots;
  slots.request_id_ = MakeRequestId(0x33);
  slots.ranges_ = {{0, 9999, "g1"}, {10000, 16383, "g2"}};
  slots.new_topology_epoch_ = 4;
  slots.config_epochs_ = {{"g1", 10}, {"g2", 20}};
  ApplyOk(stores, 7, slots);
  EXPECT_EQ(stores.topology_.SlotOwner(0), std::optional<std::string>("g1"));
  EXPECT_EQ(stores.topology_.SlotOwner(16383),
            std::optional<std::string>("g2"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 4u);

  // Replay: idempotent accept (same content, epoch already carried).
  ApplyOk(stores, 7, slots);
  EXPECT_EQ(stores.audit_.size(), 7u);

  // A range referencing an unknown group is rejected; the map is absolute,
  // so the whole command is atomic.
  keylane::meta::SetSlotMap bad = slots;
  bad.request_id_ = MakeRequestId(0x34);
  bad.new_topology_epoch_ = 5;
  bad.ranges_ = {{0, 1, "g-unknown"}};
  ApplyRejected(stores, 8, bad);
  EXPECT_EQ(stores.topology_.SlotOwner(0), std::optional<std::string>("g1"));
}

TEST(MetaStateApply, SetSlotMapCannotClearAnActiveGrantConfigEpoch) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 7)});

  keylane::meta::SetSlotMap clear_config;
  clear_config.request_id_ = MakeRequestId(0x83);
  clear_config.new_topology_epoch_ = 4;
  clear_config.config_epochs_ = {{"g1", 0}};
  ApplyRejected(stores, 7, MetaCommand{clear_config});
  EXPECT_EQ(stores.topology_.FindGroup("g1")->config_epoch_, 7u);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
}

// ---------------------------------------------------------------------------
// ActivateAuthority: the atomic failover/migration commit point.
// ---------------------------------------------------------------------------

// Asserts the pre-activation state of both halves: grant store fenced and
// grantless at term 1, topology record untouched, topology_epoch 1.
void ExpectPreActivationState(const MetaStores& stores,
                              const std::string& group_id) {
  const auto grant_state = stores.grant_.GroupState(group_id);
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_EQ(grant_state->group_term_, 1u);
  EXPECT_TRUE(grant_state->fenced_);
  EXPECT_FALSE(grant_state->grant_.has_value());
  EXPECT_EQ(grant_state->last_authority_version_, 0u);
  const auto view = stores.topology_.FindGroup(group_id);
  ASSERT_TRUE(view.has_value());
  EXPECT_TRUE(view->record_.owner_.empty());
  EXPECT_EQ(view->record_.authority_version_, 0u);
  EXPECT_EQ(view->config_epoch_, 0u);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 2u);
}

TEST(MetaStateApply, ActivateAuthorityRejectionLeavesBothHalvesUntouched) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));  // registered but not a member

  std::uint64_t index = 7;
  const auto expect_rejected_untouched =
      [&](keylane::meta::ActivateAuthority cmd) {
        ApplyRejected(stores, index, MetaCommand{std::move(cmd)});
        ExpectPreActivationState(stores, "g1");
        ++index;
      };

  // Wrong expected_term (grant store CAS).
  expect_rejected_untouched(MakeActivate("g1", 0, 1, 1, 3, 1));
  // topology_epoch not exactly current+1.
  expect_rejected_untouched(MakeActivate("g1", 1, 1, 1, 5, 1));
  // New owner not a member of the group.
  expect_rejected_untouched(MakeActivate("g1", 1, 2, 1, 3, 1));
  // New owner not registered at all.
  {
    keylane::meta::ActivateAuthority cmd = MakeActivate("g1", 1, 1, 1, 3, 1);
    cmd.new_owner_ = MakeNodeId(99);
    expect_rejected_untouched(std::move(cmd));
  }
  // Grant policy version not committed.
  expect_rejected_untouched(MakeActivate("g1", 1, 1, 1, 3, 1, "p", 99));
  // authority_version not strictly increasing.
  expect_rejected_untouched(MakeActivate("g1", 1, 1, 0, 3, 1));
  // A serving owner must have a nonzero configuration epoch on the wire.
  expect_rejected_untouched(MakeActivate("g1", 1, 1, 1, 3, 0));
}

TEST(MetaStateApply, ActivateAuthorityRequiresABegunNonzeroTerm) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  ApplyOk(stores, 2, MakeCreateGroup("g1", 1));
  ApplyOk(stores, 3, MakeAssign("g1", 1, 1));
  ApplyOk(stores, 4, MakePutPolicy("p", 1, "lease-policy"));

  ApplyRejected(stores, 5, MetaCommand{MakeActivate("g1", 0, 1, 1, 3, 1)});
  const auto grant = stores.grant_.GroupState("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_EQ(grant->group_term_, 0u);
  EXPECT_TRUE(grant->fenced_);
  EXPECT_FALSE(grant->grant_.has_value());
  const auto topology = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(topology.has_value());
  EXPECT_TRUE(topology->record_.owner_.empty());
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 2u);
}

TEST(MetaStateApply, ActivateAuthorityAcceptedWritesBothHalvesAtomically) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");

  const MetaApplyResult result =
      ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 7)});
  EXPECT_EQ(result.command_tag_,
            keylane::meta::MetaCommandTag::kActivateAuthority);

  // Grant half: grant installed under the CURRENT term (activate never moves
  // the term), unfenced, last_authority_version advanced.
  const auto grant_state = stores.grant_.GroupState("g1");
  ASSERT_TRUE(grant_state.has_value());
  ASSERT_TRUE(grant_state->grant_.has_value());
  EXPECT_FALSE(grant_state->fenced_);
  EXPECT_EQ(grant_state->group_term_, 1u);
  EXPECT_EQ(grant_state->grant_->owner_, MakeNodeId(1));
  EXPECT_EQ(grant_state->grant_->term_, 1u);
  EXPECT_EQ(grant_state->grant_->authority_version_, 1u);
  EXPECT_EQ(grant_state->last_authority_version_, 1u);

  // Topology half: owner, authority_version, config_epoch, topology_epoch.
  const auto view = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->record_.owner_, MakeNodeId(1));
  EXPECT_EQ(view->record_.authority_version_, 1u);
  EXPECT_EQ(view->config_epoch_, 7u);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
}

TEST(MetaStateApply, ActivateAuthorityReplaySameIndexIsIdempotent) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  const keylane::meta::ActivateAuthority cmd =
      MakeActivate("g1", 1, 1, 1, 3, 7);
  const MetaApplyResult first = ApplyOk(stores, 6, MetaCommand{cmd});
  const std::string state_after_first = MustSerialize(stores);

  // Re-committing the same index must not bump the epoch
  // a second time or change any verdict/state/audit.
  const MetaApplyResult second = ApplyOk(stores, 6, MetaCommand{cmd});
  EXPECT_EQ(second, first);
  EXPECT_EQ(MustSerialize(stores), state_after_first);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
  EXPECT_EQ(stores.audit_.size(), 6u);

  // The identical command at a NEW index is also the idempotent path (the
  // post-effect is already present), not a fresh activation.
  ApplyOk(stores, 7, MetaCommand{cmd});
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
  EXPECT_EQ(stores.grant_.GroupState("g1")->grant_->authority_version_, 1u);

  // A genuinely different activation reusing the already-consumed epoch is
  // rejected: it is not a replay (content differs) and the epoch rule fails.
  keylane::meta::ActivateAuthority different = cmd;
  different.request_id_ = MakeRequestId(0x45);
  different.new_config_epoch_ = 8;
  ApplyRejected(stores, 8, MetaCommand{different});
  EXPECT_EQ(stores.topology_.FindGroup("g1")->config_epoch_, 7u);
}

// ---------------------------------------------------------------------------
// operation journal + upgrade. operation_seq is the raft log index
// of the SubmitOperation; the journal persists the injected ActorContext.
// ---------------------------------------------------------------------------

keylane::meta::SubmitOperation MakeSubmit(std::uint8_t seed,
                                          std::uint8_t intent_seed) {
  keylane::meta::SubmitOperation cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.operation_id_ = MakeOperationId(seed);
  cmd.kind_ = "migration";
  cmd.intent_ = absl::StrCat("{\"slot\":", static_cast<int>(intent_seed), "}");
  cmd.intent_hash_ = MakeHash(intent_seed);
  return cmd;
}

TEST(MetaStateApply, MetaStoresDeserializeRejectsGroupStoreDrift) {
  {
    MetaStores stores;
    ASSERT_TRUE(stores.topology_.Apply(MakeCreateGroup("g1", 1)).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    ASSERT_TRUE(stores.grant_.AddGroup("g1").ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
}

TEST(MetaStateApply,
     MetaStoresDeserializeRejectsInactiveMembershipAndAnchorDrift) {
  {
    MetaStores stores;
    ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
    ASSERT_TRUE(stores.topology_.Apply(MakeAssign("g1", 9, 1)).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
    ASSERT_TRUE(stores.topology_.SetGroupTerm("g1", 1).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
}

TEST(MetaStateApply, MetaStoresDeserializeRejectsDanglingActiveReferences) {
  {
    MetaStores stores;
    SetupActivatedGroupPrerequisites(stores, 1, "g1");
    ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});
    keylane::meta::RetirePolicy retire;
    retire.policy_id_ = "p";
    retire.version_ = 1;
    ASSERT_TRUE(stores.policy_.Apply(retire).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
    ASSERT_TRUE(
        stores.topology_.SetPopulationManifest("g1", 1, MakeHash(0x91)).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    ApplyOk(stores, 1, MakePutPolicy("p", 1, "operation-policy"));
    keylane::meta::SubmitOperation submit = MakeSubmit(0x91, 0x92);
    submit.policy_references_ = {{"p", 1}};
    ApplyOk(stores, 2, MetaCommand{submit});
    keylane::meta::RetirePolicy retire;
    retire.policy_id_ = "p";
    retire.version_ = 1;
    ASSERT_TRUE(stores.policy_.Apply(retire).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
}

TEST(MetaStateApply,
     MetaStoresDeserializeRejectsUnprojectableActiveGrantAnchors) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});
  ASSERT_TRUE(stores.topology_.SetGroupConfigEpoch("g1", 0).ok());
  ExpectAggregateSnapshotFailStop(stores);
}

TEST(MetaStateApply, SubmitOperationSeqIsLogIndexAndActorPersisted) {
  MetaStores stores;
  const keylane::meta::SubmitOperation cmd = MakeSubmit(0x60, 0x11);
  ApplyOk(stores, 3, MetaCommand{cmd});

  const auto record = stores.operation_.FindOperation(cmd.operation_id_);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->operation_seq_, 3u);  // the raft log index of the submit
  EXPECT_EQ(record->lifecycle_,
            keylane::meta::MetaOperationLifecycle::kSubmitted);
  // The journal persists the trusted-entry-injected ActorContext, carried by
  // the raft-log encoding and only copied by apply.
  EXPECT_EQ(record->actor_.principal_, kActorPrincipal);
  EXPECT_EQ(record->actor_.readable_time_, kReadableTime);

  // Permanent idempotency: same id + same intent_hash -> idempotent accept,
  // no second record, no audit growth on the same index.
  ApplyOk(stores, 3, MetaCommand{cmd});
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);

  // Same id + different intent_hash: payload reuse, rejected.
  ApplyRejected(stores, 4, MetaCommand{MakeSubmit(0x60, 0x12)});
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);
}

TEST(MetaStateApply, OperationLifecycleAndArchiveThroughDispatcher) {
  MetaStores stores;
  const keylane::meta::SubmitOperation submit = MakeSubmit(0x61, 0x21);
  ApplyOk(stores, 1, MetaCommand{submit});
  const auto op_id = submit.operation_id_;

  keylane::meta::TransitionOperationPhase transition;
  transition.request_id_ = MakeRequestId(0x62);
  transition.operation_id_ = op_id;
  transition.expected_revision_ = 0;
  transition.kind_phase_blob_ = "{\"phase\":\"prepare\"}";
  ApplyOk(stores, 2, MetaCommand{transition});
  EXPECT_EQ(stores.operation_.FindOperation(op_id)->lifecycle_,
            keylane::meta::MetaOperationLifecycle::kRunning);

  keylane::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x63);
  complete.operation_id_ = op_id;
  complete.expected_revision_ = 1;
  complete.result_ = "{\"moved\":1}";
  complete.data_loss_possible_ = true;
  ApplyOk(stores, 3, MetaCommand{complete});
  const auto done = stores.operation_.FindOperation(op_id);
  ASSERT_TRUE(done.has_value());
  EXPECT_EQ(done->lifecycle_,
            keylane::meta::MetaOperationLifecycle::kCompleted);
  EXPECT_TRUE(done->data_loss_possible_);

  // Terminal states are irreversible.
  keylane::meta::TransitionOperationPhase late = transition;
  late.request_id_ = MakeRequestId(0x64);
  late.expected_revision_ = 2;
  ApplyRejected(stores, 4, MetaCommand{late});

  // Non-contiguous archival of the terminal operation (seq = submit index).
  keylane::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x65);
  archive.operation_seqs_ = {1};
  ApplyOk(stores, 5, MetaCommand{archive});
  EXPECT_FALSE(stores.operation_.FindOperation(op_id).has_value());
  const auto tombstone = stores.operation_.FindArchived(op_id);
  ASSERT_TRUE(tombstone.has_value());
  EXPECT_EQ(tombstone->operation_seq_, 1u);
  EXPECT_EQ(tombstone->terminal_lifecycle_,
            keylane::meta::MetaOperationLifecycle::kCompleted);
  EXPECT_TRUE(tombstone->data_loss_possible_);
  EXPECT_EQ(tombstone->actor_.principal_, kActorPrincipal);

  // A late duplicate submit resolves against the tombstone: same id + same
  // intent_hash -> idempotent accept ("already done").
  ApplyOk(stores, 6, MetaCommand{submit});
  EXPECT_EQ(stores.operation_.LiveCount(), 0u);
  EXPECT_EQ(stores.operation_.ArchivedCount(), 1u);
}

TEST(MetaStateApply, DirectiveResultCommitUsesFirstRaftIndexOnReplay) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));
  ApplyOk(stores, 7, MakeAssign("g1", 2, 2, 3));
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 1, 4, 1)});
  const keylane::meta::SubmitOperation submit = MakeSubmit(0x68, 0x41);
  ApplyOk(stores, 9, MetaCommand{submit});

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.recipient_node_id_ = MakeNodeId(1);
  directive.target_node_id_ = MakeNodeId(1);
  directive.target_boot_id_.fill(3);
  directive.assignment_id_.fill(1);
  directive.source_node_id_ = MakeNodeId(2);
  directive.source_assignment_id_.fill(2);
  directive.source_boot_id_.fill(5);
  directive.source_replication_history_id_.fill(6);
  directive.group_id_ = "g1";
  directive.group_term_ = 1;
  directive.authority_version_ = 1;
  directive.grant_revision_ = 8;
  directive.kind_ = "rebuild";
  directive.storage_mutating_ = true;
  keylane::meta::TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_ = {directive};
  ApplyOk(stores, 10, MetaCommand{transition});

  keylane::meta::CommitDirectiveResult commit;
  commit.operation_id_ = submit.operation_id_;
  commit.directive_id_ = directive.directive_id_;
  commit.attempt_id_ = directive.attempt_id_;
  commit.directive_revision_ = 10;
  commit.recipient_node_id_ = directive.recipient_node_id_;
  commit.recipient_boot_id_ = directive.target_boot_id_;
  commit.assignment_id_ = directive.assignment_id_;
  commit.result_ = "installed";
  commit.result_hash_ = keylane::meta::MetaSha256(commit.result_);

  const auto applied = ApplyOk(stores, 11, MetaCommand{commit});
  EXPECT_EQ(applied.command_tag_,
            keylane::meta::MetaCommandTag::kCommitDirectiveResult);
  const keylane::meta::MetaTerminalReceiptKey key{
      submit.operation_id_, directive.directive_id_, directive.attempt_id_, 10};
  ASSERT_TRUE(stores.operation_.FindTerminalReceipt(key).has_value());
  EXPECT_EQ(stores.operation_.FindTerminalReceipt(key)->committed_index_, 11u);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            2u);

  keylane::meta::CompleteOperation stale_complete;
  stale_complete.operation_id_ = submit.operation_id_;
  stale_complete.expected_revision_ = 1;
  ApplyRejected(stores, 12, MetaCommand{stale_complete});

  ApplyOk(stores, 13, MetaCommand{commit});
  EXPECT_EQ(stores.operation_.FindTerminalReceipt(key)->committed_index_, 11u);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            2u);
}

TEST(MetaStateApply, DirectiveIntentMustMatchCommittedAuthorityAndAssignment) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));
  ApplyOk(stores, 7, MakeAssign("g1", 2, 2, 3));
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 1, 4, 1)});
  const keylane::meta::SubmitOperation submit = MakeSubmit(0x69, 0x42);
  ApplyOk(stores, 9, MetaCommand{submit});

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.recipient_node_id_ = MakeNodeId(2);
  directive.target_node_id_ = MakeNodeId(2);
  directive.target_boot_id_.fill(3);
  directive.assignment_id_.fill(2);
  directive.source_node_id_ = MakeNodeId(1);
  directive.source_assignment_id_.fill(1);
  directive.source_boot_id_.fill(4);
  directive.source_replication_history_id_.fill(5);
  directive.group_id_ = "g1";
  directive.group_term_ = 1;
  directive.authority_version_ = 1;
  directive.grant_revision_ = 8;
  directive.partition_replication_epoch_ = 0;
  directive.kind_ = "rebuild";
  directive.storage_mutating_ = true;

  keylane::meta::TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_ = {directive};
  transition.current_directives_[0].grant_revision_ = 7;
  ApplyRejected(stores, 10, MetaCommand{transition});
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            0u);

  transition.current_directives_[0] = directive;
  transition.current_directives_[0].partition_replication_epoch_ = 1;
  ApplyRejected(stores, 11, MetaCommand{transition});
  transition.current_directives_[0] = directive;
  ApplyOk(stores, 12, MetaCommand{transition});
  const auto installed = stores.operation_.FindOperation(submit.operation_id_)
                             ->current_directives_;
  ASSERT_EQ(installed.size(), 1u);
  EXPECT_EQ(installed[0].spec_, directive);
  EXPECT_EQ(installed[0].directive_revision_, 12u);
}

struct InstalledDirectiveFixture {
  MetaStores stores;
  keylane::meta::SubmitOperation submit;
  keylane::meta::MetaDirectiveSpec directive;
  std::uint64_t directive_revision = 12;
};

// Builds a three-member group whose owner is separate from the directive's
// source and target. This lets membership tests remove either endpoint while
// leaving the active grant itself valid.
InstalledDirectiveFixture MakeInstalledDirectiveFixture() {
  InstalledDirectiveFixture fixture;
  SetupActivatedGroupPrerequisites(fixture.stores, 1, "g1");
  ApplyOk(fixture.stores, 6, MakeRegisterFor(2));
  ApplyOk(fixture.stores, 7, MakeAssign("g1", 2, 2, 3));
  ApplyOk(fixture.stores, 8, MakeRegisterFor(3));
  ApplyOk(fixture.stores, 9, MakeAssign("g1", 3, 3, 4));
  ApplyOk(fixture.stores, 10, MetaCommand{MakeActivate("g1", 1, 3, 1, 5, 1)});

  fixture.submit = MakeSubmit(0x6d, 0x44);
  ApplyOk(fixture.stores, 11, MetaCommand{fixture.submit});

  fixture.directive.directive_id_.fill(1);
  fixture.directive.attempt_id_.fill(2);
  fixture.directive.recipient_node_id_ = MakeNodeId(2);
  fixture.directive.target_node_id_ = MakeNodeId(2);
  fixture.directive.target_boot_id_.fill(3);
  fixture.directive.assignment_id_.fill(2);
  fixture.directive.source_node_id_ = MakeNodeId(1);
  fixture.directive.source_assignment_id_.fill(1);
  fixture.directive.source_boot_id_.fill(4);
  fixture.directive.source_replication_history_id_.fill(5);
  fixture.directive.group_id_ = "g1";
  fixture.directive.group_term_ = 1;
  fixture.directive.authority_version_ = 1;
  fixture.directive.grant_revision_ = 10;
  fixture.directive.kind_ = "rebuild";
  fixture.directive.storage_mutating_ = true;

  keylane::meta::TransitionOperationPhase transition;
  transition.operation_id_ = fixture.submit.operation_id_;
  transition.current_directives_ = {fixture.directive};
  ApplyOk(fixture.stores, fixture.directive_revision, MetaCommand{transition});
  return fixture;
}

keylane::meta::CommitDirectiveResult MakeDirectiveResult(
    const InstalledDirectiveFixture& fixture) {
  keylane::meta::CommitDirectiveResult result;
  result.operation_id_ = fixture.submit.operation_id_;
  result.directive_id_ = fixture.directive.directive_id_;
  result.attempt_id_ = fixture.directive.attempt_id_;
  result.directive_revision_ = fixture.directive_revision;
  result.recipient_node_id_ = fixture.directive.recipient_node_id_;
  result.recipient_boot_id_ = fixture.directive.target_boot_id_;
  result.assignment_id_ = fixture.directive.assignment_id_;
  result.result_ = "installed";
  result.result_hash_ = keylane::meta::MetaSha256(result.result_);
  return result;
}

void ExpectDirectiveInvalidated(const InstalledDirectiveFixture& fixture,
                                std::uint64_t expected_revision = 2) {
  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.submit.operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_TRUE(operation->current_directives_.empty());
  // Clearing a live directive is a durable operation-record mutation. The
  // revision bump forces a reconciler holding the prior phase CAS to re-read.
  EXPECT_EQ(operation->revision_, expected_revision);
}

TEST(MetaStateApply,
     RemovingSourceOrTargetInvalidatesDirectiveAcrossReplayAndRestart) {
  for (const std::uint32_t removed_node : {1u, 2u}) {
    SCOPED_TRACE("removed_node=" + std::to_string(removed_node));
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();

    keylane::meta::RemoveNodeFromGroup remove;
    remove.request_id_ = MakeRequestId(0x6e);
    remove.group_id_ = "g1";
    remove.node_id_ = MakeNodeId(removed_node);
    remove.expected_revision_ = 4;
    remove.new_topology_epoch_ = 6;
    ApplyOk(fixture.stores, 13, MetaCommand{remove});
    ExpectDirectiveInvalidated(fixture);

    const std::string after_first_apply = DomainStateBytes(fixture.stores);
    ApplyOk(fixture.stores, 13, MetaCommand{remove});
    EXPECT_EQ(DomainStateBytes(fixture.stores), after_first_apply);

    auto restored = MetaStores::Deserialize(MustSerialize(fixture.stores));
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(MustSerialize(*restored), MustSerialize(fixture.stores));

    // Reusing the stable node id with a fresh membership incarnation cannot
    // revive work authorized for the removed assignment.
    keylane::meta::AssignNodeToGroup readd =
        MakeAssign("g1", removed_node, 5, 7);
    readd.assignment_id_.fill(static_cast<std::uint8_t>(0x20 + removed_node));
    ApplyOk(fixture.stores, 14, MetaCommand{readd});
    ExpectDirectiveInvalidated(fixture);

    const auto result = MakeDirectiveResult(fixture);
    ApplyRejected(fixture.stores, 15, MetaCommand{result});
    const keylane::meta::MetaTerminalReceiptKey key{
        result.operation_id_, result.directive_id_, result.attempt_id_,
        result.directive_revision_};
    EXPECT_FALSE(
        fixture.stores.operation_.FindTerminalReceipt(key).has_value());
  }
}

TEST(MetaStateApply, AuthorityAnchorMutationsInvalidateLiveDirectives) {
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::BeginGroupTerm begin;
    begin.group_id_ = "g1";
    begin.expected_term_ = 1;
    begin.new_term_ = 2;
    ApplyOk(fixture.stores, 13, MetaCommand{begin});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::GrantAuthority renew;
    renew.group_id_ = "g1";
    renew.node_id_ = MakeNodeId(3);
    renew.term_ = 1;
    renew.authority_version_ = 1;
    renew.grant_.lease_duration_ms_ = 9000;
    renew.grant_.policy_id_ = "p";
    renew.grant_.policy_version_ = 1;
    ApplyOk(fixture.stores, 13, MetaCommand{renew});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    ApplyOk(fixture.stores, 13, MetaCommand{MakeActivate("g1", 1, 3, 2, 6, 2)});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::RevokeGrant revoke;
    revoke.group_id_ = "g1";
    revoke.expected_term_ = 1;
    ApplyOk(fixture.stores, 13, MetaCommand{revoke});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::FenceGroup fence;
    fence.group_id_ = "g1";
    fence.expected_term_ = 1;
    ApplyOk(fixture.stores, 13, MetaCommand{fence});
    ExpectDirectiveInvalidated(fixture);
  }
}

TEST(MetaStateApply, PopulationAnchorMutationsInvalidateLiveDirectives) {
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::PutPopulationManifest put;
    put.entries_ = {{1, 11}};
    put.manifest_digest_ =
        keylane::meta::MetaPopulationManifestStore::CanonicalDigest(
            put.entries_);
    ApplyOk(fixture.stores, 13, MetaCommand{put});
    EXPECT_EQ(
        fixture.stores.operation_.FindOperation(fixture.submit.operation_id_)
            ->current_directives_.size(),
        1u);

    keylane::meta::SetGroupReplicationState update;
    update.group_id_ = "g1";
    update.new_population_manifest_revision_ = 1;
    update.new_population_manifest_digest_ = put.manifest_digest_;
    update.new_topology_epoch_ = 6;
    ApplyOk(fixture.stores, 14, MetaCommand{update});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    keylane::meta::SetGroupReplicationState update;
    update.group_id_ = "g1";
    update.new_partition_replication_epoch_ = 1;
    update.new_topology_epoch_ = 6;
    ApplyOk(fixture.stores, 13, MetaCommand{update});
    ExpectDirectiveInvalidated(fixture);
  }
}

TEST(MetaStateApply, RejectedAnchorMutationDoesNotInvalidateDirective) {
  InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
  keylane::meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.directive.target_node_id_;
  remove.expected_revision_ = 99;
  remove.new_topology_epoch_ = 6;
  ApplyRejected(fixture.stores, 13, MetaCommand{remove});

  const auto operation =
      fixture.stores.operation_.FindOperation(fixture.submit.operation_id_);
  ASSERT_TRUE(operation.has_value());
  ASSERT_EQ(operation->current_directives_.size(), 1u);
  EXPECT_EQ(operation->current_directives_.front().spec_, fixture.directive);
  EXPECT_EQ(operation->revision_, 1u);
}

TEST(MetaStateApply, DirectiveResultRevalidatesCurrentAggregateAnchor) {
  InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
  keylane::meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.directive.target_node_id_;
  remove.expected_revision_ = 4;
  remove.new_topology_epoch_ = 6;
  // Deliberately bypass ApplyCommitted to model a stale/inconsistent current
  // directive. The result ingress must defend itself even if an invalidation
  // hook is missed by a future mutation path.
  ASSERT_TRUE(fixture.stores.topology_.Apply(remove).ok());
  ASSERT_EQ(
      fixture.stores.operation_.FindOperation(fixture.submit.operation_id_)
          ->current_directives_.size(),
      1u);
  ExpectAggregateSnapshotFailStop(fixture.stores);

  const auto result = MakeDirectiveResult(fixture);
  ApplyRejected(fixture.stores, 13, MetaCommand{result});
  const keylane::meta::MetaTerminalReceiptKey key{
      result.operation_id_, result.directive_id_, result.attempt_id_,
      result.directive_revision_};
  EXPECT_FALSE(fixture.stores.operation_.FindTerminalReceipt(key).has_value());
}

TEST(MetaStateApply,
     CommittedDirectiveResultRemainsReplayableAfterInvalidation) {
  InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
  const auto result = MakeDirectiveResult(fixture);
  ApplyOk(fixture.stores, 13, MetaCommand{result});

  keylane::meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.directive.target_node_id_;
  remove.expected_revision_ = 4;
  remove.new_topology_epoch_ = 6;
  ApplyOk(fixture.stores, 14, MetaCommand{remove});
  ExpectDirectiveInvalidated(fixture, 3);

  ApplyOk(fixture.stores, 15, MetaCommand{result});
  const keylane::meta::MetaTerminalReceiptKey key{
      result.operation_id_, result.directive_id_, result.attempt_id_,
      result.directive_revision_};
  const auto receipt = fixture.stores.operation_.FindTerminalReceipt(key);
  ASSERT_TRUE(receipt.has_value());
  EXPECT_EQ(receipt->committed_index_, 13u);
}

TEST(MetaStateApply, DirectiveRejectsSourceAssignmentFromBeforeRemoveAndReadd) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));
  ApplyOk(stores, 7, MakeAssign("g1", 2, 2, 3));
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 1, 4, 1)});
  const keylane::meta::SubmitOperation submit = MakeSubmit(0x6a, 0x43);
  ApplyOk(stores, 9, MetaCommand{submit});

  keylane::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = MakeRequestId(0x6b);
  remove.group_id_ = "g1";
  remove.node_id_ = MakeNodeId(2);
  remove.expected_revision_ = 3;
  remove.new_topology_epoch_ = 5;
  ApplyOk(stores, 10, MetaCommand{remove});

  keylane::meta::AssignNodeToGroup readd = MakeAssign("g1", 2, 4, 6);
  readd.request_id_ = MakeRequestId(0x6c);
  readd.assignment_id_.fill(0x22);
  ApplyOk(stores, 11, MetaCommand{readd});

  keylane::meta::MetaDirectiveSpec directive;
  directive.directive_id_.fill(1);
  directive.attempt_id_.fill(2);
  directive.recipient_node_id_ = MakeNodeId(2);
  directive.target_node_id_ = MakeNodeId(1);
  directive.target_boot_id_.fill(3);
  directive.assignment_id_.fill(1);
  directive.source_node_id_ = MakeNodeId(2);
  directive.source_assignment_id_.fill(2);  // removed incarnation
  directive.source_boot_id_.fill(4);
  directive.source_replication_history_id_.fill(5);
  directive.group_id_ = "g1";
  directive.group_term_ = 1;
  directive.authority_version_ = 1;
  directive.grant_revision_ = 8;
  directive.kind_ = "authorize-source";

  keylane::meta::TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_ = {directive};
  ApplyRejected(stores, 12, MetaCommand{transition});
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            0u);

  transition.current_directives_[0].source_assignment_id_ =
      readd.assignment_id_;
  ApplyOk(stores, 13, MetaCommand{transition});
}

TEST(MetaStateApply,
     TransitionRejectsAggregateRecipientProjectionOverDirectiveCapAtomically) {
  namespace control = keylane::cluster::control;
  static_assert(control::kMaxProjectedDirectives %
                    keylane::meta::kMaxMetaDirectivesPerOperation ==
                0);

  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 1, 3, 1)});

  const auto make_directive = [](std::size_t ordinal) {
    keylane::meta::MetaDirectiveSpec directive;
    directive.directive_id_.fill(static_cast<std::uint8_t>(ordinal + 1));
    directive.attempt_id_.fill(static_cast<std::uint8_t>(ordinal + 65));
    directive.recipient_node_id_ = MakeNodeId(1);
    directive.target_node_id_ = MakeNodeId(1);
    directive.target_boot_id_.fill(1);
    directive.assignment_id_.fill(1);
    directive.source_node_id_ = MakeNodeId(1);
    directive.source_assignment_id_.fill(1);
    directive.source_boot_id_.fill(2);
    directive.source_replication_history_id_.fill(3);
    directive.group_id_ = "g1";
    directive.group_term_ = 1;
    directive.authority_version_ = 1;
    directive.grant_revision_ = 6;
    directive.kind_ = "authorize-source";
    return directive;
  };

  std::uint64_t log_index = 7;
  const std::size_t full_operation_count =
      control::kMaxProjectedDirectives /
      keylane::meta::kMaxMetaDirectivesPerOperation;
  for (std::size_t operation_ordinal = 0;
       operation_ordinal < full_operation_count; ++operation_ordinal) {
    keylane::meta::SubmitOperation submit =
        MakeSubmit(static_cast<std::uint8_t>(operation_ordinal + 1),
                   static_cast<std::uint8_t>(operation_ordinal + 101));
    submit.replication_history_id_.fill(3);
    ASSERT_TRUE(stores.operation_.SubmitOperation(submit, log_index++).ok());

    keylane::meta::TransitionOperationPhase transition;
    transition.operation_id_ = submit.operation_id_;
    transition.kind_phase_blob_ = "dispatch";
    transition.current_directives_.reserve(
        keylane::meta::kMaxMetaDirectivesPerOperation);
    for (std::size_t directive_ordinal = 0;
         directive_ordinal < keylane::meta::kMaxMetaDirectivesPerOperation;
         ++directive_ordinal) {
      transition.current_directives_.push_back(
          make_directive(directive_ordinal));
    }
    ASSERT_TRUE(
        stores.operation_.TransitionOperationPhase(transition, log_index++)
            .ok());
  }

  keylane::meta::SubmitOperation overflow = MakeSubmit(0x70, 0x71);
  overflow.replication_history_id_.fill(3);
  ASSERT_TRUE(stores.operation_.SubmitOperation(overflow, log_index++).ok());

  keylane::meta::TransitionOperationPhase transition;
  transition.operation_id_ = overflow.operation_id_;
  transition.kind_phase_blob_ = "would-overflow";
  transition.current_directives_ = {make_directive(0)};
  const std::string domain_before = DomainStateBytes(stores);
  const std::size_t audit_size_before = stores.audit_.size();

  const MetaApplyResult first =
      ApplyRejected(stores, log_index, MetaCommand{transition});
  EXPECT_NE(first.detail_.find("current directives exceeds its entry cap"),
            std::string::npos)
      << first.detail_;
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  const auto record = stores.operation_.FindOperation(overflow.operation_id_);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->revision_, 0u);
  EXPECT_TRUE(record->current_directives_.empty());
  EXPECT_EQ(stores.audit_.size(), audit_size_before + 1);

  // Replaying the rejected log entry produces the same rejection and cannot
  // append another audit record or partially install the candidate phase.
  const MetaApplyResult replay =
      ApplyRejected(stores, log_index, MetaCommand{transition});
  EXPECT_EQ(replay, first);
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  EXPECT_EQ(stores.audit_.size(), audit_size_before + 1);
}

TEST(MetaStateApply, ArchiveOperationsRejectsNonTerminal) {
  MetaStores stores;
  ApplyOk(stores, 1, MetaCommand{MakeSubmit(0x66, 0x31)});

  keylane::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x67);
  archive.operation_seqs_ = {1};
  ApplyRejected(stores, 2, MetaCommand{archive});  // still Submitted
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);

  // References to unknown seqs reject as well.
  archive.operation_seqs_ = {42};
  ApplyRejected(stores, 3, MetaCommand{archive});
}

TEST(MetaStateApply, AuditPruneIsReplicatedAndAudited) {
  keylane::meta::MetaStores stores;
  auto create = MakeCreateGroup("g-prune", 1);
  ASSERT_EQ(keylane::meta::ApplyCommitted(stores, 1, create,
                                          "keylane://operator/test", "t")
                .verdict_,
            keylane::meta::MetaAuditVerdict::kAccepted);
  ASSERT_EQ(stores.audit_.size(), 1u);

  keylane::meta::PruneAudit prune;
  prune.request_id_[0] = 9;
  prune.through_log_index_ = 1;
  const auto result = keylane::meta::ApplyCommitted(
      stores, 2, prune, "keylane://operator/test", "t2");
  EXPECT_EQ(result.verdict_, keylane::meta::MetaAuditVerdict::kAccepted);
  EXPECT_EQ(stores.audit_.pruned_floor(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);
  EXPECT_TRUE(stores.audit_.Find(2).has_value());
}

// ---------------------------------------------------------------------------
// Full-matrix replay: a scripted
// log mixing all 20 command types, accepted and rejected. The apply layer
// locks the semantics the recovery path relies on.
// ---------------------------------------------------------------------------

struct ScriptedCommand {
  keylane::meta::MetaCommand command;
  MetaAuditVerdict expected;
};

// The script covers every command tag at least once, interleaving
// accepts and rejects. Revision/epoch/term tokens are pinned to the state the
// prefix produces.
std::vector<ScriptedCommand> MakeCommandScript() {
  std::vector<ScriptedCommand> script;
  const auto accept = MetaAuditVerdict::kAccepted;
  const auto reject = MetaAuditVerdict::kRejected;
  auto push = [&script](keylane::meta::MetaCommand cmd,
                        MetaAuditVerdict expected) {
    script.push_back(ScriptedCommand{std::move(cmd), expected});
  };

  // identity
  push(MakeRegisterFor(1), accept);
  push(MakeRegisterFor(2), accept);
  {
    keylane::meta::RegisterNode conflict = MakeRegisterFor(3);
    conflict.principal_ = "keylane://node/" + MakeNodeId(1);
    push(std::move(conflict), reject);  // principal 1:1
  }
  // topology: groups
  push(MakeCreateGroup("g1", 1), accept);
  push(MakeCreateGroup("g2", 2), accept);
  push(MakeCreateGroup("g1", 3), reject);  // existing group, epoch mismatch
  // policy
  push(MakePutPolicy("p", 1, "{\"lease_ms\":5000}"), accept);
  push(MakePutPolicy("p", 1, "{\"lease_ms\":9999}"),
       reject);  // version slot immutable
  // topology: membership
  push(MakeAssign("g1", 1, 1, 3), accept);
  push(MakeAssign("g1", 2, 2, 4), accept);
  push(MakeAssign("g2", 1, 1), reject);  // one-node-one-group
  push(MakeAssign("g1", 9, 3), reject);  // unregistered node
  // term
  {
    keylane::meta::BeginGroupTerm begin;
    begin.request_id_ = MakeRequestId(0x40);
    begin.group_id_ = "g1";
    begin.expected_term_ = 0;
    begin.new_term_ = 1;
    push(begin, accept);
    begin.request_id_ = MakeRequestId(0x41);
    begin.expected_term_ = 1;
    begin.new_term_ = 3;  // not expected+1
    push(begin, reject);
  }
  // activation + renewal
  const keylane::meta::ActivateAuthority activate =
      MakeActivate("g1", 1, 1, 1, 5, 1);
  push(activate, accept);
  push(activate, accept);  // same content, new index: idempotent path
  {
    keylane::meta::GrantAuthority renew;
    renew.request_id_ = MakeRequestId(0x44);
    renew.group_id_ = "g1";
    renew.node_id_ = MakeNodeId(1);
    renew.term_ = 1;
    renew.authority_version_ = 1;
    renew.grant_.lease_duration_ms_ = 7000;
    renew.grant_.policy_id_ = "p";
    renew.grant_.policy_version_ = 1;
    push(renew, accept);
    renew.request_id_ = MakeRequestId(0x45);
    renew.grant_.policy_version_ = 99;  // not committed
    push(renew, reject);
  }
  {
    keylane::meta::RetirePolicy retire;
    retire.request_id_ = MakeRequestId(0x51);
    retire.policy_id_ = "p";
    retire.version_ = 1;
    push(retire, reject);  // referenced by the active grant
  }
  // operation lifecycle
  const keylane::meta::SubmitOperation submit = MakeSubmit(0x60, 0x11);
  push(submit, accept);  // index 20: operation_seq == 20
  {
    keylane::meta::SubmitOperation reuse = MakeSubmit(0x60, 0x12);
    push(std::move(reuse), reject);  // id reused with a different intent
  }
  {
    keylane::meta::TransitionOperationPhase transition;
    transition.request_id_ = MakeRequestId(0x61);
    transition.operation_id_ = submit.operation_id_;
    transition.expected_revision_ = 0;
    transition.kind_phase_blob_ = "{\"phase\":\"prepare\"}";
    push(transition, accept);
    keylane::meta::CompleteOperation complete;
    complete.request_id_ = MakeRequestId(0x62);
    complete.operation_id_ = submit.operation_id_;
    complete.expected_revision_ = 1;
    complete.result_ = "{}";
    push(complete, accept);
    transition.request_id_ = MakeRequestId(0x63);
    transition.expected_revision_ = 2;
    push(transition, reject);  // terminal is irreversible
  }
  {
    keylane::meta::ArchiveOperations archive;
    archive.request_id_ = MakeRequestId(0x64);
    archive.operation_seqs_ = {20};
    push(archive, accept);
    push(archive, accept);  // already archived: idempotent
  }
  // slot map
  {
    keylane::meta::SetSlotMap slots;
    slots.request_id_ = MakeRequestId(0x33);
    slots.ranges_ = {{0, 16383, "g1"}};
    slots.new_topology_epoch_ = 6;
    slots.config_epochs_ = {{"g1", 7}};
    push(slots, accept);
  }
  {
    keylane::meta::PutPopulationManifest put;
    put.request_id_ = MakeRequestId(0x34);
    put.entries_ = {{1, 1}};
    put.manifest_digest_ =
        keylane::meta::MetaPopulationManifestStore::CanonicalDigest(
            put.entries_);
    push(put, accept);

    keylane::meta::SetGroupReplicationState replication;
    replication.request_id_ = MakeRequestId(0x35);
    replication.group_id_ = "g1";
    replication.new_population_manifest_revision_ = 1;
    replication.new_population_manifest_digest_ = put.manifest_digest_;
    replication.new_partition_replication_epoch_ = 1;
    replication.new_topology_epoch_ = 7;
    push(replication, accept);
  }
  // membership removal, retire, retired-node assign
  {
    keylane::meta::UpdateNode update;
    update.request_id_ = MakeRequestId(0x21);
    update.node_id_ = MakeNodeId(2);
    update.expected_revision_ = 1;
    update.endpoints_ = {"10.0.0.9:7000"};
    update.new_topology_epoch_ = 8;
    push(update, accept);
    keylane::meta::RemoveNodeFromGroup remove;
    remove.request_id_ = MakeRequestId(0x32);
    remove.group_id_ = "g1";
    remove.node_id_ = MakeNodeId(2);
    remove.expected_revision_ = 3;
    remove.new_topology_epoch_ = 9;
    push(remove, accept);
    keylane::meta::RetireNode retire;
    retire.request_id_ = MakeRequestId(0x22);
    retire.node_id_ = MakeNodeId(2);
    retire.expected_revision_ = 2;
    push(retire, accept);
    push(MakeAssign("g2", 2, 1), reject);  // retired node
  }
  // grant teardown, then the policy reference clears
  {
    keylane::meta::RevokeGrant revoke;
    revoke.request_id_ = MakeRequestId(0x43);
    revoke.group_id_ = "g1";
    revoke.expected_term_ = 1;
    push(revoke, accept);
    keylane::meta::RetirePolicy retire;
    retire.request_id_ = MakeRequestId(0x52);
    retire.policy_id_ = "p";
    retire.version_ = 1;
    push(retire, accept);
    keylane::meta::FenceGroup fence;
    fence.request_id_ = MakeRequestId(0x46);
    fence.group_id_ = "g1";
    fence.expected_term_ = 1;
    push(fence, accept);
  }
  return script;
}

TEST(MetaStateApply, CommandMatrixConsecutiveReplayLocksVerdictStateAudit) {
  MetaStores stores;
  const std::vector<ScriptedCommand> script = MakeCommandScript();
  std::uint64_t index = 0;
  for (const ScriptedCommand& step : script) {
    ++index;
    const MetaApplyResult first = ApplyCommitted(
        stores, index, step.command, kActorPrincipal, kReadableTime);
    EXPECT_EQ(first.verdict_, step.expected)
        << "index " << index << ": " << first.detail_;
    EXPECT_EQ(first.log_index_, index);
    const std::string domain_after_first = DomainStateBytes(stores);
    const auto audit_after_first = stores.audit_.Find(index);
    ASSERT_TRUE(audit_after_first.has_value()) << "index " << index;

    // commit() may be delivered again for the same index. Same verdict,
    // same detail, domain state unchanged, audit window unchanged.
    const MetaApplyResult duplicate = ApplyCommitted(
        stores, index, step.command, kActorPrincipal, kReadableTime);
    EXPECT_EQ(duplicate, first) << "index " << index;
    EXPECT_EQ(DomainStateBytes(stores), domain_after_first)
        << "index " << index;
    EXPECT_EQ(stores.audit_.size(), index) << "index " << index;
    EXPECT_EQ(stores.audit_.Find(index)->record_, audit_after_first->record_)
        << "index " << index;
  }
  EXPECT_EQ(stores.audit_.size(), script.size());
  EXPECT_TRUE(stores.audit_.VerifyChain());
}

TEST(MetaStateApply, WholeLogReplayFromEmptyReproducesStateAndAudit) {
  const std::vector<ScriptedCommand> script = MakeCommandScript();

  MetaStores first_run;
  std::vector<MetaApplyResult> first_results;
  std::uint64_t index = 0;
  for (const ScriptedCommand& step : script) {
    first_results.push_back(ApplyCommitted(first_run, ++index, step.command,
                                           kActorPrincipal, kReadableTime));
  }

  // Replay the identical byte stream from an empty state (the recovery path
  // with no snapshot): identical verdicts and byte-identical final state,
  // audit window and hash chain included.
  MetaStores second_run;
  index = 0;
  for (const ScriptedCommand& step : script) {
    const MetaApplyResult result = ApplyCommitted(
        second_run, ++index, step.command, kActorPrincipal, kReadableTime);
    EXPECT_EQ(result, first_results[index - 1]) << "index " << index;
  }
  EXPECT_EQ(MustSerialize(second_run), MustSerialize(first_run));

  // A snapshot round-trip of the fully populated aggregate preserves
  // everything, including the audit chain.
  const auto restored = MetaStores::Deserialize(MustSerialize(first_run));
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(MustSerialize(*restored), MustSerialize(first_run));
  EXPECT_TRUE(restored->audit_.VerifyChain());
}

}  // namespace
