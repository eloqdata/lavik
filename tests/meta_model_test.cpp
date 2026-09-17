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

#include "meta_topology_test_access.h"

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
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/encoding.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"

namespace {

using lavik::meta::MetaReader;
using lavik::meta::MetaWriter;

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
  lavik::meta::WriteFixedArray(w, id);

  MetaReader r(w.buffer());
  auto back = lavik::meta::ReadFixedArray<16>(r);
  ASSERT_TRUE(back.ok()) << back.status();
  EXPECT_EQ(*back, id);
  EXPECT_TRUE(r.Finish().ok());
  MetaReader short_r(std::string_view("\x00\x01", 2));
  EXPECT_FALSE(lavik::meta::ReadFixedArray<16>(short_r).ok());
}

// ---------------------------------------------------------------------------
// Failure classification: fail-stop decode failures vs domain
// rejections must be distinguishable at the type/enum level.
// ---------------------------------------------------------------------------

TEST(MetaModelEncoding, FailureClassesAreDistinguishable) {
  using lavik::meta::MetaFailureClass;
  using lavik::meta::MetaFailureClassOf;

  // Decode-path failures (produced by MetaReader) classify as fail-stop.
  MetaReader r(std::string_view("\x00", 1));
  auto truncated = r.ReadU64();
  ASSERT_FALSE(truncated.ok());
  EXPECT_EQ(MetaFailureClassOf(truncated.status()),
            MetaFailureClass::kFailStop);
  EXPECT_EQ(truncated.status().code(), absl::StatusCode::kInvalidArgument);

  EXPECT_EQ(MetaFailureClassOf(lavik::meta::MetaFailStopError("x")),
            MetaFailureClass::kFailStop);

  // Domain rejections (apply/propose validation) classify
  // separately: log index consumed, audit record written, state unchanged.
  const absl::Status domain = lavik::meta::MetaDomainRejectError("cas");
  EXPECT_EQ(MetaFailureClassOf(domain), MetaFailureClass::kDomainReject);
  EXPECT_EQ(domain.code(), absl::StatusCode::kFailedPrecondition);
}

// ---------------------------------------------------------------------------
// Command envelope: u16 schema_version | u16 command tag | request_id | body.
// ---------------------------------------------------------------------------

using lavik::meta::DecodeMetaCommand;
using lavik::meta::EncodeMetaCommand;
using lavik::meta::MetaCommand;
using lavik::meta::MetaRequestId;

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
  EXPECT_EQ(lavik::meta::MetaFailureClassOf(decoded.status()),
            lavik::meta::MetaFailureClass::kFailStop);
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
  const auto decoded = lavik::meta::DecodeMetaGroupRecord(bytes);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(lavik::meta::MetaFailureClassOf(decoded.status()),
            lavik::meta::MetaFailureClass::kFailStop);
}

// Encode-side cap violations are proposal-validation failures (domain
// reject): nothing out-of-spec ever reaches the wire.
void ExpectEncodeDomainReject(const MetaCommand& cmd) {
  const auto encoded = EncodeMetaCommand(cmd);
  ASSERT_FALSE(encoded.ok()) << "encoded unexpectedly";
  EXPECT_EQ(lavik::meta::MetaFailureClassOf(encoded.status()),
            lavik::meta::MetaFailureClass::kDomainReject);
}

lavik::meta::RegisterNode MakeRegisterNode() {
  lavik::meta::RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(0x10);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";  // 40 hex
  cmd.principal_ = "lavik://node/0123456789abcdef0123456789abcdef01234567";
  cmd.endpoints_ = {"10.0.0.1:7000", "10.0.0.1:17000"};

  cmd.role_ = lavik::meta::MetaNodeRole::kReplica;
  return cmd;
}

TEST(MetaModelCommands, RegisterNodeRoundTrip) {
  const lavik::meta::RegisterNode cmd = MakeRegisterNode();
  const std::string bytes = MustEncode(cmd);

  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_TRUE(std::holds_alternative<lavik::meta::RegisterNode>(*decoded));
  EXPECT_EQ(std::get<lavik::meta::RegisterNode>(*decoded), cmd);
}

TEST(MetaModelCommands, EnvelopeStartsWithFormatVersionThenTag) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  ASSERT_GE(bytes.size(), 4u);
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data());
  const std::uint16_t version = static_cast<std::uint16_t>(p[0] | (p[1] << 8));
  const std::uint16_t tag = static_cast<std::uint16_t>(p[2] | (p[3] << 8));
  EXPECT_EQ(version, 1);
  EXPECT_EQ(version, lavik::meta::kMetaCommandFormatVersion);
  EXPECT_EQ(tag, static_cast<std::uint16_t>(
                     lavik::meta::MetaCommandTag::kRegisterNode));
}

TEST(MetaModelCommands, UnknownFormatVersionFails) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  // Only the current v1 envelope is supported; larger markers are not a
  // compatibility path for earlier development layouts.
  for (const std::uint16_t bad_version : {0, 2, 3, 4, 5, 0x7FFF, 0xFFFF}) {
    std::string corrupt = bytes;
    corrupt[0] = static_cast<char>(bad_version & 0xFF);
    corrupt[1] = static_cast<char>((bad_version >> 8) & 0xFF);
    ExpectDecodeFailStop(corrupt);
  }
}

TEST(MetaModelCommands, UnknownCommandTagFails) {
  const std::string bytes = MustEncode(MakeRegisterNode());
  // Removed tags remain holes instead of being reused for a different durable
  // command type.
  for (const std::uint16_t bad_tag : {9, 14, 0xFFFF}) {
    std::string corrupt = bytes;
    corrupt[2] = static_cast<char>(bad_tag & 0xFF);
    corrupt[3] = static_cast<char>((bad_tag >> 8) & 0xFF);
    ExpectDecodeFailStop(corrupt);
  }
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
  lavik::meta::RegisterNode cmd = MakeRegisterNode();
  cmd.actor_.principal_ = "lavik://operator/alice";
  cmd.actor_.readable_time_ = "2026-09-04T01:02:03Z";

  const std::string bytes = MustEncode(cmd);
  EXPECT_NE(bytes.find("lavik://operator/alice"), std::string::npos);
  EXPECT_NE(bytes.find("2026-09-04T01:02:03Z"), std::string::npos);

  const auto decoded = DecodeMetaCommand(bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  // Equality covers the actor fields: they survive the round trip verbatim.
  EXPECT_EQ(std::get<lavik::meta::RegisterNode>(*decoded), cmd);
}

TEST(MetaModelCommands, ActorFieldCapsEnforced) {
  // Encode side: an over-cap actor field is a proposal-validation failure.
  lavik::meta::RegisterNode cmd = MakeRegisterNode();
  cmd.actor_.principal_ =
      std::string(lavik::meta::kMaxMetaPrincipalBytes + 1, 'p');
  ExpectEncodeDomainReject(cmd);
  cmd.actor_.principal_ = "lavik://operator/alice";
  cmd.actor_.readable_time_ =
      std::string(lavik::meta::kMaxMetaActorReadableTimeBytes + 1, 't');
  ExpectEncodeDomainReject(cmd);

  // Decode side: an over-cap actor length prefix on the wire is fail-stop.
  // Hand-built RegisterNode header: version | tag | request_id | actor...
  {
    lavik::meta::MetaWriter w;
    w.WriteU16(lavik::meta::kMetaCommandFormatVersion);
    w.WriteU16(
        static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kRegisterNode));
    w.WriteRaw(std::string(16, '\0'));                    // request_id
    w.WriteU32(lavik::meta::kMaxMetaPrincipalBytes + 1);  // actor prefix
    ExpectDecodeFailStop(w.buffer());
  }
  {
    lavik::meta::MetaWriter w;
    w.WriteU16(lavik::meta::kMetaCommandFormatVersion);
    w.WriteU16(
        static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kRegisterNode));
    w.WriteRaw(std::string(16, '\0'));
    w.WriteString("lavik://operator/alice");
    // readable_time prefix over its cap.
    w.WriteU32(lavik::meta::kMaxMetaActorReadableTimeBytes + 1);
    ExpectDecodeFailStop(w.buffer());
  }
}

TEST(MetaModelCommands, DecodeRejectsMissingActorFields) {
  // Bytes shaped like the obsolete pre-actor layout (request_id immediately
  // followed by node_id) are truncated input under the current layout: the
  // node_id length prefix is consumed as the actor principal prefix and the
  // decode runs out of bytes. (Every proper prefix already fails via
  // TruncatedCommandFails; this names the actor-position case explicitly.)
  lavik::meta::MetaWriter w;
  w.WriteU16(lavik::meta::kMetaCommandFormatVersion);
  w.WriteU16(
      static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kRegisterNode));
  w.WriteRaw(std::string(16, '\0'));  // request_id
  w.WriteString(MakeRegisterNode().node_id_);
  w.WriteString("lavik://node/x");
  w.WriteU32(0);  // empty endpoints
  w.WriteU64(0x5);
  w.WriteU8(1);
  ExpectDecodeFailStop(w.buffer());
}

// ---------------------------------------------------------------------------
// identity/enrollment.
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, UpdateNodeRoundTrip) {
  lavik::meta::UpdateNode cmd;
  cmd.request_id_ = MakeRequestId(0x20);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_revision_ = 41;
  cmd.endpoints_ = {"10.0.0.9:7000"};

  ExpectRoundTrip(cmd);
}

// UpdateNode must not be able to modify the principal binding because rotation
// is unimplemented. This is a schema-level guarantee, so assert it
// structurally. (The indirection through a template makes the member access
// dependent, so the requires-expression can fail softly.)
template <typename T>
concept HasPrincipalField = requires(T t) { t.principal_; };
static_assert(!HasPrincipalField<lavik::meta::UpdateNode>);

TEST(MetaModelCommands, RetireNodeRoundTrip) {
  lavik::meta::RetireNode cmd;
  cmd.request_id_ = MakeRequestId(0x21);
  cmd.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_revision_ = 42;
  ExpectRoundTrip(cmd);
}

// ---------------------------------------------------------------------------
// topology.
// ---------------------------------------------------------------------------

TEST(MetaModelCommands, CreateGroupRoundTrip) {
  lavik::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.new_topology_epoch_ = 100;  // absolute value
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, AssignNodeToGroupRoundTrip) {
  lavik::meta::AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.assignment_id_.fill(0x31);
  cmd.role_ = lavik::meta::MetaNodeRole::kReplica;
  cmd.expected_revision_ = 7;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, RemoveNodeFromGroupRoundTrip) {
  lavik::meta::RemoveNodeFromGroup cmd;
  cmd.request_id_ = MakeRequestId(0x32);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.node_id_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.expected_revision_ = 8;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, SetSlotMapRoundTrip) {
  lavik::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x33);
  cmd.ranges_ = {
      {0, 5460, "0123456789abcdef0123456789abcdef01234567"},
      {5461, 10922, "89abcdef0123456789abcdef0123456789abcdef"},
      {10923, 16383, "456789abcdef0123456789abcdef0123456789ab"},
  };
  cmd.new_topology_epoch_ = 101;  // absolute value
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, GroupRecordRoundTrip) {
  // The per-group committed record contains owner, group_term,
  // population_manifest_revision, and partition_replication_epoch.
  // replication_history_id is deliberately absent because it is scoped to a
  // data-plane boot. The record codec is defined here.
  lavik::meta::MetaGroupRecord record;
  record.owner_ = "0123456789abcdef0123456789abcdef01234567";
  record.group_term_ = 9;
  record.population_manifest_revision_ = 777;
  record.population_manifest_digest_.fill(0x77);
  record.partition_replication_epoch_ = 3;

  const auto encoded = lavik::meta::EncodeMetaGroupRecord(record);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  // Records carry the same u16 format-version envelope convention.
  ASSERT_GE(encoded->size(), 2u);
  const auto* p = reinterpret_cast<const unsigned char*>(encoded->data());
  EXPECT_EQ(static_cast<std::uint16_t>(p[0] | (p[1] << 8)),
            lavik::meta::kMetaFormatVersion);

  const auto decoded = lavik::meta::DecodeMetaGroupRecord(*encoded);
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
  lavik::meta::BeginGroupTerm cmd;
  cmd.request_id_ = MakeRequestId(0x40);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 41;  // T-1
  cmd.new_term_ = 42;       // T
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, ActivateAuthorityRoundTrip) {
  lavik::meta::ActivateAuthority cmd;
  cmd.request_id_ = MakeRequestId(0x42);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 42;
  cmd.new_owner_ = "89abcdef0123456789abcdef0123456789abcdef";
  cmd.new_topology_epoch_ = 102;
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
static_assert(!HasBareTermField<lavik::meta::ActivateAuthority>);
static_assert(!HasNewTermField<lavik::meta::ActivateAuthority>);
static_assert(requires(lavik::meta::ActivateAuthority t) { t.expected_term_; });

TEST(MetaModelCommands, FenceGroupRoundTrip) {
  lavik::meta::FenceGroup cmd;
  cmd.request_id_ = MakeRequestId(0x44);
  cmd.group_id_ = "0123456789abcdef0123456789abcdef01234567";
  cmd.expected_term_ = 42;
  cmd.new_term_ = 43;
  ExpectRoundTrip(cmd);
}

lavik::meta::MetaOperationId MakeOperationId(std::uint8_t seed);

lavik::meta::MetaFailoverCandidate MakeFailoverCandidate(std::uint8_t seed) {
  lavik::meta::MetaFailoverCandidate candidate;
  candidate.node_id_ = seed == 1 ? "0123456789abcdef0123456789abcdef01234567"
                                 : "89abcdef0123456789abcdef0123456789abcdef";
  candidate.assignment_id_.fill(static_cast<std::uint8_t>(seed + 0x10));
  candidate.boot_id_.fill(static_cast<std::uint8_t>(seed + 0x20));
  return candidate;
}

lavik::meta::MetaFailoverCandidateAction MakeFailoverAction(
    std::uint8_t seed, bool authorized = false) {
  lavik::meta::MetaFailoverCandidateAction action;
  action.action_id_.fill(static_cast<std::uint8_t>(seed + 0x30));
  action.candidate_ = MakeFailoverCandidate(seed);
  action.domain_.source_group_term_ = 41;
  action.domain_.source_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  action.domain_.source_assignment_id_.fill(0x41);
  action.domain_.source_boot_id_.fill(0x42);
  action.domain_.source_history_id_.fill(0x43);
  action.domain_.flow_count_ = 3;
  if (authorized) {
    action.authorization_ = lavik::meta::MetaFailoverAuthorization{
        .authorized_revision_ = 88,
        .loss_if_cutover_ = lavik::meta::MetaFailoverLoss::kNone};
  }
  return action;
}

template <typename Command>
void SetFailoverGroupAnchors(Command& command) {
  command.expected_owner_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  command.expected_owner_assignment_id_.fill(0x41);
  command.expected_membership_revision_ = 7;
  command.expected_group_term_ = 41;
  command.expected_population_manifest_revision_ = 9;
  command.expected_population_manifest_digest_.fill(0x44);
  command.expected_partition_replication_epoch_ = 11;
}

template <typename Command>
void ExpectFailoverCommandRoundTrip(const Command& command,
                                    lavik::meta::MetaCommandTag expected_tag) {
  ExpectRoundTrip(command);
  const std::string bytes = MustEncode(command);
  ASSERT_GE(bytes.size(), 4u);
  const auto* raw = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(raw[0] | (raw[1] << 8)),
            lavik::meta::kMetaCommandFormatVersion);
  EXPECT_EQ(static_cast<std::uint16_t>(raw[2] | (raw[3] << 8)),
            static_cast<std::uint16_t>(expected_tag));
}

TEST(MetaModelCommands, FailoverTransitionValueRoundTrips) {
  lavik::meta::MetaFailoverTransition transition;
  transition.transition_id_.fill(0x51);
  transition.revision_ = 88;
  transition.mode_ = lavik::meta::MetaFailoverMode::kControlled;
  transition.target_term_ = 42;
  transition.candidate_action_ = MakeFailoverAction(2, true);
  transition.controlled_ = lavik::meta::MetaControlledFailover{
      .operation_id_ = MakeOperationId(0x31),
      .absolute_deadline_unix_ms_ = 1'800'000'000'000};

  lavik::meta::MetaWriter writer;
  ASSERT_TRUE(
      lavik::meta::WriteMetaFailoverTransition(writer, transition).ok());
  lavik::meta::MetaReader reader(writer.buffer());
  const auto decoded = lavik::meta::ReadMetaFailoverTransition(reader);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, transition);
  EXPECT_TRUE(reader.Finish().ok());

  transition.mode_ = lavik::meta::MetaFailoverMode::kUncontrolled;
  transition.candidate_action_.reset();
  transition.controlled_.reset();
  lavik::meta::MetaWriter candidate_null_writer;
  ASSERT_TRUE(lavik::meta::WriteMetaFailoverTransition(candidate_null_writer,
                                                       transition)
                  .ok());
  lavik::meta::MetaReader candidate_null_reader(candidate_null_writer.buffer());
  const auto candidate_null =
      lavik::meta::ReadMetaFailoverTransition(candidate_null_reader);
  ASSERT_TRUE(candidate_null.ok()) << candidate_null.status();
  EXPECT_EQ(*candidate_null, transition);
  EXPECT_TRUE(candidate_null_reader.Finish().ok());
}

TEST(MetaModelCommands, TypedFailoverCommandsRoundTrip) {
  lavik::meta::BeginControlledFailover controlled;
  controlled.request_id_ = MakeRequestId(0x91);
  controlled.group_id_ = "group-a";
  controlled.transition_id_.fill(0x51);
  controlled.target_term_ = 42;
  controlled.candidate_action_ = MakeFailoverAction(2);
  controlled.operation_id_ = MakeOperationId(0x31);
  controlled.expected_operation_revision_ = 0;
  controlled.absolute_deadline_unix_ms_ = 1'800'000'000'000;
  SetFailoverGroupAnchors(controlled);
  ExpectFailoverCommandRoundTrip(
      controlled, lavik::meta::MetaCommandTag::kBeginControlledFailover);

  lavik::meta::BeginUncontrolledFailover uncontrolled;
  uncontrolled.request_id_ = MakeRequestId(0x92);
  uncontrolled.group_id_ = "group-a";
  uncontrolled.transition_id_.fill(0x52);
  uncontrolled.target_term_ = 42;
  uncontrolled.candidate_action_ = std::nullopt;
  uncontrolled.trigger_reason_ =
      lavik::meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  uncontrolled.suspect_duration_ms_ = 5000;
  uncontrolled.preempted_operation_id_ = MakeOperationId(0x32);
  uncontrolled.expected_preempted_operation_revision_ = 0;
  SetFailoverGroupAnchors(uncontrolled);
  // A valid group may not have installed a population manifest or advanced
  // its partition-replication epoch yet. The command still carries exact CAS
  // anchors for those zero values.
  uncontrolled.expected_population_manifest_revision_ = 0;
  uncontrolled.expected_population_manifest_digest_ = {};
  uncontrolled.expected_partition_replication_epoch_ = 0;
  ExpectFailoverCommandRoundTrip(
      uncontrolled, lavik::meta::MetaCommandTag::kBeginUncontrolledFailover);

  lavik::meta::BeginUncontrolledFailover manual_uncontrolled = uncontrolled;
  manual_uncontrolled.request_id_ = MakeRequestId(0x99);
  manual_uncontrolled.transition_id_.fill(0x59);
  manual_uncontrolled.candidate_action_ = MakeFailoverAction(2);
  manual_uncontrolled.trigger_reason_ =
      lavik::meta::MetaAutomaticFailoverReason::kManual;
  manual_uncontrolled.suspect_duration_ms_ = 0;
  manual_uncontrolled.preempted_operation_id_.reset();
  manual_uncontrolled.expected_preempted_operation_revision_.reset();
  ExpectFailoverCommandRoundTrip(
      manual_uncontrolled,
      lavik::meta::MetaCommandTag::kBeginUncontrolledFailover);

  lavik::meta::SetUncontrolledCandidate set_candidate;
  set_candidate.request_id_ = MakeRequestId(0x93);
  set_candidate.group_id_ = "group-a";
  set_candidate.expected_transition_ = {{}, 88};
  set_candidate.expected_transition_.transition_id_.fill(0x52);
  set_candidate.candidate_action_ = MakeFailoverAction(2);
  ExpectFailoverCommandRoundTrip(
      set_candidate, lavik::meta::MetaCommandTag::kSetUncontrolledCandidate);

  lavik::meta::AuthorizeFailoverPrepare authorize;
  authorize.request_id_ = MakeRequestId(0x94);
  authorize.group_id_ = "group-a";
  authorize.expected_transition_ = set_candidate.expected_transition_;
  authorize.action_id_ = set_candidate.candidate_action_->action_id_;
  authorize.loss_if_cutover_ = lavik::meta::MetaFailoverLoss::kUnknown;
  ExpectFailoverCommandRoundTrip(
      authorize, lavik::meta::MetaCommandTag::kAuthorizeFailoverPrepare);

  lavik::meta::AbortControlledFailover abort;
  abort.request_id_ = MakeRequestId(0x95);
  abort.operation_id_ = controlled.operation_id_;
  abort.expected_operation_revision_ = 0;
  abort.group_id_ = "group-a";
  abort.expected_transition_ = std::nullopt;
  abort.reason_ = "no eligible candidate before deadline";
  ExpectFailoverCommandRoundTrip(
      abort, lavik::meta::MetaCommandTag::kAbortControlledFailover);

  lavik::meta::DegradeControlledFailover degrade;
  degrade.request_id_ = MakeRequestId(0x96);
  degrade.operation_id_ = controlled.operation_id_;
  degrade.expected_operation_revision_ = 0;
  degrade.group_id_ = "group-a";
  degrade.expected_transition_ = set_candidate.expected_transition_;
  degrade.expected_candidate_action_ = MakeFailoverAction(2, true);
  degrade.retain_candidate_action_ = true;
  degrade.reason_ = "source incarnation was replaced";
  ExpectFailoverCommandRoundTrip(
      degrade, lavik::meta::MetaCommandTag::kDegradeControlledFailover);

  lavik::meta::CommitControlledFailover commit_controlled;
  commit_controlled.request_id_ = MakeRequestId(0x97);
  commit_controlled.operation_id_ = controlled.operation_id_;
  commit_controlled.expected_operation_revision_ = 0;
  commit_controlled.group_id_ = "group-a";
  commit_controlled.expected_transition_ = set_candidate.expected_transition_;
  commit_controlled.action_id_ = degrade.expected_candidate_action_->action_id_;
  commit_controlled.authorized_revision_ = 88;
  commit_controlled.expected_candidate_ =
      degrade.expected_candidate_action_->candidate_;
  SetFailoverGroupAnchors(commit_controlled);
  commit_controlled.new_topology_epoch_ = 101;
  ExpectFailoverCommandRoundTrip(
      commit_controlled,
      lavik::meta::MetaCommandTag::kCommitControlledFailover);

  lavik::meta::CommitUncontrolledFailover commit_uncontrolled;
  commit_uncontrolled.request_id_ = MakeRequestId(0x98);
  commit_uncontrolled.group_id_ = "group-a";
  commit_uncontrolled.expected_transition_ = set_candidate.expected_transition_;
  commit_uncontrolled.action_id_ =
      degrade.expected_candidate_action_->action_id_;
  commit_uncontrolled.authorized_revision_ = 88;
  commit_uncontrolled.loss_if_cutover_ = lavik::meta::MetaFailoverLoss::kNone;
  commit_uncontrolled.expected_candidate_ =
      degrade.expected_candidate_action_->candidate_;
  SetFailoverGroupAnchors(commit_uncontrolled);
  commit_uncontrolled.expected_group_term_ = 42;
  commit_uncontrolled.new_topology_epoch_ = 101;
  ExpectFailoverCommandRoundTrip(
      commit_uncontrolled,
      lavik::meta::MetaCommandTag::kCommitUncontrolledFailover);
}

TEST(MetaModelCommands, FailoverCodecRejectsInvalidValues) {
  lavik::meta::BeginUncontrolledFailover automatic;
  automatic.group_id_ = "group-a";
  automatic.transition_id_.fill(0x51);
  automatic.target_term_ = 42;
  automatic.trigger_reason_ =
      lavik::meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  automatic.suspect_duration_ms_ = 5000;
  SetFailoverGroupAnchors(automatic);

  automatic.candidate_action_ = MakeFailoverAction(2);
  ExpectEncodeDomainReject(automatic);
  automatic.candidate_action_.reset();

  automatic.preempted_operation_id_ = MakeOperationId(0x33);
  ExpectEncodeDomainReject(automatic);

  automatic.preempted_operation_id_.reset();
  automatic.expected_preempted_operation_revision_ = 0;
  ExpectEncodeDomainReject(automatic);

  automatic.preempted_operation_id_ = MakeOperationId(0x33);
  automatic.trigger_reason_ = lavik::meta::MetaAutomaticFailoverReason::kManual;
  automatic.suspect_duration_ms_ = 0;
  ExpectEncodeDomainReject(automatic);

  automatic.trigger_reason_ =
      lavik::meta::MetaAutomaticFailoverReason::kHeartbeatExpired;
  automatic.suspect_duration_ms_ = 5000;
  automatic.preempted_operation_id_ = lavik::meta::MetaOperationId{};
  ExpectEncodeDomainReject(automatic);

  lavik::meta::SetUncontrolledCandidate set_candidate;
  set_candidate.group_id_ = "group-a";
  set_candidate.expected_transition_.transition_id_.fill(0x52);
  set_candidate.expected_transition_.revision_ = 88;
  set_candidate.candidate_action_ = MakeFailoverAction(2, true);
  ExpectEncodeDomainReject(set_candidate);

  lavik::meta::AuthorizeFailoverPrepare authorize;
  authorize.group_id_ = "group-a";
  authorize.expected_transition_ = set_candidate.expected_transition_;
  authorize.action_id_.fill(0x71);
  authorize.loss_if_cutover_ = lavik::meta::MetaFailoverLoss::kUnknown;
  std::string encoded = MustEncode(authorize);
  encoded.back() = static_cast<char>(0x7f);
  ExpectDecodeFailStop(encoded);
}

// ---------------------------------------------------------------------------
// Policy documents are versioned raw content.
// ---------------------------------------------------------------------------

lavik::meta::MetaHash256 MakeHash(std::uint8_t seed) {
  lavik::meta::MetaHash256 h{};
  for (std::size_t i = 0; i < h.size(); ++i) {
    h[i] = static_cast<std::uint8_t>(seed ^ i);
  }
  return h;
}

TEST(MetaModelCommands, PutPolicyRoundTrip) {
  lavik::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0x50);
  cmd.policy_id_ = "migration-policy";
  cmd.version_ = 3;
  cmd.content_ = "{\"phases\":[\"prepare\",\"move\",\"cutover\"]}";
  ExpectRoundTrip(cmd);
}

// ---------------------------------------------------------------------------
// operation journal. operation_id is the client-provided stable UUID and
// idempotency key while its live/archive record is retained; operation_seq is
// the raft log index of the SubmitOperation command, assigned by apply, and
// appears in commands only as an archive reference.
// ---------------------------------------------------------------------------

lavik::meta::MetaOperationId MakeOperationId(std::uint8_t seed) {
  lavik::meta::MetaOperationId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed * 3 + i);
  }
  return id;
}

TEST(MetaModelCommands, SubmitOperationRoundTrip) {
  lavik::meta::SubmitOperation cmd;
  cmd.request_id_ = MakeRequestId(0x60);
  cmd.operation_id_ = MakeOperationId(0x01);
  cmd.kind_ = "migration";
  cmd.intent_ = "{\"slot\":42,\"to\":\"group-b\"}";
  cmd.intent_hash_ = MakeHash(0x11);
  cmd.replication_history_id_.fill(0x12);
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, TransitionOperationPhaseRoundTrip) {
  lavik::meta::TransitionOperationPhase cmd;
  cmd.request_id_ = MakeRequestId(0x61);
  cmd.operation_id_ = MakeOperationId(0x02);
  cmd.expected_revision_ = 3;
  cmd.kind_phase_blob_ = "{\"phase\":\"cutover\"}";
  lavik::meta::MetaDirectiveSpec directive;
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
  directive.population_manifest_revision_ = 777;
  directive.population_manifest_digest_ = MakeHash(0x16);
  directive.partition_replication_epoch_ = 778;
  directive.kind_ = "rebuild";
  directive.payload_ = "{\"partition\":5}";

  cmd.current_directives_ = {directive};
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, CompleteOperationRoundTrip) {
  lavik::meta::CompleteOperation cmd;
  cmd.request_id_ = MakeRequestId(0x62);
  cmd.operation_id_ = MakeOperationId(0x03);
  cmd.expected_revision_ = 4;
  cmd.result_ = "{\"moved_slots\":100}";
  cmd.data_loss_possible_ = true;
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, AbortOperationRoundTrip) {
  lavik::meta::AbortOperation cmd;
  cmd.request_id_ = MakeRequestId(0x63);
  cmd.operation_id_ = MakeOperationId(0x04);
  cmd.expected_revision_ = 2;
  cmd.reason_ = "target group fenced";
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, ArchiveOperationsRoundTrip) {
  // Non-contiguous archival of terminal operations; apply
  // rejects references to non-terminal or unknown operations.
  lavik::meta::ArchiveOperations cmd;
  cmd.request_id_ = MakeRequestId(0x64);
  cmd.operation_seqs_ = {100, 137, 4096};
  ExpectRoundTrip(cmd);
}

TEST(MetaModelCommands, DirectiveResultReceiptCommandsRoundTrip) {
  lavik::meta::CommitDirectiveResult commit;
  commit.request_id_ = MakeRequestId(0x65);
  commit.operation_id_ = MakeOperationId(0x05);
  commit.directive_id_.fill(0x20);
  commit.attempt_id_.fill(0x21);
  commit.directive_revision_ = 88;
  commit.recipient_node_id_ = "0123456789abcdef0123456789abcdef01234567";
  commit.recipient_boot_id_.fill(0x22);
  commit.assignment_id_.fill(0x23);
  commit.status_ = lavik::meta::MetaDirectiveResultStatus::kFailed;
  commit.result_ = "source rejected the replication handshake";
  ExpectRoundTrip(commit);

  lavik::meta::PruneTerminalReceipts prune;
  prune.request_id_ = MakeRequestId(0x66);
  prune.receipts_ = {{commit.operation_id_, commit.directive_id_,
                      commit.attempt_id_, commit.directive_revision_}};
  ExpectRoundTrip(prune);
}

TEST(MetaModelCommands, AdministrativeCommandsRoundTrip) {
  EXPECT_EQ(lavik::meta::kMetaFormatVersion, 1);
  EXPECT_EQ(lavik::meta::kMetaCommandFormatVersion, 1);
  // Removed command tags are permanent holes: 9 was GrantAuthority and 14
  // was RetirePolicy. Later tags must never shift into those values.
  EXPECT_EQ(static_cast<std::uint16_t>(
                lavik::meta::MetaCommandTag::kActivateAuthority),
            10);
  EXPECT_EQ(static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kPutPolicy),
            13);
  EXPECT_EQ(
      static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kSubmitOperation),
      15);

  lavik::meta::PruneAudit audit;
  audit.through_log_index_ = 42;
  ExpectRoundTrip(audit);

  lavik::meta::PruneOperationArchive operations;
  operations.operation_seqs_ = {3, 8, 13};
  ExpectRoundTrip(operations);

  lavik::meta::BindMetaMember bind;
  bind.server_id_ = 7;
  bind.principal_ = "lavik://meta/7";
  bind.data_control_endpoint_ = "10.0.0.7:7100";
  bind.ctl_endpoint_ = "10.0.0.7:7200";
  ExpectRoundTrip(bind);

  lavik::meta::RetireMetaMember retire;
  retire.server_id_ = 7;
  ExpectRoundTrip(retire);
}

TEST(MetaModelCommands, SetAuditPolicyRoundTrips) {
  lavik::meta::SetAuditPolicy command;
  command.request_id_ = MakeRequestId(0x74);
  command.policy_ = lavik::meta::MetaAuditPolicy::kStrictExport;
  command.attestation_ = "ticket OPS-4321";
  ExpectRoundTrip(command);
}

TEST(MetaModelCommands, SetGroupReplicationStateRoundTrip) {
  lavik::meta::SetGroupReplicationState command;
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
  lavik::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(0x80);
  cmd.policy_id_ = "migration-policy";
  cmd.version_ = 1;
  cmd.content_ = std::string(lavik::meta::kMaxMetaPayloadBytes + 1, 'x');
  ExpectEncodeDomainReject(cmd);
  // Exactly at the cap it still encodes (bounds are inclusive).
  cmd.content_.resize(lavik::meta::kMaxMetaPayloadBytes);
  EXPECT_TRUE(EncodeMetaCommand(cmd).ok());
}

TEST(MetaModelCommands, EncodeRejectsOverCapListsAndFields) {
  lavik::meta::RegisterNode reg = MakeRegisterNode();
  reg.endpoints_.resize(lavik::meta::kMaxMetaEndpointsPerNode + 1, "e");
  ExpectEncodeDomainReject(reg);

  lavik::meta::RegisterNode bad_id = MakeRegisterNode();
  bad_id.node_id_ = std::string(lavik::meta::kMetaNodeIdBytes + 1, 'a');
  ExpectEncodeDomainReject(bad_id);

  lavik::meta::ArchiveOperations arch;
  arch.request_id_ = MakeRequestId(0x81);
  arch.operation_seqs_.resize(lavik::meta::kMaxMetaActiveOperations + 1, 1);
  ExpectEncodeDomainReject(arch);
}

TEST(MetaModelCommands, EncodeRejectsOutOfRangeSlotRange) {
  lavik::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x82);
  cmd.ranges_ = {{100, 99, "g"}};  // first > last
  ExpectEncodeDomainReject(cmd);
  cmd.ranges_ = {{0, lavik::meta::kMetaSlotCount, "g"}};  // slot out of range
  ExpectEncodeDomainReject(cmd);
}

TEST(MetaModelCommands, EncodeRejectsOversizedCommand) {
  // 16384 maximally-sized slot assignments exceed the 1 MiB command cap.
  lavik::meta::SetSlotMap cmd;
  cmd.request_id_ = MakeRequestId(0x83);
  const std::string group_id(lavik::meta::kMaxMetaGroupIdBytes, 'g');
  for (std::uint32_t i = 0; i < lavik::meta::kMaxMetaSlotRangeCount; ++i) {
    cmd.ranges_.push_back({0, 0, group_id});
  }
  ExpectEncodeDomainReject(cmd);
}

TEST(MetaModelCommands, DecodeRejectsOverCapLengthPrefix) {
  // Hand-build a PutPolicy whose content length prefix exceeds the payload
  // cap; the reader must reject on the cap before even looking for the body.
  lavik::meta::MetaWriter w;
  w.WriteU16(lavik::meta::kMetaCommandFormatVersion);
  w.WriteU16(
      static_cast<std::uint16_t>(lavik::meta::MetaCommandTag::kPutPolicy));
  w.WriteRaw(std::string(16, '\0'));  // request_id
  w.WriteString("migration-policy");
  w.WriteU64(1);
  w.WriteU32(lavik::meta::kMaxMetaPayloadBytes + 1);  // content prefix
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
    lavik::meta::CompleteOperation cmd;
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
  bytes.resize(lavik::meta::kMaxMetaCommandBytes + 1, '\0');
  ExpectDecodeFailStop(bytes);
}

// ---------------------------------------------------------------------------
// Apply layer (src/meta/state_apply.cpp): the ApplyCommitted dispatcher.
// Tests drive the public surface only: MetaStores + ApplyCommitted with
// caller-injected actor fields, plus the whole-aggregate snapshot codec.
// ---------------------------------------------------------------------------

using lavik::meta::ApplyCommitted;
using lavik::meta::MetaApplyResult;
using lavik::meta::MetaAuditVerdict;
using lavik::meta::MetaStores;

constexpr std::string_view kActorPrincipal = "lavik://operator/alice";
constexpr std::string_view kReadableTime = "2026-09-04T00:00:00Z";

// 40 lowercase hex chars, distinct per n (the data-plane node_id convention).
std::string MakeNodeId(std::uint32_t n) {
  std::string id(40, '0');
  for (int i = 39; n > 0 && i >= 0; --i, n >>= 4) {
    id[i] = "0123456789abcdef"[n & 0xF];
  }
  return id;
}

lavik::meta::RegisterNode MakeRegisterFor(std::uint32_t n) {
  lavik::meta::RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(static_cast<std::uint8_t>(n));
  cmd.node_id_ = MakeNodeId(n);
  cmd.principal_ = "lavik://node/" + MakeNodeId(n);
  cmd.endpoints_ = {"10.0.0.1:7000"};

  cmd.role_ = lavik::meta::MetaNodeRole::kPrimary;
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
  EXPECT_EQ(lavik::meta::MetaFailureClassOf(restored.status()),
            lavik::meta::MetaFailureClass::kFailStop);
}

// The committed DOMAIN state (everything but the audit window): a rejected
// command must leave this unchanged, while the audit trail still grows by the
// rejection record; the index is consumed and state otherwise remains
// unchanged.
std::string DomainStateBytes(const MetaStores& stores) {
  std::string out = stores.identity_.Serialize();
  out += stores.topology_.Serialize();
  out += stores.policy_.Serialize();
  out += stores.operation_.Serialize().value_or("!");
  out += stores.population_manifest_.Serialize();
  return out;
}

TEST(MetaStateApply, RegisterNodeAcceptedAndAudited) {
  MetaStores stores;
  const lavik::meta::RegisterNode cmd = MakeRegisterFor(1);
  const MetaApplyResult result = ApplyOk(stores, 1, cmd);
  EXPECT_EQ(result.command_tag_, lavik::meta::MetaCommandTag::kRegisterNode);
  EXPECT_TRUE(stores.identity_.IsActiveNode(cmd.node_id_));

  // Every privileged command appends exactly one audit record keyed by its
  // raft log index; the injected actor fields are copied verbatim.
  ASSERT_EQ(stores.audit_.size(), 1u);
  const auto entry = stores.audit_.Find(1);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->log_index_, 1u);
  EXPECT_EQ(entry->actor_principal_, kActorPrincipal);
  EXPECT_EQ(entry->readable_time_, kReadableTime);
  EXPECT_EQ(entry->verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_TRUE(entry->verdict_detail_.empty());
  EXPECT_NE(entry->command_summary_.find(cmd.node_id_), std::string::npos);
}

TEST(MetaStateApply, ReplaySameIndexProducesSameVerdictStateAndAudit) {
  MetaStores stores;
  const lavik::meta::RegisterNode cmd = MakeRegisterFor(1);
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
  EXPECT_EQ(*stores.audit_.Find(1), *record_after_first);
}

TEST(MetaStateApply, RejectedCommandIsAuditedAndLeavesStateUnchanged) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  // Same principal bound to a second node_id: domain rejection because the
  // binding is globally one-to-one.
  lavik::meta::RegisterNode conflict = MakeRegisterFor(2);
  conflict.principal_ = "lavik://node/" + MakeNodeId(1);
  const std::string domain_before = DomainStateBytes(stores);
  const MetaApplyResult result = ApplyRejected(stores, 2, conflict);
  EXPECT_EQ(result.command_tag_, lavik::meta::MetaCommandTag::kRegisterNode);

  // Domain state is unchanged; the audit window still grew by the rejection
  // record (the index is consumed).
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  ASSERT_EQ(stores.audit_.size(), 2u);
  const auto entry = stores.audit_.Find(2);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->verdict_, MetaAuditVerdict::kRejected);
  EXPECT_EQ(entry->verdict_detail_, result.detail_);
  EXPECT_EQ(entry->actor_principal_, kActorPrincipal);
  // The rejection consumes the log index: the chain advanced over both
  // records.
}

TEST(MetaStateApply, UpdateAndRetireNodeThroughDispatcher) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  const std::string node_id = MakeNodeId(1);

  lavik::meta::UpdateNode update;
  update.request_id_ = MakeRequestId(0x21);
  update.node_id_ = node_id;
  update.expected_revision_ = 1;
  update.endpoints_ = {"10.0.0.9:7000"};

  update.new_topology_epoch_ = 1;
  ApplyOk(stores, 2, update);
  EXPECT_EQ(stores.identity_.FindNode(node_id)->revision_, 2u);

  // Re-applying the identical command is a replay: idempotent accept.
  ApplyOk(stores, 2, update);

  // CAS conflict: a DIFFERENT update carrying the stale expected_revision is
  // a domain rejection.
  lavik::meta::UpdateNode stale = update;
  stale.request_id_ = MakeRequestId(0x23);
  stale.endpoints_ = {"10.0.0.10:7000"};
  ApplyRejected(stores, 3, stale);
  EXPECT_EQ(stores.identity_.FindNode(node_id)->revision_, 2u);

  lavik::meta::RetireNode retire;
  retire.request_id_ = MakeRequestId(0x22);
  retire.node_id_ = node_id;
  retire.expected_revision_ = 2;
  ApplyOk(stores, 4, retire);
  EXPECT_FALSE(stores.identity_.IsActiveNode(node_id));

  // Retired is terminal: re-registering the same node_id is rejected.
  ApplyRejected(stores, 5, MakeRegisterFor(1));
  ASSERT_EQ(stores.audit_.size(), 5u);
}

TEST(MetaStateApply, CreateGroupOwnsInitiallyFencedAuthority) {
  MetaStores stores;
  lavik::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = "g1";
  cmd.new_topology_epoch_ = 1;
  ApplyOk(stores, 1, cmd);

  EXPECT_TRUE(stores.topology_.GroupExists("g1"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 1u);
  // The grant half of the group exists too (fenced, grantless): grant
  // commands operate on groups known to both stores.
  const auto grant_state = stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_FALSE(grant_state->grant_.has_value());
  EXPECT_EQ(grant_state->group_term_, 0u);

  // Replay: idempotent accept, both halves unchanged, no new audit record.
  ApplyOk(stores, 1, cmd);
  EXPECT_EQ(stores.topology_.GroupCount(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);
}

TEST(MetaStateApply, CreateGroupEpochMustBeExactlyNext) {
  MetaStores stores;
  lavik::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = "g1";
  cmd.new_topology_epoch_ = 7;  // not current(0)+1
  ApplyRejected(stores, 1, cmd);
  EXPECT_FALSE(stores.topology_.GroupExists("g1"));
  EXPECT_FALSE(stores.topology_.AuthorityFor("g1").has_value());
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 0u);
}

TEST(MetaStateApply, MetaStoresSnapshotRoundTrip) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  lavik::meta::CreateGroup group;
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
  EXPECT_TRUE(restored->topology_.AuthorityFor("g1").has_value());
  EXPECT_EQ(restored->audit_.size(), 2u);
  const auto* envelope = reinterpret_cast<const unsigned char*>(bytes.data());
  EXPECT_EQ(static_cast<std::uint16_t>(envelope[0] | (envelope[1] << 8)), 1);
  EXPECT_EQ(static_cast<std::uint16_t>(envelope[0] | (envelope[1] << 8)),
            lavik::meta::kMetaFormatVersion);
}

TEST(MetaStateApply, MetaStoresDeserializeRejectsCorruption) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  const std::string bytes = MustSerialize(stores);

  for (std::size_t len : {std::size_t{0}, std::size_t{1}, bytes.size() - 1}) {
    const auto decoded =
        MetaStores::Deserialize(std::string_view(bytes).substr(0, len));
    ASSERT_FALSE(decoded.ok()) << "len=" << len;
    EXPECT_EQ(lavik::meta::MetaFailureClassOf(decoded.status()),
              lavik::meta::MetaFailureClass::kFailStop);
  }
  const auto trailing = MetaStores::Deserialize(bytes + '\0');
  ASSERT_FALSE(trailing.ok());
  EXPECT_EQ(lavik::meta::MetaFailureClassOf(trailing.status()),
            lavik::meta::MetaFailureClass::kFailStop);

  for (const char version : {'\x02', '\x03'}) {
    std::string future = bytes;
    future[0] = version;
    future[1] = '\0';
    const auto unsupported = MetaStores::Deserialize(future);
    ASSERT_FALSE(unsupported.ok());
    EXPECT_EQ(lavik::meta::MetaFailureClassOf(unsupported.status()),
              lavik::meta::MetaFailureClass::kFailStop);
    EXPECT_NE(unsupported.status().message().find("version"),
              std::string_view::npos);
  }
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
  const lavik::meta::RegisterNode cmd = MakeRegisterFor(1);
  const std::string huge_principal(lavik::meta::kMaxMetaPrincipalBytes + 1,
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

lavik::meta::PutPolicy MakePutPolicy(const std::string& policy_id,
                                     std::uint64_t version,
                                     const std::string& content) {
  lavik::meta::PutPolicy cmd;
  cmd.request_id_ = MakeRequestId(static_cast<std::uint8_t>(0x50 + version));
  cmd.policy_id_ = policy_id;
  cmd.version_ = version;
  cmd.content_ = content;
  return cmd;
}

lavik::meta::CreateGroup MakeCreateGroup(const std::string& group_id,
                                         std::uint64_t topology_epoch) {
  lavik::meta::CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x30);
  cmd.group_id_ = group_id;
  cmd.new_topology_epoch_ = topology_epoch;
  return cmd;
}

lavik::meta::AssignNodeToGroup MakeAssign(const std::string& group_id,
                                          std::uint32_t node,
                                          std::uint64_t expected_revision,
                                          std::uint64_t topology_epoch = 0) {
  lavik::meta::AssignNodeToGroup cmd;
  cmd.request_id_ = MakeRequestId(0x31);
  cmd.group_id_ = group_id;
  cmd.node_id_ = MakeNodeId(node);
  cmd.assignment_id_.fill(static_cast<std::uint8_t>(node));
  cmd.role_ = lavik::meta::MetaNodeRole::kPrimary;
  cmd.expected_revision_ = expected_revision;
  cmd.new_topology_epoch_ =
      topology_epoch == 0 ? expected_revision + 1 : topology_epoch;
  return cmd;
}

// A fully valid ActivateAuthority against the state built by the fixture
// helpers: group with term 1 begun and owner a member.
lavik::meta::ActivateAuthority MakeActivate(const std::string& group_id,
                                            std::uint64_t expected_term,
                                            std::uint32_t owner_node,
                                            std::uint64_t topology_epoch) {
  lavik::meta::ActivateAuthority cmd;
  cmd.request_id_ = MakeRequestId(0x42);
  cmd.group_id_ = group_id;
  cmd.expected_term_ = expected_term;
  cmd.new_owner_ = MakeNodeId(owner_node);
  cmd.new_topology_epoch_ = topology_epoch;
  return cmd;
}

// Registers node, creates group (topology_epoch 1), assigns the node
// (membership revision 1 -> 2, topology_epoch 2), commits Authority Lease
// Policy version 1, and begins group term 1.
// Consumes log indexes 1..5.
void SetupActivatedGroupPrerequisites(MetaStores& stores, std::uint32_t node,
                                      const std::string& group_id) {
  ApplyOk(stores, 1, MakeRegisterFor(node));
  ApplyOk(stores, 2, MakeCreateGroup(group_id, 1));
  ApplyOk(stores, 3, MakeAssign(group_id, node, 1));
  ApplyOk(
      stores, 4,
      MakePutPolicy(std::string(lavik::meta::kAuthorityLeasePolicyId), 1,
                    "{\"kind\":\"authority-lease-v1\",\"duration_ms\":5000}"));
  lavik::meta::BeginGroupTerm begin;
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
  lavik::meta::RetireNode retire;
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

  lavik::meta::RetireNode retire;
  retire.request_id_ = MakeRequestId(0x22);
  retire.node_id_ = MakeNodeId(1);
  retire.expected_revision_ = 1;
  // The node still holds group membership: retire is a cross-store rejection.
  ApplyRejected(stores, 6, retire);
  EXPECT_TRUE(stores.identity_.IsActiveNode(MakeNodeId(1)));

  // Remove the membership first, then retire succeeds.
  lavik::meta::RemoveNodeFromGroup remove;
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
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 3)});

  lavik::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = MakeRequestId(0x32);
  remove.group_id_ = "g1";
  remove.node_id_ = MakeNodeId(1);
  remove.expected_revision_ = 2;
  remove.new_topology_epoch_ = 4;

  // The node owns the group's active grant: removing it would strand the
  // authority fact. Reject; fence into a new term first.
  ApplyRejected(stores, 7, remove);
  EXPECT_TRUE(stores.topology_.FindGroup("g1")->members_.size() == 1u);

  lavik::meta::FenceGroup fence;
  fence.request_id_ = MakeRequestId(0x44);
  fence.group_id_ = "g1";
  fence.expected_term_ = 1;
  fence.new_term_ = 2;
  ApplyOk(stores, 8, fence);
  ApplyOk(stores, 9, remove);
  EXPECT_TRUE(stores.topology_.FindGroup("g1")->members_.empty());
}

TEST(MetaStateApply, BeginGroupTermRaisesTermInBothStores) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 3)});

  lavik::meta::BeginGroupTerm begin;
  begin.request_id_ = MakeRequestId(0x41);
  begin.group_id_ = "g1";
  begin.expected_term_ = 1;
  begin.new_term_ = 2;
  ApplyOk(stores, 7, begin);

  // Grant half: term 2 and grantless. Topology half: the committed
  // GroupRecord carries the same term.
  const auto grant_state = stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_EQ(grant_state->group_term_, 2u);
  EXPECT_FALSE(grant_state->grant_.has_value());
  EXPECT_EQ(stores.topology_.FindGroup("g1")->record_.group_term_, 2u);

  // Replay at the same index: idempotent accept, no state movement, audit
  // window unchanged.
  const std::string domain_before = DomainStateBytes(stores);
  ApplyOk(stores, 7, begin);
  EXPECT_EQ(DomainStateBytes(stores), domain_before);
  EXPECT_EQ(stores.audit_.size(), 7u);

  // A stale-term activation is now rejected on both paths: the term moved on.
  ApplyRejected(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 4)});
}

TEST(MetaStateApply, GroupReplicationStateAdvancesWithTopologyEpoch) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
  lavik::meta::PutPopulationManifest put;
  put.entries_ = {{1, 1}};
  put.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
  ApplyOk(stores, 2, put);
  lavik::meta::SetGroupReplicationState update;
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

  lavik::meta::SetGroupReplicationState stale = update;
  stale.request_id_ = MakeRequestId(0x7a);
  stale.new_population_manifest_revision_ = 2;
  stale.new_topology_epoch_ = 3;
  ApplyRejected(stores, 4, stale);
}

TEST(MetaStateApply, ManifestRevisionDistinguishesAtoBtoAAndGuardsPrune) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeCreateGroup("g1", 1));

  lavik::meta::PutPopulationManifest a;
  a.entries_ = {{1, 10}};
  a.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(a.entries_);
  lavik::meta::PutPopulationManifest b;
  b.entries_ = {{1, 11}};
  b.manifest_digest_ =
      lavik::meta::MetaPopulationManifestStore::CanonicalDigest(b.entries_);
  ApplyOk(stores, 2, a);
  ApplyOk(stores, 3, b);

  lavik::meta::SetGroupReplicationState set;
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

  lavik::meta::PrunePopulationManifest prune;
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

  lavik::meta::SetSlotMap slots;
  slots.request_id_ = MakeRequestId(0x33);
  slots.ranges_ = {{0, 9999, "g1"}, {10000, 16383, "g2"}};
  slots.new_topology_epoch_ = 4;
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
  lavik::meta::SetSlotMap bad = slots;
  bad.request_id_ = MakeRequestId(0x34);
  bad.new_topology_epoch_ = 5;
  bad.ranges_ = {{0, 1, "g-unknown"}};
  ApplyRejected(stores, 8, bad);
  EXPECT_EQ(stores.topology_.SlotOwner(0), std::optional<std::string>("g1"));
}

TEST(MetaStateApply, SetSlotMapCannotClearSlotsCoveredByAnActiveGrant) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  lavik::meta::SetSlotMap initial;
  initial.request_id_ = MakeRequestId(0x82);
  initial.ranges_ = {{0, 16383, "g1"}};
  initial.new_topology_epoch_ = 3;
  ApplyOk(stores, 6, MetaCommand{initial});
  ApplyOk(stores, 7, MetaCommand{MakeActivate("g1", 1, 1, 4)});

  lavik::meta::SetSlotMap clear;
  clear.request_id_ = MakeRequestId(0x83);
  clear.new_topology_epoch_ = 5;
  const std::string before = DomainStateBytes(stores);
  ApplyRejected(stores, 8, MetaCommand{clear});
  EXPECT_EQ(DomainStateBytes(stores), before);
  EXPECT_EQ(stores.topology_.SlotOwner(0), std::optional<std::string>("g1"));
}

TEST(MetaStateApply, SetSlotMapRequiresEveryAffectedGrantToBeFenced) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));
  ApplyOk(stores, 7, MakeCreateGroup("g2", 3));
  ApplyOk(stores, 8, MakeAssign("g2", 2, 1, 4));

  lavik::meta::BeginGroupTerm begin_g2;
  begin_g2.request_id_ = MakeRequestId(0x84);
  begin_g2.group_id_ = "g2";
  begin_g2.expected_term_ = 0;
  begin_g2.new_term_ = 1;
  ApplyOk(stores, 9, MetaCommand{begin_g2});

  lavik::meta::SetSlotMap initial;
  initial.request_id_ = MakeRequestId(0x85);
  initial.ranges_ = {{0, 8191, "g1"}, {8192, 16383, "g2"}};
  initial.new_topology_epoch_ = 5;
  ApplyOk(stores, 10, MetaCommand{initial});
  ApplyOk(stores, 11, MetaCommand{MakeActivate("g1", 1, 1, 6)});
  ApplyOk(stores, 12, MetaCommand{MakeActivate("g2", 1, 2, 7)});

  lavik::meta::SetSlotMap moved = initial;
  moved.request_id_ = MakeRequestId(0x86);
  moved.ranges_ = {{0, 4095, "g1"}, {4096, 16383, "g2"}};
  moved.new_topology_epoch_ = 8;
  const std::string before = DomainStateBytes(stores);
  MetaApplyResult source_live = ApplyRejected(stores, 13, MetaCommand{moved});
  EXPECT_NE(source_live.detail_.find("fenced"), std::string::npos);
  EXPECT_EQ(DomainStateBytes(stores), before);

  lavik::meta::FenceGroup fence_g1;
  fence_g1.request_id_ = MakeRequestId(0x87);
  fence_g1.group_id_ = "g1";
  fence_g1.expected_term_ = 1;
  fence_g1.new_term_ = 2;
  ApplyOk(stores, 14, MetaCommand{fence_g1});

  // Fencing only the source is insufficient: the destination's old lease was
  // issued for a different slot projection and must not span the cut.
  MetaApplyResult destination_live =
      ApplyRejected(stores, 15, MetaCommand{moved});
  EXPECT_NE(destination_live.detail_.find("g2"), std::string::npos);
  EXPECT_EQ(stores.topology_.SlotOwner(5000), std::optional<std::string>("g1"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 7u);

  lavik::meta::FenceGroup fence_g2;
  fence_g2.request_id_ = MakeRequestId(0x88);
  fence_g2.group_id_ = "g2";
  fence_g2.expected_term_ = 1;
  fence_g2.new_term_ = 2;
  ApplyOk(stores, 16, MetaCommand{fence_g2});
  ApplyOk(stores, 17, MetaCommand{moved});
  EXPECT_EQ(stores.topology_.SlotOwner(5000), std::optional<std::string>("g2"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 8u);
}

// ---------------------------------------------------------------------------
// ActivateAuthority: the atomic failover/migration commit point.
// ---------------------------------------------------------------------------

// Asserts the pre-activation state of both halves: the grant store is
// grantless at term 1 and the topology record is untouched.
void ExpectPreActivationState(const MetaStores& stores,
                              const std::string& group_id) {
  const auto grant_state = stores.topology_.AuthorityFor(group_id);
  ASSERT_TRUE(grant_state.has_value());
  EXPECT_EQ(grant_state->group_term_, 1u);
  EXPECT_FALSE(grant_state->grant_.has_value());
  const auto view = stores.topology_.FindGroup(group_id);
  ASSERT_TRUE(view.has_value());
  EXPECT_TRUE(view->record_.owner_.empty());
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 2u);
}

TEST(MetaStateApply, ActivateAuthorityRejectionLeavesBothHalvesUntouched) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));  // registered but not a member

  std::uint64_t index = 7;
  const auto expect_rejected_untouched =
      [&](lavik::meta::ActivateAuthority cmd) {
        ApplyRejected(stores, index, MetaCommand{std::move(cmd)});
        ExpectPreActivationState(stores, "g1");
        ++index;
      };

  // Wrong expected_term (grant store CAS).
  expect_rejected_untouched(MakeActivate("g1", 0, 1, 3));
  // topology_epoch not exactly current+1.
  expect_rejected_untouched(MakeActivate("g1", 1, 1, 5));
  // New owner not a member of the group.
  expect_rejected_untouched(MakeActivate("g1", 1, 2, 3));
  // New owner not registered at all.
  {
    lavik::meta::ActivateAuthority cmd = MakeActivate("g1", 1, 1, 3);
    cmd.new_owner_ = MakeNodeId(99);
    expect_rejected_untouched(std::move(cmd));
  }
}

TEST(MetaStateApply, ActivateAuthorityRequiresABegunNonzeroTerm) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeRegisterFor(1));
  ApplyOk(stores, 2, MakeCreateGroup("g1", 1));
  ApplyOk(stores, 3, MakeAssign("g1", 1, 1));
  ApplyOk(
      stores, 4,
      MakePutPolicy(std::string(lavik::meta::kAuthorityLeasePolicyId), 1,
                    "{\"kind\":\"authority-lease-v1\",\"duration_ms\":5000}"));

  ApplyRejected(stores, 5, MetaCommand{MakeActivate("g1", 0, 1, 3)});
  const auto grant = stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant.has_value());
  EXPECT_EQ(grant->group_term_, 0u);
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
      ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 3)});
  EXPECT_EQ(result.command_tag_,
            lavik::meta::MetaCommandTag::kActivateAuthority);

  // Grant half: the one Grant for the CURRENT term is installed; activation
  // never moves the term.
  const auto grant_state = stores.topology_.AuthorityFor("g1");
  ASSERT_TRUE(grant_state.has_value());
  ASSERT_TRUE(grant_state->grant_.has_value());
  EXPECT_EQ(grant_state->group_term_, 1u);
  EXPECT_EQ(grant_state->grant_->owner_, MakeNodeId(1));

  // Topology half: owner and topology_epoch.
  const auto view = stores.topology_.FindGroup("g1");
  ASSERT_TRUE(view.has_value());
  EXPECT_EQ(view->record_.owner_, MakeNodeId(1));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
}

TEST(MetaStateApply, ActivateAuthorityReplaySameIndexIsIdempotent) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  const lavik::meta::ActivateAuthority cmd = MakeActivate("g1", 1, 1, 3);
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
  EXPECT_EQ(stores.topology_.AuthorityFor("g1")->grant_->owner_, MakeNodeId(1));

  // A genuinely different activation is rejected: the term already has its
  // one Grant and a changed topology epoch is not its exact post-effect.
  lavik::meta::ActivateAuthority different = cmd;
  different.request_id_ = MakeRequestId(0x45);
  different.new_topology_epoch_ = 4;
  ApplyRejected(stores, 8, MetaCommand{different});
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 3u);
}

TEST(MetaStateApply, FenceRequiresNewTermBeforeSameOwnerReauthorization) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MakeRegisterFor(2));
  ApplyOk(stores, 7, MakeAssign("g1", 2, 2, 3));
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 4)});

  // Even a valid next topology epoch cannot replace an active Grant, whether
  // the proposal chooses a different Owner or reauthorizes the current one.
  const std::string active = DomainStateBytes(stores);
  ApplyRejected(stores, 9, MetaCommand{MakeActivate("g1", 1, 2, 5)});
  ApplyRejected(stores, 10, MetaCommand{MakeActivate("g1", 1, 1, 5)});
  EXPECT_EQ(DomainStateBytes(stores), active);

  lavik::meta::FenceGroup fence;
  fence.group_id_ = "g1";
  fence.expected_term_ = 1;
  fence.new_term_ = 1;
  ApplyRejected(stores, 11, MetaCommand{fence});
  EXPECT_EQ(DomainStateBytes(stores), active);
  fence.new_term_ = 2;
  ApplyOk(stores, 12, MetaCommand{fence});
  EXPECT_EQ(stores.topology_.FindGroup("g1")->record_.group_term_, 2u);
  EXPECT_EQ(stores.topology_.AuthorityFor("g1")->group_term_, 2u);
  EXPECT_FALSE(stores.topology_.AuthorityFor("g1")->grant_.has_value());

  // A replacement Meta leader restores the same unused term. Old activation
  // cannot regain authority; the same Owner can receive only term two's Grant.
  auto restored = MetaStores::Deserialize(MustSerialize(stores));
  ASSERT_TRUE(restored.ok()) << restored.status();
  ApplyRejected(*restored, 13, MetaCommand{MakeActivate("g1", 1, 1, 5)});
  const auto activate = MakeActivate("g1", 2, 1, 5);
  ApplyOk(*restored, 14, MetaCommand{activate});
  ASSERT_TRUE(restored->topology_.AuthorityFor("g1")->grant_.has_value());
  EXPECT_EQ(restored->topology_.AuthorityFor("g1")->grant_->owner_,
            MakeNodeId(1));
  const std::string reauthorized = DomainStateBytes(*restored);
  ApplyRejected(*restored, 15, MetaCommand{fence});
  ApplyOk(*restored, 16, MetaCommand{activate});
  EXPECT_EQ(DomainStateBytes(*restored), reauthorized);
}

// ---------------------------------------------------------------------------
// operation journal + upgrade. operation_seq is the raft log index
// of the SubmitOperation; the journal persists the injected ActorContext.
// ---------------------------------------------------------------------------

lavik::meta::SubmitOperation MakeSubmit(std::uint8_t seed,
                                        std::uint8_t intent_seed) {
  lavik::meta::SubmitOperation cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.operation_id_ = MakeOperationId(seed);
  cmd.kind_ = "migration";
  cmd.intent_ = absl::StrCat("{\"slot\":", static_cast<int>(intent_seed), "}");
  cmd.intent_hash_ = MakeHash(intent_seed);
  return cmd;
}

lavik::meta::SubmitOperation MakeClusterCreateSubmit(
    std::uint8_t seed, std::string group_id = "group-a") {
  lavik::meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{std::string(40, '1'), "tcp://127.0.0.1:6379"}};
  manifest.groups_ = {{group_id, std::string(40, '1'), {}}};
  manifest.slot_ranges_ = {{0, 16383, group_id}};
  const auto operation_id = MakeOperationId(seed);
  const auto intent =
      lavik::meta::EncodeClusterCreateRequest(manifest, operation_id);
  EXPECT_TRUE(intent.ok()) << intent.status();

  lavik::meta::SubmitOperation cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.operation_id_ = operation_id;
  cmd.kind_ = lavik::meta::kMetaClusterCreateOperationKind;
  cmd.intent_ = intent.value_or("");
  cmd.intent_hash_ = lavik::meta::MetaSha256(cmd.intent_);
  return cmd;
}

lavik::meta::PutPolicy MakeAutomaticFailoverPolicy(std::uint64_t version = 1) {
  return MakePutPolicy(
      std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId), version,
      "{\"kind\":\"automatic-uncontrolled-failover-v1\","
      "\"suspect_after_ms\":5000}");
}

lavik::meta::PutPolicy MakeAuthorityLeasePolicy(std::uint64_t version = 1) {
  return MakePutPolicy(
      std::string(lavik::meta::kAuthorityLeasePolicyId), version,
      "{\"kind\":\"authority-lease-v1\",\"duration_ms\":5000}");
}

void SeedRequiredCurrentPolicies(MetaStores& stores) {
  ASSERT_TRUE(stores.policy_.Apply(MakeAutomaticFailoverPolicy()).ok());
  ASSERT_TRUE(stores.policy_.Apply(MakeAuthorityLeasePolicy()).ok());
}

std::string ClusterCreateFailureSummaryForTest(
    const lavik::meta::MetaOperationId& id) {
  return absl::StrCat(
      "cluster-create provisioning failed; root-operation=",
      absl::BytesToHexString(std::string_view(
          reinterpret_cast<const char*>(id.data()), id.size())));
}

TEST(MetaStateApply, ClusterCreateRootAtomicallyOwnsLifecycle) {
  MetaStores stores;
  const auto root = MakeClusterCreateSubmit(0x71);
  ApplyOk(stores, 11, MetaCommand{root});

  const auto& creating = stores.topology_.ClusterLifecycle();
  EXPECT_EQ(creating.state_, lavik::meta::MetaClusterLifecycle::kCreating);
  EXPECT_EQ(creating.root_operation_id_, root.operation_id_);
  EXPECT_EQ(creating.genesis_commit_index_, 11u);
  EXPECT_EQ(creating.Revision(), 1u);
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 0u);
  ASSERT_TRUE(stores.operation_.FindOperation(root.operation_id_).has_value());

  // A different request is rejected even when its manifest is identical.
  const auto second = MakeClusterCreateSubmit(0x72);
  ApplyRejected(stores, 12, MetaCommand{second});
  EXPECT_FALSE(
      stores.operation_.FindOperation(second.operation_id_).has_value());
  EXPECT_EQ(stores.topology_.ClusterLifecycle().root_operation_id_,
            root.operation_id_);

  const auto different = MakeClusterCreateSubmit(0x73, "group-b");
  ApplyRejected(stores, 13, MetaCommand{different});
  EXPECT_FALSE(
      stores.operation_.FindOperation(different.operation_id_).has_value());

  lavik::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x74);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  SeedRequiredCurrentPolicies(stores);
  ApplyOk(stores, 14, MetaCommand{complete});
  EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
            lavik::meta::MetaClusterLifecycle::kCreated);
  EXPECT_EQ(stores.topology_.ClusterLifecycle().Revision(), 2u);

  // Exact Genesis replay validates both already-applied halves and remains a
  // no-op after the root reaches its terminal state.
  ApplyOk(stores, 11, MetaCommand{root});
  EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
            lavik::meta::MetaClusterLifecycle::kCreated);

  lavik::meta::ArchiveOperations archive;
  archive.operation_seqs_ = {11};
  ApplyOk(stores, 15, MetaCommand{archive});
  lavik::meta::PruneOperationArchive prune;
  prune.operation_seqs_ = {11};
  ApplyOk(stores, 16, MetaCommand{prune});
  EXPECT_FALSE(stores.operation_.OperationKnown(root.operation_id_));
  EXPECT_EQ(stores.topology_.ClusterLifecycle().root_operation_id_,
            root.operation_id_);
  ApplyRejected(stores, 17, MetaCommand{second});

  // Pruning operation history does not permit the permanently recorded root
  // id to be recycled by an unrelated workflow.
  auto recycled = MakeSubmit(0x75, 0x76);
  recycled.operation_id_ = root.operation_id_;
  ApplyRejected(stores, 18, MetaCommand{recycled});
  EXPECT_FALSE(stores.operation_.OperationKnown(root.operation_id_));
}

TEST(MetaStateApply,
     ClusterCreateCompletionRequiresBothCurrentPoliciesDuringWalApply) {
  const auto expect_rejected = [](bool install_automatic) {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(
        install_automatic ? std::uint8_t{0x6a} : std::uint8_t{0x6b});
    ApplyOk(stores, 1, MetaCommand{root});
    ApplyOk(stores, 2,
            MetaCommand{install_automatic ? MakeAutomaticFailoverPolicy()
                                          : MakeAuthorityLeasePolicy()});

    lavik::meta::CompleteOperation complete;
    complete.request_id_ = MakeRequestId(0x6c);
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ApplyRejected(stores, 3, MetaCommand{complete});

    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
    ASSERT_TRUE(
        stores.operation_.FindOperation(root.operation_id_).has_value());
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
    const auto restored = MetaStores::Deserialize(MustSerialize(stores));
    EXPECT_TRUE(restored.ok()) << restored.status();
  };

  expect_rejected(/*install_automatic=*/true);
  expect_rejected(/*install_automatic=*/false);
}

TEST(MetaStateApply,
     PreseededCurrentPoliciesRemainPristineAndSurviveGenesisAdmission) {
  const std::string automatic_raw =
      R"({"suspect_after_ms":9000,"kind":"automatic-uncontrolled-failover-v1"})";
  const std::string lease_raw =
      R"({"duration_ms":7000,"kind":"authority-lease-v1"})";

  for (const bool seed_both_families : {false, true}) {
    SCOPED_TRACE(seed_both_families ? "both families" : "one family");
    MetaStores stores;
    std::uint64_t log_index = 1;
    ApplyOk(
        stores, log_index++,
        MakePutPolicy(
            std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId), 1,
            automatic_raw));
    if (seed_both_families) {
      ApplyOk(stores, log_index++,
              MakePutPolicy(std::string(lavik::meta::kAuthorityLeasePolicyId),
                            1, lease_raw));
    }
    EXPECT_FALSE(lavik::meta::HasDataClusterArtifacts(stores));

    const auto root = MakeClusterCreateSubmit(
        seed_both_families ? std::uint8_t{0x6d} : std::uint8_t{0x6e});
    ApplyOk(stores, log_index, MetaCommand{root});

    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
    ASSERT_TRUE(
        stores.policy_
            .FindVersion(
                std::string(
                    lavik::meta::kAutomaticUncontrolledFailoverPolicyId),
                1)
            .has_value());
    EXPECT_EQ(stores.policy_
                  .FindVersion(
                      std::string(
                          lavik::meta::kAutomaticUncontrolledFailoverPolicyId),
                      1)
                  ->content_,
              automatic_raw);
    EXPECT_EQ(stores.policy_.CurrentAutomaticUncontrolledFailover(),
              (lavik::meta::MetaAutomaticUncontrolledFailoverPolicy{
                  .version_ = 1, .suspect_after_ms_ = 9000}));
    if (seed_both_families) {
      ASSERT_TRUE(
          stores.policy_
              .FindVersion(std::string(lavik::meta::kAuthorityLeasePolicyId), 1)
              .has_value());
      EXPECT_EQ(
          stores.policy_
              .FindVersion(std::string(lavik::meta::kAuthorityLeasePolicyId), 1)
              ->content_,
          lease_raw);
      EXPECT_EQ(stores.policy_.CurrentAuthorityLease(),
                (lavik::meta::MetaAuthorityLeasePolicy{.version_ = 1,
                                                       .duration_ms_ = 7000}));
    }
  }
}

TEST(MetaStateApply,
     AcceptedPolicyPutRemainsSuccessfulAfterBoundedHistoryEviction) {
  MetaStores stores;
  MetaApplyResult first_result;
  const std::uint64_t last_version =
      lavik::meta::kMaxMetaPolicyVersionsPerPolicy + 1;
  for (std::uint64_t version = 1; version <= last_version; ++version) {
    const MetaApplyResult result = ApplyOk(
        stores, version, MetaCommand{MakeAutomaticFailoverPolicy(version)});
    if (version == 1) first_result = result;
  }

  EXPECT_EQ(first_result.verdict_, MetaAuditVerdict::kAccepted);
  EXPECT_FALSE(
      stores.policy_
          .FindVersion(
              std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId),
              1)
          .has_value());
  EXPECT_EQ(stores.policy_.LatestVersion(std::string(
                lavik::meta::kAutomaticUncontrolledFailoverPolicyId)),
            std::optional<std::uint64_t>(last_version));
}

TEST(MetaStateApply, ClusterCreateAbortAtomicallyRecordsSafeFailure) {
  MetaStores stores;
  const auto root = MakeClusterCreateSubmit(0x74);
  ApplyOk(stores, 21, MetaCommand{root});

  lavik::meta::AbortOperation abort;
  abort.request_id_ = MakeRequestId(0x75);
  abort.operation_id_ = root.operation_id_;
  abort.expected_revision_ = 0;
  abort.reason_ = "password=hunter2\nraw downstream failure";
  ApplyOk(stores, 22, MetaCommand{abort});

  const auto& failed = stores.topology_.ClusterLifecycle();
  EXPECT_EQ(failed.state_,
            lavik::meta::MetaClusterLifecycle::kProvisioningFailed);

  EXPECT_EQ(failed.failure_summary_.find("hunter2"), std::string::npos);
  EXPECT_NE(failed.failure_summary_.find("root-operation="), std::string::npos);
}

TEST(MetaStateApply, SnapshotValidatesClusterLifecycleAggregate) {
  {
    MetaStores stores;
    const auto root = MakeOperationId(0x76);
    ASSERT_TRUE(stores.topology_.BeginClusterCreate(root, 9).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    auto root = MakeClusterCreateSubmit(0x7d);
    const auto other = MakeClusterCreateSubmit(0x7e);
    root.intent_ = other.intent_;
    root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 10).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 10).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    // Uninitialized plus artifacts is reported as non-pristine by status; it
    // is not structural snapshot corruption.
    MetaStores stores;
    ASSERT_TRUE(stores.policy_
                    .Apply(MakePutPolicy(
                        std::string(lavik::meta::kAuthorityLeasePolicyId), 1,
                        "{\"kind\":\"authority-lease-v1\","
                        "\"duration_ms\":5000}"))
                    .ok());
    const auto restored = MetaStores::Deserialize(MustSerialize(stores));
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(restored->topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kUninitialized);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x7b);
    ApplyOk(stores, 43, MetaCommand{root});
    const auto extra = MakeClusterCreateSubmit(0x7c);
    ASSERT_TRUE(stores.operation_.SubmitOperation(extra, 44).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = extra.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x79);
    ApplyOk(stores, 40, MetaCommand{root});
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    SeedRequiredCurrentPolicies(stores);
    ApplyOk(stores, 41, MetaCommand{complete});

    const auto extra = MakeClusterCreateSubmit(0x7a);
    ASSERT_TRUE(stores.operation_.SubmitOperation(extra, 42).ok());
    ExpectAggregateSnapshotFailStop(stores);
  }
}

TEST(MetaStateApply, ClusterRootTerminalizationRejectsASingleStoreEffect) {
  MetaStores stores;
  auto root = MakeClusterCreateSubmit(0x77);
  ASSERT_TRUE(stores.operation_.SubmitOperation(root, 30).ok());

  lavik::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x78);
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  ApplyRejected(stores, 31, MetaCommand{complete});

  const auto operation = stores.operation_.FindOperation(root.operation_id_);
  ASSERT_TRUE(operation.has_value());
  EXPECT_EQ(operation->lifecycle_,
            lavik::meta::MetaOperationLifecycle::kSubmitted);
  EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
            lavik::meta::MetaClusterLifecycle::kUninitialized);
}

TEST(MetaStateApply, ClusterRootTerminalizationRequiresExactGenesisAnchor) {
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x75);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 28).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 29).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";

    ApplyRejected(stores, 30, MetaCommand{complete});
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
  {
    MetaStores stores;
    auto root = MakeClusterCreateSubmit(0x76);
    root.kind_ = "migration";
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 31).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 31).ok());
    lavik::meta::AbortOperation abort;
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";

    ApplyRejected(stores, 32, MetaCommand{abort});
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
  {
    MetaStores stores;
    auto root = MakeClusterCreateSubmit(0x77);
    const auto other = MakeClusterCreateSubmit(0x78);
    root.intent_ = other.intent_;
    root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 33).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 33).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());

    ApplyRejected(stores, 34, MetaCommand{complete});
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x79);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 35).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 36).ok());
    lavik::meta::AbortOperation abort;
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ASSERT_TRUE(stores.operation_.AbortOperation(abort).ok());

    ApplyRejected(stores, 37, MetaCommand{abort});
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
}

TEST(MetaStateApply, ClusterRootTerminalReplayRejectsEitherMissingHalf) {
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x75);
    ApplyOk(stores, 30, MetaCommand{root});
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());

    ApplyRejected(stores, 31, MetaCommand{complete});
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x76);
    ApplyOk(stores, 32, MetaCommand{root});
    ASSERT_TRUE(
        stores.topology_.CompleteClusterCreate(root.operation_id_).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";

    ApplyRejected(stores, 33, MetaCommand{complete});
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x79);
    ApplyOk(stores, 34, MetaCommand{root});
    lavik::meta::AbortOperation abort;
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ASSERT_TRUE(stores.operation_.AbortOperation(abort).ok());

    ApplyRejected(stores, 35, MetaCommand{abort});
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x7a);
    ApplyOk(stores, 36, MetaCommand{root});
    ASSERT_TRUE(stores.topology_
                    .FailClusterCreate(
                        root.operation_id_,
                        ClusterCreateFailureSummaryForTest(root.operation_id_))
                    .ok());
    lavik::meta::AbortOperation abort;
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";

    ApplyRejected(stores, 37, MetaCommand{abort});
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
  }
}

TEST(MetaStateApply, ClusterRootTerminalReplayRejectsInvalidGenesisAnchor) {
  {
    MetaStores stores;
    auto root = MakeClusterCreateSubmit(0x7b);
    root.kind_ = "migration";
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 38).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 38).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());
    ASSERT_TRUE(
        stores.topology_.CompleteClusterCreate(root.operation_id_).ok());

    ApplyRejected(stores, 39, MetaCommand{complete});
  }
  {
    MetaStores stores;
    auto root = MakeClusterCreateSubmit(0x7c);
    const auto other = MakeClusterCreateSubmit(0x7d);
    root.intent_ = other.intent_;
    root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 40).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 40).ok());
    lavik::meta::CompleteOperation complete;
    complete.operation_id_ = root.operation_id_;
    complete.expected_revision_ = 0;
    complete.result_ = "cluster-created";
    ASSERT_TRUE(stores.operation_.CompleteOperation(complete).ok());
    ASSERT_TRUE(
        stores.topology_.CompleteClusterCreate(root.operation_id_).ok());

    ApplyRejected(stores, 41, MetaCommand{complete});
  }
  {
    MetaStores stores;
    const auto root = MakeClusterCreateSubmit(0x7e);
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 42).ok());
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 43).ok());
    lavik::meta::AbortOperation abort;
    abort.operation_id_ = root.operation_id_;
    abort.expected_revision_ = 0;
    abort.reason_ = "provisioning failed";
    ASSERT_TRUE(stores.operation_.AbortOperation(abort).ok());
    ASSERT_TRUE(stores.topology_
                    .FailClusterCreate(
                        root.operation_id_,
                        ClusterCreateFailureSummaryForTest(root.operation_id_))
                    .ok());

    ApplyRejected(stores, 44, MetaCommand{abort});
  }
}

TEST(MetaStateApply, ClusterRootSubmitReplayRejectsASingleStoreEffect) {
  const auto root = MakeClusterCreateSubmit(0x78);
  {
    MetaStores stores;
    ASSERT_TRUE(stores.operation_.SubmitOperation(root, 32).ok());

    ApplyRejected(stores, 32, MetaCommand{root});

    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kUninitialized);
  }
  {
    MetaStores stores;
    ASSERT_TRUE(
        stores.topology_.BeginClusterCreate(root.operation_id_, 32).ok());

    ApplyRejected(stores, 32, MetaCommand{root});

    EXPECT_FALSE(
        stores.operation_.FindOperation(root.operation_id_).has_value());
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
    ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                    stores.topology_, "g1", 1)
                    .ok());
    EXPECT_TRUE(MetaStores::Deserialize(MustSerialize(stores)).ok());
  }
}

TEST(MetaStateApply, MetaStoresDeserializeRejectsDanglingManifestReference) {
  MetaStores stores;
  ApplyOk(stores, 1, MakeCreateGroup("g1", 1));
  ASSERT_TRUE(
      stores.topology_.SetPopulationManifest("g1", 1, MakeHash(0x91)).ok());
  ExpectAggregateSnapshotFailStop(stores);
}

TEST(MetaStateApply,
     MetaStoresDeserializeRejectsUnprojectableActiveGrantAnchors) {
  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 3)});
  ASSERT_TRUE(lavik::meta::MetaTopologyTestAccess::SetGroupTerm(
                  stores.topology_, "g1", 0)
                  .ok());
  ExpectAggregateSnapshotFailStop(stores);
}

TEST(MetaStateApply, SubmitOperationSeqIsLogIndexAndActorPersisted) {
  MetaStores stores;
  const lavik::meta::SubmitOperation cmd = MakeSubmit(0x60, 0x11);
  ApplyOk(stores, 3, MetaCommand{cmd});

  const auto record = stores.operation_.FindOperation(cmd.operation_id_);
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->operation_seq_, 3u);  // the raft log index of the submit
  EXPECT_EQ(record->lifecycle_,
            lavik::meta::MetaOperationLifecycle::kSubmitted);
  // The journal persists the trusted-entry-injected ActorContext, carried by
  // the raft-log encoding and only copied by apply.
  EXPECT_EQ(record->actor_.principal_, kActorPrincipal);
  EXPECT_EQ(record->actor_.readable_time_, kReadableTime);

  // Retained-record idempotency: same id + same intent_hash -> idempotent
  // accept, no second record, no audit growth on the same index.
  ApplyOk(stores, 3, MetaCommand{cmd});
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);

  // Same id + different intent_hash: payload reuse, rejected.
  ApplyRejected(stores, 4, MetaCommand{MakeSubmit(0x60, 0x12)});
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);
}

TEST(MetaStateApply, OperationLifecycleAndArchiveThroughDispatcher) {
  MetaStores stores;
  const lavik::meta::SubmitOperation submit = MakeSubmit(0x61, 0x21);
  ApplyOk(stores, 1, MetaCommand{submit});
  const auto op_id = submit.operation_id_;

  lavik::meta::TransitionOperationPhase transition;
  transition.request_id_ = MakeRequestId(0x62);
  transition.operation_id_ = op_id;
  transition.expected_revision_ = 0;
  transition.kind_phase_blob_ = "{\"phase\":\"prepare\"}";
  ApplyOk(stores, 2, MetaCommand{transition});
  EXPECT_EQ(stores.operation_.FindOperation(op_id)->lifecycle_,
            lavik::meta::MetaOperationLifecycle::kRunning);

  lavik::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x63);
  complete.operation_id_ = op_id;
  complete.expected_revision_ = 1;
  complete.result_ = "{\"moved\":1}";
  complete.data_loss_possible_ = true;
  ApplyOk(stores, 3, MetaCommand{complete});
  const auto done = stores.operation_.FindOperation(op_id);
  ASSERT_TRUE(done.has_value());
  EXPECT_EQ(done->lifecycle_, lavik::meta::MetaOperationLifecycle::kCompleted);
  EXPECT_TRUE(done->data_loss_possible_);

  // Terminal states are irreversible.
  lavik::meta::TransitionOperationPhase late = transition;
  late.request_id_ = MakeRequestId(0x64);
  late.expected_revision_ = 2;
  ApplyRejected(stores, 4, MetaCommand{late});

  // Non-contiguous archival of the terminal operation (seq = submit index).
  lavik::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x65);
  archive.operation_seqs_ = {1};
  ApplyOk(stores, 5, MetaCommand{archive});
  EXPECT_FALSE(stores.operation_.FindOperation(op_id).has_value());
  const auto tombstone = stores.operation_.FindArchived(op_id);
  ASSERT_TRUE(tombstone.has_value());
  EXPECT_EQ(tombstone->operation_seq_, 1u);
  EXPECT_EQ(tombstone->terminal_lifecycle_,
            lavik::meta::MetaOperationLifecycle::kCompleted);
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
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 4)});
  const lavik::meta::SubmitOperation submit = MakeSubmit(0x68, 0x41);
  ApplyOk(stores, 9, MetaCommand{submit});

  lavik::meta::MetaDirectiveSpec directive;
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
  directive.kind_ = "rebuild";
  directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_ = {directive};
  ApplyOk(stores, 10, MetaCommand{transition});

  lavik::meta::CommitDirectiveResult commit;
  commit.operation_id_ = submit.operation_id_;
  commit.directive_id_ = directive.directive_id_;
  commit.attempt_id_ = directive.attempt_id_;
  commit.directive_revision_ = 10;
  commit.recipient_node_id_ = directive.recipient_node_id_;
  commit.recipient_boot_id_ = directive.target_boot_id_;
  commit.assignment_id_ = directive.assignment_id_;
  commit.result_ = "installed";

  const auto applied = ApplyOk(stores, 11, MetaCommand{commit});
  EXPECT_EQ(applied.command_tag_,
            lavik::meta::MetaCommandTag::kCommitDirectiveResult);
  const lavik::meta::MetaTerminalReceiptKey key{
      submit.operation_id_, directive.directive_id_, directive.attempt_id_, 10};
  ASSERT_TRUE(stores.operation_.FindTerminalReceipt(key).has_value());
  EXPECT_EQ(stores.operation_.FindTerminalReceipt(key)->committed_index_, 11u);
  EXPECT_EQ(stores.operation_.FindOperation(submit.operation_id_)->revision_,
            2u);

  lavik::meta::CompleteOperation stale_complete;
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
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 4)});
  const lavik::meta::SubmitOperation submit = MakeSubmit(0x69, 0x42);
  ApplyOk(stores, 9, MetaCommand{submit});

  lavik::meta::MetaDirectiveSpec directive;
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
  directive.partition_replication_epoch_ = 0;
  directive.kind_ = "rebuild";
  directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::TransitionOperationPhase transition;
  transition.operation_id_ = submit.operation_id_;
  transition.current_directives_ = {directive};
  transition.current_directives_[0].group_term_ = 2;
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
  lavik::meta::SubmitOperation submit;
  lavik::meta::MetaDirectiveSpec directive;
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
  ApplyOk(fixture.stores, 10, MetaCommand{MakeActivate("g1", 1, 3, 5)});

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
  fixture.directive.kind_ = "rebuild";
  fixture.directive.payload_ =
      *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::TransitionOperationPhase transition;
  transition.operation_id_ = fixture.submit.operation_id_;
  transition.current_directives_ = {fixture.directive};
  ApplyOk(fixture.stores, fixture.directive_revision, MetaCommand{transition});
  return fixture;
}

lavik::meta::CommitDirectiveResult MakeDirectiveResult(
    const InstalledDirectiveFixture& fixture) {
  lavik::meta::CommitDirectiveResult result;
  result.operation_id_ = fixture.submit.operation_id_;
  result.directive_id_ = fixture.directive.directive_id_;
  result.attempt_id_ = fixture.directive.attempt_id_;
  result.directive_revision_ = fixture.directive_revision;
  result.recipient_node_id_ = fixture.directive.recipient_node_id_;
  result.recipient_boot_id_ = fixture.directive.target_boot_id_;
  result.assignment_id_ = fixture.directive.assignment_id_;
  result.result_ = "installed";
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

    lavik::meta::RemoveNodeFromGroup remove;
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
    lavik::meta::AssignNodeToGroup readd = MakeAssign("g1", removed_node, 5, 7);
    readd.assignment_id_.fill(static_cast<std::uint8_t>(0x20 + removed_node));
    ApplyOk(fixture.stores, 14, MetaCommand{readd});
    ExpectDirectiveInvalidated(fixture);

    const auto result = MakeDirectiveResult(fixture);
    ApplyRejected(fixture.stores, 15, MetaCommand{result});
    const lavik::meta::MetaTerminalReceiptKey key{
        result.operation_id_, result.directive_id_, result.attempt_id_,
        result.directive_revision_};
    EXPECT_FALSE(
        fixture.stores.operation_.FindTerminalReceipt(key).has_value());
  }
}

TEST(MetaStateApply, AuthorityAnchorMutationsInvalidateLiveDirectives) {
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    lavik::meta::BeginGroupTerm begin;
    begin.group_id_ = "g1";
    begin.expected_term_ = 1;
    begin.new_term_ = 2;
    ApplyOk(fixture.stores, 13, MetaCommand{begin});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    lavik::meta::FenceGroup fence;
    fence.group_id_ = "g1";
    fence.expected_term_ = 1;
    fence.new_term_ = 2;
    ApplyOk(fixture.stores, 13, MetaCommand{fence});
    ExpectDirectiveInvalidated(fixture);
  }
}

TEST(MetaStateApply, PopulationAnchorMutationsInvalidateLiveDirectives) {
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    lavik::meta::PutPopulationManifest put;
    put.entries_ = {{1, 11}};
    put.manifest_digest_ =
        lavik::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
    ApplyOk(fixture.stores, 13, MetaCommand{put});
    EXPECT_EQ(
        fixture.stores.operation_.FindOperation(fixture.submit.operation_id_)
            ->current_directives_.size(),
        1u);

    lavik::meta::SetGroupReplicationState update;
    update.group_id_ = "g1";
    update.new_population_manifest_revision_ = 1;
    update.new_population_manifest_digest_ = put.manifest_digest_;
    update.new_topology_epoch_ = 6;
    ApplyOk(fixture.stores, 14, MetaCommand{update});
    ExpectDirectiveInvalidated(fixture);
  }
  {
    InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
    lavik::meta::SetGroupReplicationState update;
    update.group_id_ = "g1";
    update.new_partition_replication_epoch_ = 1;
    update.new_topology_epoch_ = 6;
    ApplyOk(fixture.stores, 13, MetaCommand{update});
    ExpectDirectiveInvalidated(fixture);
  }
}

TEST(MetaStateApply, RejectedAnchorMutationDoesNotInvalidateDirective) {
  InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
  lavik::meta::RemoveNodeFromGroup remove;
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
  lavik::meta::RemoveNodeFromGroup remove;
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
  const lavik::meta::MetaTerminalReceiptKey key{
      result.operation_id_, result.directive_id_, result.attempt_id_,
      result.directive_revision_};
  EXPECT_FALSE(fixture.stores.operation_.FindTerminalReceipt(key).has_value());
}

TEST(MetaStateApply,
     CommittedDirectiveResultRemainsReplayableAfterInvalidation) {
  InstalledDirectiveFixture fixture = MakeInstalledDirectiveFixture();
  const auto result = MakeDirectiveResult(fixture);
  ApplyOk(fixture.stores, 13, MetaCommand{result});

  lavik::meta::RemoveNodeFromGroup remove;
  remove.group_id_ = "g1";
  remove.node_id_ = fixture.directive.target_node_id_;
  remove.expected_revision_ = 4;
  remove.new_topology_epoch_ = 6;
  ApplyOk(fixture.stores, 14, MetaCommand{remove});
  ExpectDirectiveInvalidated(fixture, 3);

  ApplyOk(fixture.stores, 15, MetaCommand{result});
  const lavik::meta::MetaTerminalReceiptKey key{
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
  ApplyOk(stores, 8, MetaCommand{MakeActivate("g1", 1, 1, 4)});
  const lavik::meta::SubmitOperation submit = MakeSubmit(0x6a, 0x43);
  ApplyOk(stores, 9, MetaCommand{submit});

  lavik::meta::RemoveNodeFromGroup remove;
  remove.request_id_ = MakeRequestId(0x6b);
  remove.group_id_ = "g1";
  remove.node_id_ = MakeNodeId(2);
  remove.expected_revision_ = 3;
  remove.new_topology_epoch_ = 5;
  ApplyOk(stores, 10, MetaCommand{remove});

  lavik::meta::AssignNodeToGroup readd = MakeAssign("g1", 2, 4, 6);
  readd.request_id_ = MakeRequestId(0x6c);
  readd.assignment_id_.fill(0x22);
  ApplyOk(stores, 11, MetaCommand{readd});

  lavik::meta::MetaDirectiveSpec directive;
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
  directive.kind_ = "authorize-source";
  directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});

  lavik::meta::TransitionOperationPhase transition;
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
  namespace control = lavik::cluster::control;
  static_assert(control::kMaxProjectedDirectives %
                    lavik::meta::kMaxMetaDirectivesPerOperation ==
                0);

  MetaStores stores;
  SetupActivatedGroupPrerequisites(stores, 1, "g1");
  ApplyOk(stores, 6, MetaCommand{MakeActivate("g1", 1, 1, 3)});

  const auto make_directive = [](std::size_t ordinal) {
    lavik::meta::MetaDirectiveSpec directive;
    directive.directive_id_.fill(0);
    directive.directive_id_[0] = 1;
    directive.directive_id_[14] =
        static_cast<std::uint8_t>((ordinal >> 8) & 0xff);
    directive.directive_id_[15] = static_cast<std::uint8_t>(ordinal & 0xff);
    directive.attempt_id_.fill(0);
    directive.attempt_id_[0] = 2;
    directive.attempt_id_[14] =
        static_cast<std::uint8_t>((ordinal >> 8) & 0xff);
    directive.attempt_id_[15] = static_cast<std::uint8_t>(ordinal & 0xff);
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
    directive.kind_ = "authorize-source";
    directive.payload_ = *lavik::cluster::control::EncodeRebuildRequest({3});
    return directive;
  };

  std::uint64_t log_index = 7;
  const std::size_t full_operation_count =
      control::kMaxProjectedDirectives /
      lavik::meta::kMaxMetaDirectivesPerOperation;
  // Seed all but the last per-operation block through the store seam to keep
  // this cap test linear. The final block and the one-over command below go
  // through ApplyCommitted, which is the behavior under test.
  for (std::size_t operation_ordinal = 0;
       operation_ordinal + 1 < full_operation_count; ++operation_ordinal) {
    lavik::meta::SubmitOperation submit =
        MakeSubmit(static_cast<std::uint8_t>(operation_ordinal + 1),
                   static_cast<std::uint8_t>(operation_ordinal + 101));
    submit.replication_history_id_.fill(3);
    ASSERT_TRUE(stores.operation_.SubmitOperation(submit, log_index++).ok());

    lavik::meta::TransitionOperationPhase transition;
    transition.operation_id_ = submit.operation_id_;
    transition.kind_phase_blob_ = "dispatch";
    transition.current_directives_.reserve(
        lavik::meta::kMaxMetaDirectivesPerOperation);
    for (std::size_t directive_ordinal = 0;
         directive_ordinal < lavik::meta::kMaxMetaDirectivesPerOperation;
         ++directive_ordinal) {
      transition.current_directives_.push_back(
          make_directive(directive_ordinal));
    }
    ASSERT_TRUE(
        stores.operation_.TransitionOperationPhase(transition, log_index++)
            .ok());
  }

  lavik::meta::SubmitOperation boundary_operation = MakeSubmit(0x6f, 0x70);
  boundary_operation.replication_history_id_.fill(3);
  ApplyOk(stores, log_index++, MetaCommand{boundary_operation});
  lavik::meta::TransitionOperationPhase boundary_transition;
  boundary_transition.operation_id_ = boundary_operation.operation_id_;
  boundary_transition.kind_phase_blob_ = "dispatch";
  boundary_transition.current_directives_.reserve(
      lavik::meta::kMaxMetaDirectivesPerOperation);
  for (std::size_t directive_ordinal = 0;
       directive_ordinal < lavik::meta::kMaxMetaDirectivesPerOperation;
       ++directive_ordinal) {
    boundary_transition.current_directives_.push_back(
        make_directive(directive_ordinal));
  }
  ApplyOk(stores, log_index++, MetaCommand{boundary_transition});

  const auto boundary = lavik::meta::MetaControlProjector::ProjectNode(
      lavik::meta::MetaCommittedView(stores, log_index - 1), MakeNodeId(1));
  ASSERT_TRUE(boundary.ok()) << boundary.status();
  EXPECT_EQ(boundary->full_state.current_directives.size(),
            control::kMaxProjectedDirectives);

  lavik::meta::SubmitOperation overflow = MakeSubmit(0x70, 0x71);
  overflow.replication_history_id_.fill(3);
  ApplyOk(stores, log_index++, MetaCommand{overflow});

  lavik::meta::TransitionOperationPhase transition;
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

  lavik::meta::ArchiveOperations archive;
  archive.request_id_ = MakeRequestId(0x67);
  archive.operation_seqs_ = {1};
  ApplyRejected(stores, 2, MetaCommand{archive});  // still Submitted
  EXPECT_EQ(stores.operation_.LiveCount(), 1u);

  // References to unknown seqs reject as well.
  archive.operation_seqs_ = {42};
  ApplyRejected(stores, 3, MetaCommand{archive});
}

TEST(MetaStateApply, AuditPruneIsReplicatedAndAudited) {
  lavik::meta::MetaStores stores;
  auto create = MakeCreateGroup("g-prune", 1);
  ASSERT_EQ(lavik::meta::ApplyCommitted(stores, 1, create,
                                        "lavik://operator/test", "t")
                .verdict_,
            lavik::meta::MetaAuditVerdict::kAccepted);
  ASSERT_EQ(stores.audit_.size(), 1u);

  lavik::meta::PruneAudit prune;
  prune.request_id_[0] = 9;
  prune.through_log_index_ = 1;
  const auto result = lavik::meta::ApplyCommitted(
      stores, 2, prune, "lavik://operator/test", "t2");
  EXPECT_EQ(result.verdict_, lavik::meta::MetaAuditVerdict::kAccepted);
  EXPECT_EQ(stores.audit_.pruned_floor(), 1u);
  EXPECT_EQ(stores.audit_.size(), 1u);
  EXPECT_TRUE(stores.audit_.Find(2).has_value());
}

// ---------------------------------------------------------------------------
// Scripted replay across the main identity, topology, authority, policy, and
// operation lifecycles, mixing accepted and rejected commands. The apply
// layer locks the semantics the recovery path relies on.
// ---------------------------------------------------------------------------

struct ScriptedCommand {
  lavik::meta::MetaCommand command;
  MetaAuditVerdict expected;
};

// Revision/epoch/term tokens are pinned to the state the prefix produces.
std::vector<ScriptedCommand> MakeCommandScript() {
  std::vector<ScriptedCommand> script;
  const auto accept = MetaAuditVerdict::kAccepted;
  const auto reject = MetaAuditVerdict::kRejected;
  auto push = [&script](lavik::meta::MetaCommand cmd,
                        MetaAuditVerdict expected) {
    script.push_back(ScriptedCommand{std::move(cmd), expected});
  };

  // identity
  push(MakeRegisterFor(1), accept);
  push(MakeRegisterFor(2), accept);
  {
    lavik::meta::RegisterNode conflict = MakeRegisterFor(3);
    conflict.principal_ = "lavik://node/" + MakeNodeId(1);
    push(std::move(conflict), reject);  // principal 1:1
  }
  // topology: groups
  push(MakeCreateGroup("g1", 1), accept);
  push(MakeCreateGroup("g2", 2), accept);
  push(MakeCreateGroup("g1", 3), reject);  // existing group, epoch mismatch
  // policy
  const std::string authority_policy_id(lavik::meta::kAuthorityLeasePolicyId);
  push(MakePutPolicy(authority_policy_id, 1,
                     "{\"kind\":\"authority-lease-v1\",\"duration_ms\":5000}"),
       accept);
  push(MakePutPolicy(authority_policy_id, 1,
                     "{\"kind\":\"authority-lease-v1\",\"duration_ms\":9999}"),
       reject);  // version slot immutable
  // topology: membership
  push(MakeAssign("g1", 1, 1, 3), accept);
  push(MakeAssign("g1", 2, 2, 4), accept);
  push(MakeAssign("g2", 1, 1), reject);  // one-node-one-group
  push(MakeAssign("g1", 9, 3), reject);  // unregistered node
  // term
  {
    lavik::meta::BeginGroupTerm begin;
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
  // activation
  const lavik::meta::ActivateAuthority activate = MakeActivate("g1", 1, 1, 5);
  push(activate, accept);
  push(activate, accept);  // same content, new index: idempotent path
  // operation lifecycle
  const lavik::meta::SubmitOperation submit = MakeSubmit(0x60, 0x11);
  push(submit, accept);  // index 17: operation_seq == 17
  {
    lavik::meta::SubmitOperation reuse = MakeSubmit(0x60, 0x12);
    push(std::move(reuse), reject);  // id reused with a different intent
  }
  {
    lavik::meta::TransitionOperationPhase transition;
    transition.request_id_ = MakeRequestId(0x61);
    transition.operation_id_ = submit.operation_id_;
    transition.expected_revision_ = 0;
    transition.kind_phase_blob_ = "{\"phase\":\"prepare\"}";
    push(transition, accept);
    lavik::meta::CompleteOperation complete;
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
    lavik::meta::ArchiveOperations archive;
    archive.request_id_ = MakeRequestId(0x64);
    archive.operation_seqs_ = {17};
    push(archive, accept);
    push(archive, accept);  // already archived: idempotent
  }
  // Fence the current authority into a new term before changing its
  // slot projection. This placement also proves the accepted
  // SetSlotMap path in the full matrix.
  {
    lavik::meta::FenceGroup fence;
    fence.request_id_ = MakeRequestId(0x44);
    fence.group_id_ = "g1";
    fence.expected_term_ = 1;
    fence.new_term_ = 2;
    push(fence, accept);
  }
  // slot map
  {
    lavik::meta::SetSlotMap slots;
    slots.request_id_ = MakeRequestId(0x33);
    slots.ranges_ = {{0, 16383, "g1"}};
    slots.new_topology_epoch_ = 6;
    push(slots, accept);
  }
  {
    lavik::meta::PutPopulationManifest put;
    put.request_id_ = MakeRequestId(0x34);
    put.entries_ = {{1, 1}};
    put.manifest_digest_ =
        lavik::meta::MetaPopulationManifestStore::CanonicalDigest(put.entries_);
    push(put, accept);

    lavik::meta::SetGroupReplicationState replication;
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
    lavik::meta::UpdateNode update;
    update.request_id_ = MakeRequestId(0x21);
    update.node_id_ = MakeNodeId(2);
    update.expected_revision_ = 1;
    update.endpoints_ = {"10.0.0.9:7000"};
    update.new_topology_epoch_ = 8;
    push(update, accept);
    lavik::meta::RemoveNodeFromGroup remove;
    remove.request_id_ = MakeRequestId(0x32);
    remove.group_id_ = "g1";
    remove.node_id_ = MakeNodeId(2);
    remove.expected_revision_ = 3;
    remove.new_topology_epoch_ = 9;
    push(remove, accept);
    lavik::meta::RetireNode retire;
    retire.request_id_ = MakeRequestId(0x22);
    retire.node_id_ = MakeNodeId(2);
    retire.expected_revision_ = 2;
    push(retire, accept);
    push(MakeAssign("g2", 2, 1), reject);  // retired node
  }
  // An explicit new fence advances even an already-grantless term. The matrix
  // below also checks exact replay of this command at its original index.
  {
    lavik::meta::FenceGroup fence;
    fence.request_id_ = MakeRequestId(0x46);
    fence.group_id_ = "g1";
    fence.expected_term_ = 2;
    fence.new_term_ = 3;
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
    EXPECT_EQ(stores.audit_.Find(index), audit_after_first)
        << "index " << index;
  }
  EXPECT_EQ(stores.audit_.size(), script.size());
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
  // ordered audit window included.
  MetaStores second_run;
  index = 0;
  for (const ScriptedCommand& step : script) {
    const MetaApplyResult result = ApplyCommitted(
        second_run, ++index, step.command, kActorPrincipal, kReadableTime);
    EXPECT_EQ(result, first_results[index - 1]) << "index " << index;
  }
  EXPECT_EQ(MustSerialize(second_run), MustSerialize(first_run));

  // A snapshot round-trip of the fully populated aggregate preserves
  // everything, including the audit window.
  const auto restored = MetaStores::Deserialize(MustSerialize(first_run));
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(MustSerialize(*restored), MustSerialize(first_run));
}

}  // namespace
