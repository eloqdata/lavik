#include "keylane/command_table.h"

#include <charconv>
#include <limits>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "keylane/redis_parse.h"

namespace keylane {

namespace {

constexpr std::uint32_t kKeyedRead = kCmdReadOnly | kCmdUsesDbGate;
constexpr std::uint32_t kKeyedWrite = kCmdWrite | kCmdUsesDbGate;

constexpr CommandSpec kCommandTable[] = {
    {"ping", CommandKind::kPing, 1, 2, 0, 0, 1, kCmdNoKeys},
    {"echo", CommandKind::kEcho, 2, 2, 0, 0, 1, kCmdNoKeys},
    {"select", CommandKind::kSelect, 2, 2, 0, 0, 1, kCmdNoKeys},
    {"dbsize", CommandKind::kDbSize, 1, 1, 0, 0, 1,
     kCmdReadOnly | kCmdGlobal | kCmdUsesDbGate | kCmdNoKeys},
    {"scan", CommandKind::kScan, 2, 0, 0, 0, 1,
     kCmdReadOnly | kCmdGlobal | kCmdUsesDbGate | kCmdNoKeys},
    {"type", CommandKind::kType, 2, 2, 1, 1, 1, kKeyedRead},
    {"randomkey", CommandKind::kRandomKey, 1, 1, 0, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdNoKeys},
    {"flushdb", CommandKind::kFlushDb, 1, 0, 0, 0, 1,
     kCmdWrite | kCmdGlobal | kCmdNoKeys},
    {"flushall", CommandKind::kFlushAll, 1, 0, 0, 0, 1,
     kCmdWrite | kCmdGlobal | kCmdNoKeys},
    {"get", CommandKind::kGet, 2, 2, 1, 1, 1, kKeyedRead},
    {"getdel", CommandKind::kGetDel, 2, 2, 1, 1, 1, kKeyedWrite},
    {"getex", CommandKind::kGetEx, 2, 0, 1, 1, 1, kKeyedWrite},
    {"getrange", CommandKind::kGetRange, 4, 4, 1, 1, 1, kKeyedRead},
    {"getset", CommandKind::kGetSet, 3, 3, 1, 1, 1, kKeyedWrite},
    {"append", CommandKind::kAppend, 3, 3, 1, 1, 1, kKeyedWrite},
    {"getbit", CommandKind::kGetBit, 3, 3, 1, 1, 1, kKeyedRead},
    {"setbit", CommandKind::kSetBit, 4, 4, 1, 1, 1, kKeyedWrite},
    {"bitcount", CommandKind::kBitCount, 2, 0, 1, 1, 1, kKeyedRead},
    {"bitpos", CommandKind::kBitPos, 3, 0, 1, 1, 1, kKeyedRead},
    {"bitfield", CommandKind::kBitField, 2, 0, 1, 1, 1, kKeyedWrite},
    {"bitfield_ro", CommandKind::kBitFieldRo, 2, 0, 1, 1, 1, kKeyedRead},
    {"bitop", CommandKind::kBitOp, 4, 0, 2, -1, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard},
    {"set", CommandKind::kSet, 3, 0, 1, 1, 1, kKeyedWrite},
    {"setex", CommandKind::kSetEx, 4, 4, 1, 1, 1, kKeyedWrite},
    {"psetex", CommandKind::kPSetEx, 4, 4, 1, 1, 1, kKeyedWrite},
    {"setnx", CommandKind::kSetNx, 3, 3, 1, 1, 1, kKeyedWrite},
    {"setrange", CommandKind::kSetRange, 4, 4, 1, 1, 1, kKeyedWrite},
    {"substr", CommandKind::kSubstr, 4, 4, 1, 1, 1, kKeyedRead},
    {"lcs", CommandKind::kLcs, 3, 0, 1, 2, 1, kKeyedRead | kCmdMultiShard},
    {"lpush", CommandKind::kLPush, 3, 0, 1, 1, 1, kKeyedWrite},
    {"lpushx", CommandKind::kLPushX, 3, 0, 1, 1, 1, kKeyedWrite},
    {"rpush", CommandKind::kRPush, 3, 0, 1, 1, 1, kKeyedWrite},
    {"rpushx", CommandKind::kRPushX, 3, 0, 1, 1, 1, kKeyedWrite},
    {"lpop", CommandKind::kLPop, 2, 3, 1, 1, 1, kKeyedWrite},
    {"rpop", CommandKind::kRPop, 2, 3, 1, 1, 1, kKeyedWrite},
    {"llen", CommandKind::kLLen, 2, 2, 1, 1, 1, kKeyedRead},
    {"lindex", CommandKind::kLIndex, 3, 3, 1, 1, 1, kKeyedRead},
    {"lrange", CommandKind::kLRange, 4, 4, 1, 1, 1, kKeyedRead},
    {"lset", CommandKind::kLSet, 4, 4, 1, 1, 1, kKeyedWrite},
    {"linsert", CommandKind::kLInsert, 5, 5, 1, 1, 1, kKeyedWrite},
    {"lrem", CommandKind::kLRem, 4, 4, 1, 1, 1, kKeyedWrite},
    {"ltrim", CommandKind::kLTrim, 4, 4, 1, 1, 1, kKeyedWrite},
    {"lpos", CommandKind::kLPos, 3, 0, 1, 1, 1, kKeyedRead},
    {"lmove", CommandKind::kLMove, 5, 5, 1, 2, 1, kKeyedWrite | kCmdMultiShard},
    {"rpoplpush", CommandKind::kRPopLPush, 3, 3, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    // LMPOP/BLMPOP have argument-dependent key ranges. Their handlers build
    // the concrete transaction key set after parsing numkeys.
    {"lmpop", CommandKind::kLMPop, 4, 0, 2, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"blpop", CommandKind::kBLPop, 3, 0, 1, -2, 1,
     kKeyedWrite | kCmdMultiShard | kCmdMayBlock},
    {"brpop", CommandKind::kBRPop, 3, 0, 1, -2, 1,
     kKeyedWrite | kCmdMultiShard | kCmdMayBlock},
    {"blmove", CommandKind::kBLMove, 6, 6, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard | kCmdMayBlock},
    {"brpoplpush", CommandKind::kBRPopLPush, 4, 4, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard | kCmdMayBlock},
    {"blmpop", CommandKind::kBLMPop, 5, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys |
         kCmdMayBlock},
    {"hset", CommandKind::kHSet, 4, 0, 1, 1, 1, kKeyedWrite},
    {"hmset", CommandKind::kHMSet, 4, 0, 1, 1, 1, kKeyedWrite},
    {"hsetnx", CommandKind::kHSetNx, 4, 4, 1, 1, 1, kKeyedWrite},
    {"hget", CommandKind::kHGet, 3, 3, 1, 1, 1, kKeyedRead},
    {"hmget", CommandKind::kHMGet, 3, 0, 1, 1, 1, kKeyedRead},
    {"hdel", CommandKind::kHDel, 3, 0, 1, 1, 1, kKeyedWrite},
    {"hlen", CommandKind::kHLen, 2, 2, 1, 1, 1, kKeyedRead},
    {"hexists", CommandKind::kHExists, 3, 3, 1, 1, 1, kKeyedRead},
    {"hgetall", CommandKind::kHGetAll, 2, 2, 1, 1, 1, kKeyedRead},
    {"hkeys", CommandKind::kHKeys, 2, 2, 1, 1, 1, kKeyedRead},
    {"hvals", CommandKind::kHVals, 2, 2, 1, 1, 1, kKeyedRead},
    {"hstrlen", CommandKind::kHStrlen, 3, 3, 1, 1, 1, kKeyedRead},
    {"hincrby", CommandKind::kHIncrBy, 4, 4, 1, 1, 1, kKeyedWrite},
    {"hincrbyfloat", CommandKind::kHIncrByFloat, 4, 4, 1, 1, 1, kKeyedWrite},
    {"hrandfield", CommandKind::kHRandField, 2, 4, 1, 1, 1, kKeyedRead},
    {"hscan", CommandKind::kHScan, 3, 0, 1, 1, 1, kKeyedRead},
    {"sadd", CommandKind::kSAdd, 3, 0, 1, 1, 1, kKeyedWrite},
    {"scard", CommandKind::kSCard, 2, 2, 1, 1, 1, kKeyedRead},
    {"sdiff", CommandKind::kSDiff, 2, 0, 1, -1, 1, kKeyedRead | kCmdMultiShard},
    {"sdiffstore", CommandKind::kSDiffStore, 3, 0, 1, -1, 1,
     kKeyedWrite | kCmdMultiShard},
    {"sinter", CommandKind::kSInter, 2, 0, 1, -1, 1,
     kKeyedRead | kCmdMultiShard},
    {"sintercard", CommandKind::kSInterCard, 3, 0, 2, 0, 1,
     kKeyedRead | kCmdMultiShard | kCmdMovableKeys},
    {"sinterstore", CommandKind::kSInterStore, 3, 0, 1, -1, 1,
     kKeyedWrite | kCmdMultiShard},
    {"sismember", CommandKind::kSIsMember, 3, 3, 1, 1, 1, kKeyedRead},
    {"smembers", CommandKind::kSMembers, 2, 2, 1, 1, 1, kKeyedRead},
    {"smismember", CommandKind::kSMIsMember, 3, 0, 1, 1, 1, kKeyedRead},
    {"smove", CommandKind::kSMove, 4, 4, 1, 2, 1, kKeyedWrite | kCmdMultiShard},
    {"spop", CommandKind::kSPop, 2, 3, 1, 1, 1, kKeyedWrite},
    {"srandmember", CommandKind::kSRandMember, 2, 3, 1, 1, 1, kKeyedRead},
    {"srem", CommandKind::kSRem, 3, 0, 1, 1, 1, kKeyedWrite},
    {"sscan", CommandKind::kSScan, 3, 0, 1, 1, 1, kKeyedRead},
    {"sunion", CommandKind::kSUnion, 2, 0, 1, -1, 1,
     kKeyedRead | kCmdMultiShard},
    {"sunionstore", CommandKind::kSUnionStore, 3, 0, 1, -1, 1,
     kKeyedWrite | kCmdMultiShard},
    {"bzmpop", CommandKind::kBZMPop, 5, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys |
         kCmdMayBlock},
    {"bzpopmax", CommandKind::kBZPopMax, 3, 0, 1, -2, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMayBlock},
    {"bzpopmin", CommandKind::kBZPopMin, 3, 0, 1, -2, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMayBlock},
    {"zadd", CommandKind::kZAdd, 4, 0, 1, 1, 1, kKeyedWrite},
    {"zcard", CommandKind::kZCard, 2, 2, 1, 1, 1, kKeyedRead},
    {"zcount", CommandKind::kZCount, 4, 4, 1, 1, 1, kKeyedRead},
    {"zincrby", CommandKind::kZIncrBy, 4, 4, 1, 1, 1, kKeyedWrite},
    {"zlexcount", CommandKind::kZLexCount, 4, 4, 1, 1, 1, kKeyedRead},
    {"zmpop", CommandKind::kZMPop, 4, 0, 2, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zmscore", CommandKind::kZMScore, 3, 0, 1, 1, 1, kKeyedRead},
    {"zpopmax", CommandKind::kZPopMax, 2, 3, 1, 1, 1, kKeyedWrite},
    {"zpopmin", CommandKind::kZPopMin, 2, 3, 1, 1, 1, kKeyedWrite},
    {"zrandmember", CommandKind::kZRandMember, 2, 4, 1, 1, 1, kKeyedRead},
    {"zrange", CommandKind::kZRange, 4, 0, 1, 1, 1, kKeyedRead},
    {"zrangestore", CommandKind::kZRangeStore, 5, 0, 1, 2, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard},
    {"zrangebylex", CommandKind::kZRangeByLex, 4, 0, 1, 1, 1, kKeyedRead},
    {"zrangebyscore", CommandKind::kZRangeByScore, 4, 0, 1, 1, 1, kKeyedRead},
    {"zrank", CommandKind::kZRank, 3, 0, 1, 1, 1, kKeyedRead},
    {"zrem", CommandKind::kZRem, 3, 0, 1, 1, 1, kKeyedWrite},
    {"zremrangebylex", CommandKind::kZRemRangeByLex, 4, 4, 1, 1, 1,
     kKeyedWrite},
    {"zremrangebyrank", CommandKind::kZRemRangeByRank, 4, 4, 1, 1, 1,
     kKeyedWrite},
    {"zremrangebyscore", CommandKind::kZRemRangeByScore, 4, 4, 1, 1, 1,
     kKeyedWrite},
    {"zrevrange", CommandKind::kZRevRange, 4, 0, 1, 1, 1, kKeyedRead},
    {"zrevrangebylex", CommandKind::kZRevRangeByLex, 4, 0, 1, 1, 1, kKeyedRead},
    {"zrevrangebyscore", CommandKind::kZRevRangeByScore, 4, 0, 1, 1, 1,
     kKeyedRead},
    {"zrevrank", CommandKind::kZRevRank, 3, 0, 1, 1, 1, kKeyedRead},
    {"zscan", CommandKind::kZScan, 3, 0, 1, 1, 1, kKeyedRead},
    {"zscore", CommandKind::kZScore, 3, 3, 1, 1, 1, kKeyedRead},
    {"zdiff", CommandKind::kZDiff, 3, 0, 2, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zdiffstore", CommandKind::kZDiffStore, 4, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zinter", CommandKind::kZInter, 3, 0, 2, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zintercard", CommandKind::kZInterCard, 3, 0, 2, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zinterstore", CommandKind::kZInterStore, 4, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zunion", CommandKind::kZUnion, 3, 0, 2, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"zunionstore", CommandKind::kZUnionStore, 4, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"geoadd", CommandKind::kGeoAdd, 5, 0, 1, 1, 1, kKeyedWrite},
    {"geodist", CommandKind::kGeoDist, 4, 5, 1, 1, 1, kKeyedRead},
    {"geohash", CommandKind::kGeoHash, 2, 0, 1, 1, 1, kKeyedRead},
    {"geopos", CommandKind::kGeoPos, 2, 0, 1, 1, 1, kKeyedRead},
    {"georadius", CommandKind::kGeoRadius, 6, 0, 1, 1, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"georadius_ro", CommandKind::kGeoRadiusRo, 6, 0, 1, 1, 1, kKeyedRead},
    {"georadiusbymember", CommandKind::kGeoRadiusByMember, 5, 0, 1, 1, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"georadiusbymember_ro", CommandKind::kGeoRadiusByMemberRo, 5, 0, 1, 1, 1,
     kKeyedRead},
    {"geosearch", CommandKind::kGeoSearch, 7, 0, 1, 1, 1, kKeyedRead},
    {"geosearchstore", CommandKind::kGeoSearchStore, 8, 0, 1, 2, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard},
    {"xadd", CommandKind::kXAdd, 5, 0, 1, 1, 1, kKeyedWrite},
    {"xdel", CommandKind::kXDel, 3, 0, 1, 1, 1, kKeyedWrite},
    {"xlen", CommandKind::kXLen, 2, 2, 1, 1, 1, kKeyedRead},
    {"xrange", CommandKind::kXRange, 4, 6, 1, 1, 1, kKeyedRead},
    {"xrevrange", CommandKind::kXRevRange, 4, 6, 1, 1, 1, kKeyedRead},
    {"xtrim", CommandKind::kXTrim, 4, 7, 1, 1, 1, kKeyedWrite},
    {"xsetid", CommandKind::kXSetId, 3, 7, 1, 1, 1, kKeyedWrite},
    {"xgroup", CommandKind::kXGroup, 2, 8, 2, 2, 1,
     kKeyedWrite | kCmdMovableKeys},
    {"xack", CommandKind::kXAck, 4, 0, 1, 1, 1, kKeyedWrite},
    {"xpending", CommandKind::kXPending, 3, 9, 1, 1, 1, kKeyedRead},
    {"xclaim", CommandKind::kXClaim, 6, 0, 1, 1, 1, kKeyedWrite},
    {"xautoclaim", CommandKind::kXAutoClaim, 6, 9, 1, 1, 1, kKeyedWrite},
    {"xinfo", CommandKind::kXInfo, 2, 6, 2, 2, 1, kKeyedRead | kCmdMovableKeys},
    {"xread", CommandKind::kXRead, 4, 0, 0, 0, 1,
     kCmdReadOnly | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys |
         kCmdMayBlock},
    {"xreadgroup", CommandKind::kXReadGroup, 7, 0, 0, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys |
         kCmdMayBlock},
    {"strlen", CommandKind::kStrlen, 2, 2, 1, 1, 1, kKeyedRead},
    {"incr", CommandKind::kIncr, 2, 2, 1, 1, 1, kKeyedWrite},
    {"incrby", CommandKind::kIncrBy, 3, 3, 1, 1, 1, kKeyedWrite},
    {"incrbyfloat", CommandKind::kIncrByFloat, 3, 3, 1, 1, 1, kKeyedWrite},
    {"decr", CommandKind::kDecr, 2, 2, 1, 1, 1, kKeyedWrite},
    {"decrby", CommandKind::kDecrBy, 3, 3, 1, 1, 1, kKeyedWrite},
    {"expire", CommandKind::kExpire, 3, 4, 1, 1, 1, kKeyedWrite},
    {"pexpire", CommandKind::kPExpire, 3, 4, 1, 1, 1, kKeyedWrite},
    {"expireat", CommandKind::kExpireAt, 3, 4, 1, 1, 1, kKeyedWrite},
    {"pexpireat", CommandKind::kPExpireAt, 3, 4, 1, 1, 1, kKeyedWrite},
    {"persist", CommandKind::kPersist, 2, 2, 1, 1, 1, kKeyedWrite},
    {"ttl", CommandKind::kTtl, 2, 2, 1, 1, 1, kKeyedRead},
    {"pttl", CommandKind::kPttl, 2, 2, 1, 1, 1, kKeyedRead},
    {"expiretime", CommandKind::kExpireTime, 2, 2, 1, 1, 1, kKeyedRead},
    {"pexpiretime", CommandKind::kPExpireTime, 2, 2, 1, 1, 1, kKeyedRead},
    {"del", CommandKind::kDel, 2, 0, 1, -1, 1, kKeyedWrite | kCmdMultiShard},
    {"unlink", CommandKind::kUnlink, 2, 0, 1, -1, 1,
     kKeyedWrite | kCmdMultiShard},
    {"rename", CommandKind::kRename, 3, 3, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"renamenx", CommandKind::kRenameNx, 3, 3, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"copy", CommandKind::kCopy, 3, 0, 1, 2, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard},
    {"exists", CommandKind::kExists, 2, 0, 1, -1, 1,
     kKeyedRead | kCmdMultiShard},
    {"touch", CommandKind::kTouch, 2, 0, 1, -1, 1, kKeyedRead | kCmdMultiShard},
    {"mset", CommandKind::kMSet, 3, 0, 1, -1, 2, kKeyedWrite | kCmdMultiShard},
    {"msetnx", CommandKind::kMSetNx, 3, 0, 1, -1, 2,
     kKeyedWrite | kCmdMultiShard},
    {"mget", CommandKind::kMGet, 2, 0, 1, -1, 1, kKeyedRead | kCmdMultiShard},
    {"multi", CommandKind::kMulti, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"exec", CommandKind::kExec, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"discard", CommandKind::kDiscard, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"watch", CommandKind::kWatch, 2, 0, 1, -1, 1, kCmdReadOnly},
    {"unwatch", CommandKind::kUnwatch, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"replicaof", CommandKind::kReplicaOf, 3, 3, 0, 0, 1,
     kCmdNoKeys | kCmdGlobal},
    {"info", CommandKind::kInfo, 1, 2, 0, 0, 1, kCmdNoKeys | kCmdReadOnly},
    {"cluster", CommandKind::kCluster, 2, 2, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
    {"command", CommandKind::kCommand, 1, 0, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
    {"readonly", CommandKind::kReadOnly, 1, 1, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
    {"readwrite", CommandKind::kReadWrite, 1, 1, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
    {"keys", CommandKind::kKeys, 2, 2, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
    {"tombraider", CommandKind::kTombRaider, 2, 3, 0, 0, 1,
     kCmdNoKeys | kCmdGlobal},
    {"defrag", CommandKind::kDefrag, 2, 3, 0, 0, 1, kCmdNoKeys | kCmdGlobal},
};

consteval bool CommandTableCoversEveryKind() {
  for (std::size_t value = 0;
       value < static_cast<std::size_t>(CommandKind::kUnknown); ++value) {
    bool found = false;
    for (const CommandSpec& spec : kCommandTable) {
      if (static_cast<std::size_t>(spec.kind_) == value) {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }
  return true;
}

static_assert(CommandTableCoversEveryKind());

bool EqualsIgnoreCase(std::string_view name, std::string_view lower) {
  if (name.size() != lower.size()) {
    return false;
  }
  for (std::size_t i = 0; i < name.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(name[i]);
    if (c >= 'A' && c <= 'Z') {
      c += 'a' - 'A';
    }
    if (c != static_cast<unsigned char>(lower[i])) {
      return false;
    }
  }
  return true;
}

}  // namespace

const CommandSpec* FindCommand(std::string_view name) {
  for (const CommandSpec& spec : kCommandTable) {
    if (EqualsIgnoreCase(name, spec.name_)) {
      return &spec;
    }
  }
  return nullptr;
}

std::span<const CommandSpec> CommandSpecs() noexcept { return kCommandTable; }

std::string_view CommandCanonicalName(CommandKind kind) noexcept {
  for (const CommandSpec& spec : kCommandTable) {
    if (spec.kind_ == kind) return spec.name_;
  }
  return "unknown";
}

absl::StatusOr<KeyIndexView> DetermineKeys(const CommandSpec& spec,
                                           std::size_t argc) {
  if (argc < spec.min_args_ || (spec.max_args_ != 0 && argc > spec.max_args_)) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "wrong number of arguments for '" +
                            std::string(spec.name_) + "' command");
  }
  KeyIndexView view;
  if (spec.first_key_ == 0) {
    return view;
  }
  const std::int64_t last =
      spec.last_key_ >= 0 ? static_cast<std::int64_t>(spec.last_key_)
                          : static_cast<std::int64_t>(argc) + spec.last_key_;
  if (last < spec.first_key_ || last >= static_cast<std::int64_t>(argc)) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "wrong number of arguments for '" +
                            std::string(spec.name_) + "' command");
  }
  view.first_ = spec.first_key_;
  view.last_ = static_cast<std::uint16_t>(last);
  view.step_ = spec.key_step_;
  return view;
}

absl::StatusOr<KeyIndexView> DetermineKeys(const CommandSpec& spec,
                                           std::span<const std::string> args) {
  if ((spec.flags_ & kCmdMovableKeys) == 0) {
    return DetermineKeys(spec, args.size());
  }
  if (args.size() < spec.min_args_ ||
      (spec.max_args_ != 0 && args.size() > spec.max_args_)) {
    return absl::InvalidArgumentError("wrong number of arguments for '" +
                                      std::string(spec.name_) + "' command");
  }
  if (spec.kind_ == CommandKind::kXGroup || spec.kind_ == CommandKind::kXInfo) {
    if (args.size() == 2 && EqualsIgnoreCase(args[1], "help")) {
      return KeyIndexView{};
    }
    return DetermineKeys(spec, args.size());
  }
  const std::size_t count_arg =
      spec.kind_ == CommandKind::kBLMPop || spec.kind_ == CommandKind::kBZMPop
          ? 2
          : 1;
  if (spec.kind_ == CommandKind::kXRead ||
      spec.kind_ == CommandKind::kXReadGroup) {
    std::size_t streams = args.size();
    const std::size_t option_begin =
        spec.kind_ == CommandKind::kXReadGroup ? 4 : 1;
    for (std::size_t i = option_begin; i < args.size(); ++i) {
      if (EqualsIgnoreCase(args[i], "streams")) {
        streams = i;
        break;
      }
    }
    const std::size_t remaining =
        args.size() - std::min(streams + 1, args.size());
    if (streams == args.size() || remaining < 2 || remaining % 2 != 0) {
      const std::string_view command =
          spec.kind_ == CommandKind::kXReadGroup ? "xreadgroup" : "xread";
      const std::string_view special =
          spec.kind_ == CommandKind::kXReadGroup ? ">" : "$";
      return absl::InvalidArgumentError(
          absl::StrCat("Unbalanced '", command,
                       "' list of streams: for each stream key an ID or '",
                       special, "' must be specified."));
    }
    const std::size_t keys = remaining / 2;
    const std::size_t max_key_index = std::numeric_limits<std::uint16_t>::max();
    if (streams + 1 > max_key_index || keys > max_key_index - streams) {
      return absl::InvalidArgumentError(
          "too many arguments for stream key routing");
    }
    return KeyIndexView{
        .first_ = static_cast<std::uint16_t>(streams + 1),
        .last_ = static_cast<std::uint16_t>(streams + keys),
        .step_ = 1,
    };
  }
  if (spec.kind_ == CommandKind::kGeoRadius ||
      spec.kind_ == CommandKind::kGeoRadiusByMember) {
    // The optional STORE/STOREDIST destination is parsed by the GEO
    // transaction handler.  This view identifies the mandatory source.
    return KeyIndexView{.first_ = 1, .last_ = 1, .step_ = 1};
  }
  const bool zset_aggregate = spec.kind_ == CommandKind::kZDiff ||
                              spec.kind_ == CommandKind::kZDiffStore ||
                              spec.kind_ == CommandKind::kZInter ||
                              spec.kind_ == CommandKind::kZInterCard ||
                              spec.kind_ == CommandKind::kZInterStore ||
                              spec.kind_ == CommandKind::kZUnion ||
                              spec.kind_ == CommandKind::kZUnionStore;
  if (zset_aggregate) {
    const bool store = spec.kind_ == CommandKind::kZDiffStore ||
                       spec.kind_ == CommandKind::kZInterStore ||
                       spec.kind_ == CommandKind::kZUnionStore;
    const std::size_t count_index = store ? 2 : 1;
    std::int64_t parsed_key_count = 0;
    const std::string_view text = args[count_index];
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(),
                                        parsed_key_count);
    const std::size_t first = count_index + 1;
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    }
    if (parsed_key_count < 1) {
      return absl::InvalidArgumentError(absl::StrCat(
          "at least 1 input key is needed for '", spec.name_, "' command"));
    }
    const std::uint64_t key_count =
        static_cast<std::uint64_t>(parsed_key_count);
    if (key_count > args.size() - first ||
        first + key_count - 1 > std::numeric_limits<std::uint16_t>::max()) {
      return absl::InvalidArgumentError("syntax error");
    }
    return KeyIndexView{
        .first_ = static_cast<std::uint16_t>(first),
        .last_ = static_cast<std::uint16_t>(first + key_count - 1),
        .step_ = 1,
    };
  }
  std::uint64_t count = 0;
  const std::string_view text = args[count_arg];
  const bool zset_mpop = spec.kind_ == CommandKind::kZMPop ||
                         spec.kind_ == CommandKind::kBZMPop;
  if (zset_mpop) {
    std::int64_t signed_count = 0;
    if (!ParseRedisInt64(text, &signed_count) || signed_count <= 0) {
      return absl::InvalidArgumentError("numkeys should be greater than 0");
    }
  }
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), count);
  const std::size_t first = count_arg + 1;
  const bool sintercard = spec.kind_ == CommandKind::kSInterCard;
  if (sintercard && (parsed.ec != std::errc{} ||
                     parsed.ptr != text.data() + text.size() || count == 0)) {
    return absl::InvalidArgumentError("numkeys should be greater than 0");
  }
  const bool enough_arguments =
      sintercard ? count <= args.size() - first : count < args.size() - first;
  const std::size_t max_key_index = std::numeric_limits<std::uint16_t>::max();
  const bool representable_key_range = count != 0 && first <= max_key_index &&
                                       count - 1 <= max_key_index - first;
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
      count == 0 || !representable_key_range || !enough_arguments) {
    if (sintercard) {
      return absl::InvalidArgumentError(
          "Number of keys can't be greater than number of args");
    }
    return absl::InvalidArgumentError("wrong number of arguments for '" +
                                      std::string(spec.name_) + "' command");
  }
  if (sintercard) {
    const std::size_t after_keys = first + count;
    if ((args.size() - after_keys) % 2 != 0) {
      return absl::InvalidArgumentError("syntax error");
    }
    for (std::size_t option = after_keys; option < args.size(); option += 2) {
      if (!EqualsIgnoreCase(args[option], "limit")) {
        return absl::InvalidArgumentError("syntax error");
      }
    }
  }
  return KeyIndexView{
      .first_ = static_cast<std::uint16_t>(first),
      .last_ = static_cast<std::uint16_t>(first + count - 1),
      .step_ = 1,
  };
}

}  // namespace keylane
