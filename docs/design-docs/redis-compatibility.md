<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Redis compatibility target

Lavik targets Redis Open Source 7.2 semantics for every command it exposes.
This is a command-level compatibility promise: unsupported Redis 7.2 commands
remain unsupported, while supported commands must match Redis 7.2 syntax,
atomicity, errors, and RESP2 replies.

The Redis 7.2 String command group is complete: `APPEND`, `DECR`, `DECRBY`,
`GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`,
`INCRBYFLOAT`, `LCS`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET`, `SETEX`,
`SETNX`, `SETRANGE`, `STRLEN`, and `SUBSTR`. `SET` supports
`NX`/`XX`, `GET`, `EX`/`PX`/`EXAT`/`PXAT`, and `KEEPTTL`.

The Redis 7.2 Bitmap surface is also complete: `GETBIT`, `SETBIT`,
`BITCOUNT` (including `BYTE` and `BIT` ranges), `BITPOS`, `BITFIELD`,
`BITFIELD_RO`, and atomic cross-shard `BITOP`. Bitmap values use the ordinary
String representation and therefore interoperate directly with String
commands.

The current key and expiration surface includes:

- `DEL`, `UNLINK`, `RENAME`, `RENAMENX`, `COPY`, `EXISTS`, `TOUCH`,
  `RANDOMKEY`, `TYPE`, `DUMP`, `RESTORE`, and `SCAN` with `TYPE` filtering
- `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `EXPIRE`, `PEXPIRE`,
  `EXPIREAT`, `PEXPIREAT`, and `PERSIST`
- Redis 7.2 `EXPIRE`/`PEXPIRE` conditions: `NX`, `XX`, `GT`, and `LT`

Redis 8.4 comparison options (`IFEQ`, `IFNE`, `IFDEQ`, and `IFDNE`) are not
part of this target.

`DUMP` emits a Redis RDB version 11 object payload and, like Redis, does not
include the key TTL. `RESTORE` accepts RDB payload versions 1 through 11 and
the historical Redis encodings needed by String, List, Set, Hash, Sorted Set,
and Stream values (integer/LZF strings, zipmap, ziplist, intset, quicklist,
listpack, and all three Stream listpack layouts). The `ttl` argument supplies
the new relative deadline; `ABSTTL` makes it an absolute Unix millisecond
deadline, and `REPLACE` is atomic with the write. Redis Module values and RDB
types introduced after Redis 7.2 are unsupported. `IDLETIME` and `FREQ` are
rejected explicitly because Lavik does not persist Redis eviction metadata.

Successful `RESTORE` mutations use the normal Lavik replication log. A
relative TTL is rewritten to `RESTORE ... REPLACE ABSTTL` before publication,
so replica delay cannot extend the key lifetime; an already elapsed replacement
is propagated as a deletion. `DUMP` is read-only and is never replicated.

## Startup RDB import

`--load-rdb <path>` (or the Redis-style `load-rdb <path>` configuration
directive) imports one complete Redis RDB after Lavik storage recovery and
before network listeners open. The target Lavik dataset must be empty. This
is a one-shot migration option: remove it after the first successful startup,
because a later restart recovers the imported Lavik records and therefore no
longer has an empty target.

`--load-rdb-replace` explicitly discards the contents of every configured
`--data-file` before importing. Its configuration-file equivalent is
`load-rdb-replace yes`. Replacement first validates the complete RDB and all
storage paths; only then does it erase Lavik's fixed storage metadata and
assign a new storage-set identity. Old data blocks are made unreachable, not
securely overwritten. The reset is destructive and cannot be atomic across
multiple devices, so retain the source RDB until startup succeeds and remove
both RDB options afterward.

The complete-file reader accepts RDB versions 1 through 11, including the
historical object encodings supported by `RESTORE`, logical databases 0 through
15, absolute expirations, AUX fields, resize hints, and LRU/LFU metadata. It
validates the file checksum before writing any keys and performs a complete
object-validation pass before the import pass. Self-describing Redis Function
libraries, Module 2 values, and Module auxiliary records are skipped with a
warning because Lavik cannot represent them. Pre-release Module/Function
formats, databases above 15, malformed records, and unknown object types whose
boundaries cannot be determined safely are rejected. Expired keys are
validated but not inserted.

## Redis PSYNC follower

`--redis-replicaof <host> <port>` (or `redis-replicaof <host> <port>` in the
configuration file) explicitly starts Lavik as a read-only Redis follower.
Runtime `REPLICAOF <host> <port>` and the ordinary `replicaof` configuration
directive automatically distinguish Lavik from Redis: Lavik first attempts
`KLPSYNC`, and falls back to Redis only when the peer explicitly returns an
unknown-command error for `KLPSYNC`. Authentication, network, and other
protocol errors are never treated as protocol detection failures.

Redis mode imports a length-delimited FULLRESYNC RDB and then applies the RESP
replication stream directly to storage. `SELECT`, keepalive `PING`, `REPLCONF
GETACK`, and `MULTI`/`EXEC` streams are handled explicitly. Transient reconnects
retain the Redis replication id and byte offset in memory and request partial
resynchronization. These cursors are not recovered after a Lavik process
restart because storage writes and offsets do not yet share a crash-atomic
commit record; restart therefore requests a new FULLRESYNC.

When the endpoint is a slot-owning Redis Cluster master, Lavik reads
`CLUSTER NODES` and freezes the complete 0-16383 slot layout.
`ADDREPLICAOF <host> <port>` registers each remaining master. Every addition
must report the same layout and a slot set disjoint from all registered
sources; overlap is rejected before starting a session or mutating storage.
Lavik remains `LOADING` until every advertised slot-owning master is
registered and synchronized. Each master sees the same Lavik listening port
as a downstream replica because every PSYNC connection sends `REPLCONF
listening-port`.

Each source has an independent replication id, offset, ACK stream, and
reconnect loop. Cluster FULLRESYNC advances durable replication epochs and
detaches indexes only for that source's slots. RDB keys outside those slots
stop the session as a topology error. Stale physical records are reclaimed by
normal detached-index and defragmentation work; the record format and other
masters' slots do not change. Incremental commands are checked through the
command table so all keys belong to the source. Cluster `FLUSHDB`/`FLUSHALL`
becomes a reset of that source's slots, never a global flush.

The last complete dataset remains readable during an ordinary disconnected
partial-resync retry. A required FULLRESYNC makes the node `LOADING` while that
source's slots are rebuilt. A background topology check accepts failover when
a new master owns exactly the same slot set, switches the endpoint, and tries
PSYNC. Two consecutive observations of migrating/importing, overlapping,
incomplete, or changed slot ownership stop all Redis sources and leave Lavik
in `LOADING`; re-run `REPLICAOF` and the required `ADDREPLICAOF` commands after
the cluster reaches a new stable layout. Online resharding is intentionally not
merged.

While attached to Redis, Lavik does not accept downstream Lavik
replication sessions. `REPLICAOF NO ONE` disconnects all sources, retains a
complete dataset, enables expiration authority and local writes, and makes the
node an independent Lavik source. A later `REPLICAOF <host> <port>` may
follow either Lavik or Redis through the same safe detection. The command is
local role control and is never sent to the remote server. Runtime additions
and Redis cursors are not persisted, so orchestration must replay additions
after restart. RDB Module and Function records are skipped with a warning;
incremental commands Lavik cannot replay stop the session instead of
silently diverging.

Redis 7.2's wire formatting is part of the compatibility target. Sorted Set
scores use the shortest round-trip digits with Redis 7.2 `fpconv_dtoa`'s
fixed-versus-scientific notation and unpadded exponent spelling.
Integer-valued doubles in Redis's conservative `double2ll` range (from
`-LLONG_MAX/2` through `LLONG_MAX/2`) use ordinary decimal integer notation
instead. `GEODIST` uses four digits after the decimal point, while `GEOPOS`
uses human-readable 17-place formatting with trailing fractional zeroes
removed. These paths intentionally do not share one generic floating-point
formatter because Redis 7.2 does not format them the same way.

Command-level regression cases should compare complete RESP2 bytes with Redis
Open Source 7.2. This includes the reply container type, null array versus null
bulk string, precise error text, and state after rejected commands. A newer
Redis server is not an interchangeable oracle: floating-point replies and
some Stream result shapes changed after 7.2.

The implemented connection-management subset is `CLIENT ID`, `CLIENT LIST`
with optional `TYPE NORMAL|REPLICA`, and `CLIENT KILL` using the legacy address
form or `ID`, `ADDR`, `TYPE NORMAL|REPLICA`, and `SKIPME YES|NO` filters.
Replication control and per-source-worker data-flow sockets have `flags=S` and
type `REPLICA`; their numeric peer endpoint remains attached when a socket is
adopted by another worker. Killing any replication socket expands to all
sockets carrying the same replication session, but does not change
`REPLICAOF`, so the replica reconnects normally.
Other Redis `CLIENT` subcommands and client types are not yet exposed.

## Collection storage

List, Hash, Set, Sorted Set, geospatial index, and Stream values are stored as
single atomic records. A command decodes, modifies, and rewrites one complete
key while holding its intent lock. Large-key splitting is not currently
implemented; [large-key redesign constraints](large-key-design.md)
record requirements for a future implementation.

The implemented Sorted Set surface is `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`,
`ZLEXCOUNT`, `ZMPOP`, `ZMSCORE`, `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER`,
`ZRANGE`, `ZRANGESTORE`, and the legacy range aliases, `ZRANK`, `ZREVRANK`,
`ZREM`, the three `ZREMRANGE*` commands, `ZSCAN`, and `ZSCORE`. Blocking pops
are available through `BZMPOP`, `BZPOPMIN`, and `BZPOPMAX`.
Cross-key `ZDIFF`, `ZINTER`, `ZINTERCARD`, and `ZUNION`, including their
`*STORE` forms, use the same distributed intent-lock transaction path as Set
algebra commands.

Negative-count `HRANDFIELD`, `SRANDMEMBER`, and `ZRANDMEMBER` replies larger
than 1000 samples are streamed in bounded batches. Both ordinary commands and
EXEC capture the collection's compact value once, release DB/key or transaction
locks, and drain the RESP reply from that immutable view in the same bounded
batches. The requested count therefore does not determine working-set memory,
concurrent deletion cannot fabricate replacement samples after the array header
has been sent, and slow clients do not retain storage locks.

Geospatial indexes reuse the Sorted Set representation and expose `GEOADD`,
`GEODIST`, `GEOHASH`, `GEOPOS`, `GEORADIUS`, `GEORADIUSBYMEMBER`, and
`GEOSEARCH`. The legacy radius commands support `STORE`/`STOREDIST`, and
`GEOSEARCHSTORE` is implemented on the same cross-key transaction path.

The Stream surface is `XADD`, `XDEL`, `XLEN`, `XRANGE`, `XREVRANGE`, `XTRIM`,
`XSETID`, `XREAD`, `XREADGROUP`, `XGROUP`, `XACK`, `XPENDING`, `XCLAIM`,
`XAUTOCLAIM`, and `XINFO`. Blocking List and Stream commands share a per-shard
wait registry. A key owner maintains its local FIFO lanes and sends readiness
events to the waiting command's worker; waiter state is therefore worker-local
and needs no mutex. List and consumer-group lanes wake one waiter at a time,
while non-consuming `XREAD` uses a private broadcast lane and an event-ID
predicate so every reader whose cursor is behind the append is rechecked.
Commands release the DB gate while suspended and always recheck storage after
registration or wakeup, which closes both check/register and flush races.

## Type and expiration metadata

Every indexed value carries a stable Redis `ValueType` and an absolute Unix
millisecond `expire_at_ms` in memory, on disk, and over partition replication.
Zero means persistent. Expiration belongs to the top-level key, so future list,
set, sorted-set, hash, and stream roots reuse the same expiration machinery.

Reads hide a value as soon as its absolute deadline is reached. One bounded
active-expiration coroutine per authoritative worker scans only maps containing
volatile keys and appends normal tombstones after revalidating the key mutation
identity. Replicas hide expired values locally but apply the primary's
replicated tombstone instead of creating an independent mutation sequence.

The on-disk format number remains `1` during development even when its layout
changes. Existing development data files must be recreated after such a change;
backward-compatible migrations begin only after the format is declared stable.
