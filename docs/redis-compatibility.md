# Redis compatibility target

Keylane targets Redis Open Source 7.2 semantics for every command it exposes.
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
rejected explicitly because Keylane does not persist Redis eviction metadata.

Successful `RESTORE` mutations use the normal Keylane replication log. A
relative TTL is rewritten to `RESTORE ... REPLACE ABSTTL` before publication,
so replica delay cannot extend the key lifetime; an already elapsed replacement
is propagated as a deletion. `DUMP` is read-only and is never replicated.

## Startup RDB import

`--load-rdb <path>` (or the Redis-style `load-rdb <path>` configuration
directive) imports one complete Redis RDB after Keylane storage recovery and
before network listeners open. The target Keylane dataset must be empty. This
is a one-shot migration option: remove it after the first successful startup,
because a later restart recovers the imported Keylane records and therefore no
longer has an empty target.

`--load-rdb-replace` explicitly discards the contents of every configured
`--data-file` before importing. Its configuration-file equivalent is
`load-rdb-replace yes`. Replacement first validates the complete RDB and all
storage paths; only then does it erase Keylane's fixed storage metadata and
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
warning because Keylane cannot represent them. Pre-release Module/Function
formats, databases above 15, malformed records, and unknown object types whose
boundaries cannot be determined safely are rejected. Expired keys are
validated but not inserted.

## Standalone Redis PSYNC follower

`--redis-replicaof <host> <port>` (or `redis-replicaof <host> <port>` in the
configuration file) makes Keylane a permanent read-only follower of one
standalone Redis server. This mode is separate from Keylane's native
`replicaof` protocol. It performs the Redis `PSYNC` handshake, imports a
length-delimited FULLRESYNC RDB, and then applies the single RESP replication
stream directly to storage. `SELECT`, keepalive `PING`, `REPLCONF GETACK`, and
`MULTI`/`EXEC` streams are handled explicitly.

Transient reconnects retain the Redis replication id and applied byte offset
in memory and request partial resynchronization. The cursor is deliberately
not recovered after a Keylane process restart, because it is not yet committed
atomically with storage writes; a restart therefore requests a new FULLRESYNC.
The node continues serving its last complete dataset read-only while retrying
a transient connection. If Redis requires another FULLRESYNC, reads return
`LOADING` while the replacement RDB is validated and imported.

This mode cannot be promoted with `REPLICAOF NO ONE`, cannot change upstream at
runtime, and does not accept downstream Keylane replication sessions. RDB
Module and Function records have the same skip-and-warn behavior as startup
import. Incremental commands that Keylane cannot replay stop the session and
force a retry instead of silently diverging.

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
implemented; `large-key-design.md` records constraints for a future redesign.

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
