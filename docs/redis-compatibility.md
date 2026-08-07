# Redis compatibility target

Keylane targets Redis Open Source 7.2 semantics for every command it exposes.
This is a command-level compatibility promise: unsupported Redis 7.2 commands
remain unsupported, while supported commands must match Redis 7.2 syntax,
atomicity, errors, and RESP2 replies.

The current string and expiration surface is:

- `SET key value [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]`
- `GET`, `INCR`, `DEL`, and `EXISTS`
- `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, and `PERSIST`
- Redis 7.2 `EXPIRE`/`PEXPIRE` conditions: `NX`, `XX`, `GT`, and `LT`

Redis 8.4 comparison options (`IFEQ`, `IFNE`, `IFDEQ`, and `IFDNE`) are not
part of this target.

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
