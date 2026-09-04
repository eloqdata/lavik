# Cluster data plane

## Responsibility and boundary

This subsystem makes a Keylane process a Redis Cluster data node: it decides,
for every client command, whether this node may serve it, must redirect it to
the owning node, or must refuse it with a standard cluster error. Ordinary
requests never consult an external control plane; each node answers from its
locally committed view of slot ownership, authority, and readiness.

The module under `include/keylane/cluster/` and `src/cluster/` exposes four
seams:

- `TopologyCache` holds the committed `ServingState` and publishes it
  atomically.
- `AuthorityGuard` is the admission decision point (`Admit`) plus the
  owner-side authority re-check (`AuthorityUnchanged`).
- `ClusterRouter` is a set of pure functions over a committed state: slot to
  owning node, MOVED target, and client-facing endpoint selection.
- `ClusterControlPort` is the interface through which a control plane supplies
  target state. The static file adapter and an in-memory test adapter ship
  with the data plane; a Meta/Raft adapter implements the same interface
  without the data plane learning about consensus phases, transport, or
  licensing.

The module is free of Redis wire concerns. Wire mapping (error texts,
discovery reply shapes) lives in the Redis serving layer, and internal
authority state (terms, grants, fence reasons) never crosses into RESP. The
hash-slot function is shared with storage: keys hash with
`storage::RedisSlot` (CRC16-XMODEM with `{hashtag}` support, modulo 16384),
and a static assertion ties the two slot spaces together. Slot ownership
decides *which node* serves a key; the worker mapping inside a node
(`slot % worker_count`) is unchanged, and transaction coordination keeps its
own arbitration boundary — the cluster layer reaches it only through a
generic per-shard validator hook that knows nothing about clusters.

Cluster mode is a startup-only, process-wide choice. The runtime
(`ClusterRuntime`: topology cache, resolved announce addresses) is installed
before any listener accepts a client; in standalone mode it is null and every
cluster code path is inert.

## ServingState: the published unit of truth

A `ServingState` is an immutable snapshot built and validated as one unit:
the node table, the shard groups with their slot ranges, per-group authority
(primary identity, term, grant) and readiness (storage and population), and
the computed slot-to-group map. Construction rejects malformed node ids,
duplicate node or group ids, dangling primary or replica references, and
inverted, out-of-range, or overlapping slot ranges. Coverage may be partial:
an unbound slot is a first-class state, not an error.

Publication is a single atomic `shared_ptr` swap, so topology, grants, and
readiness always appear together. A writer mutex serializes the rare
control-plane publications, and an odd/even publication sequence brackets the
snapshot store and version update. Readers accept a snapshot/version pair only
when equal even sequence reads surround it, so they cannot pair a newly stored
snapshot with the preceding version. The cache also carries a monotonic logical
version for publication ordering and tests. A content-identical republication
(decided by a content hash over the semantic state) is a no-op that keeps the
existing snapshot, version, and sequence, so reload churn is invisible.
Authority decisions never consult the global version: admission captures the
snapshot it decided against, and the owner-side re-check compares a per-group
authority token — owner identity, term, grant, and readiness, precomputed at
build time — so an unrelated group's republication does not disturb
in-flight work.

Request-path reads go through a thread-local snapshot cache that re-reads only
the publication sequence per call; a hit returns the last observed snapshot
with no shared-memory writes at all. This keeps the admission gate free of
cross-worker serialization — a direct `atomic<shared_ptr>` load per request
would serialize on the toolchain's internal spin bit.

Readiness is per group and deliberately excludes the grant bit: a fenced
group is not "still loading", it has no safe owner. Keyed requests consult
only the groups their slots map to, so one group's recovery does not stall
traffic owned by healthy groups; keyless commands consult the aggregate.

## Admission and fencing

`Admit` is a pure function over one committed snapshot and a request view
(distinct key slots, write intent, connection READONLY state, loading
whitelist membership), so the full decision matrix is testable offline. Its
evaluation order mirrors Redis `getNodeByQuery`:

The request record retains only the first slot and, when present, one distinct
slot as a CROSSSLOT witness. More distinct slots cannot change the decision,
and every request that proceeds beyond admission therefore carries exactly one
slot without a general-purpose vector allocation or footprint.

1. Loading: no committed snapshot, or an involved group is not ready. Only
   whitelisted commands (health, discovery, configuration, subscription
   management) are served; everything else is LOADING.
2. First-key coverage: the first key's slot has no owner, or its owner is
   this node's fenced group — CLUSTERDOWN. This outranks the cross-slot
   check, matching Redis.
3. Cross-slot: the remaining keys hash to other slots — CROSSSLOT.
4. Ownership: this node's granted group owns the slot — serve; a READONLY
   connection on a replica of the owning group serves reads locally as an
   explicitly stale read; otherwise MOVED to the primary. A fenced remote
   primary still receives the redirect: the target applies its own grant
   gate and answers CLUSTERDOWN, so a redirect never lands a client on a
   writable fenced node.

Commands without keys — including commands whose key extraction fails, such
as a malformed `EVAL` numkeys — admit locally (readiness still applies) and
produce their own argument errors, the same treatment Redis gives zero-key
commands.

Admission alone cannot fence writes: a request admitted just before a
topology change could mutate afterwards. Two owner-side re-check choke points
close that window, both placed after every suspending admission (publisher
admission, database gate waits, snapshot/order gates) and immediately before
mutation:

1. Non-transactional writes re-check in `ExecuteCommandBody` after the
   database gate is held. A changed authority means nothing has executed yet,
   so the request is re-admitted against the current snapshot and answered
   honestly with a redirect or error; a benign republication re-arms the
   request with the current snapshot.
2. Transactional writes install a per-shard validator on the
   `tx::Transaction`. The hook runs on the owner shard immediately before
   every shard callback and aborts that shard when authority changed. A
   single-shard transaction provably ran nothing and gets the re-admission
   answer; a multi-shard transaction may have mutated a shard whose check
   raced the fence, so its outcome is undeterminable and the connection
   closes without a fabricated reply.

`EXEC` re-evaluates the union of its queued commands' slots at execution
time: spanning slots fails the whole transaction with CROSSSLOT, and a write
transaction whose slot moved redirects or refuses as a whole. Its
single-shard fast path, which never builds a `tx::Transaction`, re-checks
after taking the key guard. Lua `redis.call` writes re-check before
dispatch and again on the owner hop; script key access is additionally
confined to the slot set the script was admitted with. Invoking a Function
loaded with the `no-cluster` flag is refused in cluster mode, with Redis's
exact error text. Blocking commands
re-run admission on every attempt after reacquiring the database gate.
Replication replay is exempt from every re-check: applied commands are
already ordered by the replication stream and carry no client fencing
semantics.

Reads are intentionally not re-checked. A read gated at admission may observe
data committed before a concurrent fence — the same staleness window Redis
Cluster clients accept across failover. Writes have no such window: the
combination of admission, the two choke points, and per-group tokens
guarantees a stale topology causes redirection or temporary unavailability,
never a second writer.

Only writes retain ownership of the admitted snapshot across suspension
points. Reads finish their decision while the thread-local cache keeps the
snapshot alive and carry no per-request shared reference afterwards, matching
their intentionally absent owner-side re-check.

Executions register their admitted groups when they pass the re-check and
unregister at completion, so a fence publisher can observe whether any
admitted write is still running for an affected group. The accounting lives
in per-group striped atomic counters owned by the published snapshot: the
request path performs one atomic increment on the registering thread's
stripe, with no lock and no allocation, and aggregation happens off the
request path. Publication shares the replaced snapshot's counter into every
group whose authority token is unchanged, so executions admitted under
token-equal snapshots drain together, while a changed group starts a fresh
counter and fencing drains the replaced snapshot's. Registration is bracketed
by publication-sequence loads — an odd sequence covers the state/version
update and a completed publication changes the even token — so a drain either
observes a concurrent registration or the registrant observes the sequence
change and rolls back. Draining
those executions before issuing a new grant is the control plane's contract
(planned follow-up work); the data plane provides the mechanism and does not
itself wait.

## Redis wire contract

All cluster errors are simple error lines with texts verbatim from Redis 7.2;
clients dispatch on the first token, and the encodings are identical under
RESP2 and RESP3.

| Condition | Reply |
|---|---|
| Slot owned by another node | `-MOVED <slot> <host>:<port>` |
| First key's slot unbound, or self is its fenced primary | `-CLUSTERDOWN Hash slot not served` |
| Keys span multiple slots (including an `EXEC` union) | `-CROSSSLOT Keys in request don't hash to the same slot` |
| No ready committed state | `-LOADING Redis is loading the dataset in memory` |
| Transient retryable condition (e.g. flush in progress) | `-TRYAGAIN <message>` |
| `SELECT 0` | `+OK` (a no-op; every other database index is rejected) |
| `SELECT` with a nonzero index | `-ERR SELECT is not allowed in cluster mode` |
| `COPY` with a `DB` option | `-ERR Copying to another database is not allowed in cluster mode` |
| `REPLICAOF` / `ADDREPLICAOF` | `-ERR REPLICAOF not allowed in cluster mode.` |
| Unknown `CLUSTER` subcommand or wrong arity | `-ERR Unknown CLUSTER subcommand or wrong number of arguments for '<sub>'` |
| Execution outcome undeterminable | No reply; the connection is closed |

MOVED always names the owning node's concrete advertised address; the
empty-host "dial the startup node" convention exists only for the self entry
in discovery replies. Both MOVED and discovery select the port by the
requesting connection's TLS state — TLS connections receive the target's TLS
port, falling back to the plain port when the target offers no TLS — mirroring
Redis `getNodeClientPort`. Discovery replies (`CLUSTER SLOTS`/`NODES`) are
built per request from one committed snapshot and never cache encoded output.

A gate rejection inside `MULTI` marks the transaction dirty, so `EXEC` fails
with EXECABORT as in Redis. `EVAL`/`EVALSHA`/`FCALL` are treated as writes for
admission — a script that only reads still redirects to the primary — while
their `_RO` variants are treated as reads; declared keys must hash to one
slot, and `redis.call` access outside the admitted slot set is rejected with
Redis's non-local-key error. Global and administrative commands
(INFO, CONFIG, DBSIZE, FLUSHDB, SCAN, script catalog management, and similar)
keep node-local semantics and are governed only by readiness.

## CLUSTER subcommands and discovery surface

Cluster mode serves `SLOTS`, `NODES`, `MYID`, `INFO`, and `KEYSLOT` from the
committed state; every other subcommand receives Redis's unknown-subcommand
error. `KEYSLOT` is a pure function of the key and answers even before the
first state is published. `CLUSTER INFO` reports `cluster_state:ok` exactly
when slot coverage is complete, the assigned/ok slot counts, the known-node
count, the number of slot-serving primaries as `cluster_size`, and the
configuration epochs; with no gossip, the pfail/fail and message counters are
always zero. `CLUSTER NODES` emits nodes.conf-format lines with the `myself`
mark on the local entry and no bus-port semantics (`@0`).

`HELLO` reports `mode:cluster`, `INFO` reports `redis_mode:cluster` in its
Server section, and a `# Cluster` section carries `cluster_enabled:1`, so
standard clients and Sentinel-style tooling detect the mode. Standalone mode
is unchanged: it keeps the legacy replication-derived `CLUSTER NODES`/`SLOTS`
shim that fakes full coverage, and its replica-redirect MOVED shim never runs
in cluster mode because the two topology sources are mutually exclusive.

## Control ports and configuration

`ClusterControlPort::RefreshTarget` reads the control source's target state
and publishes it only when the content changed; on any error the previously
published state stays in effect, so a fencing transition is only ever
published complete.

`StaticClusterControl` loads a Redis `nodes.conf`-format file shared by every
node. The file carries no `myself` mark; the local entry is identified by
matching the process's bind host and port against node lines (wildcard binds
match on port alone) and must match exactly one entry, or startup validation
fails. Migration markers (`[slot-<-id]`, `[slot->-id]`) are rejected outright
rather than silently misparsed — there is no importing/migrating flow to give
them meaning. The bus port and gossip bookkeeping fields are validated and
dropped; replica wiring is validated before groups are assembled. Only
slot-owning primaries form groups, with the primary's node id as the group
id, and statically configured primaries hold a permanent grant. Readiness
follows storage recovery: the first publication is not ready, so the gate
answers LOADING until recovery (and any startup RDB import) completes.

The static adapter loads once at startup — a first-load failure is fatal —
and reloads on SIGHUP, which wakes the main loop through its own eventfd
(the shutdown eventfd treats any write as a stop request and is never
shared). A failed reload keeps the previously published state; an identical
file republishes nothing. Node ids come from the file, keeping `MYID` stable
across restarts, and the file carries only data ports: every node is assumed
to serve TLS on one cluster-wide configured port. `InMemoryClusterControl`
publishes programmatically built states — including fenced or not-ready
states the static adapter never produces — through the same seam for
in-process tests.

Five startup-only directives configure the subsystem: `cluster-enabled`
(default `no`), `cluster-static-nodes-file`, `cluster-announce-ip`,
`cluster-announce-port`, and `cluster-announce-tls-port`. Announce values
default to the first non-wildcard bind address and the corresponding listen
ports; a wildcard bind leaves the announce host empty so discovery self
entries keep the startup-node convention. Validation requires a nodes file
when cluster mode is enabled, refuses coexistence with either replication
upstream directive (two topology sources never mix; runtime `REPLICAOF` is
rejected separately at the command layer), and requires at least one
reachable announced client port so MOVED and discovery can always name an
endpoint — a TLS-only deployment is valid.

## Current scope limits

The data plane deliberately excludes: ASK/ASKING and the
importing/migrating compatibility flow, the gossip bus protocol,
Meta-driven control (planned follow-up work, over the same control-port seam),
drain-before-new-grant enforcement (planned follow-up work consuming the
in-flight counters here), shard Pub/Sub, non-uniform TLS ports, dynamic
node-id allocation, and the remaining `CLUSTER` management subcommands
(`SETSLOT`, `MEET`, `FAILOVER`, `ADDSLOTS`, and similar).

## Verification

The admission decision matrix, builder and parser validation, content-hash
publication semantics, TLS-aware endpoint selection, in-flight counter
concurrency and cell sharing, and the CLUSTER wire texts and reply shapes are
unit-tested offline through the pure seams and the in-memory adapter. The
three-node
static-cluster end-to-end binary is registered but currently a placeholder;
multi-process discovery, redirect, and SIGHUP-driven fence coverage is
pending.

## Source map

| Claim | Repository source |
|---|---|
| ServingState model, builder validation, topology cache, content hash, striped in-flight cells, and routing functions | `include/keylane/cluster/topology.h`, `src/cluster/topology.cpp` |
| Admission decision and owner-side authority re-check | `include/keylane/cluster/authority.h`, `src/cluster/authority.cpp` |
| Control-port seam, nodes.conf adapter, and in-memory adapter | `include/keylane/cluster/control_port.h`, `src/cluster/control_port.cpp` |
| Process-wide runtime installation | `include/keylane/cluster/runtime.h`, `src/cluster/runtime.cpp` |
| Cluster admission gate, owner re-check choke points, EXEC/Lua/blocking integration, and mode-restricted command policies | `src/redis/command.cpp`, `src/redis/blocking_wait.cpp` |
| CLUSTER subcommands and discovery replies | `src/redis/cluster_command.cpp`, `src/redis/cluster_command.h` |
| Cluster wire error texts | `include/keylane/resp.h`, `src/redis/resp.cpp` |
| Per-shard transaction validator hook | `include/keylane/tx/transaction.h`, `src/tx/transaction.cpp` |
| Startup wiring, storage-ready publication, and SIGHUP reload | `src/redis/server.cpp` |
| Cluster configuration directives and validation | `include/keylane/server.h`, `src/config.cpp`, `app/keylane.cpp` |
| Decision matrix, parser, publication, and concurrency unit tests | `tests/cluster_authority_test.cpp`, `tests/cluster_control_port_test.cpp`, `tests/cluster_topology_test.cpp`, `tests/cluster_command_test.cpp` |
| Static-cluster end-to-end binary (registered placeholder; suite pending) | `tests/cluster_e2e_test.cpp` |
