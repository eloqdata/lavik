# Issue 41 research: Redis Cluster recovery and repeated failover

Status: point-in-time source audit for issue 41; this is not current Keylane
architecture documentation.

Research snapshot: 2026-09-12. The source references below use Redis upstream
commit [`669b2a1316f5b35ecf964281b77c054ff28dc934`](https://github.com/redis/redis/commit/669b2a1316f5b35ecf964281b77c054ff28dc934)
(`unstable`, reporting version `8.9.241`) so that line references do not drift.
The Redis documentation links are the official current documentation.

## Questions

1. After a failover, does Redis Cluster resynchronize the other replicas
   serially or concurrently? What limits exist, and when is an old usable data
   set discarded?
2. If the newly promoted primary fails while replicas are resynchronizing, how
   does Redis decide which replica may be promoted? Does it compare offsets
   across replication histories, and can an old-lineage replica take over?

## Executive findings

- Redis Cluster has no Sentinel-like `parallel-syncs` workflow and no durable
  rebuild queue. Every node that learns the newer slot configuration changes
  its replication target locally. Several replicas can therefore reconnect and
  synchronize at the same time.
- A normal same-shard failover is expected to use partial synchronization, not
  FULL. The promoted replica retains the former primary's replication ID as its
  secondary ID and keeps offsets continuous, specifically so siblings with the
  old ID can PSYNC from it.
- If FULL is required, the primary intentionally coalesces concurrent requests:
  disk-backed mode can serve multiple replicas from one generated RDB, and
  diskless mode streams one generated RDB to a batch of replicas concurrently.
  `repl-diskless-sync-max-replicas` is an early-start threshold, not a
  concurrency cap.
- Merely changing the replication target does not flush the replica's old data.
  With the default replica load mode (`repl-diskless-load disabled`), Redis
  receives the complete RDB into a temporary file before it attaches to the new
  history and replaces the old in-memory data. `swapdb` preserves the old data
  until a complete in-memory replacement is ready. `flushdb` discards it before
  socket loading and explicitly carries a data-loss warning if loading fails.
- Cluster election messages contain a replication offset but no replication
  ID. Eligibility is based on current topology, the failure of the replica's
  current primary, configurable data age, and voting/configuration epochs.
  Offset only biases election start time; it is not a strict winner selection.
- Consequently Redis does **not** partition same-shard candidates by
  replication history during election. Under normal one-step failover this is
  made workable by PSYNC2's one-generation history bridge and continuous
  offsets. A replica still holding the former primary's history can take over
  after it has learned that it is a replica of the latest primary, subject to
  the ordinary freshness gate. Writes accepted only by the failed promoted
  primary may then be lost, which is within Redis Cluster's documented
  asynchronous, last-failover-wins semantics.
- Current upstream treats a replica moved from a **different shard** more
  carefully: it discards the unrelated cached replication state, reports
  offset zero, and makes its data age effectively infinite. With the default
  nonzero validity factor it cannot automatically fail over its new primary
  until its first synchronization completes. This is a shard guard, not a
  general same-shard replication-history comparison.

## Q48: recovery after cutover

### Explicit behavior

#### Reconfiguration is local and asynchronous

After a node observes that a higher-epoch primary took the old primary's slots,
it calls `clusterSetMaster()` and immediately points replication at that node;
there is no Meta-like coordinator or ordered rebuild list
([`cluster_legacy.c` lines 2499-2517](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L2499-L2517),
[`cluster_legacy.c` lines 5423-5454](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L5423-L5454)).
The cluster specification likewise says replicas of the old primary reconfigure
themselves when they learn the newer slot configuration, and unreachable nodes
do so after they later receive gossip or an UPDATE
([cluster specification: replica election and promotion](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#replica-election-and-promotion)).

`replicationSetMaster()` starts connecting immediately. When a primary is
demoted, it synthesizes a cached upstream identity from its current ID and
offset, and deliberately keeps any downstream replicas connected until it
knows whether the new upstream permits a partial sync
([`replication.c` lines 3620-3671](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L3620-L3671)).

#### Same-shard followers normally avoid FULL

Promotion moves the old primary replication ID into the secondary-ID slot,
records the offset through which it is valid, and generates a new current ID
without resetting the global replication offset
([`replication.c` lines 2136-2153](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L2136-L2153)).
The PSYNC acceptor admits either the current ID or that secondary ID through its
valid offset, provided the requested bytes remain in the backlog
([`replication.c` lines 1001-1061](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L1001-L1061)).
On `+CONTINUE <new-id>`, a replica adopts the new ID while retaining its former
ID as secondary for its own downstream replicas
([`replication.c` lines 3063-3108](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L3063-L3108)).

This is an intentional failover mechanism, not just an optimization accidentally
available in standalone replication. The official replication documentation
states that the two IDs exist so replicas of the old primary can partially
resynchronize with a promoted replica, and concludes that they need not perform
a full sync after failover
([replication ID explained](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#replication-id-explained),
[partial sync after restarts and failovers](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#partial-sync-after-restarts-and-failovers)).

#### FULL requests are coalesced, not serialized for availability

The official replication documentation says one background save serves
multiple concurrent replica synchronization requests
([how Redis replication works](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#how-redis-replication-works)).
The implementation has the following behavior:

- A replica can attach to a compatible disk-backed BGSAVE already serving
  another replica; otherwise it waits for the next BGSAVE. With no BGSAVE, Redis
  may delay briefly to collect more diskless recipients
  ([`replication.c` lines 1335-1424](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L1335-L1424)).
- A disk-backed RDB is sent through an independent socket/write handler to each
  waiting replica after the shared snapshot is ready
  ([`replication.c` lines 2006-2115](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L2006-L2115)).
- A diskless child writes the same stream to all sockets in its current batch
  ([`replication.c` lines 1908-2003](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L1908-L2003)).
- Upstream tests intentionally connect three replicas at once under both
  disk-backed and diskless configurations
  ([`tests/integration/replication.tcl` lines 329-370](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/tests/integration/replication.tcl#L329-L370)).

`repl-diskless-sync-delay` controls how long Redis waits to collect a batch.
`repl-diskless-sync-max-replicas` causes the transfer to start *early* when
that many replicas are waiting; zero means no threshold. It does not limit the
number of simultaneous recipients
([`redis.conf` lines 663-708](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/redis.conf#L663-L708),
[`replication.c` lines 5261-5315](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L5261-L5315)).
Redis Cluster exposes no equivalent of Sentinel's `parallel-syncs` setting.

#### Point at which the old data stops being usable

Changing the target retains the data and cached replication state while Redis
first attempts PSYNC. A successful partial sync never clears the data
([`replication.c` lines 2964-2983](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L2964-L2983),
[`replication.c` lines 3439-3445](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L3439-L3445)).
For FULL, replacement depends on replica load mode:

| Replica load mode | When old data is replaced | Failure property |
|---|---|---|
| `disabled` (default) | The complete RDB is first downloaded to a temporary file. Redis attaches to the new history and starts replacing the old database only after transfer completion, immediately before loading the local RDB. | A network failure during download leaves the old in-memory data intact. |
| `swapdb` | The incoming RDB is parsed into temporary databases; cached history is discarded and the databases are swapped only after loading succeeds. | Failed loading discards the temporary database and retains the old one, at the cost of extra memory. |
| `flushdb` | The old database is flushed before the RDB is loaded directly from the socket. | A failed socket load can leave the replica with no usable complete copy. The sample configuration explicitly warns about this. |

The paths are visible in
[`replication.c` lines 2479-2647](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L2479-L2647),
and the configuration's safety descriptions are in
[`redis.conf` lines 717-743](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/redis.conf#L717-L743).
Once Redis attaches a FULL recipient to a genuinely different data set, it
discards the cached primary, disconnects chained replicas, and frees their
partial-sync backlog
([`replication.c` lines 2288-2300](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/replication.c#L2288-L2300)).

### Source-derived inference

There is no source-side token that limits one shard to one destructive FULL.
Several replicas can be in transfer/load concurrently. Redis instead reduces
the risk in two ways: same-shard failover normally stays on PSYNC, and the
default FULL path stages a complete RDB before replacing the old in-memory
copy. `swapdb` provides an even stronger atomic-replacement shape, while
`flushdb` explicitly trades that safety away.

Therefore, the closest Redis Cluster behavior for Keylane Q48 is not the
previously proposed “one FULL at a time” rule. It is:

1. immediately and independently point every non-owner at the committed owner;
2. try compatible incremental catch-up first;
3. if FULL is necessary, allow concurrent recovery but do not destroy the old
   complete image merely because recovery started; replace it only after a
   complete new image is locally available and validated.

If Keylane FULL already stages and atomically installs a whole-group snapshot,
serial admission adds a stronger availability policy than Redis Cluster rather
than matching Redis. If Keylane clears the only complete local image before a
network FULL completes, then either safe staging or serialization is needed to
avoid a failure window that Redis's default load path avoids.

## Q49: a second primary failure during recovery

### Explicit behavior

#### Eligibility is topology and freshness based

For automatic failover, a replica proceeds only when it is still a replica,
has a current primary, that primary is marked `FAIL`, failover is allowed, and
the primary serves slots. It rejects itself when the time since its last
interaction with the primary exceeds the configured freshness threshold
([`cluster_legacy.c` lines 4414-4482](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4414-L4482)).
The threshold is approximately
`repl-ping-replica-period + cluster-node-timeout * cluster-replica-validity-factor`;
the default factor is 10, while zero disables the age rejection in favor of
maximum availability
([`redis.conf` lines 1785-1835](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/redis.conf#L1785-L1835)).

The official cluster specification gives the same three prerequisites: failed
primary, nonempty slot ownership, and a replication disconnection no older
than a configurable limit
([cluster specification: replica election and promotion](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#replica-election-and-promotion)).

#### Rank is only a best-effort delay

A replica's rank is the count of eligible siblings of the same primary that
advertise a numerically greater replication offset. Rank adds one second per
position to election start time
([`cluster_legacy.c` lines 4267-4294](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4267-L4294),
[`cluster_legacy.c` lines 4484-4535](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4484-L4535)).
The specification explicitly says rank is not strict and voters do not choose
the best replica; a lower-ranked replica merely tends to request votes first
([cluster specification: replica rank](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#replica-rank),
[masters reply to replica vote request](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#masters-reply-to-replica-vote-request)).

#### Config epoch orders ownership, not data history

An election request carries the candidate's current epoch, its current
primary's config epoch, and that primary's slots. A voter requires the current
primary to be failed, refuses duplicate/old-epoch votes, and rejects a candidate
whose view of any claimed slot has an older config epoch than the voter's view
([`cluster_legacy.c` lines 4123-4138](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4123-L4138),
[`cluster_legacy.c` lines 4163-4264](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4163-L4264)).
The winner takes the slots under a new, higher config epoch
([`cluster_legacy.c` lines 4563-4579](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L4563-L4579)).

This stops a replica that still advertises the first, obsolete primary's slot
epoch from winning the second primary's slots. Once it learns the latest
configuration, it becomes a replica of the latest primary and advertises that
primary's config epoch. Config epoch says that the node understands the latest
ownership; it does not prove that its data has synchronized to that owner.

The specification uses three replicas A, B, and C to illustrate repeated
failover: A is promoted, A is partitioned, B is promoted, B is partitioned,
then C can be promoted under a still newer config epoch
([practical config-epoch example](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#practical-example-of-configuration-epoch-usefulness-during-partitions)).

#### Replication ID is absent from the election protocol

The cluster message header contains one numeric replication offset and the
topology epochs, but no current or secondary replication ID
([`cluster_legacy.h` lines 229-257](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.h#L229-L257)).
The sender fills that offset from `replicationGetSlaveOffset()` and receivers
store it for ranking
([`cluster_legacy.c` lines 3753-3762](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L3753-L3762),
[`cluster_legacy.c` lines 2964-2978](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L2964-L2978)).
Neither the candidate rank calculation nor vote validation checks a replication
ID.

This is notably weaker than the basic replication rule documented by Redis:
offset orders data only **for a given replication ID/history**
([replication ID explained](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#replication-id-explained)).

#### Current upstream special-cases unrelated shards

When a node changes replication target to a primary with a different shard ID,
current upstream discards its cached primary, thereby reporting offset zero,
and resets the disconnection timestamp so it is treated as never synchronized.
The default nonzero validity factor then prevents election until the first sync
finishes
([`cluster_legacy.c` lines 5427-5454](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/src/cluster_legacy.c#L5427-L5454)).
The upstream tests create cross-shard replicas with deliberately larger old
offsets, hold their first FULL open, and verify that they report zero and do not
outrank or replace the synchronized same-shard replica
([`tests/unit/cluster/replica-migration.tcl` lines 38-170](https://github.com/redis/redis/blob/669b2a1316f5b35ecf964281b77c054ff28dc934/tests/unit/cluster/replica-migration.tcl#L38-L170)).

This protection applies when the Redis shard ID changes. A normal failover
keeps the shard ID, so a sibling's cached old-primary state is intentionally
retained for PSYNC and is not reset to offset zero.

### Source-derived inference

For a normal repeated failover, a sibling can be promoted before it has consumed
the failed promoted primary's entire stream, provided that:

1. it has learned the latest slot configuration and is now considered a replica
   of that promoted primary;
2. its data-age gate permits failover; and
3. it wins a normal epoch vote.

It may still hold only the former primary's lineage. This is a legitimate
fallback in Redis's availability-first model; data written only to the failed
intermediate primary can disappear. The last elected primary's data set then
eventually replaces other replicas, consistent with Redis Cluster's documented
asynchronous “last failover wins” behavior
([cluster specification: write safety](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#write-safety)).

For the ordinary direct-descendant case, replication offsets remain numerically
meaningful across the one promotion boundary because promotion does not reset
the offset and the old ID is explicitly bridged as ID2. A replica that already
processed the promoted primary's newer writes normally advertises a larger
offset and gets an earlier election attempt than a sibling still at the parent
frontier.

For genuinely incomparable same-shard histories, however, Redis Cluster has no
wire data with which to recognize the incompatibility. It will still compare
the numeric offsets and allow the last successful election to replace the
others. The config epoch prevents stale *ownership* from winning, but does not
make cross-history offset comparison sound. This conclusion follows from the
absence of replication IDs in `clusterMsg`, the offset-only rank function, and
the history-blind vote checks; the official documentation does not promise a
stronger behavior.

## Implications for Keylane decisions

### Q48

Redis evidence does not support serializing all FULL rebuilds. A closer match
is concurrent, level-triggered `FollowOwner` reconciliation with safe local
snapshot installation. No rebuild queue or rebuild phase belongs in committed
Meta state.

Keylane has already chosen not to implement Redis's ID2-style parent-to-child
history bridge in issue 41. That is a valid scope reduction, but it means FULL
will be much more common than in Redis after failover. Therefore the safety
property to preserve is Redis's staged replacement behavior, not necessarily
its performance profile: an old complete local image should remain promotable
until a complete replacement is available, or the implementation must provide
an equivalent guard.

### Q49

Redis Cluster does not implement the previously proposed compatibility-domain
layering rule. It treats replicas of the latest configured primary as one
election population and ranks them by offset; an old-lineage member may win.
Only a stale topology epoch prevents a replica still attached to an obsolete
primary from winning.

Because Keylane already observes explicit compatibility domains, it can cheaply
avoid a weakness Redis cannot see:

- selecting only within one domain and preferring the current owner's domain is
  stronger than Redis Cluster;
- falling back to the sole remaining old domain when the current-owner domain
  has no live candidate preserves Redis-like availability, with loss reported
  as `unknown`;
- refusing automatic selection when several incomparable old domains remain is
  also stronger than Redis, but avoids an unsound cross-history offset
  comparison.

That policy is therefore compatible with a “no weaker than Redis Cluster” goal,
but it is not a literal copy of Redis's election behavior. A literal copy would
ignore domain identity after topology adoption and compare offsets, accepting
the resulting rollback risk.

## Subsequent issue 41 decisions

After this source audit, issue 41 deliberately chose concurrent destructive
FULL for the first release, with no rebuild scheduler. The resulting temporary
lack of a promotable follower after a second Owner failure is accepted. A
Redis-style secondary-history mechanism and staged population replacement are
deferred follow-up work.

The election direction was also changed to availability-first domain fallback:
try the newest recoverable compatibility domain, then successively older
domains. "Newest" means the greatest source Group term, mirroring Redis's
topology-recency preference rather than claiming data inclusion. Distinct
histories created within one source term are ordered by canonical domain
identity, and their per-history flow cursors are never compared. A recovered
newer domain does not preempt a healthy action, but it may participate when a
later action is selected. No durable domain list, ancestry graph, or monotonic
fallback cursor is added.
