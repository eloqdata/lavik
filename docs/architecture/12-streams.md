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

# Streams

## Boundary and logical state

The Redis command layer owns Stream ID allocation, range and trim semantics,
consumer-group delivery, pending-entry ownership, claims and blocking wakeups.
Storage represents a Stream either as a compact `LXS1` image or as an ordered
grouped graph under the same user key and Redis type. Writes promote at 1 MiB
of logical encoding; deleting the key retires its complete incarnation.

A Stream has an ordered sequence of messages, a last-generated ID, an
entries-added counter and a maximum-deleted ID. Consumer groups independently
retain their delivery cursor, entries-read estimate, consumers and pending
entry list (PEL). Removing a message does not remove its PEL references; ACK
removes pending state without deleting messages. An empty Stream can therefore
retain its ID history, groups and PEL.

## Durable graph

The grouped representation uses the ordered collection graph and publication
lifecycle described in [Grouped collections](09-grouped-collections.md).
Its root has Stream kind and an explicit Stream-length extension. The root's
internal record count includes metadata and is independent of the externally
visible message count, which can be zero.

Ordered pages contain binary-keyed logical records: the Stream header,
individual messages, macro-node count and individual macro-node sizes, group
count, group headers, individual consumers, PEL counts and individual pending
entries. Neither an entire group nor an entire PEL is one indivisible page
item. IDs retain both unsigned 64-bit components; name escaping preserves
binary identity and ordering. Page score fields are zero.

Logical macro nodes are independent of physical storage pages. Their persisted
live-entry counts determine approximate trimming even after page splits,
recovery or a change in `stream-node-max-entries`. A node record's key identifies
its first live message. A partial trim or deletion can change that boundary;
PEL records can continue to refer to removed messages.

One immutable root binds all record categories. Auxiliary pages are complete
replacements, and splits publish their neighbour links and retirement evidence
with the root. The existing grouped transaction decision, undo, recovery winner
selection, TTL, population-reset and garbage-collection rules apply to Streams.
An append or delivery wakes blocked readers only after successful publication.

## Access and mutation

The ordered directory retains page identities, links and counts, without
message, consumer or PEL payloads. Record lookup binary-searches page boundaries
through admitted page reads. Request-local page ownership stays on the key's
worker; loaders revalidate logical identity and retry physical relocation.

| Operation | Storage access |
|---|---|
| XLEN | Checked root metadata |
| XADD, XTRIM | Header/tail changes and a prefix retirement plan; complete removed pages need no payload reads |
| XDEL | Deduplicated ID lookups, affected message pages and logical node boundary/count updates |
| XRANGE, XREVRANGE, ordinary XREAD | ID boundary probes, directory ranks and a pinned page reader with network backpressure |
| XACK | Target group metadata, the specified pending IDs and PEL-count change |
| XREADGROUP | Selected message or pending IDs, target consumer and group counters; message fields come from a pinned reply reader |
| XCLAIM, XAUTOCLAIM | Named IDs or a bounded PEL scan, selected messages and consumer/group changes |
| XGROUP CREATE / SETID / CREATECONSUMER | Target group header, global group count when needed, and named consumer |
| XGROUP DESTROY / DELCONSUMER | Group-range retirement or an owner-filtered PEL scan; unaffected message records remain untouched |
| XSETID | Header and last live ID validation |
| XINFO / XPENDING | Summary metadata or selected inspection rows; PEL summaries scan pages without retaining the whole PEL |
| Replicated group delta | Explicitly named consumers and pending IDs plus group counters |

Partial callback views explicitly carry global message count and first ID when
lag estimation needs them. Delivery views can contain placeholder fields: the
callback may change group state only, and output fields are read from the
immutable graph under the same key intent. Omitted records are never interpreted
as deletions. Storage validates the allowed mutation scope before publication.

Sparse plans retain changed pages and necessary neighbours. Entire removed
ranges use directory retirement metadata rather than copying their contents.
Directory reconstruction and graph pin metadata still scale with page count;
a command changing many PEL rows retains its admitted mutation metadata until
atomic publication. Consumer-filtered PEL operations scan the group's ID index,
which has no separate persistent owner index.

Range, XREAD and XREADGROUP replies pin the command-position graph and generate
message fields one page at a time as the network drains. EXEC retains that
snapshot even if a later command replaces or deletes the key. XINFO FULL uses
this same reader for its message array; its requested group/consumer/PEL
inspection metadata remains admitted command state. Individual records and
explicitly selected mutation/inspection windows must fit admission. Unlimited
inspection metadata is not a constant-space operation.

## Replication and transfer

Grouped native snapshots use the portable `LSR1` logical-record stream: a
record count followed by length-framed records in routing-key order. Keys encode
logical IDs/names, never physical page identities or root incarnations. This
unreleased wire revision requires compatible peers. Both sender and receiver
process admitted pages; the receiver checks ordering and complete
message/node/group/PEL totals before committing one destination transaction.
COPY and RENAME use the pinned collection reader and the same transactional
ingestion contract.

Clock-dependent consumer-group updates replicate exact changed consumer and
pending-entry after-states, removals and group cursor/counters. Creating a group
uses its initial state; destroying one uses an explicit removal. ACK replicates
its deterministic ID removals directly. The internal delta command is accepted
only from replication; the previous complete-group replay form remains readable.

RDB import and export operate on individual listpacks/messages and admitted
logical pages. Redis places Stream metadata after message listpacks and PEL
owner associations after global pending records. Import therefore uses bounded
external sorting to order records and join owners; export uses the same scratch
mechanism to emit consumer-owned pending IDs after global PEL state. Scratch
files are unlinked immediately, contain no recovery state and close on failure
or cancellation. Memory scales with a run/page and the largest individual
record; temporary disk usage and synchronous reorder work scale with the data
being reordered. No imported root becomes visible before complete validation.

DUMP encodes a pinned collection into an unlinked temporary file to determine
its RESP bulk length, then drains bounded chunks. RESTORE retains the ordinary
RESP request-size limit and borrows that request while ingesting pages. Backup,
DUMP and native output preserve consumers and deleted-message PEL references.
The current unreleased durable schema does not migrate older development
layouts.

## Source map

| Responsibility | Source |
|---|---|
| Redis semantics, partial-view contracts and canonical group deltas | `src/redis/stream_command.cpp`; `include/lavik/storage/engine.h` |
| Logical records, sparse plans and ingestion validation | `include/lavik/storage/detail/stream_records.h`, `src/storage/engine/stream_records.cpp` |
| Incremental storage access | `src/storage/engine/grouped_stream.cpp`, `compact_api.cpp` |
| Root/page formats, publication and recovery | `src/storage/engine/grouped_collection.cpp`, `grouped_ordered_mutation.cpp`, `recovery.cpp`; `src/storage/format.cpp` |
| Snapshot projection and key transfer | `src/storage/engine/grouped_replication_source.cpp`, `collection_compact_stream.cpp`, `transfer_api.cpp`, `collection_ingest.cpp` |
| RDB conversion and bounded external reorder | `src/redis/rdb.cpp`, `rdb_collection.cpp`, `rdb_stream_encoder.h`, `rdb_record_spool.h` |
