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
| XADD without trimming | Header, entry insertion, node count and tail-node changes |
| XRANGE, XREVRANGE, ordinary XREAD | Seek to an ID boundary and load matching pages up to the requested count |
| XACK | Target group metadata, the specified pending IDs and PEL-count change |
| XREADGROUP with `>` | Target group and consumer, selected message window and matching pending IDs |
| XCLAIM | Named pending and message IDs, target consumer and group header |
| XGROUP SETID / CREATECONSUMER | Target group header and named consumer |
| Replicated group delta | Explicitly named consumers and pending IDs plus group counters |
| Other Stream callbacks | Admitted complete logical image followed by a sparse page rewrite |

Partial callback views explicitly carry the global message count and first ID
when lag estimation needs them. The storage adapter checks the callback's
allowed mutation scope before publication; omitted records are never interpreted
as deletions. Sparse plans retain only changed pages and necessary neighbours.
Directory reconstruction still scales with the number of pages.

Full-image adapters remain in use for trimming, several group management and
inspection commands, pending-history reads and automatic claims. Range replies retain
the requested output until RESP construction completes. These boundaries do
not imply constant-memory execution for an unlimited result or full-image
operation.

## Replication and transfer

Native snapshot sources project the ordered records back into portable `LXS1`
fragments one admitted page at a time. Physical page identifiers and root
incarnations are not wire data. COPY and RENAME transfer ordered records through
the existing pinned collection reader and transactional ingestion. Ingestion
checks record ordering, framing and complete message/node/group/PEL totals
before committing the destination.

Clock-dependent consumer-group updates replicate exact changed consumer and
pending-entry after-states, removals and group cursor/counters. Creating a group
uses its initial state; destroying one uses an explicit removal. ACK replicates
its deterministic ID removals directly. The internal delta command is accepted
only from replication; the previous complete-group replay form remains readable.

RDB export/import and native snapshot receiving retain their complete-image
Stream adapters and associated materialization limits. Their logical format
preserves macro nodes, groups, consumers and deleted-message PEL references.
The current unreleased durable schema does not include migration from older
development layouts.

## Source map

| Responsibility | Source |
|---|---|
| Redis semantics, partial-view contracts and canonical group deltas | `src/redis/stream_command.cpp`; `include/lavik/storage/engine.h` |
| Logical records, sparse plans and ingestion validation | `include/lavik/storage/detail/stream_records.h`, `src/storage/engine/stream_records.cpp` |
| Incremental storage access | `src/storage/engine/grouped_stream.cpp`, `compact_api.cpp` |
| Root/page formats, publication and recovery | `src/storage/engine/grouped_collection.cpp`, `grouped_ordered_mutation.cpp`, `recovery.cpp`; `src/storage/format.cpp` |
| Snapshot projection and key transfer | `src/storage/engine/grouped_replication_source.cpp`, `collection_compact_stream.cpp`, `transfer_api.cpp`, `collection_ingest.cpp` |
