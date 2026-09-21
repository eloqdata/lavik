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

# Lavik Admin and fleet management

Lavik Admin is a separate Node.js service that owns a persistent fleet catalog,
operator request tracking, browser sessions, and the HTTP UI. A private Unix
socket accepts `lavik-ctl` fleet commands. Both entry points invoke one
application boundary and one database; neither the browser nor the CLI keeps
an independent cluster list.

Each Meta Raft deployment continues to own exactly one Data cluster. The
Admin fleet catalog associates a human-readable name with numeric Meta seeds
and a server-side connection profile. It does not merge separate clusters'
Raft stores or become a source of Data serving authority.

```text
browser -- HTTP + session cookie --+
                                  |
lavik-ctl -- private Unix socket --+--> Fleet application
                                        |         |
                               SQLite worker      +-- lavik-ctl subprocess
                               catalog / requests       |
                                                  Meta Admin leader
                                                  topology / operations
                                        |
                                        +-- bounded RESP connections --> Data
```

## State and ownership

A dedicated worker exclusively owns the SQLite connection. WAL with full
synchronization stores the versioned cluster catalog, request records, and
administrative audit metadata. Disk synchronization does not block HTTP or
network processing. One private Unix socket per data directory prevents two
Admin processes from owning the same fleet workflow executor. The database
is an operator-service durability boundary, not a replicated consensus store;
its persistent volume requires an operational backup.

Meta remains authoritative for committed cluster lifecycle, membership,
authority, and operation outcomes. The service invokes the actual `lavik-ctl`
binary without a shell, reusing its discovery, TLS verification, deadlines,
and wire validation. Leader reads expose node endpoints, group membership
revision, and a bounded page of operation summaries. Summaries exclude opaque
intent and directive payloads; their previews are bounded independently of
retained operation size. Meta's live operation journal also includes work
initiated by a direct CLI client; archived records follow existing Meta
retention/export procedures.

## Mutation lifecycle

Admin persists a request identity and its intent before sending a mutation.
A unique database constraint admits one active Admin request per registered
cluster. Meta still validates every mutation and arbitrates with concurrent
direct operators and failover. A failover request retains its operation ID
and absolute deadline as an indivisible idempotency pair. Cluster creation
uses the existing single-cluster Genesis admission and retained Meta root.

Replica changes create a generic Meta operation under the same request ID.
Addition registers the specified node identity/endpoint, assigns it to an
existing group, and observes native Follow Owner synchronization. It completes
only after Meta observes current population, projection, and health. Removal
retains the reviewed membership revision, rejects the current Owner and active
failover, and commits the existing membership-removal command. It completes
when the node is unassigned and the remaining group's topology converges.
Unassigned nodes have no group-runtime acknowledgement. Neither action
provisions or deletes a machine or its storage.

The Meta publisher and native Follow Owner relationship drive Data convergence
after membership commits. A removed node retains only remote routing for its
old group; those routes contain no local population manifest. Re-adding that
node creates a new assignment incarnation. Replica resizing leaves primary
groups and slot ownership unchanged. Changing their count requires a separate
data-migration protocol and is not exposed as an available operation.

Admin restarts treat previously running requests as uncertain and observe
committed state. They never blindly repeat a possibly submitted mutation.
Explicit retry preserves the request identity, deadline, and removal revision;
expired or stale removal intent requires a new review. Abandoning an uncertain
replica request terminalizes its generic Meta operation without undoing
committed membership. Typed creation and failover remain Meta-owned workflows.

## Browser, data, and trust boundaries

The browser uses HTTP-only, SameSite session cookies with finite lifetime.
An access token in a private file establishes a session; sessions expire on
process restart. Mutation requests require a same-origin custom header and
JSON content type. The CLI socket is mode 0600 inside a mode-0700 directory.
Remote browser deployment uses a TLS reverse proxy and an explicit origin.
Connection profiles retain Meta mTLS and optional Data TLS/password settings
on the server; browser replies and the fleet database do not contain these
credentials. Plaintext Meta profiles rely on the existing trusted-network
boundary.

Data requests use bounded RESP connections to endpoints learned from Meta.
Redirects are followed only to endpoints in that cluster's topology. The
console admits an explicit command set and requires write confirmation;
Meta and native replication commands are excluded. Binary keys retain their
bytes through base64 identifiers. Scans bind their cursor to the observed
topology epoch, coalesce a bounded number of sparse pages, and provide neither
snapshot consistency nor an unbounded keyspace traversal. Collection views
are bounded previews. Metrics report partial node failures explicitly and
derive throughput from successive counter samples. Slow logs use `SLOWLOG`;
unsupported Valkey-specific observability is not simulated.

## Source map

| Responsibility | Source |
|---|---|
| HTTP, authentication, private CLI entry | `admin/server.mjs` |
| Catalog, workflow admission and observation, data tools | `admin/fleet.mjs` |
| Database owner and schema | `admin/store.mjs` |
| Meta transport reuse | `admin/meta.mjs`, `app/lavik_ctl.cpp` |
| Bounded RESP transport and binary-safe values | `admin/resp.mjs` |
| Browser views | `admin/public/` |
| Meta summaries and membership removal | `src/meta/ctl_server.cpp`, `src/meta/state_machine.cpp` |
| Docker and browser verification | `admin/test/`, `admin/Dockerfile*`, `admin/compose.yaml` |
