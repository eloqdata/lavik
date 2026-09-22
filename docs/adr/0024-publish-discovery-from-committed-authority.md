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

# Publish discovery from committed authority

## Status

Accepted for implementation in [#103](https://github.com/eloqdata/lavik/issues/103).

The Discovery Entry answers client topology queries for Meta-managed Single
deployments by joining Committed State with the current leader's volatile
Observations. The two truths can disagree about a serving Owner's condition:
Committed State lags nothing but knows only authority facts, while detector
and session evidence is fresher but leader-local and reset on every leadership
change. Three decisions fix how an answer is shaped when they disagree.

Health never gates publication; it only sets flags. A Primary address is
published whenever committed authority stands: the cluster is Created, its
immutable Client Service Mode is Single, the Group's authority is active, and
the committed Owner is not retired and advertises a usable `tcp://` client
endpoint. Detector SUSPECT or session loss adds `s_down`/`disconnected` to
that published record instead of removing it, because withdrawing a
still-authoritative address would send clients rediscovering a service that
has not moved and would hide the one address that can still serve them. The
converse also holds: flags are the only influence Observations have on the
reply, so a healthy-but-unauthoritative Owner is never published.

Objective failure is expressed by withdrawing publication, never by `o_down`.
Once committed authority is gone—a fenced Group Term, a retired Owner, no
usable endpoint—the Primary record disappears: `MASTERS` omits the service and
`GET-MASTER-ADDR-BY-NAME` resolves to null until a new authority commits. A
real Redis Sentinel instead keeps the record and adds `o_down` after a quorum
of peers votes the master objectively down. Lavik has no Sentinel peer quorum;
the committed fence is already the objective fact such a vote approximates, so
republishing the record with a flag would advertise an address that can no
longer serve. Replicas remain listed from committed membership throughout,
carrying `master_down` while no Primary is publishable.

Replica link-state fields are omitted rather than fabricated. Redis Sentinel
reports `master-link-status` and `master-link-down-time` per replica. Meta has
no truthful steady-state observation of a replica's upstream link: Heartbeat
health proves the replica process and its population, not whether its
replication connection to the Owner is currently up, and no scalar timeout can
derive link state without inventing failures during exactly the partitions
where the field matters. The considered alternative was to extend the replica
heartbeat payload with self-reported link state. That was deferred because it
widens the wire format and Data-side tracking for a purely diagnostic field no
supported client consults for correctness, and the self-reported value would
still arrive stale under the same partitions. If a real consumer appears, the
payload extension remains possible without changing this contract. The related
`slave-repl-offset` field is likewise a documented placeholder constant:
progress has no scalar form comparable across Compatibility Domains.

One deliberate optimism accompanies the flag rules. Within the leadership
observation grace after a Meta leader change, a node the new leader has never
observed is reported as unknown rather than down, so flags do not flap during
warmup; a node observed and then lost counts as down at any time. Failover
continuity across that window is accepted and validated separately in #105.

References: [operations discovery publication contract](../operations/meta-control-plane.md#discovery-publication-contract),
[Redis Sentinel client protocol](https://redis.io/docs/latest/develop/reference/sentinel-clients/).
