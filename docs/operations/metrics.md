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

# Prometheus metrics

Enable the HTTP endpoint with a separate listen port:

```sh
./lavik --port 6379 --metrics-port 9100 --data-file lavik.data
curl http://127.0.0.1:9100/metrics
```

`--metrics-port=0` disables the HTTP listener. The endpoint is `GET /metrics`
and uses the Prometheus text exposition format.

A ready-to-run Prometheus and Grafana deployment, including separate Data and Meta
dashboards and multi-node discovery, is available in
[`deploy/monitoring`](../../deploy/monitoring/README.md).

`lavik_cluster_control_wait_seconds` measures completed Meta control steps,
with `stage="observation"` for data-state collection and
`stage="lease_installation"` for authority installation. It includes
cross-worker waits and is exported as a histogram in seconds. These timings
are diagnostics, not a 1 ms scheduling guarantee or an automatic warning
threshold. Compare their distribution with the effective heartbeat interval
and lease duration; use `lavik_cluster_control_lease_expirations_total` to
identify actual local lease failures.

The Grafana overview includes a collapsed **Data → Meta Control Plane** row for
these step timings, connection state, lease decisions, expirations, reconnects, and
directive results. The metrics HTTP service runs on the data workers; the
last runtime worker remains dedicated to Meta control tasks.

## Alerting and readiness

The [alert runbook](alerts.md) documents the maintained rule set and its limits.
The deployment rules are ordinary user-editable Prometheus YAML. `up` is scrape
reachability; `lavik_server_ready` means storage startup completed and no runtime
storage failure is latched. It does not prove serving authority or FULL population
readiness. Before readiness, the exporter deliberately emits only that gauge;
other families must not be treated as missing failures during recovery.
## Meta / Raft metrics

`lavik-meta` has an independent `GET /metrics` HTTP listener. Add
`--metrics-port 9200` to the normal Meta startup command; the default port `0`
disables it. `--metrics-bind` defaults to `127.0.0.1`; set it explicitly to a
numeric private IP (or wildcard) when Prometheus is remote. IPv6 is supported.
The listener has no TLS or authentication; limit network access to monitoring
hosts. Raft, Admin, Data-control and Sentinel ports are unrelated.

The **Lavik Meta / Raft** dashboard uses job `lavik-meta` and a stable `cluster`
target label. The Data **Lavik Overview** dashboard's Data → Meta row observes
the Data client side; it does not describe the Meta process.

Collection runs on a separate worker and reads the protocol owner's published
status and independently updated counters. It never waits for RawNode,
application or storage work. There is no periodic full-state export or retained
text cache. Limits are 32 accepted connections, 16 KiB headers, an 8 MiB body,
a 3 second connection deadline and one collection at a time. Overload or failed
collection returns HTTP 503; a connection past its deadline closes. Recovery
can be scraped before the runtime exists; `lavik_meta_source_available` marks
missing sources and their fields are omitted. Shutdown stops ingress and joins
collection before destroying sources. Prometheus `up=0` covers fail-stop and
network failures when no further application metrics can be collected.

### Upstream process metrics

These six families retain etcd v3.7.0 names, HELP, types and histogram buckets.
Each histogram has finite upper bounds `0.001 * 2^i` seconds for `i=0..13`,
plus `+Inf`. They are process-wide; production runs one Meta runtime per process.
Multi-runtime tests must compare deltas rather than attribute upstream values
to a test node. A narrow registry reuses these actual collectors through the
registration API. Other default-registry collectors (including Go/process collectors),
`etcd_snap_db_*` and `etcd_server_*`, are neither collected nor exported by
this interface.

| Family | Contract |
|---|---|
| `etcd_disk_wal_fsync_duration_seconds` | WAL internal synchronization, including sync inside `Save()` |
| `etcd_disk_wal_write_duration_seconds` | Record writes into PageWriter, potentially buffered; not full durability |
| `etcd_disk_wal_write_bytes_total` | Upstream **Gauge**, despite its cumulative name; encoded WAL bytes |
| `etcd_debugging_snap_save_marshalling_duration_seconds` | etcd envelope, checksum and serialization; excludes C++ capture |
| `etcd_debugging_snap_save_total_duration_seconds` | Successful `SaveSnap()` only; excludes Lavik rename, directory sync and WAL publication |
| `etcd_snap_fsync_duration_seconds` | File write **and** synchronization, not only the fsync syscall |

### Lavik metric contracts

Names below beginning `raft_` have prefix `lavik_meta_raft_`; other names have
prefix `lavik_meta_`. Indices, bytes, states and current counts are Gauges.
`*_total` are Counters except the upstream WAL-byte Gauge noted above.

| Families | Meaning |
|---|---|
| `raft_member_id`, `raft_term`, `raft_leader_id`, `raft_role{role}`, `raft_learner` | Local identity, term, leader hint (zero unknown), complete one-hot protocol role, learner flag; none certifies authority |
| `raft_leader_authority`, `data_control_eligible`, `data_control_term`, `data_control_leadership_age_seconds` | Separate local usable Raft authority and published Data-control eligibility/term and publication age (not a freshness certificate) |
| `raft_durable_index`, `raft_commit_index`, `raft_applied_index` | Current-branch durable prefix (may regress), Raft commit, completed C++ application |
| `raft_snapshot_index`, `raft_first_log_index`, `raft_config_index` | Installed/published snapshot, first retained log, applied configuration |
| `raft_retained_log_bytes`, `raft_pending_durability_bytes` | Logical retained bytes and durability reservations, not filesystem usage |
| `raft_member{peer_id,kind}` | Current member directory; kind is voter or learner |
| `raft_peer_match_index`, `raft_peer_next_index`, `raft_peer_pending_snapshot_index`, `raft_peer_replication_state{state}` | Leader-only peer replication, distinct from application |
| `raft_peer_applied_index`, `raft_peer_fresh`, `raft_peer_heartbeat_age_seconds` | Valid Lavik heartbeat evidence, freshness and age from challenge send; unknown or stale applied is **absent**, never filled with zero |
| `raft_status_age_seconds`, `raft_leadership_validity_seconds` | Age of immutable protocol publication and heartbeat evidence validity interval |
| `raft_peer_send_failures_total`, `raft_vote_responses_total{result}` | Outbound dial/write failures; granted/rejected vote **and pre-vote** responses handed to transport |
| `raft_snapshot_consecutive_failures`, `raft_gc_consecutive_failures` | Consecutive-failure Gauges, reset on success |
| `raft_snapshot_errors_total`, `raft_gc_errors_total` | Monotonic failed local snapshot rounds and reclamation attempts |
| `proposals_total{result}`, `proposal_duration_seconds{result}`, `proposals_inflight` | First caller outcome, entry-to-result Histogram including queueing, and calls awaiting that result |
| `raft_wal_errors_total{stage}` | Fixed stages `save`, `sync`, `snapshot_marker`; no error-text labels |
| `raft_operation_duration_seconds{stage}`, `raft_operations_total{stage,result}`, `raft_operation_inflight{stage}`, `raft_operation_age_seconds{stage}` | Completed stage Histogram, success/error outcomes, active stage and monotonic age |
| `raft_lane_connected{peer_id,lane,direction}`, `raft_lane_queued_messages`, `raft_lane_queue_capacity`, `raft_lane_queue_rejections_total`, `raft_lane_errors_total{stage}` | Authorized peer lanes `control/log/snapshot`; errors `connect/read/write`; log/snapshot connect on demand, so zero alone is not a fault |
| `data_control_live_session_tasks`, `data_control_live_authority_session_tasks`, `data_control_live_leader_tasks`, `data_control_active_sessions` | Existing Data-control task/session Gauges |
| `data_control_{accepted_sessions,redirected_sessions,rejected_sessions,protocol_errors,full_states_sent,lease_grants,lease_denials,observations_accepted,observations_rejected,directive_results_committed}_total` | Existing atomic Data-control counters |
| `phase{phase}`, `source_available{source}`, `collection_errors_total`, `collection_duration_seconds` | One-hot starting/recovering/running/stopping, assembled sources, failed collections and Summary of prior completed collections (sum/count, no quantiles) |

Proposal results are `applied_success`, `applied_rejected`, `admission_rejected`
and `outcome_unknown`. Timeout, cancellation or demotion after submission is
unknown unless admission proves no append happened. Late completion never
counts again or rewrites that outcome. Proposal and operation histograms use
finite bounds 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.3, 1, 5, 30 and
120 seconds, plus infinity. Timings use monotonic clocks.

Operation stages are `persist` (one ordered executor batch through completion),
`capture`/`install` (C++ work plus snapshot encode/decode), `prepare` (file,
rename, directory sync) and `publish` (durable snapshot transaction). Received
snapshot publication includes its covering WAL commit. These layers overlap
upstream write/fsync timing and must not be added together. A prepared file
is not a published snapshot. Upstream snapshot histograms sometimes count only
success; use stage outcome counters for attempts and failures.

Raft fields share one published cut; independent counters and C++ authority do
not promise a global transaction. Peer series use current member IDs only and
stop exporting on removal. Terms are values, never labels. Lane counters retain
reconnect history for the current authorized peer incarnation; an identity or
endpoint replacement can reset them. Read errors exclude deadline expiry before
any frame-header bytes arrive; EOF and incomplete-frame deadlines still count.
Source age is not evidence of fresh quorum; combine it with freshness and
authority.

Fault queries and rehearsal examples are in the
[Meta monitoring runbook](meta-monitoring.md), including the installed Meta alert
policy. The existing optional Alertmanager handles both Data and Meta rules.

## Native replication progress and FULL recovery

Collection remains pull-based: the HTTP request asks each owner worker for its
metrics. Native replication's asynchronous `ObserveProgress()` is shared with
`INFO replication`; it copies flow state on its owner and reads the existing
consistent applied frontier. A worker-zero sampler updates observation clocks
and cumulative totals once per second. Collection batches all sessions and flows
on each participating worker into one submission. Sampling skips output rows and
INFO identity copies; HTTP and INFO still request fresh, validated snapshots.
HTTP collection aggregates source peers directly into numeric flow rows and
omits INFO identities and the last FULL diagnostic. It transfers only nonzero
worker counter rows; the exporter restores omitted rows as zero, preserving the
series set before replication starts and after sessions retire. INFO retains
per-peer details. Retired sessions are collected only by the sampler, which
records their final deltas and results before releasing them.
The sampler does not control ACKs,
timeouts, reconnects or authorization, and adds no clock read to each ONLINE
event. A scrape validates relationship/history generation before exposing active
progress. Counter and terminal-result updates may lag by one sample interval.

`lavik_replication_observation_available=0` means no coherent current snapshot.
Active progress is omitted in that case; cumulative counters remain available.
`lavik_replication_comparable_flows{side,worker,flow}` counts ONLINE connections
with comparable cursors. FULL before ONLINE has no applicable online cursor or
pending-event distance. Missing samples are never a healthy zero.

| Metric suffix (all use `lavik_replication_`) | Meaning |
|---|---|
| `cursor_next_lsn{side,worker,flow,cursor}` | Published on source; Received, Applied and ACK sent on target. Same history and flow only. |
| `pending_events{side,worker,flow}` | Source maximum Published minus observed ACK among valid downstreams; target Received minus Applied. Event positions, not bytes. |
| `native_flow_connections`, `pending_work{step}`, `no_progress_seconds{step}` | Connected flows, known pending work and longest sampled age without progress for send/apply/ack/full. |
| `transfer_bytes_total`, `transfer_events_total`, `applied_events_total` | Process cumulative data-frame bytes including framing, complete ONLINE events sent/accepted, and ONLINE target applied events. Labels are side and worker. |
| `fullsync_phase{phase}`, `fullsync_phase_seconds{phase}`, `fullsync_elapsed_seconds` | Current locally visible FULL phase, phase age and attempt age, per side/worker/flow. |
| `fullsync_items_total`, `fullsync_partitions_total` | Actual snapshot record payloads (including fragments) plus complete incremental commands, and completed partition handoffs across attempts. No estimated percentage. |
| `fullsync_results_total{side,result}`, `fullsync_failures_total{side,reason}`, `fullsync_duration_seconds{side}` | Exactly one success/failure/cancelled result per FULL attempt, bounded failure classification and terminal-duration histogram including all results. |
| `retries_total{reason}` | Native reconnect rounds; FULL busy and authority waits are separate from FULL failures. |
| `native_following`, `native_connected`, `population_ready`, `catalog_ready`, `data_readable`, `loading`, `failed_stopped`, `waiting` | Independent current facts. Data readability describes local dataset readiness; it grants no Meta authority and does not replace command routing/READONLY checks. |
| `fullsync_promotion_complete` | Target-local durable root switch during an active FULL; absent when inapplicable. This does not prove population finalization or ONLINE. |
| `wait_reason{reason}` | Current FULL admission or authority wait, separately from disconnected state. |
| `loading_no_progress_seconds` | Required population recovery without effective progress; waiting for FULL admission or authority is excluded. |

A cursor is the next event position. Received advances only after complete
reassembly, validation and queue acceptance; Applied uses the shared all-flow
frontier; ACK sent advances only after a successful write. Source observed ACK
is identity-validated before computing distance. None is a new durable
checkpoint. FULL promotion and finalization remain separate completion facts.
A replica does not know live source Published. Never subtract across nodes in
PromQL and never use `rate()` on raw LSNs: use the cumulative counters. Prometheus
floating-point samples may round very large LSNs; INFO preserves exact integers.

Labels contain only side, worker, logical flow and fixed enumerations. Source
metrics aggregate peers; maximum distance and maximum age may refer to different
peers. Exact peer/session/history and cursors are in `lavik_replication_progressN`
INFO fields. `lavik_replication_last_full_*` retains the last terminal result in
memory; a new attempt does not overwrite it until terminal. Logs record attempt
start, phase and result with identities; repeated target retry messages are
summarized at most every 30 seconds with the number of attempts. Credentials and
flow capabilities are not metric labels or observation log fields.

The [runbook](alerts.md#native-replication-stalled) and Overview's **Replication
and FULL Recovery** row cover idle, catching-up, disconnected, waiting, stalled,
FULL phases, rates, readiness and failures. These fields cover standalone and
Meta-managed native replication. Redis PSYNC import/export retains its existing
compatibility fields and behavior.

## Memory limit

`--max-memory` (also accepted as `--maxmemory`) sets the process memory limit
and accepts byte-size suffixes such as `8GiB`. A value of zero, the default,
uses 80% of the smaller of the host memory capacity and the process cgroup
limit. Commands that may grow retained memory return a Redis-compatible OOM
error when their worker's share would cross 90% of the limit. The remaining 10%
is passive headroom, not a separately admitted temporary-memory pool. The
default 5% client-request quota can occupy half of that space. Bounded command,
IO, and protocol scratch may briefly push process memory above `--max-memory`,
so this setting is a retained-memory waterline rather than a strict
instantaneous RSS ceiling. Fixed primary-index arrays, page directories, and
storage buffer pools consume this retained budget even for an empty database;
startup rejects a budget that cannot cover the recovered worker footprint.
Storage I/O allocations remain possible at the waterline to let reads and
reclamation progress, and their charges reduce headroom for later growth.

`--maxmemory-clients` bounds ordinary client request and queued `MULTI` bytes.
It accepts either a percentage such as `5%` or an absolute size such as
`512mb`; the default is `5%`, and `0` disables this client-specific limit.
Every nonzero value has a 128 KiB effective minimum so an accidentally tiny
setting does not make administrative commands unusable. The process-wide value
is divided into fixed worker shares. Ordinary reads charge received bytes;
large bulk reads charge bounded destination windows before allocation, then
consume that reservation as data arrives. Unread reservations are released
when the connection closes. The quota tracks received and reserved request
bytes, not allocator capacity or exact heap usage.

`--client-query-buffer-limit` is the independent per-connection hard limit for
an incomplete command. It accepts an absolute Redis-style memory size, defaults
to `1gb`, and must be at least `1mb`. The same
`client-query-buffer-limit 1gb` directive is accepted in Redis-style
configuration files. The connection closes if incremental parsing crosses the
limit; completed commands continue to count against `maxmemory-clients` until
execution or transaction teardown releases them. `CONFIG GET/SET
client-query-buffer-limit` reads or changes the live process-wide value;
existing connections apply a change before their next parsing round and before
committing bytes received directly into a large argument.

The process budget is split evenly across workers and unused capacity is not
borrowed across workers. SET/MSET/INCR performs one worker-local retained-growth
check. Only a new record-index arena page or bucket allocation enters a slow
path that temporarily reserves exact headroom. Full-sync coverage, replication
backlog ownership, fixed source-publisher/full-sync FIFO budgets, and
multi-frame replica staging also share the 90% retained budget because they
can accumulate beyond one request. Publisher admission normally stays within
the fixed queue budget. One command larger than that waterline may enter only
as the exclusive item; its surplus is not retained-accounted and can remain
allocated while publication is backpressured. The protocol limit bounds this
exception, but neither the passive 10% nor `maxmemory-clients` guarantees that
it fits. Increasing or disabling the default client quota reduces the assumed
headroom further. Operators requiring the publisher copy to remain inside the
retained boundary should configure
`replication-publish-queue-mb-per-worker` at least as large as their largest
accepted replicated command.
Recovery bounds its avoidable routing and live-accounting allocations with a
64 MiB process-wide batch target divided across scan workers. A single record
or external-value manifest is indivisible and may exceed one worker's share,
but that batch is applied before the worker retains another record.
Ordinary temporary allocations do not reserve headroom:
the official mimalloc global new/delete override performs no Lavik
accounting. RSS and allocator diagnostics remain outside command execution.

## Business metrics

- `lavik_commands_total`: completed Redis commands. QPS is
  `rate(lavik_commands_total[1m])`.
- `lavik_command_calls_total{command}`: completed commands by command name.
- `lavik_command_duration_seconds`: command execution histogram, from
  dispatch through reply construction; socket response writes are excluded.
- `lavik_connections`: all current TCP connections, including Redis clients,
  Prometheus scrapes, replication, and outgoing Meta control streams. The
  process counter is read atomically without scheduling work on the control
  worker. `lavik_cluster_control_connected` separately reports whether Meta
  has accepted the control session.
- `lavik_connected_clients`: current Redis client connections. This is always
  less than or equal to `lavik_connections`.

For example, per-command QPS and aggregate p99 latency are:

```promql
sum by (command) (rate(lavik_command_calls_total[1m]))
histogram_quantile(
  0.99,
  sum by (le) (rate(lavik_command_duration_seconds_bucket[5m]))
)
```

## Cluster control metrics

Meta-managed Data nodes export process-level control health without node,
group, assignment, directive, or operation identifiers as labels:

- `lavik_cluster_control_connected`: 1 while the final runtime worker (the Meta
  control worker) owns an accepted Meta session, otherwise 0.
- `lavik_cluster_control_reconnects_total`: reconnect rounds after the first
  attempt.
- `lavik_cluster_control_protocol_errors_total`: sessions closed for invalid
  framing or protocol state.
- `lavik_cluster_control_full_states_applied_total`: complete desired-state
  projections installed atomically, including lease-policy-only updates
  installed through the control worker's fast path.
- `lavik_cluster_control_lease_decisions_total{decision="granted|denied"}`:
  finite-authority outcomes returned by Meta.
- `lavik_cluster_control_lease_expirations_total`: locally detected lease
  expirations that fenced authority.
- `lavik_cluster_control_directive_results_total{result="success|failure"}`:
  terminal Data-side directive execution outcomes.

Alert on a Meta-managed node remaining disconnected, repeated protocol errors,
or lease expirations. A reconnect count alone is diagnostic: leader changes and
process restarts legitimately increment it.

## Storage metrics

`INFO STATS` reports `tx_backlog_bytes_total`, the total occupied record bytes
in sealed Tx blocks across workers, and `tx_backlog_bytes_max`, the largest
single-worker amount used by new-transaction admission. A high value with no
retired-block progress can indicate an outstanding transaction or a pinned
source block. The active Tx append block and extent blocks are excluded from
both values; all allocated blocks still consume device capacity.

The active defrag tuning values are exported alongside the work gauges so
latency graphs can be correlated with runtime A/B changes:

- `lavik_storage_defrag_max_active_per_device`
- `lavik_storage_defrag_block_sleep_seconds`
- `lavik_storage_defrag_record_sleep_seconds`
- `lavik_storage_defrag_paused`

They can be changed without restarting Lavik:

```text
DEFRAG PAUSE
DEFRAG RESUME
DEFRAG MAX-ACTIVE 1
DEFRAG BLOCK-SLEEP-MS 100
DEFRAG RECORD-SLEEP-US 10
DEFRAG STATUS
```

Reducing concurrency does not cancel relocations already in flight. A block
sleep change applies after the current block; record sleep is loaded at every
record checkpoint. Zero disables either sleep. `PAUSE` prevents new relocation
jobs from starting but lets an already active job finish; queued candidates are
woken by `RESUME`. Pausing defrag indefinitely can eventually prevent writes
from reclaiming space on a nearly full device.

- `lavik_storage_defrag_runs_total{result}`: completed defrag attempts,
  classified as `success`, `resource_exhausted`, or `error`.
- `lavik_storage_defrag_active`: defrag jobs currently running.
- `lavik_storage_defrag_pending`: workers waiting to start a defrag job.
- `lavik_storage_capacity_bytes{device,path}`: usable data capacity.
- `lavik_storage_available_bytes{device,path}`: space available to normal
  writes after preserving the defrag reserve. This is the capacity metric to
  alert on when Lavik is close to rejecting writes.
- `lavik_filesystem_available_bytes{device,path}`: free space reported by
  the filesystem containing a regular data file. It is omitted for raw block
  devices. A preallocated data file can leave this value unchanged while
  `lavik_storage_available_bytes` falls.

Defrag activity can be compared with command latency using:

```promql
sum by (result) (rate(lavik_storage_defrag_runs_total[5m]))
```

## Memory metrics

- `lavik_memory_current_bytes`: cached explicitly retained bytes used for
  limit enforcement.
- `lavik_memory_used_bytes`: explicitly retained bytes. This is not total
  process heap usage; compare RSS and mimalloc diagnostics for that view.
  Includes primary-index objects and directories, fixed read/write,
  maintenance, device-metadata and export workspaces, live overflow and heap
  read outputs, and retained entries, tables, and other tracked owners.
  Charges cover owned capacity on both io_uring and SPDK, excluding allocator
  overhead and SPDK's full hugepage reservation. Workspace reuse and registration
  do not add another charge.
- `lavik_memory_rss_bytes`: resident process memory, refreshed when metrics
  or `INFO memory` is requested.
- `lavik_memory_committed_bytes`: pages committed by mimalloc; diagnostic
  only and valid without per-allocation mimalloc statistics.
- `lavik_memory_reserved_bytes`: virtual address space reserved by mimalloc;
  diagnostic only.
- `lavik_memory_max_bytes`: configured process memory limit.
- `lavik_client_request_buffer_limit_bytes`: effective client request-buffer
  limit after resolving percentages and the nonzero 128 KiB floor.
- `lavik_client_buffered_request_bytes`: wire bytes currently retained by
  ordinary parsers, ready command batches, and queued transactions, plus
  prepaid destination bytes for incomplete direct bulk reads.
- `lavik_fullsync_reserved_memory_bytes`: reusable retained-memory headroom
  promised to active full-sync coverage maps.
- `lavik_memory_admission_pending_bytes`: short-lived worker-local permits
  held while retained page, bucket, replication-owner, or replica-staging
  ownership is constructed.
- `lavik_memory_rejected_commands_total`: commands rejected by the limit.
- `lavik_worker_retained_memory_bytes{worker}`: retained bytes charged to
  one worker's admission share, including its deterministic share of retained
  allocations created outside a bound worker.
- `lavik_worker_memory_admission_pending_bytes{worker}`: that worker's
  short-lived allocation permits.
- `lavik_worker_fullsync_reserved_memory_bytes{worker}`: that worker's
  reusable full-sync coverage reservation.
- `lavik_worker_client_buffered_request_bytes{worker}`: request bytes charged
  to that worker's independent client-buffer quota.
- `lavik_worker_memory_limit_bytes{worker}`: the worker's fixed share of the
  90% retained-memory waterline.

The provisioned Grafana **Retained Admission Utilization** gauge approximates
the process-wide admission decision as:

```promql
100 * (
  lavik_memory_current_bytes
  + lavik_fullsync_reserved_memory_bytes
  + lavik_memory_admission_pending_bytes
) / (0.9 * lavik_memory_max_bytes)
```

Admission is enforced per worker, so a single worker can still reject growth
before this process-wide aggregate reaches 100%. Oversized publisher surplus,
ordinary client buffers, untracked temporary heap usage, and RSS are absent
from this retained-admission gauge. Live overflow storage read buffers are
tracked; their charge disappears when the owner processes the lease return.

The provisioned Grafana dashboard keeps the per-worker retained-admission bars
in a collapsed **Worker Memory** row. Expanding it shows
`100 * (retained + pending + full-sync reserved) / worker limit` for every
selected instance and worker without adding any command-path accounting.

The same values are available through Redis `INFO memory`, including
`used_memory`, `used_memory_rss`, `maxmemory`, and
`oom_rejected_commands`.

Worker 0 sums the cache-line-separated worker retained counters every 100 ms.
Release builds use mimalloc's official global C++ override without Lavik
hooks. Explicit retained allocators query `mi_usable_size` only on their much
rarer allocation/free paths. The hot command path reads its own shard and adds
a conservative retained-size estimate, so it performs no allocator aggregation
or `/proc` I/O. RSS is diagnostic only and is sampled by the explicit
metrics/INFO request.

## Replication and Function catalog

`INFO replication` exposes `lavik_replication_group_id`, the current boot
and replica-incarnation IDs, the local/upstream history, and
`lavik_function_catalog_generation` plus its CRC64. Group ID is the stable
peer/reparent lineage; boot, incarnation, and history are process-scoped.
After a restart, expect a new history and whole-group full sync rather than a
continuation from the old in-memory cursor.

The catalog generation is node-local and must only be compared with that
node's promotion base. Generation zero denotes a fresh set's implicit empty
catalog; the first successful catalog commit makes it nonzero. It is useful for
detecting a successful local Function commit or an unexpected catalog change,
but equal or different generations on two nodes say nothing about replicated
equivalence. The CRC64 identifies the local dump content for diagnosis.

Backlog and full-sync metrics are worker-labelled:

- `lavik_replication_backlog_bytes` and
  `lavik_replication_backlog_capacity_bytes` show the current hard reconnect
  window allocation and quota.
- `lavik_replication_backlog_floor_lsn` and
  `lavik_replication_backlog_tail_lsn` show retained event coverage. A
  consumer below the floor must full-sync.
- `lavik_replication_backlog_pinned_cursors` counts current ACK coverage
  claims. At the hard cap, the default policy waits for cursor progress;
  `replication-backlog-backpressure no` instead revokes lagging claims and
  evicts complete old events without allowing the backlog to grow.
- `lavik_replication_backlog_backpressured` identifies workers currently
  waiting for cursor progress, and
  `lavik_replication_backlog_backpressure_waits_total` counts capacity
  conflicts that entered that state.
- `lavik_replication_backlog_coverage_revocations_total` counts those
  revocations.
- `lavik_replication_publish_queue_bytes` and its capacity expose source
  publication staging. The corresponding `lavik_fullsync_*` gauges expose
  active full-sync sessions, their command queues, admission, and waits.
- `lavik_replication_control_connections` and
  `lavik_replication_flow_connections` distinguish native control sockets
  from per-flow data sockets.

Alert on sustained backlog backpressure together with a publisher queue near
capacity. When backpressure is disabled, alert instead on sustained coverage
revocations together with replicas repeatedly leaving `online`. Also alert on
a full-sync queue remaining near capacity or a node remaining `LOADING` after
a full-sync failure. A valid catalog root does not clear that last condition:
full-sync population activation and catalog readiness must complete together.

## Update model

Each worker owns a cache-line-aligned metrics shard and updates plain integers
only in that shard. Scraping submits a short snapshot operation to each worker
and merges the returned copies. Command execution therefore does not contend
on shared QPS or histogram atomics. HTTP response buffers are retained in a
bounded per-worker pool and reused by later scrapes.
