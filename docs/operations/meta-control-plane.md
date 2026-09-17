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

# Meta control plane operations

Lavik clusters require Lavik binaries and certificates with `lavik://` URI
SANs on every member. Recreate data and Meta directories from development
builds that used obsolete identifiers; mixed identifiers and rolling upgrades
from those builds are unsupported.

## Process and storage prerequisites

Build the separate Meta executable and its operator client with:

```sh
cmake --build <build-dir> --target lavik-meta lavik-ctl
```

A cluster normally has three `lavik-meta` processes. Every process needs:

- A positive, cluster-unique `--id` that never changes for that member.
- A numeric IPv4 or IPv6 `--addr` for its local Raft listener. At genesis it
  must equal the manifest's advertised Raft endpoint; a restart may bind
  behind an explicit transport proxy without changing the durable endpoint.
  For IPv6, use the form accepted by `lavik-meta --help`.
- A distinct numeric `--data-control-addr` for the process-lifetime listener
  used by Data nodes. The durable advertised route comes from membership and
  may instead name a proxy; that route must remain stable across restart.
- A concrete numeric `--ctl-addr` for remote Admin access and leader discovery
  before the cluster grows beyond one voter. This flag is the local bind; the
  durable advertised route may name a proxy. Wildcard hosts and port zero are
  rejected. Changing an advertised route requires removing/retiring the
  member and joining a fresh server id.
- A private `--data-dir`. Never share a directory between members or reuse it
  with another id.
- The required TCP Admin listener plus an optional Unix socket. With no
  explicit `--ctl-socket`, `lavik-meta` also uses
  `<data-dir>/meta-admin.sock`.

Create the data directory as the service account with mode 0700. The control
socket itself is mode 0600 and authenticates the caller with Linux
`SO_PEERCRED`. By default only the process uid is allowed; repeat
`--ctl-allow-uid N` to replace that default with an explicit uid allowlist.
The socket parent must not be group- or world-writable. The required
`--ctl-addr` listener uses plaintext TCP unless all three `--ctl-tls-*`
arguments are supplied. Partial TLS configuration fails startup instead of
silently downgrading. A plaintext listener grants operator access to any
reachable peer, so expose it only on loopback or a trusted private network.
The TCP listener and the explicit or default Unix socket share command
dispatch, authorization, status capture, and limits; failure of either rolls
back process startup.

Before the first start, give every initial Meta process the same canonical
cluster manifest through `--initial-cluster-manifest`. The manifest's complete
`[[meta_members]]` set becomes one NuRaft genesis configuration; one, three,
and five initial voters follow the same path and use ordinary randomized
election. Each process's `--id` and initial Raft address must match its
manifest descriptor. Data-control and Admin process addresses are local binds;
their manifest values are the durable advertised routes.

The manifest is a bootstrap-only input. After `cluster_config.dat` exists,
restart with the same id, data directory, compatible listeners, and TLS mode
but omit `--initial-cluster-manifest`; replaying it is rejected. A pristine
process started without a manifest is instead an election-disabled waiting
joiner for a future `addsrv`. Internally, `initial_bindings.dat` keeps initial
transport recovery enabled only until every genesis identity binding commits;
`initial_bindings_complete.dat` permanently disambiguates that completed state
from the initial two-file publication prefix. A waiting process's
`waiting_joiner.dat` survives pre-add config replay and is removed only after
the installed config includes its local id and it applies all bindings through
that committed membership config. `raft_started.dat` is published before the
first durable vote so a later loss of both vote and WAL cannot make an active
genesis directory look unused. Once either path converges,
`transport_bindings.dat` holds the exact last-converged descriptors and their
state-machine replay watermark; future `addsrv` and `removesrv` changes update
that same reusable baseline transactionally with `cluster_config.dat`. A
transient `transport_bindings.next` is resolved during crash recovery. A late
initial member may catch up through either WAL or a snapshot after membership
has changed. Before opening Raft transport or elections, restart validates the
snapshot and reconciles its embedded membership with the durable config. It
automatically completes interrupted config/baseline/lifecycle writes when that
snapshot supplies the evidence; a later config needs its matching WAL entry
(except an election-disabled joiner's pending invite). Conflicting configurations
or missing recovery evidence prevent startup. Do not
edit or delete these files or other Raft state to turn an old member into a new
one; missing, mismatched, or malformed lifecycle state intentionally prevents
startup.

## Send administrative commands

Direct `lavik-ctl` commands send one LF-terminated request to the
selected member and print its one-line reply. `status` reports that member's
local state. Run the client as an allowed uid when using the local Unix socket:

```sh
lavik-ctl \
  --socket /var/lib/lavik/meta-1/meta-admin.sock status
```

Direct commands exit 0 for an `OK` reply, 2 for an `ERR` reply, and 1 for local,
connection, TLS, timeout, or malformed-protocol failures. The default timeout
is five seconds and `--timeout-ms` changes the whole connect/send/receive
deadline. Query every live member when locating the leader; the leader's reply
contains `leader=1`, while mutation requests sent to a follower return
`ERR not-leader`.

For cluster-wide readiness, use `lavik-ctl cluster-status`. It queries
the seed for the current leader and requests one leader-bracketed status cut;
it does not probe followers or claim their reachability or replication progress:

```sh
lavik-ctl cluster-status \
  --socket /var/lib/lavik/meta-1/meta-admin.sock

lavik-ctl cluster-status --addr 10.0.0.11:7200 \
  --allow-plaintext-admin --json
```

Connection options may also precede `cluster-status`, for example
`lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock cluster-status`.
The first human-readable line is `READY`, `NOT READY`, or `RETRYABLE`.
Corresponding exits are 0, 2, and 3; invalid options, unsafe transport choices,
TLS/identity failures, incompatible wire data, and corrupt status exit 1.
Text and JSON status include `status_explanation` and `next_action`; fatal
errors print equivalent guidance to stderr even when no trustworthy status cut
exists. `cluster-status` requires `--allow-plaintext-admin` for TCP without TLS.
Each Group also reports `automatic_failover_state`, an exact current failure
reason when present, elapsed debounce milliseconds, the effective threshold,
and a blocker code when blocked. The `triggering` state records that a Begin is
being submitted. These fields are leader-local diagnostics: they contain no
Policy document, identity, or version and do not independently make the
cluster unready.
Supplying `--tls-ca`, `--tls-cert`, and `--tls-key` together enables mTLS for
every TCP connection, with the address verified against the server
certificate's IP SAN. These credentials can accompany a Unix seed and secure
the connection to a discovered remote leader. There is no plaintext/TLS
fallback. One absolute deadline, five seconds by default, covers discovery,
redirects, capture, and response I/O.

## Create the first multi-Group cluster

`lavik-ctl cluster-create` is the v1 topology-creation path for a fresh
manifest-bootstrapped Meta cluster and one or more preconfigured Data
processes. It is not an import, expansion, or retry-existing command. One Meta
Raft cluster permanently owns at most one logical Data cluster. Only lifecycle
`uninitialized` accepts creation, and the environment must also contain no
Data identity (including retired identities), Group or Slot state, grant,
population manifest, or prior creation operation. Meta identity,
configuration, audit records, and valid pre-seeded registered Policies do not
make the environment non-pristine.
Creation is destructive for every declared Data process. Define the initial
Meta set in the same manifest shown below, then start every Meta process from
that file before starting Data:

```sh
lavik-meta --id 1 --addr 127.0.0.1:7101 \
  --data-control-addr 127.0.0.1:7301 \
  --ctl-addr 127.0.0.1:7201 \
  --data-dir /var/lib/lavik/meta-1 \
  --initial-cluster-manifest /etc/lavik/cluster.toml \
  --ctl-socket /var/lib/lavik/meta-1/meta-admin.sock
```

Start every Data process in fail-closed Meta-managed mode with its final node
id, unique client port and storage path. For example, repeat this pattern for
the ids and ports named by the manifest:

```sh
lavik --cluster-enabled \
  --cluster-node-id 1111111111111111111111111111111111111111 \
  --cluster-meta-seed 127.0.0.1:7301 \
  --cluster-announce-ip 127.0.0.1 --port 6371 \
  --data-file /var/lib/lavik/data-primary-1/lavik.data
```

Data reports LOADING while its unregistered control connection retries. Run
the same release on Meta, Data and the CLI. Version 1 names the current
multi-Group topology and full initial Meta directory; it does not provide
compatibility with the earlier scalar Meta development layout. Raft,
Data-control, and Admin advertised addresses belong to each Meta descriptor.
Local bind addresses, storage paths, Unix socket paths, TLS files, and runtime
tuning remain per-process arguments and do not belong in the manifest. A bind
may differ from its advertised route when an explicit proxy or load balancer
owns that route.
An automatically allocated two-Group topology is:

```toml
schema_version = 1
slot_strategy = "contiguous-even"

[bootstrap_policy]
automatic_uncontrolled_failover_suspect_after_ms = 5000
authority_lease_duration_ms = 5000
candidate_recovery_budget_ms = 2000

[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

[[data_nodes]]
id = "1111111111111111111111111111111111111111"
client_endpoint = "tcp://127.0.0.1:6371"

[[data_nodes]]
id = "2222222222222222222222222222222222222222"
client_endpoint = "tcp://127.0.0.1:6372"

[[data_nodes]]
id = "3333333333333333333333333333333333333333"
client_endpoint = "tcp://127.0.0.1:6373"

[[data_nodes]]
id = "4444444444444444444444444444444444444444"
client_endpoint = "tcp://127.0.0.1:6374"

[[groups]]
id = "group-1"
primary = "1111111111111111111111111111111111111111"
replicas = ["2222222222222222222222222222222222222222"]

[[groups]]
id = "group-2"
primary = "3333333333333333333333333333333333333333"
replicas = ["4444444444444444444444444444444444444444"]
```

The optional `[bootstrap_policy]` section supplies version-1 values for each
registered Policy family when that family has not been pre-seeded. Omitted
fields use the values above. These values are creation defaults only: a valid
pre-seeded current Policy is preserved, and changing the manifest later does
not reconfigure a created cluster. Automatic failover `suspect_after_ms` must
be 1,000–86,400,000; Authority Lease `duration_ms` must be
100–86,400,000; Candidate Recovery `budget_ms` must be 0–86,400,000.
Unknown fields, malformed values, and out-of-range values make
the manifest invalid.

To choose Slots explicitly, omit `slot_strategy` and add a complete table:

```toml
[[slot_ranges]]
first = 0
last = 8191
group = "group-1"

[[slot_ranges]]
first = 8192
last = 16383
group = "group-2"
```

The parser rejects files over 64 KiB, unknown or duplicate structure,
noncanonical ids or numeric endpoints, duplicate or cross-Group membership,
Groups without a valid primary, and Slot tables with gaps, overlaps, unknown
Groups, or incomplete coverage. Every declared Data must belong to exactly one
Group and every Group must own at least one Slot. `contiguous-even` sorts
Group ids and assigns boundaries with
`floor(i*16384/N)..floor((i+1)*16384/N)-1`. The CLI displays the fully
normalized order and range table before any mutation.

For mTLS Data control and replication, configure each Data process with the
[Data TLS credentials](#configure-data-nodes), `--tls-replication`, and a
`--tls-port` listener. Add its advertised TLS address to its `[[data_nodes]]`
entry before creating the cluster:

```toml
[[data_nodes]]
id = "1111111111111111111111111111111111111111"
client_endpoint = "tcp://127.0.0.1:6371"
tls_endpoint = "tls://127.0.0.1:16371"
```

Here the Data process listens with `--port 6371 --tls-port 16371`. For a
TLS-only listener, use `--port 0 --tls-port 16371` and omit `client_endpoint`
from that node's manifest entry. At least one endpoint is required; when both
are present they must use the same numeric host and distinct ports. Data socket
addresses must be unique across the manifest, including across transports.
Keep certificate paths and keys in process configuration. Meta and Admin
descriptors retain their `tcp://` address spelling; their TLS mode comes from
the process TLS options, independently of these Data listener tags.

The CLI and Meta use only the current binary version 1 creation intent. Earlier
intent layouts have no decoder or mixed-version recovery contract; run the
updated Meta and CLI together.

Repeat `[[meta_members]]` for every first-wave voter. IDs and each endpoint
class must be unique; entries are canonicalized by ID, all members are voters,
and the principal is fixed as `lavik://meta/<id>`. At admission the manifest
set must exactly equal NuRaft's committed descriptors, the identity store, and
the Admin/Data-control directories. Cluster Create never calls `add_srv`.
Its durable root first waits at a fixed Raft barrier until every remote Meta
has recently replied and reports its state machine applied through the root's
submit index; only then does Data registration begin. During that phase the
leader deliberately waits for all Meta state machines. After the root advances,
normal writes return to majority-based completion, so an unavailable minority
does not stall unrelated control-plane mutations.

Run:

```sh
lavik-ctl cluster-create --manifest cluster.toml \
  --socket /var/lib/lavik/meta-1/meta-admin.sock
```

Review the normalized plan and data-erasure warning, then enter exactly
lowercase `yes`. EOF, any other input, or a failed parse exits before any Meta
request. Automation may pass `--yes`. `--timeout-ms` is one absolute deadline
for leader discovery and the Genesis proposal response; its default is 120
seconds. It does not wait for Data initialization or READY. Remote TCP uses
the same mTLS or explicit `--allow-plaintext-admin` policy as
`cluster-status`; `--json` does not apply.

Exit 0 means the root operation and lifecycle `creating` transition committed
atomically. The CLI prints
`Cluster create accepted: genesis committed=<index> operation=<id>` and returns
immediately. Exit 1 is a local manifest/confirmation failure or a connection or
protocol failure known to precede mutation. Exit 2 is an explicit Meta
rejection. Exit 3 means the mutation was sent but its commit outcome is
uncertain; preserve the printed/root operation id and run `cluster-status`
before taking further action.

Do not issue another create after accepted or uncertain outcomes. The leader
continues the accepted root in the background, and restart or leadership
change resumes it from snapshot/WAL. All subsequent creates—whether their
manifest is identical or different—are rejected as `already-created` once the
lifecycle is `creating`, `created`, or `provisioning-failed`. The system has no
attach, retry-existing, reset, or Meta reuse path.

Run `cluster-status` to follow the root id, Genesis index, current phase, and
runtime blockers; use `getop <root-operation-id>` while the operation remains
retained. A non-terminal record includes its phase; `recovery-required` means
the retained v1 intent no longer matches safe execution conditions. Preserve
Meta/Data logs and directories and investigate rather than reinitialize. A
changed Data boot/history, invalidated attempt, or changed topology does not
authorize another destructive reset.

Graceful Meta stop cancels result waits and leaves accepted work intact; it
does not wait for an offline Data node. A deterministic Data failure fences
only that Group before aborting its child and the overall creation. Already
completed Groups are not rolled back. While replica initialization is pending,
a changed source boot/history or a new boot on a replica still awaiting its
result follows this failure path and reports the affected Group and node.
The failure is retained across Meta restart; it does not automatically start
a new destructive attempt. Meta recovery preserves successful
population work and finishes bookkeeping without initializing it again.
Meta-member addition/removal and per-Group failover have separate recovery
drivers. Other Data migrations do not become recoverable merely because the
operation journal exists.

Interpret `cluster_state` independently from `READY / NOT READY / RETRYABLE`:

| `cluster_state` | Meaning | Next action |
|---|---|---|
| `uninitialized` | No Genesis committed and no Data-cluster artifacts exist | Validate the manifest, then run `cluster-create` once |
| `non-pristine` | Lifecycle is uninitialized but Data-cluster artifacts exist | Do not create; inspect the artifacts and rebuild the Meta data directory if this is discarded development state |
| `creating` | Genesis committed and the durable workflow is running or awaiting recovery | Wait and rerun `cluster-status`; inspect the reported phase and Meta logs if it stops advancing |
| `created` | Initial workflow completed; current Data runtime may still be NOT READY | Inspect ordinary blockers and Data sessions until READY |
| `provisioning-failed` | Genesis committed but initialization failed deterministically after fencing | Do not rerun create; preserve data and inspect the failure summary, root operation, and Meta logs |

`status_explanation` summarizes the current condition and `next_action` gives
the same guidance in both text and JSON output. A `RETRYABLE` result means no
stable leader-observed cut was available; verify quorum and Admin connectivity,
then rerun `cluster-status`.

During creation, `cluster-status` distinguishes `meta_catching_up`, a declared
but not yet committed `data_unregistered`, an unregistered Data process whose
Hello is actively retrying (`data_unregistered_retrying`), and a committed
identity that has never contacted the current leader (`data_unobserved`). If a
previously accepted or actively retrying node lacks a current accepted session,
the blocker is `data_session_missing`. Later Group blockers continue to name
missing projection, health, or population evidence.

## Run a controlled Group failover

Use controlled failover for planned maintenance while the current owner is
healthy. The command discovers the current Meta leader, verifies that the
cluster is `created` and the Group exists, submits one durable operation, and
returns without waiting for cutover:

```sh
lavik-ctl failover group-1 \
  --socket /var/lib/lavik/meta-1/meta-admin.sock \
  --failover-timeout-ms 120000
```

`--failover-timeout-ms` is the absolute workflow budget and accepts 1 through
86,400,000 ms; its default is 120 seconds. It is separate from
`--timeout-ms`, which bounds leader discovery and the Admin request/response
and defaults to five seconds. The CLI generates one operation id and absolute
deadline. An embedding that retries must preserve that exact pair; extending
the deadline on retry changes the request.

Exit 0 means only that the operation was committed. The CLI prints its commit
index and operation id; follow the latter on the current leader:

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  getop <operation-id>
```

The expected replies are `OK submitted` before a Group transition is installed,
`OK running` while the controlled transition owns the Group, `OK completed
failover-completed` after cutover, or `OK aborted <reason>` after a controlled
failure. `getop` is a committed local read rather than a linearizable follower
query, so use leader discovery again after a leadership change. Generic
`abortop`, `completeop`, and `submitop` reject the failover kind as
`workflow-owned`; there is no operator abort command for an accepted failover.

Exit 2 is an explicit preflight or proposal rejection, including a
`resource-exhausted` durability/resource gate that rejects before Raft append.
Exit 3 means the mutation may have committed; the error retains the generated
operation id, which must be resolved with `getop` before submitting another
failover. Exit 1 is a local, discovery, TLS, or transport failure known to occur
before submission. A malformed or otherwise untrustworthy response after
submission is exit 3 because the commit outcome cannot be inferred from it.

During a healthy controlled transition, the old owner continues reads and
lease renewal but all source mutations, including `PUBLISH`, return `TRYAGAIN
Failover in progress`. Work admitted before the pause drains before Meta uses
the stable source frontier. The chosen candidate must catch up through that
frontier and prepare before the atomic cutover, so a completed controlled
failover records `loss=none`. After cutover, full desired state makes every
non-owner follow the new Owner through native CONTINUE, direct-parent replay
and HistorySwitch, or destructive FULL. The former Owner fences and
drains accepted writes and expiration, then freezes its own source-domain
cursor before retiring that backlog; no separate cleanup command is required.

A trustworthy complete Active population remains Ready and candidate-eligible
during partial reparent and while a selected FULL awaits source admission. Its
reported cursor stays in the actual parent domain until HistorySwitch atomically
installs the child domain and complete origin vector. On authenticated FULL
admission, the target withdraws old Ready and candidate evidence, durably fences
storage, and performs destructive rebuild. It becomes eligible again only after
FULL completes. If every surviving replica is rebuilding when the new Owner
fails, the group can have no eligible Candidate; retaining Active throughout
FULL remains dependent on isolated staged population replacement. Inspect
actual population readiness and candidate
progress when diagnosing recovery; neither node count nor partial transfer
progress proves a complete population. There is no all-replica completion
barrier or Meta queue serializing these independent relationships.

The admin `observations <group>` command separates each reporter's own
`history` from its population's `source_term`, `source_node`, and
`source_history`. A candidate may remain Ready in an older source domain during
reparent. Before requesting another controlled cutover, verify that Meta has
observed a candidate in the current Owner's source domain; successful local
reads alone do not prove that its updated progress has reached Meta.

If the candidate is confirmed unavailable while the old owner is still usable,
Meta aborts the controlled operation immediately and service remains on the old
owner. The absolute deadline also aborts the controlled operation. If the
source remains unavailable beyond the observation grace, or its boot/history
is definitely replaced, Meta records the controlled request as aborted, fences
the old authority, and continues the same Group transition in uncontrolled
mode. An uncontrolled candidate failure selects a fresh action instead of
waiting for that process to restart when an eligible replacement exists;
otherwise Meta clears the failed action and waits with no candidate. A Meta
leader change temporarily waits for fresh boot-scoped observations, then
resumes from the committed Group transition. The warmup covers the configured
election upper bound plus Data's maximum reconnect sleep (and is never shorter
than the observation TTL); do not infer failure solely from that bounded
interval. A candidate's current-session disconnect or typed action failure
takes effect immediately. An exact source disconnect starts its independent
grace and degrades only if that grace expires or replacement evidence appears.

Uncontrolled recovery has Redis Cluster-grade asynchronous loss semantics. It
tries eligible compatibility domains from newest source term to older terms,
selects the strongest candidate inside one comparable domain, and records
`loss=unknown` for every newly selected action. An exact healthy action already
authorized lossless may instead retain `loss=none` across controlled
degradation. Writes acknowledged only by the failed owner may be absent on an
unknown-loss cutover.
Before prepare authorization, an ordinary uncontrolled Candidate with a
comparable history uses a bounded recovery budget to gather retained original
events from compatible members. Explicit operator recovery has no historical
frontier and skips this gathering phase. The default
is 2000 ms; zero skips active gathering while retention and partial reparent
remain enabled. After old write authority is excluded, Meta's recovery-start
proposal carries an absolute deadline calculated at proposal time; commit,
dispatch, discovery, connection, and transfer consume the budget. The proposing
leader preserves this cutoff across retries. Once committed, the transition
shares that deadline across Candidate and Meta Leader replacement. An
uncommitted proposal lost with its leader cannot preserve its cutoff. A policy
update affects only transitions whose cutoff has not been fixed, and does not
reset automatic failure suspicion. Expiry safely drains accepted apply and prepares from the
actual complete frontier; it does not make the outcome lossless or terminate a
transition waiting for a Candidate. Data logs `candidate recovery completed`
with action, reason, and actual Applied vector.

The current Meta Leader also runs an Automatic Failover Detector for every
Created Group. Only a current authenticated Owner heartbeat matching the exact
boot, assignment, projection, term, authority, grant, storage, population, and
causally confirmed lease can prove serviceability. Exact `session_missing`,
`heartbeat_expired`, `draining`, `storage_unready`, or `population_unready`
evidence sustained for the current Policy's debounce interval submits an
uncontrolled Begin. Candidate availability does not delay the committed term
fence; the ordinary uncontrolled executor selects a recovery candidate later.
Silent loss takes approximately the observation TTL plus the full debounce and
proposal latency. A Meta Leader replacement intentionally discards accumulated
suspicion, reacquires observations, and gives the failure a fresh full
debounce. Inspect the per-Group detector fields in `cluster-status` when
diagnosing RTO rather than inferring progress from process reachability.

### Restart and operator recovery

A member that completes graceful shutdown publishes a one-use Clean Shutdown
Proof after its data and transaction decisions are durable. On restart it
consumes that proof and can report its recovered frontier for ordinary automatic
selection without rebuilding. It remains fenced until Meta commits promotion
and grants a fresh lease. This applies to the first Owner and ordinary replicas
as well as later Owners; `shutdown-checkpoint` is not required.

A crash or interrupted shutdown without a published proof requires FULL before
automatic candidacy. When the Group has no eligible Candidate and no usable
Owner can supply FULL, select one readable recovered member explicitly:

```bash
lavik-ctl --socket /path/to/leader.sock \
  promote group-1 --node 2222222222222222222222222222222222222222 \
  --accept-data-loss
```

This direct Admin command must reach the Meta leader and requires operator
credentials and Meta quorum. `OK <index> promote loss=unknown action=<id>` means
the selection committed; use `cluster-status` to observe Cutover and readiness.
The selected node establishes a new history over its recovered local data.
Acknowledged writes may be missing; `--accept-data-loss` is required and the
committed action records the recovery choice. The command rejects an eligible
automatic Candidate, a serving Owner, an occupied transition, stale membership,
and absent readable recovery evidence. Corrupt storage/catalog or incomplete
destructive FULL must be repaired or rebuilt, and cannot be promoted.

For a whole-cluster outage, make this choice separately for each affected
Group. There is no automatic selection of arbitrary crash-recovered data. If a
request times out, inspect the Group's transition/action and audit before
issuing another mutation; a timeout does not prove rejection.

For postmortems, search Meta logs for `failover event=`. Accepted transition
commits carry `mode`, `group`, `transition`, `action`, `loss`, and
`commit_index`; candidate selection/replacement/domain fallback and bounded
abort/degrade reasons add their relevant source or candidate fields. An
automatic Begin additionally records its exact trigger `reason` and
`suspect_ms` duration. Entries
with possible data loss are warnings. Opaque variable values are canonical
percent-encoded, so split fields on whitespace and the first `=` before
decoding values; embedded whitespace, control bytes, and `=` cannot create new
fields. Collection is at least once, including when logs from several Meta
replicas are combined, so deduplicate records by `commit_index`. Exact
post-effect replay suppresses the state-dependent event for
`SetUncontrolledCandidate` and post-Begin `AbortControlledFailover`, because
their original candidate classification or action identifier is no longer in
the post-state. Correlate the resulting sequence with the authoritative durable
audit history and the terminal `getop` result.

For plaintext remote administration, configure a listener and connect without
TLS arguments:

```sh
lavik-meta --id 1 --addr 10.0.0.11:7100 \
  --data-control-addr 10.0.0.11:7300 \
  --data-dir /var/lib/lavik/meta-1 \
  --initial-cluster-manifest /etc/lavik/cluster.toml \
  --ctl-addr 10.0.0.11:7200

lavik-ctl --addr 10.0.0.11:7200 status
```

All plaintext peers share the audit actor
`lavik://operator/plaintext`; source IP addresses are not treated as
authenticated identities. Use the mTLS configuration below when distinct,
cryptographically authenticated operators or data nodes need the remote
surface.

## Run a local plaintext cluster

Raft transport is plaintext when no `--tls-*` arguments are present. The
following three-process cluster is suitable for local development; production
addresses and durable directories should be managed by the service manager
instead of background shell jobs:

```sh
META_BIN=./build-clang/lavik-meta
META_ROOT=/tmp/lavik-meta-demo
install -d -m 0700 "$META_ROOT" \
  "$META_ROOT/node1" "$META_ROOT/node2" "$META_ROOT/node3"
INITIAL_MANIFEST="$META_ROOT/cluster.toml"
```

Create `cluster.toml` with the intended Data/Group sections and these three
Meta entries (the parser normalizes them by id):

```toml
[[meta_members]]
id = 1
raft_endpoint = "tcp://127.0.0.1:7101"
data_control_endpoint = "tcp://127.0.0.1:7301"
ctl_endpoint = "tcp://127.0.0.1:7201"

[[meta_members]]
id = 2
raft_endpoint = "tcp://127.0.0.1:7102"
data_control_endpoint = "tcp://127.0.0.1:7302"
ctl_endpoint = "tcp://127.0.0.1:7202"

[[meta_members]]
id = 3
raft_endpoint = "tcp://127.0.0.1:7103"
data_control_endpoint = "tcp://127.0.0.1:7303"
ctl_endpoint = "tcp://127.0.0.1:7203"
```

Start all three first-wave members with that same file:

```sh

"$META_BIN" --id 1 --addr 127.0.0.1:7101 \
  --data-control-addr 127.0.0.1:7301 \
  --ctl-addr 127.0.0.1:7201 \
  --ctl-socket "$META_ROOT/node1/meta-admin.sock" \
  --data-dir "$META_ROOT/node1" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node1.log" 2>&1 &

"$META_BIN" --id 2 --addr 127.0.0.1:7102 \
  --data-control-addr 127.0.0.1:7302 \
  --ctl-addr 127.0.0.1:7202 \
  --ctl-socket "$META_ROOT/node2/meta-admin.sock" \
  --data-dir "$META_ROOT/node2" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node2.log" 2>&1 &

"$META_BIN" --id 3 --addr 127.0.0.1:7103 \
  --data-control-addr 127.0.0.1:7303 \
  --ctl-addr 127.0.0.1:7203 \
  --ctl-socket "$META_ROOT/node3/meta-admin.sock" \
  --data-dir "$META_ROOT/node3" \
  --initial-cluster-manifest "$INITIAL_MANIFEST" \
  >"$META_ROOT/node3.log" 2>&1 &
```

Query the processes until exactly one reports `leader=1`. No `addsrv` command
is part of this initial startup:

```sh
lavik-ctl --socket "$META_ROOT/node1/meta-admin.sock" status
lavik-ctl --socket "$META_ROOT/node2/meta-admin.sock" status
lavik-ctl --socket "$META_ROOT/node3/meta-admin.sock" status
```

On every later restart, use the same process arguments but remove
`--initial-cluster-manifest`. The durable config, not this genesis file, is
then authoritative.

For later expansion, replacement, or contraction, start a pristine waiting
joiner without a manifest and use the reusable membership workflow below.
`addsrv` and `removesrv` persist a workflow before changing identity or Raft
membership. `OK` means the requested configuration and identity changes have
committed, not just that NuRaft accepted an invite. Before adding the next member, poll the
new member's `status` until it remains alive and its `committed` index reaches
the leader value observed after the add. The current `status` command does not
list the membership set; a replicated write observed on the joiner is the
stronger end-to-end check when an automation needs proof of convergence.

The server bounds the Admin wait using `--client-req-timeout-ms`. An
`ERR uncertain-outcome operation=<id>` reply or disconnected client does not
cancel the accepted task. The current leader retries in the background; after
restart or leader change it finds the task in the recovered journal and
continues without another command. An identical in-flight `addsrv/removesrv`
request waits on that same task, while a conflicting request returns
`ERR config-changing`. Query `getop <id>` for the durable phase/result; Meta
also logs `membership <id> phase=...`. Generic `abortop/completeop` cannot
release a membership reservation while an accepted Raft change may still
commit. A `recovery-required` phase requires investigation of the retained
intent and actual configuration, not a replacement change or automatic
identity reactivation. A direct request to remove the current leader remains
rejected; if a previously authorized removal target later becomes leader,
recovery performs a leadership handoff before continuing its removal.

Plaintext peers still check the claimed Raft source and destination ids against
the configuration and committed identity bindings, but those ids are not
cryptographically authenticated. Use plaintext only where network access and
routing are already trusted.

## Configure Raft mTLS

mTLS uses one CA trusted by the whole Meta cluster and a distinct certificate
and private key for every member. Member `N` must have exactly one URI SAN in
total, the canonical `lavik://meta/N` principal, plus IP or DNS SANs covering
both its Raft and Data-control advertised hosts (one SAN suffices when they
share a host).
The certificate must be usable for both TLS server and TLS client
authentication. Never copy one member's certificate or key to another member.

Use an organization-managed CA in production. The following OpenSSL commands
show the required certificate shape for a disposable development cluster:

```sh
TLS_ROOT=/tmp/lavik-meta-tls
install -d -m 0700 "$TLS_ROOT"
umask 077

openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 30 \
  -subj '/CN=Lavik Meta Development CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "$TLS_ROOT/ca.key" -out "$TLS_ROOT/ca.crt"

issue_meta_cert() {
  member_id=$1
  member_ip=$2
  openssl req -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=lavik-meta-$member_id" \
    -addext "subjectAltName=IP:$member_ip,URI:lavik://meta/$member_id" \
    -addext 'extendedKeyUsage=serverAuth,clientAuth' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
    -keyout "$TLS_ROOT/meta-$member_id.key" \
    -out "$TLS_ROOT/meta-$member_id.csr"
  openssl x509 -req -sha256 -days 30 \
    -in "$TLS_ROOT/meta-$member_id.csr" \
    -CA "$TLS_ROOT/ca.crt" -CAkey "$TLS_ROOT/ca.key" \
    -CAcreateserial -copy_extensions copy \
    -out "$TLS_ROOT/meta-$member_id.crt"
}

issue_meta_cert 1 10.0.0.11
issue_meta_cert 2 10.0.0.12
issue_meta_cert 3 10.0.0.13
```

Protect the CA key offline in a real deployment. Distribute only `ca.crt` and
the matching member leaf/key to each host. Start each process with its own leaf
and the shared CA, for example member 1:

```sh
lavik-meta \
  --id 1 --addr 10.0.0.11:7100 --data-dir /var/lib/lavik/meta-1 \
  --data-control-addr 10.0.0.11:7300 \
  --ctl-addr 10.0.0.11:7200 \
  --initial-cluster-manifest /etc/lavik/cluster.toml \
  --tls-ca /etc/lavik/meta/ca.crt \
  --tls-cert /etc/lavik/meta/meta-1.crt \
  --tls-key /etc/lavik/meta/meta-1.key \
  --ctl-tls-ca /etc/lavik/meta/ca.crt \
  --ctl-tls-cert /etc/lavik/meta/meta-1.crt \
  --ctl-tls-key /etc/lavik/meta/meta-1.key
```

Start every initial member with the equivalent member-specific certificate
and the same manifest. The first election authenticates the complete manifest
peer set; it does not join members sequentially. Omit the manifest from every
later restart. A future dynamic joiner starts with mTLS but without a manifest,
then enters through `addsrv` with all three endpoints.
The Data-control listener reuses the same CA, certificate, and key; there is no
second Data-control TLS option set. `--tls-ca`, `--tls-cert`, and `--tls-key`
are all-or-nothing; partial TLS configuration fails startup. Start the joiner
with mTLS before issuing `addsrv`, and ensure its URI SAN matches the id in that
command.

Do not mix plaintext and mTLS members. Enabling or disabling Raft TLS on an
existing cluster requires a coordinated restart of all members; it does not
change the WAL or snapshot format. `--raft-io-threads` sizes NuRaft's native
Asio pool (default 2); it does not change the single Bycorf control-session
worker or make WAL synchronization asynchronous.

## Configure Data nodes

A Meta-managed Data process needs its committed 40-character lowercase hex
node id and one or more numeric Data-control seeds. It is the only production
Cluster control mode. Before starting a new Data process, register
that identity and its client endpoint on the Meta leader. The endpoint is
tagged `tcp://` or `tls://`; a dual-listener node may supply one of each, using
the same numeric host:

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  registernode 0123456789abcdef0123456789abcdef01234567 \
  lavik://node/0123456789abcdef0123456789abcdef01234567 \
  primary tcp://10.0.1.11:6379
```

The role here is registered identity metadata, not a lease or current group
ownership. Registration alone therefore lets the node authenticate and receive
an empty-topology full state, but it remains fenced/LOADING until later
committed topology, population, and grant state make it ready.

After creating a group, add membership with `assignnode <group-id> <node-id>
<primary|replica>`. The command deliberately has no assignment-id argument:
the trusted Meta proposer generates a fresh nonzero 128-bit value from the OS
CSPRNG for that membership incarnation. Repeating the same desired membership
is idempotent; removing and later re-adding it generates another identity.

The authenticated operator surface also exposes the typed commits needed to
assemble or revoke finite authority. These are low-level, absolute-state
operations intended for controlled bootstrap and recovery workflows:

```text
putpolicy <policy-id> <version> <content>
getpolicy <policy-id>
setslotmap <first> <last> <group-id>
activateauthority <group-id> <expected-term> <owner-node-id>
fencegroup <group-id> <expected-term>
```

`putpolicy` accepts only a compiled-in Policy id and that family's strict,
compact JSON document. It stores and compares the bounded raw bytes directly;
Policy commands and state contain no content hash. Versions start at 1 and
advance consecutively. `getpolicy` returns the current version and its exact
raw document. The only accepted families and update forms are:

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  putpolicy lavik.automatic-uncontrolled-failover-v1 2 \
  '{"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000}'
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  putpolicy lavik.authority-lease-v1 2 \
  '{"kind":"authority-lease-v1","duration_ms":5000}'
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  putpolicy lavik.candidate-recovery-v1 2 \
  '{"kind":"candidate-recovery-v1","budget_ms":2000}'
```

Automatic failover is always active. Its Policy configures only the finite
suspicion threshold; neither the Policy JSON nor the bootstrap manifest accepts
an enable/disable field. Without an eligible candidate the Group remains fenced
until a candidate becomes eligible or an operator explicitly accepts data loss
and selects a recovered population with `promote`.

The same 1,000–86,400,000 ms automatic threshold and 100–86,400,000 ms lease
range, plus the 0–86,400,000 ms recovery budget, apply to runtime updates. Field reordering is accepted, but whitespace,
missing/duplicate/unknown fields, other ids or `kind` values, type mismatches,
and non-consecutive versions are rejected. `setslotmap` replaces the entire
slot map with one inclusive range—it is not an incremental assignment command.
Both slot endpoints must be within 0–16383. If the
replacement changes a group's slot coverage, every affected
source and destination group must first be fenced; apply rejects the whole map
while any such group has an active grant. Activate fresh authorities only
after the complete replacement commits.

`activateauthority` is the atomic owner/grant commit. The term must already
have been established with `begingroupterm`, and the owner must hold a current
assignment. It can install the sole Grant of a still-grantless term; it cannot
replace an active Grant. Removing a Grant advances the term, so even
reauthorizing the same Owner requires the new term. Use the typed failover
workflow for normal Owner changes: it advances the Group Term and prepares the
Candidate before activation. This low-level primitive only installs authority
in a previously reserved grantless term; it does not perform that preparation.
The expected term is an absolute value, not an increment. Raft
apply rejects stale or conflicting transitions without changing authority and
may accept an identical domain effect idempotently. The Data projection resolves
lease duration from the current global Authority Lease Policy and caps it by the
local Meta leadership-validity interval. Data derives its heartbeat interval
from that effective duration as `max(1 ms, duration / 3)`; heartbeat cadence is
not configured or transmitted independently. Grants do not store a duration
or Policy reference. Only the cluster-wide topology epoch is derived by the
leader from its committed snapshot.
`fencegroup` removes the Grant under the explicit expected-term CAS and
atomically advances the Group Term by one, leaving that new term grantless.
Treat `setslotmap`, `activateauthority`, and `fencegroup` as dangerous: verify the
current leader and intended group/owner before issuing them, and do not retry
an uncertain result until the committed state has been checked.

These commits alone do not make a newly assigned Data node population-ready.
Until a reconciliation workflow installs a matching ReadyToken, heartbeats
challenge the committed grant but receive a node-not-ready denial and the Data
node remains fenced/LOADING. Never interpret `activateauthority` returning
`OK` as proof that client writes are enabled.

For a plaintext development deployment, start the registered node with:

```sh
lavik --cluster-enabled \
  --cluster-node-id 0123456789abcdef0123456789abcdef01234567 \
  --cluster-meta-seed 10.0.0.11:7300 \
  --cluster-meta-seed 10.0.0.12:7300 \
  --cluster-meta-seed 10.0.0.13:7300 \
  --data-file /var/lib/lavik/data-1/lavik.data
```

The client first tries its volatile accepted-leader hint, then the latest
committed in-memory Meta directory, then these seeds. It does not persist that
directory, a lease, a term floor, or desired state. Every restart creates a new
boot identity and begins fenced/LOADING until a leader supplies and accepts a
complete projection and finite authority.

Lease expiry is suspend-aware: Data checks deadlines with Linux
`CLOCK_BOOTTIME`, and a new Meta leader or authority identity waits twice the
configured Raft election lower bound before its first otherwise-valid grant.
The second interval is an internally derived cross-host clock margin, not a
separate operator setting. A host suspend therefore consumes an existing lease
instead of extending it. Meta additionally detects suspend against NuRaft's
active clock, closes authority sessions, logs a quarantine warning, and
requests immediate resignation; a sole member must run for one election-lower-
bound interval before accepting authority again. Repeated client reconnects
during that interval are expected and must not be worked around by relaxing
the timing bound. The Meta listener also admits at most 4096 sockets
that have not yet completed TLS/`ClientHello` and either finished a follower
redirect or claimed a leader-side node session slot. A committed node has only
one such leader slot, including while its initial full-state transfer is
stalled; duplicates are rejected until the incumbent exits. Excess sockets are
closed and should be investigated as connection storms or untrusted-network
exposure.
Decoded-plus-encoded projections share a 2 GiB retained-capacity budget, with
1 GiB reserved while each FDS is built. These values are derived from the
512 MiB object cap and the old/new generation overlap. Budget exhaustion closes
the affected setup or publisher session; investigate an abuse-sized committed
projection or excessive concurrent FDS ownership rather than retrying without
first reducing that state.

To enable mTLS, the Data client reuses the existing replication TLS settings;
there are no separate Meta-control certificate flags. Its certificate must
have exactly one URI SAN in total, the canonical
`lavik://node/<node-id>` principal, an IP SAN for the Data endpoint, and both
client/server usages. The URI must equal the active Meta identity binding for
that node. For example:

```sh
lavik --cluster-enabled \
  --cluster-node-id 0123456789abcdef0123456789abcdef01234567 \
  --cluster-meta-seed 10.0.0.11:7300 \
  --cluster-meta-seed 10.0.0.12:7300 \
  --tls-port 6380 --tls-auth-clients yes --tls-replication \
  --tls-ca-cert-file /etc/lavik/data/ca.crt \
  --tls-cert-file /etc/lavik/data/node-01234567.crt \
  --tls-key-file /etc/lavik/data/node-01234567.key \
  --data-file /var/lib/lavik/data-1/lavik.data
```

All Data and Meta certificates used for this connection must chain to the
configured trust roots. Partial TLS inputs fail startup; neither side falls
back to plaintext. In plaintext mode, protect both Raft and Data-control ports
with the same private-network assumptions as the replication path.

Remote administration configures its server certificate independently with
`--ctl-tls-ca`, `--ctl-tls-cert`, and `--ctl-tls-key`. It may reuse that Meta
member's Raft certificate when the control listener uses an IP or DNS already
covered by the certificate and the leaf permits `serverAuth`; otherwise issue
a dedicated server leaf with a SAN covering `--ctl-addr`. The client can select
a DNS SAN instead of the numeric control address with `--tls-server-name` for
direct commands.

Every client certificate must carry exactly one canonical operator URI SAN.
The following development example uses the CA created above to issue
`lavik://operator/admin`; production deployments should use their managed
certificate issuer and normal lifetime/rotation policy:

```sh
umask 077

openssl req -newkey rsa:2048 -nodes -sha256 \
  -subj '/CN=lavik-meta-operator-admin' \
  -addext 'subjectAltName=URI:lavik://operator/admin' \
  -addext 'extendedKeyUsage=clientAuth' \
  -addext 'keyUsage=critical,digitalSignature' \
  -keyout "$TLS_ROOT/operator-admin.key" \
  -out "$TLS_ROOT/operator-admin.csr"

openssl x509 -req -sha256 -days 30 \
  -in "$TLS_ROOT/operator-admin.csr" \
  -CA "$TLS_ROOT/ca.crt" -CAkey "$TLS_ROOT/ca.key" \
  -CAcreateserial -copy_extensions copy \
  -out "$TLS_ROOT/operator-admin.crt"
```

Use that operator identity with the control client:

```sh
lavik-ctl --addr 10.0.0.11:7200 \
  --tls-ca /etc/lavik/meta/ca.crt \
  --tls-cert /etc/lavik/meta/operator-admin.crt \
  --tls-key /etc/lavik/meta/operator-admin.key \
  status

lavik-ctl cluster-status --addr 10.0.0.11:7200 \
  --tls-ca /etc/lavik/meta/ca.crt \
  --tls-cert /etc/lavik/meta/operator-admin.crt \
  --tls-key /etc/lavik/meta/operator-admin.key \
  --json
```

`cluster-status` has no DNS server-name override: every committed Admin route
is numeric and must appear as an IP SAN.

## Add and remove peers

Membership commands are accepted only by the current leader and only one
change may be active at a time. To add a peer:

1. Allocate a never-before-used positive id, private data directory, Raft
   endpoint, distinct Data-control endpoint, and concrete Admin endpoint. With
   mTLS, issue its matching certificate first.
2. Start the new `lavik-meta` process without
   `--initial-cluster-manifest`; its pristine directory becomes a durable,
   election-disabled waiting joiner.
3. On the current leader, run
   `addsrv <id> <raft-endpoint> <data-control-endpoint> <ctl-endpoint>`. A
   fifth principal argument is accepted but may only be the matching canonical
   `lavik://meta/<id>`; omitting it selects that value automatically.
4. Poll the joiner's `status` and verify replicated progress before adding
   another peer or relying on it for quorum.

For example:

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  addsrv 4 10.0.0.14:7100 10.0.0.14:7300 10.0.0.14:7200
```

The leader first commits and audits the complete membership intent, then its
background owner binds identity and invokes NuRaft `add_srv`. A successful
invite alone does not complete the task. `ERR config-changing` indicates a
different active workflow; an uncertain-outcome reply retains the original
operation id and the leader continues retrying independently of the client.
`ERR already-exists` may mean an earlier invite committed; verify the joiner
rather than creating a different identity.

To remove a peer, select a follower and run this on the leader:

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock removesrv 4
```

On success, Lavik first observes the committed configuration without the peer, then commits and
audits retirement of that member's identity before replying `OK`. Stop the
removed process after the command succeeds. The retired id and principal are
terminal and cannot be reactivated; replacing that machine requires a new id,
fresh data directory, and, with mTLS, a new certificate.

Remove one member at a time and preserve a quorum throughout. Prefer removing
a follower; directly removing the current leader returns `ERR cannot-remove-leader`.
`ERR config-changing` indicates another durable membership/creation task is active.
Never wipe or repurpose a member's data directory before its removal has
committed and the remaining cluster has elected a healthy leader.

## Status and snapshots

Send one LF-terminated command per connection or keep a connection open and
read exactly one reply line per command. `status` reports whether that member
is leader, its server id, committed and snapshot indexes, and current term. It
is a local view; compare all members when diagnosing lag.

Automatic snapshots run according to `--snapshot-distance`; `snapshot` asks
the leader for a commit-serialized capture and returns its cut index. The reply
means capture succeeded, while durable publication and WAL compaction complete
asynchronously. Monitor logs for `snapshot write failed`, snapshot decode or
install errors, and repeated snapshot failures. A leader that exceeds the
uncompacted-WAL or repeated-snapshot-failure guard rejects new proposals with
`RESOURCE_EXHAUSTED` rather than expanding indefinitely.

When that guard fires:

1. Preserve logs. Stop one affected replica at a time and take a filesystem
   snapshot or backup of its data directory before destructive intervention.
2. Confirm a quorum is healthy and compare `committed`, `snapshot_idx`, and
   `term` on every member.
3. Fix filesystem space, permissions, I/O, or snapshot-size pressure. Do not
   delete WAL segments from a running node.
4. Trigger `snapshot` on the leader and wait for `snapshot_idx` to advance and
   the error stream to stop before retrying writes.
5. If one replica remains damaged, remove it from membership before replacing
   its data directory. Retired member identities are terminal, so provision a
   new server id and matching certificate for the replacement. Never wipe a
   quorum simultaneously.

The formal WAL/snapshot format does not migrate prototype `raft_log.dat` or
`LSN1` snapshots. Back up such a directory, then bootstrap a fresh formal
cluster; startup intentionally refuses to guess at a conversion.

## Binary replacement and format compatibility

Before the first stable release, all Lavik-owned durable and control formats
use v1, including the Raft command envelope, topology store,
segmented-WAL container, membership descriptors and intents, cluster-create
intents, and cluster-status binary/JSON payloads. There is no decoder for
superseded pre-release layouts. Development directories from an incompatible
layout must be rebuilt even when their markers are also v1; marker checks
cannot detect every same-marker schema change. Equal version numbers do not
make incompatible builds safe to mix.
There is no mixed-format window or in-band format switch. For a binary-only
change that preserves every durable format, replace one follower at a time,
wait for catch-up, and replace the leader last. Before any replacement, back
up every member and record the membership, term, commit index, and snapshot
index.

For an incompatible pre-release format change, stop the old cluster and create
fresh data directories with the new binary. Do not add a new-format process to
an old-format membership or copy old snapshots/WAL into the new directory.
Unknown format markers intentionally fail loudly rather than attempting an
implicit conversion.

## Configure and export audit history

Audit defaults to `bounded-rotate`: service remains available at capacity,
while `status` exposes `audit_dropped_total` and `audit_dropped_through` so an
archival gap cannot be mistaken for complete history. The replicated choices
are `setauditpolicy disabled <attestation>`, `setauditpolicy bounded-rotate
<attestation>`, and `setauditpolicy strict-export <attestation>`. Policy
changes always produce an audit record. Use disabled only under an explicit
operational exception; the missing ordinary records are intentional.

Strict-export is the fail-safe retention mode. Before selecting it, ensure the
window has room for the policy-change record. Choose an index still in the
window and request `exportaudit <through-index>`. Decode and verify the
versioned record blob and its drop watermarks in the external archival
system, store it durably under an archive-defined deployment namespace, and
deduplicate by Raft log index, comparing full records on duplicate exports.
The export has no cryptographic chain or tamper-evidence guarantee. Lavik
does not persist a separate cluster identity. Only after that acknowledgement
should an operator issue `pruneaudit <through-index>`. The prune is replicated
and advances the prune floor; there is no in-process record of the external
acknowledgement.
At a full window, overlapping prune attempts are rejected until the outstanding
prune's Raft outcome resolves, including when its client has already timed out.

Terminal operations may be moved into bounded archive summaries with
`archiveoperations <seq>...`. `exportoperations` returns all current summaries
as a versioned hex blob. After durable external storage, `pruneoperations
<seq>...` removes exactly the listed summaries through a replicated command.
Pruning a summary ends the local late-retry tombstone window for that operation
id, so retention must cover the clients' documented retry horizon.

The current line protocol returns export bytes as hexadecimal on one line.
Protect these responses as audit data and avoid terminal logging that could
copy principals or operation results into an ungoverned sink.

## Recover from the durability fail-safe

Repeated snapshot failure or excessive uncompacted WAL puts the leader into a
durability fail-safe. Ordinary proposals then return `ERR
resource-exhausted`; Data nodes retain no local authority that can bypass this
gate. Stop automated mutators, identify the current leader with `status`, and
correct the underlying disk, permission, or size problem first.

The gate accepts one effect-producing recovery proposal at a time. It rejects a
stale or no-op command before Raft append, even when the command's verb is on
the recovery allowlist. A client timeout does not release the reservation: the
next recovery proposal remains rejected until NuRaft resolves the first one's
actual outcome. Reconcile that outcome from the leader's committed view before
moving to the next step.

If strict-export audit retention is also full, export and durably acknowledge a
large enough prefix first, then run `pruneaudit <through-index>`. The proposed
prune must make the serialized audit window smaller after accounting for the
prune command's own audit record.

For retained live generic operations, use this bounded sequence. Keep the
original operation id and sequence returned by `submitop`; `getop` does not
return the sequence and is not a linearizable read on a follower. Cluster
Create, Meta membership, and failover are workflow-owned and reject
`abortop`/`completeop`; their dedicated reconcilers, rather than this fail-safe
escape hatch, own safe terminalization.

```sh
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  abortop 00000001000000000000000000000001
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  archiveoperations 12345
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  exportoperations
# Verify and durably store the exported blob before removing its retry tombstone.
lavik-ctl --socket /var/lib/lavik/meta-1/meta-admin.sock \
  pruneoperations 12345
```

The empty `abortop` reason is intentional; `completeop <id>` with an empty
result is the equivalent successful terminalization path. Nonempty result or
reason payloads are rejected while the fail-safe is active. Each command above
must change the named committed record: repeating an already-applied archive or
prune does not consume another log or audit slot.

After enough aggregate state has been removed, run `snapshot` and wait for
`snapshot_idx` to advance. A successful snapshot compacts the WAL and clears
the durability pressure on that member; if the guard remains active, continue
with another known, effect-producing recovery item rather than sending dummy
prunes.
