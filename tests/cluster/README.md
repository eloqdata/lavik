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

# Deterministic cluster fault harness

This directory owns the reusable test boundary for cluster HA behavior. It
deliberately models protocol evidence with test-only strong types without
prescribing production Meta, lease, replication, or migration schemas.

The property engine is the in-tree `ScenarioRunner`, with GoogleTest
used for assertions and CTest discovery. This avoids adding a second test
dependency while preserving the required generated schedules, shrinking, and
seed replay. Scenarios can adopt a dedicated property library without changing
LFT1 or the scenario interface.

## Components and determinism contract

- `fault_harness.*` schedules sorted enabled actions with a stable PRNG;
  `fault_trace.cpp` owns canonical LFT1 encoding and persistence. Exact-model
  replay compares every choice, fault-checkpoint acknowledgment, observation,
  and finding. The minimizer keeps only changes that reproduce the same
  invariant ID and witness.
- `fault_controls.*` provides a manual monotonic clock, logical-envelope
  network delay/reorder/duplicate/drop/partition/disconnect controls with
  connection generations, volatile versus durable storage with partial
  persistence and injected errors, and a committed-state Meta replay/snapshot
  reference machine. It is not a Raft implementation.
- `reference_model.*` defines scenarios and their protocol-independent
  observations; `cluster_invariants.cpp` evaluates stable assertion IDs for
  authority, promotion, replication evidence, single-group population
  readiness, applied vectors, Meta directives, migration, client outcomes, and
  Redis-compatible control errors. The positive HA scenario composes all
  deterministic controls, then explores independent fault, delivery,
  disconnect, persistence, Meta recovery/directive replay, fencing, readiness,
  and response-decision orders from the supplied seed.
- `../support/process.*` provides process groups, pause/resume/stop, bounded
  waits, retained failure artifacts, and loopback port reservations. Real
  process tests use this support; a restart is a new `ChildProcess` using the
  same fixture configuration.
- `test_topology_installer.h` is an in-process-only adapter for programmatic
  `ServingState` values. It installs them through the real FDS projection seam
  and grants only finite, session-scoped leases; production targets do not
  compile or link it.

LFT1 has two declared modes. `exact-model` is byte-for-byte replayable and is
appropriate for checked-in regression inputs. `process-action-script` records
the intended actions for a real-process run; OS scheduling and transport
observations may drift, so replay must diagnose drift rather than promise an
identical execution. The current runner executes exact-model traces. Real
cluster fixtures can use the process mode as their adapter contract without
changing the durable exact-model format.

The fault-enabled server is built only with both `BUILD_TESTING=ON` and
`LAVIK_BUILD_FAULT_SERVER=ON`. It keeps existing crash/write hooks available
under optimized sanitizer builds and produces `lavik_fault_server`; release
packaging explicitly disables the option.

## Verification matrix

Population observations in this matrix are protocol-independent reference
model fields. In particular, `staging-hidden` and `atomic-activation` name an
abstract exposure boundary; they do not imply that production keeps a second
staging root or performs a physical root swap. Native production full sync is
a destructive in-place reset hidden by LOADING. The Meta-managed production
path starts fail-closed and exposes callable manager APIs to apply a complete
rebuild directive, query boot-scoped status, and authorize or revoke an exact
source export. That adapter drives `ReplicationGroup` through the existing
native reset/snapshot/tail/promote/abort path. Status exposes `NOT_READY` and
`REBUILDING` while no population is published, then may expose a ready token or
a group-identity-bound terminal `FAILED_STOPPED` result. #20 connects that
boundary to the authenticated Meta transport; focused tests inject full-state
projections, leases, and rebuild directives through that control plane.

Runtime reset still spans all 16,384 physical partitions; an authorized source
scans baseline data only for manifest members and sends empty handoffs for
non-members.
The Function-catalog row proves completion at the current native cut and uses
the durable catalog generation installed by storage. Cluster population
readiness is boot-scoped, so every restarted process begins `NOT_READY`; a
durable incomplete-full-sync fence also prevents a mixed population from being
exposed before a fresh full sync.

| Safety claim | Stable invariant | Fast model coverage |
|---|---|---|
| One full authority incarnation (node, boot, term, grant) covers admission, in-flight work, background mutation, and success decisions | `authority.single-writer`, `client.operation-single-authority`, `client.valid-authority-at-admission`, `client.safe-success-decision` | same-node-incarnation assertions, dual-authority LFT1 regression, and client history assertions |
| Candidate selection is separate from durable activation | `promotion.safe-activation`, `promotion.durable-before-write-authority` | snapshot assertions and composed failover scenario |
| Promotion binds the durable base to the validated population and current durable Function catalog generation | `promotion.durable-base-before-activation`, `promotion.catalog-token-current` | snapshot assertions and stale-catalog-promotion LFT1 regression |
| Other replicas remain intact until activation and child history is ready before writes | `promotion.keep-replicas-until-activation`, `history.child-ready-before-write` | snapshot assertions |
| Restart invalidates old partial-sync and Meta population-readiness evidence | `replication.restart-invalidates-evidence` | stale-evidence LFT1 regression, group-API reconstruction checks at each rebuild boundary, and real-process recovery of a partial SSD image |
| Live reparent requires compatible domains, an exact cursor, contiguous retained events, and a complete transaction boundary | `replication.compatible-resume-domain`, `replication.reparent-requires-complete-history` | vector tests and history-gap LFT1 regression |
| A crashed rebuild cannot expose staging or a half-active population | `population.staging-hidden`, `population.atomic-activation` | storage controls and partial-activation LFT1 regression |
| Function mutations keep staging hidden and make the complete catalog durable and installed before publication, cursor advancement, or ACK | `function.catalog-staging-hidden`, `function.catalog-durable-before-visible`, `function.catalog-installed-before-progress`, `function.catalog-durable-before-ack` | snapshot assertions and catalog-ack-before-durable LFT1 regression |
| Full sync durably invalidates old population, promotion, and catalog-readiness evidence before transfer and stays fenced until catalog plus population activation | `fullsync.destructive-invalidation-before-transfer`, `fullsync.fenced-until-activation`, `fullsync.catalog-and-population-ready` | snapshot assertions and fullsync-retains-old-state LFT1 regression |
| One process boot accepts directives for at most one replication group | `population.one-node-one-group` | assigned/directive group mismatch and matching-group assertions |
| Destructive reset requires safe-source authority bound to the accepted directive | `population.safe-source-before-destructive-reset` | reset-without-authority mutant and positive assertion |
| Readiness, readability, and candidacy use the exact boot-scoped rebuild identity and manifest | `population.readiness-identity-bound` | mutations of every identity component and manifest identity |
| Readiness requires all 16,384 reset/handoffs, exact logical-to-target-local epoch mapping, the Function catalog, every flow cut, storage promotion, and no in-flight apply | `population.readiness-proof-complete` | one missing-proof mutant per evidence component plus sparse-manifest/local-epoch group tests |
| A partial in-place rebuild remains hidden, and abstract readiness is exposed only for a complete durable population | `population.staging-hidden`, `population.atomic-activation` | abstract storage/exposure controls and partial-activation LFT1 regression |
| A failed-stopped population neither retries nor becomes ready, readable, or candidate-eligible | `population.failed-stopped-terminal` | retry and every exposure mutant, group-identity API checks, and native manager current-boot failure integration |
| Available capacity excludes committed, partial-attempt, and retired-unreclaimed populations | `population.capacity-excludes-unreclaimed` | over-reported-capacity mutant and exact-bound positive state |
| A completed abort returns runtime index use to its baseline | `population.abort-reclaims-runtime` | retained-index mutant and reclaimed positive state; production reset/abort reuse the worker-local detached-index drain |
| Candidate ordering is componentwise within one compatibility domain | `candidate.componentwise-applied-order` | vector partial-order tests |
| Meta publication/replay cannot regress committed state, and directive replay cannot reuse evidence after a target restart | `meta.committed-state-monotonic`, `meta.directive-evidence-scoped` | composed replay controls, snapshot assertions, and stale-directive LFT1 regression |
| Migration has one serving owner and a complete target | `migration.single-owner`, `migration.complete-before-serving` | snapshot assertions |
| Redis errors expose only exact public error shapes, never internal term/grant/failed/retry state | `redis.control-error-shape`, `redis.compatible-control-error`, `redis.private-control-state-hidden` | structured public-shape allowlist assertions |

An operation with `ClientOutcome::kNotReturned` may already be durable; this is
the explicit uncertain-outcome state. It must never be silently promoted to a
successful response. A success is accepted only when both durability and the
authority-at-decision evidence are true.

`population_integration_test.cpp` is the current real-process cluster-admission
test: it proves `cluster-enabled` starts LOADING, permits `PING`, and rejects
standalone `REPLICAOF`. `replication_group_test.cpp` covers directive
monotonicity, one-group assignment, safe-source/reset authorization, complete
physical reset, sparse-manifest handoff, logical-to-local epoch matching,
all-flow cuts, ready/fail-stop publication, proof invalidation, and a fresh
`NOT_READY` group after API reconstruction at each rebuild boundary.
`rebuild_protocol_integration_test.cpp` also kills a real target after an
acknowledged partial handoff, proves recovery rejects the mixed SSD population,
performs a fresh full sync, and proves the completed population becomes visible
while retaining the durable incomplete-sync fence. The
production manager consumes that API through `NodeControlInstaller`; the Meta
integration suites cover authenticated transport, lease, projection, and
directive delivery, while the native manager suite covers the real storage
transition.
`replication_manager_integration_test.cpp` calls that manager seam directly in
a real single-worker storage/runtime service. A stalling loopback native peer
keeps attempts deterministic while the test proves validation, REBUILDING
status, exact and
endpoint-conflicting replay, monotonic whole-session supersession, cold-source
rejection, and idempotent empty revocation without adding a test-only control
protocol. `source_authorization_test.cpp` separately proves same-revision
multi-target grants, exact replay, revision supersession, revoked-watermark
rejection, and idempotent empty revocation.
`serving_generation_integration_test.cpp` proves that blocked requests,
cross-worker WATCH registration, and a self-gated KEYS scan admitted before
replacement cannot cross into the new dataset, and
`rebuild_failure_integration_test.cpp` proves an uncertain native promotion
stops the current boot without retrying.
`rebuild_protocol_integration_test.cpp` exercises adversarial source behavior:
the target must reject `LVONLINE` before its local flow proof and must reject a
reset after the full-sync cut without losing the promoted population. It also
injects a divergent online LSN and proves that every continuation cursor is
discarded before the replacement full rebuild returns online, and verifies
that an acknowledged full-sync cut cannot leave only part of the resume vector
installed when the connection drops. A corrupted data frame forces a fresh
full sync, and target process crashes verify recovery of both partial and
promoted SSD images remains fail-closed.

## Running and extending

Configure a dedicated tree once:

```bash
cmake -S . -B build_cluster_fault -DCMAKE_BUILD_TYPE=Debug \
  -DLAVIK_ENABLE_OPT=OFF -DLAVIK_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON -DLAVIK_BUILD_FAULT_SERVER=ON
```

Run a bounded tier with `scripts/run_cluster_fault_tests.sh --tier model` or
`--tier integration`. A local soak accepts `--tier soak --duration 600` and
writes the first unexpected trace under the build tree. Hardware jobs skip
unless explicitly opted in; use `--tier hardware --require-hardware` in CI to
turn a missing opt-in into a failure. Hardware opt-in additionally requires an
unmounted `LAVIK_CLUSTER_SCRATCH_DEVICE` block device. The gate is a safety
precondition; raw/SPDK scenarios must use the `cluster-hardware` label and keep
destructive targets inside that allowlist.

The standard AMD64 and ARM64 software CI jobs enable this gate with a private
temporary loop device and detach it after the suite. That exercises the gate's
device checks; it does not provide physical-device or SPDK coverage.

The trace CLI can generate, replay, and minimize artifacts:

```bash
build_cluster_fault/lavik_cluster_fault \
  --scenario dual-authority --seed 7 --trace-out /tmp/failure.lft \
  --expect-finding authority.single-writer
build_cluster_fault/lavik_cluster_fault --replay /tmp/failure.lft \
  --expect-finding authority.single-writer
build_cluster_fault/lavik_cluster_fault --minimize /tmp/failure.lft \
  --trace-out /tmp/minimized.lft \
  --expect-finding authority.single-writer
```

To add a protocol scenario, implement `Scenario`/`ScenarioWorld`, expose every
nondeterministic transition as an `Action`, and serialize only stable logical
observations. Add the assertion to `CheckClusterInvariants`, give it a stable
dot-separated ID, add a positive state, add a mutant that proves the checker
fires, and check in a minimized exact-model trace. Real fixtures translate the
same action and checkpoint vocabulary to process/network/storage operations;
they do not embed production-only fields into LFT1.
