# Deterministic cluster fault harness

This directory owns the reusable test boundary for cluster HA work in issues
#15–#24. It deliberately models protocol evidence with test-only strong types;
it does not freeze production Meta, lease, replication, or migration schemas
before those protocols exist.

The initial property engine is the in-tree `ScenarioRunner`, with GoogleTest
used for assertions and CTest discovery. This avoids adding a second test
dependency while preserving the required generated schedules, shrinking, and
seed replay; a future scenario can add a dedicated property library without
changing KFT1 or the scenario interface.

## Components and determinism contract

- `fault_harness.*` schedules sorted enabled actions with a stable PRNG;
  `fault_trace.cpp` owns canonical KFT1 encoding and persistence. Exact-model
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
  authority, promotion, replication evidence, population activation, applied
  vectors, Meta directives, migration, client outcomes, and Redis-compatible
  control errors. The positive HA scenario composes all deterministic controls,
  then explores independent fault, delivery, disconnect, persistence, Meta
  recovery/directive replay, fencing, activation, and response-decision orders
  from the supplied seed.
- `../support/process.*` provides process groups, pause/resume/stop, bounded
  waits, retained failure artifacts, and loopback port reservations. Real
  process tests use this support; a restart is a new `ChildProcess` using the
  same fixture configuration.

KFT1 has two declared modes. `exact-model` is byte-for-byte replayable and is
appropriate for checked-in regression inputs. `process-action-script` records
the intended actions for a real-process run; OS scheduling and transport
observations may drift, so replay must diagnose drift rather than promise an
identical execution. The current runner executes exact-model traces. Later
cluster fixtures can use the process mode as their adapter contract without
changing the durable exact-model format.

The fault-enabled server is built only with both `BUILD_TESTING=ON` and
`KEYLANE_BUILD_FAULT_SERVER=ON`. It keeps existing crash/write hooks available
under optimized sanitizer builds and produces `keylane_fault_server`; release
packaging explicitly disables the option.

## Verification matrix

| Safety claim | Stable invariant | Fast model coverage | Real adapter owner |
|---|---|---|---|
| One full authority incarnation (node, boot, term, grant) covers admission, in-flight work, background mutation, and success decisions | `authority.single-writer`, `client.operation-single-authority`, `client.valid-authority-at-admission`, `client.safe-success-decision` | same-node-incarnation assertions, dual-authority KFT1 regression, and client history assertions | lease/fencing issue |
| Candidate selection is separate from durable activation | `promotion.safe-activation`, `promotion.durable-before-write-authority` | snapshot assertions and composed failover scenario | promotion issue |
| Other replicas remain intact until activation and child history is ready before writes | `promotion.keep-replicas-until-activation`, `history.child-ready-before-write` | snapshot assertions | promotion/full-sync issues |
| Restart invalidates old partial-sync evidence | `replication.restart-invalidates-evidence` | stale-evidence KFT1 regression | replication issue |
| Live reparent requires compatible domains, an exact cursor, contiguous retained events, and a complete transaction boundary | `replication.compatible-resume-domain`, `replication.reparent-requires-complete-history` | vector tests and history-gap KFT1 regression | replication issue |
| A crashed rebuild cannot expose staging or a half-active population | `population.staging-hidden`, `population.atomic-activation` | storage controls and partial-activation KFT1 regression | full-sync issue |
| Candidate ordering is componentwise within one compatibility domain | `candidate.componentwise-applied-order` | vector partial-order tests | promotion issue |
| Meta publication/replay cannot regress committed state, and directive replay cannot reuse evidence after a target restart | `meta.committed-state-monotonic`, `meta.directive-evidence-scoped` | composed replay controls, snapshot assertions, and stale-directive KFT1 regression | Meta issue |
| Migration has one serving owner and a complete target | `migration.single-owner`, `migration.complete-before-serving` | snapshot assertions | migration issue |
| Redis errors expose only exact public error shapes, never internal term/grant/failed/retry state | `redis.control-error-shape`, `redis.compatible-control-error`, `redis.private-control-state-hidden` | structured public-shape allowlist assertions | request-routing issue |

An operation with `ClientOutcome::kNotReturned` may already be durable; this is
the explicit uncertain-outcome state. It must never be silently promoted to a
successful response. A success is accepted only when both durability and the
authority-at-decision evidence are true.

## Running and extending

Configure a dedicated tree once:

```bash
cmake -S . -B build_cluster_fault -DCMAKE_BUILD_TYPE=Debug \
  -DKEYLANE_ENABLE_OPT=OFF -DKEYLANE_STATIC_OPENSSL=ON \
  -DBUILD_TESTING=ON -DKEYLANE_BUILD_FAULT_SERVER=ON
```

Run a bounded tier with `scripts/run_cluster_fault_tests.sh --tier model` or
`--tier integration`. A local soak accepts `--tier soak --duration 600` and
writes the first unexpected trace under the build tree. Hardware jobs skip
unless explicitly opted in; use `--tier hardware --require-hardware` in CI to
turn a missing opt-in into a failure. Hardware opt-in additionally requires an
unmounted `KEYLANE_CLUSTER_SCRATCH_DEVICE` block device. The gate is a safety
precondition; raw/SPDK scenarios added by later issues must use the
`cluster-hardware` label and keep destructive targets inside that allowlist.

The trace CLI can generate, replay, and minimize artifacts:

```bash
build_cluster_fault/keylane_cluster_fault \
  --scenario dual-authority --seed 7 --trace-out /tmp/failure.kft \
  --expect-finding authority.single-writer
build_cluster_fault/keylane_cluster_fault --replay /tmp/failure.kft \
  --expect-finding authority.single-writer
build_cluster_fault/keylane_cluster_fault --minimize /tmp/failure.kft \
  --trace-out /tmp/minimized.kft \
  --expect-finding authority.single-writer
```

To add a protocol scenario, implement `Scenario`/`ScenarioWorld`, expose every
nondeterministic transition as an `Action`, and serialize only stable logical
observations. Add the assertion to `CheckClusterInvariants`, give it a stable
dot-separated ID, add a positive state, add a mutant that proves the checker
fires, and check in a minimized exact-model trace. Real fixtures translate the
same action and checkpoint vocabulary to process/network/storage operations;
they do not embed production-only fields into KFT1.
