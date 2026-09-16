## Parent

[#21 Authority Lease、Fencing 与 Failover](https://github.com/thweetkomputer/keylane/issues/21)

## Status

Design confirmed on 2026-09-15. Implementation is pending.

## Goal

Add a Meta Leader-local Automatic Failover Detector for every Group with a
current Owner. When current authenticated observations prove that the same
Owner authority has remained unserviceable for the configured debounce
interval, the detector submits #41's `BeginUncontrolledFailover`. The Raft
commit atomically fences that authority and creates the durable Failover
Transition; execution after that point remains entirely owned by #41.

This change also turns `MetaPolicyStore` into a bounded registry for typed,
global cluster configuration and moves Authority Lease duration out of grants
and into that registry.

```text
current global Policy + committed Owner authority
                         |
current authenticated session / structured heartbeat / lease causality
                         |
                         v
             leader-local serviceability
                         |
       exact failure for suspect_after_ms
                         v
             BeginUncontrolledFailover
                         | Raft commit
                         v
       committed fence + Failover Transition
                         |
                         v
              #41 Uncontrolled Executor
```

## Responsibility boundary

Issue 42 owns:

- the two typed global Policy families and their bootstrap defaults;
- projection and enforcement of the global Authority Lease Policy;
- the Owner Serviceability predicate;
- Leader-local detector state, debounce, lifecycle, and diagnostics;
- automatic submission of `BeginUncontrolledFailover`;
- the command metadata and CAS needed to bind Begin to one Owner authority.

Issue 42 does not own Candidate selection, replacement, Promotion Preparation,
Cutover, loss assessment, Follow Owner, or transition recovery. Those remain
the #41 Uncontrolled Executor's responsibility.

## Global Policy model

`MetaPolicyStore` remains Committed State, but it no longer stores arbitrary
opaque documents. It accepts only the compiled-in Policy families below.

### Automatic uncontrolled failover

Policy id: `keylane.automatic-uncontrolled-failover-v1`

```json
{"kind":"automatic-uncontrolled-failover-v1","enabled":true,"suspect_after_ms":5000}
```

- `enabled` is required.
- `suspect_after_ms` is required and remains a legal nonzero value when the
  Policy is disabled.
- The supported range is 1,000 through 86,400,000 milliseconds.
- The bootstrap default is enabled with a 5,000 millisecond threshold.

### Authority Lease

Policy id: `keylane.authority-lease-v1`

```json
{"kind":"authority-lease-v1","duration_ms":5000}
```

- `duration_ms` is required.
- The supported range is 100 through 86,400,000 milliseconds.
- The bootstrap default is 5,000 milliseconds.

### Store rules

- Policy content is strict compact JSON. Missing, duplicate, or unknown fields,
  an unknown `kind`, invalid types, overflow, and out-of-range values are
  rejected.
- Field order is insignificant to decoding. The original raw bytes are stored,
  compared, and returned unchanged; no canonical reserialization is performed.
- `PutPolicy`, Policy snapshots, and Policy reads contain no content hash or
  fingerprint. Same-version identity is determined directly from the bounded
  raw bytes.
- The first version of a family is version 1. A later `PutPolicy` must be
  exactly `current + 1`; gaps and regressions are rejected.
- Repeating the same version with identical bytes is idempotent.
  Reusing a version with different content is rejected.
- The highest version is current and becomes active as soon as its command is
  committed.
- Each family retains its newest 32 versions. Applying version 33 and later
  automatically evicts the oldest version while preserving the existing
  document and total-byte bounds.
- `RetirePolicy` and retired tombstones are removed. With no references from
  Grants, Operations, or Failover Transitions, retirement has no domain use.
- A Created cluster missing either required current Policy is corrupt and
  fails closed during state validation/startup.

The internal Meta Admin surface keeps strict `putpolicy` and adds leader-only
`getpolicy` for the current raw document. Issue 42 does not add a public
`keylane-ctl policy` workflow; a broader configuration-management interface is
future work.

## Bootstrap Policy Defaults

The existing initial cluster manifest is the single bootstrap configuration
source. It gains optional typed fields:

```toml
[bootstrap_policy]
automatic_uncontrolled_failover_enabled = true
automatic_uncontrolled_failover_suspect_after_ms = 5000
authority_lease_duration_ms = 5000
```

Omitted fields use the defaults shown above. Unknown, duplicate, malformed, or
out-of-range fields make manifest parsing fail.

The normalized values travel in the durable cluster-create intent. After the
Genesis intent commits and before initial authority grants are created, the
cluster-create reconciler installs version 1 for each missing Policy family.
A valid family pre-seeded through `putpolicy` is allowed in an otherwise
pristine cluster and is preserved even when it differs from the manifest
default. Bootstrap values never overwrite committed Policy and have no
ongoing authority after cluster creation.

## Authority Lease and heartbeat cadence

Authority grants no longer carry lease duration or Policy id/version. At every
Full Desired State build, lease renewal, and Cutover, Meta resolves the current
global Authority Lease Policy. The duration it may actually issue is:

```text
effective_lease_ms = min(policy.duration_ms,
                         local_meta_leadership_validity_ms)
```

Raft heartbeat and election timing remain process-local Meta startup settings.
They are not Policy and are not fingerprinted into Committed State. Deployment
configuration is responsible for using the same values on every Meta node.

The Data-control heartbeat has no independent operator setting:

```text
data_heartbeat_ms = max(1, floor(effective_lease_ms / 3))
```

Meta projects the resolved lease and heartbeat scalars in Full Desired State;
Data does not receive or interpret Policy ids or versions.

An Authority Lease Policy update uses a single fail-closed projection change.
It does not retroactively shorten an already-installed finite lease. The old
lease normally bridges Full Desired State convergence; if convergence exceeds
the old remaining lease, Data self-fences until it installs a new valid lease.
No two-phase update, dual projection renewal, or intentional availability
window is added.

## Owner Serviceability

The detector evaluates only the committed current Owner. A heartbeat is useful
only when its authenticated session, Data boot, assignment, Full Desired State,
Group term, authority version, and grant revision all match the current Owner
authority.

The exact structured failure reasons are, in precedence order:

1. `session_missing`
2. `heartbeat_expired`
3. `draining`
4. `storage_unready`
5. `population_unready`

`storage_ready` comes from completion of storage recovery and remains false
after a runtime storage-readiness loss. `population_ready` requires the exact
boot-local population proof matching the installed Full Desired State,
manifest, and replication epoch. `health.summary`, replica reports, Candidate
availability, and free-form text never influence the predicate.

Data does not send a separate self-fenced flag. An Owner described by current
Full Desired State continues requesting Authority Leases even after local
expiry. A successful lease acknowledgement is treated as installed only after
a higher-sequence causal heartbeat proves the Data client processed that
acknowledgement. Until then the evidence is Indeterminate; an eventual lack of
causal progress becomes `heartbeat_expired`. Installing a later valid lease
allows Data to remove its local fence without a separate Meta state change.

Expected finite-lease handoff/quarantine, a stale session or projection, and
waiting for causal lease confirmation are Indeterminate or explicitly blocked;
they do not start SUSPECT. There is no peer vote, replica accusation, or
independent probe.

## Detector states and debounce

Every eligible Group has one Leader-local runtime state:

- `DISABLED`: the current Automatic Failover Policy is disabled.
- `HEALTHY`: current causal evidence proves the Owner serviceable.
- `SUSPECT`: an exact failure reason is accumulating debounce time.
- `BLOCKED`: an explicit control-plane condition or Indeterminate evidence
  prevents the decision from advancing.
- `TRIGGERING`: the threshold was reached and one stable Begin request is being
  submitted or reconciled.

The detector uses `steady_clock`; no wall-clock timestamp or SUSPECT state is
persisted. Its anchor is the current Meta leadership generation, Owner,
assignment, Group term, authority version, grant revision, and both current
Policy versions.

- The first exact unserviceable classification begins SUSPECT.
- Exact failure reason changes update `current_reason` without clearing elapsed
  time.
- A matching serviceable heartbeat clears all elapsed time.
- Indeterminate evidence freezes accumulated time for the same anchor. A later
  exact failure resumes it; the threshold may trigger only while the current
  classification is exact unserviceable.
- Meta leadership replacement, loss of leadership eligibility, Owner authority
  change, or either Policy version change discards the clock.
- A replacement Leader waits for its normal observation-reacquisition warmup,
  then gives an absent/unserviceable Owner a complete new debounce interval.
- Normal handoff, an existing Failover Transition, and unavailable leadership
  authority report fixed `BLOCKED` reasons.
- `DISABLED`, `SUSPECT`, and `BLOCKED` do not independently change cluster
  readiness or Data serving state. Only actual observed serving state or a
  committed Begin/fence changes those results.

For silent loss, effective trigger latency is observation TTL plus the complete
`suspect_after_ms` interval and proposal latency. Repeated Meta Leader changes
may extend pre-Begin recovery time because SUSPECT is deliberately not durable.

## Automatic Begin semantics

At the threshold, the detector creates one random 128-bit transition id and
stable request identity. Retries reuse both identities and use bounded
100/200/400/800/1,000 millisecond backoff. An uncertain proposal result is
reconciled against caught-up committed state; it never creates a second
transition id.

The automatic `BeginUncontrolledFailover` carries and apply-CAS-validates the
Group revision, current Owner and assignment, Group term, authority version,
grant revision, and absence of a Failover Transition. It also carries the
current trigger reason and accumulated suspect duration for the minimal
committed trigger audit. It carries no Policy version and no successor Grant
specification.

Immediately before append, the Leader takes one coherent read cut and
revalidates the complete serviceability and committed anchor. There is an
intentional availability-first race after that check: a later healthy heartbeat
or Policy update does not cancel an already-submitted Begin. The command may
therefore commit under the preceding health/configuration decision. Authority
and transition CAS plus the term fence still prevent overlapping write
authority.

Apply atomically validates the committed Owner authority, advances the Group
term, revokes the old authority, fences the Group, creates the #41 uncontrolled
Failover Transition with no Candidate, and writes the trigger audit in the same
Raft index. Apply never reads Leader-local observations.

An explicit domain rejection returns the detector to a reloaded state. If one
or more Submitted Controlled requests for the Group have not created a
transition, automatic Begin atomically preempts and fails all of them. One
proposer-observed request may be carried as a CAS witness; deterministic apply
also catches requests ordered between proposal construction and Begin. A new
Controlled request ordered after Begin is rejected while the transition is
active, while replay of an already-known request remains idempotent. If a
Controlled or Uncontrolled Transition is already committed, the detector is
BLOCKED and submits nothing; #41 handles Controlled Source loss by degrading
that transition in place. Candidate absence never prevents fencing an
unserviceable Owner.

## Leader lifecycle

The detector runs only on a caught-up, leadership-valid Meta Leader after the
current observation publisher is installed. Demotion cancels and joins all
detector timers and local continuations. Losing leadership eligibility clears
all local clocks even before a formal role change; regaining eligibility starts
the normal Leader observation warmup again.

Before Begin commits, a new Leader always reacquires observations and waits a
full new debounce interval. After Begin commits, no detector state is needed:
the replacement Leader resumes the committed #41 transition.

## Diagnostics

The existing `cluster-status` command is replaced with one required new reply
layout. The new CLI parses only that layout; no old reply decoder or separate
diagnostic command is added.

Each Group reports:

- detector state;
- current reason, when exact unserviceability is present;
- fixed blocker code, when BLOCKED;
- accumulated SUSPECT milliseconds;
- effective threshold milliseconds.

It does not report Policy id, version, or raw content. Detector fields do not
become readiness blockers merely because the state is DISABLED, SUSPECT, or
BLOCKED.

Structured logs cover state edges, timer resets, Begin proposal/retry/result,
domain rejection, and uncertain reconciliation. Logs include the relevant
Group/Owner authority, current reason, transition id, and commit index. No new
metrics are introduced.

The committed trigger audit contains only the current reason and accumulated
duration in addition to the transition identity already needed by the command.
It does not retain Policy versions or resolved Policy values.

## Compatibility

Old binaries, old persisted formats, and old wire layouts are outside the
product contract. Commands, snapshots, Full Desired State, cluster-create
intent, and status are replaced directly. There are no legacy decoders, data
migrations, dual writes, capability negotiation, or new/old interoperability
tests.

## Implementation sequence

1. **Policy domain and format cleanup**
   - Add the strict typed Policy decoders and registered family constants.
   - Change version admission and bounded eviction.
   - Remove retirement and every Policy/lease field from Grants, Operations,
     Failover Transitions, commands, snapshots, projections, and fixtures.
   - Add Created-state validation for both required current families.

2. **Bootstrap and internal administration**
   - Extend strict manifest parsing and cluster-create intent encoding with
     typed Bootstrap Policy Defaults.
   - Allow registered pre-create Policy state in pristine admission.
   - Install missing version-1 Policies before initial authority activation.
   - Add leader-only current `getpolicy`; keep strict `putpolicy` validation.

3. **Global Authority Lease projection**
   - Resolve current lease Policy in the Meta publisher and Cutover paths.
   - Replace FDS lease/heartbeat fields and Data decoding in place.
   - Derive Data heartbeat cadence and preserve already-installed finite leases
     across a projection replacement.

4. **Owner Serviceability evaluator**
   - Centralize causal session/boot/FDS/authority/health/lease classification.
   - Distinguish exact unserviceability, serviceability, and Indeterminate.
   - Add deterministic reason precedence and causal ack confirmation.

5. **Leader-local detector**
   - Implement the five-state per-Group runtime model with an injectable clock.
   - Integrate leadership eligibility, warmup, observation events, Policy/FDS
     changes, transition state, timer freeze/reset, cancellation, and join.
   - Submit and reconcile stable automatic Begin identities with bounded retry.

6. **Command, status, and audit**
   - Extend automatic Begin metadata and pre-append revalidation without Policy
     version CAS.
   - Replace cluster-status wire/JSON/text output with required detector fields.
   - Add structured transition logs and the minimal committed trigger audit.

7. **Verification and current documentation**
   - Update Meta/Data architecture and relevant operations guidance alongside
     the implementation.
   - Run focused unit tests, full C++ tests, and multi-process fault gates.

## Acceptance criteria

- [ ] Both registered Policy schemas reject missing, duplicate, unknown,
      malformed, overflow, and out-of-range content and accept field reordering.
- [ ] Policy versions are consecutive and idempotent, and retain exactly the
      bounded newest history without retirement.
- [ ] Policy commands, committed state, snapshots, and reads contain no Policy
      content hash; bounded raw bytes determine same-version idempotency.
- [ ] Grants, Operations, Transitions, commands, snapshots, and FDS contain no
      Policy reference; grants contain no configured lease duration.
- [ ] Manifest defaults install missing Policies and preserve valid pre-seeded
      current values.
- [ ] Created state without both valid current Policies fails closed.
- [ ] Effective leases and derived heartbeat cadence follow the current lease
      Policy and local leadership-validity cap.
- [ ] A projection update normally bridges with the old finite lease and safely
      self-fences if convergence exceeds its remainder.
- [ ] Only current authenticated Owner evidence matching every causal anchor can
      classify serviceability or clear SUSPECT.
- [ ] Stale session, boot, assignment, FDS, authority, and out-of-order heartbeat
      evidence neither triggers nor clears suspicion.
- [ ] Exact failure reasons, precedence, reason changes, Indeterminate freeze,
      recovery reset, and every anchor reset have deterministic clock tests.
- [ ] New Leader warmup and full debounce are enforced; demotion and leadership
      ineligibility cancel and join detector work.
- [ ] Threshold crossing submits one stable automatic Begin, including when no
      Candidate exists.
- [ ] Pre-append health recovery suppresses Begin; health or Policy changes
      after submission may not retract a committed fence.
- [ ] Existing transitions do not receive a second Begin, and Submitted
      Controlled requests are deterministically preempted.
- [ ] Uncertain results and retry do not create duplicate terms or transitions.
- [ ] Cluster status exposes the agreed detector fields, no Policy data, and no
      independent readiness effect.
- [ ] Structured logs and minimal audit explain the current reason and trigger;
      no new metrics exist.
- [ ] Process fault tests cover Owner kill, pause, one-way and two-way partition,
      and Meta Leader changes around threshold, proposal, and commit.
- [ ] Fault tests prove no overlapping write success; transition recovery and
      loss assessment continue to be verified by #41.
- [ ] Only the new state and wire formats are supported or tested.

## Out of scope

- Public Policy-management commands or a complete configuration-management
  workflow;
- persisted SUSPECT timestamps or cross-Leader timer recovery;
- Data self-fence reporting, peer voting, gossip, or independent health probes;
- automatic Begin cancellation after proposal submission;
- metrics for the detector;
- Candidate selection/replacement, Promotion Preparation, Cutover, loss
  assessment, Preserved Replica, Follow Owner, or redundancy restoration;
- old-format decoding, migration, or old/new interoperability.
