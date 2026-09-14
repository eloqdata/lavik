# Rebuild followers after promotion

## Status

Accepted

## Context

Promotion creates a new replication history for the promoted owner. Existing
followers may still identify the former owner's history, so their offsets are
not directly comparable with the new owner's history. Preserving a bridge from
the parent history into the promoted history would make some reparenting
incremental, but it would also add a new replication-history format and
retention protocol to the data plane.

Correctness does not require that optimization. The existing replication
protocol already falls back to a full rebuild when histories do not match.

## Decision

The initial failover design does not add a parent-to-child history bridge.
After cutover, the committed full desired state tells every non-owner replica
to follow the new owner. A replica uses the existing continuation rules when
they are valid and otherwise performs the existing safe full rebuild.

Meta persists the new topology and repeatedly projects this desired state; it
does not persist a separate reparenting workflow. Cutover removes the active
Failover Transition and completes any Controlled Operation. Replica recovery
after that boundary is ordinary steady-state reconciliation, with any
`redundancy restoring` status derived from live observations.

Every follower reconciles independently and may enter the existing destructive
full-rebuild path concurrently. The first release deliberately adds neither a
Meta rebuild queue nor Data-side full-rebuild admission control. Unlike Redis,
it also does not yet preserve a secondary parent history or stage a complete
replacement population before retiring the prior Ready proof. Consequently, a
new Owner failure while all followers are rebuilding can leave the Group with
no immediately promotable Candidate. This is an accepted first-release
availability gap, not a promised Redis-equivalent recovery property.

`Follow Owner` is level-triggered Data-node state, not a one-shot rebuild
command. A failed connection or source-side resource rejection therefore
retains the same desired relationship and retries it with the ordinary
replication coordinator's bounded backoff. Replaying an unchanged Full Desired
State must not restart a healthy transfer; replacing its Owner or replication
anchors cancels the old transfer before starting the new one.

## Consequences

- Promotion and replication reuse the existing Data-node mechanisms.
- A failover can require transferring the full dataset to each follower.
- Followers may perform those destructive rebuilds concurrently.
- Concurrent rebuilds consume one independent source session per follower and
  may increase memory, connection, and publish-queue pressure. Resource
  exhaustion is retried by the target rather than converted into a committed
  queue, while the slowest active full-sync session may backpressure writes.
- A second Owner failure during that window can keep the Group fenced until a
  follower completes or another recoverable member becomes available.
- Parent-history bridging remains a future performance optimization rather
  than a correctness dependency.
- Secondary-history continuation and staged population replacement are the
  intended follow-up mechanisms for narrowing that availability window.
