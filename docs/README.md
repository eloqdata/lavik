# Keylane documentation

This directory separates current architecture documentation, historical design
references, and operational runbooks for Keylane.

## Documentation map

| Directory | Purpose | Authority |
|---|---|---|
| [`architecture/`](architecture/README.md) | Compactly explains the current system's core design: module boundaries, lifecycles, primary flows, invariants, integrations, and stable rationale | Current architecture authority, subject to source code and tests |
| [`design-docs/`](design-docs/README.md) | Preserves past designs, compatibility research, tradeoffs, and architectural evolution for reference | Historical, non-authoritative context; it is not maintained as a description of the current code |
| [`operations/`](operations/README.md) | Provides build, packaging, storage-operation, observability, and host-tuning runbooks | Current operational guidance; scripts and configuration remain authoritative |

Keep these categories separate. Architecture is the only documentation tree
that explains the current core design, but it is not an implementation
reference or release history. Design documents preserve historical reasoning
and prior specifications as reference material. Operations documents tell an
operator or release engineer what to do; scripts and configuration remain
authoritative for those procedures.

Keep one-off source audits and task-specific investigation reports in their
task, issue, or pull-request context rather than adding them to this tree.

## Reading order

1. Start with the [architecture index](architecture/README.md).
2. Read the [system overview](architecture/01-overview.md).
3. Continue with the focused architecture document for the subsystem you will
   change.
4. Consult the [design documents index](design-docs/README.md) only when past
   rationale, alternatives, or compatibility research is relevant. Verify any
   behavioral claim there against source, tests, and current architecture.
5. Consult the [operations index](operations/README.md) before changing or
   running a build, release, storage, monitoring, or host-tuning procedure.

## Freshness and authority

The documents under [`docs/architecture/`](architecture/README.md) are the
only documentation maintained as a current explanation of the core design.
Source code and tests remain authoritative when they disagree with
documentation; repair the affected architecture document in the same change.

Update architecture with an implementation change only when the current
architectural model would otherwise become false or materially incomplete due
to an altered module boundary, primary control or data flow, ownership or
lifecycle, durable or wire format, external integration, or system-level
invariant. Local implementation, optimization, refactoring, metrics, and test
changes do not require an architecture update unless they cross one of those
boundaries. Dedicated documentation work may correct an inaccuracy, fill a
known core-design gap, or consolidate existing sediment. Follow the
[architecture authoring standard](architecture/README.md#authoring-standard).

Design documents are historical references and have no code-synchronization
requirement; changing them does not replace a required architecture update.

Update the relevant runbook when a supported command, prerequisite, packaging
step, monitoring procedure, or operational safety boundary changes.
