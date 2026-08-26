# Keylane documentation

This directory separates current architecture documentation, historical design
references, and operational runbooks for Keylane.

## Documentation map

| Directory | Purpose | Authority |
|---|---|---|
| [`architecture/`](architecture/README.md) | Describes the system as it is implemented now: module boundaries, lifecycles, primary flows, invariants, and integrations | Current architecture authority, subject to source code and tests |
| [`design-docs/`](design-docs/README.md) | Preserves past designs, compatibility research, tradeoffs, and architectural evolution for reference | Historical, non-authoritative context; it is not maintained as a description of the current code |
| [`operations/`](operations/README.md) | Provides build, packaging, storage-operation, observability, and host-tuning runbooks | Current operational guidance; scripts and configuration remain authoritative |

Keep these categories separate. Architecture is the only documentation tree
that explains the code as implemented now. Design documents preserve historical
reasoning and prior specifications as reference material. Operations documents
tell an operator or release engineer what to do; scripts and configuration
remain authoritative for those procedures.

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
only documentation maintained as a current explanation of the code. Source code
and tests remain authoritative when they disagree with documentation; repair
the affected architecture document in the same change.

Update the architecture index and the relevant focused document whenever a
change alters module boundaries, primary control or data flow, durable storage
format, lifecycle, or an external integration. Design documents are historical
references and have no code-synchronization requirement; changing them does not
replace the required architecture update.

Update the relevant runbook when a supported command, prerequisite, packaging
step, monitoring procedure, or operational safety boundary changes.
