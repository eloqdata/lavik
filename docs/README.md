# Keylane documentation

This directory separates current architecture documentation, design and
technical-reference material, and operational runbooks for Keylane.

## Documentation map

| Directory | Purpose | Authority |
|---|---|---|
| [`architecture/`](architecture/README.md) | Describes the system as it is implemented now: module boundaries, lifecycles, primary flows, invariants, and integrations | Current architecture authority, subject to source code and tests |
| [`design-docs/`](design-docs/README.md) | Holds detailed designs, compatibility and integration references, tradeoffs, investigations, and historical evolution | Technical context whose individual status must be checked; it does not replace current architecture or source evidence |
| [`operations/`](operations/README.md) | Provides build, packaging, storage-operation, observability, and host-tuning runbooks | Current operational guidance; scripts and configuration remain authoritative |

Keep these categories separate. Architecture explains what the current system
does, design documents preserve deeper specifications and reasoning, and
operations documents tell an operator or release engineer what to do.

## Reading order

1. Start with the [architecture index](architecture/README.md).
2. Read the [system overview](architecture/01-overview.md).
3. Continue with the focused architecture document for the subsystem you will
   change.
4. Consult the [design documents index](design-docs/README.md) for detailed
   specifications, compatibility targets, rationale, and historical context.
5. Consult the [operations index](operations/README.md) before changing or
   running a build, release, storage, monitoring, or host-tuning procedure.

## Freshness and authority

The documents under [`docs/architecture/`](architecture/README.md) are the
current architecture authority. Source code and tests remain authoritative when
they disagree with documentation; repair the affected architecture document in
the same change.

Update the architecture index and the relevant focused document whenever a
change alters module boundaries, primary control or data flow, durable storage
format, lifecycle, or an external integration. Moving or updating a document
under `design-docs/` does not replace that architecture update.

Update the relevant runbook when a supported command, prerequisite, packaging
step, monitoring procedure, or operational safety boundary changes.
