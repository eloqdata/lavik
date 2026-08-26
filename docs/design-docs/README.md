# Design documents

This directory records Keylane design proposals, detailed specifications,
compatibility and integration references, tradeoffs, investigations, and
architectural evolution. It is intentionally separate from the explanatory
[`architecture/`](../architecture/README.md) directory and the procedural
[`operations/`](../operations/README.md) runbooks.

Use the architecture documents to understand the system as implemented now.
Use these documents for deeper protocol and integration details, compatibility
targets, design reasoning, considered alternatives, and subsystem evolution. A
file here may describe a future design, a superseded plan, or a detailed
specification that still matches the implementation; check its stated status.
Source code, tests, and the architecture documents remain authoritative for
current behavior.

## Technical specifications and references

| Document | Scope and status |
|---|---|
| [Native replication design](replication-design.md) | Detailed native replication protocol and feature specification |
| [Recovery metadata design](recovery-metadata-design.md) | Detailed persistent metadata layout and crash-consistency rationale |
| [Storage block ownership](storage-block-ownership.md) | Worker-count-independent ownership design and transition rationale |
| [Logical databases and `SELECT`](logical-databases.md) | Detailed database identity, routing, indexing, and command behavior |
| [Memory accounting](memory-accounting.md) | Allocation accounting, admission, metrics, and verification details |
| [Redis compatibility target](redis-compatibility.md) | Supported Redis/Valkey protocol, command, RDB, and replication surface |
| [SPDK integration](spdk.md) | SPDK storage integration, prerequisites, topology, and setup reference |
| [TLS and password authentication](tls-and-auth.md) | TLS/authentication behavior, configuration, and integration constraints |

## Proposals, history, and investigations

| Document | Scope and status |
|---|---|
| [Large-key redesign constraints](large-key-design.md) | Constraints for a future redesign; explicitly not a description of the current representation |
| [Transaction design](transaction-design.md) | Original transaction proposal, decisions, milestones, and chronological extensions |
| [Cross-shard architecture design](sharding-design.md) | Historical worker/shard plan; current ownership is documented elsewhere |
| [Replication transaction-order investigation](replication-transaction-order-investigation.md) | Point-in-time source investigation, performance evidence, risks, and alternatives |

## Maintenance

Put detailed technical specifications, compatibility references, proposals,
alternatives, investigations, and design-history documents in this directory.
When implementation changes a current boundary, flow, lifecycle, durable
format, or integration, update the corresponding document under `architecture/`
in the same change.
