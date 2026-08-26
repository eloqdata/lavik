# Historical design references

This directory preserves Keylane design proposals, prior detailed
specifications, compatibility and integration research, tradeoffs, and
architectural evolution. It is intentionally separate from the explanatory
[`architecture/`](../architecture/README.md) directory and the procedural
[`operations/`](../operations/README.md) runbooks.

Every document here is historical, non-authoritative reference material. It may
describe a proposal, an implementation at an earlier point in time, a
superseded plan, or research whose conclusions no longer match the code. Use
source code, tests, and [`architecture/`](../architecture/README.md) to
understand current behavior. Consult this directory only for prior rationale,
alternatives, and research context, and verify behavioral details before using
them.

## Historical technical references

| Document | Scope and status |
|---|---|
| [Native replication design](replication-design.md) | Historical native replication protocol and feature design |
| [Recovery metadata design](recovery-metadata-design.md) | Historical persistent metadata and crash-consistency rationale |
| [Storage block ownership](storage-block-ownership.md) | Historical ownership design and transition rationale |
| [Logical databases and `SELECT`](logical-databases.md) | Historical database identity, routing, indexing, and command design |
| [Memory accounting](memory-accounting.md) | Historical allocation accounting and admission design |
| [Redis compatibility target](redis-compatibility.md) | Historical Redis/Valkey compatibility target and research |
| [SPDK integration](spdk.md) | Historical SPDK integration design and setup research |
| [TLS and password authentication](tls-and-auth.md) | Historical TLS and authentication integration design |

## Proposals and design history

| Document | Scope and status |
|---|---|
| [Large-key redesign constraints](large-key-design.md) | Constraints for a future redesign; explicitly not a description of the current representation |
| [Transaction design](transaction-design.md) | Original transaction proposal, decisions, milestones, and chronological extensions |
| [Cross-shard architecture design](sharding-design.md) | Historical worker/shard plan; current ownership is documented elsewhere |

## Maintenance

Put historical specifications, compatibility research, proposals, alternatives,
and design history in this directory. Keep current behavioral explanations in
`architecture/`. When implementation changes a current boundary, flow,
lifecycle, durable format, or integration, update the corresponding architecture
document in the same change; historical design references need not be revised.
