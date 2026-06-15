# keylane Roadmap

Durable backlog for the keylane Redis-compatible server. Runtime-level items live
in `celer/ROADMAP.md`.

- **Phase 2: pipeline command batching / squashing.** Coalesce pipelined commands
  per connection to amortize dispatch and cross-shard hops.
- **Phase 3: `CompactObj` + zero-copy RESP parsing.** Parse requests directly out
  of the recv buffer and store small values inline (Redis-style 0-alloc path).
