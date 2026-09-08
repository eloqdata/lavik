# Function catalog

## Responsibility and boundary

`FunctionCatalog` owns the process-global Redis Function definition set and
the transition between a complete hidden candidate and the catalog visible to
all workers. `LuaWorkerRuntime` owns each worker's compiled Lua closures and
registry. Storage owns the crash-durable dump and its local durability token;
replication continues to carry the original Redis Function command rather than
a storage identity or a Keylane-specific catalog transition.

The public module operations stage a complete target, snapshot its canonical
`FUNCTION DUMP`, make that dump durable, swap the worker runtimes and global
metadata, recover at startup, and expose the current local durability token.
Callers share one Function operation guard with `FCALL`, catalog reads,
catalog mutation, RDB installation, and promotion capture. Hidden staging is
therefore never externally callable.

Each in-flight EVAL or FCALL owns the worker runtime that created its Lua
thread. A catalog swap makes that runtime unavailable to new calls, but closes
it only after its last suspended or running execution releases the thread.

## Mutation lifecycle

`FUNCTION LOAD`, `DELETE`, `FLUSH`, and `RESTORE` compute the complete target
catalog while holding the Function guard. Every worker receives a new hidden
runtime containing the ordinary script cache plus all target Function
libraries. Compilation, registration, flags, descriptions, and resulting
metadata must agree on every worker. Any failure destroys all hidden runtimes
and leaves the visible and durable catalog unchanged.

A successful primary mutation follows one commit order:

```text
complete target
  -> encode existing FUNCTION DUMP format
  -> require the complete synthetic FUNCTION RESTORE event to fit the 1 GiB
     native-event limit
  -> hidden stage and cross-worker validation
  -> use the pre-mutation replication publication admission
  -> commit the complete dump to system-state v2
  -> non-failing worker-runtime and process-global metadata swaps
  -> publish the original Redis Function command
  -> client reply
```

`FUNCTION FLUSH ASYNC` has the same durable boundary as `SYNC`; ASYNC can only
affect reclamation after the catalog is no longer visible. A storage failure
before the root commit aborts hidden staging. An ambiguous root result or a
failure to publish the already durable mutation closes the client connection,
globally fences request serving as `LOADING`, and requires restart recovery.
The initiating caller cannot safely infer whether retrying is appropriate,
and no other connection may observe a runtime catalog whose durable root or
replication history is uncertain.

The native replication log identifies these online commands as catalog
mutations, but their payload remains the existing Redis command. Replica-origin
apply uses this same module without entering source publication and advances
its event cursor only after the local dump is durable and the runtime swap has
completed.

## Durable representation and recovery

The durable body is exactly the existing `FUNCTION DUMP` encoding, including
the valid encoding of an empty catalog when an operation persists one. A fresh
storage set with no system-state root denotes the canonical empty catalog and
does not allocate a body or token only to record absence. A
`CatalogDurabilityToken` contains a node-local monotonic catalog generation and
the dump CRC64. It is a reference for local recovery and promotion; it is not
exchanged as replication identity and does not make two nodes' generations
comparable.

Storage system-state v2 has one worker-zero writer shared by catalog and
promotion updates. A copy-on-write manifest retains the catalog extent list,
catalog token, full-sync readiness, population token, and optional promotion
base. Each update starts from the complete previous manifest, so a catalog
commit preserves promotion state and a promotion commit preserves the catalog.

Catalog extents and the manifest use the ordinary foreground allocator. They
are written and synchronized before the new inactive A/B system-state root is
written and synchronized on every configured device. Capacity exhaustion
fails the mutation before runtime swap or replication publication. Recovery
selects the highest valid root generation whose exact value is present on every
device, validates the manifest, all extent identities and checksums, the dump
length, and its CRC64. It does not fall back to an empty catalog when a root is
present. Before Redis becomes ready, the Function module decodes the selected
dump, stages it on every worker, verifies consistent metadata, and swaps it
into service; corruption or compilation divergence fails startup.

## RDB and full synchronization

RDB import retains its validate-functions, import-keys, install-functions
order. Final installation passes through the same durable complete-catalog
commit. RDB export and Redis `FUNCTION2` encodings remain unchanged.

Native full sync sends one synthetic `FUNCTION RESTORE <dump> FLUSH` on flow
zero at the final cut. Function mutations remain in online history but are
omitted from the full-sync FIFO; non-Function children of a mixed EXEC remain
in that FIFO. Before any target reset or frame apply, full-sync start commits a
system-state invalidation that clears population eligibility, catalog
readiness, and the prior promotion base and keeps the node `LOADING`. The
synthetic RESTORE may overwrite the durable catalog directly. If a later
population step, activation, or restart fails, that new catalog is not rolled
back; the node stays fenced until a whole-group full sync completes both
population activation and catalog readiness.

## Promotion relationship

Promotion first fences and drains replica applies while database admission is
still open. It then closes and drains that admission before taking the Function
guard, capturing the current `CatalogDurabilityToken`, crossing the storage
durability barrier, and committing `PromotionBase`. The gate-before-Function-
guard order matches FUNCTION/FCALL locking; with database admission closed, no
new catalog mutation can enter before capture. If the catalog token does not
still match at commit, preparation fails. Later primary Function mutations
advance the catalog generation while preserving the base. Recovery accepts a
catalog in the same system-state lineage only when it is not older than the
base token.

No transition identity or pre/post digest exists. Same-boot duplicate
suppression belongs to replication history and next-event cursors; a restart
creates a new history and uses full sync.

## Failure evidence and observability

Named crash points in Debug and fault-server builds cover the durable catalog
body, per-device system root, root completion, and runtime-swap boundaries.
`INFO replication` exposes the
local catalog generation and CRC64 alongside group, boot, incarnation, and
history identities. End-to-end Function coverage exercises ordinary mutation,
RDB/native transfer, and startup recovery; storage format tests cover the v1
root codec and unknown-version rejection.

## Source map

| Claim | Repository source |
|---|---|
| Module API, guard, hidden staging, durable-before-visible commit, and startup recovery | `src/redis/function_catalog.h`, `src/redis/function_catalog.cpp` |
| Worker runtime staging and non-failing swap | `src/redis/lua_eval.h`, `src/redis/lua_eval.cpp` |
| Redis commands, replication admission/publication, strict replay, and RDB installation | `src/redis/command.cpp`, `src/redis/server.cpp` |
| Existing dump and FUNCTION2 codecs | `include/keylane/rdb.h`, `src/redis/rdb.cpp` |
| System-state root, manifest, catalog token, full-sync invalidation, and promotion base | `include/keylane/storage/format.h`, `include/keylane/storage/engine.h`, `src/storage/engine/system_state.cpp` |
| Native full-sync catalog transfer and online event handling | `src/replication/replication.cpp`, `src/storage/engine/replication_log.cpp` |
| Format, runtime lifetime, and restart verification | `tests/storage_format_test.cpp`, `tests/lua_eval_test.cpp`, `tests/multi_exec_e2e_test.cpp`, `tests/list_e2e_test.cpp`, `tests/rdb_test.cpp` |
