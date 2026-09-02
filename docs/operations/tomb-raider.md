# Tomb Raider scheduling

Tomb Raider retires tombstones that no surviving disk record needs. Exactly
one of three scheduling modes is active at a time:

- `TOMBRAIDER OFF` disables future rounds. A round already running completes.
- `TOMBRAIDER INTERVAL <milliseconds>` runs one round after each interval. The
  next interval starts only after the previous round completes.
- `TOMBRAIDER DAILY <HH:MM[:SS]>` runs once per day at that time in the
  server's local timezone. If a round overlaps a later scheduled time, that
  occurrence is skipped rather than run concurrently.

`TOMBRAIDER ON` restores the schedule active before `OFF`.
`TOMBRAIDER BLOCK-SLEEP <milliseconds>` changes the pause after each scanned
block and takes effect during the current round. Zero disables the pause.
`TOMBRAIDER STATUS` reports the mode, interval, block pause, daily time,
timezone, and whether a round is running.

Runtime changes are not persisted across restarts. Startup uses
`--tomb-raider-interval-ms` and `--tomb-raider-sleep-ms`; an interval of zero
starts in off mode.

## Replication role changes

The cleanup loop is launched only when the node has expiration authority at
startup. `cluster-enabled` startup withholds that authority, so the loop is not
launched in that mode. The native FULL path also closes command database
admission and crosses the storage quiesce boundary before its first destructive
reset; the callable cluster rebuild adapter reaches that same path.

On a standalone node, an already launched loop does not recheck authority on
its own. Runtime `REPLICAOF host port` closes client database admission,
invokes the internal replica-quiesce API, and waits until the current round has
forfeited before installing the upstream. Tomb Raider remains OFF if that node
is later detached with `REPLICAOF NO ONE`; explicitly configure the desired
schedule when it becomes an expiration authority again. OFF is not persisted
across restart.

Storage's replica-quiesce seam can forfeit a running round at a safe checkpoint
and wait for it to exit. It has no operator command. Standalone role transition
and native FULL mode use it while command database gates are closed; the
cluster-managed adapter reuses native FULL rather than maintaining a separate
maintenance path.
