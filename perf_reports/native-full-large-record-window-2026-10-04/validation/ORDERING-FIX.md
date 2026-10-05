# Native FULL pending-baseline ordering correction

The final-head run for PR #263 (`eca51774`, [37261702892](https://github.com/eloqdata/lavik/actions/runs/37261702892)) passed the Sentinel and leader-recovery gates but failed `gate_full_snapshot_window` on arm64. The target returned no expiration for a value whose source retained the requested absolute deadline.

## Reproduction and cause

The unchanged local snapshot gate first passed once (107.40 s). A traced repetition then reproduced the exact expiration assertion: source PEXPIRETIME was the requested deadline, target PEXPIRETIME was -1 and remained -1 after another 500 ms. A subsequent wire trace had one pass followed by another reproduction. In the failing run, the TTL command was FULL frame 11, but its old, persistent baseline did not arrive until frame 13. See [wire order](ordering-fix/wire-order-excerpt.log).

Snapshot materialization makes a key eligible for command capture before the sender necessarily flushes its partially filled record batch. The live FIFO could therefore send a dependent PEXPIREAT before the key existed on the target. The command completed as a no-op and the later baseline installed a persistent value. The same missing ordering boundary exists without ordinary-snapshot pipelining: a controlled reproduction failed on the parent chunks implementation too. This is separate from the previously documented 16 MiB publisher-admission stall.

## Correction and controlled regression

A nonempty FIFO invokes its caller's pending-baseline flush before sending commands or storage after-images. The snapshot implementation also joins outstanding baseline completions. Empty FIFO checks retain the existing batching/window behavior. Named caller-owned callbacks stay alive throughout the awaited drain; calls outside the scan use an empty callback because no baseline batch remains.

`meta_integration.gate_full_snapshot_ordering` pauses a real source after three ordinary keys have been materialized but before their first baseline frame. It applies PEXPIREAT, APPEND and DEL, releases the source, and checks the exact deadline, complete appended value, deletion, and a single FULL attempt. The bounded pause exists only in fault-enabled builds. Fault-disabled binaries report this dedicated CTest as skipped (77).

The controlled test failed before the correction on both snapshot and chunks implementations with `AssertionError: (-1, expected_deadline)`. After correction it passed three consecutive process runs on the snapshot implementation, then three CTest repetitions on chunks (22.58 / 23.91 / 25.32 s, 71.83 s total). [Retained logs and reproduction-only patch](ordering-fix/manifest.json) distinguish the injected scheduling hook from the production correction.

## Performance provenance

The earlier original and `followup-2026-10-05` cohorts used frozen binaries before this production correction. Their measurements remain historical evidence and must not be relabeled as results for the corrected implementation. Updated comparisons apply the identical correction to the frozen main control as well as both candidates; this keeps correctness work separate from the marginal pipeline comparisons. Current remote CI results are reported on the PRs.
