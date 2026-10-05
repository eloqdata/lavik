# Meta CI gate repair — 2026-10-05

The shared test-only commit `8cc42130` repairs two asynchronous leadership assumptions. It does not modify the measured production source or binaries, and does not address the separately documented large-write baseline stall.

## Original CI evidence

- [PR #261 / amd64 shard 5](https://github.com/eloqdata/lavik/actions/runs/37210823829/job/111463227836), head `7b51f6a1`: `meta_integration.sentinel`, Redis 7.2 wire case `no-password/discovery-unknown-service`, failed with `AssertionError: truncated RESP line: b''`.
- [PR #263 / arm64 shard 1](https://github.com/eloqdata/lavik/actions/runs/37211015387/job/111464211110), head `f938f066`: `meta_integration.gate_leader_change` failed with `post-refuse: propose: ERR not-leader` after healing an asymmetric inbound fault.

Each run had one failing test shard. Both architecture aggregate checks were red because each depends on the global test-shard result; this does not represent four independent failures. Builds, formatting and the other eleven shards passed on each original run.

## Corrected assumptions

Raft's `leader=1` is published before the control worker necessarily installs its leadership term and authority eligibility. Sentinel discovery binds those latter values to the connection and correctly closes it if they change. The gate now waits for a successful bracketed `clusterstatus 1` response before discovery tests. The exact Redis wire fixtures and assertions are unchanged; the actual wire exchange is never retried. Unknown readiness errors still fail.

Healing an asymmetric link does not atomically stabilize leadership for a subsequent write. The recovery gate now reselects a leader only on `ERR not-leader`, within a 20-second proposal recovery window. Other proposal errors fail immediately. Each attempt receives a fresh operation ID; an uncertain submit/complete pair is not counted as success. The successful probe is now recorded in committed history and verified on every node. The two explicit kill/election timing checks and the final history check remain unchanged.

These are source-supported explanations and deterministic tests of the gate assumptions. The original failures were **not reproduced in local real-process repeats**: original wire test 20/20, two early-start variants 30/30 each, and original leader gate 5/5 passed. Controlled orchestration reproduces both exact error strings on the original gates and passes after the fixes; it does not prove the exact scheduler sequence of the remote CI failures.

## Validation

Commands ran with `/mnt/local_nvme/i131/env.sh` sourced, serially, before performance measurements:

```bash
python3 tests/meta_integration/test_meta_gate_readiness.py
python3 tests/meta_integration/gate_sentinel.py \
  /mnt/local_nvme/i131/build-chunks-debug/lavik-meta \
  /mnt/local_nvme/i131/build-chunks-debug/lavik
python3 tests/meta_integration/gate_leader_change.py \
  /mnt/local_nvme/i131/build-chunks-debug/lavik-meta
ctest --test-dir /mnt/local_nvme/i131/build-chunks-debug \
  -R '^meta_integration.gate_readiness$' --output-on-failure
ctest --test-dir /mnt/local_nvme/i131/build-snapshots-debug \
  -R '^meta_integration\.(gate_readiness|sentinel|gate_leader_change)$' \
  --output-on-failure
```

- Four deterministic regression cases passed: delayed discovery readiness, leader loss before proposal, unexpected proposal errors, and lack of progress before the deadline. The initial three-case run on old source failed with the two expected CI error strings; the fourth case was added to enforce the retry bound.
- Complete Sentinel suite passed 10/10 repeats, 15 cases per repeat.
- Complete leader-change gate passed 10/10 repeats.
- New CTest registration passed 1/1 in 0.20s on chunks.
- After merging into snapshots (`f36621bb`), all three affected CTests passed in 29.69s.
- Ruff checks/formatting and `git diff --check` passed.

[Logs and SHA-256 manifest](ci-fix/manifest.json) retain the exact output. Production `src/` and `include/` are byte-identical to the original measured cores `51d03f97` / `9da9c797`. No new sanitizer, mixed-version, or local repository-wide run is claimed for this test-only delta. Final-head remote CI results are tracked on the PRs; these local results alone are not a remote pass.
