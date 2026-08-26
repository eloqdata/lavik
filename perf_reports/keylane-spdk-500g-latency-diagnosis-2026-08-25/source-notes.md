# Source notes

## Analytical question

Why does the 100K-QPS memtier run have a worse tail, and can reducing the number of SPDK completions processed per poll lower mean latency?

## Report structure map

- Technical summary: direct answer and decision.
- Key evidence: native grouped bar comparing pooled p99, p99.9, and p99.99.
- Scope, definitions, and methodology: workload grain, duration, and controlled A/B dimensions.
- Results table: exact QPS, latency, CPU, and SPDK setting.
- Interpretation and robustness: sample-size, SPDK-batch, pacing, and park/wake hypotheses.
- Limitations: non-contemporaneous formal/current runs and closed-loop caveat.
- Recommendations: ordered benchmark and server-tuning actions.

## Chart contract and map

- Question: Is the P99.99 increase explained by sample size, SPDK completion batching, or client pacing?
- Takeaway: SPDK max 8 versus 1 is indistinguishable at 100K; removing memtier's coarse limiter produces a much lower tail near the same QPS.
- Family: grouped comparison bar.
- Data: `snapshot.datasets.tail_percentiles` in `artifact.json`.
- Placement: immediately after the key-evidence heading.
- Omitted visual: mean latency was not plotted because mixing mean and tail on one axis would obscure the percentile comparison; it remains in the exact table.

## Provenance

- Formal 300-second results: `perf_runs/spdk-500g-readonly-20260825/keylane-spdk-15c-readonly-500g*.json` and the corresponding report.
- Current 60-second A/B: `perf_runs/spdk-500g-readonly-20260825/latency-ab/*.json` plus server `pidstat` logs.
- memtier pacing formula: upstream memtier_benchmark 2.5.1, commit `5f634d171b83efca9640c5a87606c47b34d3d330`, `memtier_benchmark.cpp` lines 4790-4799. Per-connection timer reset and `fill_pipeline()` are in `shard_connection.cpp` lines 1437-1442 and 1487-1497.

## Robustness notes

- The limited formal run has 29,998,789 GETs, leaving roughly 3,000 observations in the worst 0.01%; sample count is not the primary explanation.
- The clean dynamic A/B uses the same 60-second load shape and changes only `spdk-max-completions-per-poll` from 8 to 1.
- The closed-loop comparison is causal-adjacent, not a substitute for a smooth open-loop 100K benchmark.
- Worker park/wake is a plausible secondary mechanism, not a confirmed cause; no request-to-park trace was available in the running build.
