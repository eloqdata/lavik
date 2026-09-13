#!/usr/bin/env bash
set -euo pipefail
mkdir -p /mnt/dev/ycsb-md0-keylane-matrix
for w in a b c d; do
  for t in 64 128 256 512; do
    echo "=== $w $t ==="
    /mnt/dev/YCSB-hreplace-dist/bin/ycsb run redis -threads "$t" -P "/mnt/dev/YCSB-hreplace-dist/workloads/workload${w}" \
      -p redis.host=172.16.0.4 -p redis.port=16379 -p recordcount=100000000 -p operationcount=100000 \
      -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant -p readallfields=true -p writeallfields=true \
      -p requestdistribution=uniform -p redis.scanindex=none -p measurementtype=hdrhistogram -p measurement.interval=op \
      -p hdrhistogram.percentiles=50,95,99,99.9,99.99 > "/mnt/dev/ycsb-md0-keylane-matrix/${w}-c${t}.log" 2>&1
  done
done
