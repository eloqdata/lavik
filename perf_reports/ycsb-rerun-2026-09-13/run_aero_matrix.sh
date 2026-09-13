#!/usr/bin/env bash
set -euo pipefail
mkdir -p /mnt/dev/ycsb-md0-aero-matrix
for w in a b c d; do
  for t in 64 128 256 512; do
    echo "=== $w $t ==="
    /mnt/dev/YCSB-aerospike-dist/bin/ycsb run aerospike -threads "$t" -P "/mnt/dev/YCSB-aerospike-dist/workloads/workload${w}" \
      -p as.host=172.16.0.4 -p as.port=3000 -p as.namespace=ycsb -p recordcount=100000000 -p operationcount=100000 \
      -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant -p readallfields=true -p writeallfields=true \
      -p requestdistribution=uniform -p measurementtype=hdrhistogram -p measurement.interval=op \
      -p hdrhistogram.percentiles=50,95,99,99.9,99.99 > "/mnt/dev/ycsb-md0-aero-matrix/${w}-c${t}.log" 2>&1
  done
done
