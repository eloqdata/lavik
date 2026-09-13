#!/usr/bin/env bash
set -euo pipefail
db=${1:?redis|aerospike}; threads=${2:?}; workload=${3:?}
out=${4:?}
if [[ $db == redis ]]; then
  /mnt/dev/YCSB-hreplace-dist/bin/ycsb run redis -threads "$threads" -P "/mnt/dev/YCSB-hreplace-dist/workloads/workload${workload,,}" \
    -p redis.host=172.16.0.4 -p redis.port=16379 -p recordcount=100000000 -p operationcount=100000 \
    -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant -p readallfields=true -p writeallfields=true \
    -p requestdistribution=uniform -p redis.scanindex=none -p measurementtype=hdrhistogram -p measurement.interval=op \
    -p hdrhistogram.percentiles=50,95,99,99.9,99.99 > "$out" 2>&1
else
  /mnt/dev/YCSB-aerospike-dist/bin/ycsb run aerospike -threads "$threads" -P "/mnt/dev/YCSB-aerospike-dist/workloads/workload${workload,,}" \
    -p as.host=172.16.0.4 -p as.port=3000 -p as.namespace=ycsb -p recordcount=100000000 -p operationcount=100000 \
    -p fieldcount=10 -p fieldlength=128 -p fieldlengthdistribution=constant -p readallfields=true -p writeallfields=true \
    -p requestdistribution=uniform -p measurementtype=hdrhistogram -p measurement.interval=op \
    -p hdrhistogram.percentiles=50,95,99,99.9,99.99 > "$out" 2>&1
fi
