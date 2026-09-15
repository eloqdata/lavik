#!/usr/bin/env python3
"""Recompute the comparison from the bundled client and server evidence.

Requires hdrhistogram==0.10.7. No network access or database traffic is needed.
Keylane means use accumulated latency, while Aerospike means use HDR buckets.
Neither QPS nor latency is an unweighted average of rounded display values.
"""
import csv
import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys
import tarfile

from hdrh.histogram import HdrHistogram

ROOT = Path(__file__).resolve().parent
# Recorded on the measurement server with os.sysconf('SC_CLK_TCK'); offline
# analysis must use the source clock, rather than the reviewing host's clock.
CLOCK_TICKS_PER_SECOND = 100


def data(path):
    return json.loads(path.read_text())


def calls(raw, operation):
    match = re.search(r'cmdstat_' + operation + r':calls=(\d+)', raw)
    return int(match[1]) if match else 0


def resources(first_cpu, last_cpu, first_disks, last_disks, seconds, count):
    def cpu(raw):
        fields = raw.rsplit(')', 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / CLOCK_TICKS_PER_SECOND

    names = {f'nvme{i}n1' for i in range(1, 7)}

    def disks(raw):
        return {f[2]: list(map(int, f[3:])) for line in raw.splitlines()
                if (f := line.split()) and f[2] in names}

    a, z = disks(first_disks), disks(last_disks)
    assert a.keys() == z.keys() == names
    reads = sum(z[d][0] - a[d][0] for d in names)
    read_bytes = sum(z[d][2] - a[d][2] for d in names) * 512
    return {'snapshot_seconds': seconds,
        'server_cpu_cores': (cpu(last_cpu) - cpu(first_cpu)) / seconds,
        'physical_read_count': reads, 'physical_read_bytes': read_bytes,
        'physical_read_iops': reads / seconds,
        'physical_reads_per_get': reads / count,
        'physical_read_bytes_per_get': read_bytes / count}


def memtier(directory, prefix):
    rounds = []
    hist = None
    for trial in range(1, 4):
        label = f'{prefix}-r{trial}.get'
        with tarfile.open(directory / (label + '.tar.gz')) as archive:
            stats = json.load(archive.extractfile(label + '.json'))['ALL STATS']
            get = stats['Gets']
            count = get['Count']
            assert count == stats['Totals']['Count'] and count > 0
            assert get.get('Connection Errors', 0) == 0
            assert get['Misses/sec'] == 0
            csv_files = [m for m in archive.getmembers() if m.name.endswith('.csv')]
            assert len(csv_files) == 80
            csv_count = 0
            csv_latency_us = 0
            for member in csv_files:
                raw = archive.extractfile(member).read().decode()
                rows = csv.DictReader(raw.split('\n\n')[0].splitlines()[1:])
                for row in rows:
                    n = int(row['GET Requests'])
                    assert int(row['SET Requests']) == 0
                    assert int(row['GET Misses']) == 0
                    assert int(row['GET Hits']) == n
                    assert int(row['GET Total Bytes RX']) == 2057 * n
                    csv_count += n
                    csv_latency_us += float(row['GET Average Latency']) * 1e6 * n
            assert count == csv_count
            mean = get['Accumulated Latency'] * 1000 / count
            assert abs(mean - csv_latency_us / count) < 1.1
            timing = json.load(archive.extractfile(label + '.timing.json'))
            assert timing['exit_code'] == 0
            raw_log = archive.extractfile(label + '.log').read().decode()
            assert not re.search(r'error response|error:|failed to', raw_log, re.I)
        counters = data(directory / (label + '.commandstats.json'))
        before, after = counters['before'], counters['after']
        assert calls(after, 'get') - calls(before, 'get') == count
        assert calls(after, 'set') == calls(before, 'set')
        for side in (before, after):
            row = next(s for s in side.splitlines() if s.startswith('cmdstat_get:'))
            assert 'failed_calls=0' in row and 'rejected_calls=0' in row
        seconds = stats['CPU']['cpu_wall_seconds']
        assert 300 <= seconds < 302
        saved = data(directory / (label + '.result.json'))
        assert saved['count'] == count and abs(saved['average_us'] - mean) < 1e-8
        rounds.append({'trial': trial, 'count': count, 'seconds': seconds,
            'qps': count / seconds, 'mean_us': mean,
            'latency_sum_us': get['Accumulated Latency'] * 1000,
            'client_cpu_cores': stats['CPU']['cpu_total_seconds'] / seconds})
        first = data(directory / (f'{prefix}-r{trial}.before.telemetry.json'))
        last = data(directory / (f'{prefix}-r{trial}.after.telemetry.json'))
        if '/proc/diskstats' in first:
            proc = next(k for k in first if re.fullmatch(r'/proc/\d+/stat', k))
            wall = (datetime.datetime.fromisoformat(last['utc']) -
                    datetime.datetime.fromisoformat(first['utc'])).total_seconds()
            rounds[-1]['resources'] = resources(first[proc], last[proc],
                first['/proc/diskstats'], last['/proc/diskstats'], wall, count)
        # memtier's compressed histogram records microseconds too. Its bucket
        # approximation is used only for pooled percentiles, not the mean.
        percentiles = get.get('Percentile Latencies', stats['Totals']['Percentile Latencies'])
        h = HdrHistogram.decode(percentiles['Histogram log format']['Compressed Histogram'].encode())
        assert h.get_total_count() == count
        rounds[-1]['p99_us'] = h.get_value_at_percentile(99)
        if hist is None:
            hist = h
        else:
            hist.add(h)
    count = sum(r['count'] for r in rounds)
    seconds = sum(r['seconds'] for r in rounds)
    return {'count': count, 'seconds': seconds, 'qps': count / seconds,
        'mean_us': sum(r['latency_sum_us'] for r in rounds) / count,
        'p99_us': hist.get_value_at_percentile(99), 'rounds': rounds,
        'errors': 0, 'misses': 0, 'records': 500_000_000, 'value_bytes': 2048,
        'connections': 80, 'pipeline': 1}


def namespace(snapshot):
    return dict(item.split('=', 1) for item in snapshot['info']['namespace/bench80'].split(';') if '=' in item)


def aerospike(directory):
    rounds = []
    hist = None
    for trial in range(1, 4):
        label = f'read-r{trial}'
        with tarfile.open(directory / (label + '.tar.gz')) as archive:
            members = [m for m in archive.getmembers() if m.name.endswith('.hdrhist') and '/hdr/read_' in m.name]
            assert len(members) == 1
            text = archive.extractfile(members[0]).read().decode()
            assert '"StartTimestamp","EndTimestamp"' in text
            rows = [line for line in text.splitlines() if line and not line.startswith('#') and 'HIST' in line]
            assert len(rows) == 1
            row = rows[0].split(',')
            h = HdrHistogram.decode(row[-1].encode())
            seconds = float(Decimal(row[1]) - Decimal(row[0]))
            count = h.get_total_count()
            first = json.load(archive.extractfile(label + '/before.json'))
            last = json.load(archive.extractfile(label + '/after.json'))
            before = namespace(first)
            after = namespace(last)
            assert int(after['client_read_success']) - int(before['client_read_success']) == count
            assert int(after['objects']) == 10_000_000
            for key in ['client_read_error', 'client_read_timeout', 'client_read_not_found',
                        'client_write_success', 'expired_objects', 'evicted_objects']:
                assert after[key] == before[key]
            saved = json.load(archive.extractfile(label + '/result.json'))
            assert saved['synchronous'] and saved['connections_configured'] == 80
            assert saved['count'] == count and abs(saved['mean_us'] - h.get_mean_value()) < 1e-8
            assert 300 <= seconds < 302
        rounds.append({'trial': trial, 'count': count, 'seconds': seconds,
            'qps': count / seconds, 'mean_us': h.get_mean_value(),
            'p99_us': h.get_value_at_percentile(99),
            'client_cpu_cores': saved['cpu_client_cores'],
            'resources': resources(first['process_stat'], last['process_stat'],
                first['host_diskstats'], last['host_diskstats'],
                last['monotonic'] - first['monotonic'], count)})
        if hist is None:
            hist = h
        else:
            hist.add(h)
    count = hist.get_total_count()
    seconds = sum(r['seconds'] for r in rounds)
    return {'count': count, 'seconds': seconds, 'qps': count / seconds,
        'mean_us': hist.get_mean_value(), 'p99_us': hist.get_value_at_percentile(99),
        'rounds': rounds, 'errors': 0, 'misses': 0, 'records': 10_000_000,
        'value_bytes': 2048, 'connections': 80, 'max_inflight_per_connection': 1}


def calculate():
    return {
        'keylane_kernel_spdk': memtier(ROOT / 'evidence/keylane-spdk', 'kernel'),
        'keylane_dpdk_spdk': memtier(ROOT / 'evidence/keylane-spdk', 'dpdk'),
        'keylane_kernel_uring': memtier(ROOT / 'evidence/keylane-uring', 'uring'),
        'aerospike': aerospike(ROOT / 'evidence/aerospike'),
    }


def main():
    result = calculate()
    if sys.argv[1:] == ['--write']:
        (ROOT / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    else:
        assert not sys.argv[1:]
        assert result == data(ROOT / 'results.json')
        for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
            digest, name = line.split('  ', 1)
            with (ROOT / name).open('rb') as f:
                assert hashlib.file_digest(f, 'sha256').hexdigest() == digest, name
        print('PASS: client/server counts, 80 CSVs per Keylane run, complete HDRs, pooled statistics and SHA256SUMS')
    for name, value in result.items():
        print(f"{name}: {value['qps']:.3f} GET/s, mean {value['mean_us']:.6f} us, p99 {value['p99_us']} us")


if __name__ == '__main__':
    main()
