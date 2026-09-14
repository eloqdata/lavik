#!/usr/bin/env python3
"""Independently audit raw client evidence and summarize complete paired runs.

QPS pools completed requests and elapsed time. Latency summaries are explicitly
medians of three run-level percentiles, not percentiles of a pooled population.
No generated validation file is trusted as the original measurement source.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import statistics

ROOT = Path(__file__).resolve().parent
BINARY_SHA = '57986c5a42dc8b1b74ec1725d1ea8b4cb2f8151451caa16d4261264e2b4c494d'


def read(name):
    return json.loads((ROOT / name).read_text())


def counter(info, op):
    match = re.search('cmdstat_' + op + r':calls=(\d+)', info)
    return int(match[1]) if match else 0


def metrics(name):
    return {line.split()[0]: float(line.split()[1])
            for line in (ROOT / name).read_text().splitlines() if line and not line.startswith('#')}


def phase(label, seconds):
    client = read(label + '.client.json')
    config = client['configuration']
    assert config['clients'] * config['threads'] == 640 and config['pipeline'] == 1
    stats = client['ALL STATS']
    count, duration = stats['Totals']['Count'], stats['Runtime']['Total duration']
    assert seconds * 1000 <= duration < (seconds + 15) * 1000
    raw = (ROOT / (label + '.client.log')).read_text()
    final = re.search(r'Overall number of requests: (\d+), QPS: ([\d.e+]+)', raw)
    assert final and int(final[1]) == count
    assert not any(int(n) for n in re.findall(r'errs: (\d+)', raw))
    assert not re.search(r'Got \d+ error responses!', raw)
    assert all(int(n) <= 1 for n in re.findall(r'max_pending: (\d+)', raw))
    server = read(label + '.commandstats.json')
    assert stats['Sets']['Count'] == counter(server['after'], 'set') - counter(server['before'], 'set')
    assert stats['Gets']['Count'] == counter(server['after'], 'get') - counter(server['before'], 'get')
    assert stats['Sets']['Count'] + stats['Gets']['Count'] == count
    hits = [int(n) for n in re.findall(r'Total hits: (\d+)', raw)]
    assert len(hits) == 16 and sum(hits) == stats['Gets']['Count']
    if stats['Gets']['Count']:
        assert re.search(r'Hit rate: 100%', raw)
    operations = {}
    for name in ('Totals', 'Gets', 'Sets'):
        data = stats[name]
        assert sum(item['Count'] for item in data['Time-Serie'].values()) == data['Count']
        qps = data['Count'] * 1000 / duration
        assert abs(qps - data['Ops/sec']) < 0.001
        p = data['Percentile Latencies']
        assert 0 <= p['p50.00'] <= p['p99.00'] <= p['p99.90']
        operations[name] = {'count': data['Count'], 'qps': qps,
             'average_ms': data['Average Latency'], 'p99_ms': p['p99.00'],
             'p999_ms': p['p99.90'], 'max_ms': data['Max Latency']}
    histograms = [[(int(low), int(high), int(n)) for low, high, n in
                   re.findall(r'^\[\s*(\d+),\s*(\d+)\s*\)\s+(\d+)', part, re.M)]
                  for part in raw.split('Latency summary, all times are in usec:')[1:]]
    assert histograms and all(h == histograms[0] for h in histograms)
    buckets = histograms[0]
    assert sum(n for _, _, n in buckets) == count
    cumulative = 0
    p9999 = None
    for low, high, n in buckets:
        cumulative += n
        if cumulative * 10000 >= count * 9999:
            p9999 = [low / 1000, high / 1000]
            break
    saved = read(label + '.validation.json')
    assert saved['requests'] == count and abs(saved['wall_qps'] - operations['Totals']['qps']) < 1e-6
    assert saved['json_sha256'] == hashlib.sha256((ROOT / (label + '.client.json')).read_bytes()).hexdigest()
    assert saved['log_sha256'] == hashlib.sha256((ROOT / (label + '.client.log')).read_bytes()).hexdigest()
    assert saved['p9999_total_bucket_ms'] == p9999
    return {'label': label, 'duration_ms': duration, 'requests': count,
            'start_utc': saved['start_utc'], 'end_utc': saved['end_utc'],
            'console_qps': float(final[2]), 'operations': operations,
            'p9999_total_bucket_ms': p9999}


def cell(item):
    workload, pair, interval = item['workload'], item['pair'], item['flush_ms']
    label = f'{workload}-p{pair}-{interval}ms'
    launch = read(label + '.server-command.json')
    assert launch['binary_sha256'] == BINARY_SHA and launch['flush_max_ms'] == interval
    assert launch['source'] == '2f02e6a75e60e942946257b8a48135cd72c8d9ff'
    argv = launch['argv']
    assert argv[argv.index('--flush-max-ms') + 1] == str(interval)
    assert argv[argv.index('--threads') + 1] == '16'
    assert '--pin-workers' in argv and '--defrag-paused' not in argv
    process = read(label + '.process.json')
    # Single-CPU affinities identify all 16 pinned worker threads; the main
    # thread and DPDK interrupt helper may retain wider affinity masks.
    affinities = list(process['task_affinities'].values())
    assert all(affinities.count(str(cpu)) == 1 for cpu in range(16))
    for stage in ('warmup', 'measured'):
        client = read(label + '.' + stage + '.client.json')['configuration']
        assert client['ratio'] == {'get': '0:1', 'set': '1:0', 'mixed': '1:1'}[workload]
        assert client['server'] == '172.16.0.4' and client['port'] == 6379
        command = read(label + '.' + stage + '.client-command.json')
        assert command['client_sha256'] == '68fbf912ddd469e621025e35b5b42cb658ed0daef476bf725a9d1e37cf0a538f'
        flags = dict(token.split('=', 1) for token in shlex.split(command['command'])
                     if token.startswith('--') and '=' in token)
        expected = {'--key_prefix': '', '--key_minimum': '0',
                    '--key_maximum': '1000000000', '--key_dist': 'U', '--d': '1024',
                    '--qps': '0', '--pipeline': '1', '--proactor_threads': '16', '--c': '40',
                    '--tcp_nodelay': 'true', '--vmodule': 'dfly_bench=1', '--logtostderr': 'true',
                    '--test_time': '30' if stage == 'warmup' else '300',
                    '--seed': str((100 if stage == 'warmup' else 42) + pair)}
        assert all(flags.get(key) == value for key, value in expected.items())
    server_log = (ROOT / (label + '.server.log')).read_text()
    assert f'flush_max_ms={interval} ' in server_log and 'defrag_paused=false' in server_log
    phase(label + '.warmup', 30)
    result = phase(label + '.measured', 300)
    for stage in ('ready', 'before', 'after'):
        data = read(label + '.' + stage + '.snapshot.json')
        assert data['dbsize'] == 1_000_000_000
        assert set(data['lengths'].values()) == {1024}
        assert 'defrag_paused:0' in data['info']
        assert all(row['effective'] == str(q) for q, row in data['irq']['queues'].items())
    stopped = read(label + '.stop.json')
    assert stopped['ExecMainStatus'] == '0' and stopped['MainPID'] == '0'
    before, after = metrics(label + '.before.metrics.txt'), metrics(label + '.after.metrics.txt')
    deltas = {k: after[k] - v for k, v in before.items()
              if k.startswith(('keylane_storage_io_operations_total', 'keylane_storage_io_bytes_total',
                               'keylane_storage_defrag_runs_total'))}
    for key in ('keylane_storage_defrag_runs_total{result="error"}',
                'keylane_storage_defrag_runs_total{result="resource_exhausted"}'):
        assert before[key] == after[key] == 0
    assert all(v >= 0 for v in deltas.values())
    gets, sets = result['operations']['Gets']['count'], result['operations']['Sets']['count']
    if workload == 'get':
        assert sets == 0
    elif workload == 'set':
        assert gets == 0
    else:
        assert abs(gets - sets) <= 6 * (gets + sets) ** 0.5 + 640
    return {**item, **result, 'server_activity_delta': deltas}


def summarize(rows):
    summaries, pairs = [], []
    for workload in ('get', 'set', 'mixed'):
        items = [r for r in rows if r['workload'] == workload]
        for interval in (1000, 100):
            group = [r for r in items if r['flush_ms'] == interval]
            if len(group) != 3:
                continue
            elapsed_ms = sum(r['duration_ms'] for r in group)
            result = {'workload': workload, 'flush_ms': interval, 'runs': 3,
                      'operations': {}, 'total_qps_range': [
                          min(r['operations']['Totals']['qps'] for r in group),
                          max(r['operations']['Totals']['qps'] for r in group)],
                      'p9999_total_buckets_ms': [r['p9999_total_bucket_ms'] for r in group]}
            for op in ('Totals', 'Gets', 'Sets'):
                count = sum(r['operations'][op]['count'] for r in group)
                if not count:
                    continue
                result['operations'][op] = {'count': count, 'qps': count * 1000 / elapsed_ms,
                    **{k: statistics.median(r['operations'][op][k] for r in group)
                       for k in ('average_ms', 'p99_ms', 'p999_ms', 'max_ms')}}
            summaries.append(result)
        for pair in (1, 2, 3):
            by_mode = {r['flush_ms']: r for r in items if r['pair'] == pair}
            if len(by_mode) == 2:
                pairs.append({'workload': workload, 'pair': pair,
                    'qps_100ms_vs_1000ms_pct': (by_mode[100]['operations']['Totals']['qps'] /
                                             by_mode[1000]['operations']['Totals']['qps'] - 1) * 100})
    return {'rows': rows, 'summaries': summaries, 'pairs': pairs}


def table(result):
    lines = ['| Workload | 轮次 | flush ms | 总 QPS | GET QPS | SET QPS | p99 ms | p999 ms | 总体 p9999 桶 ms |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---|']
    for row in result['rows']:
        op = row['operations']
        low, high = row['p9999_total_bucket_ms']
        lines.append(f"| {row['workload']} | {row['pair']} | {row['flush_ms']} | "
                     f"{op['Totals']['qps']:,.0f} | {op['Gets']['qps']:,.0f} | {op['Sets']['qps']:,.0f} | "
                     f"{op['Totals']['p99_ms']:.3f} | {op['Totals']['p999_ms']:.3f} | [{low:g}, {high:g}) |")
    return '\n'.join(lines)


def summary_table(result):
    lines = ['| Workload | flush ms | 总 QPS | GET QPS | SET QPS | p99 中位数 ms | p999 中位数 ms |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for group in result['summaries']:
        op = group['operations']
        total = op['Totals']
        lines.append(f"| {group['workload']} | {group['flush_ms']} | {total['qps']:,.0f} | "
                     f"{op.get('Gets', {}).get('qps', 0):,.0f} | "
                     f"{op.get('Sets', {}).get('qps', 0):,.0f} | "
                     f"{total['p99_ms']:.3f} | {total['p999_ms']:.3f} |")
    return '\n'.join(lines)


def paired_table(result):
    lines = ['| Workload | 汇总 QPS 变化 | 配对 1 | 配对 2 | 配对 3 |',
             '|---|---:|---:|---:|---:|']
    for workload in ('get', 'set', 'mixed'):
        groups = {g['flush_ms']: g for g in result['summaries'] if g['workload'] == workload}
        if len(groups) != 2:
            continue
        delta = (groups[100]['operations']['Totals']['qps'] /
                 groups[1000]['operations']['Totals']['qps'] - 1) * 100
        pairs = sorted((p for p in result['pairs'] if p['workload'] == workload),
                       key=lambda p: p['pair'])
        lines.append(f'| {workload} | {delta:+.2f}% | ' +
                     ' | '.join(f"{p['qps_100ms_vs_1000ms_pct']:+.2f}%" for p in pairs) + ' |')
    return '\n'.join(lines)


def mixed_table(result):
    lines = ['| flush ms | 操作 | QPS | 平均延迟中位数 ms | p99 中位数 ms | p999 中位数 ms |',
             '|---:|---|---:|---:|---:|---:|']
    for group in result['summaries']:
        if group['workload'] != 'mixed':
            continue
        for name in ('Gets', 'Sets'):
            op = group['operations'][name]
            lines.append(f"| {group['flush_ms']} | {name} | {op['qps']:,.0f} | "
                         f"{op['average_ms']:.3f} | {op['p99_ms']:.3f} | {op['p999_ms']:.3f} |")
    return '\n'.join(lines)


def activity_table(result):
    lines = ['| Workload | 轮次 | flush ms | storage read GiB | storage write GiB | NVMe FLUSH 次数 | GC 成功次数 |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for row in result['rows']:
        if row['workload'] == 'get':
            continue
        activity = row['server_activity_delta']
        reads = activity['keylane_storage_io_bytes_total{operation="read"}'] / 2 ** 30
        writes = activity['keylane_storage_io_bytes_total{operation="write"}'] / 2 ** 30
        syncs = activity['keylane_storage_io_operations_total{operation="fdatasync"}']
        gc = activity['keylane_storage_defrag_runs_total{result="success"}']
        lines.append(f"| {row['workload']} | {row['pair']} | {row['flush_ms']} | "
                     f"{reads:.2f} | {writes:.2f} | {syncs:,.0f} | {gc:,.0f} |")
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--partial', action='store_true')
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--check-readme', action='store_true')
    args = parser.parse_args()
    plan = read('matrix-plan.json')
    assert len(plan['schedule']) == 18
    rows = []
    for item in plan['schedule']:
        label = f"{item['workload']}-p{item['pair']}-{item['flush_ms']}ms"
        if args.partial and not (ROOT / (label + '.stop.json')).exists():
            continue
        rows.append(cell(item))
    if not args.partial:
        assert read('matrix-complete.json')['cells'] == len(rows) == 18
    result = summarize(rows)
    result['complete'] = len(rows) == 18
    if args.write:
        assert result['complete'], 'cannot publish partial result as final'
        with (ROOT / 'results.json').open('x') as out:
            json.dump(result, out, indent=2)
            out.write('\n')
    rendered = table(result)
    if args.check_readme:
        assert result['complete']
        readme = (ROOT / 'README.md').read_text()
        assert all(render(result) in readme for render in
                   (table, summary_table, paired_table, mixed_table, activity_table))
    print(rendered)
    print(json.dumps({'complete': result['complete'], 'rows': len(rows),
                      'summaries': result['summaries'], 'pairs': result['pairs']}, indent=2))


if __name__ == '__main__':
    main()
