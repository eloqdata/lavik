#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Aggregate accepted native FULL runs without launching workloads.

Reads ROOT/CASE/r*-LABEL/{result,acceptance}.json and optional existing
frame-spans.json. Every accepted repetition is retained. No missing phase or
memory value is inferred. CPU describes the sampled interval inside FULL,
not total CPU cost extrapolated over the complete transfer.
"""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def quantiles(values, suffix=''):
    values = sorted(values)
    if not values:
        return {'count': 0}
    return {'count': len(values), 'min' + suffix: values[0],
            'median' + suffix: median(values), 'max' + suffix: values[-1],
            **{name + suffix: values[min(len(values)-1, math.ceil(q*len(values))-1)]
               for name, q in (('p50', .50), ('p90', .90), ('p99', .99), ('p999', .999))}}


def metric_sum(sample, base):
    values = [value for key, value in sample.items()
              if (key == base or key.startswith(base + '{')) and finite(value)]
    return sum(values) if values else None


def memory_cpu(samples, label, start, end, ticks_per_second):
    rows = [(s['monotonic'], s[label]) for s in samples
            if finite(s.get('monotonic')) and start <= s['monotonic'] <= end
            and isinstance(s.get(label), dict)]
    rows.sort(key=lambda pair: pair[0])
    result = {'sample_count': len(rows), 'rss_sampled_peak_bytes': None,
              'metric_sampled_peaks': {}, 'sample_interval_counter_deltas': {}, 'cpu': None,
              'scope': 'samples whose collection-start timestamp is within FULL; no peak extrapolation'}
    rss = [row['rss_bytes'] for _, row in rows if finite(row.get('rss_bytes'))]
    if rss:
        result['rss_sampled_peak_bytes'] = max(rss)
    metrics = ('lavik_worker_retained_memory_bytes', 'lavik_fullsync_reserved_memory_bytes',
               'lavik_fullsync_publish_queue_bytes', 'lavik_fullsync_publish_queue_admitted_bytes',
               'lavik_fullsync_publish_queue_capacity_bytes', 'lavik_fullsync_sessions',
               'lavik_replication_backlog_bytes', 'lavik_replication_publish_queue_bytes',
               'lavik_replication_backlog_pinned_cursors')
    for metric in metrics:
        # Sum shards within each sample, then take the peak. Adding each shard's
        # independent maximum would invent a peak that never occurred.
        values = [value for _, sample in rows if (value := metric_sum(sample, metric)) is not None]
        if values:
            result['metric_sampled_peaks'][metric] = max(values)
    for metric in ('lavik_fullsync_publish_queue_backpressure_waits_total',
                   'lavik_replication_backlog_backpressure_waits_total'):
        values = [(when, value) for when, sample in rows
                  if (value := metric_sum(sample, metric)) is not None]
        if len(values) >= 2 and values[-1][1] >= values[0][1]:
            result['sample_interval_counter_deltas'][metric] = {
                'delta': values[-1][1] - values[0][1],
                'seconds': values[-1][0] - values[0][0]}
    cpu_rows = [(when, sample['cpu_ticks']) for when, sample in rows
                if finite(sample.get('cpu_ticks'))]
    if len(cpu_rows) >= 2 and finite(ticks_per_second) and ticks_per_second > 0:
        first, last = cpu_rows[0], cpu_rows[-1]
        elapsed, ticks = last[0] - first[0], last[1] - first[1]
        if elapsed > 0 and ticks >= 0:
            seconds = ticks / ticks_per_second
            result['cpu'] = {'valid_sample_count': len(cpu_rows),
                             'sample_start_monotonic': first[0], 'sample_end_monotonic': last[0],
                             'valid_interval_seconds': elapsed,
                             'interval_fraction_of_full': elapsed / (end - start),
                             'cpu_seconds_in_sample_interval': seconds,
                             'average_busy_cores_in_sample_interval': seconds / elapsed,
                             'note': 'node reads occur serially after each row timestamp; approximate sample timing'}
    return result


def foreground(result, directory, start, end):
    if not result['arguments'].get('write_rate', 0):
        return None
    path = directory / result.get('writer_samples_file', 'writer-samples.jsonl')
    samples = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    begun = [s for s in samples if start <= s['begin_monotonic'] < end]
    completed = [s for s in samples if s['success'] and start <= s['end_monotonic'] < end]
    successes = [s for s in begun if s['success']]
    latency = quantiles([s['latency_ms'] for s in successes], '_ms')
    derived = {'mode': result['arguments'].get('writer_mode'),
               'requested_qps': result['arguments']['write_rate'],
               'raw_samples_file': str(path), 'all_attempt_count': len(samples),
               'begun_count': len(begun), 'successful_begun_count': len(successes),
               'failed_begun_count': len(begun) - len(successes),
               'completed_count': len(completed), 'completed_qps': len(completed) / (end - start),
               'crossed_full_end_count': sum(s['end_monotonic'] >= end for s in begun),
               'latency': latency, 'before_full_as_reported': result.get('writer', {}).get('before_full'),
               'latency_population': 'successful operations begun in FULL, including later completion'}
    reported = result.get('writer', {}).get('during_full', {})
    derived['runner_count_matches_raw'] = (reported.get('count') == len(successes)
                                          and reported.get('completed_count') == len(completed))
    if len(successes) < 100:
        derived['tail_sample_note'] = 'Too few samples for a robust p99/p999 claim; retain count/max/raw values.'
    return derived


def observed_frames(result):
    if not result['arguments'].get('capture'):
        return None
    frame_counts, wire_bytes, kinds, begins = Counter(), Counter(), Counter(), Counter()
    partition_db = defaultdict(lambda: {'frames': 0, 'bytes': 0, 'records': 0})
    flows = result.get('frames', {})
    for flow in flows.values():
        frame_counts.update(flow.get('frame_counts', {}))
        wire_bytes.update(flow.get('wire_bytes', {}))
        kinds.update(flow.get('record_kinds_db_kind', {}))
        begins.update(flow.get('large_value_begins_db_key_hex', {}))
        for group, values in flow.get('partition_db', {}).items():
            for field in ('frames', 'bytes', 'records'):
                partition_db[group][field] += values[field]
    per_db = defaultdict(lambda: {'partition_groups': 0, 'record_frames': 0, 'record_bytes': 0, 'records': 0})
    for group, values in partition_db.items():
        db = group.split(':')[1]
        per_db[db]['partition_groups'] += 1
        for source, dest in (('frames', 'record_frames'), ('bytes', 'record_bytes'), ('records', 'records')):
            per_db[db][dest] += values[source]
    frame_sizes = wire_bytes.get('2', 0) / frame_counts['2'] if frame_counts.get('2') else None
    return {'capture_complete': result.get('capture_complete'), 'flows': len(flows),
            'frame_counts': dict(frame_counts), 'wire_bytes_by_kind': dict(wire_bytes),
            'mean_records_frame_wire_bytes': frame_sizes,
            'record_kinds_db_kind': dict(kinds),
            'record_kind_legend': {'1': 'Value', '2': 'Delete', '3': 'ValueBegin', '4': 'ValueChunk', '5': 'ValueCommit'},
            'large_value_begins_db_key_hex': dict(begins),
            'repeated_large_key_count': sum(count > 1 for count in begins.values()),
            'partition_db_group_count': len(partition_db),
            'single_frame_partition_db_groups': sum(v['frames'] == 1 for v in partition_db.values()),
            'multiple_frame_partition_db_groups': sum(v['frames'] > 1 for v in partition_db.values()),
            'frames_per_partition_db': quantiles([v['frames'] for v in partition_db.values()]),
            'record_bytes_per_partition_db': quantiles([v['bytes'] for v in partition_db.values()]),
            'partition_db': dict(partition_db), 'per_db': dict(per_db),
            'scope': 'actual all-origin kRecords traffic; group bytes exclude outer frame overhead'}


def summarize_run(directory, result, acceptance):
    args = result['arguments']
    match = re.fullmatch(r'r(\d+)-(.+)', directory.name)
    if not match or match[2] != args['label']:
        raise ValueError('run directory repeat/label does not match recorded variant')
    start, end = result['full']['start_monotonic'], result['full']['end_monotonic']
    if not finite(start) or not finite(end) or end <= start:
        raise ValueError('FULL monotonic bounds missing or invalid')
    if not finite(result['full'].get('seconds')) or abs(result['full']['seconds'] - (end - start)) > 1e-6:
        raise ValueError('FULL duration disagrees with monotonic bounds')
    sample_path = directory / 'samples.jsonl'
    samples = [json.loads(line) for line in sample_path.read_text().splitlines() if line.strip()]
    conditions = {key: value for key, value in args.items()
                  if key not in ('binary', 'revision', 'label', 'run_dir')}
    conditions['host'] = {key: result.get('host', {}).get(key)
                          for key in ('kernel', 'machine', 'cpu_model', 'client_affinity')}
    fingerprint = hashlib.sha256(json.dumps(conditions, sort_keys=True).encode()).hexdigest()
    summary = {'case': directory.parent.name, 'repeat': int(match[1]), 'label': args['label'],
               'run_dir': str(directory), 'result_file': str(directory / 'result.json'),
               'acceptance_file': str(directory / 'acceptance.json'), 'accepted': acceptance['accepted'],
               'acceptance_notes': acceptance.get('notes', []),
               'revision': args['revision'], 'binary': args['binary'],
               'binary_sha256': result['binary_sha256'], 'runtime_revision': args.get('runtime_revision'),
               'build_description': args.get('build_description'),
               'conditions_fingerprint': fingerprint, 'conditions': conditions,
               'full_seconds': result['full']['seconds'],
               'seed_payload_bytes': result['seed']['payload_bytes'],
               'seed_key_count': result['seed']['keys'],
               'seed_partition_db_key_counts': result['seed'].get('partition_db_key_counts'),
               'seed_payload_mib_per_full_second': result['full'].get('seed_payload_mib_per_second'),
               'observed_ping_ms': result.get('network', {}).get('target_ping_ms'),
               'source_digest': result['source_digest'], 'target_digest': result['target_digest'],
               'metric_sample_errors': result.get('metric_sample_errors', []),
               'observed_frames': observed_frames(result),
               'foreground': foreground(result, directory, start, end),
               'sampled': {label: memory_cpu(samples, label, start, end,
                          result.get('host', {}).get('clock_ticks_per_second'))
                          for label in ('source', 'target')}}
    spans = directory / 'frame-spans.json'
    if spans.exists():
        summary['capture_spans'] = json.loads(spans.read_text())
        summary['capture_spans_file'] = str(spans)
    return summary


def compare_cases(runs, recipe):
    case_specs = {case['name']: case for case in recipe['cases']}
    grouped = defaultdict(list)
    for run in runs:
        grouped[run['case']].append(run)
    comparisons = []
    expected_repeats = set(range(1, recipe['repeats'] + 1))
    for name in sorted(grouped):
        spec = case_specs.get(name)
        if spec is None:
            comparisons.append({'case': name, 'status': 'case_not_in_recipe'})
            continue
        before, after = recipe['comparisons'][spec['comparison']]
        cohorts = {label: sorted([r for r in grouped[name] if r['label'] == label], key=lambda r: r['repeat'])
                   for label in (before, after)}
        comparison = {'case': name, 'comparison': spec['comparison'], 'before_label': before,
                      'after_label': after, 'expected_repeats': recipe['repeats'],
                      'status': 'ready', 'issues': [], 'variants': {}, 'paired_values': []}
        expected_args = {**recipe.get('common', {}), **spec.get('arguments', {})}
        for rows in cohorts.values():
            for run in rows:
                differing = [key for key, value in expected_args.items()
                             if run['conditions'].get(key) != value]
                if differing:
                    comparison['issues'].append(f"{run['label']}/r{run['repeat']}: recipe mismatch {differing}")
        all_conditions = {r['conditions_fingerprint'] for rows in cohorts.values() for r in rows}
        if len(all_conditions) != 1:
            comparison['issues'].append('conditions differ or no comparable runs')
        for label, rows in cohorts.items():
            values = [r['full_seconds'] for r in rows]
            comparison['variants'][label] = {'runs': [{'repeat': r['repeat'], 'full_seconds': r['full_seconds'],
                                                       'run_dir': r['run_dir'], 'revision': r['revision'],
                                                       'binary_sha256': r['binary_sha256']} for r in rows],
                                              'full_seconds': values, 'statistics': quantiles(values, '_seconds')}
            if len(rows) != len(expected_repeats) or {r['repeat'] for r in rows} != expected_repeats:
                comparison['issues'].append(f'{label}: incomplete or duplicate repeats')
            if len({(r['revision'], r['binary_sha256']) for r in rows}) > 1:
                comparison['issues'].append(f'{label}: revision/binary changes within cohort')
        bmap, amap = ({r['repeat']: r for r in cohorts[label]} for label in (before, after))
        for repeat in sorted(set(bmap) & set(amap)):
            b, a = bmap[repeat]['full_seconds'], amap[repeat]['full_seconds']
            comparison['paired_values'].append({'repeat': repeat, 'before_seconds': b, 'after_seconds': a,
                                                'speedup': b/a, 'time_reduction_pct': 100*(1-a/b)})
        if comparison['issues']:
            comparison['status'] = 'incomplete_or_incomparable'
        else:
            b = median(comparison['variants'][before]['full_seconds'])
            a = median(comparison['variants'][after]['full_seconds'])
            comparison['median_speedup'] = b/a
            comparison['median_time_reduction_pct'] = 100*(1-a/b)
            comparison['median_paired_speedup'] = median(p['speedup'] for p in comparison['paired_values'])
            comparison['median_paired_time_reduction_pct'] = median(p['time_reduction_pct'] for p in comparison['paired_values'])
        comparisons.append(comparison)
    return comparisons


def row_for_csv(run):
    args, fg, frames = run['conditions'], run.get('foreground') or {}, run.get('observed_frames') or {}
    row = {key: run.get(key) for key in ('case', 'repeat', 'label', 'revision', 'binary_sha256',
                                        'full_seconds', 'seed_payload_bytes', 'seed_payload_mib_per_full_second', 'run_dir')}
    row.update({key: args.get(key) for key in ('workload', 'keys', 'value_bytes', 'members', 'databases',
                                              'source_workers', 'target_workers', 'rtt_ms', 'write_rate', 'writer_mode')})
    row['observed_ping_p50_ms'] = (run.get('observed_ping_ms') or {}).get('p50_ms')
    row['observed_ping_p99_ms'] = (run.get('observed_ping_ms') or {}).get('p99_ms')
    for key in ('begun_count', 'successful_begun_count', 'completed_count', 'completed_qps', 'crossed_full_end_count'):
        row['writer_' + key] = fg.get(key)
    for key in ('count', 'p50_ms', 'median_ms', 'p99_ms', 'max_ms'):
        row['writer_latency_' + key] = fg.get('latency', {}).get(key)
    for label in ('source', 'target'):
        data = run['sampled'][label]
        row[label + '_full_sample_count'] = data['sample_count']
        row[label + '_rss_sampled_peak_bytes'] = data['rss_sampled_peak_bytes']
        row[label + '_full_queue_sampled_peak_bytes'] = data['metric_sampled_peaks'].get('lavik_fullsync_publish_queue_bytes')
        row[label + '_retained_sampled_peak_bytes'] = data['metric_sampled_peaks'].get('lavik_worker_retained_memory_bytes')
        for key in ('valid_interval_seconds', 'interval_fraction_of_full', 'cpu_seconds_in_sample_interval',
                    'average_busy_cores_in_sample_interval'):
            row[label + '_' + key] = (data['cpu'] or {}).get(key)
    for key in ('capture_complete', 'flows', 'mean_records_frame_wire_bytes', 'partition_db_group_count',
                'single_frame_partition_db_groups', 'multiple_frame_partition_db_groups', 'repeated_large_key_count'):
        row[key] = frames.get(key)
    row['record_frames'] = frames.get('frame_counts', {}).get('2')
    row['record_wire_bytes'] = frames.get('wire_bytes_by_kind', {}).get('2')
    row['records_capture_span_seconds'] = run.get('capture_spans', {}).get('all_flows', {}).get('records', {}).get('capture_span_seconds')
    return row


def csv_write(path, rows):
    if not rows:
        path.write_text('')
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value) if isinstance(value, (dict, list)) else value
                             for key, value in row.items()})


def aggregate(root, recipe):
    runs, excluded = [], []
    for directory in sorted(root.glob('*/r*-*')):
        if not directory.is_dir():
            continue
        result_path = directory / 'result.json'
        try:
            acceptance = json.loads((directory / 'acceptance.json').read_text())
            if acceptance.get('accepted') is not True:
                excluded.append({'run_dir': str(directory), 'reason': 'rejected by acceptance guard',
                                 'checks': acceptance.get('checks')})
                continue
            result = json.loads(result_path.read_text())
            runs.append(summarize_run(directory, result, acceptance))
        except Exception as error:
            excluded.append({'run_dir': str(directory), 'reason': repr(error)})
    return {'schema_version': 1, 'created_at_utc': datetime.now(timezone.utc).isoformat(),
            'measurement_root': str(root), 'accepted_run_count': len(runs),
            'runs': runs, 'excluded_runs': excluded, 'comparisons': compare_cases(runs, recipe),
            'unmeasured_cases': sorted({c['name'] for c in recipe['cases']} - {r['case'] for r in runs}),
            'limits': ['Retains all accepted repetitions; missing or rejected runs never become zero values.',
                       'CPU describes only its valid sampled interval inside FULL; RSS/FIFO peaks are sampled.',
                       'Captured frame-byte spans include interleaved work and waits, never exclusive phases.',
                       'Mixed live runs may transfer different total bytes because their FULL durations differ.',
                       'Comparisons are publishable only with ready status; individual descriptive medians remain visible.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('measurements'))
    parser.add_argument('--recipe', type=Path, default=Path(str(Path(__file__).parent / 'benchmark-combined-recipe.json')))
    parser.add_argument('--output', type=Path, default=Path('aggregates'))
    args = parser.parse_args()
    report = aggregate(args.root, json.loads(args.recipe.read_text()))
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'summary.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    (args.output / 'per-run.json').write_text(json.dumps(report['runs'], indent=2, allow_nan=False) + '\n')
    csv_write(args.output / 'per-run.csv', [row_for_csv(run) for run in report['runs']])
    comparison_rows = []
    for comparison in report['comparisons']:
        row = {key: value for key, value in comparison.items() if key not in ('variants', 'paired_values')}
        for label, values in comparison.get('variants', {}).items():
            row[label + '_all_seconds'] = values['full_seconds']
            row[label + '_median_seconds'] = values['statistics'].get('median_seconds')
        comparison_rows.append(row)
    csv_write(args.output / 'comparisons.csv', comparison_rows)
    print(json.dumps({'accepted_runs': report['accepted_run_count'], 'excluded_runs': len(report['excluded_runs']),
                      'ready_comparisons': sum(c['status'] == 'ready' for c in report['comparisons']),
                      'output': str(args.output)}))
