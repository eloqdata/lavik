#!/usr/bin/env python3
"""Package already-accepted results; never launch server or workload processes."""
import csv
import copy
import hashlib
import json
from pathlib import Path
import shutil
import statistics

ROOT = Path('/mnt/local_nvme/i131')
LICENSE = '''# Copyright (C) 2026 EloqData Inc.
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
'''


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + '\n')


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def fmt(value, digits=3):
    return '—' if value is None else f'{value:.{digits}f}'


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(map(str, row)) + ' |' for row in rows])


def full_table(comparisons):
    rows = []
    for item in comparisons:
        before = item['variants'][item['before_label']]
        after = item['variants'][item['after_label']]
        row = [item['case']]
        for variant in (before, after):
            row.append(', '.join(fmt(value, 4) for value in variant['full_seconds']))
            row.append(fmt(variant['statistics']['median_seconds'], 4))
        row += [fmt(item['median_time_reduction_pct'], 2) + '%',
                fmt(item['median_speedup'], 3) + '×']
        rows.append(row)
    return table(['Case', 'Before r1 / r2 / r3 (s)', 'Before median',
                  'After r1 / r2 / r3 (s)', 'After median', 'Time reduction', 'Speedup'], rows)


def report_tables(runs, comparisons):
    ready_cases = {item['case'] for item in comparisons if item['status'] == 'ready'}
    grouped = {}
    for run in runs:
        grouped.setdefault((run['case'], run['label']), []).append(run)
    resources, frames, spans, live, condition_rows = [], [], [], [], []
    for (case, label), values in grouped.items():
        values.sort(key=lambda x: x['repeat'])
        status = 'complete' if case in ready_cases else 'partial; not a comparison'
        resource = [case, label, len(values), status]
        for role in ('source', 'target'):
            resource.append(fmt(median(
                v['sampled'][role]['rss_sampled_peak_bytes'] / 2**20
                if v['sampled'][role]['rss_sampled_peak_bytes'] is not None else None
                for v in values), 1))
            resource.append(fmt(median((v['sampled'][role].get('cpu') or {}).get(
                'average_busy_cores_in_sample_interval') for v in values)))
            fractions = [(v['sampled'][role].get('cpu') or {}).get('interval_fraction_of_full')
                         for v in values]
            fractions = [x for x in fractions if x is not None]
            resource.append(f'{min(fractions):.2f}–{max(fractions):.2f}' if fractions else '—')
        resources.append(resource)
        if values[0]['observed_frames'] is not None:
            for v in values:
                f = v['observed_frames']
                frames.append([case, label, v['repeat'], f['flows'], f['frame_counts'].get('2', 0),
                               f['wire_bytes_by_kind'].get('2', 0), f['partition_db_group_count'],
                               f['single_frame_partition_db_groups'],
                               f['multiple_frame_partition_db_groups'],
                               f['frames_per_partition_db'].get('min'),
                               f['frames_per_partition_db'].get('median'),
                               f['frames_per_partition_db'].get('max'),
                               f['repeated_large_key_count'],
                               f['frame_counts'].get('8', 0), f['wire_bytes_by_kind'].get('8', 0),
                               f['frame_counts'].get('1', 0), f['frame_counts'].get('6', 0),
                               sum(f['wire_bytes_by_kind'].values())])
            span_values = [(v.get('capture_spans') or {}).get('all_flows', {}).get(
                'records', {}).get('capture_span_seconds') for v in values]
            spans.append([case, label, len(values), status, ', '.join(fmt(x, 4) for x in span_values),
                          fmt(median(span_values), 4)])
        for v in values:
            f = v['foreground']
            if f:
                live.append([case, label, v['repeat'], f['begun_count'], f['successful_begun_count'],
                             f['completed_count'], fmt(f['completed_qps'], 2),
                             fmt(f['latency'].get('median_ms'), 2),
                             fmt(f['latency'].get('p99_ms'), 2),
                             fmt(f['latency'].get('max_ms'), 2), f['crossed_full_end_count']])
        c = values[0]['conditions']
        condition_rows.append([case, label, len(values), status, f"{c['source_workers']}→{c['target_workers']}",
                               c['rtt_ms'], ', '.join(fmt(v['observed_ping_ms']['p50_ms'], 3) for v in values),
                               c['capture']])
    return {
        'primary': full_table([c for c in comparisons if not c['case'].endswith('-nocap')]),
        'controls': full_table([c for c in comparisons if c['case'].endswith('-nocap')]),
        'resources': table(['Case', 'Variant', 'n', 'Cohort', 'Source median sampled peak RSS MiB', 'Source median sampled busy cores',
                            'Source sampled fraction', 'Target median sampled peak RSS MiB', 'Target median sampled busy cores',
                            'Target sampled fraction'], resources),
        'spans': table(['Case', 'Variant', 'n', 'Cohort', 'Available records capture spans in repeat order (s)', 'Median (s)'], spans),
        'live': table(['Case', 'Variant', 'Repeat', 'Begun', 'Latency n', 'Completed', 'QPS',
                       'Latency median ms', 'p99 ms', 'Max ms', 'Crossed FULL end'], live),
        'conditions': table(['Case', 'Variant', 'n', 'Cohort', 'Workers', 'Added RTT ms',
                             'Observed idle PING median r1 / r2 / r3 ms', 'Capture'], condition_rows),
        'frame_headers': ['case', 'label', 'repeat', 'flows', 'records_frames', 'records_wire_bytes',
                          'partition_db_groups', 'single_frame_groups', 'multiple_frame_groups',
                          'min_frames_per_group', 'median_frames_per_group', 'max_frames_per_group',
                          'repeated_large_keys', 'full_command_frames', 'full_command_native_bytes',
                          'reset_frames', 'handoff_frames', 'all_native_frame_bytes'],
        'frame_rows': frames,
    }


def package():
    summary = json.loads((ROOT / 'aggregates-final/summary.json').read_text())
    failed_case = 'large-live-128m-f1-rtt20'
    incomplete_cases = {failed_case, 'dense-records-128m-f4-rtt20'}
    assert len(summary['excluded_runs']) == 2, summary['excluded_runs']
    assert summary['unmeasured_cases'] == [failed_case], summary['unmeasured_cases']
    assert len(summary['runs']) == 164, len(summary['runs'])
    assert all(c['status'] == 'ready' for c in summary['comparisons'] if c['case'] not in incomplete_cases)
    variants = json.loads((ROOT / 'benchmark-variants.json').read_text())
    for comparison, labels in (('chunks', ['main', 'chunks']), ('records', ['chunks', 'records'])):
        out = ROOT / f'report-{comparison}'
        evidence = out / 'evidence'
        evidence.mkdir(parents=True, exist_ok=True)
        all_comps = [c for c in summary['comparisons'] if c['comparison'] == comparison]
        comps = [c for c in all_comps if c['status'] == 'ready']
        cases = {c['case'] for c in all_comps}
        runs = [r for r in summary['runs'] if r['case'] in cases]
        compact_runs = copy.deepcopy(runs)
        for run in compact_runs:
            run['comparison_status'] = next(c['status'] for c in all_comps if c['case'] == run['case'])
            run.pop('seed_partition_db_key_counts', None)
            if run['observed_frames']:
                run['observed_frames'].pop('partition_db', None)
            run['full_partition_db_maps'] = 'raw-results.jsonl: result.seed and result.frames'
        # Keep reviewer summaries compact; exact full maps remain in raw results.
        (evidence / 'per-run.json').write_text('[\n' + ',\n'.join(
            json.dumps(run, separators=(',', ':')) for run in compact_runs) + '\n]\n')
        dump(evidence / 'comparisons.json', all_comps)
        with (evidence / 'raw-results.jsonl').open('w') as target:
            for r in runs:
                run_dir = Path(r['run_dir'])
                raw = {'case': r['case'], 'repeat': r['repeat'], 'label': r['label']}
                for name in ('result', 'acceptance', 'driver-outcome'):
                    raw[name] = json.loads((run_dir / (name + '.json')).read_text())
                target.write(json.dumps(raw, separators=(',', ':')) + '\n')
        with (evidence / 'tcp-overlap.jsonl').open('w') as target:
            for run in runs:
                capture_analysis = Path(run['run_dir']) / 'tcp-overlap.json'
                if capture_analysis.exists():
                    item = {'case': run['case'], 'label': run['label'], 'repeat': run['repeat'],
                            'analysis': json.loads(capture_analysis.read_text())}
                    target.write(json.dumps(item, separators=(',', ':')) + '\n')
        manifest = []
        for run in runs:
            run_dir = Path(run['run_dir'])
            files = []
            for name in ('result.json', 'acceptance.json', 'driver-outcome.json', 'samples.jsonl',
                         'writer-samples.jsonl', 'frames.pcap', 'frame-spans.json', 'tcp-overlap.json', 'tcpdump.log',
                         'source/server.log', 'target/server.log'):
                p = run_dir / name
                if p.exists():
                    record = {'path': str(p), 'bytes': p.stat().st_size}
                    if p.suffix != '.pcap':
                        record['sha256'] = hashlib.sha256(p.read_bytes()).hexdigest()
                    files.append(record)
            manifest.append({'case': run['case'], 'repeat': run['repeat'], 'label': run['label'],
                             'files': files})
        dump(evidence / 'retained-artifacts.json', manifest)
        for csv_name in ('per-run.csv', 'comparisons.csv'):
            with (ROOT / 'aggregates-final' / csv_name).open() as source:
                reader = csv.DictReader(source)
                rows = [row for row in reader if row['case'] in cases]
                fields = list(reader.fieldnames)
                if csv_name == 'per-run.csv':
                    fields.append('comparison_status')
                    condition_fields = ('capture', 'queue_mib', 'backlog_mib', 'writer_warmup',
                                        'source_cpus', 'target_cpus', 'client_cpus')
                    fields.extend(condition_fields)
                    for row in rows:
                        row['comparison_status'] = next(c['status'] for c in all_comps if c['case'] == row['case'])
                        run = next(r for r in runs if r['case'] == row['case'] and
                                   r['label'] == row['label'] and r['repeat'] == int(row['repeat']))
                        row.update({key: run['conditions'].get(key) for key in condition_fields})
                with (evidence / csv_name).open('w') as target:
                    writer = csv.DictWriter(target, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(rows)
        tabs = report_tables(runs, comps)
        with (evidence / 'frame-distribution.csv').open('w') as target:
            writer = csv.writer(target)
            writer.writerow(tabs['frame_headers'])
            writer.writerows(tabs['frame_rows'])
        for table_name in ('resources', 'spans', 'live', 'conditions'):
            (evidence / (table_name + '.md')).write_text(tabs[table_name] + '\n')
        for label in labels:
            binary_dir = evidence / 'build' / label
            binary_dir.mkdir(parents=True, exist_ok=True)
            for name in ('CMakeCache.txt', 'build-provenance.json'):
                shutil.copy2(ROOT / 'bin' / label / name, binary_dir / name)
            provenance = json.loads((binary_dir / 'build-provenance.json').read_text())
            for key in ('configure_log', 'build_log'):
                if key in provenance and Path(provenance[key]).exists():
                    shutil.copy2(provenance[key], binary_dir / (key + '.log'))
        if comparison == 'records':
            failure_dir = ROOT / 'measurements/dense-records-128m-f4-rtt20/r2-records'
            diagnostic = evidence / 'capture-failure'
            diagnostic.mkdir(exist_ok=True)
            for name in ('result.json', 'acceptance.json', 'driver-outcome.json',
                         'diagnostic-outcome.json', 'tcpdump.log'):
                shutil.copy2(failure_dir / name, diagnostic / name)
            for side in ('source', 'target'):
                shutil.copy2(failure_dir / side / 'server.log', diagnostic / (side + '-server.log'))
        if comparison == 'chunks':
            failure_dir = ROOT / 'measurements' / failed_case / 'r1-main'
            diagnostic = evidence / 'diagnostic-failure'
            diagnostic.mkdir(exist_ok=True)
            for name in ('result.json', 'acceptance.json', 'driver-outcome.json',
                         'diagnostic-outcome.json', 'stall-snapshot.json',
                         'stall-processes.json', 'stall-clients.json', 'writer-samples.jsonl'):
                shutil.copy2(failure_dir / name, diagnostic / name)
            for side in ('source', 'target'):
                shutil.copy2(failure_dir / side / 'server.log', diagnostic / (side + '-server.log'))
            # Preserve complete, timestamped diagnostic observations without copying
            # the full periodic trace into the review artifact.
            trace = failure_dir / 'samples.jsonl'
            raw_lines = trace.read_text().splitlines()
            observed = [(number, json.loads(line)) for number, line in enumerate(raw_lines, 1)]
            selected = []
            for number, sample in observed:
                admitted = sum(value for key, value in sample.get('source', {}).items()
                               if 'fullsync_publish_queue_admitted_bytes' in key)
                if admitted == 16777933 and (not selected or
                        sample['monotonic'] - selected[-1]['sample']['monotonic'] >= 60):
                    selected.append({'original_line': number, 'sample': sample})
            if observed and (not selected or selected[-1]['original_line'] != observed[-1][0]):
                selected.append({'original_line': observed[-1][0], 'sample': observed[-1][1]})
            dump(diagnostic / 'samples-excerpt.json', {
                'source_path': str(trace), 'source_sha256': hashlib.sha256(trace.read_bytes()).hexdigest(),
                'source_line_count': len(raw_lines),
                'selection': 'First sample with admitted=16777933B, then >=60s apart, plus final sample; complete original rows',
                'observations': selected})
            dump(evidence / 'excluded-runs.json', summary['excluded_runs'])
            tools = evidence / 'historical-tools'
            tools.mkdir(exist_ok=True)
            for name in ('benchmark-recipe.json', 'benchmark-accepted-primary-recipe.json',
                         'benchmark-capture-repeat-recipe.json', 'benchmark-controls-recipe.json', 'benchmark-live-headroom-recipe.json',
                         'benchmark-combined-recipe.json', 'benchmark-variants.json',
                         'run-benchmark-matrix.py', 'check-benchmark-run.py',
                         'aggregate-benchmarks.py', 'analyze-full-pcap.py', 'analyze-tcp-overlap.py',
                         'package-benchmark-reports.py', 'plot-benchmark-results.py', 'run-offline-analysis.py'):
                shutil.copy2(ROOT / name, tools / name)
            shutil.copy2(ROOT / 'bench-dev/scripts/bench_native_full_sync.py', tools / 'bench_native_full_sync.py')
            shutil.copy2(ROOT / 'bench-dev/tests/bench_native_full_sync_test.py', tools / 'bench_native_full_sync_test.py')
            shutil.copy2(ROOT / 'benchmark-host.json', evidence / 'host.json')
            shutil.copy2(ROOT / 'benchmark-final-cleanup.json', evidence / 'final-cleanup.json')
            shutil.copy2(ROOT / 'offline-analysis-outcomes.json', evidence / 'offline-analysis-outcomes.json')
            portable = out / 'tools'
            portable.mkdir(exist_ok=True)
            for name in ('run-benchmark-matrix.py', 'check-benchmark-run.py',
                         'aggregate-benchmarks.py', 'analyze-full-pcap.py', 'analyze-tcp-overlap.py'):
                code = (tools / name).read_text()
                if name == 'run-benchmark-matrix.py':
                    code = code.replace("default='/mnt/local_nvme/i131/benchmark-recipe.json'",
                                        "default=str(Path(__file__).parent / 'benchmark-accepted-primary-recipe.json')")
                    code = code.replace("default='/mnt/local_nvme/i131/bench-dev/scripts/bench_native_full_sync.py'",
                                        "required=True")
                    code = code.replace("default='/mnt/local_nvme/i131/measurements'",
                                        "default='measurements'")
                    code = code.replace("'/mnt/local_nvme/i131/check-benchmark-run.py'",
                                        "str(Path(__file__).parent / 'check-benchmark-run.py')")
                elif name == 'aggregate-benchmarks.py':
                    code = code.replace("'/mnt/local_nvme/i131/measurements'", "'measurements'")
                    code = code.replace("'/mnt/local_nvme/i131/benchmark-recipe.json'",
                                        "str(Path(__file__).parent / 'benchmark-combined-recipe.json')")
                    code = code.replace("'/mnt/local_nvme/i131/aggregates'", "'aggregates'")
                line, rest = code.split('\n', 1)
                if 'Copyright (C) 2026 EloqData Inc.' in code:
                    (portable / name).write_text(code)
                else:
                    (portable / name).write_text(line + '\n' + LICENSE + '\n' + rest)
            for name in ('benchmark-recipe.json', 'benchmark-accepted-primary-recipe.json',
                         'benchmark-capture-repeat-recipe.json', 'benchmark-controls-recipe.json',
                         'benchmark-live-headroom-recipe.json', 'benchmark-combined-recipe.json', 'benchmark-variants.json'):
                shutil.copy2(tools / name, portable / name)
            (tools / 'README.md').write_text(
                '# Exact historical tool snapshots\n\n'
                'These files preserve the task-local tools and paths used for the original measurement and analysis. '
                'The runner and its tests carry their original Apache headers; the other scripts are preserved as historical evidence. '
                'They expect `/mnt/local_nvme/i131`, the original checkout under `bench-dev`, and the binary paths in the variants file. '
                'For a new run use the documented portable copies in `../../tools/`. '
                'The portable copies only add license headers and change default/local path resolution; they were not used for the original runs.\n')
        failure_files = []
        for name in ('result.json', 'acceptance.json', 'driver-outcome.json', 'diagnostic-outcome.json',
                     'samples.jsonl', 'writer-samples.jsonl', 'frames.pcap', 'tcpdump.log',
                     'source/server.log', 'target/server.log', 'stall-snapshot.json',
                     'stall-processes.json', 'stall-clients.json'):
            original = failure_dir / name
            if original.exists():
                entry = {'path': str(original), 'bytes': original.stat().st_size}
                if original.suffix != '.pcap':
                    entry['sha256'] = hashlib.sha256(original.read_bytes()).hexdigest()
                failure_files.append(entry)
        dump(diagnostic / 'retained-artifacts.json', failure_files)
        dump(evidence / 'cohort.json', {'comparison': comparison, 'labels': labels,
                                      'accepted_runs': len(runs), 'cases': sorted(cases),
                                      'all_accepted_formal_runs': 164,
                                      'comparable_formal_runs': 162,
                                      'diagnostic_excluded_runs': 2,
                                      'created_at_utc': summary['created_at_utc'],
                                      'variant_declarations': {k: variants[k] for k in labels}})
        for name in ('primary', 'controls'):
            (evidence / (name + '-table.md')).write_text(tabs[name] + '\n')
        print(f'{comparison}: {len(runs)} runs, {len(comps)} comparisons → {out}')


if __name__ == '__main__':
    package()
