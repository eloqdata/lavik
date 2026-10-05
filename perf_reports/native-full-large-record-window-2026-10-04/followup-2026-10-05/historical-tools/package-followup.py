#!/usr/bin/env python3
"""Package the separately declared 2026-10-05 no-capture cohort."""
import hashlib
import json
from pathlib import Path
import shutil
from statistics import median

R = Path('/mnt/local_nvme/i131')
s = json.loads((R/'aggregates-followup/summary.json').read_text())
assert s['accepted_run_count'] == 60 and not s['excluded_runs'] and not s['unmeasured_cases']
assert all(c['status'] == 'ready' for c in s['comparisons'])

def dump(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False)+'\n')

def table(head, rows):
    return '\n'.join(['| '+' | '.join(head)+' |', '| '+' | '.join(['---']*len(head))+' |']+['| '+' | '.join(map(str,row))+' |' for row in rows])

for label in ('chunks','records'):
    dest=R/f'followup-{label}'
    dest.mkdir(exist_ok=True)
    comparisons=[c for c in s['comparisons'] if c['comparison']==label]
    cases={c['case'] for c in comparisons}
    runs=[r for r in s['runs'] if r['case'] in cases]
    dump(dest/'comparisons.json', comparisons)
    dump(dest/'per-run.json',runs)
    recipe=json.loads((R/'benchmark-nocap-followup-recipe.json').read_text())
    recipe['cases']=[c for c in recipe['cases'] if c['comparison']==label]
    dump(dest/'recipe.json',recipe)
    raw=[]; writers=[]; retained=[]; fullrows=[]; latencyrows=[]; summaryrows=[]
    for r in runs:
        d=Path(r['run_dir'])
        raw.append({'case':r['case'],'repeat':r['repeat'],'label':r['label'],**{f:json.loads((d/(f+'.json')).read_text()) for f in ('result','acceptance','driver-outcome')}})
        if r['foreground']:
            writers.append({'case':r['case'],'repeat':r['repeat'],'label':r['label'],'samples':[json.loads(l) for l in (d/'writer-samples.jsonl').read_text().splitlines()]})
        for rel in ('result.json','acceptance.json','driver-outcome.json','samples.jsonl','writer-samples.jsonl','source/server.log','target/server.log'):
            p=d/rel
            if p.exists():retained.append({'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    (dest/'raw-results.jsonl').write_text(''.join(json.dumps(v,ensure_ascii=False)+'\n' for v in raw))
    (dest/'writer-samples.jsonl').write_text(''.join(json.dumps(v,ensure_ascii=False)+'\n' for v in writers))
    dump(dest/'retained-artifacts.json',retained)
    for c in comparisons:
        before,after=c['before_label'],c['after_label']
        bm=median(c['variants'][before]['full_seconds']); am=median(c['variants'][after]['full_seconds'])
        wins=sum(p['after_seconds']<p['before_seconds'] for p in c['paired_values'])
        summaryrows.append([c['case'],f'{bm:.4f}',f'{am:.4f}',f"{c['median_time_reduction_pct']:.2f}%",f"{c['median_paired_time_reduction_pct']:.2f}%",f'{wins}/10'])
        b={r['repeat']:r for r in runs if r['case']==c['case'] and r['label']==before}
        a={r['repeat']:r for r in runs if r['case']==c['case'] and r['label']==after}
        for i in range(1,11):
            row=[c['case'],i,f"{b[i]['full_seconds']:.4f}",f"{a[i]['full_seconds']:.4f}",f"{100*(1-a[i]['full_seconds']/b[i]['full_seconds']):.2f}%"]
            fullrows.append(row)
            if b[i]['foreground']:
                for run in (b[i],a[i]):
                    f=run['foreground']; lat=f['latency']
                    latencyrows.append([run['case'],i,run['label'],f['begun_count'],f['completed_count'],f"{f['completed_qps']:.3f}",f"{lat['median_ms']:.2f}",f"{lat['max_ms']:.2f}"])
    (dest/'full-table.md').write_text(table(['Case','Before median s','After median s','Median time reduction','Median paired time reduction','Faster pairs'],summaryrows)+'\n\n'+table(['Case','Pair','Before s','After s','Time reduction'],fullrows)+'\n')
    if latencyrows:(dest/'latency-table.md').write_text(table(['Case','Pair','Variant','Begun in FULL','Completed in FULL','Completed QPS','Median ms','Max ms'],latencyrows)+'\n')
    print(label, summaryrows)
    if label=='chunks':
        for c in comparisons:
            print(c['case'])
            for variant in (c['before_label'],c['after_label']):
                cohort=[r for r in runs if r['case']==c['case'] and r['label']==variant]
                v=[]
                for row in writers:
                    if row['case']!=c['case'] or row['label']!=variant:continue
                    run=next(r for r in cohort if r['repeat']==row['repeat'])
                    result=next(x['result'] for x in raw if x['case']==c['case'] and x['label']==variant and x['repeat']==row['repeat'])
                    start=result['full']['start_monotonic']; end=result['full']['end_monotonic']
                    v += [q['latency_ms'] for q in row['samples'] if start<=q['begin_monotonic']<end and q['success']]
                print(variant,'count',len(v),'median',median(v),'max',max(v),'run_maxima',[r['foreground']['latency']['max_ms'] for r in cohort])
