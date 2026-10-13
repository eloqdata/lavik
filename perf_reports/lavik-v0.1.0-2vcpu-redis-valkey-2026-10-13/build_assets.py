#!/usr/bin/env python3
"""Render the full concurrency sweeps; choose a fixed peer IO setting per command."""
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
ROOT=Path(__file__).resolve().parent
rows=list(csv.DictReader((ROOT/'results.csv').open()))
for r in rows:
    for k in ['connections','server_threads']:r[k]=int(r[k])
    for k in ['qps','p999_ms']:r[k]=float(r[k])
SYSTEMS=['lavik-spdk','redis','valkey']
LABELS={'lavik-spdk':'Lavik 0.1.0 / SPDK','redis':'Redis 8.8.0','valkey':'Valkey 9.1.0'}
COLORS={'lavik-spdk':'#087f6b','redis':'#c54a44','valkey':'#356bad'}
CONNS=[16,32,80,160,320,640,1280]
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold','svg.hashsalt':'lavik-2vcpu-20261013'})
def points(system,kind,threads=None):
    selection=[r for r in rows if r['system']==system and r['workload']==kind]
    if threads is None:threads=max(selection,key=lambda r:r['qps'])['server_threads']
    return sorted([r for r in selection if r['server_threads']==threads],key=lambda r:r['connections'])
def finish(fig,axes,name,title,ylabel):
    for ax,kind in zip(axes,['GET','SET']):
        ax.set_title(kind,loc='left');ax.set_xscale('log',base=2)
        ax.set_xticks(CONNS,[str(x) for x in CONNS]);ax.set_xlabel('Total connections (pipeline = 1)')
        ax.set_ylabel(ylabel);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2)
        ax.legend(frameon=False,fontsize=8)
    fig.suptitle(title,x=.07,ha='left',weight='bold',fontsize=15)
    fig.text(.07,.018,'1M keys × 1 KiB · same Azure L2aos_v5 · 2 vCPUs / 1 physical core · 30 s per point · single run',fontsize=9,color='#555555')
    fig.tight_layout(rect=(0,.055,1,.93))
    for ext in ['png','svg']:fig.savefig(ROOT/(name+'.'+ext),dpi=170,metadata={'Date':None} if ext=='svg' else None)
    svg=ROOT/(name+'.svg')
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    plt.close(fig)
for field,name,title,ylabel in [
    ('qps','throughput','Throughput across the full connection sweep','Throughput (k ops/s)'),
    ('p999_ms','latency','p99.9 latency at each measured concurrency','Client p99.9 latency (ms)')]:
    fig,axes=plt.subplots(1,2,figsize=(12,4.8))
    for ax,kind in zip(axes,['GET','SET']):
        for system in SYSTEMS:
            ps=points(system,kind);n=ps[0]['server_threads']
            label=LABELS[system]+(f' / IO={n}' if system!='lavik-spdk' else ' / workers=2')
            ax.plot([p['connections'] for p in ps],[p[field]/(1000 if field=='qps' else 1) for p in ps],marker='o',ms=4,lw=2,color=COLORS[system],label=label)
    finish(fig,axes,name,title,ylabel)
fig,axes=plt.subplots(1,2,figsize=(12,4.8))
for ax,kind in zip(axes,['GET','SET']):
    for system in SYSTEMS:
        for n in ([2] if system=='lavik-spdk' else [1,2]):
            ps=points(system,kind,n)
            ax.plot([p['connections'] for p in ps],[p['qps']/1000 for p in ps],marker='o',ms=3,lw=1.6,ls='--' if n==1 else '-',color=COLORS[system],label=LABELS[system]+(f' / IO={n}' if system!='lavik-spdk' else ' / workers=2'))
finish(fig,axes,'all-configurations','All tested server thread configurations','Throughput (k ops/s)')
print('Rendered 3 figures as PNG and SVG')
