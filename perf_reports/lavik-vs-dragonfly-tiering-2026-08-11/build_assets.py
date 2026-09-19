#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Render the complete 27-point storage-tier matrix from results.csv."""
import csv,hashlib,math
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
ROOT=Path(__file__).resolve().parent
SYSTEMS=['lavik-spdk','lavik-raw','lavik-files','garnet','dragonfly','pika','kvrocks','tendis','keydb']
LABELS=['Lavik · SPDK','Lavik · raw io_uring','Lavik · XFS io_uring','Garnet Storage Tier','Dragonfly Tiered Storage','Pika','Apache Kvrocks','Tendis','KeyDB On Flash']
COLORS=['#1473E6','#4496EC','#86BFF5','#C84C8A','#E97827','#7B68AE','#599E81','#B89B42','#87929E']
HATCHES=['///','\\\\','xxx','...','++','ooo','---','***','|||']
KINDS=['get','mixed','set']
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'svg.fonttype':'none','svg.hashsalt':'lavik-tiering-release','axes.spines.top':False,'axes.spines.right':False})

def load():
    rows=list(csv.DictReader((ROOT/'results.csv').open()))
    assert len(rows)==27
    data={(r['system'],r['workload']):r for r in rows}
    assert set(data)=={(s,k) for s in SYSTEMS for k in KINDS}
    for r in rows:
        assert float(r['connection_errors'])==0 and float(r['get_misses_per_second'])==0
        assert all(math.isfinite(float(r[k])) and float(r[k])>0 for k in ['qps','p99_ms','p999_ms'])
    return data

def render(data,metric,name):
    fig,ax=plt.subplots(figsize=(16,8.6),facecolor='white')
    fig.subplots_adjust(left=.08,right=.985,bottom=.17,top=.68)
    fig.text(.055,.945,'Lavik versus Redis-compatible storage tiers',fontsize=25,weight='bold',color='#17202A')
    fig.text(.055,.9,'200 million keys · 1–4 KB values · 6 NVMe drives · 80 connections · 300 seconds per workload',fontsize=13,color='#566573')
    width=.085
    for i,(system,label,color,hatch) in enumerate(zip(SYSTEMS,LABELS,COLORS,HATCHES)):
        vals=[float(data[system,k][metric]) for k in KINDS]
        xs=[x+(i-4)*width for x in range(3)]
        ax.bar(xs,vals,width=width*.9,color=color,edgecolor='white',linewidth=.6,hatch=hatch,label=label,zorder=3)
        if metric=='qps':
            for x,v in zip(xs,vals):ax.text(x,v+6000,f'{v/1000:.1f}k',ha='center',va='bottom',rotation=90,fontsize=9,color='#34495E')
    ax.set_xticks(range(3),['GET · read-only','GET + SET · 1:1','SET · write-only'])
    ax.set_xlim(-.48,2.48)
    ax.set_ylim(bottom=0)
    ax.margins(y=.22 if metric=='qps' else .1)
    ax.set_ylabel('QPS' if metric=='qps' else 'p99.9 latency (ms)',color='#34495E')
    if metric=='qps':ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_:f'{v/1000:g}k'))
    ax.grid(axis='y',color='#DEE4EA',linewidth=.8,zorder=0)
    ax.spines['left'].set_color('#98A2AE');ax.spines['bottom'].set_color('#98A2AE')
    ax.tick_params(axis='both',length=0,pad=10,colors='#34495E')
    fig.legend(*ax.get_legend_handles_labels(),ncol=3,loc='upper left',bbox_to_anchor=(.055,.855),frameon=False,fontsize=12,columnspacing=2.4,handlelength=2.5)
    fig.text(.055,.065,'Lavik: 100 ms flush, separate NVMe paths · Peers: RAID0/XFS',fontsize=11,color='#566573')
    for ext in ['svg','png']:
        path=ROOT/f'{name}.{ext}'
        fig.savefig(path,dpi=150,metadata={'Date':None} if ext=='svg' else {})
        if ext=='svg':path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines())+'\n')
    plt.close(fig)

def main():
    data=load()
    render(data,'qps','tiering-throughput')
    render(data,'p999_ms','tiering-p999')
    files=sorted([*ROOT.glob('tiering-*.svg'),*ROOT.glob('tiering-*.png')])
    (ROOT/'chart-SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in files))
    print('Validated 27 points; rendered throughput and p99.9 figures.')
if __name__=='__main__':main()
