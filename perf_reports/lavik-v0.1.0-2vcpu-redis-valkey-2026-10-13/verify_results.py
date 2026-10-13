#!/usr/bin/env python3
"""Validate every formal point against its recorded command and client JSON."""
import argparse,csv,json,pathlib,re
REPORT=pathlib.Path(__file__).resolve().parent
R=REPORT/'evidence'/'results'
CONNS=[16,32,80,160,320,640,1280]
NAMES={'lavik-spdk':'Lavik 0.1.0 SPDK (defrag enabled)','redis':'Redis 8.8.0','valkey':'Valkey 9.1.0'}

def expectations():
    for system,threads in [('lavik-spdk',2)]+[(s,n) for s in ['redis','valkey'] for n in [1,2]]:
        parent=R/'memory'/system
        if system!='lavik-spdk':parent/=f'io{threads}'
        for kind in ['GET','SET']:
            for c in CONNS:yield parent/f'{kind.lower()}-c{c}', 'memory',system,threads,kind,c,10**6,30

def load(partial=False):
    rows=[];missing=[]
    for stem,group,system,threads,kind,c,keys,seconds in expectations():
        result=stem.with_suffix('.result.json')
        if not result.exists():missing.append(str(stem.relative_to(R)));continue
        row=json.loads(result.read_text());args=json.loads(stem.with_suffix('.command.json').read_text())
        raw=json.loads(stem.with_suffix('.json').read_text());stats=raw['ALL STATS']
        cfg=raw['configuration']
        for key,value in {'threads':16,'clients':c//16,'pipeline':1,'test_time':30,'data_size':1024,'key_minimum':1,'key_maximum':1000000,'key_prefix':'','rate_limit':0,'expiry_range':'0:0','protocol':'redis'}.items():
            assert cfg[key]==value,(stem,key,cfg[key])
        required=['--threads=16','--clients='+str(c//16),'--pipeline=1','--key-minimum=1','--key-maximum='+str(keys),'--key-prefix=','--test-time='+str(seconds),'--command-key-pattern=R','--data-size=1024']
        assert '--distinct-client-seed' not in args, stem
        assert '--command='+('GET __key__' if kind=='GET' else 'SET __key__ __data__') in args, stem
        assert all(a in args for a in required),(stem,args)
        assert args[:3]==['taskset','-c','0-15'] and not any('rate-limit' in a for a in args)
        assert stats['Totals']['Connection Errors']==0,stem
        assert str(stats['Runtime']['Interrupted']).lower()=='false',stem
        client=json.loads(stem.with_suffix('.client-cpu.json').read_text())
        assert client['returncode']==0 and client['argv']==args,stem
        total=stats['Totals'];assert total['Count']>0 and total['Ops/sec']>0,stem
        assert abs(row['qps']-total['Ops/sec'])<.02,stem
        # Report means come from the original text Totals row. Reparse it
        # independently instead of comparing differently serialized means.
        body=stem.with_suffix('.txt').read_text().replace('\r','\n')
        printed=[line.split() for line in body.splitlines() if line.startswith('Totals')]
        assert len(printed)==1 and len(printed[0])==10,stem
        for column,index in [('qps',1),('avg_ms',4),('p50_ms',5),('p99_ms',6),('p999_ms',7),('p9999_ms',8)]:
            assert row[column]==float(printed[0][index]),(stem,column)
        assert not re.search(r'error response|connection error|error:|OOM command',body,re.I),stem
        for column,raw_key in [('p50_ms','p50.00'),('p99_ms','p99.00'),('p999_ms','p99.90'),('p9999_ms','p99.99')]:
            assert row[column]==total['Percentile Latencies'][raw_key],(stem,column)
        assert 0<=row['p50_ms']<=row['p99_ms']<=row['p999_ms']<=row['p9999_ms'],stem
        runtime=stats['Runtime'];assert runtime['Time unit']=='MILLISECONDS',stem
        assert runtime['Total duration'] >= seconds*1000-100,stem
        recomputed_qps=total['Count']/(runtime['Total duration']/1000)
        assert abs(recomputed_qps/total['Ops/sec']-1)<0.001,(stem,recomputed_qps,total['Ops/sec'])
        if kind=='GET':assert stats['Gets']['Misses/sec']==0 and abs(stats['Gets']['Hits/sec']-total['Ops/sec'])<.02,stem
        before=json.loads(stem.with_suffix('.before.json').read_text());after=json.loads(stem.with_suffix('.after.json').read_text())
        for snapshot in [before,after]:
            assert all(set(cpus).issubset({0,1}) for cpus in snapshot['threads'].values()),stem
            for command,rejected,failed in re.findall(r'^cmdstat_([^:]+):[^\n]*rejected_calls=(\d+),failed_calls=(\d+)',snapshot['info'],re.M):
                # Readiness PING can receive LOADING while the baseline RDB is
                # restored, before warmup. Timed commands must never fail.
                assert int(failed)==0 and (command=='ping' or int(rejected)==0),stem
            loading=re.search(r'^errorstat_LOADING:count=(\d+)',snapshot['info'],re.M)
            loading_count=int(loading[1]) if loading else 0
            ping=re.search(r'^cmdstat_ping:[^\n]*rejected_calls=(\d+)',snapshot['info'],re.M)
            assert int(ping[1])==loading_count,stem
            assert all(kind=='LOADING' or int(count)==0 for kind,count in re.findall(r'^errorstat_([^:]+):count=(\d+)',snapshot['info'],re.M)),stem
        def errors(snapshot):
            return {cmd:(int(rejected),int(failed)) for cmd,rejected,failed in re.findall(r'^cmdstat_([^:]+):[^\n]*rejected_calls=(\d+),failed_calls=(\d+)',snapshot['info'],re.M)}
        eb,ea=errors(before),errors(after)
        assert all(eb.get(cmd,(0,0))==ea.get(cmd,(0,0)) for cmd in eb.keys()|ea.keys()),stem
        for snapshot in [before,after]:
            version_line={'lavik-spdk':'lavik_version:0.1.0','redis':'redis_version:8.8.0','valkey':'valkey_version:9.1.0'}[system]
            assert version_line in snapshot['info'].splitlines(),stem
            assert int(re.search(r'^db0:keys=(\d+)',snapshot['info'],re.M)[1])==keys,stem
        def ticks(d):
            parts=d['process_stat'].split(') ',1)[1].split()
            return int(parts[11])+int(parts[12])
        # proc snapshots enclose INFO/SSH setup too, so this is diagnostic CPU
        # over the enclosing wall window rather than the exact memtier window.
        cpu=(ticks(after)-ticks(before))/before['clock_ticks']/(after['time']-before['time'])
        system_before=[int(v) for v in before['stat'].splitlines()[0].split()[1:9]]
        system_after=[int(v) for v in after['stat'].splitlines()[0].split()[1:9]]
        system_delta=[a-b for a,b in zip(system_after,system_before)]
        steal=100*system_delta[7]/sum(system_delta) if sum(system_delta)>0 else 0
        role='workers' if system=='lavik-spdk' else 'io-threads'
        r={'group':group,'product':NAMES[system],'system':system,'server_threads':threads,'thread_role':role,'workload':kind,'connections':c,'keys':keys,'value_bytes':1024,'test_seconds':seconds,'client_seed_mode':'default-correlated',
           **{k:row[k] for k in ['qps','avg_ms','p50_ms','p99_ms','p999_ms','p9999_ms']},
           'requests':total['Count'],'client_cores':stats['CPU']['cpu_cores_used'],'server_cores_enclosing_window':round(cpu,4),
           'host_steal_percent_enclosing_window':round(steal,4),
           'server_rss_mib_after':round(int(re.search(r'^VmRSS:\s+(\d+)',after['process_status'],re.M)[1])/1024,2),
           'connection_errors':total['Connection Errors'],'source':str(stem.relative_to(REPORT))+'.json'}
        rows.append(r)
    if not partial:
        assert not missing,missing
        assert len(rows)==70
        assert (R/'COMPLETE').exists() and not (R/'active-server.json').exists()
        lavik=R/'memory/lavik-spdk'
        assert json.loads((lavik/'host-state-before.json').read_text())==json.loads((lavik/'host-state-after.json').read_text())
        assert json.loads((lavik/'devices-before.json').read_text())==json.loads((lavik/'devices-after.json').read_text())
        for system in ['lavik-spdk','redis','valkey']:
            parent=R/'memory'/system
            fill=json.loads((parent/'fill.json').read_text())['ALL STATS']
            assert fill['Totals']['Count']==1000000 and fill['Totals']['Connection Errors']==0,parent
            checks=[parent] if system=='lavik-spdk' else [parent/f'io{n}' for n in [1,2]]
            for path in checks:
                assert (path/'exit-code.txt').read_text().strip()=='0',path
                server_args=json.loads((path/'server-command.json').read_text())
                assert server_args[:3]==['taskset','-c','0-1'],path
                if system!='lavik-spdk':
                    for key,val in [('--save',''),('--appendonly','no'),('--maxmemory','0')]:
                        assert server_args[server_args.index(key)+1]==val,path
                    assert server_args[server_args.index('--io-threads')+1]==path.name[2:],path
                else:
                    assert '--threads=2' in server_args and '--pin-workers' in server_args,path
                for phase in ['before','after']:
                    v=json.loads((path/f'validation-{phase}.json').read_text())
                    assert v['dbsize']=='1000000' and set(v['lengths'].values())=={'1024'}
        for path in (R/'memory/lavik-spdk').glob('*.before.json'):
            d=json.loads(path.read_text())
            assert d['defrag_config']['defrag-paused'].splitlines()==['defrag-paused','no']
        for path in (R/'memory/lavik-spdk').glob('*.after.json'):
            d=json.loads(path.read_text())
            assert d['defrag_config']['defrag-paused'].splitlines()==['defrag-paused','no']
    return rows,missing

def main():
    p=argparse.ArgumentParser();p.add_argument('--partial',action='store_true');args=p.parse_args()
    rows,missing=load(args.partial)
    if rows:
        with (REPORT/'results.csv').open('w') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');writer.writeheader();writer.writerows(rows)
    (REPORT/'validation.json').write_text(json.dumps({'formal_points':len(rows),'expected_points':70,'missing':missing,'all_present_validated_points_have_zero_connection_errors_and_get_misses':True,'full_configuration_and_cleanup_audit_passed':not args.partial,'checks':['recorded workload and affinity','binary versions in runtime INFO','throughput against counts and durations','latency text versus JSON','zero GET misses and connection errors','zero server failed/rejected command deltas in formal enclosing windows','one million keys throughout formal snapshots','exact counts and sampled value lengths before/after','clean server exits','SPDK device and hugepage/VFIO restoration']},indent=2)+'\n')
    for group in ['memory']:
        for kind in ['GET','SET']:
            for system in NAMES:
                selection=[r for r in rows if r['group']==group and r['workload']==kind and r['system']==system]
                if selection:
                    best=max(selection,key=lambda r:r['qps']); print(group,kind,system,best['server_threads'],best['connections'],round(best['qps']),best['p999_ms'])
    print('Validated formal points:',len(rows),'/ 70')
if __name__=='__main__':main()
