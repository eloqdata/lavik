#!/usr/bin/env python3
"""Validate every formal point against its recorded command and client JSON."""
import argparse,csv,hashlib,json,math,pathlib,re
REPORT=pathlib.Path(__file__).resolve().parent
R=REPORT/'evidence'
CONNS=[80,160,320,640,1280]
NAMES={'lavik':'Lavik defrag enabled / io_uring','lavik-spdk':'Lavik defrag enabled / SPDK','redis':'Redis 8.10.2','valkey':'Valkey 9.1.2','dragonfly':'Dragonfly v1.40.2','garnet':'Garnet v2.1.5'}

def expectations():
    for system,threads in [('lavik-spdk',16)]+[(s,n) for s in ['redis','valkey'] for n in [1,2,4,8,16]]:
        parent=R/'memory'/system
        if system not in ['lavik','lavik-spdk']:parent/=f'io{threads}'
        for kind in ['GET','SET']:
            for c in CONNS:yield parent/f'{kind.lower()}-c{c}', 'memory',system,threads,kind,c,10**7,30
    for kind in ['GET','SET']:
        for c in CONNS:yield R/'storage/lavik-spdk'/f'{kind.lower()}-c{c}','storage','lavik-spdk',16,kind,c,10**9,60

def load(partial=False):
    rows=[];missing=[]
    for stem,group,system,threads,kind,c,keys,seconds in expectations():
        result=stem.with_suffix('.result.json')
        if not result.exists():missing.append(str(stem.relative_to(R)));continue
        row=json.loads(result.read_text());args=json.loads(stem.with_suffix('.command.json').read_text())
        if group=='storage' and '--distinct-client-seed' not in args and partial:
            missing.append(str(stem.relative_to(R))+' (superseded correlated streams)');continue
        stats=json.loads(stem.with_suffix('.json').read_text())['ALL STATS']
        required=['--threads=16','--clients='+str(c//16),'--pipeline=1','--key-minimum=1','--key-maximum='+str(keys),'--key-prefix=','--test-time='+str(seconds),'--command-key-pattern=R','--data-size=1024']
        if group=='storage':required+=['--distinct-client-seed']
        assert all(a in args for a in required),(stem,args)
        assert args[:3]==['taskset','-c','0-15'] and not any('rate-limit' in a for a in args)
        assert stats['Totals']['Connection Errors']==0,stem
        assert str(stats['Runtime']['Interrupted']).lower()=='false',stem
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
        role={'lavik':'workers','lavik-spdk':'workers','redis':'io-threads','valkey':'io-threads','dragonfly':'proactors','garnet':'minimum thread-pool threads'}[system]
        r={'group':group,'product':NAMES[system],'system':system,'server_threads':threads,'thread_role':role,'workload':kind,'connections':c,'keys':keys,'value_bytes':1024,'test_seconds':seconds,'client_seed_mode':'distinct' if '--distinct-client-seed' in args else 'default-correlated',
           **{k:row[k] for k in ['qps','avg_ms','p50_ms','p99_ms','p999_ms','p9999_ms']},
           'requests':total['Count'],'client_cores':stats['CPU']['cpu_cores_used'],'server_cores_enclosing_window':round(cpu,4),
           'host_steal_percent_enclosing_window':round(steal,4),
           'connection_errors':total['Connection Errors'],'source':str(stem.relative_to(R))+'.json'}
        rows.append(r)
    if not partial:
        assert not missing,missing
        assert len(rows)==120
        assert (R/'COMPLETE').is_file()
        builds=json.loads((R/'builds.json').read_text())
        assert builds['builds_finished_before_formal_tests']
        experiment=json.loads((REPORT/'report-scope.json').read_text())
        assert builds['utc'] < experiment['utc']
        for root,keys in [(R/'memory/lavik-spdk',10**7),(R/'storage/lavik-spdk',10**9),(R/'memory/redis',10**7),(R/'memory/valkey',10**7)]:
            fill=json.loads((root/'fill.json').read_text())['ALL STATS']
            assert fill['Totals']['Count']==keys and fill['Totals']['Connection Errors']==0
            assert str(fill['Runtime']['Interrupted']).lower()=='false'
        for path in (R/'memory').glob('**/warmup.json'):
            warmup=json.loads(path.read_text())['ALL STATS']
            assert warmup['Totals']['Connection Errors']==0 and warmup['Gets']['Misses/sec']==0
            assert str(warmup['Runtime']['Interrupted']).lower()=='false'

        for product,version,commit in [('redis','8.10.2','498ecd0d6d007db11ddb3aea9428552598a78622'),('valkey','9.1.2','7f1dffedff6de73058b2c2a389422b6ecd56c8fb')]:
            identity=builds['products'][product]
            assert identity['version']==version and identity['commit']==commit
            assert version in (R/f'{product}-version.txt').read_text()
            assert (R/f'{product}-binary.sha256').read_text().split()[0]==identity['sha256']
        for group in ['memory','storage']:
            root=R/group/'lavik-spdk'
            command=json.loads((root/'server-command.json').read_text())
            for flag in ['--storage=spdk','--threads=16','--pin-workers','--defrag-max-active-per-device=8','--defrag-sleep-ms=0','--defrag-record-sleep-us=0']:
                assert flag in command,(group,flag)
            assert len([flag for flag in command if flag.startswith('--data-file=spdk://')])==6
            if group=='storage':assert '--shutdown-checkpoint' in command
            assert (root/'exit-code.txt').read_text().strip()=='0'
            assert json.loads((root/'host-state-before.json').read_text())==json.loads((root/'host-state-after.json').read_text())
        for product in ['redis','valkey']:
            for threads in [1,2,4,8,16]:
                root=R/'memory'/product/f'io{threads}'
                command=json.loads((root/'server-command.json').read_text())
                assert command[3]==f'/mnt/dev/benchmark/release010-latest/downloads/{product}/src/{product}-server'
                assert command[command.index('--io-threads')+1]==str(threads)
                assert (root/'exit-code.txt').read_text().strip()=='0'
                assert command[command.index('--save')+1]==''
                assert command[command.index('--appendonly')+1]=='no'
                for phase in ['before','after']:
                    persistence=json.loads((root/f'persistence-{phase}.json').read_text())
                    assert persistence['effective_config']=={'save':'','appendonly':'no'}
                    assert persistence['automatic_rdb_disabled'] and persistence['aof_disabled']


        for system in ['lavik-spdk','redis','valkey']:
            parent=R/'memory'/system
            checks=[parent] if system=='lavik-spdk' else [parent/f'io{n}' for n in [1,2,4,8,16]]
            for path in checks:
                for phase in ['before','after']:
                    v=json.loads((path/f'validation-{phase}.json').read_text())
                    assert v['dbsize']=='10000000' and set(v['lengths'].values())=={'1024'}
        for phase in ['before','after']:
            v=json.loads((R/'storage/lavik-spdk'/f'validation-{phase}.json').read_text())
            assert v['dbsize']=='1000000000' and set(v['lengths'].values())=={'1024'}
        for path in list((R/'memory/lavik-spdk').glob('*.before.json'))+list((R/'storage/lavik-spdk').glob('*.before.json')):
            d=json.loads(path.read_text())
            assert d['defrag_config']['defrag-paused'].splitlines()==['defrag-paused','no']
        for path in list((R/'memory/lavik-spdk').glob('*.after.json'))+list((R/'storage/lavik-spdk').glob('*.after.json')):
            d=json.loads(path.read_text())
            assert d['defrag_config']['defrag-paused'].splitlines()==['defrag-paused','no']
    return rows,missing

def main():
    p=argparse.ArgumentParser();p.add_argument('--partial',action='store_true');args=p.parse_args()
    rows,missing=load(args.partial)
    if rows:
        with (REPORT/'results.csv').open('w') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');writer.writeheader();writer.writerows(rows)
    (REPORT/'validation.json').write_text(json.dumps({'formal_points':len(rows),'expected_points':120,'missing':missing,'all_present_validated_points_have_zero_connection_errors_and_get_misses':True},indent=2)+'\n')
    for group in ['memory','storage']:
        for kind in ['GET','SET']:
            for system in NAMES:
                selection=[r for r in rows if r['group']==group and r['workload']==kind and r['system']==system]
                if selection:
                    best=max(selection,key=lambda r:r['qps']); print(group,kind,system,best['server_threads'],best['connections'],round(best['qps']),best['p999_ms'])
    print('Validated formal points:',len(rows),'/ 120')
if __name__=='__main__':main()
