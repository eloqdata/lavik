#!/usr/bin/env python3
"""2-vCPU acquisition adapted from the committed v0.1.0 Redis/Valkey report.
One dedicated server runs products sequentially; the remote client uses 16 CPUs.
All observations are fresh and written beneath /opt/bench2c/results.
"""
import argparse, contextlib, hashlib, json, os, pathlib, re, resource, shlex
import signal, socket, subprocess as sp, sys, time, urllib.request
ROOT = pathlib.Path('/opt/bench2c')
C = json.loads((ROOT/'config.json').read_text())
R = ROOT/'results'
R.mkdir(exist_ok=True)
S = C['server_private']; CLIENT = 'azureuser@'+C['client_private']; PORT = 6379
REMOTE = '/opt/bench2c/client-results'
CLI = '/usr/bin/redis-cli'
LAVIK = ROOT/'downloads/lavik-v0.1.0-linux-x86_64/lavik'
KEYS = 1000000
CONNECTIONS = [16,32,80,160,320,640,1280]
INFO_SECTIONS = None
DBSIZE_TIMEOUT = 300
resource.setrlimit(resource.RLIMIT_NOFILE,(65535,65535))

def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj,indent=2)+'\n')
def event(message):
    print(time.strftime('%Y-%m-%d %H:%M:%S UTC',time.gmtime()),message,flush=True)
    write(R/'status.json',{'time':time.time(),'message':message,'pid':os.getpid()})
def run(args, **kw):
    return sp.run([str(x) for x in args],check=True,**kw)
def out(args, **kw):
    return sp.check_output([str(x) for x in args],text=True,**kw).strip()
def ssh(command, **kw):
    return run(['sudo','-u','azureuser','ssh','-i',str(ROOT/'client-control'),'-o','BatchMode=yes','-o','UserKnownHostsFile=/opt/bench2c/client-known-hosts',CLIENT,command],**kw)
def cli(*args):
    if args==('INFO','ALL') and INFO_SECTIONS:
        return '\n'.join(cli('INFO',section) for section in INFO_SECTIONS)
    timeout=DBSIZE_TIMEOUT if args and args[0]=='DBSIZE' else 120
    return out([CLI,'-h',S,'-p',PORT,'--raw',*args],timeout=timeout)
def snapshot(path, pid=None):
    d={'time':time.time(),'clock_ticks':os.sysconf('SC_CLK_TCK')}
    for f in ['stat','meminfo','diskstats','interrupts','net/dev','net/softnet_stat','softirqs']:
        d[f]=pathlib.Path('/proc/'+f).read_text()
    d['irq_affinity']={str(p):p.read_text().strip() for p in pathlib.Path('/proc/irq').glob('*/smp_affinity_list')}
    if pid:
        for f in ['stat','status','io']:
            with contextlib.suppress(FileNotFoundError): d['process_'+f]=pathlib.Path(f'/proc/{pid}/{f}').read_text()
        d['threads']={t.name:sorted(os.sched_getaffinity(int(t.name))) for t in pathlib.Path(f'/proc/{pid}/task').iterdir()}
        d['info']=cli('INFO','ALL')
        if pathlib.Path(f'/proc/{pid}/exe').resolve() == LAVIK:
            d['defrag_config']=verify_defrag()
            d['defrag_status']=cli('DEFRAG','STATUS')
    write(path,d)
def validate(keys):
    event('validating dataset: '+str(keys)+' keys, 1024-byte values')
    d={'dbsize':cli('DBSIZE'),'lengths':{str(k):cli('STRLEN',str(k)) for k in [1,2,keys//2,keys-1,keys]}}
    if d['dbsize'] != str(keys) or set(d['lengths'].values()) != {'1024'}:
        raise RuntimeError('Dataset validation failed: '+str(d))
    return d

def memtier(directory, name, keys, kind, connections=160, seconds=30, pid=None, threads=16):
    if (R/'STOP').exists():raise RuntimeError('Stop requested')
    directory.mkdir(parents=True,exist_ok=True)
    relative=directory.relative_to(R).as_posix()+'/'+name
    remote_path=REMOTE+'/'+relative
    args=['taskset','-c','0-15','memtier_benchmark','--server='+S,'--port='+str(PORT),'--protocol=redis',
          '--threads='+str(threads),'--clients='+str(connections//threads),'--pipeline=1', '--key-minimum=1','--key-maximum='+str(keys),
          '--key-prefix=','--data-size=1024','--hide-histogram','--print-percentiles=50,99,99.9,99.99','--show-config',
          '--json-out-file='+remote_path+'.json']
    if kind=='fill':
        assert keys%connections==0
        args+=['--requests='+str(keys//connections),'--ratio=1:0','--key-pattern=P:P']
    else:
        args+=['--test-time='+str(seconds),'--command='+('GET __key__' if kind=='GET' else 'SET __key__ __data__'),'--command-key-pattern=R']
        if kind=='GET': args+=['--command-is-read']
    # Independent random streams keep the large working set from collapsing
    # into the same repeated client sequence. Apply the policy to every curve.
    if kind!='fill':args+=['--distinct-client-seed']
    write(directory/(name+'.command.json'),args)
    if pid:snapshot(directory/(name+'.before.json'),pid)
    event(relative)
    t=time.time()
    command='mkdir -p '+shlex.quote(str(pathlib.PurePosixPath(remote_path).parent))+'; ulimit -n 65535; exec '+shlex.join(['python3','/opt/bench2c/client_wrapper.py',remote_path+'.client-cpu.json',*args])
    with (directory/(name+'.txt')).open('w') as f:
        ssh(command,stdout=f,stderr=sp.STDOUT,timeout=10800)
    with (directory/(name+'.json')).open('w') as f:
        ssh('cat '+shlex.quote(remote_path+'.json'),stdout=f)
    with (directory/(name+'.client-cpu.json')).open('w') as f:
        ssh('cat '+shlex.quote(remote_path+'.client-cpu.json'),stdout=f)
    write(directory/(name+'.timing.json'),{'start':t,'end':time.time()})
    stats=json.loads((directory/(name+'.json')).read_text())['ALL STATS']
    assert stats['Totals']['Connection Errors']==0, 'Connection errors'
    assert str(stats['Runtime']['Interrupted']).lower()=='false', 'Interrupted client'
    if kind=='fill':assert stats['Totals']['Count']==keys, 'Incomplete fill'
    body=(directory/(name+'.txt')).read_text().replace('\r','\n')
    totals=[line.split() for line in body.splitlines() if line.startswith('Totals')]
    if len(totals)!=1 or len(totals[0])!=10:raise RuntimeError('Bad totals: '+relative)
    if re.search(r'error response|connection error|error:|OOM command',body,re.I):raise RuntimeError('Client errors: '+relative)
    if kind=='GET':
        gets=[line.split() for line in body.splitlines() if line.startswith('Gets')]
        if len(gets)!=1 or float(gets[0][3])!=0 or abs(float(gets[0][1])-float(gets[0][2]))>.01:raise RuntimeError('GET misses: '+relative)
    if pid:snapshot(directory/(name+'.after.json'),pid)
    row=totals[0]
    result={'name':relative,'kind':kind,'keys':keys,'connections':connections,'qps':float(row[1]),
            'avg_ms':float(row[4]),'p50_ms':float(row[5]),'p99_ms':float(row[6]),'p999_ms':float(row[7]),'p9999_ms':float(row[8])}
    write(directory/(name+'.result.json'),result)
    event(relative+' done: '+str(round(result['qps']))+' QPS, p999='+str(result['p999_ms'])+' ms')
    return result

@contextlib.contextmanager
def server(args, directory, env=None, cwd=None):
    directory.mkdir(parents=True,exist_ok=True)
    try:
        with socket.create_connection((S,PORT),timeout=1):raise RuntimeError('Benchmark port already occupied')
    except ConnectionRefusedError:pass
    args=['taskset','-c','0-1',*[str(x) for x in args]]
    write(directory/'server-command.json',args)
    event('starting '+str(args[3]))
    with (directory/'server.log').open('a') as f:
        p=sp.Popen(args,stdout=f,stderr=sp.STDOUT,env=env,cwd=cwd)
    write(R/'active-server.json',{'pid':p.pid,'command':args,'directory':str(directory)})
    try:
        deadline=time.monotonic()+600
        while time.monotonic()<deadline:
            if p.poll() is not None:raise RuntimeError('Server exited: '+str(directory))
            try:
                if cli('PING')=='PONG':break
            except sp.SubprocessError:pass
            time.sleep(.5)
        else:raise TimeoutError('Server startup')
        yield p.pid
    finally:
        requested_stop = p.poll() is None
        if requested_stop:
            p.send_signal(signal.SIGTERM)
            try:p.wait(timeout=300)
            except sp.TimeoutExpired:
                raise RuntimeError('Server did not shut down cleanly; left running for inspection, no SIGKILL')
        (directory/'exit-code.txt').write_text(str(p.returncode)+'\n')
        (R/'active-server.json').unlink(missing_ok=True)
        if p.returncode != 0:raise RuntimeError('Server exit code '+str(p.returncode))

def verify_defrag():
    value=cli('CONFIG','GET','defrag-paused')
    assert value.splitlines()==['defrag-paused','no'],value
    return {'defrag-paused':value}

def sweep(directory,keys,seconds,pid,storage=False):
    for kind in ['GET','SET']:
        for connections in CONNECTIONS:
            verify_defrag()
            memtier(directory,f'{kind.lower()}-c{connections}',keys,kind,connections,seconds,pid)
    write(directory/'validation-after.json',validate(keys))

def memory(product):
    version=C['versions'][product]['version']
    binary=ROOT/'bin'/f'{product}-server'
    directory=R/'memory'/product; data=R/'data'/product; data.mkdir(parents=True,exist_ok=True)
    keys=KEYS
    def args(threads):
        return [binary,'--bind',S,'--protected-mode','no','--port',PORT,'--daemonize','no','--dir',data,'--dbfilename','dump.rdb',
                '--save','','--appendonly','no','--maxmemory','0','--maxclients','10000','--io-threads',threads]
    baseline=directory/'baseline.json'
    if (data/'dump.rdb').exists():
        assert baseline.exists(), 'Unverified existing RDB'
        assert json.loads(baseline.read_text())['sha256']==hashlib.sha256((data/'dump.rdb').read_bytes()).hexdigest(), 'Baseline changed'
    else:
        with server(args(2),directory/'load') as pid:
            memtier(directory,'fill',keys,'fill',pid=pid)
            write(directory/'validation-fill.json',validate(keys))
            assert cli('SAVE')=='OK'
        write(baseline,{'sha256':hashlib.sha256((data/'dump.rdb').read_bytes()).hexdigest()})
    for threads in [1,2]:
        dest=directory/f'io{threads}'
        if (dest/'validation-after.json').exists():continue
        with server(args(threads),dest) as pid:
            write(dest/'validation-before.json',validate(keys))
            assert cli('CONFIG','GET','io-threads').splitlines()==['io-threads',str(threads)]
            warm='warmup' if not (dest/'warmup.result.json').exists() else 'warmup-resume-'+str(int(time.time()))
            memtier(dest,warm,keys,'GET',seconds=10,pid=pid)
            for kind in ['GET','SET']:
                for c in CONNECTIONS:
                    name=f'{kind.lower()}-c{c}'
                    if not (dest/(name+'.result.json')).exists():memtier(dest,name,keys,kind,c,30,pid)
            write(dest/'validation-after.json',validate(keys))
    event('COMPLETE '+product)
