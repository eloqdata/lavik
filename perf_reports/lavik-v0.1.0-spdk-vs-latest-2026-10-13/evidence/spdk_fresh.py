#!/usr/bin/env python3
"""Fresh SPDK fills on the six serial-allowlisted disposable benchmark disks."""
import argparse, json, os, pathlib, resource, subprocess, time
import runner as b
P=pathlib.Path
SETUP='/mnt/dev/benchmark/pr318/main/bycorf/third_party/spdk/scripts/setup.sh'

def inventory():
    found=[]
    expected=set(b.SERIALS.values())
    for ctrl in P('/sys/class/nvme').glob('nvme*'):
        serial=(ctrl/'serial').read_text().strip()
        if serial not in expected: continue
        pci=(ctrl/'device').resolve()
        blocks=list(P('/sys/class/block').glob(ctrl.name+'n*'))
        assert len(blocks)==1,(ctrl,blocks)
        block=blocks[0]; dev='/dev/'+block.name
        assert not list(block.glob(block.name+'p*'))
        assert not list((block/'holders').iterdir())
        assert subprocess.run(['findmnt','-rn','-S',dev],stdout=subprocess.DEVNULL).returncode!=0
        deadline=time.monotonic()+60
        while subprocess.run(['fuser',dev],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
            if time.monotonic()>deadline: raise RuntimeError('Device busy: '+dev)
            time.sleep(.5)
        assert (block/'nsid').read_text().strip()=='1'
        assert (pci/'driver').resolve().name=='nvme'
        found.append({'serial':serial,'bdf':pci.name,'device':dev,'bytes':int((block/'size').read_text())*512})
    assert {d['serial'] for d in found}==expected
    assert len(found)==6 and all(d['bytes']==1919850381312 for d in found)
    return sorted(found,key=lambda d:d['serial'])

def setup(mode,devices,directory):
    env=dict(os.environ,PCI_ALLOWED=' '.join(d['bdf'] for d in devices),DRIVER_OVERRIDE='vfio-pci',HUGEMEM='8192',TARGET_USER='azureuser')
    with (directory/('driver-'+mode+'.log')).open('a') as f:
        subprocess.run([SETUP,mode],env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
    for d in devices:
        assert (P('/sys/bus/pci/devices')/d['bdf']/'driver').resolve().name==('vfio-pci' if mode=='config' else 'nvme')

def main(keys):
    resource.setrlimit(resource.RLIMIT_MEMLOCK,(resource.RLIM_INFINITY,resource.RLIM_INFINITY))
    directory=b.R/('memory' if keys==10**7 else 'storage')/'lavik-spdk'
    directory.mkdir(parents=True,exist_ok=False)
    assert not (b.R/'active-server.json').exists()
    devices=inventory();b.write(directory/'devices-before.json',devices)
    huge=P('/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages')
    subprocess.run(['modprobe','vfio-pci'],check=True)
    unsafe=P('/sys/module/vfio/parameters/enable_unsafe_noiommu_mode')
    saved={'hugepages':huge.read_text().strip(),'vfio_noiommu':unsafe.read_text().strip()}
    b.write(directory/'host-state-before.json',saved)
    b.event('sanitizing six verified disks before fresh SPDK '+str(keys)+'-key load')
    for d in devices:subprocess.run(['blkdiscard',d['device']],check=True)
    try:
        unsafe.write_text('1');setup('config',devices,directory)
        args=[b.LAVIK,'--network=kernel','--storage=spdk','--bind='+b.S,'--port=6379','--metrics-port=0','--threads=16','--pin-workers','--maxclients=10000','--busy-poll-us=20','--foreground-budget-us=1000','--background-budget-us=10','--background-warrant-percent=1','--tomb-raider-interval-ms=0','--spdk-max-completions-per-poll=16','--spdk-foreground-pre-poll-us=5','--defrag-max-active-per-device=8','--defrag-sleep-ms=0','--defrag-record-sleep-us=0','--log-dir='+str(directory/'logs')]+['--data-file=spdk://'+d['bdf']+'/1' for d in devices]
        if keys==10**9:args+=['--shutdown-checkpoint']
        env=dict(os.environ,BYCORF_EAL_ARGS=' '.join('-a '+d['bdf'] for d in devices),BYCORF_DPDK_MEMORY_MB='8192')
        b.write(directory/'eal-environment.json',{k:v for k,v in env.items() if k.startswith('BYCORF_')})
        with b.server(args,directory,env=env) as pid:
            assert b.cli('DBSIZE')=='0'
            b.write(directory/'defrag-runtime-config.json',b.verify_defrag())
            config={k:b.cli('CONFIG','GET',k) for k in ['spdk-max-completions-per-poll','spdk-foreground-pre-poll-us']}
            assert config['spdk-max-completions-per-poll'].splitlines()==['spdk-max-completions-per-poll','16']
            b.write(directory/'runtime-config.json',config)
            b.memtier(directory,'fill',keys,'fill',640,pid=pid)
            b.write(directory/'validation-before.json',b.validate(keys))
            if keys==10**7:b.memtier(directory,'warmup',keys,'GET',seconds=10,pid=pid)
            b.sweep(directory,keys,60 if keys==10**9 else 30,pid,keys==10**9)
    finally:
        if not (b.R/'active-server.json').exists():
            setup('reset',devices,directory)
            b.write(directory/'devices-after.json',inventory())
            huge.write_text(saved['hugepages']);unsafe.write_text('1' if saved['vfio_noiommu']=='Y' else '0')
            b.write(directory/'host-state-after.json',{'hugepages':huge.read_text().strip(),'vfio_noiommu':unsafe.read_text().strip()})
    (directory/'COMPLETE').write_text(str(time.time())+'\n')
    b.event('COMPLETE fresh SPDK '+str(keys)+' keys')
if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('keys',type=int,choices=[10**7,10**9]);args=ap.parse_args();main(args.keys)
