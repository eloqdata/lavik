#!/usr/bin/env python3
"""Use only the inventoried, new local NVMe namespace on this dedicated VM.

The OS disk is excluded by its PCI controller and namespace inventory. Explicit
serial/BDF/size matching is required again immediately before destructive setup.
"""
import json,os,pathlib,resource,subprocess as sp,time
import runner as b
P=pathlib.Path
SETUP='/opt/bench2c/downloads/spdk/scripts/setup.sh'

def inventory():
    approved=b.C['nvme']
    found=[]
    for ctrl in P('/sys/class/nvme').glob('nvme*'):
        if (ctrl/'serial').read_text().strip()!=approved['serial']:continue
        pci=(ctrl/'device').resolve()
        blocks=list(P('/sys/class/block').glob(ctrl.name+'n*'))
        assert len(blocks)==1,blocks
        block=blocks[0];dev='/dev/'+block.name
        assert pci.name==approved['bdf'],pci
        assert not list(block.glob(block.name+'p*')), 'partitioned disk'
        assert not list((block/'holders').iterdir()), 'disk has holders'
        assert sp.run(['findmnt','-rn','-S',dev],stdout=sp.DEVNULL).returncode!=0, 'mounted disk'
        assert sp.run(['fuser',dev],stdout=sp.DEVNULL,stderr=sp.DEVNULL).returncode!=0, 'disk in use'
        assert dev not in b.out(['swapon','--noheadings','--show=NAME']).splitlines(), 'swap device'
        assert (block/'nsid').read_text().strip()==str(approved['nsid'])
        assert (pci/'driver').resolve().name=='nvme'
        size=int((block/'size').read_text())*512
        assert size==approved['bytes'],size
        found.append({'serial':approved['serial'],'bdf':pci.name,'device':dev,'bytes':size,'nsid':approved['nsid']})
    assert len(found)==1,found
    return found

def setup(mode,devices,directory):
    env=dict(os.environ,PCI_ALLOWED=' '.join(d['bdf'] for d in devices),DRIVER_OVERRIDE='vfio-pci',HUGEMEM='2048',TARGET_USER='azureuser')
    with (directory/('driver-'+mode+'.log')).open('a') as f:
        sp.run([SETUP,mode],env=env,stdout=f,stderr=sp.STDOUT,check=True)
    for d in devices:
        assert (P('/sys/bus/pci/devices')/d['bdf']/'driver').resolve().name==('vfio-pci' if mode=='config' else 'nvme')

def main(keys, label):
    resource.setrlimit(resource.RLIMIT_MEMLOCK,(resource.RLIM_INFINITY,resource.RLIM_INFINITY))
    directory=b.R/'memory'/label;directory.mkdir(parents=True,exist_ok=False)
    assert not (b.R/'active-server.json').exists()
    devices=inventory();b.write(directory/'devices-before.json',devices)
    huge=P('/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages')
    sp.run(['modprobe','vfio-pci'],check=True)
    unsafe=P('/sys/module/vfio/parameters/enable_unsafe_noiommu_mode')
    saved={'hugepages':huge.read_text().strip(),'vfio_noiommu':unsafe.read_text().strip()}
    b.write(directory/'host-state-before.json',saved)
    b.event('clearing the verified disposable local NVMe namespace')
    for d in devices:sp.run(['blkdiscard',d['device']],check=True)
    try:
        if not list(P('/sys/kernel/iommu_groups').glob('*')):unsafe.write_text('1')
        setup('config',devices,directory)
        args=[b.LAVIK,'--network=kernel','--storage=spdk','--bind='+b.S,'--port=6379','--metrics-port=0','--threads=2','--pin-workers','--maxclients=10000','--busy-poll-us=20','--foreground-budget-us=1000','--background-budget-us=10','--background-warrant-percent=1','--tomb-raider-interval-ms=0','--spdk-max-completions-per-poll=16','--spdk-foreground-pre-poll-us=5','--defrag-max-active-per-device=8','--defrag-sleep-ms=0','--defrag-record-sleep-us=0','--log-dir='+str(directory/'logs')]+['--data-file=spdk://'+d['bdf']+'/'+str(d['nsid']) for d in devices]
        env=dict(os.environ,BYCORF_EAL_ARGS=' '.join('-a '+d['bdf'] for d in devices),BYCORF_DPDK_MEMORY_MB='2048')
        b.write(directory/'eal-environment.json',{k:v for k,v in env.items() if k.startswith('BYCORF_')})
        with b.server(args,directory,env=env) as pid:
            assert b.cli('DBSIZE')=='0'
            b.write(directory/'defrag-runtime-config.json',b.verify_defrag())
            assert b.cli('CONFIG','GET','spdk-max-completions-per-poll').splitlines()==['spdk-max-completions-per-poll','16']
            b.memtier(directory,'fill',keys,'fill',160,pid=pid)
            b.write(directory/'validation-before.json',b.validate(keys))
            b.memtier(directory,'warmup',keys,'GET',seconds=10,pid=pid)
            b.sweep(directory,keys,30,pid)
    finally:
        if not (b.R/'active-server.json').exists():
            setup('reset',devices,directory)
            b.write(directory/'devices-after.json',inventory())
            huge.write_text(saved['hugepages']);unsafe.write_text('1' if saved['vfio_noiommu']=='Y' else '0')
            b.write(directory/'host-state-after.json',{'hugepages':huge.read_text().strip(),'vfio_noiommu':unsafe.read_text().strip()})
    (directory/'COMPLETE').write_text(str(time.time())+'\n')
    b.event('COMPLETE fresh SPDK '+str(keys)+' keys')
if __name__=='__main__':
    raise SystemExit('Use run_all.py; each dataset requires a fresh verified scratch namespace')
