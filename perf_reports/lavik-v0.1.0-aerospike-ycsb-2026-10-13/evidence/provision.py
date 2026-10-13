#!/usr/bin/env python3
"""Provision only the six explicitly authorized scratch controllers after GET/SET."""
import json,pathlib,subprocess as sp,time,os
R=pathlib.Path(__file__).resolve().parent; ROOT=R.parent
assert os.geteuid()==0
assert (ROOT/'memory/COMPLETE').exists()
assert not (R/'provision-complete.json').exists()
def run(a,**kw):return sp.run([str(x) for x in a],check=True,**kw)
def out(a):return sp.check_output([str(x) for x in a],text=True).strip()
def save(name,value):(R/name).write_text(json.dumps(value,indent=2)+'\n')
for name in ['lavik','asd','redis-server','valkey-server']:
 assert sp.run(['pgrep','-x',name],stdout=sp.DEVNULL).returncode==1,name
inv=json.loads(pathlib.Path('/mnt/dev/provisioning/bypass-hardware-reboot20261011.json').read_text())
assert {x['serial'] for x in inv['controllers']}=={f'4d4fd73b0687ec77000{i}' for i in range(1,7)}
assert out(['uname','-r'])=='6.17.0-1022-azure'
assert not pathlib.Path('/dev/md/release010').exists()
devices=[]
for x in inv['controllers']:
 matches=[p for p in pathlib.Path('/sys/class/nvme').glob('nvme*') if (p/'serial').read_text().strip()==x['serial']]
 assert len(matches)==1
 ctrl=matches[0]
 assert (ctrl/'device').resolve().name==x['pci']
 assert (ctrl/'device/driver').resolve().name=='nvme'
 blocks=list(pathlib.Path('/sys/block').glob(ctrl.name+'n*'));assert len(blocks)==1
 block=blocks[0];dev='/dev/'+block.name
 assert int((block/'size').read_text())*512==1919850381312
 assert (block/'nsid').read_text().strip()=='1'
 assert not list(block.glob(block.name+'p*')) and not list((block/'holders').iterdir())
 assert sp.run(['findmnt','-rn','-S',dev],stdout=sp.DEVNULL).returncode!=0
 assert sp.run(['fuser',dev],stdout=sp.DEVNULL,stderr=sp.DEVNULL).returncode!=0
 devices.append({**x,'device':dev})
save('devices-before.json',devices)
for x in devices:run(['blkdiscard',x['device']])
run(['mdadm','--create','/dev/md/release010','--metadata=1.2','--level=0','--raid-devices=6','--chunk=512','--run',*[x['device'] for x in devices]])
run(['udevadm','settle'])
md=str(pathlib.Path('/dev/md/release010').resolve());assert md.startswith('/dev/md')
size=2147450880;start=49152
run(['sgdisk','-o','-a','49152','-n',f'1:{start}:{start+size-1}','-c','1:release010-aerospike','-n',f'2:{start+size}:{start+2*size-1}','-c','2:release010-lavik',md])
run(['blockdev','--rereadpt',md]);run(['udevadm','settle'])
for number,name in [(1,'aerospike'),(2,'lavik')]:
 device=f'{md}p{number}'
 assert int(out(['blockdev','--getsize64',device]))==1099494850560
 (R/(name+'-device')).symlink_to(device)
 t=time.time()
 with (R/(name+'-zero.log')).open('w') as log:
  run(['dd','if=/dev/zero','of='+device,'bs=16M','count=65535','oflag=direct','conv=fdatasync','status=progress'],stdout=log,stderr=log)
 with open(device,'rb',buffering=0) as f:
  for offset in [0,1099494850560//2,1099494850560-2**20]:
   f.seek(offset);assert f.read(2**20)==bytes(2**20)
 save(name+'-zero.json',{'device':device,'bytes':1099494850560,'seconds':time.time()-t})
save('storage-after.json',{'partition_table':json.loads(out(['sfdisk','--json',md])),'mdadm':out(['mdadm','--detail',md]),'devices':devices})
pathlib.Path('/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages').write_text('0')
for p in ['/opt/aerospike/smd','/var/log/aerospike','/var/run/aerospike']:pathlib.Path(p).mkdir(parents=True,exist_ok=True)
save('provision-complete.json',{'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'disks':devices,'storage':'six-disk RAID0, 512 KiB chunk; two raw 1099494850560-byte partitions, zeroed in full','authorization':'User explicitly allowed resetting these scratch disks on 2026-10-13.'})
