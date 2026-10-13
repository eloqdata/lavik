#!/usr/bin/env python3
"""Archive only benchmark evidence after completion; exclude keys and RDBs."""
import hashlib,json,os,pathlib,subprocess,tarfile,time
p=pathlib.Path('/opt/bench2c');r=p/'results'
assert (r/'COMPLETE').exists() and not (r/'active-server.json').exists()
for product in ['lavik','redis-server','valkey-server']:
 assert subprocess.run(['pgrep','-x',product],stdout=subprocess.DEVNULL).returncode==1,product
state=json.loads((r/'memory/lavik-spdk/host-state-after.json').read_text())
assert pathlib.Path('/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages').read_text().strip()==state['hugepages']
c=json.loads((p/'config.json').read_text())
assert (pathlib.Path('/sys/bus/pci/devices')/c['nvme']['bdf']/'driver').resolve().name=='nvme'
def capture(argv):
 x=subprocess.run(argv,text=True,capture_output=True)
 return {'argv':argv,'returncode':x.returncode,'stdout':x.stdout,'stderr':x.stderr}
d={'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'service':capture(['systemctl','show','bench2c','-p','ActiveState','-p','Result','-p','ExecMainStatus']),'transparent_hugepages':pathlib.Path('/sys/kernel/mm/transparent_hugepage/enabled').read_text().strip(),'overcommit_memory':pathlib.Path('/proc/sys/vm/overcommit_memory').read_text().strip(),'hugepages':state,'network_vf':capture(['ethtool','-i','ens1']),'network_counters':capture(['ethtool','-S','eth0']),'vf_counters':capture(['ethtool','-S','ens1'])}
(p/'post-run.json').write_text(json.dumps(d,indent=2)+'\n')
with (p/'server-inventory-after.json').open('w') as f:subprocess.run(['python3',str(p/'inventory.py')],stdout=f,check=True)
archive=p/'evidence.tar.gz'
allow=['runner.py','spdk_run.py','run_all.py','collect_evidence.py','inventory.py','prepare-server.sh','config.json','experiment-plan.json','suite.log','binary-sha256.txt','build-sha256.txt','server-inventory.json','server-inventory-after.json','host-policy-before.json','post-run.json']
with tarfile.open(archive,'w:gz') as t:
 for name in allow:
  assert (p/name).is_file(),name
  t.add(p/name,arcname=name)
 for f in sorted(r.rglob('*')):
  if 'data' in f.relative_to(r).parts:continue
  if f.is_file():t.add(f,arcname=str(f.relative_to(p)))
os.chmod(archive,0o644)
print(hashlib.sha256(archive.read_bytes()).hexdigest(),archive.stat().st_size)
