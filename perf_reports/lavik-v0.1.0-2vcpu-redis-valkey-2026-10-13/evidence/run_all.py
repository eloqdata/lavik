#!/usr/bin/env python3
import os,pathlib,time
import runner as b
import spdk_run
assert os.geteuid()==0
assert os.cpu_count()==2
assert (b.ROOT/'PREPARED').exists()
assert not (b.R/'EXPERIMENT.json').exists(), 'One-shot run: inspect any partial evidence before resuming'
for name in ['lavik','redis-server','valkey-server']:
 assert b.sp.run(['pgrep','-x',name],stdout=b.sp.DEVNULL).returncode==1,name
b.write(b.R/'EXPERIMENT.json',b.C)
b.snapshot(b.R/'host-before.json')
spdk_run.main()
b.memory('redis')
b.memory('valkey')
b.snapshot(b.R/'host-after.json')
(b.R/'COMPLETE').write_text(str(time.time())+'\n')
b.event('COMPLETE: 70 fresh GET/SET points on 2 vCPUs')
