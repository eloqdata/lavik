#!/usr/bin/env python3
"""Fresh 10M comparisons with current releases, plus the 1B Lavik SPDK matrix."""
import json,os,pathlib,subprocess as sp,time,hashlib,traceback
import runner as b
import spdk_fresh as f
ROOT=b.R.parent
assert os.geteuid()==0
assert (ROOT/'PREPARED').exists()
assert (ROOT/'YCSB_ARCHIVED').exists()
assert not (b.R/'EXPERIMENT.json').exists(), 'One-shot batch; inspect partial results before any resume'
assert b.out(['uname','-r'])=='6.17.0-1022-azure'
assert hashlib.file_digest(b.LAVIK.open('rb'),'sha256').hexdigest()=='7bf8d204a137678cee0e1a621b02661181373fbbfe1366bc1c201942b9dcf420'
b.write(b.R/'EXPERIMENT.json',{'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'release':'v0.1.0','revision':'feb61224e414c5653fd09560e20c9d214db1e3ac','products':['lavik-spdk','redis-8.10.2','valkey-9.1.2'],'groups':{'memory':{'keys':10**7,'value_bytes':1024,'connections':b.CONNECTIONS,'seconds':30,'points':110},'storage':{'keys':10**9,'value_bytes':1024,'connections':b.CONNECTIONS+[2560],'seconds':60,'points':12,'distinct_client_seed':True}},'order':['lavik-memory','lavik-storage','redis','valkey'],'defrag':'enabled throughout; 8 active/device; no sleeps','server_cpus':'0-15','client_cpus':'0-15','source_report':'lavik-v0.1.0-beta.1-spdk-vs-peers-2026-09-18','all_results_fresh':True})
try:
 sp.run(['python3',str(b.R/'retire-ycsb-storage.py')],check=True)
 b.snapshot(b.R/'host-before.json')
 f.main(10**7)
 f.main(10**9)
 b.memory('redis')
 b.memory('valkey')
 b.snapshot(b.R/'host-after.json')
 (b.R/'COMPLETE').write_text(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+'\n')
 b.event('COMPLETE 122 fresh formal GET/SET points: 110 memory, 12 storage')
except BaseException:
 b.write(b.R/'failure.json',{'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'traceback':traceback.format_exc()})
 raise
