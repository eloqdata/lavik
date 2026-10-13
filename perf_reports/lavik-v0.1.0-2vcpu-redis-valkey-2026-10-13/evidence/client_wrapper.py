#!/usr/bin/env python3
"""Record whole-client CPU deltas around each memtier invocation."""
import json,pathlib,resource,subprocess,sys,time
output=pathlib.Path(sys.argv[1]);argv=sys.argv[2:]
before={'time':time.time(),'stat':pathlib.Path('/proc/stat').read_text(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'netdev':pathlib.Path('/proc/net/dev').read_text()}
p=subprocess.run(argv)
after={'time':time.time(),'stat':pathlib.Path('/proc/stat').read_text(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'netdev':pathlib.Path('/proc/net/dev').read_text()}
r=resource.getrusage(resource.RUSAGE_CHILDREN)
output.write_text(json.dumps({'argv':argv,'returncode':p.returncode,'before':before,'after':after,'cpu_user_seconds':r.ru_utime,'cpu_system_seconds':r.ru_stime,'maxrss_kib':r.ru_maxrss},indent=2)+'\n')
sys.exit(p.returncode)
