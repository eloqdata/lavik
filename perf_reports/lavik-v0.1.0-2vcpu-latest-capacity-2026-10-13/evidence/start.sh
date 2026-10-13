#!/usr/bin/env bash
set -euo pipefail
cd /opt/bench2c
test -f PREPARED
test ! -e results/EXPERIMENT.json
python3 - <<'PY'
import os,pathlib,json,subprocess
p=pathlib.Path('/opt/bench2c');assert os.cpu_count()==2
for unit in ['bench-bootstrap','bench-prepare']:
 assert subprocess.check_output(['systemctl','show',unit,'-p','Result','--value'],text=True).strip()=='success'
 assert subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip()=='inactive'
thp=pathlib.Path('/sys/kernel/mm/transparent_hugepage/enabled');over=pathlib.Path('/proc/sys/vm/overcommit_memory')
(p/'host-policy-before.json').write_text(json.dumps({'transparent_hugepages':thp.read_text().strip(),'vm.overcommit_memory':over.read_text().strip()},indent=2)+'\n')
thp.write_text('never');over.write_text('1')
c=json.loads((p/'config.json').read_text());c['status']='formal-run';c['lavik_backend']='SPDK + kernel TCP'
(p/'config.json').write_text(json.dumps(c,indent=2)+'\n')
PY
systemd-run --unit=bench2c --property=Type=oneshot --property=LimitNOFILE=65535 --property=LimitMEMLOCK=infinity /bin/bash -c 'cd /opt/bench2c; python3 run_all.py > suite.log 2>&1'
