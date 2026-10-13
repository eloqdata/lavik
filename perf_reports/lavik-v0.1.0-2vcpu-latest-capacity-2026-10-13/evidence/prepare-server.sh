#!/usr/bin/env bash
set -euo pipefail
cd /opt/bench2c/downloads
curl -fL --retry 3 https://github.com/eloqdata/lavik/releases/download/v0.1.0/lavik-v0.1.0-linux-x86_64.tar.gz -o lavik-v0.1.0-linux-x86_64.tar.gz
printf '%s\n' '06d95f2d969cffa39fbfca85f25246c4d9166ebc9577b164f2b022306c9f6fab  lavik-v0.1.0-linux-x86_64.tar.gz' | sha256sum -c -
tar -xzf lavik-v0.1.0-linux-x86_64.tar.gz
curl -fL --retry 3 https://github.com/spdk/spdk/archive/d519b163cbc0e2f28c35d9bc86d610da368b032c.tar.gz -o spdk-helper.tar.gz
mkdir -p spdk
tar -xzf spdk-helper.tar.gz --strip-components=1 -C spdk
cd /opt/bench2c
python3 inventory.py > server-inventory.json
python3 - <<'PY'
import pathlib,json,subprocess
p=pathlib.Path('/opt/bench2c');c=json.loads((p/'experiment-plan.json').read_text())
c.update(server_private='10.93.0.7',client_private='10.93.0.8',region='eastus',zone='3',server_size='Standard_L2aos_v5',client_size='Standard_F16as_v7',status='prepared')
found=[]
for ctrl in pathlib.Path('/sys/class/nvme').glob('nvme*'):
 if (ctrl/'serial').read_text().strip()!='4fd80e9625aca4470001':continue
 assert 'Direct Disk' in (ctrl/'model').read_text()
 blocks=list(pathlib.Path('/sys/class/block').glob(ctrl.name+'n*'));assert len(blocks)==1
 block=blocks[0];size=int((block/'size').read_text())*512
 assert 1400000000000<size<1500000000000
 assert not list(block.glob(block.name+'p*'))
 found.append(dict(serial=(ctrl/'serial').read_text().strip(),bdf=(ctrl/'device').resolve().name,bytes=size,nsid=int((block/'nsid').read_text()),device='/dev/'+block.name))
assert len(found)==1,found
c['nvme']=found[0];(p/'config.json').write_text(json.dumps(c,indent=2))
PY
for attempt in $(seq 1 180); do
 if sudo -u azureuser ssh -i /opt/bench2c/client-control -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/opt/bench2c/client-known-hosts azureuser@10.93.0.8 'test -f /opt/bench2c/BUILD_READY'; then break; fi
 sleep 10
done
sudo -u azureuser ssh -i /opt/bench2c/client-control -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/opt/bench2c/client-known-hosts azureuser@10.93.0.8 'hostname; test -f /opt/bench2c/BUILD_READY'
mkdir -p /opt/bench2c/bin
chown azureuser:azureuser /opt/bench2c/bin
for product in redis valkey; do
 sudo -u azureuser scp -i /opt/bench2c/client-control -o BatchMode=yes -o UserKnownHostsFile=/opt/bench2c/client-known-hosts azureuser@10.93.0.8:/opt/bench2c/downloads/$product/src/$product-server /opt/bench2c/bin/
done
sudo -u azureuser scp -i /opt/bench2c/client-control -o BatchMode=yes -o UserKnownHostsFile=/opt/bench2c/client-known-hosts azureuser@10.93.0.8:/opt/bench2c/downloads/build-sha256.txt /opt/bench2c/
/opt/bench2c/downloads/lavik-v0.1.0-linux-x86_64/lavik --version
/opt/bench2c/bin/redis-server --version
/opt/bench2c/bin/valkey-server --version
sha256sum /opt/bench2c/downloads/lavik-v0.1.0-linux-x86_64/lavik /opt/bench2c/bin/* > /opt/bench2c/binary-sha256.txt
touch /opt/bench2c/PREPARED
