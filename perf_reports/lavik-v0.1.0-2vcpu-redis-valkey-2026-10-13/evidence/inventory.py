import json,pathlib,subprocess,time
out={'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
for k,a in {'cpu':['lscpu','-J'],'topology':['lscpu','-e=CPU,CORE,SOCKET,NODE,ONLINE'],'kernel':['uname','-a'],'memory':['free','-b'],'disks':['lsblk','-b','-J','-o','NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,SERIAL'],'pci':['lspci','-nnk'],'network':['ip','-j','addr'],'routes':['ip','-j','route'],'nic':['ethtool','-i','eth0'],'interrupts':['cat','/proc/interrupts'],'limits':['sysctl','net.core.somaxconn','vm.overcommit_memory','vm.swappiness']}.items():
 p=subprocess.run(a,capture_output=True,text=True);out[k]={'argv':a,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
print(json.dumps(out,indent=2))
