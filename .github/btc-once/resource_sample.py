"""One-shot cgroup and container interface counters for the temporary serial pilot.
Rows include wall clock, CPU microseconds, resident cgroup memory, network RX/TX and block reads/writes.
Missing or reset counters are reported as unavailable. No requests are intercepted.
"""
from bisect import bisect_left
from pathlib import Path
from time import monotonic

def read():
    cpu=mem=rx=tx=None
    root=Path('/sys/fs/cgroup')
    try:
        cpu=int(dict(x.split() for x in (root/'cpu.stat').read_text().splitlines())['usage_usec'])
        mem=int((root/'memory.current').read_text())
    except (OSError,ValueError,KeyError):
        pass
    try:
        pairs=[]
        for line in Path('/proc/net/dev').read_text().splitlines():
            if ':' not in line: continue
            name,raw=line.split(':',1)
            if name.strip()!='lo':
                data=raw.split()
                pairs.append((int(data[0]),int(data[8])))
        if pairs: rx,tx=map(sum,zip(*pairs))
    except (OSError,ValueError,IndexError):
        pass
    disk_read=disk_written=None
    try:
        pairs=[]
        for line in (root/'io.stat').read_text().splitlines():
            fields=dict(part.split('=',1) for part in line.split()[1:])
            if 'rbytes' in fields and 'wbytes' in fields:
                pairs.append((int(fields['rbytes']),int(fields['wbytes'])))
        if pairs:
            disk_read,disk_written=map(sum,zip(*pairs))
    except (OSError,ValueError):
        pass
    return (monotonic(),cpu,mem,rx,tx,disk_read,disk_written)

def window(samples,begin,end):
    """Linearly allocate interval totals at boundaries, show native peak only for sampled intervals."""
    elapsed=coverage=cpu=area=mem_time=rx=tx=net_time=0
    mem_peak=cpu_peak=None
    disk_read=disk_write=disk_time=0
    count=0
    for a,b in zip(samples,samples[1:]):
        t1,t2=a[0],b[0]; dt=t2-t1
        lo,hi=max(t1,begin),min(t2,end)
        if dt<=0 or hi<=lo:continue
        d=hi-lo;w=d/dt;count+=1;elapsed+=d
        if None not in (a[1],b[1]) and b[1]>=a[1]:
            c=(b[1]-a[1])/1e6
            cpu+=c*w;coverage+=d
            if dt>=.10 and lo==t1 and hi==t2:cpu_peak=max(cpu_peak or 0,c*100/dt)
        if None not in (a[2],b[2]):
            rlo=a[2]+(b[2]-a[2])*(lo-t1)/dt;rhi=a[2]+(b[2]-a[2])*(hi-t1)/dt
            area+=(rlo+rhi)/2*d;mem_time+=d
            for t,m in ((t1,a[2]),(t2,b[2])):
                if begin<=t<=end:mem_peak=max(mem_peak or 0,m)
        if None not in (a[3],a[4],b[3],b[4]) and b[3]>=a[3] and b[4]>=a[4]:
            rx+=(b[3]-a[3])*w;tx+=(b[4]-a[4])*w;net_time+=d
        if None not in (a[5],a[6],b[5],b[6]) and b[5]>=a[5] and b[6]>=a[6]:
            disk_read+=(b[5]-a[5])*w;disk_write+=(b[6]-a[6])*w;disk_time+=d
    return dict(wall_seconds=round(end-begin,3),covered_seconds=round(elapsed,3),sample_intervals=count,
                cgroup_block_read_bytes=round(disk_read) if disk_time else None,
                cgroup_block_write_bytes=round(disk_write) if disk_time else None,
                cpu_seconds=round(cpu,3) if coverage else None,
                average_cpu_percent_one_core=round(100*cpu/coverage,1) if coverage else None,
                max_sampled_cpu_percent_one_core=round(cpu_peak,1) if cpu_peak is not None else None,
                average_memory_bytes=round(area/mem_time) if mem_time else None,max_sampled_memory_bytes=mem_peak,
                rx_bytes=round(rx) if net_time else None,tx_bytes=round(tx) if net_time else None)

class ResourceSample:
    def __init__(self):
        self.samples=[read()]
    def update(self,force=False):
        if force or monotonic()-self.samples[-1][0]>=.45:
            row=read()
            if row[0]>self.samples[-1][0]:self.samples.append(row)
    def report(self):
        self.update(force=True)
        first,last=self.samples[0],self.samples[-1]
        return dict(window(self.samples,first[0],last[0]),available=first[1] is not None,
                    scope='QA service cgroup and non-loopback network namespace',
                    sample_count=len(self.samples),start_memory_bytes=first[2],end_memory_bytes=last[2],
                    samples=self.samples)
