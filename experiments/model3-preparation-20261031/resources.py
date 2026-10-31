"""Unmodified resource helpers from collector experiment commit 37c6. No source or AI calls."""
import os
from pathlib import Path
import signal
import subprocess
import time

def proc_stats():
    found = {}
    for item in Path("/proc").iterdir():
        if not item.name.isdigit():
            continue
        try:
            data = (item / "stat").read_text()
            fields = data[data.rindex(")") + 2:].split()
            found[int(item.name)] = {
                "state": fields[0],
                "ppid": int(fields[1]), "group": int(fields[2]),
                "ticks": int(fields[11]) + int(fields[12]),
                "start": int(fields[19]), "rss": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
            }
        except (OSError, ValueError, IndexError):
            pass
    return found


def process_members(root, all_stats):
    members = {p for p, v in all_stats.items() if p == root or v["group"] == root}
    while True:
        more = {p for p, v in all_stats.items() if v["ppid"] in members}
        if more <= members:
            return members
        members |= more


def cgroup_snapshot():
    # Resolve the process's actual cgroup, not an unrelated ancestor by guess.
    paths = [Path("/sys/fs/cgroup")]
    try:
        relative = next(l.split(":", 2)[2] for l in Path("/proc/self/cgroup").read_text().splitlines()
                        if l.startswith("0::"))
        nested = Path("/sys/fs/cgroup") / relative.lstrip("/")
        if (nested / "cpu.stat").is_file():
            paths.insert(0, nested)
    except (OSError, StopIteration):
        pass
    root = next((p for p in paths if (p / "cpu.stat").is_file()), None)
    if root is None:
        return {"available": False}
    result = {"available": True}
    for name in ("memory.current", "memory.max", "memory.peak", "cpu.max"):
        try:
            value = (root / name).read_text().strip()
            result[name] = int(value) if value.isdigit() else value
        except OSError:
            result[name] = None
    for name in ("cpu.stat", "memory.events"):
        try:
            result[name] = {k: int(v) for k, v in (line.split() for line in (root / name).read_text().splitlines())}
        except (OSError, ValueError):
            result[name] = None
    return result


def clean_group(process):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
    process.wait(timeout=3)


def clean_tracked_children(job):
    # Chromium descendants can have their own process group. Only kill PIDs
    # actually observed under this job and still carrying the same start time.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        current = proc_stats()
        for pid, start in job["seen_processes"]:
            if (pid != os.getpid() and pid in current and current[pid]["start"] == start
                    and current[pid]["state"] != "Z"):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass
        time.sleep(0.1)

