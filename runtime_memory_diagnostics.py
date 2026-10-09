"""Bounded, content-free, best-effort Linux memory samples. No background work.

The time budget is checked between filesystem operations; it is not a hard
deadline for a blocked kernel read. RSS totals may count shared pages twice.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid

from runtime_allocation_diagnostics import collect_allocation_stats

PREFIX = "[MEMORY_DIAGNOSTIC]"
MAX_PROC_ENTRIES = 512
MAX_FILE_BYTES = 8192
MAX_TOTAL_BYTES = 262144
MAX_SAMPLE_SECONDS = 0.05
MAX_INTEGER = (1 << 63) - 1
PHASES = frozenset({
    "runtime_start", "runtime_poll", "watch_start", "watch_end",
    "scoring_start", "scoring_end", "archive_start", "archive_end",
    "collector_start", "playwright_ready", "browser_launched", "context_created",
    "page_created", "page_ready", "page_close", "page_close_error",
    "context_close", "context_close_error", "browser_close", "browser_close_error",
    "playwright_exit", "collector_end",
})
COUNTERS = frozenset({
    "active_collectors", "collections_started", "collections_completed",
    "collections_failed", "pages_created", "pages_closed", "page_close_errors",
    "browser_close_errors", "driver_close_errors", "timeframe_index", "attempt",
    "cache_symbols", "sampled_symbols", "history_rows", "windows", "price_samples",
    "oi_samples", "incomplete", "active_watch",
})
_identity_pid = None
_identity_uuid = None
_identity_time = None


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _identity(pid):
    global _identity_pid, _identity_uuid, _identity_time
    if _identity_pid != pid:
        _identity_uuid, _identity_time = str(uuid.uuid4()), _utc_now()
        _identity_pid = pid
    return {"boot_uuid": _identity_uuid, "pid": pid,
            "diagnostics_initialized_at_utc": _identity_time}


def _integer(value):
    if type(value) is not int or not 0 <= value <= MAX_INTEGER:
        raise ValueError()
    return value


def _number(text):
    if not text or len(text) > 19 or not text.isascii() or not text.isdecimal():
        raise ValueError()
    return _integer(int(text))


class _Reader:
    def __init__(self):
        self.deadline = time.monotonic() + MAX_SAMPLE_SECONDS
        self.bytes_read = 0
        self.reasons = set()

    def available(self):
        if time.monotonic() >= self.deadline:
            self.reasons.add("time_budget")
            return False
        if self.bytes_read >= MAX_TOTAL_BYTES:
            self.reasons.add("byte_budget")
            return False
        return True

    def read(self, path, label):
        if not self.available():
            return None
        limit = min(MAX_FILE_BYTES, MAX_TOTAL_BYTES - self.bytes_read)
        try:
            with open(path, "rb") as handle:
                raw = handle.read(limit + 1)
            self.bytes_read += len(raw)
            if len(raw) > limit:
                self.reasons.add(label + "_truncated")
                return None
            return raw.decode("utf-8")
        except Exception:
            self.reasons.add(label + "_unavailable")
            return None


def _stat(raw):
    opening, closing = raw.index("("), raw.rindex(")")
    fields = raw[closing + 1:].split()
    name = raw[opening + 1:closing].lower()
    category = ("python" if name.startswith("python") else
                "node" if name == "node" or name.startswith("nodejs") else
                "chromium" if name.startswith(("chrome", "chromium", "headless_shell"))
                else "other")
    return {"pid": _number(raw[:opening].strip()), "ppid": _number(fields[1]),
            "start_ticks": _number(fields[19]), "rss_pages": _number(fields[21]),
            "category": category}


def _selected_values(raw, keys):
    if raw is None:
        return None
    result = {}
    for line in raw.splitlines():
        parts = line.split()
        if parts and parts[0] in keys:
            if len(parts) != 2 or parts[0] in result:
                raise ValueError()
            result[parts[0]] = _number(parts[1])
    if not keys.issubset(result):
        raise ValueError()
    return result


def _cgroup(reader):
    result = {"version": 2, "current_bytes": None, "max_bytes": None,
              "max_unlimited": None, "stat": None, "events": None}
    raw = reader.read("/proc/self/cgroup", "cgroup_membership")
    if raw is None:
        return result
    try:
        members = [line[3:] for line in raw.splitlines() if line.startswith("0::")]
        if len(members) != 1 or not members[0].startswith("/"):
            raise ValueError()
        relative = Path(members[0])
        if ".." in relative.parts:
            raise ValueError()
        # Standard unified mount only. Never fall back to an ancestor's memory
        # total if membership cannot be resolved at this mount.
        folder = Path("/sys/fs/cgroup") / str(relative).lstrip("/")
    except Exception:
        reader.reasons.add("cgroup_membership_malformed")
        return result
    for file, key in (("memory.current", "current_bytes"), ("memory.max", "max_bytes")):
        raw = reader.read(folder / file, "cgroup_" + key)
        if raw is None:
            continue
        try:
            if key == "max_bytes" and raw.strip() == "max":
                result["max_unlimited"] = True
            else:
                result[key] = _number(raw.strip())
                if key == "max_bytes":
                    result["max_unlimited"] = False
        except Exception:
            reader.reasons.add("cgroup_" + key + "_malformed")
    for file, key, required in (
        ("memory.stat", "stat", {"anon", "file"}),
        ("memory.events", "events", {"low", "high", "max", "oom", "oom_kill"}),
    ):
        raw = reader.read(folder / file, "cgroup_" + key)
        try:
            result[key] = _selected_values(raw, required)
        except Exception:
            reader.reasons.add("cgroup_" + key + "_malformed")
    return result


def _tree(reader, pid, own, page_size):
    records = {pid: own} if own else {}
    scanned = 0
    try:
        with os.scandir("/proc") as entries:
            for entry in entries:
                if scanned >= MAX_PROC_ENTRIES:
                    reader.reasons.add("proc_entry_budget")
                    break
                scanned += 1
                if not reader.available():
                    break
                if not entry.name.isascii() or not entry.name.isdecimal():
                    continue
                if own and entry.name == str(pid):
                    continue
                raw = reader.read(Path("/proc") / entry.name / "stat", "proc_stat")
                if raw is None:
                    continue
                try:
                    value = _stat(raw)
                    if value["pid"] != _number(entry.name):
                        raise ValueError()
                    records[value["pid"]] = value
                except Exception:
                    reader.reasons.add("proc_stat_malformed")
    except Exception:
        reader.reasons.add("proc_scan_unavailable")
    children = defaultdict(list)
    for child_pid, record in records.items():
        if child_pid != pid:
            children[record["ppid"]].append(child_pid)
    groups = {name: {"processes": 0, "rss_bytes": 0 if page_size else None}
              for name in ("python", "node", "chromium", "other")}
    visible = {name: {"processes": 0, "rss_bytes": 0 if page_size else None}
               for name in groups}
    for record in records.values():
        group = visible[record["category"]]
        group["processes"] += 1
        if page_size:
            group["rss_bytes"] += record["rss_pages"] * page_size
    pending, visited = ([pid] if own else []), set()
    while pending:
        item = pending.pop()
        if item in visited:
            continue
        visited.add(item)
        if item in records:
            record = records[item]
            group = groups[record["category"]]
            group["processes"] += 1
            if page_size:
                group["rss_bytes"] += record["rss_pages"] * page_size
        pending.extend(children[item])
    return {"includes_main_process": bool(own), "attribution_available": bool(own),
            "root_proc_pid": pid if own else None, "categories": groups if own else None,
            "processes": sum(group["processes"] for group in groups.values()) if own else None,
            "rss_bytes": sum(group["rss_bytes"] for group in groups.values()) if own and page_size else None,
            "visible_categories": visible,
            "visible_processes": sum(group["processes"] for group in visible.values()),
            "visible_rss_bytes": sum(group["rss_bytes"] for group in visible.values()) if page_size else None,
            "proc_entries_examined": scanned, "rss_may_double_count_shared_pages": True}


def _sample(phase, pages, counters):
    reader = _Reader()
    pid = os.getpid()
    result = {"version": "runtime-memory-v1", "observed_at_utc": _utc_now(),
              **_identity(pid), "phase": phase if type(phase) is str and phase in PHASES else "invalid_phase"}
    if result["phase"] == "invalid_phase":
        reader.reasons.add("invalid_phase")
    if pages is not None:
        try:
            result["pages"] = _integer(pages)
        except Exception:
            reader.reasons.add("invalid_pages")
    result["counters"] = {}
    if counters is not None:
        if type(counters) is not dict or len(counters) > len(COUNTERS):
            reader.reasons.add("invalid_counters")
        else:
            for key, value in counters.items():
                try:
                    if type(key) is not str or key not in COUNTERS:
                        raise ValueError()
                    result["counters"][key] = _integer(value)
                except Exception:
                    reader.reasons.add("invalid_counters")
    own = None
    raw = reader.read("/proc/self/stat", "main_stat")
    if raw is not None:
        try:
            own = _stat(raw)
        except Exception:
            own = None
            reader.reasons.add("main_stat_malformed")
    try:
        page_size = _integer(os.sysconf("SC_PAGE_SIZE"))
        if not page_size:
            raise ValueError()
    except Exception:
        page_size = None
        reader.reasons.add("page_size_unavailable")
    result["process_start_ticks"] = own["start_ticks"] if own else None
    result["proc_pid"] = own["pid"] if own else None
    result["pid_namespace_differs"] = own["pid"] != pid if own else None
    result["main_rss_bytes"] = own["rss_pages"] * page_size if own and page_size else None
    result["cgroup"] = _cgroup(reader)
    result["process_tree"] = _tree(reader, own["pid"] if own else pid, own, page_size)
    result["partial_reasons"] = sorted(reader.reasons)
    result["partial"] = bool(reader.reasons)
    result["bytes_read"] = reader.bytes_read
    # Allocation availability is separate from the filesystem/process sample.
    # A missing optional allocator must not turn valid process visibility into
    # an unknown value, or suppress the already collected measurements.
    try:
        result["allocation"] = collect_allocation_stats(
            result["phase"], deadline=reader.deadline)
    except Exception:
        result["allocation"] = {
            "version": 1, "implementation": None,
            "python_allocated_blocks": None, "gc": None,
            "active_threads": None, "native_allocator": None,
            "partial": True, "reasons": ["allocation_sample_failed"],
        }
    return result


def emit_memory_sample(phase, *, pages=None, counters=None):
    """Emit one sanitized sample; collection/output failures never propagate."""
    try:
        result = _sample(phase, pages, counters)
    except Exception:
        result = {"version": "runtime-memory-v1", "phase": "sample_failed",
                  "partial": True, "partial_reasons": ["sample_failed"]}
    try:
        print(PREFIX + " " + json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    except Exception:
        pass
