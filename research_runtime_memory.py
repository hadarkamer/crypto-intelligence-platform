"""Bounded, passive Linux process-memory observations using only the standard library.

Importing starts no I/O, threads or database work. Samples persist aggregates,
never process names, command lines, environments or per-process identities.
The elapsed budget is cooperative between bounded reads, not an I/O deadline.
Kernel RSS counters are approximate, and observations across processes are not
atomic. Summed RSS may count shared pages repeatedly and is not PSS or ownership.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time

VERSION = 'runtime-memory-sample-v1'
TOP_SCOPE = 'OBSERVED_SELF_AND_DESCENDANT_RSS_NOT_PSS_OR_CONTAINER_OWNERSHIP'
MAX_OUTPUT_BYTES = 8192
MAX_TOTAL_READ_BYTES = 2 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024
MAX_PARENT_DEPTH = 64
ERROR_CODES = frozenset({'PROCFS_UNAVAILABLE', 'PERMISSION_DENIED', 'PROCESS_DISAPPEARED', 'READ_ERROR',
    'READ_LIMIT', 'PARSE_ERROR', 'TIME_BUDGET', 'PROCESS_CAP', 'DIRECTORY_CAP', 'PID_REUSED', 'PARENT_CHANGED',
    'ANCESTOR_UNVERIFIED', 'PARENT_TREE_CYCLE', 'PARENT_DEPTH_LIMIT', 'CGROUP_UNAVAILABLE',
    'CGROUP_SCOPE_UNRESOLVED', 'SELF_UNAVAILABLE', 'INVALID_CONFIGURATION', 'SAMPLER_ERROR', 'OTHER_ERROR'})
TOP_KEYS = frozenset({'version', 'status', 'platform', 'self', 'descendants', 'cgroup', 'coverage',
                      'limits', 'errors', 'elapsed_ms', 'scope'})
COVERAGE_KEYS = frozenset({'directory_entries_examined', 'processes_examined', 'identities_revalidated',
    'matched_descendants', 'skipped_processes', 'scan_complete', 'budget_exhausted', 'process_cap_reached',
    'identity_changed', 'read_calls', 'bytes_read'})
LIMIT_KEYS = frozenset({'max_processes', 'max_directory_entries', 'max_file_bytes', 'max_total_read_bytes',
                        'budget_ms', 'page_size_bytes'})
_BROWSER_NAMES = frozenset({'chrome', 'chromium', 'chromium-browse', 'headless_shell', 'playwright',
                           'firefox', 'webkitwebproces', 'webkitnetworkpr'})
_RSS = re.compile(r'^VmRSS:\s*([0-9]+)\s+kB\s*$')


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _integer(value):
    return type(value) is int and 0 <= value < 2 ** 64


def validate_sample(value):
    """Pure closed-schema check usable by a separate bounded journal."""
    try:
        if type(value) is not dict or set(value) != TOP_KEYS: return False
        if value['version'] != VERSION or value['scope'] != TOP_SCOPE or value['platform'] != 'LINUX_PROCFS': return False
        if value['status'] not in {'COMPLETE', 'PARTIAL', 'UNAVAILABLE'} or not _number(value['elapsed_ms']): return False
        own = value['self']
        if type(own) is not dict or set(own) != {'status', 'rss_bytes', 'process_count', 'rss_source'}: return False
        if own['status'] == 'OBSERVED':
            if not _integer(own['rss_bytes']) or own['process_count'] != 1 or type(own['process_count']) is not int: return False
            if own['rss_source'] not in {'PROC_STATUS_VMRSS', 'PROC_STAT_RSS_PAGES'}: return False
        elif own['status'] in {'UNAVAILABLE', 'IDENTITY_CHANGED'}:
            if own['rss_bytes'] is not None or own['process_count'] != 0 or type(own['process_count']) is not int or own['rss_source'] is not None: return False
        else: return False
        descendants = value['descendants']
        if type(descendants) is not dict or set(descendants) != {'status', 'browser_named', 'other'}: return False
        if descendants['status'] not in {'OBSERVED', 'PARTIAL', 'UNAVAILABLE'}: return False
        for name in ('browser_named', 'other'):
            group = descendants[name]
            if type(group) is not dict or set(group) != {'rss_bytes', 'process_count'} or not _integer(group['process_count']): return False
            if descendants['status'] == 'UNAVAILABLE':
                if group['rss_bytes'] is not None or group['process_count'] != 0: return False
            elif not _integer(group['rss_bytes']): return False
        cgroup = value['cgroup']
        if type(cgroup) is not dict or set(cgroup) != {'status', 'current_bytes', 'max_bytes', 'max_unlimited', 'scope'}: return False
        if cgroup['status'] == 'OBSERVED':
            if cgroup['scope'] != 'SELF_CGROUP_V2' or not _integer(cgroup['current_bytes']) or type(cgroup['max_unlimited']) is not bool: return False
            if cgroup['max_unlimited']:
                if cgroup['max_bytes'] is not None: return False
            elif not _integer(cgroup['max_bytes']): return False
        elif cgroup['status'] == 'UNAVAILABLE':
            if cgroup['scope'] != 'UNAVAILABLE' or any(cgroup[key] is not None for key in ('current_bytes', 'max_bytes', 'max_unlimited')): return False
        else: return False
        coverage, limits, errors = value['coverage'], value['limits'], value['errors']
        if type(coverage) is not dict or set(coverage) != COVERAGE_KEYS or type(limits) is not dict or set(limits) != LIMIT_KEYS: return False
        for key, item in coverage.items():
            if key in {'scan_complete', 'budget_exhausted', 'process_cap_reached'}:
                if type(item) is not bool: return False
            elif not _integer(item): return False
        if not all(_integer(item) for key, item in limits.items() if key != 'budget_ms') or not _number(limits['budget_ms']): return False
        if not 1 <= limits['max_processes'] <= 512 or not 1 <= limits['budget_ms'] <= 100: return False
        if limits['max_directory_entries'] != limits['max_processes'] * 4 + 128 or limits['max_file_bytes'] != MAX_FILE_BYTES or limits['max_total_read_bytes'] != MAX_TOTAL_READ_BYTES: return False
        page_size = limits['page_size_bytes']
        if not 512 <= page_size <= 1024 * 1024 or page_size & (page_size - 1): return False
        if coverage['processes_examined'] > limits['max_processes'] or coverage['directory_entries_examined'] > limits['max_directory_entries']: return False
        if coverage['read_calls'] > limits['max_processes'] * 2 + 16 or coverage['bytes_read'] > MAX_TOTAL_READ_BYTES: return False
        if any(coverage[key] > limits['max_processes'] + 1 for key in ('identities_revalidated', 'matched_descendants', 'skipped_processes', 'identity_changed')): return False
        if sum(descendants[name]['process_count'] for name in ('browser_named', 'other')) > coverage['matched_descendants']: return False
        if type(errors) is not dict or len(errors) > 16 or not set(errors) <= ERROR_CODES or not all(_integer(item) and item > 0 for item in errors.values()): return False
        if value['status'] == 'COMPLETE' and (own['status'] != 'OBSERVED' or descendants['status'] != 'OBSERVED' or cgroup['status'] != 'OBSERVED' or errors or not coverage['scan_complete'] or coverage['budget_exhausted'] or coverage['process_cap_reached']): return False
        if value['status'] == 'PARTIAL' and (own['status'] != 'OBSERVED' or descendants['status'] != 'PARTIAL'): return False
        if value['status'] == 'UNAVAILABLE' and own['status'] == 'OBSERVED': return False
        return len(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')) <= MAX_OUTPUT_BYTES
    except BaseException:
        return False


def _empty(max_processes=256, budget_ms=40, page_size=4096):
    return {'version': VERSION, 'status': 'UNAVAILABLE', 'platform': 'LINUX_PROCFS', 'scope': TOP_SCOPE,
        'self': {'status': 'UNAVAILABLE', 'rss_bytes': None, 'process_count': 0, 'rss_source': None},
        'descendants': {'status': 'UNAVAILABLE', 'browser_named': {'rss_bytes': None, 'process_count': 0},
                        'other': {'rss_bytes': None, 'process_count': 0}},
        'cgroup': {'status': 'UNAVAILABLE', 'current_bytes': None, 'max_bytes': None, 'max_unlimited': None, 'scope': 'UNAVAILABLE'},
        'coverage': {key: False if key in {'scan_complete', 'budget_exhausted', 'process_cap_reached'} else 0 for key in COVERAGE_KEYS},
        'limits': {'max_processes': max_processes, 'max_directory_entries': max_processes * 4 + 128,
                   'max_file_bytes': MAX_FILE_BYTES, 'max_total_read_bytes': MAX_TOTAL_READ_BYTES,
                   'budget_ms': budget_ms, 'page_size_bytes': page_size}, 'errors': {}, 'elapsed_ms': 0.}


class _OSReader:
    def read(self, path, limit):
        with open(path, 'rb', buffering=0) as handle:
            return handle.read(limit)

    def iter_pids(self, root):
        with os.scandir(root) as entries:
            for entry in entries:
                yield entry.name


class _Context:
    def __init__(self, result, reader, clock):
        self.result, self.reader, self.clock = result, reader, clock
        self.start = self.now()
        self.skipped, self.changed = set(), set()

    def now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value): raise ValueError('clock')
        return value

    def error(self, code):
        errors = self.result['errors']
        if code not in ERROR_CODES: code = 'OTHER_ERROR'
        if code not in errors and len(errors) >= 15: code = 'OTHER_ERROR'
        errors[code] = errors.get(code, 0) + 1

    def time_available(self):
        now = self.now()
        if now < self.start: raise ValueError('nonmonotonic clock')
        if (now - self.start) * 1000 >= self.result['limits']['budget_ms']:
            self.result['coverage']['budget_exhausted'] = True
            self.error('TIME_BUDGET'); return False
        return True

    def read(self, path, limit, missing='PROCESS_DISAPPEARED'):
        if not self.time_available(): return None
        coverage = self.result['coverage']
        if coverage['read_calls'] >= self.result['limits']['max_processes'] * 2 + 16:
            self.error('READ_LIMIT'); return None
        remaining = MAX_TOTAL_READ_BYTES - coverage['bytes_read']
        if remaining <= 0:
            self.error('READ_LIMIT'); return None
        limit = min(limit, remaining); coverage['read_calls'] += 1
        try:
            raw = self.reader.read(path, limit)
            if type(raw) is not bytes: raise ValueError('reader type')
            if len(raw) > limit: raise ValueError('reader ignored bound')
            coverage['bytes_read'] += len(raw)
            if len(raw) == limit:
                self.error('READ_LIMIT'); return None
            return raw
        except FileNotFoundError:
            self.error(missing)
        except PermissionError:
            self.error('PERMISSION_DENIED')
        except Exception:
            self.error('READ_ERROR')
        return None


def _stat(raw, expected_pid):
    if raw is None: return None
    text = raw.decode('utf-8', errors='replace').strip()
    left, right = text.find('('), text.rfind(')')
    if left < 1 or right <= left or int(text[:left].strip()) != expected_pid: raise ValueError('stat framing')
    tail = text[right + 1:].split()
    if len(tail) < 22 or len(tail[0]) != 1: raise ValueError('stat fields')
    parent, start, rss = int(tail[1]), int(tail[19]), int(tail[21])
    if parent < 0 or start < 0 or rss < 0: raise ValueError('stat values')
    return {'pid': expected_pid, 'ppid': parent, 'start': start, 'rss_pages': rss, 'comm': text[left + 1:right]}


def _read_stat(context, proc_root, pid):
    raw = context.read(proc_root / str(pid) / 'stat', 4096)
    try: return _stat(raw, pid)
    except Exception:
        context.error('PARSE_ERROR'); return None


def _vmrss(raw):
    if raw is None: return None
    lines = [line for line in raw.decode('ascii').splitlines() if line.startswith('VmRSS:')]
    if len(lines) != 1: raise ValueError('VmRSS field')
    match = _RSS.fullmatch(lines[0])
    if match is None: raise ValueError('VmRSS unit')
    value = int(match.group(1)) * 1024
    if not _integer(value): raise ValueError('VmRSS bound')
    return value


def _browser_named(comm):
    name = comm.lower()
    return name in _BROWSER_NAMES or name.startswith(('chrome_', 'chromium-', 'playwright-'))


def _ancestry(pid, anchor, records, context):
    path, seen = [], set()
    while pid != anchor:
        if pid in seen:
            context.error('PARENT_TREE_CYCLE'); return None
        if len(path) >= MAX_PARENT_DEPTH:
            context.error('PARENT_DEPTH_LIMIT'); return None
        if pid not in records or pid <= 0: return None
        seen.add(pid); path.append(pid); pid = records[pid]['ppid']
    return path


def _unescape_mount(value):
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match.group(1), 8)), value)


def _cgroup(context, proc_root, cgroup_root, anchor):
    raw = context.read(proc_root / str(anchor) / 'cgroup', 4096, 'CGROUP_UNAVAILABLE')
    if raw is None: return
    try:
        paths = [line.split(':', 2)[2] for line in raw.decode('utf-8').splitlines() if line.startswith('0::')]
        if len(paths) != 1: raise ValueError('v2 group')
        group = PurePosixPath(paths[0])
        if not group.is_absolute() or '..' in group.parts or '\x00' in paths[0]: raise ValueError('group path')
        if str(group) == '/': directory = cgroup_root
        else:
            mounts = context.read(proc_root / str(anchor) / 'mountinfo', MAX_FILE_BYTES, 'CGROUP_UNAVAILABLE')
            if mounts is None: return
            candidates = []
            for line in mounts.decode('utf-8').splitlines():
                if ' - cgroup2 ' not in line: continue
                fields = line.split(' - ', 1)[0].split()
                if len(fields) < 5: continue
                mount_root = PurePosixPath(_unescape_mount(fields[3]))
                mount_point = PurePosixPath(_unescape_mount(fields[4]))
                if '..' in mount_root.parts or mount_point.as_posix() != cgroup_root.as_posix(): continue
                if group.is_relative_to(mount_root): candidates.append(cgroup_root.joinpath(*group.relative_to(mount_root).parts))
            if len(candidates) != 1:
                context.error('CGROUP_SCOPE_UNRESOLVED'); return
            directory = candidates[0]
        current = context.read(directory / 'memory.current', 128, 'CGROUP_UNAVAILABLE')
        maximum = context.read(directory / 'memory.max', 128, 'CGROUP_UNAVAILABLE')
        if current is None or maximum is None: return
        current_text, max_text = current.decode('ascii').strip(), maximum.decode('ascii').strip()
        if not current_text.isdecimal() or not (max_text == 'max' or max_text.isdecimal()): raise ValueError('cgroup values')
        current_bytes = int(current_text); max_bytes = None if max_text == 'max' else int(max_text)
        if not _integer(current_bytes) or (max_bytes is not None and not _integer(max_bytes)): raise ValueError('cgroup bounds')
        context.result['cgroup'] = {'status': 'OBSERVED', 'current_bytes': current_bytes, 'max_bytes': max_bytes,
            'max_unlimited': max_text == 'max', 'scope': 'SELF_CGROUP_V2'}
    except Exception:
        context.error('PARSE_ERROR')


def _sample(proc_root, cgroup_root, pid, max_processes, budget_ms, reader, clock, page_size_bytes):
    if type(max_processes) is not int or not 1 <= max_processes <= 512 or type(budget_ms) not in (int, float) or not math.isfinite(budget_ms) or not 1 <= budget_ms <= 100:
        result = _empty(); result['errors']['INVALID_CONFIGURATION'] = 1; return result
    if reader is None and not sys.platform.startswith('linux'):
        result = _empty(max_processes, budget_ms); result['errors']['PROCFS_UNAVAILABLE'] = 1; return result
    reader = reader if reader is not None else _OSReader()
    clock = clock if clock is not None else time.monotonic
    pid = os.getpid() if pid is None else pid
    if type(pid) is not int or pid <= 0: raise ValueError('anchor pid')
    page_size = os.sysconf('SC_PAGE_SIZE') if page_size_bytes is None else page_size_bytes
    if type(page_size) is not int or not 512 <= page_size <= 1024 * 1024 or page_size & (page_size - 1): raise ValueError('page size')
    result = _empty(max_processes, budget_ms, page_size); context = _Context(result, reader, clock)
    proc_root, cgroup_root = Path(proc_root), Path(cgroup_root)
    first = _read_stat(context, proc_root, pid)
    if first is None:
        context.error('SELF_UNAVAILABLE'); _cgroup(context, proc_root, cgroup_root, pid)
        result['elapsed_ms'] = round(max(0., (context.now() - context.start) * 1000), 3); return result
    status_raw = context.read(proc_root / str(pid) / 'status', MAX_FILE_BYTES)
    try: rss = _vmrss(status_raw)
    except Exception:
        context.error('PARSE_ERROR'); rss = None
    rss_source = 'PROC_STATUS_VMRSS' if rss is not None else 'PROC_STAT_RSS_PAGES'
    # Retain useful self/cgroup observations before spending time enumerating
    # descendants. A later scan timeout must not erase this early measurement.
    second = _read_stat(context, proc_root, pid)
    if second is None:
        context.error('SELF_UNAVAILABLE')
    elif second['start'] != first['start'] or second['ppid'] != first['ppid']:
        result['self']['status'] = 'IDENTITY_CHANGED'; context.changed.add(pid)
        context.error('PID_REUSED' if second['start'] != first['start'] else 'PARENT_CHANGED')
    else:
        own_rss = rss if rss is not None else second['rss_pages'] * page_size
        if not _integer(own_rss): raise ValueError('self RSS')
        result['self'] = {'status': 'OBSERVED', 'rss_bytes': own_rss, 'process_count': 1, 'rss_source': rss_source}
        result['coverage']['identities_revalidated'] = 1
    _cgroup(context, proc_root, cgroup_root, pid)
    if result['self']['status'] != 'OBSERVED':
        result['coverage']['identity_changed'] = len(context.changed)
        result['elapsed_ms'] = round(max(0., (context.now() - context.start) * 1000), 3)
        return result
    records = {pid: first}; iterator = None
    try:
        iterator = iter(reader.iter_pids(proc_root))
        for name in iterator:
            if not context.time_available(): break
            if result['coverage']['directory_entries_examined'] >= result['limits']['max_directory_entries']:
                context.error('DIRECTORY_CAP'); break
            result['coverage']['directory_entries_examined'] += 1
            if type(name) is not str or not name.isdecimal() or len(name) > 10 or int(name) <= 0: continue
            process_pid = int(name)
            if result['coverage']['processes_examined'] >= max_processes:
                result['coverage']['process_cap_reached'] = True; context.error('PROCESS_CAP'); break
            result['coverage']['processes_examined'] += 1
            if process_pid == pid: continue
            item = _read_stat(context, proc_root, process_pid)
            if item is None: context.skipped.add(process_pid)
            else: records[process_pid] = item
        else: result['coverage']['scan_complete'] = True
    except FileNotFoundError: context.error('PROCFS_UNAVAILABLE')
    except PermissionError: context.error('PERMISSION_DENIED')
    except Exception: context.error('READ_ERROR')
    finally:
        if iterator is not None and callable(getattr(iterator, 'close', None)): iterator.close()
    paths = {}
    for process_pid in records:
        if process_pid == pid: continue
        if not context.time_available(): break
        ancestry = _ancestry(process_pid, pid, records, context)
        if ancestry is not None: paths[process_pid] = ancestry
    result['coverage']['matched_descendants'] = len(paths)
    # A fresh anchor check protects the later descendant attribution. Its
    # absence is a descendant gap; the already validated self read remains.
    second = _read_stat(context, proc_root, pid)
    valid = {}
    if second is None:
        context.error('ANCESTOR_UNVERIFIED')
        context.skipped.update(paths)
        result['descendants'] = {'status': 'PARTIAL', 'browser_named': {'rss_bytes': 0, 'process_count': 0},
                                'other': {'rss_bytes': 0, 'process_count': 0}}
    elif second['start'] != first['start'] or second['ppid'] != first['ppid']:
        result['self'] = {'status': 'IDENTITY_CHANGED', 'rss_bytes': None, 'process_count': 0, 'rss_source': None}
        context.changed.add(pid); context.error('PID_REUSED' if second['start'] != first['start'] else 'PARENT_CHANGED')
        context.skipped.update(paths)
    else:
        valid[pid] = second
        for process_pid in sorted(paths):
            if not context.time_available(): break
            observed = _read_stat(context, proc_root, process_pid)
            if observed is None:
                context.skipped.add(process_pid); continue
            original = records[process_pid]
            if observed['start'] != original['start'] or observed['ppid'] != original['ppid']:
                context.changed.add(process_pid); context.skipped.add(process_pid)
                context.error('PID_REUSED' if observed['start'] != original['start'] else 'PARENT_CHANGED'); continue
            valid[process_pid] = observed; result['coverage']['identities_revalidated'] += 1
        groups = {'browser_named': {'rss_bytes': 0, 'process_count': 0}, 'other': {'rss_bytes': 0, 'process_count': 0}}
        for process_pid, ancestry in paths.items():
            if any(parent not in valid for parent in ancestry):
                context.skipped.add(process_pid); context.error('ANCESTOR_UNVERIFIED'); continue
            item = valid[process_pid]; group = groups['browser_named' if _browser_named(item['comm']) else 'other']
            group['rss_bytes'] += item['rss_pages'] * page_size; group['process_count'] += 1
        result['descendants'] = {'status': 'OBSERVED', **groups}
    result['coverage']['skipped_processes'] = len(context.skipped)
    result['coverage']['identity_changed'] = len(context.changed)
    if result['self']['status'] == 'OBSERVED':
        result['status'] = 'PARTIAL' if result['errors'] or not result['coverage']['scan_complete'] or result['cgroup']['status'] != 'OBSERVED' else 'COMPLETE'
        if result['status'] == 'PARTIAL': result['descendants']['status'] = 'PARTIAL'
    result['elapsed_ms'] = round(max(0., (context.now() - context.start) * 1000), 3)
    return result


def sample_memory(*, proc_root='/proc', cgroup_root='/sys/fs/cgroup', pid=None, max_processes=256,
                  budget_ms=40, reader=None, clock=None, page_size_bytes=None):
    """Return bounded aggregates or explicit unavailability; diagnostic failures fail open.

    ``reader``/``clock``/``pid``/``page_size_bytes`` support pure fixtures. This
    function performs no native operation, await, thread creation or DB work.
    Its protected diagnostic boundary catches internal BaseException failures;
    cancellation of an operation outside this sampler is unaffected.
    """
    try:
        result = _sample(proc_root, cgroup_root, pid, max_processes, budget_ms, reader, clock, page_size_bytes)
        if validate_sample(result): return result
    except BaseException:
        pass
    result = _empty(); result['errors']['SAMPLER_ERROR'] = 1
    return result
