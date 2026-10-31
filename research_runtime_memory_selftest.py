"""Pure fake-procfs and clock controls; no host/process/server measurements."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import threading
import unittest
from unittest.mock import patch

import research_runtime_memory as memory


def stat(pid, parent, start, pages, name='python'):
    # Following comm: field3=state, field4=ppid, field22=start, field24=RSS.
    tail = ['S', str(parent)] + ['0'] * 17 + [str(start), '0', str(pages)]
    return (str(pid) + ' (' + name + ') ' + ' '.join(tail) + '\n').encode()


class FakeReader:
    def __init__(self, files=None, pids=None):
        self.files = dict(files or {})
        self.pids = ['self', '100', '101', '102', '103', '104'] if pids is None else pids
        self.reads = []

    def read(self, path, limit):
        name = str(path).replace('\\', '/')
        self.reads.append((name, limit))
        if name not in self.files: raise FileNotFoundError(name)
        value = self.files[name]
        if type(value) is list:
            value = value.pop(0) if len(value) > 1 else value[0]
        if isinstance(value, BaseException): raise value
        if callable(value): value = value()
        return value[:limit]

    def iter_pids(self, _root):
        if isinstance(self.pids, BaseException): raise self.pids
        yield from self.pids


def fixture():
    return FakeReader({'/proc/100/stat': stat(100, 1, 1000, 11), '/proc/100/status': b'Name:\tpython\nVmRSS:\t123 kB\n',
        '/proc/101/stat': stat(101, 100, 1010, 2, 'node'), '/proc/102/stat': stat(102, 101, 1020, 3, 'chromium'),
        '/proc/103/stat': stat(103, 102, 1030, 5, 'chrome_crashpad'), '/proc/104/stat': stat(104, 1, 1040, 999, 'chrome'),
        '/proc/100/cgroup': b'0::/\n', '/sys/fs/cgroup/memory.current': b'1000000\n', '/sys/fs/cgroup/memory.max': b'2147483648\n'})


def sample(reader=None, **kwargs):
    return memory.sample_memory(reader=reader if reader is not None else fixture(), clock=kwargs.pop('clock', lambda: 1.),
        pid=100, page_size_bytes=kwargs.pop('page_size_bytes', 4096), **kwargs)


class SamplerTests(unittest.TestCase):
    def test_exact_kib_pages_descendants_and_cgroup_bytes(self):
        value = sample()
        self.assertEqual(value['status'], 'COMPLETE')
        self.assertEqual(value['self'], {'status': 'OBSERVED', 'rss_bytes': 123 * 1024, 'process_count': 1, 'rss_source': 'PROC_STATUS_VMRSS'})
        self.assertEqual(value['descendants']['browser_named'], {'rss_bytes': 8 * 4096, 'process_count': 2})
        self.assertEqual(value['descendants']['other'], {'rss_bytes': 2 * 4096, 'process_count': 1})
        self.assertEqual(value['coverage']['matched_descendants'], 3)
        self.assertEqual(value['coverage']['identities_revalidated'], 4)
        self.assertEqual(value['cgroup']['current_bytes'], 1000000)
        self.assertEqual(value['cgroup']['max_bytes'], 2147483648)
        self.assertNotEqual(value['self']['rss_bytes'] + sum(value['descendants'][group]['rss_bytes'] for group in ('browser_named', 'other')), value['cgroup']['current_bytes'])
        self.assertTrue(memory.validate_sample(value))

    def test_page_size_is_measured_not_assumed(self):
        value = sample(page_size_bytes=65536)
        self.assertEqual(value['descendants']['browser_named']['rss_bytes'], 8 * 65536)
        self.assertEqual(value['limits']['page_size_bytes'], 65536)

    def test_status_failure_has_explicit_stat_fallback(self):
        reader = fixture(); reader.files['/proc/100/status'] = PermissionError('secret raw error')
        value = sample(reader)
        self.assertEqual(value['status'], 'PARTIAL')
        self.assertEqual(value['self']['rss_bytes'], 11 * 4096)
        self.assertEqual(value['self']['rss_source'], 'PROC_STAT_RSS_PAGES')
        self.assertIn('PERMISSION_DENIED', value['errors'])

    def test_bad_status_units_are_not_parsed_as_bytes(self):
        reader = fixture(); reader.files['/proc/100/status'] = b'VmRSS: 123 MB\n'
        value = sample(reader)
        self.assertEqual(value['self']['rss_bytes'], 11 * 4096)
        self.assertIn('PARSE_ERROR', value['errors'])

    def test_disappeared_child_and_its_descendants_are_not_counted(self):
        reader = fixture(); reader.files['/proc/101/stat'] = [stat(101, 100, 1010, 2, 'node'), FileNotFoundError('gone')]
        value = sample(reader)
        self.assertEqual(value['descendants']['status'], 'PARTIAL')
        self.assertEqual(value['descendants']['browser_named']['process_count'], 0)
        self.assertEqual(value['descendants']['other']['process_count'], 0)
        self.assertIn('PROCESS_DISAPPEARED', value['errors'])
        self.assertIn('ANCESTOR_UNVERIFIED', value['errors'])

    def test_pid_reuse_does_not_attribute_new_instance_or_children(self):
        reader = fixture(); reader.files['/proc/101/stat'] = [stat(101, 100, 1010, 2, 'node'), stat(101, 100, 9999, 9000, 'chrome')]
        value = sample(reader)
        self.assertEqual(value['coverage']['identity_changed'], 1)
        self.assertEqual(value['descendants']['browser_named']['rss_bytes'], 0)
        self.assertEqual(value['descendants']['other']['rss_bytes'], 0)
        self.assertIn('PID_REUSED', value['errors'])

    def test_reparented_child_is_excluded(self):
        reader = fixture(); reader.files['/proc/102/stat'] = [stat(102, 101, 1020, 3, 'chromium'), stat(102, 1, 1020, 3, 'chromium')]
        value = sample(reader)
        self.assertEqual(value['descendants']['browser_named']['process_count'], 0)
        self.assertEqual(value['descendants']['other']['process_count'], 1)
        self.assertIn('PARENT_CHANGED', value['errors'])

    def test_changed_anchor_identity_makes_process_metrics_unavailable(self):
        reader = fixture(); reader.files['/proc/100/stat'] = [stat(100, 1, 1000, 11), stat(100, 1, 9999, 10000)]
        value = sample(reader)
        self.assertEqual(value['status'], 'UNAVAILABLE')
        self.assertEqual(value['self']['status'], 'IDENTITY_CHANGED')
        self.assertIsNone(value['self']['rss_bytes'])
        self.assertIsNone(value['descendants']['browser_named']['rss_bytes'])
        self.assertTrue(memory.validate_sample(value))

    def test_late_anchor_disappearance_keeps_early_rss_but_no_child_attribution(self):
        reader = fixture(); same = stat(100, 1, 1000, 11)
        reader.files['/proc/100/stat'] = [same, same, FileNotFoundError('gone')]
        value = sample(reader)
        self.assertEqual(value['status'], 'PARTIAL')
        self.assertEqual(value['self']['rss_bytes'], 123 * 1024)
        self.assertEqual(value['cgroup']['current_bytes'], 1000000)
        self.assertEqual(value['descendants']['browser_named']['process_count'], 0)
        self.assertEqual(value['coverage']['skipped_processes'], 3)
        self.assertIn('ANCESTOR_UNVERIFIED', value['errors'])

    def test_late_anchor_reuse_discards_process_attribution(self):
        reader = fixture(); same = stat(100, 1, 1000, 11)
        reader.files['/proc/100/stat'] = [same, same, stat(100, 1, 9999, 999)]
        value = sample(reader)
        self.assertEqual(value['status'], 'UNAVAILABLE')
        self.assertEqual(value['self']['status'], 'IDENTITY_CHANGED')
        self.assertIsNone(value['self']['rss_bytes'])
        self.assertIsNone(value['descendants']['other']['rss_bytes'])
        self.assertTrue(memory.validate_sample(value))

    def test_permission_denied_is_gap_not_zero_absence_proof(self):
        reader = fixture(); reader.files['/proc/101/stat'] = PermissionError('private-token')
        value = sample(reader)
        self.assertEqual(value['status'], 'PARTIAL')
        self.assertIn('PERMISSION_DENIED', value['errors'])
        self.assertFalse(value['status'] == 'COMPLETE')

    def test_process_count_cap_keeps_only_observed_lower_bound(self):
        value = sample(max_processes=2)
        self.assertTrue(value['coverage']['process_cap_reached'])
        self.assertFalse(value['coverage']['scan_complete'])
        self.assertEqual(value['coverage']['processes_examined'], 2)
        self.assertEqual(value['descendants']['other']['process_count'], 1)
        self.assertEqual(value['status'], 'PARTIAL')

    def test_directory_cap_bounds_non_pid_entries_too(self):
        reader = fixture(); reader.pids = ['not-a-pid'] * 10000
        value = sample(reader, max_processes=2)
        self.assertEqual(value['coverage']['directory_entries_examined'], 136)
        self.assertIn('DIRECTORY_CAP', value['errors'])
        self.assertFalse(value['coverage']['scan_complete'])

    def test_cooperative_clock_budget_stops_additional_reads(self):
        reader = fixture(); ticks = iter(index * .005 for index in range(10000))
        value = sample(reader, clock=lambda: next(ticks), budget_ms=20)
        self.assertTrue(value['coverage']['budget_exhausted'])
        self.assertIn('TIME_BUDGET', value['errors'])
        self.assertLess(len(reader.reads), 13)
        self.assertTrue(memory.validate_sample(value))

    def test_later_enumeration_timeout_preserves_early_self_and_cgroup(self):
        reader = fixture(); elapsed = [1.]
        original = reader.iter_pids
        def slow_enumeration(root):
            for item in original(root):
                elapsed[0] += .050
                yield item
        reader.iter_pids = slow_enumeration
        value = sample(reader, clock=lambda: elapsed[0], budget_ms=40)
        self.assertEqual(value['status'], 'PARTIAL')
        self.assertEqual(value['self']['rss_bytes'], 123 * 1024)
        self.assertEqual(value['self']['rss_source'], 'PROC_STATUS_VMRSS')
        self.assertEqual(value['cgroup']['current_bytes'], 1000000)
        self.assertEqual(value['cgroup']['max_bytes'], 2147483648)
        self.assertEqual(value['descendants']['status'], 'PARTIAL')
        self.assertFalse(value['coverage']['scan_complete'])
        self.assertTrue(value['coverage']['budget_exhausted'])
        self.assertTrue(memory.validate_sample(value))

    def test_oversized_file_is_explicitly_truncated(self):
        reader = fixture(); reader.files['/proc/100/stat'] = b'x' * 10000
        value = sample(reader)
        self.assertIn('READ_LIMIT', value['errors'])
        self.assertEqual(value['status'], 'UNAVAILABLE')
        self.assertTrue(all(limit <= memory.MAX_FILE_BYTES for _path, limit in reader.reads))

    def test_parent_cycles_do_not_become_descendants(self):
        reader = fixture(); reader.pids += ['105', '106']
        reader.files['/proc/105/stat'] = stat(105, 106, 1050, 10)
        reader.files['/proc/106/stat'] = stat(106, 105, 1060, 10)
        value = sample(reader)
        self.assertIn('PARENT_TREE_CYCLE', value['errors'])
        self.assertEqual(value['coverage']['matched_descendants'], 3)

    def test_parent_depth_is_bounded(self):
        reader = fixture(); reader.pids = ['100']
        for process_pid in range(101, 167):
            reader.pids.append(str(process_pid)); reader.files['/proc/' + str(process_pid) + '/stat'] = stat(process_pid, process_pid - 1, process_pid * 10, 1)
        value = sample(reader)
        self.assertIn('PARENT_DEPTH_LIMIT', value['errors'])
        self.assertEqual(value['coverage']['matched_descendants'], 64)

    def test_unlimited_cgroup_limit_is_explicit_null(self):
        reader = fixture(); reader.files['/sys/fs/cgroup/memory.max'] = b'max\n'
        value = sample(reader)
        self.assertTrue(value['cgroup']['max_unlimited'])
        self.assertIsNone(value['cgroup']['max_bytes'])

    def test_nonroot_cgroup_mount_root_mapping(self):
        reader = fixture(); reader.files['/proc/100/cgroup'] = b'0::/service/current\n'
        reader.files['/proc/100/mountinfo'] = b'1 2 0:1 /service/current /sys/fs/cgroup rw - cgroup2 cgroup rw\n'
        value = sample(reader)
        self.assertEqual(value['cgroup']['status'], 'OBSERVED')
        self.assertEqual(value['cgroup']['current_bytes'], 1000000)

    def test_host_cgroup_mount_joins_only_actual_group_path(self):
        reader = fixture(); reader.files['/proc/100/cgroup'] = b'0::/service/current\n'
        reader.files['/proc/100/mountinfo'] = b'1 2 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n'
        reader.files['/sys/fs/cgroup/service/current/memory.current'] = b'123\n'
        reader.files['/sys/fs/cgroup/service/current/memory.max'] = b'456\n'
        value = sample(reader)
        self.assertEqual(value['cgroup']['current_bytes'], 123)
        self.assertEqual(value['cgroup']['max_bytes'], 456)

    def test_unknown_nonroot_mount_scope_never_falls_back_to_host_root(self):
        reader = fixture(); reader.files['/proc/100/cgroup'] = b'0::/service/current\n'
        reader.files['/proc/100/mountinfo'] = b'1 2 0:1 /unrelated /sys/fs/cgroup rw - cgroup2 cgroup rw\n'
        value = sample(reader)
        self.assertEqual(value['cgroup']['status'], 'UNAVAILABLE')
        self.assertIsNone(value['cgroup']['current_bytes'])
        self.assertIn('CGROUP_SCOPE_UNRESOLVED', value['errors'])
        self.assertFalse(any(path.endswith('memory.current') for path, _limit in reader.reads))

    def test_unsafe_cgroup_path_is_not_read(self):
        reader = fixture(); reader.files['/proc/100/cgroup'] = b'0::/../private\n'
        value = sample(reader)
        self.assertEqual(value['cgroup']['status'], 'UNAVAILABLE')
        self.assertIn('PARSE_ERROR', value['errors'])
        self.assertFalse(any('/private' in path for path, _limit in reader.reads))

    def test_missing_cgroup_remains_unknown_not_zero(self):
        reader = fixture(); del reader.files['/sys/fs/cgroup/memory.current']
        value = sample(reader)
        self.assertEqual(value['cgroup']['status'], 'UNAVAILABLE')
        self.assertIsNone(value['cgroup']['current_bytes'])

    def test_stat_comm_with_parentheses_is_parsed_without_leaking_name(self):
        reader = fixture(); secret = 'chrome_secret-token ) inner ( text'
        reader.files['/proc/102/stat'] = stat(102, 101, 1020, 3, secret)
        value = sample(reader)
        self.assertEqual(value['status'], 'COMPLETE')
        self.assertNotIn(secret, json.dumps(value))
        self.assertNotIn('secret-token', json.dumps(value))

    def test_internal_baseexceptions_fail_open_without_raw_error_text(self):
        for failure in (KeyboardInterrupt('private-token'), asyncio.CancelledError('private-token'), SystemExit('private-token')):
            reader = fixture(); reader.files['/proc/100/stat'] = failure
            value = sample(reader)
            self.assertEqual(value['status'], 'UNAVAILABLE')
            self.assertEqual(value['errors'], {'SAMPLER_ERROR': 1})
            self.assertNotIn('private-token', json.dumps(value))

    def test_clock_exception_or_nonmonotonic_value_is_unavailable(self):
        value = sample(clock=lambda: (_ for _ in ()).throw(RuntimeError('private-clock')))
        self.assertEqual(value['errors'], {'SAMPLER_ERROR': 1})
        ticks = iter((2., 1.))
        value = sample(clock=lambda: next(ticks))
        self.assertEqual(value['status'], 'UNAVAILABLE')

    def test_invalid_configuration_cannot_expand_bounds(self):
        for kwargs in ({'max_processes': 0}, {'max_processes': 100000}, {'max_processes': True}, {'budget_ms': 100000}, {'budget_ms': float('nan')}):
            value = sample(**kwargs)
            self.assertEqual(value['status'], 'UNAVAILABLE')
            self.assertIn('INVALID_CONFIGURATION', value['errors'])
            self.assertTrue(memory.validate_sample(value))

    def test_output_closed_schema_and_builtin_types(self):
        value = sample(); self.assertTrue(memory.validate_sample(value))
        for path, bad in ((('cmdline',), 'private-token'), (('self', 'cmdline'), 'private-token'), (('self', 'rss_bytes'), True), (('limits', 'page_size_bytes'), 0)):
            copy = deepcopy(value)
            if len(path) == 1: copy[path[0]] = bad
            else: copy[path[0]][path[1]] = bad
            self.assertFalse(memory.validate_sample(copy))
        self.assertLessEqual(len(json.dumps(value).encode()), memory.MAX_OUTPUT_BYTES)

    def test_error_kind_retention_is_bounded(self):
        value = memory._empty(); context = memory._Context(value, fixture(), lambda: 1.)
        for code in sorted(memory.ERROR_CODES): context.error(code)
        self.assertLessEqual(len(value['errors']), 16)
        self.assertIn('OTHER_ERROR', value['errors'])

    def test_total_read_bytes_are_capped_even_without_clock_advance(self):
        value = memory._empty(max_processes=512)
        reader = FakeReader({'/big': b'x' * (memory.MAX_FILE_BYTES - 1)}, pids=[])
        context = memory._Context(value, reader, lambda: 1.)
        for _ in range(200): context.read(Path('/big'), memory.MAX_FILE_BYTES)
        self.assertEqual(value['coverage']['bytes_read'], memory.MAX_TOTAL_READ_BYTES)
        self.assertLessEqual(value['coverage']['read_calls'], 512 * 2 + 16)
        self.assertIn('READ_LIMIT', value['errors'])
        self.assertTrue(memory.validate_sample(value))

    def test_import_body_has_no_io_thread_or_database_side_effect(self):
        source = Path(memory.__file__).read_bytes()
        def forbidden(*_args, **_kwargs): raise AssertionError('import side effect')
        with patch('builtins.open', forbidden), patch.object(memory.os, 'scandir', forbidden), patch.object(threading.Thread, 'start', forbidden), patch.object(sqlite3, 'connect', forbidden):
            namespace = {'__name__': '_isolated_memory_import', '__file__': memory.__file__}
            exec(compile(source, '<isolated_memory_import>', 'exec'), namespace)
        self.assertTrue(callable(namespace['sample_memory']))

    def test_nonlinux_default_has_explicit_unavailability_without_fs_reads(self):
        with patch.object(memory.sys, 'platform', 'win32'), patch.object(memory, '_OSReader', side_effect=AssertionError('no reader')):
            value = memory.sample_memory()
        self.assertEqual(value['errors'], {'PROCFS_UNAVAILABLE': 1})


if __name__ == '__main__': unittest.main()
