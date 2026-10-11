"""Content-free allocation diagnostic contracts; no service or market access.

Native calls in the main test process are synthetic. The optional real glibc
smoke runs only in a bounded child process after checking the ABI layout.
"""
from contextlib import ExitStack
import ctypes
import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import runtime_allocation_diagnostics as allocation


ABI_FIELDS = ('arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
              'fsmblks', 'uordblks', 'fordblks', 'keepcost')
GC_STATS = [
    {'collections': 10 + generation, 'collected': 100 + generation,
     'uncollectable': generation, 'PRIVATE_EXTRA': 'PRIVATE_PAYLOAD'}
    for generation in range(3)
]


class RuntimeFixture:
    """Fixed providers without actual allocator calls or recorded payloads."""
    def __init__(self):
        self.now = 100.0
        self.pid = 7744
        self.stack = ExitStack()
        self.native_values = {name: 2**32 + index + 1 for index, name in enumerate(ABI_FIELDS)}
        self.native = Mock(side_effect=lambda: SimpleNamespace(**self.native_values))
        self.initialize = Mock(return_value=(self.native, [2, 39], None))

    def __enter__(self):
        for module, name, kwargs in (
            (allocation.time, 'monotonic', {'side_effect': lambda: self.now}),
            (allocation.os, 'getpid', {'side_effect': lambda: self.pid}),
            (allocation.sys, 'getallocatedblocks', {'return_value': 123456}),
            (allocation.gc, 'isenabled', {'return_value': True}),
            (allocation.gc, 'get_count', {'return_value': (-7, 2, 0)}),
            (allocation.gc, 'get_threshold', {'return_value': (700, 10, 10)}),
            (allocation.gc, 'get_stats', {'return_value': GC_STATS}),
            (allocation.threading, 'active_count', {'return_value': 5}),
        ):
            self.stack.enter_context(patch.object(module, name, **kwargs))
        self.stack.enter_context(patch.object(allocation, '_native_state', None))
        self.stack.enter_context(patch.object(allocation, '_state_guard', threading.Lock()))
        self.stack.enter_context(patch.object(allocation, '_initialize_native', self.initialize))
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)

    def sample(self, phase='runtime_poll', *, deadline=None):
        return allocation.collect_allocation_stats(
            phase, deadline=self.now + 1 if deadline is None else deadline)


class RuntimeAllocationTests(unittest.TestCase):
    def test_fixed_aggregate_shape_and_exact_values(self):
        with RuntimeFixture() as fixture:
            result = fixture.sample()
        self.assertEqual(result['version'], 1)
        self.assertEqual(result['python_allocated_blocks'], 123456)
        self.assertEqual(result['active_threads'], 5)
        self.assertEqual(result['gc']['enabled'], True)
        self.assertEqual(result['gc']['count'], [-7, 2, 0])
        self.assertEqual(result['gc']['threshold'], [700, 10, 10])
        self.assertEqual(result['gc']['stats'], [
            {key: row[key] for key in ('collections', 'collected', 'uncollectable')}
            for row in GC_STATS])
        self.assertEqual(result['native_allocator']['stats'], fixture.native_values)
        self.assertEqual(result['native_allocator']['glibc_version'], [2, 39])
        self.assertFalse(result['partial'])
        self.assertEqual(result['reasons'], [])
        self.assertNotIn('PRIVATE', json.dumps(result))
        fixture.initialize.assert_called_once_with()
        fixture.native.assert_called_once_with()

    def test_fixed_schema_does_not_expand_with_provider_metadata(self):
        with RuntimeFixture() as fixture:
            result = fixture.sample()
        self.assertEqual(set(result), {'version', 'implementation', 'python_allocated_blocks',
                                     'gc', 'active_threads', 'native_allocator', 'partial', 'reasons'})
        self.assertEqual(set(result['implementation']), {'name', 'version'})
        self.assertEqual(set(result['gc']), {'enabled', 'count', 'threshold', 'stats'})
        self.assertEqual(set(result['native_allocator']), {
            'status', 'provider', 'glibc_version', 'stats', 'duration_us', 'budget_overrun'})
        self.assertEqual(set(result['native_allocator']['stats']), set(ABI_FIELDS))
        with RuntimeFixture() as fixture:
            with patch.object(allocation.sys, 'implementation', SimpleNamespace(name='PRIVATE_IMPLEMENTATION')), \
                 patch.object(allocation.sys, 'version_info', (3, 'PRIVATE_VERSION', 0)):
                result = fixture.sample()
        self.assertIsNone(result['implementation']['name'])
        self.assertIsNone(result['implementation']['version'])
        self.assertEqual(result['python_allocated_blocks'], 123456)
        self.assertNotIn('PRIVATE', json.dumps(result))

    def test_allocated_blocks_unknown_and_invalid_do_not_erase_other_fields(self):
        for value in (0, -1, True, 1.25, 'PRIVATE_PAYLOAD', None, 2**64):
            with self.subTest(value_type=type(value).__name__), RuntimeFixture() as fixture:
                with patch.object(allocation.sys, 'getallocatedblocks', return_value=value):
                    result = fixture.sample()
                self.assertIsNone(result['python_allocated_blocks'])
                self.assertEqual(result['gc']['count'], [-7, 2, 0])
                self.assertEqual(result['active_threads'], 5)
                self.assertEqual(result['native_allocator']['stats'], fixture.native_values)
                self.assertTrue(result['partial'])
                self.assertNotIn('PRIVATE', json.dumps(result))
                if value == 0 and type(value) is int:
                    self.assertIn('allocated_blocks_unknown', result['reasons'])
        with RuntimeFixture() as fixture:
            with patch.object(allocation.sys, 'getallocatedblocks', None):
                result = fixture.sample()
            self.assertIsNone(result['python_allocated_blocks'])
            self.assertTrue(result['partial'])

    def test_independent_provider_exceptions_are_sanitized(self):
        providers = (
            (allocation.sys, 'getallocatedblocks', ('python_allocated_blocks',)),
            (allocation.gc, 'isenabled', ('gc', 'enabled')),
            (allocation.gc, 'get_count', ('gc', 'count')),
            (allocation.gc, 'get_threshold', ('gc', 'threshold')),
            (allocation.gc, 'get_stats', ('gc', 'stats')),
            (allocation.threading, 'active_count', ('active_threads',)),
        )
        for module, name, path in providers:
            with self.subTest(provider=name), RuntimeFixture() as fixture:
                with patch.object(module, name, side_effect=RuntimeError('PRIVATE credential /secret/path')):
                    result = fixture.sample()
                value = result
                for key in path:
                    value = value[key]
                self.assertIsNone(value)
                self.assertTrue(result['partial'])
                self.assertEqual(result['native_allocator']['stats'], fixture.native_values)
                if name != 'getallocatedblocks':
                    self.assertEqual(result['python_allocated_blocks'], 123456)
                self.assertNotIn('PRIVATE', json.dumps(result))
                self.assertNotIn('/secret/path', json.dumps(result))

    def test_missing_getter_is_unknown_without_losing_other_gc_fields(self):
        for name, key in (('isenabled', 'enabled'), ('get_count', 'count'),
                          ('get_threshold', 'threshold'), ('get_stats', 'stats')):
            with self.subTest(provider=name), RuntimeFixture() as fixture:
                getter = getattr(allocation.gc, name)
                delattr(allocation.gc, name)
                try:
                    result = fixture.sample()
                finally:
                    setattr(allocation.gc, name, getter)
                self.assertIsNone(result['gc'][key])
                self.assertIn('gc_' + key + '_unavailable', result['reasons'])
                self.assertEqual(result['python_allocated_blocks'], 123456)
                self.assertEqual(result['active_threads'], 5)
                self.assertEqual(sum(value is None for value in result['gc'].values()), 1)

    def test_signed_count_and_threshold_triples_preserve_zero_and_negative(self):
        with RuntimeFixture() as fixture:
            with patch.object(allocation.gc, 'get_count', return_value=(-3, 0, 7)), \
                 patch.object(allocation.gc, 'get_threshold', return_value=(-1, 0, 10)):
                result = fixture.sample()
        self.assertEqual(result['gc']['count'], [-3, 0, 7])
        self.assertEqual(result['gc']['threshold'], [-1, 0, 10])
        self.assertFalse(result['partial'])

    def test_malformed_gc_shapes_never_walk_arbitrary_iterators(self):
        class PrivateIterable:
            def __iter__(self):
                raise AssertionError('PRIVATE object iteration forbidden')

            def __repr__(self):
                raise AssertionError('PRIVATE object repr forbidden')

        for name, key in (('get_count', 'count'), ('get_threshold', 'threshold')):
            for value in ((), (1, 2), (1, 2, 3, 4), (1, True, 3),
                          (1, 'PRIVATE_PAYLOAD', 3), (1, 2**64, 3), PrivateIterable()):
                with self.subTest(provider=name, value_type=type(value).__name__), RuntimeFixture() as fixture:
                    with patch.object(allocation.gc, name, return_value=value):
                        result = fixture.sample()
                    self.assertIsNone(result['gc'][key])
                    self.assertEqual(result['python_allocated_blocks'], 123456)
                    self.assertTrue(result['partial'])
                    self.assertNotIn('PRIVATE', json.dumps(result))
        for value in ([], GC_STATS[:2], GC_STATS + GC_STATS[:1], PrivateIterable(),
                      [{'collections': 1, 'collected': 2}] * 3,
                      [{'collections': 1, 'collected': -1, 'uncollectable': 0}] * 3,
                      [{'collections': True, 'collected': 1, 'uncollectable': 0}] * 3):
            with self.subTest(stats_type=type(value).__name__), RuntimeFixture() as fixture:
                with patch.object(allocation.gc, 'get_stats', return_value=value):
                    result = fixture.sample()
                self.assertIsNone(result['gc']['stats'])
                self.assertTrue(result['partial'])
                self.assertNotIn('PRIVATE', json.dumps(result))

    def test_boolean_enabled_and_nonnegative_integer_threads_only(self):
        for value in (0, 1, 'PRIVATE_PAYLOAD', None):
            with self.subTest(enabled_type=type(value).__name__), RuntimeFixture() as fixture:
                with patch.object(allocation.gc, 'isenabled', return_value=value):
                    result = fixture.sample()
                self.assertIsNone(result['gc']['enabled'])
                self.assertTrue(result['partial'])
        for value in (-1, True, 'PRIVATE_PAYLOAD', None, 2**64):
            with self.subTest(threads_type=type(value).__name__), RuntimeFixture() as fixture:
                with patch.object(allocation.threading, 'active_count', return_value=value):
                    result = fixture.sample()
                self.assertIsNone(result['active_threads'])
                self.assertTrue(result['partial'])

    def test_gc_zero_statistics_are_real_and_extra_keys_are_ignored(self):
        rows = [{'collections': 0, 'collected': 0, 'uncollectable': 0,
                 'PRIVATE_KEY': object()} for _ in range(3)]
        with RuntimeFixture() as fixture:
            fixture.native_values = {name: 0 for name in ABI_FIELDS}
            with patch.object(allocation.gc, 'get_stats', return_value=rows), \
                 patch.object(allocation.gc, 'isenabled', return_value=False):
                result = fixture.sample()
        self.assertFalse(result['gc']['enabled'])
        self.assertEqual(result['gc']['stats'], [
            {'collections': 0, 'collected': 0, 'uncollectable': 0}] * 3)
        self.assertFalse(result['partial'])
        self.assertEqual(result['native_allocator']['stats'], {name: 0 for name in ABI_FIELDS})
        self.assertNotIn('PRIVATE', json.dumps(result))

    def test_sampling_is_passive_and_emits_no_private_content(self):
        forbidden = (
            (gc, 'collect'), (gc, 'get_objects'), (gc, 'get_referrers'),
            (gc, 'get_referents'), (gc, 'set_threshold'), (gc, 'set_debug'),
            (gc, 'enable'), (gc, 'disable'), (gc, 'freeze'), (gc, 'unfreeze'),
            (sys, '_current_frames'), (threading, 'enumerate'),
            (threading.Thread, 'start'),
        )
        callbacks = tuple(gc.callbacks)
        with RuntimeFixture() as fixture, ExitStack() as stack:
            spies = [stack.enter_context(patch.object(module, name, side_effect=AssertionError('PRIVATE forbidden mutation/walk')))
                     for module, name in forbidden]
            result = fixture.sample('watch_start')
            for spy in spies:
                spy.assert_not_called()
        self.assertEqual(tuple(gc.callbacks), callbacks)
        self.assertNotIn('PRIVATE', json.dumps(result))
        fixture.initialize.assert_not_called()

    def test_native_only_start_and_poll_and_rate_limit_resets_for_pid(self):
        with RuntimeFixture() as fixture:
            for phase in ('watch_start', 'watch_end', 'scoring_start', 'collector_end', 'PRIVATE_PHASE'):
                result = fixture.sample(phase)
                self.assertIsNone(result['native_allocator']['stats'])
                self.assertEqual(result['native_allocator']['status'], 'phase_skipped')
            fixture.initialize.assert_not_called()
            self.assertEqual(fixture.sample('runtime_start')['native_allocator']['stats'], fixture.native_values)
            fixture.now = 159.999
            limited = fixture.sample()['native_allocator']
            self.assertIsNone(limited['stats'])
            self.assertIsNone(limited['duration_us'])
            self.assertEqual(limited['status'], 'rate_limited')
            self.assertEqual(fixture.native.call_count, 1)
            fixture.now = 160.0
            self.assertEqual(fixture.sample()['native_allocator']['stats'], fixture.native_values)
            self.assertEqual(fixture.native.call_count, 2)
            fixture.pid += 1
            fixture.now = 160.001
            self.assertEqual(fixture.sample()['native_allocator']['stats'], fixture.native_values)
            self.assertEqual(fixture.native.call_count, 3)
            self.assertEqual(fixture.initialize.call_count, 2)

    def test_first_concurrent_initialization_has_one_provider_and_no_stale_sample(self):
        with RuntimeFixture() as fixture:
            started = threading.Event()
            release = threading.Event()
            first_done = threading.Event()
            second_done = threading.Event()
            results = {}

            def initialize():
                started.set()
                if not release.wait(timeout=3):
                    raise AssertionError('Fixture release timed out')
                return fixture.native, [2, 39], None

            def sample(name, done):
                try:
                    results[name] = fixture.sample()
                finally:
                    done.set()

            fixture.initialize.side_effect = initialize
            first = threading.Thread(target=sample, args=('first', first_done), daemon=True)
            second = threading.Thread(target=sample, args=('second', second_done), daemon=True)
            first.start()
            try:
                self.assertTrue(started.wait(timeout=1))
                second.start()
                completed_without_first = second_done.wait(timeout=1)
            finally:
                release.set()
                first.join(timeout=2)
                if second.ident is not None:
                    second.join(timeout=2)
            self.assertTrue(completed_without_first, 'Second sampler waited for native initialization')
            self.assertTrue(first_done.is_set())
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(results['second']['native_allocator']['status'], 'busy')
            self.assertIsNone(results['second']['native_allocator']['stats'])
            self.assertEqual(results['first']['native_allocator']['stats'], fixture.native_values)
            fixture.initialize.assert_called_once_with()
            fixture.native.assert_called_once_with()
            self.assertEqual(fixture.sample()['native_allocator']['status'], 'rate_limited')
            self.assertEqual(fixture.native.call_count, 1)

    def test_native_lock_contention_returns_without_waiting(self):
        with RuntimeFixture() as fixture:
            state = allocation._state_for_pid(fixture.pid)
            lock = state['lock']
            entered = threading.Event()
            result = []

            def sample():
                result.append(fixture.sample())
                entered.set()

            lock.acquire()
            thread = threading.Thread(target=sample, daemon=True)
            try:
                thread.start()
                completed_while_locked = entered.wait(timeout=1)
            finally:
                lock.release()
                thread.join(timeout=2)
            self.assertTrue(completed_while_locked, 'Sampling blocked on an already-held allocator lock')
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(result), 1)
            self.assertIsNone(result[0]['native_allocator']['stats'])
            self.assertEqual(result[0]['native_allocator']['status'], 'busy')
            fixture.initialize.assert_not_called()
            fixture.native.assert_not_called()

    def test_state_publication_guard_is_nonblocking_and_fork_reset_discards_locks(self):
        with RuntimeFixture() as fixture:
            guard = allocation._state_guard
            guard.acquire()
            done = threading.Event()
            results = []

            def sample():
                results.append(fixture.sample())
                done.set()

            thread = threading.Thread(target=sample, daemon=True)
            try:
                thread.start()
                completed_while_locked = done.wait(timeout=1)
            finally:
                guard.release()
                thread.join(timeout=2)
            self.assertTrue(completed_while_locked)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results[0]['native_allocator']['status'], 'busy')
            fixture.initialize.assert_not_called()
            old_state = allocation._state_for_pid(fixture.pid)
            old_state['lock'].acquire()
            guard.acquire()
            try:
                allocation._after_fork()
                self.assertIsNone(allocation._native_state)
                self.assertIsNot(allocation._state_guard, guard)
                self.assertEqual(fixture.sample()['native_allocator']['stats'], fixture.native_values)
            finally:
                old_state['lock'].release()
                guard.release()

    def test_native_call_failure_and_bad_values_are_null_sanitized_and_throttled(self):
        for mode in ('raise', 'negative', 'bool', 'overflow', 'missing'):
            with self.subTest(mode=mode), RuntimeFixture() as fixture:
                if mode == 'raise':
                    fixture.native.side_effect = RuntimeError('PRIVATE libc path /secret')
                else:
                    values = dict(fixture.native_values)
                    if mode == 'missing':
                        del values['arena']
                    else:
                        values['arena'] = {'negative': -1, 'bool': True, 'overflow': 2**65}[mode]
                    fixture.native.side_effect = lambda: SimpleNamespace(**values)
                result = fixture.sample()
                self.assertIsNone(result['native_allocator']['stats'])
                self.assertEqual(result['python_allocated_blocks'], 123456)
                self.assertTrue(result['partial'])
                self.assertNotIn('PRIVATE', json.dumps(result))
                fixture.sample()
                self.assertEqual(fixture.native.call_count, 1)

    def test_expired_budget_prevents_native_call_and_overrun_is_reported(self):
        with RuntimeFixture() as fixture:
            result = fixture.sample(deadline=fixture.now)
            self.assertIsNone(result['native_allocator']['stats'])
            fixture.initialize.assert_not_called()
            fixture.native.assert_not_called()
            for key in ('python_allocated_blocks', 'active_threads'):
                self.assertIsNone(result[key])
            self.assertTrue(all(value is None for value in result['gc'].values()))
            allocation.sys.getallocatedblocks.assert_not_called()
            allocation.gc.get_count.assert_not_called()
            allocation.gc.get_stats.assert_not_called()
            allocation.threading.active_count.assert_not_called()
        with RuntimeFixture() as fixture:
            def delayed_native():
                fixture.now += 2
                return SimpleNamespace(**fixture.native_values)
            fixture.native.side_effect = delayed_native
            result = fixture.sample(deadline=101.0)
            self.assertEqual(result['native_allocator']['stats'], fixture.native_values)
            self.assertTrue(result['native_allocator']['budget_overrun'])
            self.assertGreaterEqual(result['native_allocator']['duration_us'], 2_000_000)
            self.assertTrue(result['partial'])

    def test_native_initialization_overrun_skips_call_and_failures_are_sanitized(self):
        with RuntimeFixture() as fixture:
            def delayed_initialize():
                fixture.now += 2
                return fixture.native, [2, 39], None
            fixture.initialize.side_effect = delayed_initialize
            result = fixture.sample(deadline=101)
            self.assertEqual(result['native_allocator']['status'], 'time_budget')
            self.assertTrue(result['native_allocator']['budget_overrun'])
            self.assertIsNone(result['native_allocator']['stats'])
            self.assertIn('native_time_budget', result['reasons'])
            fixture.native.assert_not_called()
        with RuntimeFixture() as fixture:
            fixture.initialize.side_effect = RuntimeError('PRIVATE provider /secret/path')
            result = fixture.sample()
            self.assertEqual(result['native_allocator']['status'], 'unavailable')
            self.assertIsNone(result['native_allocator']['stats'])
            self.assertIn('native_initialization_unavailable', result['reasons'])
            self.assertNotIn('PRIVATE', json.dumps(result))
            fixture.now += 61
            fixture.sample()
            fixture.initialize.assert_called_once_with()
            fixture.native.assert_not_called()

    def test_budget_expiring_during_state_lookup_prevents_initialization(self):
        with RuntimeFixture() as fixture:
            original_lookup = allocation._state_for_pid
            states = []

            def delayed_lookup(pid):
                state = original_lookup(pid)
                states.append(state)
                fixture.now += 2
                return state

            with patch.object(allocation, '_state_for_pid', side_effect=delayed_lookup):
                result = fixture.sample(deadline=101)
            self.assertEqual(result['native_allocator']['status'], 'time_budget')
            self.assertIsNone(result['native_allocator']['stats'])
            self.assertIsNone(states[0]['last_attempt'])
            self.assertFalse(states[0]['lock'].locked())
            fixture.initialize.assert_not_called()
            fixture.native.assert_not_called()


class NativeAbiTests(unittest.TestCase):
    def test_exact_ten_size_t_fields_and_offsets_before_any_native_call(self):
        struct = allocation._mallinfo2_type(ctypes)
        self.assertEqual(tuple(allocation.NATIVE_FIELDS), ABI_FIELDS)
        self.assertEqual(struct._fields_, [(name, ctypes.c_size_t) for name in ABI_FIELDS])
        width = ctypes.sizeof(ctypes.c_size_t)
        self.assertEqual(ctypes.sizeof(struct), 10 * width)
        for index, name in enumerate(ABI_FIELDS):
            self.assertEqual(getattr(struct, name).offset, index * width)
        if width == 8:
            value = struct(**{name: 2**32 + index for index, name in enumerate(ABI_FIELDS)})
            self.assertEqual([getattr(value, name) for name in ABI_FIELDS],
                             [2**32 + index for index in range(10)])

    def initialize(self, *, version=b'2.39', pointer_bytes=8, size_t_bytes=8,
                   version_error=None, native_missing=False, platform='linux', loader_error=None):
        version_fn = Mock(return_value=version, side_effect=version_error)
        if version_error is None:
            def version_call():
                self.assertEqual(version_fn.argtypes, [])
                self.assertIs(version_fn.restype, ctypes.c_char_p)
                return version
            version_fn.side_effect = version_call
        provider = Mock()
        accesses = []

        class Library:
            gnu_get_libc_version = version_fn

            @property
            def mallinfo2(self):
                accesses.append('mallinfo2')
                if native_missing:
                    raise AttributeError('PRIVATE missing native symbol')
                return provider

        def sizeof(value):
            if value is ctypes.c_void_p:
                return pointer_bytes
            if value is ctypes.c_size_t:
                return size_t_bytes
            return ctypes.sizeof(value)

        cdll = Mock(return_value=Library(), side_effect=loader_error)
        fake_ctypes = SimpleNamespace(
            CDLL=cdll, sizeof=sizeof, c_void_p=ctypes.c_void_p,
            c_size_t=ctypes.c_size_t, c_char_p=ctypes.c_char_p, Structure=ctypes.Structure)
        with patch.object(allocation.sys, 'platform', platform), \
             patch.object(allocation, '_load_ctypes', return_value=fake_ctypes) as loader:
            result = allocation._initialize_native()
        return result, SimpleNamespace(version=version_fn, provider=provider,
                                       accesses=accesses, cdll=cdll, loader=loader)

    def test_native_signature_is_set_before_any_call_and_preserves_large_values(self):
        (provider, version, reason), fake = self.initialize()
        self.assertIs(provider, fake.provider)
        self.assertEqual(version, [2, 39])
        self.assertIsNone(reason)
        self.assertEqual(fake.version.argtypes, [])
        self.assertIs(fake.version.restype, ctypes.c_char_p)
        self.assertEqual(provider.argtypes, [])
        self.assertEqual(provider.restype._fields_, [(name, ctypes.c_size_t) for name in ABI_FIELDS])
        fake.cdll.assert_called_once_with('libc.so.6')
        fake.version.assert_called_once_with()
        fake.provider.assert_not_called()
        if ctypes.sizeof(ctypes.c_size_t) == 8:
            values = {name: 2**32 + index for index, name in enumerate(ABI_FIELDS)}
            fake.provider.return_value = provider.restype(**values)
            with RuntimeFixture() as fixture:
                fixture.initialize.return_value = provider, version, reason
                result = fixture.sample()
            self.assertEqual(result['native_allocator']['stats'], values)

    def test_platform_abi_and_old_glibc_guards_prevent_native_symbol_use(self):
        for platform in ('darwin', 'win32', 'PRIVATE_PLATFORM'):
            with self.subTest(platform=platform):
                (provider, version, reason), fake = self.initialize(platform=platform)
                self.assertEqual((provider, version, reason), (None, None, 'native_platform_unsupported'))
                fake.loader.assert_not_called()
                fake.cdll.assert_not_called()
        for pointer_bytes, size_t_bytes in ((4, 8), (8, 4), (4, 4)):
            with self.subTest(pointer=pointer_bytes, size_t=size_t_bytes):
                (provider, version, reason), fake = self.initialize(pointer_bytes=pointer_bytes, size_t_bytes=size_t_bytes)
                self.assertEqual((provider, version, reason), (None, None, 'native_abi_unsupported'))
                fake.cdll.assert_not_called()
        for version in (b'2.32', b'2.9', b'1.99'):
            with self.subTest(version=version):
                (provider, parsed, reason), fake = self.initialize(version=version)
                self.assertIsNone(provider)
                self.assertEqual(reason, 'native_glibc_unsupported')
                self.assertEqual(fake.accesses, [])
                fake.provider.assert_not_called()
        (provider, version, reason), fake = self.initialize(version=b'2.33')
        self.assertIs(provider, fake.provider)
        self.assertEqual(version, [2, 33])
        self.assertIsNone(reason)

    def test_native_import_metadata_loader_and_symbol_failures_remain_unknown(self):
        with patch.object(allocation.sys, 'platform', 'linux'), \
             patch.object(allocation, '_load_ctypes', side_effect=ImportError('PRIVATE ctypes path')):
            self.assertEqual(allocation._initialize_native(), (None, None, 'native_ctypes_unavailable'))
        for value in (None, '2.39', b'PRIVATE', b'2.39.1', b'2.39-secret', b'2.-1', b'2.', b' 2.39'):
            with self.subTest(metadata_type=type(value).__name__):
                (provider, version, reason), fake = self.initialize(version=value)
                self.assertEqual((provider, version, reason), (None, None, 'native_glibc_version_invalid'))
                self.assertEqual(fake.accesses, [])
        for kwargs in ({'loader_error': OSError('PRIVATE libc path')},
                       {'version_error': AttributeError('PRIVATE non-glibc symbol')}):
            (provider, version, reason), fake = self.initialize(**kwargs)
            self.assertEqual((provider, version, reason), (None, None, 'native_libc_unavailable'))
            self.assertEqual(fake.accesses, [])
        (provider, version, reason), fake = self.initialize(native_missing=True)
        self.assertEqual((provider, version, reason), (None, [2, 39], 'native_mallinfo2_unavailable'))
        fake.provider.assert_not_called()

    def test_supported_native_smoke_in_isolated_subprocess(self):
        script = '''
import ctypes, json, os, sys, time
import runtime_allocation_diagnostics as allocation
fields = ('arena','ordblks','smblks','hblks','hblkhd','usmblks','fsmblks','uordblks','fordblks','keepcost')
struct = allocation._mallinfo2_type(ctypes)
width = ctypes.sizeof(ctypes.c_size_t)
assert struct._fields_ == [(name, ctypes.c_size_t) for name in fields]
assert ctypes.sizeof(struct) == 10 * width
assert [getattr(struct, name).offset for name in fields] == [index * width for index in range(10)]
if sys.platform != 'linux' or width != 8 or ctypes.sizeof(ctypes.c_void_p) != 8:
    print(json.dumps({'supported': False, 'reason': 'native_platform_or_abi_unsupported'}))
    raise SystemExit(0)
try:
    libc = os.confstr('CS_GNU_LIBC_VERSION')
except (AttributeError, OSError, ValueError):
    libc = None
if not libc or not libc.startswith('glibc ') or tuple(map(int, libc.split()[1].split('.')[:2])) < (2,33):
    print(json.dumps({'supported': False, 'reason': 'native_glibc_unsupported'}))
    raise SystemExit(0)
provider, version, reason = allocation._initialize_native()
assert reason is None and provider is not None
assert provider.argtypes == []
assert provider.restype._fields_ == [(name, ctypes.c_size_t) for name in fields]
first = allocation.collect_allocation_stats('runtime_start', deadline=time.monotonic()+1.0)
native = first['native_allocator']
assert native['status'] == 'ok', native['status']
assert native['provider'] == 'glibc_mallinfo2'
assert set(native['stats']) == set(fields)
assert all(type(value) is int and 0 <= value < 2**64 for value in native['stats'].values())
second = allocation.collect_allocation_stats('runtime_poll', deadline=time.monotonic()+1.0)
assert second['native_allocator']['status'] == 'rate_limited'
assert second['native_allocator']['stats'] is None
print(json.dumps({'supported': True, 'status': native['status'], 'field_count': len(native['stats']), 'glibc_version': version}))
'''
        child = subprocess.run([sys.executable, '-c', script],
                               cwd=Path(__file__).resolve().parent,
                               env={'PYTHONNOUSERSITE': '1'},
                               capture_output=True, text=True, timeout=10, check=False)
        self.assertEqual(child.returncode, 0, child.stderr)
        result = json.loads(child.stdout)
        if not result['supported']:
            self.skipTest(result['reason'])
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['field_count'], 10)


if __name__ == '__main__':
    unittest.main()
