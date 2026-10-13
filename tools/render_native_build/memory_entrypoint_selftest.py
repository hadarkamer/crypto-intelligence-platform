"""Offline memory-build adapter guards; no network, processes, or database."""
from __future__ import annotations

import base64
from contextlib import redirect_stdout
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import memory_entrypoint as memory


SECRET = 'PRIVATE_ARM_BYTES_NOT_FOR_THE_PUBLIC_TREE'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def public_summary():
    comparison = dict(order=list(memory.ORDER), rows=2000, passes_per_arm=12,
                      warm_passes=[4, 12], arms=[])
    for label in memory.ORDER:
        comparison['arms'].append(dict(label=label, source='baseline' if label.startswith('A') else 'fixed',
            endpoint_rss_bytes=list(range(10, 22)), warm_median_rss_bytes=17,
            warm_min_rss_bytes=13, warm_max_rss_bytes=21, warm_slope_bytes_per_pass=1.0,
            sampled_max_rss_bytes=21, lifetime_maxrss_bytes=22,
            total_elapsed_ns=100, total_cpu_ns=90, total_observer_ns=10))
    return dict(version=memory.VERSION, result='PASS', scope='synthetic_native_memory_abba',
        build_commit='c' * 40, checked_at_utc='2026-10-13T00:00:00+00:00',
        synthetic_rows=2000, memory_passes=48, positive_input_cases=5,
        runtime=dict(json.loads(memory.build.CONTRACT.read_text())['expected'],
                     libpq_version=170000, pq_implementation='binary', machine='x86_64'), comparison=comparison,
        receipt_sha256={name:'a' * 64 for name in memory.RECEIPTS}, limitations=memory.LIMITATIONS)


class MemoryBuildBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='memory-adapter-selftest-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_public_output_is_only_the_closed_numeric_comparison(self):
        summary = public_summary()
        public = self.root / 'public'
        with patch.object(memory.build, 'PUBLIC', public):
            memory.publish_summary(summary)
        self.assertEqual({path.name for path in public.iterdir()}, {'summary.json', 'index.html'})
        self.assertEqual(json.loads((public / 'summary.json').read_text()), summary)
        for path in public.iterdir():
            self.assertNotIn(SECRET, path.read_text())

    def test_publication_rejects_private_extras_bad_hashes_and_nonfinite_numbers(self):
        cases = ('top_extra', 'runtime_extra', 'comparison_extra', 'arm_extra',
                 'receipt_not_hash', 'nonfinite_number', 'boolean_rss')
        for case in cases:
            with self.subTest(case=case):
                summary = copy.deepcopy(public_summary())
                if case == 'top_extra':
                    summary['private'] = SECRET
                elif case == 'runtime_extra':
                    summary['runtime']['private_path'] = SECRET
                elif case == 'comparison_extra':
                    summary['comparison']['private_receipt'] = SECRET
                elif case == 'arm_extra':
                    summary['comparison']['arms'][0]['private_fixture'] = SECRET
                elif case == 'receipt_not_hash':
                    summary['receipt_sha256'][memory.RECEIPTS[0]] = SECRET
                elif case == 'nonfinite_number':
                    summary['comparison']['arms'][0]['warm_slope_bytes_per_pass'] = float('nan')
                else:
                    summary['comparison']['arms'][0]['endpoint_rss_bytes'][0] = True
                public = self.root / ('rejected-' + case)
                with patch.object(memory.build, 'PUBLIC', public), self.assertRaises(ValueError):
                    memory.publish_summary(summary)
                self.assertFalse(public.exists())
                self.assertFalse(public.with_name(public.name + '.pending').exists())

    def test_private_binary_receipts_reconstruct_exactly_and_exclude_unlisted_files(self):
        run = self.root / 'run'
        expected = {}
        for ordinal, name in enumerate(memory.RECEIPTS):
            path = run / name
            path.parent.mkdir(parents=True, exist_ok=True)
            # Includes non-UTF8 and NUL bytes, as sealed ZIPs must survive unchanged.
            raw = (bytes([ordinal]) + b'\x00\xff\xfe' + SECRET.encode()) * 31
            path.write_bytes(raw)
            expected[name] = raw
        (run / 'private-unlisted.bin').write_bytes(SECRET.encode())
        captured = io.StringIO()
        with redirect_stdout(captured):
            hashes = memory.preserve_receipts(run)
        records = [json.loads(line.removeprefix('[NATIVE_BUILD_RECEIPT] '))
                   for line in captured.getvalue().splitlines()]
        self.assertEqual(records[-1]['kind'], 'complete')
        self.assertEqual({item['path'] for item in records[-1]['files']}, set(memory.RECEIPTS))
        self.assertEqual(hashes, {name: sha(raw) for name, raw in expected.items()})
        self.assertNotIn('private-unlisted.bin', captured.getvalue())
        for name, raw in expected.items():
            chunks = [item for item in records if item.get('path') == name and item['kind'] == 'chunk']
            self.assertEqual([item['index'] for item in chunks], list(range(len(chunks))))
            self.assertTrue(all(len(item['data']) <= 2048 for item in chunks))
            decoded = base64.b64decode(''.join(item['data'] for item in chunks), validate=True)
            self.assertEqual(decoded, raw)
            complete = [item for item in records if item.get('path') == name and item['kind'] == 'file_complete']
            self.assertEqual(len(complete), 1)
            self.assertEqual(complete[0]['sha256'], sha(raw))
            self.assertEqual(complete[0]['bytes'], len(raw))

    def test_invalid_private_receipt_is_rejected_before_any_chunks_escape(self):
        for scenario in ('symlink', 'oversize'):
            with self.subTest(scenario=scenario):
                run = self.root / scenario
                first = run / memory.RECEIPTS[0]
                first.parent.mkdir(parents=True)
                first.write_bytes(b'first-valid-receipt')
                target = run / memory.RECEIPTS[-1]
                target.parent.mkdir(parents=True, exist_ok=True)
                if scenario == 'symlink':
                    target.symlink_to(first)
                else:
                    target.write_bytes(b'x' * (memory.build.MAX_RECEIPT_BYTES + 1))
                captured = io.StringIO()
                with redirect_stdout(captured), self.assertRaises(ValueError):
                    memory.preserve_receipts(run)
                self.assertEqual(captured.getvalue(), '')

    def test_staged_sources_only_change_pinned_application_module(self):
        repo = self.root / 'repo'
        baseline = b'# pinned original application\n'
        fixed = b'# changed application\n'
        companion = b'# identical measured companion\n'
        other_name = 'tools/measured_companion.py'
        old = repo / 'tools/render_native_build/baseline' / memory.MODULE
        old.parent.mkdir(parents=True)
        old.write_bytes(baseline)
        (repo / memory.MODULE).write_bytes(fixed)
        (repo / other_name).write_bytes(companion)
        run = self.root / 'run'
        run.mkdir()
        tracked = '\0'.join([memory.MODULE, other_name, str(old.relative_to(repo)), 'ignored.txt', ''])
        expected = {memory.MODULE: sha(fixed), other_name: sha(companion)}
        with patch.object(memory.build, 'ROOT', repo), \
             patch.object(memory, 'BASE_SHA', sha(baseline)), \
             patch.object(memory, 'FIX_SHA', sha(fixed)), \
             patch.object(memory, 'source_hashes', return_value=expected), \
             patch.object(memory.subprocess, 'check_output', return_value=tracked.encode()):
            self.assertEqual(memory.stage_sources(self.root / 'src', run), expected)
            staged = {arm: {str(path.relative_to(self.root / 'src' / arm)): path.read_bytes()
                            for path in (self.root / 'src' / arm).rglob('*') if path.is_file()}
                      for arm in ('baseline', 'fixed')}
            self.assertEqual(set(staged['baseline']), set(expected))
            self.assertEqual(set(staged['fixed']), set(expected))
            self.assertEqual({name for name in expected if staged['baseline'][name] != staged['fixed'][name]},
                             {memory.MODULE})
            self.assertEqual(staged['baseline'][memory.MODULE], baseline)
            self.assertEqual(staged['fixed'][memory.MODULE], fixed)
            receipt = json.loads((run / 'staging.json').read_text())
            self.assertEqual(receipt['fixed_file_sha256'], expected)
            self.assertEqual(receipt['order'], ['A1', 'B1', 'B2', 'A2'])
            # Replacing a tracked source with a symlink must not bypass source isolation.
            (repo / other_name).unlink()
            (repo / other_name).symlink_to(repo / memory.MODULE)
            with self.assertRaises(ValueError):
                memory.stage_sources(self.root / 'rejected-src', run)

    def test_launch_interruption_cleans_registered_child_before_unwinding(self):
        native = memory.build.native
        process = SimpleNamespace(pid=7654321)
        interrupted = native.RunnerInterrupted('synthetic interruption during launch')
        events = []

        def launch(_command):
            native.OWNED_PROCESSES[process.pid] = process
            native.OWNED_GROUPS.add(process.pid)
            events.append('registered')
            raise interrupted

        def stop(actual, **_kwargs):
            self.assertIs(actual, process)
            events.append('stopped')
            native.OWNED_PROCESSES.pop(process.pid)
            native.OWNED_GROUPS.remove(process.pid)
            return {'group_gone': True, 'forced': False, 'leader_exit_code': 0}

        with patch.object(native, 'OWNED_PROCESSES', {}), \
             patch.object(native, 'OWNED_GROUPS', set()), \
             patch.object(native, 'spawn_owned', side_effect=launch), \
             patch.object(native, 'stop_owned', side_effect=stop), \
             patch.object(memory.build.signal, 'signal', return_value=None):
            with self.assertRaises(native.RunnerInterrupted) as caught:
                memory.build.run_step(['not-executed'], timeout=1)
            self.assertIs(caught.exception, interrupted)
            self.assertEqual(native.OWNED_PROCESSES, {})
            self.assertEqual(native.OWNED_GROUPS, set())
        self.assertEqual(events, ['registered', 'stopped'])


if __name__ == '__main__':
    unittest.main()
