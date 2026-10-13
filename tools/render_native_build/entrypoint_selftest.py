"""Offline receipt/publication checks; never start PostgreSQL or build tools."""
import base64
from contextlib import redirect_stdout
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import entrypoint as build
from verify_ordered_binding_correctness import MODULE, SOURCE_SHA256, VERSION as CORRECTNESS_VERSION


SECRET = 'PRIVATE_FIXTURE_SENTINEL_DO_NOT_PUBLISH'
CASES = ('empty_no_query', 'duplicate_and_missing', 'existing_int_normalization',
         'signed_int64_boundaries', 'string_int64_boundaries')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + '\n')
    return digest(path.read_bytes())


def fixture(root, source_arm='fixed'):
    contract = json.loads(build.CONTRACT.read_text())
    loader_sha = SOURCE_SHA256[source_arm]
    bindings = dict(runner=build.native.sha_file(build.native.__file__),
                    contract=build.native.sha_file(build.CONTRACT),
                    verifier='a' * 64, actual_sources={'fixture_source.py':'b'*64,
                        MODULE:loader_sha, 'tools/verify_ordered_binding_correctness.py':'a'*64,
                        'tools/ordered_binding_range_check.py':'c'*64,
                        'research_common_window_metrics_store_selftest.py':'d'*64})
    token, driver = 'fixture-owned-token', {'fixture_driver': 'binary'}
    observed = dict(contract['expected'], driver_binding=driver)
    pre = dict(result='READY', source_bindings=bindings, runner_sha256=bindings['runner'],
               contract_sha256=bindings['contract'], contract=contract, observed=observed,
               private_diagnostic=SECRET + ('x' * 4096))
    pre_sha = write_json(root / 'preflight.json', pre)
    host = dict(runtime_preflight_sha256=pre_sha, host='127.0.0.1',
                engine='native PostgreSQL', dbname='postgres', user='postgres', run_token=token)
    host_sha = write_json(root / 'host/connection.json', host)
    result = dict(loaded_rows=32, full_parity_verified=True, reference_parity=True)
    manifest = dict(version=CORRECTNESS_VERSION, source_arm=source_arm, loader_sha256=loader_sha,
                    sources=bindings['actual_sources'], driver_binding=driver, run_token=token,
                    tool_sha256=bindings['verifier'], connection_receipt_sha256=host_sha,
                    expected=result)
    manifest_sha = write_json(root / 'correctness/fixture_manifest.json', manifest)
    case_rows = dict(zip(CASES, (0, 1, 1, 0, 0)))
    reference_sha = 'e'*64
    range_cases = [dict(case=name, input_decimal=str(number), backend_pid=pid,
        before_transaction_state='IDLE', sqlstate='22003', error_class='NumericValueOutOfRange',
        error_transaction_state='INERROR', after_rollback_state='IDLE', final_transaction_state='IDLE',
        read_only_transactions_verified=3, reference_rows=1, reference_sha256=reference_sha,
        recovered_sha256=reference_sha, full_result_equal=True, connection_closed=True)
        for name, number, pid in [('overflow_above_int64',2**63,101),
                                  ('overflow_below_int64',-(2**63)-1,102)]]
    correct = dict(version=CORRECTNESS_VERSION, kind='binding_correctness', result='PASS', run_token=token,
                   source_arm=source_arm, loader_sha256=loader_sha, cursor_policy='observed',
                   measured_passes=0, actual_loader=True, proxy_rewrites=0,
                   connection_closed=True, checkpoint_verified=True,
                   source_hashes=bindings['actual_sources'], driver_binding=driver,
                   tool_sha256=bindings['verifier'], connection_receipt_sha256=host_sha,
                   fixture_manifest_sha256=manifest_sha, full_scope_result=result,
                   source_semantics_audit=dict(result='PASS', actual_loader=True, proxy_used=False,
                       input_int_conversion_preserved=True, source_writes=0,
                       cases=[dict(case=x, rows=case_rows[x], sha256=reference_sha if case_rows[x] else digest(b'{}'))
                              for x in CASES]),
                   cursor_counts=dict(queries=4, fetchone=2, fetchall=2, array_targets=1,
                       scalar_conversions=0, explicit_closes=0,
                       observed_list_bindings=int(source_arm=='baseline'),
                       observed_scalar_bindings=int(source_arm=='fixed')),
                   range_audit=dict(version='ordered-binding-range-check-v1', result='PASS',
                       actual_loader=True, proxy_used=False, connection_count=2,
                       fresh_backend_connections=True, database_writes=False, read_only_enforced=True,
                       window_minutes=60, reference_sha256=reference_sha, cases=range_cases,
                       loader_sha256=loader_sha, checker_sha256='c'*64),
                   connection_count=3)
    correct_sha = write_json(root / 'correctness/correctness_receipt.json', correct)
    clean = dict(group_gone=True, forced=False, leader_exit_code=0)
    life = dict(result='PASS', native_lifecycle_tested=True, sources_unchanged=True,
                measured_passes=0, source_bindings=bindings, preflight_sha256=pre_sha,
                run_token=token, cleanup=[clean], correctness_receipt_sha256=correct_sha,
                descendant_cleanup=dict(forced=False, tracked_live_remaining=0))
    write_json(root / 'native_lifecycle.json', life)
    write_json(root / 'runner_cleanup.json', dict(exit_code=0, cleanup=[clean]))
    (root / 'host/data').mkdir()
    (root / 'host/data/PG_VERSION').write_text(SECRET)
    (root / 'unlisted-private.log').write_text(SECRET)
    return bindings


def summary(details):
    return dict(details, version=build.VERSION, result='PASS',
                scope='synthetic_native_correctness_only', build_commit='c' * 40,
                checked_at_utc='2026-10-13T00:00:00+00:00', limitations=build.LIMITATIONS)


class NativeBuildReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='native-build-receipts-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_complete_bound_fixture_publishes_only_public_summary(self):
        for arm in ('baseline', 'fixed'):
            with self.subTest(arm=arm):
                run = self.root / arm
                bindings = fixture(run, arm)
                details = build.validate_run(run, bindings)  # Calls the real verifier.
                public = self.root / (arm+'-public')
                expected = summary(details)
                build.publish_summary(public, expected)
                self.assertEqual({p.name for p in public.iterdir()}, {'summary.json', 'index.html'})
                self.assertEqual(json.loads((public / 'summary.json').read_text()), expected)
                self.assertEqual(set(expected), build.PUBLIC_KEYS)
                for path in public.iterdir():
                    self.assertNotIn(SECRET, path.read_text())
                self.assertEqual(details['memory_passes'], 0)

    def test_missing_tampered_source_and_failed_cleanup_are_rejected(self):
        for case in ('missing', 'host_bytes', 'source_binding', 'cleanup', 'exit_code',
                     'source_arm', 'range_missing', 'range_state', 'range_reference', 'range_connection'):
            with self.subTest(case=case):
                run = self.root / case
                bindings = fixture(run)
                if case == 'missing':
                    (run / 'correctness/fixture_manifest.json').unlink()
                elif case == 'host_bytes':
                    with (run / 'host/connection.json').open('ab') as stream:
                        stream.write(b' ')
                elif case == 'source_binding':
                    bindings = copy.deepcopy(bindings)
                    bindings['actual_sources']['fixture_source.py'] = 'd' * 64
                elif case in ('cleanup', 'exit_code'):
                    path = run / 'runner_cleanup.json'
                    value = json.loads(path.read_text())
                    if case == 'cleanup':
                        value['cleanup'][0]['forced'] = True
                    else:
                        value['exit_code'] = 1
                    write_json(path, value)
                else:
                    path = run / 'correctness/correctness_receipt.json'
                    value = json.loads(path.read_text())
                    if case == 'source_arm':
                        value['source_arm'] = 'baseline'
                    elif case == 'range_missing':
                        value['range_audit']['cases'].pop()
                    elif case == 'range_state':
                        value['range_audit']['cases'][0]['error_transaction_state'] = 'IDLE'
                    elif case == 'range_reference':
                        value['range_audit']['cases'][0]['recovered_sha256'] = 'f'*64
                    else:
                        value['range_audit']['cases'][0]['connection_closed'] = False
                    proof_sha = write_json(path, value)
                    life_path = run / 'native_lifecycle.json'
                    life = json.loads(life_path.read_text())
                    life['correctness_receipt_sha256'] = proof_sha
                    write_json(life_path, life)
                with self.assertRaises((ValueError, RuntimeError, OSError)):
                    build.validate_run(run, bindings)

    def test_private_chunks_reconstruct_exact_allowlisted_bytes(self):
        run = self.root / 'run'
        fixture(run)
        captured = io.StringIO()
        with redirect_stdout(captured):
            build.preserve_receipts(run)
        prefix = '[NATIVE_BUILD_RECEIPT] '
        lines = captured.getvalue().splitlines()
        self.assertTrue(all(line.startswith(prefix) for line in lines))
        records = [json.loads(line[len(prefix):]) for line in lines]
        self.assertEqual(records[-1]['kind'], 'complete')
        self.assertEqual(sum(r['kind'] == 'complete' for r in records), 1)
        self.assertEqual(records[-1]['version'], build.VERSION)
        self.assertEqual({r['path'] for r in records[-1]['files']}, set(build.RECEIPTS))
        self.assertEqual(len(records[-1]['files']), 6)
        for name in build.RECEIPTS:
            raw = (run / name).read_bytes()
            chunks = [r for r in records if r.get('path') == name and r['kind'] == 'chunk']
            done = [r for r in records if r.get('path') == name and r['kind'] == 'file_complete']
            self.assertEqual(len(done), 1)
            self.assertEqual([r['index'] for r in chunks], list(range(len(chunks))))
            self.assertTrue(all(len(r['data']) <= 2048 for r in chunks))
            self.assertEqual(base64.b64decode(''.join(r['data'] for r in chunks), validate=True), raw)
            expected = dict(path=name, bytes=len(raw), sha256=digest(raw), chunks=len(chunks))
            self.assertEqual(done[0], dict(expected, kind='file_complete'))
            self.assertIn(expected, records[-1]['files'])
            for chunk in chunks:
                self.assertEqual({k: chunk[k] for k in expected}, expected)
        self.assertGreater(sum(r.get('path') == 'preflight.json' and r['kind'] == 'chunk'
                               for r in records), 1)
        self.assertNotIn('PG_VERSION', captured.getvalue())
        self.assertNotIn('unlisted-private.log', captured.getvalue())

    def test_publication_and_receipt_path_size_guards(self):
        run = self.root / 'run'
        bindings = fixture(run)
        public_summary = summary(build.validate_run(run, bindings))
        public = self.root / 'public'
        build.publish_summary(public, public_summary)
        with self.assertRaises(ValueError):
            build.publish_summary(public, public_summary)
        link = self.root / 'linked-parent'
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            build.publish_summary(link / 'new-public', public_summary)
        oversized_summary = dict(public_summary, checked_at_utc='x' * 9000)
        with self.assertRaises(ValueError):
            build.publish_summary(self.root / 'oversized-public', oversized_summary)
        receipt = run / 'preflight.json'
        moved = run / 'original-preflight.json'
        receipt.rename(moved)
        receipt.symlink_to(moved)
        with self.assertRaises(ValueError):
            build.preserve_receipts(run)
        receipt.unlink()
        receipt.write_bytes(b'x' * (build.MAX_RECEIPT_BYTES + 1))
        captured = io.StringIO()
        with redirect_stdout(captured), self.assertRaises(ValueError):
            build.preserve_receipts(run)
        self.assertEqual(captured.getvalue(), '')
        with self.assertRaises(ValueError):
            build.receipt_bytes(run, 'host/data/PG_VERSION')


if __name__ == '__main__':
    unittest.main()
