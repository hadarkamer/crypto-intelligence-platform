"""Ordering and failure controls for the new observer's external I/O boundary."""
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import sys
import unittest
from unittest.mock import patch

import research_liquidity_forward_worker as subject


class FakeEngine:
    def __init__(self, actions, pending=False):
        self.actions = actions
        self.pending = pending
        self.bootstrap = False
        self.policy = subject.build_config()
        self.policy['study_start_utc'] = datetime.now(timezone.utc).isoformat()

    def summary(self):
        return {'bootstrap_complete': self.bootstrap, 'study_start_utc': self.policy['study_start_utc'],
                'policy_sha256': 'a' * 64, 'books': {'DOGE_only': {'active': False, 'reservation': None,
                 'choice_status_counts': {'PLANNED': 1} if self.pending else {}, 'completed_all': {'n': 0}}}}

    def observe_runtime_identity(self, packet):
        self.actions.append('registry')
        if packet['python_implementation'] != 'CPython':
            raise ValueError('Wrong runtime implementation')
        return self.summary()

    def source_code_registry(self):
        return {'a' * 40: {'main.py': 'b' * 64}}

    def eligible_event_keys(self):
        return ()

    def observe_candidates(self, packet):
        self.actions.append('candidates')
        self.bootstrap = True
        return self.summary()

    def observe_quotes(self, packet):
        self.actions.append('quotes')
        return self.summary()

    def close(self):
        self.actions.append('engine_close')


class FakeStore:
    def __init__(self, actions, engine, *, fail_checkpoint=None, acquire=True):
        self.actions, self.engine = actions, engine
        self.fail_checkpoint, self.can_acquire = fail_checkpoint, acquire
        self.journal_sequence = 0

    def acquire(self):
        self.actions.append('acquire')
        return self.can_acquire

    def restore_engine(self, klass):
        self.actions.append('restore')
        return self.engine

    def checkpoint(self, engine, status):
        self.actions.append('checkpoint')
        self.journal_sequence += 1
        if self.journal_sequence == self.fail_checkpoint:
            raise RuntimeError('COMMIT acknowledgement unavailable')

    def update_health(self, status):
        self.actions.append('health')

    def close(self):
        self.actions.append('store_close')


class WorkerTests(unittest.TestCase):
    def worker(self, *, pending=False, fail_checkpoint=None, acquire=True):
        actions = []
        engine = FakeEngine(actions, pending=pending)
        store = FakeStore(actions, engine, fail_checkpoint=fail_checkpoint, acquire=acquire)
        captured = []
        def factory(dsn, study_id, config):
            self.assertEqual(study_id, subject.STUDY_ID)
            self.assertEqual(config['books'], ['DOGE_only'])
            return store
        def source(dsn, policy, template, **kwargs):
            actions.append('source')
            captured.append(copy.deepcopy(kwargs))
            return {'poll_id': 'one', 'request_started_utc': policy['study_start_utc'],
                    'response_received_utc': datetime.now(timezone.utc).isoformat(), 'bootstrap': kwargs['bootstrap']}
        def quote(policy):
            actions.append('quote')
            self.assertEqual(policy['quote_symbols'], {'DOGE': 'DOGEUSDT'})
            return {'packet_id': 'q', 'response_received_utc': datetime.now(timezone.utc).isoformat()}
        worker = subject.LiquidityForwardWorker(store_factory=factory, engine_class=FakeEngine,
                                                candidate_fetch=source, quote_fetch=quote)
        worker._dsn = 'test-DSN-never-public'
        worker._log = lambda **kwargs: None
        worker._health['runtime_commit'] = 'a' * 40
        return worker, engine, store, actions, captured

    def test_registration_registry_and_restart_checkpoint_precede_first_source(self):
        worker, _, _, actions, _ = self.worker()
        worker._cycle_owned()
        self.assertEqual(actions[:5], ['acquire', 'restore', 'registry', 'checkpoint', 'source'])
        self.assertEqual(worker.status()['study_id'], subject.STUDY_ID)

    def test_source_checkpoint_precedes_quote_request(self):
        worker, _, _, actions, _ = self.worker(pending=True)
        worker._cycle_owned()
        self.assertEqual(actions, ['acquire', 'restore', 'registry', 'checkpoint', 'source',
                                  'candidates', 'checkpoint', 'quote', 'quotes', 'checkpoint', 'health'])

    def test_idle_book_avoids_quote_requests(self):
        worker, _, _, actions, _ = self.worker()
        worker._cycle_owned()
        self.assertNotIn('quote', actions)
        self.assertEqual(worker.status()['skipped_quote_cycles'], 1)
        self.assertEqual(worker.status()['last_quote_received_utc'], None)

    def test_registry_checkpoint_failure_prevents_all_external_reads(self):
        worker, _, _, actions, _ = self.worker(pending=True, fail_checkpoint=1)
        with self.assertRaises(RuntimeError):
            worker._cycle_owned()
        self.assertNotIn('source', actions)
        self.assertNotIn('quote', actions)
        self.assertIsNone(worker._store)
        self.assertIsNone(worker._engine)

    def test_source_checkpoint_failure_prevents_quote_and_discards_engine(self):
        worker, _, _, actions, _ = self.worker(pending=True, fail_checkpoint=2)
        with self.assertRaises(RuntimeError):
            worker._cycle_owned()
        self.assertNotIn('quote', actions)
        self.assertIn('engine_close', actions)
        self.assertIn('store_close', actions)
        self.assertEqual(worker.status()['state'], 'RETRYING')
        self.assertNotIn('test-DSN', json.dumps(worker.status()))

    def test_quote_checkpoint_failure_discards_before_next_cycle_read(self):
        worker, _, _, actions, _ = self.worker(pending=True, fail_checkpoint=3)
        with self.assertRaises(RuntimeError):
            worker._cycle_owned()
        self.assertEqual(actions.count('source'), 1)
        self.assertEqual(actions.count('quote'), 1)
        self.assertIsNone(worker._engine)

    def test_standby_never_fetches_sources_or_quotes(self):
        worker, _, _, actions, _ = self.worker(pending=True, acquire=False)
        worker._cycle_owned()
        self.assertNotIn('source', actions)
        self.assertNotIn('quote', actions)
        self.assertEqual(worker.status()['state'], 'STANDBY')

    def test_bootstrap_and_registry_mapping_reach_actual_source_adapter(self):
        worker, _, _, actions, captured = self.worker()
        worker._cycle_owned()
        self.assertTrue(captured[0]['bootstrap'])
        self.assertIn('a' * 40, captured[0]['code_registry'])
        actions.clear()
        worker._cycle_owned()
        self.assertFalse(captured[1]['bootstrap'])
        self.assertEqual(actions[0], 'source')

    def test_real_identity_uses_exact_platform_spelling_and_source_bytes(self):
        worker, _, _, _, _ = self.worker()
        packet = worker._runtime_identity()
        self.assertEqual(packet['python_implementation'], platform.python_implementation())
        self.assertEqual(packet['python_implementation'], 'CPython')
        self.assertEqual(packet['python_version'], sys.version.split()[0])
        self.assertEqual(packet['protocol_sha256'], subject.PROTOCOL_SHA256)
        self.assertEqual(set(packet['code_versions']), set(subject.core.SOURCE_CODE_FILES))
        root = Path(subject.__file__).resolve().parent
        for name, sha in packet['code_versions'].items():
            self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), sha)

    def test_invalid_runtime_commit_stops_before_registry_or_source(self):
        worker, _, _, actions, _ = self.worker()
        worker._health['runtime_commit'] = 'short'
        with self.assertRaises(ValueError):
            worker._cycle_owned()
        self.assertNotIn('source', actions)
        self.assertNotIn('registry', actions)

    def test_config_has_no_fake_epoch_or_activation(self):
        config = subject.build_config()
        self.assertNotIn('study_start_utc', config)
        self.assertNotIn('activation', config)
        self.assertEqual(config['version'], subject.STUDY_ID)
        self.assertEqual(config['protocol_sha256'], subject.PROTOCOL_SHA256)
        self.assertEqual(config['scopes'][0]['direction'], 'SHORT')
        self.assertEqual(config['scopes'][0]['window_minutes'], 720)

    def test_native_extractor_drift_prevents_registration(self):
        real_sha = subject._sha
        def drift(path):
            return '0' * 64 if Path(path).name == 'research_ordered_question_catalog.py' else real_sha(path)
        with patch.object(subject, '_sha', side_effect=drift), self.assertRaises(ValueError):
            subject.build_config()


if __name__ == '__main__':
    unittest.main(verbosity=2)
