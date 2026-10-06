"""Offline persistence invariants and optional isolated PostgreSQL integration.

The opt-in database test refuses configured production URLs and creates/drops
only a uniquely named disposable local test database.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from threading import Barrier, RLock
import unittest
from unittest.mock import patch
from uuid import uuid4

import u21_experimental_store as store

BASE = datetime(2026, 9, 25, 9, 0, tzinfo=timezone.utc)
DECISION = BASE + timedelta(minutes=15)
ENTRY = DECISION + timedelta(minutes=1)
NOW = ENTRY + timedelta(seconds=10)
CONFIG = 'frozen-u21-test-config'


def bar(when=ENTRY, *, open=100, high=100.2, low=99, close=100):
    return dict(open_at=when, open=open, high=high, low=low, close=close)


class Rows:
    def __init__(self, row=None): self.row = row
    def fetchone(self): return deepcopy(self.row)


class MemoryDatabase:
    def __init__(self):
        self.values, self.calls, self.lock = {}, [], RLock()

    def connect(self, database_url=None):
        database = self
        class Connection:
            def __enter__(self):
                database.lock.acquire()
                self.before = deepcopy(database.values)
                return self
            def __exit__(self, exc_type, exc, tb):
                if exc_type: database.values = self.before
                database.lock.release()
            def execute(self, sql, params=None):
                database.calls.append((sql, params))
                if sql.startswith('SELECT pg_advisory_xact_lock'): return Rows()
                if sql.startswith('INSERT INTO bot_settings'):
                    database.values.setdefault(params[0], params[1]); return Rows()
                if sql.startswith('SELECT value FROM bot_settings'):
                    return Rows({'value': database.values[params[0]]} if params[0] in database.values else None)
                if sql.startswith('UPDATE bot_settings'):
                    database.values[params[1]] = params[0]; return Rows()
                if sql.startswith('SELECT clock_timestamp()'): return Rows({'now': BASE})
                raise AssertionError('Unexpected SQL: ' + sql)
        return Connection()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.db = MemoryDatabase()
        self.mock = patch.object(store, '_connect', self.db.connect)
        self.mock.start(); self.addCleanup(self.mock.stop)
        store.initialize_scope('scope', BASE, config_sha256=CONFIG)

    def reserve(self, decision=DECISION, now=NOW, scope='scope'):
        return store.reserve_signal(scope, decision, decision+timedelta(minutes=1), 100,
                                    {'frozen': True}, now, text='frozen experimental message', config_sha256=CONFIG)

    def state(self): return store.snapshot('scope')

    def test_explicit_activation_restart_and_configuration_fences(self):
        self.assertIsNone(store.snapshot('missing'))
        with self.assertRaisesRegex(ValueError, 'not initialized'):
            self.reserve(scope='missing')
        with self.assertRaisesRegex(ValueError, 'configuration hash'):
            store.initialize_scope('scope', BASE, config_sha256='new-unreviewed-config')
        self.reserve()
        restored = store.initialize_scope('scope', NOW+timedelta(days=3), config_sha256=CONFIG)
        self.assertEqual(restored['activated_at'], store.iso(BASE))
        self.assertIsNotNone(restored['active'])
        self.assertEqual(restored['intents'][0]['status'], 'EXPIRED')
        self.assertTrue(all('CREATE ' not in sql and 'ALTER ' not in sql for sql, _ in self.db.calls))

    def test_old_slot_duplicate_no_match_and_no_backlog(self):
        self.assertEqual(self.reserve(BASE)['status'], 'BEFORE_ACTIVATION')
        self.assertEqual(self.reserve(now=ENTRY-timedelta(seconds=1))['status'], 'ENTRY_NOT_AVAILABLE')
        self.assertIsNone(self.state()['decision_cursor'])
        self.assertEqual(self.reserve(now=ENTRY+store.SIGNAL_TTL)['status'], 'STALE_SIGNAL')
        self.assertEqual(self.reserve()['status'], 'ALREADY_PROCESSED')
        later = DECISION+timedelta(minutes=15)
        store.record_no_signal('scope', later, later+timedelta(minutes=1), reason='NO_MATCH', config_sha256=CONFIG)
        self.assertEqual(self.reserve(later, later+timedelta(minutes=1, seconds=1))['status'], 'ALREADY_PROCESSED')
        self.assertIsNone(self.state()['active'])

    def test_concurrent_reservation_and_claim_exactly_one(self):
        barrier = Barrier(8)
        def reserve(_): barrier.wait(timeout=5); return self.reserve()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(reserve, range(8)))
        self.assertEqual(sum(r['status'] == 'RESERVED' for r in results), 1)
        barrier = Barrier(8)
        def claim(_): barrier.wait(timeout=5); return store.claim_pending('scope', NOW, config_sha256=CONFIG)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = [r for r in pool.map(claim, range(8)) if r]
        self.assertEqual(len(results), 1)
        self.assertEqual(len(self.state()['intents']), 1)
        self.assertTrue(results[0]['attempt_token'])

    def test_active_suppression_advances_cursor_and_cannot_replay(self):
        self.reserve()
        later = DECISION+timedelta(minutes=15)
        self.assertEqual(self.reserve(later, later+timedelta(minutes=1, seconds=1))['status'], 'ACTIVE_POSITION')
        store.advance_position('scope', [bar(high=100.5)], later+timedelta(minutes=2))
        self.assertEqual(self.reserve(later, later+timedelta(minutes=1, seconds=2))['status'], 'ALREADY_PROCESSED')

    def test_unknown_restart_and_explicit_failure_never_free_active_capacity(self):
        for terminal in ('UNKNOWN', 'FAILED', 'DELIVERED'):
            with self.subTest(terminal=terminal):
                scope = terminal
                store.initialize_scope(scope, BASE, config_sha256=CONFIG)
                self.reserve(scope=scope)
                claimed = store.claim_pending(scope, NOW)
                token, identity = claimed['attempt_token'], claimed['intent_id']
                self.assertFalse(store.finish_attempt(scope, identity, 'wrong', terminal, NOW,
                                                     message_id=1 if terminal == 'DELIVERED' else None))
                self.assertTrue(store.finish_attempt(scope, identity, token, terminal, NOW,
                                                    message_id=1 if terminal == 'DELIVERED' else None))
                self.assertFalse(store.finish_attempt(scope, identity, token, 'UNKNOWN', NOW))
                self.assertIsNone(store.claim_pending(scope, NOW+timedelta(minutes=1)))
                self.assertIsNotNone(store.snapshot(scope)['active'])
        self.reserve()
        store.claim_pending('scope', NOW)
        restored = store.initialize_scope('scope', NOW+store.ORPHAN_TIMEOUT, config_sha256=CONFIG)
        self.assertEqual(restored['intents'][0]['status'], 'UNKNOWN')
        self.assertIsNotNone(restored['active'])
        self.assertIsNone(store.claim_pending('scope', NOW+timedelta(days=1)))

    def test_unattempted_expiry_keeps_position_and_no_resend(self):
        self.reserve()
        self.assertIsNone(store.claim_pending('scope', ENTRY+store.SIGNAL_TTL))
        self.assertEqual(self.state()['intents'][0]['status'], 'EXPIRED')
        self.assertIsNotNone(self.state()['active'])

    def test_provisional_delivery_veto_never_closes_or_resets_attempts(self):
        position = self.reserve()['position']
        self.assertEqual(store.cancel_pending('scope', 'wrong', NOW), 0)
        self.assertEqual(store.cancel_pending('scope', position['position_id'], NOW,
                                             reason='BARRIER_ALREADY_OBSERVED_BEFORE_SEND'), 1)
        self.assertIsNotNone(self.state()['active'])
        self.assertIsNone(store.claim_pending('scope', NOW))
        self.assertEqual(self.state()['intents'][0]['status'], 'CANCELLED')
        scope = 'already-claimed'
        store.initialize_scope(scope, BASE, config_sha256=CONFIG)
        self.reserve(scope=scope)
        claimed = store.claim_pending(scope, NOW)
        self.assertEqual(store.cancel_pending(scope, claimed['position_id'], NOW), 0)
        self.assertEqual(store.snapshot(scope)['intents'][0]['status'], 'IN_FLIGHT')

    def test_contiguous_closed_bar_requirement_gap_repair_and_id_guard(self):
        created = self.reserve()['position']
        pos_id = created['position_id']
        result = store.advance_position('scope', [bar(high=100.5)], ENTRY+timedelta(seconds=59), position_id=pos_id)
        self.assertEqual(result['status'], 'AWAITING_CLOSED_BAR')
        self.assertEqual(result['processed_bars'], 0)
        result = store.advance_position('scope', [bar(ENTRY+timedelta(minutes=1), high=100.5)], ENTRY+timedelta(minutes=2), position_id=pos_id)
        self.assertEqual(result['status'], 'PRICE_GAP')
        self.assertIsNotNone(self.state()['active'])
        self.assertEqual(store.advance_position('scope', [bar(high=100.5)], ENTRY+timedelta(minutes=1), position_id='wrong')['status'], 'POSITION_CHANGED')
        result = store.advance_position('scope', [bar(), bar(ENTRY+timedelta(minutes=1), high=100.5)], ENTRY+timedelta(minutes=2), position_id=pos_id)
        self.assertEqual(result['status'], 'CLOSED')
        self.assertEqual(result['position']['outcome'], 'SL')
        self.assertEqual(result['position']['exit_at'], store.iso(ENTRY+timedelta(minutes=2)))
        self.assertIsNone(self.state()['active'])
        self.assertEqual(self.state()['intents'][0]['status'], 'EXPIRED')
        self.assertEqual(store.advance_position('scope', [], NOW, position_id=pos_id)['status'], 'NO_ACTIVE_POSITION')

    def test_exact_end_timestamp_and_strict_decision_after_exit(self):
        self.reserve()
        next_decision = DECISION+timedelta(minutes=15)
        rows = [bar(ENTRY+timedelta(minutes=n)) for n in range(13)]
        rows.append(bar(next_decision-timedelta(minutes=1), high=100.5))
        result = store.advance_position('scope', rows, next_decision)
        self.assertEqual(result['position']['exit_at'], store.iso(next_decision))
        self.assertEqual(self.reserve(next_decision, next_decision+timedelta(minutes=1))['status'], 'DECISION_NOT_AFTER_LAST_EXIT')
        later = next_decision+timedelta(minutes=15)
        self.assertEqual(self.reserve(later, later+timedelta(minutes=1))['status'], 'RESERVED')

    def test_ambiguous_bar_holds_forever_but_open_gap_resolves(self):
        self.reserve()
        result = store.advance_position('scope', [bar(high=102, low=90)], ENTRY+timedelta(minutes=1))
        self.assertEqual(result['status'], 'AMBIGUOUS')
        self.assertIsNone(store.claim_pending('scope', ENTRY+timedelta(minutes=1)))
        self.assertEqual(self.state()['intents'][0]['status'], 'CANCELLED')
        self.assertIsNone(self.state()['last_exit_at'])
        self.assertEqual(store.advance_position('scope', [bar(ENTRY+timedelta(minutes=1), low=90)], ENTRY+timedelta(days=1))['status'], 'AMBIGUOUS')
        self.assertIsNotNone(self.state()['active'])
        for opening, expected, exit_price in [(101, 'SL', 101), (91, 'TP', 92)]:
            scope = str(opening)
            store.initialize_scope(scope, BASE)
            store.reserve_signal(scope, DECISION, ENTRY, 100, {}, NOW)
            result = store.advance_position(scope, [bar(open=opening, high=102, low=90)], ENTRY+timedelta(minutes=1))
            self.assertEqual(result['position']['outcome'], expected)
            self.assertEqual(result['position']['exit_price'], exit_price)

    def test_bad_prices_and_overcapacity_roll_back_atomically(self):
        before = self.state()
        for bad in (float('nan'), float('inf'), 0, -1, True):
            with self.assertRaises(ValueError):
                store.reserve_signal('scope', DECISION, ENTRY, bad, {}, NOW)
        with patch.object(store, 'MAX_STATE_BYTES', 10):
            with self.assertRaisesRegex(ValueError, 'capacity'):
                self.reserve()
        self.assertEqual(self.state(), before)
        self.reserve()
        before = self.state()
        for rows in ([bar(low=101)], [bar(), bar()], [bar(ENTRY+timedelta(seconds=1))]):
            with self.assertRaises(ValueError):
                store.advance_position('scope', rows, ENTRY+timedelta(minutes=1))
            self.assertEqual(self.state(), before)

    def test_research_float_thresholds_are_not_decimal_recomputed(self):
        position = self.reserve()['position']
        self.assertEqual(position['stop_price'], 100 * 1.005)
        self.assertEqual(position['take_price'], 100 * .92)
        self.assertEqual(float(position['stop_price_decimal']), 100 * 1.005)
        result = store.advance_position('scope', [bar(high=100*1.005)], ENTRY+timedelta(minutes=1))
        self.assertEqual(result['position']['outcome'], 'SL')

    def test_byte_budget_prunes_old_evidence_without_resetting_exposure(self):
        self.reserve()
        state = self.state()
        current_id = state['active']['position_id']
        state['history'] = [{'position_id': 'old-'+str(i), 'features': 'x'*1000} for i in range(128)]
        state['intents'] = [{'intent_id': 'old-intent-'+str(i), 'position_id': 'old-'+str(i),
                             'status': 'DELIVERED', 'payload': 'x'*1000} for i in range(100)] + state['intents']
        with patch.object(store, 'MAX_STATE_BYTES', 12000):
            store._maintain(state, NOW)
            store._encode(state)
        self.assertEqual(state['active']['position_id'], current_id)
        self.assertEqual(state['decision_cursor'], store.iso(DECISION))
        self.assertTrue(any(i['position_id'] == current_id for i in state['intents']))


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        test_url = os.environ['TEST_DATABASE_URL']
        info = conninfo_to_dict(test_url)
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or not (
                info.get('dbname', '').startswith('test_') or info.get('dbname', '').endswith('_test')):
            raise ValueError('Explicit local test database required')
        cls.dbname = 'test_u21_' + uuid4().hex
        cls.admin = psycopg.connect(test_url, autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
        cls.dsn = make_conninfo(test_url, dbname=cls.dbname)
        try:
            with store._connect(cls.dsn) as conn:
                conn.execute('CREATE TABLE bot_settings(key text PRIMARY KEY,value text NOT NULL)')
        except BaseException:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        try: cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
        finally: cls.admin.close()

    def test_concurrent_reserve_claim_and_restart_are_durable(self):
        scope = 'test-' + uuid4().hex
        store.initialize_scope(scope, BASE, config_sha256=CONFIG, database_url=self.dsn)
        barrier = Barrier(6)
        def reserve(_):
            barrier.wait(timeout=5)
            return store.reserve_signal(scope, DECISION, ENTRY, 100, {}, NOW,
                                        config_sha256=CONFIG, database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(reserve, range(6)))
        self.assertEqual(sum(r['status'] == 'RESERVED' for r in results), 1)
        barrier = Barrier(6)
        def claim(_):
            barrier.wait(timeout=5)
            return store.claim_pending(scope, NOW, database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=6) as pool:
            claims = [r for r in pool.map(claim, range(6)) if r]
        self.assertEqual(len(claims), 1)
        restarted = store.initialize_scope(scope, NOW+store.ORPHAN_TIMEOUT,
                                           config_sha256=CONFIG, database_url=self.dsn)
        self.assertEqual(restarted['activated_at'], store.iso(BASE))
        self.assertEqual(restarted['intents'][0]['status'], 'UNKNOWN')
        self.assertIsNotNone(restarted['active'])
        self.assertIsNone(store.claim_pending(scope, NOW+store.ORPHAN_TIMEOUT, database_url=self.dsn))


if __name__ == '__main__':
    unittest.main()
