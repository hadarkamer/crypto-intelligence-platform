"""Offline outbox invariants, plus opt-in isolated local PostgreSQL tests.

No configured production URL is ever used. The optional database class accepts
only TEST_DATABASE_URL pointing to a local/CI test-named database and creates
its own disposable database, which is the only database it drops.
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

import manual_formula_alert_store as store
from manual_formula_alert_selftest import fixture

BASE = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


def pair(identifier=1, minute=1, *, scan=None, symbol='BTC', direction='LONG'):
    event, features = fixture(symbol, direction)
    event.update(event_id=identifier, alert_time_utc=BASE + timedelta(minutes=minute))
    event['engine_snapshot']['watch_scan_id'] = scan or 'scan-' + str(identifier)
    return event, features


class Rows:
    def __init__(self, row=None): self.row = row
    def fetchone(self): return deepcopy(self.row)


class MemoryDatabase:
    """Atomic row-locked transaction fake for the real store functions."""
    def __init__(self, now=BASE):
        self.values = {}
        self.now = now
        self.lock = RLock()
        self.calls = []

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
                if sql.startswith('INSERT INTO bot_settings'):
                    database.values.setdefault(params[0], params[1])
                    return Rows()
                if sql.startswith('SELECT value FROM bot_settings'):
                    return Rows({'value': database.values[params[0]]})
                if sql.startswith('UPDATE bot_settings'):
                    database.values[params[1]] = params[0]
                    return Rows()
                if sql.startswith('SELECT clock_timestamp()'):
                    return Rows({'now': database.now})
                if sql.startswith("SELECT to_regclass('bot_settings')"):
                    return Rows({'ready': True})
                raise AssertionError('Unexpected SQL boundary: ' + sql)
        return Connection()

    def read(self, chat_id):
        return json.loads(self.values[store.key_for(chat_id)])

    def write(self, chat_id, state):
        self.values[store.key_for(chat_id)] = store._encode(state)


class ReducerTests(unittest.TestCase):
    def test_activation_fence_and_freshness_are_strict(self):
        state = store._initial(BASE)
        excluded = [pair(1, -1), pair(2, 0), pair(3, 3)]
        self.assertEqual(store.record_events(state, excluded, BASE + timedelta(minutes=2)), 0)
        self.assertEqual(state['receipts'], {})
        self.assertEqual(store.record_events(state, [pair(4, 1)], BASE + timedelta(minutes=11)), 0)
        self.assertEqual(store.record_events(state, [pair(5, 1)], BASE + timedelta(minutes=10, seconds=59)), 4)
        self.assertEqual(state['counts'], {'checked': 1, 'created': 4})

    def test_late_lower_ids_are_not_lost_to_a_serial_cursor(self):
        state = store._initial(BASE)
        self.assertEqual(store.record_events(state, [pair(900, 2)], BASE + timedelta(minutes=3)), 4)
        self.assertEqual(store.record_events(state, [pair(3, 1)], BASE + timedelta(minutes=3)), 4)
        self.assertEqual(set(state['receipts']), {'900', '3'})
        self.assertEqual(store.record_events(state, [pair(900, 2), pair(3, 1)], BASE + timedelta(minutes=4)), 0)

    def test_sibling_dedup_is_per_rule_scan_coin_and_direction(self):
        state = store._initial(BASE)
        now = BASE + timedelta(minutes=3)
        self.assertEqual(store.record_events(state, [pair(1, scan='shared')], now), 4)
        self.assertEqual(store.record_events(state, [pair(2, 1.1, scan='shared')], now), 0)
        self.assertEqual(store.record_events(state, [pair(3, 1.2, scan='shared', direction='SHORT')], now), 4)
        self.assertEqual(store.record_events(state, [pair(4, 1.3, scan='shared', symbol='BNB')], now), 3)
        self.assertEqual(store.record_events(state, [pair(5, 1.4, scan='different')], now), 4)
        self.assertEqual(state['counts'], {'checked': 5, 'created': 15})

    def test_minute_fallback_dedups_without_a_scan_id(self):
        state = store._initial(BASE)
        first, second = pair(1), pair(2, 1.5)
        for event, _ in (first, second): event['engine_snapshot'].pop('watch_scan_id')
        self.assertEqual(store.record_events(state, [first, second], BASE + timedelta(minutes=2)), 4)

    def test_multiple_rules_for_same_event_are_preserved_and_text_frozen(self):
        state = store._initial(BASE)
        self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=2)), 4)
        self.assertEqual({i['payload']['rule_id'] for i in state['intents']}, set(store.rules.RULES))
        self.assertTrue(all(i['text'] == i['payload']['text'] for i in state['intents']))
        self.assertEqual(len({i['intent_id'] for i in state['intents']}), 4)

    def test_pruned_receipts_cannot_replay_expired_sources(self):
        state = store._initial(BASE)
        store.record_events(state, [pair()], BASE + timedelta(minutes=2))
        later = BASE + timedelta(minutes=22)
        self.assertEqual(store.record_events(state, [pair()], later), 0)
        self.assertEqual(state['receipts'], {})
        self.assertEqual(state['dedup'], {})
        self.assertEqual({i['status'] for i in state['intents']}, {'EXPIRED'})
        self.assertEqual(state['counts']['created'], 4)
        store.prune(state, later + timedelta(hours=1, seconds=1))
        self.assertEqual(state['intents'], [])
        self.assertEqual(store.record_events(state, [pair()], later + timedelta(hours=2)), 0)

    def test_orphan_attempt_becomes_unknown_never_pending(self):
        state = store._initial(BASE)
        store.record_events(state, [pair()], BASE + timedelta(minutes=2))
        item = state['intents'][0]
        item.update(status='IN_FLIGHT', attempted_at=store.iso(BASE + timedelta(minutes=2)), attempt_token='one')
        store.prune(state, BASE + timedelta(minutes=4))
        self.assertEqual(item['status'], 'UNKNOWN')
        self.assertEqual(item['attempt_token'], 'one')
        store.prune(state, BASE + timedelta(minutes=5))
        self.assertEqual(state['counts']['unknown'], 1)

    def test_capacity_and_version_are_frozen(self):
        state = store._initial(BASE)
        with patch.object(store, 'MAX_RECEIPTS', 1):
            store.record_events(state, [pair(1)], BASE + timedelta(minutes=2))
            with self.assertRaisesRegex(ValueError, 'capacity'):
                store.record_events(state, [pair(2)], BASE + timedelta(minutes=2))
        with patch.object(store, 'MAX_STATE_BYTES', 1), self.assertRaisesRegex(ValueError, 'capacity'):
            store._encode(store._initial(BASE))

    def test_sequence_recovery_adds_only_missing_rule_and_keeps_other_dedup(self):
        for status in ('SOURCE_UNAVAILABLE', 'SOURCE_OVERFLOW'):
            with self.subTest(status=status):
                state = store._initial(BASE)
                event, features = pair()
                unavailable = {**features, 'sequence.capture_status': status}
                unavailable.pop('sequence.30m.price_oi.entry_ordinal')
                now = BASE + timedelta(minutes=2)
                self.assertEqual(store.record_events(state, [(event, unavailable)], now), 3)
                self.assertEqual(set(state['retry']), {'1'})
                self.assertEqual(store.record_events(state, [(event, unavailable)], now + timedelta(seconds=1)), 0)
                self.assertEqual(store.record_events(state, [(event, features)], now + timedelta(seconds=2)), 1)
                self.assertEqual(state['retry'], {})
                self.assertEqual(len(state['intents']), 4)
                self.assertEqual({i['payload']['rule_id'] for i in state['intents']}, set(store.rules.RULES))
                self.assertEqual(store.record_events(state, [(event, features)], now + timedelta(seconds=3)), 0)

    def test_sequence_retry_expires_without_replaying_or_counting_twice(self):
        state = store._initial(BASE)
        event, features = pair(); features['sequence.capture_status'] = 'SOURCE_UNAVAILABLE'
        store.record_events(state, [(event, features)], BASE + timedelta(minutes=2))
        store.prune(state, BASE + timedelta(minutes=11))
        self.assertEqual(state['retry'], {})
        self.assertEqual(state['counts']['sequence_retry_expired'], 1)
        self.assertEqual(store.record_events(state, [pair()], BASE + timedelta(minutes=11)), 0)
        store.prune(state, BASE + timedelta(minutes=12))
        self.assertEqual(state['counts']['sequence_retry_expired'], 1)

    def test_sequence_retry_only_for_eligible_rule_coins_and_current_score(self):
        for symbol, score in (('HYPE', 65), ('ZEC', 65), ('BTC', 64)):
            state = store._initial(BASE)
            event, features = pair(symbol=symbol)
            features.update({'price_oi.aligned_score': score, 'sequence.capture_status': 'SOURCE_UNAVAILABLE'})
            store.record_events(state, [(event, features)], BASE + timedelta(minutes=2))
            self.assertEqual(state['retry'], {})


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.db = MemoryDatabase()
        self.connection = patch.object(store, '_connect', self.db.connect)
        self.connection.start(); self.addCleanup(self.connection.stop)
        store.initialize_scope(1, BASE)

    def collect(self, pairs=None, now=None):
        with patch.object(store.source, 'load_batch', return_value=(pairs or [pair()], {'selected_events': 1})):
            return store.collect(1, now or BASE + timedelta(minutes=2))

    def test_first_activation_clock_is_preserved_for_each_scope(self):
        original = store.initialize_scope(1, BASE + timedelta(days=1))
        self.assertEqual(original['activated_at'], store.iso(BASE))
        second = store.initialize_scope(2)
        self.assertEqual(second['activated_at'], store.iso(self.db.now))
        self.assertNotEqual(store.key_for(1), store.key_for(2))
        self.assertTrue(store.schema_ready())

    def test_collect_passes_receipts_and_activation_not_max_id(self):
        self.collect([pair(999)])
        with patch.object(store.source, 'load_batch', return_value=([], {})) as load:
            store.collect(1, BASE + timedelta(minutes=3))
        self.assertEqual(load.call_args.kwargs['processed_ids'], [999])
        self.assertEqual(load.call_args.kwargs['activated_at'], BASE)
        self.assertEqual(load.call_args.kwargs['limit'], 24)

    def test_sequence_retry_queue_is_bounded_and_rotates_oldest_attempts(self):
        state = self.db.read(1); pairs = []
        for identifier in range(1, 13):
            event, features = pair(identifier)
            features['sequence.capture_status'] = 'SOURCE_UNAVAILABLE'
            pairs.append((event, features))
        store.record_events(state, pairs, BASE + timedelta(minutes=2))
        self.db.write(1, state)
        with patch.object(store.source, 'load_batch', side_effect=[([], {}), (pairs[:8], {})]) as load:
            store.collect(1, BASE + timedelta(minutes=3))
        self.assertEqual(load.call_count, 2)
        first, retry = load.call_args_list
        self.assertEqual(first.kwargs['limit'], 24)
        self.assertNotIn('retry_event_ids', first.kwargs)
        self.assertEqual(retry.kwargs['retry_event_ids'], list(range(1, 9)))
        self.assertEqual(retry.kwargs['limit'], 8)
        self.assertEqual(retry.kwargs['processed_ids'], [])
        with patch.object(store.source, 'load_batch', side_effect=[([], {}), ([], {})]) as load:
            store.collect(1, BASE + timedelta(minutes=4))
        self.assertEqual(load.call_args.kwargs['retry_event_ids'], [9, 10, 11, 12, 1, 2, 3, 4])

    def test_capacity_failure_rolls_back_receipts_and_messages_together(self):
        before = deepcopy(self.db.values)
        with patch.object(store, 'MAX_RECEIPTS', 0), self.assertRaisesRegex(ValueError, 'capacity'):
            self.collect()
        self.assertEqual(self.db.values, before)
        with patch.object(store.rules, 'render_message', side_effect=RuntimeError('render fixture')):
            with self.assertRaisesRegex(RuntimeError, 'render fixture'): self.collect()
        self.assertEqual(self.db.values, before)

    def test_version_or_ruleset_change_cannot_reuse_scope(self):
        for key in ('version', 'rule_version', 'ruleset_sha256'):
            original = self.db.read(1); changed = deepcopy(original); changed[key] = 'changed'
            self.db.write(1, changed)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'version mismatch'):
                store.initialize_scope(1, BASE + timedelta(minutes=2))
            self.db.write(1, original)

    def test_claim_tokens_terminal_immutability_and_positive_ack(self):
        self.collect()
        when = BASE + timedelta(minutes=2)
        intent = store.claim(1, when)
        self.assertEqual(intent['status'], 'IN_FLIGHT')
        self.assertFalse(store.finish(1, intent['intent_id'], 'wrong', 'DELIVERED', when, message_id=1))
        for value in (None, 0, -1, True, '12'):
            with self.subTest(message_id=value), self.assertRaisesRegex(ValueError, 'Positive Telegram'):
                store.finish(1, intent['intent_id'], intent['attempt_token'], 'DELIVERED', when, message_id=value)
        self.assertTrue(store.finish(1, intent['intent_id'], intent['attempt_token'], 'DELIVERED', when, message_id=12))
        self.assertFalse(store.finish(1, intent['intent_id'], intent['attempt_token'], 'UNKNOWN', when))
        self.assertEqual(self.db.read(1)['intents'][0]['status'], 'DELIVERED')
        with self.assertRaisesRegex(ValueError, 'Invalid terminal'):
            store.finish(1, intent['intent_id'], intent['attempt_token'], 'PENDING', when)

    def test_claims_reserve_only_one_and_expired_are_not_claimed(self):
        self.collect()
        first = store.claim(1, BASE + timedelta(minutes=2))
        second = store.claim(1, BASE + timedelta(minutes=2))
        self.assertNotEqual(first['intent_id'], second['intent_id'])
        self.assertEqual(sum(i['status'] == 'IN_FLIGHT' for i in self.db.read(1)['intents']), 2)
        self.assertIsNone(store.claim(1, BASE + timedelta(minutes=11)))
        self.assertEqual({i['status'] for i in self.db.read(1)['intents']}, {'UNKNOWN', 'EXPIRED'})


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        test_url = os.environ['TEST_DATABASE_URL']; info = conninfo_to_dict(test_url)
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or not (
                info.get('dbname', '').startswith('test_') or info.get('dbname', '').endswith('_test')):
            raise ValueError('Explicit local test database required')
        cls.dbname = 'test_manual_formula_' + uuid4().hex
        cls.admin = psycopg.connect(test_url, autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
        cls.dsn = make_conninfo(test_url, dbname=cls.dbname)
        try:
            with store._connect(cls.dsn) as conn:
                conn.execute('CREATE TABLE bot_settings(key text PRIMARY KEY,value text NOT NULL)')
                conn.execute('CREATE TABLE research_events(event_id bigint PRIMARY KEY)')
        except BaseException:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        try: cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
        finally: cls.admin.close()

    def setUp(self):
        self.chat = int(uuid4().hex[:12], 16)
        store.initialize_scope(self.chat, BASE, database_url=self.dsn)

    def state(self):
        with store._connect(self.dsn) as conn:
            return json.loads(conn.execute('SELECT value FROM bot_settings WHERE key=%s',
                                           (store.key_for(self.chat),)).fetchone()['value'])

    def test_concurrent_collect_and_claim_commit_each_intent_once(self):
        self.assertTrue(store.schema_ready(database_url=self.dsn))
        barrier = Barrier(4)
        def collect(_):
            barrier.wait(timeout=5)
            return store.collect(self.chat, BASE + timedelta(minutes=2), database_url=self.dsn)
        with patch.object(store.source, 'load_batch', return_value=([pair()], {})):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(collect, range(4)))
        self.assertEqual(sum(r['created_intents'] for r in results), 4)
        claim_barrier = Barrier(5)
        def claim(_):
            claim_barrier.wait(timeout=5)
            return store.claim(self.chat, BASE + timedelta(minutes=2), database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=5) as pool:
            claims = [i for i in pool.map(claim, range(5)) if i is not None]
        self.assertEqual(len(claims), 4)
        self.assertEqual(len({i['intent_id'] for i in claims}), 4)
        self.assertIsNone(store.claim(self.chat, BASE + timedelta(minutes=2), database_url=self.dsn))
        for item in claims:
            self.assertTrue(store.finish(self.chat, item['intent_id'], item['attempt_token'], 'UNKNOWN',
                                          BASE + timedelta(minutes=2), database_url=self.dsn))
        self.assertIsNone(store.claim(self.chat, BASE + timedelta(minutes=3), database_url=self.dsn))

    def test_failed_collect_is_atomic_and_activation_survives_restart(self):
        before = self.state()
        with patch.object(store.source, 'load_batch', return_value=([pair()], {})), patch.object(store, 'MAX_RECEIPTS', 0):
            with self.assertRaisesRegex(ValueError, 'capacity'):
                store.collect(self.chat, BASE + timedelta(minutes=2), database_url=self.dsn)
        self.assertEqual(self.state(), before)
        restored = store.initialize_scope(self.chat, BASE + timedelta(days=1), database_url=self.dsn)
        self.assertEqual(restored['activated_at'], store.iso(BASE))


if __name__ == '__main__':
    unittest.main()
