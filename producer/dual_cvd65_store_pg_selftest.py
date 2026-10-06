"""Real PostgreSQL isolation, immutable episodes and one-attempt delivery.

Only TEST_DATABASE_URL is used, guarded to a disposable local/CI database.
No real subscription, provider, bot, research schema or transport is accessed.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from threading import Barrier
import unittest
from unittest.mock import patch
from uuid import uuid4

import dual_cvd65_alert as detector
import dual_cvd65_store as store

BASE = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


def observation(*, symbol='ZEC', status='MATCH', direction='LONG', score=65., close=None):
    close = close or BASE
    signed = abs(score) if direction == 'LONG' else -abs(score)
    return detector._observation(symbol, status=status,
        direction=direction if status == 'MATCH' else None,
        futures=None if status == 'UNKNOWN' else {'score': signed, 'close': close.isoformat()},
        spot=None if status == 'UNKNOWN' else {'score': signed, 'close': close.isoformat()},
        reasons=['UNAVAILABLE'] if status == 'UNKNOWN' else [])


def evaluation(watch, minute, observations=None):
    return {'rule_id': detector.RULE_ID, 'watch_scan_id': watch,
        'source_at_utc': (BASE+timedelta(minutes=minute)).isoformat(),
        'bundle_sha256': detector._digest({'watch': watch, 'minute': minute}),
        'observations': observations if observations is not None else [observation(close=BASE+timedelta(minutes=minute))]}


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class DualCvd65StorePostgresTests(unittest.TestCase):
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
        cls.psycopg = psycopg
        cls.dbname = 'test_dual_cvd65_' + uuid4().hex
        cls.admin = psycopg.connect(test_url, autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
        cls.dsn = make_conninfo(test_url, dbname=cls.dbname)
        try:
            if store.schema_ready(database_url=cls.dsn):
                raise AssertionError('Schema unexpectedly exists')
            migration = (Path(__file__).parent/'migrations/053_dual_cvd65_experimental_watch.sql').read_text()
            with store._connect(cls.dsn) as conn:
                conn.execute(migration, prepare=False)
                conn.execute(migration, prepare=False)
            if not store.schema_ready(database_url=cls.dsn):
                raise AssertionError('Explicit migration did not install schema')
        except BaseException:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        try:
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.dbname)))
        finally:
            cls.admin.close()

    def setUp(self):
        self.scope = 'fixture-' + uuid4().hex
        store.initialize_scope(self.scope, BASE, database_url=self.dsn)

    def cycle(self, watch, minute, observations=None, *, now=None):
        return store.record_cycle(self.scope, evaluation(watch, minute, observations),
            now or BASE+timedelta(minutes=minute, seconds=1), database_url=self.dsn)

    def intents(self):
        with store._connect(self.dsn) as conn:
            return conn.execute('SELECT * FROM dual_cvd65_intents WHERE subscription_scope=%s '
                'ORDER BY source_at_utc,intent_id', (self.scope,)).fetchall()

    def state(self):
        with store._connect(self.dsn) as conn:
            return conn.execute('SELECT state FROM dual_cvd65_scopes WHERE subscription_scope=%s',
                (self.scope,)).fetchone()['state']

    def claim(self, minute, limit=1):
        return store.claim_pending(self.scope, BASE+timedelta(minutes=minute), limit=limit, database_url=self.dsn)

    def test_first_fresh_match_fires_65_reset_unknown_preservation_and_direction_switch(self):
        self.assertEqual(self.cycle('first', 1)['created_intents'], 1)
        self.assertEqual(self.cycle('continuous', 2)['created_intents'], 0)
        self.assertEqual(self.cycle('missing', 3, [observation(status='UNKNOWN')])['counts']['UNKNOWN'], 1)
        self.assertEqual(self.state()['ZEC']['active_direction'], 'LONG')
        self.assertEqual(self.cycle('still-active', 4)['created_intents'], 0)
        reset = self.cycle('reset-at64', 5, [observation(status='NO_MATCH', score=64., close=BASE+timedelta(minutes=5))])
        self.assertEqual(reset['counts']['resets'], 1)
        self.assertIsNone(self.state()['ZEC']['active_direction'])
        self.assertEqual(self.cycle('at65-again', 6)['created_intents'], 1)
        self.assertEqual(self.cycle('switch', 7, [observation(direction='SHORT', close=BASE+timedelta(minutes=7))])['created_intents'], 1)
        self.assertEqual(self.state()['ZEC']['active_direction'], 'SHORT')
        self.assertEqual(self.state()['ZEC']['episode'], 3)
        rows = self.intents()
        self.assertEqual([row['direction'] for row in rows], ['LONG', 'LONG', 'SHORT'])
        self.assertTrue(all('ניסיוני' in row['text'] for row in rows))
        self.assertEqual(rows[-1]['text'], rows[-1]['payload']['text'])
        with store._connect(self.dsn) as conn:
            for name in ('watch_transition_scopes', 'research_events', 'research_watch_scan_formula_samples'):
                self.assertIsNone(conn.execute('SELECT to_regclass(%s) AS relation', (name,)).fetchone()['relation'])

    def test_all_eight_observations_are_retained_but_only_zec_can_notify(self):
        rows = [observation(symbol=symbol, close=BASE+timedelta(minutes=1))
                for symbol in detector.SYMBOLS]
        result = self.cycle('all-eight', 1, rows)
        self.assertEqual((result['counts']['evaluated'], result['counts']['MATCH']), (8, 8))
        self.assertEqual(result['created_intents'], 1)
        self.assertEqual([row['symbol'] for row in self.intents()], ['ZEC'])

    def test_previous_policy_pending_other_coins_expire_without_replaying_zec(self):
        rows = [observation(symbol=symbol, close=BASE+timedelta(minutes=1))
                for symbol in ('BTC', 'ZEC')]
        with patch.object(detector, 'NOTIFICATION_SYMBOLS', ('BTC', 'ZEC')):
            self.assertEqual(self.cycle('previous-policy', 1, rows)['created_intents'], 2)
        claimed = self.claim(2, limit=8)
        self.assertEqual([row['symbol'] for row in claimed], ['ZEC'])
        self.assertEqual({row['symbol']: row['status'] for row in self.intents()},
                         {'BTC': 'EXPIRED', 'ZEC': 'IN_FLIGHT'})
        self.assertEqual(self.cycle('same-episode-new-policy', 3)['created_intents'], 0)
        self.assertEqual(self.state()['ZEC']['episode'], 1)

    def test_activation_replay_changed_identity_out_of_order_and_future_are_safe(self):
        self.assertEqual(self.cycle('preactivation', -1)['record_status'], 'PRE_ACTIVATION')
        self.assertEqual(self.cycle('at-activation', 0)['record_status'], 'PRE_ACTIVATION')
        self.assertEqual(self.state(), {})
        first = self.cycle('fresh', 2)
        self.assertEqual(first['created_intents'], 1)
        with patch.object(detector, 'render_message', side_effect=AssertionError('Replay must not render')):
            replay = self.cycle('fresh', 2)
        self.assertEqual((replay['record_status'], replay['created_intents']), ('REPLAY', 0))
        changed = evaluation('fresh', 2, [observation(score=80., close=BASE+timedelta(minutes=2))])
        with self.assertRaisesRegex(ValueError, 'identity collision'):
            store.record_cycle(self.scope, changed, BASE+timedelta(minutes=3), database_url=self.dsn)
        self.assertEqual(self.cycle('older', 1, [observation(status='NO_MATCH', score=64.)])['record_status'], 'OUT_OF_ORDER')
        self.assertEqual(self.cycle('same-time', 2, [observation(status='NO_MATCH', score=64.)])['record_status'], 'OUT_OF_ORDER')
        with self.assertRaisesRegex(ValueError, 'future'):
            self.cycle('future', 5, now=BASE+timedelta(minutes=4))
        self.assertEqual(self.state()['ZEC']['active_direction'], 'LONG')
        self.assertEqual(len(self.intents()), 1)
        original = store.initialize_scope(self.scope, BASE+timedelta(days=1), database_url=self.dsn)
        self.assertEqual(original['activated_at_utc'], store._iso(BASE))
        with self.assertRaisesRegex(RuntimeError, 'not initialized'):
            store.record_cycle(self.scope+'missing', evaluation('no-scope', 1),
                BASE+timedelta(minutes=1), database_url=self.dsn)

    def test_same_cvd_generation_cannot_refire_after_false_or_restart_but_new_generation_can(self):
        closed = BASE+timedelta(minutes=1)
        self.assertEqual(self.cycle('generation-first', 1, [observation(close=closed)])['created_intents'], 1)
        self.cycle('generation-false', 2, [observation(status='NO_MATCH', score=64., close=closed)])
        store.initialize_scope(self.scope, BASE+timedelta(minutes=3), database_url=self.dsn)
        repeat = self.cycle('generation-rebound', 3, [observation(score=90., close=closed)])
        self.assertEqual((repeat['created_intents'], repeat['counts']['duplicate_generations']), (0, 1))
        self.cycle('new-reset', 4, [observation(status='NO_MATCH', score=64., close=closed)])
        self.assertEqual(self.cycle('new-candles', 5)['created_intents'], 1)
        self.assertEqual(len(self.intents()), 2)
        forged = evaluation('forged', 6)
        forged['observations'][0]['generation_key'] = 'f'*64
        with self.assertRaisesRegex(ValueError, 'generation identity'):
            store.record_cycle(self.scope, forged, BASE+timedelta(minutes=6), database_url=self.dsn)

    def test_pending_restart_recovery_and_expiry_use_both_source_and_candle_freshness(self):
        self.cycle('pending-restart', 1)
        store.initialize_scope(self.scope, BASE+timedelta(minutes=2), database_url=self.dsn)
        claim = self.claim(2)[0]
        self.assertEqual(claim['watch_scan_id'], 'pending-restart')
        self.assertTrue(claim['text'])
        self.assertEqual(claim['expires_at'], store._iso(BASE+timedelta(minutes=11)))
        self.assertEqual(self.claim(2), [])
        self.cycle('reset-expiry', 3, [observation(status='NO_MATCH', score=64., close=BASE+timedelta(minutes=3))])
        self.cycle('expiry10', 4)
        self.assertEqual(self.claim(14), [])
        self.assertEqual(self.intents()[-1]['status'], 'EXPIRED')
        self.cycle('reset-candle', 15, [observation(status='NO_MATCH', score=64., close=BASE+timedelta(minutes=15))])
        old_close = BASE-timedelta(minutes=13)  # At source16 this candle is29m old.
        self.cycle('candle-expiry', 16, [observation(close=old_close)])
        self.assertEqual(self.intents()[-1]['expires_at'], BASE+timedelta(minutes=17))
        self.assertEqual(self.claim(17), [])
        self.assertEqual(self.intents()[-1]['status'], 'EXPIRED')
        self.cycle('reset-stale', 18, [observation(status='NO_MATCH', score=64., close=BASE+timedelta(minutes=18))])
        self.assertEqual(self.cycle('too-late-to-record', 19, now=BASE+timedelta(minutes=29))['record_status'], 'EXPIRED')
        self.assertIsNone(self.state()['ZEC']['active_direction'])
        self.assertEqual(self.cycle('next-fresh', 30)['created_intents'], 1)

    def test_render_or_sql_error_rolls_back_receipt_state_and_all_intents(self):
        items = [observation(), observation(symbol='ETH')]
        original = detector.render_message
        def broken(row, source):
            if row['symbol'] == 'ZEC':
                raise RuntimeError('fixture-render-failure')
            return original(row, source)
        with patch.object(detector, 'render_message', side_effect=broken):
            with self.assertRaisesRegex(RuntimeError, 'fixture-render-failure'):
                self.cycle('atomic', 1, items)
        self.assertEqual(self.state(), {})
        self.assertEqual(self.intents(), [])
        with store._connect(self.dsn) as conn:
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM dual_cvd65_receipts WHERE subscription_scope=%s',
                (self.scope,)).fetchone()['n'], 0)
        with patch.object(detector, 'render_message', return_value='invalid\x00text'):
            with self.assertRaises((self.psycopg.Error, ValueError)):
                self.cycle('atomic', 1, items)
        self.assertEqual(self.state(), {})
        self.assertEqual(self.intents(), [])
        self.assertEqual(self.cycle('atomic', 1, items)['created_intents'], 1)

    def test_concurrent_record_and_claim_commit_one_attempt_and_terminal_evidence_is_immutable(self):
        barrier = Barrier(4)
        def record(_):
            barrier.wait(timeout=5)
            return self.cycle('concurrent', 1)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(record, range(4)))
        self.assertEqual(sum(result['created_intents'] for result in results), 1)
        claim_barrier = Barrier(4)
        def claim(_):
            claim_barrier.wait(timeout=5)
            return self.claim(2)
        with ThreadPoolExecutor(max_workers=4) as pool:
            attempts = [item for result in pool.map(claim, range(4)) for item in result]
        self.assertEqual(len(attempts), 1)
        attempt = attempts[0]
        self.assertEqual(self.intents()[0]['status'], 'IN_FLIGHT')
        self.assertFalse(store.finish_attempt(attempt['intent_id'], 'wrong', 'DELIVERED', BASE+timedelta(minutes=2), database_url=self.dsn))
        self.assertTrue(store.finish_attempt(attempt['intent_id'], attempt['attempt_token'], 'DELIVERED',
            BASE+timedelta(minutes=2, seconds=1), database_url=self.dsn))
        self.assertFalse(store.finish_attempt(attempt['intent_id'], attempt['attempt_token'], 'FAILED',
            BASE+timedelta(minutes=3), database_url=self.dsn))
        self.assertEqual(self.claim(3), [])
        for update in ("status='PENDING',attempt_token=NULL,attempted_at_utc=NULL", "text='changed'"):
            with store._connect(self.dsn) as conn:
                with self.assertRaises(self.psycopg.Error):
                    conn.execute('UPDATE dual_cvd65_intents SET '+update+' WHERE intent_id=%s', (attempt['intent_id'],))
        with store._connect(self.dsn) as conn:
            with self.assertRaises(self.psycopg.Error):
                conn.execute("UPDATE dual_cvd65_receipts SET input_sha256=%s WHERE subscription_scope=%s", ('f'*64, self.scope))

    def test_unknown_failed_and_two_minute_orphans_are_terminal_never_resent(self):
        for index, status in enumerate(('UNKNOWN', 'FAILED', 'ORPHAN')):
            start = 1+index*5
            if index:
                self.cycle('reset-'+status, start-1, [observation(status='NO_MATCH', score=64., close=BASE+timedelta(minutes=start-1))])
            self.cycle('match-'+status, start)
            attempt = self.claim(start+1)[0]
            if status == 'ORPHAN':
                self.assertEqual(store.settle_orphans(self.scope, BASE+timedelta(minutes=start+2), database_url=self.dsn), 0)
                self.assertEqual(store.settle_orphans(self.scope, BASE+timedelta(minutes=start+3), database_url=self.dsn), 1)
            else:
                self.assertTrue(store.finish_attempt(attempt['intent_id'], attempt['attempt_token'], status,
                    BASE+timedelta(minutes=start+1), database_url=self.dsn))
            self.assertEqual(self.claim(start+4), [])
            self.assertFalse(store.release_unattempted(attempt['intent_id'], attempt['attempt_token'],
                BASE+timedelta(minutes=start+4), database_url=self.dsn))
        self.assertEqual([row['status'] for row in self.intents()], ['UNKNOWN', 'FAILED', 'UNKNOWN'])

    def test_presend_release_requires_current_token_and_expired_release_cannot_requeue(self):
        self.cycle('release', 1)
        first = self.claim(2)[0]
        self.assertFalse(store.release_unattempted(first['intent_id'], 'wrong', BASE+timedelta(minutes=2), database_url=self.dsn))
        self.assertTrue(store.release_unattempted(first['intent_id'], first['attempt_token'], BASE+timedelta(minutes=2), database_url=self.dsn))
        second = self.claim(2)[0]
        self.assertNotEqual(second['attempt_token'], first['attempt_token'])
        self.assertFalse(store.finish_attempt(first['intent_id'], first['attempt_token'], 'UNKNOWN', BASE+timedelta(minutes=2), database_url=self.dsn))
        self.assertTrue(store.release_unattempted(second['intent_id'], second['attempt_token'], BASE+timedelta(minutes=11), database_url=self.dsn))
        self.assertEqual(self.claim(11), [])
        self.assertEqual(self.intents()[0]['status'], 'EXPIRED')


if __name__ == '__main__':
    unittest.main()
