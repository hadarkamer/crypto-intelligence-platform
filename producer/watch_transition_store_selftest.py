"""Real PostgreSQL transactions, restart/competition and conservative delivery."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from threading import Barrier
import unittest
from unittest.mock import patch
from uuid import uuid4

import watch_transition_store as store

BASE = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


def card(score, symbol='BTC', timeframe='12h', side='SHORT'):
    return {'symbol': symbol, 'timeframe': timeframe, 'side': side, 'score': score,
            'message': f'{symbol}:{timeframe}'}


def intents(items):
    return [{'kind': 'MAX_PAIN_SCORE_65', 'signal_key': store.signal_key(item),
             'payload': {'message': item['message'], 'score': item['score']}} for item in items]


class PureStateTests(unittest.TestCase):
    def test_bootstrap_hysteresis_unknown_and_legitimate_rearm(self):
        state, crossed = store.evaluate_state({}, [card(70), card(62, 'ETH'), card(59, 'SOL')])
        self.assertEqual(crossed, [])
        self.assertTrue(state['BTC|12h|SHORT']['active'])
        self.assertIsNone(state['ETH|12h|SHORT']['active'])
        self.assertFalse(state['SOL|12h|SHORT']['active'])
        state, crossed = store.evaluate_state(state, [card(float('nan')), card(70, 'ETH'), card(65, 'SOL')])
        self.assertEqual([item['symbol'] for item in crossed], ['SOL'])
        self.assertTrue(state['BTC|12h|SHORT']['active'])
        self.assertEqual(state['BTC|12h|SHORT']['observation'], 'UNKNOWN')
        state, crossed = store.evaluate_state(state, [card(62), card(62, 'SOL')])
        self.assertEqual(crossed, [])
        state, crossed = store.evaluate_state(state, [card(70), card(59, 'SOL')])
        self.assertEqual(crossed, [])
        state, crossed = store.evaluate_state(state, [card(65, 'SOL')])
        self.assertEqual(len(crossed), 1)
        self.assertEqual(state['SOL|12h|SHORT']['episode'], 2)
        self.assertTrue(state['BTC|12h|SHORT']['active'])  # absent is not false

    def test_canonical_unknown_and_datetime_normalization(self):
        left = {'a': -0.0, 'b': 1e20, 'missing': float('nan'), 'stamp': BASE,
                'decimal': Decimal('65.00')}
        right = {'stamp': '2026-09-13T12:00:00.000000Z', 'missing': None,
                 'b': 100000000000000000000, 'a': 0, 'decimal': 65}
        self.assertEqual(store.canonical(left), store.canonical(right))
        for value in (None, float('nan'), float('inf'), '', 'invalid', False):
            state, crossed = store.evaluate_state({}, [card(value)])
            self.assertIsNone(state['BTC|12h|SHORT']['active'])
            self.assertEqual(crossed, [])
        state, _ = store.evaluate_state({}, [card(59)])
        missing_score = card(90)
        missing_score.pop('score')
        preserved, _ = store.evaluate_state(state, [missing_score])
        self.assertFalse(preserved['BTC|12h|SHORT']['active'])

    def test_first_card_order_and_input_immutability(self):
        original = [card(59, timeframe='24h'), card(59, timeframe='12h')]
        state, _ = store.evaluate_state({}, original)
        batch = [card(70, timeframe='24h'), card(80, timeframe='12h')]
        new_state, crossing = store.evaluate_state(state, batch)
        self.assertEqual([x['timeframe'] for x in crossing], ['24h', '12h'])
        crossing[0]['score'] = 999
        self.assertEqual(batch[0]['score'], 70)
        self.assertFalse(state['BTC|24h|SHORT']['active'])
        self.assertTrue(new_state['BTC|24h|SHORT']['active'])
        with self.assertRaisesRegex(ValueError, 'duplicate signal'):
            store.evaluate_state({}, [card(59), card(70)])

    def test_database_url_is_required_without_local_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'DATABASE_URL'):
                store._connect()


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLTransitionTests(unittest.TestCase):
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
        cls.dbname = 'test_watch_transitions_' + uuid4().hex
        cls.admin = psycopg.connect(test_url, autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.dbname)))
        cls.dsn = make_conninfo(test_url, dbname=cls.dbname)
        try:
            store.init_schema(cls.dsn)
            store.init_schema(cls.dsn)
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
        self.scope = 'subscription-' + uuid4().hex

    def cycle(self, watch, minute, items, factory=intents, scope=None, policy=store.POLICY_VERSION):
        return store.record_cycle(scope or self.scope, watch, BASE + timedelta(minutes=minute), items,
                                  factory, database_url=self.dsn, policy_version=policy)

    def ready(self):
        self.cycle('baseline', 0, [card(59)])
        return self.cycle('crossing', 1, [card(70)])

    def test_replay_restart_rearm_unknown_62_and_hash_collision(self):
        bootstrap = self.cycle('bootstrap', 0, [card(70)])
        self.assertEqual(bootstrap['counts']['bootstrapped_true'], 1)
        self.assertEqual(self.cycle('restart', 1, [card(70)])['intents'], [])
        reset = self.cycle('reset', 2, [card(59)])
        self.assertEqual(reset['reset_keys'], ['BTC|12h|SHORT'])
        self.assertEqual(reset['resets'], [card(59)])
        self.assertEqual(self.cycle('reset', 2, [card(59)])['resets'], reset['resets'])
        self.assertEqual(reset['counts']['resets'], 1)
        first = self.cycle('crossing', 3, [card(70)])
        self.assertEqual(len(first['intents']), 1)
        replay = self.cycle('crossing', 3, [card(70)], lambda _: self.fail('replay called factory'))
        self.assertTrue(replay['idempotent_existing'])
        self.assertEqual(first['intents'], replay['intents'])
        with self.assertRaisesRegex(ValueError, 'identity collision'):
            self.cycle('crossing', 3, [card(71)])
        unknown = self.cycle('unknown', 4, [card(None)])
        self.assertEqual(unknown['intents'], [])
        self.assertEqual(unknown['counts']['unknown'], 1)
        self.assertTrue(self.cycle('unknown', 4, [card(float('nan'))])['idempotent_existing'])
        self.assertEqual(self.cycle('high', 5, [card(70)])['intents'], [])
        self.assertEqual(self.cycle('middle', 6, [card(62)])['intents'], [])
        self.assertEqual(self.cycle('high-again', 7, [card(70)])['intents'], [])
        self.cycle('reset-again', 8, [card(59)])
        second = self.cycle('second-episode', 9, [card(65)])
        self.assertEqual(second['state']['BTC|12h|SHORT']['episode'], 2)
        self.assertNotEqual(first['intents'][0]['intent_id'], second['intents'][0]['intent_id'])

    def test_scope_policy_and_new_symbol_bootstrap(self):
        first = self.ready()
        self.assertEqual(self.cycle('new-subscription', 2, [card(70)], scope=self.scope+'new')['intents'], [])
        self.assertEqual(self.cycle('new-policy', 2, [card(70)], policy='policy-v2')['intents'], [])
        self.assertEqual(self.cycle('new-key', 2, [card(70, 'ETH')])['intents'], [])
        self.assertEqual(len(first['intents']), 1)
        # Independent subscribers do not share reset history or episode identity.
        other = self.scope+'other'
        self.cycle('base', 0, [card(59)], scope=other)
        second = self.cycle('cross', 1, [card(70)], scope=other)
        self.assertNotEqual(first['intents'][0]['intent_id'], second['intents'][0]['intent_id'])

    def test_frozen_first_card_factory_and_pending_recovery(self):
        cards = [card(59, timeframe='24h'), card(59, timeframe='12h')]
        self.cycle('base', 0, cards)
        def first_only(crossing):
            self.assertEqual([x['timeframe'] for x in crossing], ['24h', '12h'])
            descriptors = intents(crossing)
            descriptors.append({'kind': 'FORMULA_MP65_CVD_SHORT',
                                'signal_key': store.signal_key(crossing[0]),
                                'payload': {'selected_timeframe': crossing[0]['timeframe']}})
            return descriptors
        result = self.cycle('cross', 1, [card(70, timeframe='24h'), card(80, timeframe='12h')], first_only)
        self.assertEqual(len(result['intents']), 3)
        recovered = store.claim_pending(self.scope, BASE+timedelta(minutes=2), database_url=self.dsn)
        self.assertEqual([x['intent_id'] for x in recovered], [x['intent_id'] for x in result['intents']])
        self.assertEqual(recovered[-1]['payload']['selected_timeframe'], '24h')
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=2), database_url=self.dsn), [])
        delivery = recovered[0]
        self.assertFalse(store.complete_attempt(delivery['intent_id'], 'wrong-token', 'UNKNOWN',
                                              BASE+timedelta(minutes=2), database_url=self.dsn))
        self.assertTrue(store.complete_attempt(delivery['intent_id'], delivery['attempt_token'], 'DELIVERED',
                         BASE+timedelta(minutes=2), BASE+timedelta(minutes=2, seconds=1), database_url=self.dsn))
        self.assertFalse(store.complete_attempt(delivery['intent_id'], delivery['attempt_token'], 'UNKNOWN',
                                              BASE+timedelta(minutes=2), database_url=self.dsn))

    def test_kind_filter_claims_experimental_without_consuming_ordinary_pending(self):
        self.cycle('base', 0, [card(59)])
        def factory(crossing):
            return intents(crossing) + [{'kind': 'FORMULA_MP65_CVD_SHORT',
                'signal_key': store.signal_key(crossing[0]), 'payload': {'text': 'experimental'}}]
        self.cycle('cross', 1, [card(70)], factory)
        now = BASE + timedelta(minutes=2)
        selected = store.claim_pending(self.scope, now, database_url=self.dsn,
                                       kinds=('FORMULA_MP65_CVD_SHORT',))
        self.assertEqual([row['kind'] for row in selected], ['FORMULA_MP65_CVD_SHORT'])
        self.assertEqual(store.claim_pending(self.scope, now, database_url=self.dsn,
                                            kinds=('FORMULA_MP65_CVD_SHORT',)), [])
        ordinary = store.claim_pending(self.scope, now, database_url=self.dsn,
                                       kinds=('MAX_PAIN_SCORE_65',))
        self.assertEqual([row['kind'] for row in ordinary], ['MAX_PAIN_SCORE_65'])

    def test_concurrent_cycles_and_claims_use_fresh_connections(self):
        self.cycle('base', 0, [card(59)])
        barrier = Barrier(6)
        def submit(index):
            barrier.wait(timeout=5)
            return self.cycle('concurrent-'+str(index), 1, [card(70)])
        with ThreadPoolExecutor(max_workers=6) as workers:
            results = list(workers.map(submit, range(6)))
        self.assertEqual(sum(len(result['intents']) for result in results), 1)
        claim_barrier = Barrier(4)
        def claim(_):
            claim_barrier.wait(timeout=5)
            return store.claim_pending(self.scope, BASE+timedelta(minutes=2), database_url=self.dsn)
        with ThreadPoolExecutor(max_workers=4) as workers:
            claims = list(workers.map(claim, range(4)))
        self.assertEqual(sum(len(items) for items in claims), 1)

    def test_factory_and_database_failure_roll_back_everything(self):
        self.cycle('base', 0, [card(59)])
        def broken_factory(_):
            raise RuntimeError('factory failure')
        with self.assertRaisesRegex(RuntimeError, 'factory failure'):
            self.cycle('broken-factory', 1, [card(70)], broken_factory)
        def broken_database(crossing):
            first = intents(crossing)
            first.append({'kind': 'FORMULA_MP65_CVD_SHORT', 'signal_key': store.signal_key(crossing[0]),
                          'payload': {'invalid_postgres_jsonb': '\x00'}})
            return first
        import psycopg
        with self.assertRaises(psycopg.Error):
            self.cycle('broken-db', 1, [card(70)], broken_database)
        with store._connect(self.dsn) as conn:
            row = conn.execute('SELECT state FROM watch_transition_scopes WHERE subscription_scope=%s', (self.scope,)).fetchone()
            self.assertFalse(row['state']['BTC|12h|SHORT']['active'])
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM watch_transition_batches WHERE subscription_scope=%s', (self.scope,)).fetchone()['n'], 1)
            self.assertEqual(conn.execute('SELECT count(*) AS n FROM watch_transition_intents WHERE subscription_scope=%s', (self.scope,)).fetchone()['n'], 0)
        self.assertEqual(len(self.cycle('real-crossing', 2, [card(70)])['intents']), 1)

    def test_unknown_failed_and_orphaned_attempts_are_never_retried(self):
        self.ready()
        attempt_time = BASE+timedelta(minutes=2)
        claimed = store.claim_pending(self.scope, attempt_time, database_url=self.dsn)[0]
        self.assertTrue(store.complete_attempt(claimed['intent_id'], claimed['attempt_token'], 'UNKNOWN',
                                              attempt_time, error_type='TimedOut', database_url=self.dsn))
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=3), database_url=self.dsn), [])
        self.cycle('reset', 3, [card(59)])
        self.cycle('next', 4, [card(70)])
        orphan = store.claim_pending(self.scope, BASE+timedelta(minutes=5), database_url=self.dsn)[0]
        self.assertEqual(store.settle_orphans(self.scope, BASE+timedelta(minutes=6), database_url=self.dsn), 0)
        self.assertEqual(store.settle_orphans(self.scope, BASE+timedelta(minutes=7), database_url=self.dsn), 1)
        self.assertFalse(store.complete_attempt(orphan['intent_id'], orphan['attempt_token'], 'FAILED',
                                              BASE+timedelta(minutes=7), database_url=self.dsn))
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=8), database_url=self.dsn), [])
        self.cycle('reset-failed', 8, [card(59)])
        self.cycle('cross-failed', 9, [card(70)])
        failed = store.claim_pending(self.scope, BASE+timedelta(minutes=10), database_url=self.dsn)[0]
        self.assertTrue(store.complete_attempt(failed['intent_id'], failed['attempt_token'], 'FAILED',
                                              BASE+timedelta(minutes=10), database_url=self.dsn))
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=11), database_url=self.dsn), [])

    def test_pre_send_release_requires_current_token_and_unfinished_claim(self):
        self.ready()
        moment = BASE+timedelta(minutes=2)
        claimed = store.claim_pending(self.scope, moment, database_url=self.dsn)[0]
        identity, token = claimed['intent_id'], claimed['attempt_token']
        self.assertFalse(store.release_unattempted(identity, 'wrong-token', database_url=self.dsn))
        self.assertTrue(store.release_unattempted(identity, token, database_url=self.dsn))
        self.assertFalse(store.release_unattempted(identity, token, database_url=self.dsn))
        with store._connect(self.dsn) as conn:
            released = conn.execute('SELECT status,attempt_token,attempted_at,error_type '
                                    'FROM watch_transition_intents WHERE intent_id=%s', (identity,)).fetchone()
            self.assertEqual(released, {'status': 'PENDING', 'attempt_token': None,
                                        'attempted_at': None, 'error_type': None})
        next_claim = store.claim_pending(self.scope, moment, database_url=self.dsn)[0]
        self.assertNotEqual(next_claim['attempt_token'], token)
        self.assertFalse(store.release_unattempted(identity, token, database_url=self.dsn))
        self.assertTrue(store.complete_attempt(identity, next_claim['attempt_token'], 'DELIVERED',
                        moment, moment+timedelta(seconds=1), database_url=self.dsn))
        self.assertFalse(store.release_unattempted(identity, next_claim['attempt_token'], database_url=self.dsn))
        self.cycle('reset-unknown-release', 3, [card(59)])
        self.cycle('cross-unknown-release', 4, [card(70)])
        unknown = store.claim_pending(self.scope, BASE+timedelta(minutes=5), database_url=self.dsn)[0]
        self.assertTrue(store.complete_attempt(unknown['intent_id'], unknown['attempt_token'], 'UNKNOWN',
                                              BASE+timedelta(minutes=5), database_url=self.dsn))
        self.assertFalse(store.release_unattempted(unknown['intent_id'], unknown['attempt_token'], database_url=self.dsn))
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=6), database_url=self.dsn), [])

    def test_old_batch_future_and_expiry_do_not_resend_or_reset(self):
        result = self.ready()
        older = self.cycle('old-reset', 0, [card(59)], lambda _: self.fail('older batch called factory'))
        self.assertTrue(older['rejected_older'])
        self.assertTrue(older['state']['BTC|12h|SHORT']['active'])
        same_time = self.cycle('same-time-conflicting-reset', 1, [card(59)])
        self.assertTrue(same_time['rejected_older'])
        self.assertTrue(same_time['state']['BTC|12h|SHORT']['active'])
        self.assertEqual(self.cycle('still-high', 2, [card(70)])['intents'], [])
        self.assertEqual(store.claim_pending(self.scope, BASE, database_url=self.dsn), [])
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=11), database_url=self.dsn), [])
        with store._connect(self.dsn) as conn:
            status = conn.execute('SELECT status FROM watch_transition_intents WHERE intent_id=%s',
                                  (result['intents'][0]['intent_id'],)).fetchone()['status']
            self.assertEqual(status, 'EXPIRED')
        replay = self.cycle('crossing', 1, [card(70)])
        self.assertTrue(replay['idempotent_existing'])
        self.assertEqual(replay['intents'][0]['status'], 'EXPIRED')
        self.assertEqual(store.claim_pending(self.scope, BASE+timedelta(minutes=12), database_url=self.dsn), [])

    def test_bounded_retention_and_replay_after_receipt_purge(self):
        result = self.ready()
        store.claim_pending(self.scope, BASE+timedelta(minutes=11), database_url=self.dsn)
        with store._connect(self.dsn) as conn:
            conn.execute('UPDATE watch_transition_batches SET created_at=%s WHERE subscription_scope=%s',
                         (BASE-timedelta(days=3), self.scope))
            conn.execute('UPDATE watch_transition_intents SET created_at=%s WHERE subscription_scope=%s',
                         (BASE-timedelta(days=31), self.scope))
        counts = store.prune_history(self.scope, BASE, database_url=self.dsn)
        self.assertEqual(counts, {'batches': 2, 'terminal_intents': 1})
        replay = self.cycle('crossing', 1, [card(70)], lambda _: self.fail('purged replay called factory'))
        self.assertTrue(replay['rejected_older'])
        self.assertFalse(replay['idempotent_existing'])
        self.assertEqual(replay['intents'], [])
        self.assertTrue(replay['state']['BTC|12h|SHORT']['active'])
        self.assertEqual(replay['state']['BTC|12h|SHORT']['episode'], 1)
        self.assertEqual(self.cycle('still-high', 2, [card(70)])['intents'], [])
        self.cycle('reset-after-purge', 3, [card(59)])
        legitimate = self.cycle('new-after-purge', 4, [card(70)])
        self.assertEqual(legitimate['state']['BTC|12h|SHORT']['episode'], 2)
        self.assertNotEqual(legitimate['intents'][0]['intent_id'], result['intents'][0]['intent_id'])
        # Prove per-call deletion is bounded even with a larger old backlog.
        from psycopg.types.json import Jsonb
        with store._connect(self.dsn) as conn:
            for index in range(store.PRUNE_LIMIT+2):
                conn.execute('''INSERT INTO watch_transition_batches
                    (subscription_scope,policy_version,watch_scan_id,observed_at,input_sha256,
                     result_state,result_metadata,crossing_keys,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (self.scope, store.POLICY_VERSION, 'receipt-'+str(index), BASE, 'unused-test-hash',
                     Jsonb({}), Jsonb({}), Jsonb([]), BASE-timedelta(days=3)))
        bounded = store.prune_history(self.scope, BASE, database_url=self.dsn)
        self.assertEqual(bounded['batches'], store.PRUNE_LIMIT)
        self.assertEqual(store.prune_history(self.scope, BASE, database_url=self.dsn)['batches'], 2)


if __name__ == '__main__':
    unittest.main()
