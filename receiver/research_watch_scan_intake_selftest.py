"""Frozen all-scan admission, view semantics and real PostgreSQL recovery.

PostgreSQL tests create a disposable local/CI database. They never use the
application's DATABASE_URL and never call market, Sheets or Telegram services.
"""
from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_max_pain_archive as archive
import research_watch_scan_intake as intake
import research_watch_score_capture as capture
from research_watch_score_capture_selftest import (
    BASE, archive_payload, bundle, derivatives, inputs,
)


def rehash(block):
    block['payload_sha256'] = capture.digest({k: v for k, v in block.items() if k != 'payload_sha256'})
    return block


def source_fixture(block=None, *, cycle_id='capture-test'):
    if block is None:
        block, *_ = bundle(cycle_id=cycle_id)
    parent = archive_payload(block, cycle_id=cycle_id)['set']
    return dict(snapshot_set_id=1, snapshot_key=parent['snapshot_key'],
                payload_sha256=parent['payload_sha256'], cycle_id=cycle_id,
                source=parent['source'], available_at_utc=parent['available_at_utc'],
                created_at_utc=BASE + timedelta(minutes=7), bundle=deepcopy(block))


class FrozenSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.baseline = source_fixture()

    def validate(self, source=None, *, now=None, activated_at=None):
        return intake.validate_source(deepcopy(source or self.baseline),
            now=now or BASE + timedelta(minutes=10),
            activated_at=activated_at or BASE + timedelta(minutes=8))

    def test_admits_all_112_slots_without_replaying_scores_or_fetching(self):
        with patch.object(capture, 'prepare', side_effect=AssertionError('No score replay')), \
             patch('market_confidence_engine._cached_flow', side_effect=AssertionError('No source request')), \
             patch('market_confidence_engine.coinglass_oi_regime_service.latest', side_effect=AssertionError('No source request')):
            result = self.validate()
        self.assertEqual(result['intake_status'], 'ACCEPTED')
        self.assertEqual((result['coin_count'], result['score_slot_count']), (8, 112))
        self.assertGreater(result['below_65_slots'], 0)
        self.assertGreater(result['unavailable_models'], 0)
        self.assertEqual(result['observed_at_utc'], BASE + timedelta(minutes=5))
        self.assertEqual(result['usable_from_utc'], BASE + timedelta(minutes=7))
        self.assertEqual(result['capture_phase'], 'HISTORICAL_CAPTURE')
        self.assertEqual(self.baseline['bundle']['coins']['BTC']['models']['spot_flow']['capture_status'], 'UNAVAILABLE')

    def test_phase_records_capture_timing_not_prospective_formula_evidence(self):
        result = self.validate(activated_at=BASE + timedelta(minutes=4))
        self.assertEqual(result['capture_phase'], 'FORWARD_CAPTURE')
        self.assertNotIn('PROSPECTIVE', result['capture_phase'])
        self.assertEqual(result['observed_at_utc'], BASE + timedelta(minutes=5))
        delayed_commit = self.validate(activated_at=BASE + timedelta(minutes=6))
        self.assertEqual(delayed_commit['capture_phase'], 'HISTORICAL_CAPTURE')

    def test_hash_identity_and_old_capture_are_rejected(self):
        tampered = deepcopy(self.baseline)
        tampered['bundle']['coins']['BTC']['maxpain'][0]['score'] += 1
        self.assertEqual(self.validate(tampered)['rejection_reason'], 'CAPTURE_HASH_MISMATCH')
        for key, value, reason in (
            ('version', 'watch-operational-scores-v1', 'UNSUPPORTED_CAPTURE_VERSION'),
            ('population', 'delivered-alerts', 'SOURCE_IDENTITY_MISMATCH'),
            ('cycle_id', 'another-cycle', 'SOURCE_IDENTITY_MISMATCH'),
            ('status', 'FAILED', 'CAPTURE_FAILED'),
        ):
            with self.subTest(key=key):
                source = deepcopy(self.baseline)
                source['bundle'][key] = value
                rehash(source['bundle'])
                self.assertEqual(self.validate(source)['rejection_reason'], reason)
        source = deepcopy(self.baseline)
        source['source'] = 'RESEARCH_PASSIVE'
        self.assertEqual(self.validate(source)['rejection_reason'], 'NOT_SHARED_WATCH')

    def test_future_or_inverted_source_availability_cannot_be_admitted(self):
        source = deepcopy(self.baseline)
        source['created_at_utc'] = BASE + timedelta(hours=1)
        self.assertEqual(self.validate(source)['rejection_reason'], 'FUTURE_AVAILABILITY')
        source = deepcopy(self.baseline)
        source['bundle']['computed_at_utc'] = (BASE + timedelta(minutes=8)).isoformat()
        rehash(source['bundle'])
        self.assertEqual(self.validate(source)['rejection_reason'], 'COMPUTED_AFTER_SOURCE_AVAILABILITY')

    def test_partial_inactive_missing_zero_and_hype_routes_remain_distinct(self):
        rows = inputs()
        rows = [r for r in rows if not (r['symbol'] == 'ETH' and r['timeframe'] == '12h')]
        btc = next(r for r in rows if r['symbol'] == 'BTC' and r['timeframe'] == '12h')
        btc.update(short_liquidation_amount=None, long_liquidation_amount=0)
        sol = next(r for r in rows if r['symbol'] == 'SOL' and r['timeframe'] == '12h')
        sol.update(long_max_pain=105, distance_long_pct=5)
        for row in rows:
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                           price_market='PERP', price_instrument='HYPEUSDT')
        block, *_ = bundle(rows=rows, snapshot=derivatives())
        original = deepcopy(block)
        result = self.validate(source_fixture(block))
        self.assertEqual(result['intake_status'], 'ACCEPTED')
        self.assertEqual(result['capture_status'], 'PARTIAL')
        self.assertEqual(result['score_slot_count'], 112)
        self.assertGreater(result['source_time_error_count'], 0)
        self.assertEqual(block, original)
        self.assertEqual(block['coins']['ETH']['maxpain'][0]['status'], 'MISSING_INPUT')
        self.assertEqual(block['coins']['SOL']['maxpain'][0]['status'], 'INACTIVE_TARGET')
        self.assertIsNone(block['coins']['SOL']['maxpain'][0]['score'])
        slot = block['coins']['BTC']['maxpain'][0]
        self.assertEqual(slot['near_amount'], 0)
        self.assertNotIn('far_amount', slot)
        self.assertEqual(block['coins']['HYPE']['sources']['maxpain_operational_rows'][0]['price_source'], 'binance_futures')

    def test_bad_slot_coverage_availability_and_score_components_are_rejected(self):
        mutations = (
            ('SCORE_SLOT_COVERAGE_MISMATCH', lambda b: b['coins']['BTC']['maxpain'].pop()),
            ('COIN_COVERAGE_MISMATCH', lambda b: b['coins'].pop('HYPE')),
            ('MODEL_AVAILABILITY_MISMATCH', lambda b: b['coins']['BTC']['models']['spot_flow'].update(capture_status='AVAILABLE')),
            ('SCORE_COMPONENT_MISMATCH', lambda b: b['coins']['BTC']['maxpain'][0].update(score=99)),
            ('INVALID_SCORE', lambda b: b['coins']['BTC']['maxpain'][0].update(score=True)),
        )
        for reason, mutate in mutations:
            with self.subTest(reason=reason):
                source = deepcopy(self.baseline)
                mutate(source['bundle'])
                rehash(source['bundle'])
                result = self.validate(source)
                self.assertEqual(result['intake_status'], 'REJECTED')
                self.assertEqual(result['rejection_reason'], reason)


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Explicit local/CI PostgreSQL required')
class PostgreSQLIntakeTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        info = conninfo_to_dict(os.environ['TEST_DATABASE_URL'])
        if info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'} or not (
            info.get('dbname', '').startswith('test_') or info.get('dbname', '').endswith('_test')
        ):
            raise ValueError('Explicit local test database required')
        self.psycopg, self.sql = psycopg, sql
        self.name = 'test_watch_intake_' + uuid4().hex
        self.admin = psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name)))
        self.dsn = make_conninfo(os.environ['TEST_DATABASE_URL'], dbname=self.name)
        self.addCleanup(self.drop_database)
        with self.connect() as conn:
            for name in ('007_max_pain_watch_archive_v1.sql', '046_watch_scan_research_intake.sql'):
                conn.execute((Path(__file__).parent / 'migrations' / name).read_text(), prepare=False)

    def drop_database(self):
        try:
            self.admin.execute(self.sql.SQL('DROP DATABASE {}').format(self.sql.Identifier(self.name)))
        finally:
            self.admin.close()

    def connect(self):
        from psycopg.rows import dict_row
        return self.psycopg.connect(self.dsn, row_factory=dict_row)

    def persist(self, cycle_id, *, block=None):
        if block is None:
            block, *_ = bundle(cycle_id=cycle_id)
        with patch.dict(os.environ, {'MAX_PAIN_ARCHIVE_ENABLED': '1'}):
            result = archive.persist_snapshot_payload(archive_payload(block, cycle_id=cycle_id), database_url=self.dsn)
        return result['snapshot_set_id']

    def consume(self):
        with self.connect() as conn:
            return intake.consume_page(conn)

    def test_committed_jsonb_idempotency_and_actual_eight_coin_112_slot_views(self):
        rows = inputs()
        rows[0]['short_liquidation_amount'] = -0.0
        rows[0]['long_liquidation_amount'] = 1e20
        for row in rows:
            if row['symbol'] == 'HYPE':
                row.update(price_source='binance_futures', price_pair='HYPEUSDT',
                           price_market='PERP', price_instrument='HYPEUSDT')
        block, *_ = bundle(rows=rows, cycle_id='views')
        snapshot_id = self.persist('views', block=block)
        first = self.consume()
        self.assertEqual((first['accepted'], first['rejected']), (1, 0))
        self.assertEqual(self.consume()['processed'], 0)
        with self.connect() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM research_watch_scan_intakes').fetchone()['n'], 1)
            coins = conn.execute('SELECT * FROM research_watch_scan_observations ORDER BY symbol').fetchall()
            slots = conn.execute('SELECT * FROM research_watch_scan_score_slots').fetchall()
            self.assertEqual(len(coins), 8)
            self.assertEqual(len(slots), 112)
            self.assertEqual(len({(s['symbol'], s['timeframe'], s['source_side']) for s in slots}), 112)
            self.assertTrue(all(s['snapshot_set_id'] == snapshot_id and s['population_version'] == intake.POPULATION for s in slots))
            self.assertTrue(all(s['analysis_direction'] == ('SHORT' if s['source_side'] == 'LONG' else 'LONG') for s in slots))
            self.assertTrue(all(s['outcome_status'] == 'NOT_EVALUATED' and not s['is_delivered_alert']
                                and not s['qualifies_as_prospective_formula_evidence'] for s in slots))
            self.assertTrue(any(s['score'] is not None and s['score'] < 65 for s in slots))
            hype = next(c for c in coins if c['symbol'] == 'HYPE')
            self.assertEqual(hype['sources']['maxpain_operational_rows'][0]['price_source'], 'binance_futures')
            self.assertEqual(hype['models']['spot_flow']['capture_status'], 'UNAVAILABLE')
            official = conn.execute("SELECT price_source FROM research_max_pain_snapshot_rows WHERE symbol='HYPE' LIMIT 1").fetchone()
            self.assertEqual(official['price_source'], 'hyperliquid')
            self.assertIsNone(conn.execute("SELECT to_regclass('research_events') AS relation").fetchone()['relation'])

    def test_receipt_and_cursor_roll_back_together_then_restart_recovers(self):
        self.persist('rollback')
        with self.connect() as conn:
            conn.execute('INSERT INTO research_watch_scan_intake_state(consumer_version) VALUES (%s)', (intake.VERSION,))
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'after receipt'):
                with conn.transaction():
                    self.assertEqual(intake.consume_page(conn)['accepted'], 1)
                    raise RuntimeError('fixture failure after receipt before commit')
        with self.connect() as conn:
            state = conn.execute('SELECT * FROM research_watch_scan_intake_state').fetchone()
            self.assertEqual((state['scan_cursor'], state['scan_high_water'], state['completed_laps']), (0, 0, 0))
            self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM research_watch_scan_intakes').fetchone()['n'], 0)
        self.assertEqual(self.consume()['accepted'], 1)

    def test_late_committed_lower_id_is_recovered_by_finite_cursor_lap(self):
        from psycopg.types.json import Jsonb
        block, *_ = bundle(cycle_id='late-first')
        payload = archive_payload(block, cycle_id='late-first')
        parent = payload['set']
        # This parent stays invisible while a higher sequence ID commits.
        # The parent uses the same real archive fields and immutable hashes;
        # no wall-clock sleeps or production connections are required.
        with self.connect() as late:
            columns = list(parent)
            values = [Jsonb(parent[k]) if k in {'source_metadata', 'validation_errors', 'completeness_report'} else parent[k] for k in columns]
            late_id = late.execute('INSERT INTO research_max_pain_snapshot_sets (' + ','.join(columns) + ') VALUES (' +
                ','.join(['%s'] * len(columns)) + ') RETURNING snapshot_set_id', values).fetchone()['snapshot_set_id']
            # The real archive requires complete child rows at commit through
            # a deferred constraint trigger. Keep the complete atomic source.
            for table, children, json_fields in (
                ('research_max_pain_snapshot_symbols', payload['symbols'], {'validation_errors'}),
                ('research_max_pain_snapshot_rows', payload['rows'], {'validation_errors', 'raw_provenance'}),
            ):
                for child in children:
                    child_columns = list(child)
                    child_values = [Jsonb(child[k]) if k in json_fields else child[k] for k in child_columns]
                    late.execute('INSERT INTO ' + table + ' (snapshot_set_id,' + ','.join(child_columns) + ') VALUES (' +
                        ','.join(['%s'] * (len(child_columns) + 1)) + ')', [late_id, *child_values])
            high_id = self.persist('visible-second')
            self.assertGreater(high_id, late_id)
            # Disable only the recent reserve so the test proves cyclic
            # recovery, rather than accidentally passing via the recent IDs.
            with patch.object(intake, 'RECENT_SIZE', 0):
                first = self.consume()
            self.assertEqual(first['accepted'], 1)
            self.assertEqual(first['cursor'], high_id)
            late.commit()
        # A fresh connection represents a restarted worker: no RAM cursor.
        with patch.object(intake, 'RECENT_SIZE', 0):
            self.assertEqual(self.consume()['accepted'], 1)
        with self.connect() as conn:
            ids = conn.execute('SELECT snapshot_set_id FROM research_watch_scan_intakes ORDER BY snapshot_set_id').fetchall()
            self.assertEqual([r['snapshot_set_id'] for r in ids], [late_id, high_id])
            self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM research_watch_scan_score_slots').fetchone()['n'], 224)

    def test_invalid_capture_gets_a_receipt_and_does_not_block_good_capture(self):
        failed = capture.failure('failed', 'fixture failure')
        self.persist('failed', block=failed)
        malformed, *_ = bundle(cycle_id='bad-coins')
        malformed['coins'] = ['not-an-object']
        self.persist('bad-coins', block=rehash(malformed))
        malformed, *_ = bundle(cycle_id='bad-slots')
        malformed['coins']['BTC']['maxpain'] = {'not': 'an-array'}
        self.persist('bad-slots', block=rehash(malformed))
        self.persist('good')
        result = self.consume()
        self.assertEqual((result['accepted'], result['rejected'], result['processed']), (1, 3, 4))
        self.assertEqual(self.consume()['processed'], 0)
        with self.connect() as conn:
            rejected = conn.execute("SELECT rejection_reason FROM research_watch_scan_intakes WHERE intake_status='REJECTED'").fetchone()
            self.assertTrue(rejected['rejection_reason'])
            self.assertEqual(conn.execute('SELECT COUNT(*) AS n FROM research_watch_scan_score_slots').fetchone()['n'], 112)

    def test_shared_lock_prevents_two_consumers_from_advancing(self):
        self.persist('locked')
        with self.connect() as owner:
            owner.execute('SELECT pg_advisory_xact_lock(%s)', (intake.LOCK_ID,))
            blocked = self.consume()
            self.assertTrue(blocked['locked'])
            self.assertEqual(blocked['processed'], 0)
        self.assertEqual(self.consume()['accepted'], 1)


if __name__ == '__main__':
    unittest.main()
