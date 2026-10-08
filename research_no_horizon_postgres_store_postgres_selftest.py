"""Real PostgreSQL durability, fencing and unchanged outcome semantics.

Only an explicit local/CI TEST_DATABASE_URL is accepted. Each test owns a
disposable schema. Every source, candle and result is synthetic test data;
there are no production reads, providers, notifications or trading calls.
"""
from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import research_no_horizon_cohort_outcomes as preparation
import research_no_horizon_cohort_store as local
import research_no_horizon_contract as contracts
import research_no_horizon_discovery as discovery
import research_no_horizon_ranking as ranking
import research_no_horizon_selection as selection
from research_no_horizon_selection_selftest import selection_fixture
import research_no_horizon_postgres_store as postgres
from research_no_horizon_cohort_coverage_selftest import cross_part_rows
from research_no_horizon_cohort_outcomes_selftest import outcome_fixture, _entry_time, _seal_exports
from research_no_horizon_parent_coverage_selftest import scope


TEST_DSN = os.environ.get('TEST_DATABASE_URL')
TABLES = ('research_no_horizon_plans', 'research_no_horizon_scopes',
          'research_no_horizon_candles', 'research_no_horizon_entries',
          'research_no_horizon_progress', 'research_no_horizon_receipts',
          'research_no_horizon_work_commits')
SEMANTIC_SCOPE_FIELDS = ('candidate_key', 'base_direction', 'analysis_direction',
    'threshold_pct', 'representatives', 'missing_entry_ids', 'missing_entry_locators',
    'status', 'snapshot_sha256', 'outcomes', 'gate', 'status_counts',
    'computation_complete', 'candle_evaluations', 'outcome_trial_executed')
SEMANTIC_PLAN_FIELDS = ('declared_scopes', 'outcome_trials_executed',
    'all_scopes_processed', 'computation_complete', 'source_coverage_complete',
    'coverage_receipt', 'parts_are_trials', 'scope_pooling', 'scope_ranking',
    'multiple_testing_adjusted', 'validated_discovery',
    'is_prospective_formula_evidence', 'runtime_authorized',
    'telegram_authorized', 'trading_authorized')


@unittest.skipUnless(TEST_DSN, 'Explicit local/CI TEST_DATABASE_URL is required')
class PostgresCohortStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        info = conninfo_to_dict(TEST_DSN)
        if (info.get('host') not in {'localhost', '127.0.0.1', '::1', 'postgres'}
                or not (info.get('dbname', '').startswith('test_')
                        or info.get('dbname', '').endswith('_test'))):
            raise ValueError('Integration requires an explicit local/CI test database')
        cls.psycopg, cls.sql, cls.dict_row = psycopg, sql, staticmethod(dict_row)
        migration = Path(__file__).resolve().parent / 'migrations' / '056_no_horizon_runtime.sql'
        cls.migration = migration.read_text()
        cls.original = outcome_fixture()

    def connect(self, *, schema=None):
        conn = self.psycopg.connect(TEST_DSN, row_factory=self.dict_row,
            connect_timeout=5, options='-c statement_timeout=15000 -c lock_timeout=1000')
        conn.execute(self.sql.SQL('SET search_path TO {}').format(
            self.sql.Identifier(schema or self.schema)))
        conn.commit()
        self.addCleanup(conn.close)
        return conn

    def drop_schema(self, schema):
        with self.psycopg.connect(TEST_DSN, autocommit=True, connect_timeout=5) as conn:
            conn.execute(self.sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                self.sql.Identifier(schema)))

    def create_schema(self):
        schema = 'test_no_horizon_' + uuid4().hex
        with self.psycopg.connect(TEST_DSN, autocommit=True, connect_timeout=5) as conn:
            conn.execute(self.sql.SQL('CREATE SCHEMA {}').format(self.sql.Identifier(schema)))
        self.addCleanup(self.drop_schema, schema)
        return schema

    def setUp(self):
        self.schema = self.create_schema()
        self.conn = self.connect()
        self.conn.execute(self.migration, prepare=False)
        self.conn.commit()
        self.store = postgres.PostgresCohortStore(self.conn)
        self.declaration, self.anchor, self.exports = deepcopy(self.original)

    def submit(self, *, fixture=None, store=None, **kwargs):
        declaration, anchor, exports = fixture or (self.declaration, self.anchor, self.exports)
        return (store or self.store).submit_cohort(declaration, anchor,
            lambda ordinal: exports[ordinal], **kwargs)

    def count(self, table):
        with self.conn.transaction():
            return self.conn.execute(self.sql.SQL('SELECT count(*) AS n FROM {}').format(
                self.sql.Identifier(table))).fetchone()['n']

    def states(self, plan):
        with self.conn.transaction():
            return self.conn.execute('''SELECT scope_ordinal,status,fencing_token,lease_until
                FROM research_no_horizon_scopes WHERE plan_id=%s ORDER BY scope_ordinal''',
                (plan,)).fetchall()

    def finish(self, plan, *, store=None, **kwargs):
        selected = store or self.store
        budgets = {'candle_budget': 100000, 'entry_budget': 128, 'batch_size': 128}
        budgets.update(kwargs)
        for _ in range(128):
            result = selected.report(plan)
            if result['all_scopes_processed']:
                return result
            selected.run_once('fixture-worker', plan_id=plan, **budgets)
        self.fail('bounded synthetic PostgreSQL cohort did not converge')

    def local_report(self, fixture):
        declaration, anchor, exports = fixture
        with tempfile.TemporaryDirectory() as directory:
            with local.LocalCohortStore(Path(directory) / 'reference.sqlite') as store:
                plan = store.submit_cohort(declaration, anchor, lambda ordinal: exports[ordinal])
                result = store.run_cohort(plan, 'reference', scope_budget=64,
                    candle_budget=100000, entry_budget=128, batch_size=128)
                self.assertTrue(result['all_scopes_processed'])
                return result

    def assert_semantic_parity(self, actual, expected):
        for field in SEMANTIC_PLAN_FIELDS:
            self.assertEqual(actual[field], expected[field], field)
        self.assertEqual(len(actual['scopes']), len(expected['scopes']))
        for left, right in zip(actual['scopes'], expected['scopes']):
            for field in SEMANTIC_SCOPE_FIELDS:
                self.assertEqual(left.get(field), right.get(field),
                    f"{left['candidate_key']}:{left['base_direction']}:{field}")
            for field in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
                self.assertIs(left[field], False)

    def test_fresh_migration_rollback_reapply_and_existing_evidence_preservation(self):
        temporary = self.create_schema()
        conn = self.connect(schema=temporary)
        self.assertFalse(postgres.schema_status(conn)['schema_present'])
        conn.execute(self.migration, prepare=False)
        conn.rollback()
        with conn.transaction():
            self.assertIsNone(conn.execute(
                "SELECT to_regclass('research_no_horizon_plans') AS relation").fetchone()['relation'])
        for _ in range(2):
            conn.execute(self.migration, prepare=False)
            conn.commit()
        self.assertTrue(postgres.schema_status(conn)['schema_present'])
        postgres.PostgresCohortStore(conn)
        plan = self.submit()
        before = self.finish(plan)
        self.conn.execute(self.migration, prepare=False)
        self.conn.commit()
        self.assertEqual(self.store.report(plan), before)
        self.assertEqual(self.count('research_no_horizon_plans'), 1)

    def test_six_declared_scopes_and_global_cross_part_outcomes_equal_sqlite(self):
        # The genuine capture fixture exposes one complete model. Declare all
        # six of its direction/threshold cells rather than forging availability
        # for unrelated models merely to match a real research plan's labels.
        scopes = [scope(direction, threshold=threshold)
            for threshold in (.2, .25, .3) for direction in ('SHORT', 'LONG')]
        # Keep the original SHORT fixture's actual prices when adding the
        # zero-match LONG cells; this preserves the earlier-loss/later-win
        # duplicate whose representative-selection behavior is under test.
        fixture = outcome_fixture(scopes=scopes, candles=deepcopy(self.original[2][0]['candles']))
        plan = self.submit(fixture=fixture)
        pending = self.store.report(plan)
        self.assertEqual(pending['declared_scopes'], 6)
        self.assertEqual(pending['outcome_trials_executed'], 0)
        self.assertEqual(self.count('research_no_horizon_scopes'), 6)
        actual = self.finish(plan)
        self.assert_semantic_parity(actual, self.local_report(fixture))
        self.assertEqual(actual['outcome_trials_executed'], 6)
        self.assertEqual(self.count('research_no_horizon_receipts'), 6)
        row = next(row for row in actual['scopes'] if row['candidate_key'] == 'FUTURES_CVD_TOTAL_65'
                   and row['base_direction'] == 'SHORT' and row['threshold_pct'] == .25)
        outcomes = {item['entry_id']: item['outcome']['status'] for item in row['outcomes']}
        self.assertEqual(outcomes['watch:3:BTC:SHORT'], 'FAILURE')
        self.assertNotIn('watch:4:BTC:SHORT', outcomes)
        self.assertEqual(row['gate']['selected_parents'], 5)

    def test_reopen_after_each_single_candle_batch_has_exact_final_parity(self):
        fixture = outcome_fixture(scopes=[scope(threshold=.25), scope(threshold=.2)])
        plan = self.submit(fixture=fixture)
        consumed = 0
        for _ in range(32):
            result = self.store.run_once('tiny', plan_id=plan,
                candle_budget=1, entry_budget=1, batch_size=1)
            self.assertLessEqual(result['candle_evaluations_this_run'], 1)
            consumed += result['candle_evaluations_this_run']
            self.conn.close()
            self.conn = self.connect()
            self.store = postgres.PostgresCohortStore(self.conn)
            if self.store.report(plan)['all_scopes_processed']:
                break
        else:
            self.fail('single-candle batches did not finish')
        actual = self.store.report(plan)
        self.assertEqual(consumed, 10)
        self.assert_semantic_parity(actual, self.local_report(fixture))

    def test_two_connections_cannot_share_live_lease_and_stale_claim_is_fenced(self):
        plan = self.submit()
        first = self.store.claim_job('first', lease_seconds=60, plan_id=plan)
        other_conn = self.connect()
        other = postgres.PostgresCohortStore(other_conn)
        self.assertIsNone(other.claim_job('second', plan_id=plan))
        with self.conn.transaction():
            self.conn.execute('''UPDATE research_no_horizon_scopes
                SET lease_until=clock_timestamp()-INTERVAL '1 second'
                WHERE plan_id=%s AND scope_ordinal=%s''', (plan, first['scope_ordinal']))
        second = other.claim_job('second', lease_seconds=60, plan_id=plan)
        self.assertEqual(second['scope_ordinal'], first['scope_ordinal'])
        self.assertGreater(second['fencing_token'], first['fencing_token'])
        with self.assertRaises(postgres.LeaseLost):
            self.store.process_claim(first)
        self.assertEqual(self.count('research_no_horizon_work_commits'), 0)
        self.assertEqual(self.count('research_no_horizon_receipts'), 0)
        other.process_claim(second, candle_budget=100000, entry_budget=128, batch_size=128)
        self.assertEqual(self.count('research_no_horizon_work_commits'), 1)
        self.assert_semantic_parity(self.store.report(plan), self.local_report(self.original))

    def test_concurrent_connections_claim_distinct_scopes(self):
        plan = self.submit(fixture=outcome_fixture(scopes=[scope(threshold=.25), scope(threshold=.2)]))
        first = self.store.claim_job('first', plan_id=plan)
        other = postgres.PostgresCohortStore(self.connect())
        second = other.claim_job('second', plan_id=plan)
        self.assertNotEqual(first['scope_ordinal'], second['scope_ordinal'])
        self.assertIsNone(other.claim_job('third', plan_id=plan))
        self.store.process_claim(first)
        other.process_claim(second)
        self.assertTrue(self.store.report(plan)['all_scopes_processed'])

    def test_checkpoint_receipt_and_work_counter_rollback_as_one_commit(self):
        plan = self.submit()
        claim = self.store.claim_job('interrupted', plan_id=plan)
        before = self.store.report(plan)
        # Fail after the receipt/checkpoint writes but before the work receipt
        # can commit. This exercises a real database transaction failure.
        with self.conn.transaction():
            self.conn.execute('''CREATE FUNCTION reject_fixture_commit() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture commit interruption'; END $$''')
            self.conn.execute('''CREATE TRIGGER reject_fixture_commit BEFORE INSERT
                ON research_no_horizon_work_commits FOR EACH ROW
                EXECUTE FUNCTION reject_fixture_commit()''')
        with self.assertRaisesRegex(self.psycopg.Error, 'fixture commit interruption'):
            self.store.process_claim(claim, candle_budget=100000, entry_budget=128, batch_size=128)
        self.assertEqual(self.store.report(plan), before)
        self.assertEqual(self.count('research_no_horizon_receipts'), 0)
        self.assertEqual(self.count('research_no_horizon_work_commits'), 0)
        with self.conn.transaction():
            self.conn.execute('DROP TRIGGER reject_fixture_commit ON research_no_horizon_work_commits')
        self.store.process_claim(claim, candle_budget=100000, entry_budget=128, batch_size=128)
        self.assert_semantic_parity(self.store.report(plan), self.local_report(self.original))

    def test_bad_worker_budgets_cannot_take_or_advance_a_lease(self):
        plan = self.submit()
        before = self.states(plan)
        for name in ('candle_budget', 'entry_budget', 'batch_size', 'lease_seconds'):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    self.store.run_once('invalid-budget', plan_id=plan, **{name: 0})
        self.assertEqual(self.states(plan), before)
        self.assertEqual(self.count('research_no_horizon_work_commits'), 0)

    def test_implementation_mismatch_skips_old_queue_without_reinterpreting_report(self):
        plan = self.submit()
        before = self.states(plan)
        versions = deepcopy(postgres.implementation())
        versions['test_incompatible_implementation'] = True
        with patch.object(postgres, 'implementation', return_value=versions):
            incompatible = postgres.PostgresCohortStore(self.connect())
            with self.assertRaisesRegex(ValueError, 'frozen implementation'):
                incompatible.claim_job('changed-code', plan_id=plan)
            self.assertIsNone(incompatible.claim_job('changed-code'))
            self.assertEqual(incompatible.report(plan)['declared_scopes'], 1)
        self.assertEqual(self.states(plan), before)
        expected = self.finish(plan)
        with patch.object(postgres, 'implementation', return_value=versions):
            historical = postgres.PostgresCohortStore(self.connect())
            self.assertEqual(historical.report(plan), expected)

    def test_missing_source_and_corrupt_last_part_leave_every_runtime_table_empty(self):
        rows = cross_part_rows()
        rows[1][1]['stored_source'] = None
        with self.assertRaises(preparation.CohortInputBlocked):
            self.submit(fixture=outcome_fixture(rows))
        self.exports[1]['source_rows'] = []
        with self.assertRaises(ValueError):
            self.submit()
        for table in TABLES:
            self.assertEqual(self.count(table), 0, table)

    def test_selected_missing_entry_stays_blocked_and_empty_scope_remains_a_trial(self):
        declaration, _, exports = outcome_fixture(scopes=[scope('SHORT'), scope('LONG')])
        missing = _entry_time(exports[0]['source_rows'][2]['intake']['usable_from_utc'])
        for export in exports:
            export['candles'] = [bar for bar in export['candles']
                if contracts.utc(bar['open_time_utc']) != missing]
        fixture = _seal_exports(declaration, exports)
        actual = self.finish(self.submit(fixture=fixture))
        self.assert_semantic_parity(actual, self.local_report(fixture))
        blocked = next(row for row in actual['scopes'] if row['base_direction'] == 'SHORT')
        empty = next(row for row in actual['scopes'] if row['base_direction'] == 'LONG')
        self.assertEqual(blocked['status'], 'INPUT_BLOCKED')
        self.assertIn('watch:3:BTC:SHORT', blocked['missing_entry_ids'])
        self.assertIsNone(blocked['gate'])
        self.assertEqual(empty['outcomes'], [])
        self.assertEqual(empty['status'], 'COMPLETE')
        self.assertEqual(actual['declared_scopes'], 2)
        self.assertEqual(actual['outcome_trials_executed'], 1)

    def test_idempotency_and_frozen_name_conflict_cannot_repoint_evidence(self):
        plan = self.submit(cohort_key='frozen')
        self.assertEqual(self.submit(cohort_key='frozen'), plan)
        self.assertEqual(self.submit(), plan)
        with self.assertRaisesRegex(ValueError, 'cohort.?key'):
            self.submit(fixture=outcome_fixture(mode='success'), cohort_key='frozen')
        self.assertEqual(self.count('research_no_horizon_plans'), 1)
        self.assertEqual(self.count('research_no_horizon_work_commits'), 0)
        self.assertEqual(self.count('research_no_horizon_receipts'), 0)

    def test_exact_resubmit_does_not_mix_old_counters_with_concurrent_checkpoint(self):
        plan = self.submit()
        other_conn = self.connect()
        with other_conn.transaction():
            other_conn.execute("SET lock_timeout='100ms'")
        other = postgres.PostgresCohortStore(other_conn)
        claim = other.claim_job('concurrent', plan_id=plan)
        original_verify = self.store._verify_materialized
        attempts = []

        def attempt_commit_while_verifying(*args, **kwargs):
            # A second real connection computes from the same frozen inputs,
            # but cannot commit while exact reuse verifies mutable progress.
            with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                other.process_claim(claim, candle_budget=1, entry_budget=1, batch_size=1)
            attempts.append(True)
            return original_verify(*args, **kwargs)

        with patch.object(self.store, '_verify_materialized', side_effect=attempt_commit_while_verifying):
            self.assertEqual(self.submit(), plan)
        self.assertEqual(attempts, [True])
        self.assertEqual(self.count('research_no_horizon_work_commits'), 0)
        result = other.process_claim(claim, candle_budget=1, entry_budget=1, batch_size=1)
        self.assertEqual(result['candle_evaluations_this_run'], 1)
        self.assertEqual(self.store.report(plan)['scopes'][0]['candle_evaluations'], 1)

    def test_frozen_gap_cannot_be_repaired_by_appending_a_late_candle(self):
        declaration, _, exports = outcome_fixture(mode='open')
        missing = _entry_time(exports[0]['source_rows'][0]['intake']['usable_from_utc']) + timedelta(minutes=1)
        original_bar = next(deepcopy(bar) for bar in exports[0]['candles']
            if contracts.utc(bar['open_time_utc']) == missing)
        for export in exports:
            export['candles'] = [bar for bar in export['candles']
                if contracts.utc(bar['open_time_utc']) != missing]
        fixture = _seal_exports(declaration, exports)
        plan = self.submit(fixture=fixture)
        with self.assertRaises(self.psycopg.Error):
            with self.conn.transaction():
                self.conn.execute('INSERT INTO research_no_horizon_candles VALUES(%s,%s,%s)',
                    (plan, missing, contracts.canonical(original_bar)))
        self.assert_semantic_parity(self.finish(plan), self.local_report(fixture))

    def test_input_and_receipt_update_delete_rejected_even_for_database_owner(self):
        plan = self.submit()
        before = self.finish(plan)
        immutable_columns = {'research_no_horizon_plans': 'identity_json',
            'research_no_horizon_candles': 'candle_json',
            'research_no_horizon_entries': 'metadata_json',
            'research_no_horizon_receipts': 'payload_json',
            'research_no_horizon_work_commits': 'fencing_token',
            'research_no_horizon_progress': 'checkpoint_json'}
        for table, column in immutable_columns.items():
            for operation in ('UPDATE', 'DELETE'):
                with self.subTest(table=table, operation=operation):
                    with self.assertRaises(self.psycopg.Error):
                        with self.conn.transaction():
                            query = ('UPDATE {} SET {}={}' if operation == 'UPDATE' else 'DELETE FROM {}')
                            identifiers = [self.sql.Identifier(table)]
                            if operation == 'UPDATE':
                                identifiers.extend([self.sql.Identifier(column)] * 2)
                            self.conn.execute(self.sql.SQL(query).format(*identifiers))
        with self.assertRaises(self.psycopg.Error):
            with self.conn.transaction():
                self.conn.execute("UPDATE research_no_horizon_scopes SET scope_plan_json='{}'")
        self.assertEqual(self.store.report(plan), before)

    def test_open_ambiguous_and_gap_outcomes_preserve_sqlite_semantics(self):
        for mode in ('open', 'ambiguous', 'gap'):
            with self.subTest(mode=mode):
                declaration, anchor, exports = outcome_fixture(mode='open' if mode == 'gap' else mode)
                if mode == 'gap':
                    missing = _entry_time(exports[0]['source_rows'][0]['intake']['usable_from_utc']) + timedelta(minutes=1)
                    for export in exports:
                        export['candles'] = [bar for bar in export['candles']
                            if contracts.utc(bar['open_time_utc']) != missing]
                    declaration, anchor, exports = _seal_exports(declaration, exports)
                fixture = declaration, anchor, exports
                actual = self.finish(self.submit(fixture=fixture))
                self.assert_semantic_parity(actual, self.local_report(fixture))
                self.assertEqual(actual['scopes'][0]['gate']['resolved_parents'], 0)

    def test_successful_research_gate_never_grants_runtime_delivery_or_trading(self):
        actual = self.finish(self.submit(fixture=outcome_fixture(mode='success')))
        gate = actual['scopes'][0]['gate']
        self.assertTrue(gate['experimental_eligible'])
        self.assertEqual(gate['successes'], 5)
        for value in (actual, actual['scopes'][0], gate):
            for name in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
                self.assertIs(value[name], False)

    def test_active_caller_transaction_rejected_without_committing_caller_work(self):
        conn = self.connect()
        conn.execute('CREATE TABLE caller_uncommitted(value integer)')
        with self.assertRaises(ValueError):
            postgres.PostgresCohortStore(conn)
        conn.rollback()
        with conn.transaction():
            self.assertIsNone(conn.execute("SELECT to_regclass('caller_uncommitted') AS relation").fetchone()['relation'])


    def test_discovery_ranking_reads_current_postgres_evidence_and_matches_sqlite(self):
        fixture = outcome_fixture(scopes=[scope('SHORT'), scope('LONG')],
                                  candles=deepcopy(self.original[2][0]['candles']))
        search = discovery.build_plan(fixture[0], base_directions=['SHORT', 'LONG'],
            thresholds_pct=[.25], candidate_keys=['FUTURES_CVD_TOTAL_65'])
        declared = search['calendar_plan']['windows'][0]['declaration']
        fixture = _seal_exports(declared, fixture[2])
        plan = self.submit(fixture=fixture)
        pending = ranking.rank_report(search, self.store.report(plan))
        self.assertFalse(pending['global_ranking_complete'])
        self.assertEqual(pending['ranked_scope_ids'], [])
        actual = self.finish(plan)
        expected = self.local_report(fixture)
        with self.conn.transaction():
            self.conn.execute('SET default_transaction_read_only=on')
        # The real persisted report is read under a read-only destination
        # session. Ranking itself opens no connection and creates no work.
        before = self.count('research_no_horizon_work_commits')
        postgres_rank = ranking.rank_report(search, self.store.report(plan))
        sqlite_rank = ranking.rank_report(search, expected)
        self.assertTrue(postgres_rank['global_ranking_complete'])
        self.assertEqual(postgres_rank['ranked_scope_ids'], sqlite_rank['ranked_scope_ids'])
        self.assertEqual(postgres_rank['rows'], sqlite_rank['rows'])
        self.assertEqual(before, self.count('research_no_horizon_work_commits'))
        self.assertEqual(len(postgres_rank['rows']), 2)
        for key in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
            self.assertIs(postgres_rank[key], False)




    def test_selection_two_windows_match_sqlite_without_new_work_or_parent_pooling(self):
        plan, fixtures = selection_fixture()
        ids = [self.submit(fixture=fixture) for fixture in fixtures]
        pending = selection.select_reports(plan, [self.store.report(item) for item in ids])
        self.assertFalse(pending['selection_complete'])
        self.assertEqual(pending['selected_scope_ids'], [])
        for item in ids:
            self.finish(item)
        local_reports = [self.local_report(fixture) for fixture in fixtures]
        expected = selection.select_reports(plan, local_reports)
        with self.conn.transaction():
            self.conn.execute('SET default_transaction_read_only=on')
        before = self.count('research_no_horizon_work_commits')
        actual = selection.select_reports(plan, [self.store.report(item) for item in ids])
        self.assertTrue(actual['selection_complete'])
        self.assertEqual(actual['selected_scope_ids'], expected['selected_scope_ids'])
        self.assertEqual(actual['selected_scopes'], expected['selected_scopes'])
        self.assertEqual(actual['rows'], expected['rows'])
        self.assertEqual(actual['denominator'], expected['denominator'])
        self.assertEqual(actual['matched_parent_overlap'], expected['matched_parent_overlap'])
        self.assertGreater(actual['matched_parent_overlap']['repeated_parent_count'], 0)
        self.assertEqual(actual['denominator']['declared_windows'], 2)
        self.assertTrue(actual['selected_scope_ids'])
        self.assertEqual(before, self.count('research_no_horizon_work_commits'))
        for key in ('runtime_authorized', 'telegram_authorized', 'trading_authorized',
                    'policy_registration_verified', 'window_pooling', 'validated_discovery'):
            self.assertIs(actual[key], False)


if __name__ == '__main__':
    unittest.main()
