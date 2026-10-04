"""Actual read-only source acquisition through durable admission and outcomes.

Every test owns a disposable local/CI database. Source captures, causal parents
and archived prices are genuine synthetic fixtures; none are market evidence.
No production database, external price source, alert sender or trading API runs.
"""
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import research_no_horizon_acquisition_source as source
import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_cohort as cohort
import research_no_horizon_cohort_coverage as coverage
import research_no_horizon_cohort_postgres_selftest as fixtures
import research_no_horizon_cohort_store as local
import research_no_horizon_contract as contracts
import research_no_horizon_postgres_store as executor
import research_price_archive as prices
import research_watch_scan_intake as intake
from research_no_horizon_postgres_store_postgres_selftest import (
    SEMANTIC_PLAN_FIELDS, SEMANTIC_SCOPE_FIELDS)
from research_watch_score_capture_selftest import BASE


TEST_DSN = os.environ.get('TEST_DATABASE_URL')
PREFIX = 'research_no_horizon_acquisition_'


@unittest.skipUnless(TEST_DSN, 'Explicit local/CI TEST_DATABASE_URL required')
class AcquisitionPostgresTests(unittest.TestCase):
    # Reuse real fixture setup by composition, never inherit another test suite.
    connect = fixtures.CohortPostgresTests.connect
    drop_database = fixtures.CohortPostgresTests.drop_database
    persist_on = fixtures.CohortPostgresTests.persist_on
    persist = fixtures.CohortPostgresTests.persist
    consume_intake = fixtures.CohortPostgresTests.consume_intake
    captured_bundle = fixtures.CohortPostgresTests.captured_bundle
    admit = fixtures.CohortPostgresTests.admit
    seed_causal_parent = fixtures.CohortPostgresTests.seed_causal_parent
    prepared = fixtures.CohortPostgresTests.prepared
    store_parents = fixtures.CohortPostgresTests.store_parents
    advance_parent = fixtures.CohortPostgresTests.advance_parent
    live_source = fixtures.CohortPostgresTests.live_source
    read_only = fixtures.CohortPostgresTests.read_only

    def setUp(self):
        fixtures.CohortPostgresTests.setUp(self)
        root = Path(__file__).resolve().parent / 'migrations'
        with self.connect() as conn:
            for number in ('056', '057'):
                migrations = list(root.glob(number + '_*.sql'))
                self.assertEqual(len(migrations), 1)
                conn.execute(migrations[0].read_text(), prepare=False)
        self.reopen()

    def reopen(self):
        if getattr(self, 'conn', None) is not None:
            self.conn.close()
        self.conn = self.connect()
        self.addCleanup(self.conn.close)
        self.store = acquisition.AcquisitionStore(self.conn)

    def seed_prices(self, *, omit=()):
        bars = []
        for minute in range(4):
            if minute in omit:
                continue
            opened = BASE + timedelta(minutes=7 + minute)
            bars.append(dict(open_time_utc=opened,
                close_time_utc=opened + timedelta(minutes=1, milliseconds=-1),
                open=100., high=100.2, low=98.4 if minute == 1 else 99.8,
                close=100., volume=1.))
        with self.connect() as conn:
            prices.write_bars(conn, prices.BINANCE_SPOT, 'XRP', bars)

    def declaration(self, *, single=False, both_directions=False):
        value = fixtures.CohortPostgresTests.declaration(self)
        if single:
            value['parts'] = [{**value['parts'][0], 'source_end_utc': value['source_end_utc']}]
        if both_directions:
            value['scopes'].append({**value['scopes'][0], 'base_direction': 'LONG'})
        return cohort.normalize_declaration(value)

    def count(self, table):
        with self.connect() as conn:
            return conn.execute(self.sql.SQL('SELECT count(*) AS n FROM {}').format(
                self.sql.Identifier(table))).fetchone()['n']

    def anchored(self, declaration):
        with self.connect() as conn:
            proof = source.read_anchor(conn, declaration)
        return proof, source.validate_anchor_proof(declaration, proof)

    def fetched_parts(self, declaration, anchor):
        grouped = [{'source': [], 'candles': []} for _ in declaration['parts']]
        for task in source.leaf_tasks(declaration, anchor):
            with self.connect() as conn:
                if task['kind'] == 'source':
                    proof = source.read_source_chunk(conn, declaration, anchor,
                        task['part_ordinal'], task['ordinals'])
                else:
                    proof = source.read_candle_chunk(conn, declaration, anchor,
                        task['part_ordinal'], task['ordinals'][0])
            source.validate_chunk_proof(declaration, anchor, proof)
            grouped[task['part_ordinal']][task['kind']].append(proof)
        return [source.assemble_part(declaration, anchor, ordinal,
            item['source'], item['candles']) for ordinal, item in enumerate(grouped)]

    def finish_acquisition(self, request_id, *, reopen=False):
        for _ in range(64):
            report = self.store.report(request_id, include_proofs=True)
            if report['status'] in ('ADMITTED', 'BLOCKED'):
                return report
            self.store.run_once('fixture-acquisition', request_id=request_id,
                source_connection_factory=self.connect, leaf_budget=1)
            if reopen:
                self.reopen()
        self.fail('finite acquisition fixture did not finish')

    def acquired_parts(self, declaration, report):
        anchor = report['anchor']
        result = []
        for ordinal in range(len(declaration['parts'])):
            leaves = [leaf for leaf in report['leaves'] if leaf['task']['part_ordinal'] == ordinal]
            result.append(source.assemble_part(declaration, anchor, ordinal,
                [leaf['proof'] for leaf in leaves if leaf['task']['kind'] == 'source'],
                [leaf['proof'] for leaf in leaves if leaf['task']['kind'] == 'candles']))
        return result

    def finish_executor(self, plan_id):
        store = executor.PostgresCohortStore(self.conn)
        for _ in range(32):
            report = store.report(plan_id)
            if report['all_scopes_processed']:
                return report
            store.run_once('fixture-executor', plan_id=plan_id,
                candle_budget=1, entry_budget=1, batch_size=1)
        self.fail('small admitted fixture did not finish')

    def assert_local_parity(self, declaration, acquired, actual):
        parts = self.acquired_parts(declaration, acquired)
        with tempfile.TemporaryDirectory() as directory:
            with local.LocalCohortStore(Path(directory) / 'reference.sqlite') as store:
                plan = store.submit_cohort(declaration, acquired['anchor'], lambda ordinal: parts[ordinal])
                expected = store.run_cohort(plan, 'reference', scope_budget=64, candle_budget=10000)
        for field in SEMANTIC_PLAN_FIELDS:
            self.assertEqual(actual[field], expected[field], field)
        self.assertEqual(len(actual['scopes']), len(expected['scopes']))
        for left, right in zip(actual['scopes'], expected['scopes']):
            for field in SEMANTIC_SCOPE_FIELDS:
                self.assertEqual(left.get(field), right.get(field), field)

    def test_actual_read_only_anchor_and_raw_leaf_proofs_reproduce_frozen_parts(self):
        ident = self.prepared()
        declaration = self.declaration()
        proof, anchor = self.anchored(declaration)
        self.assertEqual(proof['raw_response_sha256'], hashlib.sha256(
            proof['raw_response_text'].encode('utf-8')).hexdigest())
        self.assertEqual(proof['raw_response_bytes'], len(proof['raw_response_text'].encode('utf-8')))
        self.assertEqual(proof['query_sha256'], hashlib.sha256(proof['sql'].encode('utf-8')).hexdigest())
        self.assertEqual(anchor['extraction_receipt']['read_only'], 'on')
        self.assertFalse(anchor['extraction_receipt']['database_writes'])
        parts = self.fetched_parts(declaration, anchor)
        self.assertEqual(parts[0]['source_rows'], [])
        self.assertEqual([row['intake']['snapshot_set_id'] for row in parts[1]['source_rows']], [ident])
        self.assertEqual(self.count(PREFIX + 'requests'), 0)
        self.assertEqual(self.count('research_no_horizon_plans'), 0)

    def test_parent_progress_and_closure_do_not_rewrite_anchored_source(self):
        ident = self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        before = self.fetched_parts(declaration, anchor)
        original_live = self.live_source(ident)['payload_text']
        for minute, close in ((8, 99.), (9, 102.)):
            self.advance_parent(minute, close)
            self.assertNotEqual(self.live_source(ident)['payload_text'], original_live)
            self.assertEqual(self.fetched_parts(declaration, anchor), before)

    def test_late_accepted_source_cannot_expand_the_frozen_anchor_population(self):
        ident = self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        self.admit('late-after-anchor')
        parts = self.fetched_parts(declaration, anchor)
        ids = [row['intake']['snapshot_set_id'] for part in parts for row in part['source_rows']]
        self.assertEqual(ids, [ident])
        self.assertEqual(self.count('research_watch_scan_intakes'), 2)

    def test_deleted_anchored_source_cannot_be_treated_as_complete_empty_input(self):
        ident = self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        with self.connect() as conn:
            conn.execute('''DELETE FROM research_watch_scan_intakes
                WHERE consumer_version=%s AND snapshot_set_id=%s''', (intake.VERSION, ident))
        with self.assertRaises(ValueError):
            self.fetched_parts(declaration, anchor)
        self.assertEqual(self.count('research_no_horizon_plans'), 0)

    def test_mutated_live_score_cannot_pass_original_anchor_hash(self):
        ident = self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        with self.connect() as conn:
            conn.execute('''UPDATE research_max_pain_snapshot_sets SET source_metadata=jsonb_set(
                source_metadata,'{capture_metadata,operational_scores,coins,XRP,models,futures_flow,score}',
                '-74'::jsonb,false) WHERE snapshot_set_id=%s''', (ident,))
        with self.assertRaises(ValueError):
            self.fetched_parts(declaration, anchor)

    def test_rehashed_transport_response_with_extra_row_remains_invalid(self):
        self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        task = next(task for task in source.leaf_tasks(declaration, anchor) if task['kind'] == 'source')
        with self.connect() as conn:
            proof = source.read_source_chunk(conn, declaration, anchor, task['part_ordinal'], task['ordinals'])
        forged = deepcopy(proof)
        raw = json.loads(forged['raw_response_text'])
        raw['entries'].append(deepcopy(raw['entries'][0]))
        raw['returned_count'] = len(raw['entries'])
        forged['raw_response_text'] = contracts.canonical(raw)
        data = forged['raw_response_text'].encode('utf-8')
        forged['raw_response_sha256'] = hashlib.sha256(data).hexdigest()
        forged['raw_response_bytes'] = len(data)
        with self.assertRaises(ValueError):
            source.validate_chunk_proof(declaration, anchor, forged)

    def test_restart_after_every_bounded_step_reuses_anchor_and_matches_local_results(self):
        self.prepared()
        declaration = self.declaration(both_directions=True)
        request = self.store.register_request(declaration, request_key='restartable')
        frozen_anchor, observed_leaves = None, []
        for _ in range(64):
            report = self.store.report(request, include_proofs=True)
            if report['anchor_sha256']:
                frozen_anchor = frozen_anchor or report['anchor_sha256']
                self.assertEqual(report['anchor_sha256'], frozen_anchor)
            observed_leaves.append(report['leaves_completed'])
            if report['status'] == 'ADMITTED':
                break
            self.assertNotEqual(report['status'], 'BLOCKED')
            self.store.run_once('restarted', request_id=request,
                source_connection_factory=self.connect, leaf_budget=1)
            self.reopen()
        else:
            self.fail('restarted acquisition did not finish')
        self.assertEqual(observed_leaves, sorted(observed_leaves))
        self.assertEqual(report['leaves_completed'], report['total_leaves'])
        self.assertEqual(self.count(PREFIX + 'anchors'), 1)
        self.assertEqual(self.count('research_no_horizon_plans'), 1)
        actual = self.finish_executor(report['executor_plan_id'])
        self.assert_local_parity(declaration, report, actual)
        self.assertEqual(actual['declared_scopes'], 2)
        self.assertEqual(actual['outcome_trials_executed'], 2)
        for value in (report, actual):
            for name in ('runtime_authorized', 'telegram_authorized', 'trading_authorized'):
                self.assertIs(value[name], False)

    def test_crash_after_executor_commit_before_link_reuses_exact_plan(self):
        self.prepared()
        declaration = self.declaration(single=True)
        request = self.store.register_request(declaration)
        with patch.object(self.store, '_mark_admitted', side_effect=RuntimeError('crash after admission')):
            with self.assertRaisesRegex(RuntimeError, 'crash after admission'):
                self.finish_acquisition(request)
        self.assertEqual(self.count('research_no_horizon_plans'), 1)
        self.assertNotEqual(self.store.report(request)['status'], 'ADMITTED')
        self.reopen()
        report = self.finish_acquisition(request, reopen=True)
        self.assertEqual(report['status'], 'ADMITTED')
        self.assertEqual(self.count('research_no_horizon_plans'), 1)
        self.assert_local_parity(declaration, report, self.finish_executor(report['executor_plan_id']))

    def test_failed_leaf_retains_rejected_proof_and_never_refreshes_anchor(self):
        ident = self.prepared()
        request = self.store.register_request(self.declaration())
        self.store.run_once('anchor-only', request_id=request,
            source_connection_factory=self.connect, leaf_budget=1)
        before = self.store.report(request, include_proofs=True)
        self.assertEqual(before['leaves_completed'], 0)
        self.assertIsNotNone(before['anchor_sha256'])
        with self.connect() as conn:
            conn.execute('''DELETE FROM research_watch_scan_intakes
                WHERE consumer_version=%s AND snapshot_set_id=%s''', (intake.VERSION, ident))
        report = self.finish_acquisition(request, reopen=True)
        self.assertEqual(report['status'], 'BLOCKED')
        self.assertEqual(report['anchor_proof'], before['anchor_proof'])
        self.assertEqual(report['anchor_sha256'], before['anchor_sha256'])
        rejected = report['terminal_receipt']['rejected_proof']
        self.assertEqual(rejected['kind'], 'source')
        self.assertTrue(report['terminal_receipt']['raw_response_retained'])
        self.assertEqual(json.loads(rejected['raw_response_text'])['returned_count'], 0)
        compact = self.store.report(request)
        self.assertNotIn('"raw_response_text"', contracts.canonical(compact))
        self.assertNotIn('anchor_proof', compact)
        self.assertNotIn('leaves', compact)
        self.assertNotIn('rejected_proof', compact['terminal_receipt'])
        summary = compact['terminal_receipt']['rejected_proof_summary']
        self.assertEqual(summary['raw_response_sha256'], rejected['raw_response_sha256'])
        self.assertEqual(summary['raw_response_bytes'], rejected['raw_response_bytes'])
        self.assertTrue(summary['full_proof_omitted_from_report'])
        explicit = self.store.report(request, include_proofs=True)
        self.assertEqual(explicit['terminal_receipt']['rejected_proof'], rejected)
        self.assertEqual(self.count('research_no_horizon_plans'), 0)
        forbidden = Mock(side_effect=AssertionError('terminal request refreshed source'))
        self.assertIsNone(self.store.run_once('blocked-retry', request_id=request,
            source_connection_factory=forbidden))
        forbidden.assert_not_called()

    def test_transient_failure_after_anchor_reopens_without_refetching_population(self):
        self.prepared()
        declaration = self.declaration()
        request = self.store.register_request(declaration)
        self.store.run_once('anchor-only', request_id=request,
            source_connection_factory=self.connect, leaf_budget=1)
        before = self.store.report(request, include_proofs=True)
        with self.assertRaisesRegex(RuntimeError, 'fixture source unavailable'):
            self.store.run_once('interrupted', request_id=request, leaf_budget=1,
                source_connection_factory=Mock(side_effect=RuntimeError('fixture source unavailable')))
        failed = self.store.report(request, include_proofs=True)
        self.assertEqual(failed['status'], 'FETCHING')
        self.assertIsNone(failed['lease_until'])
        self.assertEqual(failed['anchor_proof'], before['anchor_proof'])
        self.reopen()
        report = self.finish_acquisition(request)
        self.assertEqual(report['status'], 'ADMITTED')
        self.assertEqual(report['anchor_proof'], before['anchor_proof'])
        self.assert_local_parity(declaration, report, self.finish_executor(report['executor_plan_id']))

    def test_not_due_request_never_opens_a_source_connection(self):
        declaration = self.declaration()
        with self.connect() as conn:
            now = conn.execute('SELECT clock_timestamp() AS now').fetchone()['now']
        delta = now + timedelta(days=1) - contracts.utc(declaration['source_start_utc'])
        for key in ('source_start_utc', 'source_end_utc', 'cutoff_utc'):
            declaration[key] = (contracts.utc(declaration[key]) + delta).isoformat()
        declaration['declared_at_utc'] = now.isoformat()
        for part in declaration['parts']:
            for key in ('source_start_utc', 'source_end_utc'):
                part[key] = (contracts.utc(part[key]) + delta).isoformat()
        declaration = cohort.normalize_declaration(declaration)
        request = self.store.register_request(declaration)
        factory = Mock(side_effect=AssertionError('premature source connection'))
        self.assertIsNone(self.store.run_once('too-early', request_id=request,
            source_connection_factory=factory))
        factory.assert_not_called()
        self.assertEqual(self.store.report(request)['status'], 'WAITING')
        self.assertEqual(self.count(PREFIX + 'anchors'), 0)
        self.assertEqual(self.count(PREFIX + 'leaves'), 0)
        self.assertEqual(self.count('research_no_horizon_plans'), 0)
        with self.connect() as conn:
            with self.assertRaises(source.NotDue):
                source.read_anchor(conn, declaration)

    def test_reclaimed_acquisition_lease_fences_old_worker_before_source_query(self):
        self.prepared()
        request = self.store.register_request(self.declaration())
        old = self.store.claim_request('old-owner', request_id=request)
        with self.connect() as other_conn:
            other = acquisition.AcquisitionStore(other_conn)
            self.assertIsNone(other.claim_request('new-owner', request_id=request))
            with self.conn.transaction():
                self.conn.execute('''UPDATE research_no_horizon_acquisition_requests
                    SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE request_id=%s''',
                    (request,))
            new = other.claim_request('new-owner', request_id=request)
            self.assertGreater(new['fencing_token'], old['fencing_token'])
            forbidden = Mock(side_effect=AssertionError('stale owner reached source'))
            with self.assertRaises(executor.LeaseLost):
                self.store.process_claim(old, source_connection_factory=forbidden, leaf_budget=1)
            forbidden.assert_not_called()
            self.assertEqual(self.count(PREFIX + 'anchors'), 0)
            other.process_claim(new, source_connection_factory=self.connect, leaf_budget=1)
        report = self.finish_acquisition(request)
        self.assertEqual(report['status'], 'ADMITTED')
        self.assertEqual(self.count(PREFIX + 'anchors'), 1)

    def test_complete_empty_population_is_admitted_and_reported_not_dropped(self):
        self.seed_prices()
        declaration = self.declaration(both_directions=True)
        report = self.finish_acquisition(self.store.register_request(declaration))
        self.assertEqual(report['status'], 'ADMITTED')
        actual = self.finish_executor(report['executor_plan_id'])
        self.assert_local_parity(declaration, report, actual)
        self.assertEqual(actual['declared_scopes'], 2)
        self.assertEqual(actual['outcome_trials_executed'], 2)
        self.assertTrue(all(row['outcomes'] == [] for row in actual['scopes']))

    def test_unknown_feature_blocks_all_scopes_before_executor_plan_write(self):
        self.admit('unknown-futures', futures_available=False)
        self.seed_causal_parent()
        self.seed_prices()
        declaration = self.declaration(both_directions=True)
        report = self.finish_acquisition(self.store.register_request(declaration))
        self.assertEqual(report['status'], 'BLOCKED')
        self.assertIsNone(report['executor_plan_id'])
        self.assertEqual(self.count('research_no_horizon_plans'), 0)
        self.assertEqual(self.count('research_no_horizon_receipts'), 0)
        self.assertEqual(report['leaves_completed'], report['total_leaves'])
        terminal = report['terminal_receipt']
        self.assertIsNone(terminal['rejected_proof'])
        self.assertFalse(terminal['raw_response_retained'])
        self.assertTrue(terminal['coverage_receipt_retained'])
        retained = terminal['coverage_receipt']
        parts = self.acquired_parts(declaration, report)
        expected = coverage.preflight_cohort(declaration, report['anchor'], lambda ordinal: parts[ordinal])
        self.assertEqual(retained, expected)
        self.assertEqual(retained['accepted_source_rows'], 1)
        self.assertEqual(len(retained['scopes']), 2)
        self.assertEqual({row['base_direction'] for row in retained['scopes']}, {'LONG', 'SHORT'})
        for row in retained['scopes']:
            self.assertEqual(row['coverage_status'], 'BLOCKED')
            self.assertEqual(row['source_counts']['UNKNOWN'], 1)
            self.assertEqual(row['source_counts']['UNKNOWN_SOURCE'], 0)
        self.assertEqual(retained['outcome_trials_executed'], 0)
        self.assertFalse(retained['outcomes_evaluated'])
        compact = self.store.report(report['request_id'])
        self.assertNotIn('"raw_response_text"', contracts.canonical(compact))
        self.assertNotIn('coverage_receipt', compact['terminal_receipt'])
        summary = compact['terminal_receipt']['coverage_receipt_summary']
        self.assertEqual(summary['receipt_sha256'], retained['receipt_sha256'])
        self.assertEqual(summary['declared_scope_count'], 2)
        self.assertEqual(summary['coverage_status_counts']['BLOCKED'], 2)
        self.assertTrue(summary['full_receipt_omitted_from_report'])
        self.assertEqual([row['source_counts'] for row in summary['scopes']],
                         [row['source_counts'] for row in retained['scopes']])

    def test_missing_selected_entry_keeps_blocked_scope_and_zero_match_scope(self):
        self.admit('missing-entry')
        self.seed_causal_parent()
        self.seed_prices(omit=(0,))
        declaration = self.declaration(both_directions=True)
        report = self.finish_acquisition(self.store.register_request(declaration))
        self.assertEqual(report['status'], 'ADMITTED')
        actual = self.finish_executor(report['executor_plan_id'])
        self.assert_local_parity(declaration, report, actual)
        by_direction = {row['base_direction']: row for row in actual['scopes']}
        self.assertEqual(by_direction['SHORT']['status'], 'INPUT_BLOCKED')
        self.assertEqual(by_direction['LONG']['status'], 'COMPLETE')
        self.assertEqual(actual['declared_scopes'], 2)
        self.assertEqual(actual['outcome_trials_executed'], 1)

    def test_retained_anchor_leaf_and_terminal_proof_are_immutable(self):
        self.prepared()
        report = self.finish_acquisition(self.store.register_request(self.declaration()))
        self.assertEqual(report['status'], 'ADMITTED')
        for suffix, column in (('anchors', 'proof_json'), ('leaves', 'proof_json'),
                               ('receipts', 'payload_json')):
            for operation in ('UPDATE', 'DELETE'):
                with self.subTest(table=suffix, operation=operation):
                    with self.assertRaises(self.psycopg.Error):
                        with self.conn.transaction():
                            if operation == 'UPDATE':
                                self.conn.execute(self.sql.SQL('UPDATE {} SET {}={}').format(
                                    self.sql.Identifier(PREFIX + suffix),
                                    self.sql.Identifier(column), self.sql.Identifier(column)))
                            else:
                                self.conn.execute(self.sql.SQL('DELETE FROM {}').format(
                                    self.sql.Identifier(PREFIX + suffix)))
        self.assertEqual(self.store.report(report['request_id'], include_proofs=True), report)

    def test_acquisition_migration_rollback_and_reapply_preserve_completed_evidence(self):
        root = Path(__file__).resolve().parent / 'migrations'
        sql056 = next(root.glob('056_*.sql')).read_text()
        sql057 = next(root.glob('057_*.sql')).read_text()
        with self.connect() as conn:
            with self.assertRaisesRegex(RuntimeError, 'rollback new acquisition schema'):
                with conn.transaction():
                    conn.execute('CREATE SCHEMA acquisition_migration_probe')
                    conn.execute('SET LOCAL search_path TO acquisition_migration_probe')
                    conn.execute(sql056, prepare=False)
                    conn.execute(sql057, prepare=False)
                    raise RuntimeError('rollback new acquisition schema')
            row = conn.execute("SELECT to_regclass('acquisition_migration_probe."
                "research_no_horizon_acquisition_requests') AS relation").fetchone()
            self.assertIsNone(row['relation'])
        self.prepared()
        request = self.store.register_request(self.declaration())
        before = self.finish_acquisition(request)
        self.assertEqual(before['status'], 'ADMITTED')
        with self.connect() as conn:
            conn.execute(sql057, prepare=False)
            conn.execute(sql057, prepare=False)
        self.assertEqual(self.store.report(request, include_proofs=True), before)

    def test_executor_fence_failure_before_commit_rolls_back_whole_admission(self):
        self.prepared()
        declaration = self.declaration()
        _, anchor = self.anchored(declaration)
        parts = self.fetched_parts(declaration, anchor)
        guarded = executor.PostgresCohortStore(self.conn)
        calls = []

        def fence(conn):
            self.assertIs(conn, self.conn)
            calls.append(True)
            if len(calls) == 2:
                raise executor.LeaseLost('fixture expired at final admission fence')

        with self.assertRaisesRegex(executor.LeaseLost, 'final admission fence'):
            guarded.submit_cohort(declaration, anchor, lambda ordinal: parts[ordinal],
                _transaction_guard=fence)
        self.assertEqual(calls, [True, True])
        for table in executor.TABLES:
            if table != 'research_no_horizon_schema':
                self.assertEqual(self.count(table), 0, table)


if __name__ == '__main__':
    unittest.main()
