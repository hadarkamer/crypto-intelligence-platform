"""Artificial prospective receipts exercise the isolated DOGE observer engine."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import research_liquidity_forward_engine as core

ROOT = Path(__file__).resolve().parent
BASE = datetime(2026, 11, 1, tzinfo=timezone.utc)


def stamp(seconds):
    return (BASE + timedelta(seconds=seconds)).isoformat().replace('+00:00', 'Z')


def sha(value):
    return hashlib.sha256(core._json(value).encode()).hexdigest()


def policy():
    return {'version': core.STUDY_ID, 'protocol_sha256': core.PROTOCOL_SHA256,
            'study_start_utc': stamp(0), 'books': ['DOGE_only'],
            'scopes': [{'scope_key': 'isolated-doge', 'candidate_key': core.CANDIDATE_KEY,
                        'candidate_definition_sha256': core.PROTOCOL_SHA256,
                        'formula_version': core.FORMULA_VERSION, 'symbol': 'DOGE',
                        'direction': 'SHORT', 'window_minutes': 720, 'threshold_bps': 50}],
            'required_markers': sorted(core.REQUIRED_MARKERS), 'barrier_bps': 50,
            'net_cost_pp': 0.1, 'poll_seconds': 10, 'entry_expiry_seconds': 90,
            'quote_max_elapsed_seconds': 5, 'max_quote_gap_seconds': 30,
            'quote_source': 'BINANCE_SPOT_BOOK_TICKER', 'quote_symbols': {'DOGE': 'DOGEUSDT'}}


def identity(commit='1' * 40, **extra):
    return {'protocol_sha256': core.PROTOCOL_SHA256, 'runtime_commit': commit,
            'code_versions': {name: 'a' * 64 for name in core.SOURCE_CODE_FILES},
            'python_implementation': 'CPython', 'python_version': '3.11.9',
            'observed_at_utc': stamp(0.05), **extra}


def row(event_id=1, parent='parent-1', alert=1810, parent_start=1, **extra):
    result = {'scope_key': 'isolated-doge', 'candidate_key': core.CANDIDATE_KEY,
              'candidate_definition_sha256': core.PROTOCOL_SHA256, 'candidate_formula_version': core.FORMULA_VERSION,
              'protocol_sha256': core.PROTOCOL_SHA256, 'source_point_status': 'VALID',
              'conformance_status': 'CONFORMANCE_MATCH', 'computed_entry_ordinal': 2,
              'conformance_evidence_sha256': 'b' * 64, 'source_contract': 'MAX_PAIN_CAPTURED_AMOUNTS_V1',
              'pending_prior_count': 0, 'source_history_uncertain': False, 'quality_refs': [],
              'event_id': event_id, 'source_event_id': event_id, 'symbol': 'DOGE',
              'formula_direction': 'SHORT', 'event_kind': 'ALERT', 'delivery_status': 'DELIVERED',
              'event_fingerprint': f'{event_id:064x}', 'membership_status': 'LIVE',
              'parent_evidence_eligible': True, 'btc_parent_movement_id': parent,
              'episode_policy_version': core.PARENT_VERSION, 'parent_start_time_utc': stamp(parent_start),
              'match_source_time_utc': stamp(alert)}
    result.update({name: stamp(alert) for name in core.REQUIRED_MARKERS})
    result['parent_confirmed_at_utc'] = stamp(parent_start)
    result['btc_observed_close_utc'] = stamp(alert - 0.2)
    result.update(extra)
    return result


def raw(source):
    return {key: source[key] for key in ('event_id', 'event_fingerprint', 'symbol', 'alert_time_utc')}


def packet(identifier, received, rows=(), *, observed=None, bootstrap=False, parents=(), closure=None):
    observed = [raw(item) for item in rows] if observed is None else observed
    closure = {'events': observed, 'parents': list(parents), 'proof_objects': {}} if closure is None else closure
    return {'poll_id': identifier, 'request_started_utc': stamp(received - 0.01),
            'response_received_utc': stamp(received), 'rows': list(rows), 'observed_sources': observed,
            'bootstrap': bootstrap, 'observed_parent_ids': list(parents), 'source_complete': True,
            'source_closure': closure, 'source_closure_sha256': sha(closure)}


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='_liquidity_engine_test_', dir=ROOT)
        self.target = Path(self.temp.name).resolve()
        self.assertTrue(self.target.is_relative_to(ROOT) and self.target != ROOT)
        self.addCleanup(self.cleanup_directory)
        self.path = self.target / 'study.sqlite'
        self.policy = policy()
        self.engine = core.PaperStudy(self.path, self.policy)
        self.addCleanup(lambda: self.engine.close())
        self.engine.observe_runtime_identity(identity())
        self.engine.observe_candidates(packet('bootstrap', 0.2, bootstrap=True))

    def cleanup_directory(self):
        assert self.target.is_relative_to(ROOT) and self.target != ROOT
        self.temp.cleanup()

    def observe(self, sources, received=1820, identifier=None, **extra):
        return self.engine.observe_candidates(packet(identifier or f'poll-{received}', received, sources, **extra))

    def quotes(self, received, price=100, *, started=None, symbol='DOGE', identifier=None, **extra):
        return self.engine.observe_quotes({'packet_id': identifier or f'quote-{received}',
            'request_started_utc': stamp(received - 0.1 if started is None else started),
            'response_received_utc': stamp(received), 'source': 'BINANCE_SPOT_BOOK_TICKER',
            'quotes': [{'symbol': symbol, 'bid': str(price), 'ask': str(price)}], **extra})

    def book(self):
        return self.engine.summary()['books']['DOGE_only']

    def choose(self, event_id=1, parent='parent-1', received=1820, **extra):
        self.observe([row(event_id, parent, alert=received - 1, **extra)], received=received)

    def test_only_doge_book_and_new_protocol(self):
        summary = self.engine.summary()
        self.assertEqual(set(summary['books']), {'DOGE_only'})
        self.assertEqual(summary['protocol_sha256'], core.PROTOCOL_SHA256)
        self.assertEqual(summary['study_id'], core.STUDY_ID)

    def test_registry_identity_is_idempotent(self):
        before = self.engine.summary()['journal_events']
        self.engine.observe_runtime_identity(identity(observed_at_utc=stamp(1)))
        self.assertEqual(self.engine.summary()['journal_events'], before)
        self.assertEqual(self.engine.source_code_registry()['1' * 40], identity()['code_versions'])

    def test_registry_conflicting_source_bytes_rejected(self):
        altered = identity()
        altered['code_versions']['analysis.py'] = 'c' * 64
        with self.assertRaises(ValueError):
            self.engine.observe_runtime_identity(altered)
        self.assertEqual(self.engine.source_code_registry()['1' * 40]['analysis.py'], 'a' * 64)

    def test_trusted_seed_registration_and_same_pins_restart(self):
        seed = {'2' * 40: {name: 'c' * 64 for name in core.SOURCE_CODE_FILES}}
        self.engine.observe_runtime_identity(identity(trusted_seed_registries=seed))
        self.assertEqual(set(self.engine.source_code_registry()), {'1' * 40, '2' * 40})
        self.engine.observe_runtime_identity(identity(commit='2' * 40, code_versions=seed['2' * 40]))
        self.assertEqual(len(self.engine.source_code_registry()), 2)

    def test_registry_malformed_or_before_epoch_is_rejected(self):
        for change in ({'protocol_sha256': 'c' * 64}, {'runtime_commit': 'invalid'}, {'python_version': '3.12.0'},
                       {'code_versions': {}}, {'observed_at_utc': stamp(-1)}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.engine.observe_runtime_identity(identity(**change))

    def test_full_source_closure_is_deduplicated_receipts_are_not(self):
        same = {'events': [], 'parents': [], 'proof_objects': {}}
        self.observe([], 1810, 'receipt-a', closure=same)
        self.observe([], 1820, 'receipt-b', closure=same)
        closure_key = 'SOURCE_CLOSURE:' + sha(same)
        self.assertEqual(self.engine.db.execute('SELECT count(*) FROM journal WHERE input_key=?', (closure_key,)).fetchone()[0], 1)
        receipts = self.engine.db.execute("SELECT payload_json FROM journal WHERE kind='INPUT_CANDIDATES'").fetchall()
        self.assertEqual(len(receipts), 3)  # includes the excluded bootstrap receipt
        self.assertTrue(all('source_closure' not in json.loads(value[0]) for value in receipts))

    def test_changed_closure_with_same_claimed_hash_is_rejected(self):
        value = packet('bad-hash', 1820)
        value['source_closure']['unhashed_change'] = True
        with self.assertRaises(ValueError):
            self.engine.observe_candidates(value)

    def test_incomplete_source_packet_rejected(self):
        value = packet('incomplete', 1820)
        value['source_complete'] = False
        with self.assertRaises(ValueError):
            self.engine.observe_candidates(value)

    def test_raw_first_seen_without_match(self):
        item = row(7)
        self.observe([], observed=[raw(item)], received=1820)
        summary = self.engine.summary()
        self.assertEqual(summary['raw_first_seen_count'], 1)
        self.assertEqual(summary['first_eligible_seen_count'], 0)
        self.observe([item], received=1830)
        record = self.engine._load()['first_seen']['isolated-doge:7']
        self.assertEqual(record['raw_first_seen_utc'], stamp(1820))
        self.assertEqual(record['first_eligible_seen_utc'], stamp(1830))

    def test_first_eligible_receipt_and_source_closure_frozen(self):
        self.choose()
        first = deepcopy(self.engine._load()['first_seen']['isolated-doge:1'])
        self.observe([row(1, alert=1810)], received=1900)
        self.assertEqual(self.engine._load()['first_seen']['isolated-doge:1']['first_eligible_seen_utc'], first['first_eligible_seen_utc'])
        self.assertEqual(self.engine._load()['first_seen']['isolated-doge:1']['first_eligible_source_closure_sha256'], first['first_eligible_source_closure_sha256'])

    def test_late_older_row_cannot_replace_parent_winner(self):
        self.choose(event_id=9)
        self.observe([row(3, alert=1805)], received=1830)
        self.assertEqual(self.book()['choices']['parent-1']['event_id'], 9)

    def test_same_response_tie_order_uses_marker_then_event_id(self):
        self.observe([row(9, alert=1810), row(3, alert=1810)], received=1820)
        self.assertEqual(self.book()['choices']['parent-1']['event_id'], 3)

    def test_source_clock_warmup_not_cured_by_late_receipt(self):
        self.observe([row(alert=1799)], received=1900)
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 0)
        self.assertEqual(self.engine._load()['first_seen']['isolated-doge:1']['last_ineligible_reason'], 'SOURCE_IN_30M_WARMUP')

    def test_exact_warmup_boundary_is_eligible(self):
        self.observe([row(alert=1800)], received=1810)
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 1)

    def test_preparation_ids_remain_excluded(self):
        self.observe([row(event_id=455744)], received=1820)
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 0)

    def test_native_missing_or_unverified_does_not_enter(self):
        for status in ('MISSING', 'UNVERIFIED', None):
            item = row(event_id=10 + len(str(status)), source_point_status=status)
            self.observe([item], received=1820 + len(str(status)))
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 0)

    def test_gate_rejects_protocol_predicate_direction_and_parent_drift(self):
        changes = ({'protocol_sha256': 'c' * 64}, {'computed_entry_ordinal': 3}, {'computed_entry_ordinal': True},
                   {'conformance_status': 'UNVERIFIED'}, {'symbol': 'ETH'}, {'formula_direction': 'LONG'},
                   {'membership_status': 'HISTORICAL'}, {'episode_policy_version': 'different'},
                   {'parent_evidence_eligible': 'true'}, {'conformance_evidence_sha256': 'invalid'})
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                self.observe([row(event_id=100 + index, **change)], received=1820 + index)
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 0)

    def test_source_parent_and_btc_causal_guards(self):
        changes = ({'parent_start_time_utc': stamp(0)}, {'parent_confirmed_at_utc': stamp(1811)},
                   {'event_created_at_utc': stamp(1900)}, {'btc_observed_close_utc': stamp(1750)},
                   {'btc_observed_close_utc': stamp(1811)})
        for index, change in enumerate(changes):
            self.observe([row(event_id=200 + index, **change)], received=1820 + index)
        self.assertEqual(self.engine.summary()['first_eligible_seen_count'], 0)

    def test_candidate_missing_raw_source_rejects_packet(self):
        result = self.observe([row()], observed=[], received=1820)
        self.assertEqual(result['first_eligible_seen_count'], 0)
        self.assertIn('no source', result['last_observation_rejected'])

    def test_duplicate_source_identity_rejects_packet(self):
        item = row()
        result = self.observe([item], observed=[raw(item), raw(item)], received=1820)
        self.assertIn('Duplicate raw source', result['last_observation_rejected'])

    def test_bootstrap_parents_excluded_even_without_matching_rows(self):
        self.engine.close()
        self.engine = core.PaperStudy(self.target / 'bootstrap.sqlite', self.policy)
        self.engine.observe_runtime_identity(identity())
        self.engine.observe_candidates(packet('bootstrap-parent', 0.2, bootstrap=True, parents=['parent-1']))
        self.choose()
        self.assertEqual(self.book()['choices'], {})
        self.assertEqual(self.engine._load()['first_seen']['isolated-doge:1']['last_ineligible_reason'], 'BOOTSTRAP_EXCLUDED')

    def test_no_source_processing_before_registry(self):
        self.engine.close()
        self.engine = core.PaperStudy(self.target / 'registry.sqlite', self.policy)
        result = self.engine.observe_candidates(packet('bootstrap-empty', 0.2, bootstrap=True))
        self.assertFalse(result['bootstrap_complete'])
        self.assertIn('runtime registry required', result['last_observation_rejected'])

    def test_input_receipt_idempotence_and_conflicting_reuse(self):
        value = packet('same-receipt', 1820, [row()])
        self.engine.observe_candidates(value)
        self.assertTrue(self.engine.observe_candidates(value)['idempotent_duplicate'])
        value['response_received_utc'] = stamp(1821)
        with self.assertRaises(ValueError):
            self.engine.observe_candidates(value)

    def test_declared_entry_is_next_full_minute(self):
        self.choose(received=1820)
        choice = self.book()['choices']['parent-1']
        self.assertEqual(choice['planned_entry_utc'], stamp(1860))
        self.assertEqual(choice['entry_expiry_utc'], stamp(1950))

    def test_quote_requested_before_planned_entry_cannot_open(self):
        self.choose()
        self.quotes(1860.1, started=1859.9)
        self.assertIsNone(self.book()['active'])

    def test_short_target_and_all_in_cost(self):
        self.choose()
        self.quotes(1860.1, started=1860)
        self.quotes(1861, 99.5, started=1860.9)
        position = self.book()['closed'][0]
        self.assertEqual(position['status'], 'OBSERVED_TARGET')
        self.assertEqual(position['net_return_pct'], '0.400')

    def test_short_stop(self):
        self.choose()
        self.quotes(1860.1, started=1860)
        self.quotes(1861, 100.5, started=1860.9)
        self.assertEqual(self.book()['closed'][0]['status'], 'OBSERVED_STOP')

    def test_quote_gap_uncertainty_is_permanent_and_retained(self):
        self.choose()
        self.quotes(1860.1, started=1860)
        self.quotes(1891, 100, started=1890.9)
        self.quotes(1892, 99.5, started=1891.9)
        closed = self.book()['closed'][0]
        self.assertTrue(closed['path_uncertain'])
        self.assertIn('QUOTE_GAP', closed['uncertainty_reasons'])
        self.assertEqual(self.book()['completed_all']['n'], 1)
        self.assertEqual(self.book()['completed_without_uncertainty_flag']['n'], 0)

    def test_source_history_uncertainty_kept_separate_from_quote_path(self):
        self.choose(pending_prior_count=1, source_history_uncertain=True, quality_refs=['pending:7'])
        self.quotes(1860.1, started=1860)
        active = self.book()['active']
        self.assertTrue(active['source_history_uncertain'])
        self.assertEqual(active['pending_prior_count'], 1)
        self.assertFalse(active['path_uncertain'])
        self.assertTrue(active['source_closure_sha256'])
        self.quotes(1861, 99.5, started=1860.9)
        self.assertEqual(self.book()['completed_all']['n'], 1)
        self.assertEqual(self.book()['completed_source_history_uncertain']['n'], 1)
        self.assertEqual(self.book()['completed_without_source_or_quote_uncertainty']['n'], 0)

    def test_actual_resume_permanently_marks_open_path_uncertain(self):
        self.choose()
        self.quotes(1860.1, started=1860)
        class RestartClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return BASE + timedelta(seconds=1862)
        with patch.object(core, 'datetime', RestartClock):
            self.engine.resume_observer()
        self.assertTrue(self.book()['active']['path_uncertain'])
        self.assertIn('PROCESS_RESTART', self.book()['active']['uncertainty_reasons'])
        self.quotes(1863, 99.5, started=1862.9)
        self.assertIn('PROCESS_RESTART', self.book()['closed'][0]['uncertainty_reasons'])

    def test_source_request_must_not_start_before_activation(self):
        value = packet('pre-activation-request', 1820, [row()])
        value['request_started_utc'] = stamp(-1)
        result = self.engine.observe_candidates(value)
        self.assertIn('chronology', result['last_observation_rejected'])
        self.assertEqual(result['first_eligible_seen_count'], 0)

    def test_capacity_at_planned_time_has_no_fallback(self):
        self.choose(event_id=1, parent='p1', received=1820)
        self.choose(event_id=2, parent='p2', received=1850)
        self.quotes(1860.1, started=1860)
        self.assertEqual(self.book()['choices']['p2']['status'], 'BLOCKED_CAPACITY')
        self.quotes(1861, 99.5, started=1860.9)
        self.assertIsNone(self.book()['active'])
        self.assertEqual(self.book()['choices']['p2']['status'], 'BLOCKED_CAPACITY')

    def test_expired_quote_does_not_open_or_choose_alternative(self):
        self.choose()
        self.quotes(1950.1, started=1950)
        self.assertIsNone(self.book()['active'])
        self.assertEqual(self.book()['choices']['parent-1']['status'], 'EXPIRED_NO_ENTRY_QUOTE')

    def test_other_asset_quote_does_not_open(self):
        self.choose()
        self.quotes(1860.1, started=1860, symbol='ETH')
        self.assertIsNone(self.book()['active'])

    def test_quote_straddling_deadline_waits_for_later_request(self):
        self.choose()
        self.quotes(1860.1, started=1860)
        deadline = 1860.1 + 720 * 60
        self.quotes(deadline + 0.1, 99.5, started=deadline - 0.1)
        self.assertIsNotNone(self.book()['active'])
        self.quotes(deadline + 1, 99.5, started=deadline + 0.9)
        self.assertEqual(self.book()['closed'][0]['status'], 'OBSERVED_TIMEOUT')

    def test_existing_policy_identity_cannot_change_on_reopen(self):
        altered = deepcopy(self.policy)
        altered['poll_seconds'] = 11
        with self.assertRaises(ValueError):
            core.PaperStudy(self.path, altered)

    def test_policy_rejects_ethereum_and_wrong_epoch(self):
        for field, value in (('books', ['DOGE_only', 'ETH_only']), ('study_start_utc', '2026-10-20T00:00:00Z'), ('protocol_sha256', '0' * 64)):
            altered = deepcopy(self.policy)
            altered[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                core.PaperStudy(self.target / (field + '.sqlite'), altered)

    def test_append_only_journal_cannot_update_or_delete(self):
        with self.assertRaises(sqlite3.DatabaseError):
            self.engine.db.execute("DELETE FROM journal")
        with self.assertRaises(sqlite3.DatabaseError):
            self.engine.db.execute("UPDATE journal SET kind='forged'")

    def test_recovery_placeholder_preserves_closure_idempotence(self):
        same = {'events': [], 'parents': [], 'proof_objects': {'x': 1}}
        self.observe([], 1810, 'before-recovery', closure=same)
        saved = deepcopy(self.engine._load())
        self.engine.close()
        self.engine = core.PaperStudy(self.target / 'restored.sqlite', self.policy, resume=False)
        self.engine._save(saved)
        h = sha(same)
        self.engine.db.execute('INSERT INTO journal(kind,utc,input_key,payload_json,payload_sha256) VALUES(?,?,?,?,?)',
            ('RESTORED_INPUT_IDENTITY_METADATA', stamp(1810), 'SOURCE_CLOSURE:' + h, '{}', h))
        self.observe([], 1820, 'after-recovery', closure=same)
        self.assertEqual(self.engine.db.execute("SELECT count(*) FROM journal WHERE kind='SOURCE_CLOSURE'").fetchone()[0], 0)
        self.assertEqual(self.engine.summary()['last_response_utc'], stamp(1820))

    def test_receipt_order_cannot_reverse(self):
        self.observe([], received=1820)
        result = self.observe([], received=1810)
        self.assertIn('Response time reversal', result['last_observation_rejected'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
