"""Match-row compatibility and retained payload sharing through real ingestion.

The database is synthetic; feature extraction, matching, causal sequence loading
and feature/question screens execute their production implementations. Object
identity proves removal of retained duplicate strings, not an RSS reduction.
"""
from collections import Counter
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

import research_formula_ordered_store as store
from research_formula_sequence_history_selftest import Fixture, NOW, event


def encoded(value):
    """Pre-change persisted JSON contract, independent of the patched serializer."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=False, default=str, allow_nan=False)


class IngestFixture(Fixture):
    def __init__(self, events, selected_ids, *, known_ids=(), recent_ids=()):
        super().__init__(events)
        self.selected_ids = selected_ids
        self.recent_ids = recent_ids
        self.known = [{'event_id': eid,
                       'input_sha256': store.question_store.digest(self.events[eid])}
                      for eid in known_ids]
        self.persisted = []
        self.checkpoints = []
        self.source_bounds = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self, *, name=None):
        return super().cursor(name=name) if name is not None else self

    def execute(self, sql, args=()):
        normalized = ' '.join(sql.split())
        if normalized.startswith('INSERT INTO research_ordered_formula_worker_state'):
            assert args == (store.WORKER_KEY,)
            self.rows = []
        elif normalized.startswith('SELECT last_event_id'):
            assert args == (store.WORKER_KEY,)
            self.rows = [{'last_event_id': 100}]
        elif 'SELECT event_id FROM research_events WHERE event_id>%s' in normalized:
            self.source_bounds.append(args)
            self.rows = [deepcopy(self.events[eid]) for eid in self.selected_ids]
        elif normalized.startswith('UPDATE research_ordered_formula_worker_state'):
            self.checkpoints.append(args)
            self.rows = []
        elif normalized.startswith("SELECT to_regclass('research_past_price_features')"):
            self.rows = [{'available': False}]
        elif normalized.startswith('SELECT event_id,input_sha256 FROM research_ordered_feature_screens'):
            self.rows = self.known
        else:
            return super().execute(sql, args)
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def executemany(self, sql, rows):
        # Retain the exact tuples passed to the driver: copying/encoding the
        # payload here would hide whether ingestion retained duplicate strings.
        self.persisted.append((' '.join(sql.split()), list(rows)))

    def records(self, table):
        return [row for sql, rows in self.persisted
                if sql.startswith('INSERT INTO ' + table + '(') for row in rows]


def captured_features(current, population):
    prior = [(old, store.evaluator.extract_event_features(old)
              | store.questions.extended_features(old)) for old in population]
    base = (store.evaluator.extract_event_features(current)
            | store.questions.extended_features(current)
            | (current.get('causal_past_features') or {}))
    return base | store.questions.sequence_features(current, base, prior)


def legacy_rows(events, catalog, features):
    """The former per-candidate serialization, for full persisted-tuple parity."""
    rows = []
    for current in events:
        for candidate in catalog:
            if store.evaluator.matches(candidate, features[current['event_id']], current['direction']):
                direction = current['direction']
                if candidate.get('research_orientation') == 'INVERSE':
                    direction = {'LONG': 'SHORT', 'SHORT': 'LONG'}[direction]
                rows.append((candidate['formula_id'], current['event_id'], current['symbol'],
                             direction, current['alert_time_utc'],
                             (current.get('engine_snapshot') or {}).get('sheet_snapshot_id')
                             or current['event_fingerprint'], current['current_price'],
                             encoded(features[current['event_id']])))
    return rows


class MatchPayloadTests(unittest.TestCase):
    def setUp(self):
        base = store.evaluator.candidate_catalog()
        self.catalog = base + store.questions.inverse_candidates(base)
        self.assertEqual(len(base), 7)
        self.assertEqual(len(self.catalog), 14)

    def ingest(self, fixture):
        feature_calls = []
        original = store.canonical

        def counted(value):
            # Catalog/scope fingerprints are different canonical call sites.
            # Question screens intentionally have their own serializer.
            if isinstance(value, dict) and 'sequence.capture_status' in value:
                feature_calls.append(value)
            return original(value)

        with patch.object(store, 'canonical', side_effect=counted), \
             patch.object(store, 'recent_unscreened_event_ids', return_value=list(fixture.recent_ids)), \
             patch.object(store, 'discover_scope_cells', return_value={'scope_cells_created': 0}) as scopes:
            result = store.ingest_matches(fixture, self.catalog, now=NOW, event_limit=32)
        return result, feature_calls, scopes.call_args.args[2]

    def test_complete_rows_screens_and_cursor_match_legacy_with_shared_payloads(self):
        first = event(101, NOW-timedelta(minutes=5), symbol='BTC', direction='LONG')
        second = event(102, NOW-timedelta(minutes=4), symbol='ETH', direction='SHORT')
        unmatched = event(103, NOW-timedelta(minutes=3), symbol='SOL')
        missing = event(104, NOW-timedelta(minutes=2), symbol='DOGE', direction='SHORT')
        unchanged = event(105, NOW-timedelta(minutes=1), symbol='XRP')
        selected = [first, second, unmatched, missing, unchanged]
        for current in selected:
            current['event_fingerprint'] = 'fingerprint-' + str(current['event_id'])
            current['strategy_version'] = 'גרסה-' + str(current['event_id'])
        first['engine_snapshot']['sheet_snapshot_id'] = 'sheet-101'
        second['engine_snapshot']['sheet_snapshot_id'] = ''  # fingerprint fallback
        for module in first['engine_snapshot']['market_evidence']['modules'].values():
            module['score'] = 70
        for name, module in second['engine_snapshot']['market_evidence']['modules'].items():
            module['score'] = 20 if name == 'futures_flow' else 80
        for module in unmatched['engine_snapshot']['market_evidence']['modules'].values():
            module['score'] = 10
        missing['engine_snapshot']['market_evidence']['modules'] = {}
        prior = event(11, NOW-timedelta(minutes=20), symbol='BTC')
        population = [prior, *selected]
        before = deepcopy(population)
        fixture = IngestFixture(population, [101, 102, 103, 104, 105],
                                known_ids=[105], recent_ids=[102, 105])
        result, calls, observed_cells = self.ingest(fixture)
        changed = selected[:4]
        features = {current['event_id']: captured_features(current, population) for current in changed}
        expected = legacy_rows(changed, self.catalog, features)
        actual = fixture.records('research_ordered_formula_matches')
        self.assertEqual(actual, expected)
        self.assertEqual([row[-1].encode('utf-8') for row in actual],
                         [row[-1].encode('utf-8') for row in expected])
        # Explicit expected identities avoid relying solely on a duplicate loop.
        second_keys = {'PRICE_OI_TOTAL_65', 'SPOT_CVD_TOTAL_65', 'PRICE_OI_SPOT_CVD_TOTAL_65'}
        expected_ids = {(candidate['formula_id'], eid,
                         ('SHORT' if eid == 101 else 'LONG')
                         if candidate.get('research_orientation') == 'INVERSE'
                         else ('LONG' if eid == 101 else 'SHORT'))
                        for eid in (101, 102) for candidate in self.catalog
                        if eid == 101 or candidate.get('base_candidate_key', candidate['formula_id']) in second_keys}
        self.assertEqual({(row[0], row[1], row[3]) for row in actual}, expected_ids)
        self.assertEqual(Counter(row[1] for row in actual), {101: 14, 102: 6})
        by_event = {eid: [row[-1] for row in actual if row[1] == eid] for eid in (101, 102)}
        for payloads in by_event.values():
            self.assertTrue(all(type(payload) is str and payload is payloads[0] for payload in payloads))
        self.assertIsNot(by_event[101][0], by_event[102][0])
        self.assertEqual([value['event.symbol'] for value in calls], ['BTC', 'ETH'])
        self.assertIn('גרסה-101', by_event[101][0])
        self.assertEqual(population, before)
        self.assertEqual(observed_cells, {(row[0], row[2], row[3]) for row in expected})
        self.assertEqual(fixture.checkpoints, [(105, False, store.WORKER_KEY)])
        self.assertEqual(fixture.source_bounds, [(100, store.SOURCE_START_UTC, NOW, 32)])
        self.assertEqual(fixture.records('research_ordered_formula_event_checks'),
                         [(101,), (102,), (103,), (104,), (105,)])
        screens = fixture.records('research_ordered_feature_screens')
        self.assertEqual(screens, [(current['event_id'], store.questions.VERSION,
                         store.question_store.digest(current), encoded(features[current['event_id']]), NOW)
                         for current in changed])
        self.assertEqual(features[101]['sequence.30m.prior_distinct_scans'], 1)
        self.assertEqual(features[102]['sequence.30m.prior_distinct_scans'], 0)
        question_runs = fixture.records('research_ordered_question_runs')
        self.assertEqual(len(question_runs), len(store.questions.question_map()))
        for _, _, payload, checked_at in question_runs:
            screen = json.loads(payload)
            self.assertEqual(screen['checked_event_ids'], [101, 102, 103, 104])
            self.assertEqual(screen['inverse_requested_source_event_ids'], [101, 102])
            self.assertEqual(checked_at, NOW)
            for key, count in screen['matched_candidate_counts'].items():
                self.assertEqual(count, sum(row[0] == key for row in expected))
        self.assertEqual(result, {'events_checked': 4, 'fresh_unscreened_events_selected': 2,
            'screen_feature_version': store.questions.VERSION, 'events_unchanged_skipped': 1,
            'missing_total_score_features': 1, 'matches_observed': 20,
            'symbols': ['BTC', 'DOGE', 'ETH', 'SOL'], 'cursor': 105,
            'source_start_utc': store.SOURCE_START_UTC.isoformat(),
            'inverse_source_event_ids': [101, 102], 'question_screens': len(question_runs),
            'feature_events_changed': 4, 'sequence_source_rows': 1, 'sequence_source_pages': 1,
            'sequence_projected_rows': 1, 'sequence_projection_batches': 1, 'scope_cells_created': 0})

    def test_no_match_needs_no_match_payload_serialization_but_keeps_screen_and_checkpoint(self):
        current = event(101, NOW-timedelta(minutes=1), symbol='SOL')
        current['event_fingerprint'] = 'unmatched-101'
        for module in current['engine_snapshot']['market_evidence']['modules'].values():
            module['score'] = 10
        fixture = IngestFixture([current], [101])
        result, calls, observed_cells = self.ingest(fixture)
        self.assertEqual(calls, [])
        self.assertEqual(fixture.records('research_ordered_formula_matches'), [])
        self.assertEqual(observed_cells, set())
        self.assertEqual(result['matches_observed'], 0)
        self.assertEqual(result['inverse_source_event_ids'], [])
        self.assertEqual(result['events_checked'], 1)
        self.assertEqual(result['feature_events_changed'], 1)
        self.assertEqual(fixture.checkpoints, [(101, False, store.WORKER_KEY)])
        self.assertEqual(fixture.records('research_ordered_formula_event_checks'), [(101,)])
        screen = fixture.records('research_ordered_feature_screens')
        self.assertEqual(screen, [(101, store.questions.VERSION,
            store.question_store.digest(current), encoded(captured_features(current, [current])), NOW)])
        self.assertTrue(fixture.records('research_ordered_question_runs'))


if __name__ == '__main__':
    unittest.main()
