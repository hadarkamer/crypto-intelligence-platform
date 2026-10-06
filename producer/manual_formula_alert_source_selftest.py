"""Offline tests of bounded, causal source selection; no database or Telegram."""
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import manual_formula_alert_source as source

NOW = datetime(2026, 9, 14, 11, 0, tzinfo=timezone.utc)


def event(identifier=10, *, ago=1, score=70, direction='LONG', scan='current', symbol='BTC'):
    return {'event_id': identifier, 'alert_time_utc': NOW-timedelta(minutes=ago),
            'symbol': symbol, 'direction': direction, 'event_kind': 'ALERT',
            'event_type': 'PRICE_OI_ALERT', 'delivery_status': 'DELIVERED',
            'current_price': 100, 'engine_snapshot': {'watch_scan_id': scan,
                'market_evidence': {'modules': {'positioning': {'score': score,
                                                               'direction': 'LONG'}}}}}


class Rows:
    def __init__(self, rows): self.rows = rows
    def fetchall(self): return deepcopy(self.rows)


class Connection:
    def __init__(self, current, history=(), error=None):
        self.current, self.history, self.error = current, history, error
        self.calls = []
    def transaction(self): return nullcontext()
    def execute(self, sql, params):
        self.calls.append((sql, params))
        if len(self.calls) == 1: return Rows(self.current)
        if self.error: raise self.error
        return Rows(self.history)


class SourceTests(unittest.TestCase):
    def planned(self, *, stamp=None, fingerprint='a'):
        row = event()
        row.update(event_id='watch:' + fingerprint * 64, event_fingerprint=fingerprint * 64,
                   delivery_status='NOT_ATTEMPTED', capture_stage='WATCH_PLANNED_ALERT')
        if stamp is not None: row['alert_time_utc'] = stamp
        return row

    def test_planned_batch_retains_strict_causal_same_scan_sequence(self):
        first = self.planned(stamp=NOW-timedelta(minutes=1), fingerprint='a')
        second = self.planned(stamp=NOW-timedelta(seconds=59), fingerprint='b')
        tied = self.planned(stamp=first['alert_time_utc'], fingerprint='c')
        pairs, stats = source.prepare_watch_pairs(Connection([]), [second, tied, first], NOW)
        ordinals = {e['event_fingerprint']: f['sequence.30m.price_oi.entry_ordinal'] for e, f in pairs}
        self.assertEqual(ordinals, {'a'*64: 1, 'b'*64: 2, 'c'*64: 1})
        self.assertEqual(stats['source_lane'], 'WATCH_PLANNED')
        self.assertTrue(all(e['delivery_status'] == 'NOT_ATTEMPTED' for e, _ in pairs))

    def test_planned_history_failure_preserves_other_formulas(self):
        row = self.planned()
        row['engine_snapshot']['market_evidence']['modules']['spot_flow'] = {'score': 70, 'direction': 'LONG'}
        with patch.object(source, '_sequence_history', side_effect=TimeoutError) as lookup:
            pairs, stats = source.prepare_watch_pairs(Connection([]), [row], NOW)
        import manual_formula_alert as rules
        payloads = rules.evaluate_event(*pairs[0], NOW, planned=True)
        self.assertEqual([r['rule_id'] for r in payloads], ['PRICE_OI_SPOT65'])
        self.assertEqual(stats['sequence_error_type'], 'TimeoutError')
        self.assertEqual(lookup.call_count, 2)
        self.assertEqual(stats['sequence_lookup_attempts'], 2)
        self.assertFalse(stats['sequence_retry_recovered'])

    def test_planned_transient_sequence_failure_retries_before_transport(self):
        row = self.planned()
        prior = event(1, ago=20, scan='prior')
        history = [(prior, source.totals.extract_event_features(prior))]
        with patch.object(source, '_sequence_history', side_effect=[TimeoutError, (history, False)]) as lookup:
            pairs, stats = source.prepare_watch_pairs(Connection([]), [row], NOW)
        self.assertEqual(lookup.call_count, 2)
        self.assertEqual(stats['sequence_lookup_attempts'], 2)
        self.assertTrue(stats['sequence_retry_recovered'])
        self.assertIsNone(stats['sequence_error_type'])
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 2)
        self.assertEqual(pairs[0][1]['sequence.capture_status'], 'READY')

    def test_planned_sequence_overflow_stays_explicit_without_repeating_full_query(self):
        with patch.object(source, '_sequence_history', return_value=([], True)) as lookup:
            pairs, stats = source.prepare_watch_pairs(Connection([]), [self.planned()], NOW)
        self.assertEqual(lookup.call_count, 1)
        self.assertEqual(pairs[0][1]['sequence.capture_status'], 'SOURCE_OVERFLOW')
        self.assertFalse(stats['sequence_retry_recovered'])

    def test_c0964_source_projection_uses_captured_le_and_aligned_spot(self):
        import manual_formula_alert as rules
        for direction, side, score in (('LONG', 'UPPER', 25), ('SHORT', 'LOWER', -25)):
            row = event(score=24, direction=direction)
            row.update(event_type='MAGNET_ALERT', event_fingerprint='a'*64)
            row['engine_snapshot']['magnet'] = {'side': side, 'liquidity_edge_pct': 30}
            row['engine_snapshot']['market_evidence']['modules']['spot_flow'] = {'score': score, 'direction': direction}
            pairs, _ = self.load(Connection([row]))
            self.assertEqual([r['rule_id'] for r in rules.evaluate_event(*pairs[0], NOW)], ['C0964'])

    def test_magnet_status_survives_native_projection_and_planned_capture(self):
        class ProjectedConnection(Connection):
            def execute(self, sql, params):
                result = super().execute(sql, params)
                projected = result.fetchall()
                for row in projected:
                    row['engine_snapshot'] = {
                        key: value for key, value in row['engine_snapshot'].items()
                        if key in params[6]
                    }
                return Rows(projected)

        for status in ('OBSERVATION', 'CONFIRMED', None):
            with self.subTest(status=status):
                row = event(score=24, direction='SHORT', symbol='DOGE')
                row.update(event_type='MAGNET_ALERT', event_fingerprint='a'*64)
                row['engine_snapshot']['magnet'] = {'side': 'LOWER', 'magnet_quality': 55}
                if status is not None:
                    row['engine_snapshot']['magnet_confirmation'] = {'status': status}
                pairs, _ = self.load(ProjectedConnection([row]))
                native_features = pairs[0][1]
                self.assertTrue(native_features['event.direction_mapping_valid'])
                self.assertEqual(native_features['event.analysis_direction'], 'SHORT')
                self.assertEqual(native_features.get('captured.magnet.confirmation_status'), status)
                if status is None:
                    # A low quality alone cannot invent a missing captured label.
                    self.assertNotIn('captured.magnet.confirmation_status', native_features)
                planned = deepcopy(row)
                planned.update(event_id='watch:' + 'a'*64, delivery_status='NOT_ATTEMPTED',
                               capture_stage='WATCH_PLANNED_ALERT')
                planned_pairs, _ = source.prepare_watch_pairs(Connection([]), [planned], NOW)
                self.assertEqual(planned_pairs[0][1], native_features)

    def load(self, conn, **kwargs):
        return source.load_batch(conn, activated_at=NOW-timedelta(hours=1), now=NOW,
                                 processed_ids=[], **kwargs)

    def test_second_entry_counts_scans_not_sibling_events(self):
        conn = Connection([event()], [event(1, ago=20, scan='prior'), event(2, ago=19, scan='prior')])
        pairs, stats = self.load(conn)
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 2)
        self.assertEqual(stats['sequence_source_rows'], 2)
        self.assertEqual(len(conn.calls), 2)

    def test_two_distinct_prior_scans_is_third_not_second(self):
        pairs, _ = self.load(Connection([event()], [event(1, ago=20, scan='a'), event(2, ago=19, scan='b')]))
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 3)

    def test_audited_ordinal_includes_earlier_event_from_current_scan(self):
        # Canonical v3 counts prior scan IDs and then adds the current entry;
        # it does not subtract the current watch ID from its prior scan set.
        pairs, _ = self.load(Connection([event(scan='same')], [event(1, ago=2, scan='same')]))
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 2)

    def test_equal_future_wrong_direction_symbol_and_old_excluded(self):
        old = [event(1, ago=1), event(2, ago=0), event(3, ago=20, direction='SHORT'),
               event(4, ago=20, symbol='ETH'), event(5, ago=32), event(6, ago=20, scan=None)]
        pairs, _ = self.load(Connection([event()], old))
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 1)

    def test_exact_thirty_minute_boundary_is_included(self):
        pairs, _ = self.load(Connection([event()], [event(1, ago=31, scan='prior')]))
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 2)

    def test_history_is_not_limited_to_activation(self):
        conn = Connection([event()], [event(1, ago=20, scan='prior')])
        pairs, _ = source.load_batch(conn, activated_at=NOW-timedelta(minutes=2), now=NOW, processed_ids=[])
        self.assertEqual(pairs[0][1]['sequence.30m.price_oi.entry_ordinal'], 2)
        self.assertEqual(conn.calls[0][1][0], NOW-timedelta(minutes=2))

    def test_no_qualifying_price_oi_avoids_history_query(self):
        conn = Connection([event(score=64)])
        pairs, _ = self.load(conn)
        self.assertEqual(len(conn.calls), 1)
        self.assertNotIn('sequence.30m.price_oi.entry_ordinal', pairs[0][1])

    def test_overflow_disables_only_sequence_preserves_totals(self):
        conn = Connection([event()], [event(1, ago=20), event(2, ago=19)])
        with patch.object(source, 'MAX_SEQUENCE_ROWS', 1):
            pairs, stats = self.load(conn)
        self.assertEqual(stats['sequence_overflow_events'], 1)
        self.assertNotIn('sequence.30m.price_oi.entry_ordinal', pairs[0][1])
        self.assertEqual(pairs[0][1]['price_oi.aligned_score'], 70)

    def test_history_error_fails_sequence_closed_only(self):
        pairs, stats = self.load(Connection([event()], error=TimeoutError('not logged')))
        self.assertEqual(stats['sequence_error_type'], 'TimeoutError')
        self.assertEqual(pairs[0][1]['sequence.capture_status'], 'SOURCE_UNAVAILABLE')
        self.assertEqual(pairs[0][1]['price_oi.aligned_score'], 70)

    def test_receipts_not_serial_cursor_and_compact_projection(self):
        conn = Connection([event(4, score=64)])
        source.load_batch(conn, activated_at=NOW-timedelta(hours=1), now=NOW, processed_ids=[99, 2])
        sql, params = conn.calls[0]
        self.assertEqual(params[2], [2, 99])
        self.assertIn('NOT (e.event_id=ANY', sql)
        self.assertNotIn('event_id>', sql)
        self.assertEqual(params[0], NOW-timedelta(minutes=10))
        self.assertIn('time_families,long,quality', sql)
        self.assertIn('inverse_analysis', params[6])
        self.assertIn('experimental_price_references', params[6])
        self.assertIn('capture_stage', sql)

    def test_captured_reference_reaches_native_and_planned_detector_unchanged(self):
        import manual_formula_alert as detector
        reference = {'status': 'READY', 'component': 'PRICE_OI', 'symbol': 'BTC',
                     'price': '87.5', 'anchor_time_utc': '2026-09-14T10:58:30Z',
                     'price_time_utc': '2026-09-14T10:58:30Z',
                     'source': 'BINANCE_SPOT_TRADE_1M', 'precision': 'EXACT_CAPTURE'}
        current = event()
        current['event_fingerprint'] = 'a' * 64
        current['engine_snapshot']['experimental_price_references'] = {'PRICE_OI': reference}
        prior = event(1, ago=20, scan='prior')
        pairs, _ = self.load(Connection([current], [prior]))
        payloads = detector.evaluate_event(*pairs[0], NOW)
        self.assertEqual([p['rule_id'] for p in payloads], ['PRICE_OI_ENTRY2'])
        self.assertEqual(payloads[0]['price_reference']['price'], '87.5')
        self.assertEqual(pairs[0][0]['engine_snapshot']['experimental_price_references'],
                         {'PRICE_OI': reference})
        planned = self.planned()
        planned['engine_snapshot']['experimental_price_references'] = {'PRICE_OI': reference}
        history = [(prior, source.totals.extract_event_features(prior))]
        with patch.object(source, '_sequence_history', return_value=(history, False)):
            pairs, _ = source.prepare_watch_pairs(Connection([]), [planned], NOW)
        payloads = detector.evaluate_event(*pairs[0], NOW, planned=True)
        self.assertEqual([p['rule_id'] for p in payloads], ['PRICE_OI_ENTRY2'])
        self.assertEqual(payloads[0]['price_reference']['price'], '87.5')

    def test_invalid_projection_never_evaluated(self):
        for current in ([event(ago=11)], [event(ago=-1)], [event(10), event(10)]):
            with self.subTest(current=current), self.assertRaises(RuntimeError):
                self.load(Connection(current))

    def test_naive_time_and_unbounded_receipts_rejected(self):
        with self.assertRaises(ValueError):
            source.load_batch(Connection([]), activated_at=NOW.replace(tzinfo=None), now=NOW, processed_ids=[])
        with patch.object(source, 'MAX_PROCESSED_IDS', 1), self.assertRaises(ValueError):
            source.load_batch(Connection([]), activated_at=NOW, now=NOW, processed_ids=[1, 2])

    def test_c1274_never_uses_a_native_magnet_carrier(self):
        import manual_formula_alert as detector
        current = event(score=24)
        current.update(event_type='MAGNET_ALERT', event_fingerprint='a'*64)
        current['engine_snapshot']['magnet'] = {'side': 'UPPER'}
        current['engine_snapshot']['market_evidence']['modules']['futures_flow'] = {
            'available': True, 'score': -25, 'direction': 'BEARISH',
            'time_families': {'long': {'direction': 'BEARISH', 'quality': 0.65}}}
        pairs, _ = self.load(Connection([current]))
        payloads = detector.evaluate_event(*pairs[0], NOW)
        self.assertNotIn('C1274', [p['rule_id'] for p in payloads])

    def test_maxpain_original_direction_inverted_exactly_once(self):
        import manual_formula_alert as detector
        current = event(score=24, direction='SHORT')
        current.update(event_type='MAX_PAIN_ALERT', event_fingerprint='a'*64,
                       source_side='LONG', target_price=99)
        current['engine_snapshot'].update(alert_side='LONG', consensus_hits=7, consensus_total=7)
        pairs, _ = self.load(Connection([current]))
        payloads = detector.evaluate_event(*pairs[0], NOW)
        self.assertEqual([(p['rule_id'], p['direction']) for p in payloads], [('CONSENSUS_FULL', 'LONG')])

    def test_retry_lane_exact_ids_max_eight_and_same_time_fence(self):
        conn = Connection([event(4, score=64)])
        pairs, stats = self.load(conn, retry_event_ids=[4, 9])
        self.assertEqual(stats['source_lane'], 'RETRY')
        self.assertEqual(conn.calls[0][1][3:6], ([4, 9], [4, 9], 8))
        self.assertEqual(conn.calls[0][1][0], NOW-timedelta(minutes=10))
        self.assertEqual(pairs[0][0]['event_id'], 4)

    def test_empty_retry_lane_reads_nothing(self):
        conn = Connection([])
        pairs, stats = self.load(conn, retry_event_ids=[])
        self.assertEqual(pairs, [])
        self.assertEqual(conn.calls, [])
        self.assertEqual(stats['source_lane'], 'RETRY')

    def test_retry_lane_rejects_overflow_and_unrequested_ids(self):
        with self.assertRaises(ValueError):
            self.load(Connection([]), retry_event_ids=list(range(1, 10)))
        with self.assertRaises(RuntimeError):
            self.load(Connection([event(10)]), retry_event_ids=[4])


if __name__ == '__main__':
    unittest.main()
