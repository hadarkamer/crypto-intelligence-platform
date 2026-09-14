"""Source-read and intake-binding gates for the explicit read-only probe."""
from copy import deepcopy
from datetime import timedelta
from unittest.mock import patch
import unittest

import research_watch_decision_contract_probe as probe


class Connection:
    def __init__(self, row):
        self.row, self.calls = row, []

    def execute(self, query, args=None):
        self.calls.append((query, args))
        return self

    def fetchone(self):
        return self.row


def source():
    return dict(snapshot_set_id=1308, watch_scan_id='test-cycle', bundle_sha256='s'*64,
        parent_payload_sha256='p'*64, stored_parent_sha256='p'*64,
        source_available_at_utc='2026-09-14T07:06:00+00:00',
        stored_available_at_utc='2026-09-14T07:06:00+00:00',
        usable_from_utc='2026-09-14T07:06:01+00:00',
        scores={'payload_sha256': 's'*64}, decisions={'status': 'COMPLETE'})


class SourceTests(unittest.TestCase):
    def test_one_bound_read_in_read_only_transaction(self):
        row = source()
        conn = Connection(row)
        self.assertIs(probe.read_source(conn, 1308), row)
        self.assertEqual(conn.calls[:2], [('SET TRANSACTION READ ONLY', None),
            ("SET LOCAL statement_timeout='10s'", None)])
        self.assertEqual(conn.calls[2], (probe.SOURCE_SQL, (1308, 1308)))
        self.assertIn("i.intake_status='ACCEPTED'", probe.SOURCE_SQL)
        self.assertIn('LIMIT 1', probe.SOURCE_SQL)
        self.assertNotIn('research_events', probe.SOURCE_SQL)

    def test_latest_read_does_not_skip_missing_or_failed_capture(self):
        conn = Connection(None)
        self.assertIsNone(probe.read_source(conn))
        self.assertEqual(conn.calls[-1][1], (None, None))
        self.assertNotIn('IS NOT NULL', probe.SOURCE_SQL)
        self.assertEqual(probe.evaluate_source(None)['source_reasons'], ['NO_ACCEPTED_SOURCE'])

    def test_bad_source_binding_never_calls_contract(self):
        for key in ('stored_parent_sha256', 'bundle_sha256', 'stored_available_at_utc'):
            with self.subTest(key=key), patch.object(probe.contract, 'evaluate_capture') as evaluate:
                row = source()
                row[key] = 'bad'
                result = probe.evaluate_source(row)
                self.assertEqual(result['source_status'], 'UNKNOWN')
                self.assertIsNone(result['evaluation'])
                self.assertTrue(result['source_reasons'])
                evaluate.assert_not_called()

    def test_exact_frozen_source_and_clock_forwarded_once(self):
        row = source()
        original = deepcopy(row)
        with patch.object(probe.contract, 'evaluate_capture', return_value={'status': 'VALID'}) as evaluate:
            result = probe.evaluate_source(row)
            evaluate.assert_called_once_with(row['decisions'], row['scores'],
                cycle_id=row['watch_scan_id'], available_at_utc=row['stored_available_at_utc'])
        self.assertEqual(row, original)
        self.assertEqual(result['source_status'], 'BOUND')

    def test_invalid_snapshot_identity_opens_no_query(self):
        for invalid in (True, 0, -1, '1308', 1.5):
            with self.subTest(invalid=invalid):
                conn = Connection(None)
                with self.assertRaises(ValueError):
                    probe.read_source(conn, invalid)
                self.assertEqual(conn.calls, [])

    def test_real_frozen_capture_contract_and_compact_summary(self):
        import research_watch_decision_capture as capture
        from research_watch_decision_capture_selftest import inputs, BASE
        args = inputs()
        row = source()
        row.update(watch_scan_id=args['cycle_id'], scores=args['score_bundle'],
            bundle_sha256=args['score_bundle']['payload_sha256'],
            decisions=capture.build_bundle(**args),
            source_available_at_utc=BASE+timedelta(minutes=6),
            stored_available_at_utc=BASE+timedelta(minutes=6))
        report = probe.evaluate_source(row)
        summary = probe.summarize(report)
        self.assertEqual(summary['contract_status'], 'VALID')
        self.assertEqual(summary['catalog_summary']['evaluable'], 38)
        self.assertEqual(len(summary['blocked_definitions']), 4)
        self.assertEqual(len(summary['structurally_unreachable_definitions']), 4)
        self.assertFalse(summary['flags']['cohort_eligible'])
        self.assertEqual(len(summary['coin_population_status']), 8)
        self.assertTrue(summary['populations'])
        row['decisions'] = None
        missing = probe.summarize(probe.evaluate_source(row))
        self.assertEqual(missing['contract_status'], 'UNKNOWN')
        self.assertEqual(missing['populations'], {})


if __name__ == '__main__':
    unittest.main()
