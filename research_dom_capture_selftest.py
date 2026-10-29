"""Offline diagnostics regressions; synthetic tables never enter a study."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import coinglass_dom_reader as reader
from coinglass_dom_reader_selftest import table
import research_dom_capture as capture


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 10, 29, 16, 0, tzinfo=timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


def frame(data=None, timeframe='24h'):
    data = data if data is not None else table()
    with patch.object(reader, 'datetime', FixedDatetime):
        rows = reader._parse_maxpain_table_rows([data], timeframe)
    result = capture.capture_poll([data], timeframe, rows,
        metadata={'poll': 2, 'active_label': timeframe, 'parsed_at_utc': rows[0]['collected_at_utc'] if rows else None},
        parse_number=reader._parse_number, is_symbol=reader._is_symbol_token)
    return rows, result


class CaptureTests(unittest.TestCase):
    def test_absent_and_rejected_coin_are_distinguishable_without_changing_valid_rows(self):
        omitted = table(1)
        rejected = deepcopy(omitted)
        row = deepcopy(rejected['rows'][0])
        row[1]['text'] = '2'
        row[2]['primary'] = 'HYPE'
        row[5]['amount'] = '—'
        rejected['rows'].append(row)
        before = deepcopy(rejected)
        original_rows, absent_capture = frame(omitted)
        observed_rows, rejected_capture = frame(rejected)
        self.assertEqual(original_rows, observed_rows)
        self.assertEqual(rejected, before)
        self.assertEqual(absent_capture['payload']['tracked_symbols']['HYPE']['status'], 'ABSENT')
        self.assertEqual(rejected_capture['payload']['tracked_symbols']['HYPE']['status'], 'REJECTED')
        self.assertEqual(rejected_capture['payload']['row_decisions'][1]['reason_codes'], ['SHORT_AMOUNT_MISSING_OR_NEGATIVE'])
        self.assertEqual(rejected_capture['payload']['selected_table']['rows'][1][5]['amount'], '—')
        self.assertEqual(rejected_capture['payload']['counts']['raw_rows'], 2)
        self.assertTrue(capture.validate_capture(rejected_capture, snapshot=False))

    def test_legacy_valid_row_dictionary_is_exactly_preserved(self):
        rows, diagnostic = frame(table(1))
        self.assertEqual(rows, [{
            'collected_at_utc': '2026-10-29T16:00:00+00:00', 'timeframe': '24h',
            'source': 'dom_body_maxpain', 'rank': 1, 'symbol': 'BTC', 'price': 80586.9,
            'max_short_price': 81558.27, 'max_long_price': 78853.93,
            'short_amount_usd': 26500000.0, 'long_amount_usd': 40700000.0,
            'short_distance_usd': 81558.27 - 80586.9,
            'short_distance_pct': (81558.27 / 80586.9 - 1) * 100,
            'long_distance_usd': 80586.9 - 78853.93,
            'long_distance_pct': (1 - 78853.93 / 80586.9) * 100,
            'raw_cells': [c['text'] for c in table(1)['rows'][0]], 'extra': None}])
        self.assertEqual(diagnostic['status'], 'COMPLETE')

    def test_bad_cell_count_has_explicit_reason_and_unreadable_identity_is_unknown(self):
        data = table(1)
        data['rows'].append([{'text': 'Loading', 'primary': 'Loading', 'amount': ''}])
        rows, diagnostic = frame(data)
        self.assertEqual(len(rows), 1)
        self.assertEqual(diagnostic['payload']['row_decisions'][1]['reason_codes'], ['CELL_COUNT_MISMATCH'])
        self.assertEqual(diagnostic['payload']['tracked_symbols']['HYPE']['status'], 'UNKNOWN_UNATTRIBUTABLE_ROW')

    def test_nonempty_invalid_symbol_cannot_create_absent_claim(self):
        data = table(1)
        data['rows'][0][2]['primary'] = '?'
        _, diagnostic = frame(data)
        self.assertIn('SYMBOL_TOKEN_INVALID', diagnostic['payload']['row_decisions'][0]['reason_codes'])
        self.assertEqual(diagnostic['payload']['tracked_symbols']['HYPE']['status'], 'UNKNOWN_UNATTRIBUTABLE_ROW')

    def test_text_row_and_cell_caps_are_explicit(self):
        data = table(4)
        data['rows'][0][0]['text'] = 'ש' * 3000
        with patch.object(capture, 'MAX_ROWS', 2), patch.object(capture, 'MAX_CELLS', 5):
            _, diagnostic = frame(data)
        self.assertEqual(diagnostic['status'], 'TRUNCATED')
        self.assertTrue({'TEXT_BYTE_LIMIT', 'ROW_LIMIT', 'ROW_CELL_LIMIT', 'HEADER_CELL_LIMIT'} <= set(diagnostic['payload']['truncation']))
        self.assertEqual(diagnostic['payload']['counts']['omitted_raw_rows'], 2)
        self.assertEqual(diagnostic['payload']['counts']['omitted_header_cells'], 4)
        self.assertEqual(diagnostic['payload']['row_decisions'][0]['omitted_cell_count'], 4)
        self.assertGreater(diagnostic['payload']['counts']['processed_text_utf8_bytes_omitted'], 0)
        self.assertEqual(diagnostic['payload']['tracked_symbols']['HYPE']['status'], 'UNKNOWN_TRUNCATED')
        self.assertLessEqual(len(capture.canonical(diagnostic).encode()), capture.MAX_TIMEFRAME_BYTES)

    def test_final_byte_bound_includes_presence_and_attempt_history(self):
        with patch.object(capture, 'MAX_TIMEFRAME_BYTES', 4096):
            _, diagnostic = frame(table(30))
            result = capture.with_attempt_history(diagnostic, [{'attempt': n, 'outcome': 'FAILED'} for n in (1, 2, 3)], attempt=3)
            self.assertEqual(result['status'], 'TRUNCATED')
            self.assertLessEqual(len(capture.canonical(result).encode()), 4096)
            self.assertTrue(capture.validate_capture(result, snapshot=False))

    def test_aggregate_marks_failed_and_not_attempted_frames_explicitly(self):
        _, good = frame(timeframe='24h')
        result = capture.aggregate(list(capture.TIMEFRAMES), {
            '24h': {'dom_capture': good}, '12h': {'verified': False}})
        self.assertEqual(result['status'], 'PARTIAL')
        frames = result['payload']['timeframes']
        self.assertEqual(frames['12h']['payload']['outcome'], 'FAILED')
        self.assertEqual(frames['1m']['payload']['outcome'], 'NOT_ATTEMPTED')
        self.assertEqual(set(frames), set(capture.TIMEFRAMES))
        self.assertTrue(capture.validate_capture(result))

    def test_snapshot_metadata_floor_stays_within_bound_and_visible(self):
        frames = {tf: {'dom_capture': frame(timeframe=tf)[1]} for tf in capture.TIMEFRAMES}
        with patch.object(capture, 'MAX_SNAPSHOT_BYTES', 2048):
            result = capture.aggregate(list(capture.TIMEFRAMES), frames)
            self.assertEqual(result['status'], 'UNAVAILABLE')
            self.assertLessEqual(len(capture.canonical(result).encode()), 2048)
            self.assertIn('original_requested_timeframe_count', result['payload'])
            self.assertEqual(result['payload']['original_requested_timeframe_count'], 7)
            self.assertTrue(capture.validate_capture(result))

    def test_hash_tamper_and_mixed_source_pins_cannot_claim_complete(self):
        _, good = frame()
        tampered = deepcopy(good)
        tampered['payload']['selected_table']['rows'][0][1]['text'] = '99'
        with self.assertRaisesRegex(ValueError, 'PAYLOAD_SHA256'):
            capture.validate_capture(tampered, snapshot=False)
        altered = deepcopy(good)
        altered['payload']['source_code_sha256']['coinglass_dom_reader.py'] = '0' * 64
        altered['payload_sha256'] = capture.digest(altered['payload'])
        aggregate = capture.aggregate(['24h'], {'24h': {'dom_capture': altered}})
        self.assertEqual(aggregate['status'], 'PARTIAL')
        self.assertIn('TIMEFRAME_SOURCE_PIN_MISMATCH', aggregate['payload']['integrity_gaps'])

    def test_recomputed_hash_does_not_hide_false_complete_row_counts(self):
        _, good = frame(table(1))
        good['payload']['counts']['raw_rows'] = 2
        good['payload_sha256'] = capture.digest(good['payload'])
        with self.assertRaisesRegex(ValueError, 'FALSE_COMPLETE_ROW_COUNTS'):
            capture.validate_capture(good, snapshot=False)


class ReadCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_snapshot_has_seven_receipts_and_capture_failure_preserves_operational_rows(self):
        for diagnostic_failure in (False, True):
            with self.subTest(diagnostic_failure=diagnostic_failure):
                context = SimpleNamespace(close=AsyncMock())
                browser = SimpleNamespace(new_context=AsyncMock(return_value=context), close=AsyncMock())
                api = SimpleNamespace(chromium=SimpleNamespace(launch=AsyncMock(return_value=browser)))
                manager = AsyncMock()
                manager.__aenter__.return_value = api
                async def observed(_context, _url, timeframe, **_kwargs):
                    rows, diagnostic = frame(timeframe=timeframe)
                    return {'timeframe': timeframe, 'verified': True, 'rows': rows,
                            'fingerprint': reader._rows_fingerprint(rows), 'debug': {},
                            'dom_capture': diagnostic}
                aggregate = capture.aggregate
                with patch.object(reader, 'async_playwright', return_value=manager), \
                     patch.object(reader, '_retry_timeframe_on_fresh_page', AsyncMock(side_effect=observed)) as reads, \
                     patch.object(capture, 'aggregate', side_effect=RuntimeError('synthetic fault') if diagnostic_failure else aggregate):
                    snapshot = await reader.collect_coinglass_dom_snapshot()
                self.assertTrue(snapshot['ok'])
                self.assertEqual(snapshot['row_count'], 210)
                self.assertEqual(len(snapshot['rows']), 210)
                self.assertEqual(reads.await_count, 7)
                self.assertEqual(snapshot['missing_timeframes'], [])
                self.assertEqual(snapshot['dom_capture']['status'], 'UNAVAILABLE' if diagnostic_failure else 'COMPLETE')
                self.assertEqual(set(snapshot['dom_capture']['payload']['timeframes']), set(reader.DEFAULT_TIMEFRAMES))
                self.assertTrue(capture.validate_capture(snapshot['dom_capture']))
                context.close.assert_awaited_once()
                browser.close.assert_awaited_once()

    async def test_capture_uses_accepted_poll_without_additional_dom_read(self):
        baseline = table()
        first, second = deepcopy(baseline), deepcopy(baseline)
        for data, amount in ((first, '—'), (second, '')):
            rejected = deepcopy(data['rows'][0])
            rejected[1]['text'] = '31'
            rejected[2]['primary'] = 'HYPE'
            rejected[5]['amount'] = amount
            data['rows'].append(rejected)
        page = SimpleNamespace(wait_for_timeout=AsyncMock())
        with patch.object(reader, '_extract_tables', AsyncMock(side_effect=[[baseline], [first], [second]])) as extract, \
             patch.object(reader, '_click_timeframe_verified', AsyncMock(return_value=True)), \
             patch.object(reader, '_active_timeframe_label', AsyncMock(return_value='24h')):
            result = await reader.read_timeframe(page, '24h')
        self.assertTrue(result['verified'])
        self.assertEqual(extract.await_count, 3)
        self.assertEqual(page.wait_for_timeout.await_count, 2)
        diagnostic = result['dom_capture']
        self.assertEqual(diagnostic['payload']['metadata']['poll'], 2)
        self.assertEqual(diagnostic['payload']['selected_table']['rows'][-1][5]['amount'], '')
        self.assertEqual(diagnostic['payload']['tracked_symbols']['HYPE']['status'], 'REJECTED')
        self.assertEqual(len(result['rows']), 30)

    async def test_diagnostic_failure_preserves_old_success_and_poll_counts(self):
        page = SimpleNamespace(wait_for_timeout=AsyncMock())
        with patch.object(reader, '_extract_tables', AsyncMock(return_value=[table()])) as extract, \
             patch.object(reader, '_click_timeframe_verified', AsyncMock(return_value=True)), \
             patch.object(reader, '_active_timeframe_label', AsyncMock(return_value='24h')), \
             patch.object(capture, 'capture_poll', side_effect=RuntimeError('synthetic diagnostic fault')):
            result = await reader.read_timeframe(page, '24h')
        self.assertTrue(result['verified'])
        self.assertEqual(len(result['rows']), 30)
        self.assertEqual(result['dom_capture']['status'], 'UNAVAILABLE')
        self.assertEqual(extract.await_count, 3)
        self.assertEqual(page.wait_for_timeout.await_count, 2)

    async def test_retry_history_is_bounded_and_does_not_add_retries_or_sleeps(self):
        rows, good = frame()
        failed = {'verified': False, 'rows': [], 'retryable': True, 'debug': {}}
        success = {'verified': True, 'rows': rows, 'fingerprint': 'fixture', 'debug': {'poll': 2}, 'dom_capture': good}
        pages = [SimpleNamespace(close=AsyncMock()) for _ in range(2)]
        with patch.object(reader, '_new_ready_page', AsyncMock(side_effect=pages)) as opened, \
             patch.object(reader, 'read_timeframe', AsyncMock(side_effect=[failed, success])), \
             patch.object(reader.asyncio, 'sleep', AsyncMock()) as sleep:
            result = await reader._retry_timeframe_on_fresh_page(None, 'fixture://', '24h', None, 3)
        self.assertTrue(result['verified'])
        self.assertEqual(opened.await_count, 2)
        self.assertEqual(sleep.await_count, 1)
        history = result['dom_capture']['payload']['attempt_history']
        self.assertEqual([entry['outcome'] for entry in history], ['FAILED', 'ACCEPTED'])
        self.assertEqual(result['dom_capture']['payload']['metadata']['attempt'], 2)
        for page in pages:
            page.close.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
