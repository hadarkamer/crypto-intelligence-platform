"""Offline same-read candidate capture and differential operational checks.

The embedded baseline is historical_point_nearest's exact undecorated body
from deployed history SHA 0c8ad1962720688b55155ee66e24fba1d040a6621769404fcc8f4f17d3800917.
Both versions use the same deterministic connection fixture; no provider or
database connection is opened by these tests.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import sqlite3
import types
import unittest
from unittest.mock import patch

import coinglass_history_backfill as history
import research_consumed_inputs as consumed


LEGACY_NEAREST_SOURCE = '''
def historical_point_nearest(symbol: str, target_time: datetime) -> Optional[Dict[str, Any]]:
    init_db()
    symbol = str(symbol or "").strip().upper()
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    else:
        target_time = target_time.astimezone(timezone.utc)

    before = None
    after = None
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            before = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=%s AND candle_time<=%s ORDER BY candle_time DESC LIMIT 1",
                (symbol, target_time),
            ).fetchone()
            after = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=%s AND candle_time>=%s ORDER BY candle_time ASC LIMIT 1",
                (symbol, target_time),
            ).fetchone()
    else:
        target_iso = target_time.isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            before = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=? AND candle_time<=? ORDER BY candle_time DESC LIMIT 1",
                (symbol, target_iso),
            ).fetchone()
            after = conn.execute(
                "SELECT candle_time,price_close,oi_close_usd FROM oi_price_history "
                "WHERE symbol=? AND candle_time>=? ORDER BY candle_time ASC LIMIT 1",
                (symbol, target_iso),
            ).fetchone()

    candidates = [dict(row) for row in (before, after) if row]
    if not candidates:
        return None

    def as_utc(value):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)

    data = min(candidates, key=lambda row: abs((as_utc(row["candle_time"]) - target_time).total_seconds()))
    return {
        "collected_at": data["candle_time"],
        "price": float(data["price_close"]),
        "open_interest_usd": float(data["oi_close_usd"]),
        "source": "historical_backfill",
    }
'''

TARGET = datetime(2026, 10, 29, 17, 0, tzinfo=timezone.utc)


def row(offset, price=2., typed=False):
    timestamp = TARGET + timedelta(minutes=offset)
    return {'candle_time': timestamp if typed else timestamp.isoformat(),
            'price_close': price, 'oi_close_usd': 500.}


def payloads(collector, kind):
    return [body for _, body in consumed.event_payloads(collector.package(), kind)]


class FixturePostgresError(Exception):
    pass


class DiagnosticAbort(BaseException):
    pass


class NativeAbort(BaseException):
    pass


class Fixture:
    def __init__(self, values, backend, error_at=None, error=None):
        self.values, self.backend, self.error_at = values, backend, error_at
        self.error = error if error is not None else (FixturePostgresError('private credential message')
                      if backend == 'postgresql' else sqlite3.OperationalError('private credential message'))
        self.log, self.count = [], 0
        self.sql = types.SimpleNamespace(connect=self.connect, Row=sqlite3.Row, Error=sqlite3.Error)
        self.pg = types.SimpleNamespace(connect=self.connect, Error=FixturePostgresError)
        self.dict_row = object()
    def connect(self, *args, **kwargs):
        self.log.append(('connect', args, kwargs))
        return self
    def __enter__(self):
        self.log.append(('enter',))
        return self
    def __exit__(self, exc_type, exc, tb):
        self.log.append(('exit', exc_type))
        return False
    def execute(self, sql, args):
        self.count += 1
        self.log.append(('execute', sql, args))
        if self.error_at == ('execute', self.count):
            raise self.error
        return self
    def fetchone(self):
        self.log.append(('fetchone',))
        if self.error_at == ('fetchone', self.count):
            raise self.error
        return self.values[self.count - 1]
    def init(self):
        self.log.append(('init_db',))


def legacy(fixture, symbol, target):
    namespace = {'datetime': datetime, 'timezone': timezone, 'init_db': fixture.init,
                 '_use_postgres': lambda: fixture.backend == 'postgresql',
                 'sqlite3': fixture.sql, 'psycopg': fixture.pg,
                 'dict_row': fixture.dict_row, 'DB_PATH': 'fixture.db', 'DATABASE_URL': 'fixture-url'}
    exec(compile('from __future__ import annotations\n' + LEGACY_NEAREST_SOURCE, '<pinned-legacy-nearest>', 'exec'), namespace)
    return namespace['historical_point_nearest'](symbol, target)


def current(fixture, symbol, target):
    with patch.multiple(history, init_db=fixture.init,
                        _use_postgres=lambda: fixture.backend == 'postgresql',
                        sqlite3=fixture.sql, psycopg=fixture.pg, dict_row=fixture.dict_row,
                        DB_PATH='fixture.db', DATABASE_URL='fixture-url'):
        return history.historical_point_nearest(symbol, target)


class CandidateCaptureTests(unittest.TestCase):
    def differential(self, before, after, backend='sqlite', symbol='DOGE', target=TARGET):
        original = deepcopy((before, after))
        fixture = Fixture([before, after], backend)
        expected = legacy(fixture, symbol, target)
        expected_log = fixture.log
        fixture.log, fixture.count = [], 0
        with consumed.collect() as collector:
            actual = current(fixture, symbol, target)
        self.assertEqual(actual, expected)
        self.assertEqual(fixture.log, expected_log)
        self.assertEqual((before, after), original)
        self.assertEqual(fixture.count, 2)
        consumed.validate(collector.package())
        capture = payloads(collector, 'oi_historical_nearest_candidates')
        return actual, collector, capture

    def test_both_backends_preserve_exact_queries_selection_and_returns(self):
        cases = [(row(-30), row(30), 'before'), (row(-10), row(20), 'before'),
                 (row(-20), row(10), 'after'), (row(-30), None, 'before'),
                 (None, row(30), 'after'), (None, None, None),
                 (row(0), row(0), 'before'), (row(-5, typed=True), row(10, typed=True), 'before')]
        for backend in ('sqlite', 'postgresql'):
            for before, after, side in cases:
                with self.subTest(backend=backend, before=before, after=after):
                    result, collector, captures = self.differential(before, after, backend)
                    self.assertEqual(len(captures), 1)
                    capture = captures[0]
                    self.assertEqual(capture['status'], 'COMPLETE')
                    self.assertEqual(capture['backend'], backend)
                    self.assertEqual(capture['selection']['selected_side'], side)
                    self.assertEqual(capture['selection']['status'], 'NO_CANDIDATE' if side is None else 'SELECTED')
                    self.assertEqual(capture['selection']['returned_result'], consumed.normalized(result))
                    self.assertEqual(payloads(collector, 'oi_historical_nearest')[0]['result'], consumed.normalized(result))
                    clocks = [capture['call_started_at_utc']]
                    for name, value in (('before', before), ('after', after)):
                        query = capture['queries'][name]
                        self.assertEqual(query['status'], 'SUCCESS')
                        self.assertEqual(query['row_found'], value is not None)
                        projected = history._nearest_capture_values(value, ('candle_time', 'price_close', 'oi_close_usd'))[0] if value else None
                        self.assertEqual(query['row'], projected)
                        clocks.extend([query['call_started_at_utc'], query['call_finished_at_utc']])
                    clocks.append(capture['call_finished_at_utc'])
                    self.assertEqual(clocks, sorted(clocks))
                    self.assertLessEqual(len(consumed.canonical(capture).encode()), history._NEAREST_CAPTURE_MAX_BYTES)
                    self.assertFalse(collector.errors)

    def test_query_errors_reraise_original_and_scope_absence(self):
        for backend in ('sqlite', 'postgresql'):
            for method in ('execute', 'fetchone'):
                for position in (1, 2):
                    with self.subTest(backend=backend, method=method, position=position):
                        fixture = Fixture([None, row(5)], backend, (method, position))
                        with self.assertRaises(type(fixture.error)) as baseline:
                            legacy(fixture, 'ETH', TARGET)
                        expected_log = fixture.log
                        fixture.log, fixture.count = [], 0
                        with consumed.collect() as collector:
                            with self.assertRaises(type(fixture.error)) as observed:
                                current(fixture, 'ETH', TARGET)
                        self.assertIs(observed.exception, baseline.exception)
                        self.assertEqual(fixture.log, expected_log)
                        capture = payloads(collector, 'oi_historical_nearest_candidates')[0]
                        self.assertEqual(capture['status'], 'UNAVAILABLE' if position == 1 else 'PARTIAL')
                        self.assertEqual(capture['operation_error_code'], 'POSTGRES_ERROR' if backend == 'postgresql' else 'SQLITE_ERROR')
                        failed = capture['queries']['before' if position == 1 else 'after']
                        self.assertEqual(failed['status'], 'ERROR')
                        self.assertNotIn('row_found', failed)
                        if position == 1:
                            self.assertEqual(capture['queries']['after']['status'], 'NOT_EXECUTED')
                        else:
                            self.assertFalse(capture['queries']['before']['row_found'])
                        self.assertNotIn('private credential message', consumed.canonical(collector.package()))
                        self.assertTrue(collector.errors)

    def test_capture_failures_do_not_change_reads_results_or_original_errors(self):
        original_projection = history._nearest_capture_values
        def broken_projection(value, keys):
            if 'candle_time' in keys:
                raise RuntimeError('private projection message')
            return original_projection(value, keys)
        for failure in ('projection', 'record', 'budget'):
            with self.subTest(failure=failure):
                with consumed.collect(max_raw_bytes=1 if failure == 'budget' else consumed.MAX_RAW_BYTES) as collector:
                    if failure == 'projection':
                        with patch.object(history, '_nearest_capture_values', side_effect=broken_projection):
                            result = current(Fixture([row(-5), row(5)], 'sqlite'), 'DOGE', TARGET)
                    elif failure == 'record':
                        with patch.object(consumed, 'record', side_effect=RuntimeError('private sink message')):
                            result = current(Fixture([row(-5), row(5)], 'sqlite'), 'DOGE', TARGET)
                    else:
                        result = current(Fixture([row(-5), row(5)], 'sqlite'), 'DOGE', TARGET)
                self.assertEqual(result['price'], 2.)
                self.assertTrue(collector.errors)
                self.assertNotIn('private', consumed.canonical(collector.package()))
                if failure == 'projection':
                    self.assertEqual(payloads(collector, 'oi_historical_nearest_candidates')[0]['status'], 'UNAVAILABLE')
        fixture = Fixture([row(-5), row(5)], 'sqlite', ('fetchone', 2))
        with consumed.collect() as collector, patch.object(consumed, 'record', side_effect=RuntimeError('sink')):
            with self.assertRaises(sqlite3.OperationalError) as error:
                current(fixture, 'DOGE', TARGET)
        self.assertIs(error.exception, fixture.error)
        self.assertEqual(fixture.count, 2)
        self.assertTrue(collector.errors)

    def test_initialization_and_selection_errors_remain_original(self):
        failure = sqlite3.OperationalError('private initialization message')
        with consumed.collect() as collector, patch.object(history, 'init_db', side_effect=failure):
            with self.assertRaises(sqlite3.OperationalError) as raised:
                history.historical_point_nearest('DOGE', TARGET)
        self.assertIs(raised.exception, failure)
        capture = payloads(collector, 'oi_historical_nearest_candidates')[0]
        self.assertEqual(capture['status'], 'UNAVAILABLE')
        self.assertEqual([q['status'] for q in capture['queries'].values()], ['NOT_EXECUTED', 'NOT_EXECUTED'])
        self.assertNotIn('private initialization message', consumed.canonical(collector.package()))
        fixture = Fixture([row(-5, price='invalid numeric value'), row(5)], 'sqlite')
        with self.assertRaises(ValueError):
            legacy(fixture, 'DOGE', TARGET)
        expected_log = fixture.log
        fixture.log, fixture.count = [], 0
        with consumed.collect() as collector:
            with self.assertRaises(ValueError):
                current(fixture, 'DOGE', TARGET)
        self.assertEqual(fixture.log, expected_log)
        capture = payloads(collector, 'oi_historical_nearest_candidates')[0]
        self.assertEqual(capture['status'], 'PARTIAL')
        self.assertEqual(capture['operation_error_code'], 'VALUE_ERROR')
        self.assertTrue(all(q['status'] == 'SUCCESS' for q in capture['queries'].values()))

    def test_diagnostic_baseexception_never_replaces_native_return_or_baseexception(self):
        fixture = Fixture([row(-5), row(5)], 'sqlite')
        expected = legacy(fixture, 'DOGE', TARGET)
        expected_log = fixture.log
        fixture.log, fixture.count = [], 0
        with consumed.collect() as collector, patch.object(consumed, 'record', side_effect=DiagnosticAbort('private')):
            self.assertEqual(current(fixture, 'DOGE', TARGET), expected)
        self.assertEqual(fixture.log, expected_log)
        self.assertTrue(collector.errors)
        original_projection = history._nearest_capture_values
        def interrupted_projection(value, keys):
            if 'candle_time' in keys:
                raise DiagnosticAbort('private')
            return original_projection(value, keys)
        with consumed.collect() as collector, patch.object(history, '_nearest_capture_values', side_effect=interrupted_projection):
            self.assertEqual(current(Fixture([row(-5), row(5)], 'sqlite'), 'DOGE', TARGET), expected)
        self.assertEqual(payloads(collector, 'oi_historical_nearest_candidates')[0]['status'], 'UNAVAILABLE')
        for position in (1, 2):
            fixture = Fixture([row(-5), row(5)], 'sqlite', ('fetchone', position), error=NativeAbort('native'))
            with self.assertRaises(NativeAbort) as baseline:
                legacy(fixture, 'DOGE', TARGET)
            expected_log = fixture.log
            fixture.log, fixture.count = [], 0
            with consumed.collect() as collector, patch.object(consumed, 'record', side_effect=DiagnosticAbort('private')):
                with self.assertRaises(NativeAbort) as observed:
                    current(fixture, 'DOGE', TARGET)
            self.assertIs(observed.exception, baseline.exception)
            self.assertEqual(fixture.log, expected_log)
            self.assertTrue(collector.errors)

    def test_bounds_are_explicit_and_allowlist_excludes_other_columns(self):
        before = row(-5, price='0' * 300 + '1')
        before['unrelated_secret_column'] = 'must-not-be-recorded'
        result, collector, captures = self.differential(before, row(5))
        self.assertEqual(result['price'], 1.)
        self.assertEqual(captures[0]['status'], 'TRUNCATED')
        self.assertTrue(collector.errors)
        self.assertNotIn('must-not-be-recorded', consumed.canonical(collector.package()))
        with patch.object(history, '_NEAREST_CAPTURE_MAX_BYTES', 256):
            result, collector, captures = self.differential(row(-5), row(5))
        self.assertEqual(result['price'], 2.)
        self.assertEqual(captures, [])
        self.assertTrue(collector.errors)
        self.assertTrue(payloads(collector, 'capture_gap'))

    def test_other_symbols_and_absent_collector_keep_operational_reads(self):
        _, collector, captures = self.differential(row(-5), row(5), symbol='BTC')
        self.assertEqual(captures, [])
        self.assertEqual(collector.events, [])
        fixture = Fixture([row(-5), row(5)], 'sqlite')
        self.assertEqual(current(fixture, 'DOGE', TARGET)['price'], 2.)
        self.assertEqual(fixture.count, 2)

    def test_naive_target_uses_actual_utc_query_arguments_without_changing_return(self):
        _, _, captures = self.differential(row(-5), row(5), target=TARGET.replace(tzinfo=None))
        self.assertEqual(captures[0]['arguments']['target_time'], TARGET.isoformat())


if __name__ == '__main__':
    unittest.main()
