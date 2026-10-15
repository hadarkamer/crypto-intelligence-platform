"""Offline application regression for bounded metric lookup and caller remapping.

The fake records the unchanged SQL/parameters and returns selected stored rows;
it does not emulate PostgreSQL array adaptation or its overflow errors. Native
owned-host receipts cover real SQL decoding separately.
"""
from datetime import datetime, timedelta, timezone
import re
import unittest

import research_common_window_metrics_store as metrics
import research_formula_ordered_store as ordered

AVAILABILITY_SQL = "SELECT to_regclass('research_common_window_metrics') IS NOT NULL AS available"
LOOKUP_SQL = """SELECT event_id,window_minutes,method_version,status,measurement_start_utc,
    window_end_utc,result FROM research_common_window_metrics
    WHERE event_id=ANY(%s::bigint[]) AND window_minutes=%s AND method_version=%s"""
START = datetime(2026, 9, 8, tzinfo=timezone.utc)


def stored(event_id, *, result=None, status="READY", window=60, version=metrics.METHOD_VERSION):
    return {"event_id":event_id, "window_minutes":window, "method_version":version,
            "status":status, "measurement_start_utc":START,
            "window_end_utc":START+timedelta(minutes=window), "result":result}


def normalized_sql(value):
    return " ".join(value.split())


class Reply:
    def __init__(self, *, rows=(), one=None):
        self.rows, self.one = list(rows), one

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.one


class Connection:
    """Only the two actual read queries are supported; unexpected SQL fails."""
    def __init__(self, rows=(), *, available=True, failure=None):
        self.rows, self.available, self.failure = list(rows), available, failure
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((normalized_sql(sql), params))
        if normalized_sql(sql) == AVAILABILITY_SQL:
            if params:
                raise AssertionError("Availability query unexpectedly bound values")
            return Reply(one={"available":self.available})
        if normalized_sql(sql) != normalized_sql(LOOKUP_SQL):
            raise AssertionError("Metric lookup SQL changed")
        if self.failure is not None:
            raise self.failure
        if not isinstance(params, tuple) or len(params) != 3:
            raise AssertionError("Metric query parameter shape changed")
        array, window, version = params
        if type(array) is not str or not re.fullmatch(r"\{(?:-?\d+(?:,-?\d+)*)?\}", array):
            raise AssertionError("Expected bound PostgreSQL integer-array text")
        ids = {int(value) for value in array[1:-1].split(",") if value}
        return Reply(rows=[row for row in self.rows if int(row["event_id"]) in ids
                          and row["window_minutes"] == window and row["method_version"] == version])


class MetricLookupTests(unittest.TestCase):
    def test_valid_empty_input_never_queries(self):
        for window in metrics.WINDOWS:
            with self.subTest(window=window):
                conn = Connection()
                self.assertEqual(metrics.load_by_event_ids(conn, iter(()), window), {})
                self.assertEqual(conn.calls, [])

    def test_invalid_window_is_rejected_even_for_empty_input(self):
        for window in (0, 59, "60", None):
            with self.subTest(window=window):
                conn = Connection()
                with self.assertRaises(ValueError):
                    metrics.load_by_event_ids(conn, [], window)
                self.assertEqual(conn.calls, [])

    def test_more_than_ten_thousand_unique_ids_never_queries(self):
        conn = Connection()
        with self.assertRaises(ValueError):
            metrics.load_by_event_ids(conn, range(10001), 60)
        self.assertEqual(conn.calls, [])

    def test_duplicate_input_does_not_consume_unique_id_budget(self):
        conn = Connection([stored(7)])
        self.assertEqual(set(metrics.load_by_event_ids(conn, [7]*10001, 60)), {7})
        self.assertEqual(conn.calls[0][1], ("{7}", 60, metrics.METHOD_VERSION))
        boundary = Connection()
        self.assertEqual(metrics.load_by_event_ids(boundary, range(10000), 60), {})
        array = boundary.calls[0][1][0]
        self.assertEqual(len(array[1:-1].split(",")), 10000)
        self.assertTrue(array.startswith("{0,1,2,") and array.endswith(",9999}"))

    def test_existing_int_conversion_and_fixed_parameterized_sql(self):
        conn = Connection([stored(1)])
        ids = (value for value in [True, "0001", 1.9, False, -1.9, 1,
                                   -(2**63), str(2**63-1)])
        self.assertEqual(set(metrics.load_by_event_ids(conn, ids, 60)), {1})
        self.assertEqual(conn.calls, [(normalized_sql(LOOKUP_SQL),
            ("{-9223372036854775808,-1,0,1,9223372036854775807}", 60, metrics.METHOD_VERSION))])
        other = Connection()
        metrics.load_by_event_ids(other, [912345678], 240)
        self.assertEqual(other.calls[0][0], conn.calls[0][0])
        self.assertNotIn("912345678", other.calls[0][0])
        self.assertEqual(other.calls[0][1], ("{912345678}", 240, metrics.METHOD_VERSION))

    def test_invalid_id_preserves_int_conversion_errors_before_query(self):
        for value, error in [("1); SELECT 1;--", ValueError), (None, TypeError)]:
            with self.subTest(value=value):
                conn = Connection()
                with self.assertRaises(error):
                    metrics.load_by_event_ids(conn, [value], 60)
                self.assertEqual(conn.calls, [])

    def test_stored_columns_override_conflicting_result_metadata(self):
        payload = {"event_id":999, "window_minutes":1440, "method_version":"stale",
                   "status":"SUCCESS", "measurement_start_utc":"wrong",
                   "window_end_utc":"wrong", "mfe_pct":2.5, "note":"kept"}
        row = stored(42, result=payload, status="DATA_MISSING")
        conn = Connection([row, stored(42, window=240), stored(42, version="old"), stored(43)])
        result = metrics.load_by_event_ids(conn, [42, 43, 44], 60)
        self.assertEqual(result, {
            42:{"event_id":42, "window_minutes":60, "method_version":metrics.METHOD_VERSION,
                "status":"DATA_MISSING", "measurement_start_utc":START,
                "window_end_utc":START+timedelta(hours=1), "mfe_pct":2.5, "note":"kept"},
            43:{"event_id":43, "window_minutes":60, "method_version":metrics.METHOD_VERSION,
                "status":"READY", "measurement_start_utc":START, "window_end_utc":START+timedelta(hours=1)}})
        self.assertEqual(payload["event_id"], 999)
        self.assertEqual(payload["status"], "SUCCESS")

    def test_database_failure_propagates_without_retry_or_fallback(self):
        failure = RuntimeError("synthetic query failure")
        conn = Connection(failure=failure)
        with self.assertRaises(RuntimeError) as caught:
            metrics.load_by_event_ids(conn, [1], 60)
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(conn.calls), 1)

    def test_actual_normal_caller_preserves_ids_and_missingness(self):
        row = stored(11, result={"mfe_pct":2.0})
        conn = Connection([row, stored(99)])
        scope = {"candidate_key":"PRICE_OI_TOTAL_65", "direction":"LONG", "window_minutes":60}
        result = ordered.common_window_rows(conn, scope, [{"event_id":11}, {"event_id":12}])
        self.assertEqual(set(result), {11})
        self.assertEqual(result[11]["event_id"], 11)
        self.assertEqual(result[11]["mfe_pct"], 2.0)
        self.assertEqual(conn.calls, [(AVAILABILITY_SQL, ()),
            (normalized_sql(LOOKUP_SQL), ("{11,12}", 60, metrics.METHOD_VERSION))])

    def test_actual_inverse_caller_maps_derived_ids_without_forward_fallback(self):
        conn = Connection([stored(101, result={"mfe_pct":3.0}), stored(1), stored(2)])
        scope = {"candidate_key":ordered.questions.VERSION+":INVERSE:PRICE_OI_TOTAL_65",
                 "direction":"SHORT", "window_minutes":60}
        rows = [{"event_id":original, "outcome_event_id":derived,
                 "research_orientation":"INVERSE", "source_analysis_direction":"LONG"}
                for original,derived in ((1,101),(2,None),(3,103),(4,101))]
        result = ordered.common_window_rows(conn, scope, rows)
        self.assertEqual(set(result), {1, 4})
        self.assertEqual(result[1], result[4])
        self.assertEqual(result[1]["event_id"], 101)
        self.assertEqual(result[1]["mfe_pct"], 3.0)
        self.assertEqual(conn.calls, [(AVAILABILITY_SQL, ()),
            (normalized_sql(LOOKUP_SQL), ("{101,103}", 60, metrics.METHOD_VERSION))])
        self.assertIsNone(rows[1]["outcome_event_id"])

    def test_unavailable_schema_and_unmapped_inverse_do_not_fetch_metrics(self):
        scope = {"window_minutes":60}
        for conn, rows in [(Connection(available=False), [{"event_id":1}]),
                           (Connection(), [{"event_id":1,"outcome_event_id":None}])]:
            with self.subTest(available=conn.available):
                self.assertEqual(ordered.common_window_rows(conn, scope, rows), {})
                self.assertEqual(conn.calls, [(AVAILABILITY_SQL, ())])


if __name__ == "__main__":
    unittest.main()
