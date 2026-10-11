"""Real PostgreSQL regression for inverse forward-outcome hydration.

Only TEST_DATABASE_URL on an explicit local/CI test database is allowed.
Synthetic disposable schemas; no market data, provider, transport or live URL.
The legacy loader below is frozen from commit
796f3f331436ce180edee321b1bcb721e58b3709, store blob
c8fc153db03d93a1e4b19036855889c732ed24e1. Its SQL and postprocessing
are unchanged; only its name and module-qualified constants were adjusted.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib
import json
import os
from typing import Any, Mapping
import unittest
from uuid import uuid4

store = None
FORWARD_MARKER = "FORWARD_OUTCOME_MUST_NOT_DECODE"
CUTOFF = datetime(2026, 9, 3, 21, tzinfo=timezone.utc)
NOW = CUTOFF + timedelta(days=1)


def legacy_load_scope_rows(conn:Any,scope:Mapping[str,Any],*,row_limit:int=5000,now:datetime|None=None)->tuple[list[dict[str,Any]],bool]:
    # First matching time is chosen BEFORE any outcome join. All simultaneous
    # matching cards/coins are retained, so missing labels cannot select winners.
    cutoff=store.period_contract(scope)['period_start_utc']
    now=now or datetime.now(timezone.utc)
    candidate_count=conn.execute("SELECT COUNT(*) AS n FROM (SELECT event_id FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s) AND alert_time_utc>=%s AND alert_time_utc<=%s LIMIT 10001) bounded",(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],cutoff,now)).fetchone()['n']
    rows=conn.execute('''
        WITH source_matches AS MATERIALIZED (
            SELECT * FROM research_ordered_formula_matches WHERE candidate_key=%s AND direction=%s AND (%s='ALL' OR symbol=%s)
              AND alert_time_utc>=%s AND alert_time_utc<=%s
            ORDER BY alert_time_utc,event_id LIMIT 10000
        ), matched AS MATERIALIZED (
            SELECT m.*,membership.btc_parent_movement_id,membership.membership_status,
                   parent.evidence_eligible AS parent_evidence_eligible,
                   parent.start_time_utc AS parent_start_time_utc,
                   MIN(m.alert_time_utc) OVER(PARTITION BY membership.btc_parent_movement_id) AS first_match_time
            FROM source_matches m
            JOIN research_event_btc_movements membership ON membership.event_id=m.event_id
              AND membership.episode_policy_version=%s AND membership.membership_status='LIVE'
            JOIN research_btc_parent_movements parent ON parent.btc_parent_movement_id=membership.btc_parent_movement_id
              AND parent.episode_policy_version=%s AND parent.evidence_eligible IS TRUE
              AND parent.start_time_utc>=%s
            WHERE m.candidate_key=%s AND m.direction=%s AND (%s='ALL' OR m.symbol=%s)
        ), representatives AS MATERIALIZED (
            SELECT * FROM matched WHERE alert_time_utc=first_match_time
            ORDER BY alert_time_utc,btc_parent_movement_id,event_id LIMIT %s
        )
        SELECT m.*,m.alert_time_utc AS features_observed_at_utc,
               'btc-parent-close-reversal-200bps-v1' AS episode_policy_version,
               %s::integer AS window_minutes,%s::integer AS threshold_bps,
               CASE WHEN o.event_id IS NULL THEN jsonb_build_object('window_minutes',%s::integer,'threshold_bps',%s::integer)
                    ELSE to_jsonb(o)-'calculation_audit'-'threshold_policy' END AS ordered_outcome
        FROM representatives m LEFT JOIN research_ordered_first_touch_outcomes o
          ON o.event_id=m.event_id AND o.window_minutes=%s AND o.threshold_bps=%s
             AND o.method_version='ordered-first-touch-v7'
        ORDER BY m.alert_time_utc,m.event_id
    ''',(scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],cutoff,now,store.PARENT_POLICY,store.PARENT_POLICY,cutoff,scope['candidate_key'],scope['direction'],scope['symbol'],scope['symbol'],row_limit+1,
         scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'],scope['window_minutes'],scope['threshold_bps'])).fetchall()
    truncated=len(rows)>row_limit or candidate_count>10000
    if len(rows)>row_limit:
        # Never describe a partial simultaneous cohort at the row-budget edge.
        last_parent=rows[row_limit]['btc_parent_movement_id']
        rows=[row for row in rows[:row_limit] if row['btc_parent_movement_id']!=last_parent]
    if str(scope['candidate_key']).startswith(store.questions.VERSION+':INVERSE:'):
        import research_ordered_inverse_store as inverse_store
        present=inverse_store.available(conn)
        labels=inverse_store.load_inverse_outcomes(conn,[row['event_id'] for row in rows],scope['window_minutes'],scope['threshold_bps']) if present else {}
        ids=inverse_store.outcome_event_ids(conn,[row['event_id'] for row in rows]) if present else {}
        rows=[{**row,'source_analysis_direction':{'LONG':'SHORT','SHORT':'LONG'}[scope['direction']],
            'research_orientation':'INVERSE','outcome_event_id':ids.get(row['event_id']),
            'ordered_outcome':labels.get(row['event_id'],{'window_minutes':scope['window_minutes'],'threshold_bps':scope['threshold_bps']})} for row in rows]
    return rows,truncated


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class InverseLoadingPostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        from psycopg.types.json import set_json_loads

        cls.dsn = os.environ["TEST_DATABASE_URL"]
        info = conninfo_to_dict(cls.dsn)
        if (info.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (info.get("dbname", "").startswith("test_")
                        or info.get("dbname", "").endswith("_test"))):
            raise ValueError("Integration requires an explicit local/CI test database")
        cls.psycopg, cls.sql = psycopg, sql
        cls.dict_row, cls.set_json_loads = staticmethod(dict_row), staticmethod(set_json_loads)
        global store
        store = importlib.import_module("research_formula_ordered_store")
        cls.inverse = importlib.import_module("research_ordered_inverse_store")

    def setUp(self):
        self.schema = "test_inverse_loading_" + uuid4().hex
        self.conn = self.psycopg.connect(
            self.dsn, row_factory=self.dict_row, autocommit=True,
            connect_timeout=5,
            options="-c statement_timeout=10000 -c lock_timeout=1000")
        self.conn.execute(self.sql.SQL("CREATE SCHEMA {}").format(
            self.sql.Identifier(self.schema)))
        self.addCleanup(self.cleanup)
        self.conn.execute(self.sql.SQL("SET search_path TO {}").format(
            self.sql.Identifier(self.schema)))
        self.conn.execute("""
            CREATE TABLE research_ordered_formula_matches (
                candidate_key text, event_id bigint, symbol text, direction text,
                alert_time_utc timestamptz, snapshot_id text, features jsonb,
                PRIMARY KEY(candidate_key,event_id)
            );
            CREATE TABLE research_event_btc_movements (
                event_id bigint, episode_policy_version text,
                btc_parent_movement_id text, membership_status text,
                PRIMARY KEY(event_id,episode_policy_version)
            );
            CREATE TABLE research_btc_parent_movements (
                btc_parent_movement_id text, episode_policy_version text,
                start_time_utc timestamptz, evidence_eligible boolean,
                PRIMARY KEY(btc_parent_movement_id,episode_policy_version)
            );
            CREATE TABLE research_ordered_first_touch_outcomes (
                event_id bigint, window_minutes integer, threshold_bps integer,
                method_version text, status text, direction text,
                observed_through_utc timestamptz, calculation_audit jsonb,
                threshold_policy jsonb, hydration_probe text,
                PRIMARY KEY(event_id,window_minutes,threshold_bps,method_version)
            );
            CREATE TABLE research_ordered_inverse_requests (
                linked_source_event_id bigint, inverse_version text,
                outcome_event_id bigint, queue_status text,
                PRIMARY KEY(linked_source_event_id,inverse_version)
            );
        """, prepare=False)
        self.normal_key = store.questions.VERSION + ":TEST_LOADING_NORMAL"
        self.inverse_key = store.questions.VERSION + ":INVERSE:" + self.normal_key
        # Each candidate has the same source IDs. Its analysis direction is
        # different; original labels deliberately disagree with inverse ones.
        specs = [
            (1, CUTOFF-timedelta(minutes=30), "cross", CUTOFF-timedelta(hours=2)),
            (2, CUTOFF+timedelta(minutes=30), "cross", CUTOFF-timedelta(hours=2)),
            (3, CUTOFF+timedelta(hours=2), "inside", CUTOFF+timedelta(hours=1)),
            (4, CUTOFF+timedelta(hours=2), "inside", CUTOFF+timedelta(hours=1)),
            (5, CUTOFF+timedelta(hours=3), "inside", CUTOFF+timedelta(hours=1)),
            (6, CUTOFF, "boundary", CUTOFF),
            (7, NOW+timedelta(minutes=1), "future", NOW),
            (8, CUTOFF+timedelta(hours=4), "no-label", CUTOFF+timedelta(hours=3)),
            (9, CUTOFF+timedelta(hours=5), "no-map", CUTOFF+timedelta(hours=4)),
            (10, CUTOFF+timedelta(hours=6), "ineligible", CUTOFF),
            (11, CUTOFF+timedelta(hours=7), "missing-member", CUTOFF),
            (12, CUTOFF+timedelta(hours=8), "absent-member", CUTOFF),
        ]
        for event_id, when, parent, started in specs:
            symbol = "ETH" if event_id == 3 else "BTC"
            for candidate, direction in ((self.normal_key, "LONG"),
                                         (self.inverse_key, "SHORT")):
                self.conn.execute("""
                    INSERT INTO research_ordered_formula_matches
                    VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)
                """, (candidate, event_id, symbol, direction, when,
                      "scan-" + str(event_id),
                      json.dumps({"event_id": event_id, "frozen": True, "label": "synthetic-Ω"})))
            self.conn.execute("""
                INSERT INTO research_btc_parent_movements VALUES(%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (parent, store.PARENT_POLICY, started, event_id != 10))
            if event_id != 12:
                self.conn.execute("""
                    INSERT INTO research_event_btc_movements VALUES(%s,%s,%s,%s)
                """, (event_id, store.PARENT_POLICY, parent,
                      "BTC_DATA_MISSING" if event_id == 11 else "LIVE"))
            if event_id != 3:
                self.outcome(event_id, "FAILURE", forward=True)
        for source_id, derived_id, status, queue_status in [
            (1, 1001, "SUCCESS", "READY"),
            (3, 1003, "SUCCESS", "READY"),
            (4, 1004, "FAILURE", "READY"),
            (6, 1006, "SUCCESS", "REJECTED"),
            (8, 1008, None, "READY"),
        ]:
            self.conn.execute("""
                INSERT INTO research_ordered_inverse_requests VALUES(%s,%s,%s,%s)
            """, (source_id, self.inverse.VERSION, derived_id, queue_status))
            if status is not None:
                self.outcome(derived_id, status, forward=False)

    def cleanup(self):
        self.conn.close()
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute(self.sql.SQL("DROP SCHEMA {} CASCADE").format(
                self.sql.Identifier(self.schema)))

    def outcome(self, event_id, status, *, forward):
        # The marker is deliberately NOT in calculation_audit or
        # threshold_policy, which the ordinary projection already removes.
        probe = FORWARD_MARKER + ("x" * 65536) if forward else "inverse-only"
        self.conn.execute("""
            INSERT INTO research_ordered_first_touch_outcomes
            VALUES(%s,60,25,'ordered-first-touch-v7',%s,%s,%s,%s::jsonb,%s::jsonb,%s)
        """, (event_id, status, "LONG" if forward else "SHORT", NOW,
              '{"audit":"synthetic"}', '{"threshold":"synthetic"}', probe))

    def scope(self, *, inverse=True, period="SINCE_20260904"):
        return {
            "scope_key": "fixture:" + period + (":inverse" if inverse else ":normal"),
            "candidate_key": self.inverse_key if inverse else self.normal_key,
            "symbol": "ALL", "direction": "SHORT" if inverse else "LONG",
            "window_minutes": 60, "threshold_bps": 25,
            "period_key": period, "period_start_utc": store.PERIODS[period],
        }

    @contextmanager
    def reject_forward_json(self):
        def decode(data):
            raw = data.encode() if isinstance(data, str) else bytes(data)
            if FORWARD_MARKER.encode() in raw:
                raise AssertionError("Original forward outcome was hydrated")
            return json.loads(raw)
        self.set_json_loads(decode, self.conn)
        try:
            yield
        finally:
            self.set_json_loads(json.loads, self.conn)

    def input_digest(self, scope, rows):
        # Same two-stage input hash as the worker, with fixed synthetic
        # coverage/validation metadata and no common-window record.
        value = store.question_store.evaluation_input(
            scope, [{**row, "common_window_record": None} for row in rows],
            now=NOW, population_complete=True, membership_complete=True)
        return store.digest({"input": value, "validation_available": True,
                             "registered_attempts": 17})

    def assert_parity(self, scope, before, after):
        expected_rows, expected_truncated = before
        actual_rows, actual_truncated = after
        self.assertEqual(actual_truncated, expected_truncated)
        self.assertEqual(store.canonical(actual_rows), store.canonical(expected_rows))
        self.assertEqual(self.input_digest(scope, actual_rows),
                         self.input_digest(scope, expected_rows))
        actual_summary = store.evaluator.summarize_scope(
            actual_rows, analysis_as_of_utc=NOW, truncated=actual_truncated)
        expected_summary = store.evaluator.summarize_scope(
            expected_rows, analysis_as_of_utc=NOW, truncated=expected_truncated)
        self.assertEqual(store.canonical(actual_summary), store.canonical(expected_summary))

    def test_inverse_complete_rows_and_input_digest_match_frozen_legacy(self):
        for period, expected_ids in [
            ("ALL_COMPATIBLE_SINCE_20260816", [1, 6, 3, 4, 8, 9]),
            ("SINCE_20260904", [6, 3, 4, 8, 9]),
        ]:
            with self.subTest(period=period):
                scope = self.scope(period=period)
                before = legacy_load_scope_rows(self.conn, scope, now=NOW)
                with self.reject_forward_json():
                    after = store.load_scope_rows(self.conn, scope, now=NOW)
                self.assert_parity(scope, before, after)
                rows, truncated = after
                self.assertFalse(truncated)
                self.assertEqual([row["event_id"] for row in rows], expected_ids)
                by_id = {row["event_id"]: row for row in rows}
                # Both simultaneous first matches survive, even though the
                # original outcome for event3 is missing and labels disagree.
                self.assertEqual(by_id[3]["ordered_outcome"]["status"], "SUCCESS")
                self.assertEqual(by_id[4]["ordered_outcome"]["status"], "FAILURE")
                self.assertEqual(by_id[3]["outcome_event_id"], 1003)
                for event_id, mapped in ((6, None), (8, 1008), (9, None)):
                    self.assertEqual(by_id[event_id]["outcome_event_id"], mapped)
                    self.assertEqual(by_id[event_id]["ordered_outcome"],
                                     {"window_minutes": 60, "threshold_bps": 25})
                for row in rows:
                    self.assertEqual(row["research_orientation"], "INVERSE")
                    self.assertEqual(row["source_analysis_direction"], "LONG")

    def test_normal_outcomes_are_retained_and_decoder_trap_is_effective(self):
        for period in store.PERIODS:
            with self.subTest(period=period):
                scope = self.scope(inverse=False, period=period)
                before = legacy_load_scope_rows(self.conn, scope, now=NOW)
                with self.reject_forward_json():
                    with self.assertRaisesRegex(AssertionError, "Original forward outcome was hydrated"):
                        store.load_scope_rows(self.conn, scope, now=NOW)
                after = store.load_scope_rows(self.conn, scope, now=NOW)
                self.assert_parity(scope, before, after)
                by_id = {row["event_id"]: row for row in after[0]}
                self.assertEqual(by_id[3]["ordered_outcome"],
                                 {"window_minutes": 60, "threshold_bps": 25})
                retained = by_id[4]["ordered_outcome"]
                self.assertEqual(retained["status"], "FAILURE")
                self.assertTrue(retained["hydration_probe"].startswith(FORWARD_MARKER))
                self.assertNotIn("calculation_audit", retained)
                self.assertNotIn("threshold_policy", retained)
                self.assertNotIn("research_orientation", by_id[4])

    def test_unavailable_inverse_schema_cannot_fall_back_to_forward(self):
        self.conn.execute("DROP TABLE research_ordered_inverse_requests")
        for period in store.PERIODS:
            with self.subTest(period=period):
                scope = self.scope(period=period)
                before = legacy_load_scope_rows(self.conn, scope, now=NOW)
                with self.reject_forward_json():
                    after = store.load_scope_rows(self.conn, scope, now=NOW)
                self.assert_parity(scope, before, after)
                self.assertGreater(len(after[0]), 0)
                for row in after[0]:
                    self.assertIsNone(row["outcome_event_id"])
                    self.assertEqual(row["ordered_outcome"],
                                     {"window_minutes": 60, "threshold_bps": 25})

    def test_row_limit_keeps_whole_simultaneous_parent_cohorts(self):
        for inverse in (False, True):
            for limit, expected_ids in ((2, [6]), (3, [6, 3, 4])):
                with self.subTest(inverse=inverse, limit=limit):
                    scope = self.scope(inverse=inverse)
                    before = legacy_load_scope_rows(self.conn, scope, row_limit=limit, now=NOW)
                    if inverse:
                        with self.reject_forward_json():
                            after = store.load_scope_rows(self.conn, scope, row_limit=limit, now=NOW)
                    else:
                        after = store.load_scope_rows(self.conn, scope, row_limit=limit, now=NOW)
                    self.assert_parity(scope, before, after)
                    self.assertTrue(after[1])
                    self.assertEqual([row["event_id"] for row in after[0]], expected_ids)

    def test_source_filters_preserve_all_and_single_coin_cohorts(self):
        # Earlier valid-parent decoys would replace the simultaneous first
        # cohort if either source candidate/direction predicate were lost.
        for offset, (candidate, direction) in enumerate(
                ((self.normal_key, "LONG"), (self.inverse_key, "SHORT"))):
            for index, (key, side) in enumerate((
                    (candidate, {"LONG": "SHORT", "SHORT": "LONG"}[direction]),
                    (candidate + ":UNRELATED", direction),
                    (candidate, None))):
                event_id = 20 + offset * 3 + index
                self.conn.execute("""
                    INSERT INTO research_ordered_formula_matches
                    VALUES(%s,%s,'BTC',%s,%s,%s,%s::jsonb)
                """, (key, event_id, side, CUTOFF + timedelta(minutes=90),
                      "decoy-" + str(event_id), '{"synthetic_decoy":true}'))
                self.conn.execute("""
                    INSERT INTO research_event_btc_movements VALUES(%s,%s,'inside','LIVE')
                """, (event_id, store.PARENT_POLICY))
            # ALL admits an unknown symbol, while a specific coin does not.
            self.conn.execute("""
                INSERT INTO research_ordered_formula_matches
                VALUES(%s,40,NULL,%s,%s,'null-symbol','{}'::jsonb)
            """, (candidate, direction, CUTOFF + timedelta(hours=2)))
        self.conn.execute("""
            INSERT INTO research_event_btc_movements VALUES(40,%s,'inside','LIVE')
        """, (store.PARENT_POLICY,))

        for inverse in (False, True):
            for period in store.PERIODS:
                prefix = [1] if period == "ALL_COMPATIBLE_SINCE_20260816" else []
                for symbol, expected_ids in (
                        ("ALL", prefix + [6, 3, 4, 40, 8, 9]),
                        ("BTC", prefix + [6, 4, 8, 9]),
                        ("ETH", [3])):
                    with self.subTest(inverse=inverse, period=period, symbol=symbol):
                        scope = {**self.scope(inverse=inverse, period=period),
                                 "symbol": symbol}
                        before = legacy_load_scope_rows(self.conn, scope, now=NOW)
                        after = store.load_scope_rows(self.conn, scope, now=NOW)
                        self.assert_parity(scope, before, after)
                        self.assertFalse(after[1])
                        self.assertEqual([row["event_id"] for row in after[0]], expected_ids)

    def test_reenabling_forward_join_is_detected_by_decoder_guard(self):
        class ReenabledJoin:
            def __init__(self, connection):
                self.connection, self.mutations = connection, 0

            def execute(self, sql, params=None):
                if "FROM representatives m LEFT JOIN research_ordered_first_touch_outcomes o" in sql:
                    if len(params) != 17 or params[-1] is not False:
                        raise AssertionError("Inverse join gate was not reached")
                    params = (*params[:-1], True)
                    self.mutations += 1
                return self.connection.execute(sql, params)

        mutated = ReenabledJoin(self.conn)
        with self.reject_forward_json():
            with self.assertRaisesRegex(AssertionError, "Original forward outcome was hydrated"):
                store.load_scope_rows(mutated, self.scope(), now=NOW)
        self.assertEqual(mutated.mutations, 1)


if __name__ == "__main__":
    unittest.main()
