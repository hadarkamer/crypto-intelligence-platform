"""Evidence integrity, bounded storage and opt-in isolated PostgreSQL tests."""
from __future__ import annotations

import base64
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4
import zlib

import research_consumed_inputs as graph
import research_doge_sequence_capture as capture


def fixture(*, count=2, prior_count=3, omit=()):
    when = datetime(2026, 10, 26, 13, 0, tzinfo=timezone(timedelta(hours=2)))
    events = [{"event_id": 100000 + index, "symbol": "DOGE", "direction": "SHORT",
               "alert_time_utc": when, "current_price": 1.0, "engine_snapshot": {"watch_scan_id": "same"}}
              for index in range(count)]
    prior = [{"event_id": index + 1, "symbol": "ETH" if index % 2 else "DOGE", "direction": "SHORT",
              "alert_time_utc": when - timedelta(minutes=index + 1),
              "event_kind": "ALERT", "delivery_status": "DELIVERED",
              "engine_snapshot": {"watch_scan_id": "same"}} for index in range(prior_count)]
    pairs = [(event, {"captured.value": index, "missing": None}) for index, event in enumerate(prior)]
    candidates = [{"formula_id": "normal"}, {"formula_id": "inverse", "research_orientation": "INVERSE"}]
    metadata = [{key: event[key] for key in ("event_id", "symbol", "direction", "alert_time_utc")} for event in prior]
    with capture.observe() as observation:
        def record(kind, payload):
            if kind not in omit:
                capture.record(kind, payload)
        record("current_events", {"events": events})
        record("catalog", {"candidates": candidates})
        tick = 0
        def read(phase, rows, **extra):
            nonlocal tick
            tick += 2
            record("history_read", {"read_id": "cursor-one", "source_cursor_name": "cursor-one",
                   "phase": phase, "rows": rows, "started_at_utc": when,
                   "finished_at_utc": when, "started_monotonic_ns": tick,
                   "finished_monotonic_ns": tick + 1, **extra})
        read("metadata_execute", [], query="SELECT actual_metadata", parameters=[], requested_windows=[])
        if prior:
            read("metadata_fetch", metadata, exhausted=False)
            ids = [event["event_id"] for event in prior]
            read("projection", prior, query="SELECT actual_projected", parameters=[ids], requested_event_ids=ids)
        read("metadata_fetch", [], exhausted=True)
        record("history_base", {"pairs": pairs})
        hits = []
        for event in events:
            base = {"captured.value": 2, "captured.liquidity.validity": "UNVERIFIED", "missing": None}
            record("sequence_call_input", {"event_id": event["event_id"], "event": event,
                   "base_features": base, "prior": pairs})
            sequence = {"sequence.ordinal": 2}
            outcomes = [{"candidate_key": "normal", "matched": False, "orientation": "NORMAL",
                         "source_direction": "SHORT", "routed_direction": None},
                        {"candidate_key": "inverse", "matched": True, "orientation": "INVERSE",
                         "source_direction": "SHORT", "routed_direction": "LONG"}]
            record("sequence_call_output", {"event_id": event["event_id"], "sequence_features": sequence,
                   "final_features": {**base, **sequence}, "candidate_outcomes": outcomes})
            hits.append({"candidate_key": "inverse", "event_id": event["event_id"],
                         "source_direction": "SHORT", "routed_direction": "LONG"})
        record("routing", {"rule": "ON CONFLICT(candidate_key,event_id) DO NOTHING",
               "insert_outcome": "UNOBSERVED_INSERT_OR_EXISTING", "candidate_hits": hits})
    observation.committed_at_utc = capture.utc_now()
    return observation


def replace_payload(observation, kind, change):
    index = next(i for i, item in enumerate(observation.collector.events) if item["kind"] == kind)
    payload = graph.recover(observation.collector.package(), observation.collector.events[index]["object_sha256"])
    change(payload)
    observation.closed = False
    observation.record(kind, payload)
    observation.closed = True
    observation.collector.events[index] = observation.collector.events.pop()


class CaptureTests(unittest.TestCase):
    def prepared(self, **kwargs):
        return capture.prepare(fixture(**kwargs))

    def test_complete_scope_and_roundtrip(self):
        prepared = self.prepared()
        package = capture.package_from_observation(prepared)
        self.assertEqual(prepared["manifest"]["summary"]["status"], "CAPTURED")
        self.assertTrue(prepared["manifest"]["actual_consumed_arrays_retained"])
        self.assertFalse(prepared["manifest"]["single_sql_snapshot_proven"])
        self.assertFalse(prepared["manifest"]["original_first_match_observed"])
        inputs = graph.event_payloads(package, "sequence_call_input")
        self.assertEqual(inputs[0][1]["prior"][1][0]["symbol"], "ETH")
        self.assertEqual(len(inputs[0][1]["prior"]), 3)

    def test_unverified_missing_values_remain_complete_capture(self):
        package = capture.package_from_observation(self.prepared())
        output = graph.event_payloads(package, "sequence_call_output")[0][1]
        self.assertIsNone(output["final_features"]["missing"])
        self.assertEqual(output["final_features"]["captured.liquidity.validity"], "UNVERIFIED")

    def test_exact_legacy_float_datetime_hash(self):
        package = capture.package_from_observation(self.prepared())
        value = graph.event_payloads(package, "sequence_call_input")[0][1]
        material = value["legacy_event_input"]
        self.assertIn('"current_price":1.0', material["canonical_json"])
        self.assertIn("13:00:00+02:00", material["canonical_json"])
        self.assertEqual(value["event"]["alert_time_utc"], "2026-10-26T11:00:00+00:00")
        self.assertEqual(material["sha256"], hashlib.sha256(material["canonical_json"].encode()).hexdigest())

    def test_immediate_freeze_and_nested_contexts(self):
        value = {"value": [1]}
        with capture.observe() as outer:
            capture.record("test", value)
            with capture.observe() as inner:
                capture.record("test", {"value": [2]})
            self.assertIs(capture.current(), outer)
            value["value"].append(9)
        self.assertIsNone(capture.current())
        self.assertEqual(graph.event_payloads(outer.collector.package(), "test")[0][1], {"value": [1]})
        self.assertEqual(graph.event_payloads(inner.collector.package(), "test")[0][1], {"value": [2]})

    def test_exception_is_preserved_and_discards(self):
        with self.assertRaisesRegex(RuntimeError, "scoring failed"):
            with capture.observe() as observation:
                raise RuntimeError("scoring failed")
        self.assertTrue(observation.discarded)
        with self.assertRaisesRegex(ValueError, "NOT_COMMITTED"):
            capture.prepare(observation)

    def test_requires_explicit_successful_commit(self):
        observation = fixture()
        observation.committed_at_utc = None
        with self.assertRaisesRegex(ValueError, "NOT_COMMITTED"):
            capture.prepare(observation)

    def test_each_required_kind_omission_is_partial(self):
        for kind in ("current_events", "catalog", "history_read", "history_base", "sequence_call_input", "sequence_call_output", "routing"):
            with self.subTest(kind=kind):
                prepared = self.prepared(omit=(kind,))
                self.assertEqual(prepared["manifest"]["summary"]["status"], "PARTIAL")
                self.assertFalse(prepared["manifest"]["actual_consumed_arrays_retained"])

    def test_duplicate_input_and_incomplete_output_are_partial(self):
        observation = fixture()
        header = next(event for event in observation.collector.events if event["kind"] == "sequence_call_input")
        observation.collector.events.append(deepcopy(header))
        self.assertIn("INPUT_COVERAGE_INCOMPLETE", capture.prepare(observation)["manifest"]["summary"]["gaps"])

    def test_capture_errors_are_partial(self):
        observation = fixture()
        observation.collector.gap("PACKAGE_SIZE_BOUND")
        self.assertEqual(capture.prepare(observation)["manifest"]["summary"]["status"], "PARTIAL")

    def test_exact_prior_membership_order_and_features_are_bound(self):
        for change in (lambda value: value["prior"].reverse(),
                       lambda value: value["prior"].pop(),
                       lambda value: value["prior"][0][1].update({"captured.value": 999})):
            with self.subTest(change=change):
                observation = fixture()
                replace_payload(observation, "sequence_call_input", change)
                self.assertIn("EXACT_PRIOR_POOL_BINDING_MISMATCH", capture.prepare(observation)["manifest"]["summary"]["gaps"])

    def test_output_coverage_merge_and_routing_are_bound(self):
        for change, expected in ((lambda value: value["candidate_outcomes"].pop(), "CANDIDATE_OUTCOMES_INCOMPLETE"),
                                 (lambda value: value["final_features"].update({"sequence.ordinal": 99}), "OUTPUT_MERGE_MISMATCH"),
                                 (lambda value: value["candidate_outcomes"][1].update({"routed_direction": "SHORT"}), "OUTCOME_ROUTING_MISMATCH")):
            with self.subTest(expected=expected):
                observation = fixture()
                replace_payload(observation, "sequence_call_output", change)
                self.assertIn(expected, capture.prepare(observation)["manifest"]["summary"]["gaps"])

    def test_history_source_closure_and_source_identity_required(self):
        observation = fixture()
        replace_payload(observation, "history_base", lambda value: value["pairs"].pop())
        self.assertIn("HISTORY_BASE_PROJECTED_BINDING_MISMATCH", capture.prepare(observation)["manifest"]["summary"]["gaps"])
        observation = fixture()
        replace_payload(observation, "identity", lambda value: value["code_versions"].pop("analysis.py"))
        self.assertIn("SOURCE_IDENTITY_INCOMPLETE", capture.prepare(observation)["manifest"]["summary"]["gaps"])

    def test_failing_freeze_does_not_escape(self):
        with capture.observe() as observation:
            self.assertIsNone(capture.record("bad", {"value": float("nan")}))
        self.assertEqual(observation.collector.errors, [{"code": "NONFINITE_INPUT"}])

    def test_empty_history_is_complete(self):
        self.assertEqual(self.prepared(prior_count=0)["manifest"]["summary"]["status"], "CAPTURED")

    def test_manifest_and_object_tampering_rejected(self):
        prepared = self.prepared()
        altered = deepcopy(prepared)
        altered["manifest"]["single_sql_snapshot_proven"] = True
        altered["root_sha256"] = graph.digest(altered["manifest"])
        with self.assertRaisesRegex(ValueError, "SCOPE"):
            capture.package_from_observation(altered)
        altered = deepcopy(prepared)
        ref = next(iter(altered["objects"]))
        altered["objects"][ref]["compressed_b64"] = base64.b64encode(zlib.compress(b"{}" )).decode()
        with self.assertRaises(ValueError):
            capture.package_from_observation(altered)

    def test_queue_bound_and_no_durable_claim_before_writer(self):
        observation = fixture()
        old = deepcopy(capture._METRICS)
        try:
            with patch.object(capture, "_ensure_worker", return_value=True):
                capture._METRICS["queued_items"] = capture.MAX_QUEUE_ITEMS
                receipt = capture.submit_after_commit(observation)
            self.assertEqual(receipt["status"], "DROPPED")
            self.assertNotIn("capture_status", receipt)
            self.assertFalse(observation.submitted)
        finally:
            capture._METRICS.clear()
            capture._METRICS.update(old)

    def test_no_database_is_explicit_and_no_source_query(self):
        with patch.object(capture, "_ensure_worker", return_value=False):
            receipt = capture.submit_after_commit(fixture())
        self.assertEqual(receipt["reason_code"], "DATABASE_NOT_CONFIGURED")

    def test_queue_insert_failure_restores_accounting(self):
        observation = fixture()
        old = deepcopy(capture._METRICS)
        try:
            capture._METRICS["queued_bytes"] = capture._METRICS["queued_items"] = 0
            with patch.object(capture, "_ensure_worker", return_value=True), patch.object(capture._QUEUE, "put_nowait", side_effect=RuntimeError("test queue failure")):
                receipt = capture.submit_after_commit(observation)
            self.assertEqual(receipt["status"], "FAILED")
            self.assertEqual(capture._METRICS["queued_bytes"], 0)
            self.assertEqual(capture._METRICS["queued_items"], 0)
            self.assertFalse(observation.submitted)
        finally:
            capture._METRICS.clear()
            capture._METRICS.update(old)

    def test_empty_doge_batch_does_not_freeze_following_payloads(self):
        with patch.object(capture, "runtime_identity", side_effect=AssertionError("identity must be deferred")):
            with capture.observe() as observation:
                capture.record("current_events", {"events": []})
                capture.record("catalog", {"invalid_if_normalized": object()})
        self.assertEqual(observation.collector.objects, {})
        self.assertEqual(observation.collector.errors, [])
        self.assertEqual(capture.submit_after_commit(observation)["status"], "NO_CHANGED_DOGE_EVENTS")

    def test_bound_failure_stops_later_freezing(self):
        with capture.observe() as observation:
            capture.record("current_events", {"events": [{"event_id": 1, "symbol": "DOGE"}]})
            observation.collector.max_raw_bytes = observation.collector.raw_bytes
            capture.record("exhaust", {"large": "x" * 1000})
            self.assertTrue(observation.capture_stopped)
            with patch.object(graph, "normalized", side_effect=AssertionError("must stop before normalization")):
                self.assertIsNone(capture.record("sequence_call_input", {"invalid": object()}))
        self.assertIn({"code": "PACKAGE_SIZE_BOUND"}, observation.collector.errors)

    def test_prior_cache_detects_nested_mutations_and_bool_number_changes(self):
        for original, changed in ((1, True), (True, 1), (1.0, 2.0)):
            with self.subTest(original=original, changed=changed):
                pair = ({"event_id": 1, "nested": {"value": original}}, {"x": None})
                with capture.observe() as observation:
                    first = observation._pair_ref(pair)
                    self.assertEqual(first, observation._pair_ref(pair))
                    pair[0]["nested"]["value"] = changed
                    self.assertNotEqual(first, observation._pair_ref(pair))
                self.assertEqual(observation._pair_cache, {})

    def test_database_selection_matches_intake_without_exposing_url(self):
        with patch.dict(os.environ, {"RESEARCH_DATABASE_URL": "dedicated", "DATABASE_URL": "primary", "RESEARCH_USE_PRIMARY_DATABASE": "true"}):
            self.assertEqual(capture.database_url(), "dedicated")
            self.assertNotIn("dedicated", json.dumps(capture.status()))
        with patch.dict(os.environ, {"RESEARCH_DATABASE_URL": "", "DATABASE_URL": "primary", "RESEARCH_USE_PRIMARY_DATABASE": ""}):
            self.assertEqual(capture.database_url(), "")
        with patch.dict(os.environ, {"RESEARCH_DATABASE_URL": "", "DATABASE_URL": "primary", "RESEARCH_USE_PRIMARY_DATABASE": "yes"}):
            self.assertEqual(capture.database_url(), "primary")

    def test_dense_shared_history_does_not_duplicate_pair_nodes(self):
        observation = fixture(count=128, prior_count=512)
        prepared = capture.prepare(observation)
        self.assertEqual(prepared["manifest"]["summary"]["status"], "CAPTURED")
        self.assertLess(len(prepared["objects"]), 2000)
        self.assertLess(sum(item["raw_bytes"] for item in prepared["objects"].values()), 2 * 1024 * 1024)

    def test_store_dedup_and_immutable_conflicts_atomic(self):
        with tempfile.TemporaryDirectory(prefix="doge-sequence-selftest-") as directory:
            path = Path(directory) / "evidence.sqlite"
            store = capture.SQLiteStore(path)
            prepared = self.prepared()
            store.persist(prepared)
            store.persist(prepared)
            self.assertEqual(store.read(prepared["observation_id"]), prepared)
            with closing(sqlite3.connect(path)) as conn:
                before = conn.execute("SELECT COUNT(*) FROM research_consumed_input_objects").fetchone()[0]
            conflict = self.prepared(prior_count=4)
            conflict["observation_id"] = prepared["observation_id"]
            conflict["manifest"]["observation_id"] = prepared["observation_id"]
            conflict["root_sha256"] = graph.digest(conflict["manifest"])
            with self.assertRaisesRegex(ValueError, "CONFLICT"):
                store.persist(conflict)
            with closing(sqlite3.connect(path)) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM research_consumed_input_objects").fetchone()[0], before)

    def test_valid_alternative_compression_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="doge-sequence-selftest-") as directory:
            path = Path(directory) / "evidence.sqlite"
            store, prepared = capture.SQLiteStore(path), self.prepared()
            store.persist(prepared)
            ref, item = next(iter(prepared["objects"].items()))
            packed = zlib.compress(zlib.decompress(base64.b64decode(item["compressed_b64"])), 9)
            with closing(sqlite3.connect(path)) as conn, conn:
                conn.execute("UPDATE research_consumed_input_objects SET payload=? WHERE sha256=?", (packed, ref))
            store.persist(prepared)
            with closing(sqlite3.connect(path)) as conn:
                self.assertEqual(conn.execute("SELECT payload FROM research_consumed_input_objects WHERE sha256=?", (ref,)).fetchone()[0], packed)

    def test_corrupt_retained_object_never_replaced(self):
        with tempfile.TemporaryDirectory(prefix="doge-sequence-selftest-") as directory:
            path = Path(directory) / "evidence.sqlite"
            store, prepared = capture.SQLiteStore(path), self.prepared()
            store.persist(prepared)
            ref = next(iter(prepared["objects"]))
            with closing(sqlite3.connect(path)) as conn, conn:
                conn.execute("UPDATE research_consumed_input_objects SET payload=? WHERE sha256=?", (b"corrupt", ref))
            with self.assertRaises(Exception):
                store.persist(prepared)
            with closing(sqlite3.connect(path)) as conn:
                self.assertEqual(conn.execute("SELECT payload FROM research_consumed_input_objects WHERE sha256=?", (ref,)).fetchone()[0], b"corrupt")


@unittest.skipUnless(os.getenv("RESEARCH_TEST_POSTGRES_URL"), "explicit isolated test PostgreSQL DSN not configured")
class PostgresTests(unittest.TestCase):
    def test_real_postgres_atomic_graph_and_schema_scope(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        url = os.environ["RESEARCH_TEST_POSTGRES_URL"]
        schema = "doge_sequence_test_" + uuid4().hex
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE SCHEMA {}" ).format(sql.Identifier(schema)))
        options = conninfo_to_dict(url).get("options", "") + " -c search_path=" + schema
        isolated = make_conninfo(url, options=options.strip())
        try:
            store, prepared = capture.PostgresStore(isolated), capture.prepare(fixture())
            store.persist(prepared)
            store.persist(prepared)
            self.assertEqual(store.read(prepared["observation_id"]), prepared)
            with psycopg.connect(isolated) as conn:
                self.assertEqual(conn.execute("SELECT current_schema()").fetchone()[0], schema)
                before = conn.execute("SELECT COUNT(*) FROM research_consumed_input_objects").fetchone()[0]
            conflict = capture.prepare(fixture(prior_count=4))
            conflict["observation_id"] = prepared["observation_id"]
            conflict["manifest"]["observation_id"] = prepared["observation_id"]
            conflict["root_sha256"] = graph.digest(conflict["manifest"])
            with self.assertRaisesRegex(ValueError, "CONFLICT"):
                store.persist(conflict)
            with psycopg.connect(isolated) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM research_consumed_input_objects").fetchone()[0], before)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM research_doge_sequence_observations").fetchone()[0], 1)
        finally:
            with psycopg.connect(url, autocommit=True) as admin:
                admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
