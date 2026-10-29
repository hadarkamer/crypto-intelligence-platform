"""Independent invariants for the immutable observational capture store."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

import research_consumed_inputs as capture


def complete_collector():
    collector = capture.Collector()
    for symbol in capture.EXPECTED_SYMBOLS:
        for market in ("futures", "spot"):
            for kind in ("flow_rows", "flow_analysis"):
                collector.record(kind, {"arguments": {"symbol": symbol, "market": market}, "result": []})
        for kind in ("flow_cached_generation", "oi_live_history", "oi_window_results", "oi_regime", "oi_baseline_rows", "oi_baseline_calculation"):
            collector.record(kind, {"arguments": {"symbol": symbol}, "result": {}})
        for minutes, label in ((30, "30m"), (60, "1h"), (240, "4h"), (720, "12h"), (1440, "24h"), (2880, "48h"), (4320, "72h"), (10080, "7d")):
            collector.record("oi_reference_selection", {"arguments": {"symbol": symbol, "minutes": minutes}, "result": {}})
            collector.record("oi_window_reference", {"arguments": {"symbol": symbol, "label": label}, "result": {}})
            collector.record("oi_historical_nearest", {"arguments": {"symbol": symbol, "target_time": str(minutes)}, "result": {}})
    return collector


def watch_score(cycle_id, rows, snapshot):
    score = {"version": "watch-operational-scores-v2", "cycle_id": cycle_id, "status": "COMPLETE",
             "input_universe_sha256": capture.digest(capture.operational_hash_material(rows)["value"]),
             "input_row_count": len(rows), "coins": {symbol: {"status": "CAPTURED", "sources": {
                 "derivatives_snapshot_sha256": capture.digest(capture.operational_hash_material(snapshot[symbol])["value"])}}
                 for symbol in capture.EXPECTED_SYMBOLS}}
    score["payload_sha256"] = capture.digest(score)
    return score


def cycle(collector=None, cycle_id="test-cycle", rows=None):
    rows = rows or [{"symbol": "ETH", "price": 10.0}]
    snapshot = {"ETH": {"regime": {}}, "DOGE": {"regime": {}}}
    return capture.prepare_cycle(cycle_id, collector or complete_collector(), rows=rows,
            snapshot=snapshot, score_block=watch_score(cycle_id, rows, snapshot),
            code_versions={name: "a" * 64 for name in capture.CODE_FILES})


class CaptureTests(unittest.TestCase):
    def test_bounded_encoding_preserves_existing_object_hashes(self):
        # Frozen identities from the deployed encoder, including UTF-8 and
        # oversized-parent chunking. Root hashes bind every child object.
        cases = [
            ({"text": "שלום 🌍", "numbers": [1.0, -0.0, 1e-20, True]},
             "c6bc5f32b57537938ab02794186999e664dd4359dc75cbb9fdcb0d70fa328f31"),
            ({"samples": [[i / 7, i % 2] for i in range(65000)]},
             "519e626f570564f6cd0e29ff4f25b4df568cc86382799121a05f1fba59f6943e"),
            (["x" * 270000, "y" * 270000],
             "aa74abd30799d5d35041af42db1ee9540fa8349c9883ca56acddc30f3ae8a422"),
        ]
        for value, expected in cases:
            collector = capture.Collector()
            ref = collector.record("input", value)
            self.assertEqual(ref, expected)
            self.assertEqual(capture.recover(collector.package(), ref), capture.normalized(value))
            self.assertFalse(collector.errors)
        collector = capture.Collector()
        self.assertIsNone(collector.record("input", "x" * capture.MAX_OBJECT_BYTES))
        self.assertEqual(collector.errors, [{"code": "OVERSIZED_SCALAR"}])

    def test_normalization_preserves_numeric_values_and_utc(self):
        value = {"zero": -0.0, "integral": 2.0, "fraction": 2.25,
                 "at": datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2)))}
        self.assertEqual(capture.normalized(value), {"zero": 0, "integral": 2, "fraction": 2.25,
                                                   "at": "2025-12-31T22:00:00+00:00"})
        self.assertEqual(capture.digest({"a": 0.0}), capture.digest({"a": 0}))

    def test_unsupported_or_nonfinite_inputs_are_explicit_gaps(self):
        for bad in (float("nan"), float("inf"), datetime(2026, 1, 1), object(), {2: "wrong"}):
            collector = capture.Collector()
            self.assertIsNone(collector.record("bad", bad))
            self.assertTrue(collector.package()["errors"])

    def test_context_is_nested_and_no_active_capture_is_noop(self):
        self.assertIsNone(capture.record("outside", {}))
        with capture.collect() as parent:
            capture.record("parent", {"n": 1})
            with capture.collect() as child:
                capture.record("child", {"n": 2})
            self.assertEqual([e["kind"] for e in parent.events], ["parent"])
            self.assertTrue(capture.merge(child.package(), {"generation": "g"}))
            self.assertEqual([e["kind"] for e in parent.events], ["parent", "child", "generation_use"])
        self.assertIsNone(capture.current())

    def test_safe_collection_preserves_operational_body_failures(self):
        with self.assertRaisesRegex(RuntimeError, "operational failure"):
            with capture.safe_collect():
                raise RuntimeError("operational failure")
        with patch.object(capture, "Collector", side_effect=RuntimeError("observer setup")):
            with capture.safe_collect() as collector:
                self.assertIsNone(collector)
        missing = capture.prepare_cycle("no-context", None, rows=[], snapshot={}, score_block={})
        self.assertEqual(missing["manifest"]["summary"]["status"], "PARTIAL")

    def test_input_mutation_cannot_rewrite_retained_object(self):
        collector = capture.Collector()
        value = [{"price": 2.5}]
        ref = collector.record("rows", value)
        value[0]["price"] = 99
        package = collector.package()
        self.assertEqual(capture.recover(package, ref), [{"price": 2.5}])
        package["events"][0]["kind"] = "changed"
        self.assertEqual(collector.events[0]["kind"], "rows")

    def test_repeated_payload_is_deduplicated_and_changed_row_is_new(self):
        collector = capture.Collector()
        first = collector.record("rows", [{"candle": "x", "oi": 1}])
        same = collector.record("rows", [{"oi": 1.0, "candle": "x"}])
        changed = collector.record("rows", [{"candle": "x", "oi": 2}])
        self.assertEqual(first, same)
        self.assertNotEqual(first, changed)
        self.assertEqual(len(collector.objects), 2)

    def test_chunked_rows_are_recoverable_and_stable_after_prefix_purge(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"time": (base + timedelta(hours=i)).isoformat(), "body": "x" * 900, "n": i}
                for i in range(1000)]
        first, second = capture.Collector(), capture.Collector()
        ref = first.record("rows", rows)
        second.record("rows", rows[1:])
        self.assertEqual(capture.recover(first.package(), ref), rows)
        # Only first day plus the ordered manifest can differ after one purge.
        unchanged = set(first.objects) & set(second.objects)
        self.assertGreater(len(unchanged), 35)
        self.assertLess(len(set(first.objects) - set(second.objects)), 5)

    def test_large_derived_arrays_preserve_exact_order(self):
        values = [[i / 7, i % 2] for i in range(65000)]
        collector = capture.Collector()
        ref = collector.record("derived", {"samples": values, "name": "baseline"})
        self.assertIsNotNone(ref)
        self.assertEqual(capture.recover(collector.package(), ref)["samples"], values)
        self.assertGreater(len(collector.objects), 10)

    def test_record_bound_leaves_gap_and_does_not_raise(self):
        collector = capture.Collector(max_compressed_bytes=5)
        self.assertIsNone(collector.record("large", "not compressible enough"))
        self.assertEqual(collector.errors, [{"code": "PACKAGE_SIZE_BOUND"}])

    def test_merge_is_bounded_and_does_not_alias_producer_package(self):
        source = capture.Collector()
        ref = source.record("a", {"n": 1})
        package = source.package()
        dest = capture.Collector()
        self.assertTrue(dest.merge(package))
        package["objects"][ref]["codec"] = "corrupted"
        self.assertEqual(capture.recover(dest.package(), ref), {"n": 1})
        bounded = capture.Collector(max_objects=0)
        self.assertFalse(bounded.merge(source.package()))
        self.assertFalse(bounded.objects)
        self.assertTrue(bounded.errors)

    def test_repeated_generation_records_consumption_edges_once_per_use(self):
        source = capture.Collector()
        source.record("baseline", {"samples": list(range(1000))})
        package = source.package()
        dest = capture.Collector()
        for i in range(8):
            self.assertTrue(dest.merge(package, {"used_window": i}))
        self.assertEqual(sum(e["kind"] == "baseline" for e in dest.events), 1)
        self.assertEqual(sum(e["kind"] == "generation_use" for e in dest.events), 8)
        bad = deepcopy(package)
        ref = next(iter(bad["objects"]))
        bad["objects"][ref]["compressed_b64"] = base64.b64encode(b"bad").decode()
        self.assertFalse(dest.merge(bad))
        self.assertTrue(dest.errors)

    def test_fast_path_seal_detects_changed_argument_headers_and_gap_markers(self):
        source = capture.Collector()
        source.record("source", {"arguments": {"symbol": "DOGE"}, "result": []})
        package = source.package()
        package["events"][0]["arguments"]["symbol"] = "ETH"
        dest = capture.Collector()
        self.assertFalse(dest.merge(package))
        self.assertIn({"code": "EVENT_ARGUMENT_HEADER_MISMATCH"}, dest.errors)
        source.record("capture_gap", {"reason": "missing generation"})
        package = source.package()
        package["errors"] = []
        with self.assertRaisesRegex(ValueError, "UNDECLARED"):
            capture.validate(package)

    def test_invalid_hash_missing_reference_and_clock_rejected(self):
        collector = capture.Collector()
        ref = collector.record("a", {"n": 1})
        package = collector.package()
        bad = deepcopy(package)
        raw = capture.canonical({"node": "value", "value": {"n": 2}}).encode()
        bad["objects"][ref]["compressed_b64"] = base64.b64encode(zlib.compress(raw)).decode()
        bad["objects"][ref]["raw_bytes"] = len(raw)
        with self.assertRaisesRegex(ValueError, "DIGEST"):
            capture.validate(bad)
        missing = deepcopy(package)
        missing["events"][0]["object_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "REFERENCE"):
            capture.validate(missing)
        clock = deepcopy(package)
        clock["events"][0]["recorded_at_utc"] = "2026-01-01T00:00:00"
        with self.assertRaisesRegex(ValueError, "CLOCK"):
            capture.validate(clock)

    def test_compressed_bomb_is_bounded(self):
        raw = b"x" * (capture.MAX_OBJECT_BYTES + 1)
        package = {"version": capture.VERSION, "hash_version": capture.HASH_VERSION, "events": [], "errors": [],
                   "objects": {hashlib.sha256(raw).hexdigest(): {"codec": capture.CODEC,
                       "raw_bytes": len(raw), "compressed_b64": base64.b64encode(zlib.compress(raw)).decode()}}}
        with self.assertRaisesRegex(ValueError, "SIZE"):
            capture.validate(package)

    def test_complete_and_partial_coverage_are_distinct(self):
        complete = cycle()
        self.assertEqual(complete["manifest"]["summary"]["status"], "CAPTURED")
        self.assertTrue(complete["manifest"]["producer_coverage"]["complete"])
        partial = cycle(capture.Collector())
        self.assertEqual(partial["manifest"]["summary"]["status"], "PARTIAL")
        self.assertFalse(partial["manifest"]["producer_coverage"]["complete"])
        self.assertFalse(complete["manifest"]["capture_scope"]["provider_http_payloads"])

    def test_producer_gap_marks_partial_even_when_coverage_exists(self):
        collector = complete_collector()
        collector.record("capture_gap", {"reason": "missing original generation"})
        self.assertEqual(cycle(collector)["manifest"]["summary"]["status"], "PARTIAL")

    def test_watch_score_failures_cannot_be_complete_input_capture(self):
        rows, snapshot = [{"symbol": "ETH"}], {"ETH": {"regime": {}}, "DOGE": {"regime": {}}}
        original = watch_score("watch", rows, snapshot)
        for change, expected in (({"status": "FAILED"}, "WATCH_SCORE_NOT_COMPLETE"),
                                 ({"version": "wrong"}, "WATCH_SCORE_VERSION"),
                                 ({"cycle_id": "old-watch"}, "WATCH_SCORE_CYCLE_MISMATCH"),
                                 ({"input_row_count": 999}, "WATCH_SCORE_INPUT_ROW_COUNT"),
                                 ({"input_universe_sha256": "0" * 64}, "WATCH_SCORE_INPUT_UNIVERSE"),
                                 ({"payload_sha256": "0" * 64}, "WATCH_SCORE_PAYLOAD_SHA256")):
            with self.subTest(change=change):
                score = deepcopy(original)
                score.update(change)
                result = capture.prepare_cycle("watch", complete_collector(), rows=rows, snapshot=snapshot,
                         score_block=score, code_versions={name: "a" * 64 for name in capture.CODE_FILES})
                self.assertEqual(result["manifest"]["summary"]["status"], "PARTIAL")
                self.assertIn(expected, {failure["code"] for failure in result["manifest"]["watch_binding"]["failures"]})

    def test_missing_or_mismatched_consumed_snapshot_is_partial(self):
        rows, snapshot = [], {"ETH": {"regime": {}}, "DOGE": {"regime": {}}}
        score = watch_score("watch", rows, snapshot)
        for bad in ({"ETH": snapshot["ETH"]}, {"ETH": {"regime": {"changed": True}}, "DOGE": snapshot["DOGE"]}):
            result = capture.prepare_cycle("watch", complete_collector(), rows=rows, snapshot=bad,
                     score_block=score, code_versions={name: "a" * 64 for name in capture.CODE_FILES})
            self.assertEqual(result["manifest"]["summary"]["status"], "PARTIAL")
            self.assertIn("WATCH_DERIVATIVE_SNAPSHOT_BINDING", {failure["code"] for failure in result["manifest"]["watch_binding"]["failures"]})

    def test_postgres_datetime_legacy_hash_preimage_has_exact_typed_bridge(self):
        at = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))
        rows = [{"symbol": "ETH", "fetched_at": at, "ordinary_string": "2026-01-01 12:00:00+02:00"}]
        snapshot = {"ETH": {"regime": {"price_fetched_at": at}}, "DOGE": {"regime": {}}}
        result = capture.prepare_cycle("watch", complete_collector(), rows=rows, snapshot=snapshot,
                 score_block=watch_score("watch", rows, snapshot), code_versions={name: "a" * 64 for name in capture.CODE_FILES})
        self.assertEqual(result["manifest"]["summary"]["status"], "CAPTURED")
        package = capture.package_from_cycle(result)
        material = capture.event_payloads(package, "watch_operational_hash_material")[0][1]
        legacy = material["value"]
        self.assertEqual(legacy["snapshot_by_symbol"]["ETH"]["regime"]["price_fetched_at"], "2026-01-01 12:00:00+02:00")
        self.assertEqual(len(material["datetime_paths"]), 2)
        self.assertEqual(material["datetime_paths"][0]["source_text"], str(at))
        self.assertEqual(material["datetime_paths"][0]["normalized_text"], "2026-01-01T10:00:00+00:00")
        self.assertEqual(capture.event_payloads(package, "watch_input_universe")[0][1][0]["ordinary_string"], str(at))
        self.assertEqual(capture.event_payloads(package, "watch_normalized_input_universe")[0][1][0]["fetched_at"], "2026-01-01T10:00:00+00:00")

    def test_manifest_identity_and_membership_are_verified(self):
        original = cycle()
        bad = deepcopy(original)
        bad["manifest"]["cycle_id"] = "different"
        with self.assertRaisesRegex(ValueError, "DIGEST"):
            capture.package_from_cycle(bad)
        bad = deepcopy(original)
        bad["objects"].pop(next(iter(bad["objects"])))
        with self.assertRaisesRegex(ValueError, "MEMBERSHIP"):
            capture.package_from_cycle(bad)

    def test_missing_code_pin_is_explicit_and_manifest_claims_are_checked(self):
        missing = capture.prepare_cycle("missing-code", complete_collector(), rows=[], snapshot={},
                                        score_block={}, code_versions={"test": "a" * 64})
        self.assertEqual(missing["manifest"]["summary"]["status"], "PARTIAL")
        self.assertIn({"code": "MISSING_CODE_IDENTITY"}, missing["manifest"]["errors"])
        forged = deepcopy(missing)
        forged["manifest"]["summary"]["status"] = "CAPTURED"
        forged["root_sha256"] = capture.digest(forged["manifest"])
        with self.assertRaisesRegex(ValueError, "SUMMARY"):
            capture.package_from_cycle(forged)

    def test_sqlite_roundtrip_is_exact_and_retry_does_not_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = capture.SQLiteStore(Path(directory) / "capture.sqlite")
            original = cycle()
            store.persist(original)
            store.persist(original)
            loaded = store.read_cycle(original["cycle_id"])
            self.assertEqual(loaded, original)
            self.assertEqual(capture.package_from_cycle(loaded), capture.package_from_cycle(original))
            with closing(sqlite3.connect(store.path)) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM research_consumed_input_cycles").fetchone()[0], 1)

    def test_alternate_valid_compression_preserves_retained_representation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = capture.SQLiteStore(Path(directory) / "capture.sqlite")
            original = cycle(rows=[{"n": list(range(1000))}])
            store.persist(original)
            alternative = deepcopy(original)
            for item in alternative["objects"].values():
                raw = zlib.decompress(base64.b64decode(item["compressed_b64"]))
                item["compressed_b64"] = base64.b64encode(zlib.compress(raw, 9)).decode("ascii")
            store.persist(alternative)
            self.assertEqual(store.read_cycle(original["cycle_id"]), original)

    def test_conflicting_cycle_rolls_back_new_objects(self):
        with tempfile.TemporaryDirectory() as directory:
            store = capture.SQLiteStore(Path(directory) / "capture.sqlite")
            original = cycle()
            store.persist(original)
            changed = cycle(rows=[{"new": "not in original"}])
            with self.assertRaisesRegex(ValueError, "CYCLE_CONFLICT"):
                store.persist(changed)
            with closing(sqlite3.connect(store.path)) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM research_consumed_input_objects").fetchone()[0], len(original["objects"]))
            self.assertEqual(store.read_cycle(original["cycle_id"]), original)

    def test_corrupt_stored_object_is_detected_before_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            store = capture.SQLiteStore(Path(directory) / "capture.sqlite")
            original = cycle()
            store.persist(original)
            with closing(sqlite3.connect(store.path)) as conn, conn:
                conn.execute("UPDATE research_consumed_input_objects SET payload=? WHERE sha256=?",
                             (b"invalid", next(iter(original["objects"]))))
            with self.assertRaises(Exception):
                store.read_cycle(original["cycle_id"])

    def test_missing_database_is_explicit_and_fail_open(self):
        with patch.object(capture, "_ensure_worker", return_value=False):
            result = capture.submit("test", complete_collector(), rows=[], snapshot={}, score_block={}, code_versions={"test": "a" * 64})
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(result["reason_code"], "DATABASE_NOT_CONFIGURED")
        self.assertNotIn("DATABASE_URL", capture.canonical(capture.status()))

    def test_failed_persist_releases_queue_budget_and_reports_failure(self):
        class Failing:
            def persist(self, value):
                raise RuntimeError("secret database string must not be reported")
        saved = deepcopy(capture._METRICS)
        try:
            with capture._METRICS_LOCK:
                capture._METRICS["queued_bytes"] = 100
                capture._METRICS["queued_items"] = 1
            with patch.object(capture, "_STORE", Failing()):
                capture._persist_one(cycle(), 100)
            self.assertEqual(capture.status()["queued_bytes"], 0)
            self.assertEqual(capture.status()["queued_items"], 0)
            self.assertEqual(capture.status()["last"]["reason_code"], "RuntimeError")
            self.assertNotIn("secret", capture.canonical(capture.status()))
        finally:
            with capture._METRICS_LOCK:
                capture._METRICS.clear()
                capture._METRICS.update(saved)

    def test_queue_bound_is_explicit_without_synchronous_persistence(self):
        saved = deepcopy(capture._METRICS)
        try:
            with capture._METRICS_LOCK:
                capture._METRICS["queued_items"] = capture.MAX_QUEUE_ITEMS
            with patch.object(capture, "_ensure_worker", return_value=True):
                result = capture.submit("bounded", complete_collector(), rows=[], snapshot={}, score_block={}, code_versions={"test": "a" * 64})
            self.assertEqual(result["status"], "DROPPED")
            self.assertEqual(result["reason_code"], "QUEUE_BOUND")
        finally:
            with capture._METRICS_LOCK:
                capture._METRICS.clear()
                capture._METRICS.update(saved)


if __name__ == "__main__":
    unittest.main()
