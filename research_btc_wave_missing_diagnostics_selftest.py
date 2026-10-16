"""Frozen-boundary and evidence-preservation tests for the local diagnostic tool."""
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import research_btc_wave_missing_diagnostics as diagnostic

GENERATION = "a347edfb-f296-458f-9a3d-844f3bb02ef9"
PARENT = "993ce44640aded6ecd91e0d4646c2b851f0db8031d364d3cf51640bb2a529766"


def record(**updates):
    row = {
        "method_version": diagnostic.METHOD_VERSION,
        "source_event_id": 141769, "btc_parent_movement_id": PARENT,
        "source_scope": diagnostic.SPOT, "coin_scope": "SOL", "symbol": "SOL",
        "direction": "SHORT", "status": "DATA_MISSING",
        "original_alert_time_utc": "2026-09-23T11:35:02.968731+00:00",
        "wave_end_utc": "2026-09-23T11:35:59.999000+00:00",
        "observed_at_utc": "2026-10-16T12:00:00+00:00", "observation_closed": True,
        "missing_reasons": ["ValueError: NO_CLOSED_POST_ENTRY_CANDLES"],
        "path_complete": False, "observed_prefix_complete": False,
        "mfe_pct": None, "mae_pct": None, "thresholds": [],
    }
    row.update(updates)
    return row


def payload(*rows):
    return {"generation_id": GENERATION, "records": list(rows)}


def inspect(row):
    return diagnostic.diagnose(payload(row))["groups"][0]["scope_records"][0]


class MissingDiagnosticsTests(unittest.TestCase):
    def test_exact_sol_closed_zero_window(self):
        value = inspect(record())
        self.assertEqual(value["diagnosis"], "CLOSED_WAVE_HAS_NO_FULL_POST_ENTRY_MINUTE")
        self.assertEqual(value["expected_complete_candles"], 0)
        self.assertEqual(value["first_admissible_open_utc"], "2026-09-23T11:36:00+00:00")
        self.assertEqual(value["first_admissible_close_utc"], "2026-09-23T11:36:59.999000+00:00")
        self.assertEqual(value["original_status"], "DATA_MISSING")

    def test_spot_exact_minute_and_microsecond_later(self):
        exact = inspect(record(original_alert_time_utc="2026-09-23T11:35:00Z"))
        later = inspect(record(original_alert_time_utc="2026-09-23T11:35:00.000001Z"))
        self.assertEqual(exact["expected_complete_candles"], 1)
        self.assertEqual(exact["diagnosis"], "SOURCE_PATH_REQUIRES_REVIEW")
        self.assertEqual(later["expected_complete_candles"], 0)

    def test_inclusive_close_and_one_microsecond_before(self):
        exact = inspect(record(wave_end_utc="2026-09-23T11:36:59.999000Z"))
        before = inspect(record(wave_end_utc="2026-09-23T11:36:59.998999Z"))
        self.assertEqual(exact["expected_complete_candles"], 1)
        self.assertEqual(before["expected_complete_candles"], 0)
        self.assertEqual(before["diagnosis"], "CLOSED_WAVE_HAS_NO_FULL_POST_ENTRY_MINUTE")

    def test_closed_flag_does_not_prove_permanent_zero_at_same_observation_close(self):
        value = inspect(record(wave_end_utc="2026-09-23T11:36:59.999Z",
                               observed_at_utc="2026-09-23T11:36:59.999Z"))
        self.assertEqual(value["expected_complete_candles"], 0)
        self.assertEqual(value["expected_complete_candles_at_wave_end"], 1)
        self.assertEqual(value["diagnosis"], "SOURCE_PATH_REQUIRES_REVIEW")

    def test_active_verified_boundary_and_absent_boundary(self):
        row = record(wave_end_utc=None, observation_closed=False)
        unknown = inspect(row)
        self.assertEqual(unknown["diagnosis"], "INSUFFICIENT_BOUNDARY_EVIDENCE")
        self.assertIsNone(unknown["observation_cutoff_utc"])
        self.assertIsNone(unknown["expected_complete_candles"])
        verified = inspect({**row, "btc_parent_observed_through_utc": "2026-09-23T11:35:59.999Z"})
        self.assertEqual(verified["diagnosis"], "AWAITING_FIRST_COMPLETE_POST_ENTRY_MINUTE")
        self.assertEqual(verified["expected_complete_candles"], 0)
        early = inspect({**row, "observed_at_utc": "2026-09-23T11:35:30Z"})
        self.assertEqual(early["diagnosis"], "AWAITING_FIRST_COMPLETE_POST_ENTRY_MINUTE")
        self.assertIsNone(early["expected_complete_candles"])
        self.assertEqual(early["wall_clock_upper_bound_candles"], 0)

    def test_derived_exact_minute_uses_following_minute(self):
        row = record(symbol="HYPE", coin_scope="HYPE", source_scope=diagnostic.PERP,
                     original_alert_time_utc="2026-09-23T11:35:00Z")
        value = inspect(row)
        self.assertEqual(value["first_admissible_open_utc"], "2026-09-23T11:36:00+00:00")
        self.assertEqual(value["expected_complete_candles_at_wave_end"], 0)

    def test_positive_window_only_reports_observed_symptom(self):
        reasons = ["INCOMPLETE_CLOSED_1M_PATH", "PermissionError: denied",
                   "CANONICAL_BTC_MEMBERSHIP_MISSING", "Wrong price route"]
        for reason in reasons:
            with self.subTest(reason=reason):
                value = inspect(record(wave_end_utc="2026-09-23T12:00:59.999Z", missing_reasons=[reason]))
                self.assertGreater(value["expected_complete_candles"], 0)
                self.assertEqual(value["diagnosis"], "SOURCE_PATH_REQUIRES_REVIEW")
                self.assertEqual(value["original_missing_reasons"], [reason])
        hype = inspect(record(symbol="HYPE", coin_scope="HYPE", source_scope=diagnostic.PERP,
            wave_end_utc="2026-09-23T12:00:59.999Z",
            missing_reasons=["ValueError: derived PERP TRADE entry must be a finite number"]))
        self.assertEqual(hype["diagnosis"], "DERIVED_ENTRY_UNAVAILABLE_IN_REPORT")
        self.assertIn("another stored measurement", hype["explanation"])
        unknown = inspect(record(symbol="HYPE", coin_scope="HYPE", source_scope=diagnostic.PERP,
            wave_end_utc="2026-09-23T12:00:59.999Z", missing_reasons=["Unknown entry error"]))
        self.assertEqual(unknown["diagnosis"], "SOURCE_PATH_REQUIRES_REVIEW")

    def test_offset_timestamps_are_equivalent(self):
        utc = inspect(record())
        offset = inspect(record(original_alert_time_utc="2026-09-23T14:35:02.968731+03:00",
                                wave_end_utc="2026-09-23T14:35:59.999+03:00"))
        self.assertEqual(utc, offset)

    def test_grouping_preserves_each_scope_and_duplicate_row_and_input(self):
        mixed = "SPOT_WITH_DERIVED_HYPE_PERP_V1"
        rows = [record(), record(coin_scope="ALL"),
                record(source_scope=mixed, component_source_scope=diagnostic.SPOT), record()]
        original = payload(*rows)
        frozen = deepcopy(original)
        before = json.dumps(original, sort_keys=True)
        result = diagnostic.diagnose(original)
        self.assertEqual(result["input_record_count"], 4)
        self.assertEqual(result["missing_record_count"], 4)
        self.assertEqual(result["diagnostic_group_count"], 1)
        scoped = result["groups"][0]["scope_records"]
        self.assertEqual([row["record_index"] for row in scoped], [0, 1, 2, 3])
        self.assertEqual([row["coin_scope"] for row in scoped], ["SOL", "ALL", "SOL", "SOL"])
        self.assertEqual([row["source_scope"] for row in scoped], [diagnostic.SPOT, diagnostic.SPOT, mixed, diagnostic.SPOT])
        self.assertEqual(original, frozen)
        self.assertEqual(json.dumps(original, sort_keys=True), before)
        scoped[0]["original_missing_reasons"].append("external edit")
        self.assertEqual(original, frozen)

    def test_invalid_records_fail_with_original_index(self):
        invalid = [
            {"original_alert_time_utc": "2026-09-23T11:35:02"},
            {"observed_at_utc": "not a timestamp"},
            {"observed_at_utc": "2026-09-22T00:00:00Z"},
            {"wave_end_utc": "2026-09-23T11:34:59.999Z"},
            {"observation_closed": False}, {"source_event_id": True},
            {"expected_candles": True}, {"expected_candles": 1},
            {"observation_cutoff_utc": "2026-10-16T12:00:59.999Z"},
            {"measurement_start_utc": "2026-09-23T11:36:00Z"},
            {"entry_version": "another-entry"}, {"missing_reasons": "missing"},
            {"method_version": "another-method"},
            {"source_scope": "SPOT_WITH_DERIVED_HYPE_PERP_V1"},
            {"source_scope": "SPOT_WITH_DERIVED_HYPE_PERP_V1", "symbol": "HYPE",
             "component_source_scope": diagnostic.SPOT},
            {"source_scope": "SPOT_WITH_DERIVED_HYPE_PERP_V1",
             "component_source_scope": diagnostic.PERP},
            {"mfe_pct": float("inf")},
        ]
        for update in invalid:
            with self.subTest(update=update), self.assertRaises(ValueError):
                diagnostic.diagnose(payload(record(), record(**update)))
        with self.assertRaisesRegex(ValueError, r"records\[1\]"):
            diagnostic.diagnose(payload(record(), {"status": "DATA_MISSING"}))
        for bad in ([], {}, {"generation_id": "invalid", "records": []},
                    {"generation_id": GENERATION, "records": {}}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                diagnostic.diagnose(bad)

    def test_empty_and_nonmissing_records_are_explicit(self):
        self.assertEqual(diagnostic.diagnose(payload())["groups"], [])
        ready = record(status="READY", missing_reasons=[],
                       original_alert_time_utc="2026-09-23T11:35:00Z")
        result = diagnostic.diagnose(payload(ready))
        self.assertEqual(result["input_record_count"], 1)
        self.assertEqual(result["missing_record_count"], 0)
        self.assertEqual(result["groups"], [])

    def test_cli_hash_no_overwrite_and_strict_json(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory)/"input.json", Path(directory)/"diagnostics.json"
            raw = json.dumps(payload(record())).encode()
            source.write_bytes(raw)
            self.assertEqual(diagnostic.main(["--input", str(source), "--output", str(target)]), 0)
            output = json.loads(target.read_text())
            self.assertEqual(output["input_file_sha256"], hashlib.sha256(raw).hexdigest())
            with redirect_stderr(io.StringIO()):
                self.assertEqual(diagnostic.main(["--input", str(source), "--output", str(source)]), 2)
                self.assertEqual(diagnostic.main(["--input", str(source), "--output", str(target)]), 2)
            self.assertEqual(source.read_bytes(), raw)
            for bad in ('{"generation_id":"x","generation_id":"y","records":[]}',
                        '{"value":NaN}', '{"value":1e999}', '{broken'):
                source.write_text(bad)
                with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()) as stdout:
                    self.assertEqual(diagnostic.main(["--input", str(source)]), 2)
                    self.assertEqual(stdout.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
