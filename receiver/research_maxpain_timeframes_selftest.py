"""Frozen per-timeframe capture: same calculation, source sides, bounded payloads."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import patch

import alert_engine
import research_event_capture as capture
import research_event_runtime as runtime


def _rows(symbol="BTC"):
    return [
        {
            "symbol": symbol, "timeframe": timeframe, "rank": 1,
            "current_price": 100.0,
            "long_max_pain": 99.1 - index * 0.01,
            "short_max_pain": 101.4 + index * 0.01,
            "distance_long_pct": 0.9 + index * 0.01,
            "distance_short_pct": 1.4 + index * 0.01,
            "long_liquidation_amount": 1_000.0 * (index + 1),
            "short_liquidation_amount": 700.0 * (index + 1),
        }
        for index, timeframe in enumerate(alert_engine.TIMEFRAMES)
    ]


class FrozenTimeframesTests(unittest.TestCase):
    def test_every_candidate_frozen_once_before_limit(self):
        scorer = alert_engine._score_details_for_side
        observed = {}

        def record(row, side, *args):
            details = scorer(row, side, *args)
            observed[(row["timeframe"], side)] = deepcopy(details)
            return details

        with patch.object(alert_engine, "_score_details_for_side", side_effect=record) as spy:
            items = alert_engine.build_opportunities(_rows(), limit=1)
        self.assertEqual(spy.call_count, 14, "capture must not calculate scores again")
        self.assertEqual(len(items), 1)
        frozen = items[0]["maxpain_timeframes"]
        self.assertEqual(len(frozen), 14, "display limit must not discard frozen candidates")
        for entry in frozen:
            details = observed[(entry["timeframe"], entry["source_side"])]
            self.assertEqual(entry["score"], details["score"])
            self.assertEqual(entry["components"], details["components"])
            self.assertEqual(entry["distance_pct"], details["distance"])
            self.assertEqual(entry["near_amount"], details["near_amount"])
            self.assertEqual(entry["far_amount"], details["far_amount"])
            self.assertEqual(entry["near_share_pct"], details["balance"]["near_share_pct"])
            self.assertEqual(entry["consensus_hits"], details["consensus_hits"])
            self.assertEqual(entry["consensus_total"], details["consensus_total"])
            self.assertEqual(
                entry["score"],
                items[0]["directional_scores_all_timeframes"][entry["source_side"]][entry["timeframe"]],
            )
            if entry["source_side"] == "LONG":
                self.assertLess(entry["target_price"], 100.0)
            else:
                self.assertGreater(entry["target_price"], 100.0)

    def test_missing_liquidity_is_distinct_from_observed_zero(self):
        rows = _rows()[:1]
        rows[0].pop("long_liquidation_amount")
        rows[0]["short_liquidation_amount"] = 0.0
        entries = alert_engine.build_opportunities(rows)[0]["maxpain_timeframes"]
        by_side = {entry["source_side"]: entry for entry in entries}
        self.assertNotIn("near_amount", by_side["LONG"])
        self.assertEqual(by_side["LONG"]["far_amount"], 0.0)
        self.assertNotIn("far_amount", by_side["SHORT"])
        self.assertEqual(by_side["SHORT"]["near_amount"], 0.0)
        self.assertTrue(all("near_share_pct" not in entry for entry in entries))

    def test_inactive_target_not_invented_and_symbol_data_not_mixed(self):
        btc = _rows()[:1]
        btc[0]["short_max_pain"] = 99.5  # crossed the upper target
        sol = _rows("SOL")[:1]
        sol[0]["long_liquidation_amount"] = 99_000.0
        items = alert_engine.build_opportunities(btc + sol)
        by_symbol = {item["symbol"]: item for item in items}
        self.assertEqual(len(by_symbol["BTC"]["maxpain_timeframes"]), 1)
        self.assertEqual(by_symbol["BTC"]["maxpain_timeframes"][0]["source_side"], "LONG")
        self.assertEqual(by_symbol["BTC"]["maxpain_timeframes"][0]["near_amount"], 1_000.0)
        self.assertEqual(len(by_symbol["SOL"]["maxpain_timeframes"]), 2)

    def test_forced_side_keeps_both_sides_and_independent_frozen_copies(self):
        items = alert_engine.build_opportunities(_rows(), limit=20, forced_symbol="BTC", forced_side="SHORT")
        self.assertTrue(all(item["side"] == "SHORT" for item in items))
        self.assertTrue(all(len(item["maxpain_timeframes"]) == 14 for item in items))
        before = deepcopy(items[1]["maxpain_timeframes"])
        items[0]["maxpain_timeframes"][0]["components"]["consensus"] = -999.0
        self.assertEqual(items[1]["maxpain_timeframes"], before)

    def test_compaction_omits_unavailable_and_bounds_duplicates(self):
        rows = [
            {
                "timeframe": timeframe, "source_side": side, "score": 0,
                "near_amount": None, "far_amount": float("nan"),
                "distance_pct": True, "target_price": float("inf"),
                "components": {"consensus": 0, "relative_gap": None, "raw_windows": [1] * 100_000},
                "raw": "do not capture",
            }
            for timeframe in alert_engine.TIMEFRAMES
            for side in ("LONG", "SHORT")
        ]
        invalid = [{"timeframe": "unknown", "source_side": "LONG"}, {"timeframe": "12h", "source_side": "UPPER"}]
        compact = capture.compact_maxpain_timeframes(invalid + rows * 3)
        self.assertEqual(len(compact), 14)
        self.assertEqual(len({(row["timeframe"], row["source_side"]) for row in compact}), 14)
        for row in compact:
            self.assertEqual(row["score"], 0.0)
            self.assertEqual(row["components"], {"consensus": 0.0})
            self.assertEqual(set(row), {"timeframe", "source_side", "score", "components"})
        self.assertEqual(capture.compact_maxpain_timeframes(None), [])
        self.assertEqual(capture.compact_maxpain_timeframes({"12h": 80}), [])

    def test_maxpain_snapshot_freezes_values_without_changing_identity(self):
        item = alert_engine.build_opportunities(_rows(), limit=1)[0]
        timestamp = datetime(2026, 9, 7, 10, 10, 0, 123456, tzinfo=timezone.utc)
        event = capture.build_maxpain_event(item, event_time=timestamp)
        legacy = dict(item)
        legacy.pop("maxpain_timeframes")
        previous_shape = capture.build_maxpain_event(legacy, event_time=timestamp)
        self.assertEqual(event.event_fingerprint, previous_shape.event_fingerprint)
        self.assertEqual(event.setup_key, previous_shape.setup_key)
        self.assertEqual(event.alert_time_utc, "2026-09-07T10:10:00.123456Z")
        self.assertEqual(event.engine_snapshot["maxpain_timeframes"], item["maxpain_timeframes"])
        saved = deepcopy(event.engine_snapshot["maxpain_timeframes"])
        item["maxpain_timeframes"][0]["components"]["consensus"] = -999.0
        self.assertEqual(event.engine_snapshot["maxpain_timeframes"], saved)
        self.assertLess(len(json.dumps(event.engine_snapshot).encode()), capture.MAX_ENGINE_SNAPSHOT_BYTES)
        self.assertEqual(previous_shape.engine_snapshot["maxpain_timeframes"], [])

    def test_combined_uses_identical_frozen_contract_and_keeps_delivery_arguments(self):
        item = alert_engine.build_opportunities(_rows(), limit=1)[0]
        candidate = {
            "symbol": "BTC", "side": item["side"], "signal_count": 2,
            "signal_keys": {"score_over_80:24h", "strong_confirmation:24h"},
            "top_item": item,
        }
        timestamp = datetime(2026, 9, 7, 10, 40, tzinfo=timezone.utc)
        delivered_at = datetime(2026, 9, 7, 10, 40, 7, tzinfo=timezone.utc)
        with patch.object(runtime, "_emit", return_value=True) as emit:
            self.assertTrue(runtime.capture_combined_confirmation(
                candidate, event_time=timestamp, persist=True,
                delivery_status="DELIVERED", delivery_attempted_at_utc=timestamp,
                delivered_at_utc=delivered_at,
            ))
        event = emit.call_args.args[0]
        self.assertEqual(event.engine_snapshot["maxpain_timeframes"], item["maxpain_timeframes"])
        self.assertEqual(event.source_side, item["side"])
        self.assertEqual(event.direction, "SHORT" if item["side"] == "LONG" else "LONG")
        self.assertEqual(event.alert_time_utc, "2026-09-07T10:40:00.000000Z")
        self.assertEqual(emit.call_args.kwargs, {
            "persist": True, "capture_stage": "TELEGRAM_COMBINED_ALERT",
            "delivery_status": "DELIVERED", "delivery_attempted_at_utc": timestamp,
            "delivered_at_utc": delivered_at,
        })
        self.assertLess(len(json.dumps(event.engine_snapshot).encode()), capture.MAX_ENGINE_SNAPSHOT_BYTES)


if __name__ == "__main__":
    unittest.main()
