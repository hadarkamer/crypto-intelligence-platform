"""Network-free predicate, coin-filter, direction and display regressions."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import manual_formula_alert as rules

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


def fixture(symbol="BTC", direction="LONG"):
    event = {"event_id": 123, "event_fingerprint": "a" * 64, "event_kind": "ALERT",
             "event_type": "MAGNET_ALERT", "delivery_status": "DELIVERED", "symbol": symbol,
             "direction": direction, "alert_time_utc": NOW, "current_price": 100,
             "engine_snapshot": {"magnet": {"side": "UPPER" if direction == "LONG" else "LOWER"},
                 "price_source": "binance_futures" if symbol == "HYPE" else "binance_spot",
                 "market_evidence": {"modules": {"futures_flow": {
                     "available": True, "score": -25 if direction == "LONG" else 25,
                     "time_families": {"long": {"quality": .65,
                         "direction": "BEARISH" if direction == "LONG" else "BULLISH"}}}}}}}
    features = {"event.direction_mapping_valid": True, "event.analysis_direction": direction,
                "price_oi.aligned_score": 65, "spot_cvd.aligned_score": 65,
                "sequence.capture_status": "READY", "sequence.30m.price_oi.entry_ordinal": 2,
                "max_pain.consensus_hits_full": True}
    return event, features


def result_ids(event, features, now=NOW):
    return {row["rule_id"] for row in rules.evaluate_event(event, features, now)}


def captured_references():
    """Distinct component anchors expose using a shared current quote by mistake."""
    return {
        component: {"status": "READY", "component": component, "symbol": "BTC",
                    "price": price,
                    "price_time_utc": "2026-09-14T11:59:00Z" if component == "MAX_PAIN" else stamp,
                    "anchor_time_utc": stamp,
                    "source": "BINANCE_SPOT_TRADE_1M", "precision": precision}
        for component, price, stamp, precision in (
            ("FUTURES_CVD", "90", "2026-09-14T11:57:00Z", "CLOSED_1M"),
            ("SPOT_CVD", "100", "2026-09-14T11:58:00Z", "CLOSED_1M"),
            ("PRICE_OI", "110", "2026-09-14T11:59:10Z", "EXACT_CAPTURE"),
            ("MAX_PAIN", "120", "2026-09-14T11:59:20Z", "CLOSED_1M"),
        )
    }


class ManualFormulaTests(unittest.TestCase):
    def test_each_formula_uses_its_earliest_required_anchor_and_final_direction(self):
        expected = {"C1274": ("FUTURES_CVD", "90"),
                    "PRICE_OI_ENTRY2": ("PRICE_OI", "110"),
                    "PRICE_OI_SPOT65": ("SPOT_CVD", "100"),
                    "CONSENSUS_FULL": ("MAX_PAIN", "120"),
                    "C0964": ("SPOT_CVD", "100")}
        expected_lower_upper = {"C1274": ("89.1", "90.9"),
                                "PRICE_OI_ENTRY2": ("108.9", "111.1"),
                                "PRICE_OI_SPOT65": ("98", "102"),
                                "CONSENSUS_FULL": ("117.6", "122.4"),
                                "C0964": ("98", "102")}
        for source_direction, final_direction in (("LONG", "SHORT"), ("SHORT", "LONG")):
            event, features = fixture(direction=source_direction)
            event["engine_snapshot"]["magnet"]["liquidity_edge_pct"] = 30
            event["engine_snapshot"]["experimental_price_references"] = captured_references()
            rows = rules.evaluate_event(event, features, NOW)
            self.assertEqual({row["rule_id"] for row in rows}, set(expected))
            for row in rows:
                with self.subTest(rule=row["rule_id"], direction=final_direction):
                    component, price = expected[row["rule_id"]]
                    self.assertEqual(row["price_reference"]["component"], component)
                    self.assertEqual(float(row["price_reference"]["price"]), float(price))
                    self.assertEqual(row["direction"], final_direction)
                    levels = rules.render_reference_levels(row["price_reference"],
                                                           row["threshold_bps"], final_direction, html=True)
                    self.assertIn(levels, row["text"])
                    lower, upper = expected_lower_upper[row["rule_id"]]
                    stop, target = (lower, upper) if final_direction == "LONG" else (upper, lower)
                    self.assertIn("<b>סטופלוס:</b> " + stop + "\n", row["text"])
                    self.assertIn("<b>טייק פרופיט:</b> " + target + "\n", row["text"])
                    self.assertLess(row["text"].index(levels),
                                    row["text"].index(rules.RULES[row["rule_id"]]["conditions_text"]))

    def test_reference_is_frozen_and_rerender_does_not_reprice(self):
        event, features = fixture()
        event["engine_snapshot"]["experimental_price_references"] = captured_references()
        payload = rules.evaluate_event(event, features, NOW)[0]
        frozen = deepcopy(payload)
        event["current_price"] = 999
        for reference in event["engine_snapshot"]["experimental_price_references"].values():
            reference["price"] = "888"
            reference["anchor_time_utc"] = "2026-09-14T12:01:00Z"
        self.assertEqual(payload, frozen)
        self.assertEqual(rules.render_message(payload), frozen["text"])

    def test_missing_or_invalid_reference_never_substitutes_current_price(self):
        for references in (None, False, {}, {"SPOT_CVD": False},
                           {"SPOT_CVD": {"status": "READY", "price": 0}}):
            with self.subTest(references=references):
                event, features = fixture()
                event["current_price"] = 987654.321
                event["engine_snapshot"]["magnet"]["liquidity_edge_pct"] = 30
                event["engine_snapshot"]["experimental_price_references"] = references
                rows = rules.evaluate_event(event, features, NOW)
                self.assertEqual(len(rows), 5)
                for row in rows:
                    self.assertNotEqual((row["price_reference"] or {}).get("status"), "READY")
                    self.assertIn(rules.render_reference_levels(None, row["threshold_bps"],
                                                               row["direction"], html=True), row["text"])
                    self.assertNotIn("987654", row["text"])

    def test_partial_combination_reference_is_unavailable_and_old_payload_can_render(self):
        event, features = fixture()
        references = captured_references()
        del references["SPOT_CVD"]
        event["engine_snapshot"]["experimental_price_references"] = references
        rows = {row["rule_id"]: row for row in rules.evaluate_event(event, features, NOW)}
        self.assertNotEqual((rows["PRICE_OI_SPOT65"]["price_reference"] or {}).get("status"), "READY")
        self.assertEqual(rows["PRICE_OI_ENTRY2"]["price_reference"]["status"], "READY")
        old_payload = {key: value for key, value in rows["C1274"].items()
                       if key not in ("price_reference", "text")}
        message = rules.render_message(old_payload)
        self.assertIn(rules.render_reference_levels(None, old_payload["threshold_bps"],
                                                   old_payload["direction"], html=True), message)

    def test_wrong_coin_and_future_reference_do_not_produce_prices(self):
        for fault in ("symbol", "future"):
            with self.subTest(fault=fault):
                event, features = fixture()
                event["engine_snapshot"]["magnet"]["liquidity_edge_pct"] = 30
                references = captured_references()
                for reference in references.values():
                    if fault == "symbol":
                        reference["symbol"] = "ETH"
                    else:
                        reference["anchor_time_utc"] = "2026-09-14T12:01:00Z"
                        reference["price_time_utc"] = "2026-09-14T12:01:00Z"
                event["engine_snapshot"]["experimental_price_references"] = references
                rows = rules.evaluate_event(event, features, NOW)
                self.assertEqual(len(rows), 5)
                self.assertTrue(all(row["price_reference"]["status"] == "UNAVAILABLE" for row in rows))
                self.assertTrue(all("<b>סטופלוס:</b>" not in row["text"] for row in rows))

    def test_later_required_component_cannot_be_from_after_event(self):
        event, features = fixture()
        references = captured_references()
        references["MAX_PAIN"]["anchor_time_utc"] = "2026-09-14T12:01:00Z"
        references["MAX_PAIN"]["price_time_utc"] = "2026-09-14T12:01:00Z"
        event["engine_snapshot"]["experimental_price_references"] = references
        rows = {row["rule_id"]: row for row in rules.evaluate_event(event, features, NOW)}
        self.assertEqual(rows["C1274"]["price_reference"]["status"], "UNAVAILABLE")
        self.assertEqual(rows["PRICE_OI_SPOT65"]["price_reference"]["status"], "READY")

    def test_price_display_does_not_change_matching_version_or_ruleset(self):
        self.assertEqual(rules.VERSION, "manual-formula-experimental-alerts-v2")
        self.assertEqual(rules.RULESET_SHA256,
                         "9d28a33d38faae8bafce6f03b3ce400a1f928aca16f36edecb46b9450dd234c8")

    def test_c0964_exact_predicate_btc_both_directions_and_note(self):
        for symbol in rules.SYMBOLS:
            for direction in ('LONG', 'SHORT'):
                event, features = fixture(symbol, direction)
                event['engine_snapshot']['magnet']['liquidity_edge_pct'] = 30
                features['spot_cvd.aligned_score'] = 25
                rows = {r['rule_id']: r for r in rules.evaluate_event(event, features, NOW)}
                self.assertEqual('C0964' in rows, symbol == 'BTC')
                if symbol == 'BTC':
                    row = rows['C0964']
                    self.assertEqual(row['threshold_bps'], 200)
                    self.assertEqual(row['direction'], 'SHORT' if direction == 'LONG' else 'LONG')
                    self.assertIn('<b>הערה</b>: מבוסס בעיקר על אוגוסט ועל עליות', row['text'])
        for field, bad_values in (('edge', (29.999, None, True, float('inf'), float('nan'))),
                                  ('spot', (24.999, -25, None, True, 101, float('nan')))):
            for bad in bad_values:
                event, features = fixture()
                event['engine_snapshot']['magnet']['liquidity_edge_pct'] = 30
                features['spot_cvd.aligned_score'] = 25
                if field == 'edge': event['engine_snapshot']['magnet']['liquidity_edge_pct'] = bad
                else: features['spot_cvd.aligned_score'] = bad
                self.assertNotIn('C0964', result_ids(event, features))
        event, features = fixture()
        event['engine_snapshot']['magnet']['liquidity_edge_pct'] = 30
        event['event_type'] = 'PRICE_OI_ALERT'
        self.assertNotIn('C0964', result_ids(event, features))

    def test_planned_sources_require_explicit_lane_and_original_provenance(self):
        event, features = fixture()
        event.update(event_id='watch:' + event['event_fingerprint'],
                     capture_stage='WATCH_PLANNED_ALERT', delivery_status='NOT_ATTEMPTED')
        event['engine_snapshot']['watch_scan_id'] = 'current-watch'
        self.assertFalse(rules.evaluate_event(event, features, NOW))
        self.assertEqual(len(rules.evaluate_event(event, features, NOW, planned=True)), 4)
        for field, bad in (('capture_stage', 'OBSERVED'), ('event_id', 123), ('delivery_status', 'DELIVERED')):
            changed = deepcopy(event); changed[field] = bad
            self.assertFalse(rules.evaluate_event(changed, features, NOW, planned=True))
        event['engine_snapshot']['archive_only'] = True
        self.assertFalse(rules.evaluate_event(event, features, NOW, planned=True))

    def test_all_eight_coin_filters_are_exact(self):
        expected = {"C1274": {"BTC", "BNB", "DOGE", "HYPE", "SOL"},
                    "PRICE_OI_ENTRY2": {"BTC", "BNB", "DOGE", "ETH", "SOL", "XRP"},
                    "PRICE_OI_SPOT65": {"BTC", "BNB", "DOGE", "HYPE", "SOL", "XRP", "ZEC"},
                    "CONSENSUS_FULL": {"BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"}}
        for symbol in rules.SYMBOLS:
            with self.subTest(symbol=symbol):
                self.assertEqual(result_ids(*fixture(symbol)), {key for key, symbols in expected.items() if symbol in symbols})
        self.assertFalse(result_ids(*fixture("ADA")))

    def test_threshold_headers_and_exact_notes(self):
        expected = {"C1274": {"BTC": ["כמות הופעות / אסימטריה טעונות שיפור"], "SOL": ["כמות הופעות / אסימטריה טעונות שיפור"]},
                    "PRICE_OI_ENTRY2": {"BTC": ["כמות הופעות קטנה"], "ETH": ["כמות הופעות קטנה"],
                                        "SOL": ["כמות הופעות קטנה"], "DOGE": ["אסימטריה נמוכה"]},
                    "PRICE_OI_SPOT65": {s: ["כמות הופעות קטנה"] + (["כמות ההופעות הגדולה ביותר"] if s == "ZEC" else []) for s in rules.SYMBOLS if s != "ETH"},
                    "CONSENSUS_FULL": {"SOL": ["אסימטריה טעונת שיפור"], **{s: ["הסתברות / אסימטריה גבוהות אבל מעט הופעות יחסית"] for s in ("XRP", "ETH", "BTC")}}}
        for symbol in rules.SYMBOLS:
            for row in rules.evaluate_event(*fixture(symbol), NOW):
                with self.subTest(symbol=symbol, rule=row["rule_id"]):
                    self.assertEqual(row["threshold_bps"], 100 if row["rule_id"] in ("C1274", "PRICE_OI_ENTRY2") else 200)
                    self.assertTrue(row["text"].startswith(f'🧪 <b>סף {row["threshold_bps"] / 100:g}% — ניסיוני, לא למסחר</b>'))
                    notes = [line.removeprefix("<b>הערה</b>: ") for line in row["text"].splitlines() if line.startswith("<b>הערה</b>: ")]
                    self.assertEqual(notes, expected[row["rule_id"]].get(symbol, []))
                    self.assertNotIn("שיעור הצלחה", row["text"])

    def test_single_inversion_both_directions(self):
        for direction, expected in (("LONG", "SHORT"), ("SHORT", "LONG")):
            rows = rules.evaluate_event(*fixture(direction=direction), NOW)
            self.assertEqual(len(rows), 4)
            self.assertTrue(all(r["direction"] == expected and r["source_direction"] == direction for r in rows))
        event, features = fixture(); event["source_direction"] = "SHORT"
        self.assertFalse(result_ids(event, features))

    def test_maxpain_uses_research_not_display_side(self):
        event, features = fixture(direction="LONG")
        event.update(event_type="MAX_PAIN_ALERT", source_side="SHORT")
        rows = {r["rule_id"]: r for r in rules.evaluate_event(event, features, NOW)}
        self.assertEqual(rows["CONSENSUS_FULL"]["direction"], "SHORT")
        self.assertNotIn("C1274", rows)

    def test_core_scores_boundaries_missing_boolean_and_nonfinite(self):
        for field in ("price_oi.aligned_score", "spot_cvd.aligned_score"):
            for bad in (None, True, float("nan"), float("inf"), -65, 64.999999, 100.0001, "invalid"):
                event, features = fixture(); features[field] = bad
                with self.subTest(field=field, value=bad):
                    self.assertNotIn("PRICE_OI_SPOT65", result_ids(event, features))
            for good in (65, 100, "65"):
                event, features = fixture(); features[field] = good
                self.assertIn("PRICE_OI_SPOT65", result_ids(event, features))

    def test_entry_ordinal_exact_and_ready_required(self):
        for value in (None, True, "2", 1, 3, float("nan"), float("inf")):
            event, features = fixture(); features["sequence.30m.price_oi.entry_ordinal"] = value
            self.assertNotIn("PRICE_OI_ENTRY2", result_ids(event, features))
        for value in (2, 2.0):
            event, features = fixture(); features["sequence.30m.price_oi.entry_ordinal"] = value
            self.assertIn("PRICE_OI_ENTRY2", result_ids(event, features))
        for status in (None, "MISSING_EVENT_TIME", "INCOMPLETE"):
            event, features = fixture(); features["sequence.capture_status"] = status
            self.assertNotIn("PRICE_OI_ENTRY2", result_ids(event, features))

    def test_consensus_is_strict_boolean(self):
        for value in (None, False, 1, "True", "true"):
            event, features = fixture(); features["max_pain.consensus_hits_full"] = value
            self.assertNotIn("CONSENSUS_FULL", result_ids(event, features))

    def test_c1274_exact_raw_family_predicate_both_directions(self):
        for direction in ("LONG", "SHORT"):
            for value in (.649999, None, True, float("nan"), float("inf"), 65):
                event, features = fixture(direction=direction)
                event["engine_snapshot"]["market_evidence"]["modules"]["futures_flow"]["time_families"]["long"]["quality"] = value
                self.assertNotIn("C1274", result_ids(event, features))
            for value in (.65, 1):
                event, features = fixture(direction=direction)
                event["engine_snapshot"]["market_evidence"]["modules"]["futures_flow"]["time_families"]["long"]["quality"] = value
                self.assertIn("C1274", result_ids(event, features))
        for available in (False, "true", None, 1):
            event, features = fixture(); event["engine_snapshot"]["market_evidence"]["modules"]["futures_flow"]["available"] = available
            self.assertNotIn("C1274", result_ids(event, features))
        for score in (None, True, float("nan"), float("inf"), -24.999999, 25, -101):
            event, features = fixture(); event["engine_snapshot"]["market_evidence"]["modules"]["futures_flow"]["score"] = score
            self.assertNotIn("C1274", result_ids(event, features))
        for value in ("BULLISH", "NEUTRAL", None, "SHORT"):
            event, features = fixture(); event["engine_snapshot"]["market_evidence"]["modules"]["futures_flow"]["time_families"]["long"]["direction"] = value
            self.assertNotIn("C1274", result_ids(event, features))
        for kind in ("MAGNET_ALERT", "MAGNET_CONFIRMATION", "STRONG_MAGNET_CONFIRMATION"):
            event, features = fixture(); event["event_type"] = kind
            self.assertIn("C1274", result_ids(event, features))
        event, features = fixture(); event["engine_snapshot"]["market_evidence"]["modules"] = {}
        self.assertNotIn("C1274", result_ids(event, features))

    def test_c1274_does_not_add_unrequested_magnet_le_or_total_65(self):
        event, features = fixture()
        event["engine_snapshot"]["magnet"]["liquidity_edge_pct"] = 0
        self.assertIn("C1274", result_ids(event, features))

    def test_age_future_timezone_and_exact_ten_minute_boundary(self):
        event, features = fixture()
        for now in (NOW, NOW + timedelta(minutes=10)):
            self.assertTrue(result_ids(event, features, now))
        for now in (NOW - timedelta(microseconds=1), NOW + timedelta(minutes=10, microseconds=1), "invalid", NOW.replace(tzinfo=None)):
            self.assertFalse(result_ids(event, features, now))
        event["alert_time_utc"] = NOW.replace(tzinfo=None)
        self.assertFalse(result_ids(event, features))

    def test_native_source_provenance_and_mapping_required(self):
        for key, value in (("event_kind", "DECISION_SAMPLE"), ("delivery_status", "IMPORTED"),
                           ("event_id", True), ("event_id", "123"), ("event_id", 0),
                           ("event_fingerprint", "g" * 64), ("event_fingerprint", None),
                           ("event_type", "ORDERED_INVERSE_ANALYSIS"), ("event_type", ""),
                           ("source_scope", "ARCHIVE"), ("current_price", 0), ("current_price", True),
                           ("current_price", float("nan")), ("engine_snapshot", None), ("mode", "DEMO")):
            event, features = fixture(); event[key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(result_ids(event, features))
        for key, value in (("event.direction_mapping_valid", False), ("event.direction_mapping_valid", 1),
                           ("event.analysis_direction", "SHORT"), ("event.analysis_direction", None)):
            event, features = fixture(); features[key] = value
            self.assertFalse(result_ids(event, features))
        for field in ("inverse_analysis", "archive_reconstruction", "archive_only", "telegram_archive", "archive_run_key"):
            event, features = fixture(); event["engine_snapshot"][field] = False
            self.assertFalse(result_ids(event, features))
        for field in ("data_mode", "mode", "state"):
            event, features = fixture(); event["engine_snapshot"][field] = "demo"
            self.assertFalse(result_ids(event, features))
        self.assertFalse(rules.evaluate_event({}, {}, NOW))
        self.assertFalse(rules.evaluate_event(None, {}, NOW))
        self.assertFalse(rules.evaluate_event(fixture()[0], None, NOW))

    def test_hype_futures_is_not_rejected_by_spot_only_gate(self):
        event, features = fixture("HYPE")
        event["engine_snapshot"].update(price_source="binance_futures", price_market="futures", price_pair="HYPEUSDT")
        self.assertEqual(result_ids(event, features), {"C1274", "PRICE_OI_SPOT65", "CONSENSUS_FULL"})

    def test_renderer_rejects_mutated_identity_and_does_not_mutate_input(self):
        event, features = fixture(); original = deepcopy((event, features))
        payload = rules.evaluate_event(event, features, NOW)[0]
        self.assertEqual((event, features), original)
        self.assertEqual(payload["text"], rules.render_message(payload))
        for field, bad in (("rule_id", "OTHER"), ("threshold_bps", 200), ("predicate_version", "unknown"),
                           ("symbol", "ETH"), ("direction", "LONG"), ("source_direction", "SHORT")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                rules.render_message({**payload, field: bad})


if __name__ == "__main__":
    unittest.main()
