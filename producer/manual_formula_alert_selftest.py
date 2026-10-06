"""Network-free predicate, coin-filter, direction and display regressions."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

import manual_formula_alert as rules

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
DEFAULT = object()


def fixture(symbol="BTC", direction="LONG"):
    event = {"event_id": 123, "event_fingerprint": "a" * 64, "event_kind": "ALERT",
             "event_type": "MAGNET_ALERT", "delivery_status": "DELIVERED", "symbol": symbol,
             "direction": direction, "alert_time_utc": NOW, "current_price": 100,
             "engine_snapshot": {"magnet": {"side": "UPPER" if direction == "LONG" else "LOWER",
                 "count": 3, "members": ["12h", "24h", "48h"]},
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


def captured_references(symbol="BTC"):
    """Distinct component anchors expose using a shared current quote by mistake."""
    return {
        component: {"status": "READY", "component": component, "symbol": symbol,
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


def c1274_bundle(*, direction="BULLISH", score=DEFAULT, quality=.65, available=True,
                 computed=NOW, candle_close=None, cycle_id="watch-cycle-1"):
    if score is DEFAULT:
        score = 25 if direction == "BULLISH" else -25
    candle_close = candle_close or computed - timedelta(minutes=3)
    coins = {symbol: {} for symbol in rules.SYMBOLS}
    coins["SOL"] = {
        "status": "PARTIAL",
        "source_time_errors": [],
        "models": {"futures_flow": {
            "available": available,
            "capture_status": "AVAILABLE" if available is True else "UNAVAILABLE",
            "quality_status": "PASS",
            "freshness_status": "FRESH",
            "score": score,
            "time_families": {"long": {"quality": quality, "direction": direction}},
        }},
        "sources": {"futures": {"quality": {"candle_close": candle_close.isoformat()}}},
    }
    body = {
        "version": rules._WATCH_SCORE_VERSION,
        "population": rules._WATCH_SCORE_POPULATION,
        "hash_version": rules._WATCH_SCORE_HASH_VERSION,
        "status": "PARTIAL",
        "symbols_expected": list(rules.SYMBOLS),
        "cycle_id": cycle_id,
        "computed_at_utc": computed.isoformat(),
        "coins": coins,
    }
    return {**body, "payload_sha256": rules._watch_digest(body)}


def c1274_result(bundle=None, references=None, now=NOW):
    return rules.evaluate_c1274_scan(
        bundle or c1274_bundle(),
        references if references is not None else {"SOL": captured_references("SOL")},
        now,
    )


class ManualFormulaTests(unittest.TestCase):
    def test_each_formula_uses_its_earliest_required_anchor_and_final_direction(self):
        expected = {"PRICE_OI_ENTRY2": ("PRICE_OI", "110"),
                    "PRICE_OI_SPOT65": ("SPOT_CVD", "100"),
                    "CONSENSUS_FULL": ("MAX_PAIN", "120"),
                    "C0964": ("SPOT_CVD", "100")}
        expected_lower_upper = {"PRICE_OI_ENTRY2": ("108.9", "111.1"),
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

        for family_direction, final_direction in (("BULLISH", "LONG"), ("BEARISH", "SHORT")):
            row = c1274_result(c1274_bundle(direction=family_direction))["payload"]
            with self.subTest(rule="C1274", direction=final_direction):
                self.assertEqual(row["price_reference"]["component"], "FUTURES_CVD")
                self.assertEqual(float(row["price_reference"]["price"]), 90)
                self.assertEqual(row["direction"], final_direction)
                self.assertEqual(row["source_direction"], final_direction)
                levels = rules.render_reference_levels(row["price_reference"], 150,
                                                       final_direction, html=True)
                self.assertIn(levels, row["text"])
                stop, target = (("88.65", "91.35") if final_direction == "LONG"
                                else ("91.35", "88.65"))
                self.assertIn("<b>סטופלוס:</b> " + stop + "\n", row["text"])
                self.assertIn("<b>טייק פרופיט:</b> " + target + "\n", row["text"])

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
                self.assertEqual(len(rows), 4)
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
        old_payload = {key: value for key, value in rows["PRICE_OI_ENTRY2"].items()
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
                self.assertEqual(len(rows), 4)
                self.assertTrue(all(row["price_reference"]["status"] == "UNAVAILABLE" for row in rows))
                self.assertTrue(all("<b>סטופלוס:</b>" not in row["text"] for row in rows))

    def test_later_required_component_cannot_be_from_after_event(self):
        event, features = fixture()
        event["engine_snapshot"]["magnet"]["liquidity_edge_pct"] = 30
        references = captured_references()
        references["MAX_PAIN"]["anchor_time_utc"] = "2026-09-14T12:01:00Z"
        references["MAX_PAIN"]["price_time_utc"] = "2026-09-14T12:01:00Z"
        event["engine_snapshot"]["experimental_price_references"] = references
        rows = {row["rule_id"]: row for row in rules.evaluate_event(event, features, NOW)}
        self.assertEqual(rows["C0964"]["price_reference"]["status"], "UNAVAILABLE")
        self.assertEqual(rows["PRICE_OI_SPOT65"]["price_reference"]["status"], "READY")

    def test_frozen_rule_version_and_ruleset(self):
        self.assertEqual(rules.VERSION, "manual-formula-experimental-alerts-v5")
        self.assertEqual(rules.RULESET_SHA256,
                         "6e2fdc705023e3b9ec5c81ed71df2bcc50dc4195f29a89cd89a0071346937110")
        self.assertEqual(rules.PRE_TIMEFRAME_FILTER_VERSION, "manual-formula-experimental-alerts-v4")
        self.assertEqual(rules.PRE_TIMEFRAME_FILTER_RULESET_SHA256,
                         "160901f44287a903208630abeaa20e24eba8615dd20f62fa1c33646294f85df6")

    def test_doge_observation_is_direct_short_only_without_extra_indicator_filters(self):
        rule_id = 'MAGNET_OBSERVATION_DOGE_SHORT'
        for symbol in rules.SYMBOLS:
            for direction in ('LONG', 'SHORT'):
                event, features = fixture(symbol, direction)
                event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
                event['engine_snapshot']['magnet'].update(magnet_quality=51.09, liquidity_edge_pct=-20)
                event['engine_snapshot'].pop('market_evidence')
                features = {key: value for key, value in features.items() if key.startswith('event.')}
                features['captured.magnet.confirmation_status'] = 'OBSERVATION'
                rows = rules.evaluate_event(event, features, NOW)
                with self.subTest(symbol=symbol, direction=direction):
                    self.assertEqual([row['rule_id'] for row in rows],
                                     [rule_id] if symbol == 'DOGE' and direction == 'SHORT' else [])
                    if rows:
                        row = rows[0]
                        self.assertEqual((row['direction'], row['source_direction']), ('SHORT', 'SHORT'))
                        self.assertEqual(row['prediction_mode'], 'DIRECT')
                        self.assertEqual(row['threshold_bps'], 175)
                        self.assertIn('סף 1.75%', row['text'])
                        self.assertIn('החיזוי בכיוון המגנט, ללא היפוך.', row['text'])
                        self.assertNotIn('החיזוי הפוך', row['text'])

    def test_doge_observation_rejects_other_states_invalid_direction_and_nonmagnet_source(self):
        rule_id = 'MAGNET_OBSERVATION_DOGE_SHORT'
        event, features = fixture('DOGE', 'SHORT')
        event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
        features['captured.magnet.confirmation_status'] = 'OBSERVATION'
        for status in (None, '', 'CONFIRMED', 'STRONG', 'UNCONFIRMED', 'observation'):
            for target in ('snapshot', 'features'):
                other, data = deepcopy(event), deepcopy(features)
                if target == 'snapshot':
                    other['engine_snapshot']['magnet_confirmation']['status'] = status
                else:
                    data['captured.magnet.confirmation_status'] = status
                with self.subTest(status=status, target=target):
                    self.assertNotIn(rule_id, result_ids(other, data))
        other = deepcopy(event)
        other['engine_snapshot']['magnet']['side'] = 'UPPER'
        self.assertNotIn(rule_id, result_ids(other, features))
        other = deepcopy(event)
        other['event_type'] = 'PRICE_OI_ALERT'
        self.assertNotIn(rule_id, result_ids(other, features))
        data = {**features, 'event.direction_mapping_valid': False}
        self.assertNotIn(rule_id, result_ids(event, data))

    def test_doge_observation_reference_levels_are_frozen_symmetric_and_render_cannot_flip(self):
        rule_id = 'MAGNET_OBSERVATION_DOGE_SHORT'
        event, features = fixture('DOGE', 'SHORT')
        event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
        event['engine_snapshot']['experimental_price_references'] = captured_references('DOGE')
        features['captured.magnet.confirmation_status'] = 'OBSERVATION'
        row = next(r for r in rules.evaluate_event(event, features, NOW) if r['rule_id'] == rule_id)
        self.assertEqual(row['price_reference']['required_components'], ['MAX_PAIN'])
        self.assertEqual(row['price_reference']['price'], '120')
        self.assertIn('<b>סטופלוס:</b> 122.1', row['text'])
        self.assertIn('<b>טייק פרופיט:</b> 117.9', row['text'])
        event['engine_snapshot']['experimental_price_references']['MAX_PAIN']['price'] = '999'
        self.assertEqual(rules.render_message(row), row['text'])
        for changes in ({'direction': 'LONG'}, {'source_direction': 'LONG', 'direction': 'LONG'},
                        {'symbol': 'BTC'}, {'threshold_bps': 150}, {'prediction_mode': 'INVERSE'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                rules.render_message({**row, **changes})

    def test_doge_observation_requires_three_to_seven_distinct_supported_timeframes(self):
        rule_id = 'MAGNET_OBSERVATION_DOGE_SHORT'
        supported = ['12h', '24h', '48h', '3d', '1w', '2w', '1m']
        for count in (2, 3, 7):
            for container in (list, tuple):
                event, features = fixture('DOGE', 'SHORT')
                event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
                features['captured.magnet.confirmation_status'] = 'OBSERVATION'
                event['engine_snapshot']['magnet'].update(count=count, members=container(supported[:count]))
                rows = [r for r in rules.evaluate_event(event, features, NOW) if r['rule_id'] == rule_id]
                with self.subTest(count=count, container=container.__name__):
                    self.assertEqual(len(rows), int(count >= 3))
                    if rows:
                        self.assertEqual(rows[0]['magnet_timeframes'], supported[:count])
                        self.assertEqual(rows[0]['magnet_timeframe_count'], count)
                        self.assertTrue(rules.delivery_payload_allowed(rows[0]))

    def test_doge_observation_rejects_missing_malformed_or_inconsistent_timeframes(self):
        rule_id = 'MAGNET_OBSERVATION_DOGE_SHORT'
        valid = {'count': 3, 'members': ['12h', '24h', '48h']}
        invalid = [
            {}, {'members': valid['members']}, {'count': 3},
            *[{**valid, 'count': value} for value in (None, True, '3', 3.0, -1, 2, 4, 8)],
            *[{**valid, 'members': value} for value in (
                None, '12h,24h,48h', {'12h': 1, '24h': 1, '48h': 1}, [],
                ['12h', '24h'], ['12h', '24h', '24h'], ['12h', '24h', '4h'],
                ['12h', '24h', '48H'], ['12h', '24h', None], ['12h', '24h', ['48h']],
                ['12h', '24h', '48h', '48h'])],
            {'count': 4, 'members': ['12h', '24h', '48h', '48h']},
        ]
        for metadata in invalid:
            event, features = fixture('DOGE', 'SHORT')
            event['engine_snapshot']['magnet'] = {'side': 'LOWER', **deepcopy(metadata)}
            event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
            features['captured.magnet.confirmation_status'] = 'OBSERVATION'
            with self.subTest(metadata=metadata):
                self.assertNotIn(rule_id, result_ids(event, features))

    def test_doge_timeframes_are_frozen_and_old_or_invalid_payload_cannot_deliver(self):
        event, features = fixture('DOGE', 'SHORT')
        event['engine_snapshot']['magnet_confirmation'] = {'status': 'OBSERVATION'}
        features['captured.magnet.confirmation_status'] = 'OBSERVATION'
        payload = next(r for r in rules.evaluate_event(event, features, NOW)
                       if r['rule_id'] == 'MAGNET_OBSERVATION_DOGE_SHORT')
        frozen = deepcopy(payload)
        event['engine_snapshot']['magnet']['members'].append('3d')
        event['engine_snapshot']['magnet']['count'] = 4
        self.assertEqual(payload, frozen)
        self.assertEqual(rules.render_message(payload), frozen['text'])
        for changes in ({'predicate_version': rules.PRE_TIMEFRAME_FILTER_VERSION},
                        {'predicate_version': None}, {'magnet_timeframe_count': 2},
                        {'magnet_timeframe_count': 3.0}, {'magnet_timeframes': None},
                        {'magnet_timeframes': ['12h', '24h', '24h']},
                        {'magnet_timeframes': ['12h', '24h', '4h']}):
            bad = {**payload, **changes}
            with self.subTest(changes=changes):
                self.assertFalse(rules.delivery_payload_allowed(bad))
                with self.assertRaises(ValueError):
                    rules.render_message(bad)
        for missing in ('predicate_version', 'magnet_timeframe_count', 'magnet_timeframes'):
            bad = deepcopy(payload)
            del bad[missing]
            with self.subTest(missing=missing):
                self.assertFalse(rules.delivery_payload_allowed(bad))
        for rule_id in rules.RULES:
            if rule_id != 'MAGNET_OBSERVATION_DOGE_SHORT':
                with self.subTest(legacy_rule=rule_id):
                    self.assertTrue(rules.delivery_payload_allowed(
                        {'rule_id': rule_id, 'predicate_version': rules.LEGACY_VERSION}))

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
        self.assertEqual(len(rules.evaluate_event(event, features, NOW, planned=True)), 3)
        for field, bad in (('capture_stage', 'OBSERVED'), ('event_id', 123), ('delivery_status', 'DELIVERED')):
            changed = deepcopy(event); changed[field] = bad
            self.assertFalse(rules.evaluate_event(changed, features, NOW, planned=True))
        event['engine_snapshot']['archive_only'] = True
        self.assertFalse(rules.evaluate_event(event, features, NOW, planned=True))

    def test_all_eight_coin_filters_are_exact(self):
        expected = {"PRICE_OI_ENTRY2": {"BTC", "BNB", "DOGE", "ETH", "SOL", "XRP"},
                    "PRICE_OI_SPOT65": {"BTC", "BNB", "DOGE", "HYPE", "SOL", "XRP", "ZEC"},
                    "CONSENSUS_FULL": {"BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"}}
        for symbol in rules.SYMBOLS:
            with self.subTest(symbol=symbol):
                self.assertEqual(result_ids(*fixture(symbol)), {key for key, symbols in expected.items() if symbol in symbols})
        self.assertFalse(result_ids(*fixture("ADA")))
        self.assertEqual(rules.RULES["C1274"]["symbols"], ("SOL",))
        self.assertEqual(c1274_result()["payload"]["symbol"], "SOL")

    def test_threshold_headers_and_exact_notes(self):
        expected = {"PRICE_OI_ENTRY2": {"BTC": ["כמות הופעות קטנה"], "ETH": ["כמות הופעות קטנה"],
                                        "SOL": ["כמות הופעות קטנה"], "DOGE": ["אסימטריה נמוכה"]},
                    "PRICE_OI_SPOT65": {s: ["כמות הופעות קטנה"] + (["כמות ההופעות הגדולה ביותר"] if s == "ZEC" else []) for s in rules.SYMBOLS if s != "ETH"},
                    "CONSENSUS_FULL": {"SOL": ["אסימטריה טעונת שיפור"], **{s: ["הסתברות / אסימטריה גבוהות אבל מעט הופעות יחסית"] for s in ("XRP", "ETH", "BTC")}}}
        for symbol in rules.SYMBOLS:
            for row in rules.evaluate_event(*fixture(symbol), NOW):
                with self.subTest(symbol=symbol, rule=row["rule_id"]):
                    self.assertEqual(row["threshold_bps"], 100 if row["rule_id"] == "PRICE_OI_ENTRY2" else 200)
                    self.assertTrue(row["text"].startswith(f'🧪 <b>סף {row["threshold_bps"] / 100:g}% — ניסיוני, לא למסחר</b>'))
                    notes = [line.removeprefix("<b>הערה</b>: ") for line in row["text"].splitlines() if line.startswith("<b>הערה</b>: ")]
                    self.assertEqual(notes, expected[row["rule_id"]].get(symbol, []))
                    self.assertNotIn("שיעור הצלחה", row["text"])
        row = c1274_result()["payload"]
        self.assertEqual(row["threshold_bps"], 150)
        self.assertTrue(row["text"].startswith('🧪 <b>סף 1.5% — ניסיוני, לא למסחר</b>'))
        self.assertIn("<b>הערה</b>: כמות הופעות / אסימטריה טעונות שיפור", row["text"])

    def test_single_inversion_both_directions(self):
        for direction, expected in (("LONG", "SHORT"), ("SHORT", "LONG")):
            rows = rules.evaluate_event(*fixture(direction=direction), NOW)
            self.assertEqual(len(rows), 3)
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
        for direction, expected in (("BULLISH", "LONG"), ("BEARISH", "SHORT")):
            payload = c1274_result(c1274_bundle(direction=direction))["payload"]
            self.assertEqual(payload["direction"], expected)
            self.assertEqual(payload["source_direction"], expected)
            self.assertEqual(payload["prediction_mode"], "DIRECT")
            for value in (.649999, None, True, "nan", "inf", 65):
                bundle = c1274_bundle(direction=direction, quality=value)
                self.assertIsNone(c1274_result(bundle)["payload"])
            for value in (.65, 1):
                bundle = c1274_bundle(direction=direction, quality=value)
                self.assertIsNotNone(c1274_result(bundle)["payload"])
        for available in (False, "true", None, 1):
            self.assertIsNone(c1274_result(c1274_bundle(available=available))["payload"])
        for score in (None, True, "nan", "inf", 24.999999, -25, 101, -101):
            self.assertIsNone(c1274_result(c1274_bundle(score=score))["payload"])
        for score in (25, 100, "25"):
            self.assertIsNotNone(c1274_result(c1274_bundle(score=score))["payload"])
        for score in (-25, -100, "-25"):
            self.assertIsNotNone(c1274_result(c1274_bundle(direction="BEARISH", score=score))["payload"])
        for value in ("NEUTRAL", None, "SHORT", "LONG"):
            self.assertIsNone(c1274_result(c1274_bundle(direction=value, score=25))["payload"])
        for kind in ("MAGNET_ALERT", "MAGNET_CONFIRMATION", "STRONG_MAGNET_CONFIRMATION"):
            event, features = fixture(); event["event_type"] = kind
            self.assertNotIn("C1274", result_ids(event, features))

    def test_c1274_is_futures_only_and_message_has_no_magnet_or_inversion(self):
        references = {"SOL": {"FUTURES_CVD": captured_references("SOL")["FUTURES_CVD"]}}
        payload = c1274_result(references=references)["payload"]
        self.assertEqual(payload["price_reference"]["component"], "FUTURES_CVD")
        self.assertNotIn("מגנט", payload["text"])
        self.assertNotIn("החיזוי הפוך", payload["text"])

        missing = c1274_result(references={"SOL": {}})["payload"]
        self.assertEqual(missing["price_reference"]["status"], "UNAVAILABLE")
        self.assertNotIn("<b>סטופלוס:</b>", missing["text"])

        wrong_clock = deepcopy(references)
        wrong_clock["SOL"]["FUTURES_CVD"]["anchor_time_utc"] = "2026-09-14T11:56:00Z"
        wrong_clock["SOL"]["FUTURES_CVD"]["price_time_utc"] = "2026-09-14T11:56:00Z"
        mismatched = c1274_result(references=wrong_clock)["payload"]
        self.assertEqual(mismatched["price_reference"]["reason"],
                         "REFERENCE_SOURCE_CLOCK_MISMATCH")
        self.assertNotIn("<b>סטופלוס:</b>", mismatched["text"])

    def test_c1274_rejects_modified_stale_or_bad_clock_bundle(self):
        modified = c1274_bundle()
        modified["coins"]["SOL"]["models"]["futures_flow"]["score"] = 50
        with self.assertRaises(ValueError):
            c1274_result(modified)
        with self.assertRaises(ValueError):
            c1274_result(c1274_bundle(computed=NOW - timedelta(minutes=10, microseconds=1)))
        with self.assertRaises(ValueError):
            c1274_result(c1274_bundle(candle_close=NOW + timedelta(seconds=1)))
        bad_source = c1274_bundle()
        bad_source["coins"]["SOL"]["source_time_errors"] = ["derivatives/observed:FUTURE_TIME"]
        bad_source["payload_sha256"] = rules._watch_digest(
            {key: value for key, value in bad_source.items() if key != "payload_sha256"})
        with self.assertRaises(ValueError):
            c1274_result(bad_source)

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
        self.assertEqual(result_ids(event, features), {"PRICE_OI_SPOT65", "CONSENSUS_FULL"})

    def test_renderer_rejects_mutated_identity_and_does_not_mutate_input(self):
        event, features = fixture(); original = deepcopy((event, features))
        payload = rules.evaluate_event(event, features, NOW)[0]
        self.assertEqual((event, features), original)
        self.assertEqual(payload["text"], rules.render_message(payload))
        for field, bad in (("rule_id", "OTHER"), ("threshold_bps", 200), ("predicate_version", "unknown"),
                           ("symbol", "HYPE"), ("direction", "LONG"), ("source_direction", "SHORT")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                rules.render_message({**payload, field: bad})

        direct = c1274_result()["payload"]
        for field, bad in (("threshold_bps", 100), ("symbol", "BTC"),
                           ("direction", "SHORT"), ("source_direction", "SHORT"),
                           ("prediction_mode", "INVERSE"), ("watch_scan_id", "other"),
                           ("source_bundle_sha256", "g" * 64),
                           ("source_candle_close_utc", "2026-09-14T11:56:00Z")):
            with self.subTest(direct_field=field), self.assertRaises(ValueError):
                rules.render_message({**direct, field: bad})


if __name__ == "__main__":
    unittest.main()
