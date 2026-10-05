"""Network-free regression checks for the read-only operational-source audit.

Fixtures use the production anchor/capture builders and causal BTC policy.
No database is opened and no outcome worker, service, or publisher is run.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import re
import unittest
from unittest.mock import patch

import canonical_price_path
import research_btc_parent_movement as btc_policy
import research_operational_score_source_audit as audit
import research_ordered_first_touch as ordered
import research_prospective_anchors as anchors
from research_prospective_anchors_selftest import (
    _coverage, _feature_bundle_entry, _source_rows,
)
import research_watch_score_capture as capture
from research_watch_score_capture_selftest import (
    archive_payload, bundle, derivatives, inputs,
)


UTC = timezone.utc
BASE = datetime(2026, 8, 29, 12, tzinfo=UTC)
DECISION = BASE + timedelta(minutes=34)
AS_OF = DECISION + timedelta(days=2)
MAX_AGE = 3600


def _utc(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _anchor(*, symbol="BTC", attempt_id=1, eligible=True, missing=False):
    sources = _source_rows(BASE, symbol=symbol)
    if missing:
        sources.pop("spot_cvd")
    batch = anchors.build_anchor_batch(
        now=DECISION,
        slot_open_utc=BASE,
        coverage_by_symbol={symbol: _coverage(symbol=symbol, eligible=eligible)},
        source_inputs_by_symbol={symbol: sources},
        feature_bundles_by_symbol={
            symbol: _feature_bundle_entry(DECISION, symbol=symbol),
        },
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
        strategy_version="formula-prospective-neutral-v4", code_version="test-code",
    )
    persisted = batch.atomic_persistence_bundles()[0]
    attempt = persisted["attempt"] | {"attempt_id": attempt_id}
    events = [
        item["event"].to_dict() | {
            "event_id": attempt_id * 10 + index,
            "capture_stage": item["capture_stage"],
            "delivery_status": item["delivery_status"],
        }
        for index, item in enumerate(persisted["event_persistence"], 1)
    ]
    slot = persisted["slot"]
    if slot is not None:
        slot |= {
            "anchor_slot_id": attempt_id,
            "long_event_id": events[0]["event_id"],
            "short_event_id": events[1]["event_id"],
        }
    return attempt, slot, events


def _snapshot(*, snapshot_id=101, rows=None):
    source = derivatives()
    # Timestamped unavailable source observations are not observed neutral
    # scores: keep every availability/status flag and the raw fallback zero.
    for coin in source.values():
        coin["regime"]["price_fetched_at"] = BASE.isoformat()
        coin["regime"]["oi_fetched_at"] = BASE.isoformat()
        coin["flow"]["spot"]["quality"]["candle_close"] = BASE.isoformat()
    block, *_ = bundle(rows=rows, snapshot=source)
    return archive_payload(block)["set"] | {
        "snapshot_set_id": snapshot_id,
        "created_at_utc": BASE + timedelta(minutes=7),
    }


def _block(snapshot):
    return snapshot["source_metadata"]["capture_metadata"]["operational_scores"]


def _rehash(snapshot):
    block = _block(snapshot)
    block["payload_sha256"] = capture.digest({
        key: value for key, value in block.items() if key != "payload_sha256"
    })
    return snapshot


def _bar(opened, price=100.0, *, high=None, low=None):
    return {
        "open_time_utc": opened,
        "close_time_utc": opened + timedelta(minutes=1, milliseconds=-1),
        "open": price, "high": price if high is None else high,
        "low": price if low is None else low, "close": price,
        "price_source": btc_policy.SOURCE,
    }


def _outcome(event, *, status="SUCCESS", window=60, threshold_bps=50):
    decision = _utc(event["alert_time_utc"])
    reference = event["current_price"]
    direction = event["direction"]
    threshold = threshold_bps / 100.0
    favorable_up = direction == "LONG"
    terminal = status in ("SUCCESS", "FAILURE")
    up = favorable_up if status == "SUCCESS" else not favorable_up
    count = window if status == "UNRESOLVED" else 1
    bars = [_bar(decision + timedelta(minutes=index), reference)
            for index in range(count)]
    if terminal:
        bars[0]["high" if up else "low"] = reference * (
            1 + (threshold + .1) / 100 if up else 1 - (threshold + .1) / 100
        )
    result = ordered.calculate_ordered_first_touch_outcome(
        reference_price=reference, direction=direction, event_time=decision,
        candles=bars, threshold_pct=threshold,
        observation_closed=status == "UNRESOLVED",
        path_complete=status != "DATA_MISSING",
    )
    assert result["status"] == status
    return result | {
        "event_id": event["event_id"], "window_minutes": window,
        "price_source": (
            f"reference=binance_spot|path=binance_spot:{event['symbol']}USDT:1m|"
            "provenance=SELFTEST"
        ),
        "market_pair": f"{event['symbol']}USDT",
        "data_quality_status": (
            canonical_price_path.BINANCE_PARTIAL
            if status == "DATA_MISSING" else canonical_price_path.BINANCE_COMPLETE
        ),
        "created_at_utc": AS_OF - timedelta(hours=1),
        "updated_at_utc": AS_OF - timedelta(hours=1),
    }


def _parent(event):
    decision = _utc(event["alert_time_utc"])
    bars = [_bar(decision - timedelta(minutes=4-index), price)
            for index, price in enumerate((100, 102, 110, 107.8))]
    parent = btc_policy.advance_parents(
        bars, as_of_utc=bars[-1]["close_time_utc"],
    )[-1]
    membership = btc_policy.membership(event, parent=parent, btc_bar=bars[-1])
    assert membership["membership_status"] == "LIVE"
    return membership, parent, bars[-1]


class PureAuditTests(unittest.TestCase):
    def test_transaction_identity_canonical_timezone_vector(self):
        expected = {
            "version": "stage8-postgres-transaction-identity-v1",
            "backend_pid": 777,
            "transaction_started_at_utc": "2026-09-13T12:30:00.123456Z",
            "database_snapshot_id": "10:20:11,14",
            "transaction_identity_sha256":
                "c08a62123de1fa2d03e6a0d92fd5775355ee16a3b7abbbcdba24780835f45cd4",
        }
        for started in ("2026-09-13T15:30:00.123456+03:00",
                        "2026-09-13T12:30:00.123456Z",
                        datetime(2026, 9, 13, 18, 0, 0, 123456,
                                 tzinfo=timezone(timedelta(hours=5, minutes=30)))):
            with self.subTest(started=started):
                self.assertEqual(audit.transaction_identity_from_fields(
                    backend_pid=777, transaction_started_at_utc=started,
                    database_snapshot_id="10:20:11,14"), expected)

    def test_transaction_identity_microseconds_and_distinct_transactions(self):
        arguments = {"backend_pid": 777,
                     "transaction_started_at_utc": "2026-09-13T12:30:00Z",
                     "database_snapshot_id": "10:20:"}
        original = audit.transaction_identity_from_fields(**arguments)
        self.assertEqual(original["transaction_started_at_utc"], "2026-09-13T12:30:00.000000Z")
        for changed in ({"backend_pid": 778},
                        {"transaction_started_at_utc": "2026-09-13T12:30:00.000001Z"},
                        {"database_snapshot_id": "10:21:"}):
            self.assertNotEqual(audit.transaction_identity_from_fields(
                **{**arguments, **changed})["transaction_identity_sha256"],
                original["transaction_identity_sha256"])

    def test_transaction_identity_missing_or_malformed_fields_fail_closed(self):
        arguments = {"backend_pid": 777,
                     "transaction_started_at_utc": "2026-09-13T12:30:00Z",
                     "database_snapshot_id": "10:20:"}
        for changed in ({"backend_pid": True}, {"backend_pid": 0},
                        {"backend_pid": 777.0},
                        {"transaction_started_at_utc": "2026-09-13T12:30:00"},
                        {"transaction_started_at_utc": None},
                        {"database_snapshot_id": ""},
                        {"database_snapshot_id": "10:20: 11"},
                        {"database_snapshot_id": "20:10:"},
                        {"database_snapshot_id": "10:20:11,11"},
                        {"database_snapshot_id": "10:20:14,11"},
                        {"database_snapshot_id": "10:20:20"},
                        {"database_snapshot_id": "10:20:9"},
                        {"database_snapshot_id": "10:18446744073709551616:"},
                        {"database_snapshot_id": "010:20:"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                audit.transaction_identity_from_fields(**{**arguments, **changed})

    def setUp(self):
        self.attempt, self.slot, self.events = _anchor()
        self.snapshot = _snapshot()

    def assertUnknown(self, result):
        summary = {key: result.get(key) for key in ("status", "reasons", "source_status")}
        self.assertEqual(result["status"], "UNKNOWN", summary)
        self.assertTrue(result["reasons"], summary)

    def capture_result(self, snapshot=None, *, max_age=MAX_AGE):
        return audit.validate_capture(
            self.snapshot if snapshot is None else snapshot,
            symbol="BTC", decision_time_utc=DECISION,
            max_capture_age_seconds=max_age,
        )

    def outcome_result(self, outcome, event=None):
        return audit.validate_outcome_cell(
            self.events[0] if event is None else event, outcome,
            window_minutes=60, threshold_bps=50, analysis_as_of_utc=AS_OF,
        )

    def test_valid_persisted_anchor_authority_and_no_mutation(self):
        before = deepcopy((self.attempt, self.slot, self.events))
        result = audit.validate_anchor_authority(*before)
        self.assertEqual(result["status"], "VALID", result)
        self.assertEqual(result["reasons"], [])
        self.assertEqual(before, (self.attempt, self.slot, self.events))

    def test_anchor_unknown_on_missing_pair_wrong_version_or_frozen_hash(self):
        for changes in (
            (self.attempt, None, self.events),
            (self.attempt, self.slot, self.events[:1]),
            (self.attempt | {"sampler_version": "old-anchor-version"}, self.slot, self.events),
            (self.attempt, self.slot | {"feature_bundle_sha256": "0" * 64}, self.events),
            (self.attempt, self.slot | {"input_fingerprint": "f" * 64}, self.events),
            (self.attempt, self.slot | {"short_event_id": self.slot["long_event_id"]}, self.events),
        ):
            with self.subTest(changes=changes[1] is None):
                self.assertUnknown(audit.validate_anchor_authority(*changes))
        altered = deepcopy(self.slot)
        altered["decision_feature_bundle"]["features_by_direction"]["LONG"][
            "raw.60m.price_change_pct"
        ] = 1234
        self.assertUnknown(audit.validate_anchor_authority(self.attempt, altered, self.events))

    def test_anchor_event_mismatch_and_improper_delivery_fail_closed(self):
        for key, value in (("symbol", "ETH"), ("direction", "SHORT"),
                           ("delivery_status", "DELIVERED"),
                           ("capture_stage", "POST_SEND"),
                           ("strategy_version", "formula-prospective-neutral-v3"),
                           ("current_price", 1.0)):
            events = deepcopy(self.events)
            events[0][key] = value
            with self.subTest(field=key):
                self.assertUnknown(audit.validate_anchor_authority(self.attempt, self.slot, events))

    def test_hash_consistent_empty_anchor_evidence_is_not_valid_authority(self):
        key_sets = [(name,) for name in ("coverage_snapshot", "source_timestamps", "source_provenance")]
        key_sets.append(("coverage_snapshot", "source_timestamps", "source_provenance"))
        for cleared in key_sets:
            attempt, slot, events = deepcopy((self.attempt, self.slot, self.events))
            references = [event["engine_snapshot"]["prospective_anchor"] for event in events]
            for item in (attempt, slot, *references):
                for key in cleared:
                    item[key] = {}
            fingerprint = anchors.compute_input_fingerprint(**{
                key: attempt[key] for key in (
                    "sampler_version", "coverage_policy_version", "coverage_snapshot", "symbol",
                    "source_candle_open_utc", "source_candle_close_utc", "base_eligible_at_utc",
                    "expires_at_utc", "evaluation_status", "decision_time_utc", "source_timestamps",
                    "source_provenance", "frozen_inputs", "feature_bundle_policy_version",
                    "feature_bundle_sha256",
                )
            })
            for item in (attempt, slot, *references):
                item["input_fingerprint"] = fingerprint
            attempt["attempt_fingerprint"] = anchors._sha256({
                "sampler_version": attempt["sampler_version"],
                "coverage_policy_version": attempt["coverage_policy_version"],
                "symbol": attempt["symbol"], "slot_open_utc": anchors._iso(BASE),
                "status": attempt["evaluation_status"], "reason": attempt["evaluation_reason"],
                "input_fingerprint": fingerprint,
                "event_fingerprints": [event["event_fingerprint"] for event in events],
            })
            with self.subTest(cleared=cleared):
                self.assertUnknown(audit.validate_anchor_authority(attempt, slot, events))

    def test_excluded_and_unevaluable_attempts_are_retained_not_applicable(self):
        for kwargs, expected in (({"eligible": False}, anchors.COVERAGE_EXCLUDED),
                                 ({"missing": True}, anchors.UNEVALUABLE)):
            attempt, slot, events = _anchor(**kwargs)
            self.assertEqual(attempt["evaluation_status"], expected)
            self.assertEqual(audit.validate_anchor_authority(attempt, slot, events)["status"],
                             "NOT_APPLICABLE")

    def test_capture_keeps_unavailable_zero_and_low_no_signal_scores_exact(self):
        before = deepcopy(self.snapshot)
        result = self.capture_result()
        self.assertEqual(result["status"], "VALID", result)
        self.assertEqual(result["coin"], _block(self.snapshot)["coins"]["BTC"])
        models = result["coin"]["models"]
        for name in ("spot_flow", "positioning"):
            self.assertEqual(models[name]["score"], 0)
            self.assertIs(models[name]["available"], False)
            self.assertEqual(models[name]["capture_status"], "UNAVAILABLE")
        self.assertEqual(models["futures_flow"]["score"], -30)
        self.assertEqual(models["futures_flow"]["weighted_score_before_quality"], -40)
        self.assertTrue(any(item["score"] < 65 for item in result["coin"]["maxpain"]
                            if item["score"] is not None))
        self.assertEqual(self.snapshot, before)

    def test_capture_hash_version_source_and_identity_fail_closed(self):
        cases = []
        for field, value in (("version", "watch-operational-scores-v1"),
                             ("hash_version", "other-hash"),
                             ("payload_sha256", "0" * 64),
                             ("cycle_id", "different-cycle")):
            snapshot = deepcopy(self.snapshot)
            _block(snapshot)[field] = value
            cases.append(snapshot)
        cases.extend((self.snapshot | {"source": "BACKFILL"},
                      self.snapshot | {"created_at_utc": None}))
        for snapshot in cases:
            self.assertUnknown(self.capture_result(snapshot))

    def test_capture_causal_times_and_age_boundary(self):
        age = (DECISION - _utc(self.snapshot["created_at_utc"])).total_seconds()
        self.assertEqual(self.capture_result(max_age=age)["status"], "VALID")
        self.assertUnknown(self.capture_result(max_age=age - .001))
        for field in ("available_at_utc", "created_at_utc"):
            self.assertUnknown(self.capture_result(
                self.snapshot | {field: DECISION + timedelta(microseconds=1)}))
        changed = deepcopy(self.snapshot)
        _block(changed)["computed_at_utc"] = (DECISION + timedelta(seconds=1)).isoformat()
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_capture_global_partial_failed_and_rehashed_shape_changes_fail_closed(self):
        for state in ("PARTIAL", "FAILED"):
            changed = deepcopy(self.snapshot)
            _block(changed)["status"] = state
            self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"].pop("ETH")
        self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["maxpain_additive_components"] = []
        self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["sources"]["derivatives_snapshot_sha256"] = "invalid"
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_rehashed_capture_metadata_requires_complete_typed_identity(self):
        mutations = (
            ("code_sha256", {}), ("input_universe_sha256", "not-a-sha256"),
            ("input_row_count", -1), ("input_row_count", True),
            ("input_row_count", 1.5), ("input_row_count", "56"),
            ("source_side_semantics", "SHORT means sell"),
        )
        for key, value in mutations:
            with self.subTest(key=key, value=value):
                changed = deepcopy(self.snapshot)
                _block(changed)[key] = value
                self.assertUnknown(self.capture_result(_rehash(changed)))
        for invalid_cycle in ("", "   ", None, True):
            changed = deepcopy(self.snapshot)
            changed["cycle_id"] = _block(changed)["cycle_id"] = invalid_cycle
            self.assertUnknown(self.capture_result(_rehash(changed)))
        for mutation in ("missing-file", "extra-file", "invalid-hash"):
            changed = deepcopy(self.snapshot)
            hashes = _block(changed)["code_sha256"]
            if mutation == "missing-file":
                hashes.pop("alert_engine.py")
            elif mutation == "extra-file":
                hashes["unknown.py"] = "a" * 64
            else:
                hashes["alert_engine.py"] = "not-a-sha256"
            self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_available_model_and_members_cannot_omit_source_window_references(self):
        for missing in (None, {}):
            changed = deepcopy(self.snapshot)
            future = _block(changed)["coins"]["BTC"]["sources"]["futures"]
            if missing is None:
                future.pop("window_references")
            else:
                future["window_references"] = missing
            self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["sources"]["futures"]["window_references"].pop("30m")
        self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        spot = _block(changed)["coins"]["BTC"]["models"]["spot_flow"]
        spot["time_families"]["now"]["members"][0]["available"] = True
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_rehashed_component_mutation_or_missing_available_window_time_fails_closed(self):
        for value in (999, True, "15"):
            changed = deepcopy(self.snapshot)
            _block(changed)["coins"]["BTC"]["maxpain"][0]["components"][
                "directional_alignment"
            ] = value
            self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["sources"]["futures"]["window_references"][
            "30m"
        ].pop("latest_time")
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_rehashed_duplicate_source_timeframes_or_missing_source_identity_fails_closed(self):
        changed = deepcopy(self.snapshot)
        sources = _block(changed)["coins"]["BTC"]["sources"]
        sources["maxpain_operational_rows"] = [deepcopy(sources["maxpain_operational_rows"][0])
                                               for _ in range(7)]
        self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["sources"]["maxpain_operational_rows"][0].pop("price_source")
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_hype_operational_source_preserved_separately_from_official_overlay(self):
        rows = inputs()
        for row in rows:
            if row["symbol"] == "HYPE":
                row.update(price_source="bybit_futures", price_pair="HYPEUSDT",
                           price_market="PERP", price_instrument="HYPEUSDT")
        snapshot = _snapshot(rows=rows)
        result = audit.validate_capture(snapshot, symbol="HYPE", decision_time_utc=DECISION,
                                        max_capture_age_seconds=MAX_AGE)
        self.assertEqual(result["status"], "VALID", result)
        operational_rows = result["coin"]["sources"]["maxpain_operational_rows"]
        self.assertEqual({row["price_source"] for row in operational_rows}, {"bybit_futures"})
        self.assertEqual(result["snapshot_reference"]["outer_hash_validation"],
                         "REFERENCE_ONLY_NOT_RECOMPUTED")

    def test_source_time_errors_and_hidden_future_source_times_fail_closed(self):
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["source_time_errors"] = ["fixture:FUTURE_TIME"]
        self.assertUnknown(self.capture_result(_rehash(changed)))
        changed = deepcopy(self.snapshot)
        _block(changed)["coins"]["BTC"]["sources"]["maxpain_operational_rows"][0][
            "price_fetched_at_utc"
        ] = (DECISION + timedelta(seconds=1)).isoformat()
        self.assertUnknown(self.capture_result(_rehash(changed)))

    def test_selection_uses_latest_available_or_created_not_scores_or_validity(self):
        earlier = deepcopy(self.snapshot)
        latest = deepcopy(self.snapshot) | {
            "snapshot_set_id": 102,
            "available_at_utc": DECISION - timedelta(minutes=1),
            "created_at_utc": DECISION - timedelta(minutes=1),
        }
        _block(latest)["payload_sha256"] = "0" * 64
        selected = audit.select_prior_capture(
            [latest, earlier], decision_time_utc=DECISION,
            max_capture_age_seconds=MAX_AGE,
        )
        self.assertEqual(selected["snapshot_set_id"], 102)
        self.assertUnknown(self.capture_result(selected))
        with_future_commit = latest | {"created_at_utc": DECISION + timedelta(seconds=1)}
        selected = audit.select_prior_capture(
            [with_future_commit, earlier], decision_time_utc=DECISION,
            max_capture_age_seconds=MAX_AGE,
        )
        self.assertEqual(selected["snapshot_set_id"], earlier["snapshot_set_id"])

    def test_capture_selection_exposes_bad_latest_version_or_absent_block(self):
        other_version = deepcopy(self.snapshot)
        _block(other_version)["version"] = "watch-operational-scores-v1"
        no_block = deepcopy(self.snapshot)
        no_block["source_metadata"]["capture_metadata"].pop("operational_scores")
        for snapshot in (other_version, no_block):
            selected = audit.select_prior_capture(
                [snapshot], decision_time_utc=DECISION,
                max_capture_age_seconds=MAX_AGE,
            )
            self.assertIsNotNone(selected)
            self.assertUnknown(self.capture_result(selected))
        for snapshot in (self.snapshot | {"source": "BACKFILL"},
                         self.snapshot | {"created_at_utc": None}):
            self.assertIsNone(audit.select_prior_capture(
                [snapshot], decision_time_utc=DECISION,
                max_capture_age_seconds=MAX_AGE,
            ))
        self.assertIsNone(audit.select_prior_capture(
            [self.snapshot], decision_time_utc=DECISION, max_capture_age_seconds=1,
        ))

    def test_outcome_terminal_success_and_failure_are_separate(self):
        for status in ("SUCCESS", "FAILURE"):
            result = self.outcome_result(_outcome(self.events[0], status=status))
            self.assertEqual(result["status"], "VALID", result)
            self.assertEqual(result["source_status"], status)
            self.assertEqual(result["reported_status"], status)

    def test_outcome_nonterminal_and_unresolved_statuses_never_become_success(self):
        for status, reported in (("OPEN", "OPEN"), ("UNRESOLVED", "NO_TOUCH"),
                                 ("DATA_MISSING", "DATA_MISSING")):
            result = self.outcome_result(_outcome(self.events[0], status=status))
            self.assertEqual(result["source_status"], status)
            self.assertEqual(result["reported_status"], reported)
            self.assertEqual(result["status"], "UNKNOWN", result)
            self.assertTrue(result["reasons"])
        self.assertUnknown(self.outcome_result(None))

    def test_canonical_same_candle_ambiguity_is_not_success_or_failure(self):
        event = self.events[0]
        price = event["current_price"]
        label = ordered.calculate_ordered_first_touch_outcome(
            reference_price=price, direction=event["direction"], event_time=DECISION,
            candles=[_bar(DECISION, price, high=price * 1.01, low=price * .99)],
            threshold_pct=.5, observation_closed=False, path_complete=True,
        )
        result = self.outcome_result(_outcome(event) | label)
        self.assertEqual(result["source_status"], "UNRESOLVED")
        self.assertEqual(result["reported_status"], "AMBIGUOUS")
        self.assertUnknown(result)

    def test_hype_outcome_requires_persisted_exact_spot_instrument(self):
        event = _anchor(symbol="HYPE")[2][0]
        outcome = _outcome(event) | {
            "price_source": (
                "reference=hyperliquid_spot_@107|path=hyperliquid_spot:HYPE/USDT:1m|"
                "provenance=SELFTEST"
            ),
            "market_pair": "HYPE/USDT",
            "data_quality_status": canonical_price_path.HYPERLIQUID_COMPLETE,
        }
        missing = self.outcome_result(outcome, event)
        self.assertUnknown(missing)
        exact = outcome | {"calculation_audit": {"price_provenance": {"instrument": "@107"}}}
        result = self.outcome_result(exact, event)
        self.assertEqual(result["status"], "VALID", result)
        wrong = outcome | {"calculation_audit": {"price_provenance": {"instrument": "HYPE"}}}
        self.assertUnknown(self.outcome_result(wrong, event))

    def test_outcome_method_identity_route_reference_and_future_fail_closed(self):
        original = _outcome(self.events[0])
        for key, value in (
            ("method_version", "no-dwell-first-touch-v6"),
            ("event_id", 999), ("direction", "SHORT"),
            ("window_minutes", 240), ("threshold_bps", 25),
            ("reference_price", 0), ("price_source", "bybit_futures"),
            ("price_source", "reference=bybit_futures|path=binance_spot:BTCUSDT:1m|provenance=SELFTEST"),
            ("market_pair", "ETHUSDT"),
            ("observed_through_utc", AS_OF + timedelta(seconds=1)),
            ("decision_time_utc", AS_OF + timedelta(seconds=1)),
            ("updated_at_utc", AS_OF + timedelta(seconds=1)),
        ):
            with self.subTest(field=key):
                self.assertUnknown(self.outcome_result(original | {key: value}))

    def test_outcome_duplicate_reference_or_path_segments_are_never_authoritative(self):
        outcome = _outcome(self.events[0])
        source = outcome["price_source"]
        for suffix in (
            "|reference=binance_spot", "|reference=bybit_futures",
            "|path=binance_spot:BTCUSDT:1m", "|path=bybit_futures:BTCUSDT:1m",
        ):
            self.assertUnknown(self.outcome_result(outcome | {"price_source": source + suffix}))

    def test_outcome_source_requires_exact_three_ordered_nonempty_segments(self):
        outcome = _outcome(self.events[0])
        reference, path, provenance = outcome["price_source"].split("|")
        malformed = (
            "|".join((path, reference, provenance)),
            "|".join((reference, path)),
            "|".join((reference, path, "provenance=")),
            "|".join((reference, path, "provenance")),
            "|".join(("reference=", path, provenance)),
            "|".join((reference, "path=", provenance)),
            "|".join((reference, path, "unknown=SELFTEST")),
            outcome["price_source"] + "|extra=value",
            outcome["price_source"] + "|",
        )
        for source in malformed:
            with self.subTest(source=source):
                self.assertUnknown(self.outcome_result(outcome | {"price_source": source}))

    def test_outcome_revision_chronology_allows_creation_before_terminal_evidence(self):
        outcome = _outcome(self.events[0])
        result = self.outcome_result(outcome | {
            "created_at_utc": DECISION + timedelta(seconds=10),
            "updated_at_utc": DECISION + timedelta(minutes=1),
        })
        self.assertEqual(result["status"], "VALID", result["reasons"])
        self.assertUnknown(self.outcome_result(outcome | {
            "created_at_utc": outcome["updated_at_utc"] + timedelta(seconds=1),
        }))
        self.assertUnknown(self.outcome_result(outcome | {
            "created_at_utc": DECISION - timedelta(microseconds=1),
        }))
        self.assertUnknown(self.outcome_result(outcome | {
            "created_at_utc": DECISION + timedelta(seconds=1),
            "updated_at_utc": DECISION + timedelta(seconds=30),
        }))

    def test_valid_causal_parent_membership_and_mismatches(self):
        event = self.events[0]
        membership, parent, bar = _parent(event)
        valid = audit.validate_parent_membership(event, membership, parent, bar)
        self.assertEqual(valid["status"], "VALID", valid)
        for changed in (
            membership | {"event_id": 999},
            membership | {"episode_policy_version": "old-policy"},
            membership | {"btc_parent_movement_id": "not-the-parent"},
            membership | {"decision_time_utc": DECISION - timedelta(minutes=1)},
            membership | {"membership_status": "BOUNDARY_UNVERIFIED"},
        ):
            self.assertUnknown(audit.validate_parent_membership(event, changed, parent, bar))
        self.assertUnknown(audit.validate_parent_membership(event, membership, None, bar))
        self.assertUnknown(audit.validate_parent_membership(event, membership, parent, None))
        self.assertUnknown(audit.validate_parent_membership(
            event, membership, parent | {"price_source": "bybit_futures"}, bar))
        self.assertUnknown(audit.validate_parent_membership(
            event, membership, parent, bar | {"price_source": "bybit_futures"}))

    def test_parent_must_be_known_at_decision_not_future_or_left_boundary(self):
        event = self.events[0]
        membership, parent, bar = _parent(event)
        for changed in (
            parent | {"confirmed_at_utc": DECISION + timedelta(seconds=1)},
            parent | {"evidence_eligible": False},
            parent | {"end_time_utc": DECISION},
        ):
            self.assertUnknown(audit.validate_parent_membership(event, membership, changed, bar))
        self.assertUnknown(audit.validate_parent_membership(
            event, membership, parent, _bar(DECISION)))


class _Rows:
    def __init__(self, rows):
        self.rows = deepcopy(rows)

    def fetchall(self):
        return deepcopy(self.rows)

    def fetchone(self):
        return deepcopy(self.rows[0]) if self.rows else None


class FakeConnection:
    """A finite relation fixture that rejects SQL writes and unnamed params."""

    def __init__(self, *, all_attempts=False, read_only="on", isolation="read committed",
                 autocommit=False):
        attempt, slot, events = _anchor()
        self.attempts = [attempt]
        if all_attempts:
            self.attempts += [
                _anchor(symbol="ETH", attempt_id=2, eligible=False)[0],
                _anchor(symbol="SOL", attempt_id=3, missing=True)[0],
            ]
        self.slots = [slot]
        self.events = events
        self.snapshots = [_snapshot()]
        self.outcomes = [_outcome(event, window=window, threshold_bps=threshold)
                         for event in events for window in (60, 240, 720, 1440)
                         for threshold in (25, 50, 75, 100, 125, 150, 175, 200)]
        relations = [_parent(event) for event in events]
        self.memberships = [row[0] for row in relations]
        self.parents = list({row[1]["btc_parent_movement_id"]: row[1]
                             for row in relations}.values())
        self.bars = list({row[2]["close_time_utc"]: row[2] for row in relations}.values())
        self.read_only, self.isolation = read_only, isolation
        self.autocommit = autocommit
        self.calls = []

    def _cohort(self, params):
        return [row for row in self.attempts
                if row["sampler_version"] == params["sampler_version"]
                and row["symbol"] in params["symbols"]
                and params["start_utc"] <= _utc(row["source_candle_open_utc"]) < params["end_utc"]]

    def execute(self, sql, params=None):
        params = {} if params is None else params
        assert isinstance(params, dict), "audit queries must use named parameters"
        statement = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL).strip()
        assert statement.upper().startswith("SELECT"), "source audit attempted non-SELECT SQL"
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|COMMIT|ROLLBACK|BEGIN|SET)\b",
                             statement, re.IGNORECASE), "source audit attempted a mutation"
        assert not re.search(r"\bOFFSET\b", statement, re.IGNORECASE), "offset pagination is forbidden"
        tags = re.findall(r"/\*\s*audit:([a-z-]+)\s*\*/", sql)
        assert len(tags) == 1, "audit query needs one explicit fixture contract tag"
        tag = tags[0]
        self.calls.append((tag, sql, deepcopy(params)))
        if tag == "transaction":
            rows = [{"read_only": self.read_only, "isolation": self.isolation,
                     "backend_pid": 4242,
                     "transaction_started_at_utc": AS_OF - timedelta(seconds=1),
                     "transaction_snapshot": "100:200:",
                     "observed_at_utc": AS_OF}]
        elif tag == "high-water":
            rows = [{"high_water_attempt_id": max(
                [row["attempt_id"] for row in self._cohort(params)], default=0)}]
        elif tag == "attempts":
            assert "source_candle_open_utc" in sql
            assert "LIMIT %(limit)s" in sql
            assert set(params) == {"sampler_version", "symbols", "start_utc", "end_utc",
                                   "after_attempt_id", "high_water_attempt_id", "limit"}
            rows = sorted([row for row in self._cohort(params)
                           if params["after_attempt_id"] < row["attempt_id"] <= params["high_water_attempt_id"]],
                          key=lambda row: row["attempt_id"])[:params["limit"]]
        elif tag == "slot":
            rows = [row for row in self.slots
                    if row["sampler_version"] == params["sampler_version"]
                    and row["symbol"] == params["symbol"]
                    and _utc(row["source_candle_open_utc"]) == _utc(params["source_candle_open_utc"])]
        elif tag == "events":
            rows = [row for row in self.events if row["event_id"] in params["event_ids"]]
        elif tag == "capture":
            # Independence from the audit's selector prevents a mirrored bug
            # from hiding version/status/availability-based selection bias.
            candidates = []
            for row in self.snapshots:
                if row["source"] != "WATCH_SHARED":
                    continue
                try:
                    durable = max(_utc(row["available_at_utc"]), _utc(row["created_at_utc"]))
                except (TypeError, ValueError):
                    continue
                if timedelta(0) <= params["decision_time_utc"] - durable <= params["max_age"]:
                    candidates.append((durable, row["snapshot_set_id"], row))
            candidates.sort(key=lambda item: item[:2], reverse=True)
            rows = [candidates[0][2]] if candidates else []
        elif tag == "outcomes":
            rows = [row for row in self.outcomes
                    if row["event_id"] in params["event_ids"]
                    and row["method_version"] == params["method_version"]
                    and row["window_minutes"] in params["windows"]
                    and row["threshold_bps"] in params["thresholds_bps"]][:params["limit"]]
        elif tag == "memberships":
            rows = [row for row in self.memberships if row["event_id"] in params["event_ids"]
                    and row["episode_policy_version"] == params["parent_policy"]]
        elif tag == "parents":
            rows = [row for row in self.parents
                    if row["btc_parent_movement_id"] in params["parent_ids"]
                    and row["episode_policy_version"] == params["parent_policy"]]
        elif tag == "bars":
            times = {_utc(value) for value in params["close_times"]}
            rows = [row for row in self.bars if row["close_time_utc"] in times]
        else:
            raise AssertionError("unhandled audit read: " + tag)
        return _Rows(rows)

    def commit(self):
        raise AssertionError("audit must not commit the caller transaction")

    def rollback(self):
        raise AssertionError("audit must not roll back the caller transaction")

    def close(self):
        raise AssertionError("audit must not close the caller connection")


class ReaderAuditTests(unittest.TestCase):
    def setUp(self):
        self.params = {"symbols": ["BTC", "ETH", "SOL"], "start_utc": BASE,
                       "end_utc": BASE + timedelta(minutes=30),
                       "max_capture_age_seconds": MAX_AGE}

    def page(self, conn, **kwargs):
        # Any accidental external connection/source read is a hard test
        # failure, even if credentials happen to be configured locally.
        with patch("socket.create_connection", side_effect=AssertionError("network prohibited")), \
             patch("requests.sessions.Session.request", side_effect=AssertionError("HTTP prohibited")), \
             patch("psycopg.connect", side_effect=AssertionError("database creation prohibited")):
            return audit.audit_anchor_attempt_page_from_connection(conn, **(self.params | kwargs))

    def test_complete_intersection_is_64_explicit_cells_without_delivery_inference(self):
        conn = FakeConnection()
        result = self.page(conn)
        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertEqual(row["anchor_authority"]["status"], "VALID", row["anchor_authority"])
        self.assertEqual(row["capture"]["status"], "VALID", row["capture"])
        self.assertEqual(len(row["outcome_cells"]), 64)
        self.assertTrue(all(cell["status"] == "VALID" for cell in row["outcome_cells"]))
        self.assertEqual({key: value["status"] for key, value in row["parent_memberships"].items()},
                         {"LONG": "VALID", "SHORT": "VALID"})
        self.assertEqual(row["delivery_state"], "UNKNOWN_NOT_AUDITED")
        self.assertFalse(result["cross_page_snapshot_guaranteed"])
        self.assertEqual(result["snapshot_consistency"], "STATEMENT_SNAPSHOTS_NOT_ATOMIC")
        self.assertIsNone(result["next_cursor"])
        self.assertEqual(result["examined"], 1)
        self.assertEqual(result["emitted"], len(result["rows"]))
        self.assertIs(result["has_more"], False)
        self.assertIs(result["population_page_complete"], True)
        limits = [params["limit"] for tag, _, params in conn.calls if tag == "attempts"]
        self.assertEqual(limits, [101])
        expected_transaction = audit.transaction_identity_from_fields(
            backend_pid=4242, transaction_started_at_utc=AS_OF - timedelta(seconds=1),
            database_snapshot_id="100:200:")
        self.assertEqual(result["transaction_identity_sha256"],
                         expected_transaction["transaction_identity_sha256"])

    def test_excluded_unevaluable_missing_labels_and_parent_rows_never_disappear(self):
        conn = FakeConnection(all_attempts=True)
        missing = conn.outcomes.pop(0)
        conn.memberships.pop()
        result = self.page(conn)
        self.assertEqual([row["attempt"]["attempt_id"] for row in result["rows"]], [1, 2, 3])
        first, excluded, unavailable = result["rows"]
        self.assertEqual(excluded["attempt"]["evaluation_status"], "COVERAGE_EXCLUDED")
        self.assertEqual(unavailable["attempt"]["evaluation_status"], "UNEVALUABLE")
        for row in (excluded, unavailable):
            self.assertEqual(row["anchor_authority"]["status"], "NOT_APPLICABLE")
            self.assertEqual(len(row["outcome_cells"]), 64)
            self.assertTrue(all(cell["status"] == "UNKNOWN" for cell in row["outcome_cells"]))
        absent = [cell for cell in first["outcome_cells"]
                  if cell["event_id"] == missing["event_id"]
                  and cell["window_minutes"] == missing["window_minutes"]
                  and cell["threshold_bps"] == missing["threshold_bps"]]
        self.assertEqual(len(absent), 1)
        self.assertEqual(absent[0]["reported_status"], "UNKNOWN")
        self.assertEqual(first["parent_memberships"]["SHORT"]["status"], "UNKNOWN")
        self.assertEqual(sum(tag == "capture" for tag, _, _ in conn.calls), 1)

    def test_latest_bad_capture_is_selected_before_validation_and_not_replaced(self):
        for block_state in ("missing", "v1", "failed", "hash-mismatch"):
            conn = FakeConnection()
            latest = deepcopy(conn.snapshots[0]) | {
                "snapshot_set_id": 102, "available_at_utc": DECISION - timedelta(seconds=1),
                "created_at_utc": DECISION - timedelta(seconds=1),
            }
            if block_state == "missing":
                latest["source_metadata"]["capture_metadata"].pop("operational_scores")
            elif block_state == "v1":
                _block(latest)["version"] = "watch-operational-scores-v1"
            elif block_state == "failed":
                _block(latest)["status"] = "FAILED"
                _rehash(latest)
            else:
                _block(latest)["payload_sha256"] = "0" * 64
            conn.snapshots.append(latest)
            row = self.page(conn)["rows"][0]
            self.assertEqual(row["capture"]["snapshot_reference"]["snapshot_set_id"], 102)
            self.assertEqual(row["capture"]["status"], "UNKNOWN")
            capture_sql = next(sql for tag, sql, _ in conn.calls if tag == "capture")
            where = capture_sql.lower().split("where", 1)[1].split("order by", 1)[0]
            for forbidden in ("operational_scores", "capture_version", "payload_sha256",
                              "research_eligible", "collection_status", "score", "validation_status"):
                self.assertNotIn(forbidden, where)

    def test_keyset_high_water_excludes_later_attempts_and_keeps_original_denominator(self):
        conn = FakeConnection(all_attempts=True)
        first = self.page(conn, page_size=1)
        self.assertEqual(first["high_water_attempt_id"], 3)
        self.assertEqual(first["next_cursor"]["after_attempt_id"], 1)
        conn.attempts.append(_anchor(symbol="ETH", attempt_id=99, eligible=False)[0])
        second = self.page(conn, page_size=1, cursor=first["next_cursor"])
        third = self.page(conn, page_size=1, cursor=second["next_cursor"])
        self.assertEqual([page["rows"][0]["attempt"]["attempt_id"]
                          for page in (first, second, third)], [1, 2, 3])
        self.assertEqual([page["examined"] for page in (first, second, third)], [1, 1, 1])
        for page in (first, second, third):
            self.assertEqual(page["emitted"], len(page["rows"]))
            # LIMIT+1 detects continuation; it is not a second examination
            # of the boundary attempt before that attempt's own page.
            self.assertEqual(page["examined"], page["emitted"])
            self.assertIs(page["has_more"], page["next_cursor"] is not None)
            self.assertIs(page["population_page_complete"], page["next_cursor"] is None)
        self.assertIs(first["population_page_complete"], False)
        self.assertIs(second["population_page_complete"], False)
        self.assertIs(third["population_page_complete"], True)
        self.assertIsNone(third["next_cursor"])
        self.assertEqual(sum(tag == "high-water" for tag, _, _ in conn.calls), 1)
        self.assertEqual([params["limit"] for tag, _, params in conn.calls if tag == "attempts"],
                         [2, 2, 2])

    def test_cursor_bound_to_full_query_and_tampering_rejected_before_sql(self):
        conn = FakeConnection(all_attempts=True)
        cursor = self.page(conn, page_size=1)["next_cursor"]
        changes = (
            {"symbols": ["BTC"]}, {"start_utc": BASE - timedelta(minutes=30)},
            {"end_utc": BASE + timedelta(hours=1)}, {"max_capture_age_seconds": MAX_AGE + 1},
            {"windows": (60,)}, {"thresholds_bps": (25,)}, {"page_size": 2},
            {"cursor": cursor | {"after_attempt_id": 2}},
            {"cursor": cursor | {"high_water_attempt_id": 999}},
            {"cursor": "not-a-cursor"},
        )
        for change in changes:
            with self.subTest(change=change):
                before = len(conn.calls)
                with self.assertRaises(ValueError):
                    self.page(conn, **({"page_size": 1, "cursor": cursor} | change))
                self.assertEqual(len(conn.calls), before)

    def test_invalid_bounds_rejected_and_source_candle_cohort_is_half_open(self):
        conn = FakeConnection()
        for change in (
            {"page_size": 0}, {"page_size": 101}, {"page_size": True},
            {"max_capture_age_seconds": 0}, {"max_capture_age_seconds": float("nan")},
            {"max_capture_age_seconds": True}, {"symbols": []}, {"symbols": "BTC"},
            {"symbols": ["BTC'); DROP TABLE research_events; --"]},
            {"windows": (60, 60)}, {"thresholds_bps": (51,)},
            {"start_utc": BASE.replace(tzinfo=None)}, {"end_utc": BASE},
        ):
            with self.assertRaises(ValueError):
                self.page(conn, **change)
        self.assertEqual(conn.calls, [])
        self.assertEqual(self.page(conn)["rows"][0]["attempt"]["attempt_id"], 1)
        empty = self.page(conn, start_utc=BASE - timedelta(minutes=30), end_utc=BASE)
        self.assertEqual(empty["rows"], [])
        self.assertEqual(empty["examined"], 0)
        self.assertEqual(empty["emitted"], 0)
        self.assertIs(empty["has_more"], False)
        self.assertIsNone(empty["next_cursor"])
        self.assertIs(empty["population_page_complete"], True)

    def test_readonly_required_and_snapshot_claim_matches_actual_transaction(self):
        conn = FakeConnection(read_only="off")
        with self.assertRaises(ValueError):
            self.page(conn)
        self.assertEqual([tag for tag, _, _ in conn.calls], ["transaction"])
        for isolation, autocommit, expected in (
            ("read committed", False, "STATEMENT_SNAPSHOTS_NOT_ATOMIC"),
            ("repeatable read", False, "CALLER_TRANSACTION_SNAPSHOT"),
            ("serializable", False, "CALLER_TRANSACTION_SNAPSHOT"),
            ("repeatable read", True, "STATEMENT_SNAPSHOTS_NOT_ATOMIC"),
        ):
            result = self.page(FakeConnection(isolation=isolation, autocommit=autocommit))
            self.assertEqual(result["snapshot_consistency"], expected)
            self.assertFalse(result["cross_page_snapshot_guaranteed"])

    def test_absolute_deadline_is_checked_between_source_queries(self):
        ticks = iter((0.0, 0.1, 0.2, 0.3))
        conn = FakeConnection()
        with self.assertRaises(audit.AuditDeadlineExceeded):
            self.page(
                conn, absolute_deadline_monotonic=0.25,
                monotonic=lambda: next(ticks),
            )
        self.assertEqual([tag for tag, _, _ in conn.calls], ["transaction", "high-water"])

    def test_tuple_row_connection_fails_before_any_cohort_read(self):
        class TupleConnection(FakeConnection):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                return _Rows([("on", "read committed", AS_OF)])

        conn = TupleConnection()
        with self.assertRaises(ValueError) as caught:
            self.page(conn)
        self.assertEqual(str(caught.exception),
                         "audit connection must return mapping rows; configure psycopg.rows.dict_row")
        self.assertEqual([tag for tag, _, _ in conn.calls], ["transaction"])

    def test_duplicate_projection_rows_fail_closed_in_every_join(self):
        class DuplicateConnection(FakeConnection):
            def __init__(self, duplicate_tag):
                super().__init__()
                self.duplicate_tag = duplicate_tag

            def execute(self, sql, params=None):
                result = super().execute(sql, params)
                if self.calls[-1][0] == self.duplicate_tag:
                    rows = result.fetchall()
                    self.assert_fixture_nonempty(rows)
                    return _Rows(rows + [rows[0]])
                return result

            @staticmethod
            def assert_fixture_nonempty(rows):
                assert rows, "duplicate test must corrupt an actual joined row"

        for tag in ("slot", "events", "capture", "outcomes", "memberships", "parents", "bars"):
            with self.subTest(tag=tag):
                conn = DuplicateConnection(tag)
                with self.assertRaises(ValueError):
                    self.page(conn)


if __name__ == "__main__":
    unittest.main()
