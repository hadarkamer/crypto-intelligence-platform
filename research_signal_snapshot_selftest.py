"""Deterministic checks for the silent Stage-4 signal snapshot contract."""
from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path

import alert_engine
import magnet_v1
import market_confidence_engine
import research_event_capture
import research_max_pain_archive as archive
import research_signal_snapshot as snapshots
from research_max_pain_archive_selftest import BASE, _payload


def _market_evidence(
    direction: str, *, futures: bool = True, maxpain_score: float = 0.0
) -> dict:
    bullish = direction == "LONG"
    sign = 1.0 if bullish else -1.0
    relation = "SUPPORT"
    evidence = {
        "symbol": "BTC",
        "expected_price_direction": "BULLISH" if bullish else "BEARISH",
        "maxpain_score": maxpain_score,
        "modules": {
            "positioning": {
                "family": "Price+OI",
                "available": True,
                "direction": "BULLISH" if bullish else "BEARISH",
                "relation": relation,
                "score": 71.0 * sign,
                "state": "BULLISH_BUILDUP" if bullish else "BEARISH_BUILDUP",
                "time_families": {
                    "now": {
                        "key": "NOW",
                        "direction": "BULLISH" if bullish else "BEARISH",
                        "quality": 0.9,
                        "weight": 35.0,
                        "available_windows": 1,
                        "configured_windows": 1,
                        "coverage": 1.0,
                        "net": 0.71 * sign,
                        "contribution": 24.85 * sign,
                        "members": ["30m"],
                    }
                },
            },
            "futures_flow": {
                "family": "Futures Flow",
                "available": futures,
                "direction": (
                    "BULLISH" if bullish else "BEARISH"
                ) if futures else "NEUTRAL",
                "relation": relation if futures else "NEUTRAL",
                "score": 69.0 * sign if futures else 0.0,
                "state": "BULLISH" if bullish else "BEARISH",
                "time_families": {},
            },
            "spot_flow": {
                "family": "Spot Flow",
                "available": True,
                "direction": "BEARISH" if bullish else "BULLISH",
                "relation": "OPPOSE",
                "score": -99.0 * sign,
                "state": "DIVERGENCE",
                "time_families": {},
            },
        },
    }
    expected = evidence["expected_price_direction"]
    conclusion = market_confidence_engine._conclusion(
        evidence["modules"], expected
    )
    evidence.update(conclusion)
    evidence["confirmation"] = market_confidence_engine._confirmation(
        maxpain_score, expected, evidence["modules"], conclusion
    )
    evidence["note"] = (
        "Confirmation is read-only; existing Max-Pain score and ranking are unchanged."
    )
    return evidence


def _derivatives(*, collected_at=None) -> dict:
    collected = collected_at or BASE + timedelta(minutes=5, seconds=20)
    latest = BASE + timedelta(minutes=5)
    return {
        "BTC": {
            "regime": {
                "symbol": "BTC",
                "source_snapshot_id": 91,
                "collected_at": collected.isoformat(),
                "price_fetched_at": collected.isoformat(),
                "oi_fetched_at": collected.isoformat(),
                "time_gap_seconds": 0.0,
                "data_quality_status": "PASS",
                "price_source": "binance_spot",
                "oi_source": "coinglass",
                "available": True,
                "windows": {},
                "overall": {"state": "BEARISH_BUILDUP"},
            },
            "flow": {
                "futures": {
                    "symbol": "BTC",
                    "market": "futures",
                    "available": True,
                    "windows": {},
                    "overall": {"direction": "BEARISH"},
                    "quality": {
                        "status": "PASS",
                        "freshness_status": "FRESH",
                        "usable_for_confirmation": True,
                        "rows": 80,
                        "latest_time": latest.isoformat(),
                        "candle_close": latest.isoformat(),
                        "continuous_cvd_check": "PASS",
                    },
                },
                "spot": {
                    "symbol": "BTC",
                    "market": "spot",
                    "available": True,
                    "windows": {},
                    "overall": {"direction": "BULLISH"},
                    "quality": {
                        "status": "PASS",
                        "freshness_status": "FRESH",
                        "usable_for_confirmation": True,
                        "rows": 80,
                        "latest_time": latest.isoformat(),
                        "candle_close": latest.isoformat(),
                        "continuous_cvd_check": "PASS",
                    },
                },
            },
        }
    }


def _strong_payload(
    *,
    long_target_step: float = 0.0005,
    max_short_price: float = 110.0,
    fixture_name: str = "strong",
) -> dict:
    raw_rows = []
    enriched_rows = []
    for rank, timeframe in enumerate(archive.REQUIRED_TIMEFRAMES, start=1):
        long_target = 99.0 + (rank - 1) * long_target_step
        long_amount = 100.0 * (1.4 ** (rank - 1))
        raw_rows.append(
            {
                "symbol": "BTC",
                "rank": rank,
                "timeframe": timeframe,
                "price": 100.0,
                "max_short_price": max_short_price,
                "max_long_price": long_target,
                "short_amount_usd": 50.0,
                "long_amount_usd": long_amount,
                "collected_at_utc": BASE.isoformat(),
            }
        )
        enriched_rows.append(
            {
                "symbol": "BTC",
                "rank": rank,
                "timeframe": timeframe,
                "current_price": 100.0,
                "source_observed_at_utc": BASE.isoformat(),
                "short_max_pain": max_short_price,
                "long_max_pain": long_target,
                "short_liquidation_amount": 50.0,
                "long_liquidation_amount": long_amount,
                "price_fetched_at_utc": (
                    BASE + timedelta(minutes=4)
                ).isoformat(),
                "price_observed_at_utc": (
                    BASE + timedelta(minutes=4)
                ).isoformat(),
                "price_interval": "1m",
                "price_source": "binance_spot",
                "price_pair": "BTCUSDT",
            }
        )
    return archive.build_snapshot_payload(
        cycle_id=f"selftest:RESEARCH_PASSIVE:{fixture_name}",
        cycle_time_utc=BASE,
        collection_started_at_utc=BASE,
        collection_completed_at_utc=BASE + timedelta(minutes=5),
        source="RESEARCH_PASSIVE",
        collector_version="selftest-v1",
        snapshot={
            "ok": True,
            "rows": raw_rows,
            "row_count": len(raw_rows),
            "missing_timeframes": [],
            "duplicate_pairs": [],
        },
        enriched_rows=enriched_rows,
        live_result={"skipped_symbols": []},
    )


def _confirmed_payload() -> dict:
    """Fixture that naturally emits exact CONFIRMED Max-Pain and Magnet events."""

    return _strong_payload(
        long_target_step=0.1,
        max_short_price=102.0,
        fixture_name="confirmed",
    )


def _canonical_inputs(payload: dict, derivatives: dict):
    rows_by_symbol = {}
    eligible = {
        item["symbol"]
        for item in payload["symbols"]
        if item.get("research_eligible") is True
    }
    for row in payload["rows"]:
        if row["symbol"] in eligible:
            rows_by_symbol.setdefault(row["symbol"], []).append(dict(row))
    scoring_rows = snapshots._scoring_rows_from_archive(rows_by_symbol)
    selected_derivatives = {
        symbol: derivatives[symbol] for symbol in sorted(eligible)
    }
    return (
        snapshots._canonical_opportunities(scoring_rows, selected_derivatives),
        snapshots._canonical_magnet_observations(
            scoring_rows, selected_derivatives
        ),
        snapshots._canonical_directional_evidence(selected_derivatives),
    )


def _opportunity(payload: dict, timeframe: str, tier: str = "STRONG_CONFIRMED") -> dict:
    item = next(
        value
        for value in alert_engine.build_opportunities(
            payload["rows"], limit=len(payload["rows"])
        )
        if value["timeframe"] == timeframe
    )
    score = 70.0 if tier == "CONFIRMED" else 20.0 if tier not in {
        "CONFIRMED",
        "STRONG_CONFIRMED",
    } else 84.0
    evidence = _market_evidence("SHORT", maxpain_score=score)
    item.update(
        {
            "score": score,
            "priority": score,
            "raw_score": score,
            "types": ["A", "B", "C"],
            "near_share_pct": 70.0,
            "cluster_count": 4,
            "cluster_spread_pct": 0.6,
            "cluster_median_target": 95.2,
            "cluster_members": ["12h", "24h", "48h", "3d"],
            "relative_gap_advantage": 1.5,
            "near_distance_pct": 5.0,
            "far_distance_pct": 10.0,
            "maxpain_confirmation": evidence["confirmation"],
            "market_evidence": evidence,
        }
    )
    item["components"] = {
        **dict(item.get("components") or {}),
        "consensus": 29.0,
        "target_proximity": 25.0,
        "cluster_confidence": 20.0,
        "relative_gap": 10.0,
    }
    return item


def _magnet(payload: dict) -> dict:
    magnet = next(
        value
        for value in magnet_v1.build_magnets(payload["rows"])
        if value["symbol"] == "BTC" and value["side"] == "LOWER"
    )
    evidence = _market_evidence("SHORT")
    return {
        "magnet": magnet,
        "confirmation": magnet_v1.evaluate_confirmation(magnet, evidence),
        "market_evidence": evidence,
        "current_price": 100.0,
        "price_source": "binance_spot",
        "price_pair": "BTCUSDT",
    }


def _build(
    payload: dict,
    *,
    opportunities=(),
    magnets=(),
    directional=None,
    derivatives=None,
    snapshot_set_id=17,
) -> snapshots.SignalSnapshotBatch:
    return snapshots.build_signal_snapshot_batch(
        archive_payload=payload,
        archive_persistence={
            "persisted": True,
            "snapshot_set_id": snapshot_set_id,
        },
        opportunities=opportunities,
        magnet_observations=magnets,
        derivatives_snapshot=derivatives or _derivatives(),
        directional_market_evidence=directional or {},
        derivatives_read_started_at_utc=BASE + timedelta(minutes=5, seconds=10),
        derivatives_read_completed_at_utc=BASE + timedelta(minutes=5, seconds=30),
        decision_time_utc=BASE + timedelta(minutes=6),
        code_version="selftest",
    )


def _event(batch, event_type: str):
    return next(event for event in batch.events if event.event_type == event_type)


def _raises(callable_, text: str) -> None:
    try:
        callable_()
    except (ValueError, TypeError) as exc:
        assert text.lower() in str(exc).lower(), str(exc)
    else:
        raise AssertionError(f"expected failure containing {text!r}")


def run() -> None:
    payload = _strong_payload()
    derivatives = _derivatives()
    opportunities, magnets, directional = _canonical_inputs(
        payload, derivatives
    )
    batch = _build(
        payload,
        opportunities=opportunities,
        magnets=magnets,
        directional=directional,
        derivatives=derivatives,
    )
    assert batch.counts == {"max_pain": 7, "magnet": 1, "combined": 1}
    assert all(event.event_kind == "DECISION_SAMPLE" for event in batch.events)
    assert all("SILENT" in event.categories for event in batch.events)
    assert all(
        event.engine_snapshot["signal_snapshot"]["formula_authorized"] is False
        for event in batch.events
    )
    projection = _event(batch, snapshots.PROJECTION_EVENT_TYPE)
    assert projection.engine_snapshot["projection"]["status"] == "COMPLETED"
    assert projection.engine_snapshot["projection"]["evaluation_status"] == (
        "EVALUABLE"
    )
    assert projection.engine_snapshot["projection"]["signal_event_count"] == 9
    assert batch.evaluated_symbols == ("BTC",)
    assert batch.unevaluable_symbols == ()
    assert batch.evaluation_status == "EVALUABLE"
    assert batch.events[-1].event_type == snapshots.PROJECTION_EVENT_TYPE
    assert all(
        event.event_type != snapshots.PROJECTION_EVENT_TYPE
        for event in batch.events[:-1]
    )

    signal_events = tuple(
        event
        for event in batch.events
        if event.event_type != snapshots.PROJECTION_EVENT_TYPE
    )
    frozen_commitment = projection.engine_snapshot["projection"][
        "signal_events_payload_sha256"
    ]
    assert frozen_commitment == snapshots._signal_events_payload_sha256(
        signal_events
    )
    assert frozen_commitment == snapshots._signal_events_payload_sha256(
        tuple(reversed(signal_events))
    )
    assert snapshots._signal_events_payload_sha256(()) == (
        "4531e089c2e1f379c94c07b4b14c4b53fa1e8caf146ac6cc42249b83150907fc"
    )
    mutated_snapshot = deepcopy(signal_events[0].engine_snapshot)
    mutated_snapshot["commitment_selftest"] = True
    assert snapshots._signal_events_payload_sha256(
        (replace(signal_events[0], engine_snapshot=mutated_snapshot),)
        + signal_events[1:]
    ) != frozen_commitment
    assert snapshots._signal_events_payload_sha256(signal_events[:-1]) != (
        frozen_commitment
    )
    assert snapshots._signal_events_payload_sha256(
        (replace(signal_events[0], score=65.00000000000001),)
        + signal_events[1:]
    ) != frozen_commitment

    unavailable_oi = _derivatives()
    unavailable_oi["BTC"]["regime"]["available"] = False
    unavailable_oi_batch = _build(
        payload,
        opportunities=opportunities,
        magnets=magnets,
        derivatives=unavailable_oi,
    )
    unavailable_oi_projection = _event(
        unavailable_oi_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]
    assert unavailable_oi_batch.counts == {
        "max_pain": 0,
        "magnet": 0,
        "combined": 0,
    }
    assert unavailable_oi_batch.evaluated_symbols == ()
    assert unavailable_oi_batch.unevaluable_symbols == ("BTC",)
    assert unavailable_oi_batch.evaluation_status == "UNEVALUABLE"
    assert unavailable_oi_projection["evaluation_status"] == "UNEVALUABLE"
    assert unavailable_oi_projection["symbol_evaluations"] == [
        {
            "symbol": "BTC",
            "status": "UNEVALUABLE",
            "reason": "PRICE_OI_UNAVAILABLE",
        }
    ]
    assert unavailable_oi_batch.events == (
        _event(unavailable_oi_batch, snapshots.PROJECTION_EVENT_TYPE),
    )
    unavailable_futures = _derivatives()
    unavailable_futures["BTC"]["flow"]["futures"]["quality"][
        "usable_for_confirmation"
    ] = False
    unavailable_futures_batch = _build(
        payload,
        opportunities=opportunities,
        magnets=magnets,
        derivatives=unavailable_futures,
    )
    assert unavailable_futures_batch.evaluation_status == "UNEVALUABLE"
    assert _event(
        unavailable_futures_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"] == [
        {
            "symbol": "BTC",
            "status": "UNEVALUABLE",
            "reason": "FUTURES_CVD_UNAVAILABLE",
        }
    ]

    combined = _event(batch, snapshots.COMBINED_EVENT_TYPE)
    combined_state = combined.engine_snapshot
    assert combined.direction == "SHORT" and combined.source_side == "LONG"
    assert combined_state["vote_count"] == 3
    assert combined_state["source_families"] == [
        "COINGLASS_MAX_PAIN",
        "FUTURES_CVD",
        "PRICE_OI",
    ]
    assert combined_state["indication_families"] == [
        "FUTURES_CVD",
        "MAGNET",
        "MAX_PAIN",
        "PRICE_OI",
    ]
    assert len(combined_state["maxpain_components"]) == 7
    assert combined_state["source_families"].count("COINGLASS_MAX_PAIN") == 1
    assert "SPOT_CVD" not in combined_state["source_families"]
    assert combined_state["spot_context"]["status"] == "DIVERGING"
    assert set(combined_state["dependency_lineage"]) == set(
        combined_state["source_families"]
    )

    maxpain = _event(batch, snapshots.MAX_PAIN_EVENT_TYPE)
    compact = maxpain.engine_snapshot
    assert compact["cluster"]["members"] == list(archive.REQUIRED_TIMEFRAMES)
    assert "median_target" not in compact["cluster"]
    assert "average_target" not in compact["cluster"]
    assert maxpain.score is not None and maxpain.score > 80.0
    assert "STRONG_CONFIRMED" in maxpain.categories
    assert len(json.dumps(compact, sort_keys=True).encode("utf-8")) <= (
        research_event_capture.MAX_ENGINE_SNAPSHOT_BYTES
    )

    derived_again = _build(payload, derivatives=derivatives)
    assert [event.event_fingerprint for event in batch.events] == [
        event.event_fingerprint for event in derived_again.events
    ]
    reordered = _build(
        payload,
        opportunities=list(reversed(opportunities)),
        magnets=list(reversed(magnets)),
        directional=directional,
        derivatives=derivatives,
    )
    assert _event(reordered, snapshots.COMBINED_EVENT_TYPE).event_fingerprint == (
        combined.event_fingerprint
    )
    assert combined.timeframe is None

    confirmed_payload = _confirmed_payload()
    confirmed_opportunities, confirmed_magnets, confirmed_directional = (
        _canonical_inputs(confirmed_payload, derivatives)
    )
    confirmed_batch = _build(
        confirmed_payload,
        opportunities=confirmed_opportunities,
        magnets=confirmed_magnets,
        directional=confirmed_directional,
        derivatives=derivatives,
    )
    confirmed_maxpain_events = [
        event
        for event in confirmed_batch.events
        if event.event_type == snapshots.MAX_PAIN_EVENT_TYPE
    ]
    confirmed_magnet_events = [
        event
        for event in confirmed_batch.events
        if event.event_type == snapshots.MAGNET_EVENT_TYPE
    ]
    assert {
        event.timeframe for event in confirmed_maxpain_events
    } == {"12h", "24h"}
    assert len(confirmed_magnet_events) == 1
    assert all(
        "CONFIRMED" in event.categories
        and "STRONG_CONFIRMED" not in event.categories
        and event.engine_snapshot["signal_snapshot"]["tier"] == "CONFIRMED"
        for event in (*confirmed_maxpain_events, *confirmed_magnet_events)
    )
    confirmed_combined = _event(
        confirmed_batch, snapshots.COMBINED_EVENT_TYPE
    ).engine_snapshot
    assert confirmed_combined["vote_count"] == 3
    assert confirmed_combined["source_families"] == [
        "COINGLASS_MAX_PAIN",
        "FUTURES_CVD",
        "PRICE_OI",
    ]
    assert {
        "MAGNET",
        "MAX_PAIN",
    }.issubset(confirmed_combined["indication_families"])
    assert confirmed_combined["source_families"].count(
        "COINGLASS_MAX_PAIN"
    ) == 1

    low_payload = _payload(source="RESEARCH_PASSIVE")
    derivatives_only = _build(
        low_payload,
        derivatives=derivatives,
    )
    derivative_combined = _event(
        derivatives_only, snapshots.COMBINED_EVENT_TYPE
    )
    assert derivative_combined.direction == "SHORT"
    assert derivative_combined.engine_snapshot["source_families"] == [
        "FUTURES_CVD",
        "PRICE_OI",
    ]

    one_core = _derivatives()
    one_core["BTC"]["flow"]["futures"]["overall"]["direction"] = "BULLISH"
    one_core["BTC"]["flow"]["spot"]["overall"]["direction"] = "BEARISH"
    spot_only = _build(
        low_payload,
        derivatives=one_core,
    )
    assert spot_only.counts["combined"] == 0

    archive_partial = _payload(
        symbols=("BTC", "HYPE"),
        source="RESEARCH_PASSIVE",
        generic_hype=True,
    )
    archive_partial_batch = _build(archive_partial, derivatives=derivatives)
    assert archive_partial_batch.eligible_symbols == ("BTC",)

    mixed = _payload(
        symbols=("BTC", "ETH"),
        source="RESEARCH_PASSIVE",
    )
    mixed_batch = _build(mixed, derivatives=derivatives)
    mixed_projection = _event(
        mixed_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]
    assert mixed_batch.eligible_symbols == ("BTC", "ETH")
    assert mixed_batch.evaluated_symbols == ("BTC",)
    assert mixed_batch.unevaluable_symbols == ("ETH",)
    assert mixed_batch.evaluation_status == "PARTIAL"
    assert mixed_projection["evaluation_status"] == "PARTIAL"
    assert mixed_projection["symbol_evaluations"] == [
        {"symbol": "BTC", "status": "EVALUABLE", "reason": None},
        {
            "symbol": "ETH",
            "status": "UNEVALUABLE",
            "reason": "DERIVATIVES_SNAPSHOT_MISSING",
        },
    ]
    assert all(
        event.symbol == "BTC"
        for event in mixed_batch.events
        if event.event_type != snapshots.PROJECTION_EVENT_TYPE
    )
    assert mixed_batch.events[-1].event_type == snapshots.PROJECTION_EVENT_TYPE

    malformed_nested = _derivatives()
    malformed_nested["ETH"] = {"regime": 7}
    malformed_batch = _build(mixed, derivatives=malformed_nested)
    malformed_projection = _event(
        malformed_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]
    assert malformed_batch.evaluated_symbols == ("BTC",)
    assert malformed_projection["symbol_evaluations"] == [
        {"symbol": "BTC", "status": "EVALUABLE", "reason": None},
        {
            "symbol": "ETH",
            "status": "UNEVALUABLE",
            "reason": "DERIVATIVES_SNAPSHOT_INVALID",
        },
    ]
    assert any(
        event.symbol == "BTC"
        for event in malformed_batch.events
        if event.event_type != snapshots.PROJECTION_EVENT_TYPE
    )

    malformed_engine = _derivatives()
    malformed_engine["ETH"] = deepcopy(malformed_engine["BTC"])
    malformed_engine["ETH"]["regime"]["symbol"] = "ETH"
    malformed_engine["ETH"]["flow"]["futures"]["symbol"] = "ETH"
    malformed_engine["ETH"]["flow"]["spot"]["symbol"] = "ETH"
    malformed_engine["ETH"]["regime"]["overall"] = 7
    malformed_engine_batch = _build(mixed, derivatives=malformed_engine)
    assert _event(
        malformed_engine_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][1] == {
        "symbol": "ETH",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }
    for path, replacement, rejection in (
        (("regime", "windows"), [], "DERIVATIVES_SNAPSHOT_INVALID"),
        (("flow", "futures", "windows"), [], "DERIVATIVES_SNAPSHOT_INVALID"),
        (("flow", "spot", "windows"), [], "DERIVATIVES_SNAPSHOT_INVALID"),
        (("regime", "data_quality_status"), "pass", "DERIVATIVES_SNAPSHOT_INVALID"),
        (("regime", "price_source"), 123, "DERIVATIVES_SNAPSHOT_INVALID"),
        (
            ("flow", "futures", "quality", "freshness_status"),
            "fresh",
            "FUTURES_CVD_UNAVAILABLE",
        ),
        (
            ("flow", "futures", "quality", "continuous_cvd_check"),
            "pass",
            "FUTURES_CVD_UNAVAILABLE",
        ),
    ):
        malformed_value = _derivatives()
        malformed_value["ETH"] = deepcopy(malformed_value["BTC"])
        malformed_value["ETH"]["regime"]["symbol"] = "ETH"
        malformed_value["ETH"]["flow"]["futures"]["symbol"] = "ETH"
        malformed_value["ETH"]["flow"]["spot"]["symbol"] = "ETH"
        cursor = malformed_value["ETH"]
        for key in path[:-1]:
            cursor = cursor[key]
        cursor[path[-1]] = replacement
        isolated = _build(mixed, derivatives=malformed_value)
        assert _event(
            isolated, snapshots.PROJECTION_EVENT_TYPE
        ).engine_snapshot["projection"]["symbol_evaluations"][1] == {
            "symbol": "ETH",
            "status": "UNEVALUABLE",
            "reason": rejection,
        }

    wrong_cvd_symbol = _derivatives()
    wrong_cvd_symbol["BTC"]["flow"]["futures"]["symbol"] = "ETH"
    wrong_cvd_symbol_batch = _build(payload, derivatives=wrong_cvd_symbol)
    assert _event(
        wrong_cvd_symbol_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }

    swapped_cvd_markets = _derivatives()
    swapped_cvd_markets["BTC"]["flow"]["futures"], swapped_cvd_markets[
        "BTC"
    ]["flow"]["spot"] = (
        swapped_cvd_markets["BTC"]["flow"]["spot"],
        swapped_cvd_markets["BTC"]["flow"]["futures"],
    )
    swapped_cvd_markets_batch = _build(
        payload, derivatives=swapped_cvd_markets
    )
    assert _event(
        swapped_cvd_markets_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }

    boolean_spot_rows = _derivatives()
    boolean_spot_rows["BTC"]["flow"]["spot"]["quality"]["rows"] = False
    boolean_spot_rows_batch = _build(payload, derivatives=boolean_spot_rows)
    assert _event(
        boolean_spot_rows_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }

    wide_price_oi_gap = _derivatives()
    wide_price_oi_gap["BTC"]["regime"]["oi_fetched_at"] = (
        BASE + timedelta(minutes=4, seconds=49)
    ).isoformat()
    wide_price_oi_gap["BTC"]["regime"]["time_gap_seconds"] = 31.0
    wide_price_oi_gap_batch = _build(payload, derivatives=wide_price_oi_gap)
    assert _event(
        wide_price_oi_gap_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }

    future = _derivatives(collected_at=BASE + timedelta(minutes=5, seconds=40))
    future_batch = _build(payload, derivatives=future)
    assert _event(
        future_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "DERIVATIVES_SNAPSHOT_INVALID",
    }
    stale = _derivatives(collected_at=BASE - timedelta(days=30))
    stale_batch = _build(payload, derivatives=stale)
    assert _event(
        stale_batch, snapshots.PROJECTION_EVENT_TYPE
    ).engine_snapshot["projection"]["symbol_evaluations"][0] == {
        "symbol": "BTC",
        "status": "UNEVALUABLE",
        "reason": "PRICE_OI_STALE",
    }

    tampered = deepcopy(payload)
    tampered["rows"][0]["current_price"] = 101.0
    _raises(lambda: _build(tampered), "hash mismatch")

    bad_opportunities = deepcopy(opportunities)
    bad_opportunities[0]["score"] = float("nan")
    _raises(
        lambda: _build(
            payload,
            opportunities=bad_opportunities,
            derivatives=derivatives,
        ),
        "non-finite",
    )
    wrong_direction = deepcopy(opportunities)
    wrong_direction[0]["target_price"] = 110.0
    wrong_direction[0]["target_direction"] = "UP"
    _raises(
        lambda: _build(
            payload,
            opportunities=wrong_direction,
            derivatives=derivatives,
        ),
        "engine output",
    )
    mismatched_evidence = _market_evidence("SHORT")
    _raises(
        lambda: _build(
            payload,
            directional={"BTC": {"LONG": mismatched_evidence}},
        ),
        "expected direction",
    )
    wrong_price = deepcopy(opportunities)
    wrong_price[0]["current_price"] = 101.0
    _raises(
        lambda: _build(
            payload,
            opportunities=wrong_price,
            derivatives=derivatives,
        ),
        "engine output",
    )
    wrong_target = deepcopy(opportunities)
    wrong_target[0]["target_price"] = 94.0
    _raises(
        lambda: _build(
            payload,
            opportunities=wrong_target,
            derivatives=derivatives,
        ),
        "engine output",
    )
    threshold_evidence = _market_evidence("SHORT")
    threshold_modules = threshold_evidence["modules"]
    threshold_conclusion = market_confidence_engine._conclusion(
        threshold_modules, "BEARISH"
    )
    assert market_confidence_engine._confirmation(
        64.99, "BEARISH", threshold_modules, threshold_conclusion
    )["status"] == "BELOW_SCORE"
    for score in (65.0, 74.99):
        assert market_confidence_engine._confirmation(
            score, "BEARISH", threshold_modules, threshold_conclusion
        )["status"] == "CONFIRMED"
    assert market_confidence_engine._confirmation(
        75.0, "BEARISH", threshold_modules, threshold_conclusion
    )["status"] == "STRONG_CONFIRMED"
    weak_core = deepcopy(threshold_modules)
    weak_core["futures_flow"]["score"] = -24.99
    weak_conclusion = market_confidence_engine._conclusion(
        weak_core, "BEARISH"
    )
    assert market_confidence_engine._confirmation(
        75.0, "BEARISH", weak_core, weak_conclusion
    )["status"] == "UNCONFIRMED"
    opposing = deepcopy(threshold_modules)
    opposing["futures_flow"].update(
        {"direction": "BULLISH", "relation": "OPPOSE", "score": 69.0}
    )
    opposing_conclusion = market_confidence_engine._conclusion(
        opposing, "BEARISH"
    )
    assert market_confidence_engine._confirmation(
        75.0, "BEARISH", opposing, opposing_conclusion
    )["status"] == "CONFLICT"
    early = deepcopy(threshold_modules)
    early["positioning"]["early_shift"] = {"new_direction": "BULLISH"}
    early_conclusion = market_confidence_engine._conclusion(early, "BEARISH")
    assert market_confidence_engine._confirmation(
        75.0, "BEARISH", early, early_conclusion
    )["status"] == "CONFLICT"

    imports = {
        alias.name
        for node in ast.walk(
            ast.parse(Path(snapshots.__file__).read_text(encoding="utf-8"))
        )
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any(
        forbidden in name.lower()
        for name in imports
        for forbidden in ("telegram", "formula", "outcome", "trading")
    )

    print("Research signal snapshot self-test: PASS")
    print("Family-deduplicated Combined votes: PASS")
    print("Silent/no-authority boundary: PASS")


if __name__ == "__main__":
    run()
