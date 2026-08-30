"""Deterministic self-test for prospective neutral decision anchors."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import research_event_capture
import research_prospective_anchors as anchors


UTC = timezone.utc


def _source_rows(slot: datetime, *, symbol: str = "BTC", refresh_minute: int = 33):
    refreshed = slot.replace(minute=refresh_minute)
    is_hype = symbol == "HYPE"
    return {
        "official_price": {
            "symbol": symbol,
            "observed_at_utc": slot.replace(minute=34),
            "refresh_completed_at_utc": slot.replace(minute=34),
            "source": "hyperliquid_spot_@107" if is_hype else "binance_spot",
            "quality_status": "PASS",
            "price_exchange": "Hyperliquid" if is_hype else "Binance",
            "price_market": "spot",
            "price_pair": f"{symbol}/USDT" if is_hype else f"{symbol}USDT",
            "price_instrument_id": "@107" if is_hype else None,
            "price_timeframe": "1m",
            "fallback_used": False,
            "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
            "price": 100.25,
        },
        "price_oi": {
            "symbol": symbol,
            "observation_time_utc": refreshed,
            "refresh_completed_at_utc": refreshed,
            "refresh_time_semantics": "DATABASE_READ_CONFIRMATION",
            "source_table": "oi_regime_snapshots",
            "source_record_id": 123,
            "price_source": "binance_spot",
            "oi_source": "coinglass_open_interest_exchange_list",
            "price_fetched_at_utc": slot.replace(minute=32, second=30),
            "oi_fetched_at_utc": slot.replace(minute=32, second=31),
            "quality_status": "PASS",
            "price_close": 100.0,
            "oi_close_usd": 1_000_000.0,
            "price_change_pct": 0.35,
            "oi_change_pct": 0.12,
        },
        "futures_cvd": {
            "symbol": symbol,
            "source_candle_time_utc": slot,
            "refresh_completed_at_utc": slot.replace(minute=32),
            "source": "coinglass_futures_aggregated_cvd",
            "quality_status": "PASS",
            "quality_status_basis": "ADAPTER_STRUCTURAL_VALIDATION",
            "exchange_list": "Binance,OKX,Bybit",
            "candle_timestamp_mode": "open",
            "buy_volume_usd": 20_000.0,
            "sell_volume_usd": 8_000.0,
            "api_cum_vol_delta_usd": 11_000.0,
            "continuous_cum_vol_delta_usd": 12_000.0,
        },
        "spot_cvd": {
            "symbol": symbol,
            "source_candle_time_utc": slot,
            "refresh_completed_at_utc": slot.replace(minute=32),
            "source": "coinglass_spot_aggregated_cvd",
            "quality_status": "PASS",
            "quality_status_basis": "ADAPTER_STRUCTURAL_VALIDATION",
            "exchange_list": "Binance,OKX,Bybit",
            "candle_timestamp_mode": "open",
            "buy_volume_usd": 3_000.0,
            "sell_volume_usd": 5_000.0,
            "api_cum_vol_delta_usd": -1_500.0,
            "continuous_cum_vol_delta_usd": -2_000.0,
        },
    }


def _coverage(*, symbol: str = "BTC", eligible: bool = True):
    return {
        "symbol": symbol,
        "eligible": eligible,
        "failed_gates": [] if eligible else ["minimum_utc_dates"],
        "coverage_policy_version": anchors.COVERAGE_POLICY_VERSION,
        "method_version": anchors.research_no_dwell_outcome.METHOD_VERSION,
        "replay_version": anchors.research_historical_replay.REPLAY_VERSION,
        "coverage_scope_version": (
            anchors.research_historical_replay.COVERAGE_SCOPE_VERSION
        ),
        "movement_width_calibration_version": (
            anchors.research_session_width.CALIBRATION_VERSION
        ),
        "canonical_price_method_version": (
            anchors.canonical_price_path.METHOD_VERSION
        ),
        "canonical_price_provenance_version": (
            anchors.canonical_price_path.PRICE_PROVENANCE_VERSION
        ),
        "replay_run_id": 7,
        "replay_completed_at_utc": "2026-08-29T11:00:00Z",
        "as_of_utc": "2026-08-29T11:00:00Z",
        "horizons": {
            str(horizon): {
                "eligible": eligible,
                "anchors": 300 if eligible else 40,
                "utc_dates": 18 if eligible else 2,
                "span_hours": 500.0 if eligible else 48.0,
                "min_anchor_time_utc": (
                    "2026-08-07T14:00:00Z"
                    if eligible
                    else "2026-08-26T10:00:00Z"
                ),
                "max_anchor_time_utc": "2026-08-28T10:00:00Z",
                "failed_gates": [] if eligible else ["minimum_utc_dates"],
            }
            for horizon in (60, 240, 720, 1440)
        },
    }


def run() -> None:
    assert anchors.latest_due_slot_open(datetime(2026, 8, 29, 12, 31, tzinfo=UTC)) == datetime(
        2026, 8, 29, 11, 30, tzinfo=UTC
    )
    assert anchors.latest_due_slot_open(datetime(2026, 8, 29, 12, 32, tzinfo=UTC)) == datetime(
        2026, 8, 29, 12, 0, tzinfo=UTC
    )

    slot = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)
    now = datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
    btc = _source_rows(slot)
    eth = _source_rows(slot, symbol="ETH")
    del eth["spot_cvd"]
    hype = _source_rows(slot, symbol="HYPE")
    hype_before = deepcopy(hype)
    coverage = {
        "BTC": _coverage(),
        "ETH": _coverage(symbol="ETH"),
        "HYPE": _coverage(symbol="HYPE", eligible=False),
    }
    forged_coverage = {
        **_coverage(),
        "coverage_policy_version": "forged",
        "method_version": "forged",
        "replay_version": "forged",
        "movement_width_calibration_version": "forged",
        "canonical_price_method_version": "forged",
        "canonical_price_provenance_version": "forged",
        "replay_run_id": -1,
        "replay_completed_at_utc": "2026-09-08T12:00:00Z",
        "as_of_utc": "2026-09-08T12:00:00Z",
    }
    eligible, failures = anchors._coverage_status(
        forged_coverage,
        expected_symbol="BTC",
        checked_at_utc=now,
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert eligible is False
    assert "replay_run_id_invalid" in failures
    assert "coverage_as_of_is_future" in failures
    assert "replay_completed_at_is_future" in failures
    # The frozen coverage contract is typed JSON evidence. Numeric-looking
    # strings, booleans and fractional counts must fail closed.
    for field, corrupt_value, expected_failure in (
        ("anchors", "300", "60m_anchors"),
        ("anchors", 300.5, "60m_anchors"),
        ("anchors", True, "60m_anchors"),
        ("utc_dates", "18", "60m_utc_dates"),
        ("utc_dates", 18.5, "60m_utc_dates"),
        ("utc_dates", True, "60m_utc_dates"),
        ("span_hours", "500.0", "60m_span_hours"),
        ("span_hours", True, "60m_span_hours"),
        ("span_hours", float("nan"), "60m_span_hours"),
        ("span_hours", float("inf"), "60m_span_hours"),
    ):
        malformed = deepcopy(_coverage())
        malformed["horizons"]["60"][field] = corrupt_value
        eligible, malformed_failures = anchors._coverage_status(
            malformed,
            expected_symbol="BTC",
            checked_at_utc=now,
            coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
        )
        assert eligible is False
        assert expected_failure in malformed_failures
    batch = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol=coverage,
        source_inputs_by_symbol={"BTC": btc, "ETH": eth, "HYPE": hype},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
        strategy_version="test-strategy",
        code_version="test-code",
    )
    decisions = {decision.symbol: decision for decision in batch.decisions}
    assert decisions["BTC"].evaluation_status == anchors.EVALUABLE
    assert decisions["BTC"].decision_time_utc == now
    assert [event.direction for event in decisions["BTC"].events] == ["LONG", "SHORT"]
    assert decisions["ETH"].evaluation_status == anchors.UNEVALUABLE
    assert decisions["ETH"].missing_sources == ("spot_cvd",)
    assert "MISSING_REQUIRED_SOURCE:spot_cvd" in decisions["ETH"].evaluation_reason
    assert not decisions["ETH"].events
    assert decisions["HYPE"].evaluation_status == anchors.COVERAGE_EXCLUDED
    assert not decisions["HYPE"].events
    assert hype == hype_before  # Exclusion never mutates preserved HYPE inputs.
    excluded = decisions["HYPE"]
    assert set(excluded.source_provenance) == set(anchors.REQUIRED_FAMILIES)
    assert set(excluded.source_timestamps) == set(anchors.REQUIRED_FAMILIES)
    official_audit = excluded.source_provenance["official_price"]
    assert official_audit["source"] == "hyperliquid_spot_@107"
    assert official_audit["price_exchange"] == "Hyperliquid"
    assert official_audit["price_market"] == "spot"
    assert official_audit["price_pair"] == "HYPE/USDT"
    assert official_audit["price_instrument_id"] == "@107"
    assert official_audit["price_timeframe"] == "1m"
    assert official_audit["fallback_used"] is False
    assert official_audit["fallback_policy"] == "PROVIDER_ATTESTED_NO_FALLBACK"
    assert excluded.source_timestamps["official_price"] == {
        "observed_at_utc": "2026-08-29T12:34:00.000000Z",
        "refresh_completed_at_utc": "2026-08-29T12:34:00.000000Z",
    }
    excluded_bundle = excluded.atomic_persistence_bundle()
    assert excluded_bundle["attempt"]["source_provenance"] == excluded.source_provenance
    assert excluded_bundle["attempt"]["source_timestamps"] == excluded.source_timestamps
    assert excluded_bundle["event_persistence"] == ()
    assert excluded_bundle["slot"] is None

    assert len(batch.events) == 2
    assert batch.summary()["telegram_alerts"] == 0
    assert batch.summary()["trade_execution"] is False
    bundles = batch.atomic_persistence_bundles()
    assert len(bundles) == 3
    btc_bundle = next(
        item for item in bundles if item["attempt"]["symbol"] == "BTC"
    )
    eth_bundle = next(
        item for item in bundles if item["attempt"]["symbol"] == "ETH"
    )
    assert btc_bundle["atomic_transaction_required"] is True
    assert btc_bundle["live_delivery_allowed"] is False
    assert btc_bundle["slot"]["coverage_snapshot"] == _coverage()
    assert btc_bundle["slot"]["frozen_inputs"] == decisions["BTC"].frozen_inputs
    assert btc_bundle["attempt"]["frozen_inputs"] == decisions["BTC"].frozen_inputs
    assert {item["delivery_status"] for item in btc_bundle["event_persistence"]} == {
        "NOT_APPLICABLE"
    }
    assert {item["capture_stage"] for item in btc_bundle["event_persistence"]} == {
        "SILENT_NEUTRAL_ANCHOR"
    }
    assert eth_bundle["event_persistence"] == ()
    assert eth_bundle["slot"] is None
    for event in batch.events:
        research_event_capture.validate_event(event)
        assert event.event_kind == "DECISION_SAMPLE"
        assert event.event_type == anchors.EVENT_TYPE
        assert event.timeframe == "30m"
        assert event.source_side == "RAW_NEUTRAL"
        assert event.current_price == 100.25
        contract = event.engine_snapshot["prospective_anchor"]
        assert contract["telegram_delivery_allowed"] is False
        assert contract["trade_execution_allowed"] is False
        assert contract["decision_time_utc"] == "2026-08-29T12:34:00.000000Z"
        assert set(contract["source_timestamps"]) == set(anchors.REQUIRED_FAMILIES)
        assert contract["coverage_snapshot"] == _coverage()
        assert contract["frozen_inputs"]["official_price"]["price"] == 100.25
        assert contract["frozen_inputs"]["price_oi"]["price_close"] == 100.0
        assert contract["frozen_inputs"]["price_oi"]["oi_change_pct"] == 0.12
        assert contract["frozen_inputs"]["futures_cvd"]["buy_volume_usd"] == 20_000.0
        assert "source_record_id" not in contract["frozen_inputs"]["price_oi"]

    repeat = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol=coverage,
        source_inputs_by_symbol={"BTC": btc, "ETH": eth, "HYPE": hype},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
        strategy_version="test-strategy",
        code_version="test-code",
    )
    assert anchors.shadow_event_fingerprints(batch) == anchors.shadow_event_fingerprints(repeat)
    assert [row["attempt_fingerprint"] for row in batch.ledger_records()] == [
        row["attempt_fingerprint"] for row in repeat.ledger_records()
    ]
    assert len({event.event_fingerprint for event in batch.events}) == 2

    revised = deepcopy(btc)
    revised["futures_cvd"]["continuous_cum_vol_delta_usd"] = 99_999.0
    revised_batch = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": revised},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
        strategy_version="test-strategy",
        code_version="test-code",
    )
    # Slot identity remains idempotent even if a raw row is later revised;
    # the frozen evidence hash still exposes that the input state changed.
    assert anchors.shadow_event_fingerprints(revised_batch) == tuple(
        event.event_fingerprint for event in decisions["BTC"].events
    )
    assert (
        revised_batch.decisions[0].input_fingerprint
        != decisions["BTC"].input_fingerprint
    )

    future_refresh = _source_rows(slot)
    future_refresh["spot_cvd"]["refresh_completed_at_utc"] = datetime(
        2026, 8, 29, 12, 35, tzinfo=UTC
    )
    pending = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": future_refresh},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert pending.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert "REFRESH_NOT_COMPLETE:spot_cvd" in pending.decisions[0].evaluation_reason
    assert not pending.events

    later = anchors.build_anchor_batch(
        now=datetime(2026, 8, 29, 12, 36, tzinfo=UTC),
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": future_refresh},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert later.decisions[0].evaluation_status == anchors.EVALUABLE
    assert later.decisions[0].decision_time_utc == datetime(
        2026, 8, 29, 12, 36, tzinfo=UTC
    )
    assert len(later.events) == 2

    expired = anchors.build_anchor_batch(
        now=datetime(2026, 8, 29, 13, 2, tzinfo=UTC),
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": btc},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert expired.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert "PROSPECTIVE_CAPTURE_WINDOW_EXPIRED" in expired.decisions[0].evaluation_reason
    assert not expired.events

    too_early = anchors.build_anchor_batch(
        now=datetime(2026, 8, 29, 12, 31, tzinfo=UTC),
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": btc},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert too_early.decisions[0].evaluation_status == anchors.NOT_DUE
    assert too_early.decisions[0].ledger_record() is None
    assert not too_early.events

    hype_eligible = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"HYPE": _coverage(symbol="HYPE")},
        source_inputs_by_symbol={"HYPE": hype},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert hype_eligible.decisions[0].evaluation_status == anchors.EVALUABLE
    assert len(hype_eligible.events) == 2

    bad_slot = deepcopy(btc)
    bad_slot["futures_cvd"]["source_candle_time_utc"] = datetime(
        2026, 8, 29, 11, 30, tzinfo=UTC
    )
    mismatched = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": bad_slot},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert mismatched.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert "SOURCE_SLOT_MISMATCH:futures_cvd" in mismatched.decisions[0].evaluation_reason

    fallback = deepcopy(btc)
    fallback["official_price"]["price_exchange"] = "Bybit"
    unofficial = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": fallback},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert unofficial.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert (
        "UNOFFICIAL_CURRENT_PRICE_PROVENANCE:official_price"
        in unofficial.decisions[0].evaluation_reason
    )
    assert not unofficial.events

    mutable_import_time = deepcopy(btc)
    del mutable_import_time["spot_cvd"]["refresh_completed_at_utc"]
    mutable_import_time["spot_cvd"]["imported_at"] = now
    no_live_refresh_proof = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": mutable_import_time},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert no_live_refresh_proof.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert (
        "MISSING_REFRESH_TIMESTAMP:spot_cvd"
        in no_live_refresh_proof.decisions[0].evaluation_reason
    )

    before_grace = _source_rows(slot)
    before_grace["futures_cvd"]["refresh_completed_at_utc"] = slot.replace(
        minute=31
    )
    premature_refresh = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _coverage()},
        source_inputs_by_symbol={"BTC": before_grace},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert premature_refresh.decisions[0].evaluation_status == anchors.UNEVALUABLE
    assert (
        "REFRESH_PRECEDES_CLOSE_PLUS_GRACE:futures_cvd"
        in premature_refresh.decisions[0].evaluation_reason
    )

    malformed_coverage = _coverage()
    malformed_coverage["horizons"]["720"]["anchors"] = "unknown"
    coverage_rejected = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": malformed_coverage},
        source_inputs_by_symbol={"BTC": btc},
        coverage_policy_version=anchors.COVERAGE_POLICY_VERSION,
    )
    assert coverage_rejected.decisions[0].evaluation_status == anchors.COVERAGE_EXCLUDED
    assert "720m_anchors" in coverage_rejected.decisions[0].evaluation_reason
    btc_excluded = coverage_rejected.decisions[0]
    btc_official_audit = btc_excluded.source_provenance["official_price"]
    assert btc_official_audit["source"] == "binance_spot"
    assert btc_official_audit["price_exchange"] == "Binance"
    assert btc_official_audit["price_pair"] == "BTCUSDT"
    assert btc_official_audit["fallback_used"] is False
    assert not btc_excluded.events

    migration = Path("migrations/008_prospective_neutral_anchors_v1.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS research_prospective_anchor_attempts" in migration
    assert "CREATE TABLE IF NOT EXISTS research_prospective_anchor_slots" in migration
    assert "coverage_snapshot JSONB NOT NULL" in migration
    assert "attempt_fingerprint CHAR(64) NOT NULL UNIQUE" in migration
    assert "UNIQUE (sampler_version, symbol, source_candle_open_utc)" in migration
    assert "long_event_id BIGINT NOT NULL UNIQUE" in migration
    assert "short_event_id BIGINT NOT NULL UNIQUE" in migration
    assert "base_eligible_at_utc = source_candle_close_utc + INTERVAL '2 minutes'" in migration
    assert "validate_prospective_anchor_pair" in migration
    assert "CREATE OR REPLACE VIEW research_prospective_shadow_events" in migration
    assert "event.event_kind = 'DECISION_SAMPLE'" in migration
    assert "event.delivery_status = 'NOT_APPLICABLE'" in migration
    assert "suppress_decision_sample_live_delivery" in migration
    assert "RETURN NULL" in migration
    assert "Telegram" in migration

    module_text = Path("research_prospective_anchors.py").read_text()
    assert "import ai_telegram" not in module_text
    assert "import main" not in module_text
    assert "psycopg" not in module_text
    assert "requests" not in module_text
    assert "def persistence_envelopes" not in module_text

    store_text = Path("research_prospective_anchor_store.py").read_text()
    assert "import ai_telegram" not in store_text
    assert "ON CONFLICT (attempt_fingerprint) DO NOTHING" in store_text
    assert "ON CONFLICT (event_fingerprint) DO NOTHING" in store_text
    assert "ON CONFLICT (sampler_version, symbol, source_candle_open_utc) DO NOTHING" in store_text

    print("research_prospective_anchors_selftest: PASS")


if __name__ == "__main__":
    run()
