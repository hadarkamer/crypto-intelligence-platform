"""Deterministic, database-free checks for Max-Pain archive v1."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import research_max_pain_archive as archive


BASE = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)


def _raw_rows(symbols=("BTC",), *, observed_at=BASE):
    rows = []
    for symbol in symbols:
        for rank, timeframe in enumerate(archive.REQUIRED_TIMEFRAMES, start=1):
            rows.append(
                {
                    "symbol": symbol,
                    "rank": rank,
                    "timeframe": timeframe,
                    "price": 100.0,
                    "max_short_price": 110.0,
                    "max_long_price": 95.0,
                    "short_amount_usd": 200.0,
                    "long_amount_usd": 100.0,
                    "collected_at_utc": observed_at.isoformat(),
                }
            )
    return rows


def _enriched_rows(
    symbols=("BTC",),
    *,
    fetched_at=BASE + timedelta(minutes=4),
    observed_at=BASE,
    generic_hype=False,
    short_amount=200.0,
):
    rows = []
    for symbol in symbols:
        for rank, timeframe in enumerate(archive.REQUIRED_TIMEFRAMES, start=1):
            row = {
                "symbol": symbol,
                "rank": rank,
                "timeframe": timeframe,
                "current_price": 100.0,
                "source_observed_at_utc": observed_at.isoformat(),
                "short_max_pain": 110.0,
                "long_max_pain": 95.0,
                "short_liquidation_amount": short_amount,
                "long_liquidation_amount": 100.0,
                "price_fetched_at_utc": fetched_at.isoformat(),
                "price_observed_at_utc": fetched_at.isoformat(),
                "price_interval": "1m",
            }
            if symbol == "HYPE":
                row.update(
                    {
                        "price_source": "hyperliquid",
                        "price_pair": "HYPEUSDT" if generic_hype else "HYPE/USDT",
                    }
                )
                if not generic_hype:
                    row.update(
                        {
                            "price_exchange": "hyperliquid",
                            "price_market": "spot",
                            "price_instrument": "@107",
                        }
                    )
            else:
                row.update(
                    {
                        "price_source": "binance_spot",
                        "price_pair": f"{symbol}USDT",
                    }
                )
            rows.append(row)
    return rows


def _payload(
    *,
    symbols=("BTC",),
    source="WATCH_SHARED",
    generic_hype=False,
    short_amount=200.0,
    cycle_time=BASE,
    completed_at=BASE + timedelta(minutes=5),
    snapshot_ok=True,
    include_raw=True,
    skipped=(),
):
    raw = _raw_rows(symbols, observed_at=cycle_time) if include_raw else []
    return archive.build_snapshot_payload(
        cycle_id=f"selftest:{source}:{cycle_time.isoformat()}",
        cycle_time_utc=cycle_time,
        collection_started_at_utc=cycle_time,
        collection_completed_at_utc=completed_at,
        source=source,
        collector_version="selftest-v1",
        snapshot={
            "ok": snapshot_ok,
            "rows": raw,
            "row_count": len(raw),
            "missing_timeframes": [],
            "duplicate_pairs": [],
        },
        enriched_rows=_enriched_rows(
            symbols,
            fetched_at=completed_at - timedelta(minutes=1),
            observed_at=cycle_time,
            generic_hype=generic_hype,
            short_amount=short_amount,
        ),
        live_result={"skipped_symbols": list(skipped)},
    )


def _db_shape(payload, set_id):
    set_record = {"snapshot_set_id": set_id, **payload["set"]}
    manifests = {item["symbol"]: item for item in payload["symbols"]}
    return set_record, manifests


def run() -> None:
    try:
        _payload(
            cycle_time=archive.CUTOVER_TIME_UTC - timedelta(minutes=1),
            completed_at=archive.CUTOVER_TIME_UTC + timedelta(minutes=4),
        )
    except ValueError as exc:
        assert "pre-cutover" in str(exc)
    else:
        raise AssertionError("a pre-cutover Max-Pain cycle entered the new archive")

    valid = _payload()
    assert set(valid) == {"set", "symbols", "rows"}
    assert valid["set"]["collection_status"] == "COMPLETE"
    assert valid["set"]["validation_status"] == "PASS"
    assert valid["set"]["research_eligible"] is True
    assert valid["set"]["eligible_symbol_count"] == 1
    assert valid["symbols"][0]["research_eligible"] is True
    assert len(valid["rows"]) == 7

    current_set, current_manifests = _db_shape(valid, 2)
    result = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=10),
        current_set=current_set,
        current_rows=valid["rows"],
        current_symbol_manifest=current_manifests["BTC"],
    )
    assert result["evaluation_status"] == "EVALUABLE"
    features = result["features"]
    assert features["max_pain.12h.short_target_signed_distance_pct"] == 10.0
    assert features["max_pain.12h.long_target_signed_distance_pct"] == -5.0
    assert features["max_pain.aggregate.short_long_liquidity_ratio"] == 2.0
    assert features["max_pain.aggregate.closer_downside_count"] == 7

    mixed = _payload(symbols=("BTC", "HYPE"), generic_hype=True)
    mixed_set, mixed_manifests = _db_shape(mixed, 3)
    assert mixed["set"]["validation_status"] == "PARTIAL"
    assert mixed["set"]["research_eligible"] is True
    assert mixed_manifests["BTC"]["research_eligible"] is True
    assert mixed_manifests["HYPE"]["research_eligible"] is False
    assert len([row for row in mixed["rows"] if row["symbol"] == "HYPE"]) == 7
    btc_mixed = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=10),
        current_set=mixed_set,
        current_rows=mixed["rows"],
        current_symbol_manifest=mixed_manifests["BTC"],
    )
    hype_mixed = archive.derive_prior_only_features(
        symbol="HYPE",
        decision_time_utc=BASE + timedelta(minutes=10),
        current_set=mixed_set,
        current_rows=mixed["rows"],
        current_symbol_manifest=mixed_manifests["HYPE"],
    )
    assert btc_mixed["evaluation_status"] == "EVALUABLE"
    assert hype_mixed["evaluation_status"] == "UNEVALUABLE"

    exact_hype = _payload(symbols=("HYPE",))
    assert exact_hype["symbols"][0]["research_eligible"] is True
    assert all(
        row["price_pair"] == "HYPE/USDT"
        and row["price_instrument"] == "@107"
        and row["price_source_policy_status"] == "PASS"
        for row in exact_hype["rows"]
    )

    manual = _payload(source="MANUAL")
    passive = _payload(source="RESEARCH_PASSIVE")
    enriched_only = _payload(include_raw=False)
    unverified = _payload(snapshot_ok=False)
    skipped = _payload(skipped=("BTC",))
    assert manual["set"]["research_eligible"] is False
    assert passive["set"]["research_eligible"] is True
    assert enriched_only["set"]["research_eligible"] is False
    assert unverified["set"]["research_eligible"] is False
    assert skipped["set"]["research_eligible"] is False

    duplicate_raw = _raw_rows()
    duplicate = archive.build_snapshot_payload(
        cycle_id="selftest:duplicate",
        cycle_time_utc=BASE,
        collection_started_at_utc=BASE,
        collection_completed_at_utc=BASE + timedelta(minutes=5),
        source="WATCH_SHARED",
        collector_version="selftest-v1",
        snapshot={
            "ok": True,
            "rows": duplicate_raw,
            "missing_timeframes": [],
            "duplicate_pairs": ["BTC/12h"],
        },
        enriched_rows=_enriched_rows(),
    )
    assert duplicate["set"]["research_eligible"] is False
    assert duplicate["symbols"][0]["duplicate_timeframes"] == ["12h"]

    before_available = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=4, seconds=59),
        current_set=current_set,
        current_rows=valid["rows"],
        current_symbol_manifest=current_manifests["BTC"],
    )
    stale = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=51),
        current_set=current_set,
        current_rows=valid["rows"],
        current_symbol_manifest=current_manifests["BTC"],
    )
    assert before_available["evaluation_status"] == "UNEVALUABLE"
    assert stale["evaluation_status"] == "UNEVALUABLE"
    exactly_available = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=5),
        current_set=current_set,
        current_rows=valid["rows"],
        current_symbol_manifest=current_manifests["BTC"],
    )
    assert exactly_available["evaluation_status"] == "EVALUABLE"
    assert exactly_available["snapshot_available_at_utc"].startswith(
        "2026-08-29T12:05:00"
    )

    previous = _payload(
        short_amount=100.0,
        cycle_time=BASE - timedelta(minutes=30),
        completed_at=BASE - timedelta(minutes=25),
    )
    previous_set, previous_manifests = _db_shape(previous, 1)
    delta = archive.derive_prior_only_features(
        symbol="BTC",
        decision_time_utc=BASE + timedelta(minutes=10),
        current_set=current_set,
        current_rows=valid["rows"],
        current_symbol_manifest=current_manifests["BTC"],
        previous_set=previous_set,
        previous_rows=previous["rows"],
        previous_symbol_manifest=previous_manifests["BTC"],
    )
    assert delta["change_evaluation_status"] == "EVALUABLE"
    assert (
        delta["features"]["max_pain.delta.upside_liquidity_usd_trend"]
        == "STRENGTHENING"
    )

    identical = _payload()
    changed = _payload(short_amount=201.0)
    assert identical["set"]["snapshot_key"] == valid["set"]["snapshot_key"]
    assert identical["set"]["payload_sha256"] == valid["set"]["payload_sha256"]
    assert changed["set"]["snapshot_key"] == valid["set"]["snapshot_key"]
    assert changed["set"]["payload_sha256"] != valid["set"]["payload_sha256"]

    module_source = Path(archive.__file__).read_text(encoding="utf-8")
    executable_source = "\n".join(
        line for line in module_source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "FROM max_pain_snapshots" not in executable_source
    migration = Path("migrations/007_max_pain_watch_archive_v1.sql").read_text(
        encoding="utf-8"
    )
    executable_migration = "\n".join(
        line for line in migration.splitlines() if not line.lstrip().startswith("--")
    )
    assert "max_pain_snapshots" not in executable_migration
    assert "research_max_pain_snapshot_symbols" in migration
    assert "trg_research_max_pain_symbols_append_only" in migration
    assert "trg_research_max_pain_symbols_no_truncate" in migration
    assert "price_source='binance_spot'" in migration
    assert "price_instrument='@107'" in migration

    print("Max-Pain archive v1 self-test: PASS")


if __name__ == "__main__":
    run()
