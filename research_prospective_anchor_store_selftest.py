"""Focused self-test for the prospective-anchor PostgreSQL adapter."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import research_prospective_anchor_store as store_module
import research_prospective_anchors as anchors


UTC = timezone.utc


class _Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _ReadConnection:
    def __init__(self, *, coverage_rows=None, oi_rows=None, futures_rows=None, spot_rows=None):
        self.coverage_rows = coverage_rows or []
        self.oi_rows = oi_rows or []
        self.futures_rows = futures_rows or []
        self.spot_rows = spot_rows or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        if "WITH finalized_first_touch" in sql:
            return _Result(self.coverage_rows)
        if "FROM oi_regime_snapshots" in sql:
            return _Result(self.oi_rows)
        if "FROM futures_taker_history" in sql:
            return _Result(self.futures_rows)
        if "FROM spot_taker_history" in sql:
            return _Result(self.spot_rows)
        if "FROM research_prospective_anchor_slots" in sql:
            return _Result([])
        raise AssertionError(f"unexpected read SQL: {sql[:80]}")


class _Transaction:
    def __init__(self, connection):
        self.connection = connection
        self.snapshot = None

    def __enter__(self):
        self.snapshot = deepcopy(self.connection.state)
        self.connection.transaction_entries += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.connection.state.clear()
            self.connection.state.update(self.snapshot)
            self.connection.rollbacks += 1
        else:
            self.connection.commits += 1
        return False


class _MemoryConnection:
    def __init__(self, state):
        self.state = state
        self.transaction_entries = 0
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def transaction(self):
        return _Transaction(self)

    @staticmethod
    def _decoded(params, keys):
        row = dict(params)
        for key in keys:
            if isinstance(row.get(key), str):
                row[key] = json.loads(row[key])
        return row

    def execute(self, sql, params):
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO research_prospective_anchor_attempts"):
            fingerprint = str(params["attempt_fingerprint"])
            existing = self.state["attempts"].get(fingerprint)
            if existing:
                return _Result([])
            row = self._decoded(
                params,
                (
                    "coverage_snapshot",
                    "missing_sources",
                    "source_timestamps",
                    "source_provenance",
                    "frozen_inputs",
                ),
            )
            row["attempt_id"] = self.state["next_attempt"]
            self.state["next_attempt"] += 1
            self.state["attempts"][fingerprint] = row
            return _Result([{"attempt_id": row["attempt_id"]}])
        if compact.startswith("SELECT * FROM research_prospective_anchor_attempts"):
            row = self.state["attempts"].get(str(params["attempt_fingerprint"]))
            return _Result([row] if row else [])
        if compact.startswith("INSERT INTO research_events"):
            fingerprint = str(params["event_fingerprint"])
            existing = self.state["events"].get(fingerprint)
            if existing:
                return _Result([])
            row = self._decoded(params, ("categories", "engine_snapshot"))
            row["event_id"] = self.state["next_event"]
            self.state["next_event"] += 1
            self.state["events"][fingerprint] = row
            return _Result([{"event_id": row["event_id"]}])
        if compact.startswith("SELECT * FROM research_events"):
            row = self.state["events"].get(str(params["event_fingerprint"]))
            return _Result([row] if row else [])
        if compact.startswith("INSERT INTO research_prospective_anchor_slots"):
            key = (
                str(params["sampler_version"]),
                str(params["symbol"]),
                params["source_candle_open_utc"],
            )
            existing = self.state["slots"].get(key)
            if existing:
                return _Result([])
            row = self._decoded(
                params,
                (
                    "coverage_snapshot",
                    "source_timestamps",
                    "source_provenance",
                    "frozen_inputs",
                ),
            )
            row["anchor_slot_id"] = self.state["next_slot"]
            self.state["next_slot"] += 1
            self.state["slots"][key] = row
            return _Result([{"anchor_slot_id": row["anchor_slot_id"]}])
        if "FROM research_prospective_anchor_slots slot" in compact:
            key = (
                str(params["sampler_version"]),
                str(params["symbol"]),
                params["source_candle_open_utc"],
            )
            row = deepcopy(self.state["slots"].get(key))
            if row:
                by_id = {
                    item["event_id"]: item
                    for item in self.state["events"].values()
                }
                row["long_event_fingerprint"] = by_id[
                    row["long_event_id"]
                ]["event_fingerprint"]
                row["short_event_fingerprint"] = by_id[
                    row["short_event_id"]
                ]["event_fingerprint"]
            return _Result([row] if row else [])
        raise AssertionError(f"unexpected persistence SQL: {compact[:100]}")


def _eligible_coverage():
    return {
        "eligible": True,
        "failed_gates": [],
        "method_version": store_module.FIRST_TOUCH_METHOD_VERSION,
        "horizons": {
            str(horizon): {
                "eligible": True,
                "anchors": 300,
                "utc_dates": 18,
                "span_hours": 500.0,
            }
            for horizon in (60, 240, 720, 1440)
        },
    }


def _official(symbol, observed):
    return {
        "symbol": symbol,
        "observed_at_utc": observed,
        "refresh_completed_at_utc": observed,
        "source": "binance_spot",
        "quality_status": "PASS",
        "price_exchange": "Binance",
        "price_market": "spot",
        "price_pair": f"{symbol}USDT",
        "price_timeframe": "1m",
        "price": 100.25,
    }


def run() -> None:
    slot = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)
    now = datetime(2026, 8, 29, 12, 34, tzinfo=UTC)
    coverage_rows = []
    for horizon in (60, 240, 720, 1440):
        coverage_rows.append(
            {
                "symbol": "BTC",
                "horizon_minutes": horizon,
                "anchors": 300,
                "utc_dates": 18,
                "min_anchor_time_utc": now - timedelta(hours=500),
                "max_anchor_time_utc": now - timedelta(hours=25),
                "span_hours": 475.0,
            }
        )
    coverage_store = store_module.ProspectiveAnchorStore(
        connection_factory=lambda: _ReadConnection(coverage_rows=coverage_rows),
        flow_timestamp_mode="open",
    )
    coverage = coverage_store.load_coverage(
        symbols=("BTC", "HYPE"), as_of_utc=now
    )
    assert coverage["BTC"]["eligible"] is True
    assert coverage["HYPE"]["eligible"] is False
    assert "60m_minimum_anchors" in coverage["HYPE"]["failed_gates"]
    assert "research_alert_outcomes" not in store_module._COVERAGE_SQL
    assert "outcome.status IN ('HIT', 'MISS')" in store_module._COVERAGE_SQL
    assert "observation_time_utc" in store_module._COVERAGE_SQL

    oi_row = {
        "id": 44,
        "symbol": "BTC",
        "collected_at": now - timedelta(seconds=20),
        "price": 100.0,
        "open_interest_usd": 1_000_000.0,
        "price_change_pct": 0.3,
        "oi_change_pct": 0.1,
        "price_fetched_at": now - timedelta(seconds=22),
        "oi_fetched_at": now - timedelta(seconds=21),
        "time_gap_seconds": 1.0,
        "data_quality_status": "PASS",
        "price_source": "binance_spot",
        "oi_source": "coinglass_open_interest_exchange_list",
    }
    base_flow = {
        "symbol": "BTC",
        "candle_time": slot,
        "buy_volume_usd": 20_000.0,
        "sell_volume_usd": 8_000.0,
        "api_cum_vol_delta_usd": 11_000.0,
        "continuous_cum_vol_delta_usd": 12_000.0,
        "exchange_list": "Binance,OKX,Bybit",
        "imported_at": slot + timedelta(minutes=33),
    }
    future = {**base_flow, "source": "coinglass_futures_aggregated_cvd"}
    spot = {
        **base_flow,
        "source": "coinglass_spot_aggregated_cvd",
        "continuous_cum_vol_delta_usd": -2_000.0,
    }
    input_store = store_module.ProspectiveAnchorStore(
        connection_factory=lambda: _ReadConnection(
            oi_rows=[oi_row], futures_rows=[future], spot_rows=[spot]
        ),
        flow_timestamp_mode="open",
    )
    source_inputs = input_store.load_source_inputs(
        symbols=("BTC",),
        slot_open_utc=slot,
        checked_at_utc=now,
        official_prices_by_symbol={"BTC": _official("BTC", now)},
    )
    price_oi = source_inputs["BTC"]["price_oi"]
    assert price_oi["source_table"] == "oi_regime_snapshots"
    assert price_oi["source_record_id"] == 44
    assert price_oi["price_source"] == "binance_spot"
    assert price_oi["oi_source"] == "coinglass_open_interest_exchange_list"
    assert price_oi["observation_time_utc"] == oi_row["collected_at"]
    assert source_inputs["BTC"]["futures_cvd"]["source"] == future["source"]
    assert source_inputs["BTC"]["futures_cvd"]["candle_timestamp_mode"] == "open"

    batch = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _eligible_coverage()},
        source_inputs_by_symbol=source_inputs,
        coverage_policy_version=store_module.COVERAGE_POLICY_VERSION,
        strategy_version="selftest",
        code_version="selftest",
    )
    assert batch.decisions[0].evaluation_status == anchors.EVALUABLE
    assert len(batch.events) == 2
    contract = batch.events[0].engine_snapshot["prospective_anchor"]
    assert contract["frozen_inputs"]["price_oi"]["oi_change_pct"] == 0.1
    assert contract["source_provenance"]["price_oi"]["source_record_id"] == 44
    assert "source_record_id" not in contract["frozen_inputs"]["price_oi"]

    state = {
        "attempts": {},
        "events": {},
        "slots": {},
        "next_attempt": 1,
        "next_event": 10,
        "next_slot": 100,
    }
    connections = []

    def memory_factory():
        connection = _MemoryConnection(state)
        connections.append(connection)
        return connection

    persistence = store_module.ProspectiveAnchorStore(
        connection_factory=memory_factory,
        flow_timestamp_mode="open",
    )
    bundle = batch.atomic_persistence_bundles()[0]
    first = persistence.persist_bundle(bundle)
    assert first.idempotent is False
    assert first.long_event_id != first.short_event_id
    assert len(state["attempts"]) == 1
    assert len(state["events"]) == 2
    assert len(state["slots"]) == 1
    assert connections[-1].transaction_entries == 1
    assert connections[-1].commits == 1

    repeat = persistence.persist_bundle(bundle)
    assert repeat.idempotent is True
    assert repeat.anchor_slot_id == first.anchor_slot_id
    assert len(state["events"]) == 2
    assert len(state["slots"]) == 1

    revised_inputs = deepcopy(source_inputs)
    revised_inputs["BTC"]["futures_cvd"][
        "continuous_cum_vol_delta_usd"
    ] = 99_999.0
    revised_batch = anchors.build_anchor_batch(
        now=now,
        slot_open_utc=slot,
        coverage_by_symbol={"BTC": _eligible_coverage()},
        source_inputs_by_symbol=revised_inputs,
        coverage_policy_version=store_module.COVERAGE_POLICY_VERSION,
        strategy_version="selftest",
        code_version="selftest",
    )
    before_conflict = deepcopy(state)
    try:
        persistence.persist_bundle(
            revised_batch.atomic_persistence_bundles()[0]
        )
    except store_module.ProspectiveAnchorConflictError:
        pass
    else:
        raise AssertionError("changed frozen input did not raise a conflict")
    assert state == before_conflict
    assert connections[-1].rollbacks == 1

    assert store_module.seconds_until_next_scheduler_check(now) == 60.0
    status = persistence.status()
    assert status["telegram_alerts"] == 0
    assert status["live_delivery_allowed"] is False
    assert status["atomic_pair_transaction"] is True
    print("research_prospective_anchor_store_selftest: PASS")


if __name__ == "__main__":
    run()
