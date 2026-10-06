"""Behavioral fixed-window versus First Touch and queue integration checks."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
import math
import json
import sqlite3

import research_common_window_metrics as metrics
import research_common_window_metrics_store as store
import research_ordered_first_touch as first_touch
import research_outcome_worker as worker

START = datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)
ROUTE = {"symbol": "BTC", "exchange": "binance", "market": "spot", "pair": "BTCUSDT",
    "interval": "1m", "interval_seconds": 60, "complete": True}


def candle(index, *, high=100., low=100., start=START):
    opened = start + timedelta(minutes=index)
    return SimpleNamespace(open_time_utc=opened,
        close_time_utc=opened + timedelta(seconds=59, milliseconds=999),
        open=100., high=high, low=low, close=100.)


def compute(path, **kwargs):
    args = dict(symbol="BTC", reference_price=100., direction="LONG", event_time=START,
        window_minutes=60, candles=path, observed_at=START+timedelta(hours=2), path_result=ROUTE)
    args.update(kwargs)
    return metrics.calculate_common_window_metrics(**args)


def check_full_window_vs_first_touch():
    path = [candle(i) for i in range(60)]
    path[0] = candle(0, high=101, low=99.75)
    path[-1] = candle(59, high=105, low=97)
    label = first_touch.calculate_ordered_first_touch_outcome(reference_price=100,
        direction="LONG", event_time=START, candles=path, threshold_pct=.5,
        observation_closed=True)
    assert label["status"] == "SUCCESS" and label["mfe_pct"] == 1
    result = compute(path)
    assert result["status"] == "READY" and result["mfe_pct"] == 5 and result["mae_pct"] == 3
    assert math.isclose(result["asymmetry_ratio"], 5/3)
    inverse = compute(path, direction="SHORT")
    assert inverse["mfe_pct"] == result["mae_pct"] and inverse["mae_pct"] == result["mfe_pct"]
    path[0] = candle(0, high=101, low=99)
    ambiguous = first_touch.calculate_ordered_first_touch_outcome(reference_price=100,
        direction="LONG", event_time=START, candles=path, threshold_pct=.5,
        observation_closed=True)
    assert ambiguous["first_touch_side"] == "AMBIGUOUS"
    assert compute(path)["mfe_pct"] == 5  # Ambiguous still has complete-window metrics.


def check_causality_coverage_and_quality():
    path = [candle(i) for i in range(60)]
    path[10] = candle(10, high=102, low=99)
    base = compute(path)
    assert compute(path + [candle(80, high=900, low=1)])["path_sha256"] == base["path_sha256"]
    for broken in (path[1:], path[:20]+path[21:], path+[path[-1]]):
        result = compute(broken)
        assert result["status"] == "DATA_MISSING" and result["mfe_pct"] is None
    early = compute(path, observed_at=START+timedelta(minutes=20, seconds=30))
    assert early["status"] == "OPEN" and early["path_samples"] == 20 and early["mfe_pct"] is None
    gapped_early = compute(path[:10], observed_at=START+timedelta(minutes=20))
    assert gapped_early["status"] == "DATA_MISSING"  # Never confuse missing coverage with OPEN.
    unaligned = compute([candle(-1, high=900, low=1)] + path,
        event_time=START+timedelta(seconds=30))
    assert unaligned["status"] == "READY" and unaligned["path_samples"] == 59
    assert unaligned["initial_gap_seconds"] == 30 and unaligned["trailing_partial_minute_seconds"] == 30
    assert unaligned["mfe_pct"] == 2
    for changed in ({**ROUTE,"market":"futures"}, {**ROUTE,"exchange":"bybit"}, {**ROUTE,"pair":"ETHUSDT"}):
        invalid = compute(path, path_result=changed)
        assert invalid["status"] == "DATA_MISSING" and invalid["asymmetry_ratio"] is None
    # A provider's later 24h gap must not poison a complete 60m window.
    assert compute(path, path_result={**ROUTE,"complete":False})["status"] == "READY"
    hype_route = {**ROUTE,"symbol":"HYPE","exchange":"hyperliquid","pair":"HYPE/USDT","api_coin":"@107"}
    hype = compute(path,symbol="HYPE",path_result=hype_route)
    assert hype["status"] == "READY" and hype["source"]["exchange"] == "hyperliquid"
    assert hype["data_quality_status"] == "VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES"
    assert compute(path,symbol="HYPE",path_result={**hype_route,"api_coin":"HYPE"})["status"] == "DATA_MISSING"
    try:
        compute(path, event_time=START.replace(tzinfo=None))
    except ValueError:
        pass
    else:
        raise AssertionError("naive event timestamp accepted")


def check_asymmetry_cohort():
    zero = compute([candle(i, high=101) for i in range(60)])
    assert zero["mae_pct"] == 0 and zero["asymmetry_ratio"] is None
    assert zero["asymmetry_status"] == "UNDEFINED_ZERO_MAE"
    loss = compute([candle(i, low=98) for i in range(60)])
    joined = metrics.aggregate_common_window_metrics([zero, loss], expected_count=2,window_minutes=60,threshold_bps=50)
    assert joined["coverage_complete"] and joined["asymmetry_ratio"] == .5
    # Mean-of-event ratios would discard zero-MAE and change this result.
    missing = metrics.aggregate_common_window_metrics([zero], expected_count=2,window_minutes=60,threshold_bps=50)
    assert missing["asymmetry_ratio"] is None and not missing["coverage_complete"]
    mixed = metrics.aggregate_common_window_metrics([zero,{**loss,"window_minutes":240}],expected_count=2,window_minutes=60,threshold_bps=50)
    assert not mixed["coverage_complete"]
    assert metrics.aggregate_common_window_metrics([zero],expected_count=1,window_minutes=60,threshold_bps=200)["asymmetry_status"] == "UNDEFINED_ZERO_MAE"


def check_terminal_maturity_worker():
    event = {"event_id": 8, "alert_time_utc": START, "symbol":"BTC", "direction":"LONG",
        "event_kind":"ALERT", "delivery_status":"DELIVERED", "current_price":100.,
        "engine_snapshot":{"price_source":"binance_spot","price_pair":"BTCUSDT"}}
    path = [candle(i, high=101, low=99.75) for i in range(1440)]
    writes, fetches = [], []
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*args): return False
    def fetch(*args):
        fetches.append(args)
        return {**ROUTE, "candles":path}
    def write(conn, *, event_id, metrics):
        writes.append((event_id,metrics))
        return True
    with patch.object(worker,"psycopg",SimpleNamespace(connect=lambda *a,**k: Connection())), \
         patch.object(store,"load_due_events",return_value=[event]), \
         patch.object(store,"write_metrics",side_effect=write), \
         patch.object(worker.canonical_price_path,"fetch_closed_candles",side_effect=fetch):
        result = worker.ResearchOutcomeWorker()._run_common_window_due("test",now=START+timedelta(days=2))
    assert result["common_window_written"] == 4 and len(fetches) == 1
    assert {row["window_minutes"] for _,row in writes} == {60,240,720,1440}
    assert all(row["status"] == "READY" for _,row in writes)


def check_store_immutable_and_stale():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE research_common_window_metrics(event_id INTEGER,
        window_minutes INTEGER,method_version TEXT,status TEXT,result TEXT,
        next_attempt_at_utc TEXT,updated_at_utc TEXT,PRIMARY KEY(event_id,window_minutes,method_version))""")
    db.execute("INSERT INTO research_common_window_metrics VALUES(9,60,?,'OPEN','{}',NULL,NULL)",(metrics.METHOD_VERSION,))
    class Adapter:
        def execute(self, query, params=()):
            query = query.replace("%s", "?").replace("::jsonb", "").replace("::timestamptz", "").replace("NOW()", "CURRENT_TIMESTAMP")
            params = tuple(value.isoformat(sep=" ") if isinstance(value, datetime) else value for value in params)
            return db.execute(query, params)
    adapter = Adapter()
    path = [candle(i, high=102, low=99) for i in range(60)]
    early = compute(path, observed_at=START+timedelta(minutes=20))
    assert store.write_metrics(adapter,event_id=9,metrics=early)
    assert not store.write_metrics(adapter,event_id=9,metrics=compute(path,observed_at=START+timedelta(minutes=10)))
    ready = compute(path)
    assert store.write_metrics(adapter,event_id=9,metrics=ready)
    assert not store.write_metrics(adapter,event_id=9,metrics={**early,"observed_at_utc":START+timedelta(days=3)})
    stored = json.loads(db.execute("SELECT result FROM research_common_window_metrics").fetchone()[0])
    assert stored["status"] == "READY" and stored["mfe_pct"] == 2 and stored["event_id"] == 9
    db.close()


def main():
    check_full_window_vs_first_touch()
    check_causality_coverage_and_quality()
    check_asymmetry_cohort()
    check_terminal_maturity_worker()
    check_store_immutable_and_stale()
    print("PASS common-window causal path, horizon coverage, provenance, inverse, ambiguity, zero MAE, cohort and terminal-maturity worker")


if __name__ == "__main__": main()
