"""Resumable archive-only delayed-entry v7 research; no production writes.

Every feature comes from one source message. Entry is the next full-minute
Binance Spot OPEN, under a separately frozen entry policy. Both direction
variants, all eight thresholds and four horizons are persisted independently.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

import binance_spot_price_path
import research_btc_parent_movement as parent_policy
from research_common_window_metrics import calculate_common_window_metrics
from research_ordered_first_touch import METHOD_VERSION, calculate_all_ordered_first_touch_outcomes
from research_telegram_archive_features import ENTRY_VERSION, FEATURE_VERSION, DIRECTION_VERSION, TIME_VERSION, extract_message, utc
from research_telegram_archive_stage_store import load_stage
from research_telegram_html_archive import _hash

WINDOWS = (60, 240, 720, 1440)
MINUTE = timedelta(minutes=1)
BACKFILL_VERSION = "archive-causal-delayed-entry-v7-backfill-v1"


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item), allow_nan=False)


class SpotCache:
    def __init__(self, path: Path, *, expected_sha256: str):
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected_sha256:
            raise ValueError("Reviewed Spot cache digest mismatch")
        self.source_sha256 = actual
        self.bars: dict[str, list[dict[str, Any]]] = {}
        self.opens: dict[str, list[datetime]] = {}
        self.extra_bars: list[dict[str, Any]] = []
        self.fetches = 0
        grouped: dict[str, dict[datetime, dict[str, Any]]] = {}
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for raw in csv.DictReader(handle):
                symbol = raw["symbol"]
                if raw["exchange"] != "binance" or raw["pair"] != symbol + "USDT" or raw.get("instrument") or symbol == "HYPE" or raw.get("market", "spot") != "spot":
                    raise ValueError("Cache contains non-canonical Binance Spot provenance")
                bar = parent_policy.validate_candle(raw)
                bar["volume"] = float(raw.get("volume") or 0)
                old = grouped.setdefault(symbol, {}).get(bar["open_time_utc"])
                if old is not None and old != bar:
                    raise ValueError("Conflicting duplicate cached Spot candle")
                grouped[symbol][bar["open_time_utc"]] = bar
        for symbol, bars in grouped.items():
            self._replace(symbol, bars)

    def _replace(self, symbol: str, bars: dict[datetime, dict[str, Any]]) -> None:
        self.opens[symbol] = sorted(bars)
        self.bars[symbol] = [bars[opened] for opened in self.opens[symbol]]

    def extend(self, symbol: str, start: datetime, end: datetime) -> None:
        if symbol == "HYPE":
            raise ValueError("HYPE must not be substituted with Binance Futures")
        if binance_spot_price_path.BINANCE_SPOT_BASE_URL != "https://data-api.binance.vision" or binance_spot_price_path.BINANCE_SPOT_KLINES_ENDPOINT != "/api/v3/klines":
            raise ValueError("Archive extension requires the reviewed official Binance Spot endpoint")
        import research_archived_price_path
        result = research_archived_price_path.fetch_binance(symbol, start, end)
        if result.get("exchange") != "binance" or result.get("market") != "spot" or result.get("pair") != symbol + "USDT":
            raise ValueError("Official Spot extension returned wrong source")
        current = {bar["open_time_utc"]: bar for bar in self.bars.get(symbol, [])}
        for raw in result["candles"]:
            bar = parent_policy.validate_candle(raw)
            bar["volume"] = float(raw.volume if hasattr(raw, "volume") else raw.get("volume") or 0)
            if bar["open_time_utc"] in current and current[bar["open_time_utc"]] != bar:
                raise ValueError("Fresh official Spot candle conflicts with reviewed cache")
            if bar["open_time_utc"] not in current:
                self.extra_bars.append({"symbol": symbol, **bar})
            current[bar["open_time_utc"]] = bar
        self._replace(symbol, current)
        self.fetches += 1

    def restore_extensions(self, path: Path) -> None:
        if not path.exists():
            return
        grouped = {symbol: {bar["open_time_utc"]: bar for bar in bars} for symbol, bars in self.bars.items()}
        for line in path.read_text().splitlines():
            raw = json.loads(line)
            if raw.get("exchange") != "binance" or raw.get("market") != "spot" or raw.get("source_url") != "https://data-api.binance.vision/api/v3/klines":
                raise ValueError("Unrecognized extension provenance")
            symbol = raw["symbol"]
            if symbol == "HYPE":
                raise ValueError("Unsupported archive Spot symbol")
            bar = parent_policy.validate_candle(raw)
            bar["volume"] = float(raw.get("volume") or 0)
            old = grouped.setdefault(symbol, {}).get(bar["open_time_utc"])
            if old is not None and old != bar:
                raise ValueError("Extension candle conflicts with cache")
            grouped[symbol][bar["open_time_utc"]] = bar
        for symbol, bars in grouped.items():
            self._replace(symbol, bars)

    def save_extensions(self, path: Path) -> None:
        with path.open("a", encoding="utf-8") as handle:
            for bar in self.extra_bars:
                handle.write(canonical({**bar, "exchange": "binance", "market": "spot", "source_url": "https://data-api.binance.vision/api/v3/klines"}) + "\n")
        self.extra_bars.clear()

    def window(self, symbol: str, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], bool]:
        opens = self.opens.get(symbol, [])
        rows = self.bars.get(symbol, [])[bisect_left(opens, start):bisect_left(opens, end)]
        expected = int((end - start).total_seconds() // 60)
        complete = len(rows) == expected and all(row["open_time_utc"] == start + index * MINUTE for index, row in enumerate(rows))
        return rows, complete

    def entry(self, symbol: str, start: datetime) -> float | None:
        opens = self.opens.get(symbol, [])
        index = bisect_left(opens, start)
        return self.bars[symbol][index]["open"] if index < len(opens) and opens[index] == start else None


def parent_membership(cache: SpotCache, parents: list[dict[str, Any]], decision: datetime) -> dict[str, Any]:
    index = bisect_right([parent["start_time_utc"] for parent in parents], decision) - 1
    bar_index = bisect_right(cache.opens.get("BTC", []), decision - MINUTE + timedelta(milliseconds=1)) - 1
    if index < 0 or bar_index < 0:
        return {"membership_status": "BTC_DATA_MISSING", "btc_parent_movement_id": None}
    # The pure shared function expects numeric native ids; source identity stays
    # in the caller's event key and only the returned membership is retained.
    result = parent_policy.membership({"event_id": 0, "alert_time_utc": decision}, parent=parents[index], btc_bar=cache.bars["BTC"][bar_index])
    result.pop("event_id", None)
    result["parent_evidence_eligible"] = parents[index]["evidence_eligible"]
    result["parent_start_time_utc"] = parents[index]["start_time_utc"]
    return result


def initialize(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS archive_reconstruction_runs (
            run_key TEXT PRIMARY KEY, contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS archive_reconstructed_events (
            run_key TEXT NOT NULL, event_key TEXT NOT NULL, source_time_utc TEXT,
            symbol TEXT, reconstruction_status TEXT NOT NULL, calculation_status TEXT NOT NULL DEFAULT 'PENDING',
            event_json TEXT NOT NULL, PRIMARY KEY(run_key,event_key)
        );
        CREATE TABLE IF NOT EXISTS archive_delayed_entry_outcomes (
            run_key TEXT NOT NULL, event_key TEXT NOT NULL, signal_variant TEXT NOT NULL,
            window_minutes INTEGER NOT NULL, threshold_bps INTEGER NOT NULL, outcome_id TEXT NOT NULL,
            status TEXT NOT NULL, outcome_json TEXT NOT NULL,
            PRIMARY KEY(run_key,event_key,signal_variant,window_minutes,threshold_bps)
        );
        CREATE TABLE IF NOT EXISTS archive_common_window_metrics (
            run_key TEXT NOT NULL, event_key TEXT NOT NULL, signal_variant TEXT NOT NULL,
            window_minutes INTEGER NOT NULL, status TEXT NOT NULL, metrics_json TEXT NOT NULL,
            PRIMARY KEY(run_key,event_key,signal_variant,window_minutes)
        );
        CREATE TABLE IF NOT EXISTS archive_btc_parents (
            run_key TEXT NOT NULL, btc_parent_movement_id TEXT NOT NULL, parent_json TEXT NOT NULL,
            PRIMARY KEY(run_key,btc_parent_movement_id)
        );
        CREATE INDEX IF NOT EXISTS archive_pending_calculations
            ON archive_reconstructed_events(run_key,calculation_status,source_time_utc,event_key);
    """)


def evaluator_features(event: dict[str, Any]) -> dict[str, Any]:
    """Map only source-equivalent fields; keep original predicate orientation."""
    import math
    from zoneinfo import ZoneInfo
    f = dict(event["features"])
    current, target = event.get("original_printed_price"), f.get("maxpain.target_price")
    target_direction = ("LONG" if target > current else "SHORT" if target < current else "NEUTRAL") if current and target else None
    verified = event.get("analysis_direction") in {"LONG", "SHORT"} and not event.get("conflicts") and target_direction in (None, event["analysis_direction"])
    f.update({"event.direction_mapping_valid": verified, "event.analysis_direction": event.get("analysis_direction"), "event.displayed_side": event.get("displayed_direction"), "event.strategy_version": DIRECTION_VERSION})
    timestamp = utc(event["source_message_time_utc"]).astimezone(ZoneInfo("Asia/Jerusalem"))
    f.update({"time.hour_israel": timestamp.hour, "time.weekday_israel": timestamp.weekday(), "time.weekend": timestamp.weekday() >= 5})
    aliases = {"selected_score": "selected_score", "opposite_score": "opposite_score", "selected_average": "average_score_all_timeframes", "opposite_average": "opposite_average_score_all_timeframes", "gap_score": "components.relative_gap", "selected_opposite_score_difference": "selected_opposite_difference", "selected_opposite_score_ratio": "selected_opposite_ratio", "target_distance_pct": "distance_pct"}
    if verified:
        for raw, mapped in aliases.items():
            if "maxpain." + raw in f:
                f["max_pain." + mapped] = f["maxpain." + raw]
        numerator, denominator = f.get("maxpain.consensus_hits"), f.get("maxpain.consensus_total")
        if numerator is not None and denominator is not None and int(numerator) == numerator and int(denominator) == denominator and 0 <= numerator <= denominator and denominator > 0:
            f.update({"max_pain.consensus_hits": int(numerator), "max_pain.consensus_total": int(denominator), "max_pain.consensus_hits_ratio": numerator / denominator, "max_pain.consensus_hits_full": numerator == denominator})
    share, selected, opposite = (f.get("liquidity." + key) for key in ("selected_share_pct", "selected_amount", "opposite_amount"))
    valid_amounts = all(value is not None and value >= 0 for value in (selected, opposite)) and selected + opposite > 0
    if verified and target_direction == event.get("analysis_direction") and share is not None and 0 <= share <= 100 and valid_amounts and math.isclose(share, 100 * selected / (selected + opposite), abs_tol=0.05):
        f.update({"liquidity.capture_status": "VALID", "liquidity.source_contract": "ARCHIVE_PRINTED_SELECTED_AMOUNTS_V1", "liquidity.alignment": "SUPPORTS" if share >= 60 else "OPPOSES" if share <= 40 else "BALANCED"})
    else:
        # Printed source values remain under explicit captured names but are
        # unavailable to native-source liquidity predicates until validated.
        for key in ("selected_share_pct", "selected_amount", "opposite_amount"):
            if "liquidity." + key in f:
                f["captured.archive_liquidity." + key] = f.pop("liquidity." + key)
        f["liquidity.capture_status"] = "MISSING" if share is None else "UNVERIFIED"
    return f


def iter_evidence_rows(database_path: Path, *, run_key: str):
    """Adapt isolated archive rows for the pure evaluator, preserving cohort.

    Callers must register candidates/scopes under this source and entry version,
    never insert these rows into native LIVE event tables.
    """
    with sqlite3.connect(f"file:{database_path}?mode=ro", uri=True) as conn:
        surrogate_sources: dict[int, str] = {}
        query = """SELECT e.event_json,o.outcome_json,m.metrics_json
            FROM archive_reconstructed_events e LEFT JOIN archive_delayed_entry_outcomes o
              ON e.run_key=o.run_key AND e.event_key=o.event_key
            LEFT JOIN archive_common_window_metrics m ON m.run_key=o.run_key
              AND m.event_key=o.event_key AND m.signal_variant=o.signal_variant
              AND m.window_minutes=o.window_minutes
            WHERE e.run_key=? AND e.reconstruction_status='READY_FOR_SPOT_ENTRY_PATH'
            ORDER BY e.source_time_utc,e.event_key,o.signal_variant,o.window_minutes,o.threshold_bps"""
        for event_json, outcome_json, metric_json in conn.execute(query, (run_key,)):
            event = json.loads(event_json)
            outcomes = [json.loads(outcome_json)] if outcome_json else [
                {"event_id": _hash(event["archive_event_key"], variant), "signal_variant": variant, "direction": event["analysis_direction"] if variant == "NORMAL" else ("SHORT" if event["analysis_direction"] == "LONG" else "LONG"), "window_minutes": window, "threshold_bps": threshold,
                    "status": "DATA_MISSING", "calculation_status": "NOT_CALCULATED_SOURCE_PATH_UNAVAILABLE"}
                for variant in ("NORMAL", "INVERSE") for window in WINDOWS for threshold in range(25,201,25)]
            features = evaluator_features(event)
            for outcome in outcomes:
                # Pure evaluation uses checked local numeric surrogates; the
                # full source id is always retained and never becomes LIVE.
                archive_id = outcome["event_id"]
                surrogate = int(archive_id[:15], 16) + 1
                if surrogate in surrogate_sources and surrogate_sources[surrogate] != archive_id:
                    raise ValueError("Archive evaluator surrogate identity collision")
                surrogate_sources[surrogate] = archive_id
                outcome = {**outcome, "archive_event_id": archive_id, "event_id": surrogate}
                yield {
                "event_id": surrogate, "snapshot_id": event["source_identity_key"],
                "source_event_key": event["archive_event_key"], "source_scope": "ARCHIVE_ONLY",
                "record_mode": "ARCHIVE", "entry_policy_version": ENTRY_VERSION,
                "feature_version": FEATURE_VERSION, "time_policy_version": TIME_VERSION,
                "source_revision_sha256": event["source_revision_sha256"],
                "period_scope_ids": event["period_scope_ids"], "signal_variant": outcome["signal_variant"],
                "symbol": event["symbol"], "direction": outcome["direction"],
                "entry_price": event.get("entry_price"),
                "alert_time_utc": event["entry_time_utc"], "decision_time_utc": event["entry_time_utc"],
                "features_observed_at_utc": event["source_message_time_utc"], "decision_features": features,
                "btc_parent_movement_id": event.get("btc_parent_movement_id"),
                "parent_evidence_eligible": event.get("parent_evidence_eligible", False),
                "parent_start_time_utc": event.get("parent_start_time_utc"),
                "membership_status": event.get("membership_status"),
                "episode_policy_version": parent_policy.POLICY_VERSION,
                "ordered_outcome": outcome, "common_window_metrics": json.loads(metric_json) if metric_json else None,
                "statistical_phase": "DISCOVERY", "live_union_eligible": False,
            }


def calculate_event(event: dict[str, Any], cache: SpotCache, parents: list[dict[str, Any]], *, observed_at: datetime) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    entry_time = utc(event["entry_time_utc"])
    if entry_time > observed_at:
        return {**event, "calculation_status": "NOT_YET_ENTRY", "entry_price": None}, [], []
    reference = cache.entry(event["symbol"], entry_time)
    event = {**event, "entry_price": reference, "entry_price_source": "BINANCE_SPOT_1M_OPEN" if reference else None}
    event.update(parent_membership(cache, parents, entry_time))
    if reference is None:
        event["calculation_status"] = "UNSUPPORTED_SPOT_SYMBOL" if event["symbol"] == "HYPE" else "DATA_MISSING_ENTRY_CANDLE"
        return event, [], []
    labels, metrics = [], []
    for variant in ("NORMAL", "INVERSE"):
        direction = event["analysis_direction"] if variant == "NORMAL" else ("SHORT" if event["analysis_direction"] == "LONG" else "LONG")
        event_id = _hash(event["archive_event_key"], variant)
        for horizon in WINDOWS:
            end = entry_time + timedelta(minutes=horizon)
            cutoff = min(end, observed_at.replace(second=0, microsecond=0))
            path, complete = cache.window(event["symbol"], entry_time, cutoff)
            route = {"symbol": event["symbol"], "exchange": "binance", "market": "spot", "pair": event["symbol"] + "USDT", "interval": "1m", "interval_seconds": 60, "complete": complete, "provenance": "REVIEWED_OFFICIAL_BINANCE_SPOT_CACHE_AND_API"}
            full = calculate_common_window_metrics(symbol=event["symbol"], reference_price=reference, direction=direction, event_time=entry_time, window_minutes=horizon, candles=path, observed_at=observed_at, path_result=route)
            full.update({"event_id": event_id, "signal_variant": variant, "entry_policy_version": ENTRY_VERSION, "source_scope": "ARCHIVE_ONLY"})
            metrics.append(full)
            for label in calculate_all_ordered_first_touch_outcomes(reference_price=reference, direction=direction, event_time=entry_time, candles=path, observation_closed=observed_at >= end, path_complete=complete):
                label.update({"event_id": event_id, "outcome_id": _hash(event_id, METHOD_VERSION, horizon, label["threshold_bps"]), "window_minutes": horizon, "outcome_method_version": METHOD_VERSION,
                    "data_quality_status": "VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES" if complete else "INCOMPLETE_BINANCE_SPOT_1M_PATH", "signal_variant": variant,
                    "source_scope": "ARCHIVE_ONLY", "record_mode": "ARCHIVE", "entry_policy_version": ENTRY_VERSION, "source_price_exchange": "binance", "source_price_market": "spot"})
                labels.append(label)
    event["calculation_status"] = "COMPLETE_64_LABELS" if all(label["path_complete"] and label["observation_closed"] for label in labels) else "RETRY_INCOMPLETE_PATH"
    return event, labels, metrics


def run(*, stage_dir: Path, cache_path: Path, expected_cache_sha256: str, output_dir: Path, event_limit: int, observed_at: datetime, allow_network: bool = False, max_fetches: int = 32) -> dict[str, Any]:
    if not 1 <= event_limit <= 15000 or not 0 <= max_fetches <= 100:
        raise ValueError("Bounded event/fetch limits required")
    manifest, sources = load_stage(stage_dir)
    events = [extract_message(row, manifest) for row in sources]
    output_dir.mkdir(parents=True, exist_ok=True)
    cache = SpotCache(cache_path, expected_sha256=expected_cache_sha256)
    extensions = output_dir / "official_spot_extensions.jsonl"
    cache.restore_extensions(extensions)
    failures = []
    if allow_network:
        for symbol in sorted({event["symbol"] for event in events if event["reconstruction_status"] == "READY_FOR_SPOT_ENTRY_PATH"} - {"HYPE"}):
            end = min(max(utc(event["entry_time_utc"]) for event in events if event["symbol"] == symbol and event.get("entry_time_utc")) + timedelta(minutes=1440), observed_at.replace(second=0, microsecond=0))
            cursor = cache.opens[symbol][-1] + MINUTE if cache.opens.get(symbol) else min(utc(event["entry_time_utc"]) for event in events if event["symbol"] == symbol and event.get("entry_time_utc"))
            while cursor < end and cache.fetches < max_fetches:
                next_end = min(end, cursor + timedelta(minutes=1440))
                try:
                    cache.extend(symbol, cursor, next_end)
                    cache.save_extensions(extensions)
                except Exception as exc:
                    failures.append({"symbol": symbol, "start": cursor.isoformat(), "error": str(exc)[:240]})
                    break
                cursor = next_end
    btc = [bar for bar in cache.bars.get("BTC", []) if bar["close_time_utc"] <= observed_at]
    parents = parent_policy.advance_parents(btc, as_of_utc=observed_at)
    contract = {"backfill_version": BACKFILL_VERSION, "prepared_stage_digest": manifest["prepared_stage_digest"], "entry_policy_version": ENTRY_VERSION, "feature_version": FEATURE_VERSION, "direction_version": DIRECTION_VERSION, "time_version": TIME_VERSION,
        "parent_policy_version": parent_policy.POLICY_VERSION, "source_scope": "ARCHIVE_ONLY", "threshold_bps": list(range(25, 201, 25)), "window_minutes": list(WINDOWS), "variants": ["NORMAL", "INVERSE"], "cache_sha256": cache.source_sha256,
        "live_union_eligible": False, "phase": "DISCOVERY", "formula_relevance": "NOT_EVALUATED"}
    run_key = _hash(contract)
    processed = label_count = 0
    with sqlite3.connect(output_dir / "archive_reconstructed_research.sqlite") as conn:
        initialize(conn)
        conn.execute("INSERT OR IGNORE INTO archive_reconstruction_runs VALUES (?, ?)", (run_key, canonical(contract)))
        conn.executemany("INSERT OR IGNORE INTO archive_reconstructed_events(run_key,event_key,source_time_utc,symbol,reconstruction_status,event_json) VALUES (?,?,?,?,?,?)", [(run_key, event["archive_event_key"], event.get("source_message_time_utc"), event["symbol"], event["reconstruction_status"], canonical(event)) for event in events])
        conn.executemany("INSERT OR REPLACE INTO archive_btc_parents VALUES (?,?,?)", [(run_key, parent["btc_parent_movement_id"], canonical(parent)) for parent in parents])
        conn.commit()
        pending = conn.execute("SELECT event_key,event_json FROM archive_reconstructed_events WHERE run_key=? AND reconstruction_status='READY_FOR_SPOT_ENTRY_PATH' AND calculation_status IN ('PENDING','RETRY_INCOMPLETE_PATH','DATA_MISSING_ENTRY_CANDLE','NOT_YET_ENTRY') ORDER BY CASE WHEN calculation_status='PENDING' THEN 0 ELSE 1 END,source_time_utc,event_key LIMIT ?", (run_key, event_limit)).fetchall()
        for key, encoded in pending:
            event, labels, metrics = calculate_event(json.loads(encoded), cache, parents, observed_at=observed_at)
            conn.executemany("INSERT OR REPLACE INTO archive_delayed_entry_outcomes VALUES (?,?,?,?,?,?,?,?)", [(run_key, key, label["signal_variant"], label["window_minutes"], label["threshold_bps"], label["outcome_id"], label["status"], canonical(label)) for label in labels])
            conn.executemany("INSERT OR REPLACE INTO archive_common_window_metrics VALUES (?,?,?,?,?,?)", [(run_key, key, metric["signal_variant"], metric["window_minutes"], metric["status"], canonical(metric)) for metric in metrics])
            conn.execute("UPDATE archive_reconstructed_events SET calculation_status=?,event_json=? WHERE run_key=? AND event_key=?", (event["calculation_status"], canonical(event), run_key, key))
            conn.commit()
            processed += 1
            label_count += len(labels)
        report = {**contract, "run_key": run_key, "processed_this_invocation": processed, "labels_this_invocation": label_count, "reconstruction_counts": dict(Counter(event["reconstruction_status"] for event in events)),
            "calculation_status_counts": dict(conn.execute("SELECT calculation_status,COUNT(*) FROM archive_reconstructed_events WHERE run_key=? AND reconstruction_status='READY_FOR_SPOT_ENTRY_PATH' GROUP BY calculation_status", (run_key,)).fetchall()),
            "outcome_status_counts": dict(conn.execute("SELECT status,COUNT(*) FROM archive_delayed_entry_outcomes WHERE run_key=? GROUP BY status", (run_key,)).fetchall()),
            "fixed_window_status_counts": dict(conn.execute("SELECT status,COUNT(*) FROM archive_common_window_metrics WHERE run_key=? GROUP BY status", (run_key,)).fetchall()),
            "parent_movements": len(parents), "eligible_parent_movements": sum(parent["evidence_eligible"] for parent in parents), "parent_ids": [parent["btc_parent_movement_id"] for parent in parents if parent["evidence_eligible"]],
            "official_spot_fetches_this_invocation": cache.fetches, "fetch_failures": failures, "observed_at_utc": observed_at.isoformat(), "production_rows_written": 0}
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("Reconstruction database integrity check failed")
    (output_dir / "archive_reconstruction_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-dir", type=Path, required=True)
    parser.add_argument("--spot-cache", type=Path, required=True)
    parser.add_argument("--expected-cache-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--event-limit", type=int, default=128)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--max-fetches", type=int, default=32)
    args = parser.parse_args()
    print(json.dumps(run(stage_dir=args.stage_dir, cache_path=args.spot_cache, expected_cache_sha256=args.expected_cache_sha256, output_dir=args.output_dir, event_limit=args.event_limit, observed_at=utc(args.observed_at), allow_network=args.allow_network, max_fetches=args.max_fetches), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
