"""Frozen Top8 operational scores in the existing immutable Watch archive.

No fetching, replayed scoring, alerts, Sheets or downstream cohort changes.
The archive set is the durable source; consumers must respect both its
available_at_utc and created_at_utc, not only this block's computed time.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any

import alert_engine
import market_confidence_engine

VERSION = "watch-operational-scores-v2"
HASH_VERSION = "json-integer-float-zero-normalized-v1"
POPULATION = "all-top8-watch-scans-before-display-v1"
SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP")
MAX_BYTES = 256 * 1024
ADDITIVE_COMPONENTS = ("directional_alignment", "target_proximity", "cluster_confidence", "relative_gap")
_METRICS = {"attempts": 0, "persisted": 0, "gaps": 0, "last": None}


def _numeric_normalized(value: Any) -> Any:
    # JSONB normalizes -0.0 and expands exponent notation. Normalize BEFORE
    # hashing and serializing so database readback has the same identity.
    # This changes representation only; no decimal rounding is performed.
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _numeric_normalized(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_numeric_normalized(item) for item in value]
    return value


def canonical(value: Any) -> str:
    return json.dumps(_numeric_normalized(value), sort_keys=True, separators=(",", ":"), default=str,
                      ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("source timestamp has no UTC offset")
    return parsed.astimezone(timezone.utc)


@lru_cache(maxsize=1)
def code_versions() -> dict:
    root = Path(__file__).parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in (
        "alert_engine.py", "market_confidence_engine.py", "time_family_engine.py",
        "coinglass_flow_engine.py", "coinglass_oi_regime_service.py", "live_price_provider.py",
    )}


def failure(cycle_id: str, reason: str) -> dict:
    return {"version": VERSION, "population": POPULATION, "cycle_id": cycle_id,
            "status": "FAILED", "reason": reason[:500], "symbols_expected": list(SYMBOLS)}


def prepare(rows: list, snapshot: dict, *, limit: int = 500) -> tuple[list, dict, dict]:
    """Run the usual scorer once, plus scalar capture before its global cut."""
    frozen = {symbol: [] for symbol in SYMBOLS}
    items = alert_engine.build_opportunities(rows, limit=limit, capture_by_symbol=frozen)
    items = market_confidence_engine.attach_to_opportunities(items, snapshot_by_symbol=snapshot)
    evidence = {}
    for item in items:
        if item["symbol"] in SYMBOLS:
            evidence.setdefault(item["symbol"], item["market_evidence"])
    for symbol in SYMBOLS:
        if symbol not in evidence and symbol in snapshot:
            # Missing/inactive Max-Pain targets or the global limit must not
            # discard already observed derivatives. These explicit mappings
            # prohibit combine() from fetching a different generation.
            captured = snapshot[symbol]
            evidence[symbol] = market_confidence_engine.combine(
                symbol, "NEUTRAL", captured.get("regime") or {}, captured.get("flow") or {},
            )
    return items, frozen, evidence


def _pick(value: dict, keys: tuple) -> dict:
    return {key: deepcopy(value[key]) for key in keys if key in value}


def build_bundle(*, cycle_id: str, rows: list, snapshot: dict, frozen: dict,
                 evidence: dict, computed_at_utc: Any, watch_threshold: float) -> dict:
    computed = _utc(computed_at_utc)
    by_pair = {}
    for row in rows:
        key = (str(row.get("symbol") or "").upper(), row.get("timeframe"))
        if key in by_pair:
            raise ValueError("operational input contains duplicate symbol/timeframe")
        by_pair[key] = row
    coins = {}
    for symbol in SYMBOLS:
        entries = {(x["timeframe"], x["source_side"]): x for x in frozen[symbol]}
        if len(entries) != len(frozen[symbol]):
            raise ValueError("duplicate frozen score slot")
        source_rows, slots, time_errors = [], [], []
        for tf in alert_engine.TIMEFRAMES:
            row = by_pair.get((symbol, tf))
            if row is not None:
                source_rows.append({"timeframe": tf, **_pick(row, (
                    "current_price", "rank", "source_observed_at_utc", "price_source",
                    "price_pair", "price_market", "price_instrument", "price_fetched_at_utc",
                    "short_max_pain", "long_max_pain", "short_liquidation_amount", "long_liquidation_amount",
                ))})
            for side in ("LONG", "SHORT"):
                slot = deepcopy(entries.get((tf, side)))
                if slot is None:
                    slot = {"timeframe": tf, "source_side": side, "score": None,
                            "status": "MISSING_INPUT" if row is None else "INACTIVE_TARGET"}
                else:
                    slot["status"] = "SCORED"
                    slot["missing_fields"] = [key for key in ("near_amount", "far_amount", "near_share_pct") if key not in slot]
                    total = round(sum(slot["components"][key] for key in ADDITIVE_COMPONENTS), 2)
                    if abs(total - slot["score"]) > 0.011:
                        raise ValueError("frozen additive score mismatch")
                slots.append(slot)

        captured = snapshot.get(symbol) or {}
        regime, flow = captured.get("regime") or {}, captured.get("flow") or {}
        sources = {
            "maxpain_operational_rows": source_rows,
            "derivatives_snapshot_sha256": digest(captured) if captured else None,
            "timing_observation": deepcopy(captured.get("timing_observation") or {}),
            "positioning": _pick(regime, ("price", "open_interest_usd", "price_fetched_at", "oi_fetched_at",
                "price_source", "oi_source", "data_quality_status", "time_gap_seconds")),
        }
        for family in ("futures", "spot"):
            data = flow.get(family) or {}
            sources[family] = {"market": data.get("market", family),
                               "quality": deepcopy(data.get("quality") or {})}
        for key, data in (("positioning", regime), ("futures", flow.get("futures") or {}), ("spot", flow.get("spot") or {})):
            sources[key]["window_references"] = {
                label: _pick(window, ("available", "latest_time", "reference_time", "reference_target_time", "target_time", "comparison_source"))
                for label, window in (data.get("windows") or {}).items()
            }
        def check_time(value, path):
            if value is None or value == "":
                time_errors.append(path + ":MISSING_TIME")
                return
            try:
                if _utc(value) > computed:
                    time_errors.append(path + ":FUTURE_TIME")
            except (TypeError, ValueError):
                time_errors.append(path + ":INVALID_TIME")
        for row in source_rows:
            for key in ("source_observed_at_utc", "price_fetched_at_utc"):
                check_time(row.get(key), row["timeframe"] + "/" + key)
        if captured:
            for key in ("price_fetched_at", "oi_fetched_at"):
                check_time(regime.get(key), "positioning/" + key)
            for family in ("futures", "spot"):
                check_time(sources[family]["quality"].get("candle_close"), family + "/candle_close")
            check_time(sources["timing_observation"].get("cvd_observed_at_utc"), "derivatives/observed")
        modules = deepcopy((evidence.get(symbol) or {}).get("modules") or {})
        for module in modules.values():
            # The exact operational fallback 0 is retained alongside available
            # and status, so it cannot masquerade as an observed neutral score.
            module["capture_status"] = "AVAILABLE" if module.get("available") else "UNAVAILABLE"
            module.pop("relation", None)  # Depends on the displayed side, not a model score.
            module.pop("label", None)
        coins[symbol] = {
            "status": "CAPTURED" if len(source_rows) == 7 and modules else "PARTIAL" if source_rows or modules else "ABSENT",
            "maxpain": slots, "models": modules, "sources": sources,
            "source_time_errors": time_errors,
        }
    bundle = {
        "version": VERSION, "population": POPULATION, "cycle_id": cycle_id,
        "status": "COMPLETE" if all(c["status"] == "CAPTURED" for c in coins.values()) else "PARTIAL",
        "computed_at_utc": computed.isoformat(), "code_sha256": code_versions(),
        "hash_version": HASH_VERSION,
        "input_universe_sha256": digest(rows), "input_row_count": len(rows),
        "symbols_expected": list(SYMBOLS), "watch_display_threshold": watch_threshold,
        "maxpain_additive_components": list(ADDITIVE_COMPONENTS),
        "source_side_semantics": "liquidated-side; SHORT target implies price UP, LONG target price DOWN",
        "coins": coins,
    }
    encoded = canonical(bundle).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError("operational capture exceeds 256 KiB bound")
    bundle["payload_sha256"] = hashlib.sha256(encoded).hexdigest()
    return json.loads(canonical(bundle))


def record_persistence(block: dict, result: dict) -> None:
    _METRICS["attempts"] += 1
    persisted = bool(result.get("persisted"))
    _METRICS["persisted"] += int(persisted)
    _METRICS["gaps"] += int(not persisted or block.get("status") != "COMPLETE")
    _METRICS["last"] = {"cycle_id": block.get("cycle_id"), "capture_status": block.get("status"),
                        "at_utc": datetime.now(timezone.utc).isoformat(), **result}


def status() -> dict:
    return {"version": VERSION, "population": POPULATION, "symbols": list(SYMBOLS),
            "hash_version": HASH_VERSION,
            "durable_source": "research_max_pain_snapshot_sets.source_metadata.capture_metadata.operational_scores",
            "max_bytes": MAX_BYTES, **deepcopy(_METRICS)}
