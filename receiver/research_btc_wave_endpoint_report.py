"""Read-only, explicit full-BTC-wave research report.

This is a different observation contract from the production 60/240/720/1440m
outcomes. It never updates those labels, changes an alert's immutable entry,
or admits Futures prices into the canonical Spot cohort. Its CLI reads a
bounded set of parent IDs and writes a reviewable JSON report only.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import binance_futures_mark_price_path as mark_path
import canonical_price_path
from research_btc_parent_movement import POLICY_VERSION, validate_candle
from research_common_window_metrics import utc
from research_ordered_first_touch import (
    SUPPORTED_THRESHOLDS_PCT, _excursion_metrics, _finite_number,
    calculate_all_ordered_first_touch_outcomes,
)

METHOD_VERSION = "btc-wave-endpoint-closed-1m-v1"
CANDIDATE = "MAX_PAIN_ALERT score>=65"
SPOT_SCOPE = "CANONICAL_SPOT_IMMUTABLE_ALERT"
MARK_SCOPE = "DERIVED_NATIVE_HYPE_MARK"
PERP_SCOPE = "DERIVED_NATIVE_HYPE_PERP"
PERP_MIXED_SCOPE = "SPOT_WITH_DERIVED_HYPE_PERP_V1"
MIXED_SCOPE = "SPOT_WITH_DERIVED_HYPE_MARK_V1"
REPRESENTATIVE_POLICY = "FIRST_EVENT_TIME_THEN_ID_PER_WAVE_SCOPE_DIRECTION_BEFORE_OUTCOMES"
ASYMMETRY_METHOD = "SUM_MFE_OVER_SUM_MAE_SAME_COMPLETE_CLOSED_WAVE_COHORT"
MAX_WAVES = 32
MAX_EVENTS_PER_WAVE = 5000
MAX_PATH_MINUTES = 31 * 1440
MINUTE = timedelta(minutes=1)
MILLISECOND = timedelta(milliseconds=1)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return utc(value).isoformat()
    raise TypeError(f"Cannot encode {type(value).__name__}")


def _first_open(start: datetime) -> datetime:
    floor = start.replace(second=0, microsecond=0)
    return floor if start == floor else floor + MINUTE


def latest_closed_cutoff(observed_at: Any) -> datetime:
    return utc(observed_at).replace(second=0, microsecond=0) - MILLISECOND


def wave_observation_cutoff(wave: Mapping, observed_at: Any) -> datetime:
    latest = latest_closed_cutoff(observed_at)
    if wave.get("end_time_utc"):
        # The causal reversal close itself is the known inclusive endpoint;
        # the preceding parent's observed_through is intentionally one bar older.
        return min(utc(wave["end_time_utc"]), latest)
    if not wave.get("observed_through_utc"):
        raise ValueError("Active parent requires its verified BTC observation boundary")
    return min(utc(wave["observed_through_utc"]), latest)


def select_representatives(events: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Select before testing price availability, outcome, or source eligibility.

    ALL is a separate comparison from each coin. LONG and SHORT remain
    separate strata: the same parent is never counted twice in one stratum.
    A missing first representative cannot be replaced by a later winner.
    """
    selected: dict[tuple[str, str, str], dict] = {}
    for raw in sorted(events, key=lambda e: (utc(e["alert_time_utc"]), int(e["event_id"]))):
        event = dict(raw)
        try:
            score = _finite_number(event.get("score"), name="score")
        except ValueError:
            continue
        symbol = str(event.get("symbol") or "").strip().upper()
        direction = str(event.get("direction") or "").strip().upper()
        if (event.get("event_kind") != "ALERT" or event.get("event_type") != "MAX_PAIN_ALERT"
                or event.get("delivery_status") != "DELIVERED" or score < 65
                or not symbol or direction not in {"LONG", "SHORT"}):
            continue
        wave_id = str(event["btc_parent_movement_id"])
        for scope in ("ALL", symbol):
            key = (wave_id, scope, direction)
            selected.setdefault(key, {**event, "coin_scope": scope, "symbol": symbol,
                                      "direction": direction})
    return [selected[key] for key in sorted(selected)]


def _membership_error(event: Mapping, wave: Mapping) -> str | None:
    if (wave.get("episode_policy_version") != POLICY_VERSION
            or wave.get("evidence_eligible") is not True):
        return "BTC_PARENT_BOUNDARY_NOT_ELIGIBLE"
    if wave.get("end_time_utc") and wave.get("closing_boundary_verified") is not True:
        return "BTC_PARENT_END_IS_NOT_A_VERIFIED_CAUSAL_REVERSAL"
    if (event.get("membership_status") != "LIVE"
            or event.get("membership_policy_version") != POLICY_VERSION
            or event.get("membership_parent_id") != wave["btc_parent_movement_id"]):
        return "CANONICAL_BTC_MEMBERSHIP_MISSING"
    start = utc(event["alert_time_utc"])
    observed = event.get("btc_observed_close_utc")
    if (not observed or not start - MINUTE < utc(observed) <= start
            or utc(event.get("decision_time_utc")) != start
            or start < utc(wave["start_time_utc"])
            or (wave.get("end_time_utc") and start >= utc(wave["end_time_utc"]))):
        return "CANONICAL_BTC_MEMBERSHIP_INVALID"
    return None


def _spot_reference_error(event: Mapping) -> str | None:
    # Reuse the same authoritative reference gate as production v7. Import
    # lazily so simple cohort selection and report reading need no DB driver.
    from research_outcome_worker import _alert_reference_provenance_error
    return _alert_reference_provenance_error(event)


def fetch_full_path(symbol: str, start_time: Any, end_time: Any, *, source_scope: str,
                    fetcher: Callable | None = None) -> dict:
    """Page the whole wave in <= 1440m provider requests, without overlap.

    Providers have smaller per-request limits than some causal BTC waves.
    Empty/missing pages are retained as missing, never forward-filled.
    """
    start, end = utc(start_time), utc(end_time)
    first = _first_open(start)
    if end - start > timedelta(minutes=MAX_PATH_MINUTES):
        raise ValueError("Full wave exceeds the explicit 31-day path budget")
    if source_scope not in {SPOT_SCOPE, MARK_SCOPE, PERP_SCOPE}:
        raise ValueError("Unsupported explicit report source scope")
    if source_scope in {MARK_SCOPE, PERP_SCOPE} and symbol != "HYPE":
        raise ValueError("Derived MARK source supports native HYPE only")
    if source_scope == PERP_SCOPE:
        import hyperliquid_perp_price_path as perp_path
        default_fetcher = perp_path.fetch_closed_candles
    else:
        default_fetcher = (mark_path.fetch_closed_candles if source_scope == MARK_SCOPE
                           else canonical_price_path.fetch_closed_candles)
    get = fetcher or default_fetcher
    cursor, rows, metadata, requests = first, [], None, 0
    while cursor + MINUTE - MILLISECOND <= end:
        stop = min(end, cursor + timedelta(minutes=1440) - MILLISECOND)
        page = dict(get(symbol, cursor, stop))
        route = {key: page.get(key) for key in ("symbol", "exchange", "market", "pair",
                  "api_coin", "instrument", "margin_currency", "source_url", "interval",
                  "interval_seconds", "price_kind", "method_version", "provenance")}
        if metadata is not None and any(metadata.get(key) != route[key] for key in route):
            raise ValueError("Price route changed within the full wave")
        metadata = {key: value for key, value in page.items() if key != "candles"}
        rows.extend(page.get("candles") or [])
        requests += 1
        cursor += timedelta(minutes=1440)
    if metadata is None:
        raise ValueError("NO_CLOSED_POST_ENTRY_CANDLES")
    return {**metadata, "candles": rows, "request_chunks": requests,
            "complete": False}  # Exact completeness is independently checked below.


def calculate_wave_record(*, event: Mapping, wave: Mapping, path_result: Mapping,
                          observed_at: Any, source_scope: str = SPOT_SCOPE,
                          derived_entry: Mapping | None = None) -> dict:
    """Measure a complete prefix and retain eight ordered symmetric barriers.

    Full-wave metrics include every bar through the endpoint, even after a
    terminal first touch. Active-wave metrics are explicitly provisional;
    their completed first-touch labels do not turn the wave into a closed one.
    """
    now = utc(observed_at)
    original_start = utc(event["alert_time_utc"])
    wave_end = utc(wave["end_time_utc"]) if wave.get("end_time_utc") else None
    cutoff = wave_observation_cutoff(wave, now)
    closed = bool(wave_end is not None and now >= wave_end)
    start, reference = original_start, _finite_number(event.get("current_price"), name="immutable entry")
    errors = []
    membership_error = _membership_error(event, wave)
    if membership_error:
        errors.append(membership_error)
    if source_scope == SPOT_SCOPE:
        error = _spot_reference_error(event)
        if error:
            errors.append(error)
        try:
            route = canonical_price_path.validated_route(event["symbol"], dict(path_result), require_complete=False)
            if path_result.get("symbol") != event["symbol"]:
                raise ValueError("Returned Spot symbol does not match the event")
        except (TypeError, ValueError) as exc:
            route = None
            errors.append(str(exc))
    elif source_scope in {MARK_SCOPE, PERP_SCOPE}:
        if source_scope == PERP_SCOPE:
            import hyperliquid_perp_price_path as perp_path
            from research_native_hype_perp_supplement import ENTRY_VERSION
        else:
            from research_native_hype_mark_supplement import ENTRY_VERSION
        if (event.get("symbol") != "HYPE" or not derived_entry
                or derived_entry.get("entry_policy_version") != ENTRY_VERSION
                or derived_entry.get("source_event_id") != event["event_id"]):
            raise ValueError("Explicit versioned native HYPE derived entry is required")
        start = utc(derived_entry["entry_time_utc"])
        reference = _finite_number(derived_entry["entry_price"],
                                   name="derived PERP TRADE entry" if source_scope == PERP_SCOPE else "derived MARK entry")
        if start != original_start.replace(second=0, microsecond=0) + MINUTE:
            raise ValueError("Derived entry must be the first fully post-alert minute")
        if source_scope == PERP_SCOPE:
            expected_source = perp_path.SOURCE
            if any(path_result.get(key) != value for key, value in expected_source.items()):
                raise ValueError("Exact Hyperliquid HYPE perpetual TRADE path provenance is required")
            route = {key: path_result.get(key) for key in expected_source}
        else:
            if (path_result.get("symbol") != "HYPE" or path_result.get("exchange") != "binance"
                    or path_result.get("market") != "futures" or path_result.get("pair") != "HYPEUSDT"
                    or path_result.get("price_kind") != "MARK" or path_result.get("interval_seconds") != 60
                    or path_result.get("interval") != "1m" or path_result.get("method_version") != mark_path.METHOD_VERSION
                    or path_result.get("provenance") != mark_path.PROVENANCE):
                raise ValueError("Exact Binance HYPE Futures MARK path provenance is required")
            route = {key: path_result.get(key) for key in ("symbol", "exchange", "market", "pair",
                     "price_kind", "interval", "interval_seconds", "method_version", "provenance")}
    else:
        raise ValueError("Unsupported source scope")
    if reference <= 0 or now < original_start or cutoff < start:
        raise ValueError("Invalid immutable entry or observation boundary")
    first = _first_open(start)
    if cutoff - first > timedelta(minutes=MAX_PATH_MINUTES):
        raise ValueError("Full wave exceeds the explicit 31-day path budget")
    rows = []
    for raw in path_result.get("candles") or []:
        try:
            candle = validate_candle(raw)
            if candle["open_time_utc"] >= first and candle["close_time_utc"] <= cutoff:
                rows.append(candle)
        except (TypeError, ValueError, KeyError, AttributeError) as exc:
            errors.append(str(exc))
    rows.sort(key=lambda row: row["open_time_utc"])
    expected = max(0, int((cutoff + MILLISECOND - first) // MINUTE))
    contiguous = (len(rows) == expected and all(row["open_time_utc"] == first + i * MINUTE
                                                for i, row in enumerate(rows)))
    if not contiguous:
        errors.append("INCOMPLETE_CLOSED_1M_PATH")
    if source_scope in {MARK_SCOPE, PERP_SCOPE} and rows and rows[0]["open"] != reference:
        errors.append("DERIVED_ENTRY_DOES_NOT_EQUAL_FIRST_MARK_OPEN" if source_scope == MARK_SCOPE
                      else "DERIVED_ENTRY_DOES_NOT_EQUAL_FIRST_PERP_OPEN")
    prefix_complete = bool(contiguous and not errors and expected > 0)
    if expected == 0:
        errors.append("NO_CLOSED_POST_ENTRY_CANDLES")
    labels = calculate_all_ordered_first_touch_outcomes(reference_price=reference,
        direction=event["direction"], event_time=start, candles=rows,
        observation_closed=closed, path_complete=prefix_complete)
    excursions = _excursion_metrics(reference_price=reference, direction=event["direction"],
        maximum=max([reference] + [row["high"] for row in rows]),
        minimum=min([reference] + [row["low"] for row in rows]))
    status = "DATA_MISSING" if not prefix_complete else "READY" if closed else "OPEN"
    record = {
        "method_version": METHOD_VERSION, "candidate": CANDIDATE,
        "source_scope": source_scope, "coin_scope": event["coin_scope"],
        "btc_parent_movement_id": wave["btc_parent_movement_id"],
        "episode_policy_version": wave["episode_policy_version"],
        "source_event_id": event["event_id"], "symbol": event["symbol"], "direction": event["direction"],
        "score": event["score"], "original_alert_time_utc": original_start,
        "original_alert_reference_price": event["current_price"],
        "measurement_start_utc": start, "reference_price": reference,
        "entry_version": derived_entry.get("entry_policy_version") if derived_entry else "IMMUTABLE_ALERT_REFERENCE",
        "wave_start_utc": wave["start_time_utc"], "wave_end_utc": wave_end,
        "closing_boundary_verified": wave.get("closing_boundary_verified"),
        "btc_parent_observed_through_utc": wave.get("observed_through_utc"),
        "observation_cutoff_utc": cutoff, "observed_at_utc": now,
        "observed_from_utc": rows[0]["open_time_utc"] if rows else None,
        "observed_through_utc": rows[-1]["close_time_utc"] if rows else None,
        "initial_gap_seconds": (first - original_start).total_seconds(),
        "status": status, "observation_closed": closed, "observed_prefix_complete": prefix_complete,
        "path_complete": prefix_complete and closed, "path_samples": len(rows), "expected_candles": expected,
        "source": route, "missing_reasons": sorted(set(errors)), "thresholds": labels,
        "mfe_pct": excursions["mfe_pct"] if status == "READY" else None,
        "mae_pct": excursions["mae_pct"] if status == "READY" else None,
        "provisional_observed_mfe_pct": excursions["mfe_pct"] if prefix_complete else None,
        "provisional_observed_mae_pct": excursions["mae_pct"] if prefix_complete else None,
        "asymmetry_method": ASYMMETRY_METHOD,
    }
    record["path_sha256"] = hashlib.sha256(json.dumps({"source": route, "candles": rows},
        sort_keys=True, default=_json_default, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return record


def aggregate_records(records: Iterable[Mapping]) -> list[dict]:
    """Expose all status counts; no re-selection based on available outcomes.

    Final success probability and full-wave asymmetry use the identical
    closed-wave cohort. Probability is favorable-first / decided, with all
    unresolved cases shown separately. Missing selected closed representatives
    block both final aggregates. Active-wave counts remain provisional.
    """
    groups: dict[tuple[str, str, str], list[Mapping]] = {}
    for row in records:
        if row.get("observed_prefix_complete"):
            thresholds = [label.get("threshold_bps") for label in row.get("thresholds", [])]
            if (len(thresholds) != 8 or set(thresholds) != set(range(25, 201, 25))
                    or row.get("method_version") != METHOD_VERSION):
                raise ValueError("Complete full-wave record requires the exact eight versioned thresholds")
        groups.setdefault((row["source_scope"], row["coin_scope"], row["direction"]), []).append(row)
    result = []
    for (source, scope, direction), rows in sorted(groups.items()):
        ids = [row["btc_parent_movement_id"] for row in rows]
        if len(ids) != len(set(ids)):
            raise ValueError("A parent cannot occur twice in one comparison stratum")
        closed = [row for row in rows if row["observation_closed"]]
        ready = [row for row in closed if row["status"] == "READY" and row["path_complete"]]
        complete = bool(closed and len(closed) == len(ready))
        total_mfe = sum(row["mfe_pct"] for row in ready)
        total_mae = sum(row["mae_pct"] for row in ready)
        for threshold in SUPPORTED_THRESHOLDS_PCT:
            bps = round(threshold * 100)
            def labels(items):
                return [next((label for label in row.get("thresholds", []) if label["threshold_bps"] == bps),
                             {"status": "DATA_MISSING"}) for row in items]
            counts = Counter(label["status"] for label in labels(ready))
            all_counts = Counter(label["status"] for label in labels(rows))
            decided = counts["SUCCESS"] + counts["FAILURE"]
            observed_decided = all_counts["SUCCESS"] + all_counts["FAILURE"]
            observed_complete = all(row.get("observed_prefix_complete") for row in rows)
            observed_mfe = sum(row["provisional_observed_mfe_pct"] for row in rows) if observed_complete else None
            observed_mae = sum(row["provisional_observed_mae_pct"] for row in rows) if observed_complete else None
            result.append({
                "method_version": METHOD_VERSION, "candidate": CANDIDATE, "source_scope": source,
                "coin_scope": scope, "direction": direction, "threshold_pct": threshold, "threshold_bps": bps,
                "representative_policy": REPRESENTATIVE_POLICY, "expected_representatives": len(rows),
                "independent_parent_count": len(ids), "closed_representatives": len(closed),
                "ready_closed_representatives": len(ready), "active_representatives": len(rows) - len(closed),
                "missing_representatives": sum(row["status"] == "DATA_MISSING" for row in rows),
                "closed_cohort_coverage_complete": complete, "closed_cohort_status_counts": dict(counts),
                "all_observed_status_counts": dict(all_counts),
                "success_probability": counts["SUCCESS"] / decided if complete and decided else None,
                "probability_definition": "FAVORABLE_FIRST / (FAVORABLE_FIRST + ADVERSE_FIRST); unresolved shown separately",
                "probability_denominator": decided if complete else None,
                "mfe_pct": total_mfe / len(ready) if complete else None,
                "mae_pct": total_mae / len(ready) if complete else None,
                "asymmetry_ratio": total_mfe / total_mae if complete and total_mae > 0 else None,
                "asymmetry_status": "INCOMPLETE_CLOSED_COHORT" if not complete else "UNDEFINED_ZERO_MAE" if not total_mae else "DEFINED",
                "asymmetry_method": ASYMMETRY_METHOD,
                "observed_cohort_coverage_complete": observed_complete,
                "provisional_observed_probability_denominator": observed_decided if observed_complete else None,
                "provisional_observed_success_probability": all_counts["SUCCESS"] / observed_decided if observed_complete and observed_decided else None,
                "provisional_observed_mfe_pct": observed_mfe / len(rows) if observed_complete else None,
                "provisional_observed_mae_pct": observed_mae / len(rows) if observed_complete else None,
                "provisional_observed_asymmetry_ratio": observed_mfe / observed_mae if observed_complete and observed_mae > 0 else None,
                "source_event_ids": [row["source_event_id"] for row in rows],
                "btc_parent_movement_ids": ids,
            })
    return result


def _unavailable(event: Mapping, wave: Mapping, now: datetime, source: str, reason: str) -> dict:
    return {"method_version": METHOD_VERSION, "candidate": CANDIDATE, "source_scope": source,
        "coin_scope": event["coin_scope"], "direction": event["direction"], "symbol": event["symbol"],
        "btc_parent_movement_id": wave["btc_parent_movement_id"], "source_event_id": event["event_id"],
        "original_alert_time_utc": event["alert_time_utc"], "original_alert_reference_price": event.get("current_price"),
        "wave_end_utc": wave.get("end_time_utc"), "observed_at_utc": now,
        "observation_closed": bool(wave.get("end_time_utc") and utc(wave["end_time_utc"]) <= now),
        "status": "DATA_MISSING", "path_complete": False, "observed_prefix_complete": False,
        "missing_reasons": [reason], "thresholds": [], "mfe_pct": None, "mae_pct": None}


def build_report(*, waves: Iterable[Mapping], events: Iterable[Mapping], observed_at: Any,
                 include_hype_mark: bool = False, include_hype_perp: bool = False,
                 fetcher: Callable | None = None,
                 _verified_closed_cache: Mapping | None = None) -> dict:
    if include_hype_mark and include_hype_perp:
        raise ValueError("MARK and PERP require separate explicit full-wave reports")
    derivative_scope = PERP_SCOPE if include_hype_perp else MARK_SCOPE
    mixed_scope = PERP_MIXED_SCOPE if include_hype_perp else MIXED_SCOPE
    include_derivative = include_hype_mark or include_hype_perp
    now, parents = utc(observed_at), [dict(wave) for wave in waves]
    if not 0 < len(parents) <= MAX_WAVES:
        raise ValueError("Specify between 1 and 32 exact parent IDs")
    by_id = {wave["btc_parent_movement_id"]: wave for wave in parents}
    if len(by_id) != len(parents):
        raise ValueError("Duplicate parent IDs")
    for wave in parents:
        if wave.get("end_time_utc") and "closing_boundary_verified" not in wave:
            # A data gap can close an originally eligible parent. Only an
            # eligible causal successor at exactly that endpoint proves a
            # reversal; the old parent's starting reason is insufficient.
            wave["closing_boundary_verified"] = any(
                candidate.get("episode_policy_version") == POLICY_VERSION
                and candidate.get("evidence_eligible") is True
                and candidate.get("boundary_reason") == "CAUSAL_CLOSE_REVERSAL"
                and utc(candidate["start_time_utc"]) == utc(wave["end_time_utc"])
                for candidate in parents)
    representatives = select_representatives(event for event in events if utc(event["alert_time_utc"]) <= now)
    records, cache = [], dict(_verified_closed_cache or {})
    for event in representatives:
        wave = by_id[event["btc_parent_movement_id"]]
        cutoff = wave_observation_cutoff(wave, now)
        for source in ([SPOT_SCOPE, derivative_scope] if include_derivative and event["symbol"] == "HYPE" else [SPOT_SCOPE]):
            key = (source, event["event_id"])
            if key not in cache:
                try:
                    error = _membership_error(event, wave)
                    if error:
                        raise ValueError(error)
                    if source == SPOT_SCOPE:
                        error = _spot_reference_error(event)
                        if error:
                            raise ValueError(error)
                    path = fetch_full_path(event["symbol"], event["alert_time_utc"], cutoff,
                                           source_scope=source, fetcher=fetcher)
                    entry = None
                    if source in {MARK_SCOPE, PERP_SCOPE}:
                        if source == PERP_SCOPE:
                            from research_native_hype_perp_supplement import derive_entry
                        else:
                            from research_native_hype_mark_supplement import derive_entry
                        membership = {"event_id": event["event_id"],
                            "episode_policy_version": event["membership_policy_version"],
                            "btc_parent_movement_id": event["membership_parent_id"],
                            "decision_time_utc": event["decision_time_utc"],
                            "btc_observed_close_utc": event["btc_observed_close_utc"],
                            "membership_status": event["membership_status"]}
                        entry = derive_entry(event, membership, path)
                    cache[key] = calculate_wave_record(event=event, wave=wave, path_result=path,
                        observed_at=now, source_scope=source, derived_entry=entry)
                except Exception as exc:
                    cache[key] = _unavailable(event, wave, now, source, f"{type(exc).__name__}: {exc}")
            records.append({**cache[key], "coin_scope": event["coin_scope"]})
    if include_derivative:
        # Explicit alternative measurement contract requested by the owner.
        # Selection is unchanged: replace only the measurement for that same
        # HYPE representative, never choose a different event after outcomes.
        alternatives = {(row["source_event_id"], row["coin_scope"]): row
                        for row in records if row["source_scope"] == derivative_scope}
        mixed = []
        for row in records:
            if row["source_scope"] != SPOT_SCOPE:
                continue
            component = alternatives[(row["source_event_id"], row["coin_scope"])] if row["symbol"] == "HYPE" else row
            mixed.append({**component, "source_scope": mixed_scope,
                          "component_source_scope": component["source_scope"]})
        records.extend(mixed)
    represented = {event["btc_parent_movement_id"] for event in representatives}
    return {"method_version": METHOD_VERSION, "candidate": CANDIDATE, "observed_at_utc": now,
        "latest_closed_cutoff_utc": latest_closed_cutoff(now),
        "representative_policy": REPRESENTATIVE_POLICY,
        "source_scope_policy": ("CANONICAL_SPOT_AND_PERP_SEPARATE; EXPLICIT_ALTERNATIVE_MIXED_CONTRACT_VERSIONED"
                                if include_hype_perp else "CANONICAL_SPOT_AND_MARK_SEPARATE; EXPLICIT_ALTERNATIVE_MIXED_CONTRACT_VERSIONED"),
        "mixed_source_contract": {"source_scope": mixed_scope,
            "non_hype_entry": "IMMUTABLE_ALERT_REFERENCE_AND_CANONICAL_SPOT_PATH",
            "hype_entry": ("NEXT_FULL_MINUTE_HYPERLIQUID_PERPETUAL_TRADE_OPEN_AND_TRADE_PATH" if include_hype_perp
                           else "NEXT_FULL_MINUTE_BINANCE_FUTURES_MARK_OPEN_AND_MARK_PATH"),
            "production_formula_eligible": False, "original_events_modified": False} if include_derivative else None,
        "boundary_policy": "FIRST_FULL_POST_ENTRY_MINUTE_THROUGH_INCLUSIVE_BTC_REVERSAL_CLOSE_OR_LATEST_CLOSED_MINUTE",
        "fixed_horizon_outcomes_modified": False, "waves": parents,
        "waves_without_qualifying_native_alert": [wave["btc_parent_movement_id"] for wave in parents
                                                  if wave["btc_parent_movement_id"] not in represented],
        "records": records, "summary_rows": aggregate_records(records),
        "limitations": ["Native DELIVERED alerts only; archive WATCH_CANDIDATE is a separate cohort.",
            "Wave boundaries are an operational independence policy, not statistical proof.",
            "LONG and SHORT strata and ALL versus individual coins must not be summed.",
            "Active waves expose provisional observed results and remain OPEN.",
            "This explicitly reconstructed contract does not claim to reproduce an unavailable earlier chat calculation."]}


def verified_closed_cache(*, previous_report: Mapping, previous_source: Mapping,
                   waves: Iterable[Mapping], events: Iterable[Mapping], observed_at: Any,
                   include_hype_mark: bool = False, include_hype_perp: bool = False,
                   fetcher: Callable | None = None) -> dict:
    """Refresh open/missing paths; reuse verified unchanged complete closed paths.

    The caller must reread parent metadata and the candidate source cohort.
    Old source rows are compared with the new source before reuse. Selection
    still happens anew before outcomes, so new earlier representatives cannot
    be hidden by the cache. Mixed-source rows are rebuilt from their components.
    """
    if previous_report.get("method_version") != METHOD_VERSION:
        raise ValueError("Cannot reuse a different full-wave report contract")
    parents, incoming = list(waves), list(events)
    old_events = {int(e["event_id"]): e for e in previous_source["events"]}
    new_events = {int(e["event_id"]): e for e in incoming}
    old_waves = {w["btc_parent_movement_id"]: w for w in previous_source["waves"]}
    new_waves = {w["btc_parent_movement_id"]: w for w in parents}
    fields = ("event_id", "event_kind", "event_type", "symbol", "direction", "score", "current_price",
        "delivery_status", "engine_snapshot", "event_fingerprint", "strategy_version", "code_version",
        "membership_status", "membership_policy_version", "membership_parent_id")
    def signature(event):
        value = {field: event.get(field) for field in fields}
        value.update({field: utc(event[field]).isoformat() if event.get(field) else None
            for field in ("alert_time_utc", "decision_time_utc", "btc_observed_close_utc")})
        return json.dumps(value, default=_json_default, sort_keys=True, allow_nan=False)
    cache = {}
    for row in previous_report["records"]:
        event_id, wave_id = int(row["source_event_id"]), row["btc_parent_movement_id"]
        if (row["source_scope"] not in {SPOT_SCOPE, PERP_SCOPE if include_hype_perp else MARK_SCOPE} or row["status"] != "READY"
                or not row.get("path_complete") or not row.get("observation_closed")
                or event_id not in old_events or event_id not in new_events
                or wave_id not in old_waves or wave_id not in new_waves):
            continue
        before, after = old_waves[wave_id], new_waves[wave_id]
        if (signature(old_events[event_id]) != signature(new_events[event_id])
                or any(before.get(key) != after.get(key) for key in (
                    "episode_policy_version", "evidence_eligible", "boundary_reason"))
                or any((utc(before[key]) if before.get(key) else None) != (utc(after[key]) if after.get(key) else None)
                       for key in ("start_time_utc", "end_time_utc"))
                or not after.get("end_time_utc")
                or utc(after["end_time_utc"]) > utc(observed_at)):
            continue
        verified = any(p.get("episode_policy_version") == POLICY_VERSION and p.get("evidence_eligible") is True
            and p.get("boundary_reason") == "CAUSAL_CLOSE_REVERSAL"
            and utc(p["start_time_utc"]) == utc(after["end_time_utc"]) for p in parents)
        if verified:
            cache[(row["source_scope"], event_id)] = {**row, "closing_boundary_verified": True,
                "source_reverified_at_utc": utc(observed_at)}
    return cache


def refresh_report(*, previous_report: Mapping, previous_source: Mapping,
                   waves: Iterable[Mapping], events: Iterable[Mapping], observed_at: Any,
                   include_hype_mark: bool = False, include_hype_perp: bool = False,
                   fetcher: Callable | None = None) -> dict:
    parents, incoming = list(waves), list(events)
    cache = verified_closed_cache(previous_report=previous_report, previous_source=previous_source,
        waves=parents, events=incoming, observed_at=observed_at,
        include_hype_mark=include_hype_mark, include_hype_perp=include_hype_perp)
    result = build_report(waves=parents, events=incoming, observed_at=observed_at,
        include_hype_mark=include_hype_mark, include_hype_perp=include_hype_perp,
        fetcher=fetcher, _verified_closed_cache=cache)
    result["reused_complete_closed_source_paths"] = len(cache)
    return result


def load_source(conn: Any, wave_ids: list[str]) -> tuple[list[dict], list[dict]]:
    """Read a bounded cohort in the caller's read-only repeatable-read snapshot."""
    if not 0 < len(set(wave_ids)) <= MAX_WAVES or len(set(wave_ids)) != len(wave_ids):
        raise ValueError("Specify 1..32 unique wave IDs")
    waves = list(conn.execute("""SELECT p.btc_parent_movement_id, p.episode_policy_version,
        p.start_time_utc, p.end_time_utc, p.evidence_eligible, p.boundary_reason, p.observed_through_utc,
        EXISTS (SELECT 1 FROM research_btc_parent_movements successor
            WHERE successor.episode_policy_version=p.episode_policy_version
              AND successor.start_time_utc=p.end_time_utc
              AND successor.evidence_eligible
              AND successor.boundary_reason='CAUSAL_CLOSE_REVERSAL') AS closing_boundary_verified
        FROM research_btc_parent_movements p WHERE p.btc_parent_movement_id=ANY(%s)
        ORDER BY p.start_time_utc""", (wave_ids,)).fetchall())
    if len(waves) != len(wave_ids):
        raise ValueError("An explicitly requested parent ID does not exist")
    events = []
    for wave in waves:
        rows = list(conn.execute("""SELECT e.event_id, e.event_kind, e.event_type,
            e.alert_time_utc, e.symbol, e.direction, e.score, e.current_price,
            e.delivery_status, e.engine_snapshot, e.source_side, e.timeframe,
            e.target_price, e.initial_target_distance_pct, e.event_fingerprint,
            e.strategy_version, e.code_version, m.membership_status,
            m.episode_policy_version AS membership_policy_version,
            m.btc_parent_movement_id AS membership_parent_id,
            m.decision_time_utc, m.btc_observed_close_utc
            FROM research_events e LEFT JOIN research_event_btc_movements m
              ON m.event_id=e.event_id AND m.episode_policy_version=%s
            WHERE e.event_kind='ALERT' AND e.event_type='MAX_PAIN_ALERT'
              AND e.delivery_status='DELIVERED' AND e.score>=65
              AND e.direction IN ('LONG','SHORT')
              AND e.alert_time_utc >= %s AND e.alert_time_utc < COALESCE(%s, NOW())
            ORDER BY e.alert_time_utc, e.event_id LIMIT %s""",
            (POLICY_VERSION, wave["start_time_utc"], wave["end_time_utc"], MAX_EVENTS_PER_WAVE + 1)).fetchall())
        if len(rows) > MAX_EVENTS_PER_WAVE:
            raise ValueError("Candidate source exceeds bounded per-wave budget; no partial report published")
        events.extend({**dict(row), "btc_parent_movement_id": wave["btc_parent_movement_id"]} for row in rows)
    return [dict(wave) for wave in waves], events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wave-id", action="append", default=[])
    parser.add_argument("--input", "--input-json", help="Optional JSON containing waves/events instead of readonly PostgreSQL")
    parser.add_argument("--observed-at", required=True, help="Explicit timezone-aware report cutoff")
    parser.add_argument("--include-hype-mark", action="store_true")
    parser.add_argument("--include-hype-perp", action="store_true")
    parser.add_argument("--previous-report", help="Reuse verified unchanged closed paths from this JSON report")
    parser.add_argument("--previous-source", help="Original waves/events JSON for strict source comparison")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.input:
        source = json.loads(Path(args.input).read_text())
        waves, events = source["waves"], source["events"]
        if args.wave_id:
            wanted = set(args.wave_id)
            waves = [wave for wave in waves if wave["btc_parent_movement_id"] in wanted]
            events = [event for event in events if event["btc_parent_movement_id"] in wanted]
            if {wave["btc_parent_movement_id"] for wave in waves} != wanted:
                raise ValueError("Requested wave ID missing from input")
    else:
        import psycopg
        from psycopg.rows import dict_row
        dsn = os.getenv("RESEARCH_DATABASE_URL") or os.getenv("DATABASE_URL")
        if not dsn:
            raise ValueError("RESEARCH_DATABASE_URL or DATABASE_URL is required")
        with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=5,
                options="-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=1000") as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            waves, events = load_source(conn, args.wave_id)
    if bool(args.previous_report) != bool(args.previous_source):
        raise ValueError("Refresh requires both --previous-report and --previous-source")
    kwargs = dict(waves=waves, events=events, observed_at=args.observed_at,
                  include_hype_mark=args.include_hype_mark, include_hype_perp=args.include_hype_perp)
    result = refresh_report(previous_report=json.loads(Path(args.previous_report).read_text()),
        previous_source=json.loads(Path(args.previous_source).read_text()), **kwargs) if args.previous_report else build_report(**kwargs)
    output = Path(args.output)
    output.write_text(json.dumps(result, default=_json_default, ensure_ascii=False,
                                  indent=2, allow_nan=False) + "\n")
    print(json.dumps({"method_version": METHOD_VERSION, "waves": len(waves),
        "records": len(result["records"]), "summary_rows": len(result["summary_rows"]),
        "status_counts": dict(Counter(row["status"] for row in result["records"]))}))


if __name__ == "__main__":
    main()
