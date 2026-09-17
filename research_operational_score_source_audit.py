"""Bounded, read-only Stage-8 source intersection audit (not formula evidence).

No connection creation, transaction management, source fetching, persistence,
runtime wiring, score recomputation or delivery inference lives here. Missing
rows remain UNKNOWN. Only the caller owns its read-only database transaction.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timedelta, timezone
import math
import re
import time
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

import alert_engine
import canonical_price_path
import research_btc_parent_movement as btc
import research_event_capture as events_contract
import research_feature_matrix as feature_matrix
import research_formula_ordered_v7 as ordered
import research_prospective_anchor_store as anchor_store
import research_prospective_anchors as anchors
import research_prospective_feature_freeze as features
import research_watch_score_capture as capture


VERSION = "operational-score-source-audit-v2"
TRANSACTION_IDENTITY_VERSION = "stage8-postgres-transaction-identity-v1"
MAX_PAGE_SIZE = 100
_INT64_MAX = 9223372036854775807
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_MODELS = ("positioning", "futures_flow", "spot_flow")
_CODE_FILES = {
    "alert_engine.py", "market_confidence_engine.py", "time_family_engine.py",
    "coinglass_flow_engine.py", "coinglass_oi_regime_service.py", "live_price_provider.py",
}
_SOURCE_SIDE_SEMANTICS = "liquidated-side; SHORT target implies price UP, LONG target price DOWN"


class AuditDeadlineExceeded(TimeoutError):
    """The caller-owned monotonic audit deadline expired."""


def _utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def _mapping(value: Any) -> Mapping:
    return value if isinstance(value, Mapping) else {}


def _positive_id(value: Any) -> bool:
    return type(value) is int and 0 < value <= _INT64_MAX


def transaction_identity_from_fields(*, backend_pid: int,
                                     transaction_started_at_utc: Any,
                                     database_snapshot_id: str) -> dict:
    """One versioned identity shared by coverage and projection readers.

    Only server-observed backend, transaction start and snapshot identify the
    transaction. Read-only/isolation/timeout remain separately checked session
    attributes, never alternative hash definitions. This pure helper performs
    no SQL, connection discovery or transaction management.
    """
    if (not _positive_id(backend_pid)
            or type(transaction_started_at_utc) not in (str, datetime)
            or type(database_snapshot_id) is not str
            or re.fullmatch(r"(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*):"
                            r"(?:(?:0|[1-9][0-9]*)(?:,(?:0|[1-9][0-9]*))*)?",
                            database_snapshot_id) is None):
        raise ValueError("database transaction identity fields are invalid")
    xmin_text, xmax_text, xip_text = database_snapshot_id.split(":")
    xmin, xmax = int(xmin_text), int(xmax_text)
    xips = [int(item) for item in xip_text.split(",")] if xip_text else []
    if (xmin > xmax or any(item > 18446744073709551615 for item in (xmin, xmax, *xips))
            or xips != sorted(set(xips))
            or any(not xmin <= item < xmax for item in xips)):
        raise ValueError("database transaction snapshot is invalid")
    payload = {
        "version": TRANSACTION_IDENTITY_VERSION,
        "backend_pid": backend_pid,
        "transaction_started_at_utc": _utc(transaction_started_at_utc).isoformat(
            timespec="microseconds").replace("+00:00", "Z"),
        "database_snapshot_id": database_snapshot_id,
    }
    # The payload contains only strings and an integer, so this existing strict
    # compact sorted codec is identical to Stage-8's PostgreSQL-compatible one.
    return {**payload, "transaction_identity_sha256": capture.digest(payload)}


def _same_time(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    return _utc(left) == _utc(right)


def _result(reasons: Sequence[str], **details: Any) -> dict:
    return {"status": "UNKNOWN" if reasons else "VALID",
            "reasons": sorted(set(reasons)), **deepcopy(details)}


def validate_anchor_authority(attempt: Mapping, slot: Mapping | None,
                              events: Sequence[Mapping]) -> dict:
    """Validate this attempt's exact v4 slot and its two immutable event IDs.

    Stored NOT_APPLICABLE is checked as event authority, not proof of absence
    of a separate delivery. Non-evaluable attempts never acquire a decision.
    """
    reasons: list[str] = []
    status = attempt.get("evaluation_status")
    if attempt.get("sampler_version") != anchors.SAMPLER_VERSION:
        reasons.append("ANCHOR_SAMPLER_VERSION_MISMATCH")
    if attempt.get("coverage_policy_version") != anchors.COVERAGE_POLICY_VERSION:
        reasons.append("ANCHOR_COVERAGE_POLICY_VERSION_MISMATCH")
    if status not in (anchors.EVALUABLE, anchors.UNEVALUABLE, anchors.COVERAGE_EXCLUDED):
        reasons.append("ANCHOR_EVALUATION_STATUS_UNKNOWN")
    if not _positive_id(attempt.get("attempt_id")):
        reasons.append("ANCHOR_ATTEMPT_ID_INVALID")
    if not str(attempt.get("evaluation_reason") or "").strip():
        reasons.append("ANCHOR_EVALUATION_REASON_MISSING")
    if status != anchors.EVALUABLE:
        if attempt.get("decision_time_utc") is not None:
            reasons.append("NON_EVALUABLE_ATTEMPT_HAS_DECISION_TIME")
        result = _result(reasons, source_status=status,
                         source_reason=attempt.get("evaluation_reason"),
                         model_score_status=None)
        if not reasons:
            result["status"] = "NOT_APPLICABLE"
        return result
    if not slot:
        return _result(reasons + ["ANCHOR_SLOT_MISSING"], source_status=status)
    try:
        if not _positive_id(slot.get("anchor_slot_id")):
            reasons.append("ANCHOR_SLOT_ID_INVALID")
        for key in ("sampler_version", "coverage_policy_version", "symbol", "interval_minutes",
                    "feature_bundle_policy_version", "feature_bundle_sha256", "input_fingerprint"):
            if slot.get(key) != attempt.get(key):
                reasons.append("ANCHOR_ATTEMPT_SLOT_MISMATCH:" + key)
        for key in ("source_candle_open_utc", "source_candle_close_utc", "base_eligible_at_utc",
                    "expires_at_utc", "decision_time_utc"):
            if not _same_time(attempt.get(key), slot.get(key)):
                reasons.append("ANCHOR_ATTEMPT_SLOT_MISMATCH:" + key)
        opened = _utc(attempt["source_candle_open_utc"])
        decision = _utc(attempt["decision_time_utc"])
        coverage_valid, coverage_reasons = anchors._coverage_status(
            slot.get("coverage_snapshot"), expected_symbol=slot.get("symbol"),
            checked_at_utc=decision, coverage_policy_version=slot.get("coverage_policy_version"))
        if not coverage_valid:
            reasons.extend("ANCHOR_COVERAGE_INVALID:" + reason for reason in coverage_reasons)
        frozen_sources = feature_matrix.prospective_frozen_source_rows(
            symbol=slot.get("symbol"), frozen_inputs=_mapping(slot.get("frozen_inputs")),
            source_timestamps=_mapping(slot.get("source_timestamps")),
            source_provenance=_mapping(slot.get("source_provenance")))
        for family in anchors.REQUIRED_FAMILIES:
            problem, _, _ = anchors._family_problem(
                family, frozen_sources.get(family), symbol=slot.get("symbol"),
                slot_open=opened, slot_close=_utc(slot["source_candle_close_utc"]),
                base_eligible_at=_utc(slot["base_eligible_at_utc"]), now=decision)
            if problem:
                reasons.append("ANCHOR_SOURCE_INVALID:" + problem)
        if (attempt.get("interval_minutes") != 30 or opened.second or opened.microsecond
                or opened.minute % 30 or _utc(attempt["source_candle_close_utc"]) != opened + timedelta(minutes=30)
                or _utc(attempt["base_eligible_at_utc"]) != opened + timedelta(minutes=32)
                or _utc(attempt["expires_at_utc"]) != opened + timedelta(minutes=62)
                or not opened + timedelta(minutes=32) <= decision < opened + timedelta(minutes=62)
                or decision != _utc(attempt["checked_at_utc"])
                or attempt.get("missing_sources") != []):
            reasons.append("ANCHOR_DECISION_INTERVAL_INVALID")
        bundle = _mapping(slot.get("decision_feature_bundle"))
        if bundle.get("model_score_status") != "ABSENT":
            reasons.append("ANCHOR_MODEL_SCORE_STATUS_NOT_ABSENT")
        # The persistence validator recomputes both canonical input hashes and
        # invokes the strict canonical feature-bundle/hash validator.
        anchor_store.ProspectiveAnchorStore._validate_v4_persistence_bundle({
            "attempt": attempt, "slot": slot,
            "event_persistence": [
                {"event": SimpleNamespace(engine_snapshot=event.get("engine_snapshot"))}
                for event in events],
        })
        valid, reason = features.validate_feature_bundle(
            bundle, expected_sha256=slot.get("feature_bundle_sha256"),
            expected_symbol=slot.get("symbol"), expected_decision_time_utc=decision)
        if not valid:
            reasons.append("ANCHOR_FEATURE_BUNDLE_INVALID:" + reason)
        expected_ids = [slot.get("long_event_id"), slot.get("short_event_id")]
        if (not all(_positive_id(value) for value in expected_ids)
                or len(set(expected_ids)) != 2 or len(events) != 2
                or {event.get("event_id") for event in events} != set(expected_ids)):
            reasons.append("ANCHOR_EXACT_EVENT_PAIR_MISMATCH")
        by_id = {event.get("event_id"): event for event in events}
        for direction, eid in zip(anchors.DIRECTIONS, expected_ids):
            event = by_id.get(eid)
            if not event:
                reasons.append("ANCHOR_EVENT_MISSING:" + direction)
                continue
            events_contract.validate_event(events_contract.ResearchEvent(**{
                field.name: event.get(field.name) for field in fields(events_contract.ResearchEvent)}))
            for key, expected in (("direction", direction), ("symbol", attempt.get("symbol")),
                                  ("event_kind", "DECISION_SAMPLE"), ("event_type", anchors.EVENT_TYPE),
                                  ("source_side", "RAW_NEUTRAL"), ("timeframe", anchors.TIMEFRAME),
                                  ("capture_stage", "SILENT_NEUTRAL_ANCHOR"),
                                  ("strategy_version", "formula-prospective-neutral-v4"),
                                  ("delivery_status", "NOT_APPLICABLE")):
                if event.get(key) != expected:
                    reasons.append("ANCHOR_EVENT_AUTHORITY_MISMATCH:" + direction + ":" + key)
            if not _same_time(event.get("alert_time_utc"), decision):
                reasons.append("ANCHOR_EVENT_TIME_MISMATCH:" + direction)
            expected_fingerprint = anchors._sha256({
                "sampler_version": anchors.SAMPLER_VERSION, "event_type": anchors.EVENT_TYPE,
                "symbol": attempt.get("symbol"), "direction": direction,
                "source_candle_open_utc": anchors._iso(opened)})
            if event.get("event_fingerprint") != expected_fingerprint:
                reasons.append("ANCHOR_EVENT_FINGERPRINT_MISMATCH:" + direction)
            ref = _mapping(_mapping(event.get("engine_snapshot")).get("prospective_anchor"))
            if ref.get("anchor_key") != anchors._sha256({
                    "sampler_version": anchors.SAMPLER_VERSION,
                    "symbol": attempt.get("symbol"), "source_candle_open_utc": anchors._iso(opened)}):
                reasons.append("ANCHOR_EVENT_ANCHOR_KEY_MISMATCH:" + direction)
            for key in ("sampler_version", "coverage_policy_version", "coverage_snapshot",
                        "input_fingerprint", "source_timestamps", "source_provenance", "frozen_inputs",
                        "feature_bundle_policy_version", "feature_bundle_sha256"):
                if capture.canonical(ref.get(key)) != capture.canonical(slot.get(key)):
                    reasons.append("ANCHOR_EVENT_REFERENCE_MISMATCH:" + direction + ":" + key)
            for key in ("source_candle_open_utc", "source_candle_close_utc", "base_eligible_at_utc",
                        "expires_at_utc", "decision_time_utc"):
                if not _same_time(ref.get(key), slot.get(key)):
                    reasons.append("ANCHOR_EVENT_REFERENCE_MISMATCH:" + direction + ":" + key)
            if (ref.get("sampling_frame") != "NEUTRAL_30M_BOTH_DIRECTIONS"
                    or ref.get("telegram_delivery_allowed") is not False
                    or ref.get("trade_execution_allowed") is not False
                    or ref.get("delivery_status") != "NOT_APPLICABLE"
                    or ref.get("coverage_eligible") is not True
                    or any(event.get(key) is not None for key in ("score", "target_price", "initial_target_distance_pct"))):
                reasons.append("ANCHOR_SILENT_NEUTRAL_CONTRACT_MISMATCH:" + direction)
            raw_price = _mapping(_mapping(slot.get("frozen_inputs")).get("official_price"))
            raw_price = _mapping(raw_price.get("values")) or raw_price
            if not math.isclose(float(event["current_price"]), float(raw_price["price"]), rel_tol=1e-12):
                reasons.append("ANCHOR_REFERENCE_PRICE_MISMATCH:" + direction)
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
        reasons.append("ANCHOR_AUTHORITY_INVALID:" + str(exc))
    return _result(reasons, source_status=status,
                   model_score_status=_mapping(slot.get("decision_feature_bundle")).get("model_score_status"))


def _capture_block(snapshot: Mapping) -> Mapping:
    return _mapping(_mapping(_mapping(snapshot.get("source_metadata")).get(
        "capture_metadata")).get("operational_scores"))


def _max_age(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("max_capture_age_seconds must be a positive finite number")
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("max_capture_age_seconds must be a positive finite number")
    return value


def select_prior_capture(snapshots: Sequence[Mapping], *, decision_time_utc: Any,
                         max_capture_age_seconds: float) -> Mapping | None:
    """Latest durable-time/ID candidate; never search backward for valid scores.

    This operates on the caller's supplied candidate set. The DB reader below
    performs the same selection over the archive itself.
    """
    decision, max_age = _utc(decision_time_utc), _max_age(max_capture_age_seconds)
    candidates = []
    for snapshot in snapshots:
        if snapshot.get("source") != "WATCH_SHARED":
            continue
        try:
            durable = max(_utc(snapshot["available_at_utc"]), _utc(snapshot["created_at_utc"]))
            if _positive_id(snapshot.get("snapshot_set_id")) and 0 <= (decision - durable).total_seconds() <= max_age:
                candidates.append((durable, snapshot["snapshot_set_id"], snapshot))
        except (TypeError, ValueError, KeyError, OverflowError):
            continue
    return max(candidates, key=lambda item: item[:2])[2] if candidates else None


def validate_capture(snapshot: Mapping | None, *, symbol: str, decision_time_utc: Any,
                     max_capture_age_seconds: float) -> dict:
    """Validate the selected capture in place, preserving unavailable/zero data.

    The outer archive hash identifies a row only: reconstructing and verifying
    its entire raw archive is deliberately outside this bounded audit.
    """
    if not snapshot:
        return _result(["NO_DURABLY_PRIOR_WATCH_CAPTURE_WITHIN_MAX_AGE"], snapshot_reference=None, coin=None)
    reasons: list[str] = []
    block = _capture_block(snapshot)
    coin = _mapping(_mapping(block.get("coins")).get(symbol))
    reference = {key: snapshot.get(key) for key in (
        "snapshot_set_id", "snapshot_key", "payload_sha256", "source", "cycle_id",
        "available_at_utc", "created_at_utc")}
    reference["outer_hash_validation"] = "REFERENCE_ONLY_NOT_RECOMPUTED"
    models = _mapping(coin.get("models"))
    model_observations = {name: deepcopy(models.get(name)) for name in _MODELS}
    try:
        decision, max_age = _utc(decision_time_utc), _max_age(max_capture_age_seconds)
        if snapshot.get("source") != "WATCH_SHARED":
            reasons.append("CAPTURE_SOURCE_NOT_WATCH_SHARED")
        if not _positive_id(snapshot.get("snapshot_set_id")):
            reasons.append("CAPTURE_SNAPSHOT_ID_INVALID")
        for key in ("snapshot_key", "payload_sha256"):
            if not _HASH.fullmatch(str(snapshot.get(key) or "")):
                reasons.append("CAPTURE_OUTER_REFERENCE_INVALID:" + key)
        for key, expected in (("version", capture.VERSION), ("population", capture.POPULATION),
                              ("hash_version", capture.HASH_VERSION)):
            if block.get(key) != expected:
                reasons.append("CAPTURE_CONTRACT_MISMATCH:" + key)
        if (not isinstance(block.get("cycle_id"), str) or not block["cycle_id"].strip()
                or block.get("cycle_id") != snapshot.get("cycle_id")):
            reasons.append("CAPTURE_CYCLE_ID_MISMATCH")
        code_hashes = _mapping(block.get("code_sha256"))
        if set(code_hashes) != _CODE_FILES or any(
                not isinstance(value, str) or not _HASH.fullmatch(value) for value in code_hashes.values()):
            reasons.append("CAPTURE_CODE_HASH_IDENTITIES_INVALID")
        if not _HASH.fullmatch(str(block.get("input_universe_sha256") or "")):
            reasons.append("CAPTURE_INPUT_UNIVERSE_HASH_INVALID")
        input_count = block.get("input_row_count")
        # A COMPLETE capture contains seven distinct rows for each Top8 coin;
        # the full pre-display input universe may additionally contain others.
        if type(input_count) is not int or input_count < len(capture.SYMBOLS) * len(alert_engine.TIMEFRAMES):
            reasons.append("CAPTURE_INPUT_ROW_COUNT_INVALID")
        if block.get("source_side_semantics") != _SOURCE_SIDE_SEMANTICS:
            reasons.append("CAPTURE_SOURCE_SIDE_SEMANTICS_MISMATCH")
        unsigned = {key: value for key, value in block.items() if key != "payload_sha256"}
        if (not _HASH.fullmatch(str(block.get("payload_sha256") or ""))
                or capture.digest(unsigned) != block.get("payload_sha256")):
            reasons.append("CAPTURE_INNER_HASH_MISMATCH")
        if len(capture.canonical(unsigned).encode()) > capture.MAX_BYTES:
            reasons.append("CAPTURE_SIZE_LIMIT_EXCEEDED")
        available, created = _utc(snapshot["available_at_utc"]), _utc(snapshot["created_at_utc"])
        durable = max(available, created)
        reference["durably_available_at_utc"] = durable
        reference["age_seconds"] = (decision - durable).total_seconds()
        computed = _utc(block["computed_at_utc"])
        if not 0 <= (decision - durable).total_seconds() <= max_age:
            reasons.append("CAPTURE_NOT_DURABLY_PRIOR_WITHIN_MAX_AGE")
        if computed > decision or computed > available or computed > created:
            reasons.append("CAPTURE_COMPUTED_TIME_ORDER_INVALID")
        if block.get("symbols_expected") != list(capture.SYMBOLS):
            reasons.append("CAPTURE_SYMBOL_UNIVERSE_MISMATCH")
        if block.get("status") != "COMPLETE":
            reasons.append("CAPTURE_NOT_COMPLETE")
        if set(_mapping(block.get("coins"))) != set(capture.SYMBOLS):
            reasons.append("CAPTURE_COIN_UNIVERSE_MISMATCH")
        if block.get("maxpain_additive_components") != list(capture.ADDITIVE_COMPONENTS):
            reasons.append("CAPTURE_ADDITIVE_COMPONENT_CONTRACT_MISMATCH")
        if coin.get("status") not in ("CAPTURED", "PARTIAL", "ABSENT"):
            reasons.append("CAPTURE_COIN_MISSING_OR_INVALID")
        if coin.get("status") != "CAPTURED":
            reasons.append("CAPTURE_COIN_PARTIAL_OR_ABSENT")
        errors = coin.get("source_time_errors")
        if not isinstance(errors, list):
            reasons.append("CAPTURE_SOURCE_TIME_AUDIT_MISSING")
        elif errors:
            reasons.extend("CAPTURE_SOURCE_TIME_ERROR:" + str(error) for error in errors)
        slots = coin.get("maxpain")
        expected_slots = {(tf, side) for tf in alert_engine.TIMEFRAMES for side in anchors.DIRECTIONS}
        if (not isinstance(slots, list) or len(slots) != len(expected_slots)
                or {(_mapping(item).get("timeframe"), _mapping(item).get("source_side")) for item in slots} != expected_slots):
            reasons.append("CAPTURE_MAXPAIN_SLOT_GRID_MISMATCH")
        else:
            for item in slots:
                state = item.get("status")
                score = item.get("score")
                if state in ("MISSING_INPUT", "INACTIVE_TARGET"):
                    if score is not None:
                        reasons.append("CAPTURE_UNAVAILABLE_TARGET_HAS_SCORE")
                elif state != "SCORED" or isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
                    reasons.append("CAPTURE_MAXPAIN_SCORE_STATE_INVALID")
                else:
                    components = _mapping(item.get("components"))
                    values = [components.get(name) for name in capture.ADDITIVE_COMPONENTS]
                    if any(isinstance(value, bool) or not isinstance(value, (int, float))
                           or not math.isfinite(value) for value in values):
                        reasons.append("CAPTURE_MAXPAIN_ADDITIVE_COMPONENT_INVALID")
                    elif abs(round(sum(values), 2) - score) > 0.011:
                        reasons.append("CAPTURE_MAXPAIN_ADDITIVE_SUM_MISMATCH")
        for name in _MODELS:
            model = _mapping(models.get(name))
            if type(model.get("available")) is not bool:
                reasons.append("CAPTURE_MODEL_AVAILABILITY_MISSING:" + name)
            elif model.get("capture_status") != ("AVAILABLE" if model["available"] else "UNAVAILABLE"):
                reasons.append("CAPTURE_MODEL_AVAILABILITY_MISMATCH:" + name)
            score = model.get("score")
            if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
                reasons.append("CAPTURE_MODEL_SCORE_INVALID:" + name)
        # Independently check the timestamps frozen by the producer. Missing
        # or future observations cannot become prior-only evidence even if a
        # rehashed payload has removed its source_time_errors list.
        sources = _mapping(coin.get("sources"))
        if not _HASH.fullmatch(str(sources.get("derivatives_snapshot_sha256") or "")):
            reasons.append("CAPTURE_DERIVATIVES_REFERENCE_HASH_INVALID")
        source_rows = sources.get("maxpain_operational_rows")
        if (not isinstance(source_rows, list) or len(source_rows) != len(alert_engine.TIMEFRAMES)
                or {_mapping(row).get("timeframe") for row in source_rows} != set(alert_engine.TIMEFRAMES)):
            reasons.append("CAPTURE_SOURCE_ROWS_INCOMPLETE")
            source_rows = []
        required_times = []
        for row in source_rows:
            source, pair = row.get("price_source"), row.get("price_pair")
            market, instrument = row.get("price_market"), row.get("price_instrument")
            if not isinstance(source, str) or not source.strip() or not isinstance(pair, str) or not pair.strip():
                reasons.append("CAPTURE_OPERATIONAL_PRICE_IDENTITY_MISSING:" + row["timeframe"])
            elif symbol != "HYPE":
                route = {"exchange": "binance" if source == "binance_spot" else source,
                         "market": "spot" if market is None and source == "binance_spot" else market,
                         "pair": pair, "interval": "1m", "interval_seconds": 60, "api_coin": instrument}
                try:
                    canonical_price_path.validated_route(symbol, route, require_complete=False)
                except ValueError:
                    reasons.append("CAPTURE_OPERATIONAL_PRICE_IDENTITY_INCOMPATIBLE:" + row["timeframe"])
            elif source in ("hyperliquid", "hyperliquid_spot_@107"):
                try:
                    canonical_price_path.validated_route(symbol, {"exchange": "hyperliquid", "market": market,
                        "pair": pair, "interval": "1m", "interval_seconds": 60, "api_coin": instrument}, require_complete=False)
                except ValueError:
                    reasons.append("CAPTURE_HYPE_SPOT_IDENTITY_INCOMPLETE:" + row["timeframe"])
            elif anchors._normalized_pair(pair) != "HYPEUSDT":
                reasons.append("CAPTURE_HYPE_OPERATIONAL_PAIR_MISMATCH:" + row["timeframe"])
            # A captured HYPE PERP/fallback route is preserved as operational
            # identity only. It is never relabelled to the official Spot path.
            for key in ("source_observed_at_utc", "price_fetched_at_utc"):
                required_times.append((str(row.get("timeframe")) + ":" + key, row.get(key)))
        if sources.get("derivatives_snapshot_sha256"):
            positioning = _mapping(sources.get("positioning"))
            required_times += [("positioning:" + key, positioning.get(key)) for key in ("price_fetched_at", "oi_fetched_at")]
            for family in ("futures", "spot"):
                required_times.append((family + ":candle_close", _mapping(_mapping(sources.get(family)).get("quality")).get("candle_close")))
            required_times.append(("derivatives:observed", _mapping(sources.get("timing_observation")).get("cvd_observed_at_utc")))
        for name, family in (("positioning", "positioning"), ("futures_flow", "futures"), ("spot_flow", "spot")):
            model = _mapping(models.get(name))
            references = _mapping(_mapping(sources.get(family)).get("window_references"))
            time_families = _mapping(model.get("time_families"))
            if model.get("available") is True and (
                    not time_families or not references
                    or not any(_mapping(window).get("available") is True for window in references.values())):
                reasons.append("CAPTURE_AVAILABLE_MODEL_WINDOW_EVIDENCE_MISSING:" + name)
            for family_name, time_family in time_families.items():
                time_family = _mapping(time_family)
                members = time_family.get("members")
                if not isinstance(members, list):
                    if model.get("available") is True or time_family.get("available_windows"):
                        reasons.append("CAPTURE_TIME_FAMILY_MEMBERS_MISSING:" + name + ":" + str(family_name))
                    continue
                for member in members:
                    member = _mapping(member)
                    if member.get("available") is not True:
                        continue
                    label = member.get("window")
                    window = _mapping(references.get(label))
                    if window.get("available") is not True:
                        reasons.append("CAPTURE_AVAILABLE_MEMBER_REFERENCE_MISSING:" + name + ":" + str(label))
                    for key in ("latest_time", "reference_time"):
                        required_times.append((family + ":" + str(label) + ":" + key, window.get(key)))
        for family in ("positioning", "futures", "spot"):
            for label, window in _mapping(_mapping(sources.get(family)).get("window_references")).items():
                if _mapping(window).get("available") is True:
                    for key in ("latest_time", "reference_time"):
                        if _mapping(window).get(key) is None:
                            reasons.append("CAPTURE_AVAILABLE_WINDOW_TIME_MISSING:" + family + ":" + str(label) + ":" + key)
                for key, value in _mapping(window).items():
                    if key.endswith("time") and value is not None:
                        required_times.append((family + ":" + str(label) + ":" + key, value))
        for path, value in required_times:
            try:
                if _utc(value) > computed:
                    reasons.append("CAPTURE_SOURCE_TIME_FUTURE:" + path)
            except (TypeError, ValueError, OverflowError):
                reasons.append("CAPTURE_SOURCE_TIME_UNKNOWN:" + path)
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
        reasons.append("CAPTURE_INVALID:" + str(exc))
    return _result(reasons, snapshot_reference=reference, inner_payload_sha256=block.get("payload_sha256"),
                   coin=coin, model_observations=model_observations)


def validate_outcome_cell(event: Mapping | None, outcome: Mapping | None, *,
                          window_minutes: int, threshold_bps: int,
                          analysis_as_of_utc: Any) -> dict:
    """Read one exact v7 cell; a missing label never becomes OPEN or failure."""
    if not event:
        return _result(["OUTCOME_EVENT_AUTHORITY_MISSING"], source_status=None, reported_status="UNKNOWN")
    if not outcome:
        return _result(["OUTCOME_ROW_MISSING"], source_status=None, reported_status="UNKNOWN")
    reasons = []
    if outcome.get("window_minutes") != window_minutes or outcome.get("threshold_bps") != threshold_bps:
        reasons.append("OUTCOME_REQUESTED_CELL_MISMATCH")
    try:
        result = ordered.ordered_outcome_evidence(
            {"event": event, "ordered_outcome": outcome}, analysis_as_of_utc=analysis_as_of_utc)
        # Nondecisive states remain explicit UNKNOWN audit intersections;
        # retaining the original state never turns OPEN into a hit or failure.
        diagnostics = result["exclusion_reasons"]
        reasons.extend(diagnostics)
        source = str(outcome.get("price_source") or "")
        raw_parts = source.split("|")
        source_parts = [part.split("=", 1) for part in raw_parts if "=" in part]
        if (len(raw_parts) != 3 or len(source_parts) != 3
                or [key for key, _ in source_parts] != ["reference", "path", "provenance"]
                or any(not value.strip() for _, value in source_parts)):
            reasons.append("OUTCOME_PRICE_SOURCE_GRAMMAR_INVALID")
        reference_parts = [value for key, value in source_parts if key == "reference"]
        path_parts = [value for key, value in source_parts if key == "path"]
        if len(reference_parts) != 1 or len(path_parts) != 1:
            reasons.append("OUTCOME_PRICE_SOURCE_SEGMENTS_NOT_UNIQUE")
        official = _mapping(_mapping(_mapping(event.get("engine_snapshot")).get("prospective_anchor")).get("source_provenance"))
        official = _mapping(official.get("official_price"))
        expected_reference = "hyperliquid_spot_@107" if event.get("symbol") == "HYPE" else "binance_spot"
        expected_pair = "HYPE/USDT" if event.get("symbol") == "HYPE" else str(event.get("symbol")) + "USDT"
        allowed_references = {expected_reference, expected_reference + ":" + expected_pair}
        if official.get("source") == expected_reference:
            allowed_references.add("research_event_current_price")
        if len(reference_parts) != 1 or reference_parts[0] not in allowed_references:
            reasons.append("OUTCOME_REFERENCE_SOURCE_IDENTITY_MISMATCH")
        route_match = re.fullmatch(r"([^_:|]+)_([^:|]+):([^:|]+):([^|]+)", path_parts[0]) if len(path_parts) == 1 else None
        if route_match is None:
            reasons.append("OUTCOME_PRICE_SOURCE_ROUTE_MISSING")
        else:
            exchange, market, pair, interval = route_match.groups()
            route = {"exchange": exchange, "market": market, "pair": pair,
                     "interval": interval, "interval_seconds": outcome.get("candle_interval_seconds")}
            if event.get("symbol") == "HYPE":
                # The older text route lacks an instrument. Do not invent
                # @107 from the symbol; accept explicit persisted provenance
                # only, otherwise retain this material source limitation.
                provenance = _mapping(_mapping(outcome.get("calculation_audit")).get("price_provenance"))
                route["api_coin"] = provenance.get("instrument")
            verified = canonical_price_path.validated_route(event.get("symbol"), route, require_complete=False)
            if verified["pair"] != outcome.get("market_pair"):
                reasons.append("OUTCOME_MARKET_PAIR_MISMATCH")
            if canonical_price_path.quality_status(route, complete=outcome.get("path_complete") is True) != outcome.get("data_quality_status"):
                reasons.append("OUTCOME_ROUTE_QUALITY_MISMATCH")
        cutoff = _utc(analysis_as_of_utc)
        for key in ("created_at_utc", "updated_at_utc"):
            if key not in outcome or _utc(outcome[key]) > cutoff:
                reasons.append("OUTCOME_REVISION_TIME_MISSING_OR_AFTER_AUDIT:" + key)
        created, updated = _utc(outcome.get("created_at_utc")), _utc(outcome.get("updated_at_utc"))
        if created > updated:
            reasons.append("OUTCOME_REVISION_TIME_ORDER_INVALID")
        if created < _utc(outcome.get("measurement_start_utc")) or created < _utc(event.get("alert_time_utc")):
            reasons.append("OUTCOME_CREATED_BEFORE_EVENT")
        # An existing OPEN row may predate its eventual terminal observation.
        # The current revision, however, cannot predate evidence it contains.
        for key in ("measurement_start_utc", "observed_through_utc", "decision_time_utc"):
            if outcome.get(key) is not None and _utc(outcome[key]) > updated:
                reasons.append("OUTCOME_EVIDENCE_AFTER_REVISION:" + key)
        return _result(reasons, source_status=outcome.get("status"),
                       reported_status=result["reported_status"], canonical_diagnostics=diagnostics,
                       outcome=outcome)
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
        return _result(reasons + ["OUTCOME_INVALID:" + str(exc)], source_status=outcome.get("status"),
                       reported_status="UNKNOWN", outcome=outcome)


def validate_parent_membership(event: Mapping | None, membership: Mapping | None,
                               parent: Mapping | None, btc_bar: Mapping | None) -> dict:
    """Exact canonical membership and actual one-minute source bar; no waves."""
    reasons = []
    if not event:
        return _result(["PARENT_EVENT_AUTHORITY_MISSING"], membership=membership)
    if not membership:
        return _result(["PARENT_MEMBERSHIP_MISSING"], membership=None)
    try:
        if membership.get("episode_policy_version") != btc.POLICY_VERSION:
            reasons.append("PARENT_MEMBERSHIP_POLICY_MISMATCH")
        if membership.get("event_id") != event.get("event_id") or not _positive_id(event.get("event_id")):
            reasons.append("PARENT_MEMBERSHIP_EVENT_ID_MISMATCH")
        if not _same_time(membership.get("decision_time_utc"), event.get("alert_time_utc")):
            reasons.append("PARENT_MEMBERSHIP_DECISION_TIME_MISMATCH")
        if membership.get("membership_status") == "BTC_DATA_MISSING":
            reasons.append("BTC_DATA_MISSING")
        elif membership.get("membership_status") not in ("LIVE", "BOUNDARY_UNVERIFIED"):
            reasons.append("PARENT_MEMBERSHIP_STATUS_UNKNOWN")
        if not parent:
            reasons.append("BTC_PARENT_ROW_MISSING")
        if not btc_bar:
            reasons.append("BTC_SOURCE_BAR_MISSING")
        if parent and btc_bar:
            if (parent.get("episode_policy_version") != btc.POLICY_VERSION
                    or parent.get("btc_parent_movement_id") != membership.get("btc_parent_movement_id")):
                reasons.append("BTC_PARENT_ID_OR_POLICY_MISMATCH")
            if parent.get("price_source") != btc.SOURCE or btc_bar.get("price_source") != btc.SOURCE:
                reasons.append("BTC_PARENT_OR_BAR_SOURCE_MISMATCH")
            bar = btc.validate_candle(btc_bar)
            if not _same_time(bar["close_time_utc"], membership.get("btc_observed_close_utc")):
                reasons.append("BTC_MEMBERSHIP_BAR_TIME_MISMATCH")
            start = _utc(parent["start_time_utc"])
            if parent.get("btc_parent_movement_id") != btc._identity(start):
                reasons.append("BTC_PARENT_CANONICAL_ID_MISMATCH")
            if (type(parent.get("evidence_eligible")) is not bool
                    or parent.get("direction") not in ("UP", "DOWN", "UNKNOWN")
                    or _mapping(parent.get("state_json")).get("reversal_bps") != btc.REVERSAL_BPS
                    or _utc(parent["observed_through_utc"]) < bar["close_time_utc"]
                    or (parent.get("end_time_utc") is not None and _utc(parent["end_time_utc"]) <= start)):
                reasons.append("BTC_PARENT_STATE_INVALID")
            if parent.get("evidence_eligible") is True:
                if (not _same_time(parent.get("confirmed_at_utc"), start)
                        or parent.get("boundary_reason") != "CAUSAL_CLOSE_REVERSAL"
                        or parent.get("direction") == "UNKNOWN"):
                    reasons.append("BTC_PARENT_BOUNDARY_INVALID")
            elif (parent.get("confirmed_at_utc") is not None
                  or parent.get("boundary_reason") not in ("LEFT_BOUNDARY_UNVERIFIED", "BTC_DATA_GAP")):
                reasons.append("BTC_PARENT_BOUNDARY_INVALID")
            expected = btc.membership(event, parent=parent, btc_bar=bar)
            for key, value in expected.items():
                matches = _same_time(membership.get(key), value) if key.endswith("_utc") else membership.get(key) == value
                if not matches:
                    reasons.append("BTC_CANONICAL_MEMBERSHIP_MISMATCH:" + key)
            if expected["membership_status"] != "LIVE":
                reasons.append("BTC_PARENT_BOUNDARY_UNVERIFIED")
    except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
        reasons.append("BTC_PARENT_MEMBERSHIP_INVALID:" + str(exc))
    return _result(reasons, membership=membership, parent=parent, btc_bar=btc_bar)


def _rows(conn: Any, sql: str, params: Mapping | None = None, *,
          absolute_deadline_monotonic: float | None = None,
          monotonic: Callable[[], float] = time.monotonic) -> list[dict]:
    if absolute_deadline_monotonic is not None:
        before = monotonic()
        if (isinstance(before, bool) or not isinstance(before, (int, float))
                or not math.isfinite(float(before))):
            raise ValueError("monotonic clock returned an invalid value")
        if float(before) >= absolute_deadline_monotonic:
            raise AuditDeadlineExceeded("source audit deadline expired")
    rows = conn.execute(sql, params or {}).fetchall()
    if absolute_deadline_monotonic is not None:
        after = monotonic()
        if (isinstance(after, bool) or not isinstance(after, (int, float))
                or not math.isfinite(float(after)) or float(after) < float(before)):
            raise ValueError("monotonic clock returned an invalid value")
        if float(after) >= absolute_deadline_monotonic:
            raise AuditDeadlineExceeded("source audit deadline expired")
    if any(not isinstance(row, Mapping) for row in rows):
        raise ValueError("audit connection must return mapping rows; configure psycopg.rows.dict_row")
    return [dict(row) for row in rows]


def _assert_unique(rows: Sequence[Mapping], keys: Sequence[str], label: str) -> None:
    identities = [capture.canonical([row.get(key) for key in keys]) for row in rows]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate audit projection rows: " + label)


def _cursor(binding: str, high_water: int, after: int) -> dict:
    value = {"version": VERSION, "query_sha256": binding,
             "high_water_attempt_id": high_water, "after_attempt_id": after}
    return {**value, "cursor_sha256": capture.digest(value)}


def audit_anchor_attempt_page_from_connection(
    conn: Any, *, symbols: Sequence[str], start_utc: Any, end_utc: Any,
    max_capture_age_seconds: float, windows: Sequence[int] = ordered.HORIZONS_MINUTES,
    thresholds_bps: Sequence[int] = ordered.THRESHOLDS_BPS, page_size: int = 100,
    cursor: Mapping | None = None,
    absolute_deadline_monotonic: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Read a bounded page of v4 attempts using caller-owned read-only access.

    [start,end) filters source-candle open times, including failed attempts
    without decision times. A high-water keyset bounds IDs, not a database
    snapshot; mutable outcomes and late commits can differ across calls.
    """
    start, end = _utc(start_utc), _utc(end_utc)
    max_age = _max_age(max_capture_age_seconds)
    if (absolute_deadline_monotonic is not None
            and (isinstance(absolute_deadline_monotonic, bool)
                 or not isinstance(absolute_deadline_monotonic, (int, float))
                 or not math.isfinite(float(absolute_deadline_monotonic)))):
        raise ValueError("absolute audit deadline must be a finite monotonic value")

    def read(sql: str, params: Mapping | None = None) -> list[dict]:
        return _rows(
            conn, sql, params,
            absolute_deadline_monotonic=(float(absolute_deadline_monotonic)
                                         if absolute_deadline_monotonic is not None else None),
            monotonic=monotonic,
        )
    if start >= end:
        raise ValueError("start_utc must be earlier than end_utc")
    if isinstance(symbols, (str, bytes)) or not symbols or any(symbol not in capture.SYMBOLS for symbol in symbols):
        raise ValueError("symbols must be a nonempty explicit Top8 symbol list")
    if type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    selected_windows, selected_thresholds = tuple(windows), tuple(thresholds_bps)
    for values, allowed in ((selected_windows, ordered.HORIZONS_MINUTES), (selected_thresholds, ordered.THRESHOLDS_BPS)):
        if not values or any(type(value) is not int or value not in allowed for value in values) or len(set(values)) != len(values):
            raise ValueError("requested outcome cells must be nonempty, unique supported integers")
    params = {"sampler_version": anchors.SAMPLER_VERSION, "symbols": sorted(set(symbols)),
              "start_utc": start, "end_utc": end}
    scope = {**params, "version": VERSION, "max_capture_age_seconds": max_age,
             "windows": list(selected_windows), "thresholds_bps": list(selected_thresholds),
             "page_size": page_size, "capture_version": capture.VERSION,
             "outcome_version": ordered.METHOD_VERSION, "parent_policy": btc.POLICY_VERSION}
    binding = capture.digest(scope)
    after, high_water = 0, None
    if cursor is not None:
        if not isinstance(cursor, Mapping):
            raise ValueError("cursor must be the exact returned cursor object")
        high_water, after = cursor.get("high_water_attempt_id"), cursor.get("after_attempt_id")
        if (type(high_water) is not int or type(after) is not int
                or not 0 <= after <= high_water <= _INT64_MAX
                or dict(cursor) != _cursor(binding, high_water, after)):
            raise ValueError("cursor is malformed, changed, or bound to a different query")
    tx_rows = read("""/* audit:transaction */ SELECT
        current_setting('transaction_read_only') AS read_only,
        current_setting('transaction_isolation') AS isolation,
        pg_backend_pid() AS backend_pid,
        transaction_timestamp() AS transaction_started_at_utc,
        pg_current_snapshot()::text AS transaction_snapshot,
        clock_timestamp() AS observed_at_utc""")
    if len(tx_rows) != 1 or tx_rows[0].get("read_only") not in ("on", True):
        raise ValueError("caller must supply a read-only PostgreSQL connection/transaction")
    tx = tx_rows[0]
    observed = _utc(tx["observed_at_utc"])
    isolation = str(tx.get("isolation") or "unknown").lower()
    consistent = isolation in ("repeatable read", "serializable") and getattr(conn, "autocommit", None) is False
    backend_pid = tx.get("backend_pid")
    transaction_snapshot = tx.get("transaction_snapshot")
    if (not _positive_id(backend_pid) or not isinstance(transaction_snapshot, str)
            or not transaction_snapshot.strip()):
        raise ValueError("database transaction identity is unavailable")
    transaction_identity = transaction_identity_from_fields(
        backend_pid=backend_pid,
        transaction_started_at_utc=tx.get("transaction_started_at_utc"),
        database_snapshot_id=transaction_snapshot,
    )
    transaction_identity_sha256 = transaction_identity["transaction_identity_sha256"]
    if high_water is None:
        marks = read("""/* audit:high-water */ SELECT COALESCE(MAX(attempt_id), 0) AS high_water_attempt_id
            FROM research_prospective_anchor_attempts
            WHERE sampler_version=%(sampler_version)s AND symbol=ANY(%(symbols)s)
              AND source_candle_open_utc >= %(start_utc)s AND source_candle_open_utc < %(end_utc)s""", params)
        if len(marks) != 1:
            raise ValueError("invalid audit high-water projection cardinality")
        high_water = marks[0]["high_water_attempt_id"]
        if type(high_water) is not int or not 0 <= high_water <= _INT64_MAX:
            raise ValueError("invalid attempt high-water mark")
    attempts = read("""/* audit:attempts */ SELECT * FROM research_prospective_anchor_attempts
        WHERE sampler_version=%(sampler_version)s AND symbol=ANY(%(symbols)s)
          AND source_candle_open_utc >= %(start_utc)s AND source_candle_open_utc < %(end_utc)s
          AND attempt_id > %(after_attempt_id)s AND attempt_id <= %(high_water_attempt_id)s
        ORDER BY attempt_id ASC LIMIT %(limit)s""",
        {**params, "after_attempt_id": after, "high_water_attempt_id": high_water, "limit": page_size + 1})
    ids = [row.get("attempt_id") for row in attempts]
    if (len(attempts) > page_size + 1 or any(not _positive_id(eid) for eid in ids)
            or ids != sorted(set(ids)) or any(not after < eid <= high_water for eid in ids)):
        raise ValueError("database attempt page violated its bounded keyset")
    output = []
    for attempt in attempts[:page_size]:
        if (attempt.get("sampler_version") != anchors.SAMPLER_VERSION
                or attempt.get("symbol") not in params["symbols"]
                or not start <= _utc(attempt.get("source_candle_open_utc")) < end):
            raise ValueError("database attempt page violated its cohort")
        slot, event_rows, capture_row, outcome_rows, memberships, parent_rows, bars = None, [], None, [], [], [], []
        if attempt.get("evaluation_status") == anchors.EVALUABLE:
            slots = read("""/* audit:slot */ SELECT * FROM research_prospective_anchor_slots
                WHERE sampler_version=%(sampler_version)s AND symbol=%(symbol)s
                  AND source_candle_open_utc=%(source_candle_open_utc)s LIMIT 2""", attempt)
            if len(slots) > 1:
                raise ValueError("duplicate audit projection rows: slots")
            if len(slots) == 1:
                slot = slots[0]
                event_ids = [slot.get("long_event_id"), slot.get("short_event_id")]
                event_ids = [eid for eid in event_ids if _positive_id(eid)]
                event_rows = read("""/* audit:events */ SELECT * FROM research_events
                    WHERE event_id=ANY(%(event_ids)s) ORDER BY event_id LIMIT 2""", {"event_ids": event_ids})
                outcome_rows = read("""/* audit:outcomes */ SELECT * FROM research_ordered_first_touch_outcomes
                    WHERE event_id=ANY(%(event_ids)s) AND method_version=%(method_version)s
                      AND window_minutes=ANY(%(windows)s) AND threshold_bps=ANY(%(thresholds_bps)s)
                    ORDER BY event_id, window_minutes, threshold_bps LIMIT %(limit)s""",
                    {"event_ids": event_ids, "method_version": ordered.METHOD_VERSION,
                     "windows": list(selected_windows), "thresholds_bps": list(selected_thresholds),
                     "limit": 2 * len(selected_windows) * len(selected_thresholds)})
                memberships = read("""/* audit:memberships */ SELECT * FROM research_event_btc_movements
                    WHERE event_id=ANY(%(event_ids)s) AND episode_policy_version=%(parent_policy)s LIMIT 2""",
                    {"event_ids": event_ids, "parent_policy": btc.POLICY_VERSION})
                parent_ids = list({row["btc_parent_movement_id"] for row in memberships if row.get("btc_parent_movement_id")})
                parent_rows = read("""/* audit:parents */ SELECT * FROM research_btc_parent_movements
                    WHERE btc_parent_movement_id=ANY(%(parent_ids)s) AND episode_policy_version=%(parent_policy)s LIMIT 2""",
                    {"parent_ids": parent_ids, "parent_policy": btc.POLICY_VERSION})
                close_times = [row["btc_observed_close_utc"] for row in memberships if row.get("btc_observed_close_utc")]
                bars = read("""/* audit:bars */ SELECT * FROM research_btc_price_bars
                    WHERE close_time_utc=ANY(%(close_times)s) LIMIT 2""", {"close_times": close_times})
                for rows, keys, label in (
                        (event_rows, ("event_id",), "events"),
                        (outcome_rows, ("event_id", "window_minutes", "threshold_bps", "method_version"), "outcomes"),
                        (memberships, ("event_id", "episode_policy_version"), "memberships"),
                        (parent_rows, ("btc_parent_movement_id", "episode_policy_version"), "parents"),
                        (bars, ("close_time_utc",), "bars")):
                    _assert_unique(rows, keys, label)
            # Capture association is based on this attempt's own decision,
            # even when its slot/event authority is missing or broken.
            try:
                decision = _utc(attempt.get("decision_time_utc"))
            except (TypeError, ValueError, OverflowError):
                decision = None
            if decision is not None:
                captures = read("""/* audit:capture */ SELECT * FROM research_max_pain_snapshot_sets
                    WHERE source='WATCH_SHARED'
                      AND available_at_utc IS NOT NULL AND created_at_utc IS NOT NULL
                      AND GREATEST(available_at_utc, created_at_utc) <= %(decision_time_utc)s
                      AND GREATEST(available_at_utc, created_at_utc) >= %(decision_time_utc)s - %(max_age)s
                    ORDER BY GREATEST(available_at_utc, created_at_utc) DESC, snapshot_set_id DESC LIMIT 1""",
                    {"decision_time_utc": decision, "max_age": timedelta(seconds=max_age)})
                if len(captures) > 1:
                    raise ValueError("duplicate audit projection rows: captures")
                capture_row = captures[0] if captures else None
        authority = validate_anchor_authority(attempt, slot, event_rows)
        captured = validate_capture(capture_row, symbol=attempt["symbol"],
            decision_time_utc=attempt.get("decision_time_utc"), max_capture_age_seconds=max_age)
        if attempt.get("evaluation_status") != anchors.EVALUABLE:
            captured = _result(["ATTEMPT_NOT_EVALUABLE_NO_DECISION_TIME"], snapshot_reference=None, coin=None)
        cells, parents = [], {}
        event_by_id = {row["event_id"]: row for row in event_rows}
        for direction in anchors.DIRECTIONS:
            eid = slot.get(direction.lower() + "_event_id") if slot else None
            event = event_by_id.get(eid)
            matching = [row for row in memberships if row.get("event_id") == eid]
            member = matching[0] if len(matching) == 1 else None
            parent = next((row for row in parent_rows if member and row.get("btc_parent_movement_id") == member.get("btc_parent_movement_id")), None)
            bar = next((row for row in bars if member and _same_time(row.get("close_time_utc"), member.get("btc_observed_close_utc"))), None)
            parents[direction] = validate_parent_membership(event, member, parent, bar)
            for window in selected_windows:
                for bps in selected_thresholds:
                    found = [row for row in outcome_rows if row.get("event_id") == eid
                             and row.get("window_minutes") == window and row.get("threshold_bps") == bps]
                    cell = validate_outcome_cell(event, found[0] if len(found) == 1 else None,
                        window_minutes=window, threshold_bps=bps, analysis_as_of_utc=observed)
                    cells.append({"direction": direction, "event_id": eid,
                                  "window_minutes": window, "threshold_bps": bps, **cell})
        output.append({"attempt": deepcopy(attempt), "anchor_authority": authority,
                       "capture": captured, "outcome_cells": cells, "parent_memberships": parents,
                       "delivery_state": "UNKNOWN_NOT_AUDITED"})
    next_cursor = _cursor(binding, high_water, output[-1]["attempt"]["attempt_id"]) if len(attempts) > page_size else None
    return {"version": VERSION, "scope": scope, "rows": output,
            "high_water_attempt_id": high_water, "next_cursor": next_cursor,
            "examined": len(output), "emitted": len(output), "has_more": next_cursor is not None,
            "population_page_complete": next_cursor is None,
            "snapshot_consistency": "CALLER_TRANSACTION_SNAPSHOT" if consistent else "STATEMENT_SNAPSHOTS_NOT_ATOMIC",
            "transaction_isolation": isolation, "read_started_at_utc": observed,
            "transaction_identity_sha256": transaction_identity_sha256,
            "cross_page_snapshot_guaranteed": False,
            "interpretation": "Source audit only; no delivery, independence, asymmetry, qualification or approval inference."}
