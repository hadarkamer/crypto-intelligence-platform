"""Explicit experimental delivery contract over exactly bound ordered-v7 evidence.

Qualification consumes the existing prospective policy without changing it.
The trigger is a later native delivered alert, never a historical outcome or an
archive row. This module renders research notifications, never trade orders.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

import research_formula_ordered_v7 as evidence
import research_ordered_acceptance_policy as acceptance
import research_ordered_question_catalog as questions
import research_ordered_validation as validation
import research_formula_ordered_store as formula_store
import research_ordered_inverse_store as inverse_store

VERSION = "ordered-v7-experimental-delivery-v1"
QUALIFICATION_TTL = timedelta(minutes=30)
TRIGGER_TTL = timedelta(minutes=10)
_DEFINITIONS = {c['formula_id']: {**c,'direction_mode':c.get('research_orientation','NORMAL')}
                for c in evidence.candidate_catalog(include_extended=True) if c.get('catalog_version')==questions.VERSION}


def qualification(registration: Mapping[str, Any], result: Mapping[str, Any],
                  *, evaluated_at: Any, now: Any) -> dict[str, Any]:
    """Fail closed on stale, altered, incomplete or incompatible evidence."""
    clock, assessed = evidence._utc(now), evidence._utc(evaluated_at)
    binding = registration["binding"]
    candidate = binding["candidate_definition"]
    policy = registration.get("acceptance_policy")
    expected = acceptance.policy_for(binding, candidate)
    period = formula_store.period_contract(binding)
    if (binding.get("source_scope") != "LIVE" or expected is None
            or binding.get('parent_policy_version') != formula_store.PARENT_POLICY
            or binding.get('period_version') != period['period_version']
            or validation.canonical(candidate) != validation.canonical(_DEFINITIONS.get(binding['candidate_key']))
            or validation.canonical(policy) != validation.canonical(expected)
            or binding.get("outcome_method_version") != evidence.METHOD_VERSION
            or binding.get("validation_version") != validation.VERSION
            or candidate.get("catalog_version") != questions.VERSION):
        raise ValueError("INCOMPATIBLE_EXACT_ACCEPTANCE")
    definition = validation.digest({"binding": binding, "acceptance_policy": policy})
    if (registration["definition_sha256"] != definition
            or result.get("definition_sha256") != definition
            or result.get("freeze_id") != registration["freeze_id"]
            or result.get("validation_version") != validation.VERSION
            or result.get("frozen_at_utc") != registration["frozen_at_utc"]
            or evidence._utc(registration["frozen_at_utc"]) > assessed
            or assessed > clock or assessed + QUALIFICATION_TTL <= clock):
        raise ValueError("STALE_OR_INCONSISTENT_QUALIFICATION")
    if (result.get("research_ready") is not True
            or result.get("source_coverage_complete") is not True
            or result.get("representative_entries_frozen") is not True
            or result.get("representative_conflicts")):
        raise ValueError("INCOMPLETE_OR_UNQUALIFIED_EVIDENCE")
    prospective = result["prospective"]
    attempts = result["registered_attempts"]
    if type(attempts) is not int or attempts < 1:
        raise ValueError("INVALID_MULTIPLICITY")
    standard = validation._accept(prospective, policy, minimum=5, complete=True, attempts=attempts)
    fresh = validation._accept(prospective["fresh"], policy, minimum=3, complete=True, attempts=attempts)
    route = "REGULAR" if standard["research_ready"] else "FRESH" if fresh["research_ready"] else None
    if route is None:
        raise ValueError("PROSPECTIVE_GATES_FAILED")
    metrics = prospective if route == "REGULAR" else prospective["fresh"]
    if metrics.get("common_window_complete") is not True or metrics.get("source_coverage_complete") is not True:
        raise ValueError("INCOMPLETE_COMMON_WINDOW")
    expires = assessed + QUALIFICATION_TTL
    parents = set(metrics.get("btc_parent_movement_ids") or [])
    if not parents:
        raise ValueError("MISSING_QUALIFYING_WAVES")
    if route == "FRESH":
        starts = [evidence._utc(row["parent_start_time_utc"]) for row in result.get("episodes", [])
                  if row.get("phase") == "PROSPECTIVE" and row.get("btc_parent_movement_id") in parents]
        if len(starts) != len(parents):
            raise ValueError("MISSING_FRESH_EVIDENCE_CLOCK")
        expires = min(expires, min(starts) + timedelta(days=14))
        if expires <= clock:
            raise ValueError("FRESH_EVIDENCE_EXPIRED")
    # Keep every discovery/prospective parent out of a new trigger, even when
    # a different route did not use that parent in its hit-rate denominator.
    all_parents = sorted({row["btc_parent_movement_id"] for row in result.get("episodes", [])})
    return {"route": route, "metrics": metrics, "eligible_until_utc": expires,
            "qualified_at_utc": assessed, "excluded_parent_ids": all_parents}


def notification(registration: Mapping[str, Any], result: Mapping[str, Any],
                 event: Mapping[str, Any], *, evaluated_at: Any, published_at: Any, now: Any) -> dict[str, Any]:
    q = qualification(registration, result, evaluated_at=evaluated_at, now=now)
    b = registration["binding"]
    clock, when = evidence._utc(now), evidence._utc(event["alert_time_utc"])
    features = event.get("decision_features") or {}
    mode = b["candidate_definition"].get("research_orientation", "NORMAL")
    source_direction = event.get("source_direction")
    original = {**event,'direction':source_direction}
    if inverse_store.source_error(original):
        raise ValueError('UNVERIFIED_NATIVE_SOURCE_PRICE')
    snapshot = event.get('engine_snapshot') or {}
    if any(key in snapshot for key in ('archive_reconstruction','archive_only','telegram_archive','archive_run_key')):
        raise ValueError('ARCHIVE_SOURCE_CANNOT_TRIGGER')
    mapped = {"LONG": "SHORT", "SHORT": "LONG"}.get(source_direction) if mode == "INVERSE" else source_direction
    if (event.get("event_kind") != "ALERT" or event.get("delivery_status") != "DELIVERED"
            or event.get("source_scope") != "LIVE" or event.get("candidate_key") != b["candidate_key"]
            or type(event.get("event_id")) is not int or event["event_id"] < 1
            or event.get("direction") != b["direction"] or mapped != b["direction"]
            or features.get("event.direction_mapping_valid") is not True
            or features.get("event.analysis_direction") != source_direction
            or (b["symbol"] != "ALL" and event.get("symbol") != b["symbol"])
            or not evidence._matches(features, b["conditions"])):
        raise ValueError("INVALID_LIVE_TRIGGER_OR_PREDICATE")
    published = evidence._utc(published_at)
    if not (q["qualified_at_utc"] <= published < when <= clock and clock - when <= TRIGGER_TTL):
        raise ValueError("TRIGGER_NOT_NEW_OR_FRESH")
    parent = event.get("btc_parent_movement_id")
    if (event.get("membership_status") != "LIVE" or event.get("parent_evidence_eligible") is not True
            or event.get("episode_policy_version") != b["parent_policy_version"]
            or not parent or parent in q["excluded_parent_ids"]
            or evidence._utc(event["parent_start_time_utc"]) > when
            or evidence._utc(event["parent_confirmed_at_utc"]) > when
            or evidence._utc(event["decision_time_utc"]) != when
            or not when - timedelta(minutes=1) < evidence._utc(event["btc_observed_close_utc"]) <= when):
        raise ValueError("UNVERIFIED_OR_ALREADY_USED_BTC_PARENT")
    price = evidence._number(event.get("entry_price"))
    if price is None or price <= 0 or price != evidence._number(event.get('current_price')):
        raise ValueError("MISSING_TRIGGER_PRICE")
    return {"delivery_version": VERSION, "freeze_id": registration["freeze_id"],
            "scope_key": b["scope_key"], "candidate_key": b["candidate_key"],
            "definition_sha256": registration["definition_sha256"],
            "event_id": event["event_id"], "event_fingerprint": event.get("event_fingerprint"),
            "symbol": event["symbol"], "direction": b["direction"], "orientation": mode,
            "alert_time_utc": when.isoformat(), "entry_price": price,
            "btc_parent_movement_id": parent, "threshold_bps": b["threshold_bps"],
            "window_minutes": b["window_minutes"], "period_key": b["period_key"],
            "conditions": b["conditions"], "route": q["route"], "metrics": q["metrics"],
            "qualification_at_utc": published.isoformat(),
            "expires_at_utc": min(q["eligible_until_utc"], when + TRIGGER_TTL).isoformat(),
            "source_scope": "LIVE", "live_effect": "EXPERIMENTAL_NOTIFICATION_ONLY"}


def render(payload: Mapping[str, Any]) -> str:
    if payload.get("delivery_version") != VERSION or payload.get("source_scope") != "LIVE":
        raise ValueError("unsupported experimental notification")
    m = payload["metrics"]
    direction = "עלייה (LONG)" if payload["direction"] == "LONG" else "ירידה (SHORT)"
    conditions = "; ".join(f"{c['feature']} {c['operator']} {c['value']}" for c in payload["conditions"])
    return "\n".join([
        "🧪 ניסיוני, לא למסחר", f"{payload['symbol']} | {direction}",
        f"התראה חדשה: {payload['alert_time_utc']} | מחיר בעת ההתראה: {payload['entry_price']:g}",
        f"סף שנבדק: {payload['threshold_bps'] / 100:g}% לכל כיוון | חלון: {payload['window_minutes']} דקות",
        "מסלול: " + ("מוקדם — ראיות מ־14 הימים האחרונים" if payload["route"] == "FRESH" else "רגיל"),
        f"גלים עתידיים עצמאיים שהוכרעו: {m['resolved_waves']} | הצלחה: {m['hit_rate_pct']:.1f}%",
        f"גבול הסתברות תחתון: {m['wilson_95_lower_pct']:.1f}% | יחס תנועה חיובית/נגדית: {m['common_window_asymmetry_ratio']:.2f}",
        f"תנאים: {conditions}", f"מחקר {'הפוך' if payload['orientation'] == 'INVERSE' else 'רגיל'} | תקופה: {payload['period_key']}",
        f"נוסחה: {payload['candidate_key']} | אירוע: {payload['event_id']}",
        "הנתונים מתארים מדגם מחקרי. לא בוצעה עסקה.",
    ])[:4000]
