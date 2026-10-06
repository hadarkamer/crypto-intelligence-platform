"""Pure Telegram-text rendering for verified Formula Evidence snapshots.

This module deliberately has no Telegram, database, worker or LIVE integration.
It accepts only fingerprint-verified ``EvidenceSnapshot`` values and returns
plain deterministic text plus dry-run audit metadata.  It never recalculates
acceptance, maturity, probability or asymmetry.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Dict, Iterable, Mapping, Sequence

import research_evidence_contract as evidence_contract


RENDERER_VERSION = "evidence-telegram-renderer-v2-runtime-bound-dry-run"
FAMILY_AGGREGATION_POLICY_VERSION = "latest-assessed-snapshot-per-family-v1"
EXPERIMENTAL_LABEL = "ניסיונית — אינה המלצת מסחר"

_COMPATIBILITY_LABELS = {
    evidence_contract.CURRENT_V7: "V7 נוכחי",
    evidence_contract.RETAINED_V7_1_READ_ONLY: "V7.1 שמור — קריאה בלבד",
    evidence_contract.LEGACY_SHADOW_READ_ONLY: "Legacy Shadow — קריאה בלבד",
}
_DIRECTION_LABELS = {
    "LONG": "לונג",
    "SHORT": "שורט",
}
_MATURITY_LABELS = {
    "RESEARCH_READY": "מוכנה למחקר בלבד",
    "EARLY_CURRENT_EDGE": "יתרון נוכחי מוקדם — עדיין במחקר",
    "ACCUMULATING_EVIDENCE": "צוברת ראיות",
    "STALE_OR_NOT_RECENT": "הראיות אינן עדכניות מספיק",
    "EVIDENCE_PRESENT_EDGE_NOT_ESTABLISHED": "קיימות ראיות, היתרון טרם הוכח",
}
_PATH_LABELS = {
    "PROBABILITY": "הסתברות",
    "ASYMMETRY": "אי־סימטריה",
}


def _verified_snapshot(
    value: evidence_contract.EvidenceSnapshot | Mapping[str, Any],
) -> evidence_contract.EvidenceSnapshot:
    """Fingerprint-verify both mappings and nominal snapshot instances."""

    payload = value.to_dict() if isinstance(value, evidence_contract.EvidenceSnapshot) else value
    return evidence_contract.EvidenceSnapshot.from_dict(payload)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _format_number(value: Any, *, suffix: str = "") -> str | None:
    number = _number(value)
    if number is None:
        return None
    rounded = round(number, 2)
    rendered = str(int(rounded)) if rounded.is_integer() else f"{rounded:.2f}".rstrip("0")
    return f"{rendered}{suffix}"


def _format_horizon(minutes: Any) -> str:
    horizon = int(minutes)
    if horizon % 1440 == 0:
        days = horizon // 1440
        return f"{days} יום" if days == 1 else f"{days} ימים"
    if horizon % 60 == 0:
        hours = horizon // 60
        return f"{hours} שעה" if hours == 1 else f"{hours} שעות"
    return f"{horizon} דקות"


def _format_utc(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    normalized = parsed.astimezone(timezone.utc)
    return normalized.strftime("%d.%m.%Y %H:%M UTC")


def _text_values(value: Any) -> list[str]:
    candidates: Sequence[Any]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        candidates = value
    else:
        candidates = (value,)
    return sorted(
        {
            str(candidate or "").strip().upper()
            for candidate in candidates
            if str(candidate or "").strip()
        }
    )


def _symbol_label(payload: Mapping[str, Any]) -> str:
    for container_name in ("evidence", "provenance", "metrics"):
        container = payload.get(container_name)
        if not isinstance(container, Mapping):
            continue
        for key in ("symbol", "symbols", "matched_symbols", "symbol_scope"):
            values = _text_values(container.get(key))
            if values:
                return ", ".join(values)
    return "רב־מטבעי — לא צוין מטבע יחיד ב־snapshot"


def _first_metric(
    containers: Sequence[Mapping[str, Any]], *names: str
) -> Any:
    for name in names:
        for container in containers:
            if name in container and container.get(name) is not None:
                return container.get(name)
    return None


def _probability_line(
    metrics: Mapping[str, Any], assessment_metrics: Mapping[str, Any]
) -> str:
    containers = (metrics, assessment_metrics)
    values = []
    for label, keys, suffix in (
        (
            "שיעור הצלחה משוקלל לעדכניות",
            ("recency_weighted_hit_rate_pct", "hit_rate_pct"),
            "%",
        ),
        (
            "Wilson 95% תחתון",
            (
                "recency_weighted_wilson_95_lower_approx_pct",
                "wilson_95_lower_pct",
            ),
            "%",
        ),
        (
            "שיפור מול ביקורת",
            (
                "recency_weighted_hit_rate_improvement_pct_points",
                "hit_rate_improvement_pct_points",
            ),
            " נק׳ אחוז",
        ),
    ):
        rendered = _format_number(_first_metric(containers, *keys), suffix=suffix)
        if rendered is not None:
            values.append(f"{label}: {rendered}")
    return "הסתברות: " + (" | ".join(values) if values else "לא נמסרה ב־snapshot")


def _asymmetry_line(
    metrics: Mapping[str, Any], assessment_metrics: Mapping[str, Any]
) -> str:
    containers = (metrics, assessment_metrics)
    values = []
    for label, keys, suffix in (
        (
            "דומיננטיות חיובית",
            (
                "recency_weighted_favorable_dominance_rate_pct",
                "favorable_dominance_rate_pct",
            ),
            "%",
        ),
        (
            "שיפור דומיננטיות מול ביקורת",
            (
                "recency_weighted_favorable_dominance_improvement_pct_points",
                "favorable_dominance_improvement_pct_points",
            ),
            " נק׳ אחוז",
        ),
        (
            "פער חיובי־שלילי חציוני",
            (
                "recency_weighted_median_paired_favorable_minus_adverse_pct",
                "median_paired_favorable_minus_adverse_pct",
            ),
            "%",
        ),
        (
            "MFE חציוני",
            ("recency_weighted_median_mfe_pct", "median_mfe_pct"),
            "%",
        ),
        (
            "MAE חציוני",
            ("recency_weighted_median_mae_pct", "median_mae_pct"),
            "%",
        ),
        (
            "יחס MFE/MAE שמור",
            ("median_mfe_mae_ratio",),
            "",
        ),
    ):
        rendered = _format_number(_first_metric(containers, *keys), suffix=suffix)
        if rendered is not None:
            values.append(f"{label}: {rendered}")
    return "אי־סימטריה: " + (" | ".join(values) if values else "לא נמסרה ב־snapshot")


def _risk_line(
    metrics: Mapping[str, Any], assessment_metrics: Mapping[str, Any]
) -> str:
    containers = (metrics, assessment_metrics)
    values = []
    for label, keys in (
        (
            "MAE חציוני",
            ("recency_weighted_median_mae_pct", "median_mae_pct"),
        ),
        (
            "MAE p90",
            ("recency_weighted_mae_p90_pct", "current_mae_p90_pct"),
        ),
        (
            "MAE p95",
            ("recency_weighted_mae_p95_pct", "current_mae_p95_pct"),
        ),
    ):
        rendered = _format_number(_first_metric(containers, *keys), suffix="%")
        if rendered is not None:
            values.append(f"{label}: {rendered}")
    tail_warning = assessment_metrics.get("p90_adverse_exceeds_median_favorable")
    if tail_warning is True:
        values.append("אזהרה: סיכון p90 גדול מהתנועה החיובית החציונית")
    return "סיכון: " + (" | ".join(values) if values else "לא נמסר ב־snapshot")


def _recency_line(payload: Mapping[str, Any], metrics: Mapping[str, Any]) -> str:
    values = [f"נבדק: {_format_utc(payload['assessed_at_utc'])}"]
    recent = _format_number(metrics.get("recent_sample_size"))
    effective = _format_number(metrics.get("recency_effective_sample_size"))
    age = _format_number(metrics.get("last_sample_age_hours"), suffix=" שעות")
    if recent is not None:
        values.append(f"דגימות עדכניות: {recent}")
    if effective is not None:
        values.append(f"N_eff עדכני: {effective}")
    if age is not None:
        values.append(f"גיל ההתאמה האחרונה: {age}")
    return "עדכניות: " + " | ".join(values)


def _render_verified_snapshot(
    snapshot: evidence_contract.EvidenceSnapshot,
    *,
    family_snapshot_count: int,
    family_formula_count: int,
) -> str:
    if family_snapshot_count <= 0 or family_formula_count <= 0:
        raise ValueError("family counts must be positive")
    payload = snapshot.to_dict()
    formula = payload["formula"]
    assessment = payload["assessment"]
    metrics = payload.get("metrics") or {}
    assessment_metrics = assessment.get("metrics") or {}
    if not isinstance(metrics, Mapping) or not isinstance(assessment_metrics, Mapping):
        raise ValueError("snapshot metrics must be objects")

    accepted_paths = [
        _PATH_LABELS[path]
        for path in assessment.get("accepted_paths") or []
        if path in _PATH_LABELS
    ]
    path_label = " + ".join(accepted_paths) if accepted_paths else "אין — מחקר בלבד"
    maturity = str(assessment.get("maturity") or "")
    direction = str(formula.get("direction") or "")
    matched_episodes = len(payload.get("matched_market_episode_ids") or [])
    control_episodes = len(payload.get("control_market_episode_ids") or [])
    matched_parents = len(payload.get("matched_parent_market_episode_ids") or [])
    control_parents = len(payload.get("control_parent_market_episode_ids") or [])
    control_sample = _format_number(metrics.get("control_sample_size")) or "לא נמסר"
    family_id = str(payload["formula_family_id"])
    snapshot_id = str(payload["snapshot_id"])
    snapshot_count_label = (
        "Snapshot מאומת"
        if family_snapshot_count == 1
        else f"{family_snapshot_count} Snapshots מאומתים"
    )
    formula_count_label = (
        "נוסחה אחת"
        if family_formula_count == 1
        else f"{family_formula_count} נוסחאות"
    )

    lines = [
        f"🧪 {EXPERIMENTAL_LABEL}",
        f"חוזה: {_COMPATIBILITY_LABELS[snapshot.runtime_compatibility]}",
        f"מטבע: {_symbol_label(payload)}",
        f"כיוון: {_DIRECTION_LABELS.get(direction, direction)} ({direction})",
        f"אופק: {_format_horizon(formula['horizon_minutes'])}",
        f"בשלות: {_MATURITY_LABELS.get(maturity, maturity)}",
        f"מסלול קבלה: {path_label}",
        _probability_line(metrics, assessment_metrics),
        _asymmetry_line(metrics, assessment_metrics),
        (
            "Market Episodes: "
            f"התאמות {matched_episodes} (הורים {matched_parents}; "
            f"raw {payload['raw_match_count']}) | "
            f"ביקורת {control_episodes} (הורים {control_parents}; "
            f"raw {payload['raw_control_count']})"
        ),
        (
            f"N_eff: התאמות {_format_number(payload['matched_n_eff'])} | "
            f"ביקורת {_format_number(payload['control_n_eff'])}"
        ),
        f"ביקורת: sample {control_sample}",
        _recency_line(payload, metrics),
        _risk_line(metrics, assessment_metrics),
        f"איגוד משפחה: {snapshot_count_label} | {formula_count_label}",
        f"Family: {family_id[:12]}… | Snapshot: {snapshot_id[:12]}…",
        "מצב: DRY RUN בלבד — אין משלוח Telegram ואין הרשאת LIVE",
    ]
    return "\n".join(lines)


def render_evidence_snapshot(
    value: evidence_contract.EvidenceSnapshot | Mapping[str, Any],
) -> str:
    """Render one verified snapshot without external reads or side effects."""

    snapshot = _verified_snapshot(value)
    return _render_verified_snapshot(
        snapshot,
        family_snapshot_count=1,
        family_formula_count=1,
    )


def dry_run_evidence_snapshots(
    values: Iterable[evidence_contract.EvidenceSnapshot | Mapping[str, Any]],
) -> Dict[str, Any]:
    """Deduplicate verified snapshots and render one message per evidence family."""

    if isinstance(values, (str, bytes, bytearray, Mapping)):
        raise ValueError("dry-run input must be an iterable of EvidenceSnapshots")
    supplied = list(values)
    verified = [_verified_snapshot(value) for value in supplied]
    unique_by_id = {snapshot.snapshot_id: snapshot for snapshot in verified}

    families: Dict[str, list[evidence_contract.EvidenceSnapshot]] = {}
    for snapshot in unique_by_id.values():
        families.setdefault(snapshot.formula_family_id, []).append(snapshot)

    messages = []
    for family_id in sorted(families):
        members = families[family_id]
        signatures = {
            (
                member.runtime_compatibility,
                member.to_dict()["formula"]["direction"],
                int(member.to_dict()["formula"]["horizon_minutes"]),
            )
            for member in members
        }
        if len(signatures) != 1:
            raise ValueError(
                "formula family contains incompatible compatibility, direction or horizon"
            )
        members.sort(
            key=lambda member: (
                member.to_dict()["assessed_at_utc"],
                member.snapshot_id,
            )
        )
        representative = members[-1]
        formula_keys = sorted(
            {str(member.to_dict()["formula"]["formula_key"]) for member in members}
        )
        messages.append(
            {
                "formula_family_id": family_id,
                "compatibility": representative.runtime_compatibility,
                "representative_snapshot_id": representative.snapshot_id,
                "snapshot_ids": sorted(member.snapshot_id for member in members),
                "formula_keys": formula_keys,
                "aggregated_snapshot_count": len(members),
                "aggregated_formula_count": len(formula_keys),
                "text": _render_verified_snapshot(
                    representative,
                    family_snapshot_count=len(members),
                    family_formula_count=len(formula_keys),
                ),
            }
        )

    return {
        "renderer_version": RENDERER_VERSION,
        "family_aggregation_policy_version": FAMILY_AGGREGATION_POLICY_VERSION,
        "mode": "DRY_RUN",
        "input_snapshots": len(supplied),
        "verified_unique_snapshots": len(unique_by_id),
        "duplicates_suppressed": len(supplied) - len(unique_by_id),
        "families_rendered": len(messages),
        "delivery_attempts": 0,
        "delivery_channel": "NONE",
        "live_effect": "NONE",
        "messages": messages,
    }
