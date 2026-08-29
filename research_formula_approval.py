"""Explicit, auditable owner operation for a Formula SHADOW -> LIVE transition.

This module is intentionally not imported by ``main.py``.  It is a manual CLI
and never changes deployment environment variables.  A successful transition
therefore leaves Telegram delivery disabled until the operator separately
enables ``FORMULA_LIVE_ALERTS_ENABLED`` in the production service.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import sys
from typing import Any, Dict, Mapping, Optional, Sequence

import research_feature_matrix
import research_formula_engine
import research_formula_store


OPERATION_VERSION = "formula-owner-live-approval-v1"
OWNER_TOKEN_ENV = "FORMULA_OWNER_APPROVAL_TOKEN"
OWNER_TOKEN_CONFIRM_ENV = "FORMULA_OWNER_APPROVAL_TOKEN_CONFIRM"
_TRUE = {"1", "true", "yes", "on"}
_APPROVAL_LOCK_NAMESPACE = 94_837_243


class ApprovalRefused(RuntimeError):
    """Raised when any safety, evidence, or confirmation gate is not met."""


def expected_typed_confirmation(formula_id: int) -> str:
    return (
        f"PROMOTE FORMULA {int(formula_id)} TO LIVE; "
        "ALERTS REMAIN OFF"
    )


def _live_alerts_enabled() -> bool:
    return os.getenv("FORMULA_LIVE_ALERTS_ENABLED", "").strip().lower() in _TRUE


def _verify_confirmation(
    formula_id: int,
    *,
    typed_confirmation: Optional[str],
    use_env_token: bool,
) -> str:
    methods_selected = int(typed_confirmation is not None) + int(use_env_token)
    if methods_selected != 1:
        raise ApprovalRefused(
            "choose exactly one confirmation method: --confirm or --use-env-token"
        )
    if typed_confirmation is not None:
        expected = expected_typed_confirmation(formula_id)
        if not hmac.compare_digest(str(typed_confirmation), expected):
            raise ApprovalRefused(
                "typed confirmation did not exactly match: " + expected
            )
        return "EXACT_TYPED"

    expected_token = os.getenv(OWNER_TOKEN_ENV, "")
    supplied_token = os.getenv(OWNER_TOKEN_CONFIRM_ENV, "")
    if len(expected_token) < 32:
        raise ApprovalRefused(
            f"{OWNER_TOKEN_ENV} must contain at least 32 characters"
        )
    if not supplied_token or not hmac.compare_digest(expected_token, supplied_token):
        raise ApprovalRefused(
            f"{OWNER_TOKEN_CONFIRM_ENV} must exactly match {OWNER_TOKEN_ENV}"
        )
    return "ENV_TOKEN"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _assert_current_schema(formula: Mapping[str, Any]) -> None:
    expected = {
        "formula_schema_version": research_formula_engine.FORMULA_SCHEMA_VERSION,
        "feature_schema_version": research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    }
    mismatches = {
        key: {"stored": formula.get(key), "required": required}
        for key, required in expected.items()
        if str(formula.get(key) or "") != str(required)
    }
    if mismatches:
        raise ApprovalRefused(
            "formula runtime schema is incompatible: " + _canonical_json(mismatches)
        )


def _approval_schema_present(conn) -> bool:
    required = {
        "formula_schema_version",
        "feature_schema_version",
        "outcome_method_version",
        "approval_operation_version",
        "confirmation_method",
        "approval_request_fingerprint",
        "delivery_environment_enabled",
    }
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='research_formula_live_approvals'
          AND column_name=ANY(%s)
        """,
        (sorted(required),),
    ).fetchall()
    return required == {str(row["column_name"]) for row in rows}


def _locked_formula(conn, formula_id: int) -> Mapping[str, Any]:
    formula = conn.execute(
        """
        SELECT formula_id, formula_key, formula_version,
               formula_schema_version, engine_version,
               feature_schema_version, outcome_method_version,
               direction, horizon_minutes, conditions, formula_text,
               current_stage, active, live_alert_approved,
               latest_evaluation_run_id, shadow_started_at_utc,
               last_shadow_event_id
        FROM research_formulas
        WHERE formula_id=%s
        FOR UPDATE
        """,
        (int(formula_id),),
    ).fetchone()
    if not formula:
        raise ApprovalRefused(f"formula {int(formula_id)} does not exist")
    if not bool(formula["active"]):
        raise ApprovalRefused("formula is inactive")
    if str(formula["current_stage"]) != "SHADOW":
        raise ApprovalRefused(
            f"formula must currently be SHADOW, not {formula['current_stage']}"
        )
    if bool(formula["live_alert_approved"]):
        raise ApprovalRefused("formula already has live_alert_approved=true")
    _assert_current_schema(formula)
    return formula


def _frozen_validation(
    conn,
    formula: Mapping[str, Any],
    *,
    transaction_time: datetime,
) -> tuple[Dict[str, Any], int, datetime, str]:
    all_source_rows = research_formula_store._shadow_outcome_rows(conn, formula)
    # Freeze only decisions whose full canonical outcome window closed before
    # this transaction.  Newer pending decisions remain future monitoring and
    # cannot influence the immutable approval sample in either direction.
    source_rows = [row for row in all_source_rows if bool(row.get("outcome_due"))]
    validation = research_formula_store._build_shadow_validation(
        formula,
        source_rows,
        evaluated_at_utc=transaction_time,
    )
    if not bool(validation.get("thresholds_met")):
        failed = validation.get("failed_gates") or ["unknown readiness gate"]
        raise ApprovalRefused(
            "current Shadow readiness recheck failed: " + ", ".join(map(str, failed))
        )
    independent = research_formula_store._select_independent_shadow_rows(
        source_rows,
        horizon_minutes=int(formula["horizon_minutes"]),
    )
    complete = [
        row
        for row in independent["rows"]
        if bool(row.get("outcome_available"))
    ]
    if not complete:
        raise ApprovalRefused("readiness produced no completed independent evidence")
    cutoff_event_id = max(int(row["event_id"]) for row in source_rows)
    cutoff_time = max(_as_utc(row["alert_time_utc"]) for row in source_rows)
    frozen_review = {
        "operation_version": OPERATION_VERSION,
        "formula": {
            "formula_id": int(formula["formula_id"]),
            "formula_key": str(formula["formula_key"]),
            "formula_version": int(formula["formula_version"]),
            "formula_schema_version": str(formula["formula_schema_version"]),
            "feature_schema_version": str(formula["feature_schema_version"]),
            "outcome_method_version": str(formula["outcome_method_version"]),
            "direction": str(formula["direction"]),
            "horizon_minutes": int(formula["horizon_minutes"]),
            "conditions": formula["conditions"],
        },
        "cutoff": {
            "event_id": cutoff_event_id,
            "time_utc": cutoff_time,
            "source_check_count": len(source_rows),
            "post_cutoff_pending_event_ids": sorted(
                int(row["event_id"])
                for row in all_source_rows
                if not bool(row.get("outcome_due"))
            ),
            "completed_independent_event_ids": sorted(
                int(row["event_id"]) for row in complete
            ),
        },
        "validation": validation,
    }
    fingerprint = _sha256(frozen_review)
    validation_snapshot = dict(validation)
    validation_snapshot.update(
        {
            "review_kind": "FROZEN_PROSPECTIVE",
            "validation_fingerprint": fingerprint,
            "frozen_review": frozen_review,
            "live_eligible": True,
            "live_blocker": (
                "delivery remains disabled until FORMULA_LIVE_ALERTS_ENABLED=1 "
                "is separately configured"
            ),
        }
    )
    return validation_snapshot, cutoff_event_id, cutoff_time, fingerprint


def promote_formula_to_live(
    formula_id: int,
    *,
    actor: str,
    reason: str,
    typed_confirmation: Optional[str] = None,
    use_env_token: bool = False,
) -> Dict[str, Any]:
    """Recheck current evidence and atomically record owner approval plus LIVE.

    The function refuses to run in a process where the delivery environment is
    enabled.  It never mutates deployment configuration or queues a delivery.
    """
    identifier = int(formula_id)
    normalized_actor = str(actor or "").strip()
    normalized_reason = str(reason or "").strip()
    if identifier <= 0:
        raise ApprovalRefused("formula_id must be positive")
    if not normalized_actor:
        raise ApprovalRefused("actor is required")
    if not normalized_reason:
        raise ApprovalRefused("reason is required")
    if _live_alerts_enabled():
        raise ApprovalRefused(
            "FORMULA_LIVE_ALERTS_ENABLED is already enabled; disable it before "
            "recording approval so activation remains a separate action"
        )
    confirmation_method = _verify_confirmation(
        identifier,
        typed_confirmation=typed_confirmation,
        use_env_token=use_env_token,
    )

    with research_formula_store._connect(read_only=False) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (_APPROVAL_LOCK_NAMESPACE, identifier),
        )
        if not _approval_schema_present(conn):
            raise ApprovalRefused(
                "migration 009_formula_owner_live_approval_v1.sql is not applied"
            )
        formula = _locked_formula(conn, identifier)
        transaction_row = conn.execute(
            "SELECT transaction_timestamp() AS approved_at_utc"
        ).fetchone()
        approved_at = _as_utc(transaction_row["approved_at_utc"])
        validation, cutoff_event_id, cutoff_time, validation_fingerprint = (
            _frozen_validation(conn, formula, transaction_time=approved_at)
        )
        started_at = formula.get("shadow_started_at_utc")
        if started_at is None:
            raise ApprovalRefused("formula has no shadow_started_at_utc")
        validation_started = _as_utc(started_at)
        if validation_started > cutoff_time:
            raise ApprovalRefused("Shadow start is later than the evidence cutoff")

        metrics = _as_mapping(validation.get("metrics"))
        request_fingerprint = _sha256(
            {
                "operation_version": OPERATION_VERSION,
                "formula_id": identifier,
                "formula_version": int(formula["formula_version"]),
                "validation_fingerprint": validation_fingerprint,
                "actor": normalized_actor,
                "reason": normalized_reason,
                "confirmation_method": confirmation_method,
            }
        )
        approval = conn.execute(
            """
            INSERT INTO research_formula_live_approvals (
                formula_id, formula_version, horizon_minutes,
                review_kind, validation_policy_version,
                validation_started_at_utc, validation_cutoff_event_id,
                validation_cutoff_time_utc, validation_fingerprint,
                validated_future_matches, validated_future_controls,
                validated_span_hours, validated_utc_dates,
                thresholds_met, approved_by, approval_reason,
                validation_snapshot, approved_at_utc,
                formula_schema_version, feature_schema_version,
                outcome_method_version, approval_operation_version,
                confirmation_method, approval_request_fingerprint,
                delivery_environment_enabled
            ) VALUES (
                %s, %s, %s,
                'FROZEN_PROSPECTIVE', %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                TRUE, %s, %s,
                %s::jsonb, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                FALSE
            )
            RETURNING approval_id, approved_at_utc
            """,
            (
                identifier,
                int(formula["formula_version"]),
                int(formula["horizon_minutes"]),
                str(validation["policy_version"]),
                validation_started,
                cutoff_event_id,
                cutoff_time,
                validation_fingerprint,
                int(metrics.get("sample_size") or 0),
                int(metrics.get("control_sample_size") or 0),
                float(metrics.get("time_span_hours") or 0.0),
                int(metrics.get("distinct_utc_dates") or 0),
                normalized_actor,
                normalized_reason,
                research_formula_store._json(validation),
                approved_at,
                str(formula["formula_schema_version"]),
                str(formula["feature_schema_version"]),
                str(formula["outcome_method_version"]),
                OPERATION_VERSION,
                confirmation_method,
                request_fingerprint,
            ),
        ).fetchone()

        run_id = formula.get("latest_evaluation_run_id")
        audit_reason = (
            f"explicit owner approval #{int(approval['approval_id'])}: "
            f"{normalized_reason}"
        )
        conn.execute(
            """
            INSERT INTO research_formula_stage_history (
                formula_id, run_id, from_stage, to_stage, reason, actor,
                changed_at_utc
            ) VALUES (%s, %s, 'SHADOW', 'APPROVED', %s, %s, %s)
            """,
            (identifier, run_id, audit_reason, normalized_actor, approved_at),
        )
        conn.execute(
            """
            INSERT INTO research_formula_stage_history (
                formula_id, run_id, from_stage, to_stage, reason, actor,
                changed_at_utc
            ) VALUES (%s, %s, 'APPROVED', 'LIVE', %s, %s, %s)
            """,
            (identifier, run_id, audit_reason, normalized_actor, approved_at),
        )
        updated = conn.execute(
            """
            UPDATE research_formulas
            SET current_stage='LIVE',
                live_alert_approved=TRUE,
                live_alert_approved_at_utc=%s,
                live_alert_approved_by=%s,
                live_alert_policy_version=%s,
                shadow_validation_metrics=%s::jsonb,
                updated_at_utc=%s
            WHERE formula_id=%s
              AND current_stage='SHADOW'
              AND active=TRUE
            RETURNING formula_id, formula_version, current_stage,
                      live_alert_approved, live_alert_approved_at_utc
            """,
            (
                approved_at,
                normalized_actor,
                str(validation["policy_version"]),
                research_formula_store._json(validation),
                approved_at,
                identifier,
            ),
        ).fetchone()
        if not updated:
            raise ApprovalRefused("formula stage changed during the approval transaction")
        conn.commit()

    return research_formula_store._json_safe(
        {
            "formula_id": identifier,
            "formula_version": int(formula["formula_version"]),
            "stage": "LIVE",
            "approval_id": int(approval["approval_id"]),
            "approved_at_utc": approval["approved_at_utc"],
            "approved_by": normalized_actor,
            "validation_fingerprint": validation_fingerprint,
            "readiness_rechecked": True,
            "thresholds_met": True,
            "confirmation_method": confirmation_method,
            "live_alerts_enabled_by_operation": False,
            "live_alerts_environment_in_this_process": False,
            "delivery_note": (
                "No delivery was queued. Enable FORMULA_LIVE_ALERTS_ENABLED=1 "
                "separately only after an additional explicit deployment decision."
            ),
        }
    )


def formula_approval_status(formula_id: int) -> Dict[str, Any]:
    """Return a read-only approval and delivery-gate report for one formula."""
    identifier = int(formula_id)
    with research_formula_store._connect(read_only=True) as conn:
        migration_ready = _approval_schema_present(conn)
        formula = conn.execute(
            """
            SELECT formula_id, formula_version, formula_schema_version,
                   feature_schema_version, outcome_method_version,
                   direction, horizon_minutes, formula_text, current_stage,
                   active, live_alert_approved, live_alert_approved_at_utc,
                   live_alert_approved_by, shadow_started_at_utc,
                   shadow_validation_metrics
            FROM research_formulas
            WHERE formula_id=%s
            """,
            (identifier,),
        ).fetchone()
        if not formula:
            raise ApprovalRefused(f"formula {identifier} does not exist")
        approval = conn.execute(
            """
            SELECT approval_id, approved_at_utc, approved_by,
                   approval_reason, validation_policy_version,
                   validation_fingerprint, validated_future_matches,
                   validated_future_controls, validated_span_hours,
                   validated_utc_dates, thresholds_met
            FROM research_formula_live_approvals
            WHERE formula_id=%s AND formula_version=%s
            ORDER BY approved_at_utc DESC
            LIMIT 1
            """,
            (identifier, int(formula["formula_version"])),
        ).fetchone()
    compatibility_error = None
    try:
        _assert_current_schema(formula)
    except ApprovalRefused as exc:
        compatibility_error = str(exc)
    validation = _as_mapping(formula.get("shadow_validation_metrics"))
    return research_formula_store._json_safe(
        {
            "formula": dict(formula),
            "migration_009_ready": migration_ready,
            "runtime_schema_compatible": compatibility_error is None,
            "schema_blocker": compatibility_error,
            "last_readiness_evaluation": {
                "evaluated_at_utc": validation.get("evaluated_at_utc"),
                "thresholds_met": validation.get("thresholds_met"),
                "failed_gates": validation.get("failed_gates") or [],
                "metrics": validation.get("metrics") or {},
            },
            "approval": dict(approval) if approval else None,
            "delivery_gate": {
                "formula_is_live": str(formula["current_stage"]) == "LIVE",
                "immutable_approval_present": approval is not None,
                "environment_enabled_in_this_process": _live_alerts_enabled(),
                "operation_changes_environment": False,
            },
        }
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read Formula approval status or explicitly approve one LIVE formula"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status", help="read-only formula approval report")
    status.add_argument("--formula-id", type=int, required=True)

    promote = subparsers.add_parser(
        "promote-live",
        help="transactionally recheck and approve one SHADOW formula",
    )
    promote.add_argument("--formula-id", type=int, required=True)
    promote.add_argument("--actor", required=True)
    promote.add_argument("--reason", required=True)
    confirmation = promote.add_mutually_exclusive_group(required=True)
    confirmation.add_argument("--confirm")
    confirmation.add_argument("--use-env-token", action="store_true")
    return parser


def _main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            result = formula_approval_status(args.formula_id)
        else:
            result = promote_formula_to_live(
                args.formula_id,
                actor=args.actor,
                reason=args.reason,
                typed_confirmation=args.confirm,
                use_env_token=bool(args.use_env_token),
            )
    except ApprovalRefused as exc:
        print(_canonical_json({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
