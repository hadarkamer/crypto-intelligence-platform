"""Deterministic safety checks for the explicit Formula owner approval CLI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

import research_feature_matrix
import research_formula_approval as approval
import research_formula_engine
import research_formula_store


class _Result:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConnection:
    def __init__(self, approved_at: datetime):
        self.approved_at = approved_at
        self.sql: list[str] = []
        self.insert_params = None
        self.stage_changes = 0
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.sql.append(normalized)
        if "transaction_timestamp()" in normalized:
            return _Result({"approved_at_utc": self.approved_at})
        if "INSERT INTO research_formula_live_approvals" in normalized:
            self.insert_params = params
            return _Result({"approval_id": 77, "approved_at_utc": self.approved_at})
        if "INSERT INTO research_formula_stage_history" in normalized:
            self.stage_changes += 1
            return _Result()
        if "UPDATE research_formulas" in normalized:
            return _Result(
                {
                    "formula_id": 42,
                    "formula_version": 1,
                    "current_stage": "LIVE",
                    "live_alert_approved": True,
                    "live_alert_approved_at_utc": self.approved_at,
                }
            )
        return _Result()

    def commit(self):
        self.committed = True


def _expect_refusal(callable_, fragment: str) -> None:
    try:
        callable_()
    except approval.ApprovalRefused as exc:
        assert fragment in str(exc), str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("approval unexpectedly succeeded")


def run() -> None:
    formula_id = 42
    exact = approval.expected_typed_confirmation(formula_id)
    assert exact == "PROMOTE FORMULA 42 TO LIVE; ALERTS REMAIN OFF"
    assert approval._verify_confirmation(
        formula_id,
        typed_confirmation=exact,
        use_env_token=False,
    ) == "EXACT_TYPED"
    _expect_refusal(
        lambda: approval._verify_confirmation(
            formula_id,
            typed_confirmation="PROMOTE FORMULA 42 TO LIVE",
            use_env_token=False,
        ),
        "did not exactly match",
    )
    _expect_refusal(
        lambda: approval._verify_confirmation(
            formula_id,
            typed_confirmation=exact,
            use_env_token=True,
        ),
        "exactly one confirmation method",
    )

    token = "x" * 32
    old_token = os.environ.get(approval.OWNER_TOKEN_ENV)
    old_confirm = os.environ.get(approval.OWNER_TOKEN_CONFIRM_ENV)
    try:
        os.environ[approval.OWNER_TOKEN_ENV] = token
        os.environ[approval.OWNER_TOKEN_CONFIRM_ENV] = token
        assert approval._verify_confirmation(
            formula_id,
            typed_confirmation=None,
            use_env_token=True,
        ) == "ENV_TOKEN"
        os.environ[approval.OWNER_TOKEN_CONFIRM_ENV] = "wrong"
        _expect_refusal(
            lambda: approval._verify_confirmation(
                formula_id,
                typed_confirmation=None,
                use_env_token=True,
            ),
            "must exactly match",
        )
    finally:
        for name, value in (
            (approval.OWNER_TOKEN_ENV, old_token),
            (approval.OWNER_TOKEN_CONFIRM_ENV, old_confirm),
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    compatible = {
        "formula_schema_version": research_formula_engine.FORMULA_SCHEMA_VERSION,
        "feature_schema_version": research_feature_matrix.FEATURE_SCHEMA_VERSION,
        "outcome_method_version": research_feature_matrix.VERIFIED_OUTCOME_METHOD,
    }
    approval._assert_current_schema(compatible)
    _expect_refusal(
        lambda: approval._assert_current_schema(
            {**compatible, "formula_schema_version": "obsolete"}
        ),
        "incompatible",
    )

    now = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)
    fake = _FakeConnection(now)
    formula = {
        "formula_id": formula_id,
        "formula_key": "a" * 64,
        "formula_version": 1,
        **compatible,
        "engine_version": research_formula_engine.ENGINE_VERSION,
        "direction": "LONG",
        "horizon_minutes": 60,
        "conditions": [{"feature": "raw.price", "operator": ">=", "value": 1}],
        "formula_text": "LONG WHEN raw.price >= 1",
        "current_stage": "SHADOW",
        "active": True,
        "live_alert_approved": False,
        "latest_evaluation_run_id": 9,
        "shadow_started_at_utc": now - timedelta(hours=80),
        "last_shadow_event_id": 501,
    }
    validation = {
        "policy_version": "selftest-policy",
        "thresholds_met": True,
        "failed_gates": [],
        "metrics": {
            "sample_size": 12,
            "control_sample_size": 12,
            "time_span_hours": 80.0,
            "distinct_utc_dates": 4,
        },
    }

    originals = {
        "connect": research_formula_store._connect,
        "schema": approval._approval_schema_present,
        "locked": approval._locked_formula,
        "frozen": approval._frozen_validation,
        "alerts": approval._live_alerts_enabled,
    }
    frozen_calls: list[int] = []
    try:
        research_formula_store._connect = lambda **_: fake
        approval._approval_schema_present = lambda conn: True
        approval._locked_formula = lambda conn, identifier: formula

        def frozen(conn, row, *, transaction_time):
            frozen_calls.append(int(row["formula_id"]))
            return validation, 501, now - timedelta(minutes=1), "f" * 64

        approval._frozen_validation = frozen
        approval._live_alerts_enabled = lambda: False
        result = approval.promote_formula_to_live(
            formula_id,
            actor="owner@example.test",
            reason="frozen prospective evidence reviewed",
            typed_confirmation=exact,
        )
    finally:
        research_formula_store._connect = originals["connect"]
        approval._approval_schema_present = originals["schema"]
        approval._locked_formula = originals["locked"]
        approval._frozen_validation = originals["frozen"]
        approval._live_alerts_enabled = originals["alerts"]

    assert frozen_calls == [formula_id], "current readiness was not rechecked"
    assert fake.committed and fake.stage_changes == 2
    assert fake.insert_params is not None
    assert not any("live_deliveries" in sql for sql in fake.sql)
    assert result["stage"] == "LIVE"
    assert result["live_alerts_enabled_by_operation"] is False
    assert result["readiness_rechecked"] is True

    approval._live_alerts_enabled = lambda: True
    try:
        _expect_refusal(
            lambda: approval.promote_formula_to_live(
                formula_id,
                actor="owner@example.test",
                reason="should be blocked",
                typed_confirmation=exact,
            ),
            "already enabled",
        )
    finally:
        approval._live_alerts_enabled = originals["alerts"]

    print("research formula owner approval self-test: PASS")


if __name__ == "__main__":
    run()
