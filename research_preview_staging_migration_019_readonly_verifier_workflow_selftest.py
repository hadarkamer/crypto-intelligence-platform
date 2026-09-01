"""Regressions for the migration-019 read-only verifier Workflow entry."""

from __future__ import annotations

import inspect
from pathlib import Path

import research_preview_staging_migration_019_readonly_verifier as verifier
import research_preview_staging_migration_019_readonly_verifier_workflow as workflow


ROOT = Path(__file__).resolve().parent


def run() -> None:
    assert workflow.WORKFLOW_NAME == (
        "preview-staging-migration-019-readonly-verifier"
    )
    assert workflow.TASK_NAME == (
        "preview_staging_migration_019_readonly_verify_once"
    )
    assert workflow.TASK_PLAN == "flex"
    assert workflow.TASK_TIMEOUT_SECONDS == 30
    assert workflow.TASK_MAX_RETRIES == 0
    assert workflow.TASK_RETRY_WAIT_MS == 1000
    assert workflow.TASK_RETRY_BACKOFF_SCALING == 1.0

    task_names = workflow.app._registry.get_task_names()
    assert task_names == [workflow.TASK_NAME]
    task_info = workflow.app._registry.get_task(workflow.TASK_NAME)
    assert task_info is not None
    assert task_info.options.plan == workflow.TASK_PLAN
    assert task_info.options.timeout_seconds == workflow.TASK_TIMEOUT_SECONDS
    assert task_info.options.retry is not None
    assert task_info.options.retry.max_retries == 0
    assert task_info.options.retry.wait_duration_ms == 1000
    assert task_info.options.retry.backoff_scaling == 1.0
    assert list(inspect.signature(task_info.func).parameters) == ["ctx"]

    failed = workflow.execute_verifier({})
    assert failed["status"] == "POSTCOMMIT_VERIFY_FAILED_CLOSED"
    assert failed["database_url_exposed"] is False
    assert failed["migration_019_applied"] is None
    assert failed["manual_reconciliation_resolved"] is False
    assert failed["manual_intervention_required"] is True
    assert failed["schema_mutation_allowed"] is False
    assert failed["migration_apply_allowed"] is False
    assert failed["automatic_retry_allowed"] is False
    assert failed["database_writes"] == 0
    assert failed["telegram_api_calls"] == 0
    assert failed["delivery_allowed"] is False

    captured = {}
    original = verifier.run_verifier

    def fake_run(environment):
        captured["environment"] = environment
        return {
            "status": "APPLIED_VERIFIED",
            "migration_019_applied": True,
            "database_writes": 0,
            "automatic_retry_allowed": False,
        }

    try:
        verifier.run_verifier = fake_run
        result = workflow.preview_staging_migration_019_readonly_verify_once.func(None)
    finally:
        verifier.run_verifier = original

    assert captured["environment"] is workflow.os.environ
    assert result == {
        "status": "APPLIED_VERIFIED",
        "migration_019_applied": True,
        "database_writes": 0,
        "automatic_retry_allowed": False,
    }

    source = (
        ROOT
        / "research_preview_staging_migration_019_readonly_verifier_workflow.py"
    ).read_text(encoding="utf-8")
    lower_source = source.lower()
    for forbidden in (
        "import ai_candidate_main",
        "import main",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        "psycopg",
        "database_url",
        "formulapreview",
        "formula_preview_staging_database_url",
        "formula_preview_staging_migration_019_verify",
        "render(",
        "renderasync(",
        "start_task(",
        "run_task(",
        ".commit(",
        "scheduler",
        "cron",
    ):
        assert forbidden not in lower_source
    assert "verifier.run_verifier(environment)" in source
    assert "verifier._failed_closed(exc)" in source
    assert "max_retries=TASK_MAX_RETRIES" in source
    assert "return execute_verifier(os.environ)" in source

    for runtime_path in ("ai_candidate_main.py", "main.py"):
        runtime_source = (ROOT / runtime_path).read_text(encoding="utf-8")
        assert "migration_019_readonly_verifier_workflow" not in runtime_source

    requirements = (
        ROOT / "requirements.preview-staging-workflow.txt"
    ).read_text(encoding="utf-8").splitlines()
    assert requirements == [
        "psycopg[binary]==3.2.3",
        "render==1.0.1",
    ]

    print(
        "research_preview_staging_migration_019_readonly_verifier_workflow_selftest: ok"
    )


if __name__ == "__main__":
    run()
