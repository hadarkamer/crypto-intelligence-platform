"""Regressions for the isolated migration-019 Render Workflow entry point."""

from __future__ import annotations

import inspect
from pathlib import Path

import research_preview_staging_migration_019_admin as installer
import research_preview_staging_migration_019_workflow as workflow


ROOT = Path(__file__).resolve().parent


def run() -> None:
    assert workflow.WORKFLOW_NAME == "preview-staging-migration-019"
    assert workflow.TASK_NAME == "preview_staging_migration_019_once"
    assert workflow.TASK_PLAN == "flex"
    assert workflow.TASK_TIMEOUT_SECONDS == 60
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

    failed = workflow.execute_migration({})
    assert failed["status"] == "MIGRATION_019_FAILED_CLOSED"
    assert failed["database_url_exposed"] is False
    assert failed["migration_019_applied"] is False
    assert failed["manual_read_only_reconciliation_required"] is False
    assert failed["automatic_retry_allowed"] is False
    assert failed["candidate_service_connected"] is False
    assert failed["runtime_database_registered"] is False
    assert failed["application_rows_written"] == 0
    assert failed["telegram_api_calls"] == 0
    assert failed["delivery_allowed"] is False

    captured = {}
    original = installer.run_installer

    def fake_run(environment):
        captured["environment"] = environment
        return {
            "status": "MIGRATION_019_APPLIED_AND_VERIFIED",
            "migration_019_applied": True,
            "automatic_retry_allowed": False,
            "application_rows_written": 0,
        }

    try:
        installer.run_installer = fake_run
        result = workflow.preview_staging_migration_019_once.func(None)
    finally:
        installer.run_installer = original

    assert captured["environment"] is workflow.os.environ
    assert result == {
        "status": "MIGRATION_019_APPLIED_AND_VERIFIED",
        "migration_019_applied": True,
        "automatic_retry_allowed": False,
        "application_rows_written": 0,
    }

    def uncertain_run(environment):
        del environment
        raise installer.Migration019CommitOutcomeUncertain(
            "synthetic uncertain commit"
        )

    try:
        installer.run_installer = uncertain_run
        uncertain = workflow.preview_staging_migration_019_once.func(None)
    finally:
        installer.run_installer = original

    assert uncertain["status"] == "MIGRATION_019_COMMIT_OUTCOME_UNCERTAIN"
    assert uncertain["migration_019_applied"] is None
    assert uncertain["manual_read_only_reconciliation_required"] is True
    assert uncertain["automatic_retry_allowed"] is False
    assert uncertain["database_url_exposed"] is False

    source = (
        ROOT / "research_preview_staging_migration_019_workflow.py"
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
        "formula_preview_staging_migration_019_apply",
        "render(",
        "renderasync(",
        "start_task(",
        "run_task(",
        ".commit(",
        "scheduler",
        "cron",
    ):
        assert forbidden not in lower_source
    assert "installer.run_installer(environment)" in source
    assert "installer._failed_closed(exc)" in source
    assert "max_retries=TASK_MAX_RETRIES" in source
    assert "return execute_migration(os.environ)" in source

    for runtime_path in ("ai_candidate_main.py", "main.py"):
        runtime_source = (ROOT / runtime_path).read_text(encoding="utf-8")
        assert "research_preview_staging_migration_019_workflow" not in runtime_source

    requirements = (
        ROOT / "requirements.preview-staging-workflow.txt"
    ).read_text(encoding="utf-8").splitlines()
    assert requirements == [
        "psycopg[binary]==3.2.3",
        "render==1.0.1",
    ]

    ci_workflow = (
        ROOT / ".github" / "workflows" / "production-ai-analytics-check.yml"
    ).read_text(encoding="utf-8")
    assert "git ls-files '*_workflow_selftest.py'" in ci_workflow
    assert "requirements.preview-staging-workflow.txt" in ci_workflow

    print("research_preview_staging_migration_019_workflow_selftest: ok")


if __name__ == "__main__":
    run()
