"""Regressions for the isolated Render Workflow preflight entry point."""

from __future__ import annotations

import inspect
from pathlib import Path

import research_preview_staging_readonly_preflight as preflight
import research_preview_staging_readonly_preflight_workflow as workflow


ROOT = Path(__file__).resolve().parent


def run() -> None:
    assert workflow.WORKFLOW_NAME == "preview-staging-readonly-preflight"
    assert workflow.TASK_NAME == "preview_staging_readonly_preflight_once"
    assert workflow.TASK_PLAN == "flex"
    assert workflow.TASK_TIMEOUT_SECONDS == 30

    task_names = workflow.app._registry.get_task_names()
    assert task_names == [workflow.TASK_NAME]
    task_info = workflow.app._registry.get_task(workflow.TASK_NAME)
    assert task_info is not None
    assert task_info.options.plan == "flex"
    assert task_info.options.timeout_seconds == 30
    assert task_info.options.retry is not None
    assert task_info.options.retry.max_retries == 0
    assert task_info.options.retry.wait_duration_ms == 1000
    assert list(inspect.signature(task_info.func).parameters) == ["ctx"]

    failed = workflow.execute_preflight({})
    assert failed["status"] == "PREFLIGHT_FAILED_CLOSED"
    assert failed["database_url_exposed"] is False
    assert failed["schema_mutation_allowed"] is False
    assert failed["migration_apply_allowed"] is False
    assert failed["database_writes"] == 0
    assert failed["telegram_api_calls"] == 0
    assert failed["delivery_allowed"] is False

    captured = {}
    original = preflight.run_preflight

    def fake_run(environment):
        captured["environment"] = environment
        return {
            "status": "READY_FOR_SEPARATE_MIGRATION_019_DECISION",
            "database_writes": 0,
        }

    try:
        preflight.run_preflight = fake_run
        result = workflow.preview_staging_readonly_preflight_once.func(None)
    finally:
        preflight.run_preflight = original

    assert captured["environment"] is workflow.os.environ
    assert result == {
        "status": "READY_FOR_SEPARATE_MIGRATION_019_DECISION",
        "database_writes": 0,
    }

    source = (
        ROOT / "research_preview_staging_readonly_preflight_workflow.py"
    ).read_text(encoding="utf-8")
    lower_source = source.lower()
    for forbidden in (
        "import ai_candidate_main",
        "import telegram",
        "from telegram",
        "send_message(",
        "reply_text(",
        ".commit(",
        "research_formula_schema_admin",
        "database_url",
    ):
        assert forbidden not in lower_source

    requirements = (
        ROOT / "requirements.preview-staging-workflow.txt"
    ).read_text(encoding="utf-8").splitlines()
    assert requirements == [
        "psycopg[binary]==3.2.3",
        "render==1.0.1",
    ]

    print("research_preview_staging_readonly_preflight_workflow_selftest: ok")


if __name__ == "__main__":
    run()
