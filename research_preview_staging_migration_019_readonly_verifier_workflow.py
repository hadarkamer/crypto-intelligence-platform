"""Isolated Workflow entry point for migration-019 read-only reconciliation.

This module registers one manually triggered, non-retrying task around the
post-commit verifier. It is not imported by the candidate service and does not
configure a database URL, start a run or expose a Render client.
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from render import Retry, TaskContext, Workflows

import research_preview_staging_migration_019_readonly_verifier as verifier


WORKFLOW_NAME = "preview-staging-migration-019-readonly-verifier"
TASK_NAME = "preview_staging_migration_019_readonly_verify_once"
TASK_PLAN = "flex"
TASK_TIMEOUT_SECONDS = 30
TASK_MAX_RETRIES = 0
TASK_RETRY_WAIT_MS = 1000
TASK_RETRY_BACKOFF_SCALING = 1.0

app = Workflows()


def execute_verifier(environment: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the verifier's non-secret classification or safe failure."""

    try:
        return verifier.run_verifier(environment)
    except Exception as exc:
        return verifier._failed_closed(exc)


@app.task(
    name=TASK_NAME,
    retry=Retry(
        max_retries=TASK_MAX_RETRIES,
        wait_duration_ms=TASK_RETRY_WAIT_MS,
        backoff_scaling=TASK_RETRY_BACKOFF_SCALING,
    ),
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    plan=TASK_PLAN,
)
def preview_staging_migration_019_readonly_verify_once(
    ctx: TaskContext,
) -> dict[str, Any]:
    """Execute one read-only snapshot; never retry automatically."""

    del ctx
    return execute_verifier(os.environ)


if __name__ == "__main__":
    app.start()
