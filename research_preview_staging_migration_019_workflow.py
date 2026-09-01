"""Isolated Render Workflow entry point for staging migration 019.

This module only registers one manually triggered, non-retrying task around the
fail-closed migration-019 installer. It is not imported by the candidate
service and does not configure a database URL, trigger a run, or expose a
Render client.
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from render import Retry, TaskContext, Workflows

import research_preview_staging_migration_019_admin as installer


WORKFLOW_NAME = "preview-staging-migration-019"
TASK_NAME = "preview_staging_migration_019_once"
TASK_PLAN = "flex"
TASK_TIMEOUT_SECONDS = 60
TASK_MAX_RETRIES = 0
TASK_RETRY_WAIT_MS = 1000
TASK_RETRY_BACKOFF_SCALING = 1.0

app = Workflows()


def execute_migration(environment: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the installer's non-secret success or fail-closed result."""

    try:
        return installer.run_installer(environment)
    except Exception as exc:
        return installer._failed_closed(exc)


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
def preview_staging_migration_019_once(
    ctx: TaskContext,
) -> dict[str, Any]:
    """Execute one attempt; uncertain or failed outcomes are never retried."""

    del ctx
    return execute_migration(os.environ)


if __name__ == "__main__":
    app.start()
