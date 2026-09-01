"""Isolated Render Workflow entry point for the staging DB preflight.

This module is not imported by the candidate web service.  A separately
created, manually triggered Render Workflow may register its single task and
invoke the existing fail-closed, read-only preflight runner.
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from render import Retry, TaskContext, Workflows

import research_preview_staging_readonly_preflight as preflight


WORKFLOW_NAME = "preview-staging-readonly-preflight"
TASK_NAME = "preview_staging_readonly_preflight_once"
TASK_PLAN = "flex"
TASK_TIMEOUT_SECONDS = 30

app = Workflows()


def execute_preflight(environment: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the runner's safe result, including fail-closed failures."""

    try:
        return preflight.run_preflight(environment)
    except Exception as exc:
        return preflight._failed_closed(exc)


@app.task(
    name=TASK_NAME,
    retry=Retry(max_retries=0, wait_duration_ms=1000),
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    plan=TASK_PLAN,
)
def preview_staging_readonly_preflight_once(
    ctx: TaskContext,
) -> dict[str, Any]:
    """Execute one non-retrying, read-only staging preflight."""

    del ctx
    return execute_preflight(os.environ)


if __name__ == "__main__":
    app.start()
