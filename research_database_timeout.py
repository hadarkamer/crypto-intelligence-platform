"""Shared, bounded PostgreSQL timeout for heavy research queries."""

from __future__ import annotations

import os
from typing import Mapping, Optional


ENV_HEAVY_STATEMENT_TIMEOUT_MS = "RESEARCH_HEAVY_STATEMENT_TIMEOUT_MS"
DEFAULT_HEAVY_STATEMENT_TIMEOUT_MS = 120_000
MIN_HEAVY_STATEMENT_TIMEOUT_MS = 30_000
MAX_HEAVY_STATEMENT_TIMEOUT_MS = 300_000


def heavy_statement_timeout_ms(
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Return the configured finite timeout, clamped to the safe range."""

    source = os.environ if environ is None else environ
    raw = str(source.get(ENV_HEAVY_STATEMENT_TIMEOUT_MS, "") or "").strip()
    try:
        configured = int(raw) if raw else DEFAULT_HEAVY_STATEMENT_TIMEOUT_MS
    except (TypeError, ValueError):
        configured = DEFAULT_HEAVY_STATEMENT_TIMEOUT_MS
    return max(
        MIN_HEAVY_STATEMENT_TIMEOUT_MS,
        min(MAX_HEAVY_STATEMENT_TIMEOUT_MS, configured),
    )


def heavy_timeout_status(
    environ: Optional[Mapping[str, str]] = None,
) -> dict[str, object]:
    """Describe the effective guardrail without exposing environment values."""

    return {
        "environment_variable": ENV_HEAVY_STATEMENT_TIMEOUT_MS,
        "statement_timeout_ms": heavy_statement_timeout_ms(environ),
        "default_ms": DEFAULT_HEAVY_STATEMENT_TIMEOUT_MS,
        "minimum_ms": MIN_HEAVY_STATEMENT_TIMEOUT_MS,
        "maximum_ms": MAX_HEAVY_STATEMENT_TIMEOUT_MS,
        "finite": True,
    }
