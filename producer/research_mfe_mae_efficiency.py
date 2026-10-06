"""Finite, unbounded and invalid MFE/MAE efficiency semantics.

The research pipeline persists JSON/JSONB evidence, so an economically
unbounded positive-MFE / zero-MAE observation must never be represented by an
IEEE ``Infinity``.  This module keeps the numeric ratio nullable and carries a
separate explicit state through discovery, Shadow validation, ranking and
presentation.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping, Optional


FINITE = "FINITE"
UNBOUNDED_ZERO_MAE = "UNBOUNDED_ZERO_MAE"
UNDEFINED_ZERO_ZERO = "UNDEFINED_ZERO_ZERO"
INVALID_OR_MISSING = "INVALID_OR_MISSING"
POLICY_VERSION = "mfe-mae-efficiency-v2-zero-mae-unbounded"


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


@dataclass(frozen=True)
class MfeMaeEfficiency:
    """JSON-safe MFE/MAE classification.

    ``ratio`` is populated only for a finite ratio.  Unbounded, undefined and
    invalid observations deliberately use ``None`` so callers cannot leak
    Infinity or NaN into PostgreSQL, JSON or Telegram payloads.
    """

    state: str
    ratio: Optional[float]

    def meets_threshold(self, minimum_ratio: Any) -> bool:
        threshold = _finite_number(minimum_ratio)
        if threshold is None or threshold < 0.0:
            return False
        if self.state == UNBOUNDED_ZERO_MAE:
            return True
        return (
            self.state == FINITE
            and self.ratio is not None
            and self.ratio >= threshold
        )

    def capped_quality(self, full_score_ratio: Any) -> float:
        """Return a finite score in ``[0, 1]`` for a capped ranking term."""
        cap = _finite_number(full_score_ratio)
        if cap is None or cap <= 0.0:
            return 0.0
        if self.state == UNBOUNDED_ZERO_MAE:
            return 1.0
        if self.state != FINITE or self.ratio is None:
            return 0.0
        return max(0.0, min(1.0, self.ratio / cap))

    def evidence(self) -> dict[str, Any]:
        return {"ratio": self.ratio, "state": self.state}


def classify(mfe: Any, mae: Any) -> MfeMaeEfficiency:
    """Classify median favorable/adverse excursion without synthetic caps."""
    favorable = _finite_number(mfe)
    adverse = _finite_number(mae)
    if favorable is None or adverse is None:
        return MfeMaeEfficiency(INVALID_OR_MISSING, None)
    if favorable < 0.0 or adverse < 0.0:
        return MfeMaeEfficiency(INVALID_OR_MISSING, None)
    if favorable == 0.0 and adverse == 0.0:
        return MfeMaeEfficiency(UNDEFINED_ZERO_ZERO, None)
    if adverse == 0.0:
        return MfeMaeEfficiency(UNBOUNDED_ZERO_MAE, None)
    ratio = favorable / adverse
    if not math.isfinite(ratio):
        return MfeMaeEfficiency(INVALID_OR_MISSING, None)
    return MfeMaeEfficiency(FINITE, ratio)


def from_metrics(
    metrics: Mapping[str, Any],
    *,
    mfe_key: str = "median_mfe_pct",
    mae_key: str = "median_mae_pct",
) -> MfeMaeEfficiency:
    """Derive authoritative semantics from the underlying stored medians.

    The numeric ratio and prior state fields are intentionally not trusted:
    recomputing from MFE and MAE prevents stale pre-policy metrics from being
    interpreted with the new zero-MAE semantics.
    """
    if not isinstance(metrics, Mapping):
        return MfeMaeEfficiency(INVALID_OR_MISSING, None)
    return classify(metrics.get(mfe_key), metrics.get(mae_key))
