"""Predeclared ordered-v7 research acceptance policy for new v2 scopes.

This policy labels research relevance only.  It cannot approve alerts, execute
trades, or retroactively qualify a scope frozen before this version existed.
"""
from __future__ import annotations

from typing import Any, Mapping

import research_ordered_question_catalog as questions
import research_ordered_validation as validation


VERSION = "ordered-v7-common-window-research-acceptance-v1-20260907"
DOCUMENTED_SOURCE = "docs/ORDERED_V7_ACCEPTANCE_POLICY_V1.md"
# Fixed predeclared ceiling: <=300 candidate definitions, eight supported coins
# plus ALL, two directions, four horizons, eight thresholds and two periods.
FAMILY_CAPACITY = 300 * 9 * 2 * 4 * 8 * 2


def policy_for(exact_binding: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    if candidate.get("catalog_version") != questions.VERSION:
        return None
    binding_sha = validation.digest(exact_binding)
    return {
        "policy_version": VERSION + ":" + binding_sha[:20],
        "documented_source": DOCUMENTED_SOURCE,
        "rationale": (
            "Prospectively require both directional probability and full-common-window asymmetry; "
            "small samples must clear Wilson uncertainty and no threshold was selected from this scope's outcomes."
        ),
        "binding_sha256": binding_sha,
        "combination": "PROBABILITY_AND_ASYMMETRY",
        "multiplicity": {
            "family_id": questions.VERSION + ":fixed-top8-all-grid",
            "method": "PRESPECIFIED_FIXED_FAMILY_DISCLOSURE",
            "registered_attempts": FAMILY_CAPACITY,
            "justification": (
                "Conservative fixed upper envelope disclosed before prospective results: 300 definitions x "
                "(8 coins + ALL) x 2 directions x 4 horizons x 8 thresholds x 2 period views. "
                "Actual attempted scopes are reported separately and overlapping scopes are never independent evidence."
            ),
        },
        "routes": {
            "PROBABILITY": [
                {"metric": "hit_rate_pct", "operator": ">=", "value": 70},
                {"metric": "wilson_95_lower_pct", "operator": ">=", "value": 40},
            ],
            "ASYMMETRY": [
                {"metric": "common_window_asymmetry_ratio", "operator": ">=", "value": 1.5},
                {"metric": "common_window_favorable_dominance_pct", "operator": ">=", "value": 60},
                {"metric": "common_window_median_paired_edge_pct", "operator": ">", "value": 0},
            ],
        },
    }
