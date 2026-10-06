"""Deterministic checks for correlated features and evidence families."""

from __future__ import annotations

import research_formula_families as families


def _formula(
    key: str,
    evidence: list[str],
    *,
    score: float,
    symbols: int = 1,
) -> dict:
    return {
        "formula_key": key,
        "direction": "LONG",
        "horizon_minutes": 240,
        "condition_count": 3,
        "conditions": [
            {"feature": f"model.synthetic.{key}", "operator": ">=", "value": 1.0}
        ],
        "recommended_stage": "BACKTESTED",
        "ranking_score": score,
        "holdout_metrics": {"sample_size": len(evidence), "distinct_symbols": symbols},
        "multiple_testing": {"q_value": 0.01},
        "_evidence_keys": evidence,
    }


def run() -> None:
    repeated_price = [
        {"feature": "raw.60m.price_change_pct", "operator": ">=", "value": 1.0},
        {
            "feature": "historical.60m.price_change_pct_percentile_session_matched",
            "operator": ">=",
            "value": 80.0,
        },
    ]
    rejected = families.condition_family_policy(repeated_price)
    assert rejected["valid"] is False
    assert rejected["families"] == ["price", "price"]
    assert families.CONDITION_FAMILY_POLICY_VERSION == (
        "formula-condition-family-policy-v1-all-depth-fail-closed"
    )
    repeated_price_triple = [
        *repeated_price,
        {
            "feature": "aligned.240m.price_change_pct",
            "operator": ">=",
            "value": 2.0,
        },
    ]
    rejected_price_triple = families.condition_family_policy(
        repeated_price_triple
    )
    assert rejected_price_triple["valid"] is False
    assert rejected_price_triple["families"] == ["price", "price", "price"]
    justified = families.condition_family_policy(
        repeated_price,
        justified_exceptions=(
            "price: independent multi-window confirmation retained for audit",
        ),
    )
    assert justified["valid"] is True

    repeated_open_interest = [
        {"feature": "raw.60m.oi_change_pct", "operator": ">=", "value": 1.0},
        {
            "feature": "historical.240m.oi_change_pct_median_session_matched",
            "operator": ">=",
            "value": 2.0,
        },
        {
            "feature": "historical.720m.oi_change_pct_percentile_session_matched",
            "operator": ">=",
            "value": 80.0,
        },
    ]
    rejected_open_interest_pair = families.condition_family_policy(
        repeated_open_interest[:2]
    )
    assert rejected_open_interest_pair["valid"] is False
    assert rejected_open_interest_pair["families"] == [
        "open_interest",
        "open_interest",
    ]
    rejected_open_interest_triple = families.condition_family_policy(
        repeated_open_interest
    )
    assert rejected_open_interest_triple["valid"] is False
    assert rejected_open_interest_triple["families"] == [
        "open_interest",
        "open_interest",
        "open_interest",
    ]
    independent_families = [
        repeated_price[0],
        repeated_open_interest[0],
        {
            "feature": "historical.60m.futures_cvd_change_pct_percentile_session_matched",
            "operator": ">=",
            "value": 80.0,
        },
    ]
    independent_policy = families.condition_family_policy(independent_families)
    assert independent_policy["valid"] is True
    assert len(independent_policy["families"]) == len(
        set(independent_policy["families"])
    )

    max_pain_double_count = [
        {
            "feature": "model.snapshot.maxpain_confirmation.score",
            "operator": ">=",
            "value": 83.0,
        },
        {
            "feature": "captured.snapshot.near_share_pct",
            "operator": ">=",
            "value": 65.0,
        },
    ]
    forbidden = families.condition_family_policy(
        max_pain_double_count,
        justified_exceptions=(
            "max_pain_composite: intentionally compare two model outputs",
            "max_pain_components: intentionally compare two raw components",
        ),
    )
    assert forbidden["valid"] is False
    assert any("composite Max Pain" in reason for reason in forbidden["reasons"])

    # Every raw field from the new coherent migration-007 archive belongs to
    # the same Max-Pain component family.  Otherwise a four/five-condition
    # formula could double-count distances, liquidity and consensus as if they
    # were independent evidence.
    archive_components = [
        {
            "feature": "max_pain.12h.short_target_signed_distance_pct",
            "operator": ">=",
            "value": 1.0,
        },
        {
            "feature": "max_pain.aggregate.short_long_liquidity_ratio",
            "operator": ">=",
            "value": 1.5,
        },
    ]
    assert all(
        families.feature_correlation_family(condition["feature"])
        == "max_pain_components"
        for condition in archive_components
    )
    assert families.condition_family_policy(archive_components)["valid"] is False

    new_component_with_composite = [
        max_pain_double_count[0],
        archive_components[0],
    ]
    new_forbidden = families.condition_family_policy(
        new_component_with_composite,
        justified_exceptions=(
            "max_pain_components: compare distinct archived horizons",
        ),
    )
    assert new_forbidden["valid"] is False
    assert any("composite Max Pain" in reason for reason in new_forbidden["reasons"])

    # A and B have identical evidence; the stronger B is the exact-duplicate
    # champion. C substantially overlaps B and joins its family. D uses only
    # one coin and distinct evidence, which must remain a valid family/champion.
    formula_a = _formula("a", ["D:1", "D:2", "H:3"], score=60.0)
    formula_b = _formula("b", ["H:3", "D:2", "D:1"], score=70.0)
    formula_c = _formula("c", ["D:1", "D:2", "H:4"], score=65.0)
    formula_d = _formula("d", ["D:100", "H:101"], score=55.0, symbols=1)
    grouped = families.group_formula_evidence(
        [formula_a, formula_b, formula_c, formula_d], overlap_threshold=0.50
    )
    assert grouped["exact_duplicates_collapsed"] == 1
    assert {item["formula_key"] for item in grouped["champions"]} == {"b", "d"}
    champion_b = next(item for item in grouped["champions"] if item["formula_key"] == "b")
    metadata_b = champion_b["multiple_testing"]["evidence_family"]
    assert metadata_b["exact_duplicate_count"] == 1
    assert metadata_b["family_member_count"] == 2
    champion_d = next(item for item in grouped["champions"] if item["formula_key"] == "d")
    assert (
        champion_d["multiple_testing"]["evidence_family"]["symbol_breadth_required"]
        is False
    )

    reordered = families.group_formula_evidence(
        [formula_d, formula_c, formula_a, formula_b], overlap_threshold=0.50
    )
    first_ids = sorted(
        item["multiple_testing"]["evidence_family"]["family_id"]
        for item in grouped["champions"]
    )
    second_ids = sorted(
        item["multiple_testing"]["evidence_family"]["family_id"]
        for item in reordered["champions"]
    )
    assert first_ids == second_ids
    assert families.evidence_fingerprint(["b", "a"]) == families.evidence_fingerprint(
        ["a", "b", "a"]
    )

    print("research formula families self-test: PASS")


if __name__ == "__main__":
    run()
