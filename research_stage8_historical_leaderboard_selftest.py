from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import math
import unittest

import research_btc_parent_movement as btc_parent
import research_stage8_contract as contract
import research_stage8_historical_leaderboard as leaderboard


_COHORT = {
    "start_utc": "2025-01-01T00:00:00Z",
    "end_utc_exclusive": "2026-01-01T00:00:00Z",
    "boundary_contract": "START_INCLUSIVE_END_EXCLUSIVE",
}
_SNAPSHOT = {
    "read_only": True,
    "transaction_isolation": "repeatable read",
    "snapshot_scope": "ONE_READ_ONLY_REPEATABLE_READ_TRANSACTION",
    "database_snapshot_id": "fixture-snapshot-1",
    "captured_at_utc": "2026-01-01T00:00:01Z",
}
_SOURCES = [{
    "source_id": "historical_stage8_source",
    "source_version": "fixture-source-v1",
    "high_water_field": "attempt_id",
    "high_water_value": 123,
}]
_EVALUATOR = {
    "evaluation_input_version": leaderboard.EVALUATION_INPUT_VERSION,
    "adapter_version": "fixture-historical-adapter-v1",
    "code_commit": "a" * 40,
    "query_sha256": "b" * 64,
}
_SOURCE_VERSIONS = {
    "contract_version": contract.VERSION,
    "source_version": contract.SOURCE_VERSION,
    "projection_version": contract.PROJECTION_VERSION,
    "candidate_version": contract.CANDIDATE_VERSION,
    "label_version": contract.LABEL_VERSION,
    "independence_version": contract.INDEPENDENCE_VERSION,
    "acceptance_version": contract.ACCEPTANCE_VERSION,
}


def _wilson(successes: int, total: int) -> float | None:
    if total == 0:
        return None
    z = contract.frozen_manifest()["acceptance"]["metric_definitions"]["wilson_z"]
    p = successes / total
    return 100.0 * max(0.0, (
        p + z * z / (2 * total)
        - z * math.sqrt((p * (1.0 - p) + z * z / (4 * total)) / total)
    ) / (1.0 + z * z / total))


def _binding(*, candidate_id: str, scope_id: str = "BINANCE_BTC",
             threshold_bps: int = 50) -> dict:
    return contract.exact_binding(scope_id=scope_id, candidate_id=candidate_id,
                                  threshold_bps=threshold_bps)


def _parent_ids(namespace: str, count: int) -> list[str]:
    return sorted(
        hashlib.sha256(f"{namespace}:{ordinal}".encode()).hexdigest()
        for ordinal in range(count)
    )


def _route_population(binding_sha: str, route: str, included: list[str],
                      excluded: list[dict] | None = None) -> dict:
    excluded = [] if excluded is None else excluded
    value = {
        "version": leaderboard.ROUTE_POPULATION_VERSION,
        "exact_binding_sha256": binding_sha,
        "route": route,
        "included_btc_parent_movement_ids": included,
        "excluded_btc_parents": excluded,
    }
    return {
        "included_btc_parent_movement_ids": included,
        "excluded_btc_parents": excluded,
        "route_population_sha256": contract.digest(value),
    }


def _refresh_route_population(binding_sha: str, route_name: str, route: dict) -> None:
    route.update(_route_population(
        binding_sha,
        route_name,
        route["included_btc_parent_movement_ids"],
        route["excluded_btc_parents"],
    ))


def _evaluation(binding_sha: str, *, successes: int = 0, failures: int = 0,
                asymmetry_n: int = 0, favorable: int | None = None,
                sum_mfe: float | None = None, sum_mae: float | None = None,
                edge: float | None = None) -> dict:
    probability_n = successes + failures
    probability_ids = _parent_ids("probability", probability_n)
    asymmetry_ids = _parent_ids("asymmetry", asymmetry_n)
    favorable = (asymmetry_n if favorable is None else favorable)
    sum_mfe = (2.0 * asymmetry_n if sum_mfe is None else sum_mfe)
    sum_mae = (1.0 * asymmetry_n if sum_mae is None else sum_mae)
    if asymmetry_n == 0:
        favorable, sum_mfe, sum_mae, ratio, dominance, median = 0, 0.0, 0.0, None, None, None
    else:
        ratio = None if sum_mae == 0.0 else sum_mfe / sum_mae
        dominance = 100.0 * favorable / asymmetry_n
        median = 1.0 if edge is None else edge
    return {
        "evaluation_input_version": leaderboard.EVALUATION_INPUT_VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "exact_binding_sha256": binding_sha,
        "evaluator_version": _EVALUATOR["adapter_version"],
        "analysis_snapshot_id": _SNAPSHOT["database_snapshot_id"],
        "cohort_sha256": contract.digest(_COHORT),
        "source_ledger_sha256": contract.digest(_SOURCES),
        "routes": {
            "PROBABILITY": {
                "distinct_parent_count": probability_n,
                "successes": successes,
                "failures": failures,
                "hit_rate_pct": 100.0 * successes / probability_n if probability_n else None,
                "wilson_95_lower_pct": _wilson(successes, probability_n),
            } | _route_population(binding_sha, "PROBABILITY", probability_ids),
            "ASYMMETRY": {
                "distinct_parent_count": asymmetry_n,
                "favorable_count": favorable,
                "sum_representative_mfe_pct": sum_mfe,
                "sum_representative_mae_pct": sum_mae,
                "common_window_asymmetry_ratio": ratio,
                "common_window_favorable_dominance_pct": dominance,
                "common_window_median_paired_edge_pct": median,
            } | _route_population(binding_sha, "ASYMMETRY", asymmetry_ids),
        },
        "authority": dict(leaderboard._AUTHORITY_FALSE),
    }


def _receipt(evaluations: Mapping[str, Mapping],
             overrides: Mapping[str, tuple[str, str | None, str | None]] | None = None) -> dict:
    overrides = {} if overrides is None else overrides
    statuses = []
    for row in leaderboard.frozen_binding_universe():
        binding_sha = row["exact_binding_sha256"]
        if binding_sha in overrides:
            status, reason, evaluation_sha = overrides[binding_sha]
        elif binding_sha in evaluations:
            status, reason = leaderboard.EVALUATED, None
            evaluation_sha = contract.digest(evaluations[binding_sha])
        else:
            status, reason, evaluation_sha = leaderboard.NOT_EVALUATED, "NOT_RUN", None
        statuses.append({
            "exact_binding_sha256": binding_sha,
            "status": status,
            "reason_code": reason,
            "evaluation_sha256": evaluation_sha,
        })
    return {
        "version": leaderboard.ANALYSIS_RECEIPT_VERSION,
        "manifest_sha256": contract.MANIFEST_SHA256,
        "cohort": deepcopy(_COHORT),
        "snapshot": deepcopy(_SNAPSHOT),
        "source_versions": deepcopy(_SOURCE_VERSIONS),
        "evaluator": deepcopy(_EVALUATOR),
        "sources": deepcopy(_SOURCES),
        "binding_statuses": statuses,
    }


def _build(evaluations: Mapping[str, Mapping], **kwargs) -> dict:
    return leaderboard.build_leaderboard(
        evaluations, analysis_receipt=_receipt(evaluations, **kwargs),
    )


def _row(result: Mapping, binding_sha: str) -> Mapping:
    return next(row for row in result["rows"]
                if row["exact_binding_sha256"] == binding_sha)


class DuplicateItemsMapping(Mapping):
    def __init__(self, key: str, value: Mapping):
        self.key, self.value = key, value

    def __getitem__(self, key):
        if key != self.key:
            raise KeyError(key)
        return self.value

    def __iter__(self):
        return iter((self.key,))

    def __len__(self):
        return 1

    def items(self):
        return ((self.key, self.value), (self.key, self.value))


class Stage8HistoricalLeaderboardTests(unittest.TestCase):
    def test_frozen_universe_contains_exactly_432_unique_bindings(self):
        universe = leaderboard.frozen_binding_universe()
        self.assertEqual(len(universe), 432)
        self.assertEqual(len({row["exact_binding_sha256"] for row in universe}), 432)
        self.assertEqual({row["direction"] for row in universe}, {"LONG", "SHORT"})
        self.assertEqual({row["threshold_bps"] for row in universe},
                         set(contract.THRESHOLDS_BPS))

    def test_evaluated_n0_is_no_evidence_but_blocked_and_not_evaluated_are_not(self):
        zero = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        blocked = _binding(candidate_id="FUTURES_FLOW_ALIGNED65_LONG")
        evaluations = {zero["binding_sha256"]: _evaluation(zero["binding_sha256"])}
        result = _build(evaluations, overrides={
            blocked["binding_sha256"]: (leaderboard.BLOCKED, "SOURCE_GAP", None),
        })
        zero_row = _row(result, zero["binding_sha256"])
        blocked_row = _row(result, blocked["binding_sha256"])
        not_run = next(row for row in result["rows"] if row["evaluation_status"] == leaderboard.NOT_EVALUATED)
        self.assertEqual(zero_row["probability"]["evidence_band"], leaderboard.NO_EVIDENCE)
        for row in (blocked_row, not_run):
            self.assertIsNone(row["probability"]["evidence_band"])
            self.assertIsNone(row["probability"]["metrics"])
            self.assertFalse(row["probability"]["rank_eligible"])
        self.assertEqual(result["binding_count"], 432)
        self.assertEqual(result["complete_binding_status_count"], 432)

    def test_evaluated_requires_evaluation_and_non_evaluated_forbids_one(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        receipt = _receipt({}, overrides={
            binding_sha: (leaderboard.EVALUATED, None, "0" * 64),
        })
        with self.assertRaisesRegex(ValueError, "MISSING_EVALUATION"):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)
        evaluation = _evaluation(binding_sha)
        receipt = _receipt({binding_sha: evaluation}, overrides={
            binding_sha: (leaderboard.BLOCKED, "SOURCE_GAP", None),
        })
        with self.assertRaisesRegex(ValueError, "NON_EVALUATED"):
            leaderboard.build_leaderboard({binding_sha: evaluation}, analysis_receipt=receipt)

    def test_status_receipt_rejects_missing_duplicate_unknown_and_bad_status(self):
        receipt = _receipt({})
        receipt["binding_statuses"].pop()
        with self.assertRaisesRegex(ValueError, "STATUS_MISSING_BINDING"):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)
        receipt = _receipt({})
        receipt["binding_statuses"][-1] = deepcopy(receipt["binding_statuses"][0])
        with self.assertRaisesRegex(ValueError, "STATUS_DUPLICATE_BINDING"):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)
        receipt = _receipt({})
        receipt["binding_statuses"][0]["exact_binding_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "STATUS_UNKNOWN_BINDING"):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)
        receipt = _receipt({})
        receipt["binding_statuses"][0]["status"] = "SKIPPED"
        with self.assertRaisesRegex(ValueError, "UNKNOWN_EVALUATION_STATUS"):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)

    def test_receipt_requires_half_open_cohort_atomic_snapshot_versions_and_high_water(self):
        mutations = (
            ("cohort", "boundary_contract", "CLOSED"),
            ("cohort", "end_utc_exclusive", _COHORT["start_utc"]),
            ("snapshot", "read_only", False),
            ("snapshot", "transaction_isolation", "read committed"),
            ("snapshot", "snapshot_scope", "STATEMENT"),
            ("snapshot", "captured_at_utc", _COHORT["start_utc"]),
            ("source_versions", "label_version", "wrong"),
            ("evaluator", "evaluation_input_version", "wrong"),
            ("evaluator", "query_sha256", "short"),
        )
        for section, field, value in mutations:
            with self.subTest(section=section, field=field):
                receipt = _receipt({})
                receipt[section][field] = value
                with self.assertRaises(ValueError):
                    leaderboard.build_leaderboard({}, analysis_receipt=receipt)
        receipt = _receipt({})
        receipt["sources"][0]["high_water_value"] = -1
        with self.assertRaises(ValueError):
            leaderboard.build_leaderboard({}, analysis_receipt=receipt)

    def test_asymmetry_recomputes_and_validates_ratio_dominance_and_sums(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, asymmetry_n=5, favorable=3,
                                 sum_mfe=8.0, sum_mae=4.0, edge=0.25)
        row = _row(_build({binding_sha: evaluation}), binding_sha)["asymmetry"]
        self.assertEqual(row["metrics"]["common_window_asymmetry_ratio"], 2.0)
        self.assertEqual(row["metrics"]["common_window_favorable_dominance_pct"], 60.0)
        for field, value, message in (
            ("common_window_asymmetry_ratio", 9.0, "ratio is inconsistent"),
            ("common_window_favorable_dominance_pct", 99.0, "dominance is inconsistent"),
            ("favorable_count", 6, "must not exceed"),
            ("sum_representative_mfe_pct", -1.0, "nonnegative"),
        ):
            invalid = deepcopy(evaluation)
            invalid["routes"]["ASYMMETRY"][field] = value
            with self.assertRaisesRegex(ValueError, message):
                _build({binding_sha: invalid})

    def test_zero_mae_requires_null_ratio_and_remains_unranked(self):
        binding = _binding(candidate_id="SPOT_FLOW_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, asymmetry_n=3, favorable=3,
                                 sum_mfe=3.0, sum_mae=0.0, edge=1.0)
        route = _row(_build({binding_sha: evaluation}), binding_sha)["asymmetry"]
        self.assertIsNone(route["metrics"]["common_window_asymmetry_ratio"])
        self.assertFalse(route["rank_eligible"])
        invalid = deepcopy(evaluation)
        invalid["routes"]["ASYMMETRY"]["common_window_asymmetry_ratio"] = 3.0
        with self.assertRaisesRegex(ValueError, "zero denominator"):
            _build({binding_sha: invalid})

    def test_route_parent_identity_and_exclusion_provenance_is_fail_closed(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, successes=1, failures=1)
        duplicate_parent = evaluation["routes"]["PROBABILITY"][
            "included_btc_parent_movement_ids"
        ][0]
        for mutate, message, refresh_digest in (
            (lambda route: route.update(
                included_btc_parent_movement_ids=[duplicate_parent, duplicate_parent]
            ), "sorted, unique", True),
            (lambda route: route.update(
                included_btc_parent_movement_ids=list(reversed(
                    route["included_btc_parent_movement_ids"]
                ))
            ), "sorted, unique", True),
            (lambda route: route.update(route_population_sha256="0" * 64),
             "digest mismatch", False),
        ):
            invalid = deepcopy(evaluation)
            invalid_route = invalid["routes"]["PROBABILITY"]
            mutate(invalid_route)
            if refresh_digest:
                _refresh_route_population(binding_sha, "PROBABILITY", invalid_route)
            with self.assertRaisesRegex(ValueError, message):
                _build({binding_sha: invalid})
        valid = deepcopy(evaluation)
        route = valid["routes"]["PROBABILITY"]
        route["excluded_btc_parents"] = [{
            "btc_parent_movement_id": _parent_ids("excluded", 1)[0],
            "reason_code": "NO_DECISIVE_LABEL",
        }]
        route.update(_route_population(binding_sha, "PROBABILITY",
                                       route["included_btc_parent_movement_ids"],
                                       route["excluded_btc_parents"]))
        output = _row(_build({binding_sha: valid}), binding_sha)["probability"]["metrics"]
        self.assertEqual(output["source_parent_count"], 3)

    def test_route_parent_ids_require_canonical_lowercase_sha256(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, successes=1)
        canonical_parent = btc_parent._identity(
            datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        )
        route = evaluation["routes"]["PROBABILITY"]
        route.update(_route_population(
            binding_sha, "PROBABILITY", [canonical_parent]
        ))
        output = _row(_build({binding_sha: evaluation}), binding_sha)["probability"]["metrics"]
        self.assertEqual(output["included_btc_parent_movement_ids"], [canonical_parent])

        for invalid_parent in (1001, "A" * 64, "a" * 63, "g" * 64, True):
            with self.subTest(invalid_parent=invalid_parent):
                invalid = deepcopy(evaluation)
                invalid_route = invalid["routes"]["PROBABILITY"]
                invalid_route[
                    "included_btc_parent_movement_ids"
                ] = [invalid_parent]
                _refresh_route_population(binding_sha, "PROBABILITY", invalid_route)
                with self.assertRaisesRegex(ValueError, "lowercase SHA-256"):
                    _build({binding_sha: invalid})

        for invalid_parent in (9001, "B" * 64, "b" * 63, "g" * 64, True):
            with self.subTest(excluded_parent=invalid_parent):
                invalid = deepcopy(evaluation)
                invalid_route = invalid["routes"]["PROBABILITY"]
                invalid_route["excluded_btc_parents"] = [{
                    "btc_parent_movement_id": invalid_parent,
                    "reason_code": "NO_DECISIVE_LABEL",
                }]
                _refresh_route_population(binding_sha, "PROBABILITY", invalid_route)
                with self.assertRaisesRegex(
                    ValueError, "excluded parent must be a lowercase SHA-256"
                ):
                    _build({binding_sha: invalid})

    def test_excluded_parent_provenance_rejects_order_duplicates_and_overlap(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, successes=1)
        included_parent = evaluation["routes"]["PROBABILITY"][
            "included_btc_parent_movement_ids"
        ][0]
        excluded = _parent_ids("excluded-invalid", 2)
        cases = (
            (list(reversed([
                {"btc_parent_movement_id": excluded[0], "reason_code": "NO_LABEL"},
                {"btc_parent_movement_id": excluded[1], "reason_code": "NO_OUTCOME"},
            ])), "exclusions must be sorted"),
            ([
                {"btc_parent_movement_id": excluded[0], "reason_code": "NO_LABEL"},
                {"btc_parent_movement_id": excluded[0], "reason_code": "NO_OUTCOME"},
            ], "exclusion provenance is invalid"),
            ([
                {"btc_parent_movement_id": included_parent, "reason_code": "NO_LABEL"},
            ], "exclusion provenance is invalid"),
        )
        for excluded_rows, message in cases:
            with self.subTest(message=message):
                invalid = deepcopy(evaluation)
                invalid_route = invalid["routes"]["PROBABILITY"]
                invalid_route["excluded_btc_parents"] = excluded_rows
                _refresh_route_population(binding_sha, "PROBABILITY", invalid_route)
                with self.assertRaisesRegex(ValueError, message):
                    _build({binding_sha: invalid})

    def test_route_population_digest_binds_parent_ids_exclusions_route_and_binding(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, successes=1)

        invalid = deepcopy(evaluation)
        invalid["routes"]["PROBABILITY"][
            "included_btc_parent_movement_ids"
        ] = _parent_ids("same-count-substitution", 1)
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            _build({binding_sha: invalid})

        invalid = deepcopy(evaluation)
        invalid_route = invalid["routes"]["PROBABILITY"]
        invalid_route["excluded_btc_parents"] = [{
            "btc_parent_movement_id": _parent_ids("digest-exclusion", 1)[0],
            "reason_code": "NO_LABEL",
        }]
        _refresh_route_population(binding_sha, "PROBABILITY", invalid_route)
        invalid_route["excluded_btc_parents"][0]["reason_code"] = "NO_OUTCOME"
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            _build({binding_sha: invalid})

        for wrong_binding, wrong_route in (
            ("0" * 64, "PROBABILITY"),
            (binding_sha, "ASYMMETRY"),
        ):
            with self.subTest(wrong_binding=wrong_binding, wrong_route=wrong_route):
                invalid = deepcopy(evaluation)
                invalid_route = invalid["routes"]["PROBABILITY"]
                invalid_route["route_population_sha256"] = _route_population(
                    wrong_binding,
                    wrong_route,
                    invalid_route["included_btc_parent_movement_ids"],
                    invalid_route["excluded_btc_parents"],
                )["route_population_sha256"]
                with self.assertRaisesRegex(ValueError, "digest mismatch"):
                    _build({binding_sha: invalid})

    def test_evaluation_is_bound_to_snapshot_sources_evaluator_and_digest(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha)
        for field in ("analysis_snapshot_id", "cohort_sha256", "source_ledger_sha256",
                      "evaluator_version"):
            invalid = deepcopy(evaluation)
            invalid[field] = "wrong"
            with self.assertRaisesRegex(ValueError, "PROVENANCE_MISMATCH"):
                _build({binding_sha: invalid})
        receipt = _receipt({binding_sha: evaluation})
        next(
            item for item in receipt["binding_statuses"]
            if item["exact_binding_sha256"] == binding_sha
        )["evaluation_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "EVALUATION_DIGEST_MISMATCH"):
            leaderboard.build_leaderboard({binding_sha: evaluation}, analysis_receipt=receipt)

    def test_probability_and_asymmetry_have_independent_banded_dense_ranks(self):
        bindings = [
            _binding(candidate_id="POSITIONING_ALIGNED65_LONG"),
            _binding(candidate_id="FUTURES_FLOW_ALIGNED65_LONG"),
            _binding(candidate_id="SPOT_FLOW_ALIGNED65_LONG"),
        ]
        evaluations = {
            bindings[0]["binding_sha256"]: _evaluation(bindings[0]["binding_sha256"],
                successes=2, failures=1, asymmetry_n=3, favorable=2, sum_mfe=6, sum_mae=3),
            bindings[1]["binding_sha256"]: _evaluation(bindings[1]["binding_sha256"],
                successes=2, failures=1, asymmetry_n=3, favorable=2, sum_mfe=4.5, sum_mae=3),
            bindings[2]["binding_sha256"]: _evaluation(bindings[2]["binding_sha256"],
                successes=1, failures=2, asymmetry_n=3, favorable=2, sum_mfe=6, sum_mae=3),
        }
        result = _build(evaluations)
        rows = [_row(result, binding["binding_sha256"]) for binding in bindings]
        self.assertEqual([row["probability"]["dense_rank"] for row in rows], [1, 1, 2])
        self.assertEqual([row["asymmetry"]["dense_rank"] for row in rows], [1, 2, 1])

    def test_authority_nonfinite_unknown_duplicate_and_composite_are_rejected(self):
        binding = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        binding_sha = binding["binding_sha256"]
        evaluation = _evaluation(binding_sha, successes=1, asymmetry_n=1)
        authoritative = deepcopy(evaluation)
        authoritative["authority"]["research_qualified"] = True
        with self.assertRaisesRegex(ValueError, "cannot assert authority"):
            _build({binding_sha: authoritative})
        nonfinite = deepcopy(evaluation)
        nonfinite["routes"]["ASYMMETRY"]["common_window_median_paired_edge_pct"] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite|JSON compliant"):
            _build({binding_sha: nonfinite})
        unknown_route = deepcopy(evaluation)
        unknown_route["routes"]["COMPOSITE"] = {}
        with self.assertRaisesRegex(ValueError, "exactly two known routes"):
            _build({binding_sha: unknown_route})
        with self.assertRaisesRegex(ValueError, "DUPLICATE_BINDING"):
            leaderboard.build_leaderboard(
                DuplicateItemsMapping(binding_sha, evaluation),
                analysis_receipt=_receipt({binding_sha: evaluation}),
            )

    def test_output_is_deterministic_and_has_no_authority_or_composite(self):
        first = _binding(candidate_id="POSITIONING_ALIGNED65_LONG")
        second = _binding(candidate_id="FUTURES_FLOW_ALIGNED65_LONG")
        pairs = [
            (first["binding_sha256"], _evaluation(first["binding_sha256"], successes=1)),
            (second["binding_sha256"], _evaluation(second["binding_sha256"], successes=1)),
        ]
        forward, reverse = _build(dict(pairs)), _build(dict(reversed(pairs)))
        self.assertEqual(forward, reverse)
        self.assertEqual(forward["analysis_receipt_sha256"],
                         contract.digest(forward["analysis_receipt"]))
        self.assertEqual(set(forward["authority"].values()), {False})
        self.assertEqual(forward["activation_effect"], "NONE")
        self.assertFalse(forward["ranking_contract"]["weighted_composite"])
        self.assertIsNone(forward["ranking_contract"]["composite_score"])
        for row in forward["rows"]:
            self.assertEqual(set(row["authority"].values()), {False})
            self.assertEqual(row["activation_effect"], "NONE")


if __name__ == "__main__":
    unittest.main()
