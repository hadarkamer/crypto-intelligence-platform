"""Pure and fake-connection checks for the descriptive Watch leaderboard."""
from contextlib import contextmanager
import math
import os
from unittest.mock import patch

import research_watch_scan_formula_leaderboard as leaderboard


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def catalog_rows():
    return [
        {"evaluation_version": "active-v1", "candidate_key": "A", "definition": {"a": 1},
         "definition_sha256": SHA_A, "orientation": "NORMAL", "feature_version": "f1"},
        {"evaluation_version": "active-v1", "candidate_key": "B", "definition": {"b": 1},
         "definition_sha256": SHA_B, "orientation": "INVERSE", "feature_version": "f1"},
        {"evaluation_version": "active-v1", "candidate_key": "C", "definition": {"c": 1},
         "definition_sha256": SHA_C, "orientation": "NORMAL", "feature_version": "f1"},
    ]


def comparison(candidate, *, success, failure, control_success=0, control_failure=0):
    orientation = "INVERSE" if candidate == "B" else "NORMAL"
    return {"candidate_key": candidate, "evaluation_version": "active-v1",
        "base_direction": "SHORT" if orientation == "INVERSE" else "LONG",
        "analysis_direction": "LONG",
        "threshold_bps": 25, "distinct_waves": success + failure + control_success + control_failure,
        "matched_waves": success + failure, "control_waves": control_success + control_failure,
        "shared_waves": 0, "blocked_arms": 0, "matched_success": success,
        "matched_failure": failure, "control_success": control_success,
        "control_failure": control_failure, "open_arms": 0, "missing_arms": 0,
        "ambiguous_arms": 0, "no_touch_arms": 0}


def outcome(candidate, values):
    orientation = "INVERSE" if candidate == "B" else "NORMAL"
    mfe = sum(value[0] for value in values)
    mae = sum(value[1] for value in values)
    edges = sorted(value[0] - value[1] for value in values)
    median = (edges[len(edges)//2] if len(edges) % 2
              else sum(edges[len(edges)//2-1:len(edges)//2+1]) / 2)
    return {"candidate_key": candidate, "evaluation_version": "active-v1",
        "base_direction": "SHORT" if orientation == "INVERSE" else "LONG",
        "analysis_direction": "LONG",
        "threshold_bps": 25, "full_window_row_count": len(values),
        "full_window_parent_count": len(values),
        "favorable_parent_count": sum(a > b for a, b in values),
        "sum_mfe_pct": mfe, "sum_mae_pct": mae, "median_paired_edge_pct": median}


def build(**overrides):
    kwargs = {"dimension": "COIN", "symbol_scope": "BTC", "window_minutes": 60,
        "timeframe": leaderboard.COIN_TIMEFRAME, "analysis_direction": "LONG",
        "threshold_bps": 25, "top_per_route": 2, "catalog_rows": catalog_rows(),
        "comparison_rows": [comparison("A", success=1, failure=0, control_failure=1),
                            comparison("B", success=4, failure=1, control_success=1, control_failure=1)],
        "outcome_rows": [outcome("A", [(4.0, 1.0)]),
                         outcome("B", [(2.0, 1.0), (2.0, 1.0), (2.0, 1.0),
                                        (0.5, 1.0), (0.5, 1.0)])],
        "snapshot": {"transaction_isolation": "repeatable read", "transaction_read_only": "on"}}
    kwargs.update(overrides)
    return leaderboard.build_leaderboard(**kwargs)


def check_pure():
    report = build()
    partition = report["partitions"][0]
    assert report["source_kind"] == "WATCH_ALL_FORMULA_DESCRIPTIVE_COMPARATOR"
    assert report["source_contract"] == "WATCH_DESCRIPTIVE_ALL_CAPTURE_PHASES"
    assert report["summary"]["catalog_candidate_count"] == 3
    assert report["summary"]["enumerated_candidate_cell_count"] == 3
    assert partition["candidate_count"] == 3
    assert partition["both_routes_no_evidence_count"] == 1
    assert partition["both_routes_no_evidence_candidate_keys_sample"] == ["C"]
    # There is no five-parent cutoff: both bands are returned, but they are
    # never compared as if one shared rank were meaningful.
    probability = [
        partition["probability_leaders_by_evidence_band"][
            leaderboard.DESCRIPTIVE_BAND
        ][0],
        partition["probability_leaders_by_evidence_band"][
            leaderboard.PROVISIONAL_BAND
        ][0],
    ]
    assert [row["candidate_key"] for row in probability] == ["B", "A"]
    assert probability[0]["probability_evidence_band"] == "N_GTE_5_DESCRIPTIVE"
    assert probability[1]["probability_evidence_band"] == "N_LT_5_PROVISIONAL"
    assert math.isclose(probability[0]["probability_hit_rate_pct"], 80.0)
    assert math.isclose(probability[0]["control_lift_percentage_points"], 30.0)
    assert probability[0]["analysis_direction"] == "LONG"
    assert probability[0]["base_direction"] == "SHORT"
    assert probability[0]["probability_dense_rank"] == 1
    assert probability[0]["probability_display_order"] == 1
    assert probability[1]["probability_dense_rank"] == 1
    assert partition["probability_leaders_by_evidence_band"][
        leaderboard.DESCRIPTIVE_BAND
    ][0]["candidate_key"] == "B"
    assert partition["probability_leaders_by_evidence_band"][
        leaderboard.PROVISIONAL_BAND
    ][0]["candidate_key"] == "A"
    assert "probability_leaders" not in partition
    assert "asymmetry_leaders" not in partition
    # Asymmetry is independently ranked inside the same two evidence bands.
    asymmetry = [
        partition["asymmetry_leaders_by_evidence_band"][
            leaderboard.DESCRIPTIVE_BAND
        ][0],
        partition["asymmetry_leaders_by_evidence_band"][
            leaderboard.PROVISIONAL_BAND
        ][0],
    ]
    assert [row["candidate_key"] for row in asymmetry] == ["B", "A"]
    assert asymmetry[1]["common_window_asymmetry_ratio"] == 4.0
    assert asymmetry[1]["asymmetry_evidence_band"] == "N_LT_5_PROVISIONAL"
    for route_rows in (probability, asymmetry):
        for row in route_rows:
            assert row["qualifies_as_prospective_formula_evidence"] is False
            assert row["research_qualified"] is False
            assert row["live_delivery_authorized"] is False
            assert row["telegram_delivery_authorized"] is False
            assert row["trade_execution_authorized"] is False
    assert report["ranking_contract"]["minimum_evidence_filter"] is None
    reference = report["ranking_contract"]["descriptive_reference_floors"]
    assert reference["policy_version"] == leaderboard.REFERENCE_FLOOR_POLICY_VERSION
    assert reference["documented_source"] == "docs/ORDERED_V7_ACCEPTANCE_POLICY_V1.md"
    assert reference["ranking_tier_only_not_acceptance"] is True
    assert probability[0]["probability_reference_floor_pass_count"] == 1
    leaderboard.canonical(report)


def check_all_partitions_and_zero_catalog():
    report = build(analysis_direction="ALL", threshold_bps="ALL",
        comparison_rows=[], outcome_rows=[], top_per_route=1)
    assert report["summary"]["partition_count"] == 16
    assert report["summary"]["enumerated_candidate_cell_count"] == 48
    assert all(partition["candidate_count"] == 3 for partition in report["partitions"])
    assert all(
        not any(partition["probability_leaders_by_evidence_band"].values())
        and not any(partition["asymmetry_leaders_by_evidence_band"].values())
        for partition in report["partitions"]
    )
    assert report["summary"]["probability_evidence_band_counts"]["N_0_NO_EVIDENCE"] == 48
    assert report["summary"]["returned_leader_row_upper_bound"] == 64


def check_validation_and_zero_mae():
    assert leaderboard.wilson_95_lower_pct(0, 0) is None
    assert math.isclose(leaderboard.wilson_95_lower_pct(4, 5), 37.553463, rel_tol=1e-6)
    for changes in (
        {"timeframe": "12h"}, {"dimension": "SELECTED_TIMEFRAME", "timeframe": "bad"},
        {"symbol_scope": "BTC,ETH"}, {"window_minutes": 61}, {"threshold_bps": 30},
        {"top_per_route": 21}, {"analysis_direction": "NEUTRAL"},
    ):
        try:
            build(**changes)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid exact request accepted: {changes}")
    zero = build(comparison_rows=[comparison("A", success=1, failure=0)],
        outcome_rows=[outcome("A", [(1.0, 0.0)])])
    row = zero["partitions"][0]["probability_leaders_by_evidence_band"][
        leaderboard.PROVISIONAL_BAND
    ][0]
    assert row["common_window_asymmetry_ratio"] is None
    assert zero["partitions"][0]["asymmetry_ranked_count"] == 0
    invalid_sha = catalog_rows()
    invalid_sha[0]["definition_sha256"] = "A" * 64
    mixed_version = catalog_rows()
    mixed_version[0]["evaluation_version"] = "other-active-version"
    for invalid_catalog in ([], invalid_sha, mixed_version):
        try:
            build(catalog_rows=invalid_catalog, comparison_rows=[], outcome_rows=[])
        except ValueError:
            pass
        else:
            raise AssertionError("invalid active catalog accepted")
    duplicate_parent_weight = outcome("A", [(1.0, 0.5)])
    duplicate_parent_weight["full_window_row_count"] = 2
    try:
        build(comparison_rows=[comparison("A", success=1, failure=0)],
              outcome_rows=[duplicate_parent_weight])
    except ValueError as exc:
        assert "duplicate parent" in str(exc)
    else:
        raise AssertionError("duplicate parent weighting accepted")


class Result:
    def __init__(self, rows):
        self.rows = rows
    def fetchone(self):
        assert len(self.rows) == 1
        return self.rows[0]
    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self):
        self.calls = []
    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params))
        if "transaction_timestamp()" in sql:
            return Result([{"snapshot_at_utc": "2026-09-14T00:00:00+00:00",
                            "transaction_isolation": "repeatable read", "transaction_read_only": "on"}])
        if "FROM research_watch_scan_formula_catalog" in sql:
            return Result(catalog_rows())
        if "FROM research_watch_scan_formula_comparisons" in sql:
            return Result([comparison("A", success=1, failure=0)])
        if "FROM research_watch_scan_formula_wave_outcomes" in sql:
            return Result([outcome("A", [(1.0, 0.5)])])
        raise AssertionError(normalized)


def check_fake_connection():
    conn = FakeConnection()
    with patch.object(
        leaderboard, "_attest_active_catalog",
        return_value={"status": "VERIFIED_TEST_CATALOG"},
    ):
        report = leaderboard.read_leaderboard(conn, dimension="COIN", symbol_scope="BTC",
            window_minutes=60, timeframe=leaderboard.COIN_TIMEFRAME,
            analysis_direction="LONG", threshold_bps=25, top_per_route=1)
    assert report["catalog_attestation"]["status"] == "VERIFIED_TEST_CATALOG"
    assert report["partitions"][0]["probability_leaders_by_evidence_band"][
        leaderboard.PROVISIONAL_BAND
    ][0]["candidate_key"] == "A"
    comparison_call = next(call for call in conn.calls if "formula_comparisons" in call[0])
    outcome_call = next(call for call in conn.calls if "formula_wave_outcomes" in call[0])
    assert comparison_call[1] == ["BTC", 60, ["LONG"], [25]]
    assert outcome_call[1] == comparison_call[1]
    assert "symbol_scope=%s AND window_minutes=%s" in comparison_call[0]
    assert "analysis_direction=ANY(%s::text[])" in comparison_call[0]
    assert "threshold_bps=ANY(%s::integer[])" in comparison_call[0]
    # A caller-supplied connection that is writable or read-committed is
    # rejected before any catalog/outcome data is accepted.
    bad = FakeConnection()
    original = bad.execute
    def execute(sql, params=()):
        if "transaction_timestamp()" in sql:
            return Result([{"transaction_isolation": "read committed", "transaction_read_only": "off"}])
        return original(sql, params)
    bad.execute = execute
    try:
        leaderboard.read_leaderboard(bad, dimension="COIN", symbol_scope="BTC", window_minutes=60,
            timeframe=leaderboard.COIN_TIMEFRAME, analysis_direction="LONG", threshold_bps=25)
    except RuntimeError:
        pass
    else:
        raise AssertionError("writable read-committed connection accepted")


def check_shipped_catalog_attestation():
    for dimension, module in (
        ("COIN", leaderboard.coin_formula_catalog),
        ("SELECTED_TIMEFRAME", leaderboard.timeframe_formula_catalog),
    ):
        rows = [{
            "evaluation_version": module.VERSION,
            "candidate_key": row["candidate_key"],
            "definition": row["definition"],
            "definition_sha256": row["definition_sha256"],
            "orientation": row["orientation"],
            "feature_version": module.FEATURE_VERSION,
        } for row in module.catalog_records() if row["supported"]]
        receipt = leaderboard._attest_active_catalog(rows, dimension)
        assert receipt["status"] == "VERIFIED_AGAINST_SHIPPED_ACTIVE_CATALOG"
        assert receipt["active_evaluation_version"] == module.VERSION
        assert receipt["supported_candidate_count"] == len(rows)
        try:
            leaderboard._attest_active_catalog(rows[:-1], dimension)
        except ValueError as exc:
            assert "coverage mismatch" in str(exc)
        else:
            raise AssertionError("partial active catalog accepted")


def check_exact_ai_partition_stays_structured_under_tool_limit():
    for dimension, module, timeframe in (
        ("COIN", leaderboard.coin_formula_catalog, leaderboard.COIN_TIMEFRAME),
        ("SELECTED_TIMEFRAME", leaderboard.timeframe_formula_catalog, "12h"),
    ):
        records = [row for row in module.catalog_records() if row["supported"]]
        catalog = [{
            "evaluation_version": module.VERSION,
            "candidate_key": row["candidate_key"],
            "definition": row["definition"],
            "definition_sha256": row["definition_sha256"],
            "orientation": row["orientation"],
            "feature_version": module.FEATURE_VERSION,
        } for row in records]
        largest = sorted(
            records,
            key=lambda row: len(leaderboard.canonical(row["definition"])),
            reverse=True,
        )[:8]
        comparisons, outcomes = [], []
        for index, record in enumerate(largest):
            n = 1 if index < 4 else 5
            base_direction = leaderboard._base_direction(
                "LONG", record["orientation"]
            )
            comparisons.append({
                "candidate_key": record["candidate_key"],
                "evaluation_version": module.VERSION,
                "base_direction": base_direction,
                "analysis_direction": "LONG",
                "threshold_bps": 25,
                "distinct_waves": n,
                "matched_waves": n,
                "control_waves": 0,
                "shared_waves": 0,
                "blocked_arms": 0,
                "matched_success": n,
                "matched_failure": 0,
                "control_success": 0,
                "control_failure": 0,
                "open_arms": 0,
                "missing_arms": 0,
                "ambiguous_arms": 0,
                "no_touch_arms": 0,
            })
            outcomes.append({
                "candidate_key": record["candidate_key"],
                "evaluation_version": module.VERSION,
                "base_direction": base_direction,
                "analysis_direction": "LONG",
                "threshold_bps": 25,
                "full_window_row_count": n,
                "full_window_parent_count": n,
                "favorable_parent_count": n,
                "sum_mfe_pct": 2.0 * n,
                "sum_mae_pct": 1.0 * n,
                "median_paired_edge_pct": 1.0,
            })
        report = leaderboard.build_leaderboard(
            dimension=dimension, symbol_scope="BTC", window_minutes=60,
            timeframe=timeframe, analysis_direction="LONG",
            threshold_bps=25, top_per_route=4, catalog_rows=catalog,
            comparison_rows=comparisons, outcome_rows=outcomes,
        )
        encoded = leaderboard.canonical(report)
        assert len(encoded) < 55000, (dimension, len(encoded))
        assert report["summary"]["returned_leader_row_upper_bound"] == 16


def check_dedicated_connection_contract():
    with patch.dict(os.environ, {"DATABASE_URL": "must-not-be-used"}, clear=True):
        assert leaderboard._database_url() == ""
    calls = []
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        @contextmanager
        def transaction(self): yield
        def execute(self, sql): calls.append(sql)
    class Psycopg:
        @staticmethod
        def connect(url, **kwargs):
            calls.append((url, kwargs))
            return Connection()
    with patch.dict(os.environ, {leaderboard.DATABASE_URL_ENV: "postgresql://reader"}, clear=True), \
            patch.object(leaderboard, "psycopg", Psycopg), patch.object(leaderboard, "dict_row", object()):
        with leaderboard.readonly_connection():
            pass
    assert calls[0][0] == "postgresql://reader"
    assert "default_transaction_read_only=on" in calls[0][1]["options"]
    assert calls[1] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"


def run():
    check_pure()
    check_all_partitions_and_zero_catalog()
    check_validation_and_zero_mae()
    check_fake_connection()
    check_shipped_catalog_attestation()
    check_exact_ai_partition_stays_structured_under_tool_limit()
    check_dedicated_connection_contract()
    print("PASS Watch descriptive ordered-v7 leaderboard: exact filters, catalog n=0, separate ranks, read-only snapshot")


if __name__ == "__main__":
    run()
