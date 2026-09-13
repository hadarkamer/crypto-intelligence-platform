"""Watch source-admission parity vectors and isolated PostgreSQL helper tests.

The pure vectors are also consumed by the local PGlite compatibility probe.
Native tests use an owner-provided TEST_DATABASE_URL and a rolled-back schema;
they do not apply a production migration or change existing source rows.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import json
import os
from pathlib import Path
import unittest
from uuid import uuid4

import research_operational_score_source_audit as audit
from research_operational_score_source_audit_selftest import _block, _snapshot
import research_watch_score_capture as capture


MIGRATION = Path(__file__).with_name("migrations") / "046_stage8_durable_registry.sql"


def helper_sql() -> str:
    return MIGRATION.read_text().split(
        "CREATE OR REPLACE FUNCTION research_stage8_has_forbidden_evidence_key_v1", 1
    )[0]


def watch_capture_cases() -> list[dict]:
    base = _snapshot()
    decision = base["created_at_utc"] + timedelta(seconds=30)
    cases = []

    def add(name, mutate=None, *, valid=False, symbol="BTC"):
        snapshot = deepcopy(base)
        block = _block(snapshot)
        if mutate:
            mutate(block, block["coins"][symbol])
        block["payload_sha256"] = capture.digest({
            key: value for key, value in block.items() if key != "payload_sha256"
        })
        snapshot = json.loads(capture.canonical(snapshot))
        result = audit.validate_capture(snapshot, symbol=symbol,
            decision_time_utc=decision, max_capture_age_seconds=300)
        cases.append({"name": name, "snapshot": snapshot, "symbol": symbol,
                      "decision": decision.isoformat(), "valid": valid,
                      "python_valid": result["status"] == "VALID"})

    add("producer_capture", valid=True)
    add("small_float_and_large_integral", lambda b,c: b.update(
        probe=[1e-5,1e-6,1e20,1.2345678901234567,-0.0,1000000000000000.5]), valid=True)
    add("unicode_payload", lambda b,c: b.update(probe="שלום 😀 \u2028 /\n\t"), valid=True)
    add("payload_too_large", lambda b,c: b.update(probe="x" * 262144))
    add("row_count_fractional", lambda b,c: b.update(input_row_count=56.5))
    add("cycle_blank", lambda b,c: b.update(cycle_id=" "))
    add("slot_missing", lambda b,c: c["maxpain"].pop())
    add("slot_duplicate", lambda b,c: c["maxpain"].__setitem__(0,deepcopy(c["maxpain"][1])))
    add("slot_unavailable_with_zero", lambda b,c: c["maxpain"][0].update(status="INACTIVE_TARGET",score=0))
    add("slot_unavailable_without_score", lambda b,c: c["maxpain"][0].update(status="INACTIVE_TARGET",score=None),valid=True)
    add("component_missing", lambda b,c: c["maxpain"][0]["components"].pop("relative_gap"))
    add("component_boolean", lambda b,c: c["maxpain"][0]["components"].update(relative_gap=True))
    for value,score,valid in [(2.675,2.67,True),(2.675,2.69,False),(2.685,2.69,True),(2.685,2.67,False)]:
        add(f"python_round_{value}_{score}",lambda b,c,value=value,score=score:
            c["maxpain"][0].update(score=score,components={
                "directional_alignment":value,"target_proximity":0,
                "cluster_confidence":0,"relative_gap":0}),valid=valid)
    for model_name in ("positioning","futures_flow","spot_flow"):
        add("model_availability_"+model_name,lambda b,c,n=model_name:c["models"][n].update(available="true"))
        add("model_score_"+model_name,lambda b,c,n=model_name:c["models"][n].update(score="NaN"))
    add("unselected_available_without_windows",lambda b,c:c["models"]["spot_flow"].update(available=True,capture_status="AVAILABLE"))
    add("operational_row_missing",lambda b,c:c["sources"]["maxpain_operational_rows"].pop())
    add("operational_row_duplicate",lambda b,c:c["sources"]["maxpain_operational_rows"].__setitem__(0,deepcopy(c["sources"]["maxpain_operational_rows"][1])))
    add("operational_futures_route",lambda b,c:c["sources"]["maxpain_operational_rows"][0].update(price_market="futures"))
    add("operational_wrong_pair",lambda b,c:c["sources"]["maxpain_operational_rows"][0].update(price_pair="ETHUSDT"))
    add("operational_naive_time",lambda b,c:c["sources"]["maxpain_operational_rows"][0].update(price_fetched_at_utc="2026-08-29T12:00:00"))
    add("computed_infinity",lambda b,c:b.update(computed_at_utc="infinity"))
    add("computed_naive",lambda b,c:b.update(computed_at_utc="2026-08-29T12:05:00"))
    add("computed_future",lambda b,c:b.update(computed_at_utc="2026-08-29T12:08:00+00:00"))
    add("positioning_clock_missing",lambda b,c:c["sources"]["positioning"].pop("oi_fetched_at"))
    add("spot_clock_future",lambda b,c:c["sources"]["spot"]["quality"].update(candle_close="2026-08-29T12:06:00+00:00"))
    add("cvd_clock_missing",lambda b,c:c["sources"]["timing_observation"].pop("cvd_observed_at_utc"))
    add("unselected_window_future",lambda b,c:c["sources"]["spot"]["window_references"].update(extra={"available":False,"target_time":"2026-08-29T12:06:00+00:00"}))
    add("unselected_window_naive",lambda b,c:c["sources"]["positioning"]["window_references"].update(extra={"available":False,"reference_target_time":"2026-08-29T12:00:00"}))
    add("available_window_missing_time",lambda b,c:c["sources"]["futures"]["window_references"]["1h"].pop("reference_time"))
    add("derivatives_hash_invalid",lambda b,c:c["sources"].update(derivatives_snapshot_sha256=""))
    return cases


class WatchSqlParityContractTests(unittest.TestCase):
    def test_production_builder_vectors_have_expected_python_admission(self):
        for case in watch_capture_cases():
            with self.subTest(case=case["name"]):
                self.assertEqual(case["valid"],case["python_valid"])

    def test_migration_derives_full_watch_admission_and_separate_hashes(self):
        sql = MIGRATION.read_text()
        derivation = sql.split("CREATE OR REPLACE FUNCTION research_stage8_derive_projection_attestation_v1",1)[1].split("CREATE OR REPLACE FUNCTION research_stage8_fact_insert_guard_v1",1)[0]
        for expected in ("research_stage8_watch_capture_errors_v1(",
                         "research_stage8_watch_json_sha256_v1(model)",
                         "research_stage8_watch_json_sha256_v1(model_source)",
                         "source_slot.coverage_snapshot IS DISTINCT FROM source_attempt.coverage_snapshot",
                         "source_slot.frozen_inputs IS DISTINCT FROM source_attempt.frozen_inputs",
                         "ANCHOR_DECISION_INTERVAL_OR_FEATURE_INVALID"):
            self.assertIn(expected,derivation)
        self.assertIn("REPRESENTATIVE_OUTCOME_HORIZON_NOT_ELAPSED",sql)
        self.assertIn("AND representative_horizons_elapsed",sql)
        self.assertIn("evidence->'read_finished_at_utc') > database_clock",sql)

    def test_watch_helper_acl_is_not_public(self):
        sql = MIGRATION.read_text()
        for name in ("canonical_json","json_sha256","round2","timestamp",
                     "finite_number","components_match","capture_errors"):
            self.assertIn("REVOKE EXECUTE ON FUNCTION research_stage8_watch_"+name+"_v1(",sql)
            self.assertGreaterEqual(sql.count("research_stage8_watch_"+name+"_v1("),4)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),"TEST_DATABASE_URL not configured")
class WatchSqlNativeParityTests(unittest.TestCase):
    def test_capture_vectors_in_rolled_back_native_schema(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict

        dsn = os.environ["TEST_DATABASE_URL"]
        target = conninfo_to_dict(dsn)
        if (target.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (target.get("dbname", "").startswith("test_")
                        or target.get("dbname", "").endswith("_test"))):
            raise ValueError(
                "Watch parity requires an explicit local/CI test database"
            )
        connection = psycopg.connect(dsn)
        try:
            with connection.cursor() as cursor:
                schema = "stage8_watch_parity_"+uuid4().hex
                cursor.execute('CREATE SCHEMA "'+schema+'"')
                cursor.execute('SET LOCAL search_path TO "'+schema+'", pg_catalog')
                cursor.execute(helper_sql())
                for case in watch_capture_cases():
                    snapshot=case["snapshot"]
                    block=_block(snapshot)
                    cursor.execute("SELECT research_stage8_watch_capture_errors_v1(%s::jsonb,%s,%s::jsonb,%s,%s::timestamptz,%s::timestamptz,%s::timestamptz)",
                        (json.dumps(block),case["symbol"],json.dumps(block["code_sha256"]),snapshot["cycle_id"],case["decision"],snapshot["available_at_utc"],snapshot["created_at_utc"]))
                    with self.subTest(case=case["name"]):
                        self.assertEqual(not cursor.fetchone()[0],case["valid"])
        finally:
            connection.rollback()
            connection.close()


if __name__ == "__main__":
    unittest.main()
