"""Persisted anchor admission parity, with optional native PostgreSQL 18 gates.

Vectors use production producers and coherently rehash semantic forgeries.
The native test installs only pure migration helpers in a rolled-back random
schema. PGlite is explicitly excluded from this native evidence gate.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from uuid import uuid4

import research_operational_score_source_audit as audit
from research_operational_score_source_audit_selftest import _anchor
import research_prospective_anchors as anchors
from research_prospective_anchors_selftest import _feature_bundle_entry
import research_prospective_feature_freeze as features
import research_session_width as width
from research_session_width_selftest import _series


MIGRATION = Path(__file__).with_name("migrations") / "053_stage8_durable_registry.sql"
UTC = timezone.utc
INPUT_FIELDS = (
    "sampler_version", "coverage_policy_version", "coverage_snapshot", "symbol",
    "source_candle_open_utc", "source_candle_close_utc", "base_eligible_at_utc",
    "expires_at_utc", "evaluation_status", "decision_time_utc", "source_timestamps",
    "source_provenance", "frozen_inputs", "feature_bundle_policy_version",
    "feature_bundle_sha256",
)
SHARED_FIELDS = (
    "coverage_snapshot", "source_timestamps", "source_provenance", "frozen_inputs",
    "feature_bundle_policy_version", "feature_bundle_sha256", "input_fingerprint",
)


def _json(value):
    # Keep the legacy producer's float/int distinction, not the Stage-8 codec.
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def rehash_anchor(attempt, slot, events):
    """Make a semantic forgery hash-consistent without fixing its semantics."""
    slot["feature_bundle_sha256"] = features.compute_feature_bundle_sha256(
        slot["decision_feature_bundle"]
    )
    for key in SHARED_FIELDS:
        attempt[key] = deepcopy(slot[key])
    fingerprint = anchors.compute_input_fingerprint(**{
        key: attempt[key] for key in INPUT_FIELDS
    })
    attempt["input_fingerprint"] = slot["input_fingerprint"] = fingerprint
    for event in events:
        ref = event["engine_snapshot"]["prospective_anchor"]
        for key in SHARED_FIELDS:
            ref[key] = deepcopy(slot[key])
    attempt["attempt_fingerprint"] = anchors._sha256({
        "sampler_version": attempt["sampler_version"],
        "coverage_policy_version": attempt["coverage_policy_version"],
        "symbol": attempt["symbol"],
        "slot_open_utc": anchors._iso(attempt["source_candle_open_utc"]),
        "status": attempt["evaluation_status"],
        "reason": attempt["evaluation_reason"],
        "input_fingerprint": fingerprint,
        "event_fingerprints": [event["event_fingerprint"] for event in events],
    })


def anchor_cases():
    cases = []

    def add(name, mutate=None, *, valid=False, symbol="BTC", rehash=True, after_rehash=None):
        attempt, slot, events = deepcopy(_anchor(symbol=symbol))
        if mutate:
            mutate(attempt, slot, events)
        if rehash:
            rehash_anchor(attempt, slot, events)
        if after_rehash:
            after_rehash(attempt, slot, events)
        cases.append({"name": name, "attempt": attempt, "slot": slot,
                      "events": events, "valid": valid})

    add("producer_btc", valid=True)
    add("producer_hype_spot_107", valid=True, symbol="HYPE")
    add("frozen_numeric_string_is_supported", lambda a,s,e:
        s["frozen_inputs"]["official_price"].update(price="100.25"),valid=True)
    for numeric_string, valid in (("0x1p+0",False),("+1.25e2",True),
                                  (".5",True),("\t+1.25e2\u00a0",True)):
        add("source_numeric_string_"+repr(numeric_string),
            lambda a,s,e,value=numeric_string:
            s["frozen_inputs"]["price_oi"].update(price_close=value),valid=valid)
    for separator in range(0x1c,0x20):
        add(f"source_numeric_string_ascii_separator_{separator:02x}",
            lambda a,s,e,separator=separator:
            s["frozen_inputs"]["price_oi"].update(
                price_close=chr(separator)+"1.25"+chr(separator)))
    add("event_reference_integral_float_matches_int", valid=True, after_rehash=lambda a,s,e:
        e[0]["engine_snapshot"]["prospective_anchor"]["frozen_inputs"]["price_oi"].update(price_close=100))
    add("event_reference_binary64_large_integer", lambda a,s,e:
        s["source_provenance"]["price_oi"].update(source_record_id=9007199254740994),
        valid=True,after_rehash=lambda a,s,e:
        e[0]["engine_snapshot"]["prospective_anchor"]["source_provenance"]["price_oi"].update(source_record_id=9007199254740994.0))
    for key in ("coverage_snapshot", "source_timestamps", "source_provenance"):
        add("empty_" + key, lambda a, s, e, key=key: s.update({key: {}}))
    add("coverage_last_horizon_missing", lambda a,s,e:
        s["coverage_snapshot"]["horizons"].pop("1440"))
    add("coverage_version_forged", lambda a,s,e:
        s["coverage_snapshot"].update(method_version="forged"))
    add("coverage_replay_future", lambda a,s,e:
        s["coverage_snapshot"].update(replay_completed_at_utc=anchors._iso(
            a["decision_time_utc"] + timedelta(microseconds=1))))
    add("coverage_span_not_derived", lambda a,s,e:
        s["coverage_snapshot"]["horizons"]["60"].update(span_hours=501.0))
    for field, values in (("anchors", ("300", 300.0, 300.5, True)),
                          ("utc_dates", ("18", 18.0, True)),
                          ("span_hours", ("500.0", True))):
        for value in values:
            add(f"coverage_{field}_{type(value).__name__}_{value}",
                lambda a,s,e,field=field,value=value:
                s["coverage_snapshot"]["horizons"]["60"].update({field: value}))
    add("frozen_family_missing", lambda a,s,e: s["frozen_inputs"].pop("spot_cvd"))
    add("source_refresh_future", lambda a,s,e:
        s["source_timestamps"]["futures_cvd"].update(
            refresh_completed_at_utc=anchors._iso(
                a["decision_time_utc"] + timedelta(microseconds=1))))
    add("source_naive_clock_is_supported", lambda a,s,e:
        s["source_timestamps"]["futures_cvd"].update(refresh_completed_at_utc=
            anchors._utc(s["source_timestamps"]["futures_cvd"]["refresh_completed_at_utc"])
                .replace(tzinfo=None).isoformat()),valid=True)
    add("source_exchange_set_forged", lambda a,s,e:
        s["source_provenance"]["spot_cvd"].update(exchange_list="Binance"))
    add("source_official_fallback", lambda a,s,e:
        s["source_provenance"]["official_price"].update(fallback_used=True))
    add("source_official_unicode_letter_pair", lambda a,s,e:
        s["source_provenance"]["official_price"].update(price_pair="BTCéUSDT"))
    add("source_official_fullwidth_digit_pair", lambda a,s,e:
        s["source_provenance"]["official_price"].update(price_pair="BTC１USDT"))
    add("source_null_uses_frozen_fallback",lambda a,s,e:(
        s["source_provenance"]["official_price"].update(source=None),
        s["frozen_inputs"]["official_price"].update(source="binance_spot")),valid=True)
    add("source_empty_quality_uses_data_quality_fallback",lambda a,s,e:(
        s["source_provenance"]["official_price"].update(quality_status=""),
        s["frozen_inputs"]["official_price"].update(quality_status="",data_quality_status="PASS")),valid=True)
    add("source_oi_upstream_future", lambda a,s,e:
        s["source_timestamps"]["price_oi"].update(
            oi_fetched_at_utc=anchors._iso(a["decision_time_utc"] + timedelta(seconds=1))))
    add("source_price_missing", lambda a,s,e:
        s["frozen_inputs"]["official_price"].pop("price"))
    add("feature_extra_root", lambda a,s,e:
        s["decision_feature_bundle"].update(extra=True))
    add("feature_schema_numeric_nonempty",lambda a,s,e:
        s["decision_feature_bundle"].update(feature_schema_version=123),valid=True)
    add("feature_arbitrary_precision_integer",lambda a,s,e:
        s["decision_feature_bundle"]["features_by_direction"]["LONG"].update({
            "raw.60m.price_change_pct":10**400}),valid=True)
    for whitespace in ("\t", "\u00a0"):
        add("feature_schema_whitespace_"+str(ord(whitespace)),lambda a,s,e,w=whitespace:
            s["decision_feature_bundle"].update(feature_schema_version=w))
        add("evaluation_reason_whitespace_"+str(ord(whitespace)),lambda a,s,e,w=whitespace:
            a.update(evaluation_reason=w))
        add("forbidden_width_key_whitespace_"+str(ord(whitespace)),lambda a,s,e,w=whitespace:
            s["decision_feature_bundle"]["horizon_context"]["60"]["movement_width_reference"].update({w+"mfe_pct"+w:1.0}))
    add("feature_missing_opposite_direction", lambda a,s,e:
        s["decision_feature_bundle"]["features_by_direction"].pop("LONG"))
    for name, value in (("outcome.mfe_pct", 1.0), ("sequence.30m.count", 2),
                        ("model.fake.score", 70), ("raw.60m.price_change_pct", {})):
        add("feature_forbidden_" + name, lambda a,s,e,name=name,value=value:
            s["decision_feature_bundle"]["features_by_direction"]["LONG"].update({name: value}))
    add("manifest_empty_but_nonnull_bounds", lambda a,s,e:
        s["decision_feature_bundle"]["source_series_manifest"].update(count=0))
    add("manifest_unknown_sampler", lambda a,s,e:
        s["decision_feature_bundle"]["source_series_manifest"].update(sampler_versions=["unknown"]))
    add("manifest_duplicate_sampler", lambda a,s,e:
        s["decision_feature_bundle"]["source_series_manifest"].update(
            sampler_versions=[anchors.SAMPLER_VERSION, anchors.SAMPLER_VERSION]))
    add("manifest_future", lambda a,s,e:
        s["decision_feature_bundle"]["source_series_manifest"].update(
            last_decision_time_utc=anchors._iso(a["decision_time_utc"] + timedelta(seconds=1))))
    add("manifest_integral_float_count", lambda a,s,e:
        s["decision_feature_bundle"]["source_series_manifest"].update(count=1.0))
    add("feature_wrong_hash", lambda a,s,e: s.update(feature_bundle_sha256="0"*64), rehash=False)
    add("anchor_input_wrong_hash", lambda a,s,e: s.update(input_fingerprint="0"*64), rehash=False)
    add("event_pair_duplicate", lambda a,s,e: s.update(long_event_id=s["short_event_id"]))
    add("event_pair_missing_long", lambda a,s,e: e.pop(0))
    add("attempt_naive_decision_clock", lambda a,s,e:
        a.update(decision_time_utc=a["decision_time_utc"].replace(tzinfo=None).isoformat()))
    add("unselected_long_naive_clock", lambda a,s,e:
        e[0].update(alert_time_utc=anchors._utc(e[0]["alert_time_utc"])
                   .replace(tzinfo=None).isoformat()))
    add("unselected_long_surrounding_clock_whitespace", lambda a,s,e:
        e[0].update(alert_time_utc=" "+anchors._iso(e[0]["alert_time_utc"])+" "))
    add("unselected_long_reference_naive_clock", lambda a,s,e:
        e[0]["engine_snapshot"]["prospective_anchor"].update(decision_time_utc=
            a["decision_time_utc"].replace(tzinfo=None).isoformat()))
    add("unselected_long_snapshot_oversized", lambda a,s,e:
        e[0]["engine_snapshot"].update(irrelevant_padding="x"*32001))
    # Binding used by registry integration selects SHORT; corrupt LONG only.
    for field, value in (("event_fingerprint", "f"*64), ("delivery_status", "DELIVERED"),
                         ("current_price", 1.0), ("score", 1.0),
                         ("capture_stage", "POST_SEND")):
        add("unselected_long_"+field, lambda a,s,e,field=field,value=value:
            e[0].update({field: value}))
    for field, value in (("anchor_key", "0"*64), ("telegram_delivery_allowed", True),
                         ("trade_execution_allowed", True), ("coverage_eligible", "true")):
        add("unselected_long_reference_"+field, lambda a,s,e,field=field,value=value:
            e[0]["engine_snapshot"]["prospective_anchor"].update({field: value}))
    return cases


def bundle_cases():
    cases = []

    def add(name, decision, *, mutate=None, historical_index=None, valid=True):
        bundle = _feature_bundle_entry(decision)["decision_feature_bundle"]
        if historical_index is not None:
            for horizon in features.REQUIRED_HORIZONS:
                ref = width.movement_width_reference(
                    symbol="BTC", event_time=decision, horizon_minutes=horizon,
                    as_of_utc=decision-timedelta(microseconds=1),
                    historical_index=historical_index)
                ref["as_of_utc"] = anchors._iso(ref["as_of_utc"])
                bundle["horizon_context"][str(horizon)] = {
                    "movement_width_reference": ref,
                    "session": {"active_ratio": ref["session_active_ratio"],
                                "weekend_ratio": ref["session_weekend_ratio"],
                                "composition": ref["session_composition"],
                                "segments": ref["session_segments"]}}
        if mutate:
            mutate(bundle)
        cases.append({"name": name, "bundle": bundle, "decision": anchors._iso(decision),
                      "sha256": features.compute_feature_bundle_sha256(bundle), "valid": valid})

    weekend = datetime(2026,8,29,12,tzinfo=UTC)
    samples = []
    for index in range(15):
        monday = datetime(2026,5,4,12,tzinfo=UTC) + timedelta(weeks=index)
        saturday = monday + timedelta(days=5)
        samples.extend(((monday,2.0,1.0), (monday+timedelta(hours=2),2.0,1.0),
                        (saturday,0.4,0.0), (saturday+timedelta(hours=2),0.4,0.0)))
    history = {("BTC",60): _series(samples)}
    add("unavailable_history", weekend)
    add("calibrated_weekend", weekend, historical_index=history)
    add("insufficient_history", weekend, historical_index={("BTC",60): _series(samples[:10])})
    add("active_history", datetime(2026,8,24,12,tzinfo=UTC), historical_index=history)
    add("empty_manifest", weekend, mutate=lambda b:b["source_series_manifest"].update(
        count=0,first_decision_time_utc=None,last_decision_time_utc=None,sampler_versions=[]))
    for decision in (datetime(2026,3,8,21,tzinfo=UTC), datetime(2026,11,1,22,tzinfo=UTC),
                     datetime(2026,3,8,22,tzinfo=UTC)-timedelta(seconds=1),
                     datetime(2026,3,8,22,tzinfo=UTC)):
        add("dst_"+decision.isoformat(), decision)
    add("session_width_forged_together", datetime(2026,8,24,12,tzinfo=UTC), valid=False,
        mutate=lambda b:(b["horizon_context"]["60"]["session"].update(
            active_ratio=0.0,weekend_ratio=1.0,composition="WEEKEND_ONLY"),
            b["horizon_context"]["60"]["movement_width_reference"].update(
                session_active_ratio=0.0,session_weekend_ratio=1.0,session_composition="WEEKEND_ONLY")))
    for field, value in (("horizon_minutes",60.0), ("session_segments",True),
                         ("threshold_scale_factor","0.5"), ("floor_scale_factor",0.9),
                         ("applied","true"), ("session_matched_samples",1000),
                         ("session_matched_effective_samples",1000.0),
                         ("reason","forged"), ("as_of_utc",anchors._iso(weekend+timedelta(seconds=1)))):
        add("width_"+field, weekend, valid=False, historical_index=history,
            mutate=lambda b,field=field,value=value:
                b["horizon_context"]["60"]["movement_width_reference"].update({field:value}))
    return cases


def digit_limit_cases():
    """Raw tokens avoid asking this process to parse oversized integers."""
    decision = datetime(2026,8,29,12,tzinfo=UTC)
    bundle = _feature_bundle_entry(decision)["decision_feature_bundle"]
    marker = "ANCHOR_INTEGER_DIGIT_BOUNDARY"
    bundle["features_by_direction"]["LONG"]["raw.60m.price_change_pct"] = marker
    template = features._canonical(bundle)
    cases = []
    for digits in (4300,4301):
        for sign in ("", "-"):
            token = sign+"1"+"0"*(digits-1)
            raw_bundle = template.replace('"'+marker+'"',token)
            cases.append({"name":f"integer_{digits}_{'negative' if sign else 'positive'}",
                          "raw_json":'{"x":'+token+'}',"raw_bundle":raw_bundle,
                          "decision":anchors._iso(decision),"valid":digits==4300,
                          "sha256":hashlib.sha256(raw_bundle.encode()).hexdigest()})
    return cases


def python_digit_limit_oracle(case):
    # -I ignores ambient Python configuration; -X pins the production 3.11
    # default in this child only. Never change the test runner's global limit.
    code = """
import hashlib,json,sys
sys.path.insert(0,sys.argv[1])
import research_prospective_anchors as anchors
import research_prospective_feature_freeze as features
payload=json.loads(sys.stdin.read())
try:
    value=json.loads(payload['raw_json'])
    canonical=anchors._canonical(value)
    bundle=json.loads(payload['raw_bundle'])
    valid,reason=features.validate_feature_bundle(bundle,
        expected_sha256=payload['sha256'],expected_symbol='BTC',
        expected_decision_time_utc=payload['decision'])
    result={'valid':valid,'reason':reason,'canonical':canonical,
            'sha256':hashlib.sha256(canonical.encode()).hexdigest()}
except ValueError:
    result={'valid':False,'exception':'ValueError'}
result['integer_digit_limit']=sys.get_int_max_str_digits()
print(json.dumps(result))
"""
    completed = subprocess.run(
        [sys.executable,"-I","-X","int_max_str_digits=4300","-c",code,
         str(Path(__file__).resolve().parent)],input=json.dumps(case),
        capture_output=True,text=True,check=True,timeout=15)
    return json.loads(completed.stdout)


class AnchorSqlParityContractTests(unittest.TestCase):
    def test_coherently_rehashed_anchor_vectors_have_expected_python_admission(self):
        for case in anchor_cases():
            with self.subTest(case=case["name"]):
                result = audit.validate_anchor_authority(case["attempt"],case["slot"],case["events"])
                self.assertEqual(result["status"] == "VALID", case["valid"], result)

    def test_bundle_and_session_width_vectors_have_expected_python_admission(self):
        for case in bundle_cases():
            with self.subTest(case=case["name"]):
                valid, reason = features.validate_feature_bundle(case["bundle"],
                    expected_symbol="BTC",expected_decision_time_utc=case["decision"],
                    expected_sha256=case["sha256"])
                self.assertEqual(valid,case["valid"],reason)
                if case["name"] == "dst_2026-03-08T21:00:00+00:00":
                    session = case["bundle"]["horizon_context"]["240"]["session"]
                    self.assertEqual((session["active_ratio"],session["weekend_ratio"]),(0.75,0.25))

    def test_python_integer_digit_boundary_without_global_setting_changes(self):
        original_limit = sys.get_int_max_str_digits()
        for case in digit_limit_cases():
            with self.subTest(case=case["name"]):
                oracle = python_digit_limit_oracle(case)
                self.assertEqual(oracle["integer_digit_limit"],4300)
                self.assertIs(oracle["valid"],case["valid"],oracle)
                if case["valid"]:
                    self.assertEqual(oracle["canonical"],case["raw_json"])
                else:
                    self.assertEqual(oracle["exception"],"ValueError")
        self.assertEqual(sys.get_int_max_str_digits(),original_limit)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL") and
                     os.environ.get("STAGE8_PGLITE_COMPAT") != "1",
                     "Native PostgreSQL 18 TEST_DATABASE_URL required; not a PGlite gate")
class AnchorSqlNativeParityTests(unittest.TestCase):
    def test_jsonb_readback_anchor_bundle_and_hash_parity(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict

        dsn = os.environ["TEST_DATABASE_URL"]
        target = conninfo_to_dict(dsn)
        if (target.get("host") not in {"localhost","127.0.0.1","::1","postgres"}
                or not (target.get("dbname","").startswith("test_")
                        or target.get("dbname","").endswith("_test"))):
            raise ValueError("Anchor parity requires an explicit local/CI test database")
        conn = psycopg.connect(dsn,connect_timeout=5,
            options="-c statement_timeout=15000 -c TimeZone=UTC")
        try:
            version = conn.execute("SHOW server_version_num").fetchone()[0]
            self.assertEqual(int(version)//10000,18,"Native anchor gate requires PostgreSQL 18")
            schema = "test_stage8_anchor_parity_"+uuid4().hex
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {},pg_catalog").format(sql.Identifier(schema)))
            helper_sql = MIGRATION.read_text().split(
                "CREATE TABLE IF NOT EXISTS research_stage8_binding_registry",1)[0]
            conn.execute(helper_sql,prepare=False)
            # Idempotency must preserve semantics, not just permit creation.
            conn.execute(helper_sql,prepare=False)
            for case in anchor_cases():
                with self.subTest(anchor=case["name"]):
                    values = [_json(case[key]) for key in ("attempt","slot","events")]
                    persisted = conn.execute("SELECT %s::jsonb,%s::jsonb,%s::jsonb",values).fetchone()
                    py = audit.validate_anchor_authority(*persisted)
                    self.assertEqual(py["status"] == "VALID",case["valid"],py)
                    errors = conn.execute("SELECT research_stage8_anchor_errors_v1(%s::jsonb,%s::jsonb,%s::jsonb)",values).fetchone()[0]
                    self.assertIsInstance(errors,list)
                    self.assertEqual(not errors,case["valid"],errors)
            for case in bundle_cases():
                with self.subTest(bundle=case["name"]):
                    result = conn.execute("SELECT research_stage8_anchor_bundle_valid_v1(%s::jsonb,%s,%s::timestamptz,%s)",
                        (_json(case["bundle"]),"BTC",case["decision"],case["sha256"])).fetchone()[0]
                    self.assertIs(result,case["valid"])
            golden = (
                ('{"x":1}',"5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22"),
                ('{"x":1.0}',"bf32f56236899e13ef54db875d136c6cbcc65244464829c54911aa9069b0ae25"),
                ('{"x":1e-7}',"c8b4301d31692cc55fc58ee2d4368e95e4971ac5d25036abceb45c33a05b0fcb"),
                ('{"é":1,"א":2,"😀":3,"A":4}',"fd56e1ca1554d14a39f67244f42d2177b08f5022073e8b0598f34a85d39632e2"),
                ('{"x":1e20,"zero":-0.0,"fraction":1.2345678901234567}',None),
            )
            for raw, expected in golden:
                with self.subTest(numeric=raw):
                    persisted, encoded, digest = conn.execute("SELECT %s::jsonb,research_stage8_anchor_canonical_json_v1(%s::jsonb),research_stage8_anchor_json_sha256_v1(%s::jsonb)",
                        (raw,raw,raw)).fetchone()
                    canonical = anchors._canonical(persisted)
                    self.assertEqual(encoded,canonical)
                    self.assertEqual(digest,hashlib.sha256(canonical.encode()).hexdigest())
                    if expected:
                        self.assertEqual(digest,expected)
            # JSONB loses an exponent-valued integral float's original type.
            # Never rescue its old feature hash by trying multiple encodings.
            lossy = _feature_bundle_entry(datetime(2026,8,29,12,tzinfo=UTC))["decision_feature_bundle"]
            lossy["features_by_direction"]["LONG"]["raw.60m.price_change_pct"] = 1e20
            producer_hash = features.compute_feature_bundle_sha256(lossy)
            persisted = conn.execute("SELECT %s::jsonb",(_json(lossy),)).fetchone()[0]
            python_valid, _ = features.validate_feature_bundle(persisted,expected_sha256=producer_hash)
            self.assertIs(python_valid,False)
            admitted = conn.execute("SELECT research_stage8_anchor_bundle_valid_v1(%s::jsonb,%s,%s::timestamptz,%s)",
                (_json(lossy),"BTC",lossy["decision_time_utc"],producer_hash)).fetchone()[0]
            self.assertIs(admitted,False)
            for case in digit_limit_cases():
                with self.subTest(integer_boundary=case["name"]):
                    oracle = python_digit_limit_oracle(case)
                    self.assertIs(oracle["valid"],case["valid"],oracle)
                    # Returning text avoids the driver's Python JSON parser
                    # rejecting 4301 digits before the SQL gate is exercised.
                    raw = conn.execute("SELECT (%s::jsonb)::text",(case["raw_json"],)).fetchone()[0]
                    for helper in ("research_stage8_anchor_canonical_json_v1",
                                   "research_stage8_anchor_reference_canonical_json_v1"):
                        statement = sql.SQL("SELECT {}(%s::jsonb)").format(sql.Identifier(helper))
                        if case["valid"]:
                            actual = conn.execute(statement,(raw,)).fetchone()[0]
                            self.assertEqual(actual,oracle["canonical"])
                        else:
                            conn.execute("SAVEPOINT anchor_integer_boundary")
                            with self.assertRaises(psycopg.Error):
                                conn.execute(statement,(raw,))
                            conn.execute("ROLLBACK TO SAVEPOINT anchor_integer_boundary")
                            conn.execute("RELEASE SAVEPOINT anchor_integer_boundary")
                    admitted = conn.execute("SELECT research_stage8_anchor_bundle_valid_v1(%s::jsonb,%s,%s::timestamptz,%s)",
                        (case["raw_bundle"],"BTC",case["decision"],case["sha256"])).fetchone()[0]
                    self.assertIs(admitted,case["valid"])
        finally:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
