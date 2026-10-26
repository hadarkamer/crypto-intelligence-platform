"""Selected-slot shadow correctness, fail-open boundaries and isolated persistence."""
from copy import deepcopy
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime, timezone, timedelta
import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4
import zlib

import research_combined_liquidity_shadow as shadow
import research_consumed_inputs as graph
import research_event_capture as capture

START = "2026-10-26T12:00:00+00:00"
NOW = "2026-10-26T13:00:00+00:00"
WHEN = datetime(2026, 10, 26, 12, 37, 7, tzinfo=timezone.utc)


@contextmanager
def temp_workspace():
    root = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix="_shadow_selftest_", dir=root) as directory:
        resolved = Path(directory).resolve()
        assert resolved.is_relative_to(root) and resolved != root
        try:
            yield directory
        finally:
            assert resolved.is_relative_to(root) and resolved != root


def fixture(*, side="SHORT", near=492970.0, far=595720.0):
    price = 0.07748
    target = 0.078814 if side == "SHORT" else 0.076
    slot = {"timeframe": "48h", "source_side": side, "score": 59.69,
            "target_price": target, "near_amount": near, "far_amount": far,
            "near_share_pct": 100 * near / (near + far)}
    top = {"symbol": "DOGE", "side": side, "timeframe": "48h", "score": 59.69,
           "current_price": price, "target_price": target,
           "maxpain_timeframes": [deepcopy(slot)]}
    event = capture.build_generic_alert_event(symbol="DOGE", event_type="COMBINED_CONFIRMATION",
        direction="LONG" if side == "SHORT" else "SHORT", source_side=side, event_time=WHEN,
        timeframe="48h", score=59.69, current_price=price, target_price=target,
        categories=["three_anomalies:48h"], engine_snapshot={"alert_side": side,
            "watch_scan_id": "shared-watch:fixture", "maxpain_timeframes": [deepcopy(slot)]},
        strategy_version="fixture-strategy", code_version="fixture-commit")
    return event, top


def prepared(event=None, top=None, **overrides):
    if event is None:
        event, top = fixture()
    metadata = {"event_writer_queue_accepted": True, "capture_stage": "TELEGRAM_COMBINED_ALERT",
                "delivery_status": "DELIVERED", "delivery_attempted_at_utc": WHEN,
                "delivered_at_utc": WHEN + timedelta(seconds=1)}
    metadata.update(overrides)
    with patch.object(shadow, "PROCESS_STARTED_AT_UTC", START), patch.object(shadow, "utc_now", return_value=NOW):
        return shadow.prepare_capture(event, top, **metadata)


def payloads(obs):
    return {header["kind"]: value for header, value in graph.event_payloads(shadow.package_from_observation(obs))}


def source_row(obs, event_id=77):
    row = deepcopy(payloads(obs)["producer_input"]["source_material"])
    row["event_id"] = event_id
    return row


def remake(obs, edit, manifest_edit=None):
    values = payloads(obs)
    edit(values)
    c = graph.Collector(max_raw_bytes=shadow.MAX_RAW_BYTES, max_compressed_bytes=shadow.MAX_COMPRESSED_BYTES)
    for kind, value in values.items():
        c.record(kind, value)
    package = c.package()
    result = deepcopy(obs)
    result["objects"] = package["objects"]
    result["manifest"].update(events=package["events"], errors=package["errors"], object_sha256s=sorted(package["objects"]))
    if manifest_edit:
        manifest_edit(result["manifest"], values)
    result["root_sha256"] = graph.digest(result["manifest"])
    return result


class ShadowTests(unittest.TestCase):
    def test_exact_selected_slot_without_feature_injection(self):
        event, top = fixture()
        original = deepcopy(event.to_dict()), deepcopy(top)
        obs = prepared(event, top)
        data = payloads(obs)
        context = data["derived_context"]
        self.assertEqual(context["shadow.combined_liquidity.capture_status"], "VALID")
        self.assertAlmostEqual(context["shadow.combined_liquidity.selected_share_pct"], 45.28102582002223)
        self.assertEqual(context["shadow.combined_liquidity.alignment"], "BALANCED")
        self.assertNotIn("near_amount", event.engine_snapshot)
        self.assertEqual((event.to_dict(), top), original)
        self.assertFalse(obs["manifest"]["enrolled_in_existing_study"])

    def test_both_native_side_direction_mappings(self):
        for side in ("LONG", "SHORT"):
            event, top = fixture(side=side)
            self.assertEqual(shadow.derive_context(event.to_dict(), top)["shadow.combined_liquidity.capture_status"], "VALID")

    def test_thresholds_inclusive_and_no_score65_gate(self):
        for share, expected in ((60, "SUPPORTS"), (40, "OPPOSES"), (59, "BALANCED"), (41, "BALANCED")):
            event, top = fixture(near=share, far=100-share)
            context = shadow.derive_context(event.to_dict(), top)
            self.assertEqual(context["shadow.combined_liquidity.alignment"], expected)
            self.assertEqual(event.score, 59.69)

    def test_ambiguous_raw_slot_not_silently_compacted(self):
        event, top = fixture()
        top["maxpain_timeframes"].append(deepcopy(top["maxpain_timeframes"][0]))
        result = shadow.derive_context(event.to_dict(), top)
        self.assertEqual(result["shadow.combined_liquidity.reason"], "SELECTED_SLOT_AMBIGUOUS")
        self.assertEqual(prepared(event, top)["manifest"]["capture_status"], "CAPTURED")

    def test_missing_slot_is_complete_capture_not_valid_negative(self):
        event, top = fixture()
        top.pop("maxpain_timeframes")
        obs = prepared(event, top)
        self.assertEqual(obs["manifest"]["capture_status"], "CAPTURED")
        self.assertEqual(payloads(obs)["derived_context"]["shadow.combined_liquidity.capture_status"], "MISSING")

    def test_tuple_price_target_score_slot_and_root_mismatches(self):
        edits = [lambda e,t:t.update(side="LONG"), lambda e,t:t.update(symbol="ETH"),
                 lambda e,t:t.update(timeframe="24h"), lambda e,t:t.update(current_price=0.1),
                 lambda e,t:t.update(score=60), lambda e,t:t["maxpain_timeframes"][0].update(near_amount=1),
                 lambda e,t:t.update(near_amount=1)]
        for edit in edits:
            event, top = fixture()
            edit(event, top)
            self.assertEqual(shadow.derive_context(event.to_dict(), top)["shadow.combined_liquidity.capture_status"], "UNVERIFIED")

    def test_invalid_amounts_boolean_nonfinite_and_share(self):
        for key, value in (("near_amount", -1), ("near_amount", True), ("far_amount", float("inf")), ("near_share_pct", 10)):
            event, top = fixture()
            raw = event.to_dict()
            raw["engine_snapshot"]["maxpain_timeframes"][0][key] = value
            top["maxpain_timeframes"][0][key] = value
            self.assertNotEqual(shadow.derive_context(raw, top)["shadow.combined_liquidity.capture_status"], "VALID")

    def test_wrong_analysis_direction_or_target_is_diagnostic(self):
        event, top = fixture()
        self.assertEqual(shadow.derive_context(replace(event, direction="SHORT").to_dict(), top)["shadow.combined_liquidity.reason"], "DIRECTION_MAPPING_INVALID")
        self.assertEqual(shadow.derive_context(replace(event, target_price=0.07).to_dict(), top)["shadow.combined_liquidity.reason"], "TARGET_DIRECTION_INVALID")

    def test_only_current_doge_delivered_queued_calls(self):
        event, top = fixture()
        with patch.object(shadow, "record_skip") as skip:
            self.assertIsNone(prepared(event, top, event_writer_queue_accepted=False))
            self.assertEqual(skip.call_args.args, ("SOURCE_NOT_QUEUED",))
            self.assertIsNone(prepared(event, top, delivery_status="DELIVERY_FAILED"))
            self.assertIsNone(prepared(replace(event, symbol="ETH"), top))
        with patch.object(shadow, "PROCESS_STARTED_AT_UTC", "2026-10-26T14:00:00+00:00"):
            self.assertIsNone(shadow.prepare_capture(event, top, event_writer_queue_accepted=True))

    def test_original_float_and_nested_datetime_bridge(self):
        event, top = fixture()
        top["non_formula_metadata"] = {"unit_float": 1.0, "received": datetime(2026,10,26,14,0,tzinfo=timezone(timedelta(hours=2)))}
        obs = prepared(event, top)
        value = payloads(obs)["producer_input"]
        self.assertIn('"unit_float":1.0', value["legacy_selected_top_item_canonical"])
        self.assertIn("2026-10-26 14:00:00+02:00", value["legacy_selected_top_item_canonical"])
        self.assertEqual(value["selected_top_item"]["non_formula_metadata"]["received"], "2026-10-26T12:00:00+00:00")

    def test_immediate_freeze(self):
        event, top = fixture()
        obs = prepared(event, top)
        top["maxpain_timeframes"][0]["near_amount"] = 0
        event.engine_snapshot["maxpain_timeframes"][0]["near_amount"] = 0
        self.assertEqual(payloads(obs)["derived_context"]["shadow.combined_liquidity.selected_amount"], 492970)

    def test_unsupported_extra_is_partial(self):
        event, top = fixture()
        top["unsupported_extra"] = {"set"}
        obs = prepared(event, top)
        self.assertEqual(obs["manifest"]["capture_status"], "PARTIAL")
        self.assertTrue(obs["manifest"]["errors"])

    def test_scope_epoch_manifest_and_object_tampering(self):
        for key, value in (("enrolled_in_existing_study", True), ("source_alert_time_utc", "2026-10-26T11:00:00+00:00")):
            obs = prepared()
            obs["manifest"][key] = value
            obs["root_sha256"] = graph.digest(obs["manifest"])
            with self.assertRaises(ValueError): shadow.package_from_observation(obs)
        obs = prepared()
        next(iter(obs["objects"].values()))["compressed_b64"] = "AAAA"
        with self.assertRaises(ValueError): shadow.package_from_observation(obs)

    def test_rehashed_derived_output_is_rejected(self):
        obs = remake(prepared(), lambda p:p["derived_context"].update({"shadow.combined_liquidity.selected_share_pct":99}))
        with self.assertRaisesRegex(ValueError,"SEMANTIC"): shadow.package_from_observation(obs)

    def test_rehashed_event_not_linked_to_different_serialized_body(self):
        def edit(p):
            source = p["producer_input"]
            source["event"]["current_price"] = source["selected_top_item"]["current_price"] = 0.077
            for name in ("event", "selected_top_item"):
                source["legacy_"+name+"_canonical"], source["legacy_"+name+"_datetime_paths"] = shadow.legacy_material(source[name])
            p["derived_context"] = shadow.derive_context(source["event"], source["selected_top_item"])
        def edit_manifest(m,p):
            source=p["producer_input"]
            for name, key in (("event","original_event_canonical_sha256"),("selected_top_item","selected_top_item_canonical_sha256")):
                m[key] = hashlib.sha256(source["legacy_"+name+"_canonical"].encode()).hexdigest()
        obs = remake(prepared(), edit, edit_manifest)
        with self.assertRaisesRegex(ValueError,"EVENT_SOURCE_BINDING"): shadow.package_from_observation(obs)

    def test_rehashed_missing_event_field_or_source_identity_rejected(self):
        for edit in (lambda p:p["producer_input"]["event"].pop("categories"),lambda p:p["identity"]["code_versions"].clear()):
            with self.assertRaises(ValueError): shadow.package_from_observation(remake(prepared(),edit))

    def test_source_normalization_keeps_full_metadata(self):
        obs = prepared()
        row = source_row(obs)
        row["alert_time_utc"] = WHEN.astimezone(timezone(timedelta(hours=2)))
        linked = shadow.link_receipt(obs, row)
        self.assertEqual(linked["link_status"], "SOURCE_LINKED")
        self.assertEqual(linked["event_id"], 77)
        for field in ("runtime_session_id", "capture_stage", "delivered_at_utc", "delivery_status"):
            changed = deepcopy(row)
            changed[field] = "OTHER" if field != "delivered_at_utc" else WHEN+timedelta(seconds=5)
            receipt = shadow.link_receipt(obs, changed)
            self.assertEqual(receipt["link_status"], "SOURCE_BODY_MISMATCH")
            self.assertIsNone(receipt["event_id"])

    def test_no_positive_source_identity_no_link(self):
        obs = prepared()
        for value in (None, 0, True, -1):
            self.assertEqual(shadow.link_receipt(obs,source_row(obs,value))["link_status"],"SOURCE_ID_INVALID")
        self.assertEqual(shadow.link_receipt(obs)["link_status"],"SOURCE_PENDING")

    def test_link_receipt_tamper_and_proof_scope_rejected(self):
        obs = prepared()
        for key,value in (("event_id",None),("original_event_insert_proven",True),("observed_source_material_sha256","a"*64)):
            receipt=shadow.link_receipt(obs,source_row(obs));receipt[key]=value
            with self.assertRaises(ValueError):shadow.validate_link_receipt(receipt)

    def test_sqlite_dedup_conflict_atomic_and_link_binding(self):
        with temp_workspace() as directory:
            store=shadow.SQLiteStore(Path(directory)/"shadow.sqlite")
            obs=prepared();store.persist(obs);store.persist(obs)
            self.assertEqual(store.read(obs["observation_id"]),obs)
            receipt=shadow.link_receipt(obs,source_row(obs));store.persist_link(receipt);store.persist_link(receipt)
            wrong=deepcopy(receipt);wrong["observation_root_sha256"]="a"*64
            with self.assertRaisesRegex(ValueError,"OBSERVATION_MISMATCH"):store.persist_link(wrong)
            other=prepared();other["observation_id"]=obs["observation_id"];other["manifest"]["observation_id"]=obs["observation_id"];other["root_sha256"]=graph.digest(other["manifest"])
            with self.assertRaisesRegex(ValueError,"CONFLICT"):store.persist(other)
            self.assertEqual(store.read(obs["observation_id"]),obs)

    def test_alternative_compression_and_corruption_not_overwritten(self):
        with temp_workspace() as directory:
            path=Path(directory)/"shadow.sqlite";store=shadow.SQLiteStore(path);obs=prepared();store.persist(obs)
            sha,obj=next(iter(obs["objects"].items()));packed=zlib.compress(zlib.decompress(base64.b64decode(obj["compressed_b64"])),9)
            with closing(sqlite3.connect(path)) as c, c:c.execute("UPDATE research_combined_shadow_objects SET payload=? WHERE sha256=?",(packed,sha))
            store.persist(obs)
            with closing(sqlite3.connect(path)) as c, c:c.execute("UPDATE research_combined_shadow_objects SET payload=? WHERE sha256=?",(b"corrupt",sha))
            with self.assertRaises(ValueError):store.persist(obs)
            with closing(sqlite3.connect(path)) as c:self.assertEqual(c.execute("SELECT payload FROM research_combined_shadow_objects WHERE sha256=?",(sha,)).fetchone()[0],b"corrupt")

    def test_registry_contract_conflict_and_first_epoch_immutable(self):
        with temp_workspace() as directory:
            path=Path(directory)/"shadow.sqlite";store=shadow.SQLiteStore(path);obs=prepared();store.persist(obs)
            later=prepared();later["manifest"]["process_started_at_utc"]="2026-10-26T12:01:00+00:00";later["root_sha256"]=graph.digest(later["manifest"]);store.persist(later)
            with closing(sqlite3.connect(path)) as c, c:
                self.assertEqual(c.execute("SELECT first_reported_runtime_start_utc FROM research_combined_shadow_contracts").fetchone()[0],START)
                c.execute("UPDATE research_combined_shadow_contracts SET contract_sha256='bad'")
            with self.assertRaisesRegex(ValueError,"CONTRACT_CONFLICT"):store.persist(prepared())

    def test_queue_bound_failed_put_and_durable_claims(self):
        obs=prepared()
        with patch.object(shadow,"prepare_capture",return_value=obs),patch.object(shadow,"_ensure_worker",return_value=True),patch.object(shadow,"_QUEUE",queue.Queue(maxsize=1)),patch.dict(shadow._METRICS,queued_items=0,queued_bytes=0,submitted=0):
            self.assertEqual(shadow.submit_producer_capture(None,None)["status"],"QUEUED")
            self.assertEqual(shadow.submit_producer_capture(None,None)["status"],"FAILED")
            self.assertEqual(shadow._METRICS["queued_items"],1)
            self.assertEqual(shadow._METRICS["submitted"],1)
        with patch.object(shadow,"prepare_capture",return_value=obs),patch.object(shadow,"_ensure_worker",return_value=True),patch.dict(shadow._METRICS,queued_items=shadow.MAX_QUEUE_ITEMS,queued_bytes=0):
            self.assertEqual(shadow.submit_producer_capture(None,None)["status"],"DROPPED")

    def test_store_failure_does_not_escape_and_accounting_is_released(self):
        class FailingStore:
            def persist(self,obs):raise RuntimeError("secret-not-exposed")
        with patch.object(shadow,"_STORE",FailingStore()),patch.dict(shadow._METRICS,queued_items=1,queued_bytes=10):
            shadow._persist_one(prepared(),10)
            self.assertEqual(shadow._METRICS["queued_items"],0)
            self.assertEqual(shadow._METRICS["last"]["reason_code"],"RuntimeError")

    def test_separate_link_attempts_follow_successful_persist(self):
        obs=prepared();row=source_row(obs);calls=[]
        class Store:
            def persist(self,value):calls.append("persist");return {"persisted":True}
            def find_source(self,fp):calls.append("find");return row
            def persist_link(self,receipt):calls.append(receipt["link_status"])
        with patch.object(shadow,"_STORE",Store()),patch.dict(shadow._METRICS,queued_items=1,queued_bytes=10):shadow._persist_one(obs,10)
        self.assertEqual(calls,["persist","find","SOURCE_LINKED"])

    def test_link_write_failure_preserves_durable_observation_status(self):
        obs=prepared();row=source_row(obs)
        class Store:
            def persist(self,value):return {"persisted":True}
            def find_source(self,fp):return row
            def persist_link(self,receipt):raise RuntimeError("private-link-error")
        with patch.object(shadow,"_STORE",Store()),patch.dict(shadow._METRICS,queued_items=1,queued_bytes=10,persisted=0,failed=0,link_failures=0):
            shadow._persist_one(obs,10)
            self.assertEqual(shadow._METRICS["persisted"],1)
            self.assertEqual(shadow._METRICS["failed"],0)
            self.assertEqual(shadow._METRICS["link_failures"],1)
            self.assertEqual(shadow._METRICS["last"]["status"],"PERSISTED")
            self.assertEqual(shadow._METRICS["last_link"]["source_link_status"],"LINK_WRITE_FAILED")
            self.assertNotIn("private",json.dumps(shadow.status()))

    def test_newer_queue_status_not_relabelled_with_older_source_id(self):
        obs=prepared();row=source_row(obs)
        class Store:
            def persist(self,value):return {"persisted":True}
            def find_source(self,fp):return row
            def persist_link(self,receipt):shadow._METRICS["last"]={"observation_id":"newer","status":"DROPPED"}
        with patch.object(shadow,"_STORE",Store()),patch.dict(shadow._METRICS,queued_items=1,queued_bytes=10):
            shadow._persist_one(obs,10)
            self.assertEqual(shadow._METRICS["last"],{"observation_id":"newer","status":"DROPPED"})
            self.assertEqual(shadow._METRICS["last_link"]["observation_id"],obs["observation_id"])

    def test_database_selection_and_redacted_status(self):
        with patch.dict(os.environ,{"RESEARCH_DATABASE_URL":"postgresql://private","DATABASE_URL":"postgresql://other","RESEARCH_USE_PRIMARY_DATABASE":"false"},clear=True):
            self.assertEqual(shadow.database_url(),"postgresql://private")
            self.assertNotIn("postgresql",json.dumps(shadow.status()))
        with patch.dict(os.environ,{"DATABASE_URL":"postgresql://other"},clear=True):self.assertEqual(shadow.database_url(),"")


@unittest.skipUnless(os.getenv("RESEARCH_TEST_POSTGRES_URL"),"explicit isolated test PostgreSQL DSN not configured")
class PostgresTests(unittest.TestCase):
    def test_real_postgres_graph_contract_link_and_schema_scope(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        url=os.environ["RESEARCH_TEST_POSTGRES_URL"];schema="combined_shadow_test_"+uuid4().hex
        with psycopg.connect(url,autocommit=True) as c:c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        options=conninfo_to_dict(url).get("options","")+" -c search_path="+schema
        isolated=make_conninfo(url,options=options.strip())
        try:
            store=shadow.PostgresStore(isolated);obs=prepared();store.persist(obs);store.persist(obs)
            self.assertEqual(store.read(obs["observation_id"]),obs)
            serialized=payloads(obs)["producer_input"]["serialized_event"]
            clocks={"alert_time_utc","delivery_attempted_at_utc","delivered_at_utc"}
            numbers={"score","current_price","target_price","initial_target_distance_pct"}
            json_fields={"categories","engine_snapshot"}
            definitions=[name+" "+("TIMESTAMPTZ" if name in clocks else "DOUBLE PRECISION" if name in numbers else "JSONB" if name in json_fields else "TEXT") for name in shadow.SOURCE_FIELDS]
            with psycopg.connect(isolated) as c:
                c.execute("CREATE TABLE research_events(event_id BIGINT PRIMARY KEY,"+",".join(definitions)+")")
                placeholders=["%s::jsonb" if name in json_fields else "%s" for name in shadow.SOURCE_FIELDS]
                c.execute("INSERT INTO research_events(event_id,"+",".join(shadow.SOURCE_FIELDS)+") VALUES(%s,"+",".join(placeholders)+")",[77]+[serialized[name] for name in shadow.SOURCE_FIELDS])
            found=store.find_source(obs["manifest"]["event_fingerprint"])
            self.assertEqual(found["event_id"],77)
            receipt=shadow.link_receipt(obs,found)
            self.assertEqual(receipt["link_status"],"SOURCE_LINKED")
            store.persist_link(receipt);store.persist_link(receipt)
            with psycopg.connect(isolated) as c:
                self.assertEqual(c.execute("SELECT current_schema()").fetchone()[0],schema)
                self.assertEqual(c.execute("SELECT count(*) FROM research_combined_shadow_links").fetchone()[0],1)
                before=c.execute("SELECT count(*) FROM research_combined_shadow_objects").fetchone()[0]
                c.execute("UPDATE research_events SET capture_stage='EARLIER_CAPTURE'")
            mismatch=shadow.link_receipt(obs,store.find_source(obs["manifest"]["event_fingerprint"]))
            self.assertEqual(mismatch["link_status"],"SOURCE_BODY_MISMATCH")
            self.assertIsNone(mismatch["event_id"])
            other=prepared();other["observation_id"]=obs["observation_id"];other["manifest"]["observation_id"]=obs["observation_id"];other["root_sha256"]=graph.digest(other["manifest"])
            with self.assertRaisesRegex(ValueError,"CONFLICT"):store.persist(other)
            with psycopg.connect(isolated) as c:self.assertEqual(c.execute("SELECT count(*) FROM research_combined_shadow_objects").fetchone()[0],before)
        finally:
            with psycopg.connect(url,autocommit=True) as c:c.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
