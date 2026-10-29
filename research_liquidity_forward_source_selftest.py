"""Actual capture-to-source adapter checks; all market values are synthetic."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest
from unittest.mock import patch

import research_combined_liquidity_shadow as shadow
import research_consumed_inputs as graph
import research_event_capture as capture
import research_liquidity_forward_source as source

EPOCH = datetime(2026, 11, 1, tzinfo=timezone.utc)
CURRENT = EPOCH + timedelta(minutes=45)
COMMIT = "e" * 40


def iso(value):
    return value.isoformat()


def policy():
    protocol = source.load_protocol()
    return {"version": source.STUDY_ID, "protocol_sha256": source.PROTOCOL_SHA256,
            "study_start_utc": iso(EPOCH), "scopes": [{"scope_key": "synthetic-doge-scope",
                "candidate_key": protocol["hypothesis"]["candidate_key"], "symbol": "DOGE", "direction": "SHORT"}]}


def registry():
    return {COMMIT: shadow.code_versions()}


def receipt():
    return {"receipt_id": "synthetic-actual-read", "request_started_utc": iso(CURRENT + timedelta(seconds=9)),
            "response_received_utc": iso(CURRENT + timedelta(seconds=10))}


def parent(event_id, when):
    return {"event_id": event_id, "btc_parent_movement_id": "synthetic-parent",
            "episode_policy_version": source.reference.PARENT_POLICY, "membership_status": "LIVE",
            "parent_evidence_eligible": True, "parent_start_time_utc": iso(EPOCH + timedelta(minutes=1)),
            "parent_confirmed_at_utc": iso(EPOCH + timedelta(minutes=2)),
            "parent_created_at_utc": iso(EPOCH + timedelta(minutes=2, seconds=1)),
            "membership_created_at_utc": iso(when + timedelta(seconds=1)),
            "btc_observed_close_utc": iso(when - timedelta(seconds=30)),
            "btc_bar_created_at_utc": iso(when - timedelta(seconds=29))}


def native(event_id=101, when=CURRENT, scan="current", share=40):
    event = {"event_id": event_id, "event_fingerprint": hashlib.sha256(str(event_id).encode()).hexdigest(),
             "schema_version": "research-event-v1", "symbol": "DOGE", "direction": "SHORT",
             "source_side": "LONG", "event_kind": "ALERT", "event_type": "MAX_PAIN_ALERT",
             "alert_time_utc": iso(when), "delivery_status": "DELIVERED", "delivered_at_utc": iso(when + timedelta(milliseconds=500)),
             "created_at": iso(when + timedelta(seconds=1)), "code_version": COMMIT, "current_price": 0.1,
             "target_price": None, "score": 1, "engine_snapshot": {"alert_side": "LONG", "watch_scan_id": scan,
                "near_amount": share, "far_amount": 100-share, "near_share_pct": share}}
    return {"event_id": event_id, "event_json_text": source._json(event),
            "parent_json_text": source._json(parent(event_id, when)), "combined_observations": []}


def combined(event_id=201, when=CURRENT, *, linked=True, producer_python="3.11.9"):
    slot = {"timeframe": "48h", "source_side": "LONG", "score": 59.69,
            "target_price": 0.09, "near_amount": 40, "far_amount": 60, "near_share_pct": 40.0}
    top = {"symbol": "DOGE", "side": "LONG", "timeframe": "48h", "score": 59.69,
           "current_price": 0.1, "target_price": 0.09, "maxpain_timeframes": [deepcopy(slot)]}
    event = capture.build_generic_alert_event(symbol="DOGE", event_type="COMBINED_CONFIRMATION",
        direction="SHORT", source_side="LONG", event_time=when, timeframe="48h", score=59.69,
        current_price=0.1, target_price=0.09, categories=["synthetic-source-test"],
        engine_snapshot={"alert_side": "LONG", "watch_scan_id": "combined-" + str(event_id),
                         "maxpain_timeframes": [deepcopy(slot)]}, strategy_version="synthetic", code_version=COMMIT)
    captured = iso(when + timedelta(seconds=2))
    with patch.dict("os.environ", {"RENDER_GIT_COMMIT": COMMIT}), patch.object(shadow, "PROCESS_STARTED_AT_UTC", iso(EPOCH)), patch.object(shadow, "utc_now", return_value=captured), patch.object(graph, "utc_now", return_value=captured), patch.object(shadow.platform, "python_version", return_value=producer_python):
        observation = shadow.prepare_capture(event, top, event_writer_queue_accepted=True,
            delivery_attempted_at_utc=when + timedelta(milliseconds=100), delivered_at_utc=when + timedelta(milliseconds=500))
    payloads = {header["kind"]: body for header, body in graph.event_payloads(shadow.package_from_observation(observation))}
    row = deepcopy(payloads["producer_input"]["source_material"])
    row.update(event_id=event_id, created_at=iso(when + timedelta(seconds=1)))
    links = []
    if linked:
        with patch.object(shadow, "utc_now", return_value=iso(when + timedelta(seconds=2, milliseconds=300))):
            link = shadow.link_receipt(observation, row)
        links.append({"link_receipt_id": link["link_receipt_id"], "receipt_json_text": source._json(link),
                      "created_at_utc": iso(when + timedelta(seconds=2, milliseconds=500))})
    saved = {"observation_id": observation["observation_id"], "root_sha256": observation["root_sha256"],
             "manifest_json_text": source._json(observation["manifest"]), "created_at_utc": iso(when + timedelta(seconds=2, milliseconds=100)),
             "objects": [{"sha256": sha, **value} for sha, value in observation["objects"].items()], "links": links}
    return {"event_id": event_id, "event_json_text": source._json(row),
            "parent_json_text": source._json(parent(event_id, when)), "combined_observations": [saved]}


def envelope(records):
    return {"server_statement_utc": iso(CURRENT + timedelta(seconds=9, milliseconds=100)),
            "population_count": len(records), "records": records,
            "estimated_response_bytes": 0, "budget_exceeded": False}


def derive(records, **overrides):
    args = {"code_registry": registry()}
    args.update(overrides)
    return source.derive_packet(envelope(records), policy(), receipt(), **args)


class SourceTests(unittest.TestCase):
    def test_native_opposes_and_missing_target_remain_valid(self):
        packet = derive([native(100, CURRENT - timedelta(minutes=15), "prior"), native()])
        self.assertTrue(packet["source_complete"])
        self.assertEqual([row["event_id"] for row in packet["rows"]], [101])
        self.assertEqual(packet["rows"][0]["source_point"]["alignment"], "OPPOSES")
        self.assertEqual(packet["rows"][0]["computed_entry_ordinal"], 2)

    def test_actual_combined_capture_positive_link_to_native_history(self):
        packet = derive([native(100, CURRENT - timedelta(minutes=15), "prior"), combined()])
        self.assertEqual([row["event_id"] for row in packet["rows"]], [201])
        self.assertEqual(packet["rows"][0]["source_contract"], shadow.VERSION)
        self.assertEqual(packet["rows"][0]["liquidity_evidence_created_at_utc"], iso(CURRENT + timedelta(seconds=2, milliseconds=500)))

    def test_pending_current_positive_link_is_retryable(self):
        pending = derive([native(100, CURRENT - timedelta(minutes=15), "prior"), combined(linked=False)])
        self.assertEqual(pending["rows"], [])
        self.assertEqual(pending["source_population_count"], 2)
        self.assertEqual(pending["pending_liquidity_points"], 1)
        retry = derive([native(100, CURRENT - timedelta(minutes=15), "prior"), combined(linked=True)])
        self.assertEqual([row["event_id"] for row in retry["rows"]], [201])

    def test_pending_prior_is_retained_not_assigned_negative_alignment(self):
        records = [native(99, CURRENT - timedelta(minutes=20), "prior"),
                   combined(200, CURRENT - timedelta(minutes=10), linked=False), native()]
        packet = derive(records)
        match = next(row for row in packet["rows"] if row["event_id"] == 101)
        self.assertEqual(match["computed_entry_ordinal"], 2)
        self.assertEqual(match["pending_prior_count"], 1)
        self.assertTrue(match["source_history_uncertain"])
        self.assertEqual(match["history_event_ids"], [99])
        self.assertEqual(len(packet["source_closure"]["records"]), 3)

    def test_combined_registry_mismatch_does_not_trust_shape_only(self):
        codes = registry()
        codes[COMMIT] = {**codes[COMMIT], "analysis.py": "f" * 64}
        packet = derive([combined()], code_registry=codes)
        self.assertEqual(packet["rows"], [])
        self.assertEqual(packet["source_qualities"][0]["reason"], "COMBINED_LIVE_SOURCE_CODE_PIN_MISMATCH")

    def test_combined_wrong_producer_python_is_explicitly_deferred(self):
        record = combined(producer_python="3.12.0")
        packet = derive([record])
        self.assertEqual(packet["rows"], [])
        self.assertEqual(packet["source_qualities"][0]["reason"], "COMBINED_PRODUCER_RUNTIME_NOT_CPYTHON_3_11_9")

    def test_unknown_commit_defers_and_raw_first_seen_still_observable(self):
        codes = {"f" * 40: registry()[COMMIT]}
        packet = derive([native()], code_registry=codes)
        self.assertEqual(packet["rows"], [])
        self.assertEqual(packet["observed_sources"][0]["event_id"], 101)
        self.assertEqual(packet["source_qualities"][0]["reason"], "SOURCE_COMMIT_NOT_IN_DURABLE_REGISTRY")

    def test_current_scan_prior_still_counts_and_id_order_is_not_causality(self):
        packet = derive([native(500, CURRENT - timedelta(minutes=15), "same"), native(101, scan="same")])
        self.assertEqual([row["event_id"] for row in packet["rows"]], [101])

    def test_half_open_window_and_duplicate_scan_identity(self):
        records = [native(99, CURRENT - timedelta(minutes=30), "same"),
                   native(100, CURRENT - timedelta(minutes=15), "same"), native()]
        self.assertEqual(derive(records)["rows"][-1]["computed_entry_ordinal"], 2)
        records[0] = native(99, CURRENT - timedelta(minutes=30, microseconds=1), "other")
        self.assertEqual(derive(records)["rows"][-1]["computed_entry_ordinal"], 2)

    def test_warmup_is_source_time_not_late_read_time(self):
        records = [native(99, EPOCH + timedelta(minutes=2), "prior"), native(100, EPOCH + timedelta(minutes=29), "current")]
        self.assertEqual(derive(records)["rows"], [])

    def test_static_closure_deduplicates_across_real_receipts(self):
        records = [native()]
        first = derive(records)
        later_envelope = envelope(records)
        later_envelope["server_statement_utc"] = iso(CURRENT + timedelta(seconds=19))
        later_receipt = {"receipt_id": "next-real-read", "request_started_utc": iso(CURRENT + timedelta(seconds=19)), "response_received_utc": iso(CURRENT + timedelta(seconds=20))}
        second = source.derive_packet(later_envelope, policy(), later_receipt, code_registry=registry())
        self.assertEqual(first["source_closure_sha256"], second["source_closure_sha256"])
        self.assertNotEqual(first["poll_id"], second["poll_id"])

    def test_population_overflow_and_duplicate_parent_join_fail_closed(self):
        data = envelope([native()])
        data["population_count"] = source.MAX_SOURCE_EVENTS + 1
        with self.assertRaisesRegex(source.SourceRejected, "SOURCE_POPULATION_OVERFLOW"):
            source.derive_packet(data, policy(), receipt(), code_registry=registry())
        with self.assertRaisesRegex(source.SourceRejected, "SOURCE_PARENT_JOIN_NOT_UNIQUE"):
            derive([native(), native()])

    def test_sql_byte_budget_overflow_fails_before_population_processing(self):
        data = envelope([])
        data.update(population_count=1, records=[], estimated_response_bytes=source.MAX_RESPONSE_BYTES + 1, budget_exceeded=True)
        with self.assertRaisesRegex(source.SourceRejected, "SOURCE_SQL_BYTE_BUDGET"):
            source.derive_packet(data, policy(), receipt(), code_registry=registry())

    def test_pending_source_not_skipped_by_eligible_key(self):
        records = [native(100, CURRENT - timedelta(minutes=15), "prior"), combined(linked=False)]
        packet = derive(records, eligible_keys=["synthetic-doge-scope:100"])
        self.assertEqual(len(packet["observed_sources"]), 2)
        self.assertEqual(packet["decisions"][0]["event_id"], 201)

    def test_bootstrap_parent_ids_retained_without_matches(self):
        packet = derive([native()], bootstrap=True)
        self.assertEqual(packet["rows"], [])
        self.assertEqual(packet["observed_parent_ids"], ["synthetic-parent"])

    def test_input_objects_are_not_mutated(self):
        data, cfg, received, codes = envelope([native()]), policy(), receipt(), registry()
        before = deepcopy((data, cfg, received, codes))
        source.derive_packet(data, cfg, received, code_registry=codes)
        self.assertEqual((data, cfg, received, codes), before)

    def test_sql_scope_has_no_legacy_match_or_outcome_lookup(self):
        self.assertNotIn("research_ordered_formula_matches", source.SQL_TEMPLATE)
        self.assertNotIn("outcome", source.SQL_TEMPLATE.lower())
        self.assertIn("e.alert_time_utc >= %s", source.SQL_TEMPLATE)
        self.assertIn("LIMIT %s", source.SQL_TEMPLATE)
        self.assertIn("research_event_btc_movements", source.SQL_TEMPLATE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
