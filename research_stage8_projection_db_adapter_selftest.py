"""Adversarial, network-free checks for the Stage-8 projection DB adapter."""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import re
import unittest
from unittest.mock import patch

from research_operational_score_source_audit_selftest import (
    AS_OF,
    BASE,
    DECISION,
    FakeConnection as SourceAuditFakeConnection,
    _anchor,
    _block,
    _parent,
    _rehash,
    _snapshot,
)
import research_stage8_contract as contract
import research_stage8_projection_db_adapter as adapter


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return deepcopy(self.rows)


class FakeConnection:
    """Small relational fixture; the adapter still owns every selection rule."""

    autocommit = False

    def __init__(self, *, attempts=None, slots=None, events=None, snapshots=None,
                 parent_sources=None, parents=None, btc_bars=None,
                 materialized_memberships=None, outcomes=None,
                 read_only="on", isolation="repeatable read", timeout="5s",
                 change_transaction=False):
        attempt, slot, pair = _anchor()
        selected = deepcopy(_snapshot())
        selected["available_at_utc"] = DECISION - timedelta(minutes=2)
        selected["created_at_utc"] = DECISION - timedelta(minutes=1)
        self.attempts = deepcopy([attempt] if attempts is None else attempts)
        self.slots = deepcopy([slot] if slots is None else slots)
        self.events = deepcopy(pair if events is None else events)
        self.parent_sources = deepcopy(
            {event["event_id"]: _parent(event) for event in self.events}
            if parent_sources is None else parent_sources
        )
        self.parents = deepcopy(list({
            source[1]["btc_parent_movement_id"]: source[1]
            for source in self.parent_sources.values() if source[1] is not None
        }.values()) if parents is None else parents)
        self.btc_bars = deepcopy(list({
            adapter._utc(source[2]["close_time_utc"]): source[2]
            for source in self.parent_sources.values() if source[2] is not None
        }.values()) if btc_bars is None else btc_bars)
        self.materialized_memberships = deepcopy(materialized_memberships or [])
        self.outcomes = deepcopy(outcomes or [])
        self.snapshots = deepcopy([selected] if snapshots is None else snapshots)
        self.read_only = read_only
        self.isolation = isolation
        self.timeout = timeout
        self.change_transaction = change_transaction
        self.queries = []
        self.tx_reads = 0

    def execute(self, sql, params=None):
        params = params or {}
        self.queries.append((sql, deepcopy(params)))
        if "stage8-projection:transaction" in sql:
            self.tx_reads += 1
            changed = self.change_transaction and self.tx_reads > 1
            return _Result([{
                "read_only": self.read_only,
                "isolation": self.isolation,
                "statement_timeout": self.timeout,
                "backend_pid": 777,
                "transaction_started_at_utc": DECISION + timedelta(hours=1 if changed else 0),
                "transaction_snapshot": "11:11:" if not changed else "12:12:",
                "observed_at_utc": DECISION + timedelta(hours=2, seconds=self.tx_reads),
            }])
        if "stage8-projection:archive-high-water" in sql:
            ids = [row["snapshot_set_id"] for row in self.snapshots
                   if row.get("source") == "WATCH_SHARED"]
            return _Result([{"archive_snapshot_high_water_id": max(ids, default=0)}])
        requested = set(params.get("attempt_ids", []))
        if "stage8-projection:attempts" in sql:
            return _Result(sorted(
                (row for row in self.attempts if row.get("attempt_id") in requested),
                key=lambda row: row["attempt_id"],
            ))
        if "stage8-projection:slots" in sql:
            attempts = {row["attempt_id"]: row for row in self.attempts
                        if row.get("attempt_id") in requested}
            rows = []
            for attempt_id, attempt in attempts.items():
                for slot in self.slots:
                    if (slot.get("sampler_version"), slot.get("symbol"),
                            slot.get("source_candle_open_utc")) == (
                            attempt.get("sampler_version"), attempt.get("symbol"),
                            attempt.get("source_candle_open_utc")):
                        rows.append({"adapter_attempt_id": attempt_id, **slot})
            return _Result(sorted(rows, key=lambda row: (
                row["adapter_attempt_id"], row["anchor_slot_id"]
            )))
        if "stage8-projection:events" in sql:
            selected_ids = set(params.get("event_ids", []))
            return _Result(sorted(
                (row for row in self.events if row.get("event_id") in selected_ids),
                key=lambda row: row["event_id"],
            ))
        if "stage8-projection:parent-sources" in sql:
            selected_ids = sorted(set(params.get("event_ids", [])))
            rows = []
            for event_id in selected_ids:
                event = next((row for row in self.events if row["event_id"] == event_id), None)
                if event is None:
                    continue
                decision = adapter._utc(event["alert_time_utc"])
                parents = [row for row in self.parents
                           if row["episode_policy_version"] == params["parent_policy_version"]
                           and adapter._utc(row["start_time_utc"]) <= decision
                           and (row.get("end_time_utc") is None
                                or decision < adapter._utc(row["end_time_utc"]))]
                parents.sort(key=lambda row: row["btc_parent_movement_id"])
                parent = max(parents, key=lambda row: adapter._utc(row["start_time_utc"]),
                             default=None)
                bar = max((row for row in self.btc_bars
                           if adapter._utc(row["close_time_utc"]) <= decision),
                          key=lambda row: adapter._utc(row["close_time_utc"]), default=None)
                membership = adapter.btc_parent.membership(event, parent=parent, btc_bar=bar)
                rows.append({
                    "adapter_event_id": event_id,
                    "adapter_membership_json": membership,
                    "adapter_parent_json": parent,
                    "adapter_btc_bar_json": bar,
                })
            return _Result(rows)
        if "stage8-projection:watch-selection" in sql:
            high_water = params["archive_snapshot_high_water_id"]
            max_age = params["max_capture_age"]
            rows = []
            for attempt in sorted(
                    (row for row in self.attempts
                     if row.get("attempt_id") in requested),
                    key=lambda row: row["attempt_id"]):
                decision = attempt.get("decision_time_utc")
                eligible = []
                if decision is not None:
                    for snapshot in self.snapshots:
                        if (snapshot.get("source") != "WATCH_SHARED"
                                or snapshot.get("snapshot_set_id", 0) > high_water
                                or snapshot.get("available_at_utc") is None
                                or snapshot.get("created_at_utc") is None):
                            continue
                        durable = max(snapshot["available_at_utc"], snapshot["created_at_utc"])
                        if decision - max_age <= durable <= decision:
                            eligible.append((durable, snapshot["snapshot_set_id"], snapshot))
                selected = max(eligible, default=None, key=lambda item: item[:2])
                if selected is None:
                    rows.append({
                        "adapter_attempt_id": attempt["attempt_id"],
                        "adapter_has_snapshot": False,
                    })
                else:
                    rows.append({
                        "adapter_attempt_id": attempt["attempt_id"],
                        "adapter_has_snapshot": True,
                        **selected[2],
                    })
            return _Result(rows)
        raise AssertionError("unexpected SQL: " + sql)


class ProjectionDatabaseAdapterTests(unittest.TestCase):
    def test_transaction_identity_matches_source_audit_for_same_server_fields(self):
        conn = SourceAuditFakeConnection(isolation="repeatable read")
        page = adapter.source_audit.audit_anchor_attempt_page_from_connection(
            conn, symbols=["BTC"], start_utc=BASE,
            end_utc=BASE + timedelta(minutes=30), max_capture_age_seconds=300,
            windows=(60,), thresholds_bps=(50,))
        row = {
            "read_only": "on", "isolation": "repeatable read", "statement_timeout": "5s",
            "backend_pid": 4242,
            "transaction_started_at_utc": AS_OF - timedelta(seconds=1),
            "transaction_snapshot": "100:200:", "observed_at_utc": AS_OF,
        }
        identity = adapter._transaction_identity(row, conn)
        self.assertEqual(identity["transaction_identity_sha256"],
                         page["transaction_identity_sha256"])
        self.assertTrue(identity["read_only"])
        self.assertEqual(identity["isolation"], "repeatable read")
        self.assertEqual(identity["statement_timeout_ms"], 5000.0)

    def test_timeout_metadata_not_a_second_transaction_identity(self):
        row = {
            "read_only": "on", "isolation": "repeatable read", "statement_timeout": "5s",
            "backend_pid": 777,
            "transaction_started_at_utc": "2026-09-13T15:30:00.123456+03:00",
            "transaction_snapshot": "10:20:11,14", "observed_at_utc": AS_OF,
        }
        first = adapter._transaction_identity(row, FakeConnection())
        second = adapter._transaction_identity(
            {**row, "statement_timeout": "1s",
             "transaction_started_at_utc": "2026-09-13T12:30:00.123456Z"}, FakeConnection())
        self.assertEqual(first["transaction_identity_sha256"],
                         "c08a62123de1fa2d03e6a0d92fd5775355ee16a3b7abbbcdba24780835f45cd4")
        self.assertEqual(first["transaction_identity_sha256"], second["transaction_identity_sha256"])
        self.assertNotEqual(first["statement_timeout_ms"], second["statement_timeout_ms"])
        with self.assertRaises(adapter.ProjectionAdapterError):
            adapter._transaction_identity({**row, "read_only": "off"}, FakeConnection())
        with self.assertRaises(adapter.ProjectionAdapterError):
            adapter._transaction_identity({**row, "transaction_snapshot": "invalid"}, FakeConnection())

    @staticmethod
    def _binding():
        return contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
            threshold_bps=50,
        )

    @staticmethod
    def _non_evaluable_attempts(count):
        source, _, _ = _anchor(missing=True)
        rows = []
        for attempt_id in range(1, count + 1):
            row = deepcopy(source)
            row["attempt_id"] = attempt_id
            row["attempt_fingerprint"] = format(attempt_id, "064x")
            rows.append(row)
        return rows

    def test_complete_projection_is_bound_to_db_selection_and_code(self):
        conn = FakeConnection()
        result = adapter.project_attempts_from_connection(conn, attempt_ids=[1])
        self.assertEqual(result["manifest_sha256"], contract.MANIFEST_SHA256)
        self.assertEqual(result["source_audit_version"], "operational-score-source-audit-v2")
        self.assertTrue(result["population_receipt"]["population_complete"])
        self.assertFalse(result["population_receipt"]["truncated"])
        self.assertEqual(result["population_receipt"]["query_count"], 8)
        self.assertEqual(
            result["exact_attempt_population_receipt_sha256"],
            result["population_receipt"]["exact_attempt_population_receipt_sha256"],
        )
        self.assertEqual(result["population_receipt"]["requested_attempt_ids"], [1])
        self.assertEqual(result["population_receipt"]["found_attempt_ids"], [1])
        row = result["rows"][0]
        self.assertEqual(row["source_status"], "FOUND")
        self.assertEqual(row["projection"]["fact_count"], 96)
        self.assertEqual(len(row["fact_authorities"]), 96)
        self.assertRegex(
            row["expected_watch_selection_attestation_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertRegex(row["expected_watch_code_manifest_sha256"], r"^[0-9a-f]{64}$")
        known = [authority for authority in row["fact_authorities"]
                 if authority["knowledge_status"] == "KNOWN"]
        self.assertTrue(known)
        for authority in known:
            self.assertEqual(
                authority["watch_selection_attestation_sha256"],
                row["expected_watch_selection_attestation_sha256"],
            )
            self.assertEqual(
                authority["watch_code_manifest_sha256"],
                row["expected_watch_code_manifest_sha256"],
            )
            self.assertRegex(
                authority["expected_parent_membership_evidence_sha256"],
                r"^[0-9a-f]{64}$",
            )
            self.assertIsNone(authority["expected_noneligibility_proof_sha256"])
        live = next(item for item in row["fact_ledger"]
                    if item["fact"]["knowledge_status"] == "KNOWN")
        self.assertEqual(
            live["parent_membership_evidence"]["validation_status"], "VALID"
        )
        self.assertIsNone(live["noneligibility_proof"])
        unsigned = dict(result)
        supplied = unsigned.pop("result_sha256")
        self.assertEqual(contract.digest(unsigned), supplied)

    def test_parent_identity_ignores_materialized_membership_and_outcomes(self):
        binding = contract.exact_binding(
            scope_id="BINANCE_BTC", candidate_id="FUTURES_FLOW_ALIGNED65_LONG",
            threshold_bps=50,
        )
        identities = []
        evidence = []
        event_id = _anchor()[2][0]["event_id"]
        for materialized, outcomes in (
            ([], []),
            ([{"event_id": event_id, "membership_status": "BTC_DATA_MISSING"}], []),
            ([], [{"event_id": event_id, "status": "SUCCESS"}]),
            ([{"event_id": event_id, "btc_parent_movement_id": "f" * 64}],
             [{"event_id": event_id, "status": "FAILURE"}]),
        ):
            conn = FakeConnection(materialized_memberships=materialized, outcomes=outcomes)
            ledger = adapter.project_exact_binding_attempts_from_connection(
                conn, exact_binding=binding, attempt_ids=[1],
            )["rows"][0]["fact_ledger"][0]
            parent_evidence = ledger["parent_membership_evidence"]
            self.assertEqual(parent_evidence["validation_status"], "VALID")
            evidence.append(parent_evidence)
            identities.append(adapter.representative_selector.canonical_selection_fact_identity(
                binding, ledger["fact"], source_attempt_evaluation_status="EVALUABLE",
                parent_authority_class="LIVE", parent_membership_evidence=parent_evidence,
            ))
        self.assertTrue(all(item == evidence[0] for item in evidence))
        self.assertTrue(all(item == identities[0] for item in identities))

    def test_direct_parent_sources_preserve_causal_missing_and_boundary_states(self):
        event = _anchor()[2][0]
        _, parent, bar = _parent(event)
        stale = deepcopy(bar)
        stale["close_time_utc"] = DECISION - timedelta(minutes=1)
        future = deepcopy(bar)
        future["close_time_utc"] = DECISION + timedelta(milliseconds=1)
        boundary = parent | {"evidence_eligible": False, "confirmed_at_utc": None,
                             "boundary_reason": "BTC_DATA_GAP"}
        for parents, bars, expected_status in (
            ([parent], [bar, future], "LIVE"),
            ([parent], [stale], "BTC_DATA_MISSING"),
            ([parent], [future], "BTC_DATA_MISSING"),
            ([], [bar], "BTC_DATA_MISSING"),
            ([parent | {"episode_policy_version": "other"}], [bar], "BTC_DATA_MISSING"),
            ([parent | {"end_time_utc": DECISION}], [bar], "BTC_DATA_MISSING"),
            ([boundary], [bar], "BOUNDARY_UNVERIFIED"),
            ([parent | {"confirmed_at_utc": DECISION + timedelta(seconds=1)}],
             [bar], "BTC_DATA_MISSING"),
        ):
            with self.subTest(status=expected_status, parents=parents, bars=bars):
                conn = FakeConnection(parents=parents, btc_bars=bars)
                row = conn.execute(adapter._PARENT_SOURCES_SQL, {
                    "event_ids": [event["event_id"]],
                    "parent_policy_version": adapter.btc_parent.POLICY_VERSION,
                    "limit": 2,
                }).fetchall()[0]
                membership = row["adapter_membership_json"]
                self.assertEqual(membership["membership_status"], expected_status)
                if expected_status == "BTC_DATA_MISSING":
                    self.assertIsNone(membership["btc_parent_movement_id"])
                else:
                    self.assertEqual(membership["btc_parent_movement_id"], parent["btc_parent_movement_id"])
                if bars == [bar, future]:
                    self.assertEqual(row["adapter_btc_bar_json"], bar)

    def test_sql_is_select_only_bounded_and_never_reads_outcomes(self):
        conn = FakeConnection()
        adapter.project_attempts_from_connection(conn, attempt_ids=[1])
        self.assertLessEqual(len(conn.queries), adapter.MAX_QUERIES)
        forbidden_relations = (
            "research_ordered_first_touch_outcomes", "research_signal_outcomes",
            "research_alert_outcomes",
            "research_event_btc_movements",
        )
        forbidden_commands = re.compile(
            r"\b(insert|update|delete|merge|alter|drop|create|truncate|copy|call)\b",
            re.I,
        )
        for sql, _ in conn.queries:
            self.assertIsNone(forbidden_commands.search(sql), sql)
            self.assertFalse(any(name in sql.lower() for name in forbidden_relations), sql)
            self.assertIn("select", sql.lower())
        selection_sql = next(sql for sql, _ in conn.queries
                             if "stage8-projection:watch-selection" in sql)
        self.assertIn("snapshot_set_id <=", selection_sql)
        self.assertIn("GREATEST(w.available_at_utc, w.created_at_utc) DESC", selection_sql)
        self.assertIn("w.snapshot_set_id DESC", selection_sql)
        high_index = next(index for index, (sql, _) in enumerate(conn.queries)
                          if "archive-high-water" in sql)
        value_index = next(index for index, (sql, _) in enumerate(conn.queries)
                           if "watch-selection" in sql)
        self.assertLess(high_index, value_index)

    def test_latest_invalid_snapshot_is_selected_and_never_backfilled(self):
        older = deepcopy(_snapshot())
        older.update(
            snapshot_set_id=100,
            available_at_utc=DECISION - timedelta(minutes=3),
            created_at_utc=DECISION - timedelta(minutes=3),
        )
        newer = deepcopy(_snapshot())
        newer.update(
            snapshot_set_id=102,
            snapshot_key="e" * 64,
            available_at_utc=DECISION - timedelta(minutes=1),
            created_at_utc=DECISION - timedelta(minutes=1),
        )
        _block(newer)["version"] = "malformed-newer-version"
        conn = FakeConnection(snapshots=[older, newer])
        row = adapter.project_attempts_from_connection(
            conn, attempt_ids=[1]
        )["rows"][0]
        self.assertEqual(
            row["watch_selection_attestation"]["selected_snapshot_set_id"], 102
        )
        self.assertEqual(
            row["watch_selection_observation"]["selected_snapshot_set_id"], 102
        )
        self.assertRegex(
            row["watch_selection_observation"]["selected_source_metadata_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertTrue(all(
            fact["knowledge_status"] == "UNKNOWN"
            for fact in row["projection"]["facts"]
        ))
        self.assertNotIn("WATCH_CAPTURE_NO_MATCH", row["reasons"])

    def test_late_persistence_is_not_a_durably_prior_selection(self):
        older = deepcopy(_snapshot())
        older.update(
            snapshot_set_id=100,
            available_at_utc=DECISION - timedelta(minutes=4),
            created_at_utc=DECISION - timedelta(minutes=4),
        )
        late = deepcopy(_snapshot())
        late.update(
            snapshot_set_id=102,
            snapshot_key="e" * 64,
            available_at_utc=DECISION - timedelta(minutes=1),
            created_at_utc=DECISION + timedelta(microseconds=1),
        )
        row = adapter.project_attempts_from_connection(
            FakeConnection(snapshots=[older, late]), attempt_ids=[1]
        )["rows"][0]
        self.assertEqual(
            row["watch_selection_attestation"]["selected_snapshot_set_id"], 100
        )

    def test_missing_attempt_is_retained_in_exact_population(self):
        result = adapter.project_attempts_from_connection(
            FakeConnection(), attempt_ids=[1, 999]
        )
        self.assertEqual(result["population_receipt"]["missing_attempt_ids"], [999])
        missing = result["rows"][1]
        self.assertEqual(missing["attempt_id"], 999)
        self.assertEqual(missing["knowledge_status"], "UNKNOWN")
        self.assertEqual(missing["reasons"], ["ATTEMPT_ROW_MISSING"])

    def test_missing_parent_source_is_hash_bound_unknown_not_noneligible(self):
        row = adapter.project_attempts_from_connection(
            FakeConnection(parent_sources={}), attempt_ids=[1]
        )["rows"][0]
        ledger = row["fact_ledger"][0]
        self.assertEqual(
            ledger["parent_membership_evidence"]["validation_status"], "UNKNOWN"
        )
        self.assertEqual(
            ledger["parent_membership_evidence"]["reason"], "PARENT_SOURCE_MISSING"
        )
        self.assertRegex(
            ledger["fact_authority"]["expected_parent_membership_evidence_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertIsNone(
            ledger["fact_authority"]["expected_noneligibility_proof_sha256"]
        )

    def test_non_evaluable_attempt_uses_exact_proof_for_every_fact(self):
        attempt, _, _ = _anchor(missing=True)
        result = adapter.project_attempts_from_connection(
            FakeConnection(attempts=[attempt], slots=[], events=[], parent_sources={}),
            attempt_ids=[attempt["attempt_id"]],
        )
        row = result["rows"][0]
        self.assertEqual(row["projection"]["fact_count"], 96)
        for ledger in row["fact_ledger"]:
            proof = ledger["noneligibility_proof"]
            self.assertEqual(proof["evaluation_status"], "UNEVALUABLE")
            self.assertEqual(proof["decision_time_utc"], None)
            self.assertEqual(
                ledger["fact_authority"]["expected_noneligibility_proof_sha256"],
                proof["proof_sha256"],
            )
            self.assertIsNone(
                ledger["fact_authority"]["expected_parent_membership_evidence_sha256"]
            )
            self.assertIsNone(ledger["parent_membership_source"])

    def test_wrong_or_changed_transaction_fails_closed(self):
        cases = (
            FakeConnection(read_only="off"),
            FakeConnection(isolation="read committed"),
            FakeConnection(timeout="0"),
            FakeConnection(timeout="20s"),
            FakeConnection(change_transaction=True),
        )
        for conn in cases:
            with self.subTest(conn=conn):
                with self.assertRaises(adapter.ProjectionAdapterError):
                    adapter.project_attempts_from_connection(conn, attempt_ids=[1])
        conn = FakeConnection()
        conn.autocommit = True
        with self.assertRaises(adapter.ProjectionAdapterError):
            adapter.project_attempts_from_connection(conn, attempt_ids=[1])

    def test_attempt_and_time_bounds_are_hard(self):
        invalid = (
            [], [0], [2, 1], [1, 1], list(range(1, adapter.MAX_ATTEMPTS + 2)),
        )
        for attempt_ids in invalid:
            with self.subTest(attempt_ids=attempt_ids[:3]):
                with self.assertRaisesRegex(
                        adapter.ProjectionAdapterError, "ATTEMPT_IDS_INVALID"):
                    adapter.project_attempts_from_connection(
                        FakeConnection(), attempt_ids=attempt_ids,
                    )
        with self.assertRaisesRegex(adapter.ProjectionAdapterError, "WALL_BOUND_INVALID"):
            adapter.project_attempts_from_connection(
                FakeConnection(), attempt_ids=[1],
                max_wall_seconds=adapter.MAX_WALL_SECONDS + 1,
            )

        ticks = iter([0.0, 0.0, adapter.MAX_WALL_SECONDS])
        with self.assertRaises(adapter.ProjectionAdapterError):
            adapter.project_attempts_from_connection(
                FakeConnection(), attempt_ids=[1], monotonic=lambda: next(ticks),
            )

    def test_fact_self_rehash_and_runtime_version_mutation_are_not_trusted(self):
        original = adapter.projection.project_first_tranche

        def forged(**kwargs):
            result = original(**kwargs)
            result["facts"][0]["candidate_match"] = False
            result["facts"][0].pop("fact_sha256")
            result["facts"][0]["fact_sha256"] = contract.digest(result["facts"][0])
            return result

        with patch.object(adapter.projection, "project_first_tranche", side_effect=forged):
            with self.assertRaisesRegex(
                    adapter.ProjectionAdapterError, "EMITTED_INVALID_FACT"):
                adapter.project_attempts_from_connection(FakeConnection(), attempt_ids=[1])
        with patch.object(adapter.source_audit, "VERSION", "mutated-v999"):
            with self.assertRaisesRegex(
                    adapter.ProjectionAdapterError, "RUNTIME_CONTRACT_MISMATCH"):
                adapter.project_attempts_from_connection(FakeConnection(), attempt_ids=[1])

    def test_exact_binding_mode_handles_33_and_1000_without_tranche_expansion(self):
        binding = self._binding()
        original = adapter.projection.project_binding_fact
        total_calls = 0
        for count in (33, adapter.MAX_EXACT_BINDING_ATTEMPTS):
            attempts = self._non_evaluable_attempts(count)
            conn = FakeConnection(
                attempts=attempts, slots=[], events=[], parent_sources={}, snapshots=[],
            )
            with self.subTest(count=count), \
                 patch.object(
                     adapter.projection, "project_first_tranche",
                     side_effect=AssertionError("full tranche must not run"),
                 ), \
                 patch.object(
                     adapter.projection, "project_binding_fact", wraps=original,
                 ) as exact_projector:
                result = adapter.project_exact_binding_attempts_from_connection(
                    conn, exact_binding=binding,
                    attempt_ids=list(range(1, count + 1)),
                )
            total_calls += exact_projector.call_count
            self.assertEqual(exact_projector.call_count, count)
            self.assertEqual(len(result["rows"]), count)
            self.assertTrue(all(
                row["projection"]["fact_count"] == 1
                and len(row["fact_ledger"]) == 1
                and len(row["fact_authorities"]) == 1
                for row in result["rows"]
            ))
            self.assertEqual(
                result["projection_mode"], adapter.EXACT_BINDING_PROJECTION_MODE,
            )
            self.assertEqual(
                result["exact_binding_sha256"], binding["binding_sha256"],
            )
            for envelope in (
                    result["query_scope"], result["population_receipt"],
                    result["authority_receipt"]):
                self.assertEqual(
                    envelope["projection_mode"],
                    adapter.EXACT_BINDING_PROJECTION_MODE,
                )
                self.assertEqual(
                    envelope["exact_binding_sha256"], binding["binding_sha256"],
                )
            self.assertLessEqual(len(conn.queries), adapter.MAX_QUERIES)
            unsigned = dict(result)
            supplied = unsigned.pop("result_sha256")
            self.assertEqual(contract.digest(unsigned), supplied)
        self.assertEqual(total_calls, 33 + adapter.MAX_EXACT_BINDING_ATTEMPTS)

    def test_exact_binding_mode_rejects_bad_binding_population_and_deadline(self):
        binding = self._binding()
        forged = deepcopy(binding)
        forged["binding"]["candidate"]["value"] = 64
        conn = FakeConnection()
        with self.assertRaisesRegex(
                adapter.ProjectionAdapterError, "EXACT_BINDING_INVALID"):
            adapter.project_exact_binding_attempts_from_connection(
                conn, exact_binding=forged, attempt_ids=[1],
            )
        self.assertEqual(conn.queries, [])

        with self.assertRaisesRegex(
                adapter.ProjectionAdapterError, "ATTEMPT_IDS_INVALID"):
            adapter.project_exact_binding_attempts_from_connection(
                FakeConnection(), exact_binding=binding,
                attempt_ids=list(range(1, adapter.MAX_EXACT_BINDING_ATTEMPTS + 2)),
            )

        ticks = iter([0.0, 0.0, 0.0, adapter.MAX_WALL_SECONDS])
        with self.assertRaisesRegex(
                adapter.ProjectionAdapterError, "WALL_BOUND_EXCEEDED"):
            adapter.project_exact_binding_attempts_from_connection(
                FakeConnection(), exact_binding=binding, attempt_ids=[1],
                monotonic=lambda: next(ticks),
            )

    def test_exact_binding_mode_retains_unknown_and_missing_without_fabrication(self):
        attempt = self._non_evaluable_attempts(1)[0]
        result = adapter.project_exact_binding_attempts_from_connection(
            FakeConnection(
                attempts=[attempt], slots=[], events=[], parent_sources={}, snapshots=[],
            ),
            exact_binding=self._binding(), attempt_ids=[1, 999],
        )
        found, missing = result["rows"]
        self.assertEqual(found["projection"]["fact_count"], 1)
        self.assertEqual(len(found["fact_ledger"]), 1)
        self.assertEqual(
            found["fact_ledger"][0]["fact"]["knowledge_status"], "UNKNOWN",
        )
        self.assertIsNotNone(found["fact_ledger"][0]["noneligibility_proof"])
        self.assertEqual(missing["source_status"], "MISSING")
        self.assertEqual(missing["knowledge_status"], "UNKNOWN")
        self.assertEqual(missing["fact_ledger"], [])
        self.assertEqual(result["population_receipt"]["missing_attempt_ids"], [999])


if __name__ == "__main__":
    unittest.main()
