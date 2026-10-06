"""DB adapter protocol checks; SQL effects emulated, not a PostgreSQL test."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest

import research_ordered_validation as validation
import research_ordered_validation_store as store
from research_ordered_validation_selftest import START, SCOPE, CANDIDATE, item, policy


class Cursor:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return deepcopy(self.rows)


class AdapterFixture:
    def __init__(self):
        self.freezes = {}
        self.waves = {}
        self.clock_reads = 0
        self.calls = []

    def execute(self, sql, args=()):
        self.calls.append((sql, args))
        if sql.startswith("SELECT registration"):
            return Cursor([{"registration": self.freezes[args[0]]}]) if args[0] in self.freezes else Cursor()
        if sql.startswith("SELECT policy"):
            return Cursor()
        if "SELECT clock_timestamp()" in sql:
            self.clock_reads += 1
            return Cursor([{"now": START}])
        if "INSERT INTO research_ordered_validation_freezes" in sql:
            self.freezes.setdefault(args[1], json.loads(args[4]))
            return Cursor()
        if "SELECT btc_parent_movement_id" in sql:
            return Cursor(self.waves.values())
        if "INSERT INTO research_ordered_validation_waves" in sql:
            self.waves.setdefault(args[1], {"btc_parent_movement_id": args[1], "phase": args[2],
                "parent_start_time_utc": args[3], "representative_event_ids": json.loads(args[4]),
                "representative_sha256": args[5], "representative_conflict": False})
            return Cursor()
        if "SET representative_conflict=TRUE" in sql:
            self.waves[args[1]]["representative_conflict"] = True
            return Cursor()
        raise AssertionError("Unexpected SQL in fixture: " + sql)


class AdapterTests(unittest.TestCase):
    def test_registration_uses_database_clock_once_and_does_not_refreeze(self):
        conn = AdapterFixture()
        first = store.freeze_scope(conn, SCOPE, CANDIDATE)
        second = store.freeze_scope(conn, SCOPE, CANDIDATE)
        self.assertEqual(first, second)
        self.assertEqual(first["frozen_at_utc"], START.isoformat())
        self.assertEqual(conn.clock_reads, 1)
        self.assertEqual(len(conn.freezes), 1)
        self.assertFalse(any("UPDATE research_ordered_validation_freezes" in sql for sql, _ in conn.calls))

    def test_criterion_mutation_or_posthoc_acceptance_cannot_reuse_registration(self):
        conn = AdapterFixture()
        store.freeze_scope(conn, SCOPE, CANDIDATE)
        with self.assertRaises(ValueError):
            store.freeze_scope(conn, {**SCOPE, "threshold_bps": 75}, CANDIDATE)
        with self.assertRaises(ValueError):
            store.freeze_scope(conn, SCOPE, CANDIDATE, acceptance_policy=policy())
        self.assertEqual(conn.clock_reads, 1)

    def test_same_entry_changed_label_keeps_one_wave(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        pending = item(status="OPEN")
        self.assertEqual(store._lock_representatives(conn, registration, [pending]), [])
        succeeded = item(status="SUCCESS")
        self.assertEqual(store._lock_representatives(conn, registration, [succeeded]), [])
        self.assertEqual(len(conn.waves), 1)
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1])

    def test_late_earlier_entry_marks_conflict_never_selects_new_winner(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        original = item()
        store._lock_representatives(conn, registration, [original])
        earlier = item(2, wave="wave-1", start=original["alert_time_utc"] - timedelta(seconds=20),
                       parent_start=original["parent_start_time_utc"])
        self.assertEqual(store._lock_representatives(conn, registration, [earlier, original]), ["wave-1"])
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1])
        self.assertEqual(store._lock_representatives(conn, registration, [original]), ["wave-1"])

    def test_simultaneous_cohort_keeps_all_members_no_order_invention(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        original = item()
        tied = item(2, wave="wave-1", start=original["alert_time_utc"], parent_start=original["parent_start_time_utc"])
        store._lock_representatives(conn, registration, [original, tied])
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1, 2])

    def test_incomplete_backfill_cannot_freeze_later_entry_or_poison_first_cohort(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        later = item(2, wave="wave-1", status="SUCCESS")
        self.assertEqual(store._lock_representatives(conn, registration, [later], establish_entries=False), [])
        self.assertEqual(conn.waves, {})
        earlier = item(1, wave="wave-1", status="FAILURE")
        self.assertEqual(store._lock_representatives(conn, registration, [earlier, later], establish_entries=True), [])
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1])
        # A later coverage regression cannot change the frozen losing entry.
        self.assertEqual(store._lock_representatives(conn, registration, [later], establish_entries=False), [])
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1])
        conn.waves["wave-1"]["representative_conflict"] = True
        self.assertEqual(store._lock_representatives(conn, registration, [], establish_entries=False), ["wave-1"])

    def test_unrelated_late_feature_enrichment_does_not_change_frozen_entry(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        original = item()
        store._lock_representatives(conn, registration, [original])
        prior_signature = conn.waves["wave-1"]["representative_sha256"]
        enriched = deepcopy(original)
        enriched["decision_features"].update({"historical.return_15m_pct": -0.8,
                                               "historical.btc_regime": "DOWN"})
        self.assertEqual(store._lock_representatives(conn, registration, [enriched]), [])
        self.assertEqual(conn.waves["wave-1"]["representative_sha256"], prior_signature)
        self.assertEqual(conn.waves["wave-1"]["representative_event_ids"], [1])

    def test_relevant_feature_change_conflicts_even_when_predicate_still_passes(self):
        conn = AdapterFixture()
        registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
        original = item()
        store._lock_representatives(conn, registration, [original])
        changed = deepcopy(original)
        changed["decision_features"]["price_oi.aligned_score"] = 75
        self.assertEqual(store._lock_representatives(conn, registration, [changed]), ["wave-1"])
        self.assertEqual(store._lock_representatives(conn, registration, [original]), ["wave-1"])

    def test_entry_identity_guards_survive_predicate_only_fingerprint(self):
        for field, value in (("entry_price", 101), ("snapshot_id", "replacement"),
                             ("symbol", "ETH"), ("direction", "SHORT"),
                             ("alert_time_utc", START + timedelta(hours=2))):
            with self.subTest(field=field):
                conn = AdapterFixture()
                registration = store.freeze_scope(conn, SCOPE, CANDIDATE)
                original = {**item(), "entry_price": 100, "snapshot_id": "original"}
                store._lock_representatives(conn, registration, [original])
                self.assertEqual(store._lock_representatives(conn, registration, [{**original, field: value}]), ["wave-1"])


if __name__ == "__main__":
    unittest.main()
