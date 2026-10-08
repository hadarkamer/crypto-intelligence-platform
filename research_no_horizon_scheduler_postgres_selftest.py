"""Real native SQL scheduler acceptance on TWO disposable local databases.

Requires SCHEDULER_TEST_DATABASE_URL (or TEST_DATABASE_URL as a local fallback)
and TEST_SOURCE_DATABASE_URL, distinct local database endpoints whose names
begin test_. The databases must be empty of test source
tables and requests. All data is synthetic. Historical created_at values are
explicit ENGINEERING SEED VALUES inserted only into this disposable fixture;
they are not actual preregistrations, future-market evidence or timestamp repair.
No production database, Render connector, notification or trading API is used.
"""
from copy import deepcopy
from datetime import timedelta
import json
import os
from pathlib import Path
import time
import unittest

import research_no_horizon_acquisition_source as source
import research_no_horizon_acquisition_store as acquisition
import research_no_horizon_contract as contracts
import research_no_horizon_scheduler as scheduler
import research_no_horizon_selection as selection
from research_no_horizon_selection_selftest import selection_fixture


TEST_DSN = os.getenv("SCHEDULER_TEST_DATABASE_URL") or os.getenv("TEST_DATABASE_URL")
SOURCE_DSN = os.getenv("TEST_SOURCE_DATABASE_URL")
SOURCE_TABLES = ("research_watch_scan_intakes", "research_max_pain_snapshot_sets",
                 "research_btc_parent_movements", "research_btc_price_bars",
                 "research_price_archive_bars")


def normalized(value):
    return json.loads(json.dumps(value, default=lambda item: item.isoformat()))


@unittest.skipUnless(TEST_DSN and SOURCE_DSN, "Two explicit disposable local test DSNs required")
class NativeSchedulerPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
        cls.psycopg, cls.sql = psycopg, sql
        cls.dict_row, cls.Jsonb = staticmethod(dict_row), Jsonb
        endpoints = []
        for url in (TEST_DSN, SOURCE_DSN):
            config = conninfo_to_dict(url)
            if (config.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                    or not config.get("dbname", "").startswith("test_")
                    or any(key in config for key in ("service", "hostaddr"))):
                raise ValueError("Scheduler integration requires explicit local test_ databases")
            endpoints.append(tuple(config.get(key, "5432" if key == "port" else "")
                                   for key in ("host", "port", "dbname")))
        if endpoints[0] == endpoints[1]:
            raise ValueError("Distinct disposable source and destination databases required")

    def connect(self, url):
        # PGlite's single-client socket may process disconnect asynchronously.
        # Each connection attempt has a 5s timeout; retries add at most .95s.
        for attempt in range(20):
            try:
                conn = self.psycopg.connect(url, row_factory=self.dict_row,
                                           connect_timeout=5)
                conn.prepare_threshold = None
                with conn.transaction():
                    conn.execute("SET statement_timeout='15000ms'")
                    conn.execute("SET lock_timeout='1000ms'")
                    conn.execute("SET TIME ZONE 'UTC'")
                return conn
            except self.psycopg.OperationalError:
                if attempt == 19:
                    raise
                time.sleep(.05)

    def seed_table(self, conn, name, values):
        fields = sorted(set().union(*(row.keys() for row in values)))
        types = {}
        for field in fields:
            example = next((row[field] for row in values if row.get(field) is not None), None)
            types[field] = ("timestamptz" if field.endswith("_utc") else
                "boolean" if isinstance(example, bool) else
                "bigint" if isinstance(example, int) else
                "double precision" if isinstance(example, float) else
                "jsonb" if isinstance(example, (dict, list)) else "text")
        sql = self.sql
        definition = sql.SQL(",").join(sql.SQL("{} {}").format(
            sql.Identifier(field), sql.SQL(types[field])) for field in fields)
        conn.execute(sql.SQL("CREATE TABLE public.{} ({})").format(sql.Identifier(name), definition))
        query = sql.SQL("INSERT INTO public.{} ({}) VALUES ({})").format(sql.Identifier(name),
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(sql.Placeholder() for _ in fields))
        rows = [[self.Jsonb(row[field]) if types[field] == "jsonb" and row.get(field) is not None
                 else row.get(field) for field in fields] for row in values]
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)

    def seed(self):
        base, fixtures = selection_fixture(parent_counts=(5, 5),
            directions=("SHORT", "LONG"), thresholds=(.25, .2), repeat_parent=True,
            top_k=1, required_eligible_windows=2)
        self.assertTrue(base["prior_outcomes_observed"])
        source_rows, candles = {}, {}
        for _, _, exports in fixtures:
            for export in exports:
                for row in export["source_rows"]:
                    row = normalized(row)
                    source_rows[row["intake"]["snapshot_set_id"]] = row
                for bar in export["candles"]:
                    candles[bar["open_time_utc"]] = deepcopy(bar)
        parents, prior = {}, {}
        for row in source_rows.values():
            parent = deepcopy(row["btc_parent"])
            identity = parent["btc_parent_movement_id"]
            if (identity not in parents or contracts.utc(parent["observed_through_utc"])
                    > contracts.utc(parents[identity]["observed_through_utc"])):
                parents[identity] = parent
            prior[row["btc_prior_bar"]["open_time_utc"]] = deepcopy(row["btc_prior_bar"])
        parents = sorted(parents.values(), key=lambda row: contracts.utc(row["start_time_utc"]))
        for left, right in zip(parents, parents[1:]):
            if left["end_time_utc"] is None:
                left["end_time_utc"] = right["start_time_utc"]
        tables = {"research_watch_scan_intakes": [row["intake"] for row in source_rows.values()],
            "research_max_pain_snapshot_sets": [{**row["stored_source"],
                "source_metadata": {"capture_metadata": {"operational_scores": row["scores"]}}}
                for row in source_rows.values()],
            "research_btc_parent_movements": parents,
            "research_btc_price_bars": list(prior.values()),
            "research_price_archive_bars": list(candles.values())}
        with self.connect(SOURCE_DSN) as conn:
            for name in SOURCE_TABLES:
                self.assertIsNone(conn.execute("SELECT to_regclass(%s) AS relation", (name,)).fetchone()["relation"],
                                  "Fixture must not overwrite existing source tables")
            for name, rows in tables.items():
                self.seed_table(conn, name, rows)
        plans, registrations = {}, {}
        root = Path(__file__).resolve().parent / "migrations"
        with self.connect(TEST_DSN) as conn:
            for name in ("056_no_horizon_runtime.sql", "057_no_horizon_acquisition.sql"):
                conn.execute((root / name).read_text(), prepare=False)
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM research_no_horizon_acquisition_requests").fetchone()["n"], 0)
            conn.commit()
            store = acquisition.AcquisitionStore(conn)
            for ordinal, (direction, threshold) in enumerate((("SHORT", .25), ("LONG", .25),
                                                            ("SHORT", .2), ("LONG", .2))):
                group = f"ENGINEERING_ONLY_{ordinal}"
                template = deepcopy(base["template_declaration"])
                template["cohort_key"] = group
                plan = selection.build_plan(template, base_directions=[direction],
                    thresholds_pct=[threshold], candidate_keys=["FUTURES_CVD_TOTAL_65"],
                    window_count=2, top_k=1, required_eligible_windows=2)
                ids = []
                for window, definition in enumerate(plan["discovery_plan"]["calendar_plan"]["windows"]):
                    declaration = definition["declaration"]
                    identity = {"version": acquisition.VERSION,
                        "declaration_sha256": contracts.digest(declaration),
                        "implementation_sha256": store.implementation_sha256,
                        "implementation": store.versions}
                    request_id = contracts.digest(identity)
                    # Disposable engineering insert, NOT native preregistration:
                    # no production trigger is disabled or request rewritten.
                    with conn.transaction():
                        conn.execute("""INSERT INTO research_no_horizon_acquisition_requests
                            (request_id,request_key,identity_json,declaration_json,
                             implementation_sha256,not_before_utc,created_at_utc)
                            VALUES(%s,%s,%s,%s,%s,%s,%s)""", (
                            request_id, f"{group}:{window}:SYNTHETIC_TIMESTAMP",
                            contracts.canonical(identity), contracts.canonical(declaration),
                            store.implementation_sha256,
                            max(contracts.utc(declaration[key]) for key in ("declared_at_utc", "cutoff_utc")),
                            contracts.utc(declaration["source_start_utc"]) - timedelta(days=1)))
                    info = store.registration_info(request_id)
                    registrations[request_id] = {"registration": info, "declaration": declaration,
                        "group": group, "window_ordinal": window}
                    ids.append(request_id)
                plans[group] = {"plan": plan, "requests": ids,
                    "receipt": {"plan_sha256": plan["plan_sha256"],
                                "purpose": "ENGINEERING_SYNTHETIC_TIMESTAMP_SEED_ONLY"}}
        return plans, registrations

    def test_due_native_acquisition_retry_reconnect_bounded_execution_and_selection(self):
        plans, registrations = self.seed()
        successful_connections, attempts = [], []
        def source_factory():
            attempts.append(True)
            if len(attempts) == 1:
                raise RuntimeError("ENGINEERING_INJECTED_TRANSIENT_CONNECTION_ERROR")
            conn = self.connect(SOURCE_DSN)
            successful_connections.append(True)
            return conn
        ticks, cursor = [], 0
        # Every tick closes and reopens destination. Native partial proofs and
        # outcome checkpoints must survive; no process-local state is reused.
        for _ in range(32):
            with self.connect(TEST_DSN) as conn:
                result = scheduler.run_tick(conn, plans, registrations,
                    source_connection_factory=source_factory, worker_id="ENGINEERING_ONLY",
                    provenance_verifier=lambda *_: True, cursor=cursor,
                    acquisition_leaf_budget=1, execution_passes=1,
                    candle_budget=1, entry_budget=1, batch_size=1)
            cursor = result["next_cursor"]
            ticks.append(result)
            if result["selection_complete"]:
                break
        else:
            self.fail("Synthetic original-eight scheduling did not converge")
        self.assertEqual(ticks[0]["errors"][0]["error_type"], "RuntimeError")
        self.assertEqual(len(successful_connections), 40)  # one anchor + four leaves per request
        self.assertGreater(len(ticks), 1)
        for result in ticks:
            for request in result["requests"]:
                for step in request["execution_steps"]:
                    self.assertLessEqual(step["candle_evaluations_this_run"], 1)
        self.assertTrue(any(group["status"] == "INCOMPLETE"
                            for tick in ticks for group in tick["groups"]))
        with self.connect(TEST_DSN) as conn:
            store = acquisition.AcquisitionStore(conn)
            scheduler.verify_original_registrations(store, registrations)
            for request_id, original in registrations.items():
                report = store.report(request_id, include_proofs=True)
                self.assertEqual(report["status"], "ADMITTED")
                self.assertEqual(report["proof_count"], 5)
                self.assertEqual(store.registration_info(request_id), original["registration"])
                source.validate_anchor_proof(original["declaration"], report["anchor_proof"])
                for leaf in report["leaves"]:
                    source.validate_chunk_proof(original["declaration"], report["anchor"], leaf["proof"])
            final = scheduler.run_tick(conn, plans, registrations,
                source_connection_factory=lambda: self.fail("Completed fixture reopened source"),
                worker_id="ENGINEERING_RESTART", provenance_verifier=lambda *_: True)
        self.assertTrue(final["selection_complete"])
        self.assertTrue(all(not row["execution_steps"] for row in final["requests"]))
        self.assertEqual([group["native_selection"] for group in final["groups"]],
                         [group["native_selection"] for group in ticks[-1]["groups"]])
        for group in final["groups"]:
            self.assertIs(group["native_selection"]["policy_registration_verified"], False)
            self.assertIs(group["native_selection"]["source_provenance_verified_by_this_tool"], False)


if __name__ == "__main__":
    unittest.main()
