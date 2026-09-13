"""Disposable SQL gates for the Stage-8 durable registry.

Only an explicit local/CI ``TEST_DATABASE_URL`` whose database name is visibly
test-only is accepted. Native PostgreSQL uses independent login sessions for
the five least-privilege roles. ``STAGE8_PGLITE_COMPAT=1`` is a separately
labeled, single-session SQL/trigger smoke and is never treated as proof of
native socket or role isolation. Every test uses a random schema. No runtime,
deploy, Telegram, LIVE, trading, or production connection variable is read.
"""
from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import unittest
from unittest import mock
from uuid import uuid4

import research_prospective_anchors as anchors
from research_operational_score_source_audit_selftest import (
    _anchor, _block, _outcome, _parent, _snapshot,
)
import research_operational_score_source_audit_selftest as source_fixtures
from research_stage8_feature_projection_selftest import _set_model
from research_watch_score_capture_selftest import archive_payload
import research_watch_score_capture_selftest as watch_fixtures
import research_max_pain_archive_selftest as archive_fixtures
import research_stage8_contract as contract
import research_stage8_coverage_receipt as coverage
import research_stage8_projection_db_adapter as projection_adapter
import research_stage8_registry as registry
import research_stage8_representative_selector as selector
import research_stage8_outcome_db_adapter as outcome_adapter
import research_stage8_acceptance as acceptance
import canonical_price_path
import research_common_window_metrics as common_window


UTC = timezone.utc
ROLE_NAMES = (
    registry.REGISTRAR_ROLE, registry.FACT_WRITER_ROLE,
    registry.SELECTOR_WRITER_ROLE, registry.EVALUATOR_WRITER_ROLE,
    registry.READER_ROLE,
)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"),
                     "TEST_DATABASE_URL is required for PostgreSQL integration")
class Stage8RegistryPostgreSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb

        cls.dsn = os.environ["TEST_DATABASE_URL"]
        target = conninfo_to_dict(cls.dsn)
        if (target.get("host") not in {"localhost", "127.0.0.1", "::1", "postgres"}
                or not (target.get("dbname", "").startswith("test_")
                        or target.get("dbname", "").endswith("_test"))):
            raise ValueError("Stage-8 integration requires an explicit local test database")
        cls.psycopg, cls.sql, cls.Jsonb = psycopg, sql, Jsonb
        cls.make_conninfo = staticmethod(make_conninfo)
        cls.dict_row = staticmethod(dict_row)
        cls.pglite_compat = os.environ.get("STAGE8_PGLITE_COMPAT") == "1"
        cls.role_passwords = {
            role: "stage8_" + uuid4().hex for role in ROLE_NAMES
        }
        with psycopg.connect(cls.dsn, autocommit=True, connect_timeout=5) as admin:
            for role in ROLE_NAMES:
                admin.execute(sql.SQL("""
                    DO $do$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {}) THEN
                            EXECUTE {};
                        END IF;
                    END $do$
                """).format(
                    sql.Literal(role),
                    sql.Literal("CREATE ROLE " + role +
                                " NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                                "NOINHERIT NOREPLICATION NOBYPASSRLS"),
                ))
                if not cls.pglite_compat:
                    admin.execute(
                        sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                            sql.Identifier(role), sql.Literal(cls.role_passwords[role])
                        )
                    )

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "pglite_compat", True):
            return
        try:
            with cls.psycopg.connect(
                cls.dsn, autocommit=True, connect_timeout=5
            ) as admin:
                for role in ROLE_NAMES:
                    admin.execute(
                        cls.sql.SQL("ALTER ROLE {} NOLOGIN").format(
                            cls.sql.Identifier(role)
                        )
                    )
        except cls.psycopg.Error:
            # The disposable database may already have been torn down by CI.
            pass

    def setUp(self):
        self.schema = "test_stage8_registry_" + uuid4().hex
        self.admin = self.psycopg.connect(
            self.dsn, row_factory=self.dict_row, connect_timeout=5,
            prepare_threshold=None,
            options=("-c statement_timeout=15000 -c lock_timeout=1000 "
                     "-c TimeZone=UTC"),
        )
        self.admin.execute(
            self.sql.SQL("CREATE SCHEMA {}").format(self.sql.Identifier(self.schema))
        )
        self.admin.execute(
            self.sql.SQL("SET search_path TO {}, pg_catalog, pg_temp").format(
                self.sql.Identifier(self.schema)
            )
        )
        migration_dir = Path(__file__).resolve().parent / "migrations"
        self.base_migrations = [
            path for path in sorted(migration_dir.glob("*.sql"))
            if int(path.name[:3]) < 46
        ]
        for path in self.base_migrations:
            self.admin.execute(self._schema_sql(path), prepare=False)
        self.migration_046 = migration_dir / "046_stage8_durable_registry.sql"
        self.admin.execute(self._schema_sql(self.migration_046), prepare=False)
        # Seed privileges that existed in earlier local drafts.  A second full
        # execution must converge them away, not merely add the current grants.
        for role in ROLE_NAMES:
            self.admin.execute(
                self.sql.SQL(
                    "GRANT SELECT ON research_event_btc_movements TO {}"
                ).format(self.sql.Identifier(role))
            )
        self.admin.execute(
            self.sql.SQL("""
                GRANT EXECUTE ON FUNCTION
                  research_stage8_derive_projection_attestation_v1(
                    TEXT,TEXT,BIGINT,BIGINT,BOOLEAN) TO {}
            """).format(self.sql.Identifier(registry.READER_ROLE))
        )
        # A second full execution is the migration/ACL idempotency assertion.
        self.admin.execute(self._schema_sql(self.migration_046), prepare=False)
        if self.pglite_compat:
            # PGlite's protocol queue cannot recover from a surfaced ERROR.
            # Catch expected negatives server-side only in the explicitly
            # labeled compatibility smoke.
            self.admin.execute("""
                CREATE OR REPLACE FUNCTION stage8_test_expected_rejection(statement TEXT)
                RETURNS JSONB LANGUAGE plpgsql AS $body$
                BEGIN
                    BEGIN
                        EXECUTE statement;
                    EXCEPTION WHEN OTHERS THEN
                        RETURN jsonb_build_object(
                            'sqlstate', SQLSTATE,
                            'message', SQLERRM
                        );
                    END;
                    RETURN NULL;
                END
                $body$
            """)
            for role in ROLE_NAMES:
                self.admin.execute(
                    self.sql.SQL(
                        "GRANT EXECUTE ON FUNCTION "
                        "stage8_test_expected_rejection(TEXT) TO {}"
                    ).format(self.sql.Identifier(role))
                )
        self.admin.commit()
        self.addCleanup(self._cleanup)
        self.binding = contract.exact_binding(
            scope_id="BINANCE_BTC",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )

    def _schema_sql(self, path: Path) -> str:
        return path.read_text().replace(
            "'public.research_", f"'{self.schema}.research_"
        )

    def _cleanup(self):
        self.admin.rollback()
        self.admin.execute("RESET ROLE")
        self.admin.commit()
        self.admin.close()
        with self.psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=5) as cleanup:
            cleanup.execute(
                self.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    self.sql.Identifier(self.schema)
                )
            )

    def _connect(self, *, read_only=False, role=None):
        if not self.pglite_compat and role is not None:
            conn = self.psycopg.connect(
                self.make_conninfo(
                    self.dsn, user=role, password=self.role_passwords[role]
                ),
                row_factory=self.dict_row, connect_timeout=5,
                prepare_threshold=None,
                options=("-c statement_timeout=10000 -c lock_timeout=1000 "
                         "-c TimeZone=UTC"),
            )
            self.addCleanup(conn.close)
            conn.execute(
                self.sql.SQL("SET search_path TO {}, pg_catalog, pg_temp").format(
                    self.sql.Identifier(self.schema)
                )
            )
            conn.commit()
            conn.isolation_level = self.psycopg.IsolationLevel.REPEATABLE_READ
            conn.read_only = read_only
            return conn

        # The explicitly labeled PGlite smoke reuses one protocol session and
        # SET ROLE because PGlite does not model independent authenticated
        # session_user identities. Native PostgreSQL never takes this path for
        # a dedicated role.
        conn = self.admin
        conn.rollback()
        conn.read_only = False
        conn.isolation_level = self.psycopg.IsolationLevel.READ_COMMITTED
        conn.execute("RESET ROLE")
        conn.execute(
            self.sql.SQL("SET search_path TO {}, pg_catalog, pg_temp").format(
                self.sql.Identifier(self.schema)
            )
        )
        if role is not None:
            conn.execute(self.sql.SQL("SET ROLE {}").format(self.sql.Identifier(role)))
        conn.execute("SET statement_timeout TO '10000ms'")
        conn.execute("SET TIME ZONE 'UTC'")
        conn.commit()
        conn.isolation_level = self.psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = read_only
        return conn

    def _reset_admin(self):
        self.admin.rollback()
        self.admin.read_only = False
        self.admin.isolation_level = self.psycopg.IsolationLevel.READ_COMMITTED
        self.admin.execute("RESET ROLE")
        self.admin.execute(
            self.sql.SQL("SET search_path TO {}, pg_catalog, pg_temp").format(
                self.sql.Identifier(self.schema)
            )
        )
        self.admin.commit()

    def _expect_rejection(self, conn, statement, params=()):
        if not self.pglite_compat:
            savepoint = "stage8_expected_rejection"
            conn.execute("SAVEPOINT " + savepoint)
            with self.assertRaises(self.psycopg.Error):
                conn.execute(statement, params)
            conn.execute("ROLLBACK TO SAVEPOINT " + savepoint)
            conn.execute("RELEASE SAVEPOINT " + savepoint)
            return None
        rendered = self.psycopg.ClientCursor(conn).mogrify(statement, params)
        result = conn.execute(
            "SELECT stage8_test_expected_rejection(%s) AS rejection",
            (rendered,),
        ).fetchone()["rejection"]
        self.assertIsNotNone(result, "database unexpectedly accepted guarded SQL")
        self.assertRegex(result["sqlstate"], r"^[0-9A-Z]{5}$")
        return result

    def _insert(self, table, row, *, on_conflict_do_nothing=False):
        columns = self.admin.execute(
            """SELECT column_name, data_type FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
            (self.schema, table),
        ).fetchall()
        values = {
            item["column_name"]: (
                self.Jsonb(row[item["column_name"]], dumps=contract.canonical)
                if item["data_type"] in ("json", "jsonb") else row[item["column_name"]]
            )
            for item in columns if item["column_name"] in row
        }
        statement = self.sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                self.sql.Identifier(table),
                self.sql.SQL(",").join(map(self.sql.Identifier, values)),
                self.sql.SQL(",").join(self.sql.Placeholder() for _ in values),
            )
        if on_conflict_do_nothing:
            statement += self.sql.SQL(" ON CONFLICT DO NOTHING")
        self.admin.execute(statement, tuple(values.values()))

    def _insert_snapshot(self, snapshot):
        cycle = snapshot["cycle_time_utc"]
        if not isinstance(cycle, datetime):
            cycle = datetime.fromisoformat(str(cycle).replace("Z", "+00:00"))
        with mock.patch.object(watch_fixtures, "BASE", cycle), \
             mock.patch.object(archive_fixtures, "BASE", cycle):
            payload = archive_payload(_block(snapshot))
        # Rebuild the set envelope after mutating the model observation.  The
        # archive's outer payload/hash is part of Watch authority; inserting
        # the pre-mutation fixture envelope would intentionally project an
        # UNKNOWN fact even though its child rows contain the new score.
        set_row = payload["set"] | {
            "snapshot_set_id": snapshot["snapshot_set_id"],
            "created_at_utc": snapshot["created_at_utc"],
        }
        self._insert("research_max_pain_snapshot_sets", set_row)
        for table, key in (
            ("research_max_pain_snapshot_symbols", "symbols"),
            ("research_max_pain_snapshot_rows", "rows"),
        ):
            for row in payload[key]:
                self._insert(table, row | {"snapshot_set_id": snapshot["snapshot_set_id"]})

    def _seed_attempt(
        self, *, attempt_id: int, decision: datetime,
        exact_slot_base: datetime | None = None,
    ):
        # The production sampler accepts only exact 30-minute source slots.
        # Treat the requested value as a lower bound and choose the first real
        # decision instant (slot close + two-minute grace + fixture offset).
        if exact_slot_base is None:
            base = decision.astimezone(UTC).replace(
                minute=0, second=0, microsecond=0,
            )
            if base + timedelta(minutes=34) < decision:
                base += timedelta(hours=1)
        else:
            base = exact_slot_base.astimezone(UTC).replace(
                second=0, microsecond=0,
            )
            self.assertIn(base.minute, (0, 30))
        decision = base + timedelta(minutes=34)
        with mock.patch.object(source_fixtures, "BASE", base), \
             mock.patch.object(source_fixtures, "DECISION", decision), \
             mock.patch.object(watch_fixtures, "BASE", base), \
             mock.patch.object(archive_fixtures, "BASE", base):
            attempt, slot, events = _anchor(attempt_id=attempt_id)
            snapshot = _snapshot(snapshot_id=100 + attempt_id)
        snapshot["available_at_utc"] = decision - timedelta(minutes=2)
        snapshot["created_at_utc"] = decision - timedelta(minutes=1)
        snapshot = _set_model(
            snapshot, score=-70, direction="BEARISH", available=True,
            capture_status="AVAILABLE",
        )
        snapshot["snapshot_key"] = hashlib.sha256(
            f"stage8-registry-snapshot-{attempt_id}".encode()
        ).hexdigest()
        self._insert("research_prospective_anchor_attempts", attempt)
        for event in events:
            self._insert("research_events", event | {"runtime_session_id": self.schema})
        self._insert("research_prospective_anchor_slots", slot)
        self._insert_snapshot(snapshot)
        for event in events:
            membership, parent, bar = _parent(event)
            # Each fixture is an independent causal parent.  Close it just
            # after the decision so the authoritative schema's one-active-
            # parent invariant permits the next fixture parent.
            parent["end_time_utc"] = decision + timedelta(minutes=1)
            self.admin.execute(
                """INSERT INTO research_btc_price_bars
                   (open_time_utc,close_time_utc,open,high,low,close,price_source)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                tuple(bar[key] for key in (
                    "open_time_utc", "close_time_utc", "open", "high", "low",
                    "close", "price_source",
                )),
            )
            self._insert(
                "research_btc_parent_movements", parent,
                on_conflict_do_nothing=True,
            )
            self._insert("research_event_btc_movements", membership)
        return attempt

    def _register(self):
        conn = self._connect(role=registry.REGISTRAR_ROLE)
        before = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        row = registry.register_exact_binding_from_connection(conn, self.binding)
        after = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        conn.commit()
        self.assertLessEqual(before, row["frozen_at_utc"] if isinstance(
            row["frozen_at_utc"], datetime) else datetime.fromisoformat(
                row["frozen_at_utc"].replace("Z", "+00:00")
            ))
        self.assertLessEqual(datetime.fromisoformat(
            row["frozen_at_utc"].replace("Z", "+00:00")
        ), after)
        self._reset_admin()
        return row

    def _backdate_registry_fixture(self, registered, *, delta=timedelta(days=1)):
        """Move only a disposable E2E fixture behind the wall clock.

        Registration clock ownership is asserted separately through the real
        insert trigger.  A valid prospective attempt must be both after that
        freeze and already observable by the Watch reader; those conditions
        cannot be manufactured immediately after a real-time freeze without
        waiting for a later source slot.  This admin-only fixture operation is
        therefore explicit, trigger-bypassed, and confined to the random test
        schema.  It recomputes every affected durable identity before the
        append-only guard is re-enabled.
        """
        frozen = datetime.fromisoformat(
            registered["frozen_at_utc"].replace("Z", "+00:00")
        ) - delta
        frozen_text = frozen.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        freeze_id = contract.digest({
            "version": "stage8-db-clock-freeze-id-v1",
            "exact_binding_sha256": registered["exact_binding_sha256"],
            "frozen_at_utc": frozen_text,
            "verifier_profile_sha256": registered["verifier_profile_sha256"],
        })
        record = deepcopy(registered["registry_record"])
        record["freeze_id"] = freeze_id
        record["frozen_at_utc"] = frozen_text
        record_sha = contract.digest(record)
        self.admin.execute(
            "ALTER TABLE research_stage8_binding_registry "
            "DISABLE TRIGGER trg_stage8_registry_append_only"
        )
        self.admin.execute(
            """UPDATE research_stage8_binding_registry
               SET frozen_at_utc=%s, freeze_id=%s, registry_record=%s,
                   registry_record_sha256=%s
               WHERE exact_binding_sha256=%s""",
            (frozen, freeze_id,
             self.Jsonb(record, dumps=contract.canonical), record_sha,
             registered["exact_binding_sha256"]),
        )
        self.admin.execute(
            "ALTER TABLE research_stage8_binding_registry "
            "ENABLE TRIGGER trg_stage8_registry_append_only"
        )
        self.admin.commit()
        return registered | {
            "frozen_at_utc": frozen_text,
            "freeze_id": freeze_id,
            "registry_record": record,
            "registry_record_sha256": record_sha,
        }

    def _build_full_path(self, *, attempt_count=2, recent_last=False):
        registered = self._backdate_registry_fixture(self._register())
        frozen = datetime.fromisoformat(registered["frozen_at_utc"].replace("Z", "+00:00"))
        decision_lower_bounds = [
            frozen + timedelta(hours=2 * attempt_id)
            for attempt_id in range(1, attempt_count + 1)
        ]
        exact_slot_bases = [None] * attempt_count
        if recent_last:
            now = datetime.now(UTC)
            hour = now.replace(minute=0, second=0, microsecond=0)
            candidate_bases = (
                hour,
                hour - timedelta(minutes=30),
                hour - timedelta(hours=1),
            )
            base = max(
                value for value in candidate_bases
                if value + timedelta(minutes=36) <= now
            )
            decision_lower_bounds[-1] = base + timedelta(minutes=34)
            exact_slot_bases[-1] = base
        attempts = [
            self._seed_attempt(
                attempt_id=attempt_id,
                decision=decision_lower_bounds[attempt_id - 1],
                exact_slot_base=exact_slot_bases[attempt_id - 1],
            )
            for attempt_id in range(1, attempt_count + 1)
        ]
        self.admin.commit()
        reader = self._connect(read_only=True, role=registry.READER_ROLE)
        start = frozen
        end = max(
            attempt["source_candle_open_utc"] for attempt in attempts
        ) + timedelta(microseconds=1)
        handoff = coverage.read_bounded_attempt_cohort_from_connection(
            reader, start_utc=start, end_utc=end,
            symbols=tuple(self.binding["binding"]["scope"]["symbols"]),
        )
        ids = handoff["attempt_ids"]
        self.assertEqual(ids, list(range(1, attempt_count + 1)))
        projected = projection_adapter.project_exact_binding_attempts_from_connection(
            reader, exact_binding=self.binding, attempt_ids=ids,
        )
        reference = registry.registry_reference_from_connection(reader, self.binding)
        source_rows, authorities = [], []
        for projected_row in projected["rows"]:
            fact_item = projected_row["fact_ledger"][0]
            source_rows.append({
                "attempt_id": projected_row["attempt_id"],
                "fact": fact_item["fact"],
                "parent_membership_source": fact_item["parent_membership_source"],
                "noneligibility_proof": fact_item["noneligibility_proof"],
            })
            authorities.append({
                key: fact_item["fact_authority"].get(key)
                for key in selector._FACT_AUTHORITY_KEYS
            })
        selected = selector.select_representatives(
            self.binding, source_rows, fact_authorities=authorities,
            population_receipt=handoff["coverage_receipt"],
            expected_outcome_free_population_receipt_sha256=handoff[
                "outcome_free_population_receipt_sha256"
            ],
            registry_reference=reference,
            observed_source_hashes={
                "projection_source_sha256": reference[
                    "expected_projection_source_sha256"
                ],
                "selector_source_sha256": reference[
                    "expected_selector_source_sha256"
                ],
            },
        )
        self.assertEqual(selected["status"], "COMPLETE", selected)
        fact_conn = self._connect(role=registry.FACT_WRITER_ROLE)
        durable_fact = registry.append_projection_fact_batch_from_connection(
            fact_conn, self.binding, projection_result=projected,
            coverage_audit_receipt=handoff["coverage_receipt"],
            registry_reference=reference,
        )
        fact_conn.commit()
        selector_conn = self._connect(role=registry.SELECTOR_WRITER_ROLE)
        durable_selection = registry.append_selection_receipt_from_connection(
            selector_conn, self.binding, selected,
        )
        selector_conn.commit()
        return registered, handoff, projected, selected, durable_fact, durable_selection

    def _insert_authoritative_outcome_evidence(
        self, durable_selection, *, success_count=None, common_count=None,
    ):
        """Seed exact terminal-v7 and READY common rows for selected events."""
        def json_value(value):
            if isinstance(value, datetime):
                return value.astimezone(UTC).isoformat(timespec="microseconds")
            if isinstance(value, dict):
                return {key: json_value(item) for key, item in value.items()}
            if isinstance(value, list):
                return [json_value(item) for item in value]
            return value

        self._reset_admin()
        identities = durable_selection["representative_identities"]
        if success_count is None:
            success_count = len(identities)
        if common_count is None:
            common_count = len(identities)
        for index, identity in enumerate(identities):
            event = dict(self.admin.execute(
                "SELECT * FROM research_events WHERE event_id=%s",
                (identity["event_id"],),
            ).fetchone())
            decision = event["alert_time_utc"].astimezone(UTC)
            winning = index < success_count
            outcome = _outcome(
                event, status="SUCCESS" if winning else "FAILURE",
                window=60, threshold_bps=50,
            )
            revision = decision + timedelta(minutes=2)
            outcome["created_at_utc"] = revision
            outcome["updated_at_utc"] = revision
            self._insert("research_ordered_first_touch_outcomes", outcome)

            if index >= common_count:
                continue

            reference = float(event["current_price"])
            candles = []
            for minute in range(60):
                opened = decision + timedelta(minutes=minute)
                candles.append({
                    "open_time_utc": opened,
                    "close_time_utc": opened + timedelta(minutes=1,
                                                           milliseconds=-1),
                    "open": reference,
                    "high": reference * (1.01 if winning else 1.02),
                    "low": reference * (0.98 if winning else 0.99),
                    "close": reference,
                })
            observed = decision + timedelta(minutes=61)
            metric = common_window.calculate_common_window_metrics(
                symbol=event["symbol"], reference_price=reference,
                direction=event["direction"], event_time=decision,
                window_minutes=60, candles=candles, observed_at=observed,
                path_result={
                    "symbol": event["symbol"], "exchange": "binance",
                    "market": "spot", "pair": event["symbol"] + "USDT",
                    "interval": "1m", "interval_seconds": 60,
                    "api_coin": None, "complete": True,
                    "provenance": "SELFTEST",
                },
            )
            self.assertEqual(metric["status"], "READY")
            self._insert("research_common_window_metrics", {
                "event_id": event["event_id"], "window_minutes": 60,
                "method_version": common_window.METHOD_VERSION,
                "status": "READY", "measurement_start_utc": decision,
                "window_end_utc": decision + timedelta(minutes=60),
                "next_attempt_at_utc": observed, "result": json_value(metric),
                "created_at_utc": observed, "updated_at_utc": observed,
            })
        self.admin.commit()

    def _read_authoritative_outcome_result(self, durable_selection):
        """Run the real 15-query reader under the appropriate test role."""
        outcome_reader = self._connect(
            read_only=True, role=registry.READER_ROLE,
        )
        transaction_query = outcome_adapter._TRANSACTION_SQL
        transaction_patch = (
            mock.patch.object(
                outcome_adapter, "_TRANSACTION_SQL",
                transaction_query.replace(
                    "session_user AS session_role",
                    "current_user AS session_role",
                ),
            ) if self.pglite_compat else nullcontext()
        )
        with transaction_patch:
            return outcome_adapter.evaluate_selection_outcomes_from_connection(
                outcome_reader, self.binding,
                selection_record_sha256=durable_selection[
                    "selection_record_sha256"
                ],
            )

    @staticmethod
    def _rehash_persistence_payload(payload):
        """Rehash every closed envelope after an adversarial test mutation."""
        value = deepcopy(payload)
        replay = value["fact_replay_receipt"]
        replay.pop("fact_replay_receipt_sha256", None)
        replay_sha = contract.digest(replay)
        replay["fact_replay_receipt_sha256"] = replay_sha
        value["fact_replay_receipt_sha256"] = replay_sha

        evidence = value["evidence_receipt"]
        evidence.pop("evidence_receipt_sha256", None)
        evidence_sha = contract.digest(evidence)
        evidence["evidence_receipt_sha256"] = evidence_sha
        value["evidence_receipt_sha256"] = evidence_sha

        evaluation = value["evaluation"]
        evaluation["fact_replay_receipt_sha256"] = replay_sha
        evaluation["evidence_receipt_sha256"] = evidence_sha
        value["evaluation_sha256"] = contract.digest(evaluation)
        value.pop("persistence_payload_sha256", None)
        value["persistence_payload_sha256"] = contract.digest(value)
        return value

    def test_registration_clock_idempotency_acl_and_server_owned_rejection(self):
        registered = self._register()
        conn = self._connect(role=registry.REGISTRAR_ROLE)
        same = registry.register_exact_binding_from_connection(conn, self.binding)
        conn.commit()
        self.assertEqual(same["freeze_id"], registered["freeze_id"])
        self._expect_rejection(
            conn,
            """INSERT INTO research_stage8_binding_registry
               (exact_binding,implementation_artifacts,
                expected_watch_code_manifest,frozen_at_utc)
               VALUES (%s::jsonb,%s::jsonb,%s::jsonb,clock_timestamp()-interval '1 day')""",
            (contract.canonical(self.binding),
             contract.canonical(registry.implementation_artifacts()),
             contract.canonical(registry.expected_watch_code_manifest())),
        )
        privileges = self.admin.execute("""
            SELECT
              has_table_privilege(%s,'research_stage8_binding_registry','INSERT') AS registrar_insert,
              has_table_privilege(%s,'research_stage8_binding_registry','UPDATE') AS registrar_update,
              has_table_privilege(%s,'research_stage8_projected_fact_ledger','INSERT') AS fact_insert,
              has_table_privilege(%s,'research_stage8_selection_receipts','INSERT') AS selector_insert,
              has_table_privilege(%s,'research_stage8_selection_read_v1','SELECT') AS selector_read,
              has_table_privilege(%s,'research_stage8_evaluation_receipts','INSERT') AS evaluator_insert,
              has_table_privilege(%s,'research_stage8_registry_read_v1','SELECT') AS reader_select,
              has_table_privilege(%s,'research_max_pain_snapshot_sets','SELECT') AS fact_watch_read,
              has_table_privilege(%s,'research_ordered_first_touch_outcomes','SELECT') AS fact_outcome_read,
              has_table_privilege(%s,'research_btc_parent_movements','SELECT') AS evaluator_parent_read,
              has_table_privilege(%s,'research_btc_parent_movements','UPDATE') AS evaluator_parent_update,
              has_table_privilege(%s,'research_event_btc_movements','SELECT') AS registrar_legacy_membership_read,
              has_table_privilege(%s,'research_event_btc_movements','SELECT') AS fact_legacy_membership_read,
              has_table_privilege(%s,'research_event_btc_movements','SELECT') AS selector_legacy_membership_read,
              has_table_privilege(%s,'research_event_btc_movements','SELECT') AS evaluator_legacy_membership_read,
              has_table_privilege(%s,'research_event_btc_movements','SELECT') AS reader_legacy_membership_read,
              has_function_privilege(%s,
                'research_stage8_derive_projection_attestation_v1(text,text,bigint,bigint,boolean)',
                'EXECUTE') AS fact_projection_attestation_execute,
              has_function_privilege(%s,
                'research_stage8_derive_projection_attestation_v1(text,text,bigint,bigint,boolean)',
                'EXECUTE') AS reader_projection_attestation_execute
        """, (
            registry.REGISTRAR_ROLE, registry.REGISTRAR_ROLE,
            registry.FACT_WRITER_ROLE, registry.SELECTOR_WRITER_ROLE,
            registry.SELECTOR_WRITER_ROLE, registry.EVALUATOR_WRITER_ROLE,
            registry.READER_ROLE, registry.FACT_WRITER_ROLE,
            registry.FACT_WRITER_ROLE, registry.EVALUATOR_WRITER_ROLE,
            registry.EVALUATOR_WRITER_ROLE,
            registry.REGISTRAR_ROLE, registry.FACT_WRITER_ROLE,
            registry.SELECTOR_WRITER_ROLE, registry.EVALUATOR_WRITER_ROLE,
            registry.READER_ROLE, registry.FACT_WRITER_ROLE,
            registry.READER_ROLE,
        )).fetchone()
        self.assertEqual(privileges, {
            "registrar_insert": True, "registrar_update": False,
            "fact_insert": True, "selector_insert": True, "selector_read": True,
            "evaluator_insert": True, "reader_select": True,
            "fact_watch_read": True, "fact_outcome_read": False,
            "evaluator_parent_read": True, "evaluator_parent_update": False,
            "registrar_legacy_membership_read": False,
            "fact_legacy_membership_read": False,
            "selector_legacy_membership_read": False,
            "evaluator_legacy_membership_read": False,
            "reader_legacy_membership_read": True,
            "fact_projection_attestation_execute": True,
            "reader_projection_attestation_execute": False,
        })
        fact_conn = self._connect(role=registry.FACT_WRITER_ROLE)
        rejection = self._expect_rejection(fact_conn, """
            SELECT research_stage8_derive_projection_attestation_v1(
                current_schema(), %s, 1, NULL, FALSE)
        """, (self.binding["binding_sha256"],))
        if self.pglite_compat:
            self.assertIn("trigger-internal only", rejection["message"])

    def test_native_roles_use_independent_authenticated_sessions(self):
        if self.pglite_compat:
            self.skipTest("PGlite compatibility smoke has no socket-role isolation")
        for role in ROLE_NAMES:
            with self.subTest(role=role):
                conn = self._connect(read_only=role == registry.READER_ROLE, role=role)
                identity = conn.execute(
                    "SELECT current_user, session_user, "
                    "has_schema_privilege(current_user,current_schema(),'CREATE')"
                ).fetchone()
                self.assertEqual(identity["current_user"], role)
                self.assertEqual(identity["session_user"], role)
                self.assertIs(identity["has_schema_privilege"], False)

    def test_temp_shadow_is_re_pinned_behind_the_trusted_schema(self):
        """A dedicated reader cannot redirect an unqualified durable read."""
        registered = self._register()
        reader = self._connect(read_only=False, role=registry.READER_ROLE)
        reader.execute(
            self.sql.SQL("""
                CREATE TEMP VIEW research_stage8_registry_read_v1 AS
                SELECT * FROM {}.research_stage8_registry_read_v1 WHERE FALSE
            """).format(self.sql.Identifier(self.schema))
        )
        reader.commit()
        reader.execute(
            self.sql.SQL("SET search_path TO pg_temp, {}, pg_catalog").format(
                self.sql.Identifier(self.schema)
            )
        )
        reader.commit()
        reader.isolation_level = self.psycopg.IsolationLevel.REPEATABLE_READ
        reader.read_only = True
        reference = registry.registry_reference_from_connection(
            reader, self.binding,
        )
        self.assertEqual(reference["freeze_id"], registered["freeze_id"])
        resolved = reader.execute(
            "SELECT current_schemas(true) AS schemas"
        ).fetchone()["schemas"]
        self.assertEqual(resolved[0], self.schema)
        temp_positions = [
            index for index, value in enumerate(resolved)
            if value == "pg_temp" or value.startswith("pg_temp_")
        ]
        self.assertTrue(all(index > 0 for index in temp_positions))

    def test_all_binance7_sorted_scope_reaches_a_sealed_batch(self):
        """DB compares the seven-symbol scope canonically, not manifest order."""
        self.binding = contract.exact_binding(
            scope_id="ALL_BINANCE7",
            candidate_id="FUTURES_FLOW_ALIGNED65_SHORT",
            threshold_bps=50,
        )
        _, handoff, _, _, durable_fact, durable_selection = (
            self._build_full_path(attempt_count=1)
        )
        expected = sorted(contract.BINANCE_SYMBOLS)
        self.assertEqual(handoff["coverage_receipt"]["query_scope"]["symbols"], expected)
        self.assertEqual(durable_fact["batch"]["attempt_ids"], [1])
        self.assertEqual(durable_fact["seal"]["fact_count"], 1)
        self.assertEqual(durable_selection["representative_count"], 1)

    def test_full_valid_register_fact_seal_selection_and_hard_false_evaluation(self):
        _, _, _, selected, durable_fact, durable_selection = self._build_full_path()
        self.assertEqual(durable_fact["seal"]["fact_count"], 2)
        self.assertEqual(durable_selection["representative_count"], 2)
        reader = self._connect(read_only=True, role=registry.READER_ROLE)
        evaluated = registry.evaluate_verified_from_connection(
            reader, self.binding, selected["representatives"],
            selection_record_sha256=durable_selection["selection_record_sha256"],
        )
        self.assertIs(evaluated["research_qualified"], False)
        self.assertIn("AUTHORITATIVE_OUTCOME_ADAPTER_REQUIRED",
                      evaluated["qualification_blockers"])
        outcome_reader = self._connect(
            read_only=True, role=registry.READER_ROLE,
        )
        # PGlite does not model authenticated session_user. Keep that
        # compatibility-only limitation explicit while exercising all 15
        # read queries, the closed payload validator, and the SQL guard. Native
        # PostgreSQL uses the unmodified query and a real reader login session.
        transaction_query = outcome_adapter._TRANSACTION_SQL
        transaction_patch = (
            mock.patch.object(
                outcome_adapter, "_TRANSACTION_SQL",
                transaction_query.replace(
                    "session_user AS session_role",
                    "current_user AS session_role",
                ),
            ) if self.pglite_compat else nullcontext()
        )
        with transaction_patch:
            outcome_result = (
                outcome_adapter.evaluate_selection_outcomes_from_connection(
                    outcome_reader, self.binding,
                    selection_record_sha256=durable_selection[
                        "selection_record_sha256"
                    ],
                )
            )
        self.assertIs(outcome_result["research_qualified"], False)
        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)
        evaluator.execute(
            self.sql.SQL("""
                CREATE TEMP VIEW research_stage8_evaluation_read_v1 AS
                SELECT * FROM {}.research_stage8_evaluation_read_v1 WHERE FALSE
            """).format(self.sql.Identifier(self.schema))
        )
        evaluator.commit()
        evaluator.execute(
            self.sql.SQL("SET search_path TO pg_temp, {}, pg_catalog").format(
                self.sql.Identifier(self.schema)
            )
        )
        evaluator.commit()
        raw_hashes = evaluator.execute("""
            SELECT
              (SELECT research_stage8_json_sha256_v1(
                        to_jsonb(r) || jsonb_build_object(
                          'frozen_at_utc', research_stage8_utc_text_v1(r.frozen_at_utc)))
                 FROM research_stage8_registry_read_v1 AS r
                WHERE r.exact_binding_sha256=%s) AS registry_row_sha256,
              (SELECT research_stage8_json_sha256_v1(to_jsonb(s))
                 FROM research_stage8_selection_read_v1 AS s
                WHERE s.selection_record_sha256=%s) AS selection_row_sha256,
              (SELECT research_stage8_json_sha256_v1(to_jsonb(b))
                 FROM research_stage8_fact_batch_read_v1 AS b
                WHERE b.fact_batch_record_sha256=%s) AS fact_batch_row_sha256,
              (SELECT research_stage8_json_sha256_v1(to_jsonb(z))
                 FROM research_stage8_fact_seal_read_v1 AS z
                WHERE z.fact_batch_record_sha256=%s) AS fact_seal_row_sha256,
              (SELECT research_stage8_json_sha256_v1(COALESCE(
                    jsonb_agg(research_stage8_json_sha256_v1(to_jsonb(f))
                              ORDER BY f.attempt_id), '[]'::jsonb))
                 FROM research_stage8_fact_read_v1 AS f
                WHERE f.fact_batch_record_sha256=%s)
                    AS sealed_fact_population_sha256
        """, (
            self.binding["binding_sha256"],
            durable_selection["selection_record_sha256"],
            durable_selection["fact_batch_record_sha256"],
            durable_selection["fact_batch_record_sha256"],
            durable_selection["fact_batch_record_sha256"],
        )).fetchone()
        self.assertEqual(
            dict(raw_hashes),
            outcome_result["evidence_receipt"]["raw_source_hashes"],
        )
        persisted = registry.append_evaluation_receipt_from_connection(
            evaluator, self.binding,
            persistence_payload=outcome_result["persistence_payload"],
        )
        evaluator.commit()
        self.assertEqual(persisted["result_scope"], registry.RESEARCH_RESULT_SCOPE)
        self.assertIs(persisted["evaluation"]["research_qualified"], False)

    def test_five_parent_atomic_diagnostic_is_promoted_only_by_server_replay(self):
        """Caller stays diagnostic; the trigger alone may mint qualification."""
        _, _, _, _, _, durable_selection = self._build_full_path(
            attempt_count=5,
        )
        self._insert_authoritative_outcome_evidence(durable_selection)
        outcome_result = self._read_authoritative_outcome_result(
            durable_selection,
        )
        self.assertEqual(
            outcome_result["evidence_receipt"]["parent_count"], 5,
        )
        self.assertEqual(
            outcome_result["evidence_receipt"][
                "probability_evidence_valid_count"
            ], 5,
        )
        self.assertEqual(
            outcome_result["evidence_receipt"][
                "asymmetry_evidence_valid_count"
            ], 5,
        )
        self.assertIs(
            outcome_result["evaluation"]["atomic_gate_passed"], True,
        )
        self.assertIs(outcome_result["research_qualified"], False)
        self.assertIs(
            outcome_result["evaluation"]["authoritative_fact_replay_verified"],
            False,
        )
        self.assertIn(
            "SERVER_DB_REPLAY_ATTESTATION_REQUIRED",
            outcome_result["evaluation"]["qualification_blockers"],
        )

        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)
        persisted = registry.append_evaluation_receipt_from_connection(
            evaluator, self.binding,
            persistence_payload=outcome_result["persistence_payload"],
        )
        evaluator.commit()
        self.assertIs(persisted["evaluation"]["atomic_gate_passed"], True)
        self.assertIs(persisted["server_replay_verified"], True)
        self.assertIs(persisted["research_qualified"], True)
        self.assertIs(persisted["evaluation"]["research_qualified"], True)
        self.assertEqual(persisted["evaluation"]["qualification_blockers"], [])
        self.assertIs(
            persisted["persistence_payload"]["evaluation"]["research_qualified"],
            False,
        )

        # A caller cannot pre-claim the server-owned result, even after
        # recomputing every unkeyed payload hash.
        contradictory_status = deepcopy(outcome_result["persistence_payload"])
        contradictory_status["evaluation"]["research_qualified"] = True
        contradictory_status["evaluation"]["status"] = (
            "RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY"
        )
        contradictory_status = self._rehash_persistence_payload(
            contradictory_status,
        )
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"],
                durable_selection["selection_record_sha256"],
                contract.canonical(contradictory_status),
            ))

        # Even if every caller-owned semantic pair is changed to a fresh,
        # self-consistent value and every enclosing hash is recomputed, the
        # direct SQL writer cannot turn that replay claim into qualification.
        replay_forgery = deepcopy(outcome_result["persistence_payload"])
        for comparison in replay_forgery["fact_replay_receipt"]["comparisons"]:
            comparison["semantic_hash_before"] = "a" * 64
            comparison["semantic_hash_after"] = "a" * 64
        replay_forgery = self._rehash_persistence_payload(replay_forgery)
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"],
                durable_selection["selection_record_sha256"],
                contract.canonical(replay_forgery),
            ))

        # Rehashed evidence claims are also checked against the exact durable
        # source row, rather than accepted because their outer hash matches.
        probability_forgery = deepcopy(outcome_result["persistence_payload"])
        probability_forgery["evidence_receipt"]["representatives"][0][
            "probability"
        ]["source_status"] = "OPEN"
        probability_forgery = self._rehash_persistence_payload(
            probability_forgery,
        )
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"],
                durable_selection["selection_record_sha256"],
                contract.canonical(probability_forgery),
            ))

        metric_forgery = deepcopy(outcome_result["persistence_payload"])
        metric_forgery["evidence_receipt"]["representatives"][0][
            "asymmetry"
        ]["mfe_pct"] += 0.25
        metric_forgery = self._rehash_persistence_payload(metric_forgery)
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"],
                durable_selection["selection_record_sha256"],
                contract.canonical(metric_forgery),
            ))

        # Replace one authoritative SUCCESS with an actual OPEN source row,
        # while retaining and fully rehashing the caller's SUCCESS claim.  A
        # trigger that trusted only the receipt hashes would accept this
        # classic OPEN-to-SUCCESS promotion.
        evaluator.rollback()
        self._reset_admin()
        first_event_id = durable_selection["representative_identities"][0][
            "event_id"
        ]
        event = dict(self.admin.execute(
            "SELECT * FROM research_events WHERE event_id=%s",
            (first_event_id,),
        ).fetchone())
        self.admin.execute(
            """DELETE FROM research_ordered_first_touch_outcomes
               WHERE event_id=%s AND window_minutes=60 AND threshold_bps=50
                 AND method_version='ordered-first-touch-v7'""",
            (first_event_id,),
        )
        open_outcome = _outcome(
            event, status="OPEN", window=60, threshold_bps=50,
        )
        revision = event["alert_time_utc"].astimezone(UTC) + timedelta(
            minutes=2,
        )
        open_outcome["created_at_utc"] = revision
        open_outcome["updated_at_utc"] = revision
        self._insert("research_ordered_first_touch_outcomes", open_outcome)
        self.admin.commit()

        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)
        open_to_success = deepcopy(outcome_result["persistence_payload"])
        open_to_success["evaluation"]["qualification_blockers"].append(
            "OPEN_SOURCE_REHASH_FORGERY"
        )
        open_to_success = self._rehash_persistence_payload(open_to_success)
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"],
                durable_selection["selection_record_sha256"],
                contract.canonical(open_to_success),
            ))

    def test_direct_five_fake_parents_and_qualified_evaluation_are_rejected(self):
        _, _, _, selected, _, durable = self._build_full_path()
        identity = deepcopy(durable["representative_identities"][0])
        fake = []
        for index in range(5):
            changed = deepcopy(identity)
            changed["btc_parent_movement_id"] = hashlib.sha256(
                f"fake-parent-{index}".encode()
            ).hexdigest()
            fake.append(changed)
        batch = deepcopy(durable["selection_attestation"])
        batch["representative_count"] = 5
        batch["representative_set_sha256"] = "f" * 64
        selector_conn = self._connect(role=registry.SELECTOR_WRITER_ROLE)
        self._expect_rejection(selector_conn, """
                INSERT INTO research_stage8_selection_receipts (
                  fact_batch_record_sha256,exact_binding_sha256,freeze_id,
                  registry_record_sha256,verifier_profile_sha256,
                  registry_verification_receipt_sha256,selector_version,
                  observed_projection_source_sha256,observed_selector_source_sha256,
                  observed_watch_code_manifest_sha256,cohort_query_sha256,
                  outcome_free_population_receipt_sha256,source_high_water_attempt_id,
                  representative_count,representative_set_sha256,
                  representative_identities,selection_attestation)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5,%s,%s::jsonb,%s::jsonb)
            """, (
                durable["fact_batch_record_sha256"], durable["exact_binding_sha256"],
                durable["freeze_id"], durable["registry_record_sha256"],
                durable["verifier_profile_sha256"],
                durable["registry_verification_receipt_sha256"],
                durable["selector_version"], durable["observed_projection_source_sha256"],
                durable["observed_selector_source_sha256"],
                durable["observed_watch_code_manifest_sha256"],
                durable["cohort_query_sha256"],
                durable["outcome_free_population_receipt_sha256"],
                durable["source_high_water_attempt_id"], "f" * 64,
                contract.canonical(fake), contract.canonical(batch),
            ))
        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)
        forged = {
            "version": registry.OUTCOME_PERSISTENCE_VERSION,
            "manifest_sha256": contract.MANIFEST_SHA256,
            "exact_binding_sha256": self.binding["binding_sha256"],
            "selection_record_sha256": durable["selection_record_sha256"],
            "evaluation": {"research_qualified": True},
            "persistence_payload_sha256": "a" * 64,
        }
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"], durable["selection_record_sha256"],
                contract.canonical(forged),
            ))

    def test_ten_parent_losses_cannot_be_hidden_as_unknown(self):
        """A rehashed 5W/5L receipt cannot drop losses from either denominator."""
        _, _, _, _, _, durable = self._build_full_path(attempt_count=10)
        self._insert_authoritative_outcome_evidence(durable, success_count=5)
        result = self._read_authoritative_outcome_result(durable)
        payload = deepcopy(result["persistence_payload"])
        representatives = payload["evidence_receipt"]["representatives"]
        self.assertEqual(len(representatives), 10)
        self.assertEqual(
            sum(item["probability"]["reported_status"] == "SUCCESS"
                for item in representatives),
            5,
        )
        winners = representatives[:5]
        for item in representatives[5:]:
            item["probability"].update({
                "validation_status": "UNKNOWN",
                "reported_status": "UNKNOWN",
                "reasons": ["HIDDEN_TERMINAL_LOSS"],
            })
            item["asymmetry"].update({
                "validation_status": "UNKNOWN",
                "mfe_pct": None,
                "mae_pct": None,
                "zero_denominator": False,
                "reasons": ["HIDDEN_READY_METRIC"],
            })
        payload["evidence_receipt"]["probability_evidence_valid_count"] = 5
        payload["evidence_receipt"]["asymmetry_evidence_valid_count"] = 5
        probability_ids = sorted(
            item["btc_parent_movement_id"] for item in winners
        )
        probability = payload["evaluation"]["routes"]["PROBABILITY"]
        probability.update({
            "status": "PASS", "passed": True, "distinct_parent_count": 5,
            "btc_parent_movement_ids": probability_ids,
            "successes": 5, "failures": 0, "hit_rate_pct": 100.0,
            "wilson_95_lower_pct": acceptance._frozen_wilson(
                5, 5, z=1.959963984540054,
            ),
            "checks": {
                "minimum_distinct_parents": True,
                "hit_rate_pct_gte_70": True,
                "wilson_95_lower_pct_gte_40": True,
            },
        })
        pairs = [
            (float(item["asymmetry"]["mfe_pct"]),
             float(item["asymmetry"]["mae_pct"]))
            for item in winners
        ]
        sum_mfe = sum(pair[0] for pair in pairs)
        sum_mae = sum(pair[1] for pair in pairs)
        asymmetry = payload["evaluation"]["routes"]["ASYMMETRY"]
        asymmetry.update({
            "status": "PASS", "passed": True, "distinct_parent_count": 5,
            "btc_parent_movement_ids": probability_ids,
            "sum_mfe_pct": sum_mfe, "sum_mae_pct": sum_mae,
            "common_window_asymmetry_ratio": sum_mfe / sum_mae,
            "common_window_asymmetry_state": "FINITE",
            "common_window_favorable_dominance_pct":
                100.0 * sum(mfe > mae for mfe, mae in pairs) / len(pairs),
            "common_window_median_paired_edge_pct": sorted(
                mfe - mae for mfe, mae in pairs
            )[len(pairs) // 2],
            "checks": {
                "minimum_distinct_parents": True,
                "common_window_asymmetry_ratio_gte_1_5": True,
                "common_window_favorable_dominance_pct_gte_60": True,
                "common_window_median_paired_edge_pct_gt_0": True,
            },
        })
        payload["evaluation"].update({
            "atomic_gate_passed": True,
            "structurally_eligible": True,
            "research_qualified": False,
            "status": "AWAITING_DURABLE_REGISTRY_VERIFICATION",
            "qualification_blockers": [
                "SERVER_DB_REPLAY_ATTESTATION_REQUIRED"
            ],
        })
        forged = self._rehash_persistence_payload(payload)
        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"], durable["selection_record_sha256"],
                contract.canonical(forged),
            ))

    def test_server_waits_for_all_representative_outcome_horizons(self):
        """An early terminal claim stays diagnostic until the 60m horizon."""
        _, _, _, _, _, durable = self._build_full_path(
            attempt_count=5, recent_last=True,
        )
        self._insert_authoritative_outcome_evidence(durable, common_count=4)
        result = self._read_authoritative_outcome_result(durable)
        self.assertIs(result["evaluation"]["atomic_gate_passed"], True)
        evaluator = self._connect(role=registry.EVALUATOR_WRITER_ROLE)

        future_read = deepcopy(result["persistence_payload"])
        future = (datetime.now(UTC) + timedelta(days=1)).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        future_read["evidence_receipt"]["read_started_at_utc"] = future
        future_read["evidence_receipt"]["read_finished_at_utc"] = future
        future_read = self._rehash_persistence_payload(future_read)
        self._expect_rejection(evaluator, """
                INSERT INTO research_stage8_evaluation_receipts (
                  exact_binding_sha256,selection_record_sha256,persistence_payload)
                VALUES (%s,%s,%s::jsonb)
            """, (
                self.binding["binding_sha256"], durable["selection_record_sha256"],
                contract.canonical(future_read),
            ))

        persisted = registry.append_evaluation_receipt_from_connection(
            evaluator, self.binding,
            persistence_payload=result["persistence_payload"],
        )
        evaluator.commit()
        self.assertIs(persisted["server_replay_verified"], True)
        self.assertIs(persisted["atomic_gate_passed"], True)
        self.assertIs(persisted["research_qualified"], False)
        self.assertIn(
            "REPRESENTATIVE_OUTCOME_HORIZON_NOT_ELAPSED",
            persisted["evaluation"]["qualification_blockers"],
        )

    def test_incomplete_seal_forged_fact_subset_and_append_only_are_rejected(self):
        _, _, _, _, durable_fact, durable_selection = self._build_full_path()
        batch = durable_fact["batch"]
        population = deepcopy(batch["adapter_population_receipt"])
        population["read_finished_at_utc"] = (
            datetime.now(UTC) + timedelta(microseconds=1)
        ).isoformat(timespec="microseconds")
        population.pop("population_receipt_sha256", None)
        population.pop("exact_attempt_population_receipt_sha256", None)
        exact_population_sha = contract.digest(population)
        population["exact_attempt_population_receipt_sha256"] = exact_population_sha
        population_sha = contract.digest(population)
        population["population_receipt_sha256"] = population_sha
        authority = deepcopy(batch["adapter_authority_receipt"])
        authority.pop("authority_receipt_sha256", None)
        authority["population_receipt_sha256"] = population_sha
        authority["exact_attempt_population_receipt_sha256"] = exact_population_sha
        authority_sha = contract.digest(authority)
        authority["authority_receipt_sha256"] = authority_sha
        fact_conn = self._connect(role=registry.FACT_WRITER_ROLE)
        fact_conn.execute("SAVEPOINT negative_batch")
        fact_conn.execute("""
            INSERT INTO research_stage8_projection_fact_batches (
              exact_binding_sha256,freeze_id,registry_record_sha256,
              verifier_profile_sha256,registry_verification_receipt_sha256,
              projection_adapter_version,observed_projection_source_sha256,
              observed_projection_adapter_source_sha256,
              observed_registry_adapter_source_sha256,observed_registry_migration_sha256,
              projection_source_manifest,projection_source_manifest_sha256,
              adapter_query_binding_sha256,adapter_population_receipt,
              adapter_population_receipt_sha256,adapter_authority_receipt,
              adapter_authority_receipt_sha256,adapter_result_sha256,
              coverage_query_scope,coverage_query_sha256,
              outcome_free_population_receipt_sha256,
              coverage_attempt_population_sha256,coverage_source_high_water_attempt_id,
              attempt_ids,attempt_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb,
                    %s,%s::jsonb,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s)
        """, (
            batch["exact_binding_sha256"], batch["freeze_id"],
            batch["registry_record_sha256"], batch["verifier_profile_sha256"],
            batch["registry_verification_receipt_sha256"],
            batch["projection_adapter_version"], batch["observed_projection_source_sha256"],
            batch["observed_projection_adapter_source_sha256"],
            batch["observed_registry_adapter_source_sha256"],
            batch["observed_registry_migration_sha256"],
            contract.canonical(batch["projection_source_manifest"]),
            batch["projection_source_manifest_sha256"], batch["adapter_query_binding_sha256"],
            contract.canonical(population), population_sha, contract.canonical(authority),
            authority_sha, "9" * 64, contract.canonical(batch["coverage_query_scope"]),
            batch["coverage_query_sha256"], batch["outcome_free_population_receipt_sha256"],
            batch["coverage_attempt_population_sha256"],
            batch["coverage_source_high_water_attempt_id"],
            contract.canonical(batch["attempt_ids"]), batch["attempt_count"],
        ))
        copied = fact_conn.execute("""
            SELECT fact_batch_record_sha256 FROM research_stage8_fact_batch_read_v1
            WHERE adapter_population_receipt_sha256=%s
        """, (population_sha,)).fetchone()["fact_batch_record_sha256"]
        deferred_rejection = self._expect_rejection(
            fact_conn,
            "SET CONSTRAINTS trg_stage8_fact_batch_complete_at_commit IMMEDIATE",
        )
        if self.pglite_compat:
            self.assertIn(
                "must be completed and sealed in its insert transaction",
                deferred_rejection["message"],
            )
        self._expect_rejection(fact_conn, """
                INSERT INTO research_stage8_projection_fact_batch_seals
                  (fact_batch_record_sha256,fact_count,fact_records_sha256)
                VALUES (%s,2,%s)
            """, (copied, "8" * 64))
        fact_conn.execute("ROLLBACK TO SAVEPOINT negative_batch")
        # Exact query-scope recomputation rejects a self-consistently rehashed subset.
        fact_conn.execute("SAVEPOINT subset")
        subset_population = deepcopy(population)
        subset_population["requested_attempt_ids"] = [1]
        subset_population["found_attempt_ids"] = [1]
        subset_population.pop("population_receipt_sha256", None)
        subset_population.pop("exact_attempt_population_receipt_sha256", None)
        subset_exact = contract.digest(subset_population)
        subset_population["exact_attempt_population_receipt_sha256"] = subset_exact
        subset_sha = contract.digest(subset_population)
        subset_population["population_receipt_sha256"] = subset_sha
        subset_authority = deepcopy(authority)
        subset_authority.pop("authority_receipt_sha256", None)
        subset_authority["population_receipt_sha256"] = subset_sha
        subset_authority["exact_attempt_population_receipt_sha256"] = subset_exact
        subset_authority_sha = contract.digest(subset_authority)
        subset_authority["authority_receipt_sha256"] = subset_authority_sha
        self._expect_rejection(fact_conn, """
                INSERT INTO research_stage8_projection_fact_batches (
                  exact_binding_sha256,freeze_id,registry_record_sha256,
                  verifier_profile_sha256,registry_verification_receipt_sha256,
                  projection_adapter_version,observed_projection_source_sha256,
                  observed_projection_adapter_source_sha256,
                  observed_registry_adapter_source_sha256,observed_registry_migration_sha256,
                  projection_source_manifest,projection_source_manifest_sha256,
                  adapter_query_binding_sha256,adapter_population_receipt,
                  adapter_population_receipt_sha256,adapter_authority_receipt,
                  adapter_authority_receipt_sha256,adapter_result_sha256,
                  coverage_query_scope,coverage_query_sha256,
                  outcome_free_population_receipt_sha256,
                  coverage_attempt_population_sha256,coverage_source_high_water_attempt_id,
                  attempt_ids,attempt_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb,
                        %s,%s::jsonb,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,1)
            """, (
                batch["exact_binding_sha256"], batch["freeze_id"],
                batch["registry_record_sha256"], batch["verifier_profile_sha256"],
                batch["registry_verification_receipt_sha256"],
                batch["projection_adapter_version"], batch["observed_projection_source_sha256"],
                batch["observed_projection_adapter_source_sha256"],
                batch["observed_registry_adapter_source_sha256"],
                batch["observed_registry_migration_sha256"],
                contract.canonical(batch["projection_source_manifest"]),
                batch["projection_source_manifest_sha256"], batch["adapter_query_binding_sha256"],
                contract.canonical(subset_population), subset_sha,
                contract.canonical(subset_authority), subset_authority_sha, "7" * 64,
                contract.canonical(batch["coverage_query_scope"]),
                batch["coverage_query_sha256"], "6" * 64, "5" * 64,
                batch["coverage_source_high_water_attempt_id"], "[1]",
            ))
        fact_conn.execute("ROLLBACK TO SAVEPOINT subset")
        fact_conn.rollback()
        fact_conn.read_only = False
        fact_conn.isolation_level = self.psycopg.IsolationLevel.READ_COMMITTED
        isolation_rejection = self._expect_rejection(
            fact_conn,
            """INSERT INTO research_stage8_projection_fact_batches
                   (exact_binding_sha256) VALUES (%s)""",
            (batch["exact_binding_sha256"],),
        )
        if self.pglite_compat:
            self.assertIn(
                "require an actual REPEATABLE READ transaction",
                isolation_rejection["message"],
            )
        fact_conn.rollback()
        if self.pglite_compat:
            self._reset_admin()
        for table, key, value in (
            ("research_stage8_binding_registry", "exact_binding_sha256",
             batch["exact_binding_sha256"]),
            ("research_stage8_projected_fact_ledger", "fact_batch_record_sha256",
             batch["fact_batch_record_sha256"]),
            ("research_stage8_selection_receipts", "selection_record_sha256",
             durable_selection["selection_record_sha256"]),
        ):
            with self.subTest(table=table):
                self.admin.execute("SAVEPOINT immutable")
                self._expect_rejection(
                    self.admin,
                    self.sql.SQL("UPDATE {} SET {}={} WHERE {}=%s").format(
                        self.sql.Identifier(table), self.sql.Identifier(key),
                        self.sql.Identifier(key), self.sql.Identifier(key),
                    ), (value,),
                )
                self.admin.execute("ROLLBACK TO SAVEPOINT immutable")

    def test_sql_python_canonical_hash_vectors(self):
        vectors = (
            {"minus_zero": -0.0, "tiny": 1e-6, "huge": 1e20},
            {"unicode": {"é": 1, "א": 2, "😀": 3, "A": 4}},
        )
        for value in vectors:
            with self.subTest(value=value):
                row = self.admin.execute(
                    "SELECT research_stage8_json_sha256_v1(%s::jsonb) AS digest",
                    (contract.canonical(value),),
                ).fetchone()
                self.assertEqual(row["digest"], contract.digest(value))


if __name__ == "__main__":
    unittest.main(verbosity=2)
