"""Database-free transactional/conflict checks for Stage-4 persistence."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import os
from unittest.mock import patch

import research_signal_snapshot as snapshots
import research_signal_snapshot_store as store
import research_signal_snapshot_selftest as snapshot_selftest
from research_signal_snapshot_selftest import (
    _build,
    _canonical_inputs,
    _derivatives,
    _strong_payload,
)


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return deepcopy(self._row)


class _Connection:
    def __init__(self, driver):
        self.driver = driver
        self.backup = (deepcopy(self.driver.rows), self.driver.next_id)
        self.closed = False

    def __enter__(self):
        self.backup = (deepcopy(self.driver.rows), self.driver.next_id)
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is not None:
            rows, next_id = self.backup
            self.driver.rows.clear()
            self.driver.rows.update(rows)
            self.driver.next_id = next_id
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split()).upper()
        if normalized.startswith("LOCK TABLE PUBLIC.RESEARCH_EVENTS"):
            return _Result(None)
        if "SIGNAL_SNAPSHOT:SCHEMA_READINESS" in normalized:
            return _Result(self.driver.readiness)
        if "TO_REGCLASS" in normalized:
            return _Result({"events": "research_events"})
        if "PG_TRY_ADVISORY_LOCK" in normalized:
            acquired = not self.driver.advisory_locked
            if acquired:
                self.driver.advisory_locked = True
            return _Result({"acquired": acquired})
        if "PG_ADVISORY_UNLOCK" in normalized:
            self.driver.advisory_locked = False
            return _Result({"unlocked": True})
        if normalized.startswith("INSERT INTO PUBLIC.RESEARCH_EVENTS"):
            row = dict(params or {})
            fingerprint = row["event_fingerprint"]
            if fingerprint in self.driver.rows:
                return _Result(None)
            stored = deepcopy(row)
            stored["event_id"] = self.driver.next_id
            stored["categories"] = json.loads(stored["categories"])
            stored["engine_snapshot"] = json.loads(stored["engine_snapshot"])
            self.driver.next_id += 1
            self.driver.rows[fingerprint] = stored
            return _Result({"event_id": stored["event_id"]})
        if "FROM PUBLIC.RESEARCH_EVENTS" in normalized:
            fingerprint = (params or {}).get("event_fingerprint")
            return _Result(self.driver.rows.get(fingerprint))
        raise AssertionError(f"unexpected SQL in self-test: {normalized}")

    def commit(self):
        self.backup = (deepcopy(self.driver.rows), self.driver.next_id)

    def rollback(self):
        rows, next_id = self.backup
        self.driver.rows.clear()
        self.driver.rows.update(deepcopy(rows))
        self.driver.next_id = next_id

    def close(self):
        self.closed = True


class _Driver:
    def __init__(self):
        self.rows = {}
        self.next_id = 1
        self.advisory_locked = False
        self.readiness = {
            "events": "research_events",
            "writer_ready": True,
            "triggers_ready": True,
            "indexes_ready": True,
            "functions_ready": True,
            "visibility_ready": True,
            "envelope_function_ready": True,
            "completeness_function_ready": True,
            "commitment_function_ready": True,
            "identity_function_ready": True,
        }

    def connect(self, *args, **kwargs):
        return _Connection(self)


def run() -> None:
    payload = _strong_payload()
    derivatives = _derivatives()
    opportunities, magnets, directional = _canonical_inputs(
        payload, derivatives
    )
    batch = _build(
        payload,
        opportunities=opportunities,
        magnets=magnets,
        directional=directional,
        derivatives=derivatives,
    )
    with patch.object(
        snapshot_selftest,
        "BASE",
        snapshot_selftest.BASE + timedelta(hours=1),
    ):
        fresh_payload = _strong_payload()
        fresh_derivatives = _derivatives()
        fresh_opportunities, fresh_magnets, fresh_directional = (
            _canonical_inputs(fresh_payload, fresh_derivatives)
        )
        fresh_batch = _build(
            fresh_payload,
            opportunities=fresh_opportunities,
            magnets=fresh_magnets,
            directional=fresh_directional,
            derivatives=fresh_derivatives,
            snapshot_set_id=18,
        )
    assert fresh_batch.snapshot_key != batch.snapshot_key
    event = next(
        item
        for item in batch.events
        if item.event_type == snapshots.COMBINED_EVENT_TYPE
    )
    projection = next(
        item
        for item in batch.events
        if item.event_type == snapshots.PROJECTION_EVENT_TYPE
    )
    row = store._serialize(event)
    assert row["capture_stage"] == snapshots.CAPTURE_STAGE
    assert row["delivery_status"] == "NOT_APPLICABLE"
    assert row["event_kind"] == "DECISION_SAMPLE"

    existing = deepcopy(row)
    existing["event_id"] = 1
    existing["categories"] = json.loads(existing["categories"])
    existing["engine_snapshot"] = json.loads(existing["engine_snapshot"])
    store._verify_existing(existing, row)
    changed = deepcopy(existing)
    changed["engine_snapshot"]["vote_count"] = 99
    try:
        store._verify_existing(changed, row)
    except store.SignalSnapshotConflictError as exc:
        assert "engine_snapshot" in str(exc)
    else:
        raise AssertionError("different immutable payload was accepted")
    changed_text = deepcopy(existing)
    changed_text["symbol"] = existing["symbol"] + " "
    try:
        store._verify_existing(changed_text, row)
    except store.SignalSnapshotConflictError:
        pass
    else:
        raise AssertionError("whitespace-changing immutable text was accepted")
    changed_number = deepcopy(existing)
    changed_number["current_price"] = float(existing["current_price"]) + 1e-12
    try:
        store._verify_existing(changed_number, row)
    except store.SignalSnapshotConflictError:
        pass
    else:
        raise AssertionError("near-but-different immutable number was accepted")

    projection_row = store._serialize(projection)
    projection_existing = deepcopy(projection_row)
    projection_existing["event_id"] = 99
    projection_existing["categories"] = json.loads(
        projection_existing["categories"]
    )
    projection_existing["engine_snapshot"] = json.loads(
        projection_existing["engine_snapshot"]
    )
    terminal = store._projection_result(
        projection_existing, snapshot_key=batch.snapshot_key
    )
    assert terminal["terminal"] is True
    malformed_projection = deepcopy(projection_existing)
    malformed_projection["symbol"] = "BTC"
    try:
        store._projection_result(
            malformed_projection, snapshot_key=batch.snapshot_key
        )
    except store.SignalSnapshotConflictError:
        pass
    else:
        raise AssertionError("malformed terminal projection was accepted")

    original_driver = store.psycopg
    original_row_factory = store.dict_row
    original_persistence_status = store.research_event_store.persistence_status
    original_archive_url = os.environ.get("DATABASE_URL")
    driver = _Driver()
    store.psycopg = driver
    store.dict_row = object()
    try:
        with patch.dict(os.environ, {}, clear=False), patch.object(
            store.research_event_store,
            "persistence_status",
            return_value={"enabled": True},
        ) as persistence_status:
            os.environ[store.DATABASE_URL_ENV] = (
                "postgresql://snapshot-writer@DB.EXAMPLE:5433/research"
                "?sslmode=require"
            )
            os.environ["DATABASE_URL"] = (
                "postgres://archive-reader@db.example:5433/research"
                "?application_name=archive"
            )
            configured = store.status()
            assert configured["configured"] is True
            assert configured["database_source"] == store.DATABASE_URL_ENV
            assert configured["archive_database_aligned"] is True
            assert configured["trusted_writer_role"] == store.TRUSTED_WRITER_ROLE
            assert store._database_url(None).startswith(
                "postgresql://snapshot-writer@DB.EXAMPLE:5433/research"
            )

            invalid_targets = (
                "postgresql://snapshot-writer@db.example:5433/research"
                "?host=db-override.example",
                "postgresql://snapshot-writer@db.example:5433/research"
                "?hostaddr=192.0.2.10",
                "postgresql://snapshot-writer@db.example:5433/research"
                "?port=5434",
                "postgresql://snapshot-writer@db.example:5433/research"
                "?dbname=research_override",
                "postgresql://snapshot-writer@db.example:5433/research"
                "?service=snapshot-writer",
                "postgresql://snapshot-writer@db.example:5433/research"
                "?servicefile=/etc/postgresql/pg_service.conf",
                "postgresql://snapshot-writer@db-a.example,db-b.example/research",
                "postgresql://snapshot-writer@db-a.example:5433,"
                "db-b.example:5433/research",
                "postgresql://snapshot-writer@db.example:notaport/research",
            )
            for invalid_target in invalid_targets:
                os.environ[store.DATABASE_URL_ENV] = invalid_target
                assert store._database_target(invalid_target) == ("", "", 0, "")
                assert store.status()["archive_database_aligned"] is False
                try:
                    store._database_url(None)
                except RuntimeError as exc:
                    assert (
                        "database target" in str(exc)
                        or "same DATABASE_URL" in str(exc)
                    )
                else:
                    raise AssertionError(
                        "database target override or multi-host URL was accepted"
                    )

            os.environ["DATABASE_URL"] = (
                "postgresql://archive-reader@db.example:notaport/research"
            )
            assert store.status()["archive_database_aligned"] is False
            os.environ["DATABASE_URL"] = (
                "postgres://archive-reader@db.example:5433/research"
                "?application_name=archive"
            )

            os.environ[store.DATABASE_URL_ENV] = (
                "postgresql://snapshot-writer@db.example:5433/research-next"
            )
            assert store.status()["archive_database_aligned"] is False
            try:
                store._database_url(None)
            except RuntimeError as exc:
                assert "same DATABASE_URL" in str(exc)
            else:
                raise AssertionError("misaligned archive database was accepted")

            try:
                store._database_url(
                    "postgresql://snapshot-writer@other.example:5433/research"
                )
            except RuntimeError as exc:
                assert "same DATABASE_URL" in str(exc)
            else:
                raise AssertionError("explicit cross-database override was accepted")

            os.environ[store.DATABASE_URL_ENV] = (
                "postgresql://snapshot-writer@db.example:5433/research"
            )
            persistence_status.return_value = {"enabled": False}
            try:
                store._database_url(None)
            except RuntimeError as exc:
                assert "persistence is not enabled" in str(exc)
            else:
                raise AssertionError("disabled research persistence was accepted")

            persistence_status.return_value = {"enabled": True}
            del os.environ[store.DATABASE_URL_ENV]
            missing = store.status()
            assert missing["configured"] is False
            assert missing["database_source"] is None
            assert missing["archive_database_aligned"] is False
            try:
                store._database_url(None)
            except RuntimeError as exc:
                assert "not configured" in str(exc)
            else:
                raise AssertionError("missing dedicated database URL was accepted")

        test_database_url = "postgresql://selftest@stage4-selftest/research"
        os.environ["DATABASE_URL"] = test_database_url
        store.research_event_store.persistence_status = lambda: {"enabled": True}

        inserted = store.persist_events(
            batch.events, database_url=test_database_url
        )
        assert inserted["inserted"] == len(batch.events)
        assert inserted["idempotent_existing"] == 0
        repeated = store.persist_events(
            batch.events, database_url=test_database_url
        )
        assert repeated["inserted"] == 0
        assert repeated["idempotent_existing"] == len(batch.events)
        assert repeated["event_ids"] == inserted["event_ids"]

        conflicting = replace(
            event,
            engine_snapshot={**event.engine_snapshot, "vote_count": 99},
        )
        conflicting_batch = tuple(
            conflicting if item.event_fingerprint == event.event_fingerprint else item
            for item in batch.events
        )
        try:
            store.persist_events(
                conflicting_batch, database_url=test_database_url
            )
        except store.SignalSnapshotConflictError:
            pass
        else:
            raise AssertionError("same identity/different payload was accepted")

        before_unready = deepcopy(driver.rows)
        driver.readiness["indexes_ready"] = False
        try:
            store.persist_events(
                batch.events, database_url=test_database_url
            )
        except RuntimeError as exc:
            assert "migration 023" in str(exc)
        else:
            raise AssertionError("schema without required indexes was accepted")
        assert driver.rows == before_unready
        driver.readiness["indexes_ready"] = True

        before = deepcopy(driver.rows)
        try:
            store.persist_events(
                (*fresh_batch.events, *conflicting_batch),
                database_url=test_database_url,
            )
        except store.SignalSnapshotConflictError:
            pass
        else:
            raise AssertionError("conflicting batch did not fail")
        assert driver.rows == before, "failed batch was not rolled back atomically"

        lease = store.acquire_projection_lease(
            fresh_batch.snapshot_key, database_url=test_database_url
        )
        assert lease is not None
        assert lease.load()["terminal"] is False
        lease_result = lease.persist(fresh_batch.events)
        assert lease_result["persisted"] is True
        loaded = lease.load()
        assert loaded["terminal"] is True
        assert loaded["status"] == "COMPLETED"
        assert loaded["evaluation_status"] == "EVALUABLE"
        lease.close()
        assert driver.advisory_locked is False

        second_lease = store.acquire_projection_lease(
            fresh_batch.snapshot_key, database_url=test_database_url
        )
        assert second_lease is not None
        assert second_lease.load()["terminal"] is True
        second_lease.close()
    finally:
        store.psycopg = original_driver
        store.dict_row = original_row_factory
        store.research_event_store.persistence_status = original_persistence_status
        if original_archive_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original_archive_url

    print("Research signal snapshot store self-test: PASS")
    print("Idempotent full-payload verification: PASS")
    print("Transactional conflict rollback: PASS")


if __name__ == "__main__":
    run()
