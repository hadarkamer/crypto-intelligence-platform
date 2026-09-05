"""Deterministic checks for the isolated Stage-4 experimental outbox.

The production store is deliberately PostgreSQL-only.  These checks use a
small statement-aware in-memory connection so the trust-boundary behavior is
exercised without weakening the runtime with a fallback database.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os

import research_experimental_formula_alert as alerts
import research_signal_formula_exploration as exploration
import research_stage4_candidate_search as search
import research_stage4_experimental_store as store


UTC = timezone.utc
AS_OF = datetime(2026, 9, 5, 12, 5, tzinfo=UTC)
HORIZON = 60


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _features(*names: str) -> search._CompactFeatureMapping:
    mask = sum(1 << search._BOOLEAN_FEATURES.index(name) for name in names)
    return search._CompactFeatureMapping(mask, None)


def _historical_observation(index: int) -> search.CompactStage4CandidateObservation:
    return search.CompactStage4CandidateObservation(
        observation_id=_hash(f"historical-observation-{index}"),
        projection_event_id=index,
        projection_decision_time_utc=(
            AS_OF - timedelta(hours=4) + timedelta(minutes=10 * index)
        ).isoformat(),
        symbol="BTC",
        direction="LONG",
        features=_features(
            exploration.FEATURE_MAX_PAIN_CONFIRMED,
            exploration.FEATURE_MAGNET_CONFIRMED,
        ),
        wave_binding=search._CompactWaveBinding(
            "BOUND", None, _hash(f"historical-wave-{index}")
        ),
        outcome=search._CompactOutcome(
            "AVAILABLE",
            None,
            HORIZON,
            search._CompactOutcomePath(1.5, 2.0, 0.5),
        ),
    )


def _current_observation() -> search.CompactCurrentStage4Observation:
    decision = AS_OF + timedelta(minutes=25)
    return search.CompactCurrentStage4Observation(
        observation_id=_hash("current-observation"),
        projection_event_id=10_001,
        projection_event_fingerprint=_hash("current-projection"),
        snapshot_set_id=20_001,
        snapshot_key=_hash("current-snapshot"),
        projection_decision_time_utc=decision.isoformat(),
        archive_cycle_time_utc=decision.replace(
            minute=0, second=0, microsecond=0
        ).isoformat(),
        symbol="BTC",
        direction="LONG",
        features=_features(
            exploration.FEATURE_MAX_PAIN_CONFIRMED,
            exploration.FEATURE_MAGNET_CONFIRMED,
        ),
        source_event_ids=(30_001,),
        source_event_fingerprints=(_hash("current-source"),),
        wave_binding=search._CompactWaveBinding(
            "BOUND", None, _hash("current-parent-wave")
        ),
    )


def _fixture() -> tuple[dict, dict, alerts.ExperimentalFormulaAlert]:
    result = search.search_experimental_candidates(
        [_historical_observation(index) for index in range(1, 7)],
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
        config=search.Stage4SearchConfig(wall_budget_ms=5_000),
    )
    assert result["status"] == "ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND"
    assert len(result["eligible_candidate_variants"]) >= 2
    envelope = alerts.compact_eligible_search_envelope(result)
    built = alerts.build_experimental_alerts(
        [_current_observation()],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )
    assert len(built) == 1
    empty = search.search_experimental_candidates(
        [],
        horizon_minutes=240,
        analysis_as_of_utc=AS_OF,
        config=search.Stage4SearchConfig(wall_budget_ms=5_000),
    )
    assert empty["status"] == "EMPTY_CORPUS"
    return result, empty, built[0]


class _Result:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _MemoryDatabase:
    def __init__(self, *, now: datetime):
        self.now = now
        self.commit_count = 0
        self.searches: dict[str, dict] = {}
        self.alerts: dict[str, dict] = {}
        self.subscriptions: dict[int, dict] = {}
        self.deliveries: dict[str, dict] = {}
        self.attempt_events: dict[str, dict] = {}

    def connect(self, *, read_only: bool):
        return _MemoryConnection(self, read_only=read_only)


class _MemoryConnection:
    def __init__(self, database: _MemoryDatabase, *, read_only: bool):
        self.database = database
        self.read_only = read_only

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def commit(self):
        self.database.commit_count += 1

    @staticmethod
    def _sql(statement: str) -> str:
        return " ".join(statement.lower().split())

    @staticmethod
    def _json(value):
        return json.loads(value) if isinstance(value, str) else deepcopy(value)

    def execute(self, statement: str, parameters=()):
        sql = self._sql(statement)
        if sql.startswith(
            "insert into public.research_formula_experimental_search_runs_v1"
        ):
            return self._insert_search(parameters)
        if (
            "from public.research_formula_experimental_search_runs_v1"
            in sql
            and "where search_receipt_sha256=%s" in sql
        ):
            row = self.database.searches.get(str(parameters[0]))
            return _Result([deepcopy(row)] if row else [])
        if sql.startswith(
            "update public.research_formula_experimental_subscriptions_v1"
        ):
            chat_id = int(parameters[0])
            row = self.database.subscriptions.get(chat_id)
            if row is None:
                return _Result()
            row["active"] = False
            row["updated_at_utc"] = self.database.now
            return _Result([deepcopy(row)])
        if sql.startswith(
            "insert into public.research_formula_experimental_subscriptions_v1"
        ):
            return self._upsert_subscription(parameters)
        if (
            "from public.research_formula_experimental_subscriptions_v1"
            in sql
            and "where chat_id=%s" in sql
        ):
            row = self.database.subscriptions.get(int(parameters[0]))
            return _Result([deepcopy(row)] if row else [])
        if sql.startswith(
            "insert into public.research_formula_experimental_alerts_v1"
        ):
            return self._insert_alert(parameters)
        if (
            "from public.research_formula_experimental_alerts_v1"
            in sql
            and "where alert_occurrence_id=%s" in sql
        ):
            return self._find_alert_conflict(parameters)
        if (
            "from public.research_formula_experimental_subscriptions_v1"
            in sql
            and "where active=true" in sql
        ):
            decision_time, expires_at = parameters
            rows = [
                {"chat_id": chat_id}
                for chat_id, row in sorted(self.database.subscriptions.items())
                if row["active"] is True
                and row["updated_at_utc"] <= decision_time
                and expires_at > self.database.now
            ]
            return _Result(rows)
        if sql.startswith(
            "insert into public.research_formula_experimental_deliveries_v1"
        ):
            return self._insert_delivery(parameters)
        if sql.startswith(
            "select delivery_key, alert_occurrence_id, chat_id "
            "from public.research_formula_experimental_deliveries_v1"
        ) and "or (alert_occurrence_id=%s and chat_id=%s)" in sql:
            delivery_key, alert_id, chat_id = parameters
            rows = [
                deepcopy(row)
                for row in self.database.deliveries.values()
                if row["delivery_key"] == delivery_key
                or (
                    row["alert_occurrence_id"] == alert_id
                    and row["chat_id"] == int(chat_id)
                )
            ]
            rows.sort(key=lambda row: row["delivery_key"])
            return _Result(rows)
        if (
            "from public.research_formula_experimental_deliveries_v1"
            in sql
            and "where status='in_flight'" in sql
            and "claim_expires_at_utc <= transaction_timestamp()" in sql
        ):
            rows = [
                {
                    "delivery_key": row["delivery_key"],
                    "attempt_count": row["attempt_count"],
                    "claim_token": row["claim_token"],
                }
                for row in self.database.deliveries.values()
                if row["status"] == "IN_FLIGHT"
                and row["claim_expires_at_utc"] <= self.database.now
            ]
            return _Result(sorted(rows, key=lambda row: row["delivery_key"])[:200])
        if sql.startswith(
            "update public.research_formula_experimental_deliveries_v1"
        ) and "set status='ambiguous'" in sql:
            error, delivery_key, claim_token = parameters
            row = self.database.deliveries.get(str(delivery_key))
            if row and row["status"] == "IN_FLIGHT" and row["claim_token"] == claim_token:
                row["status"] = "AMBIGUOUS"
                row["last_failure_kind"] = "AMBIGUOUS_SEND"
                row["last_error"] = error
            return _Result()
        if sql.startswith(
            "insert into public.research_formula_experimental_delivery_attempt_events_v1"
        ):
            return self._insert_attempt(parameters)
        if sql.startswith("with expired as ("):
            for row in self.database.deliveries.values():
                alert = self.database.alerts[row["alert_occurrence_id"]]
                if (
                    row["status"] in {"PENDING", "RETRYABLE"}
                    and alert["expires_at_utc"] <= self.database.now
                ):
                    row["status"] = "EXPIRED"
                    row["last_failure_kind"] = "EXPIRED_BEFORE_SEND"
                    row["last_error"] = (
                        "experimental alert expired before a safe send claim"
                    )
            return _Result()
        if (
            "join public.research_formula_experimental_alerts_v1 alert"
            in sql
            and "join public.research_formula_experimental_subscriptions_v1 subscription"
            in sql
            and "for update of delivery skip locked" in sql
        ):
            max_attempts, minimum_validity_seconds, limit = parameters
            return self._due_deliveries(
                int(limit),
                max_attempts=int(max_attempts),
                minimum_validity_seconds=int(minimum_validity_seconds),
            )
        if sql.startswith(
            "update public.research_formula_experimental_deliveries_v1"
        ) and "set status='in_flight'" in sql:
            return self._claim(parameters)
        if (
            "select delivery.*, alert.expires_at_utc" in sql
            and "where delivery.delivery_key=%s" in sql
        ):
            row = self.database.deliveries.get(str(parameters[0]))
            if row is None:
                return _Result()
            joined = deepcopy(row)
            joined["expires_at_utc"] = self.database.alerts[
                row["alert_occurrence_id"]
            ]["expires_at_utc"]
            return _Result([joined])
        if sql == "select clock_timestamp() as database_now_utc":
            return _Result([{"database_now_utc": self.database.now}])
        if sql.startswith(
            "update public.research_formula_experimental_deliveries_v1"
        ) and "set status='sent'" in sql:
            return self._complete_sent(parameters)
        if sql.startswith(
            "update public.research_formula_experimental_deliveries_v1"
        ) and "set status='retryable'" in sql:
            return self._complete_retry(parameters)
        if sql.startswith(
            "update public.research_formula_experimental_deliveries_v1"
        ) and "set status=%s" in sql:
            return self._complete_terminal(parameters)
        raise AssertionError(f"unhandled experimental-store SQL: {sql[:240]}")

    def _insert_search(self, values):
        (
            run_id,
            receipt,
            source_receipt,
            chain,
            engine,
            candidate_schema,
            feature_schema,
            label_policy,
            independence_policy,
            multiple_testing_policy,
            schedule_slot,
            analysis_as_of,
            horizon,
            input_count,
            eligible_count,
            status,
            payload,
            payload_hash,
        ) = values
        if receipt in self.database.searches or any(
            row["search_run_id"] == run_id
            for row in self.database.searches.values()
        ):
            return _Result()
        row = {
            "search_run_id": run_id,
            "search_receipt_sha256": receipt,
            "source_corpus_receipt_sha256": source_receipt,
            "input_observation_chain_sha256": chain,
            "engine_version": engine,
            "candidate_schema_version": candidate_schema,
            "feature_schema_version": feature_schema,
            "label_policy_version": label_policy,
            "independence_policy_version": independence_policy,
            "multiple_testing_policy_version": multiple_testing_policy,
            "schedule_slot_utc": schedule_slot,
            "analysis_as_of_utc": analysis_as_of,
            "horizon_minutes": horizon,
            "input_observation_count": input_count,
            "eligible_candidate_count": eligible_count,
            "search_status": status,
            "search_payload": self._json(payload),
            "search_payload_sha256": payload_hash,
            "formula_registry_effect": "NONE",
            "delivery_channel": "NONE",
            "live_eligible": False,
            "telegram_delivery_allowed": False,
            "trade_execution_allowed": False,
            "created_at_utc": self.database.now,
        }
        self.database.searches[str(receipt)] = row
        return _Result([deepcopy(row)])

    def _upsert_subscription(self, values):
        chat_id, user_id, policy, source, scope, disclaimer = values
        row = self.database.subscriptions.get(int(chat_id))
        if row is None:
            row = {
                "chat_id": int(chat_id),
                "active": True,
                "requested_by_user_id": int(user_id),
                "subscription_policy_version": policy,
                "consent_source": source,
                "delivery_scope": scope,
                "disclaimer_acknowledged": disclaimer,
                "disclaimer_acknowledged_at_utc": self.database.now,
                "subscribed_at_utc": self.database.now,
                "updated_at_utc": self.database.now,
            }
            self.database.subscriptions[int(chat_id)] = row
        else:
            if row["active"] is True:
                return _Result()
            row["active"] = True
            row["updated_at_utc"] = self.database.now
        return _Result([deepcopy(row)])

    def _insert_alert(self, values):
        alert_id = str(values[0])
        candidate_key = str(values[2])
        trigger_key = str(values[5])
        conflict = alert_id in self.database.alerts or any(
            row["candidate_key"] == candidate_key
            and row["trigger_key"] == trigger_key
            for row in self.database.alerts.values()
        )
        if conflict:
            return _Result()
        row = {
            "alert_occurrence_id": alert_id,
            "search_run_id": values[1],
            "candidate_key": candidate_key,
            "search_receipt_sha256": values[3],
            "candidate_snapshot": self._json(values[4]),
            "trigger_key": trigger_key,
            "trigger_observation_id": values[6],
            "projection_event_id": values[7],
            "projection_event_fingerprint": values[8],
            "btc_parent_movement_id": values[9],
            "symbol": values[10],
            "direction": values[11],
            "horizon_minutes": values[12],
            "decision_time_utc": values[13],
            "expires_at_utc": values[14],
            "trigger_snapshot": self._json(values[15]),
            "trigger_snapshot_sha256": values[16],
            "current_trigger_receipt_sha256": values[17],
            "current_trigger_policy_version": values[18],
            "formula_text": values[19],
            "conditions": self._json(values[20]),
            "independent_movement_count": values[21],
            "accepted_paths": self._json(values[22]),
            "metrics": self._json(values[23]),
            "experimental_reasons": self._json(values[24]),
            "renderer_version": values[25],
            "rendered_message": values[26],
            "rendered_message_sha256": values[27],
            "disclaimer": values[28],
            "delivery_channel": values[29],
            "formula_registry_effect": "NONE",
            "human_formula_approval_required": False,
            "live_eligible": False,
            "trade_execution_allowed": False,
            "telegram_delivery_allowed": True,
            "created_at_utc": self.database.now,
        }
        self.database.alerts[alert_id] = row
        return _Result(
            [{"alert_occurrence_id": alert_id, "created_at_utc": self.database.now}]
        )

    def _find_alert_conflict(self, values):
        alert_id, candidate_key, trigger_key, _preferred = values
        matches = [
            row
            for row in self.database.alerts.values()
            if row["alert_occurrence_id"] == alert_id
            or (
                row["candidate_key"] == candidate_key
                and row["trigger_key"] == trigger_key
            )
        ]
        matches.sort(key=lambda row: row["alert_occurrence_id"] != alert_id)
        return _Result([deepcopy(matches[0])] if matches else [])

    def _insert_delivery(self, values):
        delivery_key, alert_id, chat_id = values
        if delivery_key in self.database.deliveries or any(
            row["alert_occurrence_id"] == alert_id
            and row["chat_id"] == int(chat_id)
            for row in self.database.deliveries.values()
        ):
            return _Result()
        row = {
            "delivery_key": delivery_key,
            "alert_occurrence_id": alert_id,
            "chat_id": int(chat_id),
            "status": "PENDING",
            "attempt_count": 0,
            "available_at_utc": self.database.now,
            "claim_token": None,
            "claimed_at_utc": None,
            "claim_expires_at_utc": None,
            "sent_at_utc": None,
            "telegram_message_id": None,
            "last_failure_kind": None,
            "last_error": None,
            "created_at_utc": self.database.now,
            "updated_at_utc": self.database.now,
        }
        self.database.deliveries[str(delivery_key)] = row
        return _Result([{"delivery_key": delivery_key}])

    def _due_deliveries(
        self,
        limit: int,
        *,
        max_attempts: int,
        minimum_validity_seconds: int,
    ):
        rows = []
        for delivery in self.database.deliveries.values():
            alert = self.database.alerts[delivery["alert_occurrence_id"]]
            subscription = self.database.subscriptions.get(delivery["chat_id"])
            if (
                delivery["status"] in {"PENDING", "RETRYABLE"}
                and delivery["attempt_count"] < max_attempts
                and delivery["available_at_utc"] <= self.database.now
                and alert["expires_at_utc"]
                > self.database.now
                + timedelta(seconds=minimum_validity_seconds)
                and subscription
                and subscription["active"] is True
                and subscription["updated_at_utc"] <= alert["decision_time_utc"]
            ):
                rows.append(
                    {
                        "delivery_key": delivery["delivery_key"],
                        "alert_occurrence_id": delivery["alert_occurrence_id"],
                        "chat_id": delivery["chat_id"],
                        "attempt_count": delivery["attempt_count"],
                        "expires_at_utc": alert["expires_at_utc"],
                        "rendered_message": alert["rendered_message"],
                        "rendered_message_sha256": alert[
                            "rendered_message_sha256"
                        ],
                    }
                )
        rows.sort(key=lambda row: row["delivery_key"])
        return _Result(rows[:limit])

    def _claim(self, values):
        claim_token, lease_seconds, delivery_key = values
        row = self.database.deliveries.get(str(delivery_key))
        if row is None or row["status"] not in {"PENDING", "RETRYABLE"}:
            return _Result()
        row["status"] = "IN_FLIGHT"
        row["attempt_count"] += 1
        row["claim_token"] = claim_token
        row["claimed_at_utc"] = self.database.now
        row["claim_expires_at_utc"] = self.database.now + timedelta(
            seconds=int(lease_seconds)
        )
        row["last_failure_kind"] = None
        row["last_error"] = None
        row["updated_at_utc"] = self.database.now
        return _Result(
            [
                {
                    "attempt_count": row["attempt_count"],
                    "claimed_at_utc": row["claimed_at_utc"],
                    "claim_expires_at_utc": row["claim_expires_at_utc"],
                }
            ]
        )

    def _complete_sent(self, values):
        message_id, delivery_key, claim_token = values
        row = self._claimed(delivery_key, claim_token)
        if row is None:
            return _Result()
        row["status"] = "SENT"
        row["sent_at_utc"] = self.database.now
        row["telegram_message_id"] = int(message_id)
        row["last_failure_kind"] = None
        row["last_error"] = None
        row["updated_at_utc"] = self.database.now
        return _Result([deepcopy(row)])

    def _complete_retry(self, values):
        delay, error, delivery_key, claim_token = values
        row = self._claimed(delivery_key, claim_token)
        if row is None:
            return _Result()
        row["status"] = "RETRYABLE"
        row["available_at_utc"] = self.database.now + timedelta(
            seconds=int(delay)
        )
        row["last_failure_kind"] = "DEFINITE_NOT_SENT"
        row["last_error"] = error
        row["updated_at_utc"] = self.database.now
        return _Result([deepcopy(row)])

    def _complete_terminal(self, values):
        status, failure_kind, error, delivery_key, claim_token = values
        row = self._claimed(delivery_key, claim_token)
        if row is None:
            return _Result()
        row["status"] = status
        row["last_failure_kind"] = failure_kind
        row["last_error"] = error
        row["updated_at_utc"] = self.database.now
        return _Result([deepcopy(row)])

    def _claimed(self, delivery_key, claim_token):
        row = self.database.deliveries.get(str(delivery_key))
        if row is None or row["status"] != "IN_FLIGHT":
            return None
        return row if row["claim_token"] == claim_token else None

    def _insert_attempt(self, values):
        (
            event_key,
            delivery_key,
            attempt_number,
            phase,
            terminal_result,
            claim_token,
            message_id,
            error_text,
            payload,
        ) = values
        if event_key in self.database.attempt_events:
            raise AssertionError("attempt event key was reused")
        row = {
            "attempt_event_key": event_key,
            "delivery_key": delivery_key,
            "attempt_number": attempt_number,
            "event_phase": phase,
            "terminal_result": terminal_result,
            "claim_token": claim_token,
            "event_time_utc": self.database.now,
            "telegram_message_id": message_id,
            "error_text": error_text,
            "event_payload": self._json(payload),
            "created_at_utc": self.database.now,
        }
        self.database.attempt_events[str(event_key)] = row
        return _Result()


def _raises(error_type, text: str, callback) -> None:
    try:
        callback()
    except error_type as exc:
        assert text in str(exc), exc
    else:
        raise AssertionError(f"expected {error_type.__name__} containing {text!r}")


def _using_database(database: _MemoryDatabase, callback):
    original_connect = store._connect
    store._connect = database.connect
    try:
        return callback()
    finally:
        store._connect = original_connect


class _AttestedConnection:
    def __init__(self):
        self.commit_count = 0
        self.closed = False

    def execute(self, statement: str, parameters=()):
        del statement, parameters
        return _Result(
            [
                {
                    "current_user_name": store.DISPATCHER_ROLE,
                    "session_user_name": store.DISPATCHER_ROLE,
                    "rolcanlogin": True,
                    "rolinherit": False,
                    "rolsuper": False,
                    "rolcreatedb": False,
                    "rolcreaterole": False,
                    "rolreplication": False,
                    "rolbypassrls": False,
                    "has_membership": False,
                    "database_create": False,
                    "schema_create": False,
                }
            ]
        )

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


class _AttestedPsycopg:
    def __init__(self, connection: _AttestedConnection):
        self.connection = connection
        self.connect_kwargs = None

    def connect(self, _url, **kwargs):
        self.connect_kwargs = kwargs
        return self.connection


def _check_connection_attestation_transaction_boundary() -> None:
    original_psycopg = store.psycopg
    original_dispatcher_url = os.environ.get(store.DATABASE_URL_ENV)
    original_reader_url = os.environ.get(store.READER_DATABASE_URL_ENV)
    connection = _AttestedConnection()
    fake_psycopg = _AttestedPsycopg(connection)
    try:
        os.environ[store.DATABASE_URL_ENV] = (
            "postgresql://dispatcher@database.example/research"
        )
        os.environ[store.READER_DATABASE_URL_ENV] = (
            "postgresql://reader@database.example/research"
        )
        store.psycopg = fake_psycopg
        opened = store._connect(read_only=False)
        assert opened is connection
        assert connection.commit_count == 1
        assert connection.closed is False
        assert "statement_timeout=" in fake_psycopg.connect_kwargs["options"]
        opened.close()
    finally:
        store.psycopg = original_psycopg
        if original_dispatcher_url is None:
            os.environ.pop(store.DATABASE_URL_ENV, None)
        else:
            os.environ[store.DATABASE_URL_ENV] = original_dispatcher_url
        if original_reader_url is None:
            os.environ.pop(store.READER_DATABASE_URL_ENV, None)
        else:
            os.environ[store.READER_DATABASE_URL_ENV] = original_reader_url


def _persist_search(database: _MemoryDatabase, result: dict) -> dict:
    return _using_database(
        database,
        lambda: store.persist_search_run(
            result,
            source_corpus_receipt_sha256=_hash(
                f"source-corpus-{result['horizon_minutes']}"
            ),
            schedule_slot_utc=AS_OF - timedelta(minutes=5),
        ),
    )


def _check_dedicated_database_and_search_persistence() -> None:
    original_dedicated = os.environ.get(store.DATABASE_URL_ENV)
    original_generic = os.environ.get("DATABASE_URL")
    try:
        os.environ.pop(store.DATABASE_URL_ENV, None)
        os.environ["DATABASE_URL"] = "postgresql://must-not-be-used"
        assert store._database_url() == ""
        os.environ[store.DATABASE_URL_ENV] = "postgresql://dedicated"
        assert store._database_url() == "postgresql://dedicated"
    finally:
        if original_dedicated is None:
            os.environ.pop(store.DATABASE_URL_ENV, None)
        else:
            os.environ[store.DATABASE_URL_ENV] = original_dedicated
        if original_generic is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original_generic

    result, empty, _ = _fixture()
    database = _MemoryDatabase(now=AS_OF)
    inserted = _persist_search(database, result)
    assert inserted["inserted"] is True
    assert inserted["eligible_candidate_count"] == len(
        result["eligible_candidate_variants"]
    )
    assert database.searches[result["search_receipt_sha256"]][
        "search_payload"
    ]["eligible_candidate_variants"] == result["eligible_candidate_variants"]

    duplicate = _persist_search(database, result)
    assert duplicate["inserted"] is False
    assert duplicate["search_run_id"] == inserted["search_run_id"]

    empty_insert = _persist_search(database, empty)
    assert empty_insert["inserted"] is True
    assert empty_insert["search_status"] == "EMPTY_CORPUS"
    assert empty_insert["eligible_candidate_count"] == 0

    corrupt_database = _MemoryDatabase(now=AS_OF)
    _persist_search(corrupt_database, result)
    corrupt_database.searches[result["search_receipt_sha256"]][
        "engine_version"
    ] = "corrupt-engine"
    _raises(
        store.ExperimentalStoreIntegrityError,
        "stored candidate search row mismatch",
        lambda: _persist_search(corrupt_database, result),
    )
    corrupt_database.searches[result["search_receipt_sha256"]][
        "engine_version"
    ] = result["engine_version"]
    corrupt_database.searches[result["search_receipt_sha256"]][
        "live_eligible"
    ] = True
    _raises(
        store.ExperimentalStoreIntegrityError,
        "authority boundary",
        lambda: _persist_search(corrupt_database, result),
    )


def _check_alert_binding_subscriptions_and_conflicts() -> _MemoryDatabase:
    result, _, alert = _fixture()
    database = _MemoryDatabase(now=AS_OF)
    _persist_search(database, result)

    off = _using_database(
        database,
        lambda: store.set_alert_subscription(999, active=False),
    )
    assert off == {
        "chat_id": 999,
        "active": False,
        "subscribed": False,
        "delivery_scope": store.DELIVERY_SCOPE,
    }
    assert 999 not in database.subscriptions

    for chat_id in (101, 102, 103, 104, 106):
        enabled = _using_database(
            database,
            lambda chat_id=chat_id: store.set_alert_subscription(
                chat_id,
                active=True,
                requested_by_user_id=chat_id + 10_000,
            ),
        )
        assert enabled["active"] is True
        assert enabled["delivery_scope"] == "TELEGRAM_EXPERIMENTAL_ONLY"
        assert enabled["disclaimer_acknowledged"] == store.EXPERIMENTAL_LABEL

    # Repeating an already-active opt-in is idempotent: it must not advance
    # the consent epoch and thereby strand alerts that were already eligible.
    original_opt_in_epoch = database.subscriptions[101]["updated_at_utc"]
    database.now += timedelta(seconds=1)
    repeated = _using_database(
        database,
        lambda: store.set_alert_subscription(
            101, active=True, requested_by_user_id=10_101
        ),
    )
    assert repeated["active"] is True
    assert database.subscriptions[101]["updated_at_utc"] == original_opt_in_epoch

    decision_time = datetime.fromisoformat(alert.to_dict()["decision_time_utc"])
    database.now = decision_time + timedelta(seconds=1)
    late = _using_database(
        database,
        lambda: store.set_alert_subscription(
            105, active=True, requested_by_user_id=10_105
        ),
    )
    assert late["active"] is True

    outcome = _using_database(
        database, lambda: store.persist_experimental_alerts([alert])
    )
    assert outcome["alerts_inserted"] == 1
    assert outcome["deliveries_queued"] == 5
    assert {row["chat_id"] for row in database.deliveries.values()} == {
        101,
        102,
        103,
        104,
        106,
    }
    alert_row = database.alerts[alert.alert_id]
    payload = alert.to_dict()
    assert alert_row["search_receipt_sha256"] == payload["provenance"][
        "search_receipt_sha256"
    ]
    assert alert_row["candidate_snapshot"] == payload["candidate_snapshot"]
    assert alert_row["rendered_message_sha256"] == store._text_sha256(
        alert_row["rendered_message"]
    )

    duplicate = _using_database(
        database, lambda: store.persist_experimental_alerts([alert])
    )
    assert duplicate["same_wave_duplicates"] == 1
    assert duplicate["deliveries_queued"] == 0

    # ON CONFLICT must not silently bless a row whose immutable content no
    # longer matches the supplied content-addressed alert.
    alert_row["rendered_message_sha256"] = "0" * 64
    _raises(
        store.ExperimentalStoreIntegrityError,
        "stored experimental alert row mismatch",
        lambda: _using_database(
            database, lambda: store.persist_experimental_alerts([alert])
        ),
    )
    alert_row["rendered_message_sha256"] = store._text_sha256(
        alert_row["rendered_message"]
    )
    return database


def _check_delivery_conflict_is_fail_closed() -> None:
    result, _, alert = _fixture()
    database = _MemoryDatabase(now=AS_OF)
    _persist_search(database, result)
    _using_database(
        database,
        lambda: store.set_alert_subscription(
            201, active=True, requested_by_user_id=20_201
        ),
    )
    database.now = datetime.fromisoformat(
        alert.to_dict()["decision_time_utc"]
    ) + timedelta(seconds=1)
    conflicting_key = store._fingerprint(
        store.DELIVERY_KEY_VERSION,
        {"alert_occurrence_id": alert.alert_id, "chat_id": 201},
    )
    database.deliveries[conflicting_key] = {
        "delivery_key": conflicting_key,
        "alert_occurrence_id": "f" * 64,
        "chat_id": 201,
    }
    _raises(
        store.ExperimentalStoreConflictError,
        "expected immutable identity",
        lambda: _using_database(
            database, lambda: store.persist_experimental_alerts([alert])
        ),
    )


def _check_claim_and_completion_lifecycle(database: _MemoryDatabase) -> None:
    near_expiry = deepcopy(database)
    alert_expiry = min(
        row["expires_at_utc"] for row in near_expiry.alerts.values()
    )
    near_expiry.now = alert_expiry - timedelta(
        seconds=store._CLAIM_LEASE_SECONDS - 1
    )
    commits_before = near_expiry.commit_count
    assert _using_database(
        near_expiry, lambda: store.claim_pending_deliveries(limit=10)
    ) == []
    assert near_expiry.commit_count - commits_before == 2
    assert all(
        row["status"] == "PENDING" for row in near_expiry.deliveries.values()
    )

    attempts_exhausted = deepcopy(database)
    exhausted_key = sorted(attempts_exhausted.deliveries)[0]
    exhausted_row = attempts_exhausted.deliveries[exhausted_key]
    exhausted_row["status"] = "RETRYABLE"
    exhausted_row["attempt_count"] = store._DELIVERY_MAX_ATTEMPTS
    exhausted_row["claim_token"] = "c" * 64
    exhausted_row["claimed_at_utc"] = attempts_exhausted.now
    exhausted_row["claim_expires_at_utc"] = (
        attempts_exhausted.now + timedelta(seconds=store._CLAIM_LEASE_SECONDS)
    )
    exhausted_row["last_failure_kind"] = "DEFINITE_NOT_SENT"
    exhausted_row["last_error"] = "previous definite failure"
    for delivery_key, row in attempts_exhausted.deliveries.items():
        if delivery_key != exhausted_key:
            attempts_exhausted.subscriptions[row["chat_id"]]["active"] = False
    assert _using_database(
        attempts_exhausted,
        lambda: store.claim_pending_deliveries(limit=10),
    ) == []
    assert exhausted_row["attempt_count"] == store._DELIVERY_MAX_ATTEMPTS

    database.now = AS_OF + timedelta(minutes=31)
    commits_before = database.commit_count
    claimed = _using_database(
        database, lambda: store.claim_pending_deliveries(limit=10)
    )
    assert database.commit_count - commits_before == 2
    assert len(claimed) == 5
    for item in claimed:
        assert len(item["claim_token"]) == 64
        assert all(character in "0123456789abcdef" for character in item["claim_token"])
        assert item["rendered_message_sha256"] == store._text_sha256(
            item["rendered_message"]
        )
        assert item["rendered_message"].splitlines()[-1] == store.EXPERIMENTAL_LABEL

    by_chat = {item["chat_id"]: item for item in claimed}
    sent = by_chat[101]
    sent_result = _using_database(
        database,
        lambda: store.complete_delivery(
            sent["delivery_key"],
            sent["claim_token"],
            sent=True,
            telegram_message_id=77_001,
        ),
    )
    assert sent_result["status"] == "SENT"
    assert database.deliveries[sent["delivery_key"]]["telegram_message_id"] == 77_001

    ambiguous = by_chat[102]
    ambiguous_result = _using_database(
        database,
        lambda: store.complete_delivery(
            ambiguous["delivery_key"],
            ambiguous["claim_token"],
            sent=False,
            ambiguous=True,
            error="Telegram acknowledgement was indeterminate",
        ),
    )
    assert ambiguous_result["status"] == "AMBIGUOUS"
    assert ambiguous_result["ambiguous"] is True

    retry = by_chat[103]
    retry_result = _using_database(
        database,
        lambda: store.complete_delivery(
            retry["delivery_key"],
            retry["claim_token"],
            sent=False,
            error="definite network rejection before send",
        ),
    )
    assert retry_result["status"] == "RETRYABLE"
    retry_row = database.deliveries[retry["delivery_key"]]
    assert retry_row["last_failure_kind"] == "DEFINITE_NOT_SENT"
    assert retry_row["available_at_utc"] > database.now

    late_completion = by_chat[104]
    late_completion_row = database.deliveries[late_completion["delivery_key"]]
    stale = by_chat[106]
    stale_row = database.deliveries[stale["delivery_key"]]
    # Keep the intentional definite retry out of this stale-lease sweep; this
    # call should prove that the abandoned in-flight send is not re-delivered.
    database.subscriptions[103]["active"] = False
    database.now = stale_row["claim_expires_at_utc"] + timedelta(seconds=1)
    late_result = _using_database(
        database,
        lambda: store.complete_delivery(
            late_completion["delivery_key"],
            late_completion["claim_token"],
            sent=False,
            error="definite rejection reported after the lease elapsed",
        ),
    )
    assert late_result["status"] == "AMBIGUOUS"
    assert late_completion_row["last_failure_kind"] == "AMBIGUOUS_SEND"
    assert "lease expired" in late_completion_row["last_error"]
    assert _using_database(
        database, lambda: store.claim_pending_deliveries(limit=10)
    ) == []
    assert stale_row["status"] == "AMBIGUOUS"
    assert stale_row["last_failure_kind"] == "AMBIGUOUS_SEND"

    terminal_events = [
        row
        for row in database.attempt_events.values()
        if row["event_phase"] == "TERMINAL"
    ]
    assert {row["terminal_result"] for row in terminal_events} == {
        "SENT",
        "AMBIGUOUS",
        "DEFINITE_FAILURE",
    }
    stale_events = [
        row
        for row in terminal_events
        if row["delivery_key"] == stale["delivery_key"]
    ]
    assert len(stale_events) == 1
    assert stale_events[0]["terminal_result"] == "AMBIGUOUS"

    _raises(
        ValueError,
        "SENT completion cannot carry an error",
        lambda: store.complete_delivery(
            "d" * 64,
            "c" * 64,
            sent=True,
            telegram_message_id=1,
            error="impossible",
        ),
    )
    _raises(
        ValueError,
        "non-SENT completion cannot carry a Telegram message id",
        lambda: store.complete_delivery(
            "d" * 64,
            "c" * 64,
            sent=False,
            telegram_message_id=1,
        ),
    )


def run() -> None:
    _check_connection_attestation_transaction_boundary()
    _check_dedicated_database_and_search_persistence()
    _check_delivery_conflict_is_fail_closed()
    database = _check_alert_binding_subscriptions_and_conflicts()
    _check_claim_and_completion_lifecycle(database)
    print("research_stage4_experimental_store_selftest: PASS")


if __name__ == "__main__":
    run()
