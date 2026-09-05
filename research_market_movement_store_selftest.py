"""Deterministic fake-DB regressions for the Wave v5 storage adapter."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os

import research_market_movement as movement
import research_market_movement_selftest as contract_fixtures
import research_market_movement_store as storage


UTC = timezone.utc
START = datetime(2026, 8, 29, 0, 2, tzinfo=UTC)


class _Cursor:
    def __init__(self, rows=()):
        self.rows = [deepcopy(row) for row in rows]
        self.index = 0

    def fetchone(self):
        if self.index >= len(self.rows):
            return None
        row = self.rows[self.index]
        self.index += 1
        return deepcopy(row)

    def fetchall(self):
        rows = self.rows[self.index :]
        self.index = len(self.rows)
        return deepcopy(rows)


class _FakeConnection:
    def __init__(self):
        self.attempts = {}
        self.anchors = {}
        self.transitions = {}
        self.memberships = {}
        self.locks = []
        self.constraints_checked = 0
        self.fail_membership_stream = None
        self.transaction_count = 0
        self.rollback_count = 0
        self.deferred_membership_keys = []
        self.preflight_rows = [
            {
                "relation_name": name,
                "relation": f"public.{name}",
                "schema_usage": True,
                "schema_create": False,
                "relation_kind": "r",
                "relation_owner": "research_market_movement_owner",
                "writer_select": True,
                "writer_insert": True,
                "writer_update": False,
                "writer_delete": False,
                "writer_truncate": False,
                "writer_references": False,
                "writer_trigger": False,
                "relation_acl_safe": True,
                "column_acl_absent": True,
                "rls_disabled": True,
                "policies_absent": True,
                "rewrite_rules_absent": True,
                "trigger_inventory_safe": True,
            }
            for name in (
                "research_price_collection_attempts",
                "research_neutral_price_anchors",
                "research_market_movement_transitions",
                "research_market_movement_memberships",
            )
        ]
        self.preflight_role = {
            "rolname": storage.TRUSTED_WRITER_ROLE,
            "rolcanlogin": True,
            "rolinherit": False,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolreplication": False,
            "rolbypassrls": False,
            "owner_rolname": "research_market_movement_owner",
            "owner_rolcanlogin": False,
            "owner_rolinherit": False,
            "owner_rolsuper": False,
            "owner_rolcreatedb": False,
            "owner_rolcreaterole": False,
            "owner_rolreplication": False,
            "owner_rolbypassrls": False,
            "memberships_held": 0,
            "members_granted_writer": 0,
            "database_owner": "postgres",
            "public_schema_owner": "postgres",
            "transaction_read_only": "off",
            "in_recovery": False,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    @contextmanager
    def transaction(self):
        snapshot = deepcopy(
            (
                self.attempts,
                self.anchors,
                self.transitions,
                self.memberships,
                self.locks,
                self.constraints_checked,
                self.deferred_membership_keys,
            )
        )
        self.deferred_membership_keys = []
        self.transaction_count += 1
        try:
            yield
        except Exception:
            (
                self.attempts,
                self.anchors,
                self.transitions,
                self.memberships,
                self.locks,
                self.constraints_checked,
                self.deferred_membership_keys,
            ) = snapshot
            self.rollback_count += 1
            raise

    @staticmethod
    def _payload(params, key):
        value = params[key]
        return json.loads(value) if isinstance(value, str) else deepcopy(value)

    def _anchor_rows(self):
        return list(self.anchors.values())

    def _transition_rows(self):
        return [row for rows in self.transitions.values() for row in rows]

    def _membership_rows(self):
        return list(self.memberships.values())

    def _validate_constraints(self):
        transition_receipts = {
            row["transition_receipt_sha256"] for row in self._transition_rows()
        }
        anchor_by_identity = {
            (row["anchor_id"], row["anchor_receipt_sha256"]): row
            for row in self._anchor_rows()
        }
        anchor_ids = {
            anchor_id for anchor_id, _anchor_receipt in anchor_by_identity
        }
        evaluable_attempts = [
            row
            for row in self.attempts.values()
            if row["evaluation_status"] == movement.EVALUABLE
        ]
        for attempt in evaluable_attempts:
            anchor = anchor_by_identity.get(
                (attempt["anchor_id"], attempt["anchor_receipt_sha256"])
            )
            assert anchor is not None
            assert anchor["origin"] == "PROSPECTIVE_V5"
            assert anchor["symbol"] == attempt["symbol"]
            assert anchor["eligible_at_utc"] == attempt["eligible_at_utc"]
            assert anchor["decision_time_utc"] == attempt["decision_time_utc"]
        for anchor in self._anchor_rows():
            if anchor["origin"] != "PROSPECTIVE_V5":
                continue
            assert any(
                attempt["anchor_id"] == anchor["anchor_id"]
                and attempt["anchor_receipt_sha256"]
                == anchor["anchor_receipt_sha256"]
                and attempt["symbol"] == anchor["symbol"]
                and attempt["eligible_at_utc"] == anchor["eligible_at_utc"]
                and attempt["decision_time_utc"] == anchor["decision_time_utc"]
                for attempt in evaluable_attempts
            )
        parent_stream = movement.MovementIdentity.btc_parent().stream_id
        local_btc_stream = movement.MovementIdentity.for_symbol("BTC").stream_id
        parent_times = {
            row["eligible_at_utc"]
            for row in self._membership_rows()
            if row["stream_id"] == parent_stream
        }
        local_btc_times = {
            row["eligible_at_utc"]
            for row in self._membership_rows()
            if row["stream_id"] == local_btc_stream
        }
        for key in self.deferred_membership_keys:
            row = self.memberships[key]
            assert row["anchor_id"] in anchor_ids
            assert row["emitted_by_transition_receipt_sha256"] in transition_receipts
            if row["stream_id"] == local_btc_stream:
                assert row["eligible_at_utc"] in parent_times
            if row["stream_id"] == parent_stream:
                assert row["eligible_at_utc"] in local_btc_times
        self.deferred_membership_keys = []

    def execute(self, sql, params=None):
        params = dict(params or {})
        if "market_movement:runtime_role_preflight" in sql:
            return _Cursor([self.preflight_role])
        if "market_movement:runtime_preflight" in sql:
            return _Cursor(self.preflight_rows)

        if "market_movement:lock" in sql:
            self.locks.append(params["lock_key"])
            return _Cursor([{"pg_advisory_xact_lock": None}])

        if "market_movement:load_anchor_slot" in sql:
            key = (
                params["contract_version"],
                params["symbol"],
                params["eligible_at_utc"],
            )
            row = self.anchors.get(key)
            return _Cursor([row] if row is not None else [])

        if "market_movement:insert_attempt" in sql:
            key = params["attempt_receipt_sha256"]
            if key in self.attempts:
                return _Cursor()
            row = deepcopy(params)
            row["attempt_receipt"] = self._payload(params, "attempt_receipt")
            self.attempts[key] = row
            return _Cursor([{"attempt_receipt_sha256": key}])

        if "market_movement:load_attempt" in sql:
            row = self.attempts.get(params["attempt_receipt_sha256"])
            return _Cursor([row] if row is not None else [])

        if "market_movement:insert_anchor" in sql:
            key = (
                params["contract_version"],
                params["symbol"],
                params["eligible_at_utc"],
            )
            if key in self.anchors:
                return _Cursor()
            if any(
                row["anchor_id"] == params["anchor_id"]
                or row["anchor_receipt_sha256"]
                == params["anchor_receipt_sha256"]
                for row in self._anchor_rows()
            ):
                raise RuntimeError("fake unique anchor violation")
            row = deepcopy(params)
            row["anchor_receipt"] = self._payload(params, "anchor_receipt")
            self.anchors[key] = row
            return _Cursor([{"anchor_id": params["anchor_id"]}])

        if "market_movement:load_anchor_id" in sql:
            row = next(
                (
                    item
                    for item in self._anchor_rows()
                    if item["anchor_id"] == params["anchor_id"]
                ),
                None,
            )
            return _Cursor([row] if row is not None else [])

        if "market_movement:load_symbol_anchors" in sql:
            rows = [
                row
                for row in self._anchor_rows()
                if row["contract_version"] == params["contract_version"]
                and row["symbol"] == params["symbol"]
            ]
            rows.sort(key=lambda row: (row["eligible_at_utc"], row["anchor_id"]))
            return _Cursor(rows)

        if "market_movement:load_chain" in sql:
            rows = sorted(
                self.transitions.get(params["stream_id"], []),
                key=lambda row: row["chain_ordinal"],
            )
            return _Cursor(rows)

        if "market_movement:insert_transition" in sql:
            stream = params["stream_id"]
            rows = self.transitions.setdefault(stream, [])
            existing = next(
                (
                    row
                    for row in rows
                    if row["chain_ordinal"] == params["chain_ordinal"]
                ),
                None,
            )
            if existing is not None:
                return _Cursor()
            if any(
                row["transition_receipt_sha256"]
                == params["transition_receipt_sha256"]
                for row in self._transition_rows()
            ):
                raise RuntimeError("fake duplicate transition receipt")
            previous = params["previous_transition_receipt_sha256"]
            if previous is not None and any(
                row["previous_transition_receipt_sha256"] == previous
                for row in self._transition_rows()
            ):
                raise RuntimeError("fake transition fork")
            row = deepcopy(params)
            row["post_state"] = self._payload(params, "post_state")
            row["transition_receipt"] = self._payload(
                params, "transition_receipt"
            )
            rows.append(row)
            return _Cursor(
                [
                    {
                        "transition_receipt_sha256": params[
                            "transition_receipt_sha256"
                        ]
                    }
                ]
            )

        if "market_movement:load_transition_ordinal" in sql:
            row = next(
                (
                    item
                    for item in self.transitions.get(params["stream_id"], [])
                    if item["chain_ordinal"] == params["chain_ordinal"]
                ),
                None,
            )
            return _Cursor([row] if row is not None else [])

        if "market_movement:insert_membership" in sql:
            if params["stream_id"] == self.fail_membership_stream:
                raise RuntimeError("injected membership failure")
            key = (params["stream_id"], params["anchor_id"])
            if key in self.memberships:
                return _Cursor()
            if any(
                row["membership_receipt_sha256"]
                == params["membership_receipt_sha256"]
                or row["emitted_by_transition_receipt_sha256"]
                == params["emitted_by_transition_receipt_sha256"]
                or (
                    row["stream_id"] == params["stream_id"]
                    and row["movement_id"] == params["movement_id"]
                    and row["ordinal"] == params["ordinal"]
                )
                for row in self._membership_rows()
            ):
                raise RuntimeError("fake unique membership violation")
            row = deepcopy(params)
            row["membership_receipt"] = self._payload(
                params, "membership_receipt"
            )
            self.memberships[key] = row
            self.deferred_membership_keys.append(key)
            return _Cursor(
                [
                    {
                        "membership_receipt_sha256": params[
                            "membership_receipt_sha256"
                        ]
                    }
                ]
            )

        if "market_movement:load_membership" in sql:
            row = self.memberships.get(
                (params["stream_id"], params["anchor_id"])
            )
            return _Cursor([row] if row is not None else [])

        if "market_movement:load_stream_memberships" in sql:
            rows = [
                row
                for row in self._membership_rows()
                if row["stream_id"] == params["stream_id"]
            ]
            rows.sort(key=lambda row: (row["eligible_at_utc"], row["anchor_id"]))
            return _Cursor(rows)

        if "market_movement:earliest_pending" in sql:
            candidates = [
                row
                for row in self._anchor_rows()
                if row["contract_version"] == params["contract_version"]
                and row["symbol"] == params["symbol"]
                and (params["stream_id"], row["anchor_id"])
                not in self.memberships
            ]
            candidates.sort(
                key=lambda row: (row["eligible_at_utc"], row["anchor_id"])
            )
            return _Cursor(candidates[:1])

        if "market_movement:parent_membership_at_slot" in sql:
            rows = [
                row
                for row in self._membership_rows()
                if row["stream_id"] == params["stream_id"]
                and row["eligible_at_utc"] == params["eligible_at_utc"]
            ]
            return _Cursor(rows)

        if "market_movement:btc_local_membership" in sql:
            row = self.memberships.get(
                (params["stream_id"], params["anchor_id"])
            )
            return _Cursor([row] if row is not None else [])

        if "market_movement:set_constraints" in sql:
            self._validate_constraints()
            self.constraints_checked += 1
            return _Cursor()

        raise AssertionError(f"unexpected SQL in fake adapter: {sql}")


def _provider(symbol, eligible, price, calls, *, fallback=False):
    def provide():
        calls.append((symbol, eligible, price))
        provenance = contract_fixtures._provenance(symbol)
        provenance["fallback_used"] = fallback
        return {
            "price_candle": {
                "open_time_utc": eligible - timedelta(minutes=1),
                "close_time_utc": eligible - timedelta(microseconds=1000),
                "observed_at_utc": eligible - timedelta(microseconds=1000),
                "refresh_completed_at_utc": eligible + timedelta(seconds=2),
                "price": price,
            },
            "source_provenance": provenance,
        }

    return provide


def _canonical_prospective_decision(symbol, eligible, price):
    supplied = _provider(symbol, eligible, price, [])()
    decision = movement.evaluate_prospective_anchor(
        symbol=symbol,
        eligible_at_utc=eligible,
        decision_time_utc=eligible + timedelta(seconds=7),
        price_candle=supplied["price_candle"],
        source_provenance=supplied["source_provenance"],
    )
    assert decision.anchor is not None
    return decision


def _counts(database):
    return (
        len(database.attempts),
        len(database.anchors),
        sum(len(rows) for rows in database.transitions.values()),
        len(database.memberships),
    )


def _raises(expected, callback):
    try:
        callback()
    except Exception as exc:
        assert expected.lower() in str(exc).lower(), (expected, str(exc))
    else:
        raise AssertionError(f"expected failure containing {expected!r}")


def run() -> None:
    database = _FakeConnection()
    now = [START + timedelta(seconds=7)]
    store = storage.MarketMovementStore(
        connection_factory=lambda: database,
        clock=lambda: now[0],
    )
    assert store.status()["schema_auto_create"] is False
    assert store.status()["runtime_wired"] is True
    assert store.status()["runtime_owner"] == "research_market_movement_worker"
    assert "search_path=pg_catalog" in storage.CONNECTION_OPTIONS
    assert store.runtime_preflight() == {
        "ready": True,
        "relations_verified": 4,
        "trusted_writer_role": storage.TRUSTED_WRITER_ROLE,
        "schema_auto_create": False,
        "writer_role_verified": True,
        "owner_role_verified": True,
        "wave_table_acl_verified": True,
        "rls_policies_rules_verified": True,
        "user_triggers_verified": 17,
        "writable_primary_verified": True,
    }
    database.preflight_rows[0]["writer_select"] = False
    _raises("denied", store.runtime_preflight)
    database.preflight_rows[0]["writer_select"] = True
    database.preflight_rows[0]["writer_insert"] = False
    _raises("denied", store.runtime_preflight)
    database.preflight_rows[0]["writer_insert"] = True
    database.preflight_rows[0]["schema_usage"] = False
    _raises("denied", store.runtime_preflight)
    database.preflight_rows[0]["schema_usage"] = True
    database.preflight_rows[0]["schema_create"] = True
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["schema_create"] = False
    database.preflight_rows[0]["writer_update"] = True
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["writer_update"] = False
    database.preflight_rows[0]["relation_acl_safe"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["relation_acl_safe"] = True
    database.preflight_rows[0]["column_acl_absent"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["column_acl_absent"] = True
    database.preflight_rows[0]["rls_disabled"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["rls_disabled"] = True
    database.preflight_rows[0]["policies_absent"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["policies_absent"] = True
    database.preflight_rows[0]["rewrite_rules_absent"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["rewrite_rules_absent"] = True
    database.preflight_rows[0]["trigger_inventory_safe"] = False
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["trigger_inventory_safe"] = True
    database.preflight_rows[0]["relation_owner"] = storage.TRUSTED_WRITER_ROLE
    _raises("unsafe", store.runtime_preflight)
    database.preflight_rows[0]["relation_owner"] = (
        "research_market_movement_owner"
    )
    database.preflight_role["memberships_held"] = 1
    _raises("writer_role_unsafe=True", store.runtime_preflight)
    database.preflight_role["memberships_held"] = 0
    database.preflight_role["owner_rolcanlogin"] = True
    _raises("writer_role_unsafe=True", store.runtime_preflight)
    database.preflight_role["owner_rolcanlogin"] = False
    database.preflight_role["transaction_read_only"] = "on"
    _raises("writer_role_unsafe=True", store.runtime_preflight)
    database.preflight_role["transaction_read_only"] = "off"
    database.preflight_rows[0]["relation"] = None
    _raises("missing", store.runtime_preflight)
    database.preflight_rows[0]["relation"] = (
        "public.research_price_collection_attempts"
    )

    # The Wave writer never inherits the application's general database URLs.
    environment_names = (
        "DATABASE_URL",
        "RESEARCH_DATABASE_URL",
        "RESEARCH_MARKET_MOVEMENT_DATABASE_URL",
    )
    previous_environment = {
        name: os.environ.get(name) for name in environment_names
    }
    try:
        os.environ["DATABASE_URL"] = "postgresql://general-application"
        os.environ["RESEARCH_DATABASE_URL"] = "postgresql://general-research"
        os.environ.pop("RESEARCH_MARKET_MOVEMENT_DATABASE_URL", None)
        isolated = storage.MarketMovementStore()
        assert isolated.database_url == ""
        assert isolated.status()["configured"] is False
    finally:
        for name, value in previous_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    calls = []

    # BTC anchor, local state, and parent state are one transaction.
    btc = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=START,
        provider=_provider("BTC", START, 100, calls),
    )
    assert btc.provider_called is True
    assert btc.processing.status == "PROCESSED"
    assert btc.processing.local_movement_id != btc.processing.btc_parent_movement_id
    assert len(btc.processing.transition_receipts) == 2
    assert len(btc.processing.membership_receipts) == 2
    assert _counts(database) == (1, 1, 2, 2)
    parent_stream = movement.MovementIdentity.btc_parent().stream_id
    local_btc_stream = movement.MovementIdentity.for_symbol("BTC").stream_id
    assert {parent_stream, local_btc_stream} == set(database.transitions)

    # An exact retry reads the canonical row under lock before provider code.
    def forbidden_provider():
        raise AssertionError("provider was called before existing-anchor read")

    retry = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=START,
        provider=forbidden_provider,
    )
    assert retry.provider_called is False
    assert retry.idempotent_anchor is True
    assert retry.anchor == btc.anchor
    assert retry.processing.status == "ALREADY_PROCESSED"
    assert retry.processing.local_movement_id == btc.processing.local_movement_id
    assert (
        retry.processing.btc_parent_movement_id
        == btc.processing.btc_parent_movement_id
    )
    assert retry.processing.transition_receipts == ()
    assert retry.processing.membership_receipts == ()
    assert _counts(database) == (1, 1, 2, 2)

    # Retrying a later frozen-but-pending anchor is a true read-only no-op: it
    # cannot consume either that anchor or an older pending anchor.
    xrp_earlier_decision = _canonical_prospective_decision(
        "XRP", START + timedelta(minutes=120), 2
    )
    xrp_later_decision = _canonical_prospective_decision(
        "XRP", START + timedelta(minutes=150), 3
    )
    xrp_earlier = xrp_earlier_decision.anchor
    xrp_later = xrp_later_decision.anchor
    with database.transaction():
        for decision in (xrp_earlier_decision, xrp_later_decision):
            store._persist_attempt(database, decision.attempt)
            store._persist_anchor(database, decision.anchor)
        database.execute(storage._SET_CONSTRAINTS_SQL, {})
    retry_provider_calls = []

    def pending_retry_provider():
        retry_provider_calls.append(True)
        return {}

    before_pending_retry = _counts(database)
    pending_retry = store.capture_prospective(
        symbol="XRP",
        eligible_at_utc=xrp_later.eligible_at_utc,
        provider=pending_retry_provider,
    )
    xrp_stream = movement.MovementIdentity.for_symbol("XRP").stream_id
    assert pending_retry.processing.status == "ANCHOR_PENDING"
    assert pending_retry.processing.anchor_id == xrp_later.anchor_id
    assert pending_retry.processing.local_movement_id is None
    assert pending_retry.processing.transition_receipts == ()
    assert pending_retry.processing.membership_receipts == ()
    assert retry_provider_calls == []
    assert _counts(database) == before_pending_retry
    assert not any(key[0] == xrp_stream for key in database.memberships)
    assert xrp_stream not in database.transitions

    # Explicit backlog processing consumes the oldest legal pending anchor,
    # never the later anchor that happened to be retried first.
    earliest = store.process_earliest_pending("XRP")
    assert earliest.status == "PROCESSED"
    assert earliest.anchor_id == xrp_earlier.anchor_id
    assert (xrp_stream, xrp_earlier.anchor_id) in database.memberships
    assert (xrp_stream, xrp_later.anchor_id) not in database.memberships
    next_pending = store.process_earliest_pending("XRP")
    assert next_pending.status == "PROCESSED"
    assert next_pending.anchor_id == xrp_later.anchor_id

    # A missing frozen row is rejected before provider invocation once its
    # half-hour window is closed, with no attempt or anchor write.
    late_time = START + timedelta(minutes=180)
    now[0] = late_time + timedelta(minutes=30)
    late_provider_calls = []

    def late_provider():
        late_provider_calls.append(True)
        return {}

    before_late = _counts(database)
    _raises(
        "outside the eligible 30-minute capture window",
        lambda: store.capture_prospective(
            symbol="ADA",
            eligible_at_utc=late_time,
            provider=late_provider,
        ),
    )
    assert late_provider_calls == []
    assert _counts(database) == before_late

    # The second clock read is the authoritative decision time. Crossing the
    # boundary while the provider runs yields an auditable UNEVALUABLE attempt.
    crossing_time = START + timedelta(minutes=210)
    clock_ticks = iter(
        [
            crossing_time + timedelta(minutes=29, seconds=59),
            crossing_time + timedelta(minutes=30),
        ]
    )
    crossing_store = storage.MarketMovementStore(
        connection_factory=lambda: database,
        clock=lambda: next(clock_ticks),
    )
    crossing_calls = []
    crossed = crossing_store.capture_prospective(
        symbol="ADA",
        eligible_at_utc=crossing_time,
        provider=_provider("ADA", crossing_time, 4, crossing_calls),
    )
    assert crossed.provider_called is True
    assert crossed.anchor is None
    assert crossed.processing.status == "UNEVALUABLE"
    assert len(crossing_calls) == 1

    # Ordinary provider failures, including OS/network failures, are converted
    # to UNEVALUABLE rather than escaping the trusted-writer transaction.
    outage_time = START + timedelta(minutes=240)
    outage_calls = []
    outage_store = storage.MarketMovementStore(
        connection_factory=lambda: database,
        clock=lambda: outage_time + timedelta(seconds=5),
    )

    def failed_provider():
        outage_calls.append(True)
        raise OSError("provider offline")

    outage = outage_store.capture_prospective(
        symbol="ADA",
        eligible_at_utc=outage_time,
        provider=failed_provider,
    )
    assert outage.provider_called is True
    assert outage.anchor is None
    assert outage.processing.status == "UNEVALUABLE"
    assert outage_calls == [True]

    # A non-BTC local stream advances even if BTC has no exact-slot anchor.
    # Parent context is optional metadata and cannot block the local chronology.
    eth_time = START + timedelta(minutes=30)
    now[0] = eth_time + timedelta(seconds=7)
    eth_pending = store.capture_prospective(
        symbol="ETH",
        eligible_at_utc=eth_time,
        provider=_provider("ETH", eth_time, 50, calls),
    )
    assert eth_pending.processing.status == "PROCESSED"
    assert eth_pending.processing.btc_parent_movement_id is None
    eth_stream = movement.MovementIdentity.for_symbol("ETH").stream_id
    assert (eth_stream, eth_pending.anchor.anchor_id) in database.memberships

    # Creating the BTC projection later does not rewrite ETH.  An exact ETH
    # retry is a no-op, but can report the now-present same-slot parent.
    now[0] = eth_time + timedelta(seconds=8)
    btc_second = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=eth_time,
        provider=_provider("BTC", eth_time, 101, calls),
    )
    assert btc_second.processing.status == "PROCESSED"
    eth_done = store.capture_prospective(
        symbol="ETH",
        eligible_at_utc=eth_time,
        provider=forbidden_provider,
    )
    assert eth_done.processing.status == "ALREADY_PROCESSED"
    assert eth_done.processing.anchor_id == eth_pending.anchor.anchor_id
    assert eth_done.processing.btc_parent_movement_id is not None

    # Invalid official source creates only an UNEVALUABLE attempt. A later
    # valid retry can still freeze the slot because no anchor was fabricated.
    bad_time = START + timedelta(minutes=60)
    now[0] = bad_time + timedelta(seconds=7)
    failed = store.capture_prospective(
        symbol="SOL",
        eligible_at_utc=bad_time,
        provider=_provider("SOL", bad_time, 20, calls, fallback=True),
    )
    assert failed.anchor is None
    assert failed.processing.status == "UNEVALUABLE"
    assert failed.attempt_receipt_sha256 in database.attempts
    assert not any(row["symbol"] == "SOL" for row in database._anchor_rows())
    now[0] = bad_time + timedelta(seconds=8)
    recovered = store.capture_prospective(
        symbol="SOL",
        eligible_at_utc=bad_time,
        provider=_provider("SOL", bad_time, 20, calls),
    )
    assert recovered.anchor is not None
    assert recovered.processing.status == "PROCESSED"
    assert recovered.attempt_receipt_sha256 != failed.attempt_receipt_sha256

    # A second ETH slot remains live despite another absent BTC parent.
    eth_later = START + timedelta(minutes=60)
    now[0] = eth_later + timedelta(seconds=8)
    later = store.capture_prospective(
        symbol="ETH",
        eligible_at_utc=eth_later,
        provider=_provider("ETH", eth_later, 51, calls),
    )
    assert later.processing.status == "PROCESSED"
    assert later.processing.btc_parent_movement_id is None
    now[0] = eth_later + timedelta(seconds=9)
    btc_third = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=eth_later,
        provider=_provider("BTC", eth_later, 102, calls),
    )
    assert btc_third.processing.status == "PROCESSED"
    assert store.process_earliest_pending("ETH").status == "NO_PENDING"
    later_retry = store.capture_prospective(
        symbol="ETH",
        eligible_at_utc=eth_later,
        provider=forbidden_provider,
    )
    assert later_retry.processing.status == "ALREADY_PROCESSED"
    assert later_retry.processing.btc_parent_movement_id is not None

    # Exact verified v3/v4 historical import never calls a provider and uses
    # the same append-only path. Conflicting historical/live overlap fails.
    legacy_record = contract_fixtures._legacy_slot()
    historical_database = _FakeConnection()
    historical_store = storage.MarketMovementStore(
        connection_factory=lambda: historical_database,
        clock=lambda: now[0],
    )
    imported = historical_store.import_verified_historical_slot(legacy_record)
    assert imported.anchor.origin == "AUTHORIZED_LEGACY_V3_V4"
    assert imported.processing.status == "PROCESSED"
    import_retry = historical_store.import_verified_historical_slot(legacy_record)
    assert import_retry.idempotent_anchor is True
    assert import_retry.processing.status == "ALREADY_PROCESSED"
    assert import_retry.processing.transition_receipts == ()
    assert import_retry.processing.membership_receipts == ()
    conflicting_history = deepcopy(legacy_record)
    conflicting_history["frozen_inputs"]["official_price"]["price"] = 999.0
    conflicting_history["input_fingerprint"] = (
        movement.compute_authorized_legacy_input_fingerprint(conflicting_history)
    )
    _raises(
        "different frozen content",
        lambda: historical_store.import_verified_historical_slot(conflicting_history),
    )
    bad_marker = deepcopy(legacy_record)
    bad_marker["source_provenance"]["official_price"][
        "price_candle_identity_basis"
    ] = "OLD_MARKER"
    bad_marker["input_fingerprint"] = (
        movement.compute_authorized_legacy_input_fingerprint(bad_marker)
    )
    _raises(
        "identity_basis",
        lambda: historical_store.import_verified_historical_slot(bad_marker),
    )

    # Readback is authoritative: a changed projection fails before provider.
    btc_key = (
        movement.POLICY_VERSION,
        "BTC",
        START,
    )
    database.anchors[btc_key]["price"] = 999
    _raises(
        "stored price conflicts",
        lambda: store.capture_prospective(
            symbol="BTC",
            eligible_at_utc=START,
            provider=forbidden_provider,
        ),
    )
    database.anchors[btc_key]["price"] = btc.anchor.price

    # Inject a missing-parent anomaly by removing only the parent projection;
    # repair rebuilds it from frozen BTC anchors, then becomes idempotent.
    parent_membership_keys = [
        key for key in database.memberships if key[0] == parent_stream
    ]
    parent_transition_backup = deepcopy(database.transitions[parent_stream])
    parent_membership_backup = {
        key: deepcopy(database.memberships[key]) for key in parent_membership_keys
    }
    database.transitions[parent_stream] = []
    for key in parent_membership_keys:
        del database.memberships[key]
    _raises(
        "repair_missing_btc_parent",
        lambda: store.capture_prospective(
            symbol="BTC",
            eligible_at_utc=START,
            provider=forbidden_provider,
        ),
    )
    frozen_btc_anchors = tuple(
        storage._anchor_from_row(row)
        for row in sorted(
            (
                row
                for row in database._anchor_rows()
                if row["symbol"] == "BTC"
            ),
            key=lambda row: (row["eligible_at_utc"], row["anchor_id"]),
        )
    )
    expected_parent_history = movement.replay(
        frozen_btc_anchors,
        identity=movement.MovementIdentity.btc_parent(),
    )
    assert [
        row["transition_receipt_sha256"] for row in parent_transition_backup
    ] == [
        item.transition_receipt_sha256
        for item in expected_parent_history.transitions
    ]
    assert {
        key: row["membership_receipt_sha256"]
        for key, row in parent_membership_backup.items()
    } == {
        (item.stream_id, item.anchor_id): item.membership_receipt_sha256
        for item in expected_parent_history.memberships
    }

    # Exactly one complete canonical suffix anchor is repaired per call, and
    # the returned receipts are precisely those emitted for that anchor.
    for expected_member in expected_parent_history.memberships:
        expected_transitions = tuple(
            item.transition_receipt_sha256
            for item in expected_parent_history.transitions
            if item.trigger_anchor_id == expected_member.anchor_id
        )
        transitions_before = len(database.transitions[parent_stream])
        memberships_before = len(
            [key for key in database.memberships if key[0] == parent_stream]
        )
        repaired = store.repair_missing_btc_parent()
        assert repaired.status == "REPAIRED_BTC_PARENT"
        assert repaired.anchor_id == expected_member.anchor_id
        assert repaired.transition_receipts == expected_transitions
        assert repaired.membership_receipts == (
            expected_member.membership_receipt_sha256,
        )
        assert len(database.transitions[parent_stream]) == (
            transitions_before + len(expected_transitions)
        )
        assert len(
            [key for key in database.memberships if key[0] == parent_stream]
        ) == memberships_before + 1

    assert [
        row["transition_receipt_sha256"]
        for row in database.transitions[parent_stream]
    ] == [
        item.transition_receipt_sha256
        for item in expected_parent_history.transitions
    ]
    assert {
        key: row["membership_receipt_sha256"]
        for key, row in database.memberships.items()
        if key[0] == parent_stream
    } == {
        (item.stream_id, item.anchor_id): item.membership_receipt_sha256
        for item in expected_parent_history.memberships
    }
    assert store.repair_missing_btc_parent().status == "NO_REPAIR_NEEDED"

    # A pre-existing target transition without its membership is not silently
    # completed. Repair fails closed and preserves the anomalous rows.
    partial_database = deepcopy(database)
    partial_store = storage.MarketMovementStore(
        connection_factory=lambda: partial_database,
        clock=lambda: now[0],
    )
    last_parent_member = expected_parent_history.memberships[-1]
    del partial_database.memberships[
        (parent_stream, last_parent_member.anchor_id)
    ]
    partial_before = _counts(partial_database)
    _raises(
        "transition exists without its canonical membership",
        partial_store.repair_missing_btc_parent,
    )
    assert _counts(partial_database) == partial_before

    # A membership write failure during an otherwise valid one-anchor repair
    # rolls back its transition(s), leaving the entire target absent.
    repair_failure_database = deepcopy(database)
    repair_failure_store = storage.MarketMovementStore(
        connection_factory=lambda: repair_failure_database,
        clock=lambda: now[0],
    )
    target_anchor_id = last_parent_member.anchor_id
    target_transition_rows = [
        deepcopy(row)
        for row in repair_failure_database.transitions[parent_stream]
        if row["trigger_anchor_id"] == target_anchor_id
    ]
    repair_failure_database.transitions[parent_stream] = [
        row
        for row in repair_failure_database.transitions[parent_stream]
        if row["trigger_anchor_id"] != target_anchor_id
    ]
    del repair_failure_database.memberships[
        (parent_stream, target_anchor_id)
    ]
    repair_failure_database.fail_membership_stream = parent_stream
    repair_failure_before = _counts(repair_failure_database)
    _raises(
        "injected membership failure",
        repair_failure_store.repair_missing_btc_parent,
    )
    assert _counts(repair_failure_database) == repair_failure_before
    assert not any(
        row["trigger_anchor_id"] == target_anchor_id
        for row in repair_failure_database.transitions[parent_stream]
    )
    repair_failure_database.fail_membership_stream = None
    repaired_target = repair_failure_store.repair_missing_btc_parent()
    assert repaired_target.transition_receipts == tuple(
        row["transition_receipt_sha256"] for row in target_transition_rows
    )
    assert repaired_target.membership_receipts == (
        last_parent_member.membership_receipt_sha256,
    )

    # A failure after parent inserts rolls back anchor, attempt, and both stream
    # projections. This models deferred-constraint/late-write atomicity.
    atomic_time = START + timedelta(minutes=90)
    now[0] = atomic_time + timedelta(seconds=7)
    before = _counts(database)
    database.fail_membership_stream = local_btc_stream
    _raises(
        "injected membership failure",
        lambda: store.capture_prospective(
            symbol="BTC",
            eligible_at_utc=atomic_time,
            provider=_provider("BTC", atomic_time, 103, calls),
        ),
    )
    database.fail_membership_stream = None
    assert _counts(database) == before
    assert database.rollback_count >= 1
    successful_retry = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=atomic_time,
        provider=_provider("BTC", atomic_time, 103, calls),
    )
    assert successful_retry.processing.status == "PROCESSED"

    # A gap rollover emits close+open for both BTC projections. Failure on the
    # final local membership rolls back the anchor, attempt, and all four
    # transitions; the exact retry then publishes one atomic pair.
    gap_time = START + timedelta(minutes=150)
    now[0] = gap_time + timedelta(seconds=7)
    gap_before = _counts(database)
    database.fail_membership_stream = local_btc_stream
    _raises(
        "injected membership failure",
        lambda: store.capture_prospective(
            symbol="BTC",
            eligible_at_utc=gap_time,
            provider=_provider("BTC", gap_time, 110, calls),
        ),
    )
    database.fail_membership_stream = None
    assert _counts(database) == gap_before
    gap_retry = store.capture_prospective(
        symbol="BTC",
        eligible_at_utc=gap_time,
        provider=_provider("BTC", gap_time, 110, calls),
    )
    assert gap_retry.processing.status == "PROCESSED"
    assert len(gap_retry.processing.transition_receipts) == 4
    assert len(gap_retry.processing.membership_receipts) == 2
    assert [
        row["transition_type"]
        for row in database.transitions[local_btc_stream][-2:]
    ] == [movement.MOVEMENT_CLOSED, movement.OPENED_AFTER_DATA_GAP]

    # A canonical receipt can still encode the wrong reopen type. Chain
    # reconstruction binds DATA_GAP_CENSORED to OPENED_AFTER_DATA_GAP.
    saved_reopen_row = deepcopy(database.transitions[local_btc_stream][-1])
    valid_reopen = storage._transition_from_row(saved_reopen_row)
    wrong_reopen = movement._make_transition(
        previous_transition_receipt_sha256=(
            valid_reopen.previous_transition_receipt_sha256
        ),
        transition_type=movement.OPENED_AFTER_DIRECTION_END,
        trigger_anchor_id=valid_reopen.trigger_anchor_id,
        trigger_eligible_at_utc=valid_reopen.trigger_eligible_at_utc,
        trigger_decision_time_utc=valid_reopen.trigger_decision_time_utc,
        pre_state_sha256=None,
        post_state=valid_reopen.post_state,
    )
    wrong_row = storage._transition_params(
        wrong_reopen,
        chain_ordinal=saved_reopen_row["chain_ordinal"],
    )
    wrong_row["post_state"] = json.loads(wrong_row["post_state"])
    wrong_row["transition_receipt"] = json.loads(
        wrong_row["transition_receipt"]
    )
    database.transitions[local_btc_stream][-1] = wrong_row
    _raises(
        "reopen type conflicts with close reason",
        lambda: store._load_chain(
            database, movement.MovementIdentity.for_symbol("BTC")
        ),
    )
    database.transitions[local_btc_stream][-1] = saved_reopen_row

    # A malformed/forked stored chain is never extended.
    saved_ordinal = database.transitions[local_btc_stream][1]["chain_ordinal"]
    database.transitions[local_btc_stream][1]["chain_ordinal"] = 99
    _raises(
        "chain_ordinal",
        lambda: store._load_chain(
            database, movement.MovementIdentity.for_symbol("BTC")
        ),
    )
    database.transitions[local_btc_stream][1]["chain_ordinal"] = saved_ordinal

    assert database.constraints_checked > 0
    assert len(database.locks) > 0
    assert all("STREAM" in key or "ANCHOR" in key for key in database.locks)
    assert len(calls) >= 1
    print("research_market_movement_store_selftest: PASS")


if __name__ == "__main__":
    run()
