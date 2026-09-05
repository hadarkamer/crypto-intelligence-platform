"""Network-free checks for prospective open-horizon first-touch polling."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import canonical_price_path
import research_no_dwell_outcome
import research_outcome_worker as worker


START = datetime(2026, 8, 29, 18, 41, tzinfo=timezone.utc)


class _CaptureResult:
    def __init__(self) -> None:
        self.query = ""
        self.params = []

    def execute(self, query, params):
        self.query = str(query)
        self.params = list(params)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return {"event_id": 1, "inserted": 1}


class _ConnectionContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _PsycopgStub:
    @staticmethod
    def connect(*args, **kwargs):
        return _ConnectionContext()


def _candle(open_time, *, high=100.0, low=100.0, close=100.0):
    return SimpleNamespace(
        open_time_utc=open_time,
        close_time_utc=open_time + timedelta(seconds=59, milliseconds=999),
        open=100.0,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


def _strict_record(
    event,
    horizon,
    *,
    formula_schema_version="research-formula-v5-safe-replay",
):
    """Build one exact sampler-v4 check without any realized outcome input."""
    event_time = event["alert_time_utc"]
    reference = worker.research_session_width.movement_width_reference(
        symbol=event["symbol"],
        event_time=event_time,
        horizon_minutes=horizon,
        as_of_utc=event_time - timedelta(minutes=1),
        historical_index={},
    )
    session = {
        "session_active_ratio": reference["session_active_ratio"],
        "session_weekend_ratio": reference["session_weekend_ratio"],
        "session_segments": reference["session_segments"],
        "session_composition": reference["session_composition"],
    }
    snapshot = {
        "snapshot_policy_version": (
            worker._STRICT_FROZEN_SNAPSHOT_POLICY_VERSION
        ),
        "evidence_policy_version": (
            worker._STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        ),
        "decision_input_policy_version": (
            worker._STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        ),
        "horizon_minutes": horizon,
        "event": {
            "event_id": event["event_id"],
            "alert_time_utc": event_time,
            "symbol": event["symbol"],
            "direction": event["direction"],
            "event_type": event.get("event_type"),
            "setup_key": event.get("setup_key"),
        },
        "prospective_evidence": {
            "sampler_version": worker._STRICT_PROSPECTIVE_SAMPLER_VERSION,
            "anchor_slot_id": 23,
            "input_fingerprint": "e" * 64,
            "feature_bundle_policy_version": (
                worker._STRICT_FEATURE_BUNDLE_POLICY_VERSION
            ),
            "feature_bundle_sha256": "d" * 64,
            "source_timestamps": {
                "price_oi": {
                    "timestamp_utc": event_time - timedelta(minutes=1)
                }
            },
            "source_provenance": {
                "price_oi": {
                    "source": "binance_spot",
                    "pair": f"{event['symbol']}USDT",
                }
            },
        },
        "outcome_window_session": session,
        "movement_width_reference": reference,
    }
    return {
        "event_id": event["event_id"],
        "horizon_minutes": horizon,
        "formula_id": 11,
        "formula_schema_version": formula_schema_version,
        "input_snapshot": snapshot,
        "evidence_policy_version": (
            worker._STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        ),
        "prospective_anchor_slot_id": 23,
        "prospective_input_fingerprint": "e" * 64,
        "feature_bundle_sha256": "d" * 64,
        "authoritative_verified": True,
    }


def _slot_authority_row(event, horizon):
    """Build the compact shape returned by the verified slot loader."""
    record = _strict_record(event, horizon)
    snapshot = record["input_snapshot"]
    return {
        "event": dict(snapshot["event"]),
        "outcome_label": {
            "horizon_minutes": horizon,
            **dict(snapshot["outcome_window_session"]),
            "movement_width_reference": dict(
                snapshot["movement_width_reference"]
            ),
        },
        "decision_input_policy_version": (
            worker._STRICT_FROZEN_EVIDENCE_POLICY_VERSION
        ),
        "prospective_evidence": dict(snapshot["prospective_evidence"]),
        "prospective_anchor_slot_id": record[
            "prospective_anchor_slot_id"
        ],
        "prospective_input_fingerprint": record[
            "prospective_input_fingerprint"
        ],
        "feature_bundle_sha256": record["feature_bundle_sha256"],
        "authoritative_verified": True,
    }


def _run_once_with_path(
    candles,
    *,
    horizon=60,
    frozen_scales=(),
    first_touch_versions=None,
    event_kind="DECISION_SAMPLE",
    delivery_status="NOT_APPLICABLE",
    engine_snapshot=None,
    strict_current=False,
    omit_strict_width=False,
):
    now = datetime.now(timezone.utc)
    event_time = now.replace(second=0, microsecond=0) - timedelta(minutes=3)
    event = {
        "event_id": 9001,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "event_kind": event_kind,
        "delivery_status": delivery_status,
        "current_price": 100.0,
        "target_price": None,
        "engine_snapshot": dict(engine_snapshot or {}),
        "outcome_versions": {},
        "first_touch_versions": first_touch_versions or {},
        "open_first_touch_horizons": [horizon],
    }
    if strict_current:
        event["engine_snapshot"] = {
            **event["engine_snapshot"],
            "prospective_anchor": {
                "sampler_version": worker._STRICT_PROSPECTIVE_SAMPLER_VERSION,
                "input_fingerprint": "e" * 64,
                "feature_bundle_policy_version": (
                    worker._STRICT_FEATURE_BUNDLE_POLICY_VERSION
                ),
                "feature_bundle_sha256": "d" * 64,
            },
        }
        slot_row = _slot_authority_row(event, horizon)
        if omit_strict_width:
            slot_row["outcome_label"].pop("movement_width_reference")
        slot_records = [worker._slot_threshold_record(slot_row)]
        frozen_records = []
    else:
        slot_records = []
        frozen_records = [
            {
                "event_id": event["event_id"],
                "horizon_minutes": horizon,
                "formula_id": index + 1,
                "input_snapshot": {
                    "movement_width_reference": {
                        "policy": (
                            "prior raw price width; same-symbol "
                            "session-composition matched"
                        ),
                        "horizon_minutes": horizon,
                        "session_weekend_ratio": 1.0,
                        "floor_scale_factor": scale,
                        "applied": scale < 1.0,
                    },
                    "source_inputs": {
                        "price_oi": {
                            "timestamp_utc": event_time - timedelta(minutes=1)
                        }
                    },
                },
            }
            for index, scale in enumerate(frozen_scales)
        ]
    captured_writes = []
    service = worker.ResearchOutcomeWorker()
    service._load_open_first_touch_events = lambda conn, limit: [event]
    service._load_due_events = lambda conn, limit: []
    service._load_frozen_threshold_references = (
        lambda conn, event_ids: frozen_records
    )
    service._load_current_slot_threshold_references = (
        lambda events, now: (
            {event["event_id"]: slot_records} if slot_records else {}
        )
    )
    service._write_first_touch_outcome = (
        lambda conn, **kwargs: captured_writes.append(kwargs) or True
    )
    service._write_alert_reference_rejections = (
        lambda conn, rejections: len(rejections)
    )
    original_psycopg = worker.psycopg
    original_enabled = worker._ENABLED
    original_database_url = worker._database_url
    original_fetch = worker.canonical_price_path.fetch_closed_candles
    worker.psycopg = _PsycopgStub()
    worker._ENABLED = True
    worker._database_url = lambda: "postgresql://selftest"
    worker.canonical_price_path.fetch_closed_candles = lambda *args: {
        "symbol": "BTC",
        "pair": "BTCUSDT",
        "exchange": "binance",
        "market": "spot",
        "interval": "1m",
        "provenance": "SELFTEST",
        "candles": list(candles(event_time)),
    }
    try:
        result = service.run_once(limit_per_horizon=10)
    finally:
        worker.psycopg = original_psycopg
        worker._ENABLED = original_enabled
        worker._database_url = original_database_url
        worker.canonical_price_path.fetch_closed_candles = original_fetch
    return result, captured_writes


def _run_closed_current_slot_once(*, slot_authority, legacy_complete=True):
    """Exercise the closed queue through the real slot-authority method."""
    now = datetime.now(timezone.utc)
    event_time = now.replace(second=0, microsecond=0) - timedelta(minutes=61)
    event = {
        "event_id": 9101,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "event_type": "SELFTEST",
        "setup_key": "SELFTEST",
        "event_kind": "DECISION_SAMPLE",
        "delivery_status": "NOT_APPLICABLE",
        "current_price": 100.0,
        "target_price": None,
        "engine_snapshot": {
            "prospective_anchor": {
                "sampler_version": worker._STRICT_PROSPECTIVE_SAMPLER_VERSION,
                "input_fingerprint": "e" * 64,
                "feature_bundle_policy_version": (
                    worker._STRICT_FEATURE_BUNDLE_POLICY_VERSION
                ),
                "feature_bundle_sha256": "d" * 64,
            }
        },
        "outcome_versions": (
            {60: canonical_price_path.METHOD_VERSION}
            if legacy_complete
            else {}
        ),
        "first_touch_versions": {},
        "open_first_touch_horizons": [],
    }
    slot_row = _slot_authority_row(event, 60)
    loader_calls = []
    formula_event_id_calls = []
    fetch_calls = []
    first_touch_writes = []
    legacy_writes = []

    def load_slots(requested_by_horizon):
        loader_calls.append(
            {
                int(horizon): list(event_ids)
                for horizon, event_ids in requested_by_horizon.items()
            }
        )
        if not slot_authority:
            return {}
        return {(event["event_id"], 60): slot_row}

    def load_formula_references(conn, event_ids):
        formula_event_id_calls.append(list(event_ids))
        return []

    def fetch_path(*args):
        fetch_calls.append(args)
        candles = [
            _candle(
                event_time + timedelta(minutes=index),
                high=100.6 if index == 0 else 100.0,
                low=99.8 if index == 0 else 100.0,
                close=99.9 if index == 0 else 100.0,
            )
            for index in range(60)
        ]
        return {
            "symbol": "BTC",
            "pair": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "interval": "1m",
            "provenance": "SELFTEST",
            "candles": candles,
        }

    service = worker.ResearchOutcomeWorker()
    service._load_open_first_touch_events = lambda conn, limit: []
    service._load_due_events = lambda conn, limit: [event]
    service._load_frozen_threshold_references = load_formula_references
    service._write_first_touch_outcome = (
        lambda conn, **kwargs: first_touch_writes.append(kwargs) or True
    )
    service._write_outcome = (
        lambda conn, **kwargs: legacy_writes.append(kwargs) or True
    )
    service._write_alert_reference_rejections = (
        lambda conn, rejections: len(rejections)
    )

    original_psycopg = worker.psycopg
    original_enabled = worker._ENABLED
    original_database_url = worker._database_url
    original_fetch = worker.canonical_price_path.fetch_closed_candles
    original_slot_loader = (
        worker.research_feature_matrix.load_shadow_feature_rows_by_horizon
    )
    worker.psycopg = _PsycopgStub()
    worker._ENABLED = True
    worker._database_url = lambda: "postgresql://selftest"
    worker.canonical_price_path.fetch_closed_candles = fetch_path
    worker.research_feature_matrix.load_shadow_feature_rows_by_horizon = load_slots
    try:
        result = service.run_once(limit_per_horizon=10)
    finally:
        worker.psycopg = original_psycopg
        worker._ENABLED = original_enabled
        worker._database_url = original_database_url
        worker.canonical_price_path.fetch_closed_candles = original_fetch
        worker.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_slot_loader
        )
    return result, {
        "loader_calls": loader_calls,
        "formula_event_id_calls": formula_event_id_calls,
        "fetch_calls": fetch_calls,
        "first_touch_writes": first_touch_writes,
        "legacy_writes": legacy_writes,
    }


def run() -> None:
    # An open horizon is never polled merely because an event exists.  It must
    # be explicitly supplied by the read-side authorization query, which only
    # yields horizons of active Shadow formulas matched by an authorized
    # prospective event.
    ten_minutes_later = START + timedelta(minutes=10)
    assert worker._due_horizons(START, {}, {}, now=ten_minutes_later) == []
    assert worker._due_horizons(
        START,
        {},
        {},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == [240]
    assert worker._due_horizons(
        START,
        {},
        {240: research_no_dwell_outcome.METHOD_VERSION},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == []
    assert worker._due_horizons(
        START,
        {},
        {240: f"{research_no_dwell_outcome.METHOD_VERSION}:PENDING"},
        now=ten_minutes_later,
        open_first_touch_horizons=[240],
    ) == [240]

    # Closed-horizon enrichment remains unchanged and does not require open
    # authorization.  A complete first-touch label suppresses only its own
    # rewrite; a missing legacy endpoint row is still due independently.
    after_one_hour = START + timedelta(minutes=61)
    assert worker._due_horizons(
        START,
        {},
        {60: research_no_dwell_outcome.METHOD_VERSION},
        now=after_one_hour,
    ) == [60]
    assert worker._due_horizons(
        START,
        {60: canonical_price_path.METHOD_VERSION},
        {60: research_no_dwell_outcome.METHOD_VERSION},
        now=after_one_hour,
    ) == []

    assert worker._latest_closed_candle_cutoff(
        datetime(2026, 8, 29, 18, 51, 0, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 29, 18, 50, 59, 999000, tzinfo=timezone.utc)
    assert worker._first_touch_write_is_safe(
        {"status": "PENDING"}, observed_prefix_complete=False
    )
    assert not worker._first_touch_write_is_safe(
        {"status": "HIT"}, observed_prefix_complete=False
    )
    assert worker._first_touch_write_is_safe(
        {"status": "HIT"}, observed_prefix_complete=True
    )

    closed_capture = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_due_events(
        closed_capture, 200
    ) == []
    assert closed_capture.query.count("%s") == len(closed_capture.params)
    assert "ARRAY[]::integer[] AS open_first_touch_horizons" in closed_capture.query
    assert "e.event_kind, e.delivery_status" in closed_capture.query
    assert "research_formula_shadow_checks open_check" not in closed_capture.query
    assert "research_outcome_event_rejections rejected" in closed_capture.query
    assert worker._ALERT_REFERENCE_REJECTION_POLICY_VERSION in closed_capture.params

    captured = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_open_first_touch_events(
        captured, 200
    ) == []
    assert captured.query.count("%s") == len(captured.params)
    for required in (
        "research_prospective_anchor_slots authorized",
        "research_formula_shadow_checks open_check",
        "open_check.evaluation_status='MATCHED'",
        "open_check.evidence_policy_version=%s",
        "open_check.authoritative_verified=TRUE",
        "open_check.prospective_anchor_slot_id IS NOT NULL",
        "open_check.prospective_input_fingerprint",
        "open_check.feature_bundle_sha256",
        "authorized.anchor_slot_id",
        "authorized.input_fingerprint",
        "authorized.feature_bundle_sha256",
        "open_formula.current_stage='SHADOW'",
        "open_formula.active=TRUE",
        "open_ft.status IN ('HIT', 'MISS')",
        "open_first_touch_horizons",
        "WITH open_match AS MATERIALIZED",
        "DISTINCT open_match.horizon_minutes",
        "date_trunc('minute', NOW())",
        "INTERVAL '1 millisecond'",
        "e.event_kind='ALERT'",
        "e.delivery_status='DELIVERED'",
        "e.event_kind='DECISION_SAMPLE'",
        "e.delivery_status='NOT_APPLICABLE'",
        "research_outcome_event_rejections rejected",
    ):
        assert required in captured.query
    assert "e.event_kind, e.delivery_status" in captured.query
    assert "JOIN research_events e ON e.event_id=open_match.event_id" in captured.query
    assert "research_formula_live_deliveries" not in captured.query
    assert worker._ALERT_REFERENCE_REJECTION_POLICY_VERSION in captured.params

    canonical_alert = {
        "event_kind": "ALERT",
        "delivery_status": "DELIVERED",
        "symbol": "ZEC",
        "engine_snapshot": {
            "price_source": "binance_spot",
            "price_pair": "ZECUSDT",
        },
    }
    assert worker._alert_reference_provenance_error(canonical_alert) is None
    for invalid_alert in (
        {
            **canonical_alert,
            "engine_snapshot": {
                "price_source": "binance_futures_mark",
                "price_pair": "ZECUSDT",
            },
        },
        {
            **canonical_alert,
            "engine_snapshot": {
                "price_source": "binance_spot",
                "price_pair": "BTCUSDT",
            },
        },
        {
            **canonical_alert,
            "engine_snapshot": {
                "price_source": "binance_spot",
                "price_pair": "ZECUSDT",
                "price_exchange": "bybit",
            },
        },
        {
            **canonical_alert,
            "engine_snapshot": {
                "price_source": "binance_spot",
                "price_pair": "ZECUSDT",
                "price_market": "perpetual",
            },
        },
        {**canonical_alert, "engine_snapshot": {}},
    ):
        assert worker._alert_reference_provenance_error(invalid_alert) is not None

    canonical_hype = {
        "event_kind": "ALERT",
        "delivery_status": "DELIVERED",
        "symbol": "HYPE",
        "engine_snapshot": {
            "price_source": "hyperliquid",
            "price_exchange": "hyperliquid",
            "price_market": "spot",
            "price_pair": "HYPE/USDT",
            "price_instrument": "@107",
        },
    }
    assert worker._alert_reference_provenance_error(canonical_hype) is None
    for field, bad_value in (
        ("price_source", "bybit_spot"),
        ("price_exchange", "bybit"),
        ("price_market", "perpetual"),
        ("price_pair", "HYPEUSDT"),
        ("price_instrument", "HYPE"),
    ):
        bad_snapshot = dict(canonical_hype["engine_snapshot"])
        bad_snapshot[field] = bad_value
        assert worker._alert_reference_provenance_error(
            {**canonical_hype, "engine_snapshot": bad_snapshot}
        ) is not None
    # Authorized Decision Samples retain their dedicated view-based admission;
    # Alert provenance rules must not reject them or invent a replacement.
    assert worker._alert_reference_provenance_error(
        {
            "event_kind": "DECISION_SAMPLE",
            "delivery_status": "NOT_APPLICABLE",
            "symbol": "BTC",
            "engine_snapshot": {},
        }
    ) is None

    rejection_capture = _CaptureResult()
    inserted = worker.ResearchOutcomeWorker._write_alert_reference_rejections(
        rejection_capture,
        [
            {
                "event": {**canonical_alert, "event_id": 77},
                "reason": "legacy Alert has no canonical price provenance",
            }
        ],
    )
    assert inserted == 1
    assert rejection_capture.query.count("%s") == len(rejection_capture.params)
    assert "ON CONFLICT (event_id, rejection_policy_version) DO NOTHING" in (
        rejection_capture.query
    )
    assert rejection_capture.params[1] == (
        worker._ALERT_REFERENCE_REJECTION_POLICY_VERSION
    )
    rejection_payload = __import__("json").loads(rejection_capture.params[0])
    assert rejection_payload[0]["event_id"] == 77
    assert rejection_payload[0]["reason_code"] == "BINANCE_REFERENCE_PROVENANCE"
    assert rejection_payload[0]["event_snapshot"]["price_provenance"] == {
        "source": "binance_spot",
        "exchange": "",
        "market": "",
        "pair": "ZECUSDT",
        "instrument": "",
    }
    queue_priority = worker._alert_reference_queue_priority_sql("e")
    assert "CASE" in queue_priority
    assert "e.event_kind<>'ALERT'" in queue_priority
    assert "binance_spot" in queue_priority
    assert "HYPE/USDT" in queue_priority
    assert "@107" in queue_priority
    for queue_query in (captured.query, closed_capture.query):
        assert "e.event_kind<>'ALERT'" in queue_query
        assert queue_query.index("e.event_kind<>'ALERT'") < queue_query.index("LIMIT %s")
    assert "open_ft.status IN ('HIT', 'MISS')" in captured.query

    canonical_binance_path = {
        "symbol": "ZEC",
        "exchange": "binance",
        "market": "spot",
        "pair": "ZECUSDT",
        "interval": "1m",
    }
    assert worker._canonical_path_provenance_error(
        "ZEC", canonical_binance_path
    ) is None
    assert worker._canonical_path_provenance_error(
        "ZEC", {**canonical_binance_path, "market": "futures"}
    ) is not None
    canonical_hype_path = {
        "symbol": "HYPE",
        "exchange": "hyperliquid",
        "market": "spot",
        "pair": "HYPE/USDT",
        "api_coin": "@107",
        "interval": "1m",
    }
    assert worker._canonical_path_provenance_error(
        "HYPE", canonical_hype_path
    ) is None
    assert worker._canonical_path_provenance_error(
        "HYPE", {**canonical_hype_path, "api_coin": "HYPE"}
    ) is not None

    reference_capture = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_frozen_threshold_references(
        reference_capture, [7, 8, 7]
    ) == []
    assert reference_capture.query.count("%s") == len(reference_capture.params)
    assert "c.evaluation_status IN ('MATCHED', 'UNMATCHED')" in (
        reference_capture.query
    )
    assert "f.horizon_minutes" in reference_capture.query
    assert "f.formula_schema_version" in reference_capture.query
    for required in (
        "c.evidence_policy_version",
        "c.prospective_anchor_slot_id",
        "c.prospective_input_fingerprint",
        "c.feature_bundle_sha256",
        "c.authoritative_verified",
    ):
        assert required in reference_capture.query
    assert reference_capture.params == [[7, 8]]

    legacy_snapshot = {
        "movement_width_reference": {
            "policy": (
                "prior raw price width; same-symbol session-composition matched"
            ),
            "horizon_minutes": 240,
            "session_weekend_ratio": 1.0,
            "floor_scale_factor": 0.60,
            "applied": True,
        },
        "source_inputs": {
            "price_oi": {"timestamp_utc": START - timedelta(minutes=1)}
        },
    }
    legacy_record = {
        "event_id": 1,
        "horizon_minutes": 240,
        "formula_id": 1,
        "input_snapshot": legacy_snapshot,
    }
    frozen_policy = worker._frozen_threshold_policy(
        event={"alert_time_utc": START},
        horizon_minutes=240,
        snapshot_records=[legacy_record, {**legacy_record, "formula_id": 2}],
    )
    assert frozen_policy["threshold_source_kind"] == (
        "PRIOR_ONLY_SESSION_CALIBRATION"
    )
    assert frozen_policy["threshold_scale_factor"] == 0.60
    assert frozen_policy["qualifying_move_threshold_pct"] == 0.60

    # Current strictness is keyed by the evidence/snapshot policy and sampler,
    # not by the formula schema.  A v5 formula on an exact v4 anchor therefore
    # consumes the frozen width/session bundle just as strictly as v6.
    strict_event = {
        "event_id": 1,
        "alert_time_utc": START,
        "symbol": "BTC",
        "direction": "LONG",
        "event_type": "SELFTEST",
        "setup_key": "SELFTEST",
        "engine_snapshot": {
            "prospective_anchor": {
                "sampler_version": worker._STRICT_PROSPECTIVE_SAMPLER_VERSION,
                "input_fingerprint": "e" * 64,
                "feature_bundle_policy_version": (
                    worker._STRICT_FEATURE_BUNDLE_POLICY_VERSION
                ),
                "feature_bundle_sha256": "d" * 64,
            }
        },
    }
    strict_record = _strict_record(strict_event, 240)
    strict_reference = strict_record["input_snapshot"][
        "movement_width_reference"
    ]
    assert strict_record["formula_schema_version"] == (
        "research-formula-v5-safe-replay"
    )
    strict_policy = worker._frozen_threshold_policy(
        event=strict_event,
        horizon_minutes=240,
        snapshot_records=[strict_record],
    )
    assert strict_policy["threshold_reference_version"] == (
        worker.research_session_width.CALIBRATION_VERSION
    )
    assert strict_policy["threshold_reference"] == (
        worker.research_no_dwell_outcome.threshold_reference_snapshot(
            strict_reference
        )
    )
    assert strict_policy["threshold_source_kind"] == (
        "PRIOR_ONLY_SESSION_CALIBRATION"
    )
    assert strict_policy["threshold_reference_hash"]

    # Sampler-v4 threshold authority comes directly from the canonical slot,
    # without a Formula identity or Formula evaluation status in the record.
    slot_record = worker._slot_threshold_record(
        _slot_authority_row(strict_event, 240)
    )
    assert slot_record["threshold_authority_version"] == (
        worker._STRICT_SLOT_THRESHOLD_AUTHORITY_VERSION
    )
    assert "formula_id" not in slot_record
    slot_policy = worker._frozen_threshold_policy(
        event=strict_event,
        horizon_minutes=240,
        snapshot_records=[slot_record],
    )
    assert slot_policy["threshold_reference_hash"] == (
        strict_policy["threshold_reference_hash"]
    )

    tampered_slot_records = []

    wrong_slot = copy.deepcopy(slot_record)
    wrong_slot["prospective_anchor_slot_id"] = 24
    tampered_slot_records.append(("anchor slot", wrong_slot))

    wrong_hash = copy.deepcopy(slot_record)
    wrong_hash["feature_bundle_sha256"] = "c" * 64
    wrong_hash["input_snapshot"]["prospective_evidence"][
        "feature_bundle_sha256"
    ] = "c" * 64
    tampered_slot_records.append(("feature-bundle hash", wrong_hash))

    wrong_fingerprint = copy.deepcopy(slot_record)
    wrong_fingerprint["prospective_input_fingerprint"] = "f" * 64
    wrong_fingerprint["input_snapshot"]["prospective_evidence"][
        "input_fingerprint"
    ] = "f" * 64
    tampered_slot_records.append(("input fingerprint", wrong_fingerprint))

    wrong_session = copy.deepcopy(slot_record)
    session = wrong_session["input_snapshot"]["outcome_window_session"]
    active_ratio = 0.0 if float(session["session_active_ratio"]) else 1.0
    session["session_active_ratio"] = active_ratio
    session["session_weekend_ratio"] = 1.0 - active_ratio
    session["session_composition"] = (
        worker.research_session_width.session_composition_label(active_ratio)
    )
    tampered_slot_records.append(("session context", wrong_session))

    future_reference = copy.deepcopy(slot_record)
    future_reference["input_snapshot"]["movement_width_reference"][
        "as_of_utc"
    ] = START + timedelta(seconds=1)
    tampered_slot_records.append(("future as-of", future_reference))

    wrong_direction = copy.deepcopy(slot_record)
    wrong_direction["input_snapshot"]["event"]["direction"] = "SHORT"
    tampered_slot_records.append(("direction", wrong_direction))

    for tamper_name, tampered_record in tampered_slot_records:
        try:
            worker._frozen_threshold_policy(
                event=strict_event,
                horizon_minutes=240,
                snapshot_records=[tampered_record],
            )
        except worker.FrozenThresholdPolicyConflict:
            pass
        else:
            raise AssertionError(
                f"tampered sampler-v4 slot authority accepted: {tamper_name}"
            )

    # More than the shared loader's 250-ID cap is split by distinct events.
    # Each chunk requests only First-Touch-due horizons, retains separate
    # per-horizon contexts, and excludes neutral or legacy-sampler events.
    batch_now = START + timedelta(minutes=61)
    batch_events = []
    batch_events_by_id = {}
    for index in range(251):
        event_id = 10_000 + index
        event = {
            **strict_event,
            "event_id": event_id,
            "engine_snapshot": copy.deepcopy(strict_event["engine_snapshot"]),
            "outcome_versions": {60: canonical_price_path.METHOD_VERSION},
            "first_touch_versions": (
                {60: research_no_dwell_outcome.METHOD_VERSION}
                if index % 2 == 0
                else {}
            ),
            "open_first_touch_horizons": [240],
        }
        batch_events.append(event)
        batch_events_by_id[event_id] = event
    neutral_event = {
        **batch_events[0],
        "event_id": 20_001,
        "direction": "NEUTRAL",
        "first_touch_versions": {},
    }
    legacy_sampler_event = {
        **batch_events[0],
        "event_id": 20_002,
        "engine_snapshot": {
            "prospective_anchor": {
                "sampler_version": "prospective-neutral-anchor-v3-max-pain-frozen"
            }
        },
    }
    loader_calls = []

    def batch_slot_loader(requested_by_horizon):
        request = {
            int(horizon): list(event_ids)
            for horizon, event_ids in requested_by_horizon.items()
        }
        loader_calls.append(request)
        return {
            (event_id, horizon): _slot_authority_row(
                batch_events_by_id[event_id], horizon
            )
            for horizon, event_ids in request.items()
            for event_id in event_ids
        }

    original_slot_loader = (
        worker.research_feature_matrix.load_shadow_feature_rows_by_horizon
    )
    worker.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
        batch_slot_loader
    )
    try:
        batch_references = (
            worker.ResearchOutcomeWorker._load_current_slot_threshold_references(
                [*batch_events, neutral_event, legacy_sampler_event],
                now=batch_now,
            )
        )
    finally:
        worker.research_feature_matrix.load_shadow_feature_rows_by_horizon = (
            original_slot_loader
        )
    distinct_call_sizes = [
        len(
            {
                event_id
                for event_ids in request.values()
                for event_id in event_ids
            }
        )
        for request in loader_calls
    ]
    assert distinct_call_sizes == [50, 50, 50, 50, 50, 1]
    assert len(batch_references) == 251
    assert neutral_event["event_id"] not in batch_references
    assert legacy_sampler_event["event_id"] not in batch_references
    assert sorted(
        record["horizon_minutes"]
        for record in batch_references[batch_events[0]["event_id"]]
    ) == [240]
    assert sorted(
        record["horizon_minutes"]
        for record in batch_references[batch_events[1]["event_id"]]
    ) == [60, 240]
    assert all(
        "formula_id" not in record
        for records in batch_references.values()
        for record in records
    )

    missing_width = {
        **strict_record,
        "input_snapshot": {
            key: value
            for key, value in strict_record["input_snapshot"].items()
            if key != "movement_width_reference"
        },
    }
    try:
        worker._frozen_threshold_policy(
            event=strict_event,
            horizon_minutes=240,
            snapshot_records=[missing_width],
        )
    except worker.FrozenThresholdPolicyConflict as exc:
        assert "movement-width" in str(exc)
    else:
        raise AssertionError("missing sampler-v4 frozen width used a fallback")

    mismatched_session = {
        **strict_record,
        "input_snapshot": {
            **strict_record["input_snapshot"],
            "outcome_window_session": {
                **strict_record["input_snapshot"]["outcome_window_session"],
                "session_weekend_ratio": 0.25,
            },
        },
    }
    try:
        worker._frozen_threshold_policy(
            event=strict_event,
            horizon_minutes=240,
            snapshot_records=[mismatched_session],
        )
    except worker.FrozenThresholdPolicyConflict as exc:
        assert "session" in str(exc)
    else:
        raise AssertionError("mismatched sampler-v4 frozen session was accepted")

    forged_strict = {
        **strict_record,
        "input_snapshot": {
            **strict_record["input_snapshot"],
            "movement_width_reference": {
                **strict_reference,
                "as_of_utc": START + timedelta(days=10),
            }
        },
    }
    try:
        worker._frozen_threshold_policy(
            event=strict_event,
            horizon_minutes=240,
            snapshot_records=[forged_strict],
        )
    except worker.FrozenThresholdPolicyConflict as exc:
        assert "newer than decision time" in str(exc)
    else:
        raise AssertionError("future v4 movement-width calibration was accepted")

    try:
        worker._frozen_threshold_policy(
            event=strict_event,
            horizon_minutes=240,
            snapshot_records=[],
        )
    except worker.FrozenThresholdPolicyConflict as exc:
        assert "no current frozen threshold evidence" in str(exc)
    else:
        raise AssertionError("sampler-v4 event without evidence used a fallback")

    legacy_v3_event = {
        "alert_time_utc": START,
        "engine_snapshot": {
            "prospective_anchor": {
                "sampler_version": "prospective-neutral-anchor-v3-max-pain-frozen"
            }
        },
    }
    audit_policy = worker._frozen_threshold_policy(
        event=legacy_v3_event,
        horizon_minutes=240,
        snapshot_records=[],
    )
    assert audit_policy["threshold_source_kind"] == "STATIC_HORIZON_FLOOR"

    conflicting_record = {
        **legacy_record,
        "formula_id": 3,
        "input_snapshot": {
            **legacy_snapshot,
            "movement_width_reference": {
                **legacy_snapshot["movement_width_reference"],
                "floor_scale_factor": 0.55,
            },
        },
    }
    try:
        worker._frozen_threshold_policy(
            event={"alert_time_utc": START},
            horizon_minutes=240,
            snapshot_records=[legacy_record, conflicting_record],
        )
    except worker.FrozenThresholdPolicyConflict as exc:
        assert "disagree" in str(exc)
    else:
        raise AssertionError("conflicting formula width references were accepted")

    for bad_horizon in (None, 60):
        bad_reference = {
            **legacy_snapshot["movement_width_reference"],
        }
        if bad_horizon is None:
            bad_reference.pop("horizon_minutes")
        else:
            bad_reference["horizon_minutes"] = bad_horizon
        try:
            worker._frozen_threshold_policy(
                event={"alert_time_utc": START},
                horizon_minutes=240,
                snapshot_records=[
                    {
                        **legacy_record,
                        "input_snapshot": {
                            **legacy_snapshot,
                            "movement_width_reference": bad_reference,
                        },
                    }
                ],
            )
        except worker.FrozenThresholdPolicyConflict as exc:
            assert "horizon" in str(exc)
        else:
            raise AssertionError(
                "relaxed reference with a missing/mismatched horizon was accepted"
            )

    # Reproduce the closed-backlog failure mode: no open authorization and no
    # Formula references, but a direct canonical slot is enough to write the
    # terminal sampler-v4 label after 60 complete candles.
    closed_slot, closed_slot_trace = _run_closed_current_slot_once(
        slot_authority=True
    )
    assert closed_slot_trace["loader_calls"] == [{60: [9101]}]
    assert closed_slot_trace["formula_event_id_calls"] == [[]]
    assert len(closed_slot_trace["fetch_calls"]) == 1
    assert len(closed_slot_trace["first_touch_writes"]) == 1
    assert closed_slot_trace["legacy_writes"] == []
    assert closed_slot["first_touch_hits"] == 1
    assert closed_slot["first_touch_rows_written"] == 1
    assert closed_slot["first_touch_threshold_policy_conflicts"] == 0
    assert closed_slot_trace["first_touch_writes"][0]["first_touch"][
        "status"
    ] == "HIT"

    # Missing slot authority fails closed before any price-path request when
    # First-Touch is the event's only remaining work.
    missing_slot, missing_slot_trace = _run_closed_current_slot_once(
        slot_authority=False
    )
    assert missing_slot_trace["loader_calls"] == [{60: [9101]}]
    assert missing_slot_trace["fetch_calls"] == []
    assert missing_slot_trace["first_touch_writes"] == []
    assert missing_slot_trace["legacy_writes"] == []
    assert missing_slot["first_touch_threshold_policy_conflicts"] == 1
    assert missing_slot["missing_price_paths"] == 0

    # The same First-Touch conflict must not block an independently due legacy
    # outcome.  Its canonical path is still fetched and its upsert still runs.
    legacy_due, legacy_due_trace = _run_closed_current_slot_once(
        slot_authority=False,
        legacy_complete=False,
    )
    assert len(legacy_due_trace["fetch_calls"]) == 1
    assert legacy_due_trace["first_touch_writes"] == []
    assert len(legacy_due_trace["legacy_writes"]) == 1
    assert legacy_due["inserted"] == 1
    assert legacy_due["first_touch_threshold_policy_conflicts"] == 1

    # The exact v4 bundle produces a label without waiting for candle dwell.
    # Removing only its frozen width fails closed even with the same favorable
    # canonical wick: no HIT/PENDING row is written.
    strict_complete, strict_complete_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        strict_current=True,
    )
    assert strict_complete["first_touch_hits"] == 1
    assert len(strict_complete_writes) == 1
    assert strict_complete_writes[0]["first_touch"]["status"] == "HIT"
    assert strict_complete_writes[0]["first_touch"][
        "threshold_source_kind"
    ] == "PRIOR_ONLY_SESSION_CALIBRATION"

    strict_missing, strict_missing_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        strict_current=True,
        omit_strict_width=True,
    )
    assert strict_missing_writes == []
    assert strict_missing["first_touch_hits"] == 0
    assert strict_missing["first_touch_rows_written"] == 0
    assert strict_missing["first_touch_threshold_policy_conflicts"] == 1

    # A favorable wick on a gapped prefix is not allowed to freeze a terminal
    # HIT.  Once the complete prefix is available, the same wick qualifies
    # immediately even though a later candle reverses deeply and closes down.
    incomplete, incomplete_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
        ]
    )
    assert incomplete_writes == []
    assert (
        incomplete["first_touch_terminal_rows_deferred_for_incomplete_prefix"]
        == 1
    )
    complete, complete_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=99.9),
            _candle(
                event_time + timedelta(minutes=1),
                high=100.0,
                low=90.0,
                close=91.0,
            ),
            _candle(
                event_time + timedelta(minutes=2),
                high=92.0,
                low=89.0,
                close=90.0,
            ),
        ]
    )
    assert complete["first_touch_hits"] == 1
    assert len(complete_writes) == 1
    frozen = complete_writes[0]["first_touch"]
    assert frozen["status"] == "HIT"
    assert frozen["dwell_required_seconds"] == 0
    assert frozen["first_qualifying_move_time_utc"] == (
        complete_writes[0]["event"]["alert_time_utc"]
        + timedelta(seconds=59, milliseconds=999)
    )

    delivered_alert, delivered_alert_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6, low=99.8, close=100.1),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        event_kind="ALERT",
        delivery_status="DELIVERED",
        engine_snapshot={
            "price_source": "binance_spot",
            "price_pair": "BTCUSDT",
        },
    )
    assert delivered_alert["alert_reference_provenance_rejections"] == 0
    assert delivered_alert["first_touch_hits"] == 1
    assert len(delivered_alert_writes) == 1

    rejected_alert, rejected_alert_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.6),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        event_kind="ALERT",
        delivery_status="DELIVERED",
        engine_snapshot={
            "price_source": "binance_futures_mark",
            "price_pair": "BTCUSDT",
        },
    )
    assert rejected_alert_writes == []
    assert rejected_alert["alert_reference_provenance_rejections"] == 1
    assert rejected_alert["missing_price_paths"] == 0

    # A legacy PENDING row is due again.  The worker rebuilds the whole closed
    # prefix with the frozen weekend width and can discover an earlier touch;
    # no manual database mutation or future candle is needed.
    recalculated, recalculated_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=100.7, low=100.0, close=100.1),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        horizon=240,
        frozen_scales=(0.60, 0.60),
        first_touch_versions={
            240: f"{research_no_dwell_outcome.METHOD_VERSION}:PENDING"
        },
    )
    assert recalculated["first_touch_hits"] == 1
    assert recalculated["first_touch_threshold_policy_conflicts"] == 0
    recalculated_touch = recalculated_writes[0]["first_touch"]
    assert recalculated_touch["status"] == "HIT"
    assert recalculated_touch["threshold_scale_factor"] == 0.60
    assert recalculated_touch["qualifying_move_threshold_pct"] == 0.60
    assert recalculated_touch["first_qualifying_move_time_utc"] == (
        recalculated_writes[0]["event"]["alert_time_utc"]
        + timedelta(seconds=59, milliseconds=999)
    )
    assert recalculated_touch["observed_through_utc"] == (
        recalculated_writes[0]["event"]["alert_time_utc"]
        + timedelta(minutes=2, seconds=59, milliseconds=999)
    )

    rejected, rejected_writes = _run_once_with_path(
        lambda event_time: [
            _candle(event_time, high=101.5),
            _candle(event_time + timedelta(minutes=1)),
            _candle(event_time + timedelta(minutes=2)),
        ],
        horizon=240,
        frozen_scales=(0.60, 0.55),
    )
    assert rejected_writes == []
    assert rejected["first_touch_threshold_policy_conflicts"] == 1

    # A verified terminal row is absorbing at the application upsert.  A
    # legacy partial terminal row, if one ever existed, is deliberately not
    # protected and can be replaced by a later complete recalculation instead
    # of having its old semantic fields quality-laundered.
    conflict_capture = _CaptureResult()
    write = complete_writes[0]
    assert worker.ResearchOutcomeWorker._write_first_touch_outcome(
        conflict_capture,
        event=write["event"],
        horizon=write["horizon"],
        reference_price=write["reference_price"],
        reference_source=write["reference_source"],
        path_result=write["path_result"],
        first_touch=write["first_touch"],
        complete=True,
    )
    assert conflict_capture.query.count("%s") == len(conflict_capture.params)
    assert "status IN ('HIT', 'MISS')" in conflict_capture.query
    assert "data_quality_status=ANY(%s)" in conflict_capture.query
    assert "EXCLUDED.observed_through_utc >=" in conflict_capture.query
    assert "research_first_touch_outcomes.status<>'PENDING'" in (
        conflict_capture.query
    )
    assert "EXCLUDED.data_quality_status=ANY(%s)" in conflict_capture.query
    assert conflict_capture.params[-3:] == [
        list(canonical_price_path.COMPLETE_QUALITIES),
        list(canonical_price_path.COMPLETE_QUALITIES),
        list(canonical_price_path.COMPLETE_QUALITIES),
    ]
    assert "direction=EXCLUDED.direction" in conflict_capture.query
    assert "WHEN research_first_touch_outcomes.status='HIT'" not in (
        conflict_capture.query
    )

    status = worker.ResearchOutcomeWorker().status()
    policy = status["first_touch_policy"]
    assert policy["success"] == "first favorable width touch; zero dwell"
    assert policy["post_hit_reversal"] == "does not cancel success"
    assert "every minute" in policy["worker_evaluation"]
    assert "eligible delivered Alerts" in policy["worker_evaluation"]
    assert policy["current_evidence_policy"] == (
        worker._STRICT_FROZEN_EVIDENCE_POLICY_VERSION
    )
    assert policy["current_sampler"] == (
        worker._STRICT_PROSPECTIVE_SAMPLER_VERSION
    )
    assert "fails closed" in policy["current_threshold_input"]
    assert policy["legacy_evidence"] == "audit_only"
    assert "fail closed" in status["price_paths"]["alert_reference_policy"]
    assert worker._POLL_SECONDS >= 60

    print("prospective first-touch outcome worker self-test: PASS")


if __name__ == "__main__":
    run()
