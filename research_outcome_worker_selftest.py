"""Network-free checks for prospective open-horizon first-touch polling."""

from __future__ import annotations

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
        return {"event_id": 1}


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
        strict_record = _strict_record(event, horizon)
        if omit_strict_width:
            strict_record["input_snapshot"].pop("movement_width_reference")
        frozen_records = [strict_record]
    else:
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
    service._write_first_touch_outcome = (
        lambda conn, **kwargs: captured_writes.append(kwargs) or True
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

    captured = _CaptureResult()
    assert worker.ResearchOutcomeWorker._load_open_first_touch_events(
        captured, 200
    ) == []
    assert captured.query.count("%s") == len(captured.params)
    for required in (
        "research_prospective_shadow_events authorized",
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
        "open_ft.status IN ('HIT', 'MISS')",
        "open_first_touch_horizons",
        "DISTINCT open_formula.horizon_minutes",
        "date_trunc('minute', NOW())",
        "INTERVAL '1 millisecond'",
        "e.event_kind='ALERT'",
        "e.delivery_status='DELIVERED'",
        "e.event_kind='DECISION_SAMPLE'",
        "e.delivery_status='NOT_APPLICABLE'",
    ):
        assert required in captured.query
    assert "e.event_kind, e.delivery_status" in captured.query
    assert "FROM research_events e" in captured.query
    assert "research_formula_live_deliveries" not in captured.query

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
