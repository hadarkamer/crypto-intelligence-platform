"""Pure checks for historical replay time, provenance and policy safety."""

from datetime import datetime, timedelta, timezone
import inspect
import json

from binance_spot_price_path import SpotCandle
import canonical_price_path
import research_feature_matrix as feature_matrix
import research_historical_replay as replay
import research_no_dwell_outcome as no_dwell
import research_session_width as session_width


def _candle(
    open_time: datetime, close: float = 100.0, spread: float = 1.0
) -> SpotCandle:
    return SpotCandle(
        open_time_utc=open_time,
        close_time_utc=open_time + timedelta(seconds=59, milliseconds=999),
        open=close,
        high=close + spread,
        low=close - spread,
        close=close,
        volume=1.0,
    )


def run() -> None:
    coverage_source = inspect.getsource(replay._coverage)
    assert "replay_outcome_row_is_coherent" in coverage_source
    assert "sibling_reference_coherence_sql" in coverage_source
    existing_source = inspect.getsource(replay._existing_keys)
    assert "sibling_reference_coherence_sql" in existing_source
    observation = datetime(2026, 8, 29, 12, 32, tzinfo=timezone.utc)
    assert replay._hype_one_minute_observation_floor(
        datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    ) == datetime(2026, 8, 26, 0, 42, tzinfo=timezone.utc)
    candles = [
        _candle(observation + timedelta(minutes=index), 100.0 + index / 10)
        for index in range(-2, 61)
    ]
    reference = replay._reference_candle(candles, observation)
    assert reference is not None
    assert reference.open_time_utc == observation - timedelta(minutes=1)
    future = replay._outcome_candles(candles, observation, 60)
    assert len(future) == replay._expected_outcome_candles(observation, 60) == 60
    assert future[0].open_time_utc == observation
    assert future[-1].open_time_utc == observation + timedelta(minutes=59)

    partial = observation + timedelta(seconds=10)
    partial_future = replay._outcome_candles(candles, partial, 60)
    assert len(partial_future) == replay._expected_outcome_candles(partial, 60) == 59
    assert partial_future[0].open_time_utc == observation + timedelta(minutes=1)
    assert replay._expected_reference_close(observation) == (
        observation - timedelta(milliseconds=1)
    )
    after_current_close = observation.replace(
        second=59, microsecond=999500
    )
    assert replay._expected_reference_close(after_current_close) == (
        observation.replace(second=59, microsecond=999000)
    )
    exactly_at_close = observation.replace(second=59, microsecond=999000)
    assert replay._expected_reference_close(exactly_at_close) == (
        observation - timedelta(milliseconds=1)
    )

    long_metrics = replay.binance_spot_price_path.calculate_path_metrics(
        reference_price=float(reference.close),
        direction="LONG",
        event_time=observation,
        candles=future,
    )
    short_metrics = replay.binance_spot_price_path.calculate_path_metrics(
        reference_price=float(reference.close),
        direction="SHORT",
        event_time=observation,
        candles=future,
    )
    assert long_metrics["mfe_pct"] > short_metrics["mfe_pct"]
    assert long_metrics["mae_pct"] < short_metrics["mae_pct"]
    assert long_metrics["raw_return_pct"] == short_metrics["raw_return_pct"]
    policy = no_dwell.freeze_threshold_policy(
        horizon_minutes=60,
        decision_time=observation,
    )
    first_touch = no_dwell.calculate_first_touch_outcome(
        reference_price=float(reference.close),
        direction="LONG",
        event_time=observation,
        candles=future,
        horizon_minutes=60,
        horizon_closed=True,
        threshold_policy=policy,
    )
    assert first_touch["status"] == "HIT"
    assert first_touch["method_version"] == "no-dwell-first-touch-v6"
    assert first_touch["threshold_scale_factor"] == 1.0

    # The shared calibration may relax only a weekend/mixed movement width.
    # Thirty low-width WEEKEND points and thirty wider ACTIVE points produce
    # the hard 0.50 floor.  A future extreme point must not change the result.
    weekend_event = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    reference_time = weekend_event - timedelta(microseconds=1)
    samples = []
    for index in range(15):
        monday = datetime(2026, 5, 4, 12, tzinfo=timezone.utc) + timedelta(
            weeks=index
        )
        saturday = datetime(2026, 5, 9, 12, tzinfo=timezone.utc) + timedelta(
            weeks=index
        )
        samples.extend(
            [
                (monday, 2.0, 1.0),
                (monday + timedelta(hours=2), 2.0, 1.0),
                (saturday, 0.4, 0.0),
                (saturday + timedelta(hours=2), 0.4, 0.0),
            ]
        )
    samples.sort(key=lambda item: item[0])
    historical = session_width.PriceWidthSeries(
        times=tuple(item[0] for item in samples),
        abs_return_pcts=tuple(item[1] for item in samples),
        active_ratios=tuple(item[2] for item in samples),
    )
    width_index = {("BTC", 60): historical}
    shared = session_width.movement_width_reference(
        symbol="BTC",
        event_time=weekend_event,
        horizon_minutes=60,
        as_of_utc=reference_time,
        historical_index=width_index,
    )
    assert shared["calibration_version"] == session_width.CALIBRATION_VERSION
    assert shared["session_weekend_ratio"] == 1.0
    assert shared["threshold_scale_factor"] == 0.5
    assert shared["applied"] is True

    future_augmented = session_width.PriceWidthSeries(
        times=historical.times + (weekend_event + timedelta(minutes=1),),
        abs_return_pcts=historical.abs_return_pcts + (9999.0,),
        active_ratios=historical.active_ratios + (0.0,),
    )
    invariant = session_width.movement_width_reference(
        symbol="BTC",
        event_time=weekend_event,
        horizon_minutes=60,
        as_of_utc=reference_time,
        historical_index={("BTC", 60): future_augmented},
    )
    assert invariant == shared

    event = {"horizon_minutes": 60}
    feature_reference = feature_matrix._movement_width_reference(
        event=event,
        symbol="BTC",
        event_time=weekend_event,
        current_price_row={"candle_time": reference_time},
        historical_index=width_index,
    )
    replay_reference = replay._calibration_reference(
        anchor=replay.Anchor("BTC", weekend_event, weekend_event),
        horizon=60,
        reference_time_utc=reference_time,
        width_index=width_index,
    )
    assert feature_reference == replay_reference == shared
    frozen = no_dwell.freeze_threshold_policy(
        horizon_minutes=60,
        decision_time=weekend_event,
        prior_only_reference=shared,
    )
    assert frozen["threshold_reference"] == (
        no_dwell.threshold_reference_snapshot(shared)
    )
    assert frozen["threshold_reference_hash"] == (
        no_dwell.threshold_reference_hash(shared)
    )

    # Persisted route provenance must distinguish HYPE Spot @107 explicitly.
    hype_path = {
        "exchange": "hyperliquid",
        "market": "spot",
        "pair": "HYPE/USDT",
        "api_coin": "@107",
        "interval": "1m",
        "interval_seconds": 60,
        "complete": True,
        "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
    }
    hype_provenance = json.loads(
        canonical_price_path.canonical_provenance_text("HYPE", hype_path)
    )
    assert hype_provenance["instrument"] == "@107"
    try:
        canonical_price_path.canonical_provenance_text(
            "HYPE", {**hype_path, "api_coin": "@106"}
        )
    except ValueError:
        pass
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("non-@107 HYPE route was accepted")

    btc_path = {
        "exchange": "binance",
        "market": "spot",
        "pair": "BTCUSDT",
        "interval": "1m",
        "interval_seconds": 60,
        "complete": True,
        "provenance": "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED",
    }
    btc_provenance = canonical_price_path.canonical_provenance_text(
        "BTC", btc_path
    )

    class _ReferenceRows:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, query, params):
            assert "source_observation_time_utc" in query
            return self

        def fetchall(self):
            return self.rows

    def _reference_row(
        *,
        observed=weekend_event,
        source_time=weekend_event,
        reference_time=None,
        reference_price=100.0,
        quality=canonical_price_path.BINANCE_COMPLETE,
    ):
        return {
            "symbol": "BTC",
            "observation_time_utc": observed,
            "source_observation_time_utc": source_time,
            "reference_time_utc": (
                replay._expected_reference_close(observed)
                if reference_time is None
                else reference_time
            ),
            "reference_price": reference_price,
            "exchange": "binance",
            "market": "spot",
            "pair": "BTCUSDT",
            "interval_seconds": 60,
            "provenance": btc_provenance,
            "data_quality_status": quality,
            "outcome_method_version": canonical_price_path.METHOD_VERSION,
            "replay_version": replay.REPLAY_VERSION,
        }

    def _load_reference(row):
        return replay.load_canonical_reference_rows(
            _ReferenceRows([row]),
            start=weekend_event - timedelta(hours=2),
            end=weekend_event + timedelta(hours=2),
            symbols=("BTC",),
        )

    assert len(_load_reference(_reference_row())) == 1
    archive_source = weekend_event.replace(minute=30) - timedelta(hours=1)
    archive_observation = (
        replay.market_session_baseline.closed_candle_available_at(
            archive_source
        )
    )
    assert len(
        _load_reference(
            _reference_row(
                observed=archive_observation,
                source_time=archive_source,
            )
        )
    ) == 1
    for malformed_reference in (
        _reference_row(source_time=weekend_event - timedelta(seconds=1)),
        _reference_row(reference_time=weekend_event - timedelta(seconds=30)),
        _reference_row(reference_price=float("nan")),
        _reference_row(reference_price=float("inf")),
        _reference_row(quality=canonical_price_path.HYPERLIQUID_COMPLETE),
    ):
        assert _load_reference(malformed_reference) == []

    # A repeated bounded canary advances beyond already coherent v2 anchors.
    anchors = [
        replay.Anchor("BTC", observation + timedelta(minutes=index), observation)
        for index in range(5)
    ]
    completed = {
        (anchor.observation_time_utc, horizon)
        for anchor in anchors[:2]
        for horizon in (60, 240)
    }
    pending = replay._select_pending_anchors(
        anchors,
        horizons=(60, 240),
        existing_by_symbol={"BTC": completed},
        max_anchors=2,
    )
    assert pending == anchors[2:4]
    assert replay.REPLAY_VERSION.startswith(
        "historical-raw-opportunity-replay-v2-"
    )
    assert replay.fully_closed_end(
        (60, 1440), now=weekend_event
    ) == weekend_event - timedelta(minutes=1445)

    # When every requested anchor already has coherent v2 outcomes, replay
    # still persists one auditable COMPLETED run.  The recovery run must carry
    # the exact frozen contract and must not touch the outcome table.
    class _RecoveryResult:
        def __init__(self, row=None):
            self.row = row

        def fetchone(self):
            return self.row

    class _RecoveryConnection:
        def __init__(self, *, replay_run_id=812):
            self.replay_run_id = replay_run_id
            self.statements = []
            self.commits = 0
            self.rollbacks = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params=()):
            normalized = " ".join(str(query).split())
            self.statements.append((normalized, tuple(params or ())))
            if "pg_try_advisory_lock" in normalized:
                return _RecoveryResult({"acquired": True})
            if "INSERT INTO research_historical_replay_runs" in normalized:
                return _RecoveryResult(
                    {"replay_run_id": self.replay_run_id}
                )
            if "pg_advisory_unlock" in normalized:
                return _RecoveryResult({"unlocked": True})
            raise AssertionError(f"unexpected recovery SQL: {normalized}")

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    recovery_anchor = replay.Anchor("BTC", observation, observation)
    recovery_horizons = (60, 240)
    recovery_end = observation + timedelta(minutes=1)
    complete_coverage = {
        f"BTC:{horizon}": {
            "outcomes": 1,
            "first_observation_utc": observation,
            "last_observation_utc": observation,
            "utc_dates": 1,
        }
        for horizon in recovery_horizons
    }
    raw_recovery_conn = _RecoveryConnection()
    research_conn_box = {"value": _RecoveryConnection()}
    coverage_box = {"value": complete_coverage}
    outcome_write_calls = []
    original_connect = replay._connect
    original_table_exists = replay._table_exists
    original_load_anchors = replay._load_anchors
    original_research_url = replay._research_database_url
    original_raw_url = replay._raw_database_url
    original_load_references = replay.load_canonical_reference_rows
    original_build_width_index = replay.build_canonical_width_index
    original_existing_keys = replay._existing_keys
    original_coverage = replay._coverage
    original_write_outcome = replay._write_outcome
    old_backfill_flag = replay.os.environ.get("HISTORICAL_REPLAY_BACKFILL")

    def _recovery_connect(url, *, read_only):
        if url == "raw://selftest":
            assert read_only is True
            return raw_recovery_conn
        assert url == "research://selftest"
        assert read_only is False
        return research_conn_box["value"]

    def _all_existing_keys(
        conn, *, symbol, start, end, horizons, width_index
    ):
        assert conn is research_conn_box["value"]
        assert symbol == "BTC"
        assert tuple(horizons) == recovery_horizons
        return {
            (recovery_anchor.observation_time_utc, horizon)
            for horizon in recovery_horizons
        }

    def _unexpected_outcome_write(*args, **kwargs):
        outcome_write_calls.append((args, kwargs))
        raise AssertionError("metadata-only recovery wrote an outcome")

    try:
        replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = "1"
        replay._connect = _recovery_connect
        replay._table_exists = lambda conn, name: True
        replay._load_anchors = lambda *args, **kwargs: [recovery_anchor]
        replay._research_database_url = lambda: "research://selftest"
        replay._raw_database_url = lambda: "raw://selftest"
        replay.load_canonical_reference_rows = (
            lambda *args, **kwargs: []
        )
        replay.build_canonical_width_index = (
            lambda *args, **kwargs: {}
        )
        replay._existing_keys = _all_existing_keys
        replay._coverage = lambda conn: coverage_box["value"]
        replay._write_outcome = _unexpected_outcome_write

        recovered = replay.run_backfill(
            start=observation,
            end=recovery_end,
            symbols=("BTC",),
            horizons=recovery_horizons,
            chunk_days=2,
            pause_seconds=0.0,
        )
        assert recovered["replay_run_id"] == 812
        assert recovered["already_complete"] is True
        assert recovered["coverage"] == complete_coverage
        assert recovered["outcomes_written"] == 0
        assert recovered["outcomes_skipped"] == 2
        assert research_conn_box["value"].rollbacks == 0
        run_inserts = [
            statement
            for statement in research_conn_box["value"].statements
            if "INSERT INTO research_historical_replay_runs" in statement[0]
        ]
        assert len(run_inserts) == 1
        insert_sql, insert_params = run_inserts[0]
        assert "'COMPLETED'" in insert_sql
        assert "research_historical_opportunity_outcomes" not in insert_sql
        assert insert_params[0] == replay.REPLAY_VERSION
        assert insert_params[1] == canonical_price_path.METHOD_VERSION
        assert insert_params[2] == observation
        assert insert_params[3] == recovery_end
        recovery_config = json.loads(insert_params[4])
        assert set(recovery_config) == {
            "symbols",
            "horizons_minutes",
            "chunk_days",
            "max_anchors",
            "first_touch_method_version",
            "movement_width_calibration_version",
            "canonical_price_provenance_version",
            "coverage_scope_version",
            "frozen_fully_closed_end_utc",
            "completion_mode",
        }
        assert recovery_config["symbols"] == ["BTC"]
        assert recovery_config["horizons_minutes"] == [60, 240]
        assert recovery_config["chunk_days"] == 2
        assert recovery_config["max_anchors"] is None
        assert recovery_config["first_touch_method_version"] == (
            no_dwell.METHOD_VERSION
        )
        assert recovery_config["movement_width_calibration_version"] == (
            session_width.CALIBRATION_VERSION
        )
        assert recovery_config["canonical_price_provenance_version"] == (
            canonical_price_path.PRICE_PROVENANCE_VERSION
        )
        assert recovery_config["coverage_scope_version"] == (
            replay.COVERAGE_SCOPE_VERSION
        )
        assert recovery_config["frozen_fully_closed_end_utc"] == str(
            recovery_end
        )
        assert recovery_config["completion_mode"] == (
            "METADATA_ONLY_ALREADY_COHERENT"
        )
        assert json.loads(insert_params[5]) == json.loads(
            replay._json(complete_coverage)
        )
        assert insert_params[6] == 2
        assert outcome_write_calls == []
        assert not any(
            "research_historical_opportunity_outcomes" in statement
            for statement, _ in research_conn_box["value"].statements
        )

        # A complete existing-key set cannot authorize recovery metadata when
        # even one requested symbol/horizon is absent from coherent coverage.
        missing_conn = _RecoveryConnection(replay_run_id=813)
        research_conn_box["value"] = missing_conn
        coverage_box["value"] = {"BTC:60": complete_coverage["BTC:60"]}
        try:
            replay.run_backfill(
                start=observation,
                end=recovery_end,
                symbols=("BTC",),
                horizons=recovery_horizons,
                chunk_days=2,
                pause_seconds=0.0,
            )
        except RuntimeError as exc:
            assert str(exc) == (
                "coherent replay coverage is missing requested keys: BTC:240"
            )
        else:  # pragma: no cover - explicit fail-closed assertion
            raise AssertionError("incomplete recovery coverage was accepted")
        assert missing_conn.rollbacks == 1
        assert not any(
            "INSERT INTO research_historical_replay_runs" in statement
            for statement, _ in missing_conn.statements
        )
        assert not any(
            "research_historical_opportunity_outcomes" in statement
            for statement, _ in missing_conn.statements
        )
        assert outcome_write_calls == []
    finally:
        replay._connect = original_connect
        replay._table_exists = original_table_exists
        replay._load_anchors = original_load_anchors
        replay._research_database_url = original_research_url
        replay._raw_database_url = original_raw_url
        replay.load_canonical_reference_rows = original_load_references
        replay.build_canonical_width_index = original_build_width_index
        replay._existing_keys = original_existing_keys
        replay._coverage = original_coverage
        replay._write_outcome = original_write_outcome
        if old_backfill_flag is None:
            replay.os.environ.pop("HISTORICAL_REPLAY_BACKFILL", None)
        else:
            replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = old_backfill_flag

    # Every fetched segment is validated before its candles can enter the
    # merged path, and even a provenance drift on a later valid-looking
    # segment fails closed.
    original_fetch = replay.canonical_price_path.fetch_closed_candles
    original_segment_minutes = replay._FETCH_SEGMENT_MINUTES
    segment_calls = []

    def _segment_result(start, end, *, provenance):
        return {
            "symbol": "BTC",
            "pair": "BTCUSDT",
            "exchange": "binance",
            "market": "spot",
            "interval": "1m",
            "interval_seconds": 60,
            "candles": [],
            "expected_candles": 0,
            "complete": True,
            "provenance": provenance,
        }

    def _drifting_fetch(symbol, start, end):
        segment_calls.append((symbol, start, end))
        return _segment_result(
            start,
            end,
            provenance=(
                "EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED"
                if len(segment_calls) == 1
                else "UNEXPECTED_SECOND_SEGMENT_PROVENANCE"
            ),
        )

    replay.canonical_price_path.fetch_closed_candles = _drifting_fetch
    replay._FETCH_SEGMENT_MINUTES = 2
    try:
        try:
            replay._fetch_range(
                "BTC",
                weekend_event,
                weekend_event + timedelta(minutes=3),
                pause_seconds=0.0,
            )
        except ValueError as exc:
            assert "changed between replay segments" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("cross-segment route drift was accepted")
    finally:
        replay.canonical_price_path.fetch_closed_candles = original_fetch
        replay._FETCH_SEGMENT_MINUTES = original_segment_minutes
    assert len(segment_calls) == 2

    class _WriteConnection:
        def __init__(self):
            self.query = ""
            self.params = ()

        def execute(self, query, params):
            self.query = query
            self.params = params

    write_conn = _WriteConnection()
    hype_anchor = replay.Anchor("HYPE", weekend_event, weekend_event)
    hype_reference = _candle(weekend_event - timedelta(minutes=1), 100.0)
    hype_future = [
        _candle(
            weekend_event + timedelta(minutes=index), 100.0, spread=0.0
        )
        for index in range(60)
    ]
    replay._write_outcome(
        write_conn,
        run_id=7,
        anchor=hype_anchor,
        horizon=60,
        reference_candle=hype_reference,
        path_result={**hype_path, "candles": hype_future},
        future_candles=hype_future,
        width_index={},
    )
    assert "replay_version=EXCLUDED.replay_version" in write_conn.query
    assert "provenance=EXCLUDED.provenance" in write_conn.query
    long_touch = json.loads(write_conn.params[10])
    short_touch = json.loads(write_conn.params[11])
    assert long_touch["threshold_policy"] == short_touch["threshold_policy"]
    assert (
        long_touch["threshold_policy"]["threshold_reference_version"]
        == session_width.CALIBRATION_VERSION
    )
    assert len(
        long_touch["threshold_policy"]["threshold_reference_hash"]
    ) == 64
    assert json.loads(write_conn.params[22])["instrument"] == "@107"
    assert write_conn.params[24] == replay.REPLAY_VERSION
    coherent_row = {
        "symbol": write_conn.params[0],
        "observation_time_utc": write_conn.params[1],
        "source_observation_time_utc": write_conn.params[2],
        "horizon_minutes": 60,
        "reference_time_utc": write_conn.params[4],
        "reference_price": write_conn.params[5],
        "price_at_horizon": write_conn.params[6],
        "raw_return_pct": write_conn.params[7],
        "long_metrics": json.loads(write_conn.params[8]),
        "short_metrics": json.loads(write_conn.params[9]),
        "long_first_touch_metrics": long_touch,
        "short_first_touch_metrics": short_touch,
        "first_touch_method_version": write_conn.params[12],
        "first_touch_path_samples": write_conn.params[13],
        "path_samples": write_conn.params[16],
        "first_touch_data_quality_status": write_conn.params[14],
        "outcome_method_version": write_conn.params[17],
        "exchange": write_conn.params[18],
        "market": write_conn.params[19],
        "pair": write_conn.params[20],
        "interval_seconds": write_conn.params[21],
        "provenance": write_conn.params[22],
        "data_quality_status": write_conn.params[23],
        "replay_version": write_conn.params[24],
        "sibling_reference_coherent": True,
    }
    assert replay.replay_outcome_row_is_coherent(
        coherent_row, width_index={}
    ) is True
    impossible_source_time = {
        **coherent_row,
        "source_observation_time_utc": weekend_event - timedelta(seconds=1),
    }
    assert replay.replay_outcome_row_is_coherent(
        impossible_source_time, width_index={}
    ) is False
    off_lattice_reference_time = weekend_event - timedelta(seconds=30)
    off_lattice_reference = replay._calibration_reference(
        anchor=hype_anchor,
        horizon=60,
        reference_time_utc=off_lattice_reference_time,
        width_index={},
    )
    off_lattice_policy = no_dwell.freeze_threshold_policy(
        horizon_minutes=60,
        decision_time=weekend_event,
        prior_only_reference=off_lattice_reference,
    )
    off_lattice_touches = []
    for original in (long_touch, short_touch):
        forged = json.loads(json.dumps(original))
        forged["threshold_policy"] = off_lattice_policy
        forged["threshold_scale_factor"] = off_lattice_policy[
            "threshold_scale_factor"
        ]
        forged["qualifying_move_threshold_pct"] = off_lattice_policy[
            "qualifying_move_threshold_pct"
        ]
        forged["threshold_source_kind"] = off_lattice_policy[
            "threshold_source_kind"
        ]
        forged["threshold_source"] = off_lattice_policy["threshold_source"]
        off_lattice_touches.append(forged)
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "reference_time_utc": off_lattice_reference_time,
            "long_first_touch_metrics": off_lattice_touches[0],
            "short_first_touch_metrics": off_lattice_touches[1],
        },
        width_index={},
    ) is False
    numeric_provenance = json.loads(coherent_row["provenance"])
    numeric_provenance["interval_seconds"] = 60.0
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "provenance": json.dumps(numeric_provenance),
        },
        width_index={},
    ) is False

    # Persisted JSON metrics are type-strict. Python booleans and numeric
    # strings must never be admitted as numbers by float()/equality coercion.
    flat_metrics = {
        "measured_at_utc": replay._expected_last_outcome_close(
            weekend_event, 60
        ),
        "price_at_horizon": 1.0,
        "raw_return_pct": 0.0,
        "directional_return_pct": 0.0,
        "max_favorable_price": 1.0,
        "max_adverse_price": 1.0,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "time_to_first_progress_seconds": None,
        "time_to_mfe_seconds": 0,
    }
    flat_source = {
        "price_at_horizon": 1.0,
        "raw_return_pct": 0.0,
        "long_metrics": dict(flat_metrics),
        "short_metrics": dict(flat_metrics),
    }
    assert replay._coherent_path_metrics(
        flat_source,
        reference_price=1.0,
        observation_time_utc=weekend_event,
        horizon_minutes=60,
    ) is not None
    boolean_metrics = json.loads(json.dumps(flat_source, default=str))
    boolean_metrics["price_at_horizon"] = True
    boolean_metrics["raw_return_pct"] = False
    for key in ("long_metrics", "short_metrics"):
        boolean_metrics[key].update(
            {
                "price_at_horizon": True,
                "raw_return_pct": False,
                "directional_return_pct": False,
                "max_favorable_price": True,
                "max_adverse_price": True,
                "mfe_pct": False,
                "mae_pct": False,
                "time_to_mfe_seconds": False,
            }
        )
    assert replay._coherent_path_metrics(
        boolean_metrics,
        reference_price=1.0,
        observation_time_utc=weekend_event,
        horizon_minutes=60,
    ) is None
    string_metrics = json.loads(json.dumps(flat_source, default=str))
    string_metrics["long_metrics"]["price_at_horizon"] = "1.0"
    assert replay._coherent_path_metrics(
        string_metrics,
        reference_price=1.0,
        observation_time_utc=weekend_event,
        horizon_minutes=60,
    ) is None

    boolean_policy_number_long = json.loads(json.dumps(long_touch))
    boolean_policy_number_short = json.loads(json.dumps(short_touch))
    boolean_policy_number_long["threshold_policy"][
        "base_threshold_pct"
    ] = True
    boolean_policy_number_short["threshold_policy"][
        "base_threshold_pct"
    ] = True
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_first_touch_metrics": boolean_policy_number_long,
            "short_first_touch_metrics": boolean_policy_number_short,
        },
        width_index={},
    ) is False
    boolean_reference_long = json.loads(json.dumps(long_touch))
    boolean_reference_short = json.loads(json.dumps(short_touch))
    boolean_reference_long["threshold_policy"]["threshold_reference"][
        "floor_scale_factor"
    ] = True
    boolean_reference_short["threshold_policy"]["threshold_reference"][
        "floor_scale_factor"
    ] = True
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_first_touch_metrics": boolean_reference_long,
            "short_first_touch_metrics": boolean_reference_short,
        },
        width_index={},
    ) is False
    boolean_metric_threshold = json.loads(json.dumps(long_touch))
    boolean_metric_threshold["threshold_scale_factor"] = True
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_first_touch_metrics": boolean_metric_threshold,
        },
        width_index={},
    ) is False
    tampered_long = json.loads(json.dumps(long_touch))
    tampered_long["threshold_policy"]["threshold_reference"][
        "prior_points"
    ] = 999
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_first_touch_metrics": tampered_long},
        width_index={},
    ) is False
    corrupt_success = json.loads(json.dumps(long_touch))
    corrupt_success["success"] = not corrupt_success["success"]
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_first_touch_metrics": corrupt_success},
        width_index={},
    ) is False
    corrupt_threshold = json.loads(json.dumps(long_touch))
    corrupt_threshold["qualifying_move_threshold_pct"] += 0.01
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_first_touch_metrics": corrupt_threshold},
        width_index={},
    ) is False
    corrupt_source = json.loads(json.dumps(long_touch))
    corrupt_source["threshold_source"] = "not-the-frozen-policy-source"
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_first_touch_metrics": corrupt_source},
        width_index={},
    ) is False
    corrupt_price = {**coherent_row, "reference_price": -1.0}
    assert replay.replay_outcome_row_is_coherent(
        corrupt_price, width_index={}
    ) is False
    corrupt_source_time = {
        **coherent_row,
        "source_observation_time_utc": weekend_event + timedelta(seconds=1),
    }
    assert replay.replay_outcome_row_is_coherent(
        corrupt_source_time, width_index={}
    ) is False
    corrupt_path_samples = {**coherent_row, "first_touch_path_samples": 0}
    assert replay.replay_outcome_row_is_coherent(
        corrupt_path_samples, width_index={}
    ) is False
    corrupt_qualifying_price = json.loads(json.dumps(long_touch))
    corrupt_qualifying_price["qualifying_move_price"] += 1.0
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_first_touch_metrics": corrupt_qualifying_price,
        },
        width_index={},
    ) is False
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "sibling_reference_coherent": False},
        width_index={},
    ) is False
    corrupt_failure_final = json.loads(json.dumps(long_touch))
    corrupt_failure_final["failure_final"] = "true"
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_first_touch_metrics": corrupt_failure_final,
        },
        width_index={},
    ) is False

    # Full-path extrema are reference-capped by the canonical calculator.
    corrupt_long_path = json.loads(json.dumps(coherent_row["long_metrics"]))
    corrupt_short_path = json.loads(json.dumps(coherent_row["short_metrics"]))
    corrupt_long_path["max_favorable_price"] = 99.0
    corrupt_short_path["max_adverse_price"] = 99.0
    assert replay.replay_outcome_row_is_coherent(
        {
            **coherent_row,
            "long_metrics": corrupt_long_path,
            "short_metrics": corrupt_short_path,
        },
        width_index={},
    ) is False
    corrupt_zero_mfe_time = json.loads(
        json.dumps(coherent_row["long_metrics"])
    )
    corrupt_zero_mfe_time["time_to_mfe_seconds"] = 30.5
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_metrics": corrupt_zero_mfe_time},
        width_index={},
    ) is False

    # A complete path that touched the frozen wick threshold cannot be a MISS.
    threshold = float(long_touch["qualifying_move_threshold_pct"])
    qualifying_price = float(long_touch["qualifying_move_price"])
    touched_long_path = json.loads(json.dumps(coherent_row["long_metrics"]))
    touched_short_path = json.loads(json.dumps(coherent_row["short_metrics"]))
    touched_long_path.update(
        {
            "max_favorable_price": qualifying_price,
            "mfe_pct": threshold,
            "time_to_first_progress_seconds": 59,
            "time_to_mfe_seconds": 59,
        }
    )
    touched_short_path.update(
        {
            "max_adverse_price": qualifying_price,
            "mae_pct": threshold,
        }
    )
    touched_short_touch = json.loads(json.dumps(short_touch))
    touched_short_touch["pre_qualifying_mae_pct"] = threshold
    touched_row = {
        **coherent_row,
        "long_metrics": touched_long_path,
        "short_metrics": touched_short_path,
        "short_first_touch_metrics": touched_short_touch,
    }
    assert replay.replay_outcome_row_is_coherent(
        touched_row, width_index={}
    ) is False

    # The qualifying candle is included in pre-touch MAE.  A larger stored
    # candle adverse excursion is therefore internally impossible.
    valid_hit = json.loads(json.dumps(long_touch))
    hit_time = hype_future[0].close_time_utc
    valid_hit.update(
        {
            "status": "HIT",
            "success": True,
            "failure_final": False,
            "observed_through_utc": hit_time.isoformat(),
            "first_qualifying_move_time_utc": hit_time.isoformat(),
            "time_to_first_qualifying_move_seconds": 59,
            "pre_qualifying_mae_pct": 0.0,
            "qualifying_candle_adverse_excursion_pct": 0.0,
            "qualifying_candle_order_ambiguous": False,
        }
    )
    valid_hit_row = {
        **touched_row,
        "long_first_touch_metrics": valid_hit,
    }
    assert replay.replay_outcome_row_is_coherent(
        valid_hit_row, width_index={}
    ) is True
    float_lattice_speed = json.loads(json.dumps(touched_long_path))
    float_lattice_speed["time_to_first_progress_seconds"] = 59.0
    float_lattice_speed["time_to_mfe_seconds"] = 59.0
    assert replay.replay_outcome_row_is_coherent(
        {**valid_hit_row, "long_metrics": float_lattice_speed},
        width_index={},
    ) is False
    float_hit_time = json.loads(json.dumps(valid_hit))
    float_hit_time["time_to_first_qualifying_move_seconds"] = 59.0
    assert replay.replay_outcome_row_is_coherent(
        {**valid_hit_row, "long_first_touch_metrics": float_hit_time},
        width_index={},
    ) is False
    off_lattice_speed = json.loads(json.dumps(touched_long_path))
    off_lattice_speed["time_to_first_progress_seconds"] = 1
    assert replay.replay_outcome_row_is_coherent(
        {**valid_hit_row, "long_metrics": off_lattice_speed},
        width_index={},
    ) is False
    corrupt_hit = json.loads(json.dumps(valid_hit))
    corrupt_hit["qualifying_candle_adverse_excursion_pct"] = 0.1
    corrupt_hit["qualifying_candle_order_ambiguous"] = True
    assert replay.replay_outcome_row_is_coherent(
        {**touched_row, "long_first_touch_metrics": corrupt_hit},
        width_index={},
    ) is False
    off_lattice_hit = json.loads(json.dumps(valid_hit))
    off_lattice_time = hit_time + timedelta(seconds=30)
    off_lattice_hit["observed_through_utc"] = off_lattice_time.isoformat()
    off_lattice_hit[
        "first_qualifying_move_time_utc"
    ] = off_lattice_time.isoformat()
    off_lattice_hit["time_to_first_qualifying_move_seconds"] = 89
    assert replay.replay_outcome_row_is_coherent(
        {**touched_row, "long_first_touch_metrics": off_lattice_hit},
        width_index={},
    ) is False
    endpoint_outside_extrema = {
        **coherent_row,
        "price_at_horizon": 101.0,
        "raw_return_pct": 1.0,
        "long_metrics": {
            **coherent_row["long_metrics"],
            "price_at_horizon": 101.0,
            "raw_return_pct": 1.0,
            "directional_return_pct": 1.0,
        },
        "short_metrics": {
            **coherent_row["short_metrics"],
            "price_at_horizon": 101.0,
            "raw_return_pct": 1.0,
            "directional_return_pct": -1.0,
        },
    }
    assert replay.replay_outcome_row_is_coherent(
        endpoint_outside_extrema, width_index={}
    ) is False
    early_miss = json.loads(json.dumps(long_touch))
    assert early_miss["status"] == "MISS"
    early_miss["observed_through_utc"] = (
        weekend_event + timedelta(minutes=30) - timedelta(milliseconds=1)
    ).isoformat()
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "long_first_touch_metrics": early_miss},
        width_index={},
    ) is False
    print("historical replay self-test: OK")


if __name__ == "__main__":
    run()
