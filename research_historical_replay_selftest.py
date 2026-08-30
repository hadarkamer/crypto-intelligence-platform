"""Pure checks for historical replay time, provenance and policy safety."""

from datetime import datetime, timedelta, timezone
import inspect
import json

from binance_spot_price_path import SpotCandle
import canonical_price_path
import research_feature_matrix as feature_matrix
import research_formula_schema_admin as schema_admin
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
    assert "iter_query_rows" in coverage_source
    assert "stored_rows" not in coverage_source
    existing_source = inspect.getsource(replay._existing_keys)
    assert "sibling_reference_coherence_sql" in existing_source
    assert "iter_query_rows" in existing_source
    status_source = inspect.getsource(replay.status)
    assert "_coverage(" not in status_source
    assert "status='COMPLETED'" in status_source
    observation = datetime(2026, 8, 29, 12, 32, tzinfo=timezone.utc)
    hype_availability_as_of = datetime(
        2026, 8, 29, 12, 0, tzinfo=timezone.utc
    )
    hype_floor = replay._hype_one_minute_observation_floor(
        hype_availability_as_of
    )
    assert hype_floor == datetime(
        2026, 8, 26, 3, 42, tzinfo=timezone.utc
    )
    assert replay._HYPERLIQUID_ROLLING_WINDOW_SAFETY_MARGIN_MINUTES == 180
    assert replay.HYPE_ROLLING_WINDOW_POLICY_VERSION.endswith("margin-180m-v1")
    # The selected floor remains fetchable after the promised two-hour
    # operating delay and still has a further one-hour reserve.  Once the full
    # 180-minute margin is exceeded, Replay fails closed instead of falling
    # back to another venue or accepting an incomplete canonical path.
    assert replay._hype_anchor_path_is_available(
        hype_floor,
        horizon_minutes=1440,
        now=hype_availability_as_of + timedelta(minutes=120),
    )
    assert replay._hype_anchor_path_is_available(
        hype_floor,
        horizon_minutes=1440,
        now=hype_availability_as_of + timedelta(minutes=180),
    )
    assert not replay._hype_anchor_path_is_available(
        hype_floor,
        horizon_minutes=1440,
        now=hype_availability_as_of + timedelta(minutes=181),
    )
    latest_closed_hype = replay.fully_closed_end(
        (1440,), now=hype_availability_as_of
    )
    assert replay._hype_anchor_path_is_available(
        latest_closed_hype,
        horizon_minutes=1440,
        now=hype_availability_as_of,
    )
    assert not replay._hype_anchor_path_is_available(
        latest_closed_hype + timedelta(minutes=1),
        horizon_minutes=1440,
        now=hype_availability_as_of,
    )
    assert replay._replay_symbol_order(
        ("BTC", "HYPE", "ETH", "BNB")
    ) == ("HYPE", "BNB", "BTC", "ETH")
    assert "for symbol in _replay_symbol_order(grouped)" in inspect.getsource(
        replay.run_backfill
    )
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
            assert "LEFT JOIN research_historical_replay_runs replay_owner" in query
            assert "DISTINCT ON" not in query
            assert "NOT EXISTS" not in query
            assert "outcome_method_version=%s" not in query
            assert "data_quality_status=ANY(%s)" not in query
            assert query.count("%s") == 3
            assert len(params) == 3
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
        opportunity_id=1,
        horizon_minutes=60,
        replay_version=replay.REPLAY_VERSION,
        replay_run_id=7,
        first_touch_replay_run_id=7,
        first_touch_method_version=no_dwell.METHOD_VERSION,
        replay_owner_status="COMPLETED",
        provenance=btc_provenance,
    ):
        return {
            "opportunity_id": opportunity_id,
            "symbol": "BTC",
            "observation_time_utc": observed,
            "source_observation_time_utc": source_time,
            "horizon_minutes": horizon_minutes,
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
            "provenance": provenance,
            "data_quality_status": quality,
            "first_touch_data_quality_status": quality,
            "outcome_method_version": canonical_price_path.METHOD_VERSION,
            "replay_version": replay_version,
            "replay_run_id": replay_run_id,
            "first_touch_replay_run_id": first_touch_replay_run_id,
            "first_touch_method_version": first_touch_method_version,
            "replay_owner_status": replay_owner_status,
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

    # Canonical reference history uses a non-blocking index-ordered server
    # cursor.  Every row is streamed, including siblings that are themselves
    # ineligible candidates, and only one four-horizon anchor is materialized
    # at a time for exact SQL-equivalent coherence checks.
    class _ReferenceStreamCursor:
        def __init__(self, connection):
            self.connection = connection
            self.offset = 0
            self.closed = False

        def execute(self, query, params):
            normalized = " ".join(query.split())
            assert (
                "LEFT JOIN research_historical_replay_runs replay_owner"
                in normalized
            )
            assert (
                "replay_owner.replay_version=price_ref.replay_version"
                in normalized
            )
            assert "DISTINCT ON" not in normalized
            assert "NOT EXISTS" not in normalized
            assert "outcome_method_version=%s" not in normalized
            assert "data_quality_status=ANY(%s)" not in normalized
            assert (
                "ORDER BY price_ref.symbol, price_ref.observation_time_utc, "
                "price_ref.horizon_minutes, price_ref.opportunity_id"
            ) in normalized
            assert params[2] == ["BTC"]
            self.connection.queries.append(normalized)
            return self

        def fetchmany(self, size):
            assert size == replay._STREAM_BATCH_SIZE
            self.connection.fetchmany_calls += 1
            start = self.offset
            self.offset += size
            return self.connection.rows[start : self.offset]

        def fetchall(self):
            self.connection.fetchall_calls += 1
            raise AssertionError("canonical references must never use fetchall")

        def close(self):
            self.closed = True
            self.connection.closed_cursors += 1

    class _ReferenceStreamConnection:
        def __init__(self, rows):
            self.rows = rows
            self.fetchmany_calls = 0
            self.fetchall_calls = 0
            self.closed_cursors = 0
            self.cursor_names = []
            self.queries = []

        def cursor(self, *, name):
            assert name.startswith("research_replay_stream_")
            self.cursor_names.append(name)
            return _ReferenceStreamCursor(self)

        def execute(self, query, params):
            raise AssertionError("named server cursor was not used")

    streamed_rows = []
    streamed_anchors = 376
    opportunity_id = 1
    for anchor_index in range(streamed_anchors):
        observed = weekend_event + timedelta(minutes=anchor_index)
        for horizon in replay._HORIZONS:
            streamed_rows.append(
                _reference_row(
                    observed=observed,
                    source_time=observed,
                    opportunity_id=opportunity_id,
                    horizon_minutes=horizon,
                )
            )
            opportunity_id += 1
    stream_conn = _ReferenceStreamConnection(streamed_rows)
    original_selector = replay._select_reference_candidate
    maximum_anchor_group = 0

    def _tracked_reference_selector(rows, **kwargs):
        nonlocal maximum_anchor_group
        maximum_anchor_group = max(maximum_anchor_group, len(rows))
        return original_selector(rows, **kwargs)

    replay._select_reference_candidate = _tracked_reference_selector
    try:
        streamed_references = replay.load_canonical_reference_rows(
            stream_conn,
            start=weekend_event - timedelta(minutes=1),
            end=weekend_event + timedelta(minutes=streamed_anchors + 1),
            symbols=("BTC",),
        )
    finally:
        replay._select_reference_candidate = original_selector
    assert len(streamed_references) == streamed_anchors
    assert maximum_anchor_group == len(replay._HORIZONS)
    assert stream_conn.fetchall_calls == 0
    assert stream_conn.fetchmany_calls >= 4
    assert stream_conn.closed_cursors == 1
    assert len(stream_conn.cursor_names) == 1
    assert len(stream_conn.queries) == 1

    completed = _reference_row()
    assert replay._select_reference_candidate(
        [completed], include_running_run_id=None
    ) is not None
    for rejected_owner in (
        {**completed, "replay_owner_status": "FAILED"},
        {**completed, "replay_owner_status": "RUNNING"},
        {**completed, "first_touch_replay_run_id": 8},
    ):
        assert replay._select_reference_candidate(
            [rejected_owner], include_running_run_id=None
        ) is None
    running = {**completed, "replay_owner_status": "RUNNING"}
    assert replay._select_reference_candidate(
        [running], include_running_run_id=7
    ) is not None
    assert replay._select_reference_candidate(
        [running], include_running_run_id=8
    ) is None
    for invalid_running_id in (0, -1, True, 1.0):
        try:
            replay.load_canonical_reference_rows(
                _ReferenceRows([]),
                start=weekend_event - timedelta(minutes=1),
                end=weekend_event + timedelta(minutes=1),
                symbols=("BTC",),
                include_running_run_id=invalid_running_id,
            )
        except ValueError:
            pass
        else:  # pragma: no cover - explicit fail-closed assertion
            raise AssertionError("invalid running replay owner was accepted")

    # Same-version siblings from different completed owners are related.  The
    # exact eleven relational fields use IS DISTINCT FROM semantics, including
    # case-insensitive exchange/market and pair comparisons.
    cross_owner = _reference_row(
        opportunity_id=2,
        horizon_minutes=240,
        replay_run_id=8,
        first_touch_replay_run_id=8,
    )
    assert replay._select_reference_candidate(
        [completed, cross_owner], include_running_run_id=None
    ) is not None
    case_only = {
        **cross_owner,
        "exchange": "BINANCE",
        "market": "SPOT",
        "pair": "btcusdt",
    }
    assert replay._select_reference_candidate(
        [completed, case_only], include_running_run_id=None
    ) is not None
    distinct_values = {
        "source_observation_time_utc": weekend_event + timedelta(seconds=1),
        "reference_time_utc": weekend_event - timedelta(seconds=1),
        "reference_price": 101.0,
        "outcome_method_version": "wrong-method",
        "exchange": "coinbase",
        "market": "futures",
        "pair": "BTC/USDC",
        "interval_seconds": 300,
        "provenance": "different",
        "data_quality_status": canonical_price_path.BINANCE_PARTIAL,
        "first_touch_data_quality_status": canonical_price_path.BINANCE_PARTIAL,
    }
    assert set(distinct_values) == set(replay._REFERENCE_COHERENCE_FIELDS)
    for field, value in distinct_values.items():
        assert replay._select_reference_candidate(
            [completed, {**cross_owner, field: value}],
            include_running_run_id=None,
        ) is None, field

    # Candidate filtering happens after the full sibling group is loaded: a
    # related partial-quality or wrong-method sibling still poisons the good
    # candidate through one of the eleven coherence fields.
    assert replay._select_reference_candidate(
        [
            completed,
            {
                **completed,
                "opportunity_id": 2,
                "horizon_minutes": 240,
                "data_quality_status": canonical_price_path.BINANCE_PARTIAL,
            },
        ],
        include_running_run_id=None,
    ) is None
    assert replay._select_reference_candidate(
        [
            completed,
            {
                **completed,
                "opportunity_id": 2,
                "horizon_minutes": 240,
                "outcome_method_version": "wrong-method",
            },
        ],
        include_running_run_id=None,
    ) is None

    # Preserve ordinary SQL equality byte-for-byte: NULL first-touch methods
    # are not related, even inside the same run.  A RUNNING row is likewise not
    # cross-related to another completed run, but same-run siblings still are.
    null_method = {**completed, "first_touch_method_version": None}
    divergent_null_method = {
        **null_method,
        "opportunity_id": 2,
        "horizon_minutes": 240,
        "provenance": "different",
    }
    assert replay._select_reference_candidate(
        [null_method, divergent_null_method], include_running_run_id=None
    ) is not None
    completed_other_run = {
        **cross_owner,
        "provenance": "different",
    }
    assert replay._select_reference_candidate(
        [running, completed_other_run], include_running_run_id=7
    ) is not None
    running_same_run = {
        **running,
        "opportunity_id": 2,
        "horizon_minutes": 240,
        "provenance": "different",
    }
    assert replay._select_reference_candidate(
        [running, running_same_run], include_running_run_id=7
    ) is None

    # Preserve DISTINCT ON ordering and fallback nuance.  Incoherent current
    # siblings may expose a coherent legacy reference; a coherent current row
    # selected first and rejected by later canonical validation must not fall
    # back silently to that legacy row.
    legacy = _reference_row(
        opportunity_id=3,
        horizon_minutes=720,
        replay_version="historical-raw-opportunity-replay-v1",
        replay_run_id=2,
        first_touch_replay_run_id=None,
        first_touch_method_version=None,
        replay_owner_status="COMPLETED",
    )
    incoherent_current = {
        **completed,
        "opportunity_id": 2,
        "horizon_minutes": 240,
        "provenance": "different",
    }
    selected_legacy = replay._select_reference_candidate(
        [completed, incoherent_current, legacy], include_running_run_id=None
    )
    assert selected_legacy is not None
    assert selected_legacy["replay_version"] == legacy["replay_version"]
    malformed_current = {**completed, "provenance": "{}"}
    assert replay.load_canonical_reference_rows(
        _ReferenceRows([malformed_current, legacy]),
        start=weekend_event - timedelta(minutes=1),
        end=weekend_event + timedelta(minutes=1),
        symbols=("BTC",),
    ) == []

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
    assert pending == [anchors[2], anchors[4]]
    assert replay.REPLAY_VERSION == (
        "historical-raw-opportunity-replay-v2-balanced-prior-session-width"
    )
    assert replay.SELECTION_POLICY_VERSION == (
        "balanced-even-time-per-symbol-v1"
    )
    assert "bounded" in replay.COVERAGE_SCOPE_VERSION
    assert replay.MAX_BOUNDED_ANCHORS == 2000
    assert "_max_anchors_from_env" in inspect.getsource(replay.main)
    try:
        replay._select_pending_anchors(
            anchors,
            horizons=(60,),
            existing_by_symbol={},
            max_anchors=2001,
        )
    except ValueError as exc:
        assert "between 1 and 2000" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Replay accepted a limit above its hard cap")
    old_max_env = replay.os.environ.get("HISTORICAL_REPLAY_MAX_ANCHORS")
    replay.os.environ["HISTORICAL_REPLAY_MAX_ANCHORS"] = "2001"
    try:
        try:
            replay._max_anchors_from_env()
        except ValueError as exc:
            assert "between 1 and 2000" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("Replay env bypassed the hard cap")
    finally:
        if old_max_env is None:
            replay.os.environ.pop("HISTORICAL_REPLAY_MAX_ANCHORS", None)
        else:
            replay.os.environ["HISTORICAL_REPLAY_MAX_ANCHORS"] = old_max_env
    replay_index = next(
        path
        for path in schema_admin.MIGRATION_PATHS
        if path.name == "012_historical_replay_v2_streaming_index.sql"
    )
    assert replay_index.name == "012_historical_replay_v2_streaming_index.sql"
    replay_index_sql = replay_index.read_text(encoding="utf-8")
    indexed_expression = replay_index_sql.split(
        "ON research_historical_opportunity_outcomes (", 1
    )[1].split(");", 1)[0]
    ordered_index_columns = (
        "first_touch_method_version",
        "replay_version",
        "symbol",
        "observation_time_utc",
        "horizon_minutes",
    )
    positions = [indexed_expression.index(column) for column in ordered_index_columns]
    assert positions == sorted(positions)

    # The global limit is max-min fair across symbols, redistributes a short
    # symbol's unused share, and remains deterministic for shuffled input.
    balanced_anchors = [
        replay.Anchor(
            symbol,
            observation + timedelta(minutes=minute),
            observation + timedelta(minutes=minute),
        )
        for symbol, count in (("BTC", 9), ("ETH", 2), ("SOL", 5))
        for minute in range(count)
    ]
    balanced = replay._select_pending_anchors(
        balanced_anchors,
        horizons=(60,),
        existing_by_symbol={},
        max_anchors=8,
    )
    assert len(balanced) == 8
    assert {
        symbol: sum(anchor.symbol == symbol for anchor in balanced)
        for symbol in ("BTC", "ETH", "SOL")
    } == {"BTC": 3, "ETH": 2, "SOL": 3}
    assert replay._select_pending_anchors(
        list(reversed(balanced_anchors)),
        horizons=(60,),
        existing_by_symbol={},
        max_anchors=8,
    ) == balanced
    for symbol in ("BTC", "ETH", "SOL"):
        source = [anchor for anchor in balanced_anchors if anchor.symbol == symbol]
        chosen = [anchor for anchor in balanced if anchor.symbol == symbol]
        assert chosen[0] == source[0]
        assert chosen[-1] == source[-1]

    short_and_long = [
        replay.Anchor(
            symbol,
            observation + timedelta(minutes=minute),
            observation + timedelta(minutes=minute),
        )
        for symbol, minutes in (("BTC", (0,)), ("ETH", range(10)))
        for minute in minutes
    ]
    redistributed = replay._select_pending_anchors(
        short_and_long,
        horizons=(60,),
        existing_by_symbol={},
        max_anchors=5,
    )
    assert sum(anchor.symbol == "BTC" for anchor in redistributed) == 1
    assert sum(anchor.symbol == "ETH" for anchor in redistributed) == 4

    irregular = [
        replay.Anchor(
            "BTC",
            observation + timedelta(minutes=minute),
            observation + timedelta(minutes=minute),
        )
        for minute in (0, 1, 49, 51, 100)
    ]
    assert replay._evenly_time_spaced_anchors(irregular, 3) == [
        irregular[0],
        irregular[2],
        irregular[-1],
    ]
    contract = replay._selected_anchor_contract(balanced)
    assert contract["selected_anchor_count"] == 8
    assert len(contract["selected_anchor_fingerprint_sha256"]) == 64
    assert contract == replay._selected_anchor_contract(
        list(reversed(balanced))
    )
    changed_source = list(balanced)
    changed_source[0] = replay.Anchor(
        changed_source[0].symbol,
        changed_source[0].observation_time_utc,
        changed_source[0].source_observation_time_utc
        - timedelta(minutes=30),
    )
    assert replay._selected_anchor_contract(changed_source)[
        "selected_anchor_fingerprint_sha256"
    ] != contract["selected_anchor_fingerprint_sha256"]
    contract_coverage = {
        f"{symbol}:60": {
            "outcomes": bounds["count"],
            "first_observation_utc": bounds["min_observation_time_utc"],
            "last_observation_utc": bounds["max_observation_time_utc"],
            "utc_dates": 1,
        }
        for symbol, bounds in contract["selected_anchor_scope"].items()
    }
    assert replay._coverage_covers_selected_contract(
        contract_coverage, contract, horizons=(60,)
    ) is True
    incomplete_contract_coverage = dict(contract_coverage)
    incomplete_contract_coverage.pop("SOL:60")
    assert replay._coverage_covers_selected_contract(
        incomplete_contract_coverage, contract, horizons=(60,)
    ) is False
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
    original_adopt_recoverable = replay._adopt_recoverable_anchors
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
            "resume_policy_version",
            "frozen_fully_closed_end_utc",
            "hype_rolling_window_policy",
            "selection_policy_version",
            "selected_anchor_fingerprint_sha256",
            "selected_anchor_count",
            "selected_anchor_scope",
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
        assert recovery_config["resume_policy_version"] == (
            replay.RESUME_POLICY_VERSION
        )
        hype_policy = recovery_config["hype_rolling_window_policy"]
        assert hype_policy == {
            "version": replay.HYPE_ROLLING_WINDOW_POLICY_VERSION,
            "provider": "hyperliquid",
            "instrument": "@107",
            "interval": "1m",
            "provider_limit_candles": 5000,
            "reference_lookback_minutes": 2,
            "safety_margin_minutes": 180,
            "processing_order": "HYPE_FIRST_THEN_SYMBOL_ASC",
            "availability_as_of_utc": hype_policy["availability_as_of_utc"],
            "eligible_observation_floor_utc": (
                hype_policy["eligible_observation_floor_utc"]
            ),
            "selected_observation_ceiling_utc": str(recovery_end),
            "required_outcome_minutes": 240,
            "older_hype_anchors": (
                "excluded rather than approximated or mislabeled"
            ),
        }
        assert (
            datetime.fromisoformat(hype_policy["availability_as_of_utc"])
            - datetime.fromisoformat(
                hype_policy["eligible_observation_floor_utc"]
            )
        ) == timedelta(minutes=4818)
        assert recovery_config["selection_policy_version"] == (
            replay.SELECTION_POLICY_VERSION
        )
        assert recovery_config["selected_anchor_count"] == 0
        assert recovery_config["selected_anchor_scope"] == {}
        assert recovery_config["selected_anchor_fingerprint_sha256"] == (
            replay._selected_anchor_contract([])[
                "selected_anchor_fingerprint_sha256"
            ]
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

        # None remains legal for metadata-only repair, but cannot authorize a
        # replay when even one outcome cohort is pending.
        unbounded_conn = _RecoveryConnection(replay_run_id=814)
        research_conn_box["value"] = unbounded_conn
        replay._existing_keys = lambda *args, **kwargs: set()
        try:
            replay.run_backfill(
                start=observation,
                end=recovery_end,
                symbols=("BTC",),
                horizons=recovery_horizons,
                chunk_days=2,
                max_anchors=None,
                pause_seconds=0.0,
            )
        except RuntimeError as exc:
            assert str(exc) == (
                "Refusing unbounded historical replay while pending work "
                "exists; set a positive max_anchors"
            )
        else:  # pragma: no cover
            raise AssertionError("unbounded pending replay was accepted")
        assert any(
            "pg_advisory_unlock" in statement
            for statement, _ in unbounded_conn.statements
        )
        assert not any(
            "INSERT INTO research_historical_replay_runs" in statement
            for statement, _ in unbounded_conn.statements
        )
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

    # Production scans use named server cursors and bounded fetchmany calls;
    # minimal fakes without cursor support retain a small fetchall fallback.
    class _BatchCursor:
        def __init__(self, rows):
            self.rows = list(rows)
            self.offset = 0
            self.fetch_sizes = []
            self.executed = None
            self.closed = False

        def execute(self, query, params):
            self.executed = (query, tuple(params))

        def fetchmany(self, size):
            self.fetch_sizes.append(size)
            batch = self.rows[self.offset : self.offset + size]
            self.offset += len(batch)
            return batch

        def close(self):
            self.closed = True

    class _BatchConnection:
        def __init__(self, rows):
            self.rows = rows
            self.cursors = []

        def cursor(self, *, name):
            assert name.startswith("research_replay_stream_")
            cursor = _BatchCursor(self.rows)
            self.cursors.append(cursor)
            return cursor

    batch_conn = _BatchConnection([{"value": index} for index in range(1001)])
    assert [row["value"] for row in replay.iter_query_rows(
        batch_conn, "SELECT value FROM fake", (), batch_size=500
    )] == list(range(1001))
    assert batch_conn.cursors[0].fetch_sizes == [500, 500, 500, 500]
    assert batch_conn.cursors[0].closed is True

    class _FallbackRows:
        def fetchall(self):
            return [{"value": 1}, {"value": 2}]

    class _FallbackConnection:
        def execute(self, query, params):
            assert query == "SELECT fallback"
            assert params == ()
            return _FallbackRows()

    assert list(
        replay.iter_query_rows(_FallbackConnection(), "SELECT fallback")
    ) == [{"value": 1}, {"value": 2}]

    streaming_rows = [
        {
            "symbol": "BTC",
            "observation_time_utc": observation + timedelta(minutes=index),
            "horizon_minutes": 60,
        }
        for index in range(1001)
    ]
    existing_stream = _BatchConnection(streaming_rows)
    original_coherence = replay.replay_outcome_row_is_coherent
    replay.replay_outcome_row_is_coherent = lambda *args, **kwargs: True
    try:
        streamed_keys = replay._existing_keys(
            existing_stream,
            symbol="BTC",
            start=observation,
            end=observation + timedelta(days=2),
            horizons=(60,),
            width_index={},
        )
    finally:
        replay.replay_outcome_row_is_coherent = original_coherence
    assert len(streamed_keys) == 1001
    assert existing_stream.cursors[0].fetch_sizes == [500, 500, 500, 500]

    class _SingleRow:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class _CoverageConnection(_BatchConnection):
        def execute(self, query, params):
            normalized = " ".join(query.split())
            assert "MIN(observation_time_utc)" in normalized
            assert tuple(params) == (
                no_dwell.METHOD_VERSION,
                replay.REPLAY_VERSION,
            )
            return _SingleRow(
                {
                    "first_observation_utc": streaming_rows[0][
                        "observation_time_utc"
                    ],
                    "last_observation_utc": streaming_rows[-1][
                        "observation_time_utc"
                    ],
                    "symbols": ["BTC"],
                }
            )

    coverage_conn = _CoverageConnection(streaming_rows)
    original_load_references = replay.load_canonical_reference_rows
    original_build_width_index = replay.build_canonical_width_index
    original_coherence = replay.replay_outcome_row_is_coherent
    replay.load_canonical_reference_rows = lambda *args, **kwargs: []
    replay.build_canonical_width_index = lambda *args, **kwargs: {}
    replay.replay_outcome_row_is_coherent = lambda *args, **kwargs: True
    try:
        streamed_coverage = replay._coverage(coverage_conn)
    finally:
        replay.load_canonical_reference_rows = original_load_references
        replay.build_canonical_width_index = original_build_width_index
        replay.replay_outcome_row_is_coherent = original_coherence
    assert streamed_coverage["BTC:60"] == {
        "outcomes": 1001,
        "first_observation_utc": streaming_rows[0]["observation_time_utc"],
        "last_observation_utc": streaming_rows[-1]["observation_time_utc"],
        "utc_dates": 2,
    }
    assert coverage_conn.cursors[0].fetch_sizes == [500, 500, 500, 500]

    # Status reads only frozen coverage from the latest COMPLETED run under
    # this exact Replay version.  It must never trigger a full coherence scan.
    class _StatusConnection:
        def __init__(self):
            self.queries = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            normalized = " ".join(query.split())
            self.queries.append((normalized, tuple(params)))
            assert tuple(params) == (replay.REPLAY_VERSION,)
            if "status='COMPLETED'" in normalized:
                return _SingleRow(
                    {
                        "replay_run_id": 21,
                        "status": "COMPLETED",
                        "coverage": {"BTC:60": {"outcomes": 1001}},
                        "anchors_seen": 1001,
                        "outcomes_written": 1001,
                        "outcomes_skipped": 0,
                        "failures": 0,
                        "started_at_utc": observation,
                        "completed_at_utc": observation + timedelta(minutes=1),
                        "error_text": None,
                    }
                )
            return _SingleRow(
                {
                    "replay_run_id": 22,
                    "status": "FAILED",
                    "anchors_seen": 1,
                    "outcomes_written": 0,
                    "outcomes_skipped": 0,
                    "failures": 1,
                    "started_at_utc": observation + timedelta(minutes=2),
                    "completed_at_utc": observation + timedelta(minutes=3),
                    "error_text": "bounded failure",
                }
            )

    status_conn = _StatusConnection()
    original_connect = replay._connect
    original_table_exists = replay._table_exists
    original_research_url = replay._research_database_url
    original_psycopg = replay.psycopg
    original_coverage = replay._coverage
    replay._connect = lambda *args, **kwargs: status_conn
    replay._table_exists = lambda *args, **kwargs: True
    replay._research_database_url = lambda: "research://status-selftest"
    replay.psycopg = object()
    replay._coverage = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("status recomputed full coverage")
    )
    try:
        replay_status = replay.status()
    finally:
        replay._connect = original_connect
        replay._table_exists = original_table_exists
        replay._research_database_url = original_research_url
        replay.psycopg = original_psycopg
        replay._coverage = original_coverage
    assert replay_status["coverage"] == {"BTC:60": {"outcomes": 1001}}
    assert replay_status["latest_completed_run"]["replay_run_id"] == 21
    assert replay_status["latest_run"]["replay_run_id"] == 22
    assert replay_status["coverage_source"] == (
        "LATEST_EXACT_VERSION_COMPLETED_RUN"
    )
    assert all(
        params == (replay.REPLAY_VERSION,) for _, params in status_conn.queries
    )

    # An outcome failure is terminal for this run: already committed chunks
    # remain resumable, but the run itself can never be labelled COMPLETED.
    assert replay._bounded_completion_error(
        selected_anchor_count=2,
        horizon_count=4,
        outcomes_written=8,
        failures=0,
    ) is None
    assert "recorded 1 outcome failures" in replay._bounded_completion_error(
        selected_anchor_count=1,
        horizon_count=1,
        outcomes_written=0,
        failures=1,
    )
    assert "expected 2 writes, observed 1" in replay._bounded_completion_error(
        selected_anchor_count=1,
        horizon_count=2,
        outcomes_written=1,
        failures=0,
    )

    class _FailureConnection:
        def __init__(self):
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
                return _SingleRow({"acquired": True})
            if "INSERT INTO research_historical_replay_runs" in normalized:
                return _SingleRow({"replay_run_id": 901})
            if "UPDATE research_historical_replay_runs" in normalized:
                return _SingleRow(None)
            if "pg_advisory_unlock" in normalized:
                return _SingleRow({"unlocked": True})
            raise AssertionError(f"unexpected failure SQL: {normalized}")

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    failure_raw_conn = _FailureConnection()
    failure_research_conn = _FailureConnection()
    original_connect = replay._connect
    original_table_exists = replay._table_exists
    original_load_anchors = replay._load_anchors
    original_research_url = replay._research_database_url
    original_raw_url = replay._raw_database_url
    original_load_references = replay.load_canonical_reference_rows
    original_build_width_index = replay.build_canonical_width_index
    original_existing_keys = replay._existing_keys
    original_fetch_range = replay._fetch_range
    original_reference_candle = replay._reference_candle
    original_validated_route = replay.canonical_price_path.validated_route
    original_coverage = replay._coverage
    old_backfill_flag = replay.os.environ.get("HISTORICAL_REPLAY_BACKFILL")

    def _failure_connect(url, *, read_only):
        if url == "raw://failure-selftest":
            assert read_only is True
            return failure_raw_conn
        assert url == "research://failure-selftest"
        assert read_only is False
        return failure_research_conn

    replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = "1"
    replay._connect = _failure_connect
    replay._table_exists = lambda *args, **kwargs: True
    replay._load_anchors = lambda *args, **kwargs: [recovery_anchor]
    replay._research_database_url = lambda: "research://failure-selftest"
    replay._raw_database_url = lambda: "raw://failure-selftest"
    replay.load_canonical_reference_rows = lambda *args, **kwargs: []
    replay.build_canonical_width_index = lambda *args, **kwargs: {}
    replay._existing_keys = lambda *args, **kwargs: set()
    replay._adopt_recoverable_anchors = lambda *args, **kwargs: set()
    replay._fetch_range = lambda *args, **kwargs: {"candles": []}
    replay._reference_candle = lambda *args, **kwargs: None
    replay.canonical_price_path.validated_route = (
        lambda *args, **kwargs: {"validated": True}
    )
    replay._coverage = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("failed run attempted coverage")
    )
    try:
        try:
            replay.run_backfill(
                start=observation,
                end=recovery_end,
                symbols=("BTC",),
                horizons=(60,),
                chunk_days=2,
                max_anchors=1,
                pause_seconds=0.0,
            )
        except RuntimeError as exc:
            assert str(exc) == "bounded replay recorded 1 outcome failures"
        else:  # pragma: no cover
            raise AssertionError("failed bounded cohort was marked complete")
    finally:
        replay._connect = original_connect
        replay._table_exists = original_table_exists
        replay._load_anchors = original_load_anchors
        replay._research_database_url = original_research_url
        replay._raw_database_url = original_raw_url
        replay.load_canonical_reference_rows = original_load_references
        replay.build_canonical_width_index = original_build_width_index
        replay._existing_keys = original_existing_keys
        replay._adopt_recoverable_anchors = original_adopt_recoverable
        replay._fetch_range = original_fetch_range
        replay._reference_candle = original_reference_candle
        replay.canonical_price_path.validated_route = original_validated_route
        replay._coverage = original_coverage
        if old_backfill_flag is None:
            replay.os.environ.pop("HISTORICAL_REPLAY_BACKFILL", None)
        else:
            replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = old_backfill_flag
    run_updates = [
        statement
        for statement in failure_research_conn.statements
        if "UPDATE research_historical_replay_runs" in statement[0]
    ]
    assert any("status='FAILED'" in sql for sql, _ in run_updates)
    assert not any("status='COMPLETED'" in sql for sql, _ in run_updates)
    failed_update = next(
        params for sql, params in run_updates if "status='FAILED'" in sql
    )
    assert failed_update[3] == 1
    assert failure_research_conn.rollbacks == 1
    bounded_insert = next(
        params
        for sql, params in failure_research_conn.statements
        if "INSERT INTO research_historical_replay_runs" in sql
    )
    bounded_config = json.loads(bounded_insert[4])
    assert bounded_config["selection_policy_version"] == (
        replay.SELECTION_POLICY_VERSION
    )
    assert bounded_config["selected_anchor_count"] == 1
    assert bounded_config["selected_anchor_scope"]["BTC"]["count"] == 1
    assert len(bounded_config["selected_anchor_fingerprint_sha256"]) == 64

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
        "first_touch_replay_run_id": write_conn.params[15],
        "outcome_method_version": write_conn.params[17],
        "exchange": write_conn.params[18],
        "market": write_conn.params[19],
        "pair": write_conn.params[20],
        "interval_seconds": write_conn.params[21],
        "provenance": write_conn.params[22],
        "data_quality_status": write_conn.params[23],
        "replay_version": write_conn.params[24],
        "replay_run_id": write_conn.params[25],
        "sibling_reference_coherent": True,
    }
    assert replay.replay_outcome_row_is_coherent(
        coherent_row, width_index={}
    ) is True
    assert replay.replay_outcome_row_is_coherent(
        {**coherent_row, "first_touch_replay_run_id": 8},
        width_index={},
    ) is False

    owner_sql, owner_params = replay._replay_owner_scope_sql("stored")
    assert owner_params == ()
    assert "owner.status='COMPLETED'" in owner_sql
    assert (
        "stored.first_touch_replay_run_id=" in owner_sql
        and "stored.replay_run_id" in owner_sql
    )
    running_owner_sql, running_owner_params = (
        replay._replay_owner_scope_sql(
            "stored", include_running_run_id=8
        )
    )
    assert "owner.status='RUNNING'" in running_owner_sql
    assert running_owner_params == (8,)

    persisted_rows = [
        {
            **coherent_row,
            "replay_run_id": 8,
            "first_touch_replay_run_id": 8,
        }
    ]
    persisted_contract = replay._persisted_selected_anchor_contract(
        _BatchConnection(persisted_rows),
        run_id=8,
        horizons=(60,),
        width_index={},
    )
    assert persisted_contract == replay._selected_anchor_contract([hype_anchor])

    class _UpdateResult:
        rowcount = 1

    class _AdoptionConnection(_BatchConnection):
        def __init__(self, rows):
            super().__init__(rows)
            self.updates = []

        def execute(self, query, params):
            normalized = " ".join(query.split())
            assert normalized.startswith(
                "UPDATE research_historical_opportunity_outcomes stored"
            )
            self.updates.append((normalized, tuple(params)))
            return _UpdateResult()

    adoption_conn = _AdoptionConnection([coherent_row])
    adopted = replay._adopt_recoverable_anchors(
        adoption_conn,
        run_id=8,
        anchors=(hype_anchor,),
        horizons=(60,),
        width_index={},
    )
    assert adopted == {("HYPE", weekend_event, 60)}
    assert len(adoption_conn.updates) == 1
    assert adoption_conn.updates[0][1][:2] == (8, 8)
    partial_adoption_conn = _AdoptionConnection([coherent_row])
    assert replay._adopt_recoverable_anchors(
        partial_adoption_conn,
        run_id=8,
        anchors=(hype_anchor,),
        horizons=(60, 240),
        width_index={},
    ) == set()
    assert partial_adoption_conn.updates == []
    incoherent_adoption_conn = _AdoptionConnection(
        [{**coherent_row, "sibling_reference_coherent": False}]
    )
    assert replay._adopt_recoverable_anchors(
        incoherent_adoption_conn,
        run_id=8,
        anchors=(hype_anchor,),
        horizons=(60,),
        width_index={},
    ) == set()
    assert incoherent_adoption_conn.updates == []

    # End-to-end resume PoC: a complete coherent anchor from a failed owner is
    # adopted into the new bounded run, completes under the new fingerprint,
    # and never calls the external candle fetcher.
    class _ResumeResult:
        def __init__(self, row=None, *, rowcount=-1):
            self.row = row
            self.rowcount = rowcount

        def fetchone(self):
            return self.row

    class _ResumeCursor(_BatchCursor):
        def execute(self, query, params):
            normalized = " ".join(query.split())
            self.executed = (normalized, tuple(params))
            if "old_owner" in normalized:
                self.rows = [coherent_row]
            elif "WHERE stored.replay_run_id=%s" in normalized:
                self.rows = [
                    {
                        **coherent_row,
                        "replay_run_id": 902,
                        "first_touch_replay_run_id": 902,
                    }
                ]
            else:  # pragma: no cover
                raise AssertionError(f"unexpected resume cursor SQL: {normalized}")

    class _ResumeConnection:
        def __init__(self, run_id=902):
            self.run_id = run_id
            self.statements = []
            self.cursors = []
            self.commits = 0
            self.rollbacks = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def cursor(self, *, name):
            cursor = _ResumeCursor([])
            self.cursors.append(cursor)
            return cursor

        def execute(self, query, params=()):
            normalized = " ".join(str(query).split())
            self.statements.append((normalized, tuple(params or ())))
            if "pg_try_advisory_lock" in normalized:
                return _ResumeResult({"acquired": True})
            if "INSERT INTO research_historical_replay_runs" in normalized:
                return _ResumeResult({"replay_run_id": self.run_id})
            if normalized.startswith(
                "UPDATE research_historical_opportunity_outcomes stored"
            ):
                return _ResumeResult(rowcount=1)
            if "UPDATE research_historical_replay_runs" in normalized:
                return _ResumeResult(rowcount=1)
            if "pg_advisory_unlock" in normalized:
                return _ResumeResult({"unlocked": True})
            raise AssertionError(f"unexpected resume SQL: {normalized}")

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    resume_raw_conn = _ResumeConnection()
    resume_research_conn = _ResumeConnection()
    resume_fetch_calls = []
    original_connect = replay._connect
    original_table_exists = replay._table_exists
    original_load_anchors = replay._load_anchors
    original_research_url = replay._research_database_url
    original_raw_url = replay._raw_database_url
    original_load_references = replay.load_canonical_reference_rows
    original_build_width_index = replay.build_canonical_width_index
    original_existing_keys = replay._existing_keys
    original_fetch_range = replay._fetch_range
    original_write_outcome = replay._write_outcome
    original_coverage = replay._coverage
    old_backfill_flag = replay.os.environ.get("HISTORICAL_REPLAY_BACKFILL")

    def _resume_connect(url, *, read_only):
        if url == "raw://resume-selftest":
            assert read_only is True
            return resume_raw_conn
        assert url == "research://resume-selftest"
        assert read_only is False
        return resume_research_conn

    def _unexpected_resume_fetch(*args, **kwargs):
        resume_fetch_calls.append((args, kwargs))
        raise AssertionError("adopted anchor reached external fetch")

    replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = "1"
    replay._connect = _resume_connect
    replay._table_exists = lambda *args, **kwargs: True
    replay._load_anchors = lambda *args, **kwargs: [hype_anchor]
    replay._research_database_url = lambda: "research://resume-selftest"
    replay._raw_database_url = lambda: "raw://resume-selftest"
    replay.load_canonical_reference_rows = lambda *args, **kwargs: []
    replay.build_canonical_width_index = lambda *args, **kwargs: {}
    replay._existing_keys = lambda *args, **kwargs: set()
    replay._fetch_range = _unexpected_resume_fetch
    replay._write_outcome = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("adopted anchor was rewritten")
    )
    replay._coverage = lambda *args, **kwargs: {
        "HYPE:60": {
            "outcomes": 1,
            "first_observation_utc": weekend_event,
            "last_observation_utc": weekend_event,
            "utc_dates": 1,
        }
    }
    try:
        resumed = replay.run_backfill(
            start=weekend_event,
            end=weekend_event + timedelta(minutes=1),
            symbols=("HYPE",),
            horizons=(60,),
            chunk_days=2,
            max_anchors=1,
            pause_seconds=0.0,
        )
    finally:
        replay._connect = original_connect
        replay._table_exists = original_table_exists
        replay._load_anchors = original_load_anchors
        replay._research_database_url = original_research_url
        replay._raw_database_url = original_raw_url
        replay.load_canonical_reference_rows = original_load_references
        replay.build_canonical_width_index = original_build_width_index
        replay._existing_keys = original_existing_keys
        replay._fetch_range = original_fetch_range
        replay._write_outcome = original_write_outcome
        replay._coverage = original_coverage
        if old_backfill_flag is None:
            replay.os.environ.pop("HISTORICAL_REPLAY_BACKFILL", None)
        else:
            replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = old_backfill_flag
    assert resumed["replay_run_id"] == 902
    assert resumed["outcomes_written"] == 1
    assert resumed["outcomes_skipped"] == 0
    assert resumed["failures"] == 0
    assert resume_fetch_calls == []
    assert any(
        "status='COMPLETED'" in sql
        for sql, _ in resume_research_conn.statements
    )
    assert not any(
        "status='FAILED'" in sql
        for sql, _ in resume_research_conn.statements
    )

    # Mixed resume regression: after one anchor is adopted, the canonical
    # reference/width index is reloaded with the current run admitted *before*
    # a later selected anchor is freshly labelled.
    fresh_hype_anchor = replay.Anchor(
        "HYPE",
        weekend_event + timedelta(minutes=120),
        weekend_event + timedelta(minutes=120),
    )
    mixed_anchors = [hype_anchor, fresh_hype_anchor]
    mixed_raw_conn = _ResumeConnection(run_id=903)
    mixed_research_conn = _ResumeConnection(run_id=903)
    mixed_reference_loads = []
    mixed_width_builds = []
    mixed_fetches = []
    mixed_writes = []
    original_connect = replay._connect
    original_table_exists = replay._table_exists
    original_load_anchors = replay._load_anchors
    original_research_url = replay._research_database_url
    original_raw_url = replay._raw_database_url
    original_load_references = replay.load_canonical_reference_rows
    original_build_width_index = replay.build_canonical_width_index
    original_existing_keys = replay._existing_keys
    original_fetch_range = replay._fetch_range
    original_reference_candle = replay._reference_candle
    original_outcome_candles = replay._outcome_candles
    original_write_outcome = replay._write_outcome
    original_validated_route = replay.canonical_price_path.validated_route
    original_persisted_contract = replay._persisted_selected_anchor_contract
    original_coverage = replay._coverage
    old_backfill_flag = replay.os.environ.get("HISTORICAL_REPLAY_BACKFILL")

    def _mixed_connect(url, *, read_only):
        if url == "raw://mixed-resume-selftest":
            assert read_only is True
            return mixed_raw_conn
        assert url == "research://mixed-resume-selftest"
        assert read_only is False
        return mixed_research_conn

    def _mixed_load_references(*args, **kwargs):
        mixed_reference_loads.append(dict(kwargs))
        return []

    def _mixed_build_width(*args, **kwargs):
        mixed_width_builds.append(len(mixed_reference_loads))
        return {}

    def _mixed_fetch(*args, **kwargs):
        assert len(mixed_reference_loads) >= 2
        assert mixed_reference_loads[-1].get(
            "include_running_run_id"
        ) == 903
        mixed_fetches.append((args, kwargs))
        return {"candles": []}

    def _mixed_write(*args, **kwargs):
        assert kwargs["anchor"] == fresh_hype_anchor
        assert len(mixed_reference_loads) >= 2
        mixed_writes.append((args, kwargs))

    mixed_contract = replay._selected_anchor_contract(mixed_anchors)
    mixed_coverage = {
        "HYPE:60": {
            "outcomes": 2,
            "first_observation_utc": weekend_event,
            "last_observation_utc": fresh_hype_anchor.observation_time_utc,
            "utc_dates": 1,
        }
    }
    replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = "1"
    replay._connect = _mixed_connect
    replay._table_exists = lambda *args, **kwargs: True
    replay._load_anchors = lambda *args, **kwargs: mixed_anchors
    replay._research_database_url = lambda: "research://mixed-resume-selftest"
    replay._raw_database_url = lambda: "raw://mixed-resume-selftest"
    replay.load_canonical_reference_rows = _mixed_load_references
    replay.build_canonical_width_index = _mixed_build_width
    replay._existing_keys = lambda *args, **kwargs: set()
    replay._fetch_range = _mixed_fetch
    replay._reference_candle = lambda *args, **kwargs: hype_reference
    replay._outcome_candles = lambda *args, **kwargs: hype_future
    replay._write_outcome = _mixed_write
    replay.canonical_price_path.validated_route = (
        lambda *args, **kwargs: {"validated": True}
    )
    replay._persisted_selected_anchor_contract = (
        lambda *args, **kwargs: mixed_contract
    )
    replay._coverage = lambda *args, **kwargs: mixed_coverage
    try:
        mixed_result = replay.run_backfill(
            start=weekend_event,
            end=fresh_hype_anchor.observation_time_utc + timedelta(minutes=1),
            symbols=("HYPE",),
            horizons=(60,),
            chunk_days=2,
            max_anchors=2,
            pause_seconds=0.0,
        )
    finally:
        replay._connect = original_connect
        replay._table_exists = original_table_exists
        replay._load_anchors = original_load_anchors
        replay._research_database_url = original_research_url
        replay._raw_database_url = original_raw_url
        replay.load_canonical_reference_rows = original_load_references
        replay.build_canonical_width_index = original_build_width_index
        replay._existing_keys = original_existing_keys
        replay._fetch_range = original_fetch_range
        replay._reference_candle = original_reference_candle
        replay._outcome_candles = original_outcome_candles
        replay._write_outcome = original_write_outcome
        replay.canonical_price_path.validated_route = original_validated_route
        replay._persisted_selected_anchor_contract = original_persisted_contract
        replay._coverage = original_coverage
        if old_backfill_flag is None:
            replay.os.environ.pop("HISTORICAL_REPLAY_BACKFILL", None)
        else:
            replay.os.environ["HISTORICAL_REPLAY_BACKFILL"] = old_backfill_flag
    assert mixed_result["outcomes_written"] == 2
    assert mixed_result["outcomes_skipped"] == 0
    assert len(mixed_reference_loads) >= 2
    assert mixed_reference_loads[0].get("include_running_run_id") is None
    assert mixed_reference_loads[1]["include_running_run_id"] == 903
    assert len(mixed_fetches) == 1
    assert len(mixed_writes) == 1
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
