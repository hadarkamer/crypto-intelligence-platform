"""Deterministic checks for the no-lookahead research feature matrix."""

from datetime import datetime, timedelta, timezone

import research_feature_matrix as matrix


def _price(symbol, timestamp, price, oi, source="selftest"):
    return {
        "symbol": symbol,
        "candle_time": timestamp,
        "price_close": price,
        "oi_close_usd": oi,
        "price_exchange": "Binance",
        "price_pair": f"{symbol}USDT",
        "source": source,
    }


def _flow(symbol, timestamp, continuous, api, buy=60.0, sell=40.0):
    return {
        "symbol": symbol,
        "candle_time": timestamp,
        "buy_volume_usd": buy,
        "sell_volume_usd": sell,
        "api_cum_vol_delta_usd": api,
        "continuous_cum_vol_delta_usd": continuous,
        "exchange_list": "Binance,OKX,Bybit",
        "source": "selftest",
    }


def run() -> None:
    assert matrix.VERIFIED_OUTCOME_METHOD == "no-dwell-first-touch-v6"
    # The production session contract is New York local time, including DST.
    # Summer: Friday 20:00 ET = Saturday 00:00 UTC; Sunday 18:00 ET = 22:00 UTC.
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 28, 23, 59, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 30, 21, 59, tzinfo=timezone.utc)
    )
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 8, 30, 22, 0, tzinfo=timezone.utc)
    )
    active_ratio, weekend_ratio, _ = matrix.market_session_baseline.session_ratios(
        datetime(2026, 8, 28, 23, 30, tzinfo=timezone.utc),
        datetime(2026, 8, 29, 0, 30, tzinfo=timezone.utc),
    )
    assert active_ratio == 0.5 and weekend_ratio == 0.5
    boundary_event = datetime(2026, 8, 29, 0, 30, tzinfo=timezone.utc)
    boundary_windows = {
        minutes: matrix._window_features(
            event_time=boundary_event,
            minutes=minutes,
            price_series=None,
            futures_series=None,
            spot_series=None,
        )
        for minutes in matrix.CORE_WINDOWS_MINUTES
    }
    assert boundary_windows[30]["session_active_ratio"] == 0.0
    assert boundary_windows[60]["session_active_ratio"] == 0.5
    assert boundary_windows[240]["session_active_ratio"] == 0.875
    assert boundary_windows[720]["session_active_ratio"] == 0.958333
    assert boundary_windows[1440]["session_active_ratio"] == 0.979167

    # Winter boundaries move one UTC hour later and must remain exact.
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 5, 0, 59, tzinfo=timezone.utc)
    )
    assert not matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 5, 1, 0, tzinfo=timezone.utc)
    )
    assert matrix.market_session_baseline.is_active_market(
        datetime(2026, 12, 6, 23, 0, tzinfo=timezone.utc)
    )
    summer_time = matrix.market_session_baseline.market_time_features(
        datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    )
    winter_time = matrix.market_session_baseline.market_time_features(
        datetime(2026, 12, 5, 12, 0, tzinfo=timezone.utc)
    )
    assert summer_time["market_local_hour"] == 8
    assert winter_time["market_local_hour"] == 7
    assert summer_time["market_utc_offset_minutes"] == -240
    assert winter_time["market_utc_offset_minutes"] == -300
    candle_open = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    assert matrix.market_session_baseline.closed_candle_available_at(
        candle_open
    ) == datetime(2026, 8, 29, 12, 32, tzinfo=timezone.utc)
    shifted = matrix._closed_archive_rows(
        [{"symbol": "BTC", "candle_time": candle_open}]
    )[0]
    assert shifted["source_candle_time"] == candle_open
    assert shifted["candle_time"] == datetime(
        2026, 8, 29, 12, 32, tzinfo=timezone.utc
    )

    class _Fetched:
        def fetchall(self):
            return [{"event_id": 1, "symbol": "BTC"}]

    class _Connection:
        def execute(self, query, params):
            assert query.count("%s") == len(params)
            assert "e.symbol=%s" in query
            assert "e.event_type=%s" in query
            assert "e.direction=%s" in query
            assert "JOIN research_first_touch_outcomes ft" in query
            assert "ft.success AS path_success" in query
            assert "ft.pre_qualifying_mae_pct" in query
            assert "ft.method_version=%s" in query
            assert "ft.status IN ('HIT', 'MISS')" in query
            assert matrix.VERIFIED_OUTCOME_METHOD in params
            assert matrix.canonical_price_path.METHOD_VERSION in params
            return _Fetched()

    loaded = matrix._load_verified_events(
        _Connection(),
        symbol="BTC",
        event_type="SELFTEST",
        direction="LONG",
        lookback_days=30,
        horizon_minutes=240,
        limit=100,
    )
    assert loaded == [{"event_id": 1, "symbol": "BTC"}]

    # Max-Pain features are loaded only from coherent migration-007 sets that
    # were available no later than each decision.  Exercise the query boundary
    # directly and audit every executed statement against the legacy table.
    class _BatchResult:
        def __init__(self, *, one=None, many=()):
            self._one = one
            self._many = list(many)

        def fetchone(self):
            return self._one

        def fetchall(self):
            return self._many

    class _BatchConnection:
        def __init__(self):
            self.queries = []

        def execute(self, query, params):
            assert query.count("%s") == len(params)
            normalized = " ".join(query.split())
            self.queries.append(normalized)
            if "to_regclass" in normalized:
                return _BatchResult(one={"relation": "installed"})
            return _BatchResult(many=[])

    batch_conn = _BatchConnection()
    batch_result = matrix._load_max_pain_features_batch(
        batch_conn,
        [
            {
                "event_id": 999,
                "symbol": "BTC",
                "alert_time_utc": datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc),
            }
        ],
    )
    assert batch_result[999]["evaluation_status"] == "UNEVALUABLE"
    assert all("max_pain_snapshots" not in query for query in batch_conn.queries)
    candidate_query = next(
        query
        for query in batch_conn.queries
        if "research_max_pain_snapshot_sets" in query
        and "requested" in query
    )
    assert "available_at_utc<=requested.decision_time_utc" in candidate_query
    assert "created_at_utc<=requested.decision_time_utc" in candidate_query
    assert "LIMIT 2" in candidate_query

    # Prospective sampler v3 rows are accepted only when the complete slot
    # payload reproduces its fingerprint. Max Pain comes from that same frozen
    # payload and is mapped to both neutral directions.
    slot_time = datetime(2026, 8, 29, 12, 34, tzinfo=timezone.utc)
    slot_open = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    slot_close = slot_open + timedelta(minutes=30)
    eligible_at = slot_close + timedelta(minutes=2)
    expires_at = eligible_at + timedelta(minutes=30)
    slot_timestamps = {
        family: {"source_time_utc": slot_open.isoformat()}
        for family in matrix.research_prospective_anchors.REQUIRED_FAMILIES
    }
    slot_provenance = {
        "official_price": {
            "source": "binance_spot",
            "price_exchange": "Binance",
            "price_market": "spot",
            "price_pair": "BTCUSDT",
            "price_timeframe": "1m",
        },
        "price_oi": {"source": "oi_regime_snapshots"},
        "futures_cvd": {
            "source": "coinglass_futures_aggregated_cvd",
            "exchange_list": "Binance,OKX,Bybit",
        },
        "spot_cvd": {
            "source": "coinglass_spot_aggregated_cvd",
            "exchange_list": "Binance,OKX,Bybit",
        },
    }
    slot_frozen = {
        "official_price": {"price": 100.0},
        "price_oi": {"oi_close_usd": 1_000_000.0},
        "futures_cvd": {
            "buy_volume_usd": 20.0,
            "sell_volume_usd": 10.0,
            "api_cum_vol_delta_usd": 9.0,
            "continuous_cum_vol_delta_usd": 10.0,
        },
        "spot_cvd": {
            "buy_volume_usd": 11.0,
            "sell_volume_usd": 8.0,
            "api_cum_vol_delta_usd": 2.0,
            "continuous_cum_vol_delta_usd": 3.0,
        },
        "max_pain": {
            "evaluation_status": "UNEVALUABLE",
            "reason": "no prior coherent snapshot",
            "features": {},
        },
    }
    coverage_snapshot = {"symbol": "BTC", "eligible": True}
    slot_row = {
        "anchor_slot_id": 71,
        "sampler_version": matrix.PROSPECTIVE_ANCHOR_SAMPLER_VERSION,
        "coverage_policy_version": "selftest-coverage",
        "coverage_snapshot": coverage_snapshot,
        "symbol": "BTC",
        "source_candle_open_utc": slot_open,
        "source_candle_close_utc": slot_close,
        "base_eligible_at_utc": eligible_at,
        "expires_at_utc": expires_at,
        "decision_time_utc": slot_time,
        "source_timestamps": slot_timestamps,
        "source_provenance": slot_provenance,
        "frozen_inputs": slot_frozen,
        "long_event_id": 701,
        "short_event_id": 702,
        "created_at_utc": slot_time + timedelta(seconds=1),
    }
    slot_row["input_fingerprint"] = (
        matrix.research_prospective_anchors.compute_input_fingerprint(
            sampler_version=slot_row["sampler_version"],
            coverage_policy_version=slot_row["coverage_policy_version"],
            coverage_snapshot=coverage_snapshot,
            symbol="BTC",
            source_candle_open_utc=slot_open,
            source_candle_close_utc=slot_close,
            base_eligible_at_utc=eligible_at,
            expires_at_utc=expires_at,
            evaluation_status=matrix.research_prospective_anchors.EVALUABLE,
            decision_time_utc=slot_time,
            source_timestamps=slot_timestamps,
            source_provenance=slot_provenance,
            frozen_inputs=slot_frozen,
        )
    )

    class _FrozenSlotConnection:
        def __init__(self, row):
            self.row = row

        def execute(self, query, params):
            assert query.count("%s") == len(params)
            assert params[0] == matrix.PROSPECTIVE_ANCHOR_SAMPLER_VERSION
            return _BatchResult(many=[self.row])

    frozen_loaded = matrix._load_prospective_frozen_rows(
        _FrozenSlotConnection(slot_row),
        symbols=("BTC",),
        start=slot_time - timedelta(days=1),
        end=slot_time,
    )
    assert [len(values) for values in frozen_loaded[:4]] == [1, 1, 1, 1]
    assert frozen_loaded[4] == {
        701: slot_frozen["max_pain"],
        702: slot_frozen["max_pain"],
    }
    fingerprint_tampered = {**slot_row, "input_fingerprint": "0" * 64}
    rejected = matrix._load_prospective_frozen_rows(
        _FrozenSlotConnection(fingerprint_tampered),
        symbols=("BTC",),
        start=slot_time - timedelta(days=1),
        end=slot_time,
    )
    assert all(not values for values in rejected)

    coverage_time = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)

    class _CoverageRows:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def _assert_exact_coverage_contract(query, params):
        normalized = " ".join(query.split())
        assert query.count("%s") == len(params)
        assert "historical.horizon_minutes=%s" in normalized
        assert "NOW() - (%s * INTERVAL '1 day')" in normalized
        assert "historical.first_touch_method_version=%s" in normalized
        assert "historical.replay_version=%s" in normalized
        assert (
            "historical.first_touch_data_quality_status=ANY(%s)"
            in normalized
        )
        assert "historical.outcome_method_version=%s" in normalized
        assert "historical.data_quality_status=ANY(%s)" in normalized
        assert (
            "historical.first_touch_replay_run_id="
            "historical.replay_run_id" in normalized
        )
        assert "FROM research_historical_replay_runs owner_run" in normalized
        assert (
            "owner_run.replay_run_id=historical.replay_run_id" in normalized
        )
        assert (
            "owner_run.replay_version=historical.replay_version" in normalized
        )
        assert "owner_run.status='COMPLETED'" in normalized
        assert params == (
            240,
            3650,
            matrix.VERIFIED_OUTCOME_METHOD,
            matrix.research_historical_replay.REPLAY_VERSION,
            list(matrix.VERIFIED_OUTCOME_QUALITIES),
            matrix.canonical_price_path.METHOD_VERSION,
            list(matrix.VERIFIED_OUTCOME_QUALITIES),
        )

    class _StreamingCoverageCursor:
        def __init__(self, connection):
            self.connection = connection
            self.rows = []
            self.offset = 0
            self.closed = False

        def execute(self, query, params):
            _assert_exact_coverage_contract(query, params)
            normalized = " ".join(query.split())
            assert "SELECT historical.opportunity_id" in normalized
            assert "COUNT(*)" not in normalized
            self.connection.stream_queries.append(normalized)
            # The fake models PostgreSQL applying the exact current-contract
            # WHERE clause.  Its explicit legacy row must never reach this
            # bounded stream.
            self.rows = list(self.connection.current_rows)
            assert self.connection.legacy_row not in self.rows
            return self

        def fetchmany(self, size):
            assert size == matrix.REPLAY_COVERAGE_STREAM_BATCH_SIZE
            self.connection.fetchmany_calls += 1
            start = self.offset
            self.offset += size
            return self.rows[start : self.offset]

        def fetchall(self):
            self.connection.stream_fetchall_calls += 1
            raise AssertionError("coverage candidates must never use fetchall")

        def close(self):
            self.closed = True
            self.connection.closed_stream_cursors += 1

    class _CoverageConnection:
        def __init__(self):
            rows = []
            opportunity_id = 1
            for symbol in ("BTC", "ETH", "SOL", "DOGE"):
                for index in range(300):
                    rows.append(
                        {
                            "opportunity_id": opportunity_id,
                            "symbol": symbol,
                            "observation_time_utc": (
                                coverage_time
                                - timedelta(days=20)
                                + timedelta(
                                    seconds=(20 * 86400 * index / 299)
                                )
                            ),
                            "coherent": True,
                        }
                    )
                    opportunity_id += 1
            # This structurally selectable row sits outside any bounded
            # training sample and simulates a tampered reference/hash.  Full
            # coverage recomputation must reject rather than count it.
            rows.append(
                {
                    "opportunity_id": opportunity_id,
                    "symbol": "BTC",
                    "observation_time_utc": coverage_time - timedelta(days=10),
                    "coherent": False,
                }
            )
            opportunity_id += 1
            for index in range(100):
                rows.append(
                    {
                        "opportunity_id": opportunity_id,
                        "symbol": "HYPE",
                        "observation_time_utc": (
                            coverage_time
                            - timedelta(days=3)
                            + timedelta(seconds=(3 * 86400 * index / 99))
                        ),
                        "coherent": True,
                    }
                )
                opportunity_id += 1
            self.current_rows = rows
            self.legacy_row = {
                "opportunity_id": opportunity_id,
                "symbol": "LEGACY",
                "observation_time_utc": coverage_time - timedelta(days=1),
                "replay_version": "historical-raw-opportunity-replay-v1",
                "coherent": True,
            }
            self.owner_rejected_rows = [
                {
                    "opportunity_id": opportunity_id + 1,
                    "symbol": "FAILED_OWNER",
                    "observation_time_utc": coverage_time - timedelta(days=1),
                    "replay_run_id": 81,
                    "first_touch_replay_run_id": 81,
                    "owner_status": "FAILED",
                    "coherent": True,
                },
                {
                    "opportunity_id": opportunity_id + 2,
                    "symbol": "RUNNING_OWNER",
                    "observation_time_utc": coverage_time - timedelta(days=1),
                    "replay_run_id": 82,
                    "first_touch_replay_run_id": 82,
                    "owner_status": "RUNNING",
                    "coherent": True,
                },
                {
                    "opportunity_id": opportunity_id + 3,
                    "symbol": "MISMATCHED_OWNER",
                    "observation_time_utc": coverage_time - timedelta(days=1),
                    "replay_run_id": 83,
                    "first_touch_replay_run_id": 84,
                    "owner_status": "COMPLETED",
                    "coherent": True,
                },
            ]
            self.aggregate_fetchall_calls = 0
            self.stream_fetchall_calls = 0
            self.fetchmany_calls = 0
            self.closed_stream_cursors = 0
            self.stream_queries = []
            self.cursor_names = []

        def execute(self, query, params):
            _assert_exact_coverage_contract(query, params)
            normalized = " ".join(query.split())
            assert "COUNT(*)::bigint AS stored_candidates" in normalized
            assert "MIN(historical.observation_time_utc)" in normalized
            assert "MAX(historical.observation_time_utc)" in normalized
            assert "SELECT historical.opportunity_id" not in normalized
            self.aggregate_fetchall_calls += 1
            grouped = []
            for symbol in ("BTC", "DOGE", "ETH", "HYPE", "SOL"):
                symbol_rows = [
                    row for row in self.current_rows if row["symbol"] == symbol
                ]
                grouped.append(
                    {
                        "symbol": symbol,
                        "stored_candidates": len(symbol_rows),
                        "first_candidate_utc": min(
                            row["observation_time_utc"] for row in symbol_rows
                        ),
                        "last_candidate_utc": max(
                            row["observation_time_utc"] for row in symbol_rows
                        ),
                    }
                )
            return _CoverageRows(grouped)

        def cursor(self, *, name):
            assert name.startswith("research_replay_stream_")
            self.cursor_names.append(name)
            return _StreamingCoverageCursor(self)

    coverage_conn = _CoverageConnection()

    original_reference_loader = (
        matrix.research_historical_replay.load_canonical_reference_rows
    )
    original_width_builder = (
        matrix.research_historical_replay.build_canonical_width_index
    )
    original_coherence = (
        matrix.research_historical_replay.replay_outcome_row_is_coherent
    )
    matrix.research_historical_replay.load_canonical_reference_rows = (
        lambda *args, **kwargs: []
    )
    matrix.research_historical_replay.build_canonical_width_index = (
        lambda *args, **kwargs: {}
    )
    matrix.research_historical_replay.replay_outcome_row_is_coherent = (
        lambda row, **kwargs: bool(row.get("coherent"))
    )
    try:
        coverage = matrix._historical_replay_coverage(
            coverage_conn, lookback_days=3650, horizon_minutes=240
        )
    finally:
        matrix.research_historical_replay.load_canonical_reference_rows = (
            original_reference_loader
        )
        matrix.research_historical_replay.build_canonical_width_index = (
            original_width_builder
        )
        matrix.research_historical_replay.replay_outcome_row_is_coherent = (
            original_coherence
        )
    assert coverage["replacement_ready"] is True
    assert coverage["symbols"] == 4
    assert coverage["stored_symbols"] == 5
    assert coverage["eligible_symbols"] == ["BTC", "DOGE", "ETH", "SOL"]
    assert coverage["stored_anchors"] == 1301
    assert coverage["coherent_anchors"] == 1300
    assert coverage["by_symbol"]["BTC"]["anchors"] == 300
    assert coverage["by_symbol"]["BTC"]["recomputed_policy_rejections"] == 1
    assert coverage["recomputed_policy_rejections"] == 1
    assert coverage["coverage_validation"] == "full-row prior-only recomputation"
    assert coverage_conn.aggregate_fetchall_calls == 1
    assert coverage_conn.stream_fetchall_calls == 0
    assert coverage_conn.fetchmany_calls >= 3
    assert coverage_conn.closed_stream_cursors == 1
    assert len(coverage_conn.cursor_names) == 1
    assert len(coverage_conn.stream_queries) == 1
    assert "historical-raw-opportunity-replay-v1" not in str(
        coverage_conn.stream_queries
    )
    assert all(row["coherent"] for row in coverage_conn.owner_rejected_rows)
    assert all(
        row["symbol"] not in coverage["by_symbol"]
        for row in coverage_conn.owner_rejected_rows
    )
    assert coverage["excluded_symbols"]["HYPE"] == [
        "minimum_anchors",
        "minimum_utc_dates",
        "minimum_span_hours",
    ]

    class _DeliveredCoverageConnection:
        def execute(self, query, params):
            assert query.count("%s") == len(params)
            assert "LEFT JOIN research_first_touch_outcomes ft" in query
            assert "ft.method_version=%s" in query
            assert "ft.status IN ('HIT', 'MISS')" in query
            assert "ft.data_quality_status=ANY(%s)" in query
            assert matrix.VERIFIED_OUTCOME_METHOD in params
            return _CoverageRows([])

    delivered_coverage = matrix._verified_coverage(
        _DeliveredCoverageConnection(), lookback_days=30, horizon_minutes=240
    )
    assert delivered_coverage["by_symbol"] == {}

    class _OpportunityConnection:
        def __init__(self):
            self.completed = {
                "opportunity_id": 901,
                "symbol": "BTC",
                "observation_time_utc": coverage_time,
            }
            self.owner_rejected = [
                {
                    "opportunity_id": 902,
                    "symbol": "BTC",
                    "observation_time_utc": coverage_time,
                    "owner_status": "FAILED",
                    "replay_run_id": 91,
                    "first_touch_replay_run_id": 91,
                    "coherent": True,
                },
                {
                    "opportunity_id": 903,
                    "symbol": "BTC",
                    "observation_time_utc": coverage_time,
                    "owner_status": "RUNNING",
                    "replay_run_id": 92,
                    "first_touch_replay_run_id": 92,
                    "coherent": True,
                },
                {
                    "opportunity_id": 904,
                    "symbol": "BTC",
                    "observation_time_utc": coverage_time,
                    "owner_status": "COMPLETED",
                    "replay_run_id": 93,
                    "first_touch_replay_run_id": 94,
                    "coherent": True,
                },
            ]

        def execute(self, query, params):
            assert query.count("%s") == len(params)
            normalized = " ".join(query.split())
            assert "symbol=ANY(%s)" in query
            assert params[-3] == ["BTC", "ETH"]
            assert "long_first_touch_metrics" in query
            assert "short_first_touch_metrics" in query
            assert "first_touch_method_version=%s" in query
            assert "first_touch_path_samples" in query
            assert "sibling_reference_coherent" in query
            assert "NOT EXISTS" in query
            assert "first_touch_data_quality_status=ANY(%s)" in query
            assert matrix.VERIFIED_OUTCOME_METHOD in params
            assert (
                "historical.first_touch_replay_run_id="
                "historical.replay_run_id" in normalized
            )
            assert (
                "FROM research_historical_replay_runs owner_run" in normalized
            )
            assert (
                "owner_run.replay_run_id=historical.replay_run_id"
                in normalized
            )
            assert (
                "owner_run.replay_version=historical.replay_version"
                in normalized
            )
            assert "owner_run.status='COMPLETED'" in normalized
            # Model PostgreSQL applying the owner predicate: all three rows
            # below are structurally coherent but are not returned.
            assert all(row["coherent"] for row in self.owner_rejected)
            return _CoverageRows([self.completed])

    opportunity_conn = _OpportunityConnection()
    owner_bound_opportunities = matrix._load_historical_opportunities(
        opportunity_conn,
        lookback_days=3650,
        horizon_minutes=240,
        anchor_limit=100,
        symbols=["ETH", "BTC"],
    )
    assert [row["opportunity_id"] for row in owner_bound_opportunities] == [901]
    assert not {
        row["opportunity_id"] for row in opportunity_conn.owner_rejected
    }.intersection(
        row["opportunity_id"] for row in owner_bound_opportunities
    )

    replay_events = matrix._opportunity_events(
        [
            {
                "opportunity_id": 42,
                "symbol": "BTC",
                "observation_time_utc": coverage_time,
                "source_observation_time_utc": coverage_time,
                "horizon_minutes": 240,
                "reference_price": 100.0,
                "path_samples": 240,
                "long_metrics": {
                    "directional_return_pct": 8.0,
                    "mfe_pct": 9.0,
                    "mae_pct": 4.0,
                },
                "short_metrics": {
                    "directional_return_pct": -8.0,
                    "mfe_pct": 7.0,
                    "mae_pct": 5.0,
                },
                "long_first_touch_metrics": {
                    "success": False,
                    "status": "MISS",
                    "pre_qualifying_mae_pct": 1.25,
                    "qualifying_candle_order_ambiguous": False,
                },
                "short_first_touch_metrics": {
                    "success": True,
                    "status": "HIT",
                    "pre_qualifying_mae_pct": 0.40,
                    "qualifying_candle_order_ambiguous": True,
                },
                "first_touch_method_version": matrix.VERIFIED_OUTCOME_METHOD,
                "first_touch_data_quality_status": "COMPLETE",
                "legacy_outcome_method_version": "canonical-spot-path-v1",
                "legacy_data_quality_status": "COMPLETE",
            }
        ]
    )
    replay_long = next(
        item for item in replay_events if item["direction"] == "LONG"
    )
    replay_short = next(
        item for item in replay_events if item["direction"] == "SHORT"
    )
    assert replay_long["directional_return_pct"] == 8.0
    assert replay_long["path_success"] is False
    assert replay_long["first_touch_status"] == "MISS"
    assert replay_long["pre_qualifying_mae_pct"] == 1.25
    assert replay_short["directional_return_pct"] == -8.0
    assert replay_short["path_success"] is True
    assert replay_short["first_touch_status"] == "HIT"
    assert replay_short["pre_qualifying_mae_pct"] == 0.40
    assert replay_short["qualifying_candle_order_ambiguous"] is True
    assert replay_short["outcome_method_version"] == matrix.VERIFIED_OUTCOME_METHOD
    assert replay_short["legacy_outcome_method_version"] == "canonical-spot-path-v1"

    event_time = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    reference_time = event_time - timedelta(minutes=61)
    current_time = event_time - timedelta(minutes=1)
    future_time = event_time + timedelta(minutes=1)

    event = {
        "event_id": 77,
        "alert_time_utc": event_time,
        "symbol": "BTC",
        "direction": "LONG",
        "source_side": "SHORT",
        "timeframe": "1h",
        "event_type": "COMBINED_CONFIRMATION",
        "score": 82.5,
        "current_price": 102.0,
        "target_price": 104.0,
        "initial_target_distance_pct": 1.960784,
        "categories": ["OI_PRICE", "FUTURES_CVD_HIGH", "SPOT_CVD_HIGH"],
        "setup_key": "same-setup",
        "strategy_version": "selftest-v1",
        "code_version": "abc123",
        "engine_snapshot": {
            "component_scores": {
                "oi_price": 70.0,
                "futures_cvd": 65.0,
                "spot_cvd": 75.0,
            },
            "market_evidence": {
                "classification": "CORE_CONFIRMATION",
                "supporting_families": 3,
            },
        },
        "horizon_minutes": 240,
        "measured_at_utc": event_time + timedelta(hours=4),
        "reference_price": 102.0,
        "price_at_horizon": 105.0,
        "raw_return_pct": 2.941176,
        "directional_return_pct": 2.941176,
        "mfe_pct": 4.0,
        # Full-horizon reversal remains diagnostic. Formula risk must use the
        # adverse excursion only through the qualifying first touch.
        "mae_pct": 9.5,
        "path_success": True,
        "first_touch_status": "HIT",
        "first_qualifying_move_time_utc": event_time + timedelta(seconds=75),
        "time_to_first_qualifying_move_seconds": 75,
        "qualifying_move_threshold_pct": 1.0,
        "threshold_scale_factor": 1.0,
        "pre_qualifying_mae_pct": 0.3,
        "qualifying_candle_order_ambiguous": True,
        "time_to_first_progress_seconds": 120,
        "time_to_mfe_seconds": 1800,
        "time_to_closest_target_seconds": 900,
        "time_to_target_seconds": 1200,
        "target_progress_ratio": 1.5,
        "target_reached": True,
        "path_samples": 240,
        "outcome_method_version": matrix.VERIFIED_OUTCOME_METHOD,
        "legacy_outcome_method_version": "canonical-spot-path-v1",
        "data_quality_status": matrix.VERIFIED_OUTCOME_QUALITY,
    }

    historical_price_rows = []
    historical_futures_rows = []
    historical_spot_rows = []
    for index in range(1, matrix.HISTORICAL_BASELINE_MIN_SAMPLES + 1):
        week = (index + 1) // 2
        anchor = event_time - timedelta(days=7 * week) + timedelta(
            hours=2 * (index % 2)
        )
        prior = anchor - timedelta(minutes=60)
        historical_price_rows.extend(
            [
                _price("BTC", prior, 100.0 + index, 1000.0 + index),
                _price("BTC", anchor, 101.0 + index, 1010.0 + index),
            ]
        )
        historical_futures_rows.extend(
            [
                _flow("BTC", prior, 10.0 * index, 5.0 * index),
                _flow("BTC", anchor, 10.0 * index + 5.0, 5.0 * index + 2.0),
            ]
        )
        historical_spot_rows.extend(
            [
                _flow("BTC", prior, 8.0 * index, 4.0 * index),
                _flow("BTC", anchor, 8.0 * index + 3.0, 4.0 * index + 1.0),
            ]
        )

    price_rows = historical_price_rows + [
        _price("BTC", reference_time, 100.0, 1000.0, "oi_price_history"),
        _price(
            "BTC",
            current_time,
            102.0,
            1100.0,
            "oi_regime_snapshots:coinglass",
        ),
        _price("BTC", future_time, 999.0, 9999.0),
    ]
    futures_rows = historical_futures_rows + [
        _flow("BTC", reference_time, 100.0, 10.0),
        _flow("BTC", current_time, 200.0, 30.0),
        _flow("BTC", future_time, 9999.0, 9999.0),
    ]
    spot_rows = historical_spot_rows + [
        _flow("BTC", reference_time, 50.0, 5.0),
        _flow("BTC", current_time, 90.0, 15.0),
        _flow("BTC", future_time, -9999.0, -9999.0),
    ]
    prior_events = [
        {
            "event_id": 70,
            "alert_time_utc": event_time - timedelta(minutes=20),
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "COMBINED_CONFIRMATION",
            "setup_key": "same-setup",
        },
        {
            "event_id": 71,
            "alert_time_utc": event_time - timedelta(minutes=10),
            "symbol": "ETH",
            "direction": "SHORT",
            "event_type": "MAX_PAIN_CONFIRMATION",
            "setup_key": "other",
        },
        {
            "event_id": 72,
            "alert_time_utc": event_time,
            "symbol": "SOL",
            "direction": "LONG",
            "event_type": "SAME_TIMESTAMP_MUST_NOT_COUNT",
            "setup_key": "other",
        },
        {
            "event_id": 73,
            "alert_time_utc": future_time,
            "symbol": "BTC",
            "direction": "LONG",
            "event_type": "FUTURE_MUST_NOT_COUNT",
            "setup_key": "same-setup",
        },
    ]

    rows = matrix.build_feature_rows(
        [event],
        price_oi_rows=price_rows,
        futures_rows=futures_rows,
        spot_rows=spot_rows,
        prior_events=prior_events,
        windows_minutes=(60, 240),
        max_pain_by_event_id={
            77: {
                "evaluation_status": "EVALUABLE",
                "available_at_utc": event_time - timedelta(minutes=5),
                "features": {
                    "max_pain.12h.short_target_signed_distance_pct": 2.5,
                    "max_pain.aggregate.short_long_liquidity_ratio": 1.8,
                },
            }
        },
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["feature_schema_version"] == matrix.FEATURE_SCHEMA_VERSION

    latest = row["raw_features"]["latest_at_or_before_alert"]
    assert latest["price_oi"]["price_close"] == 102.0
    assert latest["price_oi"]["oi_close_usd"] == 1100.0
    assert latest["price_oi"]["source"] == "oi_regime_snapshots:coinglass"
    assert latest["futures_cvd"]["continuous_cvd_usd"] == 200.0
    assert latest["spot_cvd"]["continuous_cvd_usd"] == 90.0

    one_hour = row["raw_features"]["windows"]["60m"]
    assert one_hour["price_change_pct"] == 2.0
    assert one_hour["oi_change_pct"] == 10.0
    assert one_hour["futures_continuous_cvd_change_usd"] == 100.0
    assert one_hour["spot_continuous_cvd_change_usd"] == 40.0
    assert one_hour["spot_futures_alignment"] == "ALIGNED"
    assert one_hour["price_oi_state"] == "PRICE_UP__OI_UP"
    assert one_hour["session_active_ratio"] == 0.0
    assert one_hour["session_weekend_ratio"] == 1.0
    assert one_hour["session_composition"] == "WEEKEND_ONLY"
    assert one_hour["complete"] is True
    assert row["raw_features"]["windows"]["240m"]["complete"] is False

    # The values from 12:01 are intentionally extreme; none may enter a
    # decision-time feature for the alert at 12:00.
    serialized_inputs = str(
        {
            "raw": row["raw_features"],
            "historical": row["historical_context"],
            "model": row["model_features"],
            "sequence": row["sequence_features"],
        }
    )
    assert "9999" not in serialized_inputs and "-9999" not in serialized_inputs

    model = row["model_features"]
    assert model["alert_score"] == 82.5
    assert model["snapshot_features"]["snapshot.component_scores.spot_cvd"] == 75.0
    assert (
        model["snapshot_features"]["snapshot.market_evidence.classification"]
        == "CORE_CONFIRMATION"
    )

    sequence = row["sequence_features"]["30m"]
    assert sequence["same_symbol_alerts"] == 1
    assert sequence["same_symbol_same_direction"] == 1
    assert sequence["same_setup_repetitions"] == 1
    assert sequence["market_alerts"] == 2
    assert sequence["market_distinct_symbols"] == 2
    assert sequence["market_direction_balance_pct"] == 0.0

    assert row["time_features"]["utc_hour"] == 12
    assert row["time_features"]["market_session"] == "WEEKEND"
    assert row["time_features"]["market_regime"] == "WEEKEND"
    assert row["time_features"]["market_local_hour"] == 8
    assert row["time_features"]["market_time_bucket"] == "ET_06_09_PRE_US"
    historical_60m = row["historical_context"]["windows"]["60m"]
    assert historical_60m["session_composition"] == "WEEKEND_ONLY"
    assert historical_60m["price_change_pct_history_samples"] >= 30
    assert historical_60m["price_change_pct_percentile_session_matched"] is not None
    assert (
        historical_60m["price_change_pct_session_matched_effective_samples"]
        >= 30
    )
    assert historical_60m["sufficient_history"] is True
    assert row["outcome_label"]["mfe_pct"] == 4.0
    assert row["outcome_label"]["path_success"] is True
    assert row["outcome_label"]["first_touch_status"] == "HIT"
    assert row["outcome_label"]["mae_pct"] == 0.3
    assert row["outcome_label"]["pre_qualifying_mae_pct"] == 0.3
    assert row["outcome_label"]["full_horizon_mae_pct"] == 9.5
    assert row["outcome_label"]["time_to_first_progress_seconds"] == 75
    assert row["outcome_label"]["qualifying_candle_order_ambiguous"] is True
    assert row["outcome_label"]["session_active_ratio"] == 0.0
    assert row["outcome_label"]["session_weekend_ratio"] == 1.0
    assert row["outcome_label"]["session_composition"] == "WEEKEND_ONLY"
    width_reference = row["outcome_label"]["movement_width_reference"]
    assert width_reference["floor_scale_factor"] == 1.0
    assert width_reference["threshold_scale_factor"] == 1.0
    assert width_reference["source_kind"] == "PRIOR_ONLY_SESSION_CALIBRATION"
    assert datetime.fromisoformat(
        str(width_reference["as_of_utc"]).replace("Z", "+00:00")
    ) == event_time - timedelta(minutes=1)
    assert "outcome_label" not in row["model_features"]
    assert row["max_pain_features"]["evaluation_status"] == "EVALUABLE"
    assert (
        row["max_pain_features"]["features"][
            "max_pain.12h.short_target_signed_distance_pct"
        ]
        == 2.5
    )

    prepared = matrix._prepare_series(price_rows, time_column="candle_time")
    prior, age = matrix._prior_point(prepared["BTC"], event_time)
    assert prior["price_close"] == 102.0 and age == 1.0
    missing, missing_age = matrix._prior_point(
        prepared["BTC"], event_time + timedelta(hours=2)
    )
    assert missing is None and missing_age is None

    print("research feature matrix self-test: OK")


if __name__ == "__main__":
    run()
