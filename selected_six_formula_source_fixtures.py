"""Provider-shaped inputs passed through real Watch calculation/capture code."""
import ast
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path

import alert_engine
import market_confidence_engine
import magnet_v1
import research_watch_score_capture as scores
import research_watch_decision_capture as capture


def watch_capture(at_ms, cycle='actual-watch-fixture'):
    observed = datetime.fromtimestamp(at_ms/1000, timezone.utc)
    stamp = observed.isoformat()
    rows, snapshot = [], {}
    for symbol in scores.SYMBOLS:
        bullish = symbol == 'XRP'
        for index, tf in enumerate(alert_engine.TIMEFRAMES):
            rows.append(dict(symbol=symbol, timeframe=tf, rank=1, current_price=100.,
                short_max_pain=102. if bullish else 120., long_max_pain=80. if bullish else 98.,
                distance_short_pct=2. if bullish else 20., distance_long_pct=-20. if bullish else -2.,
                short_liquidation_amount=2_000_000*2**index if bullish else 100_000,
                long_liquidation_amount=100_000 if bullish else 2_000_000*2**index,
                source_observed_at_utc=stamp, price_fetched_at_utc=stamp,
                price_source='BINANCE_SPOT', price_pair=symbol+'USDT'))
        windows, flow_windows = {}, {}
        for name, minutes in [('30m',30),('1h',60),('4h',240),('12h',720),('24h',1440),
                              ('48h',2880),('72h',4320),('7d',10080)]:
            window = dict(available=True,
                reference_time=(observed-timedelta(minutes=minutes)).isoformat())
            windows[name] = dict(window, state='BULLISH_BUILDUP' if bullish else 'BEARISH_BUILDUP',
                price_change_pct=0.1, price_strength=dict(continuous_strength=.9),
                oi_strength=dict(continuous_strength=.9))
            flow_windows[name] = dict(window, latest_time=stamp,
                direction='BULLISH' if bullish else 'BEARISH', continuous_strength=.9)
        flow = dict(available=True, windows=flow_windows, quality=dict(status='PASS',
            freshness_status='FRESH', usable_for_confirmation=True, candle_close=stamp,
            max_age_minutes=90, age_minutes=0, missing_30m_intervals=0))
        snapshot[symbol] = dict(regime=dict(available=True, windows=windows, price=100.,
            open_interest_usd=1_000_000, price_fetched_at=stamp, oi_fetched_at=stamp,
            data_quality_status='PASS', price_source='BINANCE_SPOT', oi_source='COINGLASS'),
            flow=dict(futures=deepcopy(flow), spot=deepcopy(flow)),
            timing_observation=dict(cvd_observed_at_utc=stamp))
    items, frozen, evidence = scores.prepare(rows, snapshot)
    score = scores.build_bundle(cycle_id=cycle, rows=rows, snapshot=snapshot, frozen=frozen,
        evidence=evidence, computed_at_utc=observed, watch_threshold=65.)
    # Extract the existing pure Watch builders; this avoids application startup
    # but executes their actual code with actual market/magnet modules.
    root = Path(scores.__file__).parent
    names = {'_combined_group_key','_magnet_alert_side','_combined_magnet_confirmations',
             '_combined_confirmation_candidates'}
    parsed = ast.parse((root/'main.py').read_text())
    nodes = [x for x in parsed.body if isinstance(x, ast.FunctionDef) and x.name in names]
    scope = dict(defaultdict=defaultdict, TIMEFRAMES=alert_engine.TIMEFRAMES,
        COMBINED_MIN_SIGNALS=2, COMBINED_HIGH_SCORE_THRESHOLD=80., DERIVATIVES_HIGH_THRESHOLD=65.,
        magnet_v1=magnet_v1, market_confidence_engine=market_confidence_engine)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(root/'main.py'), 'exec'), scope)
    groups, magnets = [], []
    combined = scope['_combined_confirmation_candidates'](items, rows,
        capture_groups=groups, capture_magnet_evaluations=magnets)
    decision = capture.build_bundle(cycle_id=cycle, score_bundle=score, prepared_items=items,
        displayable_items=items, combined_candidates=combined, combined_groups=groups,
        magnet_evaluations=magnets, computed_at_utc=observed, top8_only=True, general_enabled=True)
    return score, decision, snapshot
