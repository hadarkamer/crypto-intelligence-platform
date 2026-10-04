"""Frozen ROW71205 HYPE SHORT: completed Binance Futures TRADE / BTC Spot 1m.

Only closed minutes strictly before each UTC half-hour decision enter the
features. D+1 minute OPEN is a separate execution reference, never a feature.
This is the original cap-one static 0.5% stop / 2% take rule, with no time exit.
"""
from datetime import datetime
from math import isfinite

MINUTE_MS = 60000
HOUR_MS = 60 * MINUTE_MS
DECISION_STEP_MS = 30 * MINUTE_MS
FORMULA_ID = "DOGE_GRAMMAR_HYPE_F3_ROW71205_FULL_SOURCE_HISTORY_RETROSPECTIVE"
FORMULA_VERSION = "hype-row71205-short-sl005-tp02-cap1-v1"
RANGE_24H_MAX_PCT = 5.1000000000000005


def _ms(value):
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp must be timezone-aware")
        value = value.timestamp() * 1000
    if isinstance(value, bool):
        raise ValueError("Invalid timestamp")
    number = float(value)
    if not isfinite(number) or number != int(number):
        raise ValueError("Timestamp must be integral milliseconds")
    return int(number)


def _positive(value):
    if isinstance(value, bool):
        raise ValueError("Invalid price")
    value = float(value)
    if not isfinite(value) or value <= 0:
        raise ValueError("Invalid price")
    return value


def last_closed_decision_ms(now_ms):
    value = _ms(now_ms)
    return value - value % DECISION_STEP_MS


def required_history_start_ms(decision_ms):
    d = _ms(decision_ms)
    if d % DECISION_STEP_MS:
        raise ValueError("Decision is not on the UTC 30-minute grid")
    return {"HYPE": d - 24 * HOUR_MS, "BTC": d - 12 * HOUR_MS}


def _normalize(rows, start, end):
    result = []
    for raw in rows:
        t = _ms(raw[0])
        if not start <= t < end:
            continue
        if t != start + len(result) * MINUTE_MS:
            raise ValueError("Missing, duplicate, unordered or unaligned minute")
        o, h, l, c = (_positive(raw[i]) for i in range(1, 5))
        if h < max(o, c) or l > min(o, c) or h < l:
            raise ValueError("Invalid OHLC")
        result.append((t, o, h, l, c))
    if len(result) != (end-start) // MINUTE_MS:
        raise ValueError("Incomplete closed minute history")
    return result


def predicate_values(btc_return_12h_pct, hype_range_24h_pct,
                     range_4h_to_24h, relative_return_4h):
    """Preserve the exact archived double expressions and strict boundaries."""
    values = (btc_return_12h_pct, hype_range_24h_pct, range_4h_to_24h, relative_return_4h)
    if not all(isfinite(v) for v in values):
        raise ValueError("Nonfinite feature")
    return (0 < btc_return_12h_pct < .25 and
            0 < hype_range_24h_pct <= RANGE_24H_MAX_PCT and
            range_4h_to_24h > .5 and relative_return_4h <= 0)


def evaluate_signal(hype_bars, btc_bars, decision_ms):
    result = {"formula_id": FORMULA_ID, "version": FORMULA_VERSION,
              "valid": False, "signal": False,
              "hype_price_source": "BINANCE_USDM_FUTURES_TRADE_1M",
              "btc_price_source": "BINANCE_SPOT_1M"}
    try:
        d = _ms(decision_ms)
        starts = required_history_start_ms(d)
        a = _normalize(hype_bars, starts["HYPE"], d)
        b = _normalize(btc_bars, starts["BTC"], d)
        def ret(rows, hours):
            return rows[-1][4] / rows[-hours * 60][1] - 1.0
        def price_range(hours):
            segment = a[-hours * 60:]
            return 100 * (max(r[2] for r in segment) - min(r[3] for r in segment)) / segment[0][1]
        r24, r4 = price_range(24), price_range(4)
        if r24 <= 0:
            result["reason"] = "flat_24h_range"
            return result
        values = {"btc_return_12h_pct": 100 * ret(b, 12),
                  "hype_range_24h_pct": r24, "range_4h_to_24h": r4 / r24,
                  "relative_return_4h": ret(a, 4) - ret(b, 4)}
        matched = predicate_values(**values)
        result.update(values, valid=True, signal=matched,
                      reason="match" if matched else "no_match",
                      decision_ms=d, entry_ms=d + MINUTE_MS,
                      last_closed_minute_ms=d-MINUTE_MS)
        return result
    except (ValueError, TypeError, IndexError, KeyError, OverflowError) as exc:
        result["reason"] = "invalid_input: " + str(exc)
        return result


def build_levels(entry_price):
    entry = _positive(entry_price)
    return {"direction": "SHORT", "entry_price": entry,
            "stop_loss": entry * 1.005, "take_profit": entry * .98,
            "risk_fraction": .005, "take_fraction": .02, "reward_risk": 4.0}
