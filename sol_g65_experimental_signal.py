"""Frozen SOL g65/k49 BTC12h short with original 75%/25% take-path lock.

Source: SOL_g65_Stop_Profile_20261004.json, rules and archived reference
geometry. Closed Binance Spot TRADE minute prices; no MaxPain/CVD/OI gates.
"""
from datetime import datetime
from math import isfinite

MINUTE_MS = 60000
HOUR_MS = 60 * MINUTE_MS
DECISION_STEP_MS = 30 * MINUTE_MS
RULE_ID = 'SOL_G65_K49_PROFIT_LOCK'
FORMULA_ID = 'btcgrid_SOL_SHORT_g65_k049__cap1__LOCK_075TP_025TP'
FORMULA_VERSION = 'sol-g65-k49-reference-limit-lock075tp025tp-v1'


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


def last_closed_decision_ms(now_ms):
    value = _ms(now_ms)
    return value - value % DECISION_STEP_MS


def required_history_start_ms(decision_ms):
    d = _ms(decision_ms)
    if d % DECISION_STEP_MS:
        raise ValueError('Decision must be UTC half-hour aligned')
    return {'BTC': d - 12 * HOUR_MS}


def predicate_values(btc_return_12h):
    return -.02 < btc_return_12h <= -.01


def evaluate_signal(btc_bars, decision_ms):
    result = {'formula_id': FORMULA_ID, 'version': FORMULA_VERSION, 'valid': False,
              'signal': False, 'price_source': 'BINANCE_SPOT_TRADE_1M'}
    try:
        d = _ms(decision_ms)
        start = required_history_start_ms(d)['BTC']
        rows = _normalize(btc_bars, start, d)
        ret = rows[-1][4] / rows[0][1] - 1.0
        matched = predicate_values(ret)
        result.update(valid=True, signal=matched, reason='match' if matched else 'no_match',
                      btc_return_12h=ret, decision_ms=d, reference_ms=d+MINUTE_MS,
                      last_closed_minute_ms=d-MINUTE_MS)
    except (ValueError, TypeError, IndexError, KeyError, OverflowError) as exc:
        result['reason'] = 'invalid_input: ' + str(exc)
    return result


def build_levels(reference_price):
    ref = _positive(reference_price)
    entry, stop, take = ref * 1.005, ref * 1.0125, ref * .9825
    take_distance = entry - take
    if not 0 < take < entry < stop or take_distance <= 0:
        raise ValueError('Invalid SOL g65 representable geometry')
    return {'direction': 'SHORT', 'reference_price': ref, 'entry_price': entry,
            'stop_loss': stop, 'take_profit': take, 'pending_cancel_price': ref*.99,
            'original_take_distance': take_distance,
            'lock_trigger_price': entry - .75*take_distance,
            'locked_stop_loss': entry - .25*take_distance,
            'reward_risk': take_distance/(stop-entry)}
