"""Four selected formula calculations over frozen, dated product observations.

No exchange, Telegram, SQL, scheduler or historical price reconstruction lives
here. The source adapter supplies the product's freshness deadlines and actual
receipt times. A planned entry price is never represented as an exchange fill.
The original specification is Selected_Six_Formulas_20261113.json; CT009's
six-symbol live scope is the user's separate 2026-11-14 clarification.
"""
from dataclasses import asdict, dataclass
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
import hashlib
import json
from math import fsum, isfinite


MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS
EPISODE_GAP_MS = 90 * MINUTE_MS
SFF1361 = "SFF1361_SOL_RECENT"
OCONF0034_13 = "OCONF0034_13"
CT009 = "CT009_FROZEN_3D"
XRP_LIQUIDITY_INVERSE = "XRP_LIQUIDITY_RAW_INVERSE"
HYPE_WATCH_INVERSE = "HYPE_WATCH_LIQ4050_24H_INVERSE"
HYPE_FORMULA_VERSION = "hype-watch-liq4050-24h-inverse-v1"
HYPE_SPEC_SOURCE_ID = "Formula_Activation_Handoff_20261115.json"
CT009_SYMBOLS = frozenset(("BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE"))
OCONF_SYMBOLS = CT009_SYMBOLS
FORMULA_VERSION = "selected-six-explicit-signals-v1"
SPEC_SOURCE_ID = "Selected_Six_Formulas_20261113.json"


@dataclass(frozen=True)
class FormulaDecision:
    formula_id: str
    formula_version: str
    spec_source_id: str
    event_id: str
    source_event_id: str
    source_event_ids: tuple
    symbol: str
    direction: str
    source_direction: str
    source_observed_at_ms: int
    source_received_at_ms: int
    data_available_at_ms: int
    decision_ms: int
    reference_at_ms: int
    reference_price: str
    source_target_price: str | None
    cancel_target_price: str | None
    entry_price: str
    stop_loss: str
    take_profit: str
    pending_entry_expires_at_ms: int | None
    holding_time_limit_ms: int | None
    holding_time_limit_specified: bool
    frozen_inputs_json: str

    def to_dict(self):
        """A detached serialization copy; the decision itself is immutable."""
        return asdict(self)


def _integer(value):
    if isinstance(value, bool):
        raise ValueError("invalid_timestamp")
    n = float(value)
    if not isfinite(n) or n != int(n) or n < 0:
        raise ValueError("invalid_timestamp")
    return int(n)


def _number(value):
    if isinstance(value, bool):
        raise ValueError("invalid_number")
    n = float(value)
    if not isfinite(n):
        raise ValueError("nonfinite_number")
    return n


def _price(value):
    n = _number(value)
    if n <= 0:
        raise ValueError("nonpositive_price")
    return n


def canonical_price(value):
    """Preserve the manager's existing 28-significant-digit decimal boundary."""
    if isinstance(value, bool):
        raise ValueError("invalid_price")
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError("invalid_price")
    with localcontext() as ctx:
        ctx.prec = 28
        ctx.rounding = ROUND_HALF_EVEN
        return format(+number, "f")


def _geometry(formula_id, reference, source_target=None):
    """Compute once in decimal; transport/notification use these exact strings."""
    with localcontext() as ctx:
        ctx.prec = 60
        p = Decimal(canonical_price(reference))
        original = None if source_target is None else Decimal(canonical_price(source_target))
        target = 2 * p - original if formula_id == XRP_LIQUIDITY_INVERSE else original
        if formula_id == HYPE_WATCH_INVERSE:
            entry, stop, take = p, p * Decimal('1.02'), p * Decimal('.98')
        elif formula_id == SFF1361:
            entry, stop, take = p, p * Decimal('1.01'), p * Decimal('.98')
        else:
            if not 0 < target < p:
                raise ValueError("invalid_target_geometry")
            if formula_id == OCONF0034_13:
                entry = p + Decimal('.2') * (p - target)
                stop, take = entry * Decimal('1.012'), target
            elif formula_id == CT009:
                entry = p + Decimal('0.1814560294151306') * (p - target)
                stop = entry * Decimal('1.0040077843998312')
                take = entry + Decimal('0.9365681871771814') * (target - entry)
            else:
                entry = p + Decimal('0.2342784544453025') * (p - target)
                stop = entry * Decimal('1.0105286625253104')
                take = entry + Decimal('0.8671878740191461') * (target - entry)
        return tuple(None if v is None else canonical_price(v)
                     for v in (entry, stop, take, p, original, target))


def _identity(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("missing_source_identity")
    return value


def _dated(record, now_ms):
    if not isinstance(record, Mapping):
        raise ValueError("invalid_dated_record")
    observed = _integer(record["observed_at_ms"])
    received = _integer(record["received_at_ms"])
    expires = _integer(record["valid_until_ms"])
    # An observation is usable through its declared deadline (inclusive).
    if not observed <= received <= now_ms <= expires:
        raise ValueError("unavailable_or_stale_input")
    return observed, received, expires


def _context(snapshot, now_ms, symbols, direction):
    now = _integer(now_ms)
    _identity(snapshot["source_event_id"])
    if snapshot["symbol"] not in symbols:
        raise ValueError("symbol_not_allowed")
    if snapshot["direction"] != direction:
        raise ValueError("wrong_source_direction")
    _dated(snapshot, now)
    return now


def _input(snapshot, name, now_ms):
    row = snapshot["inputs"][name]
    _dated(row, now_ms)
    if row.get("complete") is not True:
        raise ValueError("incomplete_input:" + name)
    if _integer(row["received_at_ms"]) > _integer(snapshot["received_at_ms"]):
        raise ValueError("input_received_after_snapshot")
    return row["value"]


def _available(snapshot, names):
    return max([_integer(snapshot["received_at_ms"])] +
               [_integer(snapshot["inputs"][name]["received_at_ms"]) for name in names])


def next_full_minute_ms(available_at_ms):
    t = _integer(available_at_ms)
    return (t // MINUTE_MS + 1) * MINUTE_MS


def _reference(snapshot, now_ms, available_ms, next_minute):
    ref = snapshot["reference"]
    observed, received, _ = _dated(ref, now_ms)
    if ref.get("complete") is not True or ref.get("venue") != "HYPERLIQUID_PERPETUAL":
        raise ValueError("unverified_execution_reference")
    if next_minute:
        if observed != next_full_minute_ms(available_ms):
            raise ValueError("wrong_next_minute_reference")
    elif observed < available_ms:
        raise ValueError("reference_precedes_opportunity")
    if received < available_ms:
        raise ValueError("reference_precedes_availability")
    return canonical_price(ref["value"]), observed


def _result(formula_id, signal=False, reason="no_match", decision=None, valid=True):
    return {"formula_id": formula_id, "valid": valid, "signal": signal,
            "reason": reason, "decision": decision}


def _invalid(formula_id, exc):
    return _result(formula_id, reason="invalid_input:" + str(exc), valid=False)


def _decision(formula_id, snapshot, now, names, used_sources, entry, stop, take,
              reference_price, reference_ms, source_target=None, cancel_target=None):
    if not 0 < Decimal(take) < Decimal(entry) < Decimal(stop):
        raise ValueError("invalid_short_geometry")
    available = _available(snapshot, names)
    pending = (reference_ms + MINUTE_MS if formula_id == HYPE_WATCH_INVERSE
               else None if formula_id == SFF1361 else reference_ms + DAY_MS)
    version = HYPE_FORMULA_VERSION if formula_id == HYPE_WATCH_INVERSE else FORMULA_VERSION
    spec = HYPE_SPEC_SOURCE_ID if formula_id == HYPE_WATCH_INVERSE else SPEC_SOURCE_ID
    # The admission TTL is deliberately absent: it belongs to the existing
    # transport/manager contract, not this 24-hour pending-order lifetime.
    identity = json.dumps([version, formula_id, snapshot["symbol"],
                           snapshot["source_event_id"]], separators=(",", ":"))
    frozen = {"source": {k: snapshot[k] for k in
                         ("source_event_id", "symbol", "direction", "observed_at_ms",
                          "received_at_ms", "valid_until_ms")},
              "inputs": {k: snapshot["inputs"][k] for k in names},
              "reference": snapshot["reference"]}
    if formula_id == HYPE_WATCH_INVERSE:
        frozen['capture_identity'] = snapshot['capture_identity']
        frozen['capture_guard_evidence'] = snapshot['capture_guard_evidence']
    return FormulaDecision(
        formula_id, version, spec,
        hashlib.sha256(identity.encode()).hexdigest(), snapshot["source_event_id"],
        tuple(sorted(set(used_sources) | {snapshot["source_event_id"]})),
        snapshot["symbol"], "SHORT", snapshot["direction"],
        _integer(snapshot["observed_at_ms"]), _integer(snapshot["received_at_ms"]),
        available, now, reference_ms, reference_price, source_target, cancel_target,
        entry, stop, take, pending, DAY_MS if formula_id == HYPE_WATCH_INVERSE else None,
        formula_id != SFF1361,
        json.dumps(frozen, sort_keys=True, separators=(",", ":"), allow_nan=False))


def evaluate_hype_watch_inverse(snapshot, now_ms):
    """Fixed new identity and geometry; verified capture and wave are inputs.

    A plan uses the captured next-minute OPEN, never a claimed venue fill.
    A live attempt must fit that original minute; late data cannot renew it.
    """
    names = ('capture_guard_valid', 'raw_selected_side', 'source_base_direction',
             'selected_share_pct', 'btc_parent_movement_id', 'timeframe')
    try:
        now = _context(snapshot, now_ms, {'HYPE'}, 'LONG')
        values = {k: _input(snapshot, k, now) for k in names}
        if values['capture_guard_valid'] is not True:
            raise ValueError('unverified_watch_capture')
        _identity(values['btc_parent_movement_id'])
        if (values['raw_selected_side'] != 'SHORT'
                or values['source_base_direction'] != 'LONG' or values['timeframe'] != '24h'):
            return _result(HYPE_WATCH_INVERSE, reason='not_selected_hype_upper_target')
        share = _number(values['selected_share_pct'])
        if not 40 <= share < 50:
            return _result(HYPE_WATCH_INVERSE, reason='liquidity_share_outside_40_50')
        available = _available(snapshot, names)
        if now >= next_full_minute_ms(available) + MINUTE_MS:
            return _result(HYPE_WATCH_INVERSE, reason='original_entry_minute_expired')
        p, ref_ms = _reference(snapshot, now, available, True)
        entry, stop, take, p, _, _ = _geometry(HYPE_WATCH_INVERSE, p)
        decision = _decision(HYPE_WATCH_INVERSE, snapshot, now, names,
            (values['btc_parent_movement_id'],), entry, stop, take, p, ref_ms)
        return _result(HYPE_WATCH_INVERSE, True, 'match', decision)
    except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
        return _invalid(HYPE_WATCH_INVERSE, exc)


def evaluate_sff1361(snapshot, now_ms):
    """Use existing complete 30m family scores and the source's 1h change."""
    names = ("price_oi_30m_short_score", "futures_cvd_30m_short_score",
             "one_hour_price_change_fraction", "price_history_complete_24h")
    try:
        now = _context(snapshot, now_ms, {"SOL"}, "SHORT")
        oi, futures, change = [_number(_input(snapshot, key, now)) for key in names[:3]]
        if _input(snapshot, names[3], now) is not True:
            raise ValueError("incomplete_24h_price_history")
        if not (oi >= 40 and futures >= 40 and -.005 <= change <= .005):
            return _result(SFF1361)
        p, ref_ms = _reference(snapshot, now, _available(snapshot, names), True)
        entry, stop, take, p, _, _ = _geometry(SFF1361, p)
        d = _decision(SFF1361, snapshot, now, names, (), entry, stop, take, p, ref_ms)
        return _result(SFF1361, True, "match", d)
    except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
        return _invalid(SFF1361, exc)


def _historical_row(row, current, now):
    """Validate original availability, not present-day freshness of history."""
    observed = _integer(row["observed_at_ms"])
    received = _integer(row["received_at_ms"])
    current_observed = _integer(current["observed_at_ms"])
    if row.get("symbol") != current["symbol"] or row.get("direction") != current["direction"]:
        return None
    if not current_observed - DAY_MS <= observed < current_observed:
        return None
    if received > _integer(current["received_at_ms"]) or received > now:
        return None
    _identity(row["source_event_id"])
    _dated(row, received)
    return received


def evaluate_oconf0034_13(snapshot, prior_confirmations, now_ms):
    names = ("confirmation_type", "confirmation_confirmed", "target_price",
             "oi_now_short_score", "oi_short_short_score", "futures_now_short_score",
             "futures_short_short_score", "spot_now_short_score", "spot_short_short_score")
    try:
        now = _context(snapshot, now_ms, OCONF_SYMBOLS, "SHORT")
        values = {k: _input(snapshot, k, now) for k in names}
        target = _price(values["target_price"])
        if values["confirmation_type"] != "MP" or values["confirmation_confirmed"] is not True:
            return _result(OCONF0034_13)
        fast = [(.35 * _number(values[a]) + .30 * _number(values[b])) / .65
                for a, b in ((names[3], names[4]), (names[5], names[6]), (names[7], names[8]))]
        if not (fast[0] >= 40 and fast[1] >= 40 and fast[2] >= 0):
            return _result(OCONF0034_13)
        matches = {}
        for row in prior_confirmations:
            at = _historical_row(row, snapshot, now)
            if at is None or row["source_event_id"] == snapshot["source_event_id"]:
                continue
            if (_input(row, "confirmation_type", at) != "MP" or
                    _input(row, "confirmation_confirmed", at) is not True):
                continue
            previous = _price(_input(row, "target_price", at))
            if max(previous, target) / min(previous, target) <= 1.004:
                # Several timeframes in the same decision snapshot count once.
                matches.setdefault(_integer(row["observed_at_ms"]), row["source_event_id"])
        if not matches:
            return _result(OCONF0034_13, reason="insufficient_distinct_confirmations")
        p, ref_ms = _reference(snapshot, now, _available(snapshot, names), False)
        if Decimal(canonical_price(values["target_price"])) >= Decimal(p):
            return _result(OCONF0034_13, reason="target_not_below_reference")
        entry, stop, take, p, original, cancel = _geometry(OCONF0034_13, p, values["target_price"])
        d = _decision(OCONF0034_13, snapshot, now, names, matches.values(),
                      entry, stop, take, p, ref_ms, original, cancel)
        return _result(OCONF0034_13, True, "match", d)
    except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
        return _invalid(OCONF0034_13, exc)


def evaluate_ct009(snapshot, prior_delivered_alerts, now_ms):
    names = ("ordinary_alert_delivered", "timeframe", "target_price")
    try:
        now = _context(snapshot, now_ms, CT009_SYMBOLS, "SHORT")
        values = {k: _input(snapshot, k, now) for k in names}
        target = _price(values["target_price"])
        if values["ordinary_alert_delivered"] is not True or values["timeframe"] not in ("48h", "3d"):
            return _result(CT009)
        scores = {}
        for row in prior_delivered_alerts:
            at = _historical_row(row, snapshot, now)
            if at is None or row["source_event_id"] == snapshot["source_event_id"]:
                continue
            if (_input(row, "ordinary_alert_delivered", at) is not True or
                    _input(row, "timeframe", at) != values["timeframe"]):
                continue
            previous = _price(_input(row, "target_price", at))
            if max(previous, target) / min(previous, target) <= 1.004 + 1e-12:
                score = _number(_input(row, "maxpain_score", at))
                if row["source_event_id"] in scores and scores[row["source_event_id"]] != score:
                    raise ValueError("conflicting_prior_alert_identity")
                scores[row["source_event_id"]] = score
        if not scores or fsum(scores.values()) / len(scores) < 72.8:
            return _result(CT009, reason="previous_alert_mean_below_threshold")
        p, ref_ms = _reference(snapshot, now, _available(snapshot, names), True)
        if Decimal(canonical_price(values["target_price"])) >= Decimal(p):
            return _result(CT009, reason="target_not_below_reference")
        entry, stop, take, p, original, cancel = _geometry(CT009, p, values["target_price"])
        d = _decision(CT009, snapshot, now, names, scores,
                      entry, stop, take, p, ref_ms, original, cancel)
        return _result(CT009, True, "match", d)
    except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
        return _invalid(CT009, exc)


def evaluate_xrp_liquidity_inverse(snapshot, now_ms):
    names = ("source_state", "target_price", "supporting_liquidity")
    try:
        now = _context(snapshot, now_ms, {"XRP"}, "LONG")
        values = {k: _input(snapshot, k, now) for k in names}
        if values["source_state"] != "COMBINED_QUALIFIED":
            return _result(XRP_LIQUIDITY_INVERSE)
        liquidities = values["supporting_liquidity"]
        if not isinstance(liquidities, (list, tuple)):
            raise ValueError("invalid_supporting_liquidity")
        supported = False
        for row in liquidities:
            if not isinstance(row, Mapping):
                raise ValueError("invalid_liquidity_row")
            if row.get("supports_source") is not True:
                continue
            _identity(row["timeframe"])
            upper, lower = _number(row["upper_liquidation"]), _number(row["lower_liquidation"])
            if upper < 0 or lower < 0:
                raise ValueError("negative_liquidation")
            if upper + lower > 0 and upper / (upper + lower) >= .6:
                supported = True
        if not supported:
            return _result(XRP_LIQUIDITY_INVERSE, reason="liquidity_share_below_threshold")
        p, ref_ms = _reference(snapshot, now, _available(snapshot, names), True)
        original_target = Decimal(canonical_price(values["target_price"]))
        with localcontext() as ctx:
            ctx.prec = 60
            target = 2 * Decimal(p) - original_target
        if not 0 < target < Decimal(p) < original_target:
            return _result(XRP_LIQUIDITY_INVERSE, reason="invalid_reflected_target")
        entry, stop, take, p, original, cancel = _geometry(XRP_LIQUIDITY_INVERSE, p, values["target_price"])
        d = _decision(XRP_LIQUIDITY_INVERSE, snapshot, now, names, (),
                      entry, stop, take, p, ref_ms, original, cancel)
        return _result(XRP_LIQUIDITY_INVERSE, True, "match", d)
    except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
        return _invalid(XRP_LIQUIDITY_INVERSE, exc)


def target_episode_continues(targets, new_target, previous_seen_ms, current_seen_ms,
                             max_gap_ms=None):
    """Cumulative target range, never pairwise chaining across 0.4%."""
    prices = [_price(v) for v in targets] + [_price(new_target)]
    previous, current = _integer(previous_seen_ms), _integer(current_seen_ms)
    if current < previous:
        return False
    if max_gap_ms is not None and current - previous > _integer(max_gap_ms):
        return False
    return max(prices) / min(prices) <= 1.004


def pending_cancel_reason(decision, now_ms, *, fully_filled=False,
                          observed_price=None, observed_at_ms=None):
    """A cancellation reason only, never a cancellation or a fill assertion.

    A partial fill still permits cancellation of its unfilled remainder by the
    existing manager; this helper does not change exits or filled quantities.
    Target observations must be contemporaneous actual prices, not OHLC order
    guesses. Unknown entry outcomes still require the manager's reconciliation.
    """
    now = _integer(now_ms)
    if fully_filled:
        return None
    if observed_price is not None:
        at = _integer(observed_at_ms)
        if not decision.reference_at_ms <= at <= now:
            raise ValueError("invalid_cancel_observation_time")
        if decision.cancel_target_price is not None and Decimal(canonical_price(observed_price)) <= Decimal(decision.cancel_target_price):
            return "ORIGINAL_TARGET_REACHED_BEFORE_FILL"
    if decision.pending_entry_expires_at_ms is not None and now >= decision.pending_entry_expires_at_ms:
        return "PENDING_ENTRY_EXPIRED"
    return None
