"""Frozen U21 canonical XRP SHORT predicate and one-minute outcome contract.

No I/O, no orders, no fitted thresholds. Binance Spot minute OHLC uses open-time
milliseconds. Only bars strictly before the decision enter features. The next
minute OPEN is a separate execution observation; its future HIGH/LOW/CLOSE never
participate in the predicate. Calendar is the audited 2025/2026 research calendar;
unknown years fail closed instead of guessing future exchange holidays.
"""
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from math import isfinite
from zoneinfo import ZoneInfo

MINUTE_MS = 60000
DAY_MS = 86400000
DECISION_STEP_MS = 15 * MINUTE_MS
FORMULA_ID = "U21_canonical_continuous"
FORMULA_VERSION = "u21-xrp-canonical-cap1-v1"
LOCATION_LOWER = 0.4122106385646779
LOCATION_UPPER = 0.8393408289241626
DISTANCE_LOWER = -0.017415876858352997
DISTANCE_UPPER = -0.004158176397388663
SUPPORTED_CALENDAR_YEARS = frozenset((2025, 2026))
_HOLIDAYS = frozenset((
    "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27",
    "2025-12-25", "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26",
    "2026-12-25",
))
_EARLY_CLOSE = frozenset(("2025-07-03", "2025-11-28", "2025-12-24", "2026-11-27", "2026-12-24"))
_NY = ZoneInfo("America/New_York")


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
    """Most recent 15-minute boundary; features end one minute before it."""
    value = _ms(now_ms)
    return value - value % DECISION_STEP_MS


def previous_ny_regular_session(decision_ms):
    """Latest fully completed [09:30, actual close) NY regular session."""
    decision = _ms(decision_ms)
    local = datetime.fromtimestamp(decision / 1000, timezone.utc).astimezone(_NY)
    day = local.date()
    for _ in range(20):
        if day.year not in SUPPORTED_CALENDAR_YEARS:
            raise ValueError("Unsupported NYSE calendar year")
        key = day.isoformat()
        if day.weekday() < 5 and key not in _HOLIDAYS:
            end_hour = 13 if key in _EARLY_CLOSE else 16
            start = int(datetime(day.year, day.month, day.day, 9, 30, tzinfo=_NY).timestamp() * 1000)
            end = int(datetime(day.year, day.month, day.day, end_hour, tzinfo=_NY).timestamp() * 1000)
            if end <= decision:
                return start, end
        day -= timedelta(days=1)
    raise ValueError("No known completed NY session")


def required_history_start_ms(decision_ms):
    """XRP up to 14 days; BTC seven days. Both end at decision, exclusive."""
    d = _ms(decision_ms)
    if d % DECISION_STEP_MS:
        raise ValueError("Decision is not on the UTC 15-minute grid")
    day = datetime.fromtimestamp(d / 1000, timezone.utc).date()
    monday = day - timedelta(days=day.weekday())
    prior_monday = int(datetime.combine(monday - timedelta(days=7), datetime.min.time(), timezone.utc).timestamp() * 1000)
    session_start, _ = previous_ny_regular_session(d)
    return {"XRP": min(d - 7 * DAY_MS, prior_monday, session_start), "BTC": d - 7 * DAY_MS}


def _normalize(rows, start_ms, end_ms):
    """Check chronological raw Binance rows; future rows are ignored entirely."""
    result = []
    last = None
    for raw in rows:
        t = _ms(raw[0])
        if not start_ms <= t < end_ms:
            continue
        if t % MINUTE_MS or (last is not None and t <= last):
            raise ValueError("Duplicate, unordered or unaligned minute")
        o, h, l, c = (_positive(raw[i]) for i in range(1, 5))
        if h < max(o, c) or l > min(o, c) or h < l:
            raise ValueError("Invalid OHLC")
        result.append((t, o, h, l, c))
        last = t
    return result


def _complete(rows, start_ms, end_ms):
    if len(rows) != (end_ms - start_ms) // MINUTE_MS:
        return False
    return all(row[0] == start_ms + i * MINUTE_MS for i, row in enumerate(rows))


def predicate_values(last_closed_price, session_high, session_low):
    """Exact double-precision research expression, including boundary inequalities."""
    close, high, low = map(_positive, (last_closed_price, session_high, session_low))
    if high <= low:
        raise ValueError("Flat prior NY session")
    location = (close - low) / (high - low)
    distance = close / high - 1.0
    return {
        "location": location,
        "distance": distance,
        "signal": LOCATION_LOWER < location <= LOCATION_UPPER and DISTANCE_LOWER < distance <= DISTANCE_UPPER,
    }


def evaluate_signal(xrp_bars, btc_bars, decision_ms):
    """Return valid/signal/reason plus auditable features. Never silently fill gaps.

    Unlike the archived array's execution-availability gate, this predicate does
    not require D+1 data before D. The worker must separately observe D+1 OPEN
    before accepting a simulated position, and must not substitute a later price.
    """
    result = {"formula_id": FORMULA_ID, "version": FORMULA_VERSION, "valid": False, "signal": False}
    try:
        d = _ms(decision_ms)
        starts = required_history_start_ms(d)
        result.update(decision_ms=d, entry_ms=d + MINUTE_MS)
        x = _normalize(xrp_bars, starts["XRP"], d)
        b = _normalize(btc_bars, starts["BTC"], d)
        if not _complete(x, starts["XRP"], d) or not _complete(b, starts["BTC"], d):
            result["reason"] = "missing_or_noncontiguous_history"
            return result
        # Frozen common coverage inherited from the six-group research universe.
        # These are validity gates, not directional BTC or indicator conditions.
        for width in (5, 15, 30, 60, 240, 4320):
            segment = x[-width:]
            if max(v[2] for v in segment) <= min(v[3] for v in segment):
                result["reason"] = "flat_common_window_" + str(width)
                return result
        ny_start, ny_end = previous_ny_regular_session(d)
        times = [v[0] for v in x]
        a, z = bisect_left(times, ny_start), bisect_left(times, ny_end)
        session = x[a:z]
        if not _complete(session, ny_start, ny_end):
            result["reason"] = "incomplete_prior_ny_session"
            return result
        high, low = max(v[2] for v in session), min(v[3] for v in session)
        values = predicate_values(x[-1][4], high, low)
        result.update(values, valid=True, reason="match" if values["signal"] else "no_match",
                      last_closed_price=x[-1][4], last_closed_minute_ms=d - MINUTE_MS,
                      prior_ny_start_ms=ny_start, prior_ny_end_ms=ny_end,
                      prior_ny_high=high, prior_ny_low=low)
        return result
    except (ValueError, TypeError, IndexError, KeyError, OverflowError) as exc:
        result["reason"] = "invalid_input: " + str(exc)
        return result


def build_levels(entry_price):
    entry = _positive(entry_price)
    return {"direction": "SHORT", "entry_price": entry, "stop_loss": entry * 1.005,
            "take_profit": entry * 0.92, "risk_fraction": 0.005, "take_fraction": 0.08, "reward_risk": 16.0}


def nonoverlap_allows(decision_ms, release_ms=None, active=False):
    """A release exactly at D blocks D. Unknown/open/ambiguous remains active."""
    if active:
        return False
    return release_ms is None or _ms(decision_ms) > _ms(release_ms)


def first_touch(entry_price, bars, entry_ms, observed_until_ms):
    """Static SHORT outcome from fully completed contiguous minute candles.

    Gap beyond stop fills at OPEN. Gap beyond take fills at fixed take. Both
    barriers inside one candle is AMBIGUOUS, reserves capacity, never SL-first.
    Closed release/exit timestamp is touched minute OPEN+60s (research parity).
    """
    levels = build_levels(entry_price)
    entry = levels["entry_price"]; stop = levels["stop_loss"]; take = levels["take_profit"]
    start = _ms(entry_ms); end = _ms(observed_until_ms)
    if start % MINUTE_MS or end % MINUTE_MS or end < start:
        raise ValueError("Outcome times must be aligned completed-minute boundaries")
    base = {"status": "OPEN", "status_code": 3, "exit_ms": None, "release_ms": None, "exit_price": None, "R": None, "reason_code": 0, "last_checked_ms": start - MINUTE_MS}
    try:
        values = _normalize(bars, start, end)
    except (ValueError, TypeError, IndexError) as exc:
        return dict(base, status="UNKNOWN", status_code=5, reason_code=2, reason=str(exc))
    expected = start
    for t, o, h, l, _ in values:
        if t != expected:
            return dict(base, status="UNKNOWN", status_code=5, reason_code=2, reason="missing_minute")
        base["last_checked_ms"] = t
        if o >= stop:
            return dict(base, status="SL", status_code=2, exit_ms=t+MINUTE_MS, release_ms=t+MINUTE_MS, exit_price=o, R=(entry-o)/(stop-entry), reason_code=10)
        if o <= take:
            return dict(base, status="TP", status_code=1, exit_ms=t+MINUTE_MS, release_ms=t+MINUTE_MS, exit_price=take, R=(entry-take)/(stop-entry), reason_code=11)
        if h >= stop and l <= take:
            return dict(base, status="AMBIGUOUS", status_code=4, reason_code=3)
        if l <= take:
            return dict(base, status="TP", status_code=1, exit_ms=t+MINUTE_MS, release_ms=t+MINUTE_MS, exit_price=take, R=(entry-take)/(stop-entry))
        if h >= stop:
            return dict(base, status="SL", status_code=2, exit_ms=t+MINUTE_MS, release_ms=t+MINUTE_MS, exit_price=stop, R=-1.0)
        expected = t + MINUTE_MS
    if expected != end:
        return dict(base, status="UNKNOWN", status_code=5, reason_code=2, reason="missing_minute")
    return base
