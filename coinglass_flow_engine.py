"""Stage 88: read-only Futures and Spot CVD Flow analytics.

This module reads the raw 30-minute CoinGlass Buy/Sell + CVD history stored by
Stage 87.2. It does not call CoinGlass, does not write trade decisions and does
not modify Alerts, Watch, Max-Pain score or LONG/SHORT selection.

Design rules:
- Buy/Sell and CVD are one data family, never two independent votes.
- The official CoinGlass CVD is preserved for audit; analytics use the locally
  rebuilt continuous CVD because it is continuous across API chunks.
- Every magnitude is compared only with the same symbol, market and timeframe.
- P75 is the minimum for directional evidence; P90 is strong evidence.
- 30m Buy-Sell is shown as the current impulse and is not counted again beside
  the 30m CVD change (they are mathematically the same family).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
from bisect import bisect_left
import os
from pathlib import Path
import sqlite3

import time_family_engine
import market_session_baseline as session_baseline
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")

WINDOWS: Tuple[Tuple[str, int], ...] = (
    ("30m", 1),
    ("1h", 2),
    ("4h", 8),
    ("12h", 24),
    ("24h", 48),
    ("48h", 96),
    ("72h", 144),
    ("7d", 336),
)
WINDOW_STEPS = dict(WINDOWS)
GROUPS: Dict[str, Tuple[str, ...]] = {
    "now": ("30m",),
    "short": ("1h", "4h"),
    "medium": ("12h", "24h"),
    "long": ("48h", "72h", "7d"),
    # Backward-compatible aliases retained for Stage 88 tests/helpers only.
    "momentum": ("30m", "1h"),
    "trend": ("4h", "12h", "24h"),
    "structure": ("48h", "72h", "7d"),
}
MIN_BASELINE_SAMPLES = 100
REFERENCE_TOLERANCE_MINUTES = 20


@dataclass(frozen=True)
class Percentiles:
    samples: int
    p25: float
    p50: float
    p75: float
    p90: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DirectionalBaselines:
    positive: Optional[Percentiles]
    negative: Optional[Percentiles]
    total_samples: int
    active_ratio: float = 1.0
    weekend_ratio: float = 0.0
    baseline_mode: str = "GLOBAL"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "positive": self.positive.to_dict() if self.positive else None,
            "negative": self.negative.to_dict() if self.negative else None,
            "total_samples": self.total_samples,
            "active_ratio": self.active_ratio,
            "weekend_ratio": self.weekend_ratio,
            "baseline_mode": self.baseline_mode,
        }

    def for_change(self, change: float) -> Optional[Percentiles]:
        if change > 0:
            return self.positive
        if change < 0:
            return self.negative
        return None


def _use_postgres() -> bool:
    return bool(DATABASE_URL and psycopg)


def _table(market: str) -> str:
    market = str(market or "").lower()
    if market == "futures":
        return "futures_taker_history"
    if market == "spot":
        return "spot_taker_history"
    raise ValueError("market must be futures or spot")


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _load_rows(symbol: str, market: str) -> List[Dict[str, Any]]:
    table = _table(market)
    symbol = str(symbol or "").upper()
    sql = (
        f"SELECT candle_time,buy_volume_usd,sell_volume_usd,"
        f"api_cum_vol_delta_usd,continuous_cum_vol_delta_usd "
        f"FROM {table} WHERE symbol=? ORDER BY candle_time"
    )
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            raw = conn.execute(sql.replace("?", "%s"), (symbol,)).fetchall()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            raw = conn.execute(sql, (symbol,)).fetchall()
    rows: List[Dict[str, Any]] = []
    for row in raw:
        item = dict(row)
        ts = _as_utc(item.get("candle_time"))
        if ts is None:
            continue
        if not session_baseline.is_closed_candle(ts, datetime.now(timezone.utc), interval_minutes=30, grace_minutes=2):
            continue
        try:
            buy = float(item.get("buy_volume_usd") or 0)
            sell = float(item.get("sell_volume_usd") or 0)
            api_cvd = float(item.get("api_cum_vol_delta_usd") or 0)
            continuous = float(item.get("continuous_cum_vol_delta_usd") or 0)
        except (TypeError, ValueError):
            continue
        if not all(math.isfinite(v) for v in (buy, sell, api_cvd, continuous)):
            continue
        rows.append({
            "time": ts,
            "buy": buy,
            "sell": sell,
            "delta": buy - sell,
            "api_cvd": api_cvd,
            "continuous_cvd": continuous,
        })
    return rows


def _percentile(sorted_values: Sequence[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return float(sorted_values[lower])
    weight = position - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def _nearest_index_by_time(
    rows: Sequence[Dict[str, Any]],
    times: Sequence[datetime],
    target: datetime,
    end_exclusive: Optional[int] = None,
) -> Optional[Tuple[int, float]]:
    """Find the nearest timestamp with a hard 20-minute tolerance."""
    limit = len(times) if end_exclusive is None else max(0, min(end_exclusive, len(times)))
    if limit <= 0:
        return None
    pos = bisect_left(times, target, 0, limit)
    candidates = []
    if pos < limit:
        candidates.append(pos)
    if pos > 0:
        candidates.append(pos - 1)
    if not candidates:
        return None
    best = min(candidates, key=lambda idx: abs((times[idx] - target).total_seconds()))
    offset = abs((times[best] - target).total_seconds())
    if offset > REFERENCE_TOLERANCE_MINUTES * 60:
        return None
    return best, offset


def _percentiles(values: Sequence[float]) -> Optional[Percentiles]:
    cleaned = sorted(float(v) for v in values if math.isfinite(float(v)) and float(v) >= 0)
    if len(cleaned) < MIN_BASELINE_SAMPLES:
        return None
    return Percentiles(
        samples=len(cleaned),
        p25=_percentile(cleaned, 0.25),
        p50=_percentile(cleaned, 0.50),
        p75=_percentile(cleaned, 0.75),
        p90=_percentile(cleaned, 0.90),
    )


def _weighted_percentiles(values_and_weights: Sequence[Tuple[float, float]]) -> Optional[Percentiles]:
    cleaned = [
        (float(value), float(weight))
        for value, weight in values_and_weights
        if math.isfinite(float(value)) and float(value) >= 0 and math.isfinite(float(weight)) and float(weight) > 0
    ]
    effective_samples = sum(weight for _, weight in cleaned)
    if effective_samples < MIN_BASELINE_SAMPLES:
        return None
    return Percentiles(
        samples=int(round(effective_samples)),
        p25=float(session_baseline.weighted_percentile(cleaned, 0.25)),
        p50=float(session_baseline.weighted_percentile(cleaned, 0.50)),
        p75=float(session_baseline.weighted_percentile(cleaned, 0.75)),
        p90=float(session_baseline.weighted_percentile(cleaned, 0.90)),
    )


def _blend_percentiles(active: Optional[Percentiles], weekend: Optional[Percentiles], global_pct: Optional[Percentiles], active_ratio: float, weekend_ratio: float) -> Optional[Percentiles]:
    if global_pct is None and active is None and weekend is None:
        return None
    fallback = global_pct or active or weekend
    active = active or fallback
    weekend = weekend or fallback
    if active is None or weekend is None:
        return fallback
    return Percentiles(
        samples=min(active.samples, weekend.samples),
        p25=float(session_baseline.blend_values(active.p25, weekend.p25, active_ratio, weekend_ratio, fallback.p25)),
        p50=float(session_baseline.blend_values(active.p50, weekend.p50, active_ratio, weekend_ratio, fallback.p50)),
        p75=float(session_baseline.blend_values(active.p75, weekend.p75, active_ratio, weekend_ratio, fallback.p75)),
        p90=float(session_baseline.blend_values(active.p90, weekend.p90, active_ratio, weekend_ratio, fallback.p90)),
    )


def _baseline(rows: Sequence[Dict[str, Any]], steps: int, current_end: Optional[datetime] = None) -> Optional[DirectionalBaselines]:
    """Directional CVD baseline matched to the current session composition.

    Historical changes remain intact. Each receives a similarity weight based
    on how closely its exact ACTIVE/WEEKEND ratio matches the current window.
    Positive and negative changes use separate distributions. If the matched
    sample is too small, the corresponding global directional distribution is
    used as a safe fallback.
    """
    positive_samples: List[Tuple[float, float]] = []
    negative_samples: List[Tuple[float, float]] = []
    positive_values: List[float] = []
    negative_values: List[float] = []
    times = [row["time"] for row in rows]
    window = timedelta(minutes=steps * 30)
    end = current_end or (times[-1] if times else datetime.now(timezone.utc))
    start = end - window
    current_active_ratio, current_weekend_ratio, _ = session_baseline.session_ratios(start, end)

    for idx in range(1, len(rows)):
        target = times[idx] - window
        nearest = _nearest_index_by_time(rows, times, target, end_exclusive=idx)
        if nearest is None:
            continue
        ref_idx, _ = nearest
        change = float(rows[idx]["continuous_cvd"]) - float(rows[ref_idx]["continuous_cvd"])
        if not math.isfinite(change) or change == 0:
            continue
        historical_active_ratio, _, _ = session_baseline.session_ratios(times[ref_idx], times[idx])
        magnitude = abs(change)
        if change > 0:
            positive_values.append(magnitude)
            positive_samples.append((magnitude, historical_active_ratio))
        else:
            negative_values.append(magnitude)
            negative_samples.append((magnitude, historical_active_ratio))

    total = len(positive_values) + len(negative_values)
    if total < MIN_BASELINE_SAMPLES:
        return None

    positive_global = _percentiles(positive_values)
    negative_global = _percentiles(negative_values)
    positive_matched = _weighted_percentiles(
        session_baseline.composition_weighted_values(positive_samples, current_active_ratio)
    )
    negative_matched = _weighted_percentiles(
        session_baseline.composition_weighted_values(negative_samples, current_active_ratio)
    )
    positive = positive_matched or positive_global
    negative = negative_matched or negative_global
    return DirectionalBaselines(
        positive=positive,
        negative=negative,
        total_samples=total,
        active_ratio=current_active_ratio,
        weekend_ratio=current_weekend_ratio,
        baseline_mode="SESSION_COMPOSITION_MATCHED" if (positive_matched or negative_matched) else "GLOBAL",
    )

def _nearest_reference(rows: Sequence[Dict[str, Any]], target: datetime) -> Optional[Tuple[int, float]]:
    return _nearest_index_by_time(rows, [row["time"] for row in rows], target)


def _magnitude_label(value_abs: float, baseline: Percentiles) -> Tuple[str, int]:
    """Return an unambiguous historical magnitude class and level.

    level 0: below P25 (noise/neutral)
    level 1: P25-P75 (normal, directional but not evidence)
    level 2: P75-P90 (meaningful evidence)
    level 3: >=P90 (strong evidence)
    """
    if value_abs < baseline.p25:
        return "NOISE", 0
    if value_abs < baseline.p75:
        return "NORMAL", 1
    if value_abs < baseline.p90:
        return "MEANINGFUL", 2
    return "STRONG", 3


def _window_state(rows: Sequence[Dict[str, Any]], label: str, steps: int) -> Dict[str, Any]:
    if len(rows) <= steps:
        return {"window": label, "available": False, "reason": "not enough history"}
    latest_idx = len(rows) - 1
    latest = rows[latest_idx]
    target = latest["time"] - timedelta(minutes=steps * 30)
    nearest = _nearest_reference(rows, target)
    if nearest is None:
        return {
            "window": label,
            "available": False,
            "reason": "reference outside tolerance",
            "target_time": target.isoformat(),
        }
    reference_idx, offset = nearest
    if reference_idx >= latest_idx:
        return {"window": label, "available": False, "reason": "reference is not older"}
    directional_baselines = _baseline(rows, steps, current_end=latest["time"])
    if directional_baselines is None:
        return {"window": label, "available": False, "reason": "not enough baseline samples"}

    change = float(latest["continuous_cvd"]) - float(rows[reference_idx]["continuous_cvd"])
    direction = "BULLISH" if change > 0 else "BEARISH" if change < 0 else "NEUTRAL"
    baseline = directional_baselines.for_change(change)
    if direction != "NEUTRAL" and baseline is None:
        return {
            "window": label,
            "available": False,
            "reason": f"not enough {direction.lower()} baseline samples",
        }
    if direction == "NEUTRAL":
        magnitude, level = "NOISE", 0
    else:
        magnitude, level = _magnitude_label(abs(change), baseline)
    if level == 0 or direction == "NEUTRAL":
        state = "NEUTRAL"
    elif level == 1:
        state = f"{direction}_NORMAL"
    elif level == 2:
        state = f"{direction}_MEANINGFUL"
    else:
        state = f"{direction}_STRONG"

    return {
        "window": label,
        "available": True,
        "state": state,
        "direction": direction,
        "magnitude": magnitude,
        "evidence_level": level,
        "cvd_change_usd": change,
        "latest_time": latest["time"].isoformat(),
        "reference_time": rows[reference_idx]["time"].isoformat(),
        "target_time": target.isoformat(),
        "reference_offset_seconds": offset,
        "baseline": baseline.to_dict() if baseline else None,
        "directional_baselines": directional_baselines.to_dict(),
    }


def _group_state(name: str, windows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    labels = GROUPS[name]
    significant = [
        windows[label] for label in labels
        if windows.get(label, {}).get("available")
        and int(windows[label].get("evidence_level") or 0) >= 2
    ]
    bullish = [x for x in significant if x.get("direction") == "BULLISH"]
    bearish = [x for x in significant if x.get("direction") == "BEARISH"]

    if bullish and bearish:
        state = "MIXED"
        direction = "NEUTRAL"
    elif len(bullish) >= 2:
        state = "BULLISH_CONFIRMED"
        direction = "BULLISH"
    elif len(bearish) >= 2:
        state = "BEARISH_CONFIRMED"
        direction = "BEARISH"
    elif len(bullish) == 1:
        state = "BULLISH_EVIDENCE"
        direction = "BULLISH"
    elif len(bearish) == 1:
        state = "BEARISH_EVIDENCE"
        direction = "BEARISH"
    else:
        state = "NEUTRAL"
        direction = "NEUTRAL"

    strong_count = sum(1 for x in significant if int(x.get("evidence_level") or 0) >= 3)
    return {
        "group": name,
        "state": state,
        "direction": direction,
        "significant_windows": len(significant),
        "strong_windows": strong_count,
        "members": list(labels),
    }


def _overall(groups: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    directional = [g for g in groups.values() if g.get("direction") in {"BULLISH", "BEARISH"}]
    bull = [g for g in directional if g.get("direction") == "BULLISH"]
    bear = [g for g in directional if g.get("direction") == "BEARISH"]
    if bull and bear:
        return {"state": "MIXED", "direction": "NEUTRAL", "confirmed_groups": 0}
    same = bull or bear
    if not same:
        return {"state": "NEUTRAL", "direction": "NEUTRAL", "confirmed_groups": 0}
    direction = same[0]["direction"]
    confirmed = [g for g in same if str(g.get("state", "")).endswith("CONFIRMED")]
    if len(confirmed) >= 2:
        state = f"{direction}_CONFIRMED"
    elif len(confirmed) == 1 or len(same) >= 2:
        state = f"{direction}_EVIDENCE"
    else:
        state = f"{direction}_EARLY"
    return {"state": state, "direction": direction, "confirmed_groups": len(confirmed)}


def _early_shift(groups: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    momentum = groups.get("momentum") or {}
    trend = groups.get("trend") or {}
    structure = groups.get("structure") or {}
    momentum_dir = momentum.get("direction")
    established = [g.get("direction") for g in (trend, structure) if g.get("state", "").endswith("CONFIRMED")]
    if momentum_dir not in {"BULLISH", "BEARISH"} or not established:
        return None
    if all(x == established[0] for x in established) and momentum_dir != established[0]:
        return {
            "detected": True,
            "new_direction": momentum_dir,
            "established_direction": established[0],
            "reason": "momentum opposes confirmed trend/structure flow",
        }
    return None


def _quality(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"status": "NO_DATA", "rows": 0, "reasons": ["no stored rows"]}
    # The local continuous CVD should equal cumulative Buy-Sell. Check the last
    # value against an independent sum to detect corruption or stale rebuilds.
    independent = sum(float(r["delta"]) for r in rows)
    stored = float(rows[-1]["continuous_cvd"])
    # Stage 88.2: allow tiny floating-point / storage differences without
    # treating a few dollars inside a multi-billion-dollar series as corruption.
    # A genuinely stale or unreconstructed series still fails because its
    # mismatch is typically orders of magnitude larger than this threshold.
    tolerance = max(1_000.0, abs(independent) * 0.0001)
    cvd_difference = abs(independent - stored)
    cvd_ok = cvd_difference <= tolerance
    gaps = []
    largest_gap_seconds = 0.0
    for prev, cur in zip(rows, rows[1:]):
        gap = (cur["time"] - prev["time"]).total_seconds()
        if gap > 31 * 60:
            gaps.append(gap)
            largest_gap_seconds = max(largest_gap_seconds, gap)

    reasons: List[str] = []
    if not cvd_ok:
        reasons.append(
            f"continuous CVD mismatch: difference ${cvd_difference:,.2f} exceeds tolerance ${tolerance:,.2f}"
        )
    if gaps:
        reasons.append(
            f"{len(gaps)} missing 30m interval(s); largest gap {largest_gap_seconds/60:.1f} minutes"
        )
    if not reasons:
        reasons.append("continuous CVD matches Buy-Sell sum and no 30m gaps were found")

    return {
        "status": "PASS" if cvd_ok and not gaps else "WARNING",
        "rows": len(rows),
        "continuous_cvd_check": cvd_ok,
        "continuous_cvd_difference_usd": cvd_difference,
        "continuous_cvd_tolerance_usd": tolerance,
        "missing_30m_intervals": len(gaps),
        "largest_gap_seconds": largest_gap_seconds,
        "latest_time": rows[-1]["time"].isoformat(),
        "reasons": reasons,
    }


def analyze_market(symbol: str, market: str) -> Dict[str, Any]:
    rows = _load_rows(symbol, market)
    quality = _quality(rows)
    windows = {label: _window_state(rows, label, steps) for label, steps in WINDOWS}
    # Stage 90: one shared weighted time-family model is used in regular scans
    # and in confirmation. Keep legacy group fields for backward compatibility.
    weighted = time_family_engine.aggregate(windows, time_family_engine.flow_window_evaluator)
    groups = weighted["families"]
    overall = {
        "state": f"{weighted['direction']}_EVIDENCE" if weighted["direction"] != "NEUTRAL" else "NEUTRAL",
        "direction": weighted["direction"],
        "weighted_score": weighted["score"],
        "quality": weighted["quality"],
        "confirmed_groups": sum(1 for g in groups.values() if g.get("quality", 0) >= 0.5 and g.get("direction") != "NEUTRAL"),
    }
    now_dir=(groups.get("now") or {}).get("direction")
    broader=[(groups.get(k) or {}).get("direction") for k in ("short","medium","long") if (groups.get(k) or {}).get("quality",0) >= 0.35]
    early = None
    if now_dir in {"BULLISH","BEARISH"} and broader:
        established = "BULLISH" if broader.count("BULLISH") > broader.count("BEARISH") else "BEARISH" if broader.count("BEARISH") > broader.count("BULLISH") else None
        if established and established != now_dir:
            early={"detected":True,"new_direction":now_dir,"established_direction":established,"reason":"now family opposes broader weighted families"}

    latest_impulse = None
    if rows:
        latest = rows[-1]
        directional_baseline_30m = _baseline(rows, 1, current_end=latest["time"])
        baseline_30m = directional_baseline_30m.for_change(float(latest["delta"])) if directional_baseline_30m else None
        if baseline_30m:
            magnitude, level = _magnitude_label(abs(float(latest["delta"])), baseline_30m)
            latest_impulse = {
                "delta_usd": float(latest["delta"]),
                "direction": "BULLISH" if latest["delta"] > 0 else "BEARISH" if latest["delta"] < 0 else "NEUTRAL",
                "magnitude": magnitude,
                "evidence_level": level,
                "note": "same data family as 30m CVD; not an additional vote",
            }

    return {
        "symbol": str(symbol or "").upper(),
        "market": market,
        "available": bool(rows),
        "quality": quality,
        "current_impulse_30m": latest_impulse,
        "windows": windows,
        "groups": groups,
        "overall": overall,
        "early_shift": early,
    }


def analyze_symbol(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": str(symbol or "").upper(),
        "futures": analyze_market(symbol, "futures"),
        "spot": analyze_market(symbol, "spot"),
    }


def stats(symbol: str, market: str) -> Dict[str, Any]:
    rows = _load_rows(symbol, market)
    result: Dict[str, Any] = {
        "symbol": str(symbol or "").upper(),
        "market": market,
        "rows": len(rows),
        "windows": {},
    }
    for label, steps in WINDOWS:
        baseline = _baseline(rows, steps, current_end=rows[-1]["time"] if rows else None)
        result["windows"][label] = baseline.to_dict() if baseline else None
    return result
