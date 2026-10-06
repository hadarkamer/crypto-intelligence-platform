"""Pure, symmetric price levels for an already verified experimental reference.

Callers select and verify the reference clock/source. This module neither fetches
prices nor substitutes an alert time for an unavailable reference time. Invalid
inputs raise ``ValueError`` so a caller can omit unavailable levels explicitly.
The displayed levels are research arithmetic, not exchange-rounded orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext, ROUND_HALF_UP
from html import escape
from typing import Any
from zoneinfo import ZoneInfo


_ISRAEL = ZoneInfo("Asia/Jerusalem")
_TEN_THOUSAND = Decimal(10000)


@dataclass(frozen=True)
class PriceLevels:
    reference_price: Decimal
    reference_time: datetime
    threshold_bps: Decimal
    direction: str
    stop_loss: Decimal
    take_profit: Decimal


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float, str)):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = Decimal(str(value))
    except (DecimalException, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not number.is_finite():
        raise ValueError(f"{name} must be a finite number")
    return number


def _aware_time(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("reference_time must include a timezone") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("reference_time must include a timezone")
    try:
        return value.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise ValueError("reference_time is out of range") from exc


def calculate_price_levels(
    reference_price: Any,
    reference_time: Any,
    threshold_bps: Any,
    direction: str,
) -> PriceLevels:
    """Calculate both levels from the reference, using the final alert direction.

    ``threshold_bps=200`` means 2%. The direction must already include any
    formula inversion; this function never applies a formula-specific inversion.
    The caller must supply an aware datetime or an ISO timestamp with timezone.
    """

    price = _decimal(reference_price, "reference_price")
    bps = _decimal(threshold_bps, "threshold_bps")
    if price <= 0:
        raise ValueError("reference_price must be positive")
    if not 0 < bps < _TEN_THOUSAND:
        raise ValueError("threshold_bps must be between 0 and 10000, exclusively")
    if not isinstance(direction, str) or direction not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG or SHORT")
    timestamp = _aware_time(reference_time)

    # Preserve decimal multiplication even if another caller changed its decimal
    # context. Extra precision also retains thresholds smaller than one basis point.
    precision = max(
        64,
        len(price.as_tuple().digits) + len(bps.as_tuple().digits)
        + max(0, -bps.as_tuple().exponent) + 12,
    )
    if precision > 4096:
        raise ValueError("price levels require unsupported decimal precision")
    try:
        with localcontext() as context:
            context.prec = precision
            fraction = bps / _TEN_THOUSAND
            lower = price * (Decimal(1) - fraction)
            upper = price * (Decimal(1) + fraction)
    except DecimalException as exc:
        raise ValueError("price levels cannot be represented") from exc
    if not (lower.is_finite() and upper.is_finite() and 0 < lower < price < upper):
        raise ValueError("price levels must be positive and distinct")
    return PriceLevels(
        reference_price=price,
        reference_time=timestamp,
        threshold_bps=bps,
        direction=direction,
        stop_loss=lower if direction == "LONG" else upper,
        take_profit=upper if direction == "LONG" else lower,
    )


def _formatted_prices(levels: PriceLevels) -> tuple[str, str, str]:
    values = (levels.reference_price, levels.stop_loss, levels.take_profit)
    # Eight significant figures (at least cents for larger prices) are compact
    # for BTC through DOGE. Increase precision if a small threshold needs it.
    places = max(2, 7 - levels.reference_price.adjusted())
    for extra in range(40):
        if -20 <= levels.reference_price.adjusted() <= 20 and places + extra <= 40:
            with localcontext() as context:
                context.prec = max(80, places + extra + 32)
                quantum = Decimal(1).scaleb(-(places + extra))
                rounded = tuple(value.quantize(quantum, rounding=ROUND_HALF_UP) for value in values)
            rendered = tuple(format(value, "f").rstrip("0").rstrip(".") for value in rounded)
        else:
            # Scientific notation prevents unbounded strings for unusual inputs.
            rendered = tuple(format(value, f".{8 + extra}g") for value in values)
        parsed = tuple(Decimal(value) for value in rendered)
        if len(set(parsed)) == 3 and all(value > 0 for value in parsed):
            return rendered
    raise ValueError("price levels cannot be displayed distinctly")


def render_price_levels(
    levels: PriceLevels,
    *,
    reference_label: str,
    html: bool = True,
) -> str:
    """Render prices and the caller's reference-clock label, in Israel time.

    For example a caller can label a verified reference ``סגירת נתוני CVD``.
    HTML mode escapes that label and adds Telegram-compatible bold labels;
    plain mode adds no markup. Neither mode labels the reference as a fill price.
    """

    if not isinstance(reference_label, str) or not reference_label.strip():
        raise ValueError("reference_label must identify the reference clock")
    if not isinstance(levels, PriceLevels):
        raise ValueError("levels must be validated PriceLevels")
    # Do not trust manually constructed dataclass instances to contain valid or
    # direction-consistent levels. Recalculate from the four reference inputs.
    verified = calculate_price_levels(
        levels.reference_price, levels.reference_time, levels.threshold_bps, levels.direction
    )
    if levels != verified:
        raise ValueError("levels do not match their reference inputs")
    reference, stop, target = _formatted_prices(verified)
    local_time = verified.reference_time.astimezone(_ISRAEL).strftime("%d.%m.%Y %H:%M:%S")
    clock_label = reference_label.strip()

    def line(label: str, value: str) -> str:
        if html:
            return f"<b>{escape(label)}:</b> {escape(value)}"
        return f"{label}: {value}"

    return "\n".join((
        line(f"שער בסיס — {clock_label}", reference),
        line("שעת שער הבסיס", f"{local_time} (שעון ישראל)"),
        line("סטופלוס", stop),
        line("טייק פרופיט", target),
    ))
