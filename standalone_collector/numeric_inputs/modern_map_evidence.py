"""Validate public CoinGlass labels and sample an observed Viridis price grid."""
from __future__ import annotations

import colorsys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from math import ceil, floor, isfinite
import re

from heatmap_hover import MODEL2_GREEN_BLUE_HUE
from model1_execution import StageFailure

PRICE = r'(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?'
AMOUNT = PRICE + r'[KMBT]?'
SCALE = {'K': Decimal(1000), 'M': Decimal(1000000), 'B': Decimal(1000000000), 'T': Decimal(10)**12}


# This DOM/Measure adapter uses the same Viridis thresholds as the pinned
# legacy hover reader. It groups pixels before the values are measured.
def classic_colour(rgb, model):
    green_blue = MODEL2_GREEN_BLUE_HUE if model == 2 else .48
    h, s, v = colorsys.rgb_to_hsv(*(c/255 for c in rgb))
    if .13 <= h < .24 and s >= .45 and v >= .55: return 'yellow'
    if .24 <= h < green_blue and s >= .45 and v >= .4: return 'green'
    if green_blue <= h < .66 and s >= .45 and v >= .35: return 'blue'
    return None


def prominent_runs(groups):
    prominent = any(label == 'yellow' or (label == 'green' and len(rows) > 1)
                    for label, rows in groups)
    return [(label, rows) for label, rows in groups
            if label == 'yellow' or (label == 'green' and len(rows) > 1)
            or (label == 'blue' and not prominent)]


def check(value):
    if not value:
        raise StageFailure('source_not_readable')


def number(raw, quantity=False):
    check(isinstance(raw, str) and len(raw) <= 40 and re.fullmatch(AMOUNT if quantity else PRICE, raw))
    suffix = raw[-1] in SCALE
    digits = (raw[:-1] if suffix else raw).replace(',', '')
    value = Decimal(digits)
    scale = SCALE[raw[-1]] if suffix else Decimal(1)
    return value * scale, scale * Decimal(10) ** value.as_tuple().exponent / 2


def equivalent(total, parts):
    value, error = number(total, True)
    values = [number(part, True) for part in parts]
    check(abs(value - sum(p[0] for p in values)) <= error + sum(p[1] for p in values))


def unresolved_measure(reason):
    failure = StageFailure('source_not_readable', 'hover_resolution_insufficient')
    failure.measure_detail = {'reason': reason}
    raise failure


def box_member(measured, raw_price, raw_step, raw_anchor):
    """Use a printed Box total only inside one independently labelled native cell."""
    if not raw_price or not raw_anchor:
        unresolved_measure('native_price_not_printed')
    raw_bounds = getattr(measured, 'raw_bounds', None)
    if not raw_bounds:
        unresolved_measure('box_bound_precision_missing')
    price, price_error = number(raw_price)
    anchor, anchor_error = number(raw_anchor)
    step, step_error = number(raw_step)
    if step <= step_error:
        unresolved_measure('native_step_invalid')
    index = ((price-anchor)/step).to_integral_value()
    if abs(price-anchor-index*step) > price_error+anchor_error+abs(index)*step_error:
        unresolved_measure('price_not_on_native_grid')
    low, low_error = number(raw_bounds[0])
    high, high_error = number(raw_bounds[1])
    half_bin = (step-step_error)/2
    # Allow for both printed boundary errors: the interval must enclose this
    # centre and fit strictly inside its native bin, even at worst-case rounding.
    if not (price+price_error-half_bin < low-low_error and low+low_error < price-price_error and
            price+price_error < high-high_error and high+high_error < price-price_error+half_bin):
        unresolved_measure('box_not_within_native_bin')
    amount, _ = number(measured[1], True)
    if amount <= 0:
        unresolved_measure('prominent_bin_amount_missing')
    return {'raw_price':raw_price, 'raw_value':measured[1]}


def verify_covered_maximum(groups, strongest, raw_maximum, sidebar):
    """Require all displayed ties and the independent maximum to survive the selected grid."""
    members = [m for g in groups for m in g['members']]
    check(members and (strongest or raw_maximum))
    by_price = {number(m['raw_price'])[0]: m for m in members}
    check(len(by_price) == len(members))
    required = [strongest] if strongest else []
    largest = max(members, key=lambda m:number(m['raw_value'], True)[0])
    equivalent(raw_maximum or strongest['raw_value'], [largest['raw_value']])
    if raw_maximum:
        maximum = number(raw_maximum, True)[0]
        displayed_ties = [m for m in sidebar if number(m['raw_value'], True)[0] == maximum]
        if not strongest and not displayed_ties:
            unresolved_measure('maximum_price_not_confirmed')
        required.extend(displayed_ties)
        if strongest:
            equivalent(raw_maximum, [strongest['raw_value']])
    for item in required:
        selected = by_price.get(number(item['raw_price'])[0])
        check(selected)
        equivalent(item['raw_value'], [selected['raw_value']])


def source_clock(raw, at, *, column=False, max_lag=1200):
    # The browser context is explicitly UTC. Select the nearest labelled minute
    # across midnight / New Year, and reject stale observations independently.
    fmt = '%m/%d %H:%M' if column else '%H:%M'
    try:
        if column:
            candidates = []
            for year in range(at.year-1, at.year+2):
                try:
                    candidates.append(datetime.strptime(f'{year} {raw}', '%Y '+fmt).replace(tzinfo=timezone.utc))
                except ValueError:
                    pass
            check(candidates)
        else:
            hour = datetime.strptime(raw, fmt).time()
            candidates = [datetime.combine(at.date() + timedelta(days=i), hour, timezone.utc) for i in (-1,0,1)]
    except ValueError:
        raise StageFailure('source_not_readable') from None
    selected = min(candidates, key=lambda date: abs(date - at))
    check(-120 <= (at-selected).total_seconds() <= max_lag)
    return selected


def iso(stamp):
    return stamp.isoformat().replace('+00:00', 'Z')


def observation(text, clock):
    def read(label, pattern, *, reason=None):
        match = re.search(label + r'\s*' + pattern, text, flags=re.I)
        if match is None:
            raise StageFailure('source_not_readable', reason)
        return match.group(1)
    def side(label, reason):
        # Adjacent market-card spans can render '479.9M48%'. Only a scale
        # suffix establishes a boundary when the site omits whitespace.
        percentage = r'(?:100(?:\.0+)?|[1-9]?\d(?:\.\d+)?)%'
        boundary = r'(?=\s|$|(?<=[KMBT])' + percentage + r'(?:\s|$))'
        return read(label, '(' + AMOUNT + ')' + boundary, reason=reason)
    above = side(r'Shorts\s*\(above price\)', 'map_shorts_amount_unreadable')
    below = side(r'Longs\s*\(below price\)', 'map_longs_amount_unreadable')
    combined = read(r'Open liquidation levels\s*\(total\)', '('+AMOUNT+r')(?=\s|$)')
    step = read(r'Price step\s*/\s*column period', '('+PRICE+r')\s*/')
    update = read(r'Updated', r'(\d{2}:\d{2})(?!\d)')
    equivalent(combined, [above, below])
    check(number(step)[0] > 0)
    return {'raw_above_total': above, 'raw_below_total': below, 'raw_price_step': step,
            'source_time': iso(source_clock(update, clock))}


def measurement(text):
    def read(pattern):
        match = re.search(pattern, text, flags=re.I)
        check(match)
        return match.group(1)
    reference = read(r'Reference\s*\(last price\)\s*('+PRICE+r')')
    total = read(r'Box total\s*[·•]?\s*'+PRICE+r'\s*[→–-]\s*'+PRICE+
                 r'\s*\([^)]*\)\s*('+AMOUNT+r')(?=\s|$)')
    leader = re.search(r'Largest cluster\s*('+PRICE+r')\s*[·•]\s*('+AMOUNT+r')(?=\s|$)', text, re.I)
    check(leader)
    bounds = re.search(r'Box total\s*[·•]?\s*('+PRICE+r')\s*[→–-]\s*('+PRICE+r')',text,re.I)
    low, high = sorted(number(bounds.group(i))[0] for i in (1,2))
    return reference, total, {'raw_price': leader.group(1), 'raw_value': leader.group(2)}, low, high


def sample_groups(image, bounds, model, anchor_price, price_step, y_for_price, last_price, *, audit=None):
    """Coordinates select rows. Every returned price/value is subsequently read from the source."""
    left, top, right, bottom = bounds
    check(0 <= left < right < image.width and 0 <= top < bottom < image.height)
    # Printed grid coordinates are decimal. Convert only when projecting a
    # price to image pixels, avoiding binary artefacts in persisted endpoints.
    anchor = Decimal(str(anchor_price))
    step = Decimal(str(price_step))
    y0 = y_for_price(float(anchor))
    height = y0 - y_for_price(float(anchor + step))
    check(isfinite(height) and 1.5 <= height <= (bottom-top)/3)
    first, last = ceil((y0-bottom)/height), floor((y0-top)/height)
    check(2 <= last-first <= 1600)
    labeled = []
    for index in range(first, last+1):
        price = float(anchor + index*step)
        y = y0-index*height
        votes = [classic_colour(image.getpixel((right-1, max(top, min(bottom, round(y+offset*height))))), model)
                 for offset in (-.2,0,.2)]
        colour, count = Counter(votes).most_common(1)[0]
        side = 'above' if price > last_price else 'below' if price < last_price else None
        if count < 2 or side is None:
            colour = None
        if labeled and labeled[-1][0] == colour and labeled[-1][1][-1]['side'] == side:
            labeled[-1][1].append({'y':y, 'price':price, 'side':side})
        else:
            labeled.append((colour,[{'y':y, 'price':price, 'side':side}]))
    runs = prominent_runs(labeled)
    if audit is not None:
        audit.update(native_bins_inspected=last-first+1)
        check(len(runs) <= last-first+1)
    else:
        check(1 <= len(runs) <= 40)
    return height, runs


def verified_member(row, member, step):
    price, error = number(member['raw_price'])
    check(number(member['raw_value'], True)[0] > 0 and abs(float(price) - row['price']) <= float(step)*.35)
    return float(price)


def validate_groups(groups, last_price, raw_step, above, below):
    check(1 <= len(groups) <= 40)
    used = set()
    step, step_error = number(raw_step)
    sums = {'above':Decimal(0), 'below':Decimal(0)}
    errors = sums.copy()
    for group in groups:
        side, colour, members = group['side'], group['colour'], group['members']
        check(side in sums and colour in ('yellow','green','blue') and 1 <= len(members) <= 1600)
        check(colour != 'green' or len(members) >= 2)
        equivalent(group['raw_total'], [item['raw_value'] for item in members])
        previous = None
        for item in members:
            price, error = number(item['raw_price'])
            value, _ = number(item['raw_value'], True)
            check(value > 0 and price > 0 and price not in used and
                  (price > last_price if side == 'above' else price < last_price))
            if previous:
                check(abs(price - previous[0] - step) <= min(step/2, error + previous[1] + step_error))
            previous = price, error
            used.add(price)
        value, error = number(group['raw_total'], True)
        sums[side] += value
        errors[side] += error
    for side, raw in (('above',above),('below',below)):
        total, error = number(raw, True)
        check(sums[side] <= total + error + errors[side])
    check(len({g['colour'] == 'blue' for g in groups}) == 1)
