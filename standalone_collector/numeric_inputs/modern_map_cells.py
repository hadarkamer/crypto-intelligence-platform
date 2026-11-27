"""Calculate one observed map using displayed precision and a single time column."""
from decimal import Decimal
import re

from modern_map_evidence import number, validate_groups
from model1_execution import StageFailure


def _fail(reason):
    raise StageFailure('source_not_readable', reason)


def _agree(a, b):
    return a[0] == b[0] or abs(a[0] - b[0]) < a[1] + b[1]


def _adjacent(first, second, step):
    gap = abs(second[0] - first[0])
    return gap > 0 and abs(gap - step[0]) <= min(step[0] / 2, first[1] + second[1] + step[1])


def merge_cells(book, rows, column, step):
    """Merge one ordered tooltip grid; caller owns one book per validated map update/column.

    Unknown resolution, another column and missing cells cannot add a numeric amount.
    Conflicting copies fail without changing the prior book.
    """
    result = dict(accepted=0, duplicates=0, historical=0, unresolved=0)
    grid_step = number(step)
    if not isinstance(column, str) or not column or grid_step[0] <= 0:
        _fail('hover_time_invalid' if not column else 'hover_resolution_insufficient')
    current = []
    for row in rows:
        if not row.get('time'):
            result['unresolved'] += 1
        elif row['time'] != column:
            result['historical'] += 1
        else:
            current.append(row)
    if not current:
        return result
    try:
        prices = [number(row['price']) for row in current]
        grid_prices = sorted({price[0]: price for price in prices}.values())
        explicit_steps = [number(row['step']) for row in current if row.get('step')]
    except (KeyError, StageFailure):
        result['unresolved'] += len(current)
        return result
    spacings = list(zip(grid_prices, grid_prices[1:]))
    if (not explicit_steps and not spacings or
            any(not _agree(s, grid_step) for s in explicit_steps) or
            any(not _adjacent(a, b, grid_step) for a, b in spacings)):
        result['unresolved'] += len(current)
        return result

    staged = {}
    for row, (price, _) in zip(current, prices):
        try:
            value = number(row.get('amount'), True)
        except StageFailure:
            result['unresolved'] += 1
            continue
        if price <= 0:
            result['unresolved'] += 1
            continue
        key = {'raw_price': row['price'], 'raw_value': row['amount']}
        old = staged.get(price) or book.get(price)
        if old:
            before = number(old['raw_value'], True)
            if not _agree(before, value):
                _fail('hover_evidence_mismatch')
            if value[1] < before[1]:
                staged[price] = key
            result['duplicates'] += 1
        else:
            staged[price] = key
            result['accepted'] += 1
    book.update(staged)
    return result


def _ratio(part, total):
    return part * 100 / total


def _percent(raw, signed=False):
    if not isinstance(raw, str):
        _fail('evidence_number_mismatch')
    match = re.fullmatch(r'([+−-]?)(\d+(?:\.\d+)?)%', raw)
    if not match or (not signed and match[1]):
        _fail('evidence_number_mismatch')
    value = Decimal(match[2])
    if not signed and value > 100:
        _fail('evidence_number_mismatch')
    error = Decimal(10) ** value.as_tuple().exponent / 2
    return (-value if match[1] in ('-', '−') else value), error


def _verify_percent(raw, numerator, denominator, offset=0):
    printed, margin = _percent(raw, signed=bool(offset))
    amount, amount_error = numerator
    total, total_error = denominator
    if total <= total_error:
        _fail('evidence_number_mismatch')
    low = _ratio(max(Decimal(0), amount - amount_error), total + total_error) - offset
    high = _ratio(amount + amount_error, total - total_error) - offset
    if high < printed - margin or low > printed + margin:
        _fail('evidence_number_mismatch')


def _numeric(value):
    return int(value) if value == value.to_integral_value() else float(value)


def _rounded(value):
    return float(round(value, 2))


def map_metrics(snapshot, raw_total=None, source_percentages=None):
    """Pure calculations for a validated snapshot. Extra labels must belong to this update."""
    ref = number(snapshot['raw_last_price'])
    sides = {side: number(snapshot['raw_' + side + '_total'], True) for side in ('above', 'below')}
    summed = tuple(sum(s[i] for s in sides.values()) for i in (0, 1))
    total = number(raw_total, True) if raw_total is not None else summed
    if ref[0] <= 0 or total[0] <= 0 or abs(total[0] - summed[0]) > total[1] + summed[1]:
        _fail('evidence_number_mismatch')
    if (snapshot.get('source_time') != snapshot.get('source_time_after') or
            snapshot.get('column_time') != snapshot.get('column_time_after')):
        _fail('hover_snapshot_changed')
    validate_groups(snapshot['groups'], ref[0], snapshot['raw_price_step'],
                    snapshot['raw_above_total'], snapshot['raw_below_total'])
    for side, pct in (source_percentages or {}).items():
        if side in sides and pct is not None:
            _verify_percent(pct, sides[side], total)
    shares = {side: _ratio(value[0], total[0]) for side, value in sides.items()}
    clusters = []
    for index, group in enumerate(snapshot['groups']):
        members = group['members']
        low = number(members[0]['raw_price'])[0]
        high = number(members[-1]['raw_price'])[0]
        center = (low + high) / 2
        amount = sum(number(m['raw_value'], True)[0] for m in members)
        clusters.append(dict(candidate_id=f'c{index}', side=group['side'], colour=group['colour'],
                             low=_numeric(low), high=_numeric(high), center=_numeric(center),
                             amount=_numeric(amount), raw_total=group['raw_total'],
                             distance_pct=_rounded(_ratio(center-ref[0], ref[0])),
                             liquidity_share_pct=_rounded(_ratio(amount, total[0]))))
    return dict(raw_total=raw_total, total=_numeric(total[0]), unit='source_metric',
                denominator_basis='displayed_total' if raw_total is not None else 'sum_of_displayed_side_amounts',
                side_percent={s: _rounded(p) for s, p in shares.items()},
                difference_above_minus_below=_numeric(sides['above'][0] - sides['below'][0]),
                difference_percentage_points=_rounded(shares['above'] - shares['below']),
                clusters=clusters)


def metrics_from_overview(snapshot, overview):
    """Bind supplementary DOM labels to the canonical capture before their use."""
    if not overview or overview.get('source_time') != snapshot['source_time']:
        _fail('hover_snapshot_changed')
    for key in ('raw_above_total', 'raw_below_total', 'raw_price_step', 'raw_last_price'):
        value = overview.get(key)
        if value and not _agree(number(value, key.endswith('_total')), number(snapshot[key], key.endswith('_total'))):
            _fail('evidence_number_mismatch')
    reference = number(snapshot['raw_last_price'])
    members = {number(m['raw_price'])[0]: m for group in snapshot['groups'] for m in group['members']}
    for row in overview.get('sidebar_levels', []):
        price = number(row['raw_price'])
        if row.get('raw_distance_pct'):
            _verify_percent(row['raw_distance_pct'], price, reference, offset=100)
        other = members.get(price[0])
        displayed = number(row['raw_value'], True)
        if other:
            captured = number(other['raw_value'], True)
            if not _agree(captured, displayed):
                _fail('hover_evidence_mismatch')
            if captured[1] <= displayed[1]:
                continue
        members[price[0]] = row
    result = map_metrics(snapshot, overview.get('raw_total'),
                         {s: overview.get(f'raw_{s}_pct') for s in ('above', 'below')})
    # Adjacent rounded values can tie. Report every possible leader among
    # the observed levels, with no claim about prices outside our coverage.
    leaders = {}
    for side in ('above', 'below'):
        candidates = []
        for price, row in members.items():
            if (price > reference[0] if side == 'above' else price < reference[0]):
                value, margin = number(row['raw_value'], True)
                candidates.append((value - margin, value + margin, row))
        lower_bound = max((low for low, _, _ in candidates), default=Decimal(0))
        leaders[side] = [dict(raw_price=row['raw_price'], raw_value=row['raw_value'])
                         for _, high, row in candidates if high >= lower_bound]
    result['strongest_observed_levels'] = leaders
    realized = [overview.get('raw_realized_' + key) for key in ('total', 'longs', 'shorts')]
    result['realized_24h_status'] = 'incomplete'
    if all(value is not None for value in realized):
        total, longs, shorts = [number(value, True) for value in realized]
        delta = abs(longs[0] + shorts[0] - total[0])
        result['realized_24h_status'] = ('rounding_consistent' if delta <= longs[1] + shorts[1] + total[1]
                                        else 'component_total_conflict')
    return result


def display_facts_summary(snapshot, overview, metrics, available_chars):
    """Render verified numbers in the existing worksheet audit summary, using whole lines."""
    overview = overview or {}
    basis = ('מהסך המוצג ' + metrics['raw_total'] if metrics['raw_total'] is not None
             else 'מסכום שני הצדדים המוצגים')
    lines = [f"נזילות פתוחה במדד המקור. מעל {snapshot['raw_above_total']} · מתחת {snapshot['raw_below_total']}",
             f"חלקי הנזילות {basis} · מעל ≈{metrics['side_percent']['above']:.1f}% · "
             f"מתחת ≈{metrics['side_percent']['below']:.1f}%",
             f"מחיר ייחוס {snapshot['raw_last_price']} · עמודה {snapshot['column_time']} · "
             f"מרווח מחיר {snapshot['raw_price_step']} · משך עמודה {overview.get('raw_column_period') or 'חסר'}",
             f"קלאסטרים · סכום במדד המקור · מרחק המרכז מהמחיר · חלק {basis}"]
    cluster_lines = []
    for cluster, group in zip(metrics['clusters'], snapshot['groups'], strict=True):
        low = group['members'][0]['raw_price']
        high = group['members'][-1]['raw_price']
        prices = low if low == high else f'{low}–{high}'
        cluster_lines.append(f"{prices} · ≈{cluster['raw_total']} · {cluster['distance_pct']:+.1f}% · "
                             f"≈{cluster['liquidity_share_pct']:.1f}%")
    lines.extend(cluster_lines)
    if overview:
        status = metrics['realized_24h_status']
        if status == 'component_total_conflict':
            lines.append('החיסולים שבוצעו ב־24 שעות הושמטו עקב סתירה בין הסכומים המוצגים.')
        else:
            realized = [overview.get('raw_realized_' + key) or 'חסר' for key in ('total','longs','shorts')]
            lines.append(f'חיסולים שבוצעו ב־24 שעות · סך {realized[0]} · לונג {realized[1]} · שורט {realized[2]}')
        largest = overview.get('raw_largest_value')
        if largest:
            lines.append(f"הרמה הבודדת הגדולה ביותר לפי האתר · {largest} · מחיר {overview.get('raw_largest_price') or 'חסר'}")
        for side, label in (('above','מעל'), ('below','מתחת')):
            members = metrics['strongest_observed_levels'][side]
            if members:
                candidates = ', '.join(m['raw_price'] + ' (' + m['raw_value'] + ')' for m in members)
                lines.append(f'הרמות החזקות מבין אלו שנקראו {label} למחיר · {candidates}')
        sidebar = overview.get('sidebar_levels', [])
        if sidebar:
            lines.append('רמות המפתח שהאתר מציג · מחיר · סכום · מרחק מהמחיר')
            lines.extend(f"{row['raw_price']} · {row['raw_value']} · {row.get('raw_distance_pct') or 'חסר'}"
                         for row in sidebar)
    full = '\n'.join(lines)
    if len(full) <= available_chars:
        return full
    # Reserve the audit summary's existing remaining space for a specific
    # disclosure; never cut an amount, percentage or cluster label mid-string.
    for keep in range(len(lines) - 1, -1, -1):
        omitted_groups = sum(line in cluster_lines for line in lines[keep:])
        note = f'קוצרו {len(lines)-keep} שורות, מהן {omitted_groups} קלאסטרים.'
        shortened = '\n'.join([*lines[:keep], note])
        if len(shortened) <= available_chars:
            return shortened
    return ''
