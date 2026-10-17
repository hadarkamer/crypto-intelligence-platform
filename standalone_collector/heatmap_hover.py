"""Read displayed tooltips in the existing browser; never infer money from pixels.

Pixels locate the plot only. Prices and quantities come from rendered DOM text.
One vertical sweep covers the displayed latest column, not historical columns.
"""
from __future__ import annotations
import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import re
import statistics
import time

VERSION = 'displayed-hover.v1'
MAX_ROWS = 1600
MAX_SECONDS = 55
METRIC = 'Liquidation Leverage'
NUMBER = r'\d+(?:,\d{3})*(?:\.\d+)?'
TOOLTIP = re.compile(r'^(?P<time>(?:\d{1,2} [A-Za-z]{3} \d{4}, \d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}))\s+Price\s+(?P<price>'+NUMBER+r')\s+Liquidation Leverage\s+(?P<amount>\$?'+NUMBER+r'\s*[KMB]?(?:\s*USD)?)$', re.I)


class HoverError(ValueError):
    """Fixed reason codes only; do not retain arbitrary source page text."""
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def parse_tooltip(text):
    match = TOOLTIP.fullmatch(' '.join(str(text).split()))
    if not match:
        raise HoverError('hover_text_invalid')
    timestamp = match['time']
    fmt = '%Y-%m-%d %H:%M' if timestamp[4:5] == '-' else '%d %b %Y, %H:%M'
    try:
        observed = datetime.strptime(timestamp, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        raise HoverError('hover_time_invalid') from None
    raw = match['amount'].replace(' ', '').replace(',', '').upper()
    unit = 'USD' if '$' in raw or raw.endswith('USD') else 'source_metric'
    token = raw.removeprefix('$').removesuffix('USD')
    suffix = token[-1:] if token[-1:] in ('K', 'M', 'B') else ''
    numeric = token[:-1] if suffix else token
    multiplier = {'': 1, 'K': 1000, 'M': 1000000, 'B': 1000000000}[suffix]
    amount = Decimal(numeric) * multiplier
    decimals = len(numeric.partition('.')[2])
    rounding = Decimal(multiplier) / (2 * 10**decimals)
    price = Decimal(match['price'].replace(',', ''))
    if not 0 < price < Decimal('1e12') or not 0 <= amount < Decimal('1e15'):
        raise HoverError('hover_number_invalid')
    return {'price': float(price), 'value': float(amount), 'unit': unit,
            'raw_price': match['price'], 'raw_value': match['amount'],
            'rounding_error': float(rounding),
            'source_time': observed.isoformat().replace('+00:00', 'Z')}


# Only visible DOM content. No framework state, canvas internals or private API.
READ_TOOLTIP = r'''() => {
  const candidates = [...document.querySelectorAll('div')].filter(e => {
    const t=e.innerText || ''; if(t.length>220 || !t.includes('Liquidation Leverage') || !/\bPrice\b/.test(t))return false;
    const r=e.getBoundingClientRect(),s=getComputedStyle(e);
    return r.width>0 && r.height>0 && s.display!=='none' && s.visibility!=='hidden' && Number(s.opacity)>0;
  });
  const leaves=candidates.filter(e=>!candidates.some(other=>other!==e && e.contains(other)));
  return leaves.length===1 ? leaves[0].innerText : null;
}'''


def plot_bounds(image_path, geometry):
    """Locate the large saturated heatmap rectangle within its saved canvas."""
    from PIL import Image
    with Image.open(image_path) as original:
        image = original.convert('RGB')
    try:
        if not isinstance(geometry, dict):
            raise HoverError('hover_geometry_missing')
        x0, y0 = int(geometry['x']), int(geometry['y'])
        x1 = min(image.width, x0 + int(geometry['width']))
        y1 = min(image.height, y0 + int(geometry['height']))
        if x0 < 0 or y0 < 0 or x1-x0 < 400 or not 200 <= y1-y0 <= 1600:
            raise HoverError('hover_geometry_invalid')
        pix = image.load()
        def coloured(x, y):
            values = pix[x, y]
            return max(values) - min(values) > 35 and max(values) > 45
        spans = []
        for y in range(y0, y1, 3):
            points = [x for x in range(x0, x1, 3) if coloured(x, y)]
            groups = []
            for x in points:
                if not groups or x-groups[-1][-1] > 12: groups.append([])
                groups[-1].append(x)
            if groups:
                group = max(groups, key=len)
                if group[-1]-group[0] >= (x1-x0)*.55:
                    spans.append((y, group[0], group[-1]))
        if len(spans) < 60:
            raise HoverError('hover_plot_not_found')
        left = round(statistics.median(s[1] for s in spans))
        right = round(statistics.median(s[2] for s in spans))
        top, bottom = spans[0][0], spans[-1][0]
        midx = (left+right)//2
        while top > y0 and coloured(midx, top-1): top -= 1
        while bottom+1 < y1 and coloured(midx, bottom+1): bottom += 1
        midy = (top+bottom)//2
        while right+1 < x1 and coloured(right+1, midy): right += 1
        if bottom-top < 180 or right-left < 350:
            raise HoverError('hover_plot_not_found')
        return {'left': left, 'right': right, 'top': top, 'bottom': bottom}
    finally:
        image.close()


def validate_rows(rows, *, complete=True):
    if not isinstance(rows, list) or not 3 <= len(rows) <= MAX_ROWS:
        raise HoverError('hover_row_count')
    previous = None
    gaps = []
    for index, row in enumerate(rows):
        parsed = parse_tooltip(f"{row['time_text']} Price {row['raw_price']} Liquidation Leverage {row['raw_value']}")
        if any(row.get(k) != parsed[k] for k in parsed) or row.get('id') != f'r{index}':
            raise HoverError('hover_evidence_mismatch')
        if row['source_time'] != rows[0]['source_time'] or row['unit'] != rows[0]['unit']:
            raise HoverError('hover_snapshot_changed')
        if previous is not None:
            gap = previous - row['price']
            if gap <= 0: raise HoverError('hover_row_order')
            gaps.append(gap)
        previous = row['price']
    step = statistics.median(gaps)
    # Displayed prices are rounded to cents. A missing row must not pass as coverage.
    if complete and any(abs(gap-step) > max(.025, step*.002) for gap in gaps):
        raise HoverError('hover_price_grid_gap')
    return step


def capture_column(page, image_path, geometry, *, model, timeframe):
    started = time.monotonic()
    bounds = plot_bounds(image_path, geometry)
    # UTC makes both visible tooltip timestamp formats unambiguous.
    if page.evaluate('() => Intl.DateTimeFormat().resolvedOptions().timeZone') != 'UTC':
        raise HoverError('hover_timezone_not_utc')
    merged = page.get_by_role('spinbutton', name='Merge Y ticks', exact=True)
    if merged.count() and merged.input_value() != '1':
        raise HoverError('hover_rows_merged')
    viewport = page.viewport_size
    if not viewport or bounds['bottom']-bounds['top']+40 > viewport['height']:
        raise HoverError('hover_plot_clipped')
    page.evaluate('(top) => window.scrollTo(0, top)', max(0, bounds['top']-20))
    scroll_y = page.evaluate('() => window.scrollY')
    x = bounds['right']-.25
    rows = []
    first = None
    step = 1
    last_change_y = None
    pixel_spans = []
    def read(y):
        if time.monotonic()-started > MAX_SECONDS:
            raise HoverError('hover_deadline')
        # Force the previous tooltip to leave before reading the next one.
        page.mouse.move(0, 0)
        page.wait_for_function('() => ('+READ_TOOLTIP+')() === null', timeout=450)
        page.mouse.move(x, y-scroll_y)
        handle = page.wait_for_function(READ_TOOLTIP, timeout=700, polling='raf')
        text = handle.json_value(); handle.dispose()
        row = parse_tooltip(text)
        row['time_text'] = ' '.join(text.split()).split(' Price ')[0]
        return row
    y = bounds['top']+.25
    while y <= bounds['bottom']:
        row = read(y)
        if rows and row['price'] == rows[-1]['price']:
            if any(row[k] != rows[-1][k] for k in ('value','source_time','unit')):
                raise HoverError('hover_snapshot_changed')
        else:
            if rows and row['price'] >= rows[-1]['price']:
                raise HoverError('hover_row_order')
            if last_change_y is not None:
                pixel_spans.append(y-last_change_y)
                # The first row may be clipped at the upper plot boundary.
                # Subsequent rows must span at least two screen pixels; otherwise
                # regular skipped subpixel rows could masquerade as a complete grid.
                if len(pixel_spans)>1 and pixel_spans[-1]<2:
                    raise HoverError('hover_resolution_insufficient')
            last_change_y = y
            row.update(id=f'r{len(rows)}', y_fraction=round((y-bounds['top'])/(bounds['bottom']-bounds['top']),6))
            rows.append(row)
            if first is None: first = (y, row.copy())
            if len(rows) > MAX_ROWS: raise HoverError('hover_row_count')
            if len(pixel_spans) >= 4:
                step = max(1, min(4, math.floor(min(pixel_spans)/2)))
        next_y = min(y+step, float(bounds['bottom']))
        if next_y == y: break
        y = next_y
    price_step = validate_rows(rows)
    # Subpixel source rows cannot be exhaustively sampled using screen positions.
    if (bounds['bottom']-bounds['top'])/(len(rows)-1) < 1:
        raise HoverError('hover_resolution_insufficient')
    final_anchor = read(first[0])
    if any(final_anchor[k] != first[1][k] for k in ('price','value','source_time','unit')):
        raise HoverError('hover_snapshot_changed')
    observed = datetime.fromisoformat(rows[0]['source_time'].replace('Z','+00:00'))
    age = (datetime.now(timezone.utc)-observed).total_seconds()
    if not -120 <= age <= 1200: raise HoverError('hover_source_stale')
    page.mouse.move(0, 0)
    return {'version': VERSION, 'metric': METRIC, 'unit': rows[0]['unit'],
            'scope': 'displayed_latest_column', 'heatmap_model': model,
            'timeframe': timeframe.upper(), 'source_time': rows[0]['source_time'],
            'complete': True, 'price_step': price_step, 'rows': rows,
            'total_value': float(sum(Decimal(str(r['value'])) for r in rows)),
            'rounding_error': float(sum(Decimal(str(r['rounding_error'])) for r in rows)),
            'duration_seconds': round(time.monotonic()-started,3)}


def model_content(image):
    snapshot = image.get('hover_snapshot')
    if not snapshot: return []
    validate_rows(snapshot['rows'])
    payload = {k: snapshot[k] for k in ('version','metric','unit','scope','source_time','total_value')}
    payload['rows'] = [{k:r[k] for k in ('id','price','value','y_fraction')} for r in snapshot['rows']]
    return [{'type':'input_text','text':'Verified displayed hover rows from this same map. '
             'Select source_row_ids for each visual pool. Prices and arithmetic will be enforced by code. '
             'source_metric is NOT verified USD. Total covers the displayed current column only.\n'+json.dumps(payload)}]


def ground_analysis(raw, snapshot):
    validate_rows(snapshot['rows'])
    result = copy.deepcopy(raw)
    scans = result.get('scans')
    if not isinstance(scans,list) or len(scans)!=1: raise HoverError('hover_analysis_shape')
    used = set()
    by_id = {r['id']:r for r in snapshot['rows']}
    for level in scans[0].get('prominent_levels',[]):
        if level.get('confidence') != 'high':
            raise HoverError('hover_pool_uncertain')
        ids = level.get('source_row_ids')
        if not isinstance(ids,list) or not ids or len(ids)!=len(set(ids)) or any(i not in by_id for i in ids):
            raise HoverError('hover_pool_rows_invalid')
        members = sorted((by_id[i] for i in ids), key=lambda r:r['price'])
        positions = sorted(int(i[1:]) for i in ids)
        if positions != list(range(positions[0],positions[-1]+1)) or used.intersection(ids):
            raise HoverError('hover_pool_overlap_or_gap')
        if len(members)>1 and any(r['value']==0 for r in members):
            raise HoverError('hover_pool_empty_gap')
        used.update(ids)
        low, high = members[0]['price'], members[-1]['price']
        # Do not silently broaden a selected visual range to unrelated source rows.
        if level.get('shape')=='point' and len(members)!=1:
            raise HoverError('hover_point_multiple_rows')
        if level.get('shape')=='range' and len(members)<2:
            raise HoverError('hover_range_single_row')
        level.update(price_low=low,price_high=high,
            prices=[low] if len(members)==1 else [],
            price_precision='verified_label',evidence_basis='tooltip',uncertainty_usd=0,
            confidence='high',price_evidence=f"Price {low} — Price {high}; displayed tooltip rows {ids[0]} to {ids[-1]}")
    return result


def attach_measurements(result, raw, snapshot, *, image):
    validate_rows(snapshot['rows'])
    output = copy.deepcopy(snapshot)
    output['rows'] = [{k:r[k] for k in ('id','price','value','raw_price','raw_value','rounding_error')}
                      for r in snapshot['rows']]
    output.update(source_url=result['source_url'],captured_at=result['captured_at'],
        artifact_id=result['run_id'],sha256=hashlib.sha256(image).hexdigest(),
        observed_price_range=result['observed_price_range'])
    candidates = raw['scans'][0]['prominent_levels']
    by_id = {r['id']:r for r in snapshot['rows']}
    for zone in result['zones']:
        matches = [p for p in candidates if p['price_low']==zone['price_low'] and p['price_high']==zone['price_high'] and p['side']==zone['side']]
        if len(matches)!=1: raise HoverError('hover_pool_binding')
        ids = matches[0]['source_row_ids']
        zone['source_row_ids'] = ids
        zone['source_value'] = float(sum(Decimal(str(by_id[i]['value'])) for i in ids))
    result['hover_snapshot'] = output
    # Monetary availability is deliberately unchanged unless explicit source units exist.
    return result
