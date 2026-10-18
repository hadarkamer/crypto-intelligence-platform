"""Read displayed tooltips in the existing browser; never infer money from pixels.

Pixels locate the plot and candidate bands. Numbers come from rendered DOM text.
One vertical sweep covers the displayed latest column, not historical columns.
"""
from __future__ import annotations
import copy
import colorsys
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
  const candidates = [...document.querySelectorAll('.cg-toolti-box')].filter(e => {
    const r=e.getBoundingClientRect();
    if(!r.width || !r.height || r.bottom<=0 || r.top>=innerHeight)return false;
    for(let n=e;n;n=n.parentElement){
      const s=getComputedStyle(n);
      if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity)===0)return false;
    }
    return true;
  });
  // Models 1/2 put the timestamp outside the inner Price/Leverage wrapper.
  // Read the complete observed tooltip, never its innermost matching div.
  if(candidates.length!==1)return null;
  const text=candidates[0].innerText;
  return text.length<=220 && text.includes('Liquidation Leverage') ? text : null;
}'''

READ_EXPECTED_TOOLTIP = r'''expected => {
  const text = (''' + READ_TOOLTIP + r''')();
  if(!text)return false;
  const price=/\bPrice\s+(\d[\d,.]*)/.exec(text);
  return price && Math.abs(Number(price[1].replaceAll(',',''))-expected)<0.025 ? text : false;
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
        # The same canvas also contains the coloured time navigator. Treat
        # vertically separated regions independently, or its bottom becomes
        # a false heatmap row and the first bottom hover never produces data.
        regions = []
        for span in spans:
            if not regions or span[0]-regions[-1][-1][0] > 9:
                regions.append([])
            regions[-1].append(span)
        spans = max(regions, key=len, default=[])
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


def price_overlay_point(image_path, geometry, bounds):
    """Locate the observed green Supercharts legend marker in Models 1/2.

    Only the canvas header is eligible, never navigation links, map data or
    page controls. Ambiguous/missing markers produce no click. The retained
    screenshot still contains the price candles used by the analysis.
    """
    from PIL import Image
    left, top = int(geometry['x']), max(0, int(geometry['y']))
    right, bottom = int(geometry['x']+geometry['width']), bounds['top']-2
    if not 8 <= bottom-top <= 100:return None
    with Image.open(image_path) as original:
        region = original.crop((left,top,min(right,original.width),bottom)).convert('RGB')
    try:
        pixels=region.load()
        points={(x,y) for y in range(region.height) for x in range(region.width)
                if (lambda c: .38 <= c[0] <= .48 and c[1] >= .55 and c[2] >= .45)(
                    colorsys.rgb_to_hsv(*(v/255 for v in pixels[x,y])))}
        markers=[]
        while points:
            pending=[points.pop()];component=[]
            while pending:
                x,y=pending.pop();component.append((x,y))
                for point in ((x-1,y),(x+1,y),(x,y-1),(x,y+1)):
                    if point in points:points.remove(point);pending.append(point)
            xs,ys=zip(*component);w=max(xs)-min(xs)+1;h=max(ys)-min(ys)+1
            if 6 <= w <= 16 and 6 <= h <= 16 and len(component)>=w*h*.65:
                markers.append((left+(min(xs)+max(xs))/2,top+(min(ys)+max(ys))/2))
        return markers[0] if len(markers)==1 else None
    finally:region.close()


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



def close_heatmap_settings(page):
    """Close only the observed Model 3 settings drawer before image/geometry.

    Its normal Close button preserves settings while restoring the plot width.
    Account, consent and other dialogs are outside this function's scope.
    """
    panel = page.get_by_role('dialog', name='Heatmap settings', exact=True)
    count = panel.count()
    if not count or (count == 1 and not panel.is_visible()):
        return
    if count != 1:
        raise HoverError('hover_geometry_invalid')
    merged = panel.get_by_role('spinbutton', name='Merge Y ticks', exact=True)
    if merged.count() and merged.input_value() != '1':
        raise HoverError('hover_rows_merged')
    from playwright.sync_api import TimeoutError as BrowserTimeout
    try:
        panel.get_by_role('button', name='Close heatmap settings', exact=True).click(timeout=3000)
        if panel.is_visible():
            panel.wait_for(state='hidden', timeout=1500)
    except BrowserTimeout:
        # The isolated source closed the drawer but its action wait timed out.
        # Check the resulting state; never click again or ignore an open panel.
        if panel.is_visible():
            raise


def capture_column(page, image_path, geometry, *, model, timeframe):
    started = time.monotonic()
    # Model 3's live DOM/mouse operations cost about 0.3 s each on the
    # isolated source. 55 s truncated a valid, still-progressing sweep.
    # Allow its complete grid and final anchors within a bounded budget;
    # individual tooltip waits and the other models remain unchanged.
    max_seconds = 120 if model == 3 else MAX_SECONDS
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
    if model in (1,2):
        marker=price_overlay_point(image_path,geometry,bounds)
        if marker is not None:
            page.evaluate('(top) => window.scrollTo(0, top)',max(0,marker[1]-30))
            page.mouse.click(marker[0],marker[1]-page.evaluate('() => window.scrollY'))
    page.evaluate('(top) => window.scrollTo(0, top)', max(0, bounds['top']-20))
    scroll_y = page.evaluate('() => window.scrollY')
    x = bounds['right']-.25
    read_count = 0
    def wait_for_tooltip(expression, *, arg=None, timeout_ms):
        # One DOM evaluation per sample. The source returned visible values to
        # evaluate while the separate wait_for_function mechanism timed out.
        # Poll the SAME predicate inside this evaluation; return its value with
        # no JSHandle round-trips, extra preflight read or animation-frame wait.
        remaining = max_seconds - (time.monotonic()-started)
        if remaining <= 0:
            raise HoverError('hover_deadline')
        result = page.evaluate(r"""({value, timeout}) => new Promise((resolve,reject) => {
            const predicate = (""" + expression + r""");
            const end = performance.now() + timeout;
            function observe() {
                try {
                    const current = predicate(value);
                    if (current) { resolve(current); return; }
                    const left = end-performance.now();
                    if (left <= 0) { resolve(null); return; }
                    setTimeout(observe, Math.min(25,left));
                } catch (error) { reject(error); }
            }
            observe();
        })""", {'value': arg, 'timeout': max(1,min(timeout_ms,remaining*1000))})
        if time.monotonic()-started > max_seconds:
            raise HoverError('hover_deadline')
        if not result:
            raise HoverError('hover_update_timeout')
        return result

    def read(y, expected=None):
        nonlocal read_count
        if time.monotonic()-started > max_seconds:
            raise HoverError('hover_deadline')
        if expected is None:
            # Calibration must observe disappearance before another pointer move.
            # Row centres have an expected price, so they need no leave/re-enter.
            page.mouse.move(0, 0)
            wait_for_tooltip('() => ('+READ_TOOLTIP+')() === null', timeout_ms=450)
        page.mouse.move(x, y-scroll_y)
        text = wait_for_tooltip(
            READ_TOOLTIP if expected is None else READ_EXPECTED_TOOLTIP,
            arg=expected, timeout_ms=700)
        read_count += 1
        row = parse_tooltip(text)
        row['time_text'] = ' '.join(text.split()).split(' Price ')[0]
        return row

    def edge(y, direction):
        try:
            return read(y), y
        except HoverError as exc:
            if exc.reason != 'hover_update_timeout':raise
        # A painted border may not be interactive. Move at most one pixel
        # inward, less than the minimum accepted native row height below.
        # This is a boundary probe, never a restart of the source scan.
        return read(y+direction), y+direction

    top_y, bottom_y = bounds['top']+.25, float(bounds['bottom'])
    top_row, top_y = edge(top_y, 1)
    calibration = [top_row]
    transitions = []
    y = top_y
    # Establish the native price step without skipping any screen pixel. Stop
    # after three complete transitions; do not sweep the whole plot this way.
    while len(calibration) < 4:
        next_y = y+1
        if next_y >= bottom_y or next_y-top_y > 120:
            raise HoverError('hover_resolution_insufficient')
        row = read(next_y)
        if row['price'] != calibration[-1]['price']:
            transitions.append((y+next_y)/2)
            calibration.append(row)
        elif any(row[k] != calibration[-1][k] for k in ('value','unit','source_time')):
            raise HoverError('hover_snapshot_changed')
        y = next_y
    for i,row in enumerate(calibration):row['id']=f'r{i}'
    initial_step = validate_rows(calibration)
    if min(b-a for a,b in zip(transitions,transitions[1:])) < 2:
        raise HoverError('hover_resolution_insufficient')

    bottom_row, bottom_y = edge(bottom_y, -1)
    count = round((top_row['price']-bottom_row['price'])/initial_step)+1
    if not 4 <= count <= MAX_ROWS:raise HoverError('hover_row_count')
    # Locate the last boundary too. This prevents a one-pixel calibration error
    # from accumulating across the whole height of the chart.
    previous_y = bottom_y
    last_boundary = None
    for offset in range(1,122):
        target = bottom_y-offset
        if target <= top_y:break
        row = read(target)
        if row['price'] != bottom_row['price']:
            if abs(row['price']-bottom_row['price']-initial_step) > max(.025,initial_step*.002):
                raise HoverError('hover_price_grid_gap')
            last_boundary = (previous_y+target)/2
            break
        if any(row[k] != bottom_row[k] for k in ('value','unit','source_time')):
            raise HoverError('hover_snapshot_changed')
        previous_y = target
    if last_boundary is None:raise HoverError('hover_resolution_insufficient')
    height = (last_boundary-transitions[0])/(count-2)
    if height < 2 or any(abs(t-(transitions[0]+i*height))>1.1 for i,t in enumerate(transitions)):
        raise HoverError('hover_resolution_insufficient')
    price_step = (top_row['price']-bottom_row['price'])/(count-1)
    rows = []
    positions = []
    for i in range(count):
        target = top_y if i==0 else bottom_y if i==count-1 else transitions[0]+(i-.5)*height
        expected = top_row['price']-i*price_step
        row = calibration[i] if i<len(calibration) else bottom_row if i==count-1 else read(target,expected)
        if abs(row['price']-expected)>.025:raise HoverError('hover_price_grid_gap')
        row.update(id=f'r{i}',y_fraction=round((target-bounds['top'])/(bounds['bottom']-bounds['top']),6))
        rows.append(row);positions.append(target)
    price_step = validate_rows(rows)
    # Recheck distributed anchors after the sweep. A refresh must fail this
    # observation rather than combine values from different source moments.
    for i in (0,len(rows)//2,len(rows)-1):
        anchor = read(positions[i],rows[i]['price'])
        if any(anchor[k] != rows[i][k] for k in ('price','value','source_time','unit')):
            raise HoverError('hover_snapshot_changed')
    observed = datetime.fromisoformat(rows[0]['source_time'].replace('Z','+00:00'))
    age = (datetime.now(timezone.utc)-observed).total_seconds()
    if not -120 <= age <= 1200:raise HoverError('hover_source_stale')
    page.mouse.move(0,0)
    return {'version':VERSION,'metric':METRIC,'unit':rows[0]['unit'],
            'scope':'displayed_latest_column','heatmap_model':model,
            'timeframe':timeframe.upper(),'source_time':rows[0]['source_time'],
            'complete':True,'price_step':price_step,'rows':rows,
            'total_value':float(sum(Decimal(str(r['value'])) for r in rows)),
            'rounding_error':float(sum(Decimal(str(r['rounding_error'])) for r in rows)),
            'tooltip_reads':read_count,'row_height_pixels':round(height,3),'plot_bounds':bounds,
            'duration_seconds':round(time.monotonic()-started,3)}


def candidate_pools(snapshot, image_path):
    """Locate classic-palette bands; calculate only from the verified rows.

    Candidates still require visual confirmation in the existing single GPT call.
    Three samples within each row reduce boundary/candlestick pixel noise. A gap
    or colour change always splits a band; no interpolation creates quantities.
    """
    from PIL import Image
    validate_rows(snapshot['rows'])
    bounds = snapshot['plot_bounds']
    x = math.floor(bounds['right']-.25)
    height = snapshot['row_height_pixels']
    def colour(rgb):
        h,s,v=colorsys.rgb_to_hsv(*(c/255 for c in rgb))
        if .13 <= h < .24 and s >= .45 and v >= .55:return 'yellow'
        if .24 <= h < .48 and s >= .45 and v >= .4:return 'green'
        if .48 <= h < .66 and s >= .45 and v >= .35:return 'blue'
        return None
    groups=[]
    with Image.open(image_path) as original:
        image=original.convert('RGB')
        try:
            for row in snapshot['rows']:
                y=bounds['top']+row['y_fraction']*(bounds['bottom']-bounds['top'])
                samples=[colour(image.getpixel((x,round(max(bounds['top'],min(bounds['bottom'],y+d*height))))))
                         for d in (-.2,0,.2)]
                label=next((c for c in ('yellow','green','blue') if samples.count(c)>=2),None)
                if row['value']==0:label=None
                if groups and groups[-1][0]==label:groups[-1][1].append(row)
                else:groups.append((label,[row]))
        finally:image.close()
    prominent=any(label=='yellow' or (label=='green' and len(members)>1) for label,members in groups)
    pools=[]
    for label,members in groups:
        if label is None or (label=='green' and len(members)<2) or (label=='blue' and prominent):continue
        value=float(sum(Decimal(str(r['value'])) for r in members))
        pools.append({'id':f'c{len(pools)}','row_colour':label,
            'shape':'point' if len(members)==1 else 'range',
            'intensity':{'yellow':'many','green':'normal','blue':'few'}[label],
            'source_row_ids':[r['id'] for r in members],
            'price_low':members[-1]['price'],'price_high':members[0]['price'],
            'y_top':members[0]['y_fraction'],'y_bottom':members[-1]['y_fraction'],
            'source_value':value,
            'rounding_error':float(sum(Decimal(str(r['rounding_error'])) for r in members)),
            'percent':value/snapshot['total_value']*100 if snapshot['total_value'] else None})
    if not 1 <= len(pools) <= 40:raise HoverError('hover_candidates_unresolved')
    return pools


def prepare_candidates(images):
    # Called after capture_heatmaps returns and its browser is closed.
    for image in images:
        snapshot=image['hover_snapshot']
        snapshot['candidates']=candidate_pools(snapshot,image['image'])


def model_content(image):
    snapshot = image.get('hover_snapshot')
    if not snapshot: return []
    validate_rows(snapshot['rows'])
    payload = {k: snapshot[k] for k in ('version','metric','unit','scope','source_time','total_value')}
    payload['candidates'] = snapshot['candidates']
    return [{'type':'input_text','text':'Candidate bands from this same map and verified displayed hover rows. '
             'Verify every candidate visually and return its candidate_id. Do not invent or change its rows or numbers. '
             'If any boundary, colour or continuity is wrong, mark that candidate confidence=low. '
             'source_metric is NOT verified USD. Total covers the displayed current column only.\n'+json.dumps(payload)}]


def ground_analysis(raw, snapshot):
    validate_rows(snapshot['rows'])
    result = copy.deepcopy(raw)
    scans = result.get('scans')
    if not isinstance(scans,list) or len(scans)!=1: raise HoverError('hover_analysis_shape')
    from model1_price_range import validate_price_range
    current=validate_price_range(scans[0].get('current_price_range'))
    candidates={p['id']:p for p in snapshot['candidates']}
    selected=scans[0].get('prominent_levels')
    if (not isinstance(selected,list) or len(selected)!=len(candidates)
        or {p.get('candidate_id') for p in selected}!=set(candidates)):
        raise HoverError('hover_candidate_missing')
    used = set()
    by_id = {r['id']:r for r in snapshot['rows']}
    for level in selected:
        if level.get('confidence') != 'high':
            raise HoverError('hover_pool_uncertain')
        candidate=candidates[level['candidate_id']]
        if any(level.get(k)!=candidate[k] for k in ('row_colour','shape','intensity')):
            raise HoverError('hover_candidate_disagreement')
        ids = candidate['source_row_ids']
        level['source_row_ids']=ids
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
        # Classification is arithmetic against the fully validated reference
        # range. The normalizer still omits pools intersecting that range.
        if low>current['high']:level['side']='above'
        elif high<current['low']:level['side']='below'
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
