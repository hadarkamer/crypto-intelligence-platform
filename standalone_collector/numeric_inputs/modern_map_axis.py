"""Calibrate the right edge from numerical axis labels and the printed last-price tag."""
from itertools import groupby
import re

from modern_map_evidence import PRICE, number, sample_groups, verified_member
from model1_execution import StageFailure


def axis_marks(page,box,prior,current):
    from modern_map_tooltip import _DOM
    dom=page.evaluate(_DOM,list(box))
    def painted(marks):
        return {(v[0],round(v[1]),round(v[4])) for v in marks
                if len(v)>4 and re.fullmatch(PRICE,v[0])}
    # Moving the cursor changes crosshair values. Only stable axis labels
    # from both positions may supplement the pointer-off DOM observations.
    return [*dom,*sorted(painted(prior) & painted(current))]


def axis_scale(image,box,bounds,marks,*,reference=None,audit=None):
    def fail(reason):
        if audit is not None:audit['condition']=reason
        return None
    left,top,right,bottom=bounds
    axis_end=min(image.width,box[0]+box[2])
    if axis_end-right<14:return fail('axis_geometry_invalid')
    # The source renders one light, labelled last-price tag beside its dashed
    # horizontal reference. Require the clean (cursor-off) source pixels.
    def light(y):
        return sum(min(pixel)>178 and max(pixel)-min(pixel)<12 for pixel in
                   (image.getpixel((x,y)) for x in range(right+1,axis_end)))>(axis_end-right)*.35
    runs=[list(rows) for state,rows in groupby(range(top,bottom+1),key=light) if state]
    bands=[r for r in runs if 5<=len(r)<=min(45,(bottom-top)*.15)]
    if audit is not None:
        audit['tag_bands']=[[band[0],band[-1]] for band in bands]
    if len(bands)!=1:return fail('tag_missing_or_ambiguous')
    band=bands[0]
    center=(band[0]+band[-1])/2
    visible=[(raw,float(number(raw)[0]),y) for raw,x,y,*_ in marks if isinstance(raw,str)
             and re.fullmatch(PRICE,raw) and right<x<axis_end and top<=y<=bottom]
    candidates={v:raw for raw,v,y in visible if band[0]-1<=y<=band[-1]+1}
    if audit is not None:
        audit.update(tag_y=center,reference_candidates=list(candidates.values()),
                     ticks=[dict(text=raw,y=y) for raw,_,y in visible if not band[0]-1<=y<=band[-1]+1])
    if len(candidates)>1:return fail('reference_conflict')
    if reference is None and not candidates:return fail('reference_missing')
    if reference is not None:
        # The overview explicitly labels this field "Reference (last price)".
        # Direct ticks plus the reference line can calibrate it without OCR.
        from modern_map_cells import _agree
        if candidates and not _agree(number(reference),number(next(iter(candidates.values())))):
            return fail('reference_conflict')
    raw_last=reference or next(iter(candidates.values()))
    last=float(number(raw_last)[0])
    if audit is not None:audit['reference']=raw_last
    # Repeated identical labels (canvas plus DOM) supply one coordinate.
    ticks={}
    for raw,v,y in visible:
        if v==last or band[0]-1<=y<=band[-1]+1:continue
        if v in ticks and abs(ticks[v]-y)>2:return fail('tick_position_conflict')
        ticks[v]=y
    if len(ticks)<4:return fail('ticks_insufficient')
    lo,hi=min(ticks),max(ticks)
    if not lo<last<hi or ticks[lo]-ticks[hi]<(bottom-top)*.60:return fail('tick_span_insufficient')
    slope=(ticks[hi]-ticks[lo])/(hi-lo)
    def where(price):return ticks[lo]+(price-lo)*slope
    tick_error=max(abs(where(p)-y) for p,y in ticks.items())
    tag_error=abs(where(last)-center)
    if audit is not None:
        audit.update(tick_residual=tick_error,tag_residual=tag_error)
    if tick_error>1.6 or tag_error>2.2:return fail('axis_position_conflict')
    # A dashed line must pass through the clean tag's centre. Contrast is
    # measured against its neighbouring rows rather than a hard-coded colour.
    def dashed(y):
        changes=[abs(image.getpixel((x,y))[1]-image.getpixel((x,y-2))[1])>32
                 and abs(image.getpixel((x,y))[1]-image.getpixel((x,y+2))[1])>32 for x in range(left,right-2)]
        transitions=sum(a!=b for a,b in zip(changes,changes[1:]))
        return len(changes)*.12<sum(changes)<len(changes)*.85 and transitions>len(changes)*.05
    if not any(dashed(y) for y in range(max(top+2,round(center)-2),min(bottom-1,round(center)+3))):
        return fail('reference_line_missing')
    if audit is not None:audit['condition']='verified'
    return raw_last,where


def match_displayed_bins(image,box,bounds,marks,overview,model,anchor,step):
    """Return the sidebar only when it covers every native significant bin and maximum."""
    if not overview.get('raw_largest_value') or not overview.get('sidebar_levels'):
        return None
    if {'sidebar_levels','sidebar_shape','raw_largest_value'} & set(overview.get('ambiguous_fields',())):
        return None
    scale=axis_scale(image,box,bounds,marks)
    if scale is None:return None
    raw_last,where=scale
    try:
        height,runs=sample_groups(image,bounds,model,anchor,step,where,float(number(raw_last)[0]))
    except StageFailure:
        return None
    # A one-pixel uncertainty may not change the promoted rows. Interpolate
    # displayed coordinate resolution, never the amount or individual prices.
    signature=lambda groups:[(c,[round(v['price'],5) for v in rs]) for c,rs in groups]
    try:
        for dy in (-1,1):
            _,shifted=sample_groups(image,bounds,model,anchor,step,lambda p:where(p)+dy,float(number(raw_last)[0]))
            if signature(shifted)!=signature(runs):return None
    except StageFailure:
        return None
    rows=overview['sidebar_levels']
    native_max=number(overview['raw_largest_value'],True)[0]
    book={}
    for colour,members in runs:
        for row in members:
            matches=[r for r in rows if abs(float(number(r['raw_price'])[0])-row['price'])<=step*.35]
            if len(matches)!=1:return None
            source=matches[0]
            member={k:source[k] for k in ('raw_price','raw_value')}
            verified_member(row,member,step)
            price=number(member['raw_price'])[0]
            # The retained chart independently corroborates its tied maximum.
            # A weaker numerical sidebar row needs its own latest-column
            # reading before it can bypass Hover or Measure.
            if price in book or number(member['raw_value'],True)[0]!=native_max:return None
            book[price]=member
    leaders=[r for r in rows if number(r['raw_value'],True)[0]==native_max]
    if not leaders or any(number(r['raw_price'])[0] not in book for r in leaders):return None
    return raw_last,height,runs,book,book[number(leaders[0]['raw_price'])[0]]
