"""Offline, evidence-bound extraction of CURRENT CoinGlass heatmap band cores.

No browser, network, inference, environment credentials or database operations.
The caller supplies image-specific, verified geometry and >=3 price-axis anchors.
This module is NOT a replacement for verifying model/timeframe or reading ticks.
Colours are calibrated to the image's own colour bar, never dollar liquidity.
Every accepted band has support in the final columns, not just historical pixels.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from functools import lru_cache
from hashlib import sha256
from io import BytesIO
import math
from statistics import median
from typing import Sequence

from PIL import Image

VERSION = 'current-edge-core.v1'
MAX_IMAGE_BYTES = 4 * 1024 * 1024
MAX_IMAGE_PIXELS = 16_000_000


class EvidenceError(ValueError):
    """Fail closed on unverifiable geometry, calibration or input identity."""


@dataclass(frozen=True)
class Policy:
    stripe_width: int = 20
    terminal_width: int = 4
    border_inset: int = 1
    min_support: float = 0.80
    min_palette_level: float = 0.40
    max_rgb_distance: float = 26.0
    core_fraction_of_peak: float = 0.80
    min_core_rows: int = 2
    many_level: float = 0.80
    normal_level: float = 0.55
    round_to: int = 50
    max_anchor_residual_px: float = 1.5

    def validate(self):
        ints = (self.stripe_width, self.terminal_width, self.border_inset,
                self.min_core_rows, self.round_to)
        if any(type(x) is not int or x <= 0 for x in ints):
            raise EvidenceError('invalid_policy')
        if not 2 <= self.terminal_width <= self.stripe_width <= 100:
            raise EvidenceError('invalid_policy')
        vals = (self.min_support, self.min_palette_level, self.core_fraction_of_peak,
                self.many_level, self.normal_level, self.max_rgb_distance,
                self.max_anchor_residual_px)
        if any(type(x) not in (int, float) or not math.isfinite(x) for x in vals):
            raise EvidenceError('invalid_policy')
        if not (0.75 <= self.min_support <= 1 and
                0 < self.min_palette_level <= self.normal_level < self.many_level < 1 and
                0.6 <= self.core_fraction_of_peak <= 1 and
                0 < self.max_rgb_distance <= 35 and
                0 < self.max_anchor_residual_px <= 3):
            raise EvidenceError('invalid_policy')


@dataclass(frozen=True)
class Calibration:
    intercept: float
    slope: float
    residual_px: float
    anchors: tuple[tuple[float, float], ...]

    def price(self, y: float) -> float:
        return self.intercept + self.slope * y


def _number(v):
    if type(v) not in (int, float) or not math.isfinite(v):
        raise EvidenceError('non_finite_number')
    return float(v)


def _box(value, size, *, minimum=(1, 1)):
    if (not isinstance(value, (tuple, list)) or len(value) != 4 or
            any(type(x) is not int for x in value)):
        raise EvidenceError('invalid_rectangle')
    l, t, r, b = value
    w, h = size
    if not (0 <= l < r <= w and 0 <= t < b <= h and
            r-l >= minimum[0] and b-t >= minimum[1]):
        raise EvidenceError('rectangle_outside_image')
    return l, t, r, b


def calibrate(anchors, plot, policy=Policy()):
    policy.validate()
    if not isinstance(anchors, (list, tuple)) or not 3 <= len(anchors) <= 20:
        raise EvidenceError('three_axis_anchors_required')
    pts = []
    for pair in anchors:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise EvidenceError('invalid_axis_anchor')
        y, p = map(_number, pair)
        if not plot[1]-2 <= y <= plot[3]+2 or p <= 0:
            raise EvidenceError('axis_anchor_outside_plot')
        pts.append((y, p))
    pts.sort()
    if any(y2 <= y1 or p2 >= p1 for (y1,p1),(y2,p2) in zip(pts,pts[1:])):
        raise EvidenceError('axis_not_strictly_monotonic')
    if pts[-1][0]-pts[0][0] < (plot[3]-plot[1]) * 0.5:
        raise EvidenceError('axis_span_too_small')
    ym = sum(y for y,p in pts)/len(pts)
    pm = sum(p for y,p in pts)/len(pts)
    slope = sum((y-ym)*(p-pm) for y,p in pts)/sum((y-ym)**2 for y,p in pts)
    intercept = pm-slope*ym
    residual = max(abs(p-(intercept+slope*y))/abs(slope) for y,p in pts)
    if residual > policy.max_anchor_residual_px:
        raise EvidenceError('axis_not_linear_or_tick_misread')
    result = Calibration(intercept, slope, residual, tuple(pts))
    if result.price(plot[3]) <= 0:
        raise EvidenceError('non_positive_axis')
    return result


def _median_rgb(pixels):
    return tuple(int(median([p[i] for p in pixels])) for i in range(3))


def _palette(image, legend):
    l,t,r,b = legend
    if b-t < 64 or r-l < 3:
        raise EvidenceError('colour_bar_too_small')
    x = (l+r)//2
    # Interior sampling excludes rounded caps; y increases downward, value decreases.
    colors = [_median_rgb([image.getpixel((xx, yy)) for xx in range(x-1,x+2)])
              for yy in range(b-3, t+1, -1)]
    low, high = colors[0], colors[-1]
    # Explicitly limited to the familiar purple/teal/green/yellow palette.
    # Other palettes / white or masked bars are rejected, not silently interpreted.
    if not (low[1] < 80 and low[0] > low[1] and low[2] > low[1] and
            high[0] > 150 and high[1] > 170 and high[2] < 130):
        raise EvidenceError('unsupported_or_obstructed_palette')
    # Compact adjacent equal colours while preserving normalized legend position.
    palette=[]
    for i, color in enumerate(colors):
        position=i/(len(colors)-1)
        if not palette or color != palette[-1][0]:
            palette.append((color,position))
    if len(palette) < 32:
        raise EvidenceError('insufficient_palette_variation')
    return palette


def _runs(mask, offset=0):
    start=None
    for i,on in enumerate([*mask,False]):
        if on and start is None: start=i
        if not on and start is not None:
            yield start+offset, i+offset  # half-open pixel rows
            start=None


def extract_current_cores(image_bytes: bytes, *, expected_sha256: str,
                          plot: Sequence[int], legend: Sequence[int],
                          anchors, price_interval: Sequence[float],
                          identity: dict, policy=Policy()) -> dict:
    """Measure band cores using independently supplied image-specific anchors.

    Geometry is in ORIGINAL image coordinates, rectangles are half-open.
    No hardcoded chart size, dollar levels or fixture-specific decision rules.
    The current-price interval is external evidence, not inferred from heat colours.
    The output is a candidate observation for review, NEVER a database write.
    """
    policy.validate()
    if (not isinstance(image_bytes, bytes) or len(image_bytes) > MAX_IMAGE_BYTES or
            not image_bytes.startswith(b'\x89PNG\r\n\x1a\n')):
        raise EvidenceError('invalid_png')
    digest=sha256(image_bytes).hexdigest()
    if not isinstance(expected_sha256,str) or digest != expected_sha256:
        raise EvidenceError('image_hash_mismatch')
    if (not isinstance(identity,dict) or set(identity) != {'symbol','model','timeframe'} or
            identity.get('symbol') != 'BTC' or type(identity.get('model')) is not int or
            identity['model'] not in (1,2,3) or identity.get('timeframe') not in ('12H','24H','48H')):
        raise EvidenceError('unsupported_identity')
    if not isinstance(price_interval,(list,tuple)) or len(price_interval)!=2:
        raise EvidenceError('invalid_price_interval')
    low_price,high_price=map(_number,price_interval)
    if not 0 < low_price < high_price or (high_price-low_price)/((high_price+low_price)/2)>0.01:
        raise EvidenceError('invalid_price_interval')
    with Image.open(BytesIO(image_bytes)) as im:
        if im.format!='PNG' or im.width*im.height > MAX_IMAGE_PIXELS:
            raise EvidenceError('image_size_limit')
        if im.mode not in ('RGB','RGBA'):
            raise EvidenceError('unsupported_png_mode')
        if im.mode=='RGBA' and im.getchannel('A').getextrema() != (255,255):
            raise EvidenceError('transparent_source')
        image=im.convert('RGB')
    plot=_box(plot,image.size,minimum=(200,160))
    legend=_box(legend,image.size,minimum=(3,64))
    if not (legend[2] <= plot[0] or legend[0] >= plot[2]):
        raise EvidenceError('legend_overlaps_plot')
    axis=calibrate(anchors,plot,policy)
    if not axis.price(plot[3]) <= low_price < high_price <= axis.price(plot[1]):
        raise EvidenceError('price_interval_outside_axis')
    palette=_palette(image,legend)
    max_d2=policy.max_rgb_distance**2

    @lru_cache(maxsize=20000)
    def colour_level(rgb):
        distance,level=min((sum((a-b)**2 for a,b in zip(rgb,c)),p) for c,p in palette)
        return level if distance <= max_d2 else None

    right=plot[2]-policy.border_inset
    left=right-policy.stripe_width
    pre_left=left-policy.stripe_width
    if pre_left < plot[0]: raise EvidenceError('plot_too_narrow')
    profile=[]
    support=[]
    valid_palette_rows=0
    for y in range(plot[1],plot[3]):
        values=[colour_level(image.getpixel((x,y))) for x in range(pre_left,right)]
        current=values[policy.stripe_width:]
        before=values[:policy.stripe_width]
        terminal=current[-policy.terminal_width:]
        valid=sum(v is not None for v in current)/len(current)
        valid_palette_rows += valid >= policy.min_support
        lit=lambda seq: sum(v is not None and v>=policy.min_palette_level for v in seq)/len(seq)
        ratio=lit(current)
        persists=(ratio >= policy.min_support and lit(before) >= policy.min_support and
                  lit(terminal) >= policy.min_support and
                  current[-1] is not None and current[-1]>=policy.min_palette_level)
        profile.append(median([v for v in current if v is not None]) if persists else 0.0)
        support.append(ratio if persists else 0.0)
    # White overlays/axes accidentally passed as the edge cannot be successful.
    if valid_palette_rows/len(profile) < 0.70:
        raise EvidenceError('edge_not_readable_in_palette')
    bands=[]
    omitted=[]
    core_mask=[False]*len(profile)
    for env_start, env_end in _runs([v>=policy.min_palette_level for v in profile]):
        vals=profile[env_start:env_end]
        ordered=sorted(vals)
        peak=ordered[min(len(ordered)-1, int(0.95*(len(ordered)-1)))]
        cutoff=max(policy.min_palette_level,peak*policy.core_fraction_of_peak)
        for start,end in _runs([v>=cutoff for v in vals],env_start):
            if end-start<policy.min_core_rows: continue
            y_top,y_bottom=plot[1]+start,plot[1]+end
            raw_low=axis.price(y_bottom-0.5)
            raw_high=axis.price(y_top-0.5)
            # Conservative outward rounding: never invent fine-resolution ticks.
            lo=math.floor(raw_low/policy.round_to)*policy.round_to
            hi=math.ceil(raw_high/policy.round_to)*policy.round_to
            if lo<=high_price and hi>=low_price:
                omitted.append({'reason':'overlaps_current_price','pixel_y':[y_top,y_bottom]})
                continue
            side='above' if lo>high_price else 'below'
            q=sorted(profile[start:end])[(end-start)//2]
            intensity='many' if q>=policy.many_level else ('normal' if q>=policy.normal_level else 'few')
            region={'side':side,'price_low':lo,'price_high':hi,'intensity':intensity,
                    'pixel_y':[y_top,y_bottom], 'edge_columns':[left,right],
                    'unrounded_bounds':[round(raw_low,2),round(raw_high,2)],
                    'envelope_pixel_y':[plot[1]+env_start,plot[1]+env_end],
                    'legend_fraction':round(q,4),'min_row_support':round(min(support[start:end]),4),
                    'boundary_definition':'core >= 80% of local envelope peak; no gap bridging'}
            bands.append(region)
            for row in range(start,end):core_mask[row]=True
    # Rounded presentation ranges must not duplicate or overlap each other.
    # Keep pixel bands separate; flag overlapping rounded labels, don't merge them.
    for i,band in enumerate(bands):
        band['rounded_overlap']=any(i!=j and band['side']==other['side'] and
            max(band['price_low'],other['price_low']) < min(band['price_high'],other['price_high'])
            for j,other in enumerate(bands))
    for side in ('above','below'):
        group=[b for b in bands if b['side']==side]
        for rank,b in enumerate(sorted(group,key=lambda b:b['legend_fraction'],reverse=True),1):b['strength_rank']=rank
    bands.sort(key=lambda b:(b['side']!='above', b['price_low'] if b['side']=='above' else -b['price_high']))
    return {'schema_version':VERSION,'identity':identity,'source_sha256':digest,
            'image_size':list(image.size),'plot':list(plot),'legend':list(legend),
            'price_interval':[low_price,high_price],'axis':asdict(axis),'policy':asdict(policy),
            'bands':bands,'omitted':omitted,'scan_calls':0,'model_calls':0,'sheet_writes':0,
            'intensity_basis':'relative colour-bar position; engineering policy, not liquidity dollars',
            'calibration_requirement':'anchors must be verified for THIS image; no live auto-calibration claimed',
            '_profile':profile,'_core_mask':core_mask}


def audit_saved_zones(measurement: dict, saved_zones: list[dict]) -> list[dict]:
    """Compare old intervals to measured support, never rewrite old observations."""
    out=[]
    axis=Calibration(**measurement['axis'])
    top=measurement['plot'][1]
    p=measurement['policy']
    for z in saved_zones:
        lo,hi=_number(z['price_low']),_number(z['price_high'])
        if not 0<lo<=hi or z.get('side') not in ('above','below'):
            raise EvidenceError('invalid_saved_zone')
        ys=max(0,math.floor((hi-axis.intercept)/axis.slope)-top)
        ye=min(len(measurement['_profile']),math.ceil((lo-axis.intercept)/axis.slope)-top+1)
        vals=measurement['_profile'][ys:ye]
        if not vals:raise EvidenceError('saved_zone_outside_axis')
        fraction=sum(v>=p['min_palette_level'] for v in vals)/len(vals)
        bright=sum(v>=p['many_level'] for v in vals)/len(vals)
        match=[b for b in measurement['bands'] if b['side']==z['side'] and
            max(lo,b['unrounded_bounds'][0]) < min(hi,b['unrounded_bounds'][1])]
        reason=('unsupported_current_edge' if fraction==0 else
                'no_high_intensity_support' if z.get('intensity')=='many' and bright==0 else
                'mixed_or_shifted_bounds' if fraction<0.8 or len(match)!=1 else 'supported_region')
        out.append({**z,'edge_support_fraction':round(fraction,4),
                    'high_intensity_fraction':round(bright,4),'core_intersections':len(match),
                    'verdict':reason})
    return out


def public_report(measurement):
    return {k:v for k,v in measurement.items() if not k.startswith('_')}
