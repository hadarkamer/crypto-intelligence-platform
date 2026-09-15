"""Prepare a full-image/detail pair from one existing chart PNG.

This module does not open websites or call a model. The existing scanner appends
its output to the same input message and still requests one result per horizon.
"""
import base64
import hashlib
import math
from pathlib import Path
from PIL import Image
from image_detail import enlarge_png

DETAIL_TEXT='''This is an enlarged crop of the SAME screenshot directly above,
not a second timeframe or independent observation. Use the last visible candle
and the adjacent PRICE-axis ticks to estimate the current chart price. Do not
confuse an axis extreme, a historical candle, a heatmap band or the monetary
colour legend with the current price. Return ONE scan for the full/crop pair.
Use the full image for symbol/model/timeframe verification. Keep uncertainty:
do not assign high confidence unless the candle and bracketing price ticks are
unambiguous. Enlargement alone is not new evidence and must not raise confidence.
Do not invent digits or an exact price label absent from the image.'''


def invalid():
    from model1_execution import StageFailure
    raise StageFailure('image_evidence_invalid')


def make_detail_file(image_path,geometry):
    """Only crop and resize saved pixels; no re-rendering or second screenshot."""
    path=Path(image_path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size>4*1024*1024: invalid()
    try:
        x,y,w,h,pw=(geometry[k] for k in ('x','y','width','height','page_width'))
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (x,y,w,h,pw)): invalid()
        with Image.open(path) as source:
            iw,ih=source.size
        if x<0 or y<0 or w<400 or h<200 or pw<400: invalid()
        scale=iw/pw
        if not 0.5<=scale<=3 or (x+w)*scale>iw+3 or (y+h)*scale>ih+3: invalid()
        box=(max(0,math.floor((x+0.6*w)*scale)),max(0,math.floor((y-12)*scale)),
            min(iw,math.ceil((x+w+120)*scale)),min(ih,math.ceil((y+h+12)*scale)))
        raw,metadata=enlarge_png(path.read_bytes(),box)
    except (TypeError,KeyError,ValueError,OSError,OverflowError):
        invalid()
    target=path.with_name(path.stem+'_price_detail.png')
    target.write_bytes(raw);target.chmod(0o600)
    return {'path':str(target),**metadata}


def verified_detail(image):
    metadata=image.get('price_detail')
    if metadata is None:return None
    if not isinstance(metadata,dict):invalid()
    source=Path(image['image']);target=Path(metadata.get('path',''))
    if target.resolve()!=source.with_name(source.stem+'_price_detail.png').resolve():invalid()
    for path in (source,target):
        if path.is_symlink() or not path.is_file() or path.stat().st_size>4*1024*1024:invalid()
    raw=target.read_bytes()
    if (hashlib.sha256(source.read_bytes()).hexdigest()!=metadata.get('source_sha256')
        or hashlib.sha256(raw).hexdigest()!=metadata.get('sha256')
        or not raw.startswith(b'\x89PNG\r\n\x1a\n') or metadata.get('scale')!=2):invalid()
    return raw,metadata


def detail_content(image):
    verified=verified_detail(image)
    if verified is None:return []
    raw,_=verified
    return [{'type':'input_text','text':DETAIL_TEXT},
        {'type':'input_image','detail':'original',
         'image_url':'data:image/png;base64,'+base64.b64encode(raw).decode('ascii')}]


def detail_provenance(image):
    verified=verified_detail(image)
    if verified is None:return None
    _,metadata=verified
    return {k:metadata[k] for k in ('source_sha256','sha256','crop_box','scale','dimensions','method')}
