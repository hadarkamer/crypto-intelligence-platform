"""Read the printed price tag and ticks once, using the bundled English Tesseract."""
import csv
from hashlib import sha256
from itertools import groupby
from math import ceil, floor
from statistics import median
from typing import NamedTuple
from io import BytesIO, StringIO
import os
from pathlib import Path
import re
import signal
import subprocess
import time

from PIL import Image, ImageOps

from modern_map_axis import axis_scale
from modern_map_evidence import PRICE

BINARY = Path(__file__).resolve().parent / 'ocr_runtime' / 'tesseract'
TIMEOUT = 10.0


def _tsv(png, timeout, audit, *, segmentation=11):
    # Only the OCR process is limited. Do not pass the collector's environment
    # (database credentials, session keys, etc.) to the binary.
    command = ['/usr/bin/prlimit', '--as=536870912', '--cpu=8:9', '--', str(BINARY),
               'stdin','stdout','-l','eng','--oem','1','--psm',str(segmentation),
               '-c','tessedit_char_whitelist=0123456789,.','tsv']
    start = time.monotonic()
    audit.update(ocr_calls=audit.get('ocr_calls',0)+1,timeout_ms=round(timeout*1000))
    try:
        with subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL,start_new_session=True,
                              env={'PATH':'/usr/bin:/bin','LANG':'C','OMP_THREAD_LIMIT':'1'}) as proc:
            try:
                output,_=proc.communicate(png,timeout=timeout)
            finally:
                if proc.poll() is None:
                    try:os.killpg(proc.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    proc.wait()
                audit['exit_code']=proc.returncode
            if proc.returncode:
                return None,'ocr_process_failed'
            return output.decode('utf8'),''
    except subprocess.TimeoutExpired:
        return None,'ocr_timed_out'
    except (OSError,UnicodeError):
        return None,'ocr_runtime_unavailable'
    finally:
        audit['ocr_elapsed_ms']=audit.get('ocr_elapsed_ms',0)+round((time.monotonic()-start)*1000)


def _market_marks(marks,bounds,end):
    _,top,right,bottom=bounds
    return [(raw,x,y) for raw,x,y,*_ in marks if isinstance(raw,str) and len(raw)<=40
            and re.fullmatch(PRICE,raw) and right<x<end and top<=y<=bottom]


class PrintedWord(NamedTuple):
    text: str
    left: float
    top: float
    width: float
    height: float
    confidence: float

    @property
    def mark(self):
        return (self.text, self.left + self.width / 2, self.top + self.height / 2)


def _ocr_marks(strip,timeout,audit):
    bright=strip.point(lambda v:255 if v>178 else 0)
    flags=(bright.crop((0,y,strip.width,y+1)).histogram()[255]>strip.width*.35 for y in range(strip.height))
    normal=ImageOps.invert(strip)
    for state,rows in groupby(enumerate(flags),key=lambda item:item[1]):
        if not state:continue
        ys=[y for y,_ in rows]
        a,b=ys[0],ys[-1]+1
        if 5<=b-a<=min(45,strip.height*.15):
            x0,_,x1,_=bright.crop((0,a,strip.width,b)).getbbox()
            normal.paste(strip.crop((x0,a,x1,b)),(x0,a))

    bordered=ImageOps.expand(normal,border=4,fill=255)
    enlarged=bordered.resize((bordered.width*4,bordered.height*4),Image.Resampling.LANCZOS)
    with BytesIO() as buf:
        enlarged.save(buf,format='PNG')
        text,error=_tsv(buf.getvalue(),timeout,audit)
    if error:
        return [],error
    return _word_boxes(text,strip.size),''


def _word_boxes(text,size):
    labels=[]
    for word in csv.DictReader(StringIO(text),delimiter='\t',quoting=csv.QUOTE_NONE):
        if word['level']!='5' or not re.fullmatch(PRICE,word['text']) or len(word['text'])>40:continue
        label = PrintedWord(word['text'], int(word['left']) / 4 - 4, int(word['top']) / 4 - 4,
                            int(word['width']) / 4, int(word['height']) / 4, float(word['conf']))
        _, x, y = label.mark
        if 0 <= x < size[0] and 0 <= y < size[1]:
            labels.append(label)
    return labels


def _touches_tag_fragment(word, band, regular_height, image):
    if regular_height is None or word.height >= regular_height / 2 or word.width < 3 * word.height:
        return False
    top, bottom = band
    if top - 1 <= word.mark[2] <= bottom + 1:
        return False  # The tag itself remains subject to ordinary reference checks.
    gap = max(0, top - (word.top + word.height), word.top - (bottom + 1))
    if gap > 1:
        return False
    # Prove the opaque printed tag actually covers this word's horizontal extent.
    # At the nearest interior row the background must span the word, even if
    # the outermost row of the rounded tag is narrower.
    y = min(top + 2, bottom) if word.top < top else max(bottom - 2, top)
    row = [image.getpixel((x, y)) for x in range(max(0, int(word.left)), min(image.width, int(word.left + word.width)))]
    return bool(row) and all(min(rgb) > 178 and max(rgb) - min(rgb) < 12 for rgb in row)


def _covered_punctuation(word, band, image, words):
    """Omit a physically overprinted, uncertain tick. Never repair decimal punctuation."""
    if word.confidence >= 50 or not any(c in word.text for c in ',.'):
        return False
    top, bottom = band
    above = word.mark[2] < top - 1 and word.top + word.height >= top + 1
    below = word.mark[2] > bottom + 1 and word.top <= bottom - 1
    if not (above or below):
        return False
    # This is an interior tick of an otherwise readable scale.
    intact = [w for w in words if w.confidence >= 90 and (w.top + w.height < top - 1 or w.top > bottom + 1)]
    if min(sum(w.mark[2] < top for w in intact),sum(w.mark[2] > bottom for w in intact)) < 2:
        return False
    y = min(top+2, bottom) if above else max(top, bottom-2)
    xs = range(ceil(word.left+2),floor(word.left+word.width-2))
    return len(xs)>=4 and xs[0]>=0 and xs[-1]<image.width and all(_light(image.getpixel((x,y))) for x in xs)


def _light(rgb):
    return min(rgb)>178 and max(rgb)-min(rgb)<12


def _adjacent_tick_only(image,right,end,mark,words,height,band):
    """Prove the pointer line is empty; every other pixel in its five-pixel strip belongs to one fixed tick."""
    if height is None:
        return None
    _,x,y=mark
    row=round(y)
    if row-2<0 or row+2>=image.height or band[0]-3 <= y <= band[1]+3:
        return None
    background=image.getpixel((right+1,row))
    if max(background)>=128:
        return None
    deviations=[(cx,cy) for cy in range(row-2,row+3) for cx in range(right+1,end)
                if image.getpixel((cx,cy)) != background]
    if not deviations or any(abs(cy-row)<2 for _,cy in deviations):
        return None
    if len({cy for _,cy in deviations}) != 1:
        return None
    close=[w for w in words if w.top-2<=y<=w.top+w.height+2 and w.left-2<=x<=w.left+w.width+2]
    if len(close)!=1:
        return None
    w=close[0]
    if w.confidence<90 or w.height<height/2 or w.top<=y<w.top+w.height:
        return None
    if not all(floor(w.left)-1<=cx<=ceil(w.left+w.width) and floor(w.top)<=cy<=ceil(w.top+w.height)
               for cx,cy in deviations):
        return None
    return w


def _tag_ink(image,right,end,band):
    """Find the common light interior. Preserve suspicious geometry for rejection."""
    top,bottom=band
    if bottom-top<9:
        return None
    rows=(top+1,top+2,bottom-2,bottom-1)
    sampled=[{x for x in range(right+1,end) if _light(image.getpixel((x,y)))} for y in rows]
    xs=sorted(set.intersection(*sampled))
    rectangular=bool(xs) and xs[-1]-xs[0]+1==len(xs)
    if not xs:
        xs=sorted(set.union(*sampled))
    if not xs:
        return None
    rect=(xs[0],top+2,xs[-1]+1,bottom-1)
    crop=image.crop(rect).convert('L')
    ink=crop.point(lambda level:255 if level<128 else 0).getbbox()
    if not ink:
        return None
    return rect,crop,ink,rectangular


def _refine_partial_tag(image,right,end,band,printed,deadline,audit,cache,key):
    inside=[w for w in printed if band[0]-1<=w.mark[2]<=band[1]+1]
    if len(inside)!=1:
        return None
    partial=inside[0]
    source=_tag_ink(image,right,end,band)
    if not source:
        return None
    (x,y,_,_),crop,ink,rectangular=source
    left,top,stop,bottom=ink
    # Compare printed dark ink with the original word box even when a rounded
    # or truncated prefix could numerically fit the otherwise trustworthy scale.
    missing_left=partial.left-(x+left)>2
    missing_right=x+stop-(partial.left+partial.width)>2
    if not missing_left and not missing_right:
        return None
    tag_key=(key,'tag')
    cached=cache is not None and tag_key in cache
    receipt=dict(partial=partial.text,status='unconfirmed',cache_hit=cached)
    audit['tag_refinement']=receipt
    if not rectangular:
        receipt['status']='tag_shape_unconfirmed'
        return None
    if left==0 or top==0 or stop==crop.width or bottom==crop.height:
        receipt['status']='ink_touches_crop'
        return None
    if missing_left==missing_right or partial.left+partial.width<x+left or partial.left>x+stop:
        return None
    if cached:
        w,error=cache[tag_key]
    else:
        bordered=ImageOps.expand(crop,border=4,fill=255)
        enlarged=bordered.resize((bordered.width*4,bordered.height*4),Image.Resampling.LANCZOS)
        with BytesIO() as buf:
            enlarged.save(buf,format='PNG')
            remaining=deadline-time.monotonic()
            if remaining<=0:
                receipt['status']='budget_exhausted'
                return None
            text,error=_tsv(buf.getvalue(),remaining,audit,segmentation=7)
        w=None
        if not error:
            # Do not discard unrecognized or extra words from the single-line read.
            rows=[v for v in csv.DictReader(StringIO(text),delimiter='\t',quoting=csv.QUOTE_NONE) if v.get('level')=='5']
            boxes=_word_boxes(text,crop.size)
            if len(rows)==1 and len(boxes)==1:
                w=boxes[0]
        if cache is not None:
            cache[tag_key]=(w,error)
    if error or w is None:
        receipt['status']=error or 'unconfirmed'
        return None
    if (w.confidence<90 or not (w.text.startswith(partial.text) if missing_right else w.text.endswith(partial.text))
            or len(w.text)<=len(partial.text)
            or any(abs(a-b)>1.5 for a,b in zip((w.left,w.top,w.left+w.width,w.top+w.height),ink))):
        return None
    receipt.update(status='read',text=w.text,confidence=w.confidence)
    return partial,w._replace(left=x+w.left,top=y+w.top)


def _pointer_off_blank(image, right, end, y, bounds):
    # A uniform dark patch across the entire axis, centred on the reported
    # pointer, proves that no printed number occupied this row in the capture.
    top, bottom = bounds[1], bounds[3]
    row = round(y)
    if row - 2 < top or row + 2 > bottom:
        return False
    with image.crop((right + 1, row - 2, end, row + 3)) as patch:
        return all(lo == hi and hi < 128 for lo, hi in patch.getextrema())


def _record_mark(mark, reason):
    text, x, y = mark
    return dict(text=text, x=x, y=y, reason=reason)


def axis_scale_live(image,box,bounds,marks,*,reference=None,observe=None,timeout=TIMEOUT,cache=None,pointer_y=None):
    """Calibrate the pointer-off chart and verify any label reported at the active pointer."""
    start=time.monotonic()
    l,t,r,b=bounds
    end=min(image.width,box[0]+box[2])
    audit=dict(method='dom_canvas',ocr_calls=0,plot_rect=list(bounds),
               raw_direct_reference=reference,condition='axis_geometry_invalid')
    def report(labels):
        return [dict(text=text,x=x,y=y) for text,x,y in labels]
    try:
        if not (0<=l<r<end and end-r>=14 and 0<=t<b<image.height):
            return None
        direct=_market_marks(marks,bounds,end)
        audit['direct_marks']=report(direct)
        fixed = []
        at_pointer = []
        pointer_evidence = []
        for mark in direct:
            if pointer_y is None or abs(mark[2] - pointer_y) > .5:
                fixed.append(mark)
            elif _pointer_off_blank(image, r, end, mark[2], bounds):
                pointer_evidence.append(_record_mark(mark, 'absent_on_pointer_off_axis'))
            else:
                at_pointer.append(mark)
        if pointer_y is not None:
            audit.update(pointer_y=pointer_y, pointer_evidence=pointer_evidence)
        scale=axis_scale(image,box,bounds,fixed,reference=reference,audit=audit)
        if scale is not None and not at_pointer:
            return scale
        if scale is None and audit['condition'] not in ('reference_missing','ticks_insufficient','tick_span_insufficient'):
            return None
        if end-r>150 or b-t>4096:
            audit['condition']='axis_crop_outside_budget'
            return None
        audit['method']='tesseract'
        strip=image.crop((r+1,t,end,b+1)).convert('L')
        key=(strip.size,sha256(strip.tobytes()).digest()) if cache is not None else None
        if cache is not None and key in cache:
            local,error=cache[key]
            audit['ocr_cache_hit']=True
        else:
            local,error=_ocr_marks(strip,min(TIMEOUT,timeout),audit)
            if cache is not None:
                # Keep one strip per map, including unsuccessful transcriptions.
                # Calibration, reference, direct marks and line still run on every call.
                cache.clear()
                cache[key]=(local,error)
        if error:
            audit['condition']=error
            return None
        words = [word._replace(left=r + 1 + word.left, top=t + word.top) for word in local]
        audit['ocr_marks'] = report(word.mark for word in words)
        band = audit['tag_bands'][0]  # The direct pass already verified a unique band.
        regular_heights = [word.height for word in words if word.top + word.height < band[0] - 1
                           or word.top > band[1] + 2]
        regular_height = median(regular_heights) if len(regular_heights) >= 4 else None
        printed = []
        fragments = []
        for word in words:
            if _touches_tag_fragment(word, band, regular_height, image):
                fragments.append(word._asdict())
            else:
                printed.append(word)
        if fragments:
            audit['tag_occluded_fragments'] = fragments
        for mark in at_pointer:
            raw, x, y = mark
            if band[0] - 1 <= y <= band[1] + 1:
                # At the reference tag the direct number is independent evidence.
                # Keep it so the original reference-conflict check sees both values.
                fixed.append(mark)
                pointer_evidence.append(_record_mark(mark, 'at_reference_tag'))
                continue
            same_row = [w for w in printed if w.top <= y < w.top + w.height
                        and w.left - 2 <= x < w.left + w.width + 2]
            if len(same_row) == 1 and same_row[0].text != raw:
                # Replacing a contradictory pointer label needs a complete and
                # unambiguous printed tick. Low-confidence words can still
                # corroborate the same number; they cannot displace another.
                word = same_row[0]
                if regular_height is None or word.height < regular_height / 2 or word.confidence < 90:
                    same_row = []
            if not same_row:
                neighbour=_adjacent_tick_only(image,r,end,mark,printed,regular_height,band)
                if neighbour is not None:
                    pointer_evidence.append(dict(_record_mark(mark,'blank_pointer_row_adjacent_to_tick'),printed=neighbour.text))
                    continue
            if len(same_row) != 1:
                pointer_evidence.append(_record_mark(mark, 'ambiguous_pointer_off_axis'))
                audit['condition'] = 'pointer_price_unverified'
                return None
            shown = same_row[0].text
            reason = 'fixed_label_on_pointer_off_axis' if shown == raw else 'different_label_on_pointer_off_axis'
            pointer_evidence.append(dict(_record_mark(mark, reason), printed=shown))
            # The full printed word already supplies its own centre exactly once.
        deadline=start+min(TIMEOUT,timeout)
        refined=_refine_partial_tag(image,r,end,band,printed,deadline,audit,cache,key)
        if refined is not None:
            partial,full_tag=refined
            printed=[full_tag if w is partial else w for w in printed]
        elif 'tag_refinement' in audit:
            audit['condition']='reference_tag_incomplete'
            return None
        scale=axis_scale(image,box,bounds,[*fixed,*(w.mark for w in printed)],reference=reference,audit=audit)
        if scale is None and audit['condition'] in ('tick_span_insufficient','axis_position_conflict'):
            covered=[w for w in printed if _covered_punctuation(w,band,image,printed)]
            if len(covered)==1:
                omitted=covered[0]
                audit['occluded_ticks']=[dict(zip(('text','x','y'),omitted.mark))]
                retained=[w.mark for w in printed if w is not omitted]
                scale=axis_scale(image,box,bounds,[*fixed,*retained],reference=reference,audit=audit)
        return scale
    finally:
        if observe:
            audit['elapsed_ms']=round(1000*(time.monotonic()-start))
            observe(audit)
