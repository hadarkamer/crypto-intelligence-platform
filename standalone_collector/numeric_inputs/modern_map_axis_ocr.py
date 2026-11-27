"""Read the printed price tag and ticks once, using the bundled English Tesseract."""
import csv
from hashlib import sha256
from itertools import groupby
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


def _tsv(png, timeout, audit):
    # Only the OCR process is limited. Do not pass the collector's environment
    # (database credentials, session keys, etc.) to the binary.
    command = ['/usr/bin/prlimit', '--as=536870912', '--cpu=8:9', '--', str(BINARY),
               'stdin','stdout','-l','eng','--oem','1','--psm','11',
               '-c','tessedit_char_whitelist=0123456789,.','tsv']
    start = time.monotonic()
    audit.update(ocr_calls=1,timeout_ms=round(timeout*1000))
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
        audit['ocr_elapsed_ms']=round((time.monotonic()-start)*1000)


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
    labels=[]
    for word in csv.DictReader(StringIO(text),delimiter='\t',quoting=csv.QUOTE_NONE):
        if word['level']!='5' or not re.fullmatch(PRICE,word['text']) or len(word['text'])>40:continue
        label = PrintedWord(word['text'], int(word['left']) / 4 - 4, int(word['top']) / 4 - 4,
                            int(word['width']) / 4, int(word['height']) / 4, float(word['conf']))
        _, x, y = label.mark
        if 0 <= x < strip.width and 0 <= y < strip.height:
            labels.append(label)
    return labels,''


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
            local=cache[key]
            audit['ocr_cache_hit']=True
        else:
            local,error=_ocr_marks(strip,min(TIMEOUT,timeout),audit)
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
            if len(same_row) != 1:
                pointer_evidence.append(_record_mark(mark, 'ambiguous_pointer_off_axis'))
                audit['condition'] = 'pointer_price_unverified'
                return None
            shown = same_row[0].text
            reason = 'fixed_label_on_pointer_off_axis' if shown == raw else 'different_label_on_pointer_off_axis'
            pointer_evidence.append(dict(_record_mark(mark, reason), printed=shown))
            # The full printed word already supplies its own centre exactly once.
        scale=axis_scale(image,box,bounds,[*fixed, *(word.mark for word in printed)],reference=reference,audit=audit)
        # Memoize transcription only. Every observation rechecks all direct
        # marks, tag position, calibrated ticks and the current plotted line.
        if scale is not None and cache is not None and key not in cache:
            cache.clear()
            cache[key]=local
        return scale
    finally:
        if observe:
            audit['elapsed_ms']=round(1000*(time.monotonic()-start))
            observe(audit)
