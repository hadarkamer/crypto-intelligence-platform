"""Read the printed price tag and ticks once, using the bundled English Tesseract."""
import csv
from hashlib import sha256
from itertools import groupby
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
        y=(int(word['top'])+int(word['height'])/2)/4-4
        x=(int(word['left'])+int(word['width'])/2)/4-4
        if 0<=x<strip.width and 0<=y<strip.height:
            labels.append((word['text'],x,y))
    return labels,''


def axis_scale_live(image,box,bounds,marks,*,reference=None,observe=None,timeout=TIMEOUT,cache=None):
    """Keep all direct labels. One OCR pass can complete a partial price axis."""
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
        scale=axis_scale(image,box,bounds,direct,reference=reference,audit=audit)
        if scale is not None or audit['condition'] not in ('reference_missing','ticks_insufficient','tick_span_insufficient'):
            return scale
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
        optical=[(text,r+1+x,t+y) for text,x,y in local]
        audit['ocr_marks']=report(optical)
        scale=axis_scale(image,box,bounds,[*direct,*optical],reference=reference,audit=audit)
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
