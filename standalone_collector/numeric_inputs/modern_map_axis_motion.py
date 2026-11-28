"""Resolve a price-label conflict from visible movement at the already verified latest column."""
from contextlib import ExitStack, nullcontext
from datetime import datetime
from hashlib import sha256
from io import BytesIO
from math import ceil, floor
import re
import time

from PIL import Image, ImageChops

import modern_map_reader as ui
from model1_execution import StageFailure
from modern_map_axis import MAX_TAG_RESIDUAL
from modern_map_axis_ocr import TIMEOUT, _ocr_marks, axis_scale_live
from modern_map_cursor import _INSTALL, decode
from modern_map_evidence import PRICE, number, source_clock
from modern_map_surface import _INSPECT
from modern_map_tooltip import _DOM


_POINTER = """w => {
 let last=null;
 const move=e=>{if(e.isTrusted)last=[e.clientX,e.clientY]};
 document.addEventListener('pointermove',move,true);
 w.nativePoint=()=>last;
 w.releasePoint=()=>document.removeEventListener('pointermove',move,true);
}"""


class UnverifiedMotion(Exception):
    """Only a local inability to identify the moving label."""


def need(condition, reason):
    if not condition:
        raise UnverifiedMotion(reason)


def ambiguous_mark(receipt):
    if receipt.get('condition') not in ('reference_conflict', 'pointer_price_unverified'):
        return None
    candidates = {(m['text'], m['x'], m['y']) for m in receipt.get('pointer_evidence', ())
                  if m.get('reason') in ('at_reference_tag', 'ambiguous_pointer_off_axis')}
    return candidates.pop() if len(candidates) == 1 and len(receipt.get('tag_bands', ())) == 1 else None


def _unobstructed(page, box, rect):
    x,y,w,h = rect
    handle = page.evaluate_handle(_INSPECT, list(box))
    try:
        facts = handle.evaluate('s=>s.facts')
        need(all(c<=x or a>=x+w or d<=y or b>=y+h for a,b,c,d in facts['unknown']+facts['covered']),
             'motion_crop_obstructed')
    finally:
        handle.dispose()


def _frame_marks(frame, dom, bounds, end):
    _,top,right,bottom = bounds
    labels = {(m[0],m[1],m[4],'canvas') for m in frame['marks']
              if len(m)>4 and isinstance(m[0],str) and len(m[0])<=40 and re.fullmatch(PRICE,m[0])}
    labels.update((text,x,y,'dom') for text,x,y in dom if re.fullmatch(PRICE,text) and len(text)<=40)
    return [dict(text=raw,x=x,y=y,source=source) for raw,x,y,source in sorted(labels)
            if right<x<end and top<=y<=bottom]


def _acquire(page, box, bounds, sample, guard, phase, emit, stack, frames):
    bx,by,bw,bh = box
    left,top,right,bottom = bounds
    x,y = sample['latest_x'],sample['pointer_y']
    end = bx+bw
    need(x==right and bx<=left<right<end and by<=top<bottom<by+bh, 'motion_geometry_changed')
    clip_left = max(left, right-48)
    clip = (clip_left, top, end-clip_left, bottom-top+1)
    need(16<clip[2]<=450 and 32<clip[3]<=1400, 'motion_clip_outside_budget')
    rect=dict(zip(('x','y','width','height'),clip))
    target_time=datetime.fromisoformat(sample['latest_time'])
    off=(max(0,bx-2),max(0,by-2))
    y2=round(top+.72*(bottom-top))
    need(y2!=y, 'motion_positions_indistinct')
    with getattr(page,'pointer_scope',nullcontext)():
        watch=page.evaluate_handle(ui.PAINT,[*box,True])
        def clean():
            try:watch.evaluate('w=>{w.releasePoint?.();w.restore()}')
            finally:
                try:watch.dispose()
                finally:page.mouse.move(*off)
        try:
            watch.evaluate(_INSTALL,[box,bounds])
            watch.evaluate(_POINTER)
            page.mouse.move(*off)
            ui.wait_for(page,lambda:watch.evaluate('w=>w.cursorOff()'),timeout=3)
            for state,point in [('y1',(x,y)),('y2',(x,y2)),('off',off)]:
                guard()
                watch.evaluate('w=>w.cursorMove()')
                page.mouse.move(*point)
                def settled():
                    if watch.evaluate('w=>w.nativePoint()') != list(point):
                        return None
                    view=watch.evaluate('w=>w.cursorRead()')
                    if state=='off':
                        return view if not view['tooltip'] else None
                    value=decode(view,x,sample['period_seconds'],True)
                    if not value:
                        return None
                    stamp=source_clock(value[0],target_time,column=True,max_lag=sample['period_seconds'])
                    return view if stamp==target_time else None
                try:
                    ui.wait_for(page,settled,timeout=3)
                except StageFailure as error:
                    if error.code=='source_not_readable' and error.reason is None:
                        raise UnverifiedMotion('motion_column_or_cursor_unverified_'+state) from error
                    raise
                _unobstructed(page,box,clip)
                png=ui.protected_clip(page,rect,guard)
                _unobstructed(page,box,clip)
                view=settled()
                need(view is not None, 'motion_column_or_cursor_changed')
                im=stack.enter_context(Image.open(BytesIO(png)))
                need(im.format=='PNG' and im.size==clip[2:] and len(png)<=192*1024, 'motion_crop_outside_budget')
                rgb=stack.enter_context(im.convert('RGB'))
                marks=_frame_marks(view,page.evaluate(_DOM,list(box)),bounds,end)
                frames[state]=dict(image=rgb,marks=marks,point=point)
                emit('motion_frame',dict(phase=phase,state=state,column_time=sample['latest_time'],
                     pointer_css=list(point),clip_css=list(clip),png=png,marks=marks))
        except BaseException:
            try:clean()
            except Exception:pass
            raise
        else:clean()
    return clip


def _words(frame, local_axis, clip, cache, metrics):
    x,y,_,height=clip
    image=frame['image']
    with image.crop((local_axis,0,image.width,height)).convert('L') as strip:
        key=(strip.size,sha256(strip.tobytes()).digest())
        if key in cache:
            words,error=cache[key]
        else:
            words,error=_ocr_marks(strip,ui.limited_timeout(1000*TIMEOUT)/1000,metrics)
            cache[key]=(words,error)
    need(not error,'motion_axis_ocr_failed')
    return [w._replace(left=w.left+local_axis) for w in words]


def _palette(patch):
    colors=patch.getcolors(patch.width*patch.height)
    background=max(colors)[1]
    def contrast(color):
        return max(abs(a-b) for a,b in zip(color,background))
    span=max(contrast(color) for count,color in colors)
    ink={color for count,color in colors if count>=2 and contrast(color)>span/2}
    return background,ink


def _glyph_overlaps_band(word, restored, word_top, band):
    """An actual changed letter edge must enter the stationary band, even if its centre does not."""
    top=max(0,band[0]-word_top)
    bottom=min(word.height,band[1]+1-word_top)
    if top>=bottom:
        return False
    background,ink=_palette(word)
    current,previous=word.load(),restored.load()
    for y in range(top,bottom):
        for x in range(word.width):
            if current[x,y] not in ink:
                continue
            # An unchanged foreground pixel can acquire a new contrasting edge. The
            # foreground and neighbouring background must both be inside the overlap.
            for nx,ny in ((x-1,y),(x+1,y),(x,y-1),(x,y+1)):
                if 0<=nx<word.width and top<=ny<bottom and current[nx,ny]==background:
                    if current[x,y]!=previous[x,y] or background!=previous[nx,ny]:
                        return True
    return False


def _moving_value(frames, state, peer, clip, band, tag_covered):
    x,y,_,_=clip
    f=frames[state]
    py=f['point'][1]-y
    candidates=[]
    for word in f['words']:
        if not word.top<=py<word.top+word.height:continue
        if any(w.text==word.text and w.top<=py<w.top+w.height
               for other in (peer,'off') for w in frames[other]['words']):continue
        if not any(m['text']==word.text and word.top<=m['y']-y<word.top+word.height
                   for m in f['marks']):continue
        rect=(floor(word.left),floor(word.top),ceil(word.left+word.width),ceil(word.top+word.height))
        with f['image'].crop(rect) as printed, frames['off']['image'].crop(rect) as outside, frames[peer]['image'].crop(rect) as other:
            if outside.tobytes()!=other.tobytes():continue
            if tag_covered and not _glyph_overlaps_band(printed,outside,rect[1],band):continue
            with ImageChops.difference(printed,outside) as diff:
                extent=diff.getbbox()
                if extent and extent[2]-extent[0]>=printed.width-2 and extent[3]-extent[1]>=printed.height-2:
                    candidates.append(word)
    need(len(candidates)==1,'moving_text_unverified_'+state)
    return candidates[0]


def _roles(frames, clip, right, tag_band, source, cache, metrics):
    x,y,w,h=clip
    axis=right+1-x
    lo,hi=(n-y for n in tag_band)
    need(0<axis<w and 0<lo<=hi<h-1,'motion_geometry_changed')
    off=frames['off']['image']
    # Bind reverse movement to the source raster whose scale/levels are being validated.
    with source.crop((right+1,y,x+w,y+h)) as original, off.crop((axis,0,w,h)) as restored:
        need(original.tobytes()==restored.tobytes(),'motion_axis_changed')
    for f in frames.values():
        f['words']=_words(f,axis,clip,cache,metrics)
    def tag(state):
        words=[word for word in frames[state]['words'] if lo<=word.mark[2]<=hi]
        need(len(words)==1,'motion_tag_not_unique')
        return words[0].text
    reference=tag('off')
    tag_rect=(axis,lo,w,hi+1)
    visible=[]
    with off.crop(tag_rect) as printed:
        for state in ('y1','y2'):
            with frames[state]['image'].crop(tag_rect) as other:
                if printed.tobytes()==other.tobytes():
                    need(tag(state)==reference,'motion_stationary_tag_disagrees')
                    visible.append(state)
    need(visible,'motion_tag_not_visible_twice')
    w1=_moving_value(frames,'y1','y2',clip,(lo,hi),'y1' not in visible)
    w2=_moving_value(frames,'y2','y1',clip,(lo,hi),'y2' not in visible)
    need(w1.top+w1.height<=w2.top or w2.top+w2.height<=w1.top,'motion_positions_overlap')
    if len(visible)==1:
        def palette(state,word):
            box=(floor(word.left), floor(word.top), ceil(word.left+word.width), ceil(word.top+word.height))
            with frames[state]['image'].crop(box) as patch:
                return _palette(patch)
        bg1,ink1=palette('y1',w1)
        bg2,ink2=palette('y2',w2)
        need(bg1==bg2 and ink1 & ink2,'motion_covering_label_style_disagrees')
    delta=frames['y2']['point'][1]-frames['y1']['point'][1]
    need(len({number(s)[0] for s in (reference,w1.text,w2.text)})==3 and
         (number(w2.text)[0]-number(w1.text)[0])*delta<0,'motion_price_direction_ambiguous')
    height=min(w1.height,w2.height)
    need(abs(w2.mark[2]-w1.mark[2]-delta)<=height/2 and
         min(abs(w1.left-w2.left),abs(w1.left+w1.width-w2.left-w2.width))<=height/2,
         'motion_displacement_ambiguous')
    return reference,w1,w2


def resolve(page,frame,box,bounds,labels,reference,sample,cache,receipt,mark,phase,guard,emit):
    """Keep all original axis checks, removing only the mark demonstrably moving with the pointer."""
    started=time.monotonic()
    audit=dict(name='basic_'+phase+'_axis_motion',status='failed',initial_condition=receipt['condition'],
               condition='motion_unverified',column_time=sample['latest_time'],captures=0,ocr_calls=0)
    frames={}
    try:
        with ui.map_access_budget(ui.limited_timeout(1000*TIMEOUT)/1000), ExitStack() as stack:
            guard()
            clip=_acquire(page,box,bounds,sample,guard,phase,emit,stack,frames)
            guard()
            price,w1,w2=_roles(frames,clip,bounds[2],receipt['tag_bands'][0],frame,cache,audit)
            audit.update(reference=price,cursor_values=[w1.text,w2.text])
            need(number(mark[0])[0]==number(w1.text)[0], 'motion_original_mark_disagrees')
            # Prove that the actual sampled text belongs to this axis label, not another
            # number with the same value in another chart control.
            need(any(m['text']==mark[0] and abs(m['x']-mark[1])<=1 and abs(m['y']-mark[2])<=1
                     for m in frames['y1']['marks']), 'motion_original_mark_disagrees')
            need(reference is None or number(reference)[0]==number(price)[0], 'motion_explicit_price_disagrees')
            kept=[m for m in labels if tuple(m[:3])!=mark]
            need(len(kept)<len(labels),'motion_original_mark_disagrees')
            verification={}
            result=axis_scale_live(frame,box,bounds,kept,reference=reference,cache=cache,
                                   pointer_y=sample['pointer_y'],observe=verification.update,
                                   timeout=ui.limited_timeout(1000*TIMEOUT)/1000)
            audit['ocr_calls']+=verification.get('ocr_calls',0)
            need(result is not None and number(result[0])[0]==number(price)[0],
                 verification.get('condition','motion_axis_unverified'))
            for s,word in [('y1',w1),('y2',w2)]:
                need(abs(result[1](float(number(word.text)[0]))-frames[s]['point'][1])<=MAX_TAG_RESIDUAL,
                     'motion_cursor_scale_disagrees')
            audit.update(status='ok',condition='verified')
            # The phase readout accounts for its original OCR. Additional calls belong
            # to the motion stage alone. Keep the original pointer conflict in its evidence.
            receipt.update((name,value) for name,value in verification.items()
                           if name not in ('ocr_calls','ocr_elapsed_ms','elapsed_ms','pointer_evidence'))
            receipt['method']='tesseract'
            return result
    except UnverifiedMotion as exc:
        audit['condition']=str(exc)
        return None
    except StageFailure as exc:
        audit['condition']=getattr(exc,'basic_reason',None) or exc.reason or exc.code
        raise
    finally:
        audit.update(captures=len(frames),elapsed_ms=round(1000*(time.monotonic()-started)))
        emit('stage',audit)
