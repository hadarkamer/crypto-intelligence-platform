"""Clear source hover with native input and one fresh paint; laboratory only."""
from __future__ import annotations
import math
import time

NO_TOOLTIP = r'''() => ![...document.querySelectorAll('.cg-toolti-box')].some(e => {
  const r=e.getBoundingClientRect();
  if(!r.width||!r.height||r.right<=0||r.bottom<=0||r.left>=innerWidth||r.top>=innerHeight)return false;
  for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
    if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity)===0)return false;
  }
  return true;
})'''
GEOMETRY = r'''() => [...document.querySelectorAll('canvas')].filter(e=>{
  const r=e.getBoundingClientRect(),s=getComputedStyle(e);
  return r.width>0&&r.height>0&&s.display!=='none'&&s.visibility!=='hidden';
}).map(e=>{const r=e.getBoundingClientRect();return {
  x:r.x,y:r.y,width:r.width,height:r.height,pixel_width:e.width,pixel_height:e.height};})'''
TWO_FRAMES = r'''timeout => new Promise(resolve => {
  let first=null,second=null,done=false;
  const timer=setTimeout(()=>finish(false),timeout);
  function finish(ok){
    if(done)return;done=true;clearTimeout(timer);
    if(first!==null)cancelAnimationFrame(first);
    if(second!==null)cancelAnimationFrame(second);
    resolve(ok);
  }
  first=requestAnimationFrame(()=>{first=null;
    second=requestAnimationFrame(()=>{second=null;finish(true)})});
})'''


def outside_point(rect,viewport):
    if not isinstance(rect,dict) or not isinstance(viewport,dict):
        raise ValueError('clean_frame_geometry_missing')
    values=[rect.get(k) for k in ('x','y','width','height')]+[viewport.get(k) for k in ('width','height')]
    if any(type(v) not in (int,float) or not math.isfinite(v) for v in values):
        raise ValueError('clean_frame_geometry_invalid')
    x,y,w,h,vw,vh=values
    if w<=0 or h<=0 or vw<2 or vh<2:raise ValueError('clean_frame_geometry_invalid')
    for px,py in ((0,0),(vw-1,0),(0,vh-1),(vw-1,vh-1)):
        if not x-1<=px<=x+w+1 or not y-1<=py<=y+h+1:return px,py
    raise ValueError('clean_frame_no_outside_point')


def prepare_frame(page,paint,*,timeout_ms=12000):
    """Observe source leave handlers before a single fresh paint.

    The original software renderer took 6-8 seconds in measured lab attempts.
    Use one bounded 12-second laboratory budget, shared by all operations;
    this is NOT a sleep and does not alter the reader's source deadlines.
    Two animation frames BEFORE painting allow native crosshair cleanup.
    The redundant two-frame wait AFTER the explicit paint is removed.
    Never remove/hide DOM, mutate source values or edit screenshot pixels.
    """
    from playwright.sync_api import TimeoutError as BrowserTimeout
    if type(timeout_ms) is not int or not 50<=timeout_ms<=15000:
        raise ValueError('clean_frame_invalid_budget')
    started=time.monotonic();steps={}
    def budget():
        left=timeout_ms-int((time.monotonic()-started)*1000)
        if left<=0:raise ValueError('clean_frame_deadline')
        return left
    def observed(name,action):
        budget();at=time.monotonic()
        value=action();steps[name]=round(time.monotonic()-at,6);budget()
        return value
    geometry=observed('geometry_before',lambda:page.evaluate(GEOMETRY))
    if len(geometry)!=1:raise ValueError('clean_frame_unique_canvas_required')
    point=outside_point(geometry[0],page.viewport_size)
    observed('native_pointer_leave',lambda:page.mouse.move(*point))
    try:
        handle=observed('tooltip_disappearance',lambda:page.wait_for_function(NO_TOOLTIP,polling=25,timeout=budget()))
        handle.dispose()
    except BrowserTimeout:
        raise ValueError('clean_frame_tooltip_timeout') from None
    if not observed('source_animation_frames',lambda:page.evaluate(TWO_FRAMES,budget())):
        raise ValueError('clean_frame_animation_timeout')
    observed('fresh_paint',lambda:paint(page))
    if not observed('tooltip_recheck',lambda:page.evaluate(NO_TOOLTIP)):
        raise ValueError('clean_frame_tooltip_reappeared')
    if observed('geometry_after',lambda:page.evaluate(GEOMETRY))!=geometry:
        raise ValueError('clean_frame_geometry_changed')
    return {'tooltip_hidden':True,'fresh_paint':True,'waited_animation_frames':2,
            'elapsed_seconds':round(time.monotonic()-started,6),'operations':steps}
