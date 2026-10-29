"""Clear source hover by normal pointer movement; never hide source DOM/canvas."""
from __future__ import annotations

NO_TOOLTIP = r'''() => ![...document.querySelectorAll('.cg-toolti-box')].some(e => {
  const r=e.getBoundingClientRect();
  if(!r.width || !r.height || r.bottom<=0 || r.top>=innerHeight)return false;
  for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
    if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity)===0)return false;
  }
  return true;
})'''
TWO_FRAMES = r'''timeout => new Promise((resolve,reject) => {
  const timer=setTimeout(()=>reject(new Error('clean_frame_animation_timeout')), timeout);
  requestAnimationFrame(()=>requestAnimationFrame(()=>{clearTimeout(timer);resolve(true)}));
})'''


def prepare_frame(page, paint, *, timeout_ms=5000):
    """Keep the real source's mouseleave/animation handlers and one fresh paint.

    A missing or stuck tooltip transition fails; no CSS hiding, synthetic price,
    viewport change, geometry alteration, or screenshot pixel editing is used.
    """
    import time
    start=time.monotonic()
    def budget():
        left=timeout_ms-int((time.monotonic()-start)*1000)
        if left <= 0:raise ValueError('clean_frame_deadline')
        return left
    page.mouse.move(0, 0)
    page.wait_for_function(NO_TOOLTIP, polling=25, timeout=budget())
    page.evaluate(TWO_FRAMES, budget())
    paint(page)
    page.evaluate(TWO_FRAMES, budget())
    if not page.evaluate(NO_TOOLTIP):raise ValueError('clean_frame_tooltip_reappeared')
    return {'tooltip_hidden':True,'fresh_paint':True,
            'elapsed_seconds':round(time.monotonic()-start,6)}
