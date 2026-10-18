"""Isolated, passive close-button inspection after the unchanged action fails."""
import time

CONTROL_STATE = r'''panel => {
  const rect=e=>{const r=e.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height}};
  const style=e=>{const s=getComputedStyle(e);return {display:s.display,visibility:s.visibility,
    opacity:s.opacity,pointerEvents:s.pointerEvents,position:s.position,transform:s.transform}};
  const buttonInfo=e=>{
    const r=e.getBoundingClientRect(),hit=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
    const ancestors=[];for(let p=e.parentElement;p&&ancestors.length<5;p=p.parentElement)
      ancestors.push({tag:p.tagName,rect:rect(p),style:style(p)});
    return {tag:e.tagName,label:e.getAttribute('aria-label'),title:e.getAttribute('title'),
      connected:e.isConnected,disabled:e.disabled===true,ariaDisabled:e.getAttribute('aria-disabled'),
      rect:rect(e),style:style(e),hitTarget:!!hit&&(hit===e||e.contains(hit)),
      hitWithinPanel:!!hit&&panel.contains(hit),hitWithinCookieBanner:!!hit&&!!hit.closest('#termly-code-snippet-support'),
      animations:e.getAnimations({subtree:false}).map(a=>({playState:a.playState})),ancestors};
  };
  const closes=[...panel.querySelectorAll('button,[role="button"]')]
    .filter(e=>(e.getAttribute('aria-label')||(e.textContent||'').trim())==='Close heatmap settings');
  return {panel:{connected:panel.isConnected,rect:rect(panel),style:style(panel)},
    closeCount:closes.length,close:closes.slice(0,2).map(buttonInfo),
    buttonLabels:[...panel.querySelectorAll('button,[role="button"]')].slice(0,12)
      .map(e=>({tag:e.tagName,label:e.getAttribute('aria-label'),title:e.getAttribute('title')})),
    scrollY,viewport:{width:innerWidth,height:innerHeight},visibilityState:document.visibilityState};
}'''

FRAME_STATE = r'''() => new Promise(resolve=>{
  const start=performance.now();let frames=0,frameId;
  const tick=()=>{frames++;frameId=requestAnimationFrame(tick)};
  frameId=requestAnimationFrame(tick);
  setTimeout(()=>{cancelAnimationFrame(frameId);resolve({frames,elapsed_ms:performance.now()-start,
    visibilityState:document.visibilityState})},250);
})'''


def install():
    from model3_cookie_diagnostic import install as install_existing
    from model3_diagnostic import emit
    install_existing()
    import heatmap_hover as h
    original = h.close_heatmap_settings

    def close_observed(page):
        try:
            return original(page)
        except Exception:
            event = {'phase': 'close_control_state'}
            started = time.monotonic()
            try:
                panel = page.get_by_role('dialog', name='Heatmap settings', exact=True)
                event['control'] = panel.evaluate(CONTROL_STATE, timeout=1500)
                event['frame_sample'] = page.evaluate(FRAME_STATE)
            except Exception as diagnostic_error:
                event['observation_error_class'] = type(diagnostic_error).__name__
            event['seconds'] = round(time.monotonic() - started, 4)
            emit(event)
            raise

    h.close_heatmap_settings = close_observed
    original_cookie = h.dismiss_cookie_banner

    def cookie_observed(page):
        try:
            return original_cookie(page)
        except Exception:
            event = {'phase': 'cookie_control_state'}
            try:
                panel = page.locator('#termly-code-snippet-support')
                event['control'] = panel.evaluate(CONTROL_STATE.replace('Close heatmap settings', 'Decline'), timeout=1500)
                event['frame_sample'] = page.evaluate(FRAME_STATE)
            except Exception as diagnostic_error:
                event['observation_error_class'] = type(diagnostic_error).__name__
            emit(event)
            raise

    h.dismiss_cookie_banner = cookie_observed
