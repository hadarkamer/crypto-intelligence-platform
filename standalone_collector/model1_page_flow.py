"""DOM-only page synchronization for the app's copy of the legacy capture.

No secrets, cookie reads, account requests or response bodies are accessed here.
Selectors observe only links already rendered in the page. These are UI signals,
not proof that a saved session is valid. Original source authentication is unchanged.
"""
import json
import time

UI_SCRIPT = r'''() => {
  const visible = e => {
    const r=e.getBoundingClientRect();
    if(r.width<=0||r.height<=0) return false;
    for(let n=e;n;n=n.parentElement) {
      const s=getComputedStyle(n);
      if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity)===0) return false;
    }
    return true;
  };
  return {
    account_link_present: !!document.querySelector('a[href="/account"]'),
    login_link_visible: [...document.querySelectorAll('a[href="/login"]')].some(visible)
  };
}'''


def ui_state(page):
    try:
        result=page.evaluate(UI_SCRIPT)
        if isinstance(result,dict):
            return {k:result.get(k) is True for k in ('account_link_present','login_link_visible')}
    except Exception:
        pass
    return {'account_link_present':False,'login_link_visible':False}


def before_controls(page,timeout_ms=15000):
    """Let the page finish initializing before the legacy timeframe click.

    An anonymous SSR login link can disappear during normal client initialization.
    Absence of the account link is not reported as a rejected session. A bounded
    wait prevents blocking forever and does not start another page navigation.
    """
    deadline=time.monotonic()+max(0,min(timeout_ms,15000))/1000
    state=ui_state(page)
    while not (state['account_link_present'] and not state['login_link_visible']):
        if time.monotonic()>=deadline:
            break
        page.wait_for_timeout(250)
        state=ui_state(page)
    print('MODEL1_PAGE_FLOW '+json.dumps({'stage':'before_source_controls',**state}),flush=True)
    return state


def after_render_check(page,ready):
    state={'stage':'after_render_check','chart_ready':ready is True,**ui_state(page)}
    print('MODEL1_PAGE_FLOW '+json.dumps(state),flush=True)
    return state
