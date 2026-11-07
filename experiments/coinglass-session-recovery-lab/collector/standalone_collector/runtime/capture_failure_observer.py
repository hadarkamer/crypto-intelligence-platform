"""One bounded, passive failure observation; never a capture or access decision."""
from urllib.parse import urlsplit

OBSERVATION_TIMEOUT_MS=1500
IDENTITIES=frozenset({'legacy','canonical','invalid','unavailable'})
CLICK_FAILURES=frozenset({'not_visible','outside_viewport','unstable','pointer_intercepted',
    'disabled','detached','target_closed','action_timeout','unclassified'})
BOOL_FIELDS=('heading_visible','chart_present','login_gate_visible','dialog_visible',
    'loading_visible','challenge_visible','trigger_present','trigger_visible',
    'trigger_disabled','trigger_hit_target','model_selected','symbol_selected')
REASONS=frozenset({'login-required','challenge-visible','visible-dialog','loading-indicator',
    'no-chart','control-disabled','state-observed','unverified'})
STATE_SCRIPT=r'''model => {
  const limited=selector=>[...document.querySelectorAll(selector)].slice(0,16);
  const visible=el=> {
    const r=el.getBoundingClientRect();
    if(r.width<=0 || r.height<=0) return false;
    for(let n=el,depth=0;n && depth<6;n=n.parentElement,depth++) {
      const s=getComputedStyle(n);
      if(s.display==='none' || s.visibility==='hidden' || Number(s.opacity||1)===0) return false;
    }
    return true;
  };
  const text=el=>(el.textContent||'').slice(0,2048).trim();
  const headings=limited('h1,h2,[role="heading"]');
  const dialogs=limited('[role="dialog"],.MuiModal-root').filter(visible);
  const triggers=limited('[role="combobox"],[aria-haspopup="listbox"],button.MuiSelect-button');
  const trigger=triggers.find(e=>visible(e) && /^(12|24|48) hour$/i.test(text(e)));
  const tabs=limited('[role="tab"]');
  const state={heading_visible:headings.some(e=>visible(e) && /^BTC Liquidation Heatmap$/i.test(text(e))),
    chart_present:limited('canvas').some(e=> {
      const r=e.getBoundingClientRect(); return visible(e) && r.width>=400 && r.height>=200;
    }),
    login_gate_visible:dialogs.some(e=>/\blog\s*in\s+to\s+unlock\s+full\s+data\b/i.test(text(e))),
    dialog_visible:dialogs.length>0,
    loading_visible:limited('[role="progressbar"],[aria-busy="true"],.MuiCircularProgress-root,.ant-spin-spinning,[class*="spinner" i],[data-loading="true"]').some(visible),
    challenge_visible:limited('#challenge-running,#challenge-stage,iframe[src*="challenges.cloudflare.com"]').some(visible) ||
      headings.some(e=>visible(e) && /^(verify you are human|performing security verification|just a moment\.\.\.)$/i.test(text(e))),
    trigger_present:triggers.some(e=>/^(12|24|48) hour$/i.test(text(e))),trigger_visible:!!trigger,
    model_selected:tabs.some(e=>visible(e) && text(e)==='Model '+model && e.getAttribute('aria-selected')==='true'),
    symbol_selected:tabs.some(e=>visible(e) && text(e)==='Symbol' && e.getAttribute('aria-selected')==='true')};
  if(trigger) {
    state.label=text(trigger).toLowerCase();
    state.trigger_disabled=trigger.disabled===true || trigger.getAttribute('aria-disabled')==='true';
    const r=trigger.getBoundingClientRect(),hit=document.elementFromPoint(r.left+r.width/2,r.top+r.height/2);
    state.trigger_hit_target=!!hit && (hit===trigger || trigger.contains(hit));
  }
  state.reason=state.login_gate_visible?'login-required':state.challenge_visible?'challenge-visible':
    state.dialog_visible?'visible-dialog':state.loading_visible?'loading-indicator':
    !state.chart_present?'no-chart':state.trigger_disabled?'control-disabled':'state-observed';
  return state;
}'''


def safe_failure_state(value):
    if not isinstance(value,dict):value={}
    out={'observation_status':'observed' if value.get('observation_status')=='observed' else 'unavailable',
        'source_identity':value.get('source_identity') if isinstance(value.get('source_identity'),str) and value['source_identity'] in IDENTITIES else 'unavailable',
        'reason':value.get('reason') if isinstance(value.get('reason'),str) and value['reason'] in REASONS else 'unverified'}
    if isinstance(value.get('label'),str) and value['label'] in ('12 hour','24 hour','48 hour'):
        out['label']=value['label']
    for key in BOOL_FIELDS:
        if type(value.get(key)) is bool:out[key]=value[key]
    return out


def source_identity(url,model):
    from capture_readiness import valid_source,SOURCE_PATH_ALIASES
    if not valid_source(url,model):return 'invalid'
    return 'canonical' if urlsplit(url).path in SOURCE_PATH_ALIASES.get(model,()) else 'legacy'


def click_failure(exc):
    """Recognize fixed Playwright call-log facts without retaining any error text."""
    inner=exc.__cause__ if exc.__cause__ is not None else exc
    text=str(inner)[:6000].lower()
    for marker,reason in (
        ('intercepts pointer events','pointer_intercepted'),
        ('element was detached from the dom','detached'),
        ('element is not attached to the dom','detached'),
        ('element is not enabled','disabled'),
        ('element is not visible','not_visible'),
        ('element is outside of the viewport','outside_viewport'),
        ('element is not stable','unstable'),
        ('target page, context or browser has been closed','target_closed')):
        if marker in text:return reason
    return 'action_timeout' if type(inner).__name__=='TimeoutError' else 'unclassified'


def observe_capture_failure(page,model,diagnostics,exc):
    """Attach facts while the browser is open, leaving the exception unchanged."""
    state={'observation_status':'unavailable','source_identity':'unavailable','reason':'unverified'}
    try:state['source_identity']=source_identity(page.url,model)
    except Exception:pass
    handle=None
    try:
        handle=page.wait_for_function(STATE_SCRIPT,arg=model,timeout=OBSERVATION_TIMEOUT_MS,polling=1000)
        observed=handle.json_value()
        if isinstance(observed,dict):state.update(observed,observation_status='observed')
    except Exception:pass
    finally:
        if handle is not None:
            try:handle.dispose()
            except Exception:pass
    try:exc._model1_source_capture_state=safe_failure_state(state)
    except Exception:pass
    try:exc._model1_click_failure=click_failure(exc)
    except Exception:pass
    try:
        from capture_readiness import safe_network
        exc._model1_source_network=safe_network(diagnostics.summary())
    except Exception:pass
