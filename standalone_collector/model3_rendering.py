"""Chart-local Model 3 scheduling via Pixi's existing application-init hook.

No public JavaScript rewriting, source-data endpoint or numerical-reader change.
Only the inspected Pixi 8.21.0 contract is enabled. Unknown/missing hooks keep
normal rendering. A selected visible canvas and listener contract are required.
"""
from __future__ import annotations
from contextlib import contextmanager
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
_STATE='_coinglass_model3_render_state'
FPS=.5
DISCOVERY=r'''(() => {
  if (globalThis.__cgModel3Discovery) return;
  const apps=[], observed=new WeakSet();
  let unsupported=false;
  const descriptor=Object.getOwnPropertyDescriptor(globalThis,'__PIXI_APP_INIT__');
  const prior=descriptor && descriptor.value;
  const usable=(!descriptor || ('value' in descriptor && descriptor.writable!==false))
    && (prior===undefined || prior===null || typeof prior==='function');
  function init(app,version) {
    const result=typeof prior==='function'?Reflect.apply(prior,this,arguments):undefined;
    try {
      if(version!=='8.21.0'){unsupported=true;return result;}
      if(app && typeof app==='object' && !observed.has(app)){
        observed.add(app);apps.push(app);
      }
    } catch (_) { /* Observation must not alter the source init result. */ }
    return result;
  }
  if(usable)globalThis.__PIXI_APP_INIT__=init;
  const api={attachSelected(rect){
    if(!usable || globalThis.__PIXI_APP_INIT__!==init)
      return {supported:false,reason:'init_hook_unavailable'};
    const matches=apps.filter(app=>{
      try{
        const canvas=app.renderer && app.renderer.canvas;
        if(!canvas || !canvas.isConnected)return false;
        const r=canvas.getBoundingClientRect();
        return ['x','y','width','height'].every(k=>r[k]===rect[k]) && r.width>0 && r.height>0;
      }catch(_){return false;}
    });
    if(matches.length!==1)return {supported:false,reason:matches.length>1?'ambiguous_chart':
      unsupported?'renderer_version_unrecognized':'selected_chart_unobserved'};
    try{
      const key=globalThis.__cg29RenderLab.attach(matches[0]);
      const entry=globalThis.__cg29RenderLab.snapshot().find(e=>e.key===key);
      if(!entry)return {supported:false,reason:'chart_hook_unavailable'};
      return {supported:true,version:'8.21.0',entry};
    }catch(_){return {supported:false,reason:'chart_hook_incompatible'};}
  }};
  Object.defineProperty(globalThis,'__cgModel3Discovery',{value:api,configurable:true});
})();'''


def _log(status,reason):
    print('MODEL3_RENDER '+json.dumps({'status':status,'reason':reason}),flush=True)


def configure_context(context,model:int)->None:
    if model!=3:return
    if getattr(context,_STATE,False) is True:raise RuntimeError('model3_render_context_already_configured')
    # One init script preserves ordering; source request routing stays untouched.
    context.add_init_script(script=(ROOT/'model3_render_gate.js').read_text()+ '\n'+DISCOVERY)
    setattr(context,_STATE,True)


def _restore(page,key,count):
    result=page.evaluate('key => globalThis.__cg29RenderLab.disable(key)',key)
    if (not isinstance(result,dict) or result.get('enabled') is not False
            or result.get('ticker_count')!=count):
        raise RuntimeError('model3_render_restore_unverified')


@contextmanager
def preparation(page,model:int):
    """Limit the already visible chart while controls and readiness update.

    This is only an optimization: a missing/unready chart keeps the original
    path. The normal post-selection readiness and clean PNG checks still run.
    End this scope before observation so that its final geometry is rebound.
    """
    if model!=3 or getattr(page.context,_STATE,False) is not True:
        yield
        return
    from capture_readiness import STATE_SCRIPT, geometry_from_state, valid_source
    geometry=None
    if valid_source(page.url,model):
        try:
            state=page.evaluate(STATE_SCRIPT)
            measured=state.get('_render_observation',{})
            geometry=geometry_from_state(state)
            if (state.get('ready') is not True or measured.get('canvas_count')!=1
                    or not geometry):
                geometry=None
            else:
                # Discovery compares viewport rectangles; readiness uses page
                # coordinates. The same observation supplies the scroll offset.
                geometry['x']-=measured['scroll_x']
                geometry['y']-=measured['scroll_y']
        except Exception:
            geometry=None
    if geometry is None:
        _log('original','preparation_chart_unverified')
        yield
        return
    with observation(page,model,geometry):
        yield


@contextmanager
def observation(page,model:int,geometry:dict):
    if model!=3:
        yield lambda:None
        return
    if getattr(page.context,_STATE,False) is not True:
        _log('original','not_configured')
        yield lambda:None
        return
    wanted={key:geometry.get(key) for key in ('x','y','width','height')}
    discovered=page.evaluate('rect => globalThis.__cgModel3Discovery ? globalThis.__cgModel3Discovery.attachSelected(rect) : null',wanted)
    if not isinstance(discovered,dict) or discovered.get('supported') is not True:
        allowed={'init_hook_unavailable','ambiguous_chart','renderer_version_unrecognized',
                 'selected_chart_unobserved','chart_hook_unavailable','chart_hook_incompatible'}
        reason=discovered.get('reason') if isinstance(discovered,dict) else None
        _log('original',reason if reason in allowed else 'chart_hook_unavailable')
        yield lambda:None
        return
    entry=discovered.get('entry')
    if (not isinstance(entry,dict) or discovered.get('version')!='8.21.0'
            or type(entry.get('key')) is not int or type(entry.get('ticker_count')) is not int
            or (entry.get('canvas') or {}).get('rect')!=wanted):
        raise RuntimeError('model3_render_discovery_unverified')
    if entry.get('enabled') is not False:
        raise RuntimeError('model3_render_unexpected_active_hook')
    key,count=entry['key'],entry['ticker_count']
    activated=False;failed=False
    try:
        activated=True
        result=page.evaluate('({key,fps}) => globalThis.__cg29RenderLab.enable(key,fps)',{'key':key,'fps':FPS})
        if not isinstance(result,dict) or result.get('enabled') is not True or result.get('ticker_count')!=count:
            raise RuntimeError('model3_render_activation_unverified')
        _log('active','verified_pixi_init_hook')
        def prepare():
            from model3_clean_frame import prepare_frame
            def paint(target):return target.evaluate('key => globalThis.__cg29RenderLab.forcePaint(key)',key)
            return prepare_frame(page,paint)
        yield prepare
    except BaseException:
        failed=True
        raise
    finally:
        if activated:
            try:_restore(page,key,count)
            except Exception:
                _log('failed','restore_unverified')
                if not failed:raise
