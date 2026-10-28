"""Observe source rendering and bounded network facts without changing the page."""
from collections import Counter
import json
import math
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

LABELS={'12h':'12 hour','24h':'24 hour','48h':'48 hour'}
# Model 3's legacy URL now redirects to this observed public heatmap route.
# Models 1 and 2 still use their original routes; do not infer other aliases.
SOURCE_PATH_ALIASES={3:frozenset({'/liquidation-heatmap-model3'})}
REASONS=frozenset({'render-checks-passed','loading-indicator','blur','no-chart',
    'visible-dialog','login-required','wrong-label','invalid-source','unverified',
    'axis-not-ready','render-pending'})
NETWORK_KINDS=frozenset({'timeout','connection','cancelled','blocked','other'})
LOGIN_GATE_SCRIPT=r'''() => {
  const visible = el => {
    const r=el.getBoundingClientRect();
    if(r.width<=0 || r.height<=0) return false;
    for(let n=el;n;n=n.parentElement) {
      const s=getComputedStyle(n);
      if(s.display==='none' || s.visibility==='hidden' || Number(s.opacity||1)===0) return false;
    }
    return true;
  };
  return [...document.querySelectorAll('[role="dialog"], .MuiModal-root')].some(e =>
    visible(e) && /\blog\s*in\s+to\s+unlock\s+full\s+data\b/i.test(e.innerText||''));
}'''
STATE_SCRIPT=r'''() => {
  const visible = el => {
    const r=el.getBoundingClientRect();
    if(r.width<=0 || r.height<=0) return false;
    for(let n=el;n;n=n.parentElement) {
      const s=getComputedStyle(n);
      if(s.display==='none' || s.visibility==='hidden' || Number(s.opacity||1)===0) return false;
    }
    return true;
  };
  const triggers=[...document.querySelectorAll('[role="combobox"], [aria-haspopup="listbox"], button.MuiSelect-button')];
  const trigger=triggers.find(e=>visible(e) && /^(12|24|48) hour$/i.test((e.textContent||'').trim()));
  const label=trigger ? trigger.textContent.trim().toLowerCase() : '';
  const ui={login_link_visible:[...document.querySelectorAll('a[href="/login"]')].some(visible),
    account_link_present:!!document.querySelector('a[href="/account"]')};
  const state=(ready,reason)=>{
    const result={ready,reason,label,...ui};
    // Observation only: none of these fields participates in readiness or waits.
    // Reuse this DOM evaluation; no extra browser request, timer or screenshot.
    try {
      const charts=[...document.querySelectorAll('canvas')].filter(e=>{
        const r=e.getBoundingClientRect();return visible(e)&&r.width>=400&&r.height>=200;
      }).sort((a,b)=>{
        const ar=a.getBoundingClientRect(),br=b.getBoundingClientRect();
        return br.width*br.height-ar.width*ar.height;
      });
      const notices=[...document.querySelectorAll('img[alt], [role="status"], [role="alert"], .ant-empty-description')].slice(0,64).filter(visible);
      const words=e=>(e.innerText||e.getAttribute('alt')||'').trim().slice(0,256);
      const rect=charts.length?charts[0].getBoundingClientRect():null;
      result._render_observation={observed_ms:performance.now(),canvas_count:charts.length,
        visible_nodata:notices.some(e=>/^(?:no\s*data|no data available)$/i.test(words(e))),
        visible_request_limit:notices.some(e=>/too many requests|requests? limit|rate limit|request too frequent/i.test(words(e))),
        viewport:{width:innerWidth,height:innerHeight},scroll_x:scrollX,scroll_y:scrollY};
      if(rect)result._render_observation.canvas_rect={x:rect.x+scrollX,y:rect.y+scrollY,width:rect.width,height:rect.height};
    } catch (_) {} // Diagnostic collection must never change the original verdict.
    return result;
  };
  if([...document.querySelectorAll('[role="dialog"], .MuiModal-root')].some(e =>
      visible(e) && /\blog\s*in\s+to\s+unlock\s+full\s+data\b/i.test(e.innerText||'')))
    return state(false,'login-required');
  const canvases=[...document.querySelectorAll('canvas')].filter(e=> {
    const r=e.getBoundingClientRect(); return visible(e) && r.width>=400 && r.height>=200;
  });
  if(!canvases.length) return state(false,'no-chart');
  const canvas=canvases.sort((a,b)=> {
    const ar=a.getBoundingClientRect(),br=b.getBoundingClientRect();
    return br.width*br.height-ar.width*ar.height;
  })[0];
  const cr=canvas.getBoundingClientRect();
  const intersection=e=> {
    const r=e.getBoundingClientRect();
    return Math.max(0,Math.min(r.right,cr.right)-Math.max(r.left,cr.left)) *
      Math.max(0,Math.min(r.bottom,cr.bottom)-Math.max(r.top,cr.top));
  };
  const indicators=document.querySelectorAll('[role="progressbar"], [aria-busy="true"], .MuiCircularProgress-root, .ant-spin-spinning, [class*="spinner" i], [data-loading="true"]');
  if([...indicators].some(e=>visible(e) && intersection(e)>0)) return state(false,'loading-indicator');
  const blurred=value=> [...(value||'').matchAll(/blur\(\s*([\d.]+)px\s*\)/g)].some(m=>Number(m[1])>0);
  for(let n=canvas;n;n=n.parentElement) {
    if(blurred(getComputedStyle(n).filter)) return state(false,'blur');
  }
  // A sibling veil can blur the canvas with backdrop-filter, rather than filter.
  if([...document.querySelectorAll('body *')].some(e=>visible(e) &&
      intersection(e)>=cr.width*cr.height*0.25 &&
      blurred(getComputedStyle(e).backdropFilter || getComputedStyle(e).webkitBackdropFilter)))
    return state(false,'blur');
  const dialogs=document.querySelectorAll('[role="dialog"], .MuiModal-root');
  if([...dialogs].some(e=>visible(e) && intersection(e)>0)) return state(false,'visible-dialog');
  return state(true,'render-checks-passed');
}'''

# Model 3 exposes its price ticks as visible DOM text. A purple canvas without
# those ticks is an unfinished chart, even when no spinner remains.
MODEL3_AXIS_SCRIPT=r'''() => {
  const axes=[...document.querySelectorAll('[data-axis="y"][role="button"]')].filter(e=>{
    const r=e.getBoundingClientRect();
    if(r.width<=0||r.height<=0)return false;
    for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
      if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity||1)===0)return false;
    }return true;
  });
  if(axes.length!==1)return null;
  const texts=axes[0].innerText.trim().split(/\s+/);
  if(texts.length<2||texts.length>64||texts.some(t=>!/^\d+(?:,\d{3})*(?:\.\d+)?$/.test(t)))return null;
  const ticks=texts.map(t=>Number(t.replaceAll(',',''))).sort((a,b)=>a-b);
  return ticks.every((v,i)=>v>0&&v<1e12&&(!i||v>ticks[i-1]))?ticks:null;
}'''

MODEL3_PAINT_SCRIPT=r'''timeout => new Promise(resolve=>{
  const read=('''+MODEL3_AXIS_SCRIPT+r''');let previous='',stable=0,frame;
  const timer=setTimeout(()=>{cancelAnimationFrame(frame);resolve(false)},timeout);
  const check=()=>{const ticks=read(),key=ticks?JSON.stringify(ticks):'';
    stable=key&&key===previous?stable+1:0;previous=key;
    if(stable>=2){clearTimeout(timer);resolve(true)}else frame=requestAnimationFrame(check);
  };frame=requestAnimationFrame(check);
})'''


def valid_source(url,heatmap_model):
    from heatmap_models import source_url
    if not isinstance(url,str):return False
    try:
        value=urlsplit(url); expected=urlsplit(source_url(heatmap_model))
        query=parse_qs(value.query,keep_blank_values=True)
        return (value.scheme=='https' and value.hostname in ('coinglass.com','www.coinglass.com')
            and not value.username and not value.password and value.port in (None,443)
            and value.path in ({expected.path}|SOURCE_PATH_ALIASES.get(heatmap_model,frozenset()))
            and query.get('coin')==['BTC']
            and query.get('type')==['symbol'])
    except (ValueError,TypeError):return False


def safe_readiness(value):
    if not isinstance(value,dict):value={}
    reason=value.get('reason')
    out={'ready':value.get('ready') is True and reason=='render-checks-passed',
        'reason':reason if isinstance(reason,str) and reason in REASONS else 'unverified'}
    label=value.get('label')
    if isinstance(label,str) and label in LABELS.values():out['label']=label
    for key in ('login_link_visible','account_link_present'):
        if type(value.get(key)) is bool:out[key]=value[key]
    ticks=value.get('price_axis_ticks')
    if (isinstance(ticks,list) and 2<=len(ticks)<=64
        and all(type(v) in (int,float) and 0<v<1e12 for v in ticks)
        and all(a<b for a,b in zip(ticks,ticks[1:]))):
        out['price_axis_ticks']=ticks
    return out


def _safe_render_measurement(value):
    """Retain finite geometry and fixed booleans only; never page text or URLs."""
    if not isinstance(value,dict):return {}
    out={}
    for key in ('visible_nodata','visible_request_limit'):
        if type(value.get(key)) is bool:out[key]=value[key]
    count=value.get('canvas_count')
    if type(count) is int and 0<=count<=1000:out['canvas_count']=count
    for key in ('observed_ms','scroll_x','scroll_y'):
        number=value.get(key)
        if type(number) in (int,float) and math.isfinite(number) and 0<=number<=1e12:
            out[key]=number
    for key,fields in (('canvas_rect',('x','y','width','height')),('viewport',('width','height'))):
        coordinates=value.get(key)
        if not isinstance(coordinates,dict):continue
        numbers={name:coordinates.get(name) for name in fields}
        if (all(type(v) in (int,float) and math.isfinite(v) and -1e6<=v<=1e6 for v in numbers.values())
            and numbers['width']>0 and numbers['height']>0):out[key]=numbers
    return out


def safe_render_observations(value):
    """Bound the two original observations surrounding the existing screenshot."""
    if not isinstance(value,list):return []
    result=[]
    for item in value[:2]:
        if not isinstance(item,dict) or item.get('stage') not in ('before_screenshot','after_screenshot'):continue
        measured=_safe_render_measurement(item)
        if not measured:continue
        state=safe_readiness(item)
        result.append({'stage':item['stage'],'ready':state['ready'],'reason':state['reason'],
            **({'label':state['label']} if 'label' in state else {}),**measured})
    return result


def current_state(page,timeframe,heatmap_model):
    if timeframe not in LABELS or not valid_source(page.url,heatmap_model):
        return {'ready':False,'reason':'invalid-source'}
    try:
        observed=page.evaluate(STATE_SCRIPT)
        state=safe_readiness(observed)
    except Exception:return {'ready':False,'reason':'unverified'}
    try:
        measurement=_safe_render_measurement(observed.get('_render_observation'))
        if measurement:state['_render_observation']=measurement
    except Exception:pass
    if state['reason']!='login-required' and state.get('label')!=LABELS[timeframe]:
        state.update(ready=False,reason='wrong-label')
    if heatmap_model==3 and state['ready']:
        try:ticks=page.evaluate(MODEL3_AXIS_SCRIPT)
        except Exception:ticks=None
        checked=safe_readiness({**state,'price_axis_ticks':ticks})
        if 'price_axis_ticks' not in checked:state.update(ready=False,reason='axis-not-ready')
        else:state['price_axis_ticks']=checked['price_axis_ticks']
    return state


def require_unblocked_source(page,heatmap_model,*,phase):
    """An explicit visible source-data modal is a gate; a header Login is not."""
    from model1_execution import StageFailure
    try:
        if not valid_source(page.url,heatmap_model):return
        blocked=page.evaluate(LOGIN_GATE_SCRIPT) is True
    except Exception:return
    if not blocked:return
    error=StageFailure('source_login_required')
    error._model1_capture_phase=phase
    error._model1_source_readiness={'ready':False,'reason':'login-required'}
    raise error


def wait_for_render(page,timeframe,heatmap_model,timeout_ms=60_000):
    """Poll only existing DOM; no navigation, refresh, requests, clicks or styling."""
    state=current_state(page,timeframe,heatmap_model)
    def painted(value):
        if heatmap_model!=3 or not value['ready']:return value
        try:ready=page.evaluate(MODEL3_PAINT_SCRIPT,6000) is True
        except Exception:ready=False
        return current_state(page,timeframe,heatmap_model) if ready else {**value,'ready':False,'reason':'render-pending'}
    if state['ready'] or state['reason'] in ('invalid-source','login-required'):return painted(state)
    timeout=max(1,min(int(timeout_ms),60_000))
    predicate='expected => { const s=('+STATE_SCRIPT+')(); return s.reason === "login-required" || (s.ready === true && s.label === expected); }'
    if heatmap_model==3:
        predicate=predicate.replace('s.label === expected','s.label === expected && ('+MODEL3_AXIS_SCRIPT+')()')
    try:
        page.wait_for_function(predicate,arg=LABELS[timeframe],timeout=timeout,polling=1000)
        page.wait_for_timeout(1000)
    except Exception:pass
    return painted(current_state(page,timeframe,heatmap_model))


def safe_network(value):
    if not isinstance(value,dict):value={}
    out={'http_errors':[],'network_errors':[],'page_error_count':0}
    count=value.get('page_error_count')
    if type(count) is int:out['page_error_count']=max(0,min(count,1000))
    for field in ('http_errors','network_errors'):
        rows=value.get(field)
        if not isinstance(rows,list):continue
        for row in rows[:6]:
            if not isinstance(row,dict) or row.get('host_group') not in ('coinglass','other'):continue
            count=row.get('count')
            if type(count) is not int or not 1<=count<=1000:continue
            item={'host_group':row['host_group'],'count':count}
            if field=='http_errors':
                status=row.get('status')
                if type(status) is not int or not 400<=status<=599:continue
                item['status']=status
            else:
                kind=row.get('kind')
                if not isinstance(kind,str) or kind not in NETWORK_KINDS:continue
                item['kind']=kind
            out[field].append(item)
    return out


class CaptureNetworkDiagnostics:
    """Passive counts only; never read headers, cookies, bodies, query strings or messages."""
    def __init__(self,page):
        self.statuses=Counter();self.failures=Counter();self.page_errors=0
        page.on('response',self.response_seen)
        page.on('requestfailed',self.request_failed)
        page.on('pageerror',self.page_error)

    def host_group(self,url):
        from model1_execution import AD_HOSTS
        if not isinstance(url,str) or AD_HOSTS.match(url):return None
        try:host=urlsplit(url).hostname or ''
        except ValueError:return None
        return 'coinglass' if host=='coinglass.com' or host.endswith('.coinglass.com') else 'other'

    def response_seen(self,response):
        group=self.host_group(response.url);status=response.status
        if group and type(status) is int and 400<=status<=599:
            self.statuses[(group,status)]=min(1000,self.statuses[(group,status)]+1)

    def request_failed(self,request):
        group=self.host_group(request.url)
        if not group:return
        kind={'net::ERR_TIMED_OUT':'timeout','net::ERR_CONNECTION_TIMED_OUT':'timeout',
            'net::ERR_CONNECTION_RESET':'connection','net::ERR_CONNECTION_REFUSED':'connection',
            'net::ERR_NAME_NOT_RESOLVED':'connection','net::ERR_ABORTED':'cancelled',
            'net::ERR_BLOCKED_BY_CLIENT':'blocked'}.get(request.failure,'other')
        self.failures[(group,kind)]=min(1000,self.failures[(group,kind)]+1)

    def page_error(self,error):self.page_errors=min(1000,self.page_errors+1)

    def summary(self):
        return safe_network({'http_errors':[{'host_group':h,'status':s,'count':n}
            for (h,s),n in self.statuses.most_common(6)],
            'network_errors':[{'host_group':h,'kind':k,'count':n}
            for (h,k),n in self.failures.most_common(6)],'page_error_count':self.page_errors})


def verify_saved_render(page,timeframe,heatmap_model,state,image_path,diagnostics):
    """Keep the one original PNG even on failure, then stop before paid analysis."""
    from model1_execution import StageFailure
    before=safe_readiness(state);observed_after=current_state(page,timeframe,heatmap_model)
    after=safe_readiness(observed_after)
    verdict=before if before['ready'] is not True else after
    network=safe_network(diagnostics.summary())
    payload={'source_readiness':verdict,'source_network':network,'assessment_validated':False}
    try:
        observations=safe_render_observations([
            {**before,**state.get('_render_observation',{}),'stage':'before_screenshot'},
            {**after,**observed_after.get('_render_observation',{}),'stage':'after_screenshot'}])
        if observations:payload['render_observations']=observations
    except Exception:pass
    if heatmap_model==3:
        payload['price_axis_before']=before.get('price_axis_ticks')
        payload['price_axis_after']=after.get('price_axis_ticks')
        if before['ready'] and after['ready'] and before.get('price_axis_ticks')!=after.get('price_axis_ticks'):
            verdict={**after,'ready':False,'reason':'render-pending'}
            payload['source_readiness']=verdict
    target=Path(image_path).with_suffix('.render.json')
    target.write_text(json.dumps(payload,allow_nan=False),encoding='utf-8');target.chmod(0o600)
    if verdict['ready'] is not True:
        error=StageFailure('source_login_required' if verdict['reason']=='login-required' else 'source_not_readable')
        error._model1_capture_phase='render_readiness'
        error._model1_source_readiness=verdict
        error._model1_source_network=network
        raise error
    return verdict
