"""Lab-only exact public-asset patch and chart-local render controls.

No production files/settings, source API, market-data extraction, or scanner
threshold changes. A new isolated BrowserContext is required for installation.
"""
from __future__ import annotations
import hashlib
from pathlib import Path

HERE=Path(__file__).resolve().parent
ASSET_URL='https://s3.coinglass.com/v1/cg/_next/static/chunks/69001-464e676a1cd1221f.js'
ASSET_SHA256='1cef0cf46e89017f01d14b69ee34193903ffb37b71def91453d12918451a5417'
PIXI_URL='https://s3.coinglass.com/v1/cg/_next/static/chunks/10679-f5d0c63436a34cc4.js'
PIXI_SHA256='4c6bfb003a2a7713014a21c00bea7bc61b58a06294c6c56bbc76881910ee9150'
MARKER=b'await this.app.init({resolution:1,background:this.state.color[0],resizeTo:document.getElementById("zxc")});'
INSERT=b'globalThis.__cg29RenderLab.attach(this.app);'


def patched_asset(body:bytes)->bytes:
    if hashlib.sha256(body).hexdigest()!=ASSET_SHA256:
        raise ValueError('render_lab_chart_asset_hash_changed')
    if body.count(MARKER)!=1:raise ValueError('render_lab_chart_insertion_changed')
    return body.replace(MARKER,MARKER+INSERT,1)


def install_context(context):
    """Install before new_page/navigation; return explicit interception evidence.

    Both actual network-loaded assets must match the independently observed
    bytes. Unseen/changed filenames cannot silently become an enabled candidate:
    require_ready() checks both route evidence and the chart hook before use.
    """
    state={'chart_seen':False,'pixi_seen':False,'failures':[],
           'chart_sha256':ASSET_SHA256,'pixi_sha256':PIXI_SHA256}
    context.add_init_script(path=str(HERE/'render_gate.js'))

    def intercept(route):
        url=route.request.url
        try:
            response=route.fetch(timeout=20000,max_redirects=0,max_retries=0)
            if response.status!=200:raise ValueError('render_lab_asset_http_status')
            body=response.body()
            headers={key:value for key,value in response.headers.items()
                     if key.lower() not in ('content-length','content-encoding','transfer-encoding','etag')}
            if url==ASSET_URL:
                body=patched_asset(body);state['chart_seen']=True
                route.fulfill(response=response,headers=headers,body=body)
            elif url==PIXI_URL:
                if hashlib.sha256(body).hexdigest()!=PIXI_SHA256:
                    raise ValueError('render_lab_pixi_asset_hash_changed')
                state['pixi_seen']=True
                route.fulfill(response=response,headers=headers,body=body)
            else:raise ValueError('render_lab_unexpected_asset_route')
        except Exception as exc:
            state['failures'].append(str(exc) if isinstance(exc,ValueError) else type(exc).__name__)
            route.abort()

    context.route(ASSET_URL,intercept)
    context.route(PIXI_URL,intercept)
    return state


def snapshot(page):
    return page.evaluate('() => globalThis.__cg29RenderLab ? globalThis.__cg29RenderLab.snapshot() : null')


def require_ready(page,state):
    if state.get('failures') or not state.get('chart_seen') or not state.get('pixi_seen'):
        raise ValueError('render_lab_assets_not_verified')
    entries=snapshot(page)
    if not isinstance(entries,list) or len(entries)!=1:
        raise ValueError('render_lab_unique_chart_required')
    return entries[0]


def _key(page,key):
    if key is not None:return key
    entries=snapshot(page)
    if not isinstance(entries,list) or len(entries)!=1:
        raise ValueError('render_lab_unique_chart_required')
    return entries[0]['key']


def enable(page,key=None,*,fps=.5):
    return page.evaluate('({key,fps})=>globalThis.__cg29RenderLab.enable(key,fps)',{'key':_key(page,key),'fps':fps})


def force_paint(page,key=None):
    return page.evaluate('key=>globalThis.__cg29RenderLab.forcePaint(key)',_key(page,key))


def disable(page,key=None):
    return page.evaluate('key=>globalThis.__cg29RenderLab.disable(key)',_key(page,key))


def restore_all(page):
    result=page.evaluate('()=>globalThis.__cg29RenderLab.restoreAll()')
    if not isinstance(result,list) or not result or any(item.get('restored') is not True for item in result):
        raise ValueError('render_lab_restore_failed')
    return result
