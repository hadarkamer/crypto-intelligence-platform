"""Bounded Model 3 render scheduling; numerical/source validation stays unchanged.

Only a reviewed public chart asset and its reviewed Pixi runtime are eligible.
Unrecognized source versions use the original renderer, not a guessed patch.
No market numbers, account state or private source APIs are inspected here.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CHART_URL = 'https://s3.coinglass.com/v1/cg/_next/static/chunks/69001-464e676a1cd1221f.js'
CHART_SHA = '1cef0cf46e89017f01d14b69ee34193903ffb37b71def91453d12918451a5417'
PIXI_URL = 'https://s3.coinglass.com/v1/cg/_next/static/chunks/10679-f5d0c63436a34cc4.js'
PIXI_SHA = '4c6bfb003a2a7713014a21c00bea7bc61b58a06294c6c56bbc76881910ee9150'
MARKER = b'await this.app.init({resolution:1,background:this.state.color[0],resizeTo:document.getElementById("zxc")});'
INSERT = b'try{globalThis.__cg29RenderLab.attach(this.app)}catch(_cgRenderObservationError){};'
_STATE = '_coinglass_model3_render_state'
FPS = .5


def _log(status, reason):
    # Arguments are internal fixed labels, never source text, URL or secrets.
    print('MODEL3_RENDER ' + json.dumps({'status': status, 'reason': reason}), flush=True)


def patch_public_chart(body: bytes) -> bytes | None:
    if hashlib.sha256(body).hexdigest() != CHART_SHA or body.count(MARKER) != 1:
        return None
    return body.replace(MARKER, MARKER + INSERT, 1)


def configure_context(context, model: int) -> None:
    if model != 3:
        return
    if isinstance(getattr(context, _STATE, None), dict):
        raise RuntimeError('model3_render_context_already_configured')
    state = {'chart': False, 'pixi': False, 'reason': 'assets_unobserved'}
    setattr(context, _STATE, state)
    context.add_init_script(path=str(ROOT / 'model3_render_gate.js'))

    def intercept(route):
        url = route.request.url
        try:
            response = route.fetch(timeout=20000, max_redirects=0, max_retries=0)
            body = response.body()
        except Exception:
            # One normal browser request is left to the browser's existing path.
            # No source-data endpoint or retry loop is introduced.
            state['reason'] = 'asset_fetch_unavailable'
            route.continue_()
            return
        headers = {key: value for key, value in response.headers.items()
                   if key.lower() not in ('content-length', 'content-encoding', 'transfer-encoding', 'etag')}
        if response.status != 200:
            state['reason'] = 'asset_http_status'
            route.fulfill(response=response, headers=headers, body=body)
            return
        rewritten = body
        if url == CHART_URL:
            candidate = patch_public_chart(body)
            state['chart'] = candidate is not None
            if candidate is not None:
                rewritten = candidate
            else:
                state['reason'] = 'chart_version_unrecognized'
        elif url == PIXI_URL:
            state['pixi'] = hashlib.sha256(body).hexdigest() == PIXI_SHA
            if not state['pixi']:
                state['reason'] = 'pixi_version_unrecognized'
        else:
            raise RuntimeError('model3_render_unexpected_asset')
        route.fulfill(response=response, headers=headers, body=rewritten)

    context.route(CHART_URL, intercept)
    context.route(PIXI_URL, intercept)


def _snapshot(page):
    return page.evaluate('() => globalThis.__cg29RenderLab ? globalThis.__cg29RenderLab.snapshot() : null')


def _restore(page, key, count):
    result = page.evaluate('key => globalThis.__cg29RenderLab.disable(key)', key)
    if (not isinstance(result, dict) or result.get('enabled') is not False
            or result.get('ticker_count') != count):
        raise RuntimeError('model3_render_restore_unverified')


@contextmanager
def observation(page, model: int, geometry: dict):
    """Scope acceleration to this map's one screenshot and original full read.

    A missing/version-changed optional hook preserves the pre-existing path.
    After activation, cleanup or state failures reject the observation rather
    than accepting an unverified screenshot. The browser is still closed by
    the existing capture lifecycle on either success or failure.
    """
    if model != 3:
        yield lambda: None
        return
    state = getattr(page.context, _STATE, None)
    if not isinstance(state, dict) or not state.get('chart') or not state.get('pixi'):
        _log('original', state.get('reason', 'not_configured') if isinstance(state, dict) else 'not_configured')
        yield lambda: None
        return
    entries = _snapshot(page)
    if (not isinstance(entries, list) or len(entries) != 1
            or not isinstance(entries[0], dict)):
        _log('original', 'chart_hook_unavailable')
        yield lambda: None
        return
    entry = entries[0]
    wanted = {key: geometry.get(key) for key in ('x', 'y', 'width', 'height')}
    if (entry.get('enabled') is not False or type(entry.get('key')) is not int
            or type(entry.get('ticker_count')) is not int
            or (entry.get('canvas') or {}).get('rect') != wanted):
        # An already-active hook is not an untouched fallback and must fail.
        if entry.get('enabled') is True:
            raise RuntimeError('model3_render_unexpected_active_hook')
        _log('original', 'chart_hook_incompatible')
        yield lambda: None
        return
    key, count = entry['key'], entry['ticker_count']
    activated = False
    failed = False
    try:
        # The gate verifies app/renderer/canvas identity and listener count.
        # Even a partial activation is restored by the finally block.
        activated = True
        result = page.evaluate('({key,fps}) => globalThis.__cg29RenderLab.enable(key,fps)',
                               {'key': key, 'fps': FPS})
        if (not isinstance(result, dict) or result.get('enabled') is not True
                or result.get('ticker_count') != count):
            raise RuntimeError('model3_render_activation_unverified')
        _log('active', 'verified_chart_local_gate')

        def prepare():
            from model3_clean_frame import prepare_frame
            def paint(target):
                return target.evaluate('key => globalThis.__cg29RenderLab.forcePaint(key)', key)
            return prepare_frame(page, paint)

        yield prepare
    except BaseException:
        failed = True
        raise
    finally:
        if activated:
            try:
                _restore(page, key, count)
            except Exception:
                _log('failed', 'restore_unverified')
                if not failed:
                    raise
