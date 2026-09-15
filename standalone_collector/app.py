"""Independent Model 1 service; no bot process and no scheduled collection.

--probe reuses the configured CoinGlass session for ONE 12H capture. Without a
session it stops before opening a browser. It never calls OpenAI or writes sheets.
Authenticated diagnostics are private; the previous public image URL is retired.
"""
import asyncio
import contextlib
import hmac
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
RUNTIME = HERE / 'runtime'
sys.path.insert(0, str(RUNTIME))
DIAG = HERE / 'diagnostics'
SESSION_KEYS = ('COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON')
IMAGE_NAMES = {'not-ready.png', 'coinglass_btc_heatmap_12h.png'}
SAFE_REASONS = {
    'loading-indicator', 'blur', 'no-canvas', 'wrong-label', 'invalid-source',
    'source-not-ready', 'source-session-missing', 'source-session-invalid',
}


def source_session_configured():
    """Presence only; never return credentials, cookie names, or account details."""
    return any(bool(os.getenv(name, '').strip()) for name in SESSION_KEYS)


def _private_directory(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)


def _save_report(directory, report):
    path = directory / 'report.json'
    path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    path.chmod(0o600)
    print('MODEL1_SOURCE_PROBE ' + json.dumps(report), flush=True)
    return report


def probe(*, capture_fn=None, directory=None):
    """Reuse the legacy session-loading path; no extraction or transfer of secrets.

    Render invokes this on an explicitly initiated build. One call means at most
    one capture, no retries, no AI call. Presence does not prove a valid login.
    capture_fn and directory exist for offline tests only, never HTTP input.
    """
    directory = Path(directory) if directory is not None else DIAG
    _private_directory(directory)
    # Never serve a leftover success image after a failed or unconfigured run.
    for name in IMAGE_NAMES | {'not-ready.json', 'report.json'}:
        (directory / name).unlink(missing_ok=True)
    supplied = source_session_configured()
    report = {
        'timeframe_requested': '12H', 'source_attempts': 0, 'ai_calls': 0,
        'sheet_writes': 0, 'time_utc': datetime.now(timezone.utc).isoformat(),
        'mode': 'configured_session', 'source_session_supplied': supplied,
        'status': 'not_configured', 'code': 'SOURCE_SESSION_MISSING',
    }
    if not supplied:
        return _save_report(directory, report)

    # Disable passive network diagnostics for authenticated visits. Credential
    # values remain only in the environment read by the existing capture code.
    previous_diagnostics = os.environ.get('MODEL1_NETWORK_DIAGNOSTICS')
    os.environ['MODEL1_NETWORK_DIAGNOSTICS'] = 'false'
    try:
        if capture_fn is None:
            from market_vision.coinglass_heatmap_capture import capture_heatmaps
            capture_fn = capture_heatmaps
        report.update(status='failed', code='SOURCE_CAPTURE_FAILED', source_attempts=1)
        images = capture_fn(directory, timeframes=('12h',))
        expected = directory / 'coinglass_btc_heatmap_12h.png'
        if (not isinstance(images, list) or len(images) != 1
                or images[0].get('timeframe') != '12h'
                or Path(images[0].get('image', '')).resolve() != expected.resolve()
                or not expected.is_file() or expected.stat().st_size > 4 * 1024 * 1024):
            report['code'] = 'INVALID_CAPTURE_EVIDENCE'
        else:
            with expected.open('rb') as stream:
                is_png = stream.read(8) == b'\x89PNG\r\n\x1a\n'
            if not is_png:
                report['code'] = 'INVALID_CAPTURE_EVIDENCE'
            else:
                report.update(
                    status='render_checks_passed', code='VISUAL_REVIEW_REQUIRED',
                    image=expected.name,
                    captured_at=datetime.now(timezone.utc).isoformat(),
                    note='Readiness checks passed; visual review is required before AI or saving.',
                )
    except Exception as exc:
        # Never stringify provider/browser exceptions: those can include cookies,
        # headers, URLs, local paths, or account information.
        report['status'] = 'failed'
        if type(exc).__name__ == 'SourceNotReady':
            report['code'] = 'SOURCE_NOT_READY'
        elif type(exc).__name__ == 'TimeoutError':
            report['code'] = 'SOURCE_TIMEOUT'
        else:
            report['code'] = 'SOURCE_CAPTURE_FAILED'
        state_path = directory / 'not-ready.json'
        if state_path.is_file() and state_path.stat().st_size <= 8192:
            try:
                state = json.loads(state_path.read_text())
                reason = state.get('reason')
                report['reason'] = reason if reason in SAFE_REASONS else 'source-not-ready'
                if state.get('label') in {'12 hour', '24 hour'}:
                    report['observed_timeframe'] = state['label']
            except (ValueError, OSError, AttributeError, TypeError):
                pass
        if (directory / 'not-ready.png').is_file():
            report['image'] = 'not-ready.png'
    finally:
        if previous_diagnostics is None:
            os.environ.pop('MODEL1_NETWORK_DIAGNOSTICS', None)
        else:
            os.environ['MODEL1_NETWORK_DIAGNOSTICS'] = previous_diagnostics
        for name in IMAGE_NAMES | {'not-ready.json'}:
            path = directory / name
            if path.is_file():
                path.chmod(0o600)
    return _save_report(directory, report)


def read_public_probe_report():
    """Expose only a small allowlist, even when old diagnostic files exist."""
    path = DIAG / 'report.json'
    if not path.is_file() or path.stat().st_size > 8192:
        return None
    try:
        value = json.loads(path.read_text())
    except (ValueError, OSError):
        return None
    if not isinstance(value, dict):
        return None
    keys = {'timeframe_requested', 'source_attempts', 'ai_calls', 'sheet_writes',
            'time_utc', 'captured_at', 'mode', 'source_session_supplied',
            'status', 'code', 'reason', 'observed_timeframe'}
    return {k: v for k, v in value.items() if k in keys}


async def diagnostic_image(request):
    """Private evidence; GET never opens the source or starts a model call."""
    from aiohttp import web
    token = os.getenv('COINGLASS_COLLECTOR_TOKEN', '')
    header = request.headers.get('Authorization', '')
    if (len(token) < 32 or len(header) > 1024
            or not hmac.compare_digest(header.encode(), ('Bearer ' + token).encode())):
        raise web.HTTPUnauthorized()
    report_path = DIAG / 'report.json'
    if not report_path.is_file() or report_path.stat().st_size > 8192:
        raise web.HTTPNotFound()
    try:
        report = json.loads(report_path.read_text())
        name = report.get('image')
    except (ValueError, OSError, AttributeError):
        raise web.HTTPNotFound() from None
    if not isinstance(name, str) or name not in IMAGE_NAMES:
        raise web.HTTPNotFound()
    path = DIAG / name
    if not path.is_file() or path.is_symlink():
        raise web.HTTPNotFound()
    return web.FileResponse(path, headers={
        'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
    })


async def run_worker(store):
    # Independent consumer; no Telegram or production bot is imported.
    from collection_bridge import run_existing_scanner, BridgeError
    while True:
        held = None
        try:
            claimed = await asyncio.to_thread(store.claim)
            if claimed:
                held, row = claimed
                jid = str(row['id'])
                try:
                    result, image = await run_existing_scanner(row['timeframe'], jid)
                    await asyncio.to_thread(store.finish, jid, result, image)
                except asyncio.CancelledError:
                    await asyncio.to_thread(store.finish, jid, None, None, 'interrupted')
                    raise
                except BridgeError as exc:
                    await asyncio.to_thread(store.finish, jid, None, None, exc.code)
                except Exception:
                    await asyncio.to_thread(store.finish, jid, None, None, 'scan_failed')
        except asyncio.CancelledError:
            raise
        except Exception:
            print('MODEL1_WORKER temporary store failure', flush=True)
        finally:
            if held is not None:
                await asyncio.to_thread(held.close)
        await asyncio.sleep(2)


def create_app():
    from aiohttp import web
    from collection_bridge import JobStore, register_collection_routes
    application = web.Application(client_max_size=2048)
    enabled = os.getenv('COLLECTION_BRIDGE_ENABLED', '').lower() == 'true'
    configured = bool(os.getenv('DATABASE_URL') and os.getenv('OPENAI_API_KEY')
                      and len(os.getenv('COINGLASS_COLLECTOR_TOKEN', '')) >= 32
                      and source_session_configured())
    active = enabled and configured

    async def health(_):
        result = {
            'ok': True, 'service': 'decision-hub-model1-collector',
            'mode': 'collection' if active else 'not_configured',
            'bot_started': False, 'scheduled_collection': False,
            'source_session_configured': source_session_configured(),
        }
        report = read_public_probe_report()
        if report is not None:
            result['source_probe'] = report
        return web.json_response(result, headers={'Cache-Control': 'no-store'})

    application.router.add_get('/', health)
    application.router.add_get('/health', health)
    # /source-probe.png is deliberately NOT registered, even when the old
    # PUBLIC_SOURCE_DIAGNOSTIC flag is set. Login-session captures are private.
    application.router.add_get('/api/collection/model1/source-probe/evidence', diagnostic_image)
    store = JobStore(os.getenv('DATABASE_URL', ''), hourly_limit=2) if active else None
    register_collection_routes(application, store=store, enabled=active, start_worker=False)
    if active:
        async def lifecycle(app):
            await asyncio.to_thread(store.initialize)
            task = asyncio.create_task(run_worker(store))
            yield
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        application.cleanup_ctx.append(lifecycle)
    return application


if __name__ == '__main__':
    if '--probe' in sys.argv:
        probe()
    else:
        from aiohttp import web
        web.run_app(create_app(), host='0.0.0.0', port=int(os.getenv('PORT', '10000')), access_log=None)
