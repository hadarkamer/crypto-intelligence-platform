"""Lab-only provider fixtures loaded by the UNCHANGED scanner child entrypoint.

No queue, worker, capture, reader, result, or validator is replaced. Network
providers and wall clock are controlled. Never add this directory to production.
"""
import sys
from pathlib import Path

if Path(sys.argv[0]).name == 'collection_model1_task.py':
    import base64, hashlib, resource
    import atexit
    import importlib.util
    import json
    import os
    import time
    from contextlib import contextmanager
    from datetime import datetime, timezone
    from types import SimpleNamespace

    lab = Path(__file__).resolve().parent
    root = Path(os.environ['QUEUE_LAB_COLLECTOR'])
    state = json.loads(Path(os.environ['QUEUE_LAB_CONTROL']).read_text())
    at = datetime.fromisoformat(state['now'].replace('Z', '+00:00'))
    model = int(os.environ['COLLECTION_HEATMAP_MODEL'])
    tf, jid, output = sys.argv[1:]
    audit = Path(os.environ['QUEUE_LAB_AUDIT'])
    audit.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    counts = dict(contexts=0, screenshots=0, ai_requests=0, early_active=0, late_active=0, restored=0)
    browsers = []
    input_digests = []
    evidence_dir = audit / jid
    evidence_dir.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(root / 'standalone_collector/runtime'))
    spec = importlib.util.spec_from_file_location('fixture', root / 'experiments/model3-preparation-fix-20261031/check_capture.py')
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    import model3_rendering as rendering
    import heatmap_hover
    from market_vision import openai_heatmap_scanner as scanner
    from playwright.sync_api import Browser, Page

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)
    heatmap_hover.datetime = Clock
    original_context, original_shot = Browser.new_context, Page.screenshot
    original_preparation, original_observation = rendering.preparation, rendering.observation
    html = fixture.HTML.replace('MODEL', str(model)).replace('STAMP', at.strftime('%Y-%m-%d %H:%M'))

    def context(browser, *args, **kwargs):
        value = original_context(browser, *args, **kwargs)
        counts['contexts'] += 1
        browsers.append(browser)
        value.route('**/*', lambda route: route.fulfill(status=200, content_type='text/html', body=html))
        return value

    def shot(page, *args, **kwargs):
        value = original_shot(page, *args, **kwargs)
        counts['screenshots'] += 1
        (evidence_dir/'capture.png').write_bytes(value)
        # The application and the queue use the same isolated test clock.
        if kwargs.get('path'):
            os.utime(kwargs['path'], (at.timestamp(), at.timestamp()))
        return value

    def enabled(page):
        return page.evaluate('()=>globalThis.__cg29RenderLab?.snapshot().some(x=>x.enabled)||false')

    @contextmanager
    def early(page, model):
        with original_preparation(page, model):
            counts['early_active'] += int(enabled(page))
            yield
        assert not enabled(page)
        counts['restored'] += 1

    @contextmanager
    def late(page, model, geometry):
        with original_observation(page, model, geometry) as clean:
            counts['late_active'] += int(enabled(page))
            yield clean
        assert not enabled(page)

    def response(url, **kwargs):
        assert url == scanner.OPENAI_RESPONSES_URL
        assert all(not browser.is_connected() for browser in browsers)
        counts['ai_requests'] += 1
        contents = kwargs['json']['input'][0]['content']
        safe = json.loads(json.dumps(kwargs['json']))
        for content in safe['input'][0]['content']:
            if content.get('type') == 'input_image':
                encoded=content['image_url'].split(',',1)[1]
                raw=base64.b64decode(encoded)
                digest=hashlib.sha256(raw).hexdigest();input_digests.append(digest)
                (evidence_dir/f'ai-input-{len(input_digests)}.png').write_bytes(raw)
                content['image_url']='sha256:'+digest
        (evidence_dir/'ai-request-sanitized.json').write_text(json.dumps(safe,indent=2))
        supplied = next(c['text'] for c in contents if c.get('text', '').startswith('Code-computed bands'))
        data = json.loads(supplied.split('\n', 1)[1].split('\nCoordinate mapping', 1)[0])
        scan = dict(timeframe=tf.lower(), observed_timeframe=tf.lower(), observed_symbol='BTC',
                    observed_mode='Symbol', observed_model=f'Model {model}', readable=True,
                    blocking_condition='none', short_summary='Isolated real-worker fixture',
                    price_axis_range=dict(low=96.1, high=100, price_evidence='96.1 100'),
                    prominent_levels=[dict(candidate_id=c['id'], right_edge_visible=True, strength_score=.8)
                                      for c in data['candidates']])
        if state.get('failedMap') == f'{model}:{tf}':
            scan['observed_timeframe']='12h' if tf=='24H' else '24h'
        body = dict(status='completed', output_text=json.dumps(dict(symbol='BTC', analysis_mode='visual_screenshot', scans=[scan])),
                    usage=dict(input_tokens=0, output_tokens=0, total_tokens=0))
        (evidence_dir/'ai-response.json').write_text(json.dumps(body,indent=2))
        return SimpleNamespace(ok=True, status_code=200, json=lambda: body)

    def save_audit():
        value = dict(run_id=jid, model=model, timeframe=tf, pid=os.getpid(), **counts,
                     browsers_closed=all(not b.is_connected() for b in browsers),
                     wall_seconds=round(time.monotonic()-started, 3),
                     injected_failure=state.get('failedMap') == f'{model}:{tf}',
                     external_source=False, external_ai=False, ai_input_sha256=input_digests, cpu_user_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_utime, cpu_system_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_stime)
        (audit / (jid + '.json')).write_text(json.dumps(value, indent=2))
    atexit.register(save_audit)
    Browser.new_context, Page.screenshot = context, shot
    rendering.preparation, rendering.observation = early, late
    scanner.requests.post = response

