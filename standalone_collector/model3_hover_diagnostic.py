"""Test branch only: observe Model 3 hover failures without changing its reader.

No source retries. At a terminal reader failure, inspect a few adjacent points
in the same loaded map. Never dismiss dialogs, change settings or pass a gate.
Only bounded numeric/structural evidence is written to private Render logs.
"""
import argparse
import inspect
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent


def emit(kind, value):
    print(json.dumps({'diagnostic': kind, **value}, separators=(',', ':')), flush=True)


def install_observer():
    sys.path.insert(0, str(HERE / 'runtime'))
    import heatmap_hover as hover
    from playwright.sync_api import TimeoutError as BrowserTimeout
    from capture_failure_observer import STATE_SCRIPT
    original = hover.capture_column

    def tooltip(page):
        text = page.evaluate(hover.READ_TOOLTIP)
        if text is None:
            return {'visible': False}
        try:
            row = hover.parse_tooltip(text)
            return {'visible': True, 'price': row['price'], 'value': row['value'],
                    'unit': row['unit'], 'time': row['source_time']}
        except hover.HoverError:
            return {'visible': True, 'parseable': False}

    class ObservedPage:
        def __init__(self, page):
            self.page = page
            self.events = []

        def __getattr__(self, name):
            return getattr(self.page, name)

        def wait_for_function(self, expression, **kwargs):
            try:
                return self.page.wait_for_function(expression, **kwargs)
            except BrowserTimeout:
                # Read only our Python reader's numeric locals, never browser
                # internals, credentials or arbitrary page content.
                frame = inspect.currentframe().f_back
                local = frame.f_locals
                parent = frame.f_back
                for _ in range(4):
                    if parent is None or parent.f_code.co_name == 'capture_column':
                        break
                    parent = parent.f_back
                outer = parent.f_locals if parent is not None else {}
                event = {key: local.get(key) for key in
                         ('x', 'y', 'expected', 'scroll_y', 'read_count')}
                event.update(step='clear' if '=== null' in expression else
                    'expected_price' if kwargs.get('arg') is not None else 'show',
                    timeout_ms=kwargs.get('timeout'), bounds=outer.get('bounds'),
                    rows_collected=len(outer.get('rows', [])))
                del frame, parent, local, outer
                try:
                    event['tooltip'] = tooltip(self.page)
                except Exception:
                    event['tooltip'] = {'observation_failed': True}
                self.events.append(event)
                raise

    def observed_capture(page, image, geometry, **kwargs):
        probe = ObservedPage(page)
        try:
            return original(probe, image, geometry, **kwargs)
        except hover.HoverError as exc:
            if exc.reason != 'hover_update_timeout' or not probe.events:
                raise
            event = probe.events[-1]
            emit('reader_timeout', {'timeframe': kwargs['timeframe'],
                'events': probe.events[-3:], 'viewport': page.viewport_size})
            state = page.evaluate(STATE_SCRIPT, 3)
            emit('page_state', {'timeframe': kwargs['timeframe'], 'state': state})
            x, y = event['x'], event['y'] - event['scroll_y']
            structure = page.evaluate(r'''point => {
                const visible=e=>{const r=e.getBoundingClientRect(),s=getComputedStyle(e);
                    return r.width>0&&r.height>0&&s.display!=='none'&&s.visibility!=='hidden';};
                const hit=document.elementFromPoint(point.x,point.y);
                const dialogs=[...document.querySelectorAll('[role="dialog"],.MuiModal-root')].filter(visible);
                return {hit:hit?{tag:hit.tagName,role:hit.getAttribute('role'),
                    class:String(hit.className||'').slice(0,100)}:null,
                    dialogs:dialogs.slice(0,3).map(e=>{const t=e.innerText||'';return {
                        cookie:/cookies?|consent/i.test(t),login:/log\s*in|sign\s*in/i.test(t),
                        subscription:/subscribe|subscription|upgrade|trial/i.test(t),
                        security:/verify.*human|security verification/i.test(t)};})};
            }''', {'x': x, 'y': y})
            emit('pointer_state', {'timeframe': kwargs['timeframe'], **structure})
            if any(state.get(k) for k in ('login_gate_visible', 'challenge_visible', 'dialog_visible')):
                raise
            # Diagnostic observations only. Preserve the original failure;
            # neither these samples nor any timeout can become acceptance data.
            for label, px, py in [('same', x, y), ('left3', x-3, y),
                                  ('left10', x-10, y), ('down2', x, y+2),
                                  ('up2', x, y-2)]:
                page.mouse.move(0, 0)
                cleared = True
                try:
                    h = page.wait_for_function('() => ('+hover.READ_TOOLTIP+')() === null', timeout=450)
                    h.dispose()
                except BrowserTimeout:
                    cleared = False
                page.mouse.move(px, py)
                appeared = True
                try:
                    h = page.wait_for_function(hover.READ_TOOLTIP, timeout=700, polling='raf')
                    h.dispose()
                except BrowserTimeout:
                    appeared = False
                emit('point_probe', {'timeframe': kwargs['timeframe'], 'point': label,
                    'x': px, 'y': py, 'cleared': cleared, 'appeared': appeared,
                    'tooltip': tooltip(page)})
            raise

    hover.capture_column = observed_capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--single', choices=('12H', '24H', '48H'))
    args = parser.parse_args()
    from hover_source_check import single, run_probe
    if args.single:
        if os.environ.get('COLLECTION_HEATMAP_MODEL') != '3':
            raise RuntimeError('Diagnostic must use Model 3')
        install_observer()
        single(3, args.single, args.output)
        return 0
    args.output.mkdir(parents=True, exist_ok=False)
    reports = []
    for tf in ('12H', '24H', '48H'):
        dest = args.output / tf
        env = {**os.environ, 'COLLECTION_HEATMAP_MODEL': '3',
               'OPENAI_API_KEY': '', 'DATABASE_URL': '', 'COLLECTION_BRIDGE_ENABLED': '0'}
        run_probe([sys.executable, str(Path(__file__).resolve()), '--single', tf,
                   '--output', str(dest)], env)
        report = json.loads((dest / 'report.json').read_text())
        reports.append(report)
        emit('case_result', report)
        (args.output / 'summary.json').write_text(json.dumps(reports, indent=2))
        if (report.get('source_capture_state') or {}).get('challenge_visible'):
            break
    return int(any(r['status'] != 'captured' for r in reports))


if __name__ == '__main__':
    sys.exit(main())
