"""Isolated Model 3 render-policy experiment; never an application entry point.

Use the prepared, pinned collector runtime. A and B read the same live page;
all images/raw observations stay in a mode-0700 temporary directory. No API
analysis, database connection, job creation or application write is performed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time


READER_SHA = '693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820'
CAPTURE_SHA = '6b05e7dd62b4a2ef228c2cf31eb75d9598aa36ef0a15c4665871cda085e40b35'
ROW_KEYS = ('id', 'price', 'value', 'unit', 'source_time', 'raw_price', 'raw_value', 'rounding_error')


class CheckFailure(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise CheckFailure(code)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def rows_key(snapshot):
    return [[row[key] for key in ROW_KEYS] for row in snapshot['rows']]


def cpu_usage():
    try:
        values = dict(line.split() for line in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
        return int(values['usage_usec']) / 1e6
    except (OSError, ValueError, KeyError):
        return None


def safe_error(exc):
    value = getattr(exc, 'reason', None) or (str(exc) if isinstance(exc, (CheckFailure, ValueError)) else None)
    return value if isinstance(value, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,100}', value) else type(exc).__name__


def image_compare(first, second, bounds):
    """Exact RGB comparison of the map only; clocks/ads elsewhere are irrelevant."""
    from PIL import Image, ImageChops
    box = tuple(int(bounds[key]) for key in ('left', 'top', 'right', 'bottom'))
    box = (box[0], box[1], box[2] + 1, box[3] + 1)
    with Image.open(first) as a, Image.open(second) as b:
        require(a.size == b.size, 'image_dimensions_changed')
        left, right = a.convert('RGB').crop(box), b.convert('RGB').crop(box)
        delta = ImageChops.difference(left, right)
        red, green, blue = delta.split()
        maximum = ImageChops.lighter(ImageChops.lighter(red, green), blue)
        changed = left.width * left.height - maximum.histogram()[0]
        return {'equal': changed == 0, 'changed_pixels': changed,
                'pixels': left.width * left.height, 'max_channel_difference': maximum.getextrema()[1],
                'difference_bbox_relative_to_plot': maximum.getbbox(),
                'tiles_8_by_4_changed_pixels': [
                    (tile.width * tile.height - tile.histogram()[0])
                    for iy in range(4) for ix in range(8)
                    for tile in [maximum.crop((ix*left.width//8, iy*left.height//4,
                                               (ix+1)*left.width//8, (iy+1)*left.height//4))]],
                'first_rgb_sha256': hashlib.sha256(left.tobytes()).hexdigest(),
                'second_rgb_sha256': hashlib.sha256(right.tobytes()).hexdigest()}



def private_plot_preview(label, image, bounds):
    """Bounded plot-only preview in private build logs; never public artifacts."""
    import base64
    import io
    from PIL import Image
    with Image.open(image) as source:
        box = tuple(int(bounds[key]) for key in ('left', 'top', 'right', 'bottom'))
        crop = source.convert('RGB').crop((box[0], box[1], box[2]+1, box[3]+1))
        crop.thumbnail((384, 216))
        encoded = io.BytesIO()
        crop.save(encoded, format='WEBP', quality=48, method=4)
        data = encoded.getvalue()
        if len(data) > 12000:
            crop.thumbnail((256, 144))
            encoded = io.BytesIO()
            crop.save(encoded, format='WEBP', quality=35, method=4)
            data = encoded.getvalue()
        if len(data) > 12000:
            return
        value = base64.b64encode(data).decode('ascii')
        chunks = [value[i:i+4000] for i in range(0, len(value), 4000)]
        for index, chunk in enumerate(chunks):
            print('CG29_PRIVATE_PLOT ' + json.dumps({'label':label, 'part':index,
                'parts':len(chunks), 'sha256':hashlib.sha256(data).hexdigest(),
                'mime_type':'image/webp', 'base64':chunk}, separators=(',', ':')), flush=True)


def run(timeframes, runtime, fps, requested_output=None):
    os.umask(0o077)
    require(not any(os.getenv(key, '').strip() for key in
                    ('DATABASE_URL', 'OPENAI_API_KEY', 'SUPABASE_SERVICE_ROLE_KEY')),
            'lab_service_credentials_must_be_absent')
    os.environ['COLLECTION_HEATMAP_MODEL'] = '3'
    sys.path.insert(0, str(runtime))
    import heatmap_hover as reader
    import capture_readiness as readiness
    from market_vision.coinglass_heatmap_capture import capture_heatmaps
    from playwright.sync_api import Browser, BrowserContext
    import candidate

    reader_path = Path(reader.__file__)
    capture_path = runtime / 'market_vision/coinglass_heatmap_capture.py'
    require(hashlib.sha256(reader_path.read_bytes()).hexdigest() == READER_SHA, 'reader_hash_mismatch')
    require(hashlib.sha256(capture_path.read_bytes()).hexdigest() == CAPTURE_SHA, 'capture_hash_mismatch')
    if requested_output is None:
        output = Path(tempfile.mkdtemp(prefix='cg29-m3-render-ab-'))
    else:
        output = requested_output.resolve()
        require(output.parent == Path('/tmp'), 'lab_output_must_be_private_tmp_child')
        output.mkdir(mode=0o700, exist_ok=False)
    report = {'schema': 'model3-render-ab.v1', 'status': 'running', 'timeframes': list(timeframes),
              'reader_sha256': READER_SHA, 'capture_sha256': CAPTURE_SHA, 'experimental_fps': fps,
              'scope': 'isolated original-reader same-page paired experiment; no application acceptance test',
              'database_writes': 0, 'openai_calls': 0, 'pairs': [], 'cleanup': [],
              'artifact_directory': str(output),
              'cpu_affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
              'cpu_max': Path('/sys/fs/cgroup/cpu.max').read_text().strip()
                         if Path('/sys/fs/cgroup/cpu.max').exists() else None,
              'memory_max': Path('/sys/fs/cgroup/memory.max').read_text().strip()
                            if Path('/sys/fs/cgroup/memory.max').exists() else None}
    installs = {}
    original_new_context, original_close = Browser.new_context, BrowserContext.close
    original_reader = reader.capture_column

    def persist():
        (output / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False))

    def emit(event, payload):
        print('CG29_RENDER_AB ' + json.dumps({'event': event, **payload}, separators=(',', ':'), allow_nan=False), flush=True)

    def new_context(browser, *args, **kwargs):
        context = original_new_context(browser, *args, **kwargs)
        installs[id(context)] = candidate.install_context(context)
        return context

    def close_context(context, *args, **kwargs):
        try:
            for page in context.pages:
                if not page.is_closed():
                    try:
                        report['cleanup'].append({'restoration': candidate.restore_all(page)})
                    except Exception as exc:
                        report['cleanup'].append({'error': safe_error(exc)})
        finally:
            original_close(context, *args, **kwargs)

    def save_evidence(name, snapshot, image):
        reader.validate_rows(snapshot['rows'])
        require(snapshot.get('complete') is True, 'incomplete_column')
        pools = reader.candidate_pools(snapshot, image)
        candle = reader.source_price_range(snapshot, image)
        evidence = {'snapshot': snapshot, 'candidates': pools, 'source_price_range': candle,
                    'image_sha256': hashlib.sha256(Path(image).read_bytes()).hexdigest()}
        (output / (name + '.json')).write_text(json.dumps(evidence, indent=2, allow_nan=False))
        return evidence

    def measure(page, image, geometry, kwargs):
        before = cpu_usage()
        snapshot = original_reader(page, image, geometry, **kwargs)
        after = cpu_usage()
        return snapshot, {'hover_seconds': snapshot['duration_seconds'], 'rows': len(snapshot['rows']),
                          'tooltip_reads': snapshot['tooltip_reads'], 'row_digest': digest(rows_key(snapshot)),
                          'source_time': snapshot['source_time'],
                          'cpu_seconds': None if before is None or after is None else round(after - before, 6)}

    def own_image(page, timeframe, image, expected_geometry, axes, *, force):
        page.mouse.move(0, 0)
        if force:
            candidate.force_paint(page)
        state = readiness.wait_for_render(page, timeframe, 3)
        require(state.get('ready') is True, 'render_not_ready')
        require(state.get('price_axis_ticks') == axes, 'source_axis_changed')
        geometry = readiness.geometry_from_state(state)
        require(geometry == expected_geometry, 'chart_geometry_changed')
        reader.require_cookie_banner_hidden(page)
        page.screenshot(path=str(image), full_page=False)
        diagnostics = readiness.CaptureNetworkDiagnostics(page)
        readiness.verify_saved_render(page, timeframe, 3, state, image, diagnostics)
        return geometry

    def anchors(page, snapshot):
        bounds = snapshot['plot_bounds']
        scroll = page.evaluate('() => window.scrollY')
        for index in (0, len(snapshot['rows']) // 2, len(snapshot['rows']) - 1):
            expected = snapshot['rows'][index]
            y = bounds['top'] + expected['y_fraction'] * (bounds['bottom'] - bounds['top']) - scroll
            page.mouse.move(bounds['right'] - .25, y)
            text = page.evaluate(r'''({expected})=>new Promise(resolve=>{
                const read=(''' + reader.READ_EXPECTED_TOOLTIP + r'''); const end=performance.now()+700;
                function check(){const value=read(expected);if(value||performance.now()>=end){resolve(value);return}
                    setTimeout(check,25)}check();})''', {'expected': expected['price']})
            require(bool(text), 'restored_anchor_timeout')
            actual = reader.parse_tooltip(text)
            require(all(actual[key] == expected[key] for key in ('price', 'value', 'unit', 'source_time')),
                    'restored_snapshot_changed')
        reader.verify_source_axis(page, snapshot['price_axis_ticks'], snapshot['rows'])
        page.mouse.move(0, 0)

    def paired(page, image, geometry, **kwargs):
        timeframe = kwargs['timeframe']
        pair = {'timeframe': timeframe, 'status': 'running'}
        report['pairs'].append(pair)
        entry = candidate.require_ready(page, installs[id(page.context)])
        require(entry['enabled'] is False, 'baseline_render_gate_enabled')
        require(entry['canvas']['rect'] == {key: geometry[key] for key in ('x', 'y', 'width', 'height')},
                'candidate_canvas_is_not_selected_map')
        pair['chart'] = {'key': entry['key'], 'canvas': entry['canvas'], 'ticker_count': entry['ticker_count']}
        if 'graphics_backend' not in report:
            session = page.context.browser.new_browser_cdp_session()
            try:
                gpu = session.send('SystemInfo.getInfo').get('gpu', {})
                report['graphics_backend'] = {
                    'devices': [{key: device.get(key) for key in ('vendorString', 'deviceString', 'driverVendor', 'driverVersion')}
                                for device in gpu.get('devices', [])],
                    'renderer': gpu.get('auxAttributes', {}).get('glRenderer'),
                    'display_type': gpu.get('auxAttributes', {}).get('displayType')}
            finally:
                session.detach()
        emit('pair_started', {'timeframe': timeframe})
        a, pair['A'] = measure(page, image, geometry, kwargs)
        first = save_evidence(timeframe + '-A', a, image)
        pair['A']['status'] = 'passed_original_validators'
        persist()
        emit('A_completed', {'timeframe': timeframe, **pair['A']})
        # Diagnostic observation only: unchanged renderer, after the original
        # sweep. It does not replace the original PNG or the acceptance gate.
        control_image = output / (timeframe + '-A-control.png')
        own_image(page, timeframe, control_image, geometry, a['price_axis_ticks'], force=True)
        pair['baseline_control_pixels'] = image_compare(image, control_image, a['plot_bounds'])
        emit('baseline_control', {'timeframe':timeframe, 'pixels':pair['baseline_control_pixels']})
        second_image = output / (timeframe + '-B.png')
        try:
            enabled = candidate.enable(page, fps=fps)
            require(enabled['enabled'] is True and enabled['ticker_count'] == entry['ticker_count'],
                    'candidate_listener_contract_changed')
            own_image(page, timeframe, second_image, geometry, a['price_axis_ticks'], force=True)
            b, pair['B'] = measure(page, second_image, geometry, kwargs)
            second = save_evidence(timeframe + '-B', b, second_image)
            checks = {'row_values_exact': rows_key(a) == rows_key(b),
                      'identity_exact': all(a.get(key) == b.get(key) for key in
                                            ('heatmap_model', 'timeframe', 'metric', 'unit', 'source_time', 'scope')),
                      'axis_exact': a['price_axis_ticks'] == b['price_axis_ticks'],
                      'plot_bounds_exact': a['plot_bounds'] == b['plot_bounds'],
                      'total_exact': a['total_value'] == b['total_value'],
                      'pool_membership_and_values_exact': first['candidates'] == second['candidates'],
                      'candle_range_exact': first['source_price_range'] == second['source_price_range']}
            pair['checks'] = checks
            pair['pixels'] = image_compare(image, second_image, a['plot_bounds'])
            pair['gate_after_B'] = candidate.snapshot(page)
            pair['candidate_vs_control_pixels'] = image_compare(control_image, second_image, a['plot_bounds'])
            emit('pixel_comparison', {'timeframe':timeframe, 'original_vs_candidate':pair['pixels'],
                 'unchanged_control_vs_candidate':pair['candidate_vs_control_pixels']})
            if not pair['pixels']['equal']:
                for label, path in (('original', image), ('unchanged-control', control_image), ('candidate', second_image)):
                    private_plot_preview(timeframe+'-'+label, path, a['plot_bounds'])
            require(all(checks.values()), 'paired_snapshot_or_output_mismatch')
            require(pair['pixels']['equal'], 'paired_map_pixels_changed')
        except Exception as exc:
            pair.update(status='failed', error=safe_error(exc))
            persist()
            raise
        finally:
            restored = candidate.disable(page)
            pair['restored'] = restored['enabled'] is False and restored['ticker_count'] == entry['ticker_count']
            # Keep the original failure, but finish this observation before
            # closing the lab browser. Do not accept changed pixels as success.
            if pair.get('error') == 'paired_map_pixels_changed' and pair['restored']:
                try:
                    recovery_image = output / (timeframe + '-failed-pair-restored.png')
                    own_image(page, timeframe, recovery_image, geometry, a['price_axis_ticks'], force=True)
                    pair['failed_pair_restored_pixels'] = image_compare(second_image, recovery_image, a['plot_bounds'])
                    pair['control_vs_restored_pixels'] = image_compare(control_image, recovery_image, a['plot_bounds'])
                    emit('failed_pair_restored', {'timeframe':timeframe,
                         'candidate_vs_restored':pair['failed_pair_restored_pixels'],
                         'unchanged_control_vs_restored':pair['control_vs_restored_pixels']})
                    private_plot_preview(timeframe+'-restored', recovery_image, a['plot_bounds'])
                except Exception as diagnostic_error:
                    pair['restoration_observation_error'] = safe_error(diagnostic_error)
            persist()
        require(pair['restored'], 'render_listener_not_restored')
        anchors(page, a)
        restored_image = output / (timeframe + '-restored.png')
        own_image(page, timeframe, restored_image, geometry, a['price_axis_ticks'], force=True)
        pair['restored_pixels'] = image_compare(second_image, restored_image, a['plot_bounds'])
        require(pair['restored_pixels']['equal'], 'restored_map_pixels_changed')
        pair.update(status='passed', restored_anchors=3,
                    measured_hover_ratio=a['duration_seconds'] / b['duration_seconds'])
        persist()
        emit('pair_completed', {'timeframe': timeframe, 'status': pair['status'], 'A': pair['A'], 'B': pair['B'],
                               'checks': pair['checks'], 'pixels_equal': pair['pixels']['equal'],
                               'restored': pair['restored'], 'restored_pixels_equal': pair['restored_pixels']['equal']})
        return a

    Browser.new_context, BrowserContext.close, reader.capture_column = new_context, close_context, paired
    begun = time.monotonic()
    old_alarm = signal.getsignal(signal.SIGALRM)
    def deadline(_signum, _frame):
        raise CheckFailure('lab_diagnostic_deadline')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(min(840, 600 * len(timeframes)))
    emit('started', {key: report[key] for key in ('timeframes', 'reader_sha256', 'experimental_fps', 'cpu_affinity', 'cpu_max')})
    try:
        capture_heatmaps(output, timeframes=timeframes)
        require(all(pair['status'] == 'passed' for pair in report['pairs']), 'pair_not_passed')
        require(bool(report['cleanup']) and not any(item.get('error') for item in report['cleanup']), 'final_restore_failed')
        report['status'] = 'passed'
    except Exception as exc:
        report['status'] = 'partial' if any(pair['status'] == 'passed' for pair in report['pairs']) else 'failed'
        report['error'] = safe_error(exc)
        for pair in report['pairs']:
            if pair['status'] == 'running':
                pair.update(status='failed', error=report['error'])
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_alarm)
        Browser.new_context, BrowserContext.close, reader.capture_column = original_new_context, original_close, original_reader
        report['wall_seconds'] = round(time.monotonic() - begun, 3)
        report['reader_unchanged'] = hashlib.sha256(reader_path.read_bytes()).hexdigest() == READER_SHA
        report['capture_unchanged'] = hashlib.sha256(capture_path.read_bytes()).hexdigest() == CAPTURE_SHA
        persist()
        emit('finished', {key: report.get(key) for key in
                          ('status', 'error', 'wall_seconds', 'reader_unchanged', 'capture_unchanged', 'artifact_directory')})
    return 0 if report['status'] == 'passed' else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--timeframes', default='12h')
    parser.add_argument('--runtime', type=Path, default=Path.cwd() / 'standalone_collector/runtime')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--fps', type=float, default=.5)
    args = parser.parse_args()
    timeframes = tuple(args.timeframes.split(','))
    require(1 <= len(timeframes) <= 2 and len(set(timeframes)) == len(timeframes)
            and all(value in ('12h', '24h') for value in timeframes), 'invalid_lab_timeframes')
    require(.1 <= args.fps <= 5, 'invalid_lab_fps')
    return run(timeframes, args.runtime.resolve(), args.fps, args.output)


if __name__ == '__main__':
    sys.exit(main())
