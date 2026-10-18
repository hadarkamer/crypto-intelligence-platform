"""One authorized, build-only GPT check per model; no service or database."""
import argparse
import base64
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
CASES = ((1, '12H'), (2, '24H'), (3, '48H'))
ENDPOINT = 'https://api.openai.com/v1/responses'


def emit(kind, value):
    print(json.dumps({'diagnostic': kind, **value}, separators=(',', ':')), flush=True)


class OneRequest:
    """Guard actual transport, not just the number of task invocations."""
    def __init__(self, post):
        self.post = post
        self.calls = 0
        self.seconds = 0
        self.http_status = None

    def __call__(self, url, *args, **kwargs):
        if self.calls or url != ENDPOINT or kwargs.get('allow_redirects') is not False:
            raise RuntimeError('Isolated API destination or one-request budget rejected')
        self.calls += 1
        started = time.monotonic()
        try:
            response = self.post(url, *args, **kwargs)
            self.http_status = response.status_code
            return response
        finally:
            self.seconds = round(time.monotonic() - started, 3)


def emit_artifact(model, timeframe, name, data):
    # Private build logs are the bounded evidence channel. Never publish these
    # artifacts in the static site's public output or include environment data.
    if name not in ('input-hover.json', 'raw-analysis.json', 'result.json', 'map.jpg'):
        raise ValueError('Unknown evidence type')
    if len(data) > 262144:
        raise ValueError('Evidence too large')
    encoded = base64.b64encode(data).decode('ascii')
    chunks = [encoded[i:i+6000] for i in range(0, len(encoded), 6000)]
    for index, chunk in enumerate(chunks):
        emit('ai_artifact', {'model': model, 'timeframe': timeframe, 'name': name,
             'index': index, 'count': len(chunks), 'data': chunk})


def single(model, timeframe, output):
    sys.path.insert(0, str(HERE / 'runtime'))
    import collection_model1_task as task
    from market_vision import openai_heatmap_scanner as scanner
    from model1_execution import run_task
    output.mkdir(parents=True, exist_ok=False)
    original_analyze = scanner.analyze_heatmap_images
    original_post = scanner.requests.post
    transport = OneRequest(original_post)
    captured = {}

    def analyze(images, **kwargs):
        # The existing task closes its browser before this analysis call.
        captured.update(rows=len(images[0]['hover_snapshot']['rows']),
                        hover_seconds=images[0]['hover_snapshot']['duration_seconds'],
                        candidate_pools=len(images[0]['hover_snapshot']['candidates']))
        (output / 'input-hover.json').write_text(json.dumps(images[0]['hover_snapshot']))
        shutil.copyfile(images[0]['image'], output / 'input-map.png')
        raw = original_analyze(images, **kwargs)
        (output / 'raw-analysis.json').write_text(json.dumps(raw, ensure_ascii=False))
        return raw

    scanner.analyze_heatmap_images = analyze
    scanner.requests.post = transport
    started = time.monotonic()
    try:
        code = run_task(task.main, timeframe, str(uuid.uuid4()), str(output))
    finally:
        scanner.analyze_heatmap_images = original_analyze
        scanner.requests.post = original_post
    report = {'model': model, 'timeframe': timeframe,
              'status': 'validated' if code == 0 else 'failed',
              'ai_calls': transport.calls, 'database_writes': 0,
              'api_seconds': transport.seconds, 'http_status': transport.http_status,
              'elapsed_seconds': round(time.monotonic() - started, 3), **captured}
    if code:
        failure = output / 'error.json'
        if failure.is_file():
            facts = json.loads(failure.read_text())
            report.update({key: facts[key] for key in ('code', 'stage', 'validation_reason',
                'blocking_condition', 'source_capture_state', 'capture_phase', 'usage') if key in facts})
    else:
        result = json.loads((output / 'result.json').read_text())
        report.update(actual_model=result['model'], zones=len(result['zones']), usage=result.get('usage'),
                      unit=result['hover_snapshot']['unit'], source_time=result['hover_snapshot']['source_time'])
    # Evidence export must not erase the task's actual outcome.
    report['evidence_errors'] = []
    for name in ('input-hover.json', 'raw-analysis.json', 'result.json', 'map.jpg'):
        try:
            path = output / name
            if name == 'map.jpg' and (output / 'input-map.png').is_file():
                from PIL import Image
                with Image.open(output / 'input-map.png') as original:
                    picture = original.convert('RGB')
                    picture.thumbnail((1400, 1400))
                    buffer = io.BytesIO()
                    picture.save(buffer, format='JPEG', quality=80)
                    picture.close()
                emit_artifact(model, timeframe, name, buffer.getvalue())
            elif path.is_file():
                emit_artifact(model, timeframe, name, path.read_bytes())
        except (OSError, ValueError):
            report['evidence_errors'].append(name)
    (output / 'report.json').write_text(json.dumps(report))
    emit('ai_case_result', report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--single', nargs=2)
    args = parser.parse_args()
    if args.single:
        model, timeframe = args.single
        if (int(model), timeframe) not in CASES: parser.error('Unapproved case')
        single(int(model), timeframe, args.output)
        return 0
    args.output.mkdir(parents=True, exist_ok=False)
    from hover_source_check import run_probe
    from model3_hover_diagnostic import ResourceSample
    reports = []
    for model, timeframe in CASES:
        output = args.output / f'model{model}-{timeframe}'
        env = {**os.environ, 'COLLECTION_HEATMAP_MODEL': str(model), 'DATABASE_URL': '',
               'COINGLASS_COLLECTOR_TOKEN': '', 'COLLECTION_BRIDGE_ENABLED': '0'}
        with ResourceSample() as resources:
            try:
                run_probe([sys.executable, str(Path(__file__).resolve()), '--single',
                           str(model), timeframe, '--output', str(output)], env, timeout=300)
                report = json.loads((output / 'report.json').read_text())
            except (subprocess.SubprocessError, OSError, ValueError):
                report = {'model': model, 'timeframe': timeframe, 'status': 'failed',
                          'code': 'probe_process_failed', 'ai_calls': 'unknown_at_most_one'}
        report['resources'] = resources.report()
        reports.append(report)
        (args.output / 'summary.json').write_text(json.dumps(reports))
        emit('ai_case_summary', report)
        if (report.get('code') in ('analysis_unauthorized', 'analysis_rate_limited', 'analysis_quota_exceeded')
            or report.get('blocking_condition') == 'challenge'
            or (report.get('source_capture_state') or {}).get('challenge_visible')):
            break
    return int(len(reports) != len(CASES) or any(r['status'] != 'validated' for r in reports))


if __name__ == '__main__':
    sys.exit(main())
