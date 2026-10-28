"""Bounded failure facts from existing observations, never another source read.

This sidecar is separate from the existing 4 KiB error report.  It contains
only explicitly selected numeric fields, fixed enums and provenance hashes;
never exception messages, page text, credentials or a dump of frame locals.
All collection and retention are best effort and cannot change scan results.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from uuid import UUID

FORMAT_VERSION = 'capture-failure.v1'
SIDECAR_NAME = 'capture_failure.json'
MAX_BYTES = 16 * 1024
MAX_IMAGE = 4 * 1024 * 1024
_HERE = Path(__file__).resolve().parent
_HEX64 = re.compile(r'[a-f0-9]{64}')
_HEX40 = re.compile(r'[a-f0-9]{40}')
_STAMP = re.compile(r'20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z')
_SUBREASONS = frozenset({
    'calibration_reached_bottom', 'calibration_window_exhausted',
    'observed_spacing_below_2', 'last_boundary_missing',
    'computed_height_below_2', 'grid_deviation_above_1_1', 'unresolved',
})
_FUNCTIONS = {
    'heatmap_hover': frozenset({'capture_column', 'candle_points', 'read_candle',
        'read', 'edge', 'wait_for_tooltip', 'plot_bounds', 'validate_rows',
        'parse_tooltip', 'parse_candle', 'validate_candle'}),
    'capture_readiness': frozenset({'verify_saved_render', 'wait_for_render',
        'current_state', 'require_unblocked_source'}),
    'coinglass_heatmap_capture': frozenset({'capture_heatmaps'}),
}


def _number(value, low=-1e15, high=1e15):
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise ValueError('Invalid diagnostic number')
    return value


def _integer(value, low=0, high=100000):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid diagnostic integer')
    return value


def _stamp(value):
    # ISO UTC only, bounded years; no arbitrary source text is retained.
    if not isinstance(value, str) or not _STAMP.fullmatch(value):
        raise ValueError('Invalid diagnostic timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not 2000 <= parsed.year < 2100:
        raise ValueError('Invalid diagnostic timestamp')
    return value


def _uuid(value):
    if not isinstance(value, str) or len(value) != 36 or str(UUID(value)) != value:
        raise ValueError('Invalid diagnostic job')
    return value


def _object(value):
    if type(value) is not dict:
        raise ValueError('Invalid diagnostic object')
    return value


def _numbers(value, fields, *, low=-1e6, high=1e6):
    value = _object(value)
    return {key: _number(value[key], low, high) for key in fields if key in value}


def _array(value, limit):
    if not isinstance(value, (list, tuple)) or len(value) > limit:
        raise ValueError('Invalid diagnostic array')
    return value


def _row(value):
    value = _object(value)
    out = {}
    for key in ('price', 'value', 'rounding_error'):
        if key in value:
            out[key] = _number(value[key], 0, 1e12 if key == 'price' else 1e15)
    if 'source_time' in value:
        out['source_time'] = _stamp(value['source_time'])
    if 'unit' in value:
        if value['unit'] not in ('USD', 'source_metric'):
            raise ValueError('Invalid diagnostic unit')
        out['unit'] = value['unit']
    return out


def _resolution(value):
    value = _object(value)
    subreason = value.get('subreason')
    if subreason not in _SUBREASONS:
        raise ValueError('Invalid resolution subreason')
    out = {'subreason': subreason}
    for key in ('top_y', 'bottom_y', 'y', 'next_y', 'sample_y', 'previous_y',
                'target', 'height', 'minimum_transition_spacing', 'max_deviation'):
        if key in value:
            out[key] = _number(value[key], -1e6, 1e6)
    for key in ('initial_step', 'price_step', 'gap', 'tolerance'):
        if key in value:
            out[key] = _number(value[key], -1e12, 1e12)
    for key in ('count', 'offset', 'failing_transition_index'):
        if key in value:
            out[key] = _integer(value[key], 0, 100000)
    if 'last_boundary' in value:
        out['last_boundary'] = None if value['last_boundary'] is None else _number(value['last_boundary'], -1e6, 1e6)
    for key in ('transitions', 'expected_transitions', 'transition_residuals'):
        if key in value:
            out[key] = [_number(item, -1e6, 1e6) for item in _array(value[key], 4)]
    if 'calibration' in value:
        out['calibration'] = [_row(item) for item in _array(value['calibration'], 4)]
    for key in ('top_row', 'bottom_row', 'last_inner', 'last_row'):
        if key in value:
            out[key] = _row(value[key])
    # Constants identify the rule that ran, not a proposed replacement rule.
    out['limits'] = {'calibration_pixels': 120, 'bottom_probes': 121,
        'minimum_height_pixels': 2, 'maximum_deviation_pixels': 1.1}
    return out


def _resolution_from_locals(values):
    """Classify the existing rejection after it occurred; do not rerun capture."""
    out = {}
    keys = ('top_y', 'bottom_y', 'y', 'next_y', 'sample_y', 'previous_y',
        'target', 'height', 'initial_step', 'price_step', 'gap', 'tolerance',
        'count', 'offset', 'last_boundary', 'transitions', 'calibration',
        'top_row', 'bottom_row', 'last_inner')
    for key in keys:
        if key in values:
            out[key] = values[key]
    if type(values.get('row')) is dict:
        out['last_row'] = values['row']
    transitions = values.get('transitions', [])
    if len(transitions) >= 2:
        out['minimum_transition_spacing'] = min(b-a for a, b in zip(transitions, transitions[1:]))
    subreason = 'unresolved'
    if 'height' in values and transitions:
        height = values['height']
        expected = [transitions[0]+i*height for i in range(len(transitions))]
        residuals = [actual-target for actual, target in zip(transitions, expected)]
        out['expected_transitions'] = expected
        out['transition_residuals'] = residuals
        out['max_deviation'] = max(abs(item) for item in residuals)
        if height < 2:
            subreason = 'computed_height_below_2'
        elif out['max_deviation'] > 1.1:
            subreason = 'grid_deviation_above_1_1'
            out['failing_transition_index'] = next(i for i, item in enumerate(residuals) if abs(item) > 1.1)
    elif 'last_boundary' in values and values['last_boundary'] is None:
        subreason = 'last_boundary_missing'
    elif out.get('minimum_transition_spacing', 2) < 2:
        subreason = 'observed_spacing_below_2'
    elif all(key in values for key in ('next_y', 'top_y', 'bottom_y')):
        if values['next_y'] >= values['bottom_y']:
            subreason = 'calibration_reached_bottom'
        elif values['next_y']-values['top_y'] > 120:
            subreason = 'calibration_window_exhausted'
    out['subreason'] = subreason
    return _resolution(out)


def _reason(exc):
    from model1_execution import VALIDATION_REASONS
    value = getattr(exc, 'reason', None)
    if isinstance(value, str) and value in VALIDATION_REASONS:
        return value
    value = getattr(exc, 'code', None)
    if value in ('source_not_readable', 'source_login_required', 'source_timeout', 'source_capture_failed'):
        return value
    return 'source_timeout' if type(exc).__name__ in ('TimeoutError', 'ReadTimeout', 'ConnectTimeout') else 'source_capture_failed'


def _safe_candle(value):
    # The reader supplies an observational dictionary, validated here before
    # it crosses either the sidecar or database boundary.
    value = _object(value)
    out = {}
    phases = ('initial', 'final')
    outcomes = ('not_started', 'legend_missing', 'no_pixel_groups',
        'ambiguous_pixel_groups', 'liquidity_wait', 'candle_search',
        'all_candidates_timeout', 'accepted', 'parse_rejected', 'candle_changed')
    reasons = ('no_visible_tooltip', 'multiple_visible_tooltips',
        'text_too_long', 'not_candle', 'accepted')

    def enum(item, key, choices, target):
        if key in item:
            if not isinstance(item[key], str) or item[key] not in choices:
                raise ValueError('Invalid candle diagnostic enum')
            target[key] = item[key]

    def observation(item):
        item = _object(item)
        result = {}
        if 'visible_count' in item:
            result['visible_count'] = _integer(item['visible_count'], 0, 9)
        if 'text_length' in item:
            result['text_length'] = _integer(item['text_length'], 0, 221)
        enum(item, 'kind', ('none', 'multiple', 'candle', 'liquidity', 'other'), result)
        enum(item, 'reason', reasons, result)
        if 'rect' in item:
            result['rect'] = _numbers(item['rect'], ('x', 'y', 'width', 'height'))
        return result

    def tooltip(item):
        item = _object(item)
        result = {}
        if 'poll_count' in item:
            result['poll_count'] = _integer(item['poll_count'], 0, 1000)
        for key in ('first', 'last'):
            if key in item:
                result[key] = observation(item[key])
        if 'reason_counts' in item:
            counts = _object(item['reason_counts'])
            result['reason_counts'] = {key: _integer(counts[key], 0, 1000)
                for key in reasons if key in counts}
        return result

    enum(value, 'phase', phases, out)
    enum(value, 'outcome', outcomes, out)
    if 'marker' in value:
        out['marker'] = None if value['marker'] is None else _numbers(value['marker'], ('x', 'y'))
    for key in ('pixel_count', 'pixel_group_count'):
        if key in value:
            out[key] = _integer(value[key], 0, 1000000)
    if 'pixel_groups' in value:
        out['pixel_groups'] = []
        for group in _array(value['pixel_groups'], 8):
            group = _object(group)
            out['pixel_groups'].append({
                'pixel_count': _integer(group.get('pixel_count'), 0, 1000000),
                'bounds': _numbers(group.get('bounds'), ('left', 'right', 'top', 'bottom'))})
    if 'points' in value:
        out['points'] = [_numbers(point, ('x', 'y')) for point in _array(value['points'], 8)]
    if 'attempts' in value:
        out['attempts'] = []
        for attempt in _array(value['attempts'], 9):
            attempt = _object(attempt)
            item = _numbers(attempt, ('x', 'y', 'viewport_x', 'viewport_y'))
            enum(attempt, 'phase', phases, item)
            enum(attempt, 'outcome', ('pending', 'timeout', 'accepted', 'parse_rejected', 'error'), item)
            if 'point_index' in attempt:
                item['point_index'] = _integer(attempt['point_index'], 0, 7)
            if 'timeout_ms' in attempt:
                if attempt['timeout_ms'] != 350 or type(attempt['timeout_ms']) is not int:
                    raise ValueError('Invalid diagnostic deadline')
                item['timeout_ms'] = 350
            if 'elapsed_ms' in attempt:
                item['elapsed_ms'] = _number(attempt['elapsed_ms'], 0, 1000000)
            if 'tooltip' in attempt:
                item['tooltip'] = tooltip(attempt['tooltip'])
            out['attempts'].append(item)
    for key in ('initial_candle', 'final_candle'):
        if key in value:
            candle = _object(value[key])
            out[key] = _numbers(candle, ('open', 'high', 'low', 'close'), low=0, high=1e12)
            if 'source_time' in candle:
                out[key]['source_time'] = _stamp(candle['source_time'])
    return out


def safe_failure_diagnostics(value, *, job_id=None, timeframe=None,
                             heatmap_model=None, image_sha256=None):
    """Strictly parse saved data; return None for invalid known fields/identity."""
    try:
        value = _object(value)
        if value.get('format_version') != FORMAT_VERSION:
            return None
        out = {'format_version': FORMAT_VERSION, 'job_id': _uuid(value.get('job_id')),
            'heatmap_model': _integer(value.get('heatmap_model'), 1, 3),
            'recorded_at': _stamp(value.get('recorded_at'))}
        if value.get('timeframe') not in ('12H', '24H', '48H'):
            return None
        out['timeframe'] = value['timeframe']
        for expected, actual in ((job_id, out['job_id']), (timeframe, out['timeframe']),
                                 (heatmap_model, out['heatmap_model'])):
            if expected is not None and expected != actual:
                return None
        commit = value.get('deployment_commit')
        if commit is not None and (not isinstance(commit, str) or not _HEX40.fullmatch(commit)):
            return None
        out['deployment_commit'] = commit
        reader = value.get('reader_sha256')
        if not isinstance(reader, str) or not _HEX64.fullmatch(reader):
            return None
        out['reader_sha256'] = reader
        image = value.get('image_sha256')
        if image is not None and (not isinstance(image, str) or not _HEX64.fullmatch(image)):
            return None
        if image != image_sha256:
            return None
        out['image_sha256'] = image
        from model1_execution import VALIDATION_REASONS
        reason = value.get('reason')
        if not isinstance(reason, str) or reason not in VALIDATION_REASONS | {
                'source_not_readable', 'source_login_required', 'source_timeout', 'source_capture_failed'}:
            return None
        out['reason'] = reason
        location = value.get('failure_location')
        if location is not None:
            location = _object(location)
            module = location.get('module')
            if module not in _FUNCTIONS or location.get('function') not in _FUNCTIONS[module]:
                return None
            out['failure_location'] = {'module': module, 'function': location['function'],
                'line': _integer(location.get('line'), 1, 100000)}
        for key, fields in {
            'geometry': ('x', 'y', 'width', 'height', 'page_width'),
            'bounds': ('left', 'right', 'top', 'bottom'),
            'viewport': ('width', 'height'),
        }.items():
            if key in value:
                out[key] = _numbers(value[key], fields)
        for key in ('scroll_y', 'x'):
            if key in value:
                out[key] = _number(value[key], -1e6, 1e6)
        if 'resolution' in value:
            out['resolution'] = _resolution(value['resolution'])
        if 'candle' in value:
            out['candle'] = _safe_candle(value['candle'])
        if 'render_observations' in value:
            from capture_readiness import safe_render_observations
            observations = safe_render_observations(value['render_observations'])
            if observations:
                out['render_observations'] = observations
        if len(json.dumps(out, allow_nan=False, separators=(',', ':')).encode('utf-8')) > MAX_BYTES:
            return None
        return out
    except (ValueError, TypeError, OverflowError, AttributeError, ImportError):
        return None


def _read_json(path, limit):
    if not path.is_file() or path.is_symlink() or path.stat().st_size > limit:
        return None
    data = path.read_bytes()
    if len(data) > limit:
        return None
    # Reject non-finite JSON literals rather than Python's permissive default.
    def invalid_constant(_):
        raise ValueError('Invalid diagnostic JSON')
    return json.loads(data, parse_constant=invalid_constant)


def _image_hash(root, timeframe):
    path = root/'capture'/f'coinglass_btc_heatmap_{timeframe.lower()}.png'
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_IMAGE:
        return None
    data = path.read_bytes()
    if len(data) > MAX_IMAGE or not data.startswith(b'\x89PNG\r\n\x1a\n'):
        return None
    return hashlib.sha256(data).hexdigest()


def save_failure_diagnostics(exc, root, *, job_id, timeframe, heatmap_model):
    """Write existing failure facts atomically; diagnostic errors stay private."""
    temporary = None
    try:
        root = Path(root)
        _uuid(job_id)
        if timeframe not in ('12H', '24H', '48H'):
            return False
        _integer(heatmap_model, 1, 3)
        reader_path = _HERE/'heatmap_hover.py'
        reader_bytes = reader_path.read_bytes()
        if len(reader_bytes) > 1024*1024:
            return False
        commit = os.environ.get('RENDER_GIT_COMMIT')
        commit = commit if isinstance(commit, str) and _HEX40.fullmatch(commit) else None
        payload = {'format_version': FORMAT_VERSION, 'job_id': job_id,
            'heatmap_model': heatmap_model, 'timeframe': timeframe,
            'recorded_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'deployment_commit': commit, 'reader_sha256': hashlib.sha256(reader_bytes).hexdigest(),
            'image_sha256': _image_hash(root, timeframe), 'reason': _reason(exc)}
        tb = exc.__traceback__
        while tb is not None:
            frame = tb.tb_frame
            path = Path(frame.f_code.co_filename).resolve()
            name = frame.f_code.co_name
            module = path.stem
            approved_path = (_HERE/'market_vision/coinglass_heatmap_capture.py' if module == 'coinglass_heatmap_capture'
                else _HERE/(module+'.py'))
            if path == approved_path and module in _FUNCTIONS and name in _FUNCTIONS[module]:
                payload['failure_location'] = {'module': module, 'function': name, 'line': tb.tb_lineno}
                values = frame.f_locals
                if name == 'capture_column':
                    for key, fields in {'geometry': ('x', 'y', 'width', 'height', 'page_width'),
                            'bounds': ('left', 'right', 'top', 'bottom'),
                            'viewport': ('width', 'height')}.items():
                        try:
                            if type(values.get(key)) is dict:
                                payload[key] = _numbers(values[key], fields)
                        except (ValueError, TypeError, OverflowError):
                            pass
                    for key in ('scroll_y', 'x'):
                        try:
                            if key in values:
                                payload[key] = _number(values[key], -1e6, 1e6)
                        except (ValueError, TypeError, OverflowError):
                            pass
                    if payload['reason'] == 'hover_resolution_insufficient':
                        try:
                            payload['resolution'] = _resolution_from_locals(values)
                        except (ValueError, TypeError, OverflowError, KeyError):
                            pass
                if name == 'capture_heatmaps' and type(values.get('chart_rect')) is dict:
                    try:
                        payload.setdefault('geometry', _numbers(values['chart_rect'],
                            ('x', 'y', 'width', 'height', 'page_width')))
                    except (ValueError, TypeError, OverflowError):
                        pass
                if type(values.get('candle_diagnostics')) is dict:
                    try:
                        payload['candle'] = _safe_candle(values['candle_diagnostics'])
                    except (ValueError, TypeError, OverflowError):
                        pass
            tb = tb.tb_next
        render_path = root/'capture'/f'coinglass_btc_heatmap_{timeframe.lower()}.render.json'
        try:
            render = _read_json(render_path, 8192)
            if type(render) is dict and 'render_observations' in render:
                from capture_readiness import safe_render_observations
                observations = safe_render_observations(render['render_observations'])
                if observations:
                    payload['render_observations'] = observations
        except (ValueError, TypeError, OverflowError, OSError, ImportError):
            pass
        payload = safe_failure_diagnostics(payload, job_id=job_id, timeframe=timeframe,
            heatmap_model=heatmap_model, image_sha256=payload['image_sha256'])
        if payload is None:
            return False
        encoded = json.dumps(payload, allow_nan=False, separators=(',', ':')).encode('utf-8')
        if len(encoded) > MAX_BYTES:
            return False
        root.mkdir(parents=True, exist_ok=True)
        temporary = root/(SIDECAR_NAME+'.tmp')
        if temporary.is_symlink():
            return False
        with temporary.open('wb') as stream:
            os.chmod(temporary, 0o600)
            stream.write(encoded)
        temporary.replace(root/SIDECAR_NAME)
        return True
    except Exception:
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def read_failure_diagnostics(root, *, job_id, timeframe, heatmap_model, image_sha256):
    """Load only a bounded sidecar bound to this job and retained screenshot."""
    try:
        _uuid(job_id)
        _integer(heatmap_model, 1, 3)
        value = _read_json(Path(root)/SIDECAR_NAME, MAX_BYTES)
        return safe_failure_diagnostics(value, job_id=job_id, timeframe=timeframe,
            heatmap_model=heatmap_model, image_sha256=image_sha256)
    except Exception:
        return None
