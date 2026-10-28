"""Failure-only retention with real guard expressions; no source/browser/DB calls."""
import ast
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE/'runtime'))
import capture_failure_diagnostics as diagnostics
import heatmap_hover as hover
import model1_execution as execution
from model1_evidence_format import evidence_payload

JOB = '11111111-2222-4333-8444-555555555555'
OTHER_JOB = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jR3sAAAAASUVORK5CYII=')
IMAGE_HASH = hashlib.sha256(PNG).hexdigest()
SECRET = 'PRIVATE_SOURCE_TEXT_AND_COOKIE'


def row(price):
    return {'price': price, 'value': 1000000, 'rounding_error': 5000,
        'unit': 'source_metric', 'source_time': '2026-10-28T08:10:00Z',
        'raw_price': SECRET, 'raw_value': SECRET, 'page_text': SECRET}


def fixture_values():
    return {'geometry': {'x': 300.25, 'y': 536.125, 'width': 1259.75,
                'height': 730.375, 'page_width': 1600, 'cookie': SECRET},
        'bounds': {'left': 372, 'top': 576, 'right': 1494, 'bottom': 1173},
        'viewport': {'width': 1600, 'height': 1000}, 'scroll_y': 556, 'x': 1493.75,
        'top_y': 576.25, 'bottom_y': 1173, 'top_row': row(84420),
        'calibration': [row(84420-i*56.26) for i in range(4)],
        'transitions': [576.75, 581.75, 587.75], 'grid_origin': 576.75, 'initial_step': 56.26,
        'bottom_row': row(77556), 'last_inner': row(77612.26),
        'last_boundary': 1164.75, 'count': 122}


def execute_existing_guard(selector, values):
    """Run a guard copied from the actual reader, retaining its source line.

    No fake implementation of the calibration decision is tested: the real
    existing if/raise expression executes with explicit observed numeric inputs.
    """
    path = Path(hover.__file__).resolve()
    tree = ast.parse(path.read_text())
    original = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == 'capture_column')
    guards = [node for node in ast.walk(original) if isinstance(node, ast.If)
        and any(isinstance(child, ast.Raise) and isinstance(child.exc, ast.Call)
            and child.exc.args and isinstance(child.exc.args[0], ast.Constant)
            and child.exc.args[0].value == 'hover_resolution_insufficient' for child in node.body)]
    guard = next(node for node in guards if selector in ast.unparse(node.test))
    function = ast.FunctionDef(name='capture_column', args=ast.arguments(
        posonlyargs=[], args=[ast.arg(arg=name) for name in values], vararg=None,
        kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=[copy.deepcopy(guard)], decorator_list=[])
    ast.copy_location(function, original)
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    namespace = {'HoverError': hover.HoverError}
    exec(compile(module, str(path), 'exec'), namespace)
    try:
        namespace['capture_column'](**values)
    except hover.HoverError as exc:
        return exc, next(child.lineno for child in guard.body if isinstance(child, ast.Raise))
    raise AssertionError('Fixture did not reach the existing rejection')


class CaptureFailureDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'capture').mkdir()
        (self.root/'capture/coinglass_btc_heatmap_12h.png').write_bytes(PNG)

    def record(self, exc, *, image=True):
        if not image:
            (self.root/'capture/coinglass_btc_heatmap_12h.png').unlink(missing_ok=True)
        self.assertTrue(diagnostics.save_failure_diagnostics(exc, self.root,
            job_id=JOB, timeframe='12H', heatmap_model=1))
        raw = json.loads((self.root/diagnostics.SIDECAR_NAME).read_text())
        retained = diagnostics.read_failure_diagnostics(self.root, job_id=JOB,
            timeframe='12H', heatmap_model=1, image_sha256=IMAGE_HASH if image else None)
        self.assertEqual(raw, retained)
        return retained

    def basic_payload(self):
        return self.record(hover.HoverError('hover_candle_not_found'))

    def test_exact_existing_resolution_branches_retain_inputs_and_source_line(self):
        cases = [
            ('next_y', {'top_y': 0, 'bottom_y': 125, 'next_y': 125,
                'calibration': [row(84420)], 'transitions': []}, 'calibration_reached_bottom'),
            ('next_y', {'top_y': 0, 'bottom_y': 597, 'next_y': 121,
                'calibration': [row(84420)], 'transitions': []}, 'calibration_window_exhausted'),
            ('min(', {'transitions': [578, 579, 580]}, 'observed_spacing_below_2'),
            ('last_boundary is None', {'last_boundary': None, 'offset': 121}, 'last_boundary_missing'),
            ('height < 2', {'height': 1.9}, 'computed_height_below_2'),
            ('height < 2', {'height': 4.9}, 'grid_deviation_above_1_1'),
        ]
        for selector, changes, expected in cases:
            with self.subTest(expected=expected):
                values = fixture_values()
                if selector == 'next_y':
                    for key in ('last_boundary', 'last_inner', 'initial_step', 'bottom_row', 'count'):
                        values.pop(key)
                elif selector == 'min(':
                    values.pop('last_boundary')
                values.update(changes)
                exc, line = execute_existing_guard(selector, values)
                original_reason = exc.reason
                data = self.record(exc)
                self.assertEqual(exc.reason, original_reason)
                self.assertEqual(data['reason'], 'hover_resolution_insufficient')
                self.assertEqual(data['resolution']['subreason'], expected)
                self.assertEqual(data['failure_location'], {
                    'module': 'heatmap_hover', 'function': 'capture_column', 'line': line})
                self.assertEqual(data['geometry']['y'], 536.125)
                self.assertEqual(data['geometry']['height'], 730.375)
                self.assertNotIn(SECRET, json.dumps(data))
                if expected == 'grid_deviation_above_1_1':
                    self.assertAlmostEqual(data['resolution']['max_deviation'], 1.2)
                    self.assertEqual(data['resolution']['failing_transition_index'], 2)

    def test_no_geometry_still_retains_original_reason_and_identity(self):
        path = str(Path(hover.__file__).resolve())
        namespace = {'HoverError': hover.HoverError}
        exec(compile("def capture_column():\n    geometry=None\n    viewport=None\n    raise HoverError('hover_geometry_missing')\n", path, 'exec'), namespace)
        try:
            namespace['capture_column']()
        except hover.HoverError as exc:
            data = self.record(exc, image=False)
        self.assertEqual(data['reason'], 'hover_geometry_missing')
        self.assertEqual(data['job_id'], JOB)
        self.assertIsNone(data['image_sha256'])
        self.assertNotIn('geometry', data)
        self.assertNotIn('viewport', data)

    def test_borderline_rejection_preserves_fractional_inputs_without_rounding(self):
        values = fixture_values()
        values['height'] = 4.94896
        values['last_boundary'] = values['grid_origin']+(values['count']-2)*values['height']
        exc, _ = execute_existing_guard('height < 2', values)
        detail = self.record(exc)['resolution']
        self.assertEqual(detail['height'], 4.94896)
        self.assertEqual(detail['subreason'], 'grid_deviation_above_1_1')
        self.assertAlmostEqual(detail['max_deviation'], 1.10208, places=9)
        self.assertNotEqual(detail['max_deviation'], round(detail['max_deviation'], 3))

    def test_failure_reports_the_fitted_origin_and_last_boundary(self):
        values = fixture_values()
        values.update(height=4.9, grid_origin=576.35)
        values['last_boundary'] = values['grid_origin']+(values['count']-2)*values['height']
        exc, _ = execute_existing_guard('height < 2', values)
        detail = self.record(exc)['resolution']
        self.assertEqual(detail['grid_origin'], 576.35)
        self.assertAlmostEqual(detail['max_deviation'], 1.6)
        self.assertAlmostEqual(detail['last_boundary_residual'], 0)
        self.assertEqual(detail['failing_transition_index'], 2)

        values.update(transitions=[576.35, 581.25, 586.15])
        values['last_boundary'] += 1.2
        exc, _ = execute_existing_guard('height < 2', values)
        detail = self.record(exc)['resolution']
        self.assertEqual(detail['subreason'], 'grid_deviation_above_1_1')
        self.assertAlmostEqual(detail['max_deviation'], 1.2)
        self.assertAlmostEqual(detail['last_boundary_residual'], 1.2)
        self.assertEqual(detail['failing_transition_index'], values['count']-2)

    def test_only_canonical_identity_hashes_and_bounded_timestamps_cross_storage(self):
        payload = self.basic_payload()
        self.assertRegex(payload['reader_sha256'], r'^[a-f0-9]{64}$')
        for key, bad in (('job_id', SECRET), ('deployment_commit', SECRET),
                ('reader_sha256', SECRET), ('image_sha256', SECRET),
                ('timeframe', '7D'), ('heatmap_model', True),
                ('recorded_at', '2026-02-30T00:00:00Z')):
            with self.subTest(key=key):
                variant = {**payload, key: bad}
                self.assertIsNone(diagnostics.safe_failure_diagnostics(variant, image_sha256=IMAGE_HASH))
        for expected in ({'job_id': OTHER_JOB}, {'timeframe': '24H'}, {'heatmap_model': 2},
                {'image_sha256': 'f'*64}):
            kwargs = {'job_id': JOB, 'timeframe': '12H', 'heatmap_model': 1, 'image_sha256': IMAGE_HASH}
            kwargs.update(expected)
            self.assertIsNone(diagnostics.read_failure_diagnostics(self.root, **kwargs))

    def test_sanitizer_removes_unknown_keys_and_rejects_nonfinite_or_unbounded_fields(self):
        payload = self.basic_payload()
        payload.update(token=SECRET, arbitrary_error=SECRET,
            geometry={'x': 300.25, 'height': 730.375, 'url': SECRET})
        clean = diagnostics.safe_failure_diagnostics(payload, image_sha256=IMAGE_HASH)
        self.assertEqual(clean['geometry'], {'x': 300.25, 'height': 730.375})
        self.assertNotIn(SECRET, json.dumps(clean))
        for bad in (float('nan'), float('inf'), -float('inf'), True, 1e100, SECRET):
            with self.subTest(bad=repr(bad)):
                self.assertIsNone(diagnostics.safe_failure_diagnostics(
                    {**payload, 'geometry': {'x': bad}}, image_sha256=IMAGE_HASH))
        for stamp in ('1999-12-31T23:59:59Z', '2100-01-01T00:00:00Z',
                '2026-10-28T08:10:00+00:00', SECRET):
            value = {**payload, 'resolution': {'subreason': 'unresolved',
                'calibration': [{'price': 81000, 'source_time': stamp}]}}
            self.assertIsNone(diagnostics.safe_failure_diagnostics(value, image_sha256=IMAGE_HASH))

    def test_candle_schema_keeps_numbers_and_rejection_summary_without_raw_tooltips(self):
        payload = self.basic_payload()
        observation = {'visible_count': 1, 'kind': 'liquidity', 'reason': 'not_candle',
            'text_length': 80, 'text': SECRET, 'rect': {'x': 1370.25, 'y': 300.5, 'width': 200, 'height': 60}}
        payload['candle'] = {'phase': 'initial', 'outcome': 'all_candidates_timeout',
            'marker': {'x': 962, 'y': 548}, 'pixel_count': 1, 'pixel_group_count': 1,
            'pixel_groups': [{'pixel_count': 1, 'bounds': {'left': 1492, 'right': 1492, 'top': 861, 'bottom': 861}}],
            'points': [{'x': 1492.5, 'y': 861.5}],
            'attempts': [{'phase': 'initial', 'point_index': 0, 'x': 1492.5, 'y': 861.5,
                'viewport_x': 1492.5, 'viewport_y': 305.5, 'timeout_ms': 350,
                'elapsed_ms': 350.125, 'outcome': 'timeout', 'tooltip': {'poll_count': 15,
                    'first': observation, 'last': observation, 'reason_counts': {'not_candle': 15, SECRET: 1}}}],
            'initial_candle': {'open': 81000, 'high': 81001, 'low': 80999, 'close': 81000,
                'source_time': '2026-10-28T07:30:00Z', 'raw_text': SECRET}}
        clean = diagnostics.safe_failure_diagnostics(payload, image_sha256=IMAGE_HASH)
        self.assertNotIn(SECRET, json.dumps(clean))
        attempt = clean['candle']['attempts'][0]
        self.assertEqual(attempt['tooltip']['reason_counts'], {'not_candle': 15})
        self.assertEqual(attempt['x'], 1492.5)
        self.assertEqual(attempt['timeout_ms'], 350)
        for mutation in ({'attempts': payload['candle']['attempts']*10},
                {'pixel_groups': payload['candle']['pixel_groups']*9},
                {'outcome': SECRET}):
            changed = {**payload, 'candle': {**payload['candle'], **mutation}}
            self.assertIsNone(diagnostics.safe_failure_diagnostics(changed, image_sha256=IMAGE_HASH))

    def test_sidecar_is_bounded_private_and_invalid_files_do_not_affect_png_retention(self):
        self.basic_payload()
        path = self.root/diagnostics.SIDECAR_NAME
        self.assertLessEqual(path.stat().st_size, diagnostics.MAX_BYTES)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertFalse(path.with_name(path.name+'.tmp').exists())
        image, detail = evidence_payload(self.root, '12H', job_id=JOB, heatmap_model=1)
        self.assertEqual(image, PNG)
        self.assertEqual(detail['capture_failure']['job_id'], JOB)
        self.assertFalse(detail['assessment_validated'])
        for invalid in ('{'+' '*diagnostics.MAX_BYTES, '{"x":NaN}', '{broken'):
            path.write_text(invalid)
            image, detail = evidence_payload(self.root, '12H', job_id=JOB, heatmap_model=1)
            self.assertEqual(image, PNG)
            self.assertNotIn('capture_failure', detail)
        path.unlink()
        path.symlink_to(self.root/'capture/coinglass_btc_heatmap_12h.png')
        self.assertIsNone(diagnostics.read_failure_diagnostics(self.root, job_id=JOB,
            timeframe='12H', heatmap_model=1, image_sha256=IMAGE_HASH))

    def test_optional_broken_render_json_does_not_erase_failure_identity(self):
        (self.root/'capture/coinglass_btc_heatmap_12h.render.json').write_text('{broken')
        data = self.basic_payload()
        self.assertEqual(data['reason'], 'hover_candle_not_found')

    def test_existing_render_observations_survive_failure_and_timeout_without_new_source_reads(self):
        before = {'stage': 'before_screenshot', 'ready': True, 'reason': 'render-checks-passed',
            'label': '12 hour', 'visible_nodata': True, 'visible_request_limit': False,
            'canvas_count': 2, 'canvas_rect': {'x': 300.25, 'y': 536.125, 'width': 1259.75, 'height': 730.375},
            'viewport': {'width': 1600, 'height': 1000}, 'scroll_x': 0, 'scroll_y': 0,
            'observed_ms': 20000.125, 'cookie': SECRET}
        after = {**before, 'stage': 'after_screenshot', 'observed_ms': 20500.25}
        sidecar = {'source_readiness': {'ready': True, 'reason': 'render-checks-passed'},
            'render_observations': [before, after], 'text': SECRET}
        (self.root/'capture/coinglass_btc_heatmap_12h.render.json').write_text(json.dumps(sidecar))
        retained = self.record(hover.HoverError('hover_plot_not_found'))
        self.assertEqual(len(retained['render_observations']), 2)
        self.assertTrue(retained['render_observations'][0]['visible_nodata'])
        self.assertTrue(retained['render_observations'][0]['ready'])
        self.assertEqual(retained['render_observations'][0]['canvas_rect']['y'], 536.125)
        self.assertNotIn(SECRET, json.dumps(retained))
        image, detail = evidence_payload(self.root, '12H', job_id=JOB, heatmap_model=1)
        self.assertEqual(image, PNG)
        self.assertIn('render_observations', detail['capture_failure'])
        # A killed child may have written render metadata but no exception file.
        (self.root/diagnostics.SIDECAR_NAME).unlink()
        image, detail = evidence_payload(self.root, '12H', job_id=JOB, heatmap_model=1)
        self.assertEqual(image, PNG)
        self.assertNotIn('capture_failure', detail)
        self.assertEqual(detail['render_observations'], retained['render_observations'])
        self.assertFalse(detail['assessment_validated'])
        self.assertNotIn(SECRET, json.dumps(detail))

    def test_failed_atomic_write_leaves_no_partial_sidecar(self):
        with patch.object(diagnostics.os, 'chmod', side_effect=OSError(SECRET)):
            self.assertFalse(diagnostics.save_failure_diagnostics(
                hover.HoverError('hover_candle_not_found'), self.root,
                job_id=JOB, timeframe='12H', heatmap_model=1))
        self.assertFalse((self.root/diagnostics.SIDECAR_NAME).exists())
        self.assertFalse((self.root/(diagnostics.SIDECAR_NAME+'.tmp')).exists())

    def test_diagnostic_failure_preserves_existing_error_file_and_return_code(self):
        def main(*_):
            execution.stage('capture')
            raise hover.HoverError('hover_resolution_insufficient')
        with patch.object(diagnostics, 'save_failure_diagnostics', side_effect=OSError(SECRET)) as save:
            self.assertEqual(execution.run_task(main, '12H', JOB, self.root), 1)
        save.assert_called_once()
        error_path = self.root/'error.json'
        error = json.loads(error_path.read_text())
        self.assertEqual(error['code'], 'source_capture_failed')
        self.assertEqual(error['validation_reason'], 'hover_resolution_insufficient')
        self.assertLessEqual(error_path.stat().st_size, 4096)
        self.assertNotIn(SECRET, error_path.read_text())
        self.assertFalse((self.root/diagnostics.SIDECAR_NAME).exists())

    def test_success_and_analysis_failures_do_not_write_capture_sidecar(self):
        with patch.object(diagnostics, 'save_failure_diagnostics') as save:
            self.assertEqual(execution.run_task(lambda *_: None, '12H', JOB, self.root), 0)
            save.assert_not_called()
            def fail_analysis(*_):
                execution.stage('analysis')
                raise ValueError(SECRET)
            self.assertEqual(execution.run_task(fail_analysis, '12H', JOB, self.root), 1)
            save.assert_not_called()

    def test_commit_is_selected_nonsecret_metadata_only(self):
        with patch.dict(os.environ, {'RENDER_GIT_COMMIT': 'a'*40, 'UNRELATED_TOKEN': SECRET}):
            payload = self.basic_payload()
        self.assertEqual(payload['deployment_commit'], 'a'*40)
        self.assertNotIn(SECRET, json.dumps(payload))


if __name__ == '__main__':
    unittest.main()
