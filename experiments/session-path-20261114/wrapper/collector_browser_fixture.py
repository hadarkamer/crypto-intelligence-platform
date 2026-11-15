#!/usr/bin/env python3
"""Isolated worker with original synthetic fixtures and a gated source probe.

The --live-worker path is only for observation_supervisor.run_source_probe:
one BTC/M1/12H source capture, no AI or database, using the pinned reader.
The synthetic fixture entry remains separate and does not use an account.
"""
import argparse
import ast
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import time
import types
from urllib.parse import parse_qsl, urlsplit
from unittest.mock import patch

ROOT = Path('/lab/collector')
RUNTIME = ROOT / 'standalone_collector/runtime'
FIXTURE = ROOT / 'experiments/model3-preparation-fix-20261031/check_capture.py'
CHROMIUM = Path('/opt/playwright/chromium-1187/chrome-linux/chrome')
EVIDENCE = Path('/lab/evidence/collector')
COMMIT = '33bad18f8be322009108584a6b71f3d65fa9b351'
HTML_SHA = '566b3bb9bec4345df2a95f404c9ed8ba4a57fbbe92402e4a683c542f306e3a01'
HASHES = {
    'capture_readiness.py': 'a21fcb00acfc4ba20bb1d54d6342e2f89521ad2d941792822cbbd20de7cc437a',
    'model1_execution.py': '5a82f410b089307d3ad465d3abb016f4616a0932c3f7c75a00a2584a95117693',
    'heatmap_models.py': 'de2ed269648628bd124b83e159d4abb302cb024479721a7babfb0ad92ec7bf7e',
    'market_vision/coinglass_heatmap_capture.py': '3a8778e0c6de4b4c1e24dc8dea3292e2e6c5ce2e2163bad251c3edc12a2f129a',
    'heatmap_hover.py': '693aa6739a8b1f50d0550cd802dde99cff4c68edfad6c643c1adf8d6bc05f820',
    'model3_rendering.py': 'b3193d4cf58304637485a8aeb2852dff5dbb45be5ea2628396b31ebd5863ce8f',
    'model3_render_gate.js': '80b533ef6ad274f7fc2532964de308d7bb8a066fe386508551866f04f3dbda8a',
}
CASES = ('capture', 'gates', 'failure', 'timeout')

# The separately approved live path is fixed to one asset/model/range. It does
# not change any installed collector module; these guards exist only in this
# isolated process. Only a verified map crop and filtered hover may be retained.
LIVE_PHASES = frozenset({'startup', 'capture', 'select_12h', 'model_selection',
    'symbol_selection', 'wait_for_chart', 'screenshot', 'navigation', 'browser_launch',
    'browser_context', 'source_session_setup', 'page_creation', 'overlay_dismissal',
    'timeframe_selection', 'legend_preparation', 'chart_geometry', 'render_settle',
    'context_close', 'browser_close', 'render_readiness', 'timeframe_trigger_lookup',
    'timeframe_open', 'timeframe_option_lookup', 'timeframe_select',
    'timeframe_confirmation', 'hover_collection'})
LIVE_REASONS = frozenset({'capture_verified', 'work_deadline', 'second_navigation_forbidden',
    'reload_forbidden', 'retry_forbidden', 'forced_click_forbidden', 'access_modal_blocked',
    'native_capture_rejected', 'live_setup_failed', 'live_capture_failed',
    'identity_failed', 'output_invalid', 'ephemeral_cleanup_failed',
    'browser_close_unverified', 'unexpected_environment', 'runtime_pin_mismatch',
    'account_auth_setup_failed', 'account_auth_cleanup_failed'})
LIVE_NATIVE_CODES = frozenset({'source_capture_failed', 'source_timeout',
    'source_not_readable', 'source_login_required', 'screenshot_identity_mismatch',
    'image_evidence_invalid', 'price_uncertain', 'zones_invalid', 'worker_crashed'})
LIVE_NATIVE_REASONS = frozenset(('hover_deadline hover_geometry_invalid hover_geometry_missing '
    'hover_plot_clipped hover_plot_not_found hover_point_multiple_rows hover_pool_binding '
    'hover_pool_empty_gap hover_pool_overlap_or_gap hover_pool_rows_invalid hover_pool_uncertain '
    'hover_price_grid_gap hover_range_single_row hover_resolution_insufficient hover_row_count '
    'hover_row_order hover_rows_merged hover_snapshot_changed hover_source_stale hover_text_invalid '
    'hover_time_invalid hover_timezone_not_utc hover_update_timeout hover_candidates_unresolved '
    'hover_candidate_missing hover_candidate_disagreement hover_cookie_overlay hover_price_axis_changed '
    'hover_price_axis_mismatch hover_image_evidence_missing hover_image_changed hover_candidate_evidence_mismatch '
    'hover_pool_not_visible hover_candle_text_invalid hover_candle_time_invalid hover_candle_number_invalid '
    'hover_candle_evidence_mismatch hover_candle_time_mismatch hover_candle_not_found hover_candle_outside_axis '
    'hover_candle_changed hover_number_invalid').split())
LIVE_BOOLS = ('runtime_pins_verified', 'source_identity_verified', 'native_hover_validated',
              'png_validated', 'png_deleted', 'browsers_disconnected')
LIVE_COUNTS = {'screenshots': 1, 'browser_launches': 1, 'contexts': 1, 'navigation_calls': 1,
               'requests_seen': 100000, 'responses_seen': 100000,
               'blocked_navigation_requests': 100000, 'row_count': 1600,
               'tooltip_reads': 100000, 'png_width': 20000, 'png_height': 20000}
LIVE_ACCESS_MODAL = r'''() => [...document.querySelectorAll('[role="dialog"],.MuiModal-root')].some(e=>{
 const r=e.getBoundingClientRect();if(!r.width||!r.height)return false;
 for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
 if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity||1)===0)return false;}
 return /\blog\s*in\b|\bsign\s*in\b|unlock\s+full\s+data/i.test(e.innerText||'');
})'''


class LiveStop(BaseException):
    """Not swallowed by the original best-effort controls or retry handler."""
    def __init__(self, reason):
        self.reason = reason if reason in LIVE_REASONS else 'live_capture_failed'


def empty_restore_evidence():
    return {'exclusive_input_validated': False, 'legacy_header_absent': False,
            'context_state_applied': False, 'cookies_snapshot_observed': False,
            'cookies_changed': None, 'private_state_released': False}


def restore_evidence_valid(value):
    template = empty_restore_evidence()
    if not isinstance(value, dict) or set(value) != set(template): return False
    if any(type(value[k]) is not bool for k in template if k != 'cookies_changed'): return False
    if value['cookies_changed'] is not None and type(value['cookies_changed']) is not bool: return False
    return value['cookies_snapshot_observed'] == (value['cookies_changed'] is not None)


def empty_live_report():
    return {'format': 'collector-live-child.v1', 'model': 1, 'timeframe': '12H',
            'coin': 'BTC', 'commit': COMMIT, 'status': 'failed', 'reason': 'live_setup_failed',
            'phase': 'startup', 'native_code': None, 'native_reason': None,
            **{key: False for key in LIVE_BOOLS}, **{key: 0 for key in LIVE_COUNTS},
            'png_sha256': None, 'source_time': None, 'candle_close': None,
            'session_restore': empty_restore_evidence(), 'access_verified': False,
            'account_auth': None, 'artifact_status': 'not_ready', 'artifact_files': [],
            'account_observation': None,
            'map_png_sha256': None, 'hover_sha256': None, 'site_code_sha256': None, 'private_output_deleted': False,
            'total_value': None, 'network': {'http_errors': [], 'network_errors': [], 'page_error_count': 0}}


def _sanitize_live_report(value):
    """Exact receipt schema; raw output, source text, headers and URLs cannot pass."""
    template = empty_live_report()
    if not isinstance(value, dict) or set(value) != set(template): return None
    for key in ('format', 'model', 'timeframe', 'coin', 'commit'):
        if type(value[key]) is not type(template[key]) or value[key] != template[key]: return None
    if value['status'] not in ('passed', 'stopped', 'failed') or value['reason'] not in LIVE_REASONS: return None
    if value['phase'] not in LIVE_PHASES: return None
    if value['native_code'] is not None and value['native_code'] not in LIVE_NATIVE_CODES: return None
    if value['native_reason'] is not None and value['native_reason'] not in LIVE_NATIVE_REASONS: return None
    if any(type(value[k]) is not bool for k in LIVE_BOOLS): return None
    if type(value['private_output_deleted']) is not bool: return None
    if value['artifact_status'] not in ('not_ready', 'retained', 'unavailable', 'cleanup_failed'): return None
    base_files = value['artifact_files'][:-1] if value['artifact_files'] and value['artifact_files'][-1] == 'site_code.json' else value['artifact_files']
    if base_files not in ([], ['receipt.json'], ['map.png', 'hover.json', 'receipt.json']): return None
    for key in ('map_png_sha256', 'hover_sha256', 'site_code_sha256'):
        if value[key] is not None and not (isinstance(value[key], str) and re.fullmatch('[0-9a-f]{64}', value[key])): return None
    if ('site_code.json' in value['artifact_files']) != (value['site_code_sha256'] is not None): return None
    if value['account_auth'] is not None: return None
    if not restore_evidence_valid(value['session_restore']) or type(value['access_verified']) is not bool: return None
    if value['account_observation'] is not None and sanitize_account_observation(value['account_observation']) is None: return None
    if value['artifact_status'] == 'retained':
        if (base_files != ['map.png', 'hover.json', 'receipt.json']
                or value['png_deleted'] or not value['private_output_deleted']
                or not value['map_png_sha256'] or not value['hover_sha256']): return None
    if any(type(value[k]) is not int or not 0 <= value[k] <= maximum for k, maximum in LIVE_COUNTS.items()): return None
    if value['png_sha256'] is not None and not (isinstance(value['png_sha256'], str) and re.fullmatch('[0-9a-f]{64}', value['png_sha256'])): return None
    if value['source_time'] is not None and not (isinstance(value['source_time'], str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value['source_time'])): return None
    for key in ('candle_close', 'total_value'):
        number = value[key]
        if number is not None and (type(number) not in (float, int) or not math.isfinite(number) or not 0 <= number < 1e18): return None
    network = value['network']
    if not isinstance(network, dict) or set(network) != {'http_errors', 'network_errors', 'page_error_count'}: return None
    if type(network['page_error_count']) is not int or not 0 <= network['page_error_count'] <= 1000: return None
    for field in ('http_errors', 'network_errors'):
        rows = network[field]
        if not isinstance(rows, list) or len(rows) > 6: return None
        detail = 'status' if field == 'http_errors' else 'kind'
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'host_group', 'count', detail}: return None
            if row['host_group'] not in ('coinglass', 'other') or type(row['count']) is not int or not 1 <= row['count'] <= 1000: return None
            if detail == 'status' and (type(row[detail]) is not int or not 400 <= row[detail] <= 599): return None
            if detail == 'kind' and row[detail] not in ('timeout', 'connection', 'cancelled', 'blocked', 'other'): return None
    if value['status'] == 'passed':
        if value['reason'] != 'capture_verified' or not all(value[k] for k in LIVE_BOOLS if k != 'png_deleted'): return None
        if any(value[k] != 1 for k in ('screenshots', 'browser_launches', 'contexts', 'navigation_calls')): return None
        if not value['row_count'] or not value['png_sha256'] or not value['source_time'] or not value['candle_close']: return None
    return dict(value)


def sanitize_live_report(value):
    try: return _sanitize_live_report(value)
    except (TypeError, ValueError, OverflowError): return None


def load_private_storage(capture):
    """Use the pinned loader, validate cookie-only scope, erase auth inputs."""
    from session_restore_state import validate_storage_state, Rejected
    try:
        if os.environ.get('COINGLASS_COOKIE_HEADER') != '':
            raise LiveStop('unexpected_environment')
        raw = os.environ.get('COINGLASS_STORAGE_STATE_JSON')
        validated = json.loads(validate_storage_state(raw))
        state = capture._load_authenticated_storage_state()
        if state != validated:
            raise LiveStop('live_setup_failed')
        return state
    except Rejected:
        raise LiveStop('live_setup_failed') from None
    finally:
        os.environ.pop('COINGLASS_STORAGE_STATE_JSON', None)
        os.environ.pop('COINGLASS_COOKIE_HEADER', None)



def live_acceptance_enabled(environment):
    # Parent approval lives in memory; child receives only these nonsecret gates.
    return (environment.get('COLLECTOR_SOURCE_SUPERVISED') == '1'
            and environment.get('COLLECTOR_ACCOUNT_AUTH_ACCEPTANCE') == '1')


class SessionRestoreLifecycle:
    """Passive observation and cookie-only snapshot before native context close.

    The snapshot lives in memory only and is discarded after comparison. A
    changed cookie is not evidence that the server renewed authentication.
    """
    def __init__(self, initial_state, report, managed=None):
        self.initial_state, self.report = initial_state, report
        self.managed = managed
        self.context = None
        self.baseline = None
        self.cookie_urls = []
        self.approved_domains = {item['domain'] for item in initial_state['cookies']}
        self.render_guard_passed = False
        self.attempted = self.finished = self.cleanup_failed = False
        self.account_observer = None

    def establish_context(self, context):
        if self.context is not None: raise LiveStop('live_setup_failed')
        self.context = context
        self.cookie_urls = sorted({'https://' + item['domain'].lstrip('.') + item['path']
                                  for item in self.initial_state['cookies']})
        self.baseline = self.read_cookie_comparison()
        if not self.baseline: raise LiveStop('live_setup_failed')

    def read_cookie_comparison(self):
        # The browser filters by approved URLs before the exact-domain filter.
        snapshot = self.context.cookies(urls=self.cookie_urls)
        return sorted(json.dumps(item, sort_keys=True, separators=(',', ':'))
                      for item in snapshot if item.get('domain') in self.approved_domains)

    def attach(self, page):
        if self.attempted: raise LiveStop('live_setup_failed')
        self.attempted = True
        if page.context is not self.context: raise LiveStop('live_setup_failed')
        if self.managed is not None:
            self.managed.attach(self.context, page)

    def finish(self):
        if self.finished: return
        self.finished = True
        # The unchanged candidate snapshots and sends through its private pipe
        # before native close or this comparison-only lifecycle clears state.
        if self.managed is not None:
            try: self.managed.finish(self.context)
            except BaseException: self.cleanup_failed = True
            finally: self.managed = None
        snapshot = original = current = None
        try:
            if self.context is not None:
                if self.baseline is None: raise ValueError('cookie_baseline_unavailable')
                current = self.read_cookie_comparison()
                self.report['session_restore']['cookies_snapshot_observed'] = True
                self.report['session_restore']['cookies_changed'] = current != self.baseline
        except BaseException:
            # An unavailable snapshot is not an unchanged session.
            self.report['session_restore']['cookies_snapshot_observed'] = False
            self.report['session_restore']['cookies_changed'] = None
        finally:
            snapshot = original = current = None
            self.initial_state.clear()
            self.initial_state = self.context = self.baseline = None
            self.cookie_urls.clear(); self.approved_domains.clear()
        if self.account_observer is not None:
            try:
                self.account_observer.close()
                self.report['account_observation'] = sanitize_account_observation(self.account_observer.evidence())
                self.cleanup_failed |= (self.report['account_observation'] is None
                                        or not self.report['account_observation']['listeners_removed'])
            except BaseException:
                self.cleanup_failed = True
        self.report['access_verified'] = bool(self.render_guard_passed
            and map_access_observed(self.report['account_observation']))
        self.report['session_restore']['private_state_released'] = True

    def before_close(self, original):
        def close(resource, *args, **kwargs):
            self.finish()
            return original(resource, *args, **kwargs)
        return close


ACCOUNT_RESULTS = frozenset(('not_seen pending accepted rejected missing_success invalid_success '
    'invalid_json duplicate_success body_unavailable redirected failed ambiguous deadline processing_limit').split())
ACCOUNT_MARKERS = frozenset(('request response finished failed body_begin body_end guard_begin guard_end '
    'navigation:end symbol_selection:end timeframe_selection:begin timeframe_selection:end '
    'timeframe_selection:error timeframe_select:begin timeframe_select:end timeframe_select:error '
    'timeframe_confirmation:end context_close:begin').split())
ACCOUNT_TIMES = ('request_ms', 'response_ms', 'completed_ms', 'failed_ms', 'body_started_ms', 'body_ended_ms')
SITE_CODE_LIMIT = 3 * 1024 * 1024
SITE_CODE_PATH = re.compile(r'^/v1/cg/_next/static/[A-Za-z0-9_./\[\]-]+\.js$')
# Restoration needs no new public-code collection. Keep the prior observer schema.
SITE_CODE_ALLOWED_PATHS = frozenset()
SCRIPT_REJECTIONS = ('origin', 'path', 'query', 'redirect', 'frame', 'method', 'invalid', 'status',
    'mime', 'body_unavailable', 'processing_limit', 'deadline', 'duplicate_asset')
SAFE_MAP_MESSAGES = frozenset(('Please login', 'Please log in', 'Login required', 'login required',
    'Unauthorized', 'Please login first', 'Please log in first', 'User not logged in', 'Not logged in',
    'Please log in to access this feature', 'Token expired', 'token expired', 'Invalid token',
    'invalid token', 'Invalid session', 'invalid session', 'Login expired', 'login expired',
    'fail', 'error', 'Request failed'))
MAP_RESULTS = frozenset(('pending observed missing_success invalid_success duplicate_fields invalid_json '
    'body_unavailable redirected failed deadline processing_limit').split())
MAP_CODE_ENUMS = frozenset(('OK', 'SUCCESS', 'UNAUTHORIZED', 'FORBIDDEN', 'LOGIN_REQUIRED', 'AUTH_REQUIRED', 'ACCESS_DENIED'))
MAP_MESSAGE_CLASSES = {
    'login required': 'login_required', 'please login': 'login_required',
    'please log in': 'login_required', 'not logged in': 'login_required',
    'not login': 'login_required', 'log in to unlock full data': 'login_required',
    '请登录': 'login_required', '请先登录': 'login_required', '未登录': 'login_required',
    '登录失效': 'login_required', '登录已过期': 'login_required',
    'unauthorized': 'access_denied', 'forbidden': 'access_denied', 'access denied': 'access_denied',
    'rate limit exceeded': 'rate_limited', 'too many requests': 'rate_limited',
    'success': 'success', 'ok': 'success',
}


def empty_map_identity():
    # The URL encodes coin/range; do not copy requested model into measured identity.
    return {'state': 'unknown', 'coin': None, 'model': None,
        'model_basis': 'not_encoded', 'timeframe': None, 'interval': None, 'limit': None}


def map_request_identity(url):
    result = empty_map_identity()
    try:
        pairs = parse_qsl(urlsplit(url).query, keep_blank_values=True, max_num_fields=32)
        fields = {key: [value for name, value in pairs if name == key]
                  for key in ('symbol', 'interval', 'limit')}
        if any(len(values) > 1 for values in fields.values()):
            result['state'] = 'ambiguous'; return result
        if any(len(values) != 1 for values in fields.values()): return result
        coin, interval, limit = (fields[key][0] for key in ('symbol', 'interval', 'limit'))
        timeframe = {('5', '144'): '12H', ('5', '288'): '24H', ('15', '192'): '48H'}.get((interval, limit))
        if coin == 'BTC' and timeframe:
            result.update(state='matched', coin='BTC', model=1, model_basis='endpoint_code',
                timeframe=timeframe, interval=int(interval), limit=int(limit))
    except (ValueError, TypeError): pass
    return result


def empty_transport_evidence():
    return {'state': 'unknown', 'source': None, **dict.fromkeys((
        'cookie_header_present', 'obe_header_present', 'obe_cookie_present',
        'obe_header_matches_reference', 'obe_cookie_matches_reference',
        'http_only_cookie_present', 'http_only_cookie_blocked'))}


def sanitize_transport_evidence(value):
    expected = empty_transport_evidence()
    if not isinstance(value, dict) or set(value) != set(expected): return None
    if value['state'] not in ('unknown', 'ambiguous', 'verified'): return None
    if value['source'] not in (None, 'cdp_extra_info'): return None
    flags = set(expected) - {'state', 'source'}
    if any(value[key] is not None and type(value[key]) is not bool for key in flags): return None
    if value['state'] == 'verified':
        if value['source'] != 'cdp_extra_info': return None
    elif value['source'] is not None or any(value[key] is not None for key in flags): return None
    return dict(value)


def empty_web_session():
    return {'requests_seen': 0, 'request_ms': None, 'response_ms': None, 'completed_ms': None,
        'failed_ms': None, 'http_status': None, 'completed': False, 'failed': False,
        'set_cookie_present': None, 'auth': empty_transport_evidence()}


def sanitize_web_session(value):
    if not isinstance(value, dict) or set(value) != set(empty_web_session()): return None
    if type(value['requests_seen']) is not int or not 0 <= value['requests_seen'] <= 2: return None
    if any(value[key] is not None and (type(value[key]) is not int or not 0 <= value[key] <= 60000)
           for key in ('request_ms', 'response_ms', 'completed_ms', 'failed_ms')): return None
    if value['http_status'] is not None and (type(value['http_status']) is not int or not 100 <= value['http_status'] <= 599): return None
    if any(type(value[key]) is not bool for key in ('completed', 'failed')): return None
    if value['set_cookie_present'] is not None and type(value['set_cookie_present']) is not bool: return None
    if sanitize_transport_evidence(value['auth']) is None: return None
    return json.loads(json.dumps(value))


def empty_map_observation():
    return {'eligible_requests': 0, 'ignored_requests': 0, 'truncated': False, 'rows': []}


def empty_site_code_observation():
    return {'eligible_requests': 0, 'body_reads': 0, 'response_failures': 0,
        'selected_files': 0, 'retained_bytes': 0, 'truncated': False, 'status': 'not_seen', 'written': False,
        'rejection_counts': dict.fromkeys(SCRIPT_REJECTIONS, 0)}


def empty_map_row(slot, stamp):
    return {'slot': slot, 'request_ms': stamp, 'response_ms': None, 'completed_ms': None,
        'failed_ms': None, 'http_status': None, 'completed': False, 'failed': False,
        'success': None, 'result': 'pending', 'code': None, 'code_type': 'absent', 'code_field': 'absent',
        'message_category': 'absent', 'message_text': None, 'message_redaction_reason': 'absent',
        'identity': empty_map_identity(), 'auth': empty_transport_evidence(),
        'body_reads': 0, 'whole_body_received': False}


def sanitize_map_observation(value):
    if not isinstance(value, dict) or set(value) != set(empty_map_observation()): return None
    if any(type(value[k]) is not int or not 0 <= value[k] <= 1000 for k in ('eligible_requests', 'ignored_requests')): return None
    if type(value['truncated']) is not bool or not isinstance(value['rows'], list) or len(value['rows']) > 2: return None
    slots = set()
    for row in value['rows']:
        if not isinstance(row, dict) or set(row) != set(empty_map_row('before_selection', 0)): return None
        if row['slot'] not in ('before_selection', 'after_selection') or row['slot'] in slots: return None
        slots.add(row['slot'])
        if any(row[k] is not None and (type(row[k]) is not int or not 0 <= row[k] <= 60000)
               for k in ('request_ms', 'response_ms', 'completed_ms', 'failed_ms')): return None
        if row['request_ms'] is None: return None
        if row['http_status'] is not None and (type(row['http_status']) is not int or not 100 <= row['http_status'] <= 599): return None
        if any(type(row[k]) is not bool for k in ('completed', 'failed', 'whole_body_received')): return None
        if row['success'] is not None and type(row['success']) is not bool: return None
        if row['result'] not in MAP_RESULTS or type(row['body_reads']) is not int or not 0 <= row['body_reads'] <= 1: return None
        if row['code_type'] not in ('absent', 'number', 'numeric_string', 'safe_enum', 'unavailable'): return None
        if row['code_field'] not in ('absent', 'code', 'errorCode', 'error_code', 'multiple', 'ambiguous'): return None
        if row['code_field'] == 'ambiguous' and (row['code'] is not None or row['code_type'] != 'unavailable'): return None
        if row['code_type'] in ('absent', 'unavailable') and row['code'] is not None: return None
        if row['code_type'] in ('number', 'numeric_string') and (type(row['code']) is not int or not -999999 <= row['code'] <= 999999): return None
        if row['code_type'] == 'safe_enum' and row['code'] not in MAP_CODE_ENUMS: return None
        identity = row['identity']
        if not isinstance(identity, dict) or set(identity) != set(empty_map_identity()): return None
        if identity['state'] not in ('unknown', 'ambiguous', 'matched'): return None
        if identity['state'] == 'matched':
            if identity['coin'] != 'BTC' or identity['model'] != 1 or type(identity['model']) is not int or identity['model_basis'] != 'endpoint_code': return None
            if (identity['interval'], identity['limit']) not in ((5, 144), (5, 288), (15, 192)): return None
            if type(identity['interval']) is not int or type(identity['limit']) is not int: return None
            if identity['timeframe'] != {(5, 144): '12H', (5, 288): '24H', (15, 192): '48H'}[(identity['interval'], identity['limit'])]: return None
        elif any(identity[key] is not None for key in ('coin', 'model', 'timeframe', 'interval', 'limit')) or identity['model_basis'] != 'not_encoded': return None
        if sanitize_transport_evidence(row['auth']) is None: return None
        if row['message_text'] is not None and (type(row['message_text']) is not str or row['message_text'] not in SAFE_MAP_MESSAGES): return None
        if row['message_redaction_reason'] not in ('absent', 'none', 'unreviewed_text', 'non_string', 'conflicting_fields'): return None
        if (row['message_text'] is not None) != (row['message_redaction_reason'] == 'none'): return None
        if row['message_category'] not in ('absent', 'other', 'login_required', 'access_denied', 'rate_limited', 'success'): return None
        if row['success'] is not None and (row['result'] != 'observed' or not row['completed'] or row['failed']
                or row['body_reads'] != 1 or not row['whole_body_received']): return None
    return json.loads(json.dumps(value, allow_nan=False))


def map_access_observed(account_observation):
    if not isinstance(account_observation, dict): return False
    maps = sanitize_map_observation(account_observation.get('map_observation'))
    if maps is None or maps['truncated']: return False
    matching = [row for row in maps['rows'] if
        row['identity'].get('state') == 'matched' and row['identity'].get('coin') == 'BTC'
        and row['identity'].get('model') == 1 and row['identity'].get('timeframe') == '12H'
        and row['identity'].get('interval') == 5 and row['identity'].get('limit') == 144]
    if not matching: return False
    latest = max(row['request_ms'] for row in matching)
    rows = [row for row in matching if row['request_ms'] == latest]
    if len(rows) != 1: return False
    row = rows[0]
    if not (row['completed'] and not row['failed'] and type(row['http_status']) is int
            and 200 <= row['http_status'] < 300 and row['body_reads'] == 1
            and row['whole_body_received'] and row['result'] in ('observed', 'missing_success')
            and row['success'] is not False and row['code_field'] != 'ambiguous'
            and row['code_type'] != 'unavailable'
            and row['message_category'] in ('absent', 'success')):
        return False
    code_ok = row['code_type'] in ('number', 'numeric_string') and row['code'] == 0
    explicit_ok = row['success'] is True and row['code_field'] == 'absent'
    return code_ok or explicit_ok


def sanitize_site_code_observation(value):
    if not isinstance(value, dict) or set(value) != set(empty_site_code_observation()): return None
    for key, maximum in {'eligible_requests': 1000, 'body_reads': 64, 'response_failures': 1000,
                         'selected_files': 16, 'retained_bytes': SITE_CODE_LIMIT}.items():
        if type(value[key]) is not int or not 0 <= value[key] <= maximum: return None
    if value['body_reads'] > 3 or value['selected_files'] > 3: return None
    if any(type(value[k]) is not bool for k in ('truncated', 'written')): return None
    if value['status'] not in ('not_seen', 'collected', 'partial', 'unavailable'): return None
    counts = value['rejection_counts']
    if not isinstance(counts, dict) or set(counts) != set(SCRIPT_REJECTIONS): return None
    if any(type(count) is not int or not 0 <= count <= 1000 for count in counts.values()): return None
    return json.loads(json.dumps(value))


def public_site_code_bytes(rows):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 16: raise ValueError('invalid_public_code')
    paths = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'path', 'sha256', 'source'}: raise ValueError('invalid_public_code')
        path, source = row['path'], row['source']
        if (type(path) is not str or len(path) > 512 or not SITE_CODE_PATH.fullmatch(path) or path not in SITE_CODE_ALLOWED_PATHS or '..' in path or path in paths
                or type(source) is not str
                or hashlib.sha256(source.encode('utf-8')).hexdigest() != row['sha256']): raise ValueError('invalid_public_code')
        paths.add(path)
    data = json.dumps({'version': 1, 'files': rows}, ensure_ascii=True, separators=(',', ':')).encode('utf-8')
    if len(data) > SITE_CODE_LIMIT: raise ValueError('public_code_limit')
    return data


def empty_account_observation():
    return {'requests_seen': 0, 'http_status': None, 'completed': False, 'failed': False,
        'failure_kind': None, 'success': None, 'result': 'not_seen', 'body_reads': 0,
        'whole_body_received': False, **{key: None for key in ACCOUNT_TIMES},
        'network_failure_observed': False, 'cors_error_reported': None,
        'events': [], 'ui': [], 'truncated': False, 'closed': False, 'listeners_removed': False,
        'map_observation': empty_map_observation(), 'site_code': empty_site_code_observation(),
        'reference_legacy_session_format': None, 'web_session': empty_web_session()}


def sanitize_account_observation(value):
    template = empty_account_observation()
    if not isinstance(value, dict) or set(value) != set(template): return None
    if sanitize_map_observation(value['map_observation']) is None or sanitize_site_code_observation(value['site_code']) is None: return None
    if value['reference_legacy_session_format'] is not None and type(value['reference_legacy_session_format']) is not bool: return None
    if sanitize_web_session(value['web_session']) is None: return None
    def stamp(v): return v is None or type(v) is int and 0 <= v <= 60000
    def flag(v): return v is None or type(v) is bool
    if type(value['requests_seen']) is not int or not 0 <= value['requests_seen'] <= 2: return None
    if type(value['body_reads']) is not int or not 0 <= value['body_reads'] <= 1: return None
    if value['http_status'] is not None and (type(value['http_status']) is not int or not 100 <= value['http_status'] <= 599): return None
    if any(type(value[k]) is not bool for k in ('completed', 'failed', 'whole_body_received', 'network_failure_observed', 'truncated', 'closed', 'listeners_removed')): return None
    if not flag(value['cors_error_reported']): return None
    if value['failure_kind'] not in (None, 'timeout', 'connection', 'cancelled', 'blocked', 'other'): return None
    if not flag(value['success']) or value['result'] not in ACCOUNT_RESULTS: return None
    if any(not stamp(value[k]) for k in ACCOUNT_TIMES): return None
    if value['result'] in ('accepted', 'rejected'):
        if (value['success'] is not (value['result'] == 'accepted') or value['requests_seen'] != 1
                or not value['completed'] or value['failed'] or value['body_reads'] != 1
                or not value['whole_body_received'] or value['body_ended_ms'] is None): return None
    elif value['success'] is not None: return None
    if not isinstance(value['events'], list) or len(value['events']) > 48: return None
    for row in value['events']:
        if not isinstance(row, dict) or set(row) != {'marker', 'ms', 'phase', 'completed', 'success', 'outcome', 'selection'}: return None
        if row['marker'] not in ACCOUNT_MARKERS or row['phase'] not in LIVE_PHASES or row['ms'] is None or not stamp(row['ms']): return None
        if type(row['completed']) is not bool or not flag(row['success']): return None
        if row['outcome'] not in (None, 'no_exception', 'login_required', 'other_error'): return None
        if row['selection'] not in ('not_started', 'in_progress', 'returned', 'failed'): return None
    if not isinstance(value['ui'], list) or len(value['ui']) > 12: return None
    for row in value['ui']:
        if not isinstance(row, dict) or set(row) != {'marker', 'started_ms', 'observed_ms', 'state', 'login_visible', 'account_visible', 'login_gate'}: return None
        if row['marker'] not in ACCOUNT_MARKERS or any(row[k] is None or not stamp(row[k]) for k in ('started_ms', 'observed_ms')): return None
        if row['state'] not in ('authenticated_ui', 'guest_ui', 'unknown'): return None
        if any(not flag(row[k]) for k in ('login_visible', 'account_visible', 'login_gate')): return None
    return json.loads(json.dumps(value, allow_nan=False))


class AccountResponseObservation:
    """Observe one existing account response; never fetch, replay or alter it.

    response.body() returns the FULL body to RAM. json.loads transiently parses
    the JSON envelope, including nested objects. Only outer success is accessed;
    data/profile are never inspected or decrypted. The post-read processing cap
    is not a download/memory bound. All response locals are discarded afterward.
    """
    def __init__(self, page, deadline, login_gate_script, *, clock=time.monotonic):
        self.page, self.deadline, self.clock = page, deadline, clock
        self.started = clock()
        self.row = empty_account_observation()
        self.request = self.response = None
        self.map_requests, self.site_requests, self.site_sources = {}, {}, []
        self.transport = None
        self.transport_listeners = []
        self.transport_records, self.transport_urls = {}, {}
        self.transport_limited = False
        self.reference = self.decoder = self.session_request = None
        self.phase, self.selection = 'page_creation', 'not_started'
        self.listeners = [('request', self.request_seen), ('response', self.response_seen),
                          ('requestfinished', self.finished_seen), ('requestfailed', self.failed_seen)]
        self.ui_script = r'''() => { const visible=e=>{const r=e.getBoundingClientRect();
          if(r.width<=0||r.height<=0)return false;for(let n=e;n;n=n.parentElement){const s=getComputedStyle(n);
          if(s.display==='none'||s.visibility==='hidden'||Number(s.opacity||1)===0)return false;}return true;};
          return {login_visible:[...document.querySelectorAll('a[href="/login"]')].some(visible),
          account_visible:[...document.querySelectorAll('a[href="/account"]')].some(visible),
          login_gate:(''' + login_gate_script + ')()};}'
        for name, callback in self.listeners: page.on(name, callback)

    def attach_transport(self, experiment, decoder=None):
        # Reuse the existing session and decoder, with no new CDP connection or request.
        self.transport, self.decoder = experiment, decoder
        self.reference = getattr(experiment, '_value', None)
        self.row['reference_legacy_session_format'] = (bool(re.fullmatch(r's_[0-9a-f]{32}', self.reference))
            if type(self.reference) is str else None)
        for name, callback in (('Network.loadingFailed', self.network_failed),
                ('Network.requestWillBeSent', self.transport_request),
                ('Network.requestWillBeSentExtraInfo', self.transport_extra)):
            experiment._session.on(name, callback)
            self.transport_listeners.append((name, callback))

    @staticmethod
    def credential_target(url, method):
        try:
            parsed = urlsplit(url)
            if not (parsed.scheme == 'https' and parsed.port in (None, 443)
                    and parsed.username is None and parsed.password is None and not parsed.fragment): return None
            if (method == 'GET' and parsed.hostname == 'capi.coinglass.com'
                    and parsed.path == '/api/index/aggregate/liqHeatMap'): return 'map'
            if (method == 'POST' and parsed.hostname == 'fapi.coinglass.com'
                    and parsed.path == '/coin-community/api/user/web/session' and not parsed.query): return 'session'
        except (ValueError, TypeError): pass
        return None

    def transport_request(self, event):
        if self.row['closed'] or self.transport is None: return
        try:
            network_id = event.get('requestId')
            if not isinstance(network_id, str) or len(network_id) > 128: return
            previous = self.transport_records.get(network_id)
            if previous is not None:
                previous['ambiguous'] = True; return
            request = event.get('request', {})
            kind = self.credential_target(request.get('url'), request.get('method'))
            if kind is None: return
            if len(request['url']) > 8192: self.transport_limited = True; return
            if len(self.transport_records) >= 32: self.transport_limited = True; return
            trusted = (event.get('frameId') == getattr(self.transport, '_frame_id', None)
                and event.get('frameId') is not None and event.get('type') in ('XHR', 'Fetch')
                and event.get('initiator', {}).get('type') == 'script')
            self.transport_records[network_id] = {'kind': kind, 'url': request['url'],
                'ambiguous': not trusted or 'redirectResponse' in event, 'extra': []}
        except Exception: self.transport_limited = True

    def transport_extra(self, event):
        if self.row['closed'] or self.transport is None: return
        # Unknown IDs are ignored, not buffered: never inspect unrelated headers.
        record = self.transport_records.get(event.get('requestId'))
        if record is None or record['ambiguous']: return
        if record['extra']:
            record['ambiguous'] = True; record['extra'].clear(); return
        evidence = empty_transport_evidence()
        headers = event.get('headers')
        if not isinstance(headers, dict): return
        try:
            values = {key: [value for name, value in headers.items()
                if isinstance(name, str) and name.lower() == key] for key in ('obe', 'cookie')}
            if any(len(items) > 1 for items in values.values()): record['ambiguous'] = True; return
            if any(items and type(items[0]) is not str for items in values.values()): return
            header = values['obe'][0] if values['obe'] else None
            cookie = values['cookie'][0] if values['cookie'] else None
            evidence.update(obe_header_present=bool(values['obe']), cookie_header_present=bool(values['cookie']))
            def matches(value):
                if type(value) is not str or type(self.reference) is not str: return None
                return bool(value.isascii() and self.reference.isascii() and hmac.compare_digest(value, self.reference))
            if header is not None: evidence['obe_header_matches_reference'] = matches(header)
            if cookie is None: evidence['obe_cookie_present'] = False
            elif len(cookie) <= 65536:
                names = [part.strip().partition('=') for part in cookie.split(';')]
                count = sum(1 for name, separator, _ in names if separator and name.strip() == 'obe')
                evidence['obe_cookie_present'] = count > 0
                if count == 1 and self.decoder is not None:
                    evidence['obe_cookie_matches_reference'] = matches(self.decoder({'COINGLASS_COOKIE_HEADER': cookie}))
            associated = event.get('associatedCookies')
            if isinstance(associated, list) and len(associated) <= 128:
                # Candidate-cookie metadata only; blocked does not mean sent.
                flags = [(item.get('cookie', {}).get('httpOnly'), item.get('blockedReasons'))
                    for item in associated if isinstance(item, dict) and isinstance(item.get('cookie'), dict)]
                if len(flags) == len(associated) and all(type(flag) is bool and isinstance(blocks, list) for flag, blocks in flags):
                    evidence['http_only_cookie_present'] = any(flag for flag, _ in flags)
                    evidence['http_only_cookie_blocked'] = any(flag and bool(blocks) for flag, blocks in flags)
            evidence.update(state='verified', source='cdp_extra_info')
            record['extra'].append(evidence)
        except Exception:
            record['ambiguous'] = True; record['extra'].clear()

    def register_transport_request(self, kind, request):
        key = (kind, request.url)
        if key not in self.transport_urls and len(self.transport_urls) >= 32:
            self.transport_limited = True; return
        self.transport_urls[key] = min(2, self.transport_urls.get(key, 0) + 1)

    def transport_evidence(self, kind, request):
        result = empty_transport_evidence()
        if request is None or self.transport is None: return result
        key = (kind, request.url)
        records = [(network_id, row) for network_id, row in self.transport_records.items()
            if (row['kind'], row['url']) == key]
        if self.transport_urls.get(key) != 1 or len(records) > 1:
            result['state'] = 'ambiguous'; return result
        if not records or self.transport_limited: return result
        network_id, row = records[0]
        original = self.transport._records.get(network_id)
        if (row['ambiguous'] or request.redirected_to is not None or not original
                or original.get('hop_count') != 1 or original.get('redirected')
                or not original.get('terminal') or len(row['extra']) != 1): return result
        return dict(row['extra'][0])

    def script_rejection(self, reason):
        counts = self.row['site_code']['rejection_counts']
        counts[reason] = min(1000, counts[reason] + 1)

    def script_request_rejection(self, request):
        if request.resource_type != 'script': return None
        try:
            parsed, frame = urlsplit(request.url), urlsplit(request.frame.url)
            if request.method != 'GET': return 'method'
            if request.redirected_from is not None: return 'redirect'
            if not (request.frame == self.page.main_frame and frame.scheme == 'https'
                and frame.hostname == 'www.coinglass.com' and frame.port in (None, 443)
                and frame.username is None and frame.password is None): return 'frame'
            if not (parsed.scheme == 'https' and parsed.hostname == 's3.coinglass.com'
                and parsed.port in (None, 443) and parsed.username is None and parsed.password is None): return 'origin'
            if parsed.query or parsed.fragment: return 'query'
            if parsed.path not in SITE_CODE_ALLOWED_PATHS: return 'path'
        except Exception: return 'invalid'
        return None

    def network_failed(self, event):
        if self.row['closed'] or self.transport is None or self.row['requests_seen'] > 1: return
        record = self.transport._records.get(event.get('requestId'))
        if not (record and record['original_target'] and record['page_request']
                and record['hop_count'] == 1 and not record['redirected']): return
        self.row.update(network_failure_observed=True,
                        cors_error_reported=isinstance(event.get('corsErrorStatus'), dict))

    def ms(self): return max(0, min(60000, round((self.clock() - self.started) * 1000)))

    def target(self, request):
        try:
            parsed, frame = urlsplit(request.url), urlsplit(request.frame.url)
            return (parsed.scheme == 'https' and parsed.hostname == 'capi.coinglass.com'
                and parsed.port in (None, 443) and parsed.username is None and parsed.password is None
                and parsed.path == '/coin-community/api/userapi/info' and not parsed.fragment
                and request.method == 'GET' and request.resource_type in ('xhr', 'fetch')
                and request.redirected_from is None and request.frame == self.page.main_frame
                and frame.scheme == 'https' and frame.hostname == 'www.coinglass.com'
                and frame.port in (None, 443) and frame.username is None and frame.password is None)
        except Exception: return False

    def additional_target(self, request):
        try:
            parsed, frame = urlsplit(request.url), urlsplit(request.frame.url)
            if not (parsed.scheme == 'https' and parsed.port in (None, 443)
                    and parsed.username is None and parsed.password is None and not parsed.fragment
                    and request.redirected_from is None and request.frame == self.page.main_frame
                    and frame.scheme == 'https' and frame.hostname == 'www.coinglass.com'
                    and frame.port in (None, 443) and frame.username is None and frame.password is None): return None
            kind = self.credential_target(request.url, request.method)
            if kind is not None and request.resource_type in ('xhr', 'fetch'): return kind
            if (request.resource_type == 'script' and self.script_request_rejection(request) is None): return 'script'
        except Exception: pass
        return None

    def additional_request(self, request):
        kind = self.additional_target(request)
        if request.resource_type == 'script' and kind != 'script':
            self.script_rejection(self.script_request_rejection(request) or 'invalid')
        if kind in ('map', 'session'): self.register_transport_request(kind, request)
        if kind == 'session':
            row = self.row['web_session']
            row['requests_seen'] = min(2, row['requests_seen'] + 1)
            if self.session_request is None:
                self.session_request = request; row['request_ms'] = self.ms()
            return
        if kind == 'map':
            if id(request) in self.map_requests: return
            observation = self.row['map_observation']
            observation['eligible_requests'] = min(1000, observation['eligible_requests'] + 1)
            slot = 'before_selection' if self.selection == 'not_started' else 'after_selection'
            if any(row['slot'] == slot for row in observation['rows']):
                observation['ignored_requests'] = min(1000, observation['ignored_requests'] + 1)
                observation['truncated'] = True; return
            row = empty_map_row(slot, self.ms())
            row['identity'] = map_request_identity(request.url)
            observation['rows'].append(row)
            self.map_requests[id(request)] = [request, None, row]
        elif kind == 'script':
            if id(request) in self.site_requests: return
            observation = self.row['site_code']
            observation['eligible_requests'] = min(1000, observation['eligible_requests'] + 1)
            if len(self.site_requests) >= 64:
                observation['truncated'] = True; return
            self.site_requests[id(request)] = [request, None, False]

    def additional_response(self, response):
        request = response.request
        if request is self.session_request:
            row = self.row['web_session']; status = response.status
            row.update(response_ms=self.ms(), http_status=status if type(status) is int and 100 <= status <= 599 else None)
            # Inspect presence only. Never read the session body, value or account data.
            try:
                value = response.header_value('set-cookie')
                row['set_cookie_present'] = value is not None
                value = None
            except Exception: row['set_cookie_present'] = None
        item = self.map_requests.get(id(request))
        if item is not None and item[0] is request:
            item[1] = response
            status = response.status
            item[2].update(response_ms=self.ms(), http_status=status if type(status) is int and 100 <= status <= 599 else None)
        item = self.site_requests.get(id(request))
        if item is not None and item[0] is request: item[1] = response

    def additional_failed(self, request):
        if request is self.session_request: self.row['web_session'].update(failed=True, failed_ms=self.ms())
        item = self.map_requests.get(id(request))
        if item is not None and item[0] is request:
            item[1] = None
            item[2].update(failed=True, failed_ms=self.ms(), success=None, result='failed')
        item = self.site_requests.get(id(request))
        if item is not None and item[0] is request:
            item[1] = None; item[2] = True
            code = self.row['site_code']; code['response_failures'] = min(1000, code['response_failures'] + 1)

    def map_finished(self, item):
        request, response, row = item
        if row['completed'] or row['failed']: return
        row.update(completed=True, completed_ms=self.ms())
        if row['body_reads'] or row['failed']: return
        if request.redirected_to is not None or row['http_status'] is not None and 300 <= row['http_status'] <= 399:
            row['result'] = 'redirected'; item[1] = None; return
        if response is None: row['result'] = 'body_unavailable'; return
        if self.clock() >= self.deadline: row['result'] = 'deadline'; item[1] = None; return
        body = payload = fields = success = codes = code = code_fields = messages = message = None
        class Envelope(list): pass
        row['body_reads'] = 1
        try:
            # Existing browser response only. Full bytes enter RAM; no decryption,
            # data traversal, independent request or response replacement occurs.
            body = response.body(); row['whole_body_received'] = isinstance(body, bytes)
            if row['failed']: row.update(success=None, result='failed'); return
            if not isinstance(body, bytes): row['result'] = 'body_unavailable'; return
            if len(body) > 2 * 1024 * 1024: row['result'] = 'processing_limit'; return
            try: payload = json.loads(body, object_pairs_hook=Envelope)
            except (ValueError, UnicodeError, RecursionError): row['result'] = 'invalid_json'; return
            if self.clock() >= self.deadline: row['result'] = 'deadline'; return
            if not isinstance(payload, Envelope): row['result'] = 'invalid_json'; return
            wanted = ('success', 'code', 'errorCode', 'error_code', 'msg', 'message', 'error')
            fields = {name: [value for key, value in payload if key == name] for name in wanted}
            if any(len(values) > 1 for values in fields.values()): row['result'] = 'duplicate_fields'; return
            success = fields['success']
            row['result'] = 'missing_success' if not success else 'observed' if type(success[0]) is bool else 'invalid_success'
            if row['result'] == 'observed': row['success'] = success[0]
            code_fields = [name for name in ('code', 'errorCode', 'error_code') if fields[name]]
            codes = [fields[name][0] for name in code_fields]
            if codes:
                code = codes[0]
                row['code_type'] = 'unavailable'
                row['code_field'] = code_fields[0] if len(code_fields) == 1 else 'multiple'
                if len(codes) > 1 and any(type(other) not in (int, str) for other in codes): row['code_field'] = 'ambiguous'
                elif any(type(other) is not type(code) or other != code for other in codes[1:]): row['code_field'] = 'ambiguous'
                elif type(code) is int and -999999 <= code <= 999999: row.update(code=code, code_type='number')
                elif type(code) is str and re.fullmatch(r'-?\d{1,6}', code): row.update(code=int(code), code_type='numeric_string')
                elif type(code) is str and code in MAP_CODE_ENUMS: row.update(code=code, code_type='safe_enum')
            messages = [fields[name][0] for name in ('msg', 'message', 'error') if fields[name]]
            if messages:
                message = messages[0]
                if any(type(value) is not str for value in messages): row['message_redaction_reason'] = 'non_string'
                elif any(value != message for value in messages[1:]): row['message_redaction_reason'] = 'conflicting_fields'
                elif message in SAFE_MAP_MESSAGES: row.update(message_text=message, message_redaction_reason='none')
                else: row['message_redaction_reason'] = 'unreviewed_text'
                row['message_category'] = MAP_MESSAGE_CLASSES.get(message.strip().lower(), 'other') if type(message) is str and len(message) <= 100 else 'other'
        except Exception: row.update(success=None, result='body_unavailable')
        finally:
            body = payload = fields = success = codes = code = code_fields = messages = message = None
            item[1] = None

    def script_finished(self, item):
        request, response, finished = item
        if finished: return
        item[2] = True
        code = self.row['site_code']
        if response is None or request.redirected_to is not None or response.status != 200:
            self.script_rejection('redirect' if request.redirected_to is not None else 'status')
            code['response_failures'] = min(1000, code['response_failures'] + 1); item[1] = None; return
        if any(row['path'] == urlsplit(request.url).path for row in self.site_sources):
            self.script_rejection('duplicate_asset'); item[1] = None; return
        if code['body_reads'] >= 3 or self.clock() >= self.deadline:
            self.script_rejection('deadline' if self.clock() >= self.deadline else 'processing_limit')
            code['truncated'] = True; item[1] = None; return
        body = source = candidate = encoded = None
        try:
            mime = response.header_value('content-type')
            if (type(mime) is not str or mime.split(';', 1)[0].strip().lower() not in
                    ('application/javascript', 'text/javascript', 'application/ecmascript', 'text/ecmascript')):
                self.script_rejection('mime')
                code['response_failures'] = min(1000, code['response_failures'] + 1); return
            if self.clock() >= self.deadline:
                self.script_rejection('deadline'); code['truncated'] = True; return
            code['body_reads'] += 1
            body = response.body()
            if not isinstance(body, bytes) or len(body) > SITE_CODE_LIMIT:
                self.script_rejection('processing_limit' if isinstance(body, bytes) else 'body_unavailable')
                code['truncated'] = True; return
            source = body.decode('utf-8')
            if source.lstrip().startswith('<'):
                self.script_rejection('mime')
                code['response_failures'] = min(1000, code['response_failures'] + 1); return
            path = urlsplit(request.url).path
            if any(row['path'] == path for row in self.site_sources):
                self.script_rejection('duplicate_asset'); return
            candidate = {'path': path, 'sha256': hashlib.sha256(source.encode('utf-8')).hexdigest(), 'source': source}
            if len(self.site_sources) >= 3:
                self.script_rejection('processing_limit'); code['truncated'] = True; return
            try: encoded = public_site_code_bytes(self.site_sources + [candidate])
            except ValueError:
                self.script_rejection('processing_limit'); code['truncated'] = True; return
            self.site_sources.append(candidate)
            code.update(selected_files=len(self.site_sources), retained_bytes=len(encoded))
        except Exception:
            self.script_rejection('body_unavailable')
            code['response_failures'] = min(1000, code['response_failures'] + 1)
        finally:
            body = source = candidate = encoded = None
            item[1] = None

    def additional_finished(self, request):
        if request is self.session_request: self.row['web_session'].update(completed=True, completed_ms=self.ms())
        item = self.map_requests.get(id(request))
        if item is not None and item[0] is request: self.map_finished(item)
        item = self.site_requests.get(id(request))
        if item is not None and item[0] is request: self.script_finished(item)

    def event(self, marker, *, outcome=None, ui=False):
        if self.row['closed'] or marker not in ACCOUNT_MARKERS: return
        if len(self.row['events']) < 48:
            self.row['events'].append({'marker': marker, 'ms': self.ms(), 'phase': self.phase,
                'completed': self.row['completed'], 'success': self.row['success'],
                'outcome': outcome, 'selection': self.selection})
        else: self.row['truncated'] = True
        if not ui: return
        if len(self.row['ui']) >= 12 or self.clock() >= self.deadline:
            self.row['truncated'] = True; return
        began = self.ms()
        values = dict.fromkeys(('login_visible', 'account_visible', 'login_gate'))
        try:
            measured = self.page.evaluate(self.ui_script)
            if isinstance(measured, dict):
                values = {key: measured.get(key) if type(measured.get(key)) is bool else None for key in values}
        except Exception: pass
        state = ('authenticated_ui' if values['account_visible'] is True and values['login_visible'] is False
                 else 'guest_ui' if values['login_visible'] is True and values['account_visible'] is False else 'unknown')
        self.row['ui'].append({'marker': marker, 'started_ms': began, 'observed_ms': self.ms(), 'state': state, **values})

    def operation(self, phase, edge):
        self.phase = phase if phase in LIVE_PHASES else 'capture'
        marker = phase + ':' + edge
        if phase == 'timeframe_select':
            self.selection = {'begin': 'in_progress', 'end': 'returned', 'error': 'failed'}[edge]
        self.event(marker, ui=marker in ACCOUNT_MARKERS and phase != 'context_close')

    def guard(self, original):
        def observed(page, model, *, phase):
            self.phase = phase if phase in LIVE_PHASES else 'capture'
            self.event('guard_begin')
            try: result = original(page, model, phase=phase)
            except BaseException as error:
                self.event('guard_end', outcome='login_required' if getattr(error, 'code', None) == 'source_login_required' else 'other_error', ui=True)
                raise
            self.event('guard_end', outcome='no_exception', ui=True)
            return result
        return observed

    def request_seen(self, request):
        if self.row['closed']: return
        self.additional_request(request)
        if request is self.request or not self.target(request): return
        self.row['requests_seen'] = min(2, self.row['requests_seen'] + 1)
        if self.request is not None:
            self.row.update(success=None, result='ambiguous', cors_error_reported=None); return
        self.request = request
        self.row.update(request_ms=self.ms(), result='pending')
        self.event('request')

    def response_seen(self, response):
        if self.row['closed']: return
        self.additional_response(response)
        if response.request is not self.request: return
        self.response = response
        status = response.status
        self.row.update(response_ms=self.ms(), http_status=status if type(status) is int and 100 <= status <= 599 else None)
        self.event('response')

    def failed_seen(self, request):
        if self.row['closed']: return
        self.additional_failed(request)
        if request is not self.request: return
        kind = {'net::ERR_TIMED_OUT': 'timeout', 'net::ERR_CONNECTION_TIMED_OUT': 'timeout',
            'net::ERR_CONNECTION_RESET': 'connection', 'net::ERR_CONNECTION_REFUSED': 'connection',
            'net::ERR_NAME_NOT_RESOLVED': 'connection', 'net::ERR_ABORTED': 'cancelled',
            'net::ERR_BLOCKED_BY_CLIENT': 'blocked'}.get(request.failure, 'other')
        # ERR_FAILED alone does not prove CORS or an authentication rejection.
        self.row.update(failed=True, failed_ms=self.ms(), failure_kind=kind, success=None,
                        result='ambiguous' if self.row['requests_seen'] > 1 else 'failed')
        self.event('failed')

    def finished_seen(self, request):
        if self.row['closed']: return
        self.additional_finished(request)
        if request is not self.request: return
        self.row.update(completed=True, completed_ms=self.ms()); self.event('finished')
        if self.row['requests_seen'] > 1 or self.row['failed'] or self.row['body_reads']: return
        if request.redirected_to is not None or self.row['http_status'] is not None and 300 <= self.row['http_status'] <= 399:
            self.row.update(result='redirected', success=None); return
        if self.response is None: self.row['result'] = 'body_unavailable'; return
        if self.clock() >= self.deadline: self.row['result'] = 'deadline'; return
        body = payload = success = outer_success = None
        self.row.update(body_reads=1, body_started_ms=self.ms()); self.event('body_begin')
        class Envelope(list): pass
        try:
            body = self.response.body()
            self.row['whole_body_received'] = isinstance(body, bytes)
            if not isinstance(body, bytes): self.row['result'] = 'body_unavailable'; return
            if len(body) > 64 * 1024: self.row['result'] = 'processing_limit'; return
            try: payload = json.loads(body, object_pairs_hook=Envelope)
            except (ValueError, UnicodeError, RecursionError): self.row['result'] = 'invalid_json'; return
            if self.row['requests_seen'] > 1: self.row.update(success=None, result='ambiguous'); return
            if self.row['failed']: self.row.update(success=None, result='failed'); return
            if self.clock() >= self.deadline: self.row['result'] = 'deadline'; return
            if not isinstance(payload, Envelope): self.row['result'] = 'invalid_json'; return
            # Inspect root pairs only. Nested data/profile objects are merely
            # transient JSON containers; their keys/values are never traversed.
            outer_success = [value for key, value in payload if key == 'success']
            if len(outer_success) > 1: self.row['result'] = 'duplicate_success'; return
            if not outer_success: self.row['result'] = 'missing_success'; return
            success = outer_success[0]
            if type(success) is not bool: self.row['result'] = 'invalid_success'; return
            self.row.update(success=success, result='accepted' if success else 'rejected')
        except Exception:
            self.row.update(success=None, result='body_unavailable')
        finally:
            body = payload = success = outer_success = None
            self.response = None
            self.row['body_ended_ms'] = self.ms(); self.event('body_end')

    def close(self):
        if self.row['closed']: return
        self.row['closed'] = True
        removed = True
        if self.transport is not None:
            for item in self.map_requests.values(): item[2]['auth'] = self.transport_evidence('map', item[0])
            self.row['web_session']['auth'] = self.transport_evidence('session', self.session_request)
            for name, callback in self.transport_listeners:
                try: self.transport._session.remove_listener(name, callback)
                except Exception: removed = False
            self.transport = None
        self.transport_listeners.clear(); self.transport_records.clear(); self.transport_urls.clear()
        self.reference = self.decoder = self.session_request = None
        for name, callback in self.listeners:
            try: self.page.remove_listener(name, callback)
            except Exception: removed = False
        self.listeners.clear(); self.request = self.response = None
        code = self.row['site_code']
        if any(not item[2] for item in self.site_requests.values()): code['truncated'] = True
        self.map_requests.clear(); self.site_requests.clear()
        code['status'] = ('partial' if code['selected_files'] and (code['truncated'] or code['response_failures'])
                          else 'collected' if code['selected_files'] else 'unavailable' if code['eligible_requests'] else 'not_seen')
        self.row['listeners_removed'] = removed

    def evidence(self): return sanitize_account_observation(self.row)


def filtered_hover(snapshot):
    """Project already-validated map data; never serialize tooltip/account text."""
    def number(value, minimum=0, maximum=1e18):
        if type(value) not in (int, float) or not math.isfinite(value) or not minimum <= value < maximum:
            raise ValueError('invalid_map_number')
        return value
    def stamp(value):
        if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value):
            raise ValueError('invalid_map_time')
        return value
    if (not isinstance(snapshot, dict) or snapshot.get('complete') is not True
            or snapshot.get('heatmap_model') != 1 or snapshot.get('timeframe') != '12H'
            or snapshot.get('version') != 'displayed-hover.v1'
            or snapshot.get('metric') != 'Liquidation Leverage'
            or snapshot.get('scope') != 'displayed_latest_column'
            or snapshot.get('unit') not in ('USD', 'source_metric')):
        raise ValueError('invalid_map_identity')
    result = {k: snapshot[k] for k in ('version', 'metric', 'scope', 'heatmap_model', 'timeframe', 'complete', 'unit')}
    result['source_time'] = stamp(snapshot['source_time'])
    for key in ('price_step', 'total_value', 'rounding_error', 'row_height_pixels', 'duration_seconds'):
        result[key] = number(snapshot[key])
    reads = snapshot['tooltip_reads']
    if type(reads) is not int or not 0 <= reads <= 100000: raise ValueError('invalid_map_reads')
    result['tooltip_reads'] = reads
    bounds = snapshot['plot_bounds']
    if not isinstance(bounds, dict) or set(bounds) != {'left', 'right', 'top', 'bottom'}:
        raise ValueError('invalid_map_bounds')
    result['plot_bounds'] = {key: number(value, maximum=20000) for key, value in bounds.items()}
    if not (bounds['left'] < bounds['right'] and bounds['top'] < bounds['bottom']): raise ValueError('invalid_map_bounds')
    candle = snapshot['source_candle']
    result['source_candle'] = {key: number(candle[key], maximum=1e12) for key in ('open', 'high', 'low', 'close')}
    result['source_candle']['source_time'] = stamp(candle['source_time'])
    rows = snapshot['rows']
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1600: raise ValueError('invalid_map_rows')
    result['rows'] = []
    for index, row in enumerate(rows):
        if row.get('id') != f'r{index}' or row.get('unit') != result['unit'] or row.get('source_time') != result['source_time']:
            raise ValueError('invalid_map_row_identity')
        result['rows'].append({'id': f'r{index}', 'unit': result['unit'], 'source_time': result['source_time'],
            **{key: number(row[key]) for key in ('price', 'value', 'rounding_error')},
            'y_fraction': number(row['y_fraction'], maximum=1.000001)})
    return result


def retain_live_artifacts(output, report, snapshot, geometry, site_sources=None):
    """Delete profiles/raw output; retain only an approved map crop and projection."""
    if output is None: return
    retained = set()
    report['artifact_status'] = 'unavailable'
    try:
        if report['status'] == 'passed' and report['native_hover_validated'] and report['png_validated']:
            data = filtered_hover(snapshot)
            if (not isinstance(geometry, dict) or any(type(geometry.get(k)) not in (int, float)
                    or not math.isfinite(geometry[k]) for k in ('x', 'y', 'width', 'height', 'page_width'))):
                raise ValueError('map_geometry_unavailable')
            from PIL import Image
            source = output / 'coinglass_btc_heatmap_12h.png'
            if source.is_symlink(): raise ValueError('invalid_artifact_path')
            bounds = data['plot_bounds']
            with Image.open(source) as image:
                if (image.format != 'PNG' or geometry['page_width'] != image.width
                        or not 0 <= geometry['x'] < geometry['x'] + geometry['width'] <= image.width
                        or not 0 <= geometry['y'] < geometry['y'] + geometry['height'] <= image.height
                        or not geometry['x'] <= bounds['left'] < bounds['right'] <= geometry['x'] + geometry['width']
                        or not geometry['y'] <= bounds['top'] < bounds['bottom'] <= geometry['y'] + geometry['height']):
                    raise ValueError('map_geometry_unavailable')
                # Inner integer bounds cannot include pixels outside the proven plot.
                box = (math.ceil(bounds['left']), math.ceil(bounds['top']), math.floor(bounds['right']), math.floor(bounds['bottom']))
                if box[0] >= box[2] or box[1] >= box[3]: raise ValueError('map_geometry_unavailable')
                with image.crop(box) as cropped:
                    cropped.info.clear()
                    cropped.save(output / 'map.png', format='PNG')
            (output / 'hover.json').write_text(json.dumps(data, allow_nan=False, separators=(',', ':')), encoding='utf8')
            for name in ('map.png', 'hover.json'):
                (output / name).chmod(0o600)
            report['map_png_sha256'] = hashlib.sha256((output / 'map.png').read_bytes()).hexdigest()
            report['hover_sha256'] = hashlib.sha256((output / 'hover.json').read_bytes()).hexdigest()
            retained = {'map.png', 'hover.json'}
            report['artifact_status'] = 'retained'
    except Exception:
        # Capture success and artifact availability are separate observations.
        report['map_png_sha256'] = report['hover_sha256'] = None
    finally:
        try:
            if site_sources:
                try:
                    public = public_site_code_bytes(site_sources)
                    path = output / 'site_code.json'
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                    with os.fdopen(fd, 'wb') as stream: stream.write(public)
                    report['site_code_sha256'] = hashlib.sha256(public).hexdigest()
                    report['account_observation']['site_code']['written'] = True
                    retained.add('site_code.json')
                except Exception:
                    report['site_code_sha256'] = None
                    if report['account_observation'] is not None:
                        report['account_observation']['site_code'].update(written=False, status='unavailable')
                finally:
                    public = None
                    site_sources.clear()
            for path in output.iterdir():
                if path.name in retained and path.is_file() and not path.is_symlink(): continue
                if path.is_dir() and not path.is_symlink(): shutil.rmtree(path)
                else: path.unlink()
            report['private_output_deleted'] = {path.name for path in output.iterdir()} == retained
            report['png_deleted'] = 'map.png' not in retained
            report['artifact_files'] = ['map.png', 'hover.json', 'receipt.json'] if 'map.png' in retained else ['receipt.json']
            if 'site_code.json' in retained: report['artifact_files'].append('site_code.json')
        except Exception:
            report.update(status='failed', reason='ephemeral_cleanup_failed', artifact_status='cleanup_failed')
            report['artifact_files'] = []
            report['site_code_sha256'] = None
            report['private_output_deleted'] = False


class LiveGuard:
    """Trial envelope only. It never accepts data or changes reader thresholds."""
    def __init__(self, report, deadline, access_check):
        self.report, self.deadline, self.access_check = report, deadline, access_check
        self.navigation_requests = 0

    def check(self):
        if time.monotonic() >= self.deadline: raise LiveStop('work_deadline')
        if self.report['blocked_navigation_requests']: raise LiveStop('second_navigation_forbidden')

    def invoke(self, action, original, *args, **kwargs):
        self.check()
        if action == 'reload': raise LiveStop('reload_forbidden')
        if action == 'retry': raise LiveStop('retry_forbidden')
        if kwargs.get('force'): raise LiveStop('forced_click_forbidden')
        if action == 'goto':
            if self.report['navigation_calls']: raise LiveStop('second_navigation_forbidden')
            self.report['navigation_calls'] += 1
        else:
            self.access_check()
        result = original(*args, **kwargs)
        self.check()
        if action != 'goto': self.access_check()
        return result


class FixtureFailure(Exception):
    pass


def check(condition, code):
    if not condition:
        raise FixtureFailure(code)


def apply_browser_close_check(report):
    """Keep object-state observations separate from the supervisor's process proof."""
    if report['browsers_disconnected']:
        report['browser_close_verification'] = 'playwright_object_disconnected'
        return
    # This exact injection occurs before capture's explicit try/finally. Its
    # sync_playwright manager has already unwound, so the cached Browser flag
    # is not an authoritative process-liveness observation. Preserve that flag;
    # only the unchanged parent census can accept cleanup for this case.
    expected_setup_failure = (
        report.get('case') == 'failure' and report.get('status') == 'passed'
        and report.get('reason') == 'injected_setup_failure_observed'
        and report.get('browser_launches') == 1 and report.get('contexts') == 1
        and report.get('screenshots') == 0 and report.get('fulfilled_documents') == 0)
    if expected_setup_failure:
        report['browser_close_verification'] = 'requires_supervisor_process_census'
        return
    report.update(status='failed', reason='browser_close_unverified',
                  browser_close_verification='unverified')


def deny_python_network(event, _args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.bind'):
        raise FixtureFailure('python_network_forbidden')


def fixture_html(model):
    for relative, expected in HASHES.items():
        check(hashlib.sha256((RUNTIME / relative).read_bytes()).hexdigest() == expected,
              'runtime_pin_mismatch')
    tree = ast.parse(FIXTURE.read_text(encoding='utf8'))
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'HTML'
                           for target in node.targets)]
    check(len(assignments) == 1, 'html_fixture_missing')
    original = ast.literal_eval(assignments[0].value)
    check(isinstance(original, str) and hashlib.sha256(original.encode()).hexdigest() == HTML_SHA,
          'html_fixture_pin_mismatch')
    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
    return original.replace('MODEL', str(model)).replace('STAMP', stamp)


def gate_html(html, timeframe, scenario):
    hours = int(timeframe[:-1])
    initial = 24 if hours == 12 else 12
    selected = initial if scenario == 'wrong-label' else hours
    content = html.replace('</script>', f'\nwindow.select({selected});\n</script>')
    notices = {
        'no-data': '<div role="status" style="position:absolute;left:60px;top:180px">No data</div>',
        'request-limit': '<div role="alert" style="position:absolute;left:60px;top:180px">Too many requests</div>',
        'loading-indicator': '<span role="progressbar" style="position:absolute;left:70px;top:180px;width:40px;height:40px"></span>',
    }
    if scenario in notices:
        content = content.replace('<canvas width="800" height="420"></canvas>',
                                  '<canvas width="800" height="420"></canvas>' + notices[scenario])
    if scenario == 'login-required':
        content += ('<div class="MuiModal-root" style="position:absolute;inset:0">'
                    '<div role="dialog" style="position:absolute;left:80px;top:180px;width:400px;height:120px">'
                    '<div>Log in to unlock full data</div><button>Login</button>'
                    '<button>Continue with Google</button></div></div>')
    return content


def exercise_gates(browser, args, capture, readiness, execution, rendering, html, active_html):
    context = browser.new_context(viewport={'width': 1600, 'height': 1300 if args.model == 3 else 1000})
    rendering.configure_context(context, args.model)
    page = context.new_page()
    cases = []
    url = capture.COINGLASS_HEATMAP_URL
    for hours in (12, 24, 48):
        timeframe = f'{hours}h'
        active_html[0] = html
        page.goto(url, wait_until='domcontentloaded', timeout=5000)
        check(readiness.valid_source(page.url, args.model), 'fixture_identity_failed')
        check(not readiness.valid_source(url.replace('coin=BTC', 'coin=ETH'), args.model),
              'wrong_coin_accepted')
        check(not readiness.valid_source(url + '&coin=BTC', args.model), 'duplicate_coin_accepted')
        execution.control(capture._select_timeframe, page, timeframe)
        state = readiness.current_state(page, timeframe, args.model)
        check(state['ready'] is True and state['label'] == f'{hours} hour', 'normal_control_failed')
        check(readiness.wait_for_render(page, timeframe, args.model, 1500)['ready'] is True,
              'normal_paint_failed')
        cases.append({'timeframe': timeframe.upper(), 'scenario': 'ready', 'passed': True})
        for reason in ('no-data', 'request-limit', 'loading-indicator', 'login-required', 'wrong-label'):
            active_html[0] = gate_html(html, timeframe, reason)
            page.goto(url, wait_until='domcontentloaded', timeout=5000)
            state = readiness.current_state(page, timeframe, args.model)
            check(state['ready'] is False and state['reason'] == reason, 'negative_gate_failed')
            if reason == 'login-required':
                calls = []
                def _select_timeframe(_page, value):
                    calls.append(value)
                try:
                    execution.control(_select_timeframe, page, timeframe)
                except execution.StageFailure as error:
                    check(error.code == 'source_login_required' and not calls, 'pre_control_gate_failed')
                else:
                    raise FixtureFailure('pre_control_gate_missing')
            cases.append({'timeframe': timeframe.upper(), 'scenario': reason, 'passed': True})
        # Same unchanged guard must reject a gate appearing after the control.
        active_html[0] = gate_html(html, timeframe, 'ready')
        page.goto(url, wait_until='domcontentloaded', timeout=5000)
        for raises in (False, True):
            page.locator('[data-fixture-gate]').evaluate_all('(nodes) => nodes.forEach(n => n.remove())')
            def _select_timeframe(target, _value):
                target.evaluate("""() => { const d=document.createElement('div');
                    d.dataset.fixtureGate='1';d.setAttribute('role','dialog');
                    d.style.cssText='position:absolute;left:80px;top:180px;width:400px;height:120px';
                    d.textContent='Log in to unlock full data';document.body.appendChild(d); }""")
                if raises:
                    raise TimeoutError('synthetic_control_timeout')
            try:
                execution.control(_select_timeframe, page, timeframe)
            except execution.StageFailure as error:
                check(error.code == 'source_login_required', 'post_control_gate_failed')
            else:
                raise FixtureFailure('post_control_gate_missing')
            cases.append({'timeframe': timeframe.upper(),
                          'scenario': 'gate_after_control_error' if raises else 'gate_after_control_success',
                          'passed': True})
    return {'gate_checks': cases, 'gate_check_count': len(cases)}


def worker(args):
    check(os.environ.get('COLLECTOR_LAB_SUPERVISED') == '1', 'supervisor_required')
    check(set(os.environ) <= {'PATH', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH',
                              'PYTHONDONTWRITEBYTECODE', 'COLLECTION_HEATMAP_MODEL',
                              'COLLECTOR_LAB_SUPERVISED'}, 'unexpected_environment')
    check(os.environ.get('COLLECTION_HEATMAP_MODEL') == str(args.model), 'model_environment_mismatch')
    sys.addaudithook(deny_python_network)
    html = fixture_html(args.model)
    check(CHROMIUM.is_file(), 'pinned_chromium_missing')
    sys.path.insert(0, str(RUNTIME))
    import capture_readiness as readiness
    import model1_execution as execution
    import model3_rendering as rendering
    from market_vision import coinglass_heatmap_capture as capture
    from playwright.sync_api import Browser, BrowserType, Page, sync_playwright
    check(capture.HEATMAP_MODEL == args.model, 'runtime_model_mismatch')
    check('market_vision.openai_heatmap_scanner' not in sys.modules, 'ai_module_loaded')
    output = EVIDENCE / f'm{args.model}-{args.timeframe.lower()}-{args.case}'
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    (output / 'fixture.html').write_text(html, encoding='utf8')
    browsers, contexts = [], []
    active_html = [html]
    counts = {'browser_launches': 0, 'contexts': 0, 'screenshots': 0,
              'fulfilled_documents': 0, 'aborted_requests': 0}
    original_launch, original_context, original_shot = BrowserType.launch, Browser.new_context, Page.screenshot
    original_preparation, original_observation = rendering.preparation, rendering.observation
    render_facts = {'early_active': 0, 'late_active': 0, 'restored': 0}
    def model3_enabled(page):
        return page.evaluate('() => globalThis.__cg29RenderLab?.snapshot().some(value => value.enabled) || false') is True
    @contextmanager
    def preparation(page, model):
        with original_preparation(page, model):
            if model == 3:
                render_facts['early_active'] += int(model3_enabled(page))
            yield
        if model == 3:
            render_facts['restored'] += int(not model3_enabled(page))
    @contextmanager
    def observation(page, model, geometry):
        with original_observation(page, model, geometry) as prepare:
            if model == 3:
                render_facts['late_active'] += int(model3_enabled(page))
            yield prepare
        if model == 3:
            render_facts['restored'] += int(not model3_enabled(page))
    def launch(browser_type, *a, **kw):
        check(browser_type.name == 'chromium', 'unexpected_browser')
        kw.update(executable_path=str(CHROMIUM), headless=True)
        kw['args'] = [*kw.get('args', []), '--disable-background-networking',
                      '--disable-component-update', '--no-first-run', '--host-resolver-rules=MAP * ~NOTFOUND']
        result = original_launch(browser_type, *a, **kw)
        browsers.append(result)
        counts['browser_launches'] += 1
        check(result.version.startswith('140.'), 'browser_version_mismatch')
        return result
    def context(browser, *a, **kw):
        check('storage_state' not in kw and 'extra_http_headers' not in kw, 'session_input_forbidden')
        kw.update(offline=True, service_workers='block')
        result = original_context(browser, *a, **kw)
        contexts.append(result)
        counts['contexts'] += 1
        def fulfill(route):
            request = route.request
            if (request.url == capture.COINGLASS_HEATMAP_URL and request.method == 'GET'
                    and request.resource_type == 'document' and request.is_navigation_request()
                    and request.frame == request.frame.page.main_frame):
                counts['fulfilled_documents'] += 1
                route.fulfill(status=200, content_type='text/html', body=active_html[0])
            else:
                counts['aborted_requests'] += 1
                route.abort()
        result.route('**/*', fulfill)
        if args.case in ('failure', 'timeout'):
            print('FIXTURE_BROWSER_READY', flush=True)
        if args.case == 'failure':
            raise FixtureFailure('injected_after_context')
        if args.case == 'timeout':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            while True:
                time.sleep(1)
        return result
    def screenshot(page, *a, **kw):
        counts['screenshots'] += 1
        return original_shot(page, *a, **kw)
    report = {'format': 'collector-browser-fixture.v1', 'model': args.model,
              'timeframe': args.timeframe, 'case': args.case, 'commit': COMMIT,
              'status': 'failed', 'reason': 'fixture_incomplete', 'runtime_pins_verified': True,
              'real_source_requests': 0, 'ai_client_calls': 0, 'database_connections': 0,
              'native_hover_validated': False, 'png_validated': False,
              'scope': 'synthetic_html_native_capture_hover' if args.case == 'capture' else 'synthetic_dom_guards'}
    try:
        with ExitStack() as stack:
            stack.enter_context(patch.object(BrowserType, 'launch', launch))
            stack.enter_context(patch.object(Browser, 'new_context', context))
            stack.enter_context(patch.object(Page, 'screenshot', screenshot))
            stack.enter_context(patch.object(rendering, 'preparation', preparation))
            stack.enter_context(patch.object(rendering, 'observation', observation))
            if args.case == 'gates':
                with sync_playwright() as driver:
                    browser = driver.chromium.launch(headless=True)
                    try:
                        report.update(exercise_gates(browser, args, capture, readiness, execution,
                                                     rendering, html, active_html))
                    finally:
                        browser.close()
            else:
                images = capture.capture_heatmaps(output, timeframes=(args.timeframe.lower(),))
                check(args.case == 'capture', 'injection_not_reached')
                check(all(not item.is_connected() for item in browsers), 'native_browser_close_failed')
                report['native_browser_disconnected_before_wrapper_cleanup'] = True
                check(len(images) == 1 and images[0]['timeframe'] == args.timeframe.lower(), 'capture_identity_failed')
                snapshot = images[0]['hover_snapshot']
                rows = snapshot.get('rows', [])
                check(snapshot.get('complete') is True and len(rows) == 40, 'native_hover_incomplete')
                check(snapshot.get('heatmap_model') == args.model and snapshot.get('timeframe') == args.timeframe,
                      'native_hover_identity_failed')
                check([row['price'] for row in rows] == [round(100 - i / 10, 1) for i in range(40)],
                      'native_hover_price_failed')
                check(snapshot.get('total_value') == 40_000_000, 'native_hover_total_failed')
                if args.model in (1, 2):
                    check(snapshot['source_candle']['close'] == 98.05, 'native_candle_failed')
                png = output / f'coinglass_btc_heatmap_{args.timeframe.lower()}.png'
                check(Path(images[0]['image']) == png and png.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n',
                      'png_failed')
                check(counts['screenshots'] == 1 and counts['fulfilled_documents'] == 1,
                      'capture_count_failed')
                if args.model == 3:
                    check(render_facts == {'early_active': 1, 'late_active': 2, 'restored': 3},
                          'model3_render_fixture_gate_failed')
                (output / 'hover.json').write_text(json.dumps(snapshot, allow_nan=False, indent=2), encoding='utf8')
                report.update(native_hover_validated=True, png_validated=True, rows=40,
                              tooltip_reads=snapshot['tooltip_reads'],
                              png_sha256=hashlib.sha256(png.read_bytes()).hexdigest(),
                              artifacts=[str(output / 'fixture.html'), str(png), str(output / 'hover.json')])
            report.update(status='passed', reason='fixture_verified')
    except FixtureFailure as error:
        if args.case == 'failure' and error.args == ('injected_after_context',):
            report.update(status='passed', reason='injected_setup_failure_observed')
        else:
            report['reason'] = error.args[0] if error.args else 'fixture_assertion_failed'
    except BaseException as error:
        report['reason'] = 'native_capture_rejected' if getattr(error, 'code', None) else 'fixture_execution_failed'
        allowed = execution.CODES | execution.VALIDATION_REASONS
        code = getattr(error, 'code', None)
        reason = getattr(error, 'reason', None)
        if isinstance(code, str) and code in allowed:
            report['native_code'] = code
        if isinstance(reason, str) and reason in allowed:
            report['native_reason'] = reason
    finally:
        close_requests = 0
        for item in reversed(contexts):
            try:
                item.close()
                close_requests += 1
            except BaseException:
                pass
        for item in reversed(browsers):
            try:
                if item.is_connected():
                    item.close()
            except BaseException:
                pass
        report.update(counts, context_close_requests_completed=close_requests,
                      browsers_disconnected=all(not item.is_connected() for item in browsers),
                      browser_version=browsers[0].version if browsers else None,
                      model3_render_observations=render_facts,
                      ai_module_loaded='market_vision.openai_heatmap_scanner' in sys.modules)
    apply_browser_close_check(report)
    check(report['ai_module_loaded'] is False, 'ai_module_loaded')
    return report


def live_worker():
    """One approved source-only capture, using the installed unedited reader."""
    # Do not pass any inherited cancellation mask onward to the driver/browser.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM, signal.SIGHUP})
    report = empty_live_report()
    pages, browsers, contexts, diagnostics = [], [], [], []
    output = None
    snapshot = geometry = lifecycle = managed = None
    state = None
    failure_phase = [None]
    current_phase = ['startup']
    try:
        if not live_acceptance_enabled(os.environ): raise LiveStop('live_setup_failed')
        allowed = {'PATH', 'HOME', 'LANG', 'LC_CTYPE', 'PLAYWRIGHT_BROWSERS_PATH', 'PYTHONDONTWRITEBYTECODE',
                   'COLLECTION_HEATMAP_MODEL', 'COLLECTOR_SOURCE_SUPERVISED',
                   'COINGLASS_COOKIE_HEADER', 'COINGLASS_STORAGE_STATE_JSON',
                   'COINGLASS_SESSION_RETURN_FD', 'COINGLASS_SESSION_RUN_ID', 'COINGLASS_SESSION_REVISION',
                   'COLLECTOR_SOURCE_RUNTIME', 'COLLECTOR_SOURCE_OUTPUT',
                   'COLLECTOR_LIVE_DEADLINE_MONOTONIC', 'COLLECTOR_ACCOUNT_AUTH_ACCEPTANCE'}
        if (set(os.environ) - allowed or os.environ.get('COLLECTOR_SOURCE_SUPERVISED') != '1'
                or os.environ.get('COLLECTION_HEATMAP_MODEL') != '1'
                or os.environ.get('COINGLASS_COOKIE_HEADER') != ''
                or not os.environ.get('COINGLASS_STORAGE_STATE_JSON', '').strip()):
            raise LiveStop('unexpected_environment')
        import resource
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        deadline = float(os.environ['COLLECTOR_LIVE_DEADLINE_MONOTONIC'])
        if not math.isfinite(deadline) or not 0 < deadline - time.monotonic() <= 40:
            raise LiveStop('work_deadline')
        runtime = Path(os.environ['COLLECTOR_SOURCE_RUNTIME']).resolve(strict=True)
        candidate_output = Path(os.environ['COLLECTOR_SOURCE_OUTPUT'])
        parent = candidate_output.parent
        if (candidate_output.name != 'capture' or parent.is_symlink()
                or parent.resolve() != parent or not parent.is_relative_to('/tmp')
                or parent.stat().st_uid != os.geteuid() or parent.stat().st_mode & 0o077):
            raise LiveStop('live_setup_failed')
        for relative, expected in HASHES.items():
            if hashlib.sha256((runtime / relative).read_bytes()).hexdigest() != expected:
                raise LiveStop('runtime_pin_mismatch')
        report['runtime_pins_verified'] = True
        candidate_output.mkdir(mode=0o700, exist_ok=False)
        output = candidate_output
        sys.path.insert(0, str(runtime))
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import capture_readiness as readiness
        import model1_execution as execution
        import heatmap_hover as hover
        from market_vision import coinglass_heatmap_capture as capture
        from playwright.sync_api import Browser, BrowserType, Page, Locator, Keyboard, Mouse
        from session_capture import CaptureSession
        if capture.HEATMAP_MODEL != 1: raise LiveStop('identity_failed')
        # The worker supplied one verified cookie-only state. The legacy
        # header is explicitly empty; no account-header adapter is loaded.
        managed = CaptureSession.from_environment()
        if managed is None: raise LiveStop('live_setup_failed')
        state = managed.state
        held_state = [state]
        report['session_restore'].update(exclusive_input_validated=True, legacy_header_absent=True)
        lifecycle = SessionRestoreLifecycle(state, report, managed)
        # Chromium/driver temporary profiles must remain in the same owned
        # subtree even if the supervisor has to kill them before normal close.
        browser_tmp = output / 'browser-tmp'
        browser_tmp.mkdir(mode=0o700, exist_ok=False)
        os.environ['TMPDIR'] = str(browser_tmp)
        sys.addaudithook(deny_python_network)
        def access_check():
            # The original capture keeps every access gate in its original place.
            # This wrapper enforces execution limits, not a different predicate.
            return None
        guard = LiveGuard(report, deadline, access_check)
        original_operation = execution.capture_operation
        @contextmanager
        def operation(phase):
            previous = current_phase[0]
            current_phase[0] = phase if phase in LIVE_PHASES else 'capture'
            if phase not in ('context_close', 'browser_close'):
                guard.check()
                print('LIVE_PHASE ' + current_phase[0], flush=True)
            observation = lifecycle.account_observer
            if observation is not None: observation.operation(phase, 'begin')
            try:
                with original_operation(phase): yield
            except BaseException:
                if observation is not None: observation.operation(phase, 'error')
                if failure_phase[0] is None: failure_phase[0] = current_phase[0]
                raise
            else:
                if observation is not None: observation.operation(phase, 'end')
            finally:
                current_phase[0] = previous
        original_launch, original_context = BrowserType.launch, Browser.new_context
        original_goto, original_shot = Page.goto, Page.screenshot
        from playwright.sync_api import BrowserContext
        original_new_page = BrowserContext.new_page
        original_click, original_press = Locator.click, Locator.press
        original_key, original_mouse = Keyboard.press, Mouse.click
        original_wait = capture.wait_for_render
        original_hover = hover.capture_column
        original_select = capture.select_visible_control
        selected_controls = set()
        map_ready = [False]
        def launch(browser_type, *args, **kwargs):
            guard.check()
            if browsers or browser_type.name != 'chromium': raise LiveStop('live_setup_failed')
            kwargs['timeout'] = min(kwargs.get('timeout', 30000), max(1, int((deadline-time.monotonic())*1000)))
            browser = original_launch(browser_type, *args, **kwargs)
            browsers.append(browser); report['browser_launches'] = 1
            if not browser.version.startswith('140.'): raise LiveStop('live_setup_failed')
            return browser
        def context(browser, *args, **kwargs):
            guard.check()
            if contexts or kwargs.get('storage_state') is not state or any(k in kwargs for k in ('extra_http_headers', 'record_har_path', 'record_video_dir')):
                raise LiveStop('live_setup_failed')
            context = original_context(browser, *args, **kwargs)
            report['session_restore']['context_state_applied'] = True
            contexts.append(context); report['contexts'] = 1
            lifecycle.establish_context(context)
            def one_navigation(route):
                request = route.request
                if request.is_navigation_request() and request.frame == request.frame.page.main_frame:
                    expected_page = len(pages) == 1 and request.frame.page == pages[0]
                    if (not expected_page or guard.navigation_requests or
                            request.url != capture.COINGLASS_HEATMAP_URL or request.method != 'GET'):
                        report['blocked_navigation_requests'] = min(100000, report['blocked_navigation_requests'] + 1)
                        route.abort(); return
                    guard.navigation_requests += 1
                route.fallback()
            # Includes browser-created popups; the unchanged advertising filter
            # is installed later and remains active. No bodies/headers are read.
            context.route('**/*', one_navigation)
            return context
        def new_page(context, *args, **kwargs):
            guard.check()
            if pages: raise LiveStop('second_navigation_forbidden')
            page = original_new_page(context, *args, **kwargs)
            pages.append(page)
            diagnostics.append(readiness.CaptureNetworkDiagnostics(page))
            page.on('request', lambda _request: report.__setitem__('requests_seen', min(100000, report['requests_seen'] + 1)))
            page.on('response', lambda _response: report.__setitem__('responses_seen', min(100000, report['responses_seen'] + 1)))
            lifecycle.account_observer = AccountResponseObservation(page, deadline, readiness.LOGIN_GATE_SCRIPT)
            lifecycle.attach(page)
            return page
        def goto(page, url, *args, **kwargs):
            if url != capture.COINGLASS_HEATMAP_URL: raise LiveStop('identity_failed')
            return guard.invoke('goto', original_goto, page, url, *args, **kwargs)
        def reload(*args, **kwargs): return guard.invoke('reload', None, *args, **kwargs)
        def retry(*args, **kwargs): return guard.invoke('retry', None, *args, **kwargs)
        def click(*args, **kwargs): return guard.invoke('click', original_click, *args, **kwargs)
        def press(*args, **kwargs): return guard.invoke('key', original_press, *args, **kwargs)
        def key(*args, **kwargs): return guard.invoke('key', original_key, *args, **kwargs)
        def mouse(*args, **kwargs): return guard.invoke('click', original_mouse, *args, **kwargs)
        def wait_for_render(*args, **kwargs):
            state = original_wait(*args, **kwargs)
            report['source_identity_verified'] = (readiness.valid_source(args[0].url, 1)
                                                  and selected_controls == {'Model 1', 'Symbol'})
            map_ready[0] = bool(report['source_identity_verified'] and state.get('ready') is True
                                and state.get('label') == '12 hour')
            return state
        def select_control(page, label):
            result = original_select(page, label)
            if label not in ('Model 1', 'Symbol') or result is not True: raise LiveStop('identity_failed')
            selected_controls.add(label)
            return result
        def screenshot(page, *args, **kwargs):
            guard.check(); access_check()
            target = output / 'coinglass_btc_heatmap_12h.png'
            if report['screenshots'] or args or kwargs.get('path') != str(target): raise LiveStop('output_invalid')
            report['screenshots'] = 1
            return original_shot(page, **kwargs)
        def native_hover(*args, **kwargs):
            guard.check(); access_check()
            snapshot = original_hover(*args, **kwargs)
            guard.check(); access_check()
            final = readiness.current_state(pages[0], '12h', 1)
            if not final.get('ready') or final.get('label') != '12 hour': raise LiveStop('identity_failed')
            return snapshot
        original_guard = readiness.require_unblocked_source
        def observed_guard(page, model, *, phase):
            observation = lifecycle.account_observer
            try:
                result = (original_guard(page, model, phase=phase) if observation is None
                          else observation.guard(original_guard)(page, model, phase=phase))
            except BaseException as error:
                managed.note_source_error(error)
                raise
            if phase == 'render_readiness' and map_ready[0]:
                lifecycle.render_guard_passed = True
                report['access_verified'] = map_access_observed(observation.row if observation is not None else None)
            return result
        with ExitStack() as stack:
            for owner, name, function in ((BrowserType, 'launch', launch), (Browser, 'new_context', context),
                    (BrowserContext, 'new_page', new_page), (Page, 'goto', goto), (Page, 'reload', reload),
                    (Page, 'screenshot', screenshot), (Locator, 'click', click), (Locator, 'press', press),
                    (Keyboard, 'press', key), (Mouse, 'click', mouse),
                    (capture, 'record_source_reload', retry),
                    (readiness, 'require_unblocked_source', observed_guard),
                    (capture, 'require_unblocked_source', observed_guard),
                    (BrowserContext, 'close', lifecycle.before_close(BrowserContext.close)),
                    (Browser, 'close', lifecycle.before_close(Browser.close)),
                    (capture, 'capture_operation', operation), (execution, 'capture_operation', operation),
                    (capture, 'select_visible_control', select_control),
                    (capture, 'wait_for_render', wait_for_render), (hover, 'capture_column', native_hover),
                    (capture, '_load_authenticated_storage_state', lambda: held_state.pop()),
                    (capture, '_load_cookie_header', lambda: [])):
                stack.enter_context(patch.object(owner, name, function))
            results = capture.capture_heatmaps(output, timeframes=('12h',))
        if len(results) != 1 or results[0]['timeframe'] != '12h' or not report['source_identity_verified']:
            raise LiveStop('identity_failed')
        snapshot = results[0]['hover_snapshot']
        geometry = results[0].get('price_geometry')
        if not (snapshot.get('complete') is True and snapshot.get('heatmap_model') == 1
                and snapshot.get('timeframe') == '12H' and isinstance(snapshot.get('rows'), list)):
            raise LiveStop('identity_failed')
        # Native capture_column already validates all rows, source timestamp,
        # distributed anchors and initial/final candle consistency.
        png = output / 'coinglass_btc_heatmap_12h.png'
        from PIL import Image
        with Image.open(png) as image:
            report['png_width'], report['png_height'] = image.size
            if image.format != 'PNG': raise LiveStop('output_invalid')
            image.verify()
        report.update(png_validated=True, png_sha256=hashlib.sha256(png.read_bytes()).hexdigest(),
            native_hover_validated=True, row_count=len(snapshot['rows']), tooltip_reads=snapshot['tooltip_reads'],
            source_time=snapshot['source_time'], candle_close=snapshot['source_candle']['close'],
            total_value=snapshot['total_value'], status='passed', reason='capture_verified')
    except LiveStop as error:
        report.update(status='stopped', reason=error.reason)
    except BaseException as error:
        code, reason = getattr(error, 'code', None), getattr(error, 'reason', None)
        report['reason'] = 'native_capture_rejected' if code in LIVE_NATIVE_CODES or reason in LIVE_NATIVE_REASONS else 'live_capture_failed'
        if code in LIVE_NATIVE_CODES: report['native_code'] = code
        if reason in LIVE_NATIVE_REASONS: report['native_reason'] = reason
    finally:
        if lifecycle is not None: lifecycle.finish()
        elif managed is not None:
            try: managed.finish(None)
            except BaseException: report.update(status='failed', reason='live_capture_failed')
        if isinstance(state, dict): state.clear()
        state = None
        os.environ.pop('COINGLASS_COOKIE_HEADER', None)
        os.environ.pop('COINGLASS_STORAGE_STATE_JSON', None)
        report['phase'] = failure_phase[0] or ('hover_collection' if report['native_hover_validated'] else current_phase[0])
        for context in reversed(contexts):
            try: context.close()
            except BaseException: pass
        for browser in reversed(browsers):
            try:
                if browser.is_connected(): browser.close()
            except BaseException: pass
        report['browsers_disconnected'] = bool(browsers) and all(not item.is_connected() for item in browsers)
        if diagnostics:
            try: report['network'] = diagnostics[0].summary()
            except BaseException: pass
        if lifecycle is not None and lifecycle.cleanup_failed:
            report.update(status='failed', reason='account_auth_cleanup_failed')
        if report['status'] == 'passed' and not report['browsers_disconnected']:
            report.update(status='failed', reason='browser_close_unverified')
        site_sources = lifecycle.account_observer.site_sources if lifecycle is not None and lifecycle.account_observer is not None else None
        retain_live_artifacts(output, report, snapshot, geometry, site_sources)
    if 'market_vision.openai_heatmap_scanner' in sys.modules:
        report.update(status='failed', reason='live_capture_failed')
    report = sanitize_live_report(report) or empty_live_report()
    if output is not None and report['private_output_deleted']:
        try:
            path = output / 'receipt.json'
            path.write_text(json.dumps(report, allow_nan=False, separators=(',', ':')), encoding='utf8')
            path.chmod(0o600)
        except Exception:
            report.update(status='failed', reason='ephemeral_cleanup_failed', artifact_status='cleanup_failed')
            report['artifact_files'] = []
    return report


def main():
    if sys.argv[1:] == ['--live-worker']:
        report = live_worker()
        print(json.dumps(report, allow_nan=False, separators=(',', ':')), flush=True)
        return 0 if report['status'] == 'passed' else 1
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=int, choices=(1, 2, 3), required=True)
    parser.add_argument('--timeframe', choices=('12H', '24H', '48H'), required=True)
    parser.add_argument('--case', choices=CASES, default='capture')
    parser.add_argument('--worker', action='store_true')
    args = parser.parse_args()
    if not args.worker:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import collector_matrix
        report = collector_matrix.single(args.model, args.timeframe, args.case)
        within_limit = collector_matrix.emit_report(report)
        return 0 if within_limit and report['status'] == 'passed' else 1
    try:
        report = worker(args)
    except BaseException:
        report = {'format': 'collector-browser-fixture.v1', 'status': 'failed',
                  'reason': 'fixture_setup_failed', 'case': args.case,
                  'model': args.model, 'timeframe': args.timeframe}
    serialized = json.dumps(report, allow_nan=False, separators=(',', ':'))
    if len(serialized.encode('utf8')) > 32 * 1024:
        report = {'format': 'collector-browser-fixture.v1', 'status': 'failed', 'reason': 'output_limit',
                  'case': args.case, 'model': args.model, 'timeframe': args.timeframe}
        serialized = json.dumps(report, separators=(',', ':'))
    print('FIXTURE_RESULT ' + serialized, flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
