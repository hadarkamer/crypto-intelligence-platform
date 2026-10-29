"""Bounded diagnostics of the existing Max-Pain DOM parse, never decisions.

Only the already extracted ranking table is accepted here. No browser, network,
database or notification clients are used. Raw text belongs to the accepted
poll, before its parser discarded rows; diagnostics never repair market inputs.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re

VERSION = 'coinglass-dom-row-capture-v1'
MAX_TIMEFRAME_BYTES = 512 * 1024
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_NODES = 262144
MAX_DEPTH = 16
MAX_ROWS = 512
MAX_CELLS = 32
MAX_TEXT_BYTES = 1024
MAX_METADATA_TEXT_BYTES = 4096
MAX_ATTEMPTS = 3
TRACKED_SYMBOLS = ('HYPE', 'ZEC')
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
REQUIRED_HEADERS = ('#', 'Symbol', 'Price · 24h', 'Long pain below', 'Short pain above')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


@lru_cache(maxsize=1)
def source_pins():
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in ('coinglass_dom_reader.py', 'research_dom_capture.py')}


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _text(value, truncation, maximum=MAX_TEXT_BYTES, *, stats=None):
    if not isinstance(value, str):
        truncation.add('NONSTRING_RAW_FIELD')
        return ''
    encoded = value.encode('utf-8')
    if len(encoded) > maximum:
        truncation.add('TEXT_BYTE_LIMIT')
        if stats is not None:
            stats['processed_text_fields_truncated'] += 1
            stats['processed_text_utf8_bytes_omitted'] += len(encoded) - len(encoded[:maximum].decode('utf-8', errors='ignore').encode('utf-8'))
        return encoded[:maximum].decode('utf-8', errors='ignore')
    return value


def _metadata(values, truncation):
    result = {}
    for key, value in (values or {}).items():
        if key not in {'poll', 'attempt', 'active_label', 'baseline_label', 'expected_label',
                       'stable_count', 'clicked', 'active_ok', 'baseline_changed',
                       'previous_changed', 'unique_symbols', 'row_count_ok', 'duplicate_of',
                       'fingerprint', 'baseline_fingerprint', 'previous_fingerprint',
                       'parsed_at_utc', 'table_read_completed_at_utc', 'capture_completed_at_utc',
                       'error_code', 'parsed_count'}:
            continue
        result[key] = _text(value, truncation, MAX_METADATA_TEXT_BYTES) if isinstance(value, str) else value
    return result


def _seal(status, payload):
    return {'version': VERSION, 'status': status, 'payload': payload, 'payload_sha256': digest(payload)}


def unavailable(timeframe, reason, *, outcome='FAILED', metadata=None):
    truncation = set()
    try:
        pins = source_pins()
    except Exception:
        pins = {}
    captured_metadata = _metadata(metadata, truncation)
    captured_metadata['capture_completed_at_utc'] = _utc()
    return _seal('UNAVAILABLE', {'kind': 'TIMEFRAME', 'timeframe': str(timeframe)[:32],
                 'outcome': outcome, 'source_code_sha256': pins,
                 'capture_scope': 'Already extracted #mp-table ranking table only; no page/network/cookies',
                 'metadata': captured_metadata, 'unavailable_reason': str(reason)[:80],
                 'selected_table': None, 'row_decisions': [], 'attempt_history': [],
                 'truncation': sorted(truncation),
                 'tracked_symbols': {s: {'status': 'UNAVAILABLE', 'row_indices': []} for s in TRACKED_SYMBOLS}})


def unavailable_snapshot(requested_timeframes, reason):
    requested = [tf for tf in TIMEFRAMES if tf in requested_timeframes]
    payload = {'kind': 'SNAPSHOT', 'requested_timeframes': requested,
               'timeframes': {tf: unavailable(tf, reason) for tf in requested},
               'unavailable_reason': str(reason)[:80],
               'original_requested_timeframe_count': len(requested_timeframes)}
    result = _seal('UNAVAILABLE', payload)
    if len(canonical(result).encode('utf-8')) > MAX_SNAPSHOT_BYTES:
        result = _seal('UNAVAILABLE', {'kind': 'SNAPSHOT', 'requested_timeframes': [], 'timeframes': {},
                       'unavailable_reason': 'SNAPSHOT_MINIMUM_METADATA_BOUND',
                       'original_requested_timeframe_count': len(requested_timeframes)})
    if len(canonical(result).encode('utf-8')) > MAX_SNAPSHOT_BYTES:
        raise ValueError('DOM_CAPTURE_MINIMUM_SNAPSHOT_BOUND')
    return result


def _decision(cells, columns, header_count, index, parse_number, is_symbol):
    # This diagnostic classification mirrors the unchanged parser predicate.
    # It runs only after the operational parse and cannot change its rows.
    symbol_text = ''
    try:
        if columns['Symbol'] < len(cells):
            symbol_text = cells[columns['Symbol']].get('primary', '')
        if len(cells) != header_count:
            return {'row_index': index, 'status': 'REJECTED', 'reason_codes': ['CELL_COUNT_MISMATCH'],
                    'symbol_text': symbol_text, 'symbol': symbol_text.strip().upper()}
        rank = cells[columns['#']]['text'].strip()
        symbol = cells[columns['Symbol']]['primary'].strip().upper()
        price_texts = [cells[columns[key]]['primary'] for key in ('Price · 24h', 'Short pain above', 'Long pain below')]
        prices = [parse_number(value) for value in price_texts]
        amounts = [parse_number(cells[columns[key]]['amount']) for key in ('Short pain above', 'Long pain below')]
        reasons = []
        if not rank.isdigit():
            reasons.append('RANK_NOT_DIGITS')
        if not is_symbol(symbol):
            reasons.append('SYMBOL_TOKEN_INVALID')
        if any('%' in value for value in price_texts):
            reasons.append('PERCENTAGE_IN_PRICE')
        for name, value in zip(('CURRENT_PRICE_NOT_POSITIVE', 'SHORT_TARGET_NOT_POSITIVE', 'LONG_TARGET_NOT_POSITIVE'), prices):
            if value is None or value <= 0:
                reasons.append(name)
        for name, value in zip(('SHORT_AMOUNT_MISSING_OR_NEGATIVE', 'LONG_AMOUNT_MISSING_OR_NEGATIVE'), amounts):
            if value is None or value < 0:
                reasons.append(name)
        return {'row_index': index, 'status': 'REJECTED' if reasons else 'ACCEPTED',
                'reason_codes': reasons, 'symbol_text': symbol_text, 'symbol': symbol}
    except Exception:
        return {'row_index': index, 'status': 'REJECTED', 'reason_codes': ['RAW_ROW_SHAPE_UNREADABLE'],
                'symbol_text': '', 'symbol': ''}


def _presence(decisions, truncated):
    result = {}
    ambiguous = {'CELL_COUNT_MISMATCH', 'SYMBOL_TOKEN_INVALID', 'RAW_ROW_SHAPE_UNREADABLE'}
    unattributed = any(row['status'] == 'REJECTED' and
                       (not row['symbol'] or ambiguous.intersection(row['reason_codes']))
                       for row in decisions)
    for symbol in TRACKED_SYMBOLS:
        found = [row for row in decisions if row['symbol'] == symbol]
        statuses = {row['status'] for row in found}
        status = ('PRESENT_MIXED' if len(statuses) > 1 else next(iter(statuses))) if found else (
            'UNKNOWN_TRUNCATED' if truncated else 'UNKNOWN_UNATTRIBUTABLE_ROW' if unattributed else 'ABSENT')
        result[symbol] = {'status': status, 'row_indices': [row['row_index'] for row in found]}
    return result


def capture_poll(tables, timeframe, parsed_rows, *, metadata, parse_number, is_symbol):
    """Retain one accepted poll's selected table; no new DOM read is performed."""
    truncation = set()
    matches = []
    for index, table in enumerate(tables):
        headers = [re.sub(r'\s+', ' ', h).strip(' ↑↓') for h in table['headers']]
        if all(headers.count(name) == 1 for name in REQUIRED_HEADERS):
            matches.append((index, table, {name: headers.index(name) for name in REQUIRED_HEADERS}))
    if len(matches) != 1:
        return unavailable(timeframe, 'SELECTED_TABLE_NOT_UNIQUE', metadata=metadata)
    table_index, selected, columns = matches[0]
    header_count, raw_count = len(selected['headers']), len(selected['rows'])
    stats = {'processed_text_fields_truncated': 0, 'processed_text_utf8_bytes_omitted': 0}
    if header_count > MAX_CELLS:
        truncation.add('HEADER_CELL_LIMIT')
    headers = [_text(value, truncation, stats=stats) for value in selected['headers'][:MAX_CELLS]]
    rows, decisions = [], []
    if raw_count > MAX_ROWS:
        truncation.add('ROW_LIMIT')
    for index, cells in enumerate(selected['rows'][:MAX_ROWS]):
        if len(cells) > MAX_CELLS:
            truncation.add('ROW_CELL_LIMIT')
        if any(not isinstance(cell, dict) for cell in cells[:MAX_CELLS]):
            truncation.add('RAW_CELL_SHAPE_UNREADABLE')
        copied = [{key: _text(cell.get(key, ''), truncation, stats=stats) for key in ('text', 'primary', 'amount')}
                  if isinstance(cell, dict) else {'text': '', 'primary': '', 'amount': ''}
                  for cell in cells[:MAX_CELLS]]
        decision = _decision(cells, columns, header_count, index, parse_number, is_symbol)
        decision['symbol_text'] = _text(decision['symbol_text'], truncation)
        decision['symbol'] = _text(decision['symbol'], truncation)
        decision.update(original_cell_count=len(cells), retained_cell_count=len(copied),
                        omitted_cell_count=len(cells) - len(copied))
        rows.append(copied)
        decisions.append(decision)
    payload = {'kind': 'TIMEFRAME', 'timeframe': str(timeframe)[:32], 'outcome': 'ACCEPTED',
               'source_code_sha256': source_pins(),
               'capture_scope': 'Already extracted #mp-table ranking table only; no page/network/cookies',
               'metadata': _metadata(metadata, truncation),
               'selection': {'raw_table_count': len(tables), 'matching_table_count': len(matches), 'selected_table_index': table_index},
               'selected_table': {'headers': headers, 'rows': rows}, 'row_decisions': decisions,
               'counts': {'raw_rows': raw_count, 'retained_raw_rows': len(rows), 'parsed_accepted_rows': len(parsed_rows),
                          'retained_accepted_rows': sum(d['status'] == 'ACCEPTED' for d in decisions),
                          'retained_rejected_rows': sum(d['status'] == 'REJECTED' for d in decisions),
                          'omitted_raw_rows': raw_count - len(rows), 'original_header_cells': header_count,
                          'retained_header_cells': len(headers), 'omitted_header_cells': header_count - len(headers), **stats},
               'attempt_history': [],
               'truncation': sorted(truncation)}
    payload['metadata']['capture_completed_at_utc'] = _utc()
    payload['truncation'] = sorted(truncation)
    return _bound_timeframe(payload, 'COMPLETE')


def _bound_timeframe(payload, intended_status):
    """Bound the final envelope, including presence and retry annotations."""
    truncation = set(payload.get('truncation', []))
    table = payload.get('selected_table')
    rows = table['rows'] if isinstance(table, dict) else []
    decisions = payload.get('row_decisions', [])
    while True:
        if payload.get('outcome') == 'ACCEPTED':
            payload['tracked_symbols'] = _presence(decisions, bool(truncation))
        if 'counts' in payload:
            payload['counts'].update(retained_raw_rows=len(rows),
                omitted_raw_rows=payload['counts']['raw_rows'] - len(rows),
                retained_accepted_rows=sum(d['status'] == 'ACCEPTED' for d in decisions),
                retained_rejected_rows=sum(d['status'] == 'REJECTED' for d in decisions))
        payload['truncation'] = sorted(truncation)
        status = 'TRUNCATED' if truncation and intended_status != 'UNAVAILABLE' else intended_status
        result = _seal(status, payload)
        if len(canonical(result).encode('utf-8')) <= MAX_TIMEFRAME_BYTES:
            return result
        truncation.add('TIMEFRAME_BYTE_LIMIT')
        if rows:
            del rows[max(0, len(rows) // 2):]
            del decisions[len(rows):]
        else:
            # Normally unreachable with the declared 512KiB limit. A reduced
            # metadata-only marker still makes omitted evidence explicit.
            payload['selected_table'] = None
            payload['metadata'] = {k: v for k, v in payload.get('metadata', {}).items()
                                   if k in ('attempt', 'poll', 'parsed_at_utc')}
            payload['attempt_history'] = []
            truncation.add('TIMEFRAME_METADATA_REDUCED')
            if len(canonical(_seal(status, payload)).encode('utf-8')) > MAX_TIMEFRAME_BYTES:
                raise ValueError('DOM_CAPTURE_MINIMUM_ENVELOPE_BOUND')


def with_attempt_history(capture, history, *, attempt=None):
    result = deepcopy(capture)
    payload = result['payload']
    if attempt is not None:
        payload.setdefault('metadata', {})['attempt'] = attempt
    payload['attempt_history'] = list(history[:MAX_ATTEMPTS])
    if len(history) > MAX_ATTEMPTS:
        payload['truncation'] = sorted(set(payload.get('truncation', [])) | {'ATTEMPT_HISTORY_LIMIT'})
        if result['status'] == 'COMPLETE':
            result['status'] = 'TRUNCATED'
    return _bound_timeframe(payload, result['status'])


def aggregate(requested_timeframes, by_timeframe):
    requested = list(dict.fromkeys(requested_timeframes))
    if len(requested) > len(TIMEFRAMES) or any(tf not in TIMEFRAMES for tf in requested):
        return unavailable_snapshot(requested, 'UNSUPPORTED_DIAGNOSTIC_TIMEFRAME_SCOPE')
    frames = {}
    for timeframe in requested:
        capture = (by_timeframe.get(timeframe) or {}).get('dom_capture')
        if capture is None:
            attempted = timeframe in by_timeframe
            capture = unavailable(timeframe, 'TIMEFRAME_CAPTURE_UNAVAILABLE' if attempted else 'TIMEFRAME_NOT_ATTEMPTED',
                                  outcome='FAILED' if attempted else 'NOT_ATTEMPTED')
        try:
            validate_capture(capture, snapshot=False)
        except Exception:
            capture = unavailable(timeframe, 'TIMEFRAME_CAPTURE_INVALID')
        frames[timeframe] = capture
    pins = source_pins()
    gaps = ['TIMEFRAME_SOURCE_PIN_MISMATCH'] if any(c['payload'].get('source_code_sha256') != pins for c in frames.values()) else []
    status = 'COMPLETE' if frames and not gaps and all(c['status'] == 'COMPLETE' for c in frames.values()) else (
        'UNAVAILABLE' if not frames or all(c['status'] == 'UNAVAILABLE' for c in frames.values()) else 'PARTIAL')
    payload = {'kind': 'SNAPSHOT', 'requested_timeframes': requested, 'timeframes': frames,
               'source_code_sha256': pins, 'truncation': [], 'integrity_gaps': gaps}
    result = _seal(status, payload)
    if len(canonical(result).encode('utf-8')) > MAX_SNAPSHOT_BYTES:
        payload['truncation'] = ['SNAPSHOT_BYTE_LIMIT']
        for timeframe in reversed(requested):
            original = frames[timeframe]
            frames[timeframe] = _seal('TRUNCATED', {'kind': 'TIMEFRAME', 'timeframe': timeframe,
                'outcome': original['payload'].get('outcome', 'FAILED'), 'original_payload_sha256': original['payload_sha256'],
                'selected_table': None, 'row_decisions': [], 'attempt_history': original['payload'].get('attempt_history', []),
                'truncation': ['SNAPSHOT_BYTE_LIMIT'], 'tracked_symbols': _presence([], True)})
            result = _seal('PARTIAL', payload)
            if len(canonical(result).encode('utf-8')) <= MAX_SNAPSHOT_BYTES:
                break
    if len(canonical(result).encode('utf-8')) > MAX_SNAPSHOT_BYTES:
        return unavailable_snapshot(requested, 'SNAPSHOT_METADATA_BYTE_LIMIT')
    return result


def validate_capture(capture, *, snapshot=True):
    """Validate finite JSON shape, depth/node/byte bounds and producer hashes."""
    stack, nodes = [(capture, 0)], 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError('DOM_CAPTURE_STRUCTURE_BOUND')
        if isinstance(value, dict):
            if not all(isinstance(k, str) for k in value):
                raise ValueError('DOM_CAPTURE_KEY_TYPE')
            stack.extend((v, depth + 1) for v in value.values())
        elif isinstance(value, list):
            stack.extend((v, depth + 1) for v in value)
        elif value is not None and not isinstance(value, (str, bool, int, float)):
            raise ValueError('DOM_CAPTURE_VALUE_TYPE')
    limit = MAX_SNAPSHOT_BYTES if snapshot else MAX_TIMEFRAME_BYTES
    if len(canonical(capture).encode('utf-8')) > limit:
        raise ValueError('DOM_CAPTURE_BYTE_BOUND')
    if capture.get('version') != VERSION or not isinstance(capture.get('payload'), dict):
        raise ValueError('DOM_CAPTURE_VERSION')
    payload = capture['payload']
    if capture.get('payload_sha256') != digest(payload):
        raise ValueError('DOM_CAPTURE_PAYLOAD_SHA256')
    allowed = {'COMPLETE', 'PARTIAL', 'UNAVAILABLE'} if snapshot else {'COMPLETE', 'TRUNCATED', 'UNAVAILABLE'}
    if capture.get('status') not in allowed or payload.get('kind') != ('SNAPSHOT' if snapshot else 'TIMEFRAME'):
        raise ValueError('DOM_CAPTURE_STATUS_OR_KIND')
    if snapshot:
        frames, requested = payload.get('timeframes'), payload.get('requested_timeframes')
        if not isinstance(frames, dict) or not isinstance(requested, list) or set(frames) != set(requested):
            raise ValueError('DOM_CAPTURE_TIMEFRAME_COVERAGE')
        for timeframe, item in frames.items():
            validate_capture(item, snapshot=False)
            if item['payload'].get('timeframe') != timeframe:
                raise ValueError('DOM_CAPTURE_TIMEFRAME_IDENTITY')
        if capture['status'] == 'COMPLETE' and (payload.get('truncation') or payload.get('integrity_gaps') or not frames or any(item['status'] != 'COMPLETE' for item in frames.values())):
            raise ValueError('DOM_CAPTURE_FALSE_COMPLETE')
    elif capture['status'] == 'COMPLETE':
        if payload.get('outcome') != 'ACCEPTED' or payload.get('truncation') or not isinstance(payload.get('selected_table'), dict):
            raise ValueError('DOM_CAPTURE_FALSE_COMPLETE')
        rows, decisions = payload['selected_table'].get('rows'), payload.get('row_decisions')
        if not isinstance(rows, list) or not isinstance(decisions, list) or len(rows) != len(decisions):
            raise ValueError('DOM_CAPTURE_ROW_DECISION_COVERAGE')
        for index, decision in enumerate(decisions):
            if (not isinstance(decision, dict) or decision.get('row_index') != index or
                    decision.get('status') not in {'ACCEPTED', 'REJECTED'} or
                    not isinstance(decision.get('reason_codes'), list) or
                    (decision['status'] == 'ACCEPTED') != (decision['reason_codes'] == [])):
                raise ValueError('DOM_CAPTURE_ROW_DECISION_SCHEMA')
        counts = payload.get('counts', {})
        if (counts.get('raw_rows') != len(rows) or counts.get('retained_raw_rows') != len(rows) or
                counts.get('omitted_raw_rows') != 0 or
                counts.get('parsed_accepted_rows') != sum(d['status'] == 'ACCEPTED' for d in decisions)):
            raise ValueError('DOM_CAPTURE_FALSE_COMPLETE_ROW_COUNTS')
    if capture['status'] == 'COMPLETE':
        pins = payload.get('source_code_sha256')
        if (not isinstance(pins, dict) or set(pins) != {'coinglass_dom_reader.py', 'research_dom_capture.py'} or
                any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value) for value in pins.values())):
            raise ValueError('DOM_CAPTURE_SOURCE_PIN_SHAPE')
        if snapshot and any(item['payload'].get('source_code_sha256') != pins for item in payload['timeframes'].values()):
            raise ValueError('DOM_CAPTURE_MIXED_SOURCE_PINS')
    return True
