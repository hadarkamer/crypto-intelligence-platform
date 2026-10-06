"""Prospective Watch decision evidence, beside the unchanged score capture.

This module copies the actual operational selections. It performs no scoring,
Magnet construction, collection, transition tracking, or formula evaluation.
An observed empty result and an unavailable source remain distinct.
"""
from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path

import research_watch_score_capture as scores

VERSION = 'watch-operational-decisions-v1'
POPULATION = 'watch-all-scan-operational-decisions-v1'
HASH_VERSION = scores.HASH_VERSION
SYMBOLS = scores.SYMBOLS
TIMEFRAMES = tuple(scores.alert_engine.TIMEFRAMES)
MAX_BYTES = 256 * 1024
SELECTION_VERSION = 'actual-watch-items-groups-and-top-item-v1'
canonical, digest, utc = scores.canonical, scores.digest, scores._utc
ITEM_FIELDS = (
    'symbol', 'timeframe', 'side', 'score', 'current_price', 'target_price',
    'target_direction', 'distance_pct', 'price_source', 'price_pair', 'types',
    'components', 'average_score_all_timeframes', 'opposite_average_score_all_timeframes',
    'directional_scores_all_timeframes', 'opposite_score', 'consensus_hits',
    'consensus_total', 'gap_consensus_supporting', 'gap_consensus_total',
    'calculation_validation_errors', 'component_sum_check',
)
LIQUIDITY_FIELDS = ('near_amount', 'far_amount', 'near_share_pct')
MAGNET_FIELDS = (
    'symbol', 'side', 'count', 'members', 'min_target', 'max_target', 'average_target',
    'spread_pct', 'magnet_quality', 'liquidity_edge_pct', 'gross_liquidity_timeframe',
    'gross_candidate_liquidity', 'gross_opposite_liquidity', 'liquidity_calculation_version',
)
CONFIRMATION_FIELDS = (
    'status', 'label', 'score_threshold', 'strong_score_threshold', 'score_ok',
    'score_confirmation', 'strong_score_ok', 'early_shift_opposes', 'oi_opposes',
    'supporting_families', 'opposing_families', 'strong_core', 'strong_evidence_threshold',
    'magnet_quality', 'liquidity_edge_pct', 'liquidity_status', 'liquidity_label', 'derivatives',
)
MODULE_FIELDS = (
    'available', 'score', 'direction', 'relation', 'quality', 'early_shift',
    'data_quality_status', 'freshness_status', 'confidence',
)
EVIDENCE_FIELDS = (
    'symbol', 'expected_price_direction', 'maxpain_score', 'supporting_families',
    'opposing_families', 'core_supporting_families', 'core_qualified_supporting_families',
    'core_opposing_families', 'classification', 'relation_to_alert',
)
SIGNAL_LISTS = ('normal_confirmations', 'strong_confirmations', 'high_scores',
                'anomaly_setups', 'liquidity_imbalances', 'derivatives_high')
SELECTED_MAGNET_FIELDS = ('status', 'magnet_quality', 'liquidity_edge_pct',
                          'members', 'magnet_side', 'label')


def _pick(value, keys):
    if not isinstance(value, dict):
        return {}
    return {key: deepcopy(value[key]) for key in keys if key in value}


def _finite(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)


class CaptureValidationError(ValueError):
    """Only fixed internal reason codes may enter capture diagnostics."""


def error_reason(exc):
    return str(exc)[:160] if isinstance(exc, CaptureValidationError) else type(exc).__name__[:80]


def _require(condition, reason):
    if not condition:
        raise CaptureValidationError(reason)


def _list(value, name, bound=2048):
    _require(isinstance(value, list) and len(value) <= bound, 'INVALID_' + name)
    _require(all(isinstance(item, dict) for item in value), 'INVALID_' + name + '_RECORD')
    return value


def _item_id(item):
    symbol, tf, side = item.get('symbol'), item.get('timeframe'), item.get('side')
    _require(symbol in SYMBOLS and tf in TIMEFRAMES and side in ('LONG', 'SHORT'),
             'INVALID_ITEM_IDENTITY')
    return '|'.join((symbol, tf, side))


def _confirmation(value):
    result = _pick(value, CONFIRMATION_FIELDS)
    if 'derivatives' in result:
        result['derivatives'] = _pick(result['derivatives'], (
            'status', 'label', 'supporting_families', 'opposing_families',
            'early_shift_opposes', 'oi_opposes', 'strong_core', 'positioning_score',
            'futures_score', 'minimum_engine_score'))
    return result


def _evidence(value):
    result = _pick(value, EVIDENCE_FIELDS)
    value = value if isinstance(value, dict) else {}
    result['modules'] = {name: _pick(module, MODULE_FIELDS) for name, module in
                         (value.get('modules') or {}).items()
                         if name in ('positioning', 'futures_flow', 'spot_flow')}
    result['confirmation'] = _confirmation(value.get('confirmation'))
    return result


@lru_cache(maxsize=1)
def code_versions():
    root = Path(__file__).parent
    return {name: hashlib.sha256((root/name).read_bytes()).hexdigest() for name in (
        'main.py', 'magnet_v1.py', 'alert_engine.py', 'market_confidence_engine.py',
        'research_watch_decision_capture.py')}


def failure(cycle_id, reason):
    return {'version': VERSION, 'hash_version': HASH_VERSION, 'population': POPULATION,
            'cycle_id': str(cycle_id)[:200], 'status': 'FAILED', 'reason': str(reason)[:500]}


def _validate_scores(block, cycle_id, available):
    _require(isinstance(block, dict), 'MISSING_SCORE_CAPTURE')
    _require(block.get('version') == scores.VERSION and block.get('hash_version') == HASH_VERSION
             and block.get('population') == scores.POPULATION, 'SCORE_VERSION_MISMATCH')
    _require(block.get('cycle_id') == cycle_id and block.get('status') in ('COMPLETE', 'PARTIAL'),
             'SCORE_IDENTITY_OR_STATUS_MISMATCH')
    _require(len(canonical(block).encode()) <= scores.MAX_BYTES + 100, 'SCORE_CAPTURE_TOO_LARGE')
    _require(digest({k: v for k, v in block.items() if k != 'payload_sha256'}) ==
             block.get('payload_sha256'), 'SCORE_HASH_MISMATCH')
    _require(utc(block['computed_at_utc']) <= available, 'FUTURE_SCORE_CAPTURE')
    _require(set(block['coins']) == set(SYMBOLS) and set(block['symbols_expected']) == set(SYMBOLS),
             'SCORE_COIN_COVERAGE_MISMATCH')
    for coin in block['coins'].values():
        slots = _list(coin['maxpain'], 'SCORE_SLOTS', 14)
        _require(len(slots) == 14 and {(s['timeframe'], s['source_side']) for s in slots} ==
                 {(tf, side) for tf in TIMEFRAMES for side in ('LONG', 'SHORT')},
                 'SCORE_SLOT_COVERAGE_MISMATCH')


def _source_item(item, score_coin):
    result = _pick(item, ITEM_FIELDS)
    result['item_id'] = _item_id(item)
    result['maxpain_confirmation'] = _confirmation(item.get('maxpain_confirmation'))
    result['market_evidence'] = _evidence(item.get('market_evidence'))
    # Keep operational fallback values separate from actually observed amounts.
    result['operational_liquidity'] = _pick(item, LIQUIDITY_FIELDS)
    slot = next((s for s in score_coin['maxpain'] if s['timeframe'] == item['timeframe']
                 and s['source_side'] == item['side']), {})
    row = next((r for r in score_coin['sources'].get('maxpain_operational_rows', [])
                if r.get('timeframe') == item['timeframe']), {})
    result['source_liquidity'] = {
        'captured_amounts': _pick(slot, LIQUIDITY_FIELDS),
        'raw_amounts': _pick(row, ('short_liquidation_amount', 'long_liquidation_amount')),
    }
    result['item_sha256'] = digest(result)
    return result


def _group(group):
    result = _pick(group, ('key', 'symbol', 'side', 'signal_count'))
    signals = group.get('signal_keys')
    _require(isinstance(signals, (list, set, tuple)) and all(isinstance(s, str) for s in signals),
             'INVALID_SIGNAL_KEYS')
    result['signal_keys'] = sorted(signals)
    for name in SIGNAL_LISTS:
        result[name] = [_pick(entry, ('timeframe', 'score', 'count', 'types', 'share_pct', 'title'))
                        for entry in _list(group.get(name), name)]
    result['magnet'] = _pick(group.get('magnet'), SELECTED_MAGNET_FIELDS) if group.get('magnet') is not None else None
    result['top_item_id'] = _item_id(group.get('top_item') or {})
    return result


def build_bundle(*, cycle_id, score_bundle, prepared_items, displayable_items,
                 combined_candidates, combined_groups, magnet_evaluations,
                 computed_at_utc, top8_only, general_enabled):
    """Copy completed, side-effect-free operational builder results once.

    Missing sink execution must call failure(), never pass invented empty lists.
    Invalid source/selection bindings raise ValueError for the caller's existing
    capture-failure isolation; valid unavailable observations are retained.
    """
    computed = utc(computed_at_utc)
    _validate_scores(score_bundle, cycle_id, computed)
    for value, name in ((prepared_items, 'PREPARED_ITEMS'), (displayable_items, 'DISPLAYABLE_ITEMS'),
                        (combined_candidates, 'CANDIDATES'), (combined_groups, 'GROUPS'),
                        (magnet_evaluations, 'MAGNET_EVALUATIONS')):
        _list(value, name)
    _require(type(top8_only) is bool and type(general_enabled) is bool, 'INVALID_WATCH_SCOPE')
    coins = {}
    for symbol in SYMBOLS:
        selected = [item for item in prepared_items if item.get('symbol') == symbol]
        compact = [_source_item(item, score_bundle['coins'][symbol]) for item in selected]
        by_id = {item['item_id']: item for item in compact}
        _require(len(by_id) == len(compact), 'DUPLICATE_PREPARED_ITEM')
        display = [item for item in displayable_items if item.get('symbol') == symbol]
        # Comparing compact evidence also proves sinks retained the same item,
        # not a later score generation carrying an identical tuple identity.
        def reference(item):
            key = _item_id(item)
            _require(key in by_id and _source_item(item, score_bundle['coins'][symbol]) == by_id[key],
                     'ITEM_REFERENCE_MISMATCH')
            return key
        display_ids = [reference(item) for item in display]
        groups = []
        for group in combined_groups:
            if group.get('symbol') != symbol:
                continue
            frozen = _group(group)
            frozen['qualified'] = group.get('qualified')
            frozen['ordered_item_ids'] = [reference(item) for item in
                                           _list(group.get('ordered_items'), 'ORDERED_ITEMS', 7)]
            reference(group['top_item'])
            groups.append(frozen)
        candidates = []
        for candidate in combined_candidates:
            if candidate.get('symbol') == symbol:
                reference(candidate['top_item'])
                candidates.append(_group(candidate))
        magnets = []
        for record in magnet_evaluations:
            if record.get('symbol') != symbol:
                continue
            frozen = _pick(record, ('symbol', 'alert_side', 'evaluation_status', 'reason', 'error_type'))
            frozen['magnet'] = _pick(record.get('magnet'), MAGNET_FIELDS)
            frozen['source_item_id'] = reference(record['source_item']) if record.get('source_item') is not None else None
            frozen['confirmation'] = _confirmation(record.get('confirmation'))
            frozen['magnet_id'] = digest(frozen['magnet'])
            magnets.append(frozen)
        coin = {'prepared_items': compact, 'displayable_item_ids': display_ids,
                'combined_groups': groups, 'combined_candidates': candidates,
                'magnet_evaluations': magnets, 'builders_completed': True}
        missing = _validate_coin(symbol, coin, score_bundle['coins'][symbol])
        coin['missing_evidence'] = missing
        coin['status'] = _coin_status(coin, score_bundle['coins'][symbol], missing)
        coins[symbol] = coin
    block = {'version': VERSION, 'hash_version': HASH_VERSION, 'population': POPULATION,
             'selection_version': SELECTION_VERSION, 'cycle_id': cycle_id,
             'computed_at_utc': computed.isoformat(), 'source_score_sha256': score_bundle['payload_sha256'],
             'input_universe_sha256': score_bundle['input_universe_sha256'],
             'symbols_expected': list(SYMBOLS), 'code_sha256': code_versions(),
             'top8_only': top8_only, 'general_enabled': general_enabled,
             'operational_input_counts': {'prepared_items': len(prepared_items),
                 'displayable_items': len(displayable_items), 'combined_groups': len(combined_groups),
                 'combined_candidates': len(combined_candidates), 'magnet_evaluations': len(magnet_evaluations)},
             'coins': coins, 'status': 'COMPLETE' if all(c['status'] == 'COMPLETE' for c in coins.values()) else 'PARTIAL'}
    _require(len(canonical(block).encode()) <= MAX_BYTES, 'DECISION_CAPTURE_TOO_LARGE')
    block['payload_sha256'] = digest(block)
    return json.loads(canonical(block))


def _validate_item(item, symbol, score_coin):
    key = _item_id(item)
    _require(item['symbol'] == symbol and item.get('item_id') == key, 'ITEM_SYMBOL_MISMATCH')
    _require(digest({k: v for k, v in item.items() if k != 'item_sha256'}) == item.get('item_sha256'),
             'ITEM_HASH_MISMATCH')
    slot = next(s for s in score_coin['maxpain'] if (s['timeframe'], s['source_side']) ==
                (item['timeframe'], item['side']))
    _require(slot.get('status') == 'SCORED' and slot.get('selected') is True, 'ITEM_NOT_SELECTED_SCORE_SLOT')
    for name in ('score', 'components', 'target_price', 'distance_pct', 'consensus_hits', 'consensus_total'):
        _require(item.get(name) == slot.get(name), 'ITEM_SCORE_SLOT_MISMATCH:' + name)
    opposite = next(s for s in score_coin['maxpain'] if s['timeframe'] == item['timeframe']
                    and s['source_side'] != item['side'])
    _require(item.get('opposite_score') == opposite.get('score'), 'ITEM_OPPOSITE_SCORE_MISMATCH')
    rows = [row for row in score_coin['sources'].get('maxpain_operational_rows', [])
            if row.get('timeframe') == item['timeframe']]
    _require(len(rows) == 1, 'ITEM_SOURCE_ROW_MISSING_OR_DUPLICATE')
    row = rows[0]
    for name in ('current_price', 'price_source', 'price_pair'):
        _require(item.get(name) == row.get(name), 'ITEM_QUOTE_MISMATCH:' + name)
    liquidity = item['source_liquidity']
    _require(liquidity == {'captured_amounts': _pick(slot, LIQUIDITY_FIELDS),
             'raw_amounts': _pick(row, ('short_liquidation_amount', 'long_liquidation_amount'))},
             'ITEM_SOURCE_LIQUIDITY_MISMATCH')
    for name, value in liquidity['captured_amounts'].items():
        _require(item['operational_liquidity'].get(name) == value, 'ITEM_OPERATIONAL_LIQUIDITY_MISMATCH')
    missing = []
    if set(liquidity['captured_amounts']) != set(LIQUIDITY_FIELDS):
        missing.append('SOURCE_LIQUIDITY_UNAVAILABLE:' + key)
    if not item.get('maxpain_confirmation', {}).get('status'):
        missing.append('MAXPAIN_CONFIRMATION_UNAVAILABLE:' + key)
    if item.get('calculation_validation_errors'):
        missing.append('ITEM_CALCULATION_VALIDATION_ERRORS:' + key)
    expected = {side: {s['timeframe']: s['score'] for s in score_coin['maxpain']
                       if s['source_side'] == side and s.get('status') == 'SCORED'}
                for side in ('LONG', 'SHORT')}
    _require(item.get('directional_scores_all_timeframes') == expected, 'ITEM_DIRECTIONAL_SCORES_MISMATCH')
    for name, side in (('average_score_all_timeframes', item['side']),
                       ('opposite_average_score_all_timeframes', 'LONG' if item['side'] == 'SHORT' else 'SHORT')):
        values = list(expected[side].values())
        actual = round(sum(values)/len(values), 2) if values else None
        _require(item.get(name) == actual, 'ITEM_AVERAGE_MISMATCH:' + name)
    return missing


def _validate_coin(symbol, coin, score_coin):
    _require(coin.get('builders_completed') is True, 'BUILDERS_NOT_COMPLETED')
    prepared = _list(coin['prepared_items'], 'COIN_ITEMS', 7)
    by_id = {item['item_id']: item for item in prepared}
    _require(len(by_id) == len(prepared), 'DUPLICATE_ITEM_ID')
    missing = [reason for item in prepared for reason in _validate_item(item, symbol, score_coin)]
    display = coin['displayable_item_ids']
    _require(isinstance(display, list) and len(display) == len(set(display)) and set(display) <= set(by_id),
             'INVALID_DISPLAYABLE_SELECTION')
    expected_display = [item['item_id'] for item in prepared
                        if _finite(item.get('distance_pct')) and item['distance_pct'] >= 0]
    _require(display == expected_display, 'DISPLAYABLE_SELECTION_MISMATCH')
    selected = {'|'.join((symbol, s['timeframe'], s['source_side'])) for s in score_coin['maxpain']
                if s.get('selected') is True}
    if selected - by_id.keys():
        missing.append('SELECTED_ITEMS_OUTSIDE_PREPARED_POPULATION')
    if score_coin.get('source_time_errors'):
        missing.append('SCORE_SOURCE_TIME_ERRORS')
    if any(s.get('status') == 'MISSING_INPUT' for s in score_coin['maxpain']):
        missing.append('MAXPAIN_SOURCE_INPUT_MISSING')
    groups = _list(coin['combined_groups'], 'COIN_GROUPS', 2)
    candidates = _list(coin['combined_candidates'], 'COIN_CANDIDATES', 2)
    expected_sides = {by_id[key]['side'] for key in display}
    _require(len(groups) == len(expected_sides) and {g['side'] for g in groups} == expected_sides,
             'COMBINED_GROUP_COVERAGE_MISMATCH')
    qualified = []
    for group in groups:
        side = group['side']
        _require(group.get('symbol') == symbol and group.get('key') == symbol+'|'+side
                 and type(group.get('qualified')) is bool, 'COMBINED_GROUP_IDENTITY_MISMATCH')
        expected_order = sorted((key for key in display if by_id[key]['side'] == side),
                                key=lambda key: (-by_id[key]['score'], TIMEFRAMES.index(by_id[key]['timeframe'])))
        _require(group.get('ordered_item_ids') == expected_order and group.get('top_item_id') == expected_order[0],
                 'COMBINED_TOP_SELECTION_MISMATCH')
        keys = group['signal_keys']
        _require(isinstance(keys, list) and all(isinstance(key, str) for key in keys)
                 and keys == sorted(set(keys)) and type(group.get('signal_count')) is int
                 and group['signal_count'] == len(keys), 'COMBINED_SIGNAL_IDENTITY_MISMATCH')
        _require(group['qualified'] == (len(keys) >= 2), 'COMBINED_QUALIFICATION_MISMATCH')
        if group['qualified']:
            qualified.append({k: v for k, v in group.items() if k not in ('qualified', 'ordered_item_ids')})
    _require(sorted(candidates, key=lambda g: g['key']) == sorted(qualified, key=lambda g: g['key']),
             'COMBINED_CANDIDATE_SELECTION_MISMATCH')
    magnets = _list(coin['magnet_evaluations'], 'COIN_MAGNETS', 100)
    for record in magnets:
        magnet = record['magnet']
        side = {'UPPER': 'SHORT', 'LOWER': 'LONG'}.get(magnet.get('side'))
        _require(record.get('symbol') == symbol and magnet.get('symbol') == symbol
                 and record.get('alert_side') == side and side is not None
                 and record.get('magnet_id') == digest(magnet), 'MAGNET_IDENTITY_MISMATCH')
        _require(record.get('source_item_id') == (display[0] if display else None), 'MAGNET_SOURCE_SELECTION_MISMATCH')
        status = record.get('evaluation_status')
        _require(status in ('EVALUATED', 'NOT_EVALUATED', 'ERROR'), 'INVALID_MAGNET_EVALUATION_STATUS')
        if status == 'EVALUATED':
            _require(record['source_item_id'] is not None and isinstance(record['confirmation'].get('status'), str)
                     and bool(record['confirmation']['status']), 'MAGNET_CONFIRMATION_MISSING')
        else:
            _require(not record.get('confirmation') and bool(record.get('reason') or record.get('error_type')),
                     'MAGNET_UNAVAILABILITY_REASON_MISSING')
            missing.append('MAGNET_' + status + ':' + record['magnet_id'])
    for group in groups:
        eligible = [record for record in magnets if record['alert_side'] == group['side']
                    and record['evaluation_status'] == 'EVALUATED'
                    and record['confirmation'].get('status') in ('CONFIRMED', 'STRONG_CONFIRMED')]
        def rank(record):
            return (record['confirmation']['status'] == 'STRONG_CONFIRMED',
                    float(record['magnet'].get('magnet_quality') or 0),
                    float(record['magnet'].get('liquidity_edge_pct') or 0), len(record['magnet'].get('members') or []))
        if eligible:
            chosen = max(eligible, key=rank)
            expected = {'status': chosen['confirmation']['status'],
                        'magnet_quality': float(chosen['magnet'].get('magnet_quality') or 0),
                        'liquidity_edge_pct': chosen['magnet'].get('liquidity_edge_pct'),
                        'members': list(chosen['magnet'].get('members') or []),
                        'magnet_side': chosen['magnet']['side'], 'label': chosen['confirmation'].get('label')}
            _require(group.get('magnet') == expected, 'COMBINED_MAGNET_SELECTION_MISMATCH')
        else:
            _require(group.get('magnet') is None, 'COMBINED_MAGNET_WITHOUT_EVALUATION')
    return sorted(set(missing))


def _coin_status(coin, score_coin, missing):
    if not missing:
        return 'COMPLETE'
    return 'ABSENT' if score_coin.get('status') == 'ABSENT' and not coin['prepared_items'] else 'PARTIAL'


def validate_bundle(block, score_bundle, *, cycle_id, available_at_utc):
    """Return valid evidence; raise ValueError for identity, time or proof errors.

    SQL coverage is only a projection. Consumers must run this validator before
    interpreting decision evidence, including blocks marked COMPLETE by SQL.
    """
    try:
        _require(isinstance(block, dict), 'MISSING_DECISION_CAPTURE')
        _require(block.get('version') == VERSION and block.get('population') == POPULATION
                 and block.get('hash_version') == HASH_VERSION and block.get('selection_version') == SELECTION_VERSION,
                 'DECISION_VERSION_MISMATCH')
        _require(block.get('cycle_id') == cycle_id and block.get('status') in ('COMPLETE', 'PARTIAL'),
                 'DECISION_IDENTITY_OR_STATUS_MISMATCH')
        _require(len(canonical(block).encode()) <= MAX_BYTES + 100, 'DECISION_CAPTURE_TOO_LARGE')
        _require(block.get('payload_sha256') == digest({k: v for k, v in block.items() if k != 'payload_sha256'}),
                 'DECISION_HASH_MISMATCH')
        available, computed = utc(available_at_utc), utc(block['computed_at_utc'])
        _require(computed <= available, 'DECISION_COMPUTED_AFTER_AVAILABILITY')
        _validate_scores(score_bundle, cycle_id, computed)
        _require(block.get('source_score_sha256') == score_bundle['payload_sha256']
                 and block.get('input_universe_sha256') == score_bundle['input_universe_sha256'],
                 'DECISION_SOURCE_HASH_MISMATCH')
        _require(set(block['coins']) == set(SYMBOLS) and set(block['symbols_expected']) == set(SYMBOLS),
                 'DECISION_COIN_COVERAGE_MISMATCH')
        _require(type(block.get('top8_only')) is bool and type(block.get('general_enabled')) is bool,
                 'INVALID_WATCH_SCOPE')
        code = block.get('code_sha256')
        _require(isinstance(code, dict) and set(code) == set(code_versions()) and all(
            isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)
            for value in code.values()), 'INVALID_CODE_PROVENANCE')
        counts = block.get('operational_input_counts')
        count_keys = ('prepared_items', 'displayable_items', 'combined_groups',
                      'combined_candidates', 'magnet_evaluations')
        _require(isinstance(counts, dict) and set(counts) == set(count_keys) and all(
            type(counts[key]) is int and 0 <= counts[key] <= 2048 for key in count_keys),
            'INVALID_OPERATIONAL_INPUT_COUNTS')
        for key in count_keys:
            coin_key = 'displayable_item_ids' if key == 'displayable_items' else key
            _require(counts[key] >= sum(len(c[coin_key]) for c in block['coins'].values()),
                     'OPERATIONAL_INPUT_COUNT_MISMATCH')
        for symbol, coin in block['coins'].items():
            missing = _validate_coin(symbol, coin, score_bundle['coins'][symbol])
            _require(coin.get('missing_evidence') == missing and
                     coin.get('status') == _coin_status(coin, score_bundle['coins'][symbol], missing),
                     'DECISION_COMPLETENESS_MISMATCH')
        status = 'COMPLETE' if all(c['status'] == 'COMPLETE' for c in block['coins'].values()) else 'PARTIAL'
        _require(block['status'] == status, 'DECISION_COMPLETENESS_MISMATCH')
        return block
    except (KeyError, TypeError, AttributeError, OverflowError, StopIteration) as exc:
        raise ValueError('MALFORMED_DECISION_CAPTURE') from exc
