"""Original selected-item predicates evaluated separately for each captured TF.

The actual scorer-selected side is retained. An unselected or inactive side is
not a false-signal control, and unresolved selection blocks both directions.
This pure adapter never computes outcomes, chooses a best timeframe or trades.
"""
from __future__ import annotations

import math
from typing import Mapping

import research_watch_scan_formula_score_change as previous
import research_watch_scan_formula_maxpain as maxpain

legacy = previous.legacy
VERSION = 'watch-scan-timeframe-formulas-v1'
FEATURE_VERSION = 'watch-captured-selected-timeframe-liquidity-and-score-difference-v1'
SELECTION_VERSION = 'watch-selected-timeframe-first-arm-unknown-blocks-v1'
CATALOG_SIZE = 298
SUPPORTED_COUNT = 34
TIMEFRAMES = maxpain.TIMEFRAMES
DIRECTIONS = legacy.DIRECTIONS
MAPPING = 'event.direction_mapping_valid'
SHARE = 'liquidity.selected_share_pct'
ALIGNMENT = 'liquidity.alignment'
DIFFERENCE = 'max_pain.selected_opposite_difference'
NEW_FEATURES = frozenset((SHARE, ALIGNMENT, DIFFERENCE))
SUPPORTED_FEATURES = previous.SUPPORTED_FEATURES | NEW_FEATURES
canonical = legacy.canonical
digest = legacy.digest


def catalog_records() -> list[dict]:
    originals = previous.catalog_records()
    rows = []
    for row in originals:
        if row['supported']:
            continue
        missing = sorted({item['feature'] for item in legacy.existing._conditions(row['definition'])} - SUPPORTED_FEATURES)
        if not missing:
            rows.append({**row, 'supported': True, 'unsupported_features': []})
    if len(originals) != CATALOG_SIZE or len(rows) != SUPPORTED_COUNT:
        raise ValueError('TIMEFRAME_CATALOG_SUPPORT_DRIFT')
    return rows


def _count(value):
    return int(value) if legacy._finite(value) and value >= 0 and int(value) == value else None


def _opposite(side):
    return 'SHORT' if side == 'LONG' else 'LONG'


def _source_for_timeframe(observation, timeframe):
    rows = observation['sources'].get('maxpain_operational_rows')
    slots = observation.get('maxpain_slots')
    selected_rows = [row for row in rows if isinstance(row, Mapping) and row.get('timeframe') == timeframe] if isinstance(rows, list) else []
    selected_slots = [slot for slot in slots if isinstance(slot, Mapping) and slot.get('timeframe') == timeframe] if isinstance(slots, list) else []
    errors = []
    if len(selected_rows) != 1:
        errors.append('MISSING_OR_DUPLICATE_TIMEFRAME_SOURCE')
    if (len(selected_slots) != 2 or {slot.get('source_side') for slot in selected_slots} != set(DIRECTIONS)):
        errors.append('MISSING_OR_DUPLICATE_TIMEFRAME_SCORE_SLOTS')
    errors.extend('CAPTURE_TIME_ERROR:'+error for error in observation['source_time_errors'] if error.split('/', 1)[0] == timeframe)
    return (dict(selected_rows[0]) if len(selected_rows) == 1 else None,
        {slot['source_side']: dict(slot) for slot in selected_slots if slot.get('source_side') in DIRECTIONS}, errors)


def _selection(observation, row, slots, errors):
    errors = list(errors)
    if row is None or set(slots) != set(DIRECTIONS):
        return None, 'UNKNOWN', sorted(set(errors or ['MISSING_TIMEFRAME_SOURCE']))
    price = row.get('current_price')
    if not legacy._finite(price) or price <= 0:
        errors.append('INVALID_TIMEFRAME_SOURCE_PRICE')
    for key in ('price_source', 'price_pair'):
        if not isinstance(row.get(key), str) or not row[key].strip():
            errors.append('MISSING_QUOTE_PROVENANCE:'+key)
    observed = legacy.measurement.utc(observation['observed_at_utc'])
    for key in ('source_observed_at_utc', 'price_fetched_at_utc'):
        try:
            if legacy.measurement.utc(row.get(key)) > observed:
                errors.append('FUTURE_TIMEFRAME_SOURCE:'+key)
        except (ValueError, TypeError, OverflowError):
            errors.append('INVALID_TIMEFRAME_SOURCE_TIME:'+key)
    active = []
    for side in DIRECTIONS:
        slot = slots[side]
        target = row.get('short_max_pain' if side == 'SHORT' else 'long_max_pain')
        target_valid = target is None or (legacy._finite(target) and target > 0)
        if not target_valid:
            errors.append('INVALID_SOURCE_TARGET:'+side)
        is_active = (target is not None and target_valid and legacy._finite(price) and price > 0
            and ((target > price) if side == 'SHORT' else (target < price)))
        state = slot.get('status')
        if state == 'INACTIVE_TARGET':
            if (is_active or not target_valid or slot.get('score') is not None
                    or slot.get('selected') not in (None, False)):
                errors.append('INACTIVE_TARGET_MISMATCH:'+side)
            continue
        if state != 'SCORED':
            errors.append('MISSING_OR_INVALID_SCORE_SLOT:'+side)
            continue
        score = slot.get('score')
        if type(slot.get('selected')) is not bool:
            errors.append('MISSING_SELECTED_FLAG:'+side)
        if not legacy._finite(score) or not 0 <= score <= 100:
            errors.append('INVALID_SCORE:'+side)
        if not is_active or not legacy._finite(slot.get('target_price')) or slot['target_price'] != target:
            errors.append('SCORED_TARGET_DIRECTION_MISMATCH:'+side)
        components = slot.get('components')
        if not isinstance(components, Mapping) or not all(legacy._finite(components.get(key)) for key in maxpain.ADDITIVE_COMPONENTS):
            errors.append('INVALID_ADDITIVE_COMPONENTS:'+side)
        elif legacy._finite(score) and abs(round(sum(float(components[key]) for key in maxpain.ADDITIVE_COMPONENTS), 2)-score) > 0.01:
            errors.append('SCORE_COMPONENT_MISMATCH:'+side)
        hits, total, cluster = (_count(slot.get(key)) for key in ('consensus_hits', 'consensus_total', 'cluster_count'))
        if hits is None or total is None or hits > total:
            errors.append('INVALID_CONSENSUS_COUNTS:'+side)
        if cluster is None or hits is None or cluster > hits:
            errors.append('INVALID_CLUSTER_COUNT:'+side)
        if is_active and legacy._finite(score):
            distance = abs((target-price)/price*100.0)
            if not legacy._finite(distance) or distance <= 0:
                errors.append('INVALID_TARGET_DISTANCE:'+side)
            else:
                active.append((side, score, distance))
    if errors:
        return None, 'UNKNOWN', sorted(set(errors))
    if not active:
        return None, 'NO_ACTIVE_TARGET', ['NO_ACTIVE_TARGET']
    # This reproduces _choose_scored_side, not ordinary closest-target
    # consensus (which has a different exact-tie rule).
    winner = max(active, key=lambda item: (item[1], -item[2], 1 if item[0] == 'LONG' else 0))[0]
    if [side for side in DIRECTIONS if slots[side].get('selected') is True] != [winner]:
        return None, 'UNKNOWN', ['CAPTURED_SELECTION_MISMATCH']
    return winner, 'SELECTED', []


def _liquidity(row, slot, side):
    reasons = []
    values = {}
    for name, raw_key in (('near_amount', 'short_liquidation_amount' if side == 'SHORT' else 'long_liquidation_amount'),
            ('far_amount', 'long_liquidation_amount' if side == 'SHORT' else 'short_liquidation_amount')):
        value, raw = slot.get(name), row.get(raw_key)
        # The scorer float-converts present amounts but defaults absent inputs
        # to zero. The capture intentionally omits that fallback from research.
        try:
            source_value = float(raw) if raw not in (None, '') and not isinstance(raw, bool) else None
        except (ValueError, TypeError, OverflowError):
            source_value = None
        if (not legacy._finite(value) or value < 0 or not legacy._finite(source_value)
                or source_value < 0 or value != source_value):
            reasons.append('MISSING_OR_UNVERIFIED_'+name.upper())
        else:
            values[name] = value
    share = slot.get('near_share_pct')
    if not legacy._finite(share) or not 0 <= share <= 100:
        reasons.append('MISSING_OR_INVALID_SELECTED_SHARE')
    total = sum(values.values()) if len(values) == 2 else None
    if total is None or not legacy._finite(total) or total <= 0:
        reasons.append('MISSING_OR_ZERO_LIQUIDITY_DENOMINATOR')
    elif legacy._finite(share) and not math.isclose(share, 100*values['near_amount']/total, abs_tol=0.05):
        reasons.append('SELECTED_SHARE_SOURCE_MISMATCH')
    if reasons:
        return {}, sorted(set(reasons))
    return {SHARE: share, ALIGNMENT: 'SUPPORTS' if share >= 60 else 'OPPOSES' if share <= 40 else 'BALANCED',
        'liquidity.selected_amount': values['near_amount'], 'liquidity.opposite_amount': values['far_amount'],
        'liquidity.source_contract': 'MAX_PAIN_CAPTURED_AMOUNTS_V1', 'liquidity.capture_status': 'VALID'}, []


def evaluate_coin(observation: dict) -> dict:
    base_payload = legacy.evaluate_coin(observation)
    records, evaluations, provenance = catalog_records(), [], {}
    for timeframe in TIMEFRAMES:
        row, slots, errors = _source_for_timeframe(observation, timeframe)
        selected, selection, reasons = _selection(observation, row, slots, errors)
        selected_base = _opposite(selected) if selected is not None else None
        features_by_direction = {}
        for base in DIRECTIONS:
            old = base_payload['features_by_direction'][base]
            features, missing = dict(old['features']), dict(old['unavailable_features'])
            row_selection = 'UNKNOWN' if selection == 'UNKNOWN' else 'SELECTED' if base == selected_base else 'NOT_SELECTED'
            if row_selection == 'SELECTED':
                features[MAPPING] = True
                selected_slot, opposite_slot = slots[selected], slots[_opposite(selected)]
                liquidity, liquidity_reasons = _liquidity(row, selected_slot, selected)
                features.update(liquidity)
                if liquidity_reasons:
                    missing.update({key: liquidity_reasons for key in (SHARE, ALIGNMENT)})
                if opposite_slot.get('status') == 'SCORED':
                    features[DIFFERENCE] = selected_slot['score']-opposite_slot['score']
                else:
                    missing[DIFFERENCE] = ['OPPOSITE_TARGET_INACTIVE_NO_SCORE']
            else:
                missing.update({key: reasons or ['SOURCE_SIDE_NOT_SELECTED'] for key in (MAPPING, *NEW_FEATURES)})
            features_by_direction[base] = {'features': features, 'unavailable_features': missing,
                'selection_status': row_selection}
            for candidate in records:
                if row_selection == 'NOT_SELECTED':
                    decision = {'match_status': 'NOT_APPLICABLE', 'missing_features': []}
                else:
                    decision = legacy.evaluate_candidate(candidate['definition'], features)
                    if row_selection == 'UNKNOWN':
                        decision['match_status'] = 'UNKNOWN'
                evaluations.append({'candidate_key': candidate['candidate_key'], 'timeframe': timeframe,
                    'base_direction': base, 'analysis_direction': _opposite(base) if candidate['orientation'] == 'INVERSE' else base,
                    **decision, 'selection_status': row_selection})
        provenance[timeframe] = {'selection_status': selection, 'selected_source_side': selected,
            'selected_base_direction': selected_base, 'validation_reasons': reasons,
            'source_row': row, 'slots_by_source_side': slots, 'features_by_direction': features_by_direction}
    identity = {key: base_payload[key] for key in ('consumer_version', 'population_version', 'snapshot_set_id',
        'symbol', 'bundle_sha256', 'parent_payload_sha256', 'source_version', 'usable_from_utc', 'source_observed_at_utc')}
    payload = legacy._normalized_payload({**identity, 'version': VERSION, 'feature_version': FEATURE_VERSION,
        'selection_version': SELECTION_VERSION, 'timeframe_provenance': provenance, 'evaluations': evaluations})
    payload['feature_sha256'] = digest(payload)
    return payload
