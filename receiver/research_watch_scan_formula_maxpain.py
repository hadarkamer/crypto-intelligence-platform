"""Versioned coin-level MaxPain averages and consensus on frozen Watch data.

The seven actual source timeframes prove the aggregate denominator. Inactive
targets are excluded, missing inputs stay unknown, and no timeframe-specific
liquidity, component hypothesis or Combined/top-item feature is manufactured.
"""
from __future__ import annotations

from typing import Mapping

import research_watch_scan_formula as legacy

VERSION = 'watch-scan-formulas-v2-maxpain'
PREVIOUS_VERSION = legacy.VERSION
FEATURE_VERSION = 'watch-captured-total-and-maxpain-features-v2'
CATALOG_SHA256 = legacy.CATALOG_SHA256
CATALOG_SIZE = 298
SUPPORTED_COUNT = 82
DIRECTIONS = legacy.DIRECTIONS
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
ADDITIVE_COMPONENTS = ('directional_alignment', 'target_proximity', 'cluster_confidence', 'relative_gap')
MAPPING = 'event.direction_mapping_valid'
AVERAGE = 'max_pain.average_score_all_timeframes'
OPPOSITE_AVERAGE = 'max_pain.opposite_average_score_all_timeframes'
CONSENSUS = 'max_pain.consensus_hits_full'
SUPPORTED_FEATURES = legacy.SUPPORTED_FEATURES | {MAPPING, AVERAGE, OPPOSITE_AVERAGE, CONSENSUS}
canonical = legacy.canonical
digest = legacy.digest


def catalog_records() -> list[dict]:
    records = []
    # The legacy guard freezes the entire original array, including unsupported
    # definitions; only the versioned feature support declaration changes.
    for row in legacy.catalog_records():
        unsupported = sorted({condition['feature'] for condition in
            legacy.existing._conditions(row['definition'])} - SUPPORTED_FEATURES)
        records.append({**row, 'supported': not unsupported, 'unsupported_features': unsupported})
    if len(records) != CATALOG_SIZE or sum(row['supported'] for row in records) != SUPPORTED_COUNT:
        raise ValueError('MAXPAIN_CATALOG_SUPPORT_DRIFT')
    return records


def _count(value):
    return int(value) if legacy._finite(value) and value >= 0 and value == int(value) else None


def _opposite(direction):
    return 'SHORT' if direction == 'LONG' else 'LONG'


def _profiles(observation):
    observed = legacy.measurement.utc(observation['observed_at_utc'])
    common, rows, slots = [], {}, {}
    raw_rows = observation['sources'].get('maxpain_operational_rows')
    if not isinstance(raw_rows, list):
        common.append('MISSING_MAXPAIN_SOURCE_ROWS')
        raw_rows = []
    for row in raw_rows:
        if not isinstance(row, Mapping) or row.get('timeframe') not in TIMEFRAMES:
            common.append('INVALID_SOURCE_TIMEFRAME')
            continue
        tf = row['timeframe']
        if tf in rows:
            common.append('DUPLICATE_SOURCE_TIMEFRAME:' + tf)
        rows[tf] = row
    raw_slots = observation.get('maxpain_slots')
    if not isinstance(raw_slots, list):
        common.append('MISSING_MAXPAIN_SLOTS')
        raw_slots = []
    for slot in raw_slots:
        if (not isinstance(slot, Mapping) or slot.get('timeframe') not in TIMEFRAMES
                or slot.get('source_side') not in DIRECTIONS):
            common.append('INVALID_SCORE_SLOT_IDENTITY')
            continue
        key = (slot['timeframe'], slot['source_side'])
        if key in slots:
            common.append('DUPLICATE_SCORE_SLOT:' + '/'.join(key))
        slots[key] = slot
    if set(slots) != {(tf, side) for tf in TIMEFRAMES for side in DIRECTIONS}:
        common.append('INCOMPLETE_SCORE_SLOT_COVERAGE')
    for error in observation['source_time_errors']:
        if error.split('/', 1)[0] in TIMEFRAMES:
            common.append('CAPTURE_TIME_ERROR:' + error)

    active, targets, closest, quotes = {}, {}, {}, {}
    for tf in TIMEFRAMES:
        row = rows.get(tf)
        if row is None:
            common.append('MISSING_SOURCE_TIMEFRAME:' + tf)
            continue
        price = row.get('current_price')
        valid_price = legacy._finite(price) and price > 0
        if not valid_price:
            common.append('INVALID_SOURCE_PRICE:' + tf)
        for field in ('price_source', 'price_pair'):
            if not isinstance(row.get(field), str) or not row[field].strip():
                common.append('MISSING_QUOTE_PROVENANCE:' + tf + '/' + field)
        # Operational HYPE quotes may be perpetual/futures or another captured
        # supported provider. They are not relabelled as the outcome price route.
        quote = {field: row[field] for field in ('price_source', 'price_pair', 'price_market', 'price_instrument')
                 if isinstance(row.get(field), str)}
        if valid_price:
            quote['current_price'] = price
        for field in ('source_observed_at_utc', 'price_fetched_at_utc'):
            try:
                value = legacy.measurement.utc(row.get(field))
                quote[field] = value
                if value > observed:
                    common.append('FUTURE_SOURCE_TIME:' + tf + '/' + field)
            except (ValueError, TypeError, OverflowError):
                common.append('INVALID_SOURCE_TIME:' + tf + '/' + field)
        quotes[tf] = quote
        distances = []
        # Existing analysis.side_from_distances lists SHORT first, so an exact
        # tie belongs to SHORT. Scorer-selected side is a different concept.
        for side in ('SHORT', 'LONG'):
            target = row.get('short_max_pain' if side == 'SHORT' else 'long_max_pain')
            targets[(tf, side)] = target
            if target is None:
                active[(tf, side)] = False
                continue
            if not legacy._finite(target) or target <= 0:
                common.append('INVALID_SOURCE_TARGET:' + tf + '/' + side)
                continue
            if not valid_price:
                continue
            signed = (target - price) / price * 100.0
            if not legacy._finite(signed):
                common.append('INVALID_SOURCE_DISTANCE:' + tf + '/' + side)
                continue
            is_active = signed > 0 if side == 'SHORT' else signed < 0
            active[(tf, side)] = is_active
            if is_active:
                distances.append((side, abs(signed)))
        closest[tf] = min(distances, key=lambda item: item[1])[0] if distances else None

    total = sum(side is not None for side in closest.values())
    result = {}
    for side in DIRECTIONS:
        errors, values, active_tfs, inactive_tfs, missing_tfs = list(common), {}, [], [], []
        hits = sum(value == side for value in closest.values())
        for tf in TIMEFRAMES:
            slot = slots.get((tf, side))
            if slot is None or tf not in rows or slot.get('status') == 'MISSING_INPUT':
                missing_tfs.append(tf)
                errors.append('MISSING_INPUT:' + tf)
                continue
            state = slot.get('status')
            if state == 'INACTIVE_TARGET':
                if active.get((tf, side)) is not False or slot.get('score') is not None:
                    errors.append('INACTIVE_TARGET_MISMATCH:' + tf)
                inactive_tfs.append(tf)
                continue
            if state != 'SCORED':
                errors.append('INVALID_SLOT_STATUS:' + tf)
                missing_tfs.append(tf)
                continue
            active_tfs.append(tf)
            score, target = slot.get('score'), slot.get('target_price')
            if not legacy._finite(score) or not 0 <= score <= 100:
                errors.append('INVALID_SCORE:' + tf)
            else:
                values[tf] = score
            if (active.get((tf, side)) is not True or not legacy._finite(target)
                    or target != targets.get((tf, side))):
                errors.append('SCORED_TARGET_DIRECTION_MISMATCH:' + tf)
            components = slot.get('components')
            if not isinstance(components, Mapping) or not all(legacy._finite(components.get(key)) for key in ADDITIVE_COMPONENTS):
                errors.append('INVALID_ADDITIVE_COMPONENTS:' + tf)
            elif legacy._finite(score):
                # Exactly the original item calculation-validation check, which
                # is deliberately tighter than the intake's 0.011 tolerance.
                component_sum = round(sum(float(components[key]) for key in ADDITIVE_COMPONENTS), 2)
                if abs(component_sum - score) > 0.01:
                    errors.append('SCORE_COMPONENT_MISMATCH:' + tf)
            recorded_hits, recorded_total = _count(slot.get('consensus_hits')), _count(slot.get('consensus_total'))
            if (recorded_hits is None or recorded_total is None or recorded_hits > recorded_total
                    or (recorded_hits, recorded_total) != (hits, total)):
                errors.append('CONSENSUS_MISMATCH:' + tf)
            cluster_count = _count(slot.get('cluster_count'))
            if cluster_count is None or recorded_hits is None or cluster_count > recorded_hits:
                errors.append('CLUSTER_COUNT_VALIDATION:' + tf)
        if not active_tfs:
            errors.append('NO_ACTIVE_SOURCE_TARGET')
        if total == 0:
            errors.append('NO_VALID_CONSENSUS_DENOMINATOR')
        ordered_values = [float(values[tf]) for tf in TIMEFRAMES if tf in values]
        score_sum = sum(ordered_values) if ordered_values else None
        average = round(score_sum / len(ordered_values), 2) if ordered_values else None
        result[side] = {'source_side': side, 'timeframe_values': values,
            'active_timeframes': active_tfs, 'inactive_timeframes': inactive_tfs,
            'missing_timeframes': missing_tfs, 'denominator': len(ordered_values),
            'score_sum': score_sum, 'rounded_average': average,
            'consensus_hits': hits, 'consensus_total': total,
            'consensus_by_timeframe': closest, 'source_quotes_by_timeframe': quotes,
            'validation_status': 'UNAVAILABLE' if errors else 'VALID',
            'validation_reasons': sorted(set(errors))}
    return result


def evaluate_coin(observation: dict) -> dict:
    payload = legacy.evaluate_coin(observation)
    profiles = _profiles(observation)
    provenance = {}
    for base in DIRECTIONS:
        side = _opposite(base)
        own, opposite = profiles[side], profiles[base]
        provenance[base] = {**own, 'opposite_coverage': opposite}
        direction = payload['features_by_direction'][base]
        features, unavailable = direction['features'], direction['unavailable_features']
        if own['validation_status'] == 'VALID':
            features.update({MAPPING: True, AVERAGE: own['rounded_average'],
                             CONSENSUS: own['consensus_hits'] == own['consensus_total']})
            if opposite['validation_status'] == 'VALID':
                features[OPPOSITE_AVERAGE] = opposite['rounded_average']
            else:
                unavailable[OPPOSITE_AVERAGE] = opposite['validation_reasons']
        else:
            for feature in (MAPPING, AVERAGE, OPPOSITE_AVERAGE, CONSENSUS):
                unavailable[feature] = own['validation_reasons']
    evaluations = []
    for candidate in catalog_records():
        if not candidate['supported']:
            continue
        for base in DIRECTIONS:
            evaluations.append({'candidate_key': candidate['candidate_key'], 'base_direction': base,
                'analysis_direction': _opposite(base) if candidate['orientation'] == 'INVERSE' else base,
                **legacy.evaluate_candidate(candidate['definition'], payload['features_by_direction'][base]['features'])})
    payload.pop('feature_sha256')
    payload.update(version=VERSION, feature_version=FEATURE_VERSION, evaluations=evaluations,
                   maxpain_provenance_by_direction=provenance)
    payload = legacy._normalized_payload(payload)
    payload['feature_sha256'] = digest(payload)
    return payload
