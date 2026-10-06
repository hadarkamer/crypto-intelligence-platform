"""Original score-change predicates under an explicit all-Watch predecessor policy.

This population intentionally differs from delivered-alert history. The nearest
eligible distinct captured scan is used for every module; missing model evidence
never causes a search for an older, more convenient value. No events are created.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Mapping

import research_watch_scan_formula_asset_context as previous

legacy = previous.legacy
measurement = previous.measurement
VERSION = 'watch-scan-formulas-v5-score-change'
PREVIOUS_VERSION = previous.VERSION
FEATURE_VERSION = 'watch-captured-total-maxpain-btc-asset-and-score-change-features-v5'
SCORE_CHANGE_CONTEXT_VERSION = 'watch-prior-scan-aligned-total-change-30m-v1'
SELECTION_VERSION = 'watch-latest-earlier-distinct-scan-30m-no-skip-v1'
CONTEXT_REQUIRED = True
ASSET_CONTEXT_REQUIRED = True
SCORE_CHANGE_CONTEXT_REQUIRED = True
BTC_CONTEXT_PREDECESSORS = (previous.VERSION, previous.previous.VERSION)
ASSET_CONTEXT_PREDECESSORS = (previous.VERSION,)
CATALOG_SHA256 = previous.CATALOG_SHA256
CATALOG_SIZE = 298
SUPPORTED_COUNT = 170
DIRECTIONS = previous.DIRECTIONS
MODULES = ('price_oi', 'futures_cvd', 'spot_cvd')
SCORE_CHANGE_FEATURES = frozenset('sequence.30m.'+name+'.score_change' for name in MODULES)
SUPPORTED_FEATURES = previous.SUPPORTED_FEATURES | SCORE_CHANGE_FEATURES
canonical = previous.canonical
digest = previous.digest
build_btc_context = previous.build_btc_context
build_asset_context = previous.build_asset_context


def catalog_records() -> list[dict]:
    rows = []
    for row in previous.catalog_records():
        missing = sorted({item['feature'] for item in
            legacy.existing._conditions(row['definition'])} - SUPPORTED_FEATURES)
        rows.append({**row, 'supported': not missing, 'unsupported_features': missing})
    if len(rows) != CATALOG_SIZE or sum(row['supported'] for row in rows) != SUPPORTED_COUNT:
        raise ValueError('SCORE_CHANGE_CATALOG_SUPPORT_DRIFT')
    return rows


def _identity(observation: Mapping) -> dict:
    identity = previous._identity(observation)
    watch_id = observation.get('watch_scan_id')
    if not isinstance(watch_id, str) or not watch_id.strip():
        raise ValueError('INVALID_SCORE_CHANGE_WATCH_IDENTITY')
    return {**identity, 'watch_scan_id': watch_id,
        'intake_status': observation['intake_status'],
        **{key: measurement.utc(observation[key]) for key in ('observed_at_utc',
            'source_available_at_utc', 'source_created_at_utc')}}


def _model_evidence(observation: Mapping) -> dict:
    payload = legacy.evaluate_coin(dict(observation))
    keys = {name+'.aligned_score' for name in MODULES}
    return {base: {category: {key: value for key, value in
        payload['features_by_direction'][base][category].items() if key in keys}
        for category in ('features', 'unavailable_features')} for base in DIRECTIONS}


def _predecessor(observation: Mapping, current: Mapping) -> dict:
    identity = _identity(observation)
    if any(identity[key] != current[key] for key in ('consumer_version', 'population_version', 'source_version', 'symbol')):
        raise ValueError('SCORE_CHANGE_PREDECESSOR_POPULATION_MISMATCH')
    if (identity['snapshot_set_id'] == current['snapshot_set_id']
            or identity['watch_scan_id'] == current['watch_scan_id']):
        raise ValueError('SCORE_CHANGE_PREDECESSOR_NOT_DISTINCT_SCAN')
    if (not current['usable_from_utc']-timedelta(minutes=30) <= identity['usable_from_utc'] < current['usable_from_utc']
            or identity['observed_at_utc'] >= current['observed_at_utc']):
        raise ValueError('SCORE_CHANGE_PREDECESSOR_NOT_CAUSAL_30M')
    model_evidence = _model_evidence(observation)
    # Keep enough original source evidence to revalidate both the signed
    # totals and their availability. Unrelated MaxPain rows are not copied.
    source = {**identity,
        'models': {key: observation['models'].get(key) for key, _ in legacy.MODULES.values()},
        'sources': {key: observation['sources'].get(key) for key in
            ('positioning', 'futures', 'spot', 'timing_observation')},
        'source_time_errors': observation['source_time_errors']}
    return {'snapshot_set_id': identity['snapshot_set_id'],
        'observation': legacy._normalized_payload(source), 'model_evidence': model_evidence}


def _derived(current: Mapping, predecessors: list, selection_status: str) -> tuple[dict, dict]:
    features, unavailable = {}, {}
    for base in DIRECTIONS:
        values, missing = {}, {}
        for name in MODULES:
            key, total = 'sequence.30m.'+name+'.score_change', name+'.aligned_score'
            reasons = []
            if total not in current[base]['features']:
                reasons.extend('CURRENT:'+reason for reason in current[base]['unavailable_features'].get(total, ['MISSING_TOTAL']))
            if selection_status == 'NO_PREDECESSOR':
                reasons.append('NO_CAUSAL_PREDECESSOR_IN_30M')
            elif selection_status == 'AMBIGUOUS':
                reasons.append('AMBIGUOUS_LATEST_PREDECESSOR_TIMESTAMP')
            elif total not in predecessors[0]['model_evidence'][base]['features']:
                reasons.extend('PREDECESSOR:'+reason for reason in
                    predecessors[0]['model_evidence'][base]['unavailable_features'].get(total, ['MISSING_TOTAL']))
            if reasons:
                missing[key] = sorted(set(reasons))
            else:
                values[key] = current[base]['features'][total]-predecessors[0]['model_evidence'][base]['features'][total]
        features[base], unavailable[base] = values, missing
    return features, unavailable


def build_score_change_context(observation: Mapping, predecessors: list[dict], *, computed_at) -> dict:
    """Freeze the nearest eligible source, supplied by a bounded metadata query.

    The caller supplies the latest two eligible candidates, so a tie at the
    latest usable time can be detected without scanning an unbounded history.
    Candidate eligibility is rechecked here. The older second candidate is
    discarded; missing fields in the nearest candidate are never bypassed.
    """
    identity = _identity(observation)
    if measurement.utc(computed_at) < identity['usable_from_utc']:
        raise ValueError('SCORE_CHANGE_COMPUTED_BEFORE_SOURCE_USABILITY')
    if not isinstance(predecessors, list) or len(predecessors) > 2:
        raise ValueError('INVALID_SCORE_CHANGE_PREDECESSOR_BUDGET')
    proofs = [_predecessor(item, identity) for item in predecessors]
    if len({item['observation']['snapshot_set_id'] for item in proofs}) != len(proofs):
        raise ValueError('DUPLICATE_SCORE_CHANGE_PREDECESSOR')
    proofs.sort(key=lambda item: (measurement.utc(item['observation']['usable_from_utc']),
        item['observation']['watch_scan_id'], item['observation']['snapshot_set_id']), reverse=True)
    if not proofs:
        selection = 'NO_PREDECESSOR'
    elif len(proofs) == 2 and proofs[0]['observation']['usable_from_utc'] == proofs[1]['observation']['usable_from_utc']:
        selection = 'AMBIGUOUS'
    else:
        selection, proofs = 'SELECTED', proofs[:1]
    current = _model_evidence(observation)
    features, unavailable = _derived(current, proofs, selection)
    known = sum(len(values) for values in features.values())
    status = 'READY' if known == len(MODULES)*len(DIRECTIONS) else 'PARTIAL' if known else 'DATA_MISSING'
    result = legacy._normalized_payload({**identity, 'version': SCORE_CHANGE_CONTEXT_VERSION,
        'selection_version': SELECTION_VERSION, 'window_minutes': 30,
        'window_start_utc': identity['usable_from_utc']-timedelta(minutes=30),
        'window_end_exclusive_utc': identity['usable_from_utc'],
        'current_model_evidence': current, 'predecessors': proofs,
        'selection_status': selection, 'features_by_direction': features,
        'unavailable_features_by_direction': unavailable, 'status': status})
    result['context_sha256'] = digest(result)
    return result


def _validate_context(observation: Mapping, context: Mapping) -> dict:
    if not isinstance(context, Mapping):
        raise ValueError('INVALID_SCORE_CHANGE_CONTEXT')
    identity = legacy._normalized_payload(_identity(observation))
    if any(context.get(key) != value for key, value in identity.items()):
        raise ValueError('SCORE_CHANGE_CONTEXT_IDENTITY_MISMATCH')
    if (not measurement._hash(context.get('context_sha256'))
            or digest({key: value for key, value in context.items() if key != 'context_sha256'}) != context['context_sha256']):
        raise ValueError('SCORE_CHANGE_CONTEXT_HASH_MISMATCH')
    proofs = context.get('predecessors')
    if (not isinstance(proofs, list) or not all(isinstance(proof, Mapping)
            and isinstance(proof.get('observation'), Mapping) for proof in proofs)):
        raise ValueError('INVALID_SCORE_CHANGE_PREDECESSOR_PROOF')
    expected = build_score_change_context(observation, [proof['observation'] for proof in proofs],
        computed_at=identity['usable_from_utc'])
    if expected != context:
        raise ValueError('SCORE_CHANGE_CONTEXT_PROOF_MISMATCH')
    return expected


def evaluate_coin(observation: dict, *, btc_context=None, asset_context=None, score_change_context=None) -> dict:
    payload = previous.evaluate_coin(observation, btc_context=btc_context, asset_context=asset_context)
    context = _validate_context(observation, score_change_context) if score_change_context is not None else None
    for base in DIRECTIONS:
        direction = payload['features_by_direction'][base]
        if context is None:
            direction['unavailable_features'].update({key: ['MISSING_SCORE_CHANGE_CONTEXT'] for key in sorted(SCORE_CHANGE_FEATURES)})
        else:
            direction['features'].update(context['features_by_direction'][base])
            direction['unavailable_features'].update(context['unavailable_features_by_direction'][base])
    evaluations = []
    for candidate in catalog_records():
        if not candidate['supported']:
            continue
        for base in DIRECTIONS:
            evaluations.append({'candidate_key': candidate['candidate_key'], 'base_direction': base,
                'analysis_direction': ('SHORT' if base == 'LONG' else 'LONG') if candidate['orientation'] == 'INVERSE' else base,
                **legacy.evaluate_candidate(candidate['definition'], payload['features_by_direction'][base]['features'])})
    payload.pop('feature_sha256')
    payload.update(version=VERSION, feature_version=FEATURE_VERSION, evaluations=evaluations,
        score_change_context_provenance=context)
    payload = legacy._normalized_payload(payload)
    payload['feature_sha256'] = digest(payload)
    return payload


def validate_score_change_context(observation: Mapping, context: Mapping) -> dict:
    """Public validation before a sibling reuses the scan's frozen source IDs."""
    return _validate_context(observation, context)
