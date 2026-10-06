"""Existing Watch predicates plus causal, shared BTC Spot price context.

Only the original BTC prior-hour direction and four-hour regime are exposed.
The cutoff is source usability, before the delayed research entry. All math
and window proof come from the existing past-price method; no provider, event,
outcome or database is read here.
"""
from __future__ import annotations

from typing import Mapping

import research_past_price_features as past
import research_watch_scan_formula_maxpain as previous

legacy = previous.legacy
measurement = legacy.measurement
VERSION = 'watch-scan-formulas-v3-btc-context'
PREVIOUS_VERSION = previous.VERSION
FEATURE_VERSION = 'watch-captured-total-maxpain-and-btc-context-features-v3'
CONTEXT_VERSION = 'watch-prior-btc-spot-closed-1m-v1'
CONTEXT_REQUIRED = True
CATALOG_SHA256 = previous.CATALOG_SHA256
CATALOG_SIZE = 298
SUPPORTED_COUNT = 106
DIRECTIONS = previous.DIRECTIONS
BTC_DIRECTION = 'historical.closed_1m.1h.btc_direction'
BTC_REGIME = 'historical.closed_1m.4h.btc_market_regime'
CONTEXT_FEATURES = frozenset((BTC_DIRECTION, BTC_REGIME))
SUPPORTED_FEATURES = previous.SUPPORTED_FEATURES | CONTEXT_FEATURES
WINDOWS = {'1h': 60, '4h': 240}
BOUNDARY_POLICY = 'LAST_FULL_CLOSED_MINUTE_BEFORE_SOURCE_USABILITY_EXCLUDE_DECISION_MINUTE'
canonical = previous.canonical
digest = previous.digest


def catalog_records() -> list[dict]:
    rows = []
    for row in previous.catalog_records():
        missing = sorted({item['feature'] for item in
            legacy.existing._conditions(row['definition'])} - SUPPORTED_FEATURES)
        rows.append({**row, 'supported': not missing, 'unsupported_features': missing})
    if len(rows) != CATALOG_SIZE or sum(row['supported'] for row in rows) != SUPPORTED_COUNT:
        raise ValueError('BTC_CONTEXT_CATALOG_SUPPORT_DRIFT')
    return rows


def _identity(observation: Mapping) -> dict:
    if (not isinstance(observation, Mapping)
            or observation.get('consumer_version') != measurement.CONSUMER_VERSION
            or observation.get('population_version') != measurement.POPULATION
            or observation.get('source_version') != measurement.SOURCE_VERSION
            or observation.get('intake_status') != 'ACCEPTED'
            or type(observation.get('snapshot_set_id')) is not int
            or observation['snapshot_set_id'] <= 0
            or not measurement._hash(observation.get('bundle_sha256'))
            or not measurement._hash(observation.get('parent_payload_sha256'))):
        raise ValueError('INVALID_BTC_CONTEXT_SOURCE_IDENTITY')
    observed = measurement.utc(observation['observed_at_utc'])
    available = measurement.utc(observation['source_available_at_utc'])
    created = measurement.utc(observation['source_created_at_utc'])
    usable = measurement.utc(observation['usable_from_utc'])
    if observed > available or usable != max(observed, available, created):
        raise ValueError('INVALID_BTC_CONTEXT_SOURCE_AVAILABILITY')
    return {**{key: observation[key] for key in ('consumer_version', 'population_version',
        'source_version', 'snapshot_set_id', 'bundle_sha256', 'parent_payload_sha256')},
        'usable_from_utc': usable}


def _feature_proof(windows: Mapping, usable) -> tuple[dict, dict]:
    # The original flattener independently checks each window's route, exact
    # boundary, sample count, finite statistics, direction and regime policy.
    # Using BTC for both sides produces the same shared BTC context for every
    # observation symbol without inventing an asset's Spot/perpetual history.
    record = {'method_version': past.METHOD_VERSION, 'symbol': 'BTC',
        'event_time_utc': usable, 'windows': {
            name: {'asset': window, 'btc': window} for name, window in windows.items()}}
    flattened = past.flatten_event_features(record, 'LONG')
    features = {key: flattened[key] for key in sorted(CONTEXT_FEATURES) if key in flattened}
    unavailable = {}
    for name, feature in (('1h', BTC_DIRECTION), ('4h', BTC_REGIME)):
        if feature not in features:
            unavailable[feature] = [str(windows[name].get('missing_reason')
                or 'INVALID_OR_INCOMPLETE_BTC_CLOSED_1M_LOOKBACK')]
    return features, unavailable


def _status(features: Mapping) -> str:
    return 'READY' if len(features) == len(CONTEXT_FEATURES) else 'PARTIAL' if features else 'DATA_MISSING'


def build_btc_context(observation: Mapping, path: Mapping, *, computed_at) -> dict:
    """Freeze two independently validated BTC windows at source usability.

    Identity deliberately excludes the observation coin: one context belongs
    to the immutable scan and is shared by its eight coin samples. Computation
    time proves this calculation is not premature but does not affect its hash.
    """
    identity = _identity(observation)
    usable = identity['usable_from_utc']
    if measurement.utc(computed_at) < usable:
        raise ValueError('BTC_CONTEXT_COMPUTED_BEFORE_SOURCE_USABILITY')
    route_error = None
    try:
        if not isinstance(path, Mapping):
            raise ValueError('INVALID_PATH')
        measurement._source('BTC', path)
    except (ValueError, TypeError, KeyError, AttributeError):
        route_error = 'INVALID_BTC_SPOT_ARCHIVE_ROUTE'
    accepted_path = path if route_error is None else {}
    candles = list(accepted_path.get('candles') or ())
    windows = {name: past._window(symbol='BTC', event_time=usable, minutes=minutes,
        candles=candles, path_result=accepted_path) for name, minutes in WINDOWS.items()}
    features, unavailable = _feature_proof(windows, usable)
    result = legacy._normalized_payload({**identity, 'version': CONTEXT_VERSION,
        'method_version': past.METHOD_VERSION, 'market_regime_version': past.REGIME_VERSION,
        'boundary_policy': BOUNDARY_POLICY, 'cutoff_utc': usable.replace(second=0, microsecond=0),
        'price_route': measurement.BINANCE_SPOT, 'route_validation_reason': route_error,
        'windows': windows, 'features': features, 'unavailable_features': unavailable,
        'status': _status(features)})
    result['context_sha256'] = digest(result)
    return result


def _validate_context(observation: Mapping, context: Mapping) -> dict:
    if not isinstance(context, Mapping):
        raise ValueError('INVALID_BTC_CONTEXT')
    expected = legacy._normalized_payload(_identity(observation))
    if any(context.get(key) != value for key, value in expected.items()):
        raise ValueError('BTC_CONTEXT_IDENTITY_MISMATCH')
    if (not measurement._hash(context.get('context_sha256'))
            or digest({key: value for key, value in context.items() if key != 'context_sha256'})
                != context['context_sha256']):
        raise ValueError('BTC_CONTEXT_HASH_MISMATCH')
    usable = measurement.utc(expected['usable_from_utc'])
    contracts = {'version': CONTEXT_VERSION, 'method_version': past.METHOD_VERSION,
        'market_regime_version': past.REGIME_VERSION, 'boundary_policy': BOUNDARY_POLICY,
        'price_route': measurement.BINANCE_SPOT,
        'cutoff_utc': usable.replace(second=0, microsecond=0).isoformat()}
    if any(context.get(key) != value for key, value in contracts.items()):
        raise ValueError('BTC_CONTEXT_CONTRACT_MISMATCH')
    windows = context.get('windows')
    if (not isinstance(windows, Mapping) or set(windows) != set(WINDOWS)
            or not all(isinstance(window, Mapping) for window in windows.values())):
        raise ValueError('BTC_CONTEXT_WINDOW_COVERAGE_MISMATCH')
    features, unavailable = _feature_proof(windows, usable)
    if (context.get('features') != features or context.get('unavailable_features') != unavailable
            or context.get('status') != _status(features)
            or context.get('route_validation_reason') not in (None, 'INVALID_BTC_SPOT_ARCHIVE_ROUTE')
            or (context.get('route_validation_reason') is not None and features)):
        raise ValueError('BTC_CONTEXT_FEATURE_PROOF_MISMATCH')
    return legacy._normalized_payload(dict(context))


def evaluate_coin(observation: dict, *, btc_context=None) -> dict:
    payload = previous.evaluate_coin(observation)
    context = _validate_context(observation, btc_context) if btc_context is not None else None
    for base in DIRECTIONS:
        direction = payload['features_by_direction'][base]
        if context is None:
            direction['unavailable_features'].update({key: ['MISSING_BTC_CONTEXT']
                for key in sorted(CONTEXT_FEATURES)})
        else:
            direction['features'].update(context['features'])
            direction['unavailable_features'].update(context['unavailable_features'])
    evaluations = []
    for candidate in catalog_records():
        if not candidate['supported']:
            continue
        for base in DIRECTIONS:
            evaluations.append({'candidate_key': candidate['candidate_key'], 'base_direction': base,
                'analysis_direction': previous._opposite(base) if candidate['orientation'] == 'INVERSE' else base,
                **legacy.evaluate_candidate(candidate['definition'], payload['features_by_direction'][base]['features'])})
    payload.pop('feature_sha256')
    payload.update(version=VERSION, feature_version=FEATURE_VERSION, evaluations=evaluations,
                   btc_context_provenance=context)
    payload = legacy._normalized_payload(payload)
    payload['feature_sha256'] = digest(payload)
    return payload
