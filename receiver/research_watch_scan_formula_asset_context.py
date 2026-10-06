"""Original prior-asset Spot predicates from immutable, closed-minute evidence.

The established BTC context is reused unchanged, including missing windows.
HYPE's active perpetual price route is not the original Spot feature contract.
No database, provider, outcome, event, score or definition is changed here.
"""
from __future__ import annotations

from typing import Mapping

import research_watch_scan_formula_btc_context as previous

legacy = previous.legacy
measurement = previous.measurement
past = previous.past
VERSION = 'watch-scan-formulas-v4-asset-context'
PREVIOUS_VERSION = previous.VERSION
FEATURE_VERSION = 'watch-captured-total-maxpain-btc-and-asset-context-features-v4'
ASSET_CONTEXT_VERSION = 'watch-prior-asset-spot-closed-1m-v1'
CONTEXT_REQUIRED = True
BTC_CONTEXT_PREDECESSORS = (previous.VERSION,)
ASSET_CONTEXT_REQUIRED = True
CATALOG_SHA256 = previous.CATALOG_SHA256
CATALOG_SIZE = 298
SUPPORTED_COUNT = 158
DIRECTIONS = previous.DIRECTIONS
WINDOWS = dict(past.LOOKBACKS)
ASSET_PRICE_SYMBOLS = measurement.SPOT_SYMBOLS
BOUNDARY_POLICY = previous.BOUNDARY_POLICY
HYPE_UNAVAILABLE = 'HYPE_SPOT_LOOKBACK_NOT_ENABLED_FOR_WATCH'
PREFIX = 'historical.closed_1m.'
ASSET_FEATURES = frozenset([PREFIX+name+'.direction' for name in WINDOWS] + [
    PREFIX+'4h.market_regime', PREFIX+'1h.relative_strength_pct', PREFIX+'1h.alignment'])
SUPPORTED_FEATURES = previous.SUPPORTED_FEATURES | ASSET_FEATURES
canonical = previous.canonical
digest = previous.digest
build_btc_context = previous.build_btc_context


def catalog_records() -> list[dict]:
    rows = []
    for row in previous.catalog_records():
        missing = sorted({item['feature'] for item in
            legacy.existing._conditions(row['definition'])} - SUPPORTED_FEATURES)
        rows.append({**row, 'supported': not missing, 'unsupported_features': missing})
    if len(rows) != CATALOG_SIZE or sum(row['supported'] for row in rows) != SUPPORTED_COUNT:
        raise ValueError('ASSET_CONTEXT_CATALOG_SUPPORT_DRIFT')
    return rows


def _identity(observation: Mapping) -> dict:
    identity = previous._identity(observation)
    symbol = observation.get('symbol')
    if symbol not in (*ASSET_PRICE_SYMBOLS, 'HYPE'):
        raise ValueError('INVALID_ASSET_CONTEXT_SYMBOL')
    return {**identity, 'symbol': symbol}


def _proof(windows: Mapping, btc_context: Mapping, identity: Mapping) -> tuple[dict, dict]:
    # Reuse the original validator/flattener. BTC is needed only for relative1h;
    # exposing only the nine new keys cannot overwrite any existing feature.
    record = {'method_version': past.METHOD_VERSION, 'symbol': identity['symbol'],
        'event_time_utc': identity['usable_from_utc'], 'windows': {
            name: {'asset': window, 'btc': btc_context['windows'].get(name, {})}
            for name, window in windows.items()}}
    features_by_direction, unavailable_by_direction = {}, {}
    for base in DIRECTIONS:
        flattened = past.flatten_event_features(record, base)
        features = {key: flattened[key] for key in sorted(ASSET_FEATURES) if key in flattened}
        unavailable = {}
        for key in sorted(ASSET_FEATURES - features.keys()):
            name = key[len(PREFIX):].split('.')[0]
            reasons = []
            if identity['symbol'] == 'HYPE':
                reasons.append(HYPE_UNAVAILABLE)
            else:
                if windows[name].get('status') != 'READY' or not features.get(PREFIX+name+'.direction'):
                    reasons.append(str(windows[name].get('missing_reason')
                        or 'INVALID_OR_INCOMPLETE_ASSET_CLOSED_1M_LOOKBACK'))
                if key.endswith('.relative_strength_pct') and previous.BTC_DIRECTION not in btc_context['features']:
                    reasons.extend(btc_context['unavailable_features'].get(previous.BTC_DIRECTION)
                        or ['INVALID_OR_INCOMPLETE_BTC_CLOSED_1M_LOOKBACK'])
                if not reasons:
                    reasons.append('INVALID_OR_INCOMPLETE_ASSET_CLOSED_1M_LOOKBACK')
            unavailable[key] = sorted(set(reasons))
        features_by_direction[base] = features
        unavailable_by_direction[base] = unavailable
    return features_by_direction, unavailable_by_direction


def _status(features_by_direction: Mapping) -> str:
    count = sum(len(features_by_direction[base]) for base in DIRECTIONS)
    return 'READY' if count == len(ASSET_FEATURES)*len(DIRECTIONS) else 'PARTIAL' if count else 'DATA_MISSING'


def build_asset_context(observation: Mapping, path: Mapping | None, *, btc_context, computed_at) -> dict:
    identity = _identity(observation)
    usable, symbol = identity['usable_from_utc'], identity['symbol']
    if measurement.utc(computed_at) < usable:
        raise ValueError('ASSET_CONTEXT_COMPUTED_BEFORE_SOURCE_USABILITY')
    frozen_btc = previous._validate_context(observation, btc_context)
    route_error = HYPE_UNAVAILABLE if symbol == 'HYPE' else None
    if route_error is None:
        try:
            if not isinstance(path, Mapping):
                raise ValueError('INVALID_PATH')
            measurement._source(symbol, path)
        except (ValueError, TypeError, KeyError, AttributeError):
            route_error = 'INVALID_ASSET_SPOT_ARCHIVE_ROUTE'
    accepted = path if route_error is None else {}
    candles = list(accepted.get('candles') or ())
    windows = {}
    for name, minutes in WINDOWS.items():
        if symbol == 'BTC' and name in previous.WINDOWS:
            # Even a missing old window is immutable. Reusing it also makes
            # BTC relative to itself exactly zero whenever its hour is known.
            windows[name] = frozen_btc['windows'][name]
        else:
            window = past._window(symbol=symbol, event_time=usable, minutes=minutes,
                candles=candles, path_result=accepted)
            if route_error is not None:
                window['missing_reason'] = route_error
            windows[name] = window
    features, unavailable = _proof(windows, frozen_btc, identity)
    result = legacy._normalized_payload({**identity, 'version': ASSET_CONTEXT_VERSION,
        'method_version': past.METHOD_VERSION, 'market_regime_version': past.REGIME_VERSION,
        'boundary_policy': BOUNDARY_POLICY, 'cutoff_utc': usable.replace(second=0, microsecond=0),
        'price_route': measurement.BINANCE_SPOT if symbol in ASSET_PRICE_SYMBOLS else None,
        'route_validation_reason': route_error, 'btc_context_sha256': frozen_btc['context_sha256'],
        'windows': windows, 'features_by_direction': features,
        'unavailable_features_by_direction': unavailable, 'status': _status(features)})
    result['context_sha256'] = digest(result)
    return result


def _validate_context(observation: Mapping, context: Mapping, btc_context: Mapping) -> dict:
    if not isinstance(context, Mapping):
        raise ValueError('INVALID_ASSET_CONTEXT')
    expected = legacy._normalized_payload(_identity(observation))
    if any(context.get(key) != value for key, value in expected.items()):
        raise ValueError('ASSET_CONTEXT_IDENTITY_MISMATCH')
    if (not measurement._hash(context.get('context_sha256'))
            or digest({key: value for key, value in context.items() if key != 'context_sha256'})
                != context['context_sha256']):
        raise ValueError('ASSET_CONTEXT_HASH_MISMATCH')
    frozen_btc = previous._validate_context(observation, btc_context)
    symbol = expected['symbol']
    contracts = {'version': ASSET_CONTEXT_VERSION, 'method_version': past.METHOD_VERSION,
        'market_regime_version': past.REGIME_VERSION, 'boundary_policy': BOUNDARY_POLICY,
        'price_route': measurement.BINANCE_SPOT if symbol in ASSET_PRICE_SYMBOLS else None,
        'btc_context_sha256': frozen_btc['context_sha256'],
        'cutoff_utc': measurement.utc(expected['usable_from_utc']).replace(second=0, microsecond=0).isoformat()}
    if any(context.get(key) != value for key, value in contracts.items()):
        raise ValueError('ASSET_CONTEXT_CONTRACT_MISMATCH')
    windows = context.get('windows')
    if (not isinstance(windows, Mapping) or set(windows) != set(WINDOWS)
            or not all(isinstance(window, Mapping) for window in windows.values())):
        raise ValueError('ASSET_CONTEXT_WINDOW_COVERAGE_MISMATCH')
    if symbol == 'BTC' and any(windows[name] != frozen_btc['windows'][name] for name in previous.WINDOWS):
        raise ValueError('ASSET_CONTEXT_BTC_WINDOW_REUSE_MISMATCH')
    features, unavailable = _proof(windows, frozen_btc, expected)
    reason = context.get('route_validation_reason')
    valid_reason = reason == HYPE_UNAVAILABLE if symbol == 'HYPE' else reason in (None, 'INVALID_ASSET_SPOT_ARCHIVE_ROUTE')
    # An invalid own route cannot supply windows. BTC's two independently
    # proven frozen windows remain usable despite an invalid new archive read.
    accepted_from_frozen = set(previous.WINDOWS) if symbol == 'BTC' else set()
    invalid_route_features = reason is not None and any(
        key[len(PREFIX):].split('.')[0] not in accepted_from_frozen
        for values in features.values() for key in values)
    if (not valid_reason or invalid_route_features
            or context.get('features_by_direction') != features
            or context.get('unavailable_features_by_direction') != unavailable
            or context.get('status') != _status(features)):
        raise ValueError('ASSET_CONTEXT_FEATURE_PROOF_MISMATCH')
    return legacy._normalized_payload(dict(context))


def evaluate_coin(observation: dict, *, btc_context=None, asset_context=None) -> dict:
    payload = previous.evaluate_coin(observation, btc_context=btc_context)
    context = _validate_context(observation, asset_context, btc_context) if asset_context is not None else None
    for base in DIRECTIONS:
        direction = payload['features_by_direction'][base]
        if context is None:
            direction['unavailable_features'].update({key: ['MISSING_ASSET_CONTEXT'] for key in sorted(ASSET_FEATURES)})
        else:
            direction['features'].update(context['features_by_direction'][base])
            direction['unavailable_features'].update(context['unavailable_features_by_direction'][base])
    evaluations = []
    for candidate in catalog_records():
        if not candidate['supported']:
            continue
        for base in DIRECTIONS:
            evaluations.append({'candidate_key': candidate['candidate_key'], 'base_direction': base,
                'analysis_direction': previous.previous._opposite(base) if candidate['orientation'] == 'INVERSE' else base,
                **legacy.evaluate_candidate(candidate['definition'], payload['features_by_direction'][base]['features'])})
    payload.pop('feature_sha256')
    payload.update(version=VERSION, feature_version=FEATURE_VERSION, evaluations=evaluations,
        asset_context_provenance=context)
    payload = legacy._normalized_payload(payload)
    payload['feature_sha256'] = digest(payload)
    return payload
