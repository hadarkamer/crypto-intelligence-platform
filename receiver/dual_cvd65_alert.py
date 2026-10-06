"""Pure, explicitly experimental delivery screen for the existing dual-CVD rule.

Only one frozen operational bundle is inspected. No scoring, prices, events,
historical backfill, statistical acceptance or network calls belong here.
"""
from __future__ import annotations

from html import escape
import hashlib
import json
from typing import Mapping
from zoneinfo import ZoneInfo

import research_watch_scan_formula as source_features

RULE_ID = 'captured-question-search-v3-experimental-binding:CORE_FUTURES_CVD_SPOT_CVD_TOTAL_65'
VERSION = 'dual-cvd65-experimental-watch-v1'
CAPTURE_VERSION = 'watch-operational-scores-v2'
CAPTURE_HASH_VERSION = 'json-integer-float-zero-normalized-v1'
CAPTURE_POPULATION = 'all-top8-watch-scans-before-display-v1'
SYMBOLS = ('BTC', 'ETH', 'SOL', 'HYPE', 'DOGE', 'ZEC', 'BNB', 'XRP')
MAX_BUNDLE_BYTES = 256 * 1024 + 100
MAX_CVD_AGE_MINUTES = 30
MIN_SCORE = 65
_ISRAEL = ZoneInfo('Asia/Jerusalem')


def _canonical(value):
    return json.dumps(source_features._normalized_payload(value), sort_keys=True,
        separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _utc(value):
    return source_features.measurement.utc(value)


def _observation(symbol, *, status='UNKNOWN', direction=None, futures=None, spot=None, reasons=()):
    result = {'symbol': symbol, 'status': status, 'direction': direction,
        'futures_score': futures.get('score') if futures else None,
        'spot_score': spot.get('score') if spot else None,
        'futures_close_at_utc': futures.get('close') if futures else None,
        'spot_close_at_utc': spot.get('close') if spot else None,
        'generation_key': None, 'reasons': sorted(set(reasons))}
    if status == 'MATCH':
        result['generation_key'] = _digest({'symbol': symbol, 'direction': direction,
            'futures_close_at_utc': result['futures_close_at_utc'],
            'spot_close_at_utc': result['spot_close_at_utc']})
    return result


def _result(observations, *, watch_scan_id=None, source_at_utc=None, bundle_sha256=None):
    return {'rule_id': RULE_ID, 'version': VERSION, 'watch_scan_id': watch_scan_id,
        'source_at_utc': source_at_utc, 'bundle_sha256': bundle_sha256,
        'observations': observations,
        'counts': {status: sum(row['status'] == status for row in observations)
            for status in ('MATCH', 'NO_MATCH', 'UNKNOWN')}}


def _validate_bundle(bundle, observed):
    if not isinstance(bundle, dict):
        raise ValueError('INVALID_BUNDLE')
    if (bundle.get('version') != CAPTURE_VERSION or bundle.get('hash_version') != CAPTURE_HASH_VERSION
            or bundle.get('population') != CAPTURE_POPULATION
            or bundle.get('status') not in ('COMPLETE', 'PARTIAL')):
        raise ValueError('UNSUPPORTED_CAPTURE_CONTRACT')
    if (not isinstance(bundle.get('cycle_id'), str) or not bundle['cycle_id'].strip()
            or not isinstance(bundle.get('coins'), dict) or set(bundle['coins']) != set(SYMBOLS)
            or not isinstance(bundle.get('symbols_expected'), list)
            or sorted(bundle['symbols_expected']) != sorted(SYMBOLS)):
        raise ValueError('INVALID_CAPTURE_COVERAGE_OR_IDENTITY')
    encoded = _canonical(bundle).encode()
    if len(encoded) > MAX_BUNDLE_BYTES:
        raise ValueError('CAPTURE_TOO_LARGE')
    if (not source_features.measurement._hash(bundle.get('payload_sha256'))
            or _digest({key: value for key, value in bundle.items() if key != 'payload_sha256'}) != bundle['payload_sha256']):
        raise ValueError('CAPTURE_HASH_MISMATCH')
    source_at = _utc(bundle['computed_at_utc'])
    if source_at > observed:
        raise ValueError('FUTURE_CAPTURE_TIME')
    return source_at


def _model(coin, model_name, source_name, source_at, observed):
    models, sources, errors = coin.get('models'), coin.get('sources'), coin.get('source_time_errors')
    if (not isinstance(models, Mapping) or not isinstance(sources, Mapping)
            or not isinstance(errors, list) or not all(isinstance(item, str) for item in errors)):
        return None, ['INVALID_FROZEN_MODEL_SOURCE_BLOCK']
    module = models.get(model_name)
    score, direction, problems = source_features._model_feature(module, sources, errors, source_name, source_at)
    if problems:
        return None, ['INVALID_OR_UNAVAILABLE_'+source_name.upper()+'_SOURCE']
    if str(module.get('quality_status') or '').upper() not in ('PASS', 'WARNING'):
        return None, ['INVALID_'+source_name.upper()+'_QUALITY']
    if str(module.get('freshness_status') or '').upper() != 'FRESH':
        return None, ['STALE_OR_UNKNOWN_'+source_name.upper()+'_FRESHNESS']
    raw_direction = str(module.get('direction') or '').upper()
    explicit_direction = source_features.ALIASES.get(raw_direction)
    if explicit_direction is None:
        # Low-score neutral observations are trustworthy failures of65; a
        # large score labelled NEUTRAL cannot manufacture directional agreement.
        if raw_direction != 'NEUTRAL' or abs(score) >= MIN_SCORE:
            return None, ['INVALID_'+source_name.upper()+'_DIRECTION']
        direction = 'NEUTRAL'
    elif ((score <= 0 and explicit_direction == 'LONG')
            or (score >= 0 and explicit_direction == 'SHORT')):
        return None, ['CONTRADICTING_'+source_name.upper()+'_SCORE_DIRECTION']
    else:
        direction = explicit_direction
    try:
        closed = _utc(sources[source_name]['quality']['candle_close'])
        age_minutes = (observed-closed).total_seconds()/60
        if closed > source_at or not 0 <= age_minutes <= MAX_CVD_AGE_MINUTES:
            return None, ['STALE_OR_FUTURE_'+source_name.upper()+'_CANDLE']
    except (KeyError, ValueError, TypeError, OverflowError):
        return None, ['INVALID_'+source_name.upper()+'_CANDLE_CLOSE']
    return {'score': score, 'direction': direction, 'close': closed.isoformat()}, []


def evaluate_bundle(bundle, observed_at) -> dict:
    """Return one conservative observation per coin, never infer a missing score.

    Source validity is evaluated at capture time; actual candle age is checked
    at the caller's current observation time. UNKNOWN preserves alert state.
    Only two validated modules can produce a resetting NO_MATCH observation.
    """
    try:
        observed = _utc(observed_at)
        source_at = _validate_bundle(bundle, observed)
    except (KeyError, ValueError, TypeError, OverflowError, AttributeError):
        return _result([_observation(symbol, reasons=['INVALID_OR_UNTRUSTED_CAPTURE']) for symbol in SYMBOLS])
    observations = []
    for symbol in SYMBOLS:
        coin = bundle['coins'][symbol]
        if not isinstance(coin, Mapping) or coin.get('status') not in ('CAPTURED', 'PARTIAL'):
            observations.append(_observation(symbol, reasons=['INVALID_COIN_CAPTURE']))
            continue
        futures, future_problems = _model(coin, 'futures_flow', 'futures', source_at, observed)
        spot, spot_problems = _model(coin, 'spot_flow', 'spot', source_at, observed)
        problems = future_problems+spot_problems
        if problems:
            observations.append(_observation(symbol, futures=futures, spot=spot, reasons=problems))
            continue
        matching = (abs(futures['score']) >= MIN_SCORE and abs(spot['score']) >= MIN_SCORE
            and futures['direction'] == spot['direction'] and futures['direction'] in ('LONG', 'SHORT'))
        observations.append(_observation(symbol, status='MATCH' if matching else 'NO_MATCH',
            direction=futures['direction'] if matching else None, futures=futures, spot=spot))
    return _result(observations, watch_scan_id=bundle['cycle_id'], source_at_utc=source_at.isoformat(),
        bundle_sha256=bundle['payload_sha256'])


def render_message(observation, source_at_utc) -> str:
    """A small experimental notification of the shared CVD direction only."""
    if (not isinstance(observation, Mapping) or observation.get('status') != 'MATCH'
            or observation.get('direction') not in ('LONG', 'SHORT')):
        raise ValueError('Only a matching dual-CVD observation can be rendered')
    values = [observation.get(key) for key in ('futures_score', 'spot_score')]
    if any(not source_features._finite(value) or not MIN_SCORE <= abs(value) <= 100 for value in values):
        raise ValueError('Invalid matching dual-CVD scores')
    stamp = _utc(source_at_utc).astimezone(_ISRAEL)
    direction = 'עלייה — LONG' if observation['direction'] == 'LONG' else 'ירידה — SHORT'
    scores = [f'{abs(value):.4f}'.rstrip('0').rstrip('.') for value in values]
    return (
        '🧪 <b>ניסיוני: הסכמת שני CVD — 65 ומעלה</b>\n'
        f"<b>{escape(str(observation['symbol']))} | {direction}</b>\n"
        f'Futures CVD: <b>{scores[0]}/100</b>\n'
        f'Spot CVD: <b>{scores[1]}/100</b>\n'
        'שני הציונים באותו כיוון; זה כיוון התחזית.\n'
        f'זמן הסקירה בישראל: {stamp:%d.%m.%Y %H:%M:%S}\n\n'
        'זו התאמה לנוסחה ניסיונית שטרם הוכחה, ולא הוראת מסחר.'
    )
