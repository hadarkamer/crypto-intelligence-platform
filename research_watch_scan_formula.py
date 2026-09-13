"""Outcome-blind existing-catalog evaluation on frozen Watch model totals.

Only the three captured total scores and the original observation's Israel
weekend flag are mapped. Unsupported catalog predicates remain explicit; this
adapter does not manufacture alert families, histories, features or formulas.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import research_formula_ordered_v7 as existing
import research_watch_scan_measurement as measurement

VERSION = 'watch-scan-formulas-v1'
FEATURE_VERSION = 'watch-captured-total-model-features-v1'
CATALOG_SHA256 = 'b3ed42745e935ea1295267afb70135b97d1d3534579ec5f8070b61929e862082'
SUPPORTED_FEATURES = frozenset(('price_oi.aligned_score', 'futures_cvd.aligned_score',
                                'spot_cvd.aligned_score', 'time.weekend'))
MODULES = {'price_oi': ('positioning', 'positioning'),
           'futures_cvd': ('futures_flow', 'futures'),
           'spot_cvd': ('spot_flow', 'spot')}
DIRECTIONS = ('LONG', 'SHORT')
ALIASES = {'LONG': 'LONG', 'BUY': 'LONG', 'BULLISH': 'LONG', 'UP': 'LONG',
           'SHORT': 'SHORT', 'SELL': 'SHORT', 'BEARISH': 'SHORT', 'DOWN': 'SHORT'}


def _json(value):
    if isinstance(value, datetime):
        return measurement.utc(value).isoformat()
    raise TypeError('Unsupported canonical value')


def canonical(value: Any) -> str:
    """Exact original definition serialization; do not round/coerce cutoffs."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False, default=_json)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _normalized_payload(value):
    # JSONB normalizes integer floats and negative zero. Normalize the derived
    # feature payload, never the original candidate definition or model score.
    if isinstance(value, datetime):
        return measurement.utc(value).isoformat()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Mapping):
        return {key: _normalized_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalized_payload(item) for item in value]
    return value


def catalog_records() -> list[dict]:
    """All existing definitions retain their exact IDs, metadata and hashes."""
    originals = existing.candidate_catalog(include_extended=True)
    if digest(originals) != CATALOG_SHA256:
        raise ValueError('CATALOG_DRIFT_REQUIRES_NEW_ADAPTER_VERSION')
    records = []
    for candidate in originals:
        conditions = existing._conditions(candidate)
        unsupported = sorted({c['feature'] for c in conditions}-SUPPORTED_FEATURES)
        orientation = candidate.get('research_orientation', 'NORMAL')
        if type(candidate.get('repeat_count')) is not int or candidate['repeat_count'] != 1 or orientation not in ('NORMAL', 'INVERSE'):
            raise ValueError('UNSUPPORTED_CATALOG_SEMANTICS')
        records.append({'candidate_key': candidate['formula_id'], 'definition': candidate,
            'definition_sha256': digest(candidate), 'orientation': orientation,
            'supported': not unsupported, 'unsupported_features': unsupported})
    if len({row['candidate_key'] for row in records}) != len(records):
        raise ValueError('DUPLICATE_CANDIDATE_IDENTITY')
    return records


def _finite(value) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except (ValueError, OverflowError):
        return False


def evaluate_candidate(candidate: Mapping, features: Mapping) -> dict:
    """Three-valued conjunction; missing never means a known false signal."""
    if type(candidate.get('repeat_count', 1)) is not int or candidate.get('repeat_count', 1) != 1:
        raise ValueError('UNSUPPORTED_REPEAT_SEMANTICS')
    missing, failed = set(), False
    for condition in existing._conditions(candidate):
        name, operator, expected = condition['feature'], condition['operator'], condition['value']
        actual = features.get(name)
        if type(expected) is bool:
            known = type(actual) is bool
        elif type(expected) in (int, float):
            known = _finite(expected) and _finite(actual)
        elif isinstance(expected, str):
            known = isinstance(actual, str)
        else:
            raise ValueError('UNSUPPORTED_CONDITION_VALUE')
        if not known:
            missing.add(name)
            continue
        if operator != '==' and (not _finite(actual) or not _finite(expected)):
            raise ValueError('ORDERING_REQUIRES_NUMERIC_CONDITION')
        passed = {'==': lambda: actual == expected, '>': lambda: actual > expected,
                  '<': lambda: actual < expected, '>=': lambda: actual >= expected,
                  '<=': lambda: actual <= expected}[operator]()
        failed = failed or not passed
    return {'match_status': 'NO_MATCH' if failed else 'UNKNOWN' if missing else 'MATCH',
            'missing_features': sorted(missing)}


def _source_problems(sources: Mapping, source_errors: list, source_name: str,
                     observed: datetime) -> list[str]:
    problems = []
    scoped = source_name + '/'
    for error in source_errors:
        if error.startswith(scoped) or (source_name in ('futures', 'spot') and error.startswith('derivatives/observed:')):
            problems.append('CAPTURE_TIME_ERROR:' + error)
    source = sources.get(source_name)
    if not isinstance(source, Mapping):
        return sorted(set(problems + ['MISSING_MODEL_SOURCE']))

    def check(value, name, *, required=True):
        if value in (None, ''):
            if required:
                problems.append('MISSING_TIME:' + name)
            return None
        try:
            timestamp = measurement.utc(value)
            if timestamp > observed:
                problems.append('FUTURE_TIME:' + name)
            return timestamp
        except (ValueError, TypeError, OverflowError):
            problems.append('INVALID_TIME:' + name)
            return None

    positioning_latest = None
    if source_name == 'positioning':
        # OI regime windows contain reference points only. Their current
        # price/OI inputs live at source level; no per-window latest_time is
        # produced by _window_results. Require both actual fetch timestamps
        # and bound references against both, without inventing a bar time.
        fetched = [check(source.get(key), source_name + '/' + key)
                   for key in ('price_fetched_at', 'oi_fetched_at')]
        if all(timestamp is not None for timestamp in fetched):
            positioning_latest = min(fetched)
    else:
        quality = source.get('quality')
        timing = sources.get('timing_observation')
        check(quality.get('candle_close') if isinstance(quality, Mapping) else None, source_name + '/candle_close')
        check(timing.get('cvd_observed_at_utc') if isinstance(timing, Mapping) else None, 'derivatives/observed')
    windows = source.get('window_references')
    if not isinstance(windows, Mapping):
        problems.append('MISSING_WINDOW_REFERENCES')
    else:
        for window_name, window in windows.items():
            prefix = source_name + '/' + str(window_name) + '/'
            if not isinstance(window, Mapping):
                problems.append('INVALID_WINDOW_REFERENCE:' + str(window_name))
                continue
            available = window.get('available')
            if type(available) is not bool:
                problems.append('INVALID_WINDOW_AVAILABILITY:' + str(window_name))
            latest = check(window.get('latest_time'), prefix+'latest_time',
                           required=available is True and source_name != 'positioning')
            reference = check(window.get('reference_time'), prefix+'reference_time', required=available is True)
            for key in ('reference_target_time', 'target_time'):
                if key in window:
                    check(window[key], prefix+key, required=False)
            if latest is not None and reference is not None and reference > latest:
                problems.append('REFERENCE_AFTER_LATEST:' + str(window_name))
            if positioning_latest is not None and reference is not None and reference > positioning_latest:
                problems.append('REFERENCE_AFTER_CURRENT_INPUT:' + str(window_name))
    return sorted(set(problems))


def _model_feature(module: Any, sources: Mapping, source_errors: list,
                   source_name: str, observed: datetime) -> tuple[float | None, str | None, list]:
    if not isinstance(module, Mapping):
        return None, None, ['MISSING_MODEL']
    if module.get('available') is not True or module.get('capture_status') != 'AVAILABLE':
        return None, None, ['MODEL_UNAVAILABLE']
    score = module.get('score')
    if not _finite(score) or not -100 <= score <= 100:
        return None, None, ['INVALID_MODEL_SCORE']
    raw_direction = module.get('direction')
    if raw_direction is not None and not isinstance(raw_direction, str):
        return None, None, ['INVALID_MODEL_DIRECTION']
    raw_direction = (raw_direction or '').upper()
    if raw_direction in ALIASES:
        direction = ALIASES[raw_direction]
    elif raw_direction in ('', 'NEUTRAL'):
        # Existing total-score extraction uses the sign for neutral/absent
        # direction labels, including available scores below the ±12 label cut.
        direction = 'LONG' if score > 0 else 'SHORT' if score < 0 else 'NEUTRAL'
    else:
        return None, None, ['INVALID_MODEL_DIRECTION']
    problems = _source_problems(sources, source_errors, source_name, observed)
    return (None, None, problems) if problems else (score, direction, [])


def evaluate_coin(observation: dict) -> dict:
    """Evaluate supported unchanged predicates before any price/outcome join."""
    if not isinstance(observation, Mapping):
        raise ValueError('INVALID_OBSERVATION')
    identity = ('consumer_version', 'population_version', 'snapshot_set_id', 'symbol',
                'bundle_sha256', 'parent_payload_sha256')
    if (observation.get('consumer_version') != measurement.CONSUMER_VERSION
            or observation.get('population_version') != measurement.POPULATION
            or observation.get('source_version') != measurement.SOURCE_VERSION
            or observation.get('intake_status') != 'ACCEPTED'
            or type(observation.get('snapshot_set_id')) is not int or observation['snapshot_set_id'] <= 0
            or not measurement._hash(observation.get('bundle_sha256'))
            or not measurement._hash(observation.get('parent_payload_sha256'))):
        raise ValueError('INVALID_INTAKE_IDENTITY')
    measurement.route_for_symbol(observation.get('symbol'))
    observed = measurement.utc(observation['observed_at_utc'])
    available = measurement.utc(observation['source_available_at_utc'])
    created = measurement.utc(observation['source_created_at_utc'])
    usable = measurement.utc(observation['usable_from_utc'])
    if observed > available or usable != max(observed, available, created):
        raise ValueError('INVALID_SOURCE_AVAILABILITY')
    models, sources, source_errors = (observation.get(key) for key in ('models', 'sources', 'source_time_errors'))
    if not isinstance(models, Mapping) or not isinstance(sources, Mapping) or not isinstance(source_errors, list) or not all(isinstance(e, str) for e in source_errors):
        raise ValueError('INVALID_FROZEN_FEATURE_BLOCK')
    weekend = observed.astimezone(ZoneInfo('Asia/Jerusalem')).weekday() >= 5
    directions = {direction: {'features': {'time.weekend': weekend}, 'unavailable_features': {}}
                  for direction in DIRECTIONS}
    for feature, (module_name, source_name) in MODULES.items():
        score, direction, problems = _model_feature(models.get(module_name), sources, source_errors, source_name, observed)
        key = feature + '.aligned_score'
        for base in DIRECTIONS:
            if problems:
                directions[base]['unavailable_features'][key] = problems
            else:
                directions[base]['features'][key] = abs(score) if direction == base else -abs(score)
    evaluations = []
    for candidate in catalog_records():
        if not candidate['supported']:
            continue
        for base in DIRECTIONS:
            decision = evaluate_candidate(candidate['definition'], directions[base]['features'])
            evaluations.append({'candidate_key': candidate['candidate_key'], 'base_direction': base,
                'analysis_direction': ('SHORT' if base == 'LONG' else 'LONG') if candidate['orientation']=='INVERSE' else base,
                **decision})
    payload = _normalized_payload({**{key: observation[key] for key in identity},
        'version': VERSION, 'feature_version': FEATURE_VERSION,
        'source_version': measurement.SOURCE_VERSION, 'usable_from_utc': usable,
        'source_observed_at_utc': observed, 'features_by_direction': directions,
        'evaluations': evaluations})
    payload['feature_sha256'] = digest(payload)
    return payload
