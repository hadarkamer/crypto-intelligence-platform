"""Inactive, outcome-blind predicate probe over frozen Watch decisions.

The three new populations preserve literal predicates, not delivered-event
cohorts. This module never activates support, creates controls, or reads prices.
"""
from __future__ import annotations

from copy import deepcopy

import research_watch_decision_capture as capture
import research_watch_scan_formula as legacy

VERSION = 'watch-decision-contract-v1'
SELECTION_VERSION = 'watch-frozen-item-cluster-qualified-group-v1'
MAXPAIN = 'MAXPAIN_SELECTED_ITEM'
MAGNET = 'MAGNET_CLUSTER'
COMBINED = 'QUALIFIED_COMBINED_GROUP'
EVENT = 'EVENT_REQUIRED'
POPULATIONS = (MAXPAIN, MAGNET, COMBINED)
MAPPING = 'event.direction_mapping_valid'
MP_STATUS = 'captured.maxpain_confirmation_status'
MAGNET_STATUS = 'captured.magnet.confirmation_status'
TOP_MEAN = 'max_pain.top_item_average_score_all_timeframes'
LITERALS = {MAXPAIN: ('BELOW_SCORE', 'CONFLICT', 'UNCONFIRMED', 'CONFIRMED', 'STRONG_CONFIRMED'),
            MAGNET: ('OBSERVATION', 'NOT_CONFIRMED', 'LIQUIDITY_UNAVAILABLE',
                     'LIQUIDITY_CONFLICT', 'CONFIRMED', 'STRONG_CONFIRMED')}
PRODUCERS = {MAXPAIN: ('market_confidence_engine.py',),
             MAGNET: ('market_confidence_engine.py', 'magnet_v1.py'),
             COMBINED: ('main.py', 'alert_engine.py', 'market_confidence_engine.py', 'magnet_v1.py')}
FLAGS = {'contract_status': 'INACTIVE', 'support_activation': False,
         'population_equivalent_to_event': False, 'cohort_eligible': False,
         'qualifies_as_prospective_formula_evidence': False,
         'is_delivered_alert': False, 'is_false_signal_control': False,
         'outcome_reuse_authorized': False}
canonical, digest = capture.canonical, capture.digest


def catalog_contracts():
    """Return all 42 original definitions unchanged, with separate metadata."""
    rows = []
    for original in legacy.catalog_records():
        conditions = legacy.existing._conditions(original['definition'])
        names = {condition['feature'] for condition in conditions}
        population = (MAXPAIN if MP_STATUS in names else MAGNET if MAGNET_STATUS in names
                      else COMBINED if TOP_MEAN in names else EVENT
                      if any(c['feature'] == 'event.event_type' and c['value'] == 'COMBINED_CONFIRMATION'
                             for c in conditions) else None)
        if population is None:
            continue
        unreachable = population == MAXPAIN and any(c['feature'] == MP_STATUS and
            c['value'] in ('NOT_CONFIRMED', 'OBSERVATION') for c in conditions)
        rows.append({key: deepcopy(original[key]) for key in
                     ('candidate_key', 'definition', 'definition_sha256', 'orientation')} |
                    {'population': population, 'evaluable': population != EVENT,
                     'block_reason': 'COMBINED_EVENT_IDENTITY_NOT_CAPTURED' if population == EVENT else None,
                     'producer_code_sha256': {name: capture.code_versions()[name]
                                             for name in PRODUCERS.get(population, ())},
                     'known_literals': list(LITERALS.get(population, ())),
                     'structurally_unreachable': unreachable,
                     'reachability_scope': 'CURRENT_MAXPAIN_PRODUCER' if unreachable else None,
                     **FLAGS})
    counts = {population: sum(r['population'] == population for r in rows)
              for population in (*POPULATIONS, EVENT)}
    if counts != {MAXPAIN: 8, MAGNET: 8, COMBINED: 22, EVENT: 4}:
        raise ValueError('DECISION_CONTRACT_CATALOG_DRIFT')
    return sorted(rows, key=lambda row: row['candidate_key'])


def _opposite(direction):
    return {'LONG': 'SHORT', 'SHORT': 'LONG'}.get(direction)


def _source_reasons(item, score_coin, *, derivatives=False, all_timeframes=False):
    """Only source clocks used by this unit; unrelated missing liquidity is OK."""
    if item is None:
        return ['MISSING_SOURCE_ITEM']
    prefixes = set(capture.TIMEFRAMES) if all_timeframes else {item['timeframe']}
    if derivatives:
        prefixes.update(('positioning', 'futures', 'derivatives'))
    reasons = ['SOURCE_TIME:' + error for error in score_coin['source_time_errors']
               if error.split('/', 1)[0] in prefixes]
    if all_timeframes and any(slot['status'] == 'MISSING_INPUT' for slot in score_coin['maxpain']):
        reasons.append('MAXPAIN_SOURCE_INPUT_MISSING')
    return sorted(reasons)


def _item_reasons(item, score_coin, **kwargs):
    reasons = _source_reasons(item, score_coin, **kwargs)
    if item is None:
        return reasons
    if item.get('calculation_validation_errors'):
        reasons.append('ITEM_CALCULATION_VALIDATION_ERRORS')
    current, target = item.get('current_price'), item.get('target_price')
    if not all(legacy._finite(value) and value > 0 for value in (current, target)):
        reasons.append('INVALID_ITEM_PRICE_OR_TARGET')
    elif (target > current) != (item['side'] == 'SHORT') or target == current:
        reasons.append('ITEM_DIRECTION_MAPPING_INVALID')
    return sorted(set(reasons))


def _unit(population, key, base, features, reasons, records, *, selection='SELECTED', **provenance):
    reasons = sorted(set(reasons))
    if selection == 'SELECTED' and reasons:
        selection = 'UNKNOWN'
    evaluations = []
    for record in records:
        if record['population'] != population:
            continue
        decision = (legacy.evaluate_candidate(record['definition'], features)
                    if selection != 'NOT_APPLICABLE'
                    else {'match_status': 'NOT_APPLICABLE', 'missing_features': []})
        if selection == 'UNKNOWN':
            decision['match_status'] = 'UNKNOWN'
        evaluations.append({'candidate_key': record['candidate_key'],
            'definition_sha256': record['definition_sha256'], 'orientation': record['orientation'],
            'base_direction': base,
            'analysis_direction': _opposite(base) if record['orientation'] == 'INVERSE' else base,
            'selection_status': selection, **decision, 'missing_reasons': reasons})
    return {'unit_id': key, 'population': population, 'base_direction': base,
            'selection_status': selection, 'missing_reasons': reasons,
            'features': features, 'evaluations': evaluations, **provenance}


def _coin(symbol, coin, score_coin, records, cycle_id, source_code):
    items = {item['item_id']: item for item in coin['prepared_items']}
    units = []
    for key in coin['displayable_item_ids']:
        item = items[key]
        # The selected score includes multi-timeframe consensus/cluster inputs.
        reasons = _item_reasons(item, score_coin, derivatives=True, all_timeframes=True)
        features = {} if reasons else {MAPPING: True}
        status = item.get('maxpain_confirmation', {}).get('status')
        if status in LITERALS[MAXPAIN]:
            features[MP_STATUS] = status
        else:
            reasons.append('MAXPAIN_CONFIRMATION_UNAVAILABLE' if not status else 'UNRECOGNIZED_MAXPAIN_LITERAL')
        reasons.extend('PRODUCER_CODE_MISMATCH:'+name for name in PRODUCERS[MAXPAIN]
                       if source_code[name] != capture.code_versions()[name])
        units.append(_unit(MAXPAIN, cycle_id+'|'+MAXPAIN+'|'+key, _opposite(item['side']),
            features, reasons, records, source_item_id=key, source_item_sha256=item['item_sha256'],
            source_side=item['side'], timeframe=item['timeframe'], literal_status=status))
    for ordinal, record in enumerate(coin['magnet_evaluations']):
        magnet = record['magnet']
        source = items.get(record['source_item_id'])
        # A nonmember target can change maximal-cluster selection, so every
        # source timeframe contributes to the captured cluster population.
        reasons = _source_reasons(source, score_coin, derivatives=True, all_timeframes=True)
        status = record.get('confirmation', {}).get('status')
        if record['evaluation_status'] != 'EVALUATED':
            reasons.append('MAGNET_'+record['evaluation_status'])
        if status not in LITERALS[MAGNET]:
            reasons.append('MAGNET_CONFIRMATION_UNAVAILABLE' if not status else 'UNRECOGNIZED_MAGNET_LITERAL')
        reasons.extend('PRODUCER_CODE_MISMATCH:'+name for name in PRODUCERS[MAGNET]
                       if source_code[name] != capture.code_versions()[name])
        features = {} if reasons else {MAPPING: True, MAGNET_STATUS: status}
        # source_item.side is an evidence-provider identity, not this direction.
        base = {'UPPER': 'LONG', 'LOWER': 'SHORT'}[magnet['side']]
        units.append(_unit(MAGNET, cycle_id+'|'+MAGNET+'|'+symbol+'|'+record['magnet_id'],
            base, features, reasons, records, magnet_id=record['magnet_id'], encounter_ordinal=ordinal,
            source_item_id=record['source_item_id'], source_side=magnet['side'],
            evaluation_status=record['evaluation_status'], literal_status=status,
            evaluation_reason=record.get('reason') or record.get('error_type')))
    for group in coin['combined_groups']:
        item = items[group['top_item_id']]
        reasons = _item_reasons(item, score_coin, derivatives=True, all_timeframes=True)
        reasons.extend('PRODUCER_CODE_MISMATCH:'+name for name in PRODUCERS[COMBINED]
                       if source_code[name] != capture.code_versions()[name])
        for reason in coin['missing_evidence']:
            if reason in ('SELECTED_ITEMS_OUTSIDE_PREPARED_POPULATION', 'MAXPAIN_SOURCE_INPUT_MISSING'):
                reasons.append('GROUP_SELECTION:'+reason)
            elif reason.startswith('ITEM_CALCULATION_VALIDATION_ERRORS:') and any(
                    reason.endswith(':'+key) for key in group['ordered_item_ids']):
                reasons.append('GROUP_QUALIFICATION:'+reason)
            elif not group['qualified'] and (reason.startswith('MAGNET_') or any(
                    reason.endswith(':'+key) for key in group['ordered_item_ids'])):
                reasons.append('GROUP_QUALIFICATION:'+reason)
        features = {} if reasons else {MAPPING: True}
        average = item.get('average_score_all_timeframes')
        if legacy._finite(average):
            features[TOP_MEAN] = average
        else:
            reasons.append('TOP_ITEM_AVERAGE_UNAVAILABLE')
        units.append(_unit(COMBINED, cycle_id+'|'+COMBINED+'|'+group['key'],
            _opposite(group['side']), features, reasons, records,
            selection='UNKNOWN' if reasons else 'SELECTED' if group['qualified'] else 'NOT_APPLICABLE',
            group_key=group['key'], top_item_id=group['top_item_id'], source_item_sha256=item['item_sha256'],
            ordered_item_ids=group['ordered_item_ids'], source_side=group['side'],
            timeframe=item['timeframe'], qualified=group['qualified'],
            signal_count=group['signal_count'], signal_keys=group['signal_keys']))
    populations = {}
    for population in POPULATIONS:
        selected = [unit for unit in units if unit['population'] == population]
        reasons = []
        # Missing raw liquidity or another family's failed evaluation must not
        # suppress known literal predicates. Missing population is not emptiness.
        for reason in coin['missing_evidence']:
            if reason in ('SELECTED_ITEMS_OUTSIDE_PREPARED_POPULATION', 'MAXPAIN_SOURCE_INPUT_MISSING'):
                reasons.append(reason)
        reasons.extend(reason for unit in selected if unit['selection_status'] == 'UNKNOWN'
                       for reason in unit['missing_reasons'])
        if not selected and any(error.split('/', 1)[0] in capture.TIMEFRAMES
                                for error in score_coin['source_time_errors']):
            reasons.append('EMPTY_POPULATION_SOURCE_TIME_UNRESOLVED')
        state = 'PARTIAL' if selected and reasons else 'UNKNOWN' if reasons else 'AVAILABLE' if selected else 'EMPTY'
        populations[population] = {'status': state, 'reasons': sorted(set(reasons)),
                                   'unit_count': len(selected)}
    return {'capture_status': coin['status'], 'populations': populations, 'units': units}


def evaluate_capture(decision_bundle, score_bundle, *, cycle_id, available_at_utc):
    """Probe validated frozen proof. The caller must also prove accepted intake.

    Invalid/missing capture returns an UNKNOWN source gate and no invented units.
    Inactive flags prohibit treating even MATCH/NO_MATCH as cohort membership.
    """
    records = catalog_contracts()
    result = {'version': VERSION, 'selection_version': SELECTION_VERSION, **FLAGS, 'flags': dict(FLAGS),
              'status': 'UNKNOWN', 'source_gate': {'status': 'UNKNOWN', 'reasons': []},
              'capture_status': None, 'source': {'cycle_id': str(cycle_id)}, 'coins': {},
              'catalog_summary': {'total': 42, 'evaluable': 38, 'blocked': 4,
                                  'structurally_unreachable': 4},
              'catalog_contract_sha256': digest(records),
              'blocked_definitions': [{key: row[key] for key in
                  ('candidate_key', 'definition_sha256', 'orientation', 'block_reason')}
                  for row in records if not row['evaluable']]}
    try:
        if not isinstance(decision_bundle, dict):
            raise capture.CaptureValidationError('MISSING_DECISION_CAPTURE')
        if decision_bundle.get('status') == 'FAILED':
            raise capture.CaptureValidationError('DECISION_CAPTURE_FAILED')
        capture.validate_bundle(decision_bundle, score_bundle, cycle_id=cycle_id,
                                available_at_utc=available_at_utc)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        result['source_gate']['reasons'] = [capture.error_reason(exc)]
    else:
        result.update(status='VALID', capture_status=decision_bundle['status'],
            source_gate={'status': 'VALIDATED', 'reasons': []},
            source={'cycle_id': cycle_id, 'score_payload_sha256': score_bundle['payload_sha256'],
                    'decision_payload_sha256': decision_bundle['payload_sha256'],
                    'available_at_utc': capture.utc(available_at_utc).isoformat(),
                    'computed_at_utc': decision_bundle['computed_at_utc'],
                    'capture_version': decision_bundle['version'],
                    'capture_population': decision_bundle['population'],
                    'capture_selection_version': decision_bundle['selection_version'],
                    'code_sha256': decision_bundle['code_sha256']},
            coins={symbol: _coin(symbol, decision_bundle['coins'][symbol],
                                score_bundle['coins'][symbol], records, cycle_id, decision_bundle['code_sha256'])
                   for symbol in capture.SYMBOLS})
    result = legacy._normalized_payload(result)
    result['payload_sha256'] = digest(result)
    return result
