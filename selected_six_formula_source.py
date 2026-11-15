"""Explicit adapters from the existing Watch captures and delivered alerts.

No scoring is reconstructed. No order, signature or Telegram API lives here.
The existing forwarder's caller may run one bounded symbol step AFTER draining
its durable queue. Market inputs are shared by all candidates for that symbol.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import time

import selected_six_formula_signal as rules
import selected_six_formula_store as store

MINUTE_MS = 60_000
# Existing ordinary-alert source window, not a research/position holding rule.
SOURCE_WINDOW_MS = 10 * MINUTE_MS
MAX_HISTORY = 4096
NATIVE_KEYS = ('watch_scan_id', 'inverse_analysis', 'archive_import', 'archive_reconstruction',
    'telegram_archive_import', 'archive_only', 'telegram_archive', 'archive_run_key',
    'source_scope', 'data_mode', 'state', 'mode', 'record_mode')
ARCHIVE_KEYS = frozenset(NATIVE_KEYS[2:8])
_SYMBOL_OFFSET = 0
HYPE_CANDIDATE_KEY = ('captured-question-search-v3-experimental-binding:INVERSE:'
    'captured-question-search-v3-experimental-binding:LIQUIDITY_40_50')
HYPE_CONSUMER = 'watch-all-scan-intake-v1'
HYPE_MEASUREMENT = 'watch-scan-measurements-v1'
HYPE_EVALUATION = 'watch-scan-timeframe-formulas-v1'
HYPE_POPULATION = 'watch-all-scan-observations-v1'
HYPE_PARENT_POLICY = 'btc-parent-close-reversal-200bps-v1'


def map_hype_watch_row(row, now_ms):
    """Verify a joined existing intake/timeframe/measurement observation.

    The immediate Watch bundle lacks BTC membership. Only the persisted
    measurement's actual parent identity is usable; no substitute is made.
    UNKNOWN retains a valid parent so it can block an earlier/same-time arm.
    """
    import research_watch_scan_formula_timeframe as capture
    result = dict(arm='UNKNOWN', reason='unverified_capture', wave=None,
                  usable_ms=None, snapshot_set_id=None, candidate=None)
    try:
        intake, tf, measurement, observation = (row[k] for k in
            ('intake', 'timeframe', 'measurement', 'observation'))
        membership = measurement['payload']['membership']
        wave = membership['btc_parent_movement_id']
        if not isinstance(wave, str) or not wave or len(wave) > 200:
            raise ValueError('missing_existing_btc_parent')
        if (membership['membership_status'] != 'LIVE'
                or membership['episode_policy_version'] != HYPE_PARENT_POLICY):
            raise ValueError('unverified_existing_btc_parent')
        usable, snapshot_id = ms(intake['usable_from_utc']), intake['snapshot_set_id']
        if type(snapshot_id) is not int or snapshot_id <= 0:
            raise ValueError('invalid_snapshot_identity')
        if ms(membership['decision_time_utc']) != usable:
            raise ValueError('membership_clock_mismatch')
        result.update(wave=wave, usable_ms=usable, snapshot_set_id=snapshot_id)
        fp, mp = tf['payload'], measurement['payload']
        if (intake['consumer_version'] != HYPE_CONSUMER or intake['intake_status'] != 'ACCEPTED'
                or tf['status'] != 'READY' or tf['evaluation_version'] != HYPE_EVALUATION
                or measurement['measurement_version'] != HYPE_MEASUREMENT):
            raise ValueError('capture_not_accepted_ready')
        for payload in (fp, mp, observation):
            for key, expected in (('consumer_version', HYPE_CONSUMER),
                    ('population_version', HYPE_POPULATION), ('snapshot_set_id', snapshot_id),
                    ('symbol', 'HYPE')):
                if payload[key] != expected:
                    raise ValueError('capture_identity_mismatch:' + key)
            for key in ('bundle_sha256', 'parent_payload_sha256'):
                digest = intake[key]
                if (not isinstance(digest, str) or len(digest) != 64
                        or any(c not in '0123456789abcdef' for c in digest)
                        or payload[key] != digest):
                    raise ValueError('capture_hash_binding_mismatch:' + key)
            if ms(payload['usable_from_utc']) != usable:
                raise ValueError('capture_clock_mismatch')
        for sample in (tf, measurement):
            if (sample['consumer_version'] != HYPE_CONSUMER
                    or sample['snapshot_set_id'] != snapshot_id or sample['symbol'] != 'HYPE'
                    or ms(sample['usable_from_utc']) != usable):
                raise ValueError('capture_row_identity_mismatch')
        if (fp['version'] != HYPE_EVALUATION
                or fp['feature_version'] != capture.FEATURE_VERSION
                or fp['selection_version'] != capture.SELECTION_VERSION
                or mp['version'] != HYPE_MEASUREMENT
                or mp['entry_version'] != 'watch-next-full-minute-route-open-v1'
                or mp['outcome_method_version'] != 'ordered-first-touch-v7'
                or mp['price_route'] != 'HYPERLIQUID_HYPE_PERP_TRADE_1M'
                or measurement['price_route'] != mp['price_route']):
            raise ValueError('capture_version_or_route_mismatch')
        original_entry = rules.next_full_minute_ms(usable)
        if (ms(mp['entry_time_utc']) != original_entry
                or ms(measurement['entry_time_utc']) != original_entry):
            raise ValueError('measurement_entry_clock_mismatch')
        observed = ms(intake['observed_at_utc'])
        if (usable != max(observed, ms(intake['source_available_at_utc']),
                           ms(intake['source_created_at_utc']))
                or observed > ms(intake['source_available_at_utc'])
                or ms(observation['observed_at_utc']) != observed):
            raise ValueError('source_availability_mismatch')
        available = max(usable, ms(tf['created_at_utc']), ms(tf['updated_at_utc']))
        if available > now_ms:
            raise ValueError('future_capture_availability')
        raw, slots, errors = capture._source_for_timeframe(observation, '24h')
        selected, selection, reasons = capture._selection(observation, raw, slots, errors)
        provenance = fp['timeframe_provenance']['24h']
        if (provenance['source_row'] != raw or provenance['slots_by_source_side'] != slots
                or provenance['selection_status'] != selection
                or provenance['selected_source_side'] != selected
                or provenance['selected_base_direction'] !=
                    ('LONG' if selected == 'SHORT' else 'SHORT' if selected == 'LONG' else None)):
            raise ValueError('captured_timeframe_selection_mismatch')
        if selection == 'UNKNOWN':
            raise ValueError('unknown_selected_timeframe:' + ','.join(reasons))
        if selected != 'SHORT':
            result.update(arm='NOT_APPLICABLE', reason='source_side_not_selected')
            return result
        features, errors = capture._liquidity(raw, slots[selected], selected)
        if errors:
            raise ValueError('unknown_liquidity:' + ','.join(errors))
        share = features[capture.SHARE]
        expected = 'MATCH' if 40 <= share < 50 else 'NO_MATCH'
        evaluations = [x for x in fp['evaluations'] if x['candidate_key'] == HYPE_CANDIDATE_KEY
                       and x['timeframe'] == '24h' and x['base_direction'] == 'LONG']
        if (len(evaluations) != 1 or evaluations[0]['match_status'] != expected
                or evaluations[0]['selection_status'] != 'SELECTED'
                or evaluations[0]['analysis_direction'] != 'SHORT'):
            raise ValueError('captured_hype_predicate_mismatch')
        result.update(arm=expected, reason=expected.lower())
        if expected != 'MATCH':
            return result
        reference_ms = rules.next_full_minute_ms(available)
        # HYPE's original entry minute starts after both captured components
        # became available. The ordinary-alert observed+10m window belongs to
        # other formulas; imposing it here rejects a valid late-ready capture.
        # This deadline never moves to a later polling or retry time.
        expires = reference_ms + MINUTE_MS - 1
        snapshot = dict(source_event_id=f'{HYPE_CONSUMER}:{snapshot_id}:HYPE:24h:{wave}',
            symbol='HYPE', direction='LONG', observed_at_ms=observed,
            received_at_ms=available, valid_until_ms=expires, inputs={})
        for name, value in (('capture_guard_valid', True), ('raw_selected_side', 'SHORT'),
                ('source_base_direction', 'LONG'), ('selected_share_pct', share),
                ('btc_parent_movement_id', wave), ('timeframe', '24h')):
            _field(snapshot, name, value)
        snapshot['capture_identity'] = dict(snapshot_set_id=snapshot_id,
            bundle_sha256=intake['bundle_sha256'], parent_payload_sha256=intake['parent_payload_sha256'],
            usable_ms=usable, sample_ready_ms=available,
            parent_policy_version=HYPE_PARENT_POLICY, raw_selected_side='SHORT')
        snapshot['capture_guard_evidence'] = deepcopy(row)
        result['candidate'] = dict(formula_id=rules.HYPE_WATCH_INVERSE, snapshot=snapshot,
            metadata=dict(cycle_id=str(snapshot_id), timeframe='24h', episode_key=wave))
        return result
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        result.update(arm='UNKNOWN', reason=str(exc), candidate=None)
        return result


def select_hype_wave_candidates(state, records, now_ms):
    """First MATCH only, with early UNKNOWN; a rejection consumes the wave."""
    if state.get('hype_activated_ms') is None:
        return 0
    grouped = {}
    for row in records:
        if row['wave'] is not None:
            grouped.setdefault(row['wave'], []).append(row)
    added = 0
    for wave, rows in grouped.items():
        key = rules.HYPE_WATCH_INVERSE + ':wave:' + wave
        if key in state['episodes']:
            continue
        if sum(k.startswith(rules.HYPE_WATCH_INVERSE + ':wave:') for k in state['episodes']) >= store.MAX_CANDIDATES:
            # Fail locally; never grow shared state until other formulas can
            # no longer commit. A rejected input may be reconsidered only
            # after its same original source/entry window has been checked.
            store.count(state, 'hype_wave_state_capacity')
            continue
        matches = sorted((x for x in rows if x['arm'] == 'MATCH'),
                         key=lambda x: (x['usable_ms'], x['snapshot_set_id']))
        if not matches:
            continue
        first = matches[0]
        first_snapshot = first['candidate']['snapshot']
        # The reference OPEN does not exist before this minute. Do not freeze
        # a temporary UNKNOWN while earlier captured rows are still becoming
        # ready for that same minute; the existing cycle will revisit them.
        if now_ms < rules.next_full_minute_ms(first_snapshot['received_at_ms']):
            continue
        outcome = dict(identity=wave, approved=False, representative_snapshot_set_id=first['snapshot_set_id'],
            representative_usable_ms=first['usable_ms'],
            representative_entry_expires_ms=first_snapshot['valid_until_ms'], status='SELECTED')
        state['episodes'][key] = outcome
        if any(x['arm'] == 'UNKNOWN' and x['usable_ms'] <= first['usable_ms'] for x in rows):
            outcome['status'] = 'UNKNOWN_BLOCKED'
        elif first['usable_ms'] <= state['hype_activated_ms']:
            outcome['status'] = 'BEFORE_ACTIVATION'
        elif now_ms > first['candidate']['snapshot']['valid_until_ms']:
            outcome['status'] = 'ORIGINAL_ENTRY_WINDOW_EXPIRED'
        elif any(x['rule_id'] == rules.HYPE_WATCH_INVERSE and x['symbol'] == 'HYPE'
                 for x in state['active']):
            outcome['status'] = 'CAPACITY_REJECTED'
        else:
            added += _capture_candidates(state, [first['candidate']])
        store.count(state, 'hype_wave_' + outcome['status'].lower())
    return added


def ms(value):
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('source timestamp has no timezone')
    return int(result.timestamp() * 1000)


def dated(value, observed, received, expires, *, complete=True, **provenance):
    if not 0 <= observed <= received <= expires:
        raise ValueError('unavailable or stale source field')
    return dict(value=value, observed_at_ms=observed, received_at_ms=received,
        valid_until_ms=expires, complete=complete, **provenance)


def _base(identity, symbol, direction, observed, received):
    return dict(source_event_id=identity, symbol=symbol, direction=direction,
        observed_at_ms=observed, received_at_ms=received,
        valid_until_ms=observed + SOURCE_WINDOW_MS, inputs={})


def _field(snapshot, name, value, observed=None, expires=None, **provenance):
    snapshot['inputs'][name] = dated(value,
        snapshot['observed_at_ms'] if observed is None else observed,
        snapshot['received_at_ms'],
        min(snapshot['valid_until_ms'], expires) if expires is not None else snapshot['valid_until_ms'],
        **provenance)


def _family(coin, module_name, family_name, observed, received):
    module = coin['models'][module_name]
    if module.get('available') is not True or module.get('capture_status') != 'AVAILABLE':
        raise ValueError('model unavailable:' + module_name)
    family = module['time_families'][family_name]
    wanted = ('30m',) if family_name == 'now' else ('1h', '4h')
    if (tuple(family['windows']) != wanted or family['coverage'] != 1
            or family['available_windows'] != len(wanted)
            or family['configured_windows'] != len(wanted)
            or any(m.get('available') is not True for m in family['members'])
            or len(family['members']) != len(wanted)):
        raise ValueError('incomplete model family:' + module_name + '/' + family_name)
    net = Decimal(str(family['net']))
    if not net.is_finite() or not -1 <= net <= 1:
        raise ValueError('invalid family net')
    sources = coin['sources']
    expires = observed + SOURCE_WINDOW_MS
    if module_name == 'positioning':
        source = sources['positioning']
        if source.get('data_quality_status') != 'PASS':
            raise ValueError('positioning quality unavailable')
        price_at, oi_at = ms(source['price_fetched_at']), ms(source['oi_fetched_at'])
        source_time = max(price_at, oi_at)
        # PASS only proves time alignment in this provider. It does not renew
        # an old cached sample; apply the existing source window to each actual
        # observation and keep the earlier deadline.
        expires = min(expires, price_at + SOURCE_WINDOW_MS, oi_at + SOURCE_WINDOW_MS)
    else:
        source = sources['futures' if module_name == 'futures_flow' else 'spot']
        quality = source['quality']
        if (quality.get('freshness_status') != 'FRESH'
                or quality.get('usable_for_confirmation') is not True
                or quality.get('status') not in ('PASS', 'WARNING')):
            raise ValueError('flow quality stale or unavailable')
        source_time = ms(quality['candle_close'])
        age = Decimal(str(quality['max_age_minutes']))
        if not age.is_finite() or age <= 0:
            raise ValueError('missing provider freshness deadline')
        expires = min(expires, source_time + int(age * MINUTE_MS))
    if source_time > observed or received > expires:
        raise ValueError('model source time unavailable')
    return dated(str(-100 * net), source_time, received, expires,
        source_module=module_name, source_family=family_name)


def map_watch_capture(score_bundle, decision_bundle, raw_snapshot, received_at_ms):
    """Map authenticated product capture shapes; independently isolate formulas.

    The production capture validator binds actual MP/Combined decisions to the
    frozen scores. Raw OI data is accepted only when its canonical hash equals
    the captured hash; it supplies the numeric 1h feature omitted by that view.
    """
    import research_watch_decision_capture as capture
    import research_watch_score_capture as scores
    import alert_delivery_policy
    cycle = score_bundle['cycle_id']
    capture.validate_bundle(decision_bundle, score_bundle, cycle_id=cycle,
        available_at_utc=datetime.fromtimestamp(received_at_ms / 1000, timezone.utc))
    observed = ms(decision_bundle['computed_at_utc'])
    if not observed <= received_at_ms <= observed + SOURCE_WINDOW_MS:
        raise ValueError('stale Watch capture')
    candidates, confirmations, errors = [], [], []
    for symbol in sorted(rules.CT009_SYMBOLS):
        coin, decisions = score_bundle['coins'][symbol], decision_bundle['coins'][symbol]
        for item in decisions['prepared_items']:
            if not alert_delivery_policy.selected_rule_enabled(rules.OCONF0034_13):
                break
            if item['side'] != 'LONG' or item['maxpain_confirmation'].get('status') not in ('CONFIRMED', 'STRONG_CONFIRMED'):
                continue
            identity = cycle + ':' + item['item_id']
            snapshot = _base(identity, symbol, 'SHORT', observed, received_at_ms)
            try:
                if Decimal(str(item['target_price'])) >= Decimal(str(item['current_price'])):
                    raise ValueError('MP target does not point down')
                for name, value in (('confirmation_type', 'MP'), ('confirmation_confirmed', True),
                                    ('target_price', item['target_price'])):
                    source_row = next(x for x in coin['sources']['maxpain_operational_rows'] if x['timeframe'] == item['timeframe'])
                    target_at = ms(source_row['source_observed_at_utc'])
                    _field(snapshot, name, value, observed=target_at, expires=target_at+SOURCE_WINDOW_MS)
                confirmations.append(deepcopy(snapshot))
                for prefix, module in (('oi', 'positioning'), ('futures', 'futures_flow'), ('spot', 'spot_flow')):
                    for family in ('now', 'short'):
                        snapshot['inputs'][prefix + '_' + family + '_short_score'] = _family(
                            coin, module, family, observed, received_at_ms)
                candidates.append(dict(formula_id=rules.OCONF0034_13, snapshot=snapshot,
                    metadata=dict(cycle_id=cycle, timeframe=item['timeframe'])))
            except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                errors.append(dict(formula_id=rules.OCONF0034_13, symbol=symbol, reason=str(exc)))
        if symbol == 'SOL':
            try:
                raw = raw_snapshot[symbol]
                if scores.digest(raw) != coin['sources']['derivatives_snapshot_sha256']:
                    raise ValueError('raw OI snapshot does not match captured provenance')
                snapshot = _base(cycle + ':SFF1361:SOL', symbol, 'SHORT', observed, received_at_ms)
                for name, module in (('price_oi_30m_short_score', 'positioning'),
                                     ('futures_cvd_30m_short_score', 'futures_flow')):
                    snapshot['inputs'][name] = _family(coin, module, 'now', observed, received_at_ms)
                window = raw['regime']['windows']['1h']
                if window.get('available') is not True:
                    raise ValueError('1h price window unavailable')
                # This is the original bot price/OI feature, not HL execution
                # price returns and not a reweighted family score.
                _field(snapshot, 'one_hour_price_change_fraction', str(Decimal(str(window['price_change_pct'])) / 100),
                    observed=ms(raw['regime']['price_fetched_at']),
                    expires=ms(raw['regime']['price_fetched_at'])+SOURCE_WINDOW_MS,
                    source_field='regime.windows.1h.price_change_pct')
                candidates.append(dict(formula_id=rules.SFF1361, snapshot=snapshot,
                    metadata=dict(cycle_id=cycle, timeframe='30m'),
                    history_cursor_ms=(observed // MINUTE_MS) * MINUTE_MS - store.DAY_MS,
                    history_end_ms=(observed // MINUTE_MS) * MINUTE_MS))
            except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                errors.append(dict(formula_id=rules.SFF1361, symbol=symbol, reason=str(exc)))
        if symbol == 'XRP':
            by_id = {x['item_id']: x for x in decisions['prepared_items']}
            for group in decisions['combined_groups']:
                if group.get('qualified') is not True or group['side'] != 'SHORT':
                    continue
                try:
                    item = by_id[group['top_item_id']]
                    supporting = []
                    supported_tf = {x['timeframe'] for x in group['liquidity_imbalances']}
                    for identity in group['ordered_item_ids']:
                        source_item = by_id[identity]
                        if source_item['timeframe'] not in supported_tf:
                            continue
                        amounts = source_item['source_liquidity']['raw_amounts']
                        supporting.append(dict(timeframe=source_item['timeframe'],
                            upper_liquidation=amounts['short_liquidation_amount'],
                            lower_liquidation=amounts['long_liquidation_amount'], supports_source=True))
                    snapshot = _base(cycle + ':XRP:' + group['key'], 'XRP', 'LONG', observed, received_at_ms)
                    for name, value in (('source_state', 'COMBINED_QUALIFIED'), ('target_price', item['target_price']),
                                        ('supporting_liquidity', supporting)):
                        _field(snapshot, name, value)
                    candidates.append(dict(formula_id=rules.XRP_LIQUIDITY_INVERSE, snapshot=snapshot,
                        metadata=dict(cycle_id=cycle, timeframe=item['timeframe'])))
                except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    errors.append(dict(formula_id=rules.XRP_LIQUIDITY_INVERSE, symbol=symbol, reason=str(exc)))
    return dict(candidates=candidates, confirmations=confirmations, errors=errors)


def map_delivered_alert(event, received_at_ms=None):
    if (event.get('symbol') not in rules.CT009_SYMBOLS or event.get('direction') != 'SHORT'
            or event.get('timeframe') not in ('48h', '3d') or event.get('event_kind') != 'ALERT'
            or event.get('event_type') != 'MAX_PAIN_ALERT' or event.get('delivery_status') != 'DELIVERED'
            or event.get('capture_stage') != 'TELEGRAM_ALERT'):
        raise ValueError('not an authorized observed delivered ordinary alert')
    target, score = Decimal(str(event['target_price'])), Decimal(str(event['score']))
    if not target.is_finite() or target <= 0 or not score.is_finite():
        raise ValueError('invalid ordinary target or score')
    observed = ms(event['alert_time_utc'])
    available = max(ms(event['delivered_at_utc']), ms(event['created_at']))
    if received_at_ms is not None and available > received_at_ms:
        raise ValueError('future delivery record')
    snapshot = _base('research-event:' + str(event['event_id']), event['symbol'], 'SHORT', observed, available)
    snapshot['database_observed_at_ms'] = received_at_ms
    for name, value in (('ordinary_alert_delivered', True), ('timeframe', event['timeframe']),
                        ('target_price', event['target_price']), ('maxpain_score', event['score'])):
        _field(snapshot, name, value)
    # Imports/reconstruction and derived inverses must never become CT sources.
    engine = event.get('engine_snapshot')
    if (not isinstance(engine, dict) or 'inverse_analysis' in engine or ARCHIVE_KEYS.intersection(engine)
            or any(str(engine.get(k, '')).upper() in ('ARCHIVE','ARCHIVE_ONLY','IMPORTED','DEMO')
                   for k in ('source_scope','data_mode','state','mode','record_mode'))):
        raise ValueError('non-native ordinary alert')
    return dict(formula_id=rules.CT009, snapshot=snapshot,
        metadata=dict(cycle_id=str(engine.get('watch_scan_id') or snapshot['source_event_id']),
                      timeframe=event['timeframe']))


def capture_watch(conn, scope, score_bundle, decision_bundle, raw_snapshot, received_at_ms):
    mapped = map_watch_capture(score_bundle, decision_bundle, raw_snapshot, received_at_ms)
    store.initialize(conn, scope, received_at_ms)
    state = store.load(conn, scope, locked=True)
    store.apply_entry_policy(state, received_at_ms)
    store.prune(state, received_at_ms)
    for record in mapped['confirmations']:
        target = record['inputs']['target_price']
        compact = [record['source_event_id'], record['symbol'], record['observed_at_ms'],
            record['received_at_ms'], record['valid_until_ms'], str(target['value']),
            target['observed_at_ms'], target['valid_until_ms']]
        if not any(x[0] == compact[0] for x in state['confirmations']):
            state['confirmations'].append(compact)
    # MP repeat needs any earlier distinct snapshot, not every duplicate TF
    # carrying an identical target. Two latest distinct snapshots per exact
    # target preserve the current+previous test while keeping stationary
    # 24h captures bounded. Different target prices retain their own evidence.
    keep, seen = [], {}
    for record in sorted(state['confirmations'], key=lambda x: x[2], reverse=True):
        key = (record[1], record[5])
        times = seen.setdefault(key, set())
        if record[2] in times or len(times) >= 2:
            continue
        times.add(record[2])
        keep.append(record)
    state['confirmations'] = keep
    added = _capture_candidates(state, mapped['candidates'])
    store.count(state, 'mapping_errors', len(mapped['errors']))
    store.save(conn, scope, state)
    return dict(added=added, errors=mapped['errors'])


def _capture_candidates(state, candidates):
    import alert_delivery_policy
    added = 0
    for item in candidates:
        if not alert_delivery_policy.selected_rule_enabled(item['formula_id']):
            continue
        source = item['snapshot']
        identity = item['formula_id'] + ':' + source['source_event_id']
        if identity in state['observed']:
            continue
        state['observed'][identity] = source['received_at_ms']
        if source['received_at_ms'] <= state['activated_ms']:
            continue
        item = deepcopy(item)
        if item['formula_id'] != rules.SFF1361:
            try:
                episode, already_approved = _episode(state, item)
                item['metadata']['episode_key'] = episode
                item['episode_already_approved'] = already_approved
            except (ValueError, TypeError, KeyError, ArithmeticError):
                store.count(state, 'invalid_episode_source')
                continue
        if item['formula_id'] == rules.OCONF0034_13:
            # Pin the causal predecessor before later captures compact the
            # shared repeat history. A delayed price cannot replace it with
            # future confirmations or lose an inclusive 24-hour boundary.
            current = source
            target = Decimal(str(current['inputs']['target_price']['value']))
            prior = []
            for row in _confirmation_history(state['confirmations']):
                if (row['symbol'] == current['symbol'] and current['observed_at_ms']-store.DAY_MS <= row['observed_at_ms'] < current['observed_at_ms']
                        and row['received_at_ms'] <= current['received_at_ms']):
                    previous = Decimal(str(row['inputs']['target_price']['value']))
                    if max(previous,target)/min(previous,target) <= Decimal('1.004'):
                        prior = [row]
                        break
            item['prior_confirmations'] = prior
        state['candidates'][identity] = item
        added += 1
    return added


def read_delivered(conn, state, now_ms):
    """Finite source history, once for all six coins; no high-water replay bug."""
    import alert_delivery_policy
    if not alert_delivery_policy.selected_rule_enabled(rules.CT009):
        return 0
    rows = conn.execute('SELECT event_id,symbol,direction,timeframe,event_kind,event_type,delivery_status,'
        'capture_stage,alert_time_utc,delivered_at_utc,created_at,target_price,score,'
        "(SELECT COALESCE(jsonb_object_agg(field.key,field.value),'{}'::jsonb) "
        "FROM jsonb_each(CASE WHEN jsonb_typeof(engine_snapshot)='object' THEN engine_snapshot ELSE '{}'::jsonb END) field "
        "WHERE field.key=ANY(%s::text[])) AS engine_snapshot "
        "FROM research_events WHERE alert_time_utc >= %s AND alert_time_utc <= %s "
        "AND event_kind='ALERT' AND event_type='MAX_PAIN_ALERT' AND delivery_status='DELIVERED' "
        "AND capture_stage='TELEGRAM_ALERT' "
        "AND jsonb_typeof(engine_snapshot)='object' AND NOT(engine_snapshot ?| %s::text[]) "
        "AND NOT EXISTS (SELECT 1 FROM jsonb_each_text(engine_snapshot) flag "
        "WHERE flag.key IN ('source_scope','data_mode','state','mode','record_mode') "
        "AND upper(flag.value) IN ('ARCHIVE','ARCHIVE_ONLY','IMPORTED','DEMO')) "
        "AND direction='SHORT' AND timeframe IN ('48h','3d') AND symbol=ANY(%s) "
        'ORDER BY alert_time_utc,event_id LIMIT %s',
        (list(NATIVE_KEYS),datetime.fromtimestamp((now_ms-SOURCE_WINDOW_MS-store.DAY_MS)/1000, timezone.utc),
         datetime.fromtimestamp(now_ms/1000, timezone.utc),
         sorted(ARCHIVE_KEYS | {'inverse_analysis'}), sorted(rules.CT009_SYMBOLS), MAX_HISTORY+1)).fetchall()
    if len(rows) > MAX_HISTORY:
        raise ValueError('CT009 source history exceeds existing finite bound')
    historical, candidates = [], []
    for row in rows:
        try:
            snapshot = map_delivered_alert(row)['snapshot']
            historical.append([snapshot['source_event_id'], snapshot['symbol'], snapshot['observed_at_ms'],
                snapshot['received_at_ms'], snapshot['valid_until_ms'], row['timeframe'],
                str(row['target_price']), row['score']])
            if ms(row['alert_time_utc']) >= now_ms - SOURCE_WINDOW_MS:
                candidates.append(map_delivered_alert(row, now_ms))
        except (ValueError, KeyError, TypeError, ArithmeticError):
            store.count(state, 'invalid_delivered_source')
    state['delivered_alerts'] = historical
    return _capture_candidates(state, candidates)


def read_hype_watch(conn, state, now_ms):
    """Read existing durable captures once; no new collector or provider call.

    All observations in each currently relevant real parent wave are read,
    including pre-activation UNKNOWN/first MATCH rows. Oversize input fails
    closed before selecting any partial wave. Paused HYPE performs no query.
    """
    import alert_delivery_policy
    if (not alert_delivery_policy.selected_rule_enabled(rules.HYPE_WATCH_INVERSE)
            or state.get('hype_activated_ms') is None):
        return 0
    prune_hype_waves(conn,state,now_ms)
    consumed_waves = [v['identity'] for k,v in state['episodes'].items()
                      if k.startswith(rules.HYPE_WATCH_INVERSE + ':wave:')]
    rows = conn.execute('''WITH current_waves AS (
        SELECT DISTINCT m.payload#>>'{membership,btc_parent_movement_id}' AS wave
        FROM research_watch_scan_tf_formula_samples f
        JOIN research_watch_scan_intakes i USING(consumer_version,snapshot_set_id)
        JOIN research_watch_scan_measurements m USING(consumer_version,snapshot_set_id)
        WHERE i.consumer_version=%s AND m.measurement_version=%s AND m.symbol='HYPE'
            AND f.evaluation_version=%s AND f.symbol='HYPE' AND f.status='READY'
            AND GREATEST(f.usable_from_utc,f.created_at_utc,f.updated_at_utc) >= %s
            AND GREATEST(f.usable_from_utc,f.created_at_utc,f.updated_at_utc) <= %s
            AND m.payload#>>'{membership,btc_parent_movement_id}' IS NOT NULL
            AND NOT(m.payload#>>'{membership,btc_parent_movement_id}'=ANY(%s::text[]))
    ) SELECT
        jsonb_build_object('consumer_version',i.consumer_version,'snapshot_set_id',i.snapshot_set_id,
            'intake_status',i.intake_status,'bundle_sha256',i.bundle_sha256,
            'parent_payload_sha256',i.parent_payload_sha256,'observed_at_utc',i.observed_at_utc,
            'usable_from_utc',i.usable_from_utc,'source_available_at_utc',i.source_available_at_utc,
            'source_created_at_utc',i.source_created_at_utc) AS intake,
        jsonb_build_object('consumer_version',f.consumer_version,'snapshot_set_id',f.snapshot_set_id,
            'symbol',f.symbol,'evaluation_version',f.evaluation_version,'status',f.status,
            'created_at_utc',f.created_at_utc,'updated_at_utc',f.updated_at_utc,
            'usable_from_utc',f.usable_from_utc,'payload',
            (f.payload - 'timeframe_provenance' - 'evaluations') || jsonb_build_object(
                'timeframe_provenance',jsonb_build_object('24h',f.payload#>'{timeframe_provenance,24h}'),
                'evaluations',(SELECT COALESCE(jsonb_agg(e),'[]'::jsonb)
                    FROM jsonb_array_elements(f.payload->'evaluations') e
                    WHERE e->>'candidate_key'=%s AND e->>'timeframe'='24h'
                    AND e->>'base_direction'='LONG'))) AS timeframe,
        jsonb_build_object('consumer_version',m.consumer_version,'snapshot_set_id',m.snapshot_set_id,
            'symbol',m.symbol,'measurement_version',m.measurement_version,'price_route',m.price_route,
            'usable_from_utc',m.usable_from_utc,'entry_time_utc',m.entry_time_utc,
            'payload',m.payload - 'records' - 'price_source') AS measurement,
        jsonb_build_object('consumer_version',o.consumer_version,'snapshot_set_id',o.snapshot_set_id,
            'population_version',o.population_version,'symbol',o.symbol,
            'bundle_sha256',o.bundle_sha256,'parent_payload_sha256',o.parent_payload_sha256,
            'usable_from_utc',o.usable_from_utc,'observed_at_utc',o.observed_at_utc,
            'source_time_errors',o.source_time_errors,'sources',jsonb_build_object(
                'maxpain_operational_rows',(SELECT COALESCE(jsonb_agg(r),'[]'::jsonb)
                    FROM jsonb_array_elements(o.sources->'maxpain_operational_rows') r
                    WHERE r->>'timeframe'='24h')),
            'maxpain_slots',(SELECT COALESCE(jsonb_agg(s),'[]'::jsonb)
                FROM jsonb_array_elements(o.maxpain_slots) s WHERE s->>'timeframe'='24h')) AS observation
        FROM research_watch_scan_intakes i
        JOIN research_watch_scan_measurements m USING(consumer_version,snapshot_set_id)
        LEFT JOIN research_watch_scan_tf_formula_samples f ON
            f.consumer_version=i.consumer_version AND f.snapshot_set_id=i.snapshot_set_id
            AND f.symbol=m.symbol AND f.evaluation_version=%s
        LEFT JOIN research_watch_scan_observations o ON
            o.consumer_version=i.consumer_version AND o.snapshot_set_id=i.snapshot_set_id AND o.symbol=m.symbol
        WHERE i.consumer_version=%s AND m.measurement_version=%s AND m.symbol='HYPE'
            AND i.usable_from_utc <= %s
            AND m.payload#>>'{membership,btc_parent_movement_id}' IN (SELECT wave FROM current_waves)
        ORDER BY i.usable_from_utc,i.snapshot_set_id LIMIT %s''',
        (HYPE_CONSUMER,HYPE_MEASUREMENT,HYPE_EVALUATION,
         datetime.fromtimestamp((now_ms-SOURCE_WINDOW_MS)/1000,timezone.utc),
         datetime.fromtimestamp(now_ms/1000,timezone.utc),consumed_waves,HYPE_CANDIDATE_KEY,
         HYPE_EVALUATION,HYPE_CONSUMER,HYPE_MEASUREMENT,
         datetime.fromtimestamp(now_ms/1000,timezone.utc),MAX_HISTORY+1)).fetchall()
    if len(rows) > MAX_HISTORY:
        raise ValueError('HYPE full-wave capture exceeds existing finite bound')
    records = [map_hype_watch_row(row,now_ms) for row in rows]
    store.count(state,'hype_capture_unknown',sum(x['arm']=='UNKNOWN' for x in records))
    return select_hype_wave_candidates(state,records,now_ms)


def prune_hype_waves(conn, state, now_ms):
    """Compact only ended parents after their selected original entry window.

    An open wave survives long source gaps and restart. Late readiness can
    place the original entry minute after the parent ended; that consumed
    wave must survive through its own deadline so capacity cannot re-arm it.
    Unknown deadlines are retained until their expiry can be proved.
    Permanent outbox receipts and approved decision history are unaffected.
    """
    prefix = rules.HYPE_WATCH_INVERSE + ':wave:'
    identities = [v['identity'] for k,v in state['episodes'].items() if k.startswith(prefix)]
    if len(identities) < 2:
        return 0
    rows = conn.execute('SELECT btc_parent_movement_id,end_time_utc FROM research_btc_parent_movements '
        'WHERE episode_policy_version=%s AND btc_parent_movement_id=ANY(%s) '
        'AND end_time_utc IS NOT NULL AND end_time_utc < %s',
        (HYPE_PARENT_POLICY,identities,datetime.fromtimestamp((now_ms-SOURCE_WINDOW_MS)/1000,timezone.utc))).fetchall()
    active_waves = {x['metadata']['episode_key'] for x in state['candidates'].values()
                    if x['formula_id']==rules.HYPE_WATCH_INVERSE}
    count = 0
    for row in rows:
        wave = row['btc_parent_movement_id']
        outcome = state['episodes'].get(prefix+wave)
        expires = outcome.get('representative_entry_expires_ms') if outcome else None
        if (wave not in active_waves and outcome is not None
                and type(expires) is int and expires < now_ms):
            del state['episodes'][prefix+wave]
            count += 1
    return count


def _confirmation_history(records):
    result = []
    for row in records:
        if isinstance(row, dict):  # API callers may provide already normalized evidence.
            result.append(row)
            continue
        snapshot = _base(row[0], row[1], 'SHORT', row[2], row[3])
        for name, value in (('confirmation_type', 'MP'), ('confirmation_confirmed', True), ('target_price', row[5])):
            _field(snapshot, name, value, observed=row[6], expires=row[7])
        result.append(snapshot)
    return result


def _delivered_history(records):
    result = []
    for row in records:
        if isinstance(row, dict):
            result.append(row)
            continue
        snapshot = _base(row[0], row[1], 'SHORT', row[2], row[3])
        for name, value in (('ordinary_alert_delivered', True), ('timeframe', row[5]),
                            ('target_price', row[6]), ('maxpain_score', row[7])):
            _field(snapshot, name, value)
        result.append(snapshot)
    return result


def _episode(state, candidate):
    snapshot, formula = candidate['snapshot'], candidate['formula_id']
    if formula == rules.SFF1361:
        return snapshot['source_event_id'], False
    if formula == rules.HYPE_WATCH_INVERSE:
        wave = snapshot['inputs']['btc_parent_movement_id']['value']
        previous = state['episodes'].get(_episode_scope(candidate))
        if previous is None or previous['identity'] != wave:
            raise ValueError('HYPE candidate has no selected captured parent')
        return wave, previous['approved']
    key = _episode_scope(item=candidate)
    target = snapshot['inputs']['target_price']['value']
    previous = state['episodes'].get(key)
    if previous and snapshot['observed_at_ms'] < previous['last_seen_ms']:
        # An out-of-order late commit stays historical evidence, never rewinds
        # an already advanced operational episode or reopens its old target.
        return previous['identity'], True
    gap = rules.EPISODE_GAP_MS if formula in (rules.CT009, rules.XRP_LIQUIDITY_INVERSE) else None
    if previous and rules.target_episode_continues(previous['targets'], target,
            previous['last_seen_ms'], snapshot['observed_at_ms'], gap):
        previous['targets'] = [str(min(previous['targets'] + [target], key=lambda x: Decimal(str(x)))),
                               str(max(previous['targets'] + [target], key=lambda x: Decimal(str(x))))]
        previous['last_seen_ms'] = snapshot['observed_at_ms']
        return previous['identity'], previous['approved']
    identity = hashlib.sha256((key + ':' + snapshot['source_event_id']).encode()).hexdigest()
    state['episodes'][key] = dict(identity=identity, targets=[str(target)],
        last_seen_ms=snapshot['observed_at_ms'], approved=False)
    return identity, False


def _episode_scope(item):
    if item['formula_id'] == rules.HYPE_WATCH_INVERSE:
        return rules.HYPE_WATCH_INVERSE + ':wave:' + item['snapshot']['inputs']['btc_parent_movement_id']['value']
    key = item['formula_id'] + ':' + item['snapshot']['symbol']
    if item['formula_id'] in (rules.CT009, rules.XRP_LIQUIDITY_INVERSE):
        key += ':' + item['metadata']['timeframe']
    return key


def evaluate_ready(state, rows, fetched_ms, *, conn=None, scope=None):
    """One shared actual symbol response feeds all its formulas and vetoes."""
    import alert_delivery_policy
    by_minute = {r[0]: r for r in rows}
    now_open = fetched_ms // MINUTE_MS * MINUTE_MS
    created = 0
    for identity, item in list(state['candidates'].items()):
        snapshot, formula = item['snapshot'], item['formula_id']
        if not alert_delivery_policy.selected_rule_enabled(formula):
            if store.entry_policy_retired(formula):
                del state['candidates'][identity]
            continue
        if snapshot['valid_until_ms'] < fetched_ms:
            continue
        if formula == rules.SFF1361 and 'price_history_complete_24h' not in snapshot['inputs']:
            cursor, end = item['history_cursor_ms'], item['history_end_ms']
            while cursor < end and cursor in by_minute:
                cursor += MINUTE_MS
            item['history_cursor_ms'] = cursor
            if cursor != end:
                continue
            snapshot['received_at_ms'] = max(snapshot['received_at_ms'], fetched_ms)
            _field(snapshot, 'price_history_complete_24h', True, observed=end,
                   source_venue='HYPERLIQUID_PERPETUAL', history_start_ms=end-store.DAY_MS,
                   history_end_ms=end, contiguous_closed_minutes=1440)
        available = snapshot['received_at_ms']
        if formula == rules.OCONF0034_13:
            row = by_minute.get(now_open)
            if row is None or fetched_ms < available:
                continue
            price, reference_ms = row[4], fetched_ms
            extra = dict(reference_kind='LIVE_REFERENCE', source_candle_open_ms=row[0],
                         source_candle_close_ms=row[0]+MINUTE_MS, observed_at_response=True)
        else:
            reference_ms = rules.next_full_minute_ms(available)
            row = by_minute.get(reference_ms)
            if row is None or reference_ms > fetched_ms:
                continue
            price = row[1]
            extra = dict(reference_kind='NEXT_FULL_MINUTE_OPEN', source_candle_open_ms=row[0])
        snapshot['reference'] = dated(rules.canonical_price(price), reference_ms, fetched_ms,
            snapshot['valid_until_ms'], venue='HYPERLIQUID_PERPETUAL', **extra)
        if formula == rules.SFF1361:
            result = rules.evaluate_sff1361(snapshot, fetched_ms)
        elif formula == rules.OCONF0034_13:
            result = rules.evaluate_oconf0034_13(snapshot,
                item.get('prior_confirmations', _confirmation_history(state['confirmations'])), fetched_ms)
        elif formula == rules.CT009:
            result = rules.evaluate_ct009(snapshot, _delivered_history(state['delivered_alerts']), fetched_ms)
        elif formula == rules.HYPE_WATCH_INVERSE:
            result = rules.evaluate_hype_watch_inverse(snapshot, fetched_ms)
        else:
            result = rules.evaluate_xrp_liquidity_inverse(snapshot, fetched_ms)
        store.count(state, 'evaluated_' + formula)
        if result['signal']:
            decision = result['decision']
            if (decision.cancel_target_price is not None and any(
                    r[0] >= decision.reference_at_ms
                    and Decimal(str(r[3])) <= Decimal(decision.cancel_target_price) for r in rows)):
                # A forming candle after the reference is a valid live veto,
                # never a closed feature or evidence of order/fill ordering.
                store.count(state, 'target_reached_before_approval')
                del state['candidates'][identity]
                continue
            if 'episode_key' in item['metadata']:
                episode = item['metadata']['episode_key']
                current_episode = state['episodes'].get(_episode_scope(item))
                duplicate = item.get('episode_already_approved', False) or bool(
                    current_episode and current_episode['identity'] == episode and current_episode['approved'])
            else:
                episode, duplicate = _episode(state, item)
            # Source state is an approval fence only. Filled/pending venue
            # positions remain the existing manager's authoritative guard.
            if not duplicate:
                metadata = dict(item['metadata'], episode_key=episode)
                tombstone = (conn is not None and store.completed_handoff(
                    conn, scope, result['decision'], metadata, fetched_ms))
                if not tombstone:
                    created += int(store.approve_state(state, result['decision'], metadata, fetched_ms)['created'])
                episode_state = state['episodes'].get(_episode_scope(item))
                if episode_state is not None and episode_state['identity'] == episode:
                    episode_state['approved'] = True
            else:
                store.count(state, 'episode_duplicates')
        elif not result['valid']:
            store.count(state, 'invalid_calculation')
        del state['candidates'][identity]
    return created


def observe_cancellations(state, rows, now_ms):
    by_minute = {r[0]: r for r in rows if r[0] + MINUTE_MS <= now_ms}
    changed = 0
    for position in list(state['active']):
        if position['pending_expires_ms'] is None:
            continue
        if now_ms >= position['pending_expires_ms']:
            changed += int(store.cancel_state(state, position['position_id'], 'PENDING_ENTRY_EXPIRED', now_ms))
            continue
        if position['cancel_target_price'] is None:
            continue
        target = Decimal(position['cancel_target_price'])
        known_veto = any((r[0] >= position['reference_ms'] and Decimal(str(r[3])) <= target)
            or (r[0] == now_ms // MINUTE_MS * MINUTE_MS and now_ms >= position['reference_ms']
                and Decimal(str(r[4])) <= target) for r in rows)
        if known_veto:
            changed += int(store.cancel_state(state, position['position_id'],
                'ORIGINAL_TARGET_OBSERVED_CANCEL_REMAINDER', now_ms))
            continue
        cursor = position['checked_through_ms'] // MINUTE_MS * MINUTE_MS
        # A closed candle can veto an unfilled remainder; it cannot prove fill
        # ordering or close an actual position. Manager reconciliation owns it.
        while cursor in by_minute:
            row = by_minute[cursor]
            if Decimal(str(row[3])) <= Decimal(position['cancel_target_price']):
                changed += int(store.cancel_state(state, position['position_id'],
                    'ORIGINAL_TARGET_OBSERVED_CANCEL_REMAINDER', now_ms))
                break
            cursor += MINUTE_MS
            position['checked_through_ms'] = cursor
    return changed


def tick(scopes, *, database_url=None, fetch=None, clock_ms=None, connect=None):
    """At most one 1,000-minute shared price call; caller drains queue first.

    No database lock/transaction spans the provider call. A slow or unavailable
    provider leaves durable source candidates pending until their original TTL.
    """
    global _SYMBOL_OFFSET
    from watch_transition_store import _connect
    import experimental_hyperliquid_source as prices
    connect, fetch = connect or _connect, fetch or prices.fetch_rows
    clock_ms = clock_ms or (lambda: int(time.time()*1000))
    now = clock_ms()
    snapshots = {}
    for scope in scopes:
        try:
            with connect(database_url) as conn:
                store.initialize(conn, scope, now)
                state = store.load(conn, scope, locked=True)
                store.apply_entry_policy(state, now)
                store.retire_acknowledged(conn, scope, state, now)
                store.prune(state, now)
                try:
                    with conn.transaction():
                        read_delivered(conn, state, now)
                except Exception:
                    store.count(state, 'delivered_source_errors')
                try:
                    with conn.transaction():
                        read_hype_watch(conn, state, now)
                except Exception:
                    store.count(state, 'hype_source_errors')
                observe_cancellations(state, [], now)
                store.save(conn, scope, state)
                store.synchronize(conn, scope, state, now)
                snapshots[scope] = state
        except Exception:
            # A local source/database failure never runs exchange recovery or
            # discards a previous decision, and does not stop another scope.
            continue
    symbols = sorted({x['snapshot']['symbol'] for s in snapshots.values() for x in s['candidates'].values()}
        | {x['symbol'] for s in snapshots.values() for x in s['active'] if x['pending_expires_ms'] is not None})
    if not symbols:
        return dict(symbol=None, shared_price_reads=0, approved=0)
    symbol = symbols[_SYMBOL_OFFSET % len(symbols)]
    _SYMBOL_OFFSET += 1
    current = now // MINUTE_MS * MINUTE_MS
    starts = [current]
    for state in snapshots.values():
        for item in state['candidates'].values():
            if item['snapshot']['symbol'] == symbol:
                starts.append(item.get('history_cursor_ms', rules.next_full_minute_ms(item['snapshot']['received_at_ms'])))
        starts.extend(p['checked_through_ms'] // MINUTE_MS * MINUTE_MS for p in state['active']
                      if p['symbol'] == symbol and p['pending_expires_ms'] is not None)
    start = min(starts)
    end = min(current + MINUTE_MS, start + 1000 * MINUTE_MS)
    if end <= start:
        return dict(symbol=symbol, shared_price_reads=0, approved=0)
    try:
        rows = fetch(symbol, start, end)
        fetched = clock_ms()
    except Exception as exc:
        return dict(symbol=symbol, shared_price_reads=1, approved=0, error=type(exc).__name__)
    approved, source_errors = 0, 0
    for scope in snapshots:
        try:
            with connect(database_url) as conn:
                state = store.load(conn, scope, locked=True)
                # Evaluate only this symbol; restore other independent candidates.
                others = {k: x for k, x in state['candidates'].items() if x['snapshot']['symbol'] != symbol}
                state['candidates'] = {k: x for k, x in state['candidates'].items() if x['snapshot']['symbol'] == symbol}
                created_for_scope = evaluate_ready(state, rows, fetched, conn=conn, scope=scope)
                state['candidates'].update(others)
                relevant = [x for x in state['active'] if x['symbol'] == symbol]
                other_active = [x for x in state['active'] if x['symbol'] != symbol]
                state['active'] = relevant
                observe_cancellations(state, rows, fetched)
                state['active'].extend(other_active)
                store.prune(state, fetched)
                store.save(conn, scope, state)
                store.synchronize(conn, scope, state, fetched)
            approved += created_for_scope
        except Exception:
            source_errors += 1
    return dict(symbol=symbol, shared_price_reads=1, approved=approved, source_errors=source_errors)
