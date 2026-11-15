"""Frozen selected-formula decisions in the existing settings/outbox tables.

The caller owns the PostgreSQL transaction. There is no exchange or Telegram
I/O under its row lock. An approved plan is not a position or a claimed fill.
"""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import approved_alert_contract as contract
import selected_six_formula_signal as rules

VERSION = 'selected-six-approved-source-v1'
SPEC_SHA256 = hashlib.sha256((Path(__file__).parent / 'docs/selected-six-20261114/Selected_Six_Formulas_20261113.json').read_bytes()).hexdigest()
CONFIG_SHA256 = hashlib.sha256(json.dumps([VERSION, rules.FORMULA_VERSION, SPEC_SHA256,
    sorted(rules.CT009_SYMBOLS)], separators=(',', ':')).encode()).hexdigest()
MAX_BYTES = 1024 * 1024
MAX_INTENTS = 1024
MAX_CANDIDATES = 256
DAY_MS = 86_400_000
FORMULA_FAMILIES = {rules.SFF1361: 'sol_sff1361', rules.OCONF0034_13: 'oconf0034_13',
    rules.CT009: 'ct009', rules.XRP_LIQUIDITY_INVERSE: 'xrp_liquidity_inverse'}
FORMULA_NAMES = {rules.SFF1361: 'SOL SFF1361', rules.OCONF0034_13: 'OCONF0034_13',
    rules.CT009: 'CT009', rules.XRP_LIQUIDITY_INVERSE: 'XRP Liquidity inverse'}


def key_for(scope):
    if not isinstance(scope, str) or not scope.startswith('general-watch:'):
        raise ValueError('selected formula scope must be the existing Watch scope')
    return VERSION + ':' + hashlib.sha256(scope.encode()).hexdigest()


def _initial(now_ms):
    return dict(version=VERSION, config_sha256=CONFIG_SHA256, activated_ms=now_ms,
        candidates={}, observed={}, confirmations=[], delivered_alerts=[], episodes={},
        active=[], history=[], intents=[], counters={})


def _encode(state):
    raw = json.dumps(state, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    if len(raw.encode()) > MAX_BYTES or len(state['intents']) > MAX_INTENTS or len(state['candidates']) > MAX_CANDIDATES:
        raise ValueError('selected formula state capacity; no partial commit')
    return raw


def initialize(conn, scope, now_ms):
    key = key_for(scope)
    conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING',
                 (key, _encode(_initial(now_ms))))
    return load(conn, scope)


def load(conn, scope, *, locked=False):
    row = conn.execute('SELECT value FROM bot_settings WHERE key=%s' + (' FOR UPDATE' if locked else ''),
                       (key_for(scope),)).fetchone()
    if row is None:
        return None
    state = json.loads(row['value'])
    if state.get('version') != VERSION or state.get('config_sha256') != CONFIG_SHA256:
        raise ValueError('selected formula source configuration mismatch')
    return state


def save(conn, scope, state):
    conn.execute('UPDATE bot_settings SET value=%s WHERE key=%s', (_encode(state), key_for(scope)))


def count(state, key, amount=1):
    state['counters'][key] = state['counters'].get(key, 0) + amount


def prune(state, now_ms):
    # Only source-window receipts may expire. Approved plans stay until the
    # ordinary outbox can retain their cancellation tombstone independently.
    state['observed'] = {k: v for k, v in state['observed'].items() if v > now_ms - 2 * DAY_MS}
    for key in ('confirmations', 'delivered_alerts'):
        history_start = min([now_ms] + [x['snapshot']['observed_at_ms'] for x in state['candidates'].values()]) - DAY_MS
        state[key] = [x for x in state[key] if (x['observed_at_ms'] if isinstance(x, dict) else x[2]) >= history_start]
    for item in state['intents']:
        if item['status'] == 'IN_FLIGHT' and now_ms - item['attempted_ms'] >= 120_000:
            item.update(status='UNKNOWN', acknowledged_ms=now_ms)
        if item['status'] == 'PENDING' and now_ms >= item['expires_ms']:
            item.update(status='EXPIRED', acknowledged_ms=now_ms)
    # A one-shot SFF handoff receipt is not a position state. Retain its saved
    # notification through an uncertain Telegram attempt, then compact after
    # the finite notification window; the SQL intake tombstone is permanent.
    for item in state['history']:
        if item.get('status') == 'INTAKE_COMPLETE':
            intent = next((x for x in state['intents'] if x['position_id'] == item['position_id']), None)
            if intent and intent['status'] in ('PENDING', 'IN_FLIGHT'):
                item['ended_ms'] = now_ms
    keep = {x['position_id'] for x in state['active']}
    state['history'] = [x for x in state['history'] if x['ended_ms'] > now_ms -
        (3_600_000 if x.get('status') == 'INTAKE_COMPLETE' else 2 * DAY_MS)]
    keep.update(x['position_id'] for x in state['history'])
    state['intents'] = [x for x in state['intents'] if x['position_id'] in keep]
    for key, item in list(state['candidates'].items()):
        if item['snapshot']['valid_until_ms'] < now_ms:
            del state['candidates'][key]
            count(state, 'expired_candidates')


def retire_acknowledged(conn, scope, state, now_ms):
    """Compact only a proved SFF receiver handoff; no fill/closure inference."""
    selected = [x['position_id'] for x in state['active'] if x['rule_id'] == rules.SFF1361]
    if not selected:
        return 0
    rows = conn.execute('SELECT source_position_id FROM approved_alert_execution_outbox_mainnet '
        'WHERE source_key=%s AND source_position_id=ANY(%s) AND payload_json IS NULL '
        'AND NOT canceled AND acknowledged_sequence=source_sequence', (key_for(scope), selected)).fetchall()
    completed = {x['source_position_id'] for x in rows}
    for position in list(state['active']):
        if position['position_id'] in completed:
            state['active'].remove(position)
            position.update(status='INTAKE_COMPLETE', ended_ms=now_ms)
            state['history'].append(position)
    count(state, 'intake_completed', len(completed))
    return len(completed)


def envelope(decision, metadata, approved_ms):
    p = decision.to_dict() if hasattr(decision, 'to_dict') else deepcopy(decision)
    family = FORMULA_FAMILIES[p['formula_id']]
    if p['formula_id'] == rules.CT009 and p['symbol'] not in rules.CT009_SYMBOLS:
        raise ValueError('CT009 symbol not authorized')
    # Preserve actual source observation and evaluation separately; neither is
    # the planned entry price nor an exchange fill timestamp.
    reference = p['reference_at_ms']
    if approved_ms != p['decision_ms']:
        raise ValueError('approval must retain actual frozen evaluation time')
    policy = dict(name=p['formula_id'] + '_V1', entry_kind='LIMIT_AFTER_ALERT',
        source_price=f"HYPERLIQUID_{p['symbol']}_PERPETUAL_TRADE_1M")
    if p['pending_entry_expires_at_ms'] is not None:
        policy.update(pending_expires_at=contract.iso_ms(p['pending_entry_expires_at_ms']),
                      cancel_before_fill_price=p['cancel_target_price'])
    value = dict(version=contract.VERSION, kind='ALERT', family=family, rule_id=p['formula_id'],
        symbol=p['symbol'], side='SHORT', source_environment='mainnet', execution_environment='mainnet',
        source_price_kind='TRADE_1M', source_at=contract.iso_ms(reference),
        created_at=contract.iso_ms(approved_ms), approved_at=contract.iso_ms(approved_ms),
        arm_at=contract.iso_ms(approved_ms), expires_at=contract.iso_ms(approved_ms + contract.LEASE_MS),
        entry=p['entry_price'], stop=p['stop_loss'], take_profit=p['take_profit'],
        original_target=p['cancel_target_price'], policy=policy,
        proof=dict(source_contract_version=contract.SOURCE_CONTRACT, source_config_sha256=CONFIG_SHA256,
            cycle_id=metadata['cycle_id'], episode_key=metadata['episode_key'], timeframe=metadata['timeframe'],
            decision_at=contract.iso_ms(p['source_observed_at_ms']),
            data_available_at=contract.iso_ms(p['data_available_at_ms']),
            reference_at=contract.iso_ms(reference), reference_price=p['reference_price'],
            source_target=p['source_target_price']),
        source_sequence=approved_ms * 10 + contract.RANK['ALERT'], source_as_of=contract.iso_ms(approved_ms),
        source_state='APPROVED', valid_until=None, cancel_reason=None)
    value['occurrence_id'] = contract.occurrence_id(value)
    value = contract.validate(value)
    if not contract.new_entry_allowed(value):
        raise ValueError('formula is not authorized for new entries')
    return value


def render(value):
    # Decimal strings are already frozen and manager-validated. Do not round
    # or recalculate here: the displayed plan must equal the submitted plan.
    return ('אות מסחר מאושר — ' + FORMULA_NAMES[value['rule_id']] + '\n'
        + value['symbol'] + ' · SHORT\n'
        + 'כניסה מתוכננת: ' + value['entry'] + '\n'
        + 'סטופ מתוכנן: ' + value['stop'] + '\n'
        + 'יעד מתוכנן: ' + value['take_profit'] + '\n'
        + 'האות נשמר בתור ההעברה למנהל העסקאות; אין בכך אישור למילוי בבורסה.')


def approve_state(state, decision, metadata, now_ms):
    # JSON roundtrip normalizes dataclass tuples to the representation read
    # from PostgreSQL; it does not change any numeric string or source value.
    p = json.loads(json.dumps(decision.to_dict() if hasattr(decision, 'to_dict') else decision))
    import alert_delivery_policy
    if not alert_delivery_policy.selected_rule_enabled(p['formula_id']):
        return dict(created=False, reason='FORMULA_DISABLED')
    existing = next((x for x in state['intents'] if x['intent_id'] == p['event_id']), None)
    if existing is not None:
        frozen = existing['decision']
        # Retry clock is not source identity. A changed source/reference/price
        # is a conflict; a later evaluation cannot revise the first approval.
        if any(frozen[k] != p[k] for k in p if k != 'decision_ms'):
            raise ValueError('selected formula decision revision')
        return dict(created=False, envelope=deepcopy(existing['payload']), intent=deepcopy(existing))
    if p['source_received_at_ms'] <= state['activated_ms']:
        return dict(created=False, reason='BEFORE_ACTIVATION')
    value = envelope(p, metadata, now_ms)
    position = dict(position_id=p['event_id'], rule_id=p['formula_id'], symbol=p['symbol'],
        direction='SHORT', status='APPROVED', approved_ms=now_ms,
        reference_ms=p['reference_at_ms'], checked_through_ms=((p['reference_at_ms'] + 59_999) // 60_000) * 60_000,
        cancel_target_price=p['cancel_target_price'], pending_expires_ms=p['pending_entry_expires_at_ms'])
    intent = dict(intent_id=p['event_id'], position_id=p['event_id'], decision=p,
        payload=value, text=render(value), status='PENDING', created_ms=now_ms,
        expires_ms=now_ms + contract.LEASE_MS, attempted_ms=None, attempt_token=None)
    state['active'].append(position)
    state['intents'].append(intent)
    count(state, 'approved')
    return dict(created=True, envelope=deepcopy(value), intent=deepcopy(intent))


def synchronize(conn, scope, state, now_ms):
    import approved_alert_outbox
    return approved_alert_outbox.synchronize(conn, key_for(scope), state, now_ms)


def approve(conn, scope, decision, metadata, now_ms):
    initialize(conn, scope, now_ms)
    state = load(conn, scope, locked=True)
    if completed_handoff(conn, scope, decision, metadata, now_ms):
        return dict(created=False, reason='INTAKE_ALREADY_COMPLETE')
    result = approve_state(state, decision, metadata, now_ms)
    save(conn, scope, state)
    synchronize(conn, scope, state, now_ms)
    return result


def completed_handoff(conn, scope, decision, metadata, now_ms):
    value = envelope(decision, metadata, now_ms)
    row = conn.execute('SELECT 1 FROM approved_alert_execution_outbox_mainnet '
        'WHERE source_key=%s AND occurrence_id=%s AND payload_json IS NULL '
        'AND acknowledged_sequence=source_sequence', (key_for(scope), value['occurrence_id'])).fetchone()
    return row is not None


def cancel_state(state, identity, reason, now_ms):
    position = next((x for x in state['active'] if x['position_id'] == identity), None)
    if position is None:
        return False
    position.update(status='CANCELED', ended_ms=now_ms, cancel_reason=reason)
    state['active'].remove(position)
    state['history'].append(position)
    intent = next(x for x in state['intents'] if x['position_id'] == identity)
    value = intent['payload']
    value.update(kind='CANCEL', source_state='CANCELED', cancel_reason=reason,
        source_sequence=now_ms * 10 + contract.RANK['CANCEL'], source_as_of=contract.iso_ms(now_ms))
    contract.validate(value)
    if intent['status'] == 'PENDING':
        intent.update(status='CANCELED', acknowledged_ms=now_ms)
    count(state, 'canceled')
    return True


def claim(conn, scope, now_ms):
    state = load(conn, scope, locked=True)
    if state is None:
        return None
    apply_entry_policy(state, now_ms)
    prune(state, now_ms)
    pending = next((x for x in state['intents'] if x['status'] == 'PENDING'), None)
    if pending is not None:
        pending.update(status='IN_FLIGHT', attempted_ms=now_ms, attempt_token=uuid4().hex)
    save(conn, scope, state)
    return deepcopy(pending)


def apply_entry_policy(state, now_ms):
    """Retire approvals only; the manager owns entry remainder cancellation.

    Historical decisions and outbox identities are retained. A source CANCEL
    never asserts an exchange cancellation or changes a filled position.
    """
    import alert_delivery_policy
    for identity, item in list(state['candidates'].items()):
        if not alert_delivery_policy.selected_rule_enabled(item['formula_id']):
            del state['candidates'][identity]
            count(state, 'policy_retired_candidates')
    for position in list(state['active']):
        if not alert_delivery_policy.selected_rule_enabled(position['rule_id']):
            cancel_state(state, position['position_id'], 'FORMULA_NEW_ENTRIES_DISABLED', now_ms)
    for intent in state['intents']:
        if (intent['status'] == 'PENDING'
                and not alert_delivery_policy.selected_rule_enabled(intent['payload']['rule_id'])):
            intent.update(status='CANCELED', acknowledged_ms=now_ms)
            count(state, 'policy_retired_notifications')


def finish(conn, scope, intent_id, attempt_token, status, now_ms):
    if status not in ('DELIVERED', 'UNKNOWN'):
        raise ValueError('invalid notification outcome')
    state = load(conn, scope, locked=True)
    intent = next((x for x in state['intents'] if x['intent_id'] == intent_id), None)
    if intent is None or intent['attempt_token'] != attempt_token or intent['status'] != 'IN_FLIGHT':
        return False
    intent.update(status=status, acknowledged_ms=now_ms)
    save(conn, scope, state)
    return True
