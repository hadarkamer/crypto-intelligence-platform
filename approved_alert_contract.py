"""Authenticated approved-alert data, separate from prospective v1 plans.

An ALERT states that the producer has already approved an occurrence. Its three
prices are copied from that alert, not recalculated from a formula here. The
first-admission deadline is not a pending-order or position lifetime. A CANCEL
retires entry permission only; exchange fills remain owned by the receiver.

The facade keeps v1 readable for historical recovery. Release admission must
explicitly choose the supported version; validation is never order authority.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
import re

import experimental_execution_contract as legacy

VERSION = 'experimental-approved-alert-v2'
SOURCE_CONTRACT = legacy.SOURCE_CONTRACT
# Existing producer notification freshness, never a v2 order/position lease.
LEASE_MS = legacy.LEASE_MS
FIELDS = legacy.FIELDS | {'approved_at'}
PROOF_FIELDS = frozenset(('source_contract_version', 'source_config_sha256',
                          'cycle_id', 'episode_key', 'timeframe'))
POLICY_FIELDS = frozenset(('name', 'entry_kind', 'source_price'))
RANK = {'ALERT': 1, 'CANCEL': 3}
TEMPORAL = legacy.TEMPORAL
HEX = legacy.HEX
DOGE_PARTIAL_RULE = 'DOGE_MAXPAIN_ADVERSE_HALF_PART75_H24'
DOGE_SOURCE_CONFIG_SHA256 = 'd14e1ef6d8936143ce74e31b0d6015eb0d5051e030d12607413ea55f87b33c43'
# Approved source observations only. This does not add a prospective-v1
# adapter: its price/proof contract has different entry and exit semantics.
SPECS = {**legacy.SPECS,
    DOGE_PARTIAL_RULE: ('DOGE', 1., 3., .5, .8, False, legacy.TIMEFRAMES, False, 'BOTH')}
DOGE_PARTIAL_POLICY_FIELDS = POLICY_FIELDS | frozenset((
    'partial_take_price', 'partial_fraction', 'hold_until', 'stop_policy'))
FORMULAS = {'r2732': ('R2732_XRP_SHORT_NY_WEEKDAYS_LOCK', 'XRP'),
    'hype_row71205': ('HYPE_ROW71205_SHORT', 'HYPE'),
    'sol_g65': ('SOL_G65_K49_PROFIT_LOCK', 'SOL')}
# Explicit source adapters, not a configurable formula interpreter. Historical
# CT009 research symbols remain readable; its new entries stay paused.
CT009_SYMBOLS = frozenset(('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'HYPE'))
HYPE_WATCH_RULE = 'HYPE_WATCH_LIQ4050_24H_INVERSE'
HYPE_WATCH_FAMILY = 'hype_watch_liq4050'
SELECTED_FORMULAS = {
    HYPE_WATCH_FAMILY: (HYPE_WATCH_RULE, frozenset(('HYPE',))),
    'sol_sff1361': ('SFF1361_SOL_RECENT', frozenset(('SOL',))),
    'oconf0034_13': ('OCONF0034_13', CT009_SYMBOLS),
    'ct009': ('CT009_FROZEN_3D', CT009_SYMBOLS | frozenset(('BNB', 'ZEC'))),
    'xrp_liquidity_inverse': ('XRP_LIQUIDITY_RAW_INVERSE', frozenset(('XRP',))),
}
PENDING_FAMILIES = frozenset(('oconf0034_13', 'ct009', 'xrp_liquidity_inverse'))
SELECTED_PROOF_FIELDS = PROOF_FIELDS | frozenset((
    'decision_at', 'data_available_at', 'reference_at', 'reference_price', 'source_target'))
SELECTED_PENDING_POLICY_FIELDS = POLICY_FIELDS | frozenset((
    'pending_expires_at', 'cancel_before_fill_price'))
HYPE_WATCH_POLICY_FIELDS = POLICY_FIELDS | frozenset((
    'entry_expires_at', 'holding_time_limit_ms'))
# The user approved the new HYPE 40 USD / 5% formula allocation on 2026-11-15.
# Capital attribution is enforced separately by the existing manager journal.

# Operational activation decision 2026-11-15. Historical contracts stay readable;
# pausing admission does not revoke management of already filled positions.
NEW_ENTRY_IDENTITIES = frozenset((
    (HYPE_WATCH_FAMILY, HYPE_WATCH_RULE, 'HYPE'),
    ('r2732', 'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK', 'XRP'),
    ('sol_sff1361', 'SFF1361_SOL_RECENT', 'SOL'),
    ('xrp_liquidity_inverse', 'XRP_LIQUIDITY_RAW_INVERSE', 'XRP'),
))
RETIRED_ENTRY_IDENTITIES = frozenset((
    ('hype_row71205', 'HYPE_ROW71205_SHORT', 'HYPE'),
)) | frozenset((family, rule, symbol)
    for family in ('ct009', 'oconf0034_13')
    for rule, symbols in (SELECTED_FORMULAS[family],)
    for symbol in symbols)


def entry_retirement_required(message):
    """Only the three explicitly retired/paused entry identities; never a coin ban."""
    return (message.get('family'), message.get('rule_id'),
            message.get('symbol')) in RETIRED_ENTRY_IDENTITIES


def new_entry_allowed(message):
    return (message.get('family'), message.get('rule_id'), message.get('symbol')) in NEW_ENTRY_IDENTITIES


def new_entry_roster():
    """Current executable admission policy for existing read-only health views."""
    return [dict(family=family, rule_id=rule,
                 symbols=sorted(symbol for f, r, symbol in NEW_ENTRY_IDENTITIES
                                if (f, r) == (family, rule)))
            for family, rule in sorted({(f, r) for f, r, _ in NEW_ENTRY_IDENTITIES})]


def is_hype_watch(message):
    return (is_approved(message) and message.get('family') == HYPE_WATCH_FAMILY
            and message.get('rule_id') == HYPE_WATCH_RULE and message.get('symbol') == 'HYPE')


def immediate_entry_deadline(message):
    """A fixed live-entry minute, not an order wait or position holding limit."""
    return moment_ms(message['policy']['entry_expires_at']) if is_hype_watch(message) else None


def position_holding_limit_ms(message):
    """Measured from the first proved actual fill, never a historical candle fill."""
    return message['policy']['holding_time_limit_ms'] if is_hype_watch(message) else None


def pending_entry_deadline(message):
    """Order lifetime, deliberately separate from first-admission expires_at."""
    if is_approved(message) and message.get('family') in PENDING_FAMILIES:
        return moment_ms(message['policy']['pending_expires_at'])
    return None


def pending_cancel_price(message):
    if is_approved(message) and message.get('family') in PENDING_FAMILIES:
        return positive(message['policy']['cancel_before_fill_price'])
    return None


def selected_price(value):
    """Canonical source decimal at the existing 28-significant-digit boundary."""
    with localcontext() as ctx:
        ctx.prec = 28
        ctx.rounding = ROUND_HALF_EVEN
        result = format(+Decimal(str(value)), 'f')
    positive(result)
    return result


MAX_BYTES = legacy.MAX_BYTES
ContractError = legacy.ContractError
moment_ms = legacy.moment_ms
iso_ms = legacy.iso_ms
price = legacy.price
positive = legacy.positive


def is_approved(message):
    """Version discriminator only; callers still validate untrusted messages."""
    return isinstance(message, dict) and message.get('version') == VERSION


def occurrence_id(value):
    # Deliberately identical to v1: changing execution protocols or recipients
    # must not manufacture a second occurrence of the same producer decision.
    return legacy.occurrence_id(value)


def immutable(value):
    if not is_approved(value):
        return legacy.immutable(value)
    # Plan discovery time can be recipient-local. The deterministic approved
    # closed-bar time, source identity, deadline and exact alert prices cannot.
    return {key: deepcopy(val) for key, val in value.items()
            if key not in TEMPORAL and key != 'created_at'}


def plan_digest(value):
    return hashlib.sha256(legacy._canonical(immutable(value))).hexdigest()


def _validate_selected_formula(value, times):
    """Validate four fixed, approved source adapters and their frozen prices."""
    require = legacy._require
    family, policy, proof = value['family'], value['policy'], value['proof']
    legacy._keys(proof, SELECTED_PROOF_FIELDS, 'SELECTED_FORMULA_PROOF')
    require(proof['source_contract_version'] == SOURCE_CONTRACT
            and isinstance(proof['source_config_sha256'], str)
            and HEX.fullmatch(proof['source_config_sha256']) is not None,
            'SOURCE_PROVENANCE')
    for key in ('cycle_id', 'episode_key'):
        require(isinstance(proof[key], str) and 0 < len(proof[key]) <= 200,
                'SELECTED_FORMULA_SOURCE_IDENTITY')
    decision, available, reference = (moment_ms(proof[key]) for key in
        ('decision_at', 'data_available_at', 'reference_at'))
    require(decision <= available <= reference == times['source_at']
            and reference <= times['created_at'] <= times['approved_at'],
            'SELECTED_FORMULA_AVAILABILITY')
    if family != 'oconf0034_13':
        require(reference == (available // 60_000 + 1) * 60_000,
                'SELECTED_FORMULA_NEXT_AVAILABLE_MINUTE')
    allowed_tf = (('24h',) if family == HYPE_WATCH_FAMILY else
        ('30m',) if family == 'sol_sff1361' else
        ('48h', '3d') if family == 'ct009' else
        (*legacy.TIMEFRAMES, 'ALL6') if family == 'oconf0034_13' else legacy.TIMEFRAMES)
    require(proof['timeframe'] in allowed_tf, 'SELECTED_FORMULA_TIMEFRAME')
    expected_policy = dict(name=value['rule_id'] + '_V1', entry_kind='LIMIT_AFTER_ALERT',
        source_price=f"HYPERLIQUID_{value['symbol']}_PERPETUAL_TRADE_1M")
    with localcontext() as ctx:
        ctx.prec = 60
        p = positive(proof['reference_price'])
        if family == HYPE_WATCH_FAMILY:
            legacy._keys(policy, HYPE_WATCH_POLICY_FIELDS, 'HYPE_WATCH_POLICY')
            require(proof['source_target'] is None and value['original_target'] is None,
                    'HYPE_WATCH_TARGET_IS_FIXED_RETURN')
            require(moment_ms(policy['entry_expires_at']) == reference + 60_000
                    and times['approved_at'] < reference + 60_000
                    and type(policy['holding_time_limit_ms']) is int
                    and policy['holding_time_limit_ms'] == 86_400_000,
                    'HYPE_WATCH_LIVE_CLOCKS')
            expected_policy.update(entry_kind='NEXT_FULL_MINUTE_IOC',
                entry_expires_at=policy['entry_expires_at'], holding_time_limit_ms=86_400_000)
            entry, stop, take = p, p * Decimal('1.02'), p * Decimal('.98')
        elif family == 'sol_sff1361':
            legacy._keys(policy, POLICY_FIELDS, 'SELECTED_FORMULA_POLICY')
            require(proof['source_target'] is None and value['original_target'] is None,
                    'SELECTED_FORMULA_TARGET')
            entry, stop, take = p, p * Decimal('1.01'), p * Decimal('.98')
        else:
            legacy._keys(policy, SELECTED_PENDING_POLICY_FIELDS, 'SELECTED_FORMULA_POLICY')
            target = positive(proof['source_target'])
            if family == 'xrp_liquidity_inverse':
                require(target > p, 'SELECTED_FORMULA_SOURCE_TARGET')
                target = 2 * p - target
            require(0 < target < p, 'SELECTED_FORMULA_TARGET')
            require(positive(value['original_target']) == target,
                    'SELECTED_FORMULA_TARGET')
            if family == 'oconf0034_13':
                entry = p + Decimal('.2') * (p - target)
                stop, take = entry * Decimal('1.012'), target
            elif family == 'ct009':
                entry = p + Decimal('0.1814560294151306') * (p - target)
                stop = entry * Decimal('1.0040077843998312')
                take = entry + Decimal('0.9365681871771814') * (target - entry)
            else:
                entry = p + Decimal('0.2342784544453025') * (p - target)
                stop = entry * Decimal('1.0105286625253104')
                take = entry + Decimal('0.8671878740191461') * (target - entry)
            require(moment_ms(policy['pending_expires_at']) == reference + 86_400_000
                    and times['approved_at'] < reference + 86_400_000
                    and positive(policy['cancel_before_fill_price']) == target,
                    'SELECTED_FORMULA_PENDING_TERMS')
            expected_policy.update(pending_expires_at=policy['pending_expires_at'],
                cancel_before_fill_price=policy['cancel_before_fill_price'])
        require(policy == expected_policy, 'SELECTED_FORMULA_POLICY')
        require(all(positive(value[key]) == Decimal(selected_price(expected)) for key, expected in
                    (('entry', entry), ('stop', stop), ('take_profit', take))),
                'SELECTED_FORMULA_FROZEN_PRICES')


def _validate_approved(message):
    require = legacy._require
    legacy._canonical(message)
    legacy._keys(message, FIELDS, 'APPROVED_ALERT_FIELDS')
    value = deepcopy(message)
    require(value['version'] == VERSION and value['kind'] in RANK
            and value['family'] in ('maxpain', *FORMULAS, *SELECTED_FORMULAS), 'APPROVED_ALERT_VERSION')
    if value['family'] == 'maxpain':
        require(value['rule_id'] in SPECS, 'APPROVED_ALERT_RULE')
        spec = SPECS[value['rule_id']]
        require(value['symbol'] == spec[0] and value['side'] in ('LONG', 'SHORT')
                and (spec[-1] == 'BOTH' or value['side'] == spec[-1]),
                'APPROVED_ALERT_IDENTITY')
    elif value['family'] in SELECTED_FORMULAS:
        rule, symbols = SELECTED_FORMULAS[value['family']]
        require(value['rule_id'] == rule and value['symbol'] in symbols
                and value['side'] == 'SHORT', 'APPROVED_ALERT_IDENTITY')
    else:
        require((value['rule_id'], value['symbol']) == FORMULAS[value['family']]
                and value['side'] == 'SHORT', 'APPROVED_ALERT_IDENTITY')
    require(value['source_environment'] == 'mainnet'
            and value['execution_environment'] == 'mainnet'
            and value['source_price_kind'] == 'TRADE_1M', 'VENUE_CONTRACT')

    times = {key: moment_ms(value[key]) for key in ('source_at', 'created_at',
             'arm_at', 'approved_at', 'expires_at', 'source_as_of')}
    require(times['source_at'] <= times['created_at'] <= times['approved_at']
            and times['arm_at'] == times['approved_at']
            and (value['family'] in SELECTED_FORMULAS or times['approved_at'] % 60_000 == 0)
            and times['expires_at'] == (min(times['approved_at'] + LEASE_MS,
                times['source_at'] + 60_000) if value['family'] == HYPE_WATCH_FAMILY
                else times['approved_at'] + LEASE_MS)
            and times['source_as_of'] >= times['approved_at'], 'APPROVED_ALERT_TIMES')
    require(value['valid_until'] is None, 'APPROVED_ALERT_HAS_NO_HEARTBEAT_LEASE')
    require(type(value['source_sequence']) is int and value['source_sequence'] ==
            times['source_as_of'] * 10 + RANK[value['kind']], 'SOURCE_SEQUENCE')
    if value['kind'] == 'ALERT':
        require(value['source_state'] == 'APPROVED' and value['cancel_reason'] is None,
                'APPROVED_ALERT_STATE')
    else:
        require(value['source_state'] == 'CANCELED'
                and isinstance(value['cancel_reason'], str)
                and re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', value['cancel_reason']) is not None,
                'APPROVED_ALERT_CANCEL_STATE')

    policy, proof = value['policy'], value['proof']
    if value['family'] == 'maxpain':
        if value['rule_id'] == DOGE_PARTIAL_RULE:
            legacy._keys(policy, DOGE_PARTIAL_POLICY_FIELDS, 'DOGE_PARTIAL_POLICY')
            require(policy['name'] == 'DOGE_PARTIAL_TAKE_75_H24_V1'
                    and policy['entry_kind'] == 'LIMIT_AFTER_ALERT'
                    and policy['source_price'] == 'HYPERLIQUID_DOGE_PERPETUAL_TRADE_1M'
                    and policy['partial_fraction'] == '0.75'
                    and policy['stop_policy'] == 'KEEP_ORIGINAL', 'DOGE_PARTIAL_POLICY')
            partial = positive(policy['partial_take_price'])
            entry, take = positive(value['entry']), positive(value['take_profit'])
            require(entry < partial < take if value['side'] == 'LONG'
                    else take < partial < entry, 'DOGE_PARTIAL_PRICE_GEOMETRY')
            require(moment_ms(policy['hold_until']) == times['approved_at'] - 60_000 + 86_400_000,
                    'DOGE_PARTIAL_HOLDING_DEADLINE')
        else:
            legacy._keys(policy, POLICY_FIELDS, 'APPROVED_ALERT_POLICY')
            require(policy == dict(name='APPROVED_ALERT_LIMIT_GTC_V1',
                    entry_kind='LIMIT_AFTER_ALERT',
                    source_price=f"HYPERLIQUID_{value['symbol']}_PERPETUAL_TRADE_1M"),
                    'APPROVED_ALERT_POLICY')
        legacy._keys(proof, PROOF_FIELDS, 'APPROVED_ALERT_PROOF')
        require(proof['source_contract_version'] == SOURCE_CONTRACT
                and isinstance(proof['source_config_sha256'], str)
                and HEX.fullmatch(proof['source_config_sha256']) is not None,
                'SOURCE_PROVENANCE')
        if value['rule_id'] == DOGE_PARTIAL_RULE:
            require(proof['source_config_sha256'] == DOGE_SOURCE_CONFIG_SHA256,
                    'DOGE_PARTIAL_SOURCE_CONFIGURATION')
        require(isinstance(proof['cycle_id'], str) and 0 < len(proof['cycle_id']) <= 200
                and proof['timeframe'] in legacy.TIMEFRAMES, 'APPROVED_ALERT_SOURCE_IDENTITY')
        target = positive(value['original_target'])
        require(proof['episode_key'] == format(target.normalize(), 'f'),
                'APPROVED_ALERT_TARGET_IDENTITY')
    elif value['family'] in SELECTED_FORMULAS:
        _validate_selected_formula(value, times)
    else:
        require(proof['source_contract_version'] == SOURCE_CONTRACT
                and isinstance(proof['source_config_sha256'], str)
                and HEX.fullmatch(proof['source_config_sha256']) is not None, 'SOURCE_PROVENANCE')
        reference = moment_ms(proof['decision_at']) + 60_000
        original_times = dict(source_at=reference, created_at=reference, arm_at=reference,
            expires_at=None if value['family'] == 'sol_g65' else reference + LEASE_MS)
        getattr(legacy, '_validate_' + value['family'])(value, original_times)
        require(times['source_at'] == times['created_at'] == reference,
                'APPROVED_FORMULA_REFERENCE')
        require(times['approved_at'] >= reference + 60_000 if value['family'] == 'sol_g65'
                else times['approved_at'] == reference, 'APPROVED_FORMULA_TIME')
    entry, stop, take = (positive(value[key]) for key in ('entry', 'stop', 'take_profit'))
    require(stop < entry < take if value['side'] == 'LONG' else take < entry < stop,
            'PRICE_GEOMETRY')
    require(isinstance(value['occurrence_id'], str)
            and HEX.fullmatch(value['occurrence_id']) is not None
            and value['occurrence_id'] == occurrence_id(value), 'OCCURRENCE_IDENTITY')
    return value


def validate(message):
    if not is_approved(message):
        return legacy.validate(message)
    try:
        return _validate_approved(message)
    except ContractError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ContractError('INVALID_APPROVED_ALERT_SHAPE') from None


validate_message = validate
normalize = validate
