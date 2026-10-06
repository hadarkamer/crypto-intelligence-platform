"""Strict, pure contract for prospective experimental Testnet plans.

This contract is deliberately separate from delivered Telegram cards. Its
authentication, ordering, tombstones and venue admission belong to the receiver.
Validation grants no authority to send an exchange order.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

VERSION = 'experimental-execution-plan-v1'
SOURCE_CONTRACT = 'all-hyperliquid-perpetual-trade1m-v2'
LEASE_MS = 90_000
MAX_BYTES = 65536
TIMEFRAMES = ('12h', '24h', '48h', '3d', '1w', '2w', '1m')
TIERS = tuple(map(Decimal, ('.15', '.20', '.15', '.25', '.25', '.30')))
SPECS = {
    'SOL_MAXPAIN_DIST1_3_RANGE24': ('SOL', 1., 3., 2., 1., False, TIMEFRAMES, True, 'BOTH'),
    'HYPE_MAXPAIN_DIST05_15_LONG_TF': ('HYPE', .5, 1.5, 2., .5, True, TIMEFRAMES[3:], False, 'BOTH'),
    'DOGE_MAXPAIN_DIST15_25_LONG_TF': ('DOGE', 1.5, 2.5, 2., .5, False, TIMEFRAMES[3:], False, 'BOTH'),
    'XRP_MAXPAIN_LONG_DIST2_4_SHORT_TF': ('XRP', 2., 4., .5, 1., True, TIMEFRAMES[:3], False, 'LONG'),
    'ETH_MAXPAIN_LONG_DIST1_3': ('ETH', 1., 3., 2., .5, True, TIMEFRAMES, False, 'LONG'),
}
FIELDS = frozenset(('version', 'kind', 'family', 'occurrence_id', 'rule_id', 'symbol', 'side',
    'source_environment', 'execution_environment', 'source_price_kind', 'source_at',
    'created_at', 'arm_at', 'expires_at', 'entry', 'stop', 'take_profit', 'original_target',
    'policy', 'proof', 'source_sequence', 'source_as_of', 'source_state', 'valid_until', 'cancel_reason'))
TEMPORAL = frozenset(('kind', 'source_sequence', 'source_as_of', 'source_state', 'valid_until', 'cancel_reason'))
RANK = {'PLAN': 1, 'HEARTBEAT': 2, 'CANCEL': 3}
HEX = re.compile(r'[0-9a-f]{64}\Z')


class ContractError(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise ContractError(reason)


def _keys(value, keys, reason):
    _require(isinstance(value, dict) and set(value) == set(keys), reason)


def _canonical(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        _require(len(raw) <= MAX_BYTES, 'CONTRACT_TOO_LARGE')
        return raw
    except (TypeError, ValueError, RecursionError):
        raise ContractError('INVALID_CONTRACT_JSON') from None


def moment_ms(value):
    try:
        _require(isinstance(value, str) and len(value) <= 40, 'UTC_TIME_REQUIRED')
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        _require(dt.utcoffset() is not None and dt.utcoffset().total_seconds() == 0,
                 'UTC_TIME_REQUIRED')
        stamp = dt.timestamp() * 1000
        _require(stamp.is_integer() and stamp > 0, 'MILLISECOND_TIME_REQUIRED')
        return int(stamp)
    except (ValueError, OverflowError, TypeError):
        raise ContractError('UTC_TIME_REQUIRED') from None


def iso_ms(value):
    _require(type(value) is int and value > 0, 'MILLISECOND_TIME_REQUIRED')
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def positive(value):
    try:
        _require(isinstance(value, str) and len(value) <= 80 and
                 re.fullmatch(r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', value), 'PRICE_STRING_REQUIRED')
        number = Decimal(value)
        _require(number.is_finite() and Decimal('1e-15') <= number <= Decimal('1e15'), 'PRICE_RANGE')
        return number
    except (InvalidOperation, TypeError):
        raise ContractError('PRICE_STRING_REQUIRED') from None


def price(value):
    _require(not isinstance(value, bool), 'PRICE_STRING_REQUIRED')
    result = format(Decimal(str(value)), 'f')
    positive(result)
    return result


def _same(actual, expected):
    _require(positive(actual) == Decimal(str(expected)), 'FROZEN_PRICE_MISMATCH')


def occurrence_id(value):
    proof = value['proof']
    identity = dict(family=value['family'], rule_id=value['rule_id'], symbol=value['symbol'],
                    source_contract_version=proof['source_contract_version'])
    if value['family'] in ('r2732', 'hype_row71205', 'sol_g65'):
        identity['decision_at'] = iso_ms(moment_ms(proof['decision_at']))
    else:
        identity.update(cycle_id=proof['cycle_id'], episode_key=proof['episode_key'],
                        timeframe=proof['timeframe'], side=value['side'])
    return hashlib.sha256(_canonical(identity)).hexdigest()


def immutable(value):
    """Exclude source leases and recipient-local observation bookkeeping."""
    result = {key: deepcopy(val) for key, val in value.items() if key not in TEMPORAL}
    # Same Watch episode can be first observed at different local times by two
    # recipients. That must not manufacture a second execution occurrence.
    if isinstance(result.get('proof'), dict):
        result['proof'].pop('episode_first_ms', None)
        result['proof'].pop('episode_generation', None)
    return result


def plan_digest(value):
    return hashlib.sha256(_canonical(immutable(value))).hexdigest()


def _validate_r2732(value, times):
    policy, proof = value['policy'], value['proof']
    _require(value['rule_id'] == 'R2732_XRP_SHORT_NY_WEEKDAYS_LOCK' and value['symbol'] == 'XRP'
             and value['side'] == 'SHORT' and value['original_target'] is None, 'R2732_IDENTITY')
    _keys(proof, ('source_contract_version', 'source_config_sha256', 'decision_at'), 'R2732_PROOF')
    _keys(policy, ('name', 'original_risk_distance', 'lock_trigger_price', 'locked_stop',
                   'trigger_R', 'profit_R', 'effective', 'barrier_precedence', 'source_price'), 'R2732_POLICY')
    _require(policy['name'] == 'R2732_CLOSED_MINUTE_LOCK_V1' and policy['trigger_R'] == '2'
             and policy['profit_R'] == '0.5' and policy['effective'] == 'NEXT_MINUTE'
             and policy['barrier_precedence'] == 'OLD_STOP_TAKE_FIRST'
             and policy['source_price'] == 'HYPERLIQUID_XRP_PERPETUAL_TRADE_1M', 'R2732_POLICY')
    decision = moment_ms(proof['decision_at'])
    _require(decision % 900_000 == 0 and times['source_at'] == decision + 60_000
             and times['created_at'] == times['source_at'] == times['arm_at']
             and times['expires_at'] == times['source_at'] + 90_000, 'R2732_TIMES')
    entry = float(positive(value['entry']))
    stop = entry * 1.005
    risk = abs(entry - stop)
    _same(value['stop'], stop); _same(value['take_profit'], entry * .92)
    _same(policy['original_risk_distance'], risk)
    _same(policy['lock_trigger_price'], entry - 2 * risk)
    _same(policy['locked_stop'], entry - .5 * risk)


def _validate_hype_row71205(value, times):
    policy, proof = value['policy'], value['proof']
    _require(value['rule_id'] == 'HYPE_ROW71205_SHORT' and value['symbol'] == 'HYPE'
             and value['side'] == 'SHORT' and value['original_target'] is None, 'HYPE_ROW71205_IDENTITY')
    _keys(proof, ('source_contract_version', 'source_config_sha256', 'decision_at'), 'HYPE_ROW71205_PROOF')
    _keys(policy, ('name', 'entry_kind', 'capacity', 'holding_timeout', 'source_price'), 'HYPE_ROW71205_POLICY')
    _require(policy == dict(name='HYPE_ROW71205_MARKET_NEXT_MINUTE_V1', entry_kind='MARKET_REFERENCE',
        capacity=1, holding_timeout=None, source_price='HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M')
        and type(policy['capacity']) is int, 'HYPE_ROW71205_POLICY')
    decision = moment_ms(proof['decision_at'])
    _require(decision % 1800_000 == 0 and times['source_at'] == decision + 60_000
             and times['created_at'] == times['source_at'] == times['arm_at']
             and times['expires_at'] == times['source_at'] + 90_000, 'HYPE_ROW71205_TIMES')
    entry = float(positive(value['entry']))
    _same(value['stop'], entry * 1.005); _same(value['take_profit'], entry * .98)


def _validate_sol_g65(value, times):
    policy, proof = value['policy'], value['proof']
    _require(value['rule_id'] == 'SOL_G65_K49_PROFIT_LOCK' and value['symbol'] == 'SOL'
             and value['side'] == 'SHORT' and value['original_target'] is None, 'SOL_G65_IDENTITY')
    _keys(proof, ('source_contract_version', 'source_config_sha256', 'decision_at', 'reference_price'), 'SOL_G65_PROOF')
    _keys(policy, ('name', 'entry_kind', 'capacity', 'pending_timeout', 'holding_timeout',
        'pending_cancel_price', 'original_take_distance', 'lock_trigger_price', 'locked_stop',
        'trigger_take_fraction', 'profit_take_fraction', 'effective', 'entry_bar_trigger',
        'later_bar_trigger', 'barrier_precedence', 'source_price'), 'SOL_G65_POLICY')
    _require(policy['name'] == 'SOL_G65_CLOSED_MINUTE_LOCK_V1' and policy['entry_kind'] == 'LIMIT_PRETOUCH'
        and type(policy['capacity']) is int and policy['capacity'] == 1
        and policy['pending_timeout'] is None and policy['holding_timeout'] is None
        and policy['trigger_take_fraction'] == '0.75' and policy['profit_take_fraction'] == '0.25'
        and policy['effective'] == 'NEXT_MINUTE' and policy['entry_bar_trigger'] == 'CLOSE_ONLY'
        and policy['later_bar_trigger'] == 'LOW' and policy['barrier_precedence'] == 'OLD_STOP_TAKE_FIRST'
        and policy['source_price'] == 'HYPERLIQUID_SOL_PERPETUAL_TRADE_1M', 'SOL_G65_POLICY')
    decision = moment_ms(proof['decision_at'])
    _require(decision % 1800_000 == 0 and times['source_at'] == decision + 60_000
        and times['arm_at'] == times['created_at'] == times['source_at']
        and times['expires_at'] is None, 'SOL_G65_TIMES')
    if value['kind'] == 'PLAN':
        _require(times['source_at'] <= times['source_as_of'] < times['source_at'] + 60_000,
                 'PROSPECTIVE_PLAN_REQUIRED')
    reference = float(positive(proof['reference_price']))
    entry, take = reference * 1.005, reference * .9825
    distance = entry - take
    _same(value['entry'], entry); _same(value['stop'], reference * 1.0125)
    _same(value['take_profit'], take); _same(policy['pending_cancel_price'], reference * .99)
    _same(policy['original_take_distance'], distance)
    _same(policy['lock_trigger_price'], entry - .75 * distance)
    _same(policy['locked_stop'], entry - .25 * distance)


def _validate_clusters(value):
    proof = value['proof']
    comparisons = proof['cluster_comparisons']
    _require(isinstance(comparisons, list) and len(comparisons) <= 256, 'CLUSTER_PROOF_SIZE')
    _require(not comparisons or value['policy']['liquidity_growth'], 'GROWTH_NOT_ALLOWED')
    for comparison in comparisons:
        _keys(comparison, ('previous_target', 'previous_timeframe', 'previous_direction', 'tiers'), 'CLUSTER_PROOF')
        _require(comparison['previous_timeframe'] in TIMEFRAMES and
                 type(comparison['previous_direction']) is int and
                 comparison['previous_direction'] == (1 if value['side'] == 'LONG' else -1), 'CLUSTER_DIRECTION')
        old, new = TIMEFRAMES.index(comparison['previous_timeframe']), TIMEFRAMES.index(proof['timeframe'])
        tiers = comparison['tiers']
        _require(new > old and isinstance(tiers, list) and len(tiers) == new - old + 1, 'CLUSTER_ADJACENT_CHAIN_REQUIRED')
        amounts, targets = [], []
        for index, tier in enumerate(tiers):
            _keys(tier, ('timeframe', 'target_price', 'liquidation_amount', 'source_side', 'provider_valid'), 'CLUSTER_TIER')
            _require(tier['timeframe'] == TIMEFRAMES[old + index] and tier['provider_valid'] is True
                     and tier['source_side'] == proof['source_side'], 'CLUSTER_TIER_LINEAGE')
            targets.append(positive(tier['target_price'])); amounts.append(positive(tier['liquidation_amount']))
        _require(targets[0] == positive(comparison['previous_target']) and
                 targets[-1] == positive(value['original_target']), 'CLUSTER_ENDPOINTS')
        _require(max(targets) - min(targets) <= Decimal('.002') * min(targets), 'CLUSTER_TARGET_DISTANCE')
        _require(all(amounts[i + 1] >= amounts[i] * (1 + TIERS[old + i])
                     for i in range(len(amounts) - 1)), 'CLUSTER_LIQUIDITY_GROWTH')
        _require(amounts[-1] == positive(proof['liquidation_amount']), 'CLUSTER_INCOMING_AMOUNT')


def _validate_maxpain(value, times):
    policy, proof = value['policy'], value['proof']
    _require(value['rule_id'] in SPECS, 'MAXPAIN_FORMULA')
    coin, lower, upper, adverse, take, growth, tfs, range24, direction = SPECS[value['rule_id']]
    _require(value['symbol'] == coin, 'MAXPAIN_SYMBOL')
    _keys(policy, ('name', 'entry_adverse', 'take_fraction', 'stop_distance_multiplier',
                   'overlap_target_fraction', 'liquidity_growth', 'source_price'), 'MAXPAIN_POLICY')
    _require(policy['name'] == 'MAXPAIN_LIMIT_PRETOUCH_V1' and
             positive(policy['entry_adverse']) == Decimal(str(adverse)) and
             positive(policy['take_fraction']) == Decimal(str(take)) and
             policy['stop_distance_multiplier'] == '5' and policy['overlap_target_fraction'] == '0.002'
             and policy['liquidity_growth'] is growth and
             policy['source_price'] == f'HYPERLIQUID_{coin}_PERPETUAL_TRADE_1M', 'MAXPAIN_POLICY')
    _keys(proof, ('source_contract_version', 'source_config_sha256', 'cycle_id', 'episode_key',
        'episode_generation', 'episode_first_ms', 'timeframe', 'source_side', 'source_observed_ms',
        'source_quote', 'liquidation_amount', 'cluster_comparisons', 'range24_low', 'range24_high',
        'source_price'), 'MAXPAIN_PROOF')
    _require(isinstance(proof['cycle_id'], str) and 0 < len(proof['cycle_id']) <= 200 and
             proof['timeframe'] in tfs and type(proof['episode_generation']) is int and
             proof['episode_generation'] >= 1 and type(proof['episode_first_ms']) is int and
             0 < proof['episode_first_ms'] < times['arm_at'], 'MAXPAIN_EPISODE')
    target, source = float(positive(value['original_target'])), float(positive(proof['source_price']))
    signed = target - source
    _require(signed != 0 and value['side'] == ('LONG' if signed > 0 else 'SHORT') and
             proof['source_side'] == ('SHORT' if signed > 0 else 'LONG') and
             (direction == 'BOTH' or value['side'] == direction), 'MAXPAIN_DIRECTION')
    _require(proof['episode_key'] == format(Decimal(str(target)).normalize(), 'f'), 'MAXPAIN_TARGET_KEY')
    _require(lower <= abs(signed) / source * 100 < upper, 'MAXPAIN_DISTANCE')
    _same(value['entry'], source - adverse * signed)
    _same(value['stop'], source - 5 * signed)
    _same(value['take_profit'], source + take * signed)
    _require(times['arm_at'] % 60_000 == 0 and times['created_at'] < times['arm_at'] <= times['created_at'] + 360_000
             and times['expires_at'] == times['arm_at'] + 86400_000, 'MAXPAIN_TIMES')
    _require(type(proof['source_observed_ms']) is int and proof['source_observed_ms'] == times['source_at']
             and times['created_at'] - 1800_000 <= times['source_at'] <= times['created_at'], 'MAXPAIN_SOURCE_TIME')
    quote = proof['source_quote']
    _keys(quote, ('price_source', 'price_market', 'price_pair', 'price_instrument'), 'MAXPAIN_SOURCE_QUOTE')
    _require(quote == dict(price_source=policy['source_price'], price_market='perpetual',
                          price_pair=coin + '-PERP', price_instrument=coin), 'MAXPAIN_SOURCE_QUOTE')
    # Liquidity is evidence for the near-target growth exception, not a
    # universal predicate of an ordinary nonoverlapping MaxPain plan.
    if proof['liquidation_amount'] is None:
        _require(proof['cluster_comparisons'] == [], 'CLUSTER_INCOMING_AMOUNT_REQUIRED')
    else:
        positive(proof['liquidation_amount'])
    if range24:
        _require(positive(proof['range24_low']) <= positive(value['original_target']) <=
                 positive(proof['range24_high']), 'MAXPAIN_RANGE24')
    else:
        _require(proof['range24_low'] is None and proof['range24_high'] is None, 'MAXPAIN_RANGE24')
    _validate_clusters(value)


def _validate(message):
    """Return a detached validated envelope; no wall clock, state or I/O."""
    _canonical(message)
    _keys(message, FIELDS, 'CONTRACT_FIELDS')
    value = deepcopy(message)
    _require(value['version'] == VERSION and value['kind'] in RANK and value['family'] in
             ('r2732', 'maxpain', 'hype_row71205', 'sol_g65'), 'CONTRACT_VERSION')
    _require(value['source_environment'] == 'mainnet' and value['execution_environment'] == 'testnet'
             and value['source_price_kind'] == 'TRADE_1M', 'VENUE_CONTRACT')
    _require(value['side'] in ('LONG', 'SHORT'), 'FINAL_DIRECTION_REQUIRED')
    times = {key: moment_ms(value[key]) for key in
             ('source_at', 'created_at', 'arm_at', 'source_as_of', 'valid_until')}
    times['expires_at'] = (None if value['family'] == 'sol_g65' and value['expires_at'] is None
                           else moment_ms(value['expires_at']))
    _require(times['source_as_of'] >= times['created_at'] and times['valid_until'] <=
             min(times['expires_at'] or times['source_as_of'] + LEASE_MS,
                 times['source_as_of'] + LEASE_MS), 'SOURCE_LEASE')
    if value['kind'] != 'CANCEL':
        _require(times['source_as_of'] < times['valid_until'] and value['cancel_reason'] is None, 'SOURCE_LEASE')
        _require(value['source_state'] == 'PENDING', 'PENDING_SOURCE_REQUIRED')
    else:
        _require(isinstance(value['cancel_reason'], str) and
                 re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', value['cancel_reason']) is not None,
                 'CANCEL_REASON_REQUIRED')
        _require(isinstance(value['source_state'], str) and
                 re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', value['source_state']) is not None, 'CANCEL_STATE_REQUIRED')
    _require(type(value['source_sequence']) is int and value['source_sequence'] ==
             times['source_as_of'] * 10 + RANK[value['kind']], 'SOURCE_SEQUENCE')
    entry, stop, take = (positive(value[key]) for key in ('entry', 'stop', 'take_profit'))
    _require(stop < entry < take if value['side'] == 'LONG' else take < entry < stop, 'PRICE_GEOMETRY')
    if value['family'] == 'r2732':
        _validate_r2732(value, times)
    elif value['family'] == 'hype_row71205':
        _validate_hype_row71205(value, times)
    elif value['family'] == 'sol_g65':
        _validate_sol_g65(value, times)
    else:
        _validate_maxpain(value, times)
        if value['kind'] == 'PLAN':
            _require(times['source_as_of'] < times['arm_at'], 'PROSPECTIVE_PLAN_REQUIRED')
    proof = value['proof']
    _require(proof['source_contract_version'] == SOURCE_CONTRACT and
             isinstance(proof['source_config_sha256'], str) and
             HEX.fullmatch(proof['source_config_sha256']), 'SOURCE_PROVENANCE')
    _require(isinstance(value['occurrence_id'], str) and HEX.fullmatch(value['occurrence_id']) and
             value['occurrence_id'] == occurrence_id(value), 'OCCURRENCE_IDENTITY')
    return value


def validate(message):
    """All malformed external shapes use the same explicit validation boundary."""
    try:
        return _validate(message)
    except ContractError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError, InvalidOperation):
        raise ContractError('INVALID_CONTRACT_SHAPE') from None


normalize = validate
