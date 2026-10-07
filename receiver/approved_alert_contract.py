"""Authenticated approved-alert data, separate from prospective v1 plans.

An ALERT states that the producer has already approved an occurrence. Its three
prices are copied from that alert, not recalculated from a formula here. The
first-admission deadline is not a pending-order or position lifetime. A CANCEL
retires entry permission only; exchange fills remain owned by the receiver.

The facade keeps v1 readable for historical recovery. Release admission must
explicitly choose the supported version; validation is never order authority.
"""
from copy import deepcopy
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
SPECS = legacy.SPECS
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


def _validate_approved(message):
    require = legacy._require
    legacy._canonical(message)
    legacy._keys(message, FIELDS, 'APPROVED_ALERT_FIELDS')
    value = deepcopy(message)
    require(value['version'] == VERSION and value['kind'] in RANK
            and value['family'] == 'maxpain', 'APPROVED_ALERT_VERSION')
    require(value['rule_id'] in SPECS, 'APPROVED_ALERT_RULE')
    spec = SPECS[value['rule_id']]
    require(value['symbol'] == spec[0] and value['side'] in ('LONG', 'SHORT')
            and (spec[-1] == 'BOTH' or value['side'] == spec[-1]),
            'APPROVED_ALERT_IDENTITY')
    require(value['source_environment'] == 'mainnet'
            and value['execution_environment'] == 'testnet'
            and value['source_price_kind'] == 'TRADE_1M', 'VENUE_CONTRACT')

    times = {key: moment_ms(value[key]) for key in ('source_at', 'created_at',
             'arm_at', 'approved_at', 'expires_at', 'source_as_of')}
    require(times['source_at'] <= times['created_at'] <= times['approved_at']
            and times['arm_at'] == times['approved_at']
            and times['approved_at'] % 60_000 == 0
            and times['expires_at'] == times['approved_at'] + LEASE_MS
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
    require(isinstance(proof['cycle_id'], str) and 0 < len(proof['cycle_id']) <= 200
            and proof['timeframe'] in legacy.TIMEFRAMES, 'APPROVED_ALERT_SOURCE_IDENTITY')
    target = positive(value['original_target'])
    require(proof['episode_key'] == format(target.normalize(), 'f'),
            'APPROVED_ALERT_TARGET_IDENTITY')
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
