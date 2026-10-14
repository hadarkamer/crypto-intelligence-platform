"""Keep a valid delivered alert even when Testnet cannot prepare its asset.

This is an unprepared DATA card, never an order or an execution queue. Missing
exchange precision is not guessed. A later preparation needs its own reviewed
transition; this module never silently turns an archived alert into a trade.
"""
from copy import deepcopy
import alert_cards_wire as wire
from .risk_policy import card_profile

VERSION = 'mainnet-unavailable-source-card-v1'
STATE = 'RECORDED_ASSET_UNAVAILABLE'
REASON = 'ASSET_UNAVAILABLE'


def prepare(value, *, _historical_risk_usd=None):
    """Revalidate the exact allowlisted producer contract before any storage."""
    from .trade_cards import checksum, ROLES
    spec = wire.normalize(value)
    source = spec['signal']
    card = dict(version=VERSION, environment='mainnet',
        card_id=checksum(['mainnet', spec['source_stream'], source['event_id']]),
        source_stream=spec['source_stream'], event_id=source['event_id'],
        record_kind='received_alert',
        rule=dict(id=spec['rule_id'], threshold_pct=spec['threshold_pct']),
        prepared=dict(source=deepcopy(source), execution=None,
            audit=dict(source_digest=checksum(source), rounding_applied=False,
                       source_time_changed=False, preparation_blocker=REASON)),
        account_role=ROLES[source['side']],
        planning=dict(quantity=None, distance_risk_usd=None, cancel_price=None,
                      positive_quantity=False),
        risk=card_profile(_historical_risk_usd),
        state=STATE, actual_execution=None, dispatch_enabled=False, revision=1,
        delivery=deepcopy(value))
    if spec.get('source_expires_at') is not None:
        card['source_expires_at'] = spec['source_expires_at']
    return card


def validate(card):
    from .trade_cards import canonical, CardError
    try:
        expected = prepare(card['delivery'],_historical_risk_usd=card['risk']['planned_usd'])
        if canonical(expected) != canonical(card):
            raise ValueError()
        return deepcopy(card)
    except (KeyError, TypeError, ValueError):
        raise CardError('UNPREPARED_CARD_CONTENT_NOT_VERIFIED') from None
