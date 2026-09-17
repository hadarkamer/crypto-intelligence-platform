"""Keep a valid delivered alert even when Testnet cannot prepare its asset.

This is an unprepared DATA card, never an order or an execution queue. Missing
exchange precision is not guessed. A later preparation needs its own reviewed
transition; this module never silently turns an archived alert into a trade.
"""
from copy import deepcopy
import alert_cards_wire as wire
from .risk_policy import CURRENT_RISK_USD, VERSION as RISK_VERSION

VERSION = 'testnet-unavailable-source-card-v1'
STATE = 'RECORDED_ASSET_UNAVAILABLE'
REASON = 'ASSET_UNAVAILABLE'


def prepare(value):
    """Revalidate the exact allowlisted producer contract before any storage."""
    from .trade_cards import checksum, ROLES
    spec = wire.normalize(value)
    source = spec['signal']
    return dict(version=VERSION, environment='testnet',
        card_id=checksum(['testnet', spec['source_stream'], source['event_id']]),
        source_stream=spec['source_stream'], event_id=source['event_id'],
        record_kind='received_alert',
        rule=dict(id=spec['rule_id'], threshold_pct=spec['threshold_pct']),
        prepared=dict(source=deepcopy(source), execution=None,
            audit=dict(source_digest=checksum(source), rounding_applied=False,
                       source_time_changed=False, preparation_blocker=REASON)),
        account_role=ROLES[source['side']],
        planning=dict(quantity=None, distance_risk_usd=None, cancel_price=None,
                      positive_quantity=False),
        risk=dict(planned_usd=CURRENT_RISK_USD, policy=RISK_VERSION, costs_included=False),
        state=STATE, actual_execution=None, dispatch_enabled=False, revision=1,
        delivery=deepcopy(value))


def validate(card):
    from .trade_cards import canonical, CardError
    try:
        expected = prepare(card['delivery'])
        if canonical(expected) != canonical(card):
            raise ValueError()
        return deepcopy(card)
    except (KeyError, TypeError, ValueError):
        raise CardError('UNPREPARED_CARD_CONTENT_NOT_VERIFIED') from None
