"""Allowlisted, one-way Testnet card rows for the journal application.

These are display data only. The application cannot return a row as execution
evidence, an instruction, or an acknowledgement from Hyperliquid.
"""
from datetime import datetime, timezone

from . import card_display_projection as display, card_lifecycle as life
from . import trade_cards

FIELDS = frozenset({
    'symbol', 'side', 'entry_price', 'stop_price', 'take_profit_price',
    'status', 'quantity_entered', 'quantity_closed', 'quantity_remaining',
    'realized_pnl', 'pnl_verified', 'closure_verified',
})


def _observed(ms):
    life.moment(ms)
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def received(card, *, observed_at_ms):
    """Show a received alert without suggesting that an order was sent."""
    card = trade_cards.validate_card(card)
    if card['record_kind'] != 'received_alert' or card['state'] != 'RECORDED_ONLY':
        raise life.LifecycleError('RECEIVED_ALERT_CARD_REQUIRED')
    prices = card['prepared']['execution']
    payload = dict(symbol=prices['symbol'], side=prices['side'].lower(),
        entry_price=prices['entry'], stop_price=prices['stop'],
        take_profit_price=prices['take_profit'], status='RECORDED_ONLY',
        quantity_entered=None, quantity_closed=None, quantity_remaining=None,
        realized_pnl=None, pnl_verified=False, closure_verified=False)
    return dict(card_id=card['card_id'], revision=card['revision'],
                observed_at=_observed(observed_at_ms), payload=payload)


def lifecycle(projected, card_id):
    """Flatten one locally verified lifecycle view into the app's fixed shape."""
    if (not isinstance(projected, dict) or projected.get('version') != display.VERSION
            or projected.get('environment') != 'testnet'
            or projected.get('read_only') is not True):
        raise life.LifecycleError('VERIFIED_DISPLAY_COPY_REQUIRED')
    life.ident(card_id, r'[0-9a-f]{64}')
    matches = [c for c in projected['cards'] if c['card_id'] == card_id]
    if len(matches) != 1:
        raise life.LifecycleError('DISPLAY_CARD_ID_MISMATCH')
    card = matches[0]
    prices = card['prices']
    side = card['direction']
    if side not in ('LONG', 'SHORT') or set(prices) != {'entry', 'stop', 'take_profit'}:
        raise life.LifecycleError('DISPLAY_CARD_TERMS_INVALID')
    verified = card['closure_verified'] is True and card['final_net_usdc'] is not None
    payload = dict(symbol=card['symbol'], side=side.lower(),
        entry_price=prices['entry'], stop_price=prices['stop'],
        take_profit_price=prices['take_profit'], status=card['state'],
        quantity_entered=card['entry_quantity'], quantity_closed=card['exit_quantity'],
        quantity_remaining=card['remaining_quantity'],
        realized_pnl=card['final_net_usdc'] if verified else None,
        pnl_verified=verified, closure_verified=card['closure_verified'] is True)
    return dict(card_id=card_id, revision=projected['revision'],
                observed_at=_observed(projected['observed_at_ms']), payload=payload)
