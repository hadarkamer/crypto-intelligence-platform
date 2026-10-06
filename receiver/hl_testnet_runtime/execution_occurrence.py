"""Exact delivered occurrence deduplication across notification recipients.

Keep every immutable card and delivery receipt. A second destination's intent
must not open the same strategy occurrence after the first card closes. This
does not merge nearby timestamps, prices, formulas, families or market waves.
No environment access, network, mutation or execution authorization.
"""
import re

from . import trade_cards

STREAM = re.compile(r'(manual|dual_cvd65|u21_xrp_short):[0-9a-f]{64}\Z')


def identity(card):
    """Exact producer-family occurrence key, or None for unsupported records."""
    card = trade_cards.validate_card(card)
    family = STREAM.fullmatch(card['source_stream'])
    if (family is None or card['record_kind'] != 'received_alert'
            or card['state'] != 'RECORDED_ONLY'):
        return None
    source = card['prepared']['source']
    return trade_cards.checksum(['delivered-strategy-occurrence-v1', family[1],
        card['rule'], source['at'], source['symbol'], source['side'],
        source['entry'], source['stop'], source['take_profit']])


def duplicate_attempt(state, card_id):
    """Return an exact previously attempted peer card; never infer from price.

    Dispatch persists its attempt marker before HTTP, and retains it after
    restart and finality. An unresolved request also blocks the ordinary
    dispatcher globally. Old final rejection fences cover earlier records
    without an attempt-timing marker. Locally recorded, never-attempted peers
    do not prevent the earliest legitimate candidate from being considered.
    """
    current = state['originals'][card_id]['card']
    key = identity(current)
    if key is None:
        return None
    attempted = ({binding['card_id'] for binding in state['bindings']}
                 | set(state.get('entry_timing_armed', {})))
    for peer_id, original in sorted(state['originals'].items()):
        if peer_id == card_id:
            continue
        if (peer_id not in attempted
                and original.get('entry_rejected_no_retry') is not True
                and original.get('entry_unsent_no_retry') is not True):
            continue
        peer = original['card']
        if (peer['account_role'] == current['account_role']
                and identity(peer) == key):
            return peer_id
    return None
