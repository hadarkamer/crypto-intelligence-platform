"""Select delivered, fresh Testnet alerts without choosing a formula.

The alert producer decides which formulas are active. This module only reads
signed, recorded cards and returns their identities. It never starts orders.
"""
from datetime import datetime, timezone

from .filled_dispatch_store import DispatchError
from .source_window import source_fresh, timestamp
from .trade_card_store import CardStore
from . import trade_cards


def eligible(card, *, not_before, now):
    card = trade_cards.validate_card(card)
    source_at = timestamp(card['prepared']['source']['at'])
    return (card['record_kind'] == 'received_alert'
            and card['state'] == 'RECORDED_ONLY'
            and card['account_role'] in ('long_account', 'short_account')
            and card['dispatch_enabled'] is False
            and source_at >= not_before
            and 'source_expires_at' in card
            and source_fresh(source_at, card['source_expires_at'], now=now))


def next_cards(journal, *, not_before, now=None):
    """Return bounded card IDs; caller still needs account and venue gates."""
    now = datetime.now(timezone.utc) if now is None else now
    not_before = timestamp(not_before)
    if not_before > now:
        raise DispatchError('ALERT_SELECTION_START_IN_FUTURE')
    with journal._transaction() as conn:
        CardStore(journal).ready(conn)
        rows = conn.execute('''SELECT card_id,manifest,digest FROM hl_testnet_cards_v1.cards
            WHERE created_at >= %s
              AND (manifest->>'source_expires_at')::timestamptz > %s
              AND EXISTS (SELECT 1 FROM hl_testnet_cards_v1.delivery_receipts r
                  WHERE r.card_id=cards.card_id AND r.status='RECORDED')
            ORDER BY created_at,card_id LIMIT 65''', (not_before, now)).fetchall()
    if len(rows) > 64:
        raise DispatchError('FRESH_ALERT_PAGE_OVERFLOW_REQUIRES_REVIEW')
    result = []
    for card_id, raw, digest in rows:
        if trade_cards.checksum(raw) != digest:
            raise DispatchError('STORED_CARD_CHECKSUM_MISMATCH')
        card = trade_cards.validate_card(raw)
        if card['card_id'] != card_id:
            raise DispatchError('STORED_CARD_ID_MISMATCH')
        if eligible(card, not_before=not_before, now=now):
            result.append(card_id)
    return result
