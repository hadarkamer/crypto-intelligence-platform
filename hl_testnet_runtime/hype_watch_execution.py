"""New HYPE execution timing from actual owned fills; no sender or scheduler."""
import approved_alert_contract as contract
from . import card_lifecycle as life

RULE = 'HYPE_WATCH_LIQ4050_24H_INVERSE'


def is_hype(source):
    return (contract.is_approved(source)
            and source.get('family') == 'hype_watch_liq4050'
            and source.get('rule_id') == RULE and source.get('symbol') == 'HYPE')


def holding_deadline(source, entry_fills):
    """Start 24h at the first exchange-confirmed fill, never at a planned OPEN.

    Later partial fills do not extend it. Empty/unconfirmed fills provide no
    deadline and cannot authorize a close. Frozen research time stays in source.
    """
    rows = tuple(entry_fills)
    if not is_hype(source) or not rows:
        return None
    limit = contract.position_holding_limit_ms(source)
    if limit != 86400000:
        raise ValueError('HYPE_EXACT_HOLDING_LIMIT_REQUIRED')
    times = [life.moment(row['at_ms']) for row in rows]
    return min(times) + limit


def timeout_due(trade, now):
    deadline = holding_deadline(trade['source'], trade['entry_fills'].values())
    return deadline is not None and now >= deadline
