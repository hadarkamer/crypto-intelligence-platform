"""Read back one selected DATA receipt on the server; no network or order action."""
import hashlib
import re
import alert_cards_wire as wire
from .trade_card_store import CardStore
from .postgres_journal import JournalError


def review(identity, journal):
    if not isinstance(identity, str) or not wire.HEX.fullmatch(identity):
        raise JournalError('EXACT_RECEIPT_ID_REQUIRED')
    with journal._transaction() as conn:
        row = conn.execute('''SELECT status,reason,card_id,source
            FROM hl_testnet_cards_v1.delivery_receipts WHERE receipt_id=%s''',
            (identity,)).fetchone()
    report = dict(receipt_id=identity, status='RECEIPT_NOT_FOUND', order_requests_sent=0,
                  source_matches_card=False, stored_source_hash_matches=False)
    if row is None:
        return report
    report['status'] = row[0] if row[0] in ('RECORDED','REJECTED') else 'UNKNOWN_STATUS'
    report['reason'] = row[1] if isinstance(row[1],str) and re.fullmatch(r'[A-Z0-9_]{1,100}',row[1]) else None
    if row[3] is not None:
        report['stored_source_hash_matches'] = hashlib.sha256(wire.encoded(row[3])).hexdigest() == identity
    if row[0]=='RECORDED' and row[2] is not None:
        card = CardStore(journal).load(row[2])
        spec = wire.normalize(row[3])
        report.update(source_matches_card=card['prepared']['source']==spec['signal'],
                      state=card['state'], account_role=card['account_role'],
                      original_source_time=card['prepared']['source']['at'],
                      actual_execution_is_empty=card['actual_execution'] is None,
                      dispatch_enabled=card['dispatch_enabled'])
    return report
