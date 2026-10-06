"""One-record server review; no external reads or writes in tests."""
from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import Mock,patch
from . import card_receipt_review as r
from .postgres_journal import JournalError
from . import trade_cards
from alert_cards_forwarder_selftest import delivery
import hashlib
import alert_cards_wire as wire

class Journal:
    def __init__(self,row):
        self.conn=Mock();self.conn.execute.return_value.fetchone.return_value=row
    @contextmanager
    def _transaction(self):yield self.conn

class ReceiptReviewTests(TestCase):
    def test_invalid_id_is_blocked_before_database(self):
        with self.assertRaises(JournalError):r.review('not a receipt',object())
    def test_missing_receipt_is_not_success(self):
        j=Journal(None);out=r.review('a'*64,j)
        self.assertEqual(out['status'],'RECEIPT_NOT_FOUND')
        self.assertEqual(j.conn.execute.call_args.args[1],('a'*64,))
    def test_fixed_rejection_reason_is_visible_without_private_data(self):
        j=Journal(('REJECTED','ASSET_UNAVAILABLE',None,None))
        out=r.review('a'*64,j)
        self.assertEqual(out['reason'],'ASSET_UNAVAILABLE')
        self.assertEqual(out['order_requests_sent'],0)
    def test_arbitrary_exception_text_never_logged(self):
        out=r.review('a'*64,Journal(('REJECTED','postgresql://PRIVATE',None,None)))
        self.assertIsNone(out['reason'])
        self.assertNotIn('PRIVATE',str(out))
    def test_committed_record_original_source_and_empty_execution_verified(self):
        value=delivery();spec=wire.normalize(value)
        card=trade_cards.prepare_card(spec['signal'],{'universe':[{'name':'DEMO','szDecimals':2}]},
            rule_id=spec['rule_id'],threshold_pct=spec['threshold_pct'],record_kind='received_alert',source_stream=spec['source_stream'])
        identity=hashlib.sha256(wire.encoded(value)).hexdigest()
        with patch.object(r.CardStore,'load',return_value=card):
            result=r.review(identity,Journal(('RECORDED',None,card['card_id'],value)))
        self.assertTrue(result['stored_source_hash_matches'])
        self.assertTrue(result['source_matches_card'])
        self.assertTrue(result['actual_execution_is_empty'])
        self.assertFalse(result['dispatch_enabled'])
