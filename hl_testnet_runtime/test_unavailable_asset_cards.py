"""Valid DATA records without invented Testnet execution; no real network/orders."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import unittest
from unittest.mock import Mock, patch

import alert_cards_wire as wire
from alert_cards_forwarder_selftest import delivery
from . import unavailable_asset_cards as unprepared, trade_cards as cards
from . import alert_cards_intake as intake
from . import test_alert_cards_intake as fixtures
from .postgres_journal import PostgresJournal, JournalError

ABSENT = {'universe':[{'name':'OTHER','szDecimals':2}]}


class UnpreparedCardTests(unittest.TestCase):
    def test_exact_source_and_time_are_preserved(self):
        value=delivery(); original=deepcopy(value)
        card=unprepared.prepare(value)
        self.assertEqual(card['prepared']['source'],wire.normalize(value)['signal'])
        self.assertEqual(card['delivery'],value); self.assertEqual(value,original)
        self.assertFalse(card['prepared']['audit']['source_time_changed'])
    def test_no_prices_size_fill_or_pnl_are_invented(self):
        card=unprepared.prepare(delivery())
        self.assertIsNone(card['prepared']['execution'])
        self.assertIsNone(card['planning']['quantity'])
        self.assertIsNone(card['planning']['cancel_price'])
        self.assertIsNone(card['actual_execution'])
        self.assertFalse(card['dispatch_enabled'])
        self.assertFalse(card['prepared']['audit']['rounding_applied'])
        self.assertEqual(card['state'],'RECORDED_ASSET_UNAVAILABLE')
        self.assertEqual(card['risk']['planned_usd'],'10')
    def test_both_final_directions_keep_their_account(self):
        for side,role in [('LONG','long_account'),('SHORT','short_account')]:
            self.assertEqual(unprepared.prepare(delivery(side))['account_role'],role)
    def test_same_identity_as_a_prepared_card(self):
        value=delivery(); spec=wire.normalize(value)
        ready=cards.prepare_card(spec['signal'],fixtures.fixtures.META,
            rule_id=spec['rule_id'],threshold_pct=spec['threshold_pct'],
            record_kind='received_alert',source_stream=spec['source_stream'])
        self.assertEqual(unprepared.prepare(value)['card_id'],ready['card_id'])
    def test_new_alert_not_deduplicated_by_symbol_or_prices(self):
        self.assertNotEqual(unprepared.prepare(delivery(identity='a'))['card_id'],
                            unprepared.prepare(delivery(identity='b'))['card_id'])
    def test_tampered_cards_are_rejected(self):
        for field,value in [('dispatch_enabled',True),('environment','mainnet'),
                ('state','RECORDED_ONLY'),('actual_execution',{}),('account_role','short_account')]:
            card=unprepared.prepare(delivery());card[field]=value
            with self.assertRaises(cards.CardError):cards.validate_card(card)
    def test_guessed_precision_or_quantity_is_rejected(self):
        card=unprepared.prepare(delivery());card['planning']['quantity']='1'
        with self.assertRaises(cards.CardError):cards.validate_card(card)
        card=unprepared.prepare(delivery());card['prepared']['execution']=card['prepared']['source']
        with self.assertRaises(cards.CardError):cards.validate_card(card)
    def test_missing_fields_extra_secrets_and_bad_numbers_not_captured(self):
        for value in ({**delivery(),'secret':'DO_NOT_STORE'},
                      {**delivery(),'side':'BUY'}, {**delivery(),'text':'incomplete'}):
            with self.assertRaises(wire.WireError):unprepared.prepare(value)
    def test_existing_journal_projection_keeps_blocked_state_and_nulls(self):
        card=unprepared.prepare(delivery());out=cards.journal_projection(card)
        self.assertEqual(out['external_id'],card['card_id'])
        self.assertEqual(out['machine_fields']['status'],unprepared.STATE)
        self.assertIsNone(out['machine_fields']['rounded'])
        self.assertIsNone(out['machine_fields']['planned_quantity'])
        self.assertIsNone(out['machine_fields']['pnl'])
        self.assertFalse(out['delivery_enabled'])
        self.assertNotIn('notes',str(out));self.assertNotIn('conclusions',str(out))
    def test_validation_returns_independent_copy(self):
        card=unprepared.prepare(delivery());before=deepcopy(card)
        cards.validate_card(card)['delivery']['text']='changed'
        self.assertEqual(card,before)
    def test_no_io_or_account_key_is_needed(self):
        with patch('http.client.HTTPSConnection',side_effect=AssertionError('No network')):
            self.assertEqual(cards.validate_card(unprepared.prepare(delivery()))['state'],unprepared.STATE)
    def test_strict_order_builder_still_rejects_unprepared_card(self):
        import hyperliquid_testnet_executor as sender
        with self.assertRaises(sender.TestnetError):
            sender.build_action(unprepared.prepare(delivery())['prepared']['execution'],ABSENT,
                                '0x'+'1'*40,exit_type='tp_limit_sl_market')


@unittest.skipUnless(fixtures.fixtures.CI,'Requires disposable localhost PostgreSQL')
class UnavailableIntakePostgresTests(unittest.TestCase):
    setUp=fixtures.IntakePostgresTests.setUp
    counts=fixtures.IntakePostgresTests.counts
    def accept_absent(self,value=None):
        return intake.accept(wire.encoded(delivery() if value is None else value),self.receipts,
                             read_metadata=lambda:ABSENT)
    def old_rejection(self,value=None,reason='ASSET_UNAVAILABLE'):
        value=delivery() if value is None else value
        identity=hashlib.sha256(wire.encoded(value)).hexdigest()
        self.receipts.save(identity,'REJECTED',None,value,reason)
        return identity
    def audit_rows(self):
        with self.journal._transaction() as conn:
            return conn.execute('SELECT previous_status,previous_reason,source FROM hl_testnet_cards_v1.asset_receipt_resolutions').fetchall()
    def test_missing_asset_records_one_unprepared_card_without_orders(self):
        result=self.accept_absent();self.assertEqual(result['status'],'RECORDED')
        self.assertEqual(result['card_state'],unprepared.STATE);self.assertEqual(self.counts(),(1,1,0))
        card=self.store.load(self.receipts.get(result['receipt_id'])['card_id'])
        self.assertEqual(card,unprepared.prepare(delivery()))
    def test_supported_asset_path_is_unchanged(self):
        result=intake.accept(wire.encoded(delivery()),self.receipts,read_metadata=lambda:fixtures.fixtures.META)
        self.assertEqual(result['card_state'],'RECORDED_ONLY');self.assertEqual(self.counts(),(1,1,0))
    def test_delisted_asset_records_no_fake_precision(self):
        result=intake.accept(wire.encoded(delivery()),self.receipts,
            read_metadata=lambda:{'universe':[{'name':'DEMO','szDecimals':2,'isDelisted':True}]})
        self.assertEqual(result['card_state'],unprepared.STATE)
        self.assertIsNone(self.store.load(self.receipts.get(result['receipt_id'])['card_id'])['prepared']['execution'])
    def test_restart_replay_uses_existing_card_without_metadata(self):
        self.accept_absent()
        result=intake.accept(wire.encoded(delivery()),
            intake.ReceiptStore(PostgresJournal.for_ci(fixtures.fixtures.CI)),
            read_metadata=Mock(side_effect=AssertionError('No new preparation on replay')))
        self.assertEqual(result['status'],'DUPLICATE');self.assertEqual(self.counts(),(1,1,0))
    def test_concurrent_delivery_does_not_duplicate(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            results=list(pool.map(lambda _:self.accept_absent(),range(5)))
        self.assertEqual(sum(r['status']=='RECORDED' for r in results),1)
        self.assertEqual(self.counts(),(1,1,0))
    def test_old_asset_rejection_has_preserved_audit_and_original_received_time(self):
        identity=self.old_rejection()
        with self.journal._transaction() as conn:
            old_time=conn.execute('SELECT received_at FROM hl_testnet_cards_v1.delivery_receipts').fetchone()[0]
        self.assertEqual(self.accept_absent()['status'],'RECORDED')
        self.assertEqual(self.accept_absent()['status'],'DUPLICATE')
        self.assertEqual(self.audit_rows(),[('REJECTED','ASSET_UNAVAILABLE',delivery())])
        with self.journal._transaction() as conn:
            now_time=conn.execute('SELECT received_at FROM hl_testnet_cards_v1.delivery_receipts').fetchone()[0]
            audit_time=conn.execute('SELECT previous_received_at FROM hl_testnet_cards_v1.asset_receipt_resolutions').fetchone()[0]
        self.assertEqual(old_time,now_time);self.assertEqual(old_time,audit_time)
        self.assertEqual(self.receipts.get(identity)['status'],'RECORDED')
    def test_concurrent_recovery_has_one_resolution(self):
        self.old_rejection()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _:self.accept_absent(),range(4)))
        self.assertEqual(self.counts(),(1,1,0));self.assertEqual(len(self.audit_rows()),1)
    def test_lost_resolution_response_is_safe_to_retry(self):
        self.old_rejection();original=self.receipts.resolve_asset_rejection
        def lost(*args):
            original(*args)
            raise JournalError('SIMULATED_LOST_DATA_COMMIT_RESPONSE')
        with patch.object(self.receipts,'resolve_asset_rejection',side_effect=lost):
            with self.assertRaises(JournalError):self.accept_absent()
        self.assertEqual(self.accept_absent()['status'],'DUPLICATE')
        self.assertEqual(len(self.audit_rows()),1);self.assertEqual(self.counts(),(1,1,0))
    def test_failure_between_card_commit_and_resolution_recovers(self):
        self.old_rejection()
        with patch.object(self.receipts,'resolve_asset_rejection',side_effect=JournalError('SIMULATED_UNAVAILABLE')):
            with self.assertRaises(JournalError):self.accept_absent()
        self.assertEqual(self.counts(),(1,1,0))
        self.assertEqual(self.accept_absent()['status'],'DUPLICATE')
        self.assertEqual(len(self.audit_rows()),1)
    def test_other_rejection_is_never_reclassified(self):
        identity=self.old_rejection(reason='MESSAGE_DIRECTION_MISMATCH')
        self.assertEqual(self.accept_absent()['status'],'REJECTED')
        self.assertEqual(self.counts(),(0,1,0));self.assertEqual(self.audit_rows(),[])
        self.assertEqual(self.receipts.get(identity)['reason'],'MESSAGE_DIRECTION_MISMATCH')
    def test_changed_source_cannot_replace_unprepared_card(self):
        result=self.accept_absent();old=self.store.load(self.receipts.get(result['receipt_id'])['card_id'])
        value=delivery();value['text']=value['text'].replace('טייק פרופיט:</b> 102','טייק פרופיט:</b> 103')
        self.assertEqual(self.accept_absent(value)['status'],'REJECTED')
        self.assertEqual(self.store.load(old['card_id']),old)
    def test_missing_metadata_response_remains_retryable_not_fake_success(self):
        with self.assertRaises(RuntimeError):
            intake.accept(wire.encoded(delivery()),self.receipts,read_metadata=Mock(side_effect=RuntimeError('offline')))
        self.assertEqual(self.counts(),(0,0,0))
    def test_corrupted_source_prevents_receipt_recovery(self):
        identity=self.old_rejection();card=unprepared.prepare(delivery());self.store.record(card)
        changed=delivery();changed['rule_id']='OTHER'
        with self.assertRaises(JournalError):self.receipts.resolve_asset_rejection(identity,card['card_id'],changed)
        self.assertEqual(self.receipts.get(identity)['status'],'REJECTED');self.assertEqual(self.audit_rows(),[])
    def test_selected_receipt_readback_matches_source_and_no_execution(self):
        from .card_receipt_review import review
        result=self.accept_absent();audit=review(result['receipt_id'],self.journal)
        self.assertTrue(audit['source_matches_card']);self.assertTrue(audit['stored_source_hash_matches'])
        self.assertTrue(audit['actual_execution_is_empty']);self.assertFalse(audit['dispatch_enabled'])
        self.assertEqual(audit['state'],unprepared.STATE)


if __name__=='__main__':unittest.main(verbosity=2)
