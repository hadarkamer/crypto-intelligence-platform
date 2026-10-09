"""Real source contract through R2732 local admission; no external I/O."""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message
from . import r2732_entry as entry, card_lifecycle as life
from . import r2732_stop_dispatch as stops

T = 1791288960000
NOW = T+10000
ACCOUNT = '0x'+'2'*40
ROUTES = dict(short_account=dict(account=ACCOUNT), long_account=dict(account='0x'+'1'*40))
META = {'universe': [{'name': 'XRP', 'szDecimals': 1}]}


def message(at=T, **kwargs):
    return r2732_message(entry=2.3, decision_ms=at-60000, **kwargs)


def fixture(at=T):
    msg = message(at)
    state = entry.receive(entry.initial(ROUTES, not_before_ms=T-60000), msg, now_ms=at+10000)
    source = dict(environment='mainnet', symbol='XRP', reference_at_ms=at, at_ms=at+10000,
        history_complete=True, high='2.3', low='2.299', price='2.3', price_source=entry.condition.SOURCE)
    market = dict(environment='testnet', symbol='XRP', reference_at_ms=at, at_ms=at+10000,
        history_complete=True, high='2.3', low='2.299', price='2.3', account=ACCOUNT, price_kind='MARK')
    ownership = dict(environment='testnet', account=ACCOUNT, account_role='short_account',
        at_ms=at+10000, complete=True, unresolved_request=False, occurrences=[])
    return state, msg, market, source, ownership


def admit(values=None, *, now_ms=NOW, metadata=META):
    s, msg, m, src, owned = fixture() if values is None else values
    return entry.entry_admission(s, msg['occurrence_id'], metadata, m, src, owned, now_ms=now_ms)


def owned_row(cid='f'*64, *, family='r2732', symbol='XRP', phase='OPEN',
              remaining='40', working=True, request_id='e'*64):
    return dict(occurrence_id=cid, request_id=request_id, family=family, symbol=symbol,
                phase=phase, remaining_quantity=remaining, working_orders=working)


class R2732EntryTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'hyperliquid_testnet_executor._wallet', 'hyperliquid_testnet_executor._signed_body'):
            guard=patch(target, side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            guard.start(); self.addCleanup(guard.stop)

    def test_real_contract_rounding_unchanged_current_risk_and_record_only(self):
        s,msg,m,src,own=fixture(); p=admit((s,msg,m,src,own))
        self.assertEqual(p['source'],msg)
        self.assertEqual(p['execution'],stops.prepare_execution(msg,META))
        self.assertEqual((p['environment'],p['account_role'],p['account']),('testnet','short_account',ACCOUNT))
        self.assertLessEqual(life.number(p['planned_risk_usd']),entry.budget())
        self.assertFalse(p['dispatch_enabled']);self.assertEqual(p['order_requests_sent'],0)
        self.assertNotIn('action',p)
        self.assertIsNone(s['records'][msg['occurrence_id']]['request'])

    def test_begins_once_and_json_restart_never_retries(self):
        s,msg,m,src,own=fixture();p=admit((s,msg,m,src,own))
        after=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        self.assertEqual(after['revision'],s['revision']+1)
        self.assertEqual(after['records'][msg['occurrence_id']]['request']['phase'],'OUTCOME_UNKNOWN')
        restarted=json.loads(json.dumps(after))
        with self.assertRaisesRegex(entry.EntryError,'REVISION'):
            entry.begin_entry(restarted,p,META,m,src,own,now_ms=NOW)
        with self.assertRaisesRegex(entry.EntryError,'ALREADY_ATTEMPTED'):
            admit((restarted,msg,m,src,own))

    def test_source_retry_across_recipients_does_not_create_another_occurrence(self):
        s,msg,m,src,own=fixture()
        self.assertEqual(entry.receive(s,deepcopy(msg),now_ms=NOW),s)
        p=admit((s,msg,m,src,own));s=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        self.assertEqual(entry.receive(s,msg,now_ms=NOW),s)
        self.assertEqual(len(s['records']),1)

    def test_seen_closed_occurrence_remains_duplicate(self):
        values=fixture(); values[-1]['occurrences']=[owned_row(values[1]['occurrence_id'],
            phase='CLOSED',remaining='0',working=False)]
        with self.assertRaisesRegex(entry.EntryError,'ALREADY_KNOWN'):
            admit(values)

    def test_same_formula_cap_uses_demo_owned_positions(self):
        for phase in ('OPEN','PARTIALLY_OPEN','PARTIALLY_CLOSED'):
            values=fixture(); values[-1]['occurrences']=[owned_row(phase=phase)]
            with self.subTest(phase=phase),self.assertRaisesRegex(entry.EntryError,'CAP_ONE'):
                admit(values)

    def test_shared_xrp_market_remains_exclusive_even_other_formula(self):
        values=fixture();values[-1]['occurrences']=[owned_row(family='u21')]
        with self.assertRaisesRegex(entry.EntryError,'SHARED_MARKET'):
            admit(values)

    def test_other_coin_known_position_does_not_block_account(self):
        values=fixture();values[-1]['occurrences']=[owned_row(family='maxpain',symbol='DOGE')]
        self.assertIsNotNone(admit(values))

    def test_unknown_attempt_any_formula_blocks_new_work(self):
        for phase in ('PREPARED','OUTCOME_UNKNOWN','ACK_UNVERIFIED'):
            values=fixture(); values[-1]['occurrences']=[owned_row(phase=phase,symbol='DOGE',family='other')]
            with self.subTest(phase=phase),self.assertRaisesRegex(entry.EntryError,'PRIOR_ATTEMPT'):
                admit(values)

    def test_new_reference_cannot_erase_prior_unknown_attempt(self):
        s,msg,m,src,own=fixture();p=admit((s,msg,m,src,own))
        s=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        _,new,nm,ns,no=fixture(T+900000)
        s=entry.receive(s,new,now_ms=NOW+900000)
        with self.assertRaisesRegex(entry.EntryError,'PRIOR_ATTEMPT'):
            admit((s,new,nm,ns,no),now_ms=NOW+900000)
        no['occurrences']=[owned_row(msg['occurrence_id'],phase='CLOSED',remaining='0',
            working=False,request_id=p['proposal_id'])]
        self.assertIsNotNone(admit((s,new,nm,ns,no),now_ms=NOW+900000))
        self.assertEqual(len(s['records']),2)

    def test_wrong_prior_request_cannot_release_unknown_attempt(self):
        s,msg,m,src,own=fixture();p=admit((s,msg,m,src,own))
        s=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        _,new,nm,ns,no=fixture(T+900000);s=entry.receive(s,new,now_ms=NOW+900000)
        no['occurrences']=[owned_row(msg['occurrence_id'],phase='CLOSED',remaining='0',working=False)]
        with self.assertRaisesRegex(entry.EntryError,'PRIOR_ATTEMPT'):
            admit((s,new,nm,ns,no),now_ms=NOW+900000)

    def test_prior_attempt_cannot_relabel_symbol_or_formula_to_evade_cap(self):
        s,msg,m,src,own=fixture();p=admit((s,msg,m,src,own))
        s=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        _,new,nm,ns,no=fixture(T+900000);s=entry.receive(s,new,now_ms=NOW+900000)
        for symbol,family in (('BTC','other'),('XRP','other'),('BTC','r2732')):
            no['occurrences']=[owned_row(msg['occurrence_id'],symbol=symbol,family=family,
                                       request_id=p['proposal_id'])]
            with self.subTest(symbol=symbol,family=family),self.assertRaisesRegex(entry.EntryError,'PRIOR_ATTEMPT'):
                admit((s,new,nm,ns,no),now_ms=NOW+900000)

    def test_original_90_second_boundary_is_exclusive(self):
        values=fixture()
        with self.assertRaisesRegex(entry.EntryError,'WINDOW'):
            admit(values,now_ms=T+90000)

    def test_original_window_not_shortened_when_price_ranges_remain_fresh(self):
        values=fixture();future=T+45000
        values[2]['at_ms']=values[3]['at_ms']=values[4]['at_ms']=future
        self.assertIsNotNone(admit(values,now_ms=future))

    def test_source_staleness_is_independent_of_fresh_demo(self):
        values=fixture();future=T+45000
        values[2]['at_ms']=values[4]['at_ms']=future
        with self.assertRaisesRegex(entry.EntryError,'FRESH_REFERENCE_PRICE'):
            admit(values,now_ms=future)

    def test_newer_source_heartbeat_does_not_extend_original_expiry(self):
        s,msg,m,src,own=fixture(); late=T+80000
        hb=message(kind='HEARTBEAT',as_of_ms=late)
        s=entry.receive(s,hb,now_ms=late)
        for x in (m,src,own):x['at_ms']=late
        self.assertIsNotNone(admit((s,hb,m,src,own),now_ms=late))
        for x in (m,src,own):x['at_ms']=T+90000
        with self.assertRaisesRegex(entry.EntryError,'WINDOW'):
            admit((s,hb,m,src,own),now_ms=T+90000)

    def test_cancellation_and_cancel_first_tombstone_never_admit(self):
        for cancel_first in (False,True):
            s,msg,m,src,own=fixture();cancel=message(kind='CANCEL',cancel_reason='SOURCE_EXIT',as_of_ms=NOW)
            if cancel_first:s=entry.initial(ROUTES,not_before_ms=T-60000)
            s=entry.receive(s,cancel,now_ms=NOW)
            with self.assertRaisesRegex(entry.EntryError,'PERMISSION_RETIRED'):
                admit((s,msg,m,src,own))

    def test_bootstrap_cannot_replay_pre_activation_reference(self):
        s=entry.initial(ROUTES,not_before_ms=NOW)
        with self.assertRaises(ValueError):entry.receive(s,message(),now_ms=NOW)

    def test_old_reference_cannot_be_backfilled_after_new_one(self):
        s,msg,m,src,own=fixture(T+900000)
        with self.assertRaisesRegex(entry.EntryError,'OLDER_REFERENCE'):
            entry.receive(s,message(),now_ms=NOW+900000)

    def test_contract_rule_price_direction_and_execution_venue_are_validated(self):
        for mutate in (lambda v:v.update(side='LONG'),lambda v:v.update(stop='2.32'),
                       lambda v:v.update(execution_environment='mainnet')):
            msg=message();mutate(msg)
            with self.assertRaises(ValueError):entry.receive(entry.initial(ROUTES,not_before_ms=T-60000),msg,now_ms=NOW)

    def test_initial_stop_take_and_lock_visited_source_veto_even_after_recovery(self):
        for change,error in ((dict(high='2.32'),'SOURCE_INITIAL_EXIT'),
                              (dict(low='2.1'),'SOURCE_INITIAL_EXIT'),
                              (dict(low='2.27'),'SOURCE_LOCK')):
            values=fixture();values[3].update(change)
            with self.subTest(change=change),self.assertRaisesRegex(entry.EntryError,error):admit(values)

    def test_initial_stop_take_and_lock_visited_testnet_veto(self):
        for change,error in ((dict(high='2.32'),'TESTNET_INITIAL_EXIT'),
                              (dict(low='2.1'),'TESTNET_INITIAL_EXIT'),
                              (dict(low='2.27'),'TESTNET_LOCK')):
            values=fixture();values[2].update(change)
            with self.subTest(change=change),self.assertRaisesRegex(entry.EntryError,error):admit(values)

    def test_source_and_testnet_price_evidence_cannot_be_substituted(self):
        for index,change in ((2,dict(environment='mainnet')),(3,dict(environment='testnet')),
                            (3,dict(price_source='BINANCE_SPOT')),(2,dict(price_kind='TRADE')),
                            (2,dict(account='0x'+'1'*40)),(3,dict(reference_at_ms=T-60000))):
            values=fixture();values[index].update(change)
            with self.subTest(index=index,change=change),self.assertRaises(life.LifecycleError):admit(values)

    def test_incomplete_stale_future_range_or_ownership_blocks(self):
        for index in (2,3,4):
            for change in (dict(at_ms=NOW-15001),dict(at_ms=NOW+1),
                           {'complete' if index==4 else 'history_complete':False}):
                values=fixture();values[index].update(change)
                with self.subTest(index=index,change=change),self.assertRaises(life.LifecycleError):admit(values)

    def test_unavailable_delisted_duplicate_xrp_and_coarse_price_rejected(self):
        for meta in ({'universe':[{'name':'BTC','szDecimals':1}]},
                     {'universe':[{'name':'XRP','szDecimals':1,'isDelisted':True}]},
                     {'universe':[{'name':'XRP','szDecimals':1}]*2},
                     {'universe':[{'name':'XRP','szDecimals':6}]}):
            with self.subTest(meta=meta),self.assertRaises(ValueError):admit(metadata=meta)

    def test_begin_rechecks_source_demo_capacity_metadata_and_exact_size(self):
        for variant in ('source','demo','capacity','metadata','size'):
            values=fixture();s,msg,m,src,own=values;p=admit(values);meta=META
            if variant=='source':src['low']='2.27'
            if variant=='demo':m['high']='2.32'
            if variant=='capacity':own['occurrences']=[owned_row()]
            if variant=='metadata':meta={'universe':[{'name':'BTC','szDecimals':1},{'name':'XRP','szDecimals':1}]}
            if variant=='size':
                p['quantity']='9999';p['proposal_id']=life.digest({k:v for k,v in p.items() if k!='proposal_id'})
            with self.subTest(variant=variant),self.assertRaises(ValueError):
                entry.begin_entry(s,p,meta,m,src,own,now_ms=NOW)

    def test_revision_conflict_same_occurrence_different_proposals_cannot_both_begin(self):
        values=fixture();s,msg,m,src,own=values;p=admit(values);q=admit(values)
        first=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        with self.assertRaisesRegex(entry.EntryError,'REVISION'):
            entry.begin_entry(first,q,META,m,src,own,now_ms=NOW)

    def test_fresh_revalidation_can_change_observation_without_changing_terms(self):
        values=fixture();s,msg,m,src,own=values;p=admit(values)
        m['price']='2.299';m['at_ms']=src['at_ms']=own['at_ms']=NOW+1
        result=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW+1)
        self.assertEqual(result['records'][msg['occurrence_id']]['request']['proposal'],p)

    def test_inputs_remain_immutable(self):
        values=fixture();before=deepcopy(values);admit(values)
        self.assertEqual(values,before)

    def test_actual_contract_admission_partial_fill_then_conditional_management(self):
        s,msg,m,src,own=fixture();p=admit((s,msg,m,src,own))
        begun=entry.begin_entry(s,p,META,m,src,own,now_ms=NOW)
        self.assertIsNotNone(begun['records'][msg['occurrence_id']]['request'])
        prices={k:p['execution']['prepared']['execution'][k] for k in ('entry','stop','take_profit')}
        binding=dict(card_id=life.digest(['r2732',msg['occurrence_id']]),card_digest=life.digest(msg),
            account=ACCOUNT,role='short_account',symbol='XRP',side='SHORT',
            planned_quantity=p['quantity'],prices=prices,
            orders=dict(ENTRY=['10'],STOP=['11'],TAKE_PROFIT=['12']),environment='testnet')
        filled='40';pending=life.text(life.number(p['quantity'])-life.number(filled))
        opens=[]
        for leg,oid,q in (('ENTRY','10',pending),('STOP','11',filled),('TAKE_PROFIT','12',filled)):
            price=prices['entry' if leg=='ENTRY' else 'stop' if leg=='STOP' else 'take_profit']
            opens.append(dict(account=ACCOUNT,symbol='XRP',oid=oid,quantity=q,price=price,
                trigger_price=None if leg=='ENTRY' else price,side='A' if leg=='ENTRY' else 'B',
                reduce_only=leg!='ENTRY',state='ACTIVE',
                order_type='LIMIT' if leg=='ENTRY' else 'SL_MARKET' if leg=='STOP' else 'TP_LIMIT'))
        snapshot=dict(environment='testnet',account=ACCOUNT,symbol='XRP',at_ms=T+60000,
            history_complete=True,orders_complete=True,position_quantity='-40',open_orders=opens,
            terminal_orders=[],fills=[dict(account=ACCOUNT,symbol='XRP',oid='10',fill_id='actual-partial',
                quantity=filled,price=prices['entry'],fee='0.01',fee_token='USDC',side='A',at_ms=NOW+1000)])
        monitor=stops.initialize(msg,binding,snapshot,metadata=META,now_ms=T+60000)
        monitor=stops.advance(monitor,[dict(open_at_ms=T,open='2.3',high='2.3',low='2.275',close='2.28')],
                              now_ms=T+60000)
        reserved=stops.reserve(monitor,META,dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        amendment=reserved['requests'][-1]['proposal']
        self.assertEqual(amendment['quantity'],filled)
        self.assertEqual(stops.wire.requested_order(amendment['action'])['p'],p['execution']['locked_stop'])
        self.assertEqual(reserved['original_binding']['planned_quantity'],p['quantity'])
        self.assertEqual(reserved['original_binding']['prices'],prices)
        self.assertEqual(reserved['condition']['levels']['original_risk_distance'],
                         msg['policy']['original_risk_distance'])


if __name__=='__main__':unittest.main()
