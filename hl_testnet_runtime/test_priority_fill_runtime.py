"""Priority, durable notification barriers and actual reader admission wiring."""
from copy import deepcopy
from contextlib import redirect_stdout
from datetime import datetime,timezone
import io
import json
import threading
import unittest
from unittest.mock import Mock, patch, call

from . import card_lifecycle as life, checks, card_sync_evidence as evidence
from . import filled_quantity_dispatch as dispatch, long_stream_runtime as stream
from . import fill_wakeups as wake
from .test_filled_quantity_dispatch import NoExternal,state_from_case
from .test_card_lifecycle import A,B,T,binding,closed,opened,order,snapshot,terminal
from .test_fill_wakeups import Clock,fills


def ready(feed,account):
    generation=feed._opened(account)
    for channel in wake.CHANNELS:
        feed._receive(account,generation,json.dumps(dict(channel='subscriptionResponse',
            data=dict(method='subscribe',subscription=dict(type=channel,user=account)))))
    feed._receive(account,generation,json.dumps(fills(account,snapshot=True,rows=[])))


class PriorityTests(NoExternal):
    @staticmethod
    def historical_flat(side='LONG'):
        state=state_from_case(q='100',side=side,stop='100',take='100')
        state['evidence']['snapshot']=closed(state['bindings'][0])
        return state

    def test_current_fill_and_pending_request_precede_old_flat_history(self):
        flat={'symbol':'BTC','bindings':None,'pending':None,
              'evidence':{'snapshot':{'at_ms':T,'position_qty':'0','open_orders':[]}}}
        live=state_from_case(q='40',stop='40',take='40')
        uncovered=state_from_case(q='40',stop='20',take='20')
        pending=deepcopy(flat);pending['pending']='known-request'
        self.assertLess(stream._maintenance_priority(live),stream._maintenance_priority(flat))
        self.assertLessEqual(stream._maintenance_priority(uncovered),stream._maintenance_priority(live))
        self.assertLess(stream._maintenance_priority(pending),stream._maintenance_priority(flat))
        self.assertEqual(stream._maintenance_priority(flat,('BTC',)),2)

    def test_full_protection_fast_path_rejects_partial_work_and_uncertain_proofs(self):
        protected=state_from_case(q='100',stop='100',take='100')
        self.assertTrue(dispatch._fully_protected_no_work(protected,T))
        for state in (state_from_case(q='40',stop='40',take='40'),
                      state_from_case(q='100',stop='40',take='100'),
                      state_from_case(q='100',stop='100',take='40')):
            self.assertFalse(dispatch._fully_protected_no_work(state,T))
        for field in ('history_complete','orders_complete'):
            state=deepcopy(protected);state['evidence']['snapshot'][field]=False
            self.assertFalse(dispatch._fully_protected_no_work(state,T))
        self.assertFalse(dispatch._fully_protected_no_work(protected,T+15001))
        state=deepcopy(protected);state['pending']='uncertain'
        self.assertFalse(dispatch._fully_protected_no_work(state,T))

    def test_terminal_history_keeps_later_protected_position_quiet_but_orphans_do_not(self):
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        for side in ('LONG','SHORT'):
            for final in ('CLOSED','CANCELED_WITHOUT_FILL'):
                with self.subTest(side=side,terminal=final):
                    old,live=binding(side=side),binding(2,side=side,qty='60')
                    history=(closed(old) if final=='CLOSED' else snapshot(old,
                        terms=[terminal(old,leg,'0') for leg in life.LEGS]))
                    current=opened(live)
                    for key in ('fills','open_orders','terminal_orders'):
                        history[key]+=current[key]
                    history['position_quantity']=current['position_quantity']
                    state=dict(bucket=life.digest(['testnet',old['account'],'DOGE']),
                        account=old['account'],symbol='DOGE',revision=1,
                        bindings=[old,live],originals={},pending=None,
                        evidence=dict(bindings=[old,live],snapshot=history))
                    self.assertTrue(dispatch._fully_protected_no_work(state,T+1))
                    self.assertEqual(dispatch._collection_priority(state['evidence'],T+1,
                        fill_wakeups=Feed(),pending_clear=True),'protection')
                    controller=self.controller(state);controller.venue.now.return_value=T+1
                    result=stream.tick(controller,{'account':old['account']},
                        datetime.fromtimestamp(T/1000,timezone.utc),new_entries=False,
                        role=old['role'],notification_continuity=True)
                    self.assertEqual(result['maintenance_active'],1)
                    controller.cycle.assert_not_called()

                    # One old exit still working needs cleanup even though the
                    # other card has exact protection and the account is quiet.
                    orphan=deepcopy(state);snap=orphan['evidence']['snapshot']
                    oid=old['orders']['STOP'][0]
                    snap['terminal_orders']=[row for row in snap['terminal_orders'] if row['oid']!=oid]
                    snap['open_orders'].append(order(old,'STOP'))
                    self.assertFalse(dispatch._fully_protected_no_work(orphan,T+1))
                    self.assertEqual(dispatch._collection_priority(orphan['evidence'],T+1,
                        fill_wakeups=Feed(),pending_clear=True),'protection')
                    controller=self.controller(orphan);controller.venue.now.return_value=T+1
                    controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
                    stream.tick(controller,{'account':old['account']},
                        datetime.fromtimestamp(T/1000,timezone.utc),new_entries=False,
                        role=old['role'],notification_continuity=True)
                    controller.cycle.assert_called_once_with(state['bucket'],send=True,allow_new_entries=False)

                    # A correctly covered partial fill can grow again. Historical
                    # terminal rows must not hide its still-working entry.
                    partial=deepcopy(state);partial['bindings'][1]['planned_quantity']='100'
                    partial['evidence']['bindings']=deepcopy(partial['bindings'])
                    snap=partial['evidence']['snapshot'];entry=live['orders']['ENTRY'][0]
                    snap['terminal_orders']=[row for row in snap['terminal_orders'] if row['oid']!=entry]
                    snap['open_orders'].append(order(partial['bindings'][1],'ENTRY','40'))
                    self.assertFalse(dispatch._fully_protected_no_work(partial,T+1))

    def test_live_exposure_retains_protection_reserve_even_with_quiet_healthy_feed(self):
        class Feed:
            def dirty_symbols(self,account):return ()
            def entry_allowed(self,account):return True
        for side in ('LONG','SHORT'):
            protected=state_from_case(q='100',side=side,stop='100',take='100')
            self.assertTrue(dispatch._fully_protected_no_work(protected,T+1))
            for age in (1,9999,15001):
                with self.subTest(side=side,age=age):
                    self.assertEqual(dispatch._collection_priority(protected['evidence'],T+age,
                        fill_wakeups=Feed(),pending_clear=True),'protection')
            flat=self.historical_flat(side)
            self.assertEqual(dispatch._collection_priority(flat['evidence'],T+15001,
                fill_wakeups=Feed(),pending_clear=True),'background')
            # A leftover live exit requires cleanup even when exposure is zero.
            working=deepcopy(flat);snap=working['evidence']['snapshot']
            oid=working['bindings'][0]['orders']['STOP'][0]
            snap['terminal_orders']=[row for row in snap['terminal_orders'] if row['oid']!=oid]
            snap['open_orders'].append(order(working['bindings'][0],'STOP'))
            self.assertEqual(dispatch._collection_priority(working['evidence'],T+1,
                fill_wakeups=Feed(),pending_clear=True),'protection')

    def controller(self,state):
        controller=Mock()
        controller.store.for_account.return_value=[state]
        controller.venue.now.return_value=T+100
        controller.routes={'long_account':{'account':A},'short_account':{'account':B}}
        return controller

    def test_only_current_saved_complete_proof_and_inventory_release_gap(self):
        clock=Clock();feed=wake.FillWakeups({'long_account':A},clock=clock)
        ready(feed,A);token=feed.begin_reconciliation(A)
        state=state_from_case(q='100',stop='100',take='100')
        controller=self.controller(state)
        with patch.object(stream,'_account_owned',return_value=True) as owned:
            self.assertTrue(stream._finish_notification_reconciliation(controller,feed,token,None,T))
        owned.assert_called_once()
        self.assertEqual(owned.call_args.kwargs['priority'],'protection')
        self.assertTrue(feed.entry_allowed(A))

    def test_event_during_rest_retains_dirty_account_and_cannot_release_entry(self):
        clock=Clock();feed=wake.FillWakeups({'long_account':A},clock=clock)
        ready(feed,A);token=feed.begin_reconciliation(A)
        state=state_from_case(q='100',stop='100',take='100')
        feed._receive(A,token.generation,json.dumps(fills(A,rows=[dict(coin='BTC',tid=8,oid=9,time=T)])))
        with patch.object(stream,'_account_owned',return_value=True):
            self.assertFalse(stream._finish_notification_reconciliation(self.controller(state),feed,token,None,T))
        self.assertFalse(feed.entry_allowed(A))
        self.assertIn(A,feed.pending_accounts())

    def test_old_or_incomplete_checkpoint_and_unknown_inventory_cannot_clear(self):
        for variant in ('old','incomplete','uncovered','unknown-symbol','inventory'):
            clock=Clock();feed=wake.FillWakeups({'long_account':A},clock=clock)
            ready(feed,A);token=feed.begin_reconciliation(A)
            state=state_from_case(q='100',stop='100',take='100')
            symbols=None
            started=T
            if variant=='old':started=T+1
            elif variant=='incomplete':state['evidence']['snapshot']['orders_complete']=False
            elif variant=='uncovered':state=state_from_case(q='100',stop='40',take='100')
            elif variant=='unknown-symbol':symbols=('UNKNOWN',)
            failure=dispatch.DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')
            with patch.object(stream,'_account_owned',side_effect=failure if variant=='inventory' else None,
                              return_value=True):
                if variant=='inventory':
                    with self.assertRaises(dispatch.DispatchError):
                        stream._finish_notification_reconciliation(self.controller(state),feed,token,symbols,started)
                else:
                    self.assertFalse(stream._finish_notification_reconciliation(self.controller(state),feed,token,symbols,started))
            self.assertFalse(feed.entry_allowed(A))

    def test_old_terminal_gap_uses_fresh_inventory_without_replaying_history(self):
        # A normal restart can have many older final buckets. Their terminal
        # facts remain historical; a fresh account inventory proves flat now.
        for role,account,side in (('long_account',A,'LONG'),('short_account',B,'SHORT')):
            with self.subTest(role=role):
                state=self.historical_flat(side)
                before=deepcopy(state);controller=self.controller(state)
                now=T+2*60*60*1000
                controller.venue.now.return_value=now
                clock=Clock();feed=wake.FillWakeups({role:account},clock=clock)
                ready(feed,account);token=feed.begin_reconciliation(account)
                result=stream.tick(controller,{'account':account},
                    datetime.fromtimestamp(T/1000,timezone.utc),new_entries=False,
                    role=role,full_reconciliation=True)
                self.assertEqual(result['order_requests_sent'],0)
                controller.cycle.assert_not_called()
                reader=Mock();reader.read.side_effect=[[],dict(assetPositions=[])]
                with patch.object(evidence,'PublicReader',return_value=reader):
                    self.assertTrue(stream._finish_notification_reconciliation(
                        controller,feed,token,None,now))
                self.assertEqual(reader.read.call_args_list,
                    [call('frontendOpenOrders',account),
                     call('clearinghouseState',account)])
                self.assertTrue(feed.entry_allowed(account))
                self.assertEqual(state,before)

    def test_old_terminal_proof_cannot_hide_current_unknown_exposure(self):
        for current in ('position','order'):
            with self.subTest(current=current):
                state=self.historical_flat();controller=self.controller(state)
                now=T+2*60*60*1000;controller.venue.now.return_value=now
                clock=Clock();feed=wake.FillWakeups({'long_account':A},clock=clock)
                ready(feed,A);token=feed.begin_reconciliation(A)
                reader=Mock()
                orders=[dict(coin='DOGE',oid=999999)] if current=='order' else []
                positions=[dict(position=dict(coin='DOGE',szi='1'))] if current=='position' else []
                reader.read.side_effect=[orders,dict(assetPositions=positions)]
                with patch.object(evidence,'PublicReader',return_value=reader):
                    with self.assertRaisesRegex(dispatch.DispatchError,'UNOWNED_ACCOUNT_'):
                        stream._finish_notification_reconciliation(controller,feed,token,None,now)
                self.assertFalse(feed.entry_allowed(A))

    def test_explicit_dirty_terminal_symbol_requires_new_complete_checkpoint(self):
        state=self.historical_flat();controller=self.controller(state)
        now=T+2*60*60*1000;controller.venue.now.return_value=now
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        result=stream.tick(controller,{'account':A},datetime.fromtimestamp(T/1000,timezone.utc),
            new_entries=False,dirty_symbols=('DOGE',))
        self.assertEqual(result['order_requests_sent'],0)
        controller.cycle.assert_called_once_with(state['bucket'],send=True,allow_new_entries=False)
        clock=Clock();feed=wake.FillWakeups({'long_account':A},clock=clock)
        ready(feed,A);token=feed.begin_reconciliation(A)
        with patch.object(stream,'_account_owned',return_value=True) as inventory:
            self.assertFalse(stream._finish_notification_reconciliation(
                controller,feed,token,('DOGE',),now))
            inventory.assert_not_called()
            state['evidence']['snapshot']['at_ms']=now
            self.assertTrue(stream._finish_notification_reconciliation(
                controller,feed,token,('DOGE',),now))
        self.assertTrue(feed.entry_allowed(A))

    def test_retained_flat_requires_exact_ownership_completeness_and_no_unresolved_emergency(self):
        state=self.historical_flat()
        self.assertTrue(stream._immutable_flat_checkpoint(state))
        for invalid in ('bindings','history','orders','account','symbol','pending','emergency'):
            with self.subTest(invalid=invalid):
                candidate=deepcopy(state)
                if invalid=='bindings':candidate['evidence']['bindings']=[]
                elif invalid=='history':candidate['evidence']['snapshot']['history_complete']=False
                elif invalid=='orders':candidate['evidence']['snapshot']['orders_complete']=False
                elif invalid=='account':candidate['account']=B
                elif invalid=='symbol':candidate['symbol']='BTC'
                elif invalid=='pending':candidate['pending']='a'*64
                else:candidate['emergency']=dict(phase='ACTIVE',pending_close='a'*64,pending_cancel=None)
                self.assertFalse(stream._immutable_flat_checkpoint(candidate))

    def test_dirty_flat_never_precedes_uncovered_or_working_entry(self):
        flat=self.historical_flat()
        for risk in (state_from_case(q='40',stop='40',take='40'),
                     state_from_case(q='100',stop='40',take='100')):
            with self.subTest(risk=risk['evidence']['snapshot']['position_quantity']):
                self.assertLess(stream._maintenance_priority(risk,('DOGE',)),
                                stream._maintenance_priority(flat,('DOGE',)))

    def test_short_risk_account_precedes_long_startup_history(self):
        flat=self.historical_flat();risk=state_from_case(q='40',side='SHORT',stop='20',take='20')
        controller=self.controller(flat)
        controller.store.for_account.side_effect=lambda account:[flat] if account==A else [risk]
        controller.venue.env={'long_enabled':'false','short_enabled':'false'}
        controller.venue.sent=0
        class Feed:
            def pending_accounts(self):return (A,)
            def dirty_symbols(self,account):return None if account==A else ()
            def begin_reconciliation(self,account):return wake.ReconciliationToken(account,1,1,100)
            def entry_allowed(self,account):return account==B
            def health(self):
                return {role:dict(connected=True,snapshot_received=True,
                                  subscriptions_acknowledged=2)
                        for role in ('long_account','short_account')}
        controller.venue.fill_wakeups=Feed()
        stop=Mock();stop.is_set.side_effect=[False,True]
        routes=[('long_account',dict(account=A),None,'long_enabled'),
                ('short_account',dict(account=B),None,'short_enabled')]
        outcome=dict(status='ENTRIES_DISABLED',active_buckets=0,maintenance_active=0,
                     order_requests_sent=0,new_cards_registered=0)
        with patch.object(stream,'_stop',stop),patch.object(stream,'_wake',Mock()), \
                patch.object(stream,'tick',return_value=outcome) as tick, \
                patch.object(stream,'_finish_notification_reconciliation',return_value=True), \
                redirect_stdout(io.StringIO()):
            stream._loop(controller,routes)
        self.assertEqual([call.args[1]['account'] for call in tick.call_args_list],[B,A])

    def test_quiet_protected_rest_cadence_never_delays_new_or_uncertain_exposure(self):
        protected=state_from_case(q='100',stop='100',take='100')
        partial=state_from_case(q='40',stop='40',take='40')
        uncovered=state_from_case(q='100',stop='40',take='100')
        pending=deepcopy(protected);pending['pending']='a'*64
        variants=[
            ('quiet',protected,9999,True,(),False,0),
            ('fallback-deadline',protected,10000,True,(),False,1),
            ('missing-continuity',protected,1,False,(),False,1),
            ('new-notification',protected,1,True,('DOGE',),False,1),
            ('gap',protected,1,True,(),True,1),
            ('working-entry',partial,1,True,(),False,1),
            ('uncovered-stop',uncovered,1,True,(),False,1),
            ('unknown-request',pending,1,True,(),False,1)]
        for name,state,age,continuity,dirty,gap,calls in variants:
            with self.subTest(case=name):
                controller=self.controller(deepcopy(state))
                controller.venue.now.return_value=T+age
                controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
                result=stream.tick(controller,{'account':A},datetime.fromtimestamp(T/1000,timezone.utc),
                    new_entries=False,notification_continuity=continuity,
                    dirty_symbols=dirty,full_reconciliation=gap)
                self.assertEqual(result['order_requests_sent'],0)
                self.assertEqual(result['maintenance_active'],1)
                self.assertEqual(controller.cycle.call_count,calls)
                if calls:
                    controller.cycle.assert_called_once_with(state['bucket'],send=True,allow_new_entries=False)

    def test_priority_scan_failure_keeps_both_role_workers_retryable(self):
        controller=self.controller(self.historical_flat())
        controller.store.for_account.side_effect=dispatch.DispatchError('STORAGE_TEMPORARILY_UNAVAILABLE')
        controller.venue.env={'long_enabled':'false','short_enabled':'false'}
        controller.venue.sent=0
        class Feed:
            def pending_accounts(self):return ()
            def entry_allowed(self,account):return True
            def health(self):
                return {role:dict(connected=True,snapshot_received=True,
                                  subscriptions_acknowledged=2)
                        for role in ('long_account','short_account')}
        controller.venue.fill_wakeups=Feed()
        stop=Mock();stop.is_set.side_effect=[False,True]
        routes=[('long_account',dict(account=A),None,'long_enabled'),
                ('short_account',dict(account=B),None,'short_enabled')]
        outcome=dict(status='ENTRIES_DISABLED',active_buckets=0,maintenance_active=0,
                     order_requests_sent=0,new_cards_registered=0)
        with patch.object(stream,'_stop',stop),patch.object(stream,'_wake',Mock()), \
                patch.object(stream,'tick',return_value=outcome) as tick, \
                redirect_stdout(io.StringIO()):
            stream._loop(controller,routes)
        self.assertEqual([call.args[1]['account'] for call in tick.call_args_list],[A,B])

    def test_unbootstrapped_feed_keeps_protection_running_without_gap_replay(self):
        risk=state_from_case(q='40',stop='20',take='20')
        for disconnected,snapshot,acknowledged in ((True,True,2),(False,False,2),(False,True,1)):
            with self.subTest(disconnected=disconnected,snapshot=snapshot,acks=acknowledged):
                controller=self.controller(risk)
                controller.venue.env={'enabled':'false'};controller.venue.sent=0
                controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
                feed=Mock();feed.pending_accounts.return_value=(A,)
                feed.dirty_symbols.return_value=None;feed.entry_allowed.return_value=False
                feed.health.return_value={'long_account':dict(connected=not disconnected,
                    snapshot_received=snapshot,subscriptions_acknowledged=acknowledged)}
                controller.venue.fill_wakeups=feed
                stop=Mock();stop.is_set.side_effect=[False,True]
                with patch.object(stream,'_stop',stop),patch.object(stream,'_wake',Mock()), \
                        patch.object(stream,'tick',wraps=stream.tick) as tick, \
                        patch.object(stream,'_finish_notification_reconciliation') as finish, \
                        patch.object(stream,'observed_trades',return_value=[]), \
                        redirect_stdout(io.StringIO()):
                    stream._loop(controller,[('long_account',dict(account=A),
                        datetime.fromtimestamp(T/1000,timezone.utc),'enabled')])
                controller.cycle.assert_called_once_with(risk['bucket'],send=True,allow_new_entries=False)
                self.assertFalse(tick.call_args.kwargs.get('full_reconciliation',False))
                self.assertFalse(tick.call_args.kwargs['new_entries'])
                self.assertFalse(tick.call_args.kwargs['notification_continuity'])
                feed.begin_reconciliation.assert_not_called();finish.assert_not_called()


class ReaderAdmissionTests(NoExternal):
    def response(self,body):
        connection=Mock()
        connection.getresponse.return_value.status=200
        connection.getresponse.return_value.read.return_value=json.dumps(body).encode()
        return connection

    def test_both_reader_types_charge_before_http_and_use_explicit_protection_priority(self):
        for reader in (checks.InfoReader,evidence.PublicReader):
            budget=Mock();connection=self.response([])
            order=[]
            budget.acquire.side_effect=lambda *a,**k:(order.append('admit') or budget.permit)
            budget.permit.check.side_effect=lambda:order.append('permit')
            connection.request.side_effect=lambda *a,**k:order.append('http')
            with patch('http.client.HTTPSConnection',return_value=connection):
                result=reader(budget=budget,priority='protection').read('clearinghouseState',
                    **({'user':A} if reader is checks.InfoReader else {}),
                    **({} if reader is checks.InfoReader else {'account':A}))
            self.assertEqual(result,[])
            self.assertEqual(order,['admit','permit','http'])
            self.assertEqual(budget.acquire.call_args.kwargs['priority'],'protection')
            budget.permit.finish.assert_called_once_with([])

    def test_budget_denial_and_expired_permit_send_no_http(self):
        for reader in (checks.InfoReader,evidence.PublicReader):
            for stage in ('acquire','check'):
                from .request_budget import BudgetError
                budget=Mock();connection=self.response([])
                if stage=='acquire':budget.acquire.side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
                else:budget.acquire.return_value.check.side_effect=BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')
                with patch('http.client.HTTPSConnection',return_value=connection):
                    with self.assertRaises(BudgetError):
                        instance=reader(budget=budget,priority='protection')
                        if reader is checks.InfoReader:instance.read('clearinghouseState',user=A)
                        else:instance.read('clearinghouseState',A)
                connection.request.assert_not_called()

    def test_parallel_observation_workers_share_one_budget_and_priority(self):
        budget=Mock();connection=self.response([])
        with patch('http.client.HTTPSConnection',return_value=connection), \
             patch.object(evidence,'history',side_effect=lambda reader,account,start,end:
                          reader.read('userFillsByTime',account,start=start,end=end)):
            reader=evidence.PublicReader(parallel=True,budget=budget,priority='protection')
            reader.observation_inputs(A,['1','2'],T,T+1)
        self.assertEqual(budget.acquire.call_count,5)
        self.assertTrue(all(call.kwargs['priority']=='protection' for call in budget.acquire.call_args_list))


if __name__=='__main__':unittest.main()
