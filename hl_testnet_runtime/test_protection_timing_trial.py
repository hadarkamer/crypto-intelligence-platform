"""Trial orchestration regressions; no exchange, signer or service credentials."""
from contextlib import ExitStack
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from . import protection_timing_trial as trial, emergency_close as emergency
from .test_filled_quantity_dispatch import NoExternal,state_from_case,ROUTES2
from .test_long_stream_runtime import env
from .test_card_lifecycle import T,terminal,fill
from .filled_dispatch_store import DispatchError
from .request_budget import BudgetError
from . import filled_quantity_dispatch as dispatch


class TrialTests(NoExternal):
    def setUp(self):
        super().setUp()
        self.state=state_from_case(q='0',expiry_seconds=300)
        self.cid=self.state['bindings'][0]['card_id']
        self.card=self.state['originals'][self.cid]['card']

    def protected(self,timing=True):
        state=state_from_case(q='40',stop='40',take='40',expiry_seconds=300)
        state['entry_timing_armed']={self.cid:T}
        state['protection_timing']={self.cid:dict(stop_observed_at_ms=T+100000,
            fill_to_stop_public_ms=500 if timing else None)}
        return state

    def test_protection_success_does_not_invent_venue_timing(self):
        for timing,status in [(True,'PROTECTED_TIMING_COMPLETE'),
                              (False,'PROTECTED_VENUE_TIME_UNKNOWN')]:
            state=self.protected(timing)
            observation=trial.outcome(state,self.cid,T)
            self.assertTrue(observation['protection_verified'])
            self.assertFalse(observation['terminal_verified'])
            self.assertEqual(trial.measured_status(state,self.cid,observation),status)
            self.assertEqual(observation['entry_quantity'],'40')
            self.assertEqual(observation['working_entry_order_ids'],['10'])

    def test_stale_or_pending_protection_is_not_success(self):
        state=self.protected()
        self.assertIsNone(trial.measured_status(state,self.cid,
            trial.outcome(state,self.cid,T+15001)))
        state['pending']='a'*64
        self.assertIsNone(trial.measured_status(state,self.cid,
            trial.outcome(state,self.cid,T)))

    def test_durable_timing_is_read_upper_bound_and_does_not_rewrite_old_record(self):
        state=self.protected();item=state['protection_timing'][self.cid]
        item['first_fill_at_ms']=T
        before=deepcopy(item)
        measured=trial.durable_timing(state,self.cid,T+100500)
        self.assertEqual(measured['fill_to_durable_record_read_upper_bound_ms'],100500)
        self.assertEqual(measured['stop_record_read_at_ms'],T+100500)
        self.assertEqual(item,before)
        self.assertNotIn('fill_to_durable_record_read_upper_bound_ms',
            trial.durable_timing(state,self.cid,T+500))

    def canceled(self):
        state=deepcopy(self.state);b=state['bindings'][0]
        state['evidence']['snapshot']['open_orders']=[]
        state['evidence']['snapshot']['terminal_orders']=[terminal(b,'ENTRY','0')]
        state['entry_timing_armed']={self.cid:T}
        return state

    def test_only_verified_terminal_cancellation_finishes_without_fill(self):
        self.assertIsNone(trial.measured_status(self.state,self.cid,
            trial.outcome(self.state,self.cid,T)))
        state=self.canceled();view=trial.outcome(state,self.cid,T)
        self.assertTrue(view['terminal_verified'])
        self.assertEqual(trial.measured_status(state,self.cid,view),'CANCELED_WITHOUT_FILL')

    def test_partial_take_then_observed_emergency_close_is_verified_terminal(self):
        state=self.protected();b=state['bindings'][0];snap=state['evidence']['snapshot']
        snap['open_orders']=[];snap['position_quantity']='0'
        snap['fills']=[fill(b,qty='40'),fill(b,'TAKE_PROFIT',qty='10'),
                       fill(b,'STOP',qty='30')]
        snap['terminal_orders']=[terminal(b,'ENTRY','40'),
            terminal(b,'TAKE_PROFIT','10'),terminal(b,'STOP','30')]
        state['emergency']=dict(phase='CLOSED_VERIFIED',requests=[dict(
            phase='OBSERVED',observed_oid=b['orders']['STOP'][0],
            proposal=dict(operation='EMERGENCY_CLOSE'))])
        view=trial.outcome(state,self.cid,T)
        self.assertTrue(view['closure_verified'])
        self.assertTrue(view['terminal_verified'])
        self.assertEqual(trial.measured_status(state,self.cid,view),'CLOSED')

    def run_script(self,script,*,shutdown=True,notifications=None,setup_failure=False,
                   hint_during_read=False,cycle_failure=None):
        state=deepcopy(self.state)
        state['bindings']=[];state['evidence']['bindings']=[]
        state['evidence']['snapshot']['open_orders']=[]
        shared=dict(state=state,now=T,calls=[],shutdown=None)
        class Store:
            journal=object()
            def load(self,bucket):return deepcopy(shared['state'])
            def for_account(self,account):return [self.load(None)]
            def request(self,rid):return deepcopy(shared['state']['request'])
        class Venue:
            sent=0
            def now(self):return shared['now']
        class Base:
            store=Store();venue=Venue();routes=ROUTES2
            def refresh(self,bucket):return self.store.load(bucket)
        class Controlled(Base):
            def register(self,cid):
                if setup_failure:
                    raise DispatchError('TEST_SETUP_FAILURE')
                return self.store.load(None)
            def cycle(self,bucket,**options):
                if notifications is not None and 'allowed_entry_card_id' in options:
                    if not notifications.entry_allowed(ROUTES2[self.card_role]['account']):
                        raise AssertionError('No ENTRY before authoritative gap reconciliation')
                shared['calls'].append((shared['now'],options))
                if cycle_failure is not None:
                    raise cycle_failure
                if notifications is not None and hint_during_read:
                    notifications.allowed=False
                    notifications.wake_event.set()
                new=script(len(shared['calls']),shared['now'])
                if new is not None:
                    shared['state']=deepcopy(new)
                shared['state']['evidence']['snapshot']['at_ms']=shared['now']
                return dict(status='WORKING')
        Controlled.card_role=self.card['account_role']
        class ClockEvent:
            def wait(self,seconds):shared['now']+=100000
            def set(self):shared['shutdown']=True
        event=ClockEvent()
        if notifications is not None:
            wake_flag=[False]
            notifications.wake_event.clear.side_effect=lambda:wake_flag.__setitem__(0,False)
            notifications.wake_event.set.side_effect=lambda:wake_flag.__setitem__(0,True)
            def wake_wait(seconds):
                if wake_flag[0]:
                    return True
                shared['now']+=int(seconds*1000)
                return False
            notifications.wake_event.wait.side_effect=wake_wait
        environment={**env(),'HL_TESTNET_EMERGENCY_CLOSE':emergency.APPROVAL,
                     'HL_TESTNET_SHORT_ENTRY_ENABLED':'false'}
        def stop(stopped,timeout):
            stopped.set()
            return shutdown
        with ExitStack() as stack:
            stack.enter_context(patch.object(trial.dispatch,'controller_from_env',
                side_effect=[Base(),Controlled()]))
            stack.enter_context(patch.object(trial,'CardStore')).return_value.load.return_value=self.card
            stack.enter_context(patch.object(trial.threading,'Event',return_value=event))
            stack.enter_context(patch.object(trial.time,'monotonic',
                side_effect=lambda:(shared['now']-T)/1000))
            stack.enter_context(patch.object(emergency,'start',return_value=True))
            stack.enter_context(patch.object(emergency,'healthy',return_value=True))
            stack.enter_context(patch.object(emergency,'stop_supervisor',side_effect=stop,create=True))
            if notifications is not None:
                stack.enter_context(patch.object(trial.dispatch,'TestnetVenue',Venue))
                from . import fill_wakeups
                factory=stack.enter_context(patch.object(fill_wakeups,'FillWakeups',return_value=notifications))
                stack.enter_context(patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')))
                stack.enter_context(patch.object(trial.stream,'_finish_notification_reconciliation',
                    side_effect=lambda base,feed,token,symbols,started:feed.finish_reconciliation(token,complete=True)))
            result=trial.run_one(environment,self.cid)
            if notifications is not None:
                factory.assert_called_once_with({self.card['account_role']:ROUTES2[self.card['account_role']]['account']})
        self.assertTrue(shared['shutdown'])
        return result,shared['calls']

    def notification_feed(self, *, ready=True, reconcile=True):
        feed=Mock()
        feed.allowed=False
        feed.health.return_value={self.card['account_role']:dict(connected=ready,
            snapshot_received=ready,subscriptions_acknowledged=2 if ready else 0)}
        feed.entry_allowed.side_effect=lambda account:feed.allowed
        feed.dirty_symbols.return_value=None
        feed.begin_reconciliation.side_effect=lambda account:SimpleNamespace(
            account=account,generation=1,revision=1)
        def finish(token, *, complete):
            feed.allowed=complete and reconcile
            return feed.allowed
        feed.finish_reconciliation.side_effect=finish
        return feed

    def test_real_venue_trial_shares_one_feed_and_reconciles_before_entry_then_stops(self):
        feed=self.notification_feed()
        result,calls=self.run_script(lambda call,now:self.protected(),notifications=feed)
        self.assertEqual(result['status'],'PROTECTED_TIMING_COMPLETE')
        self.assertEqual(calls[0][1]['allowed_entry_card_id'],self.cid)
        feed.start.assert_called_once_with()
        feed.finish_reconciliation.assert_called_once()
        feed.stop.assert_called_once_with()
        self.assertEqual(result['entry_authorization_expires_at_ms'],T+90000)

    def test_notification_bootstrap_timeout_prevents_entry_and_cleans_up_feed(self):
        for feed in (self.notification_feed(ready=False),self.notification_feed(reconcile=False)):
            with self.assertRaisesRegex(DispatchError,'TIMING_TRIAL_FILL_NOTIFICATION_GAP_NO_NEW_ENTRY'):
                self.run_script(lambda call,now:self.fail('ENTRY must not run'),notifications=feed)
            feed.start.assert_called_once_with()
            feed.stop.assert_called_once_with()

    def test_trial_setup_failure_after_feed_start_stops_feed(self):
        feed=self.notification_feed()
        with self.assertRaisesRegex(DispatchError,'TEST_SETUP_FAILURE'):
            self.run_script(lambda call,now:self.fail('No trial cycle on setup failure'),
                            notifications=feed,setup_failure=True)
        feed.start.assert_called_once_with()
        feed.stop.assert_called_once_with()
        feed.finish_reconciliation.assert_called_once()

    def test_notification_reconciliation_requires_full_authoritative_sweep_not_snapshot_alone(self):
        feed=self.notification_feed()
        base=Mock();base.venue.now.return_value=T
        role=self.card['account_role'];route=ROUTES2[role]
        with patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')) as tick, \
             patch.object(trial.stream,'_finish_notification_reconciliation',return_value=False) as finish:
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card))

        self.assertFalse(feed.entry_allowed(route['account']))
        self.assertFalse(tick.call_args.kwargs['new_entries'])
        self.assertTrue(tick.call_args.kwargs['full_reconciliation'])
        self.assertEqual(finish.call_args.args[-2:],(None,T))

    def test_new_notification_before_entry_requires_another_saved_reconciliation(self):
        feed=self.notification_feed()
        role=self.card['account_role'];route=ROUTES2[role]
        base=Mock();base.venue.now.return_value=T
        with patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')) as tick, \
             patch.object(trial.stream,'_finish_notification_reconciliation',
                          side_effect=lambda *args:feed.finish_reconciliation(args[2],complete=True)):
            self.assertTrue(trial._reconcile_notifications(base,feed,role,route,self.card))
            feed.allowed=False
            feed.dirty_symbols.return_value=('SOL',)
            self.assertTrue(trial._reconcile_notifications(base,feed,role,route,self.card))
        self.assertEqual(tick.call_count,2)
        self.assertFalse(tick.call_args.kwargs['new_entries'])
        self.assertFalse(tick.call_args.kwargs['full_reconciliation'])
        self.assertEqual(tick.call_args.kwargs['dirty_symbols'],('SOL',))

    def bootstrap_retry(self):
        from . import fill_wakeups
        from .test_priority_fill_runtime import ready
        from .test_fill_wakeups import Clock
        role=self.card['account_role'];route=ROUTES2[role]
        clock=Clock();feed=fill_wakeups.FillWakeups({role:route['account']},clock=clock)
        ready(feed,route['account'])
        state=state_from_case(q='100',stop='100',take='100')
        base=Mock();base.routes=ROUTES2
        base.store.for_account.return_value=[state]
        base.venue.now.return_value=T+100
        reconciliation={}
        with patch.object(trial.stream,'tick',
                side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')):
            with self.assertRaises(BudgetError):
                trial._reconcile_notifications(base,feed,role,route,self.card,
                    reconciliation=reconciliation)
        return base,feed,role,route,state,reconciliation

    def test_budget_bootstrap_retry_reuses_new_durable_proof_with_fresh_inventory(self):
        base,feed,role,route,state,reconciliation=self.bootstrap_retry()
        state['evidence']['snapshot']['at_ms']=T+500
        base.venue.now.return_value=T+1000
        with patch.object(trial.stream,'tick') as tick, \
             patch.object(trial.stream,'_account_owned',return_value=True) as owned:
            self.assertTrue(trial._reconcile_notifications(base,feed,role,route,self.card,
                reconciliation=reconciliation))
        tick.assert_not_called()
        owned.assert_called_once_with(base.venue,route['account'],[state],
            role=role,priority='protection')
        self.assertTrue(feed.entry_allowed(route['account']))
        self.assertEqual(reconciliation,{})

    def test_bootstrap_reuse_cannot_accept_old_incomplete_changed_or_new_boundary_proof(self):
        from .test_fill_wakeups import fills
        from .test_priority_fill_runtime import ready
        for variant in ('before-boundary','stale','bindings','history','orders','hint','generation'):
            with self.subTest(variant=variant):
                base,feed,role,route,state,reconciliation=self.bootstrap_retry()
                snapshot=state['evidence']['snapshot'];snapshot['at_ms']=T+500
                base.venue.now.return_value=T+1000
                if variant=='before-boundary':snapshot['at_ms']=T+99
                elif variant=='stale':base.venue.now.return_value=T+16001
                elif variant=='bindings':state['evidence']['bindings']=[]
                elif variant=='history':snapshot['history_complete']=False
                elif variant=='orders':snapshot['orders_complete']=False
                elif variant=='hint':
                    feed._receive(route['account'],reconciliation['boundary'][0].generation,
                        json.dumps(fills(route['account'],rows=[dict(
                            coin=state['symbol'],tid=99,oid=999,time=T+600)])))
                elif variant=='generation':ready(feed,route['account'])
                with patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')) as tick, \
                     patch.object(trial.stream,'_account_owned') as owned:
                    self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                        reconciliation=reconciliation))
                tick.assert_called_once()
                owned.assert_not_called()
                self.assertFalse(feed.entry_allowed(route['account']))

    def test_reused_bootstrap_proof_still_fails_closed_on_unknown_account_inventory(self):
        base,feed,role,route,state,reconciliation=self.bootstrap_retry()
        state['evidence']['snapshot']['at_ms']=T+500
        base.venue.now.return_value=T+1000
        with patch.object(trial.stream,'tick') as tick, \
             patch.object(trial.stream,'_account_owned',
                 side_effect=DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')):
            with self.assertRaisesRegex(DispatchError,'UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY'):
                trial._reconcile_notifications(base,feed,role,route,self.card,
                    reconciliation=reconciliation)
        tick.assert_not_called()
        self.assertFalse(feed.entry_allowed(route['account']))

    def test_new_hint_during_reused_inventory_does_not_clear_notification_gap(self):
        from .test_fill_wakeups import fills
        base,feed,role,route,state,reconciliation=self.bootstrap_retry()
        state['evidence']['snapshot']['at_ms']=T+500
        base.venue.now.return_value=T+1000
        def hint_during_inventory(*args,**kwargs):
            feed._receive(route['account'],reconciliation['boundary'][0].generation,
                json.dumps(fills(route['account'],rows=[dict(
                    coin=state['symbol'],tid=100,oid=999,time=T+1000)])))
            return True
        with patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')), \
             patch.object(trial.stream,'_account_owned',side_effect=hint_during_inventory):
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                reconciliation=reconciliation))
        self.assertFalse(feed.entry_allowed(route['account']))
        self.assertEqual(feed.dirty_symbols(route['account']),None)

    def shared_bootstrap(self):
        from . import fill_wakeups
        from .test_priority_fill_runtime import ready
        from .test_fill_wakeups import Clock
        role=self.card['account_role'];route=ROUTES2[role]
        clock=Clock();feed=fill_wakeups.FillWakeups({role:route['account']},clock=clock)
        ready(feed,route['account'])
        state=state_from_case(q='100',stop='100',take='100')
        base=Mock();base.routes=ROUTES2
        base.venue=dispatch.TestnetVenue(env())
        base.venue.now=Mock(return_value=T+100)
        base.store.for_account.return_value=[state]
        return base,feed,role,route,state,clock

    def test_initial_bootstrap_wait_shares_deployed_post_token_proof_without_private_reads(self):
        base,feed,role,route,state,clock=self.shared_bootstrap()
        timing={'seconds':0.0};stopped=Mock()
        def wait(seconds):
            timing['seconds']+=seconds;clock.now+=seconds
            base.venue.now.return_value=T+100+int(timing['seconds']*1000)
            if timing['seconds']>=6:
                state['revision']+=1
                state['evidence']['snapshot']['at_ms']=base.venue.now()
        stopped.wait.side_effect=wait
        with patch.object(trial.time,'monotonic',side_effect=lambda:timing['seconds']), \
             patch.object(trial.stream,'tick') as tick, \
             patch.object(trial.stream,'_account_owned',return_value=True) as owned:
            trial._wait_notifications(base,feed,role,route,self.card,T+90100,stopped)
        tick.assert_not_called();owned.assert_called_once()
        self.assertTrue(feed.entry_allowed(route['account']))
        self.assertGreaterEqual(timing['seconds'],6)
        self.assertLess(timing['seconds'],10)

    def test_shared_bootstrap_original_ten_second_wait_falls_back_without_new_grant(self):
        base,feed,role,route,state,clock=self.shared_bootstrap()
        timing={'seconds':0.0};stopped=Mock()
        def wait(seconds):
            timing['seconds']+=seconds;clock.now+=seconds
            base.venue.now.return_value=T+100+int(timing['seconds']*1000)
        stopped.wait.side_effect=wait
        def private_read(*args,**kwargs):
            self.assertGreaterEqual(timing['seconds'],10)
            state['evidence']['snapshot']['at_ms']=base.venue.now()
            return dict(status='ENTRIES_DISABLED')
        with patch.object(trial.time,'monotonic',side_effect=lambda:timing['seconds']), \
             patch.object(trial.stream,'tick',side_effect=private_read) as tick, \
             patch.object(trial.stream,'_account_owned',return_value=True):
            trial._wait_notifications(base,feed,role,route,self.card,T+90100,stopped)
        tick.assert_called_once();self.assertLess(timing['seconds'],10.2)
        self.assertTrue(feed.entry_allowed(route['account']))

    def test_new_hint_while_shared_bootstrap_waits_requires_direct_reconciliation(self):
        from .test_fill_wakeups import fills
        base,feed,role,route,state,clock=self.shared_bootstrap()
        reconciliation={'shared_wait_deadline':10.0}
        with patch.object(trial.time,'monotonic',return_value=0), \
             patch.object(trial.stream,'tick') as tick:
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                reconciliation=reconciliation))
        tick.assert_not_called();original=reconciliation['boundary']
        feed._receive(route['account'],original[0].generation,json.dumps(fills(
            route['account'],rows=[dict(coin=state['symbol'],tid=303,oid=888,time=T+200)])))
        # The initial full snapshot leaves dirty_all set. A changed token may
        # still wait without ENTRY authority, but its old proof cannot clear it.
        base.venue.now.return_value=T+300
        state['evidence']['snapshot']['at_ms']=T+200
        with patch.object(trial.time,'monotonic',return_value=1), \
             patch.object(trial.stream,'tick') as tick, \
             patch.object(trial.stream,'_account_owned') as owned:
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                reconciliation=reconciliation))
        tick.assert_not_called();owned.assert_not_called()
        self.assertNotEqual(reconciliation['boundary'][0].revision,original[0].revision)
        self.assertEqual(reconciliation['shared_wait_deadline'],10.0)
        self.assertFalse(feed.entry_allowed(route['account']))

    def test_shared_bootstrap_state_change_during_inventory_does_not_release_or_recollect(self):
        base,feed,role,route,state,reconciliation=self.bootstrap_retry()
        state['evidence']['snapshot']['at_ms']=T+500
        base.venue.now.return_value=T+1000
        def changed(*args,**kwargs):
            state['revision']+=1
            return True
        with patch.object(trial.stream,'_account_owned',side_effect=changed) as owned, \
             patch.object(trial.stream,'tick') as tick:
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                reconciliation=reconciliation))
        owned.assert_called_once();tick.assert_not_called()
        self.assertFalse(feed.entry_allowed(route['account']))

    def test_shared_bootstrap_unprotected_or_unresolved_work_is_never_deferred(self):
        for variant in ('pending','missing-stop','missing-take','working-entry'):
            with self.subTest(variant=variant):
                base,feed,role,route,state,clock=self.shared_bootstrap()
                if variant=='pending':state['pending']='b'*64
                elif variant=='missing-stop':state.update(state_from_case(q='100',stop='40',take='100'))
                elif variant=='missing-take':state.update(state_from_case(q='100',stop='100',take='40'))
                elif variant=='working-entry':state.update(state_from_case(q='40',stop='40',take='40'))
                with patch.object(trial.time,'monotonic',return_value=0), \
                     patch.object(trial.stream,'tick',return_value=dict(status='ENTRIES_DISABLED')) as tick, \
                     patch.object(trial.stream,'_account_owned') as owned:
                    self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card,
                        reconciliation={'shared_wait_deadline':10.0}))
                tick.assert_called_once();owned.assert_not_called()

    def pause(self,state=None,**changes):
        state=deepcopy(self.state) if state is None else state
        feed=self.notification_feed();feed.allowed=True
        arguments=dict(feed=feed,account=ROUTES2[self.card['account_role']]['account'],
            now_ms=T,expiry_ms=T+90000,source_expiry_ms=T+300000,
            observation_deadline_ms=T+315000,wall_remaining=315)
        arguments.update(changes)
        return trial._trial_pause(state,trial.outcome(state,self.cid,T),**arguments)

    def test_quiet_entry_pause_requires_gap_clear_current_zero_exposure_proof(self):
        self.assertEqual(self.pause(),5)
        feed=self.notification_feed()
        self.assertEqual(self.pause(feed=feed),.25)
        self.assertEqual(self.pause(self.protected()),.25)
        pending=deepcopy(self.state);pending['pending']='f'*64
        self.assertEqual(self.pause(pending),.25)
        emergency_state=deepcopy(self.state)
        observation=trial.outcome(emergency_state,self.cid,T)
        emergency_state['emergency']={'phase':'LATCHED'}
        feed.allowed=True
        self.assertEqual(trial._trial_pause(emergency_state,observation,feed=feed,
            account=ROUTES2[self.card['account_role']]['account'],now_ms=T,
            expiry_ms=T+90000,source_expiry_ms=T+300000,
            observation_deadline_ms=T+315000,wall_remaining=315),.25)
        stale=deepcopy(self.state);stale['evidence']['snapshot']['at_ms']=T-5001
        self.assertEqual(self.pause(stale),.25)
        incomplete=deepcopy(self.state);incomplete['evidence']['snapshot']['orders_complete']=False
        self.assertEqual(self.pause(incomplete),.25)

    def test_sends_continue_immediately_and_pauses_respect_original_deadlines(self):
        self.assertEqual(self.pause(cycle_result=dict(order_requests_sent=1)),0)
        self.assertEqual(self.pause(expiry_ms=T+100),.1)
        self.assertEqual(self.pause(source_expiry_ms=T+150),.15)
        self.assertEqual(self.pause(wall_remaining=.07),.07)
        self.assertEqual(self.pause(observation_deadline_ms=T+20),.02)
        self.assertEqual(self.pause(expiry_ms=T-1),5)
        self.assertEqual(self.pause(feed=None),.25)

    def test_hint_during_reconciliation_is_not_cleared_before_wait(self):
        feed=self.notification_feed()
        waiting=deepcopy(self.state);waiting['entry_timing_armed']={self.cid:T}
        result,calls=self.run_script(lambda call,now:waiting if call==1 else self.protected(),
                                   notifications=feed,hint_during_read=True)
        self.assertEqual(result['status'],'PROTECTED_TIMING_COMPLETE')
        self.assertEqual(calls[0][0],calls[1][0])
        self.assertEqual(feed.wake_event.clear.call_count,2)
        feed.wake_event.wait.assert_called_once_with(.25)

    def test_definitely_unsent_marker_is_terminal_without_fill_or_closure_claim(self):
        state=deepcopy(self.state)
        state['bindings']=[];state['evidence']['bindings']=[]
        state['evidence']['snapshot']['open_orders']=[]
        state['originals'][self.cid]['entry_unsent_no_retry']=True
        observed=trial.outcome(state,self.cid,T)
        self.assertEqual(observed['lifecycle'],'ABORTED_UNSENT')
        self.assertTrue(observed['terminal_verified'])
        self.assertEqual(observed['entry_quantity'],'0')
        self.assertFalse(observed['closure_verified'])
        self.assertFalse(observed['protection_verified'])
        result,calls=self.run_script(lambda call,now:state)
        self.assertEqual(result['status'],'ABORTED_UNSENT')
        self.assertEqual(len(calls),1)

    def test_management_continues_after_entry_grant_expires_without_new_entry(self):
        waiting=deepcopy(self.state);waiting['entry_timing_armed']={self.cid:T}
        result,calls=self.run_script(lambda call,now: waiting if call==1 else self.protected())
        self.assertEqual(result['status'],'PROTECTED_TIMING_COMPLETE')
        self.assertEqual(result['entry_attempts'],1)
        self.assertEqual(len(calls),2)
        self.assertGreater(calls[1][0],result['entry_authorization_expires_at_ms'])
        self.assertEqual(calls[0][1]['allowed_entry_card_id'],self.cid)
        self.assertEqual(calls[1][1]['allow_new_entries'],False)
        self.assertTrue(result['ongoing_management_required'])
        self.assertFalse(result['deployed_worker_handoff_verified'])
        self.assertTrue(result['trial_supervisor_stopped'])

    def test_unknown_reply_timeout_remains_unresolved_and_not_successful(self):
        state=deepcopy(self.state);state['bindings']=[];state['evidence']['bindings']=[]
        state['evidence']['snapshot']['open_orders']=[]
        state['pending']='f'*64
        state['entry_timing_armed']={self.cid:T}
        state['request']=dict(phase='OUTCOME_UNKNOWN',attempts=1,
            proposal=dict(operation='ENTRY',card_id=self.cid))
        result,calls=self.run_script(lambda call,now:state)
        self.assertEqual(result['status'],'RECONCILIATION_REQUIRED')
        self.assertEqual(result['observation']['pending_request_id'],'f'*64)
        self.assertTrue(result['ongoing_management_required'])
        self.assertEqual(result['entry_attempts'],1)
        self.assertTrue(all(options.get('allow_new_entries') is False for _,options in calls[1:]))

    def test_venue_time_unknown_is_reported_without_repeating_entry(self):
        result,calls=self.run_script(lambda call,now:self.protected(False))
        self.assertEqual(result['status'],'PROTECTED_VENUE_TIME_UNKNOWN')
        self.assertEqual(result['timing']['fill_to_stop_public_ms'],None)
        self.assertEqual(len(calls),1)

    def test_expired_grant_without_submission_never_invents_an_entry_attempt(self):
        result,calls=self.run_script(lambda call,now:None)
        self.assertEqual(result['status'],'ENTRY_NOT_SUBMITTED')
        self.assertEqual(result['entry_attempts'],0)
        self.assertFalse(result['ongoing_management_required'])
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][1]['allowed_entry_card_id'],self.cid)

    def test_verified_cancellation_after_grant_expiry_is_terminal(self):
        waiting=deepcopy(self.state);waiting['entry_timing_armed']={self.cid:T}
        result,calls=self.run_script(lambda call,now:waiting if call==1 else self.canceled())
        self.assertEqual(result['status'],'CANCELED_WITHOUT_FILL')
        self.assertTrue(result['observation']['terminal_verified'])
        self.assertFalse(result['ongoing_management_required'])
        self.assertEqual(len(calls),2)

    def test_shutdown_failure_prevents_complete_trial_result(self):
        result,_=self.run_script(lambda call,now:self.protected(),shutdown=False)
        self.assertFalse(result['trial_supervisor_stopped'])
        self.assertEqual(result['status'],'RECONCILIATION_REQUIRED')

    def test_quota_denial_waits_before_entry_and_preserves_grant(self):
        feed=self.notification_feed()
        result,calls=self.run_script(lambda *args:self.fail('no attempt'),notifications=feed,
            cycle_failure=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED'))
        self.assertEqual(result['status'],'ENTRY_NOT_SUBMITTED')
        self.assertEqual(result['entry_authorization_expires_at_ms'],T+90000)
        self.assertEqual(result['entry_attempts'],0)
        self.assertEqual(result['order_requests_sent'],0)
        self.assertTrue(all(b-a>=5000 for (a,_),(b,__) in zip(calls,calls[1:])))
        self.assertLessEqual(len(calls),19)

    def test_unknown_budget_policy_error_is_not_retried(self):
        with self.assertRaisesRegex(BudgetError,'TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE'):
            self.run_script(lambda *args:self.fail('no attempt'),
                cycle_failure=BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE'))

    def test_public_setup_wait_has_fixed_deadline_and_no_attempts(self):
        clock=[T]
        venue=Mock();venue.now.side_effect=lambda:clock[0]
        stopped=Mock();stopped.wait.side_effect=lambda seconds:clock.__setitem__(0,clock[0]+int(seconds*1000))
        operation=Mock(side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED'))
        with patch.object(trial.time,'monotonic',side_effect=lambda:(clock[0]-T)/1000):
            value,reason=trial._wait_entry_budget(operation,venue=venue,expiry=T+7000,
                wall_deadline=7,stopped=stopped)
        self.assertIsNone(value)
        self.assertEqual(reason,'ENTRY_REQUEST_BUDGET_WAIT_EXPIRED')
        self.assertEqual(clock[0],T+7000)
        self.assertEqual(operation.call_count,2)
        self.assertEqual([c.args[0] for c in stopped.wait.call_args_list],[5,2])
        result=trial._unsubmitted(self.cid,self.card,T+7000,T+315000,reason)
        self.assertEqual(result['entry_attempts'],0)
        self.assertEqual(result['order_requests_sent'],0)
        self.assertIsNone(result['timing'])
        self.assertIsNone(result['observation'])

    def test_setup_recovery_runs_fresh_operation_without_extending_expiry(self):
        clock=[T]
        venue=Mock();venue.now.side_effect=lambda:clock[0]
        stopped=Mock();stopped.wait.side_effect=lambda seconds:clock.__setitem__(0,clock[0]+int(seconds*1000))
        original=deepcopy(self.card)
        operation=Mock(side_effect=[BudgetError('TESTNET_REQUEST_BUDGET_BUSY'),self.card])
        with patch.object(trial.time,'monotonic',side_effect=lambda:(clock[0]-T)/1000):
            value,reason=trial._wait_entry_budget(operation,venue=venue,expiry=T+90000,
                wall_deadline=90,stopped=stopped)
        self.assertEqual(value,original)
        self.assertIsNone(reason)
        self.assertEqual(clock[0],T+5000)
        self.assertEqual(self.card,original)
        self.assertEqual(operation.call_count,2)

    def test_setup_wall_deadline_blocks_even_if_venue_clock_stalls(self):
        venue=Mock();venue.now.return_value=T
        stopped=Mock();wall=[0]
        stopped.wait.side_effect=lambda seconds:wall.__setitem__(0,wall[0]+seconds)
        operation=Mock(side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED'))
        with patch.object(trial.time,'monotonic',side_effect=lambda:wall[0]):
            value,reason=trial._wait_entry_budget(operation,venue=venue,expiry=T+90000,
                wall_deadline=6,stopped=stopped)
        self.assertEqual(wall[0],6)
        self.assertEqual(operation.call_count,2)
        self.assertEqual(reason,'ENTRY_REQUEST_BUDGET_WAIT_EXPIRED')

    def test_expired_setup_never_runs_and_unknown_errors_propagate(self):
        venue=Mock();venue.now.return_value=T
        operation=Mock();stopped=Mock()
        value,reason=trial._wait_entry_budget(operation,venue=venue,expiry=T,
            wall_deadline=trial.time.monotonic()+10,stopped=stopped)
        operation.assert_not_called();stopped.wait.assert_not_called()
        self.assertEqual(reason,'ENTRY_REQUEST_BUDGET_WAIT_EXPIRED')
        for failure in (BudgetError('TESTNET_SHARED_REQUEST_BUDGET_UNAVAILABLE'),
                        DispatchError('UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')):
            operation.side_effect=failure
            with self.assertRaises(type(failure)):
                trial._wait_entry_budget(operation,venue=venue,expiry=T+90000,
                    wall_deadline=trial.time.monotonic()+10,stopped=stopped)
        self.assertEqual(operation.call_count,2)
        stopped.wait.assert_not_called()

    def test_connected_feed_quota_wait_does_not_spin_or_renew_original_grant(self):
        clock=[T]
        base=Mock();base.venue.now.side_effect=lambda:clock[0]
        stopped=Mock();stopped.wait.side_effect=lambda seconds:clock.__setitem__(0,clock[0]+int(seconds*1000))
        feed=self.notification_feed();role=self.card['account_role'];route=ROUTES2[role]
        with patch.object(trial.time,'monotonic',side_effect=lambda:(clock[0]-T)/1000), \
             patch.object(trial,'_reconcile_notifications',side_effect=
                [BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')]*3+[True]) as reconcile:
            trial._wait_notifications(base,feed,role,route,self.card,T+90000,stopped)
        self.assertEqual(clock[0],T+15000)
        self.assertEqual(reconcile.call_count,4)
        self.assertEqual([call.args[0] for call in stopped.wait.call_args_list],[5,5,5])
        self.assertTrue(all(call.kwargs['reconciliation'] is
            reconcile.call_args_list[0].kwargs['reconciliation']
            for call in reconcile.call_args_list))

    def test_notification_quota_expires_before_registration_or_entry(self):
        clock=[T]
        base=Mock();base.venue.now.side_effect=lambda:clock[0]
        stopped=Mock();stopped.wait.side_effect=lambda seconds:clock.__setitem__(0,clock[0]+int(seconds*1000))
        role=self.card['account_role'];route=ROUTES2[role]
        with patch.object(trial.time,'monotonic',side_effect=lambda:(clock[0]-T)/1000), \
             patch.object(trial,'_reconcile_notifications',side_effect=
                BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')) as reconcile:
            with self.assertRaisesRegex(DispatchError,'TIMING_TRIAL_FILL_NOTIFICATION_GAP_NO_NEW_ENTRY'):
                trial._wait_notifications(base,self.notification_feed(),role,route,self.card,T+7000,stopped)
        self.assertEqual(clock[0],T+7000)
        self.assertEqual(reconcile.call_count,2)

    def test_notification_quota_failure_is_distinct_from_unknown_public_failure(self):
        feed=self.notification_feed();base=Mock();base.venue.now.return_value=T
        role=self.card['account_role'];route=ROUTES2[role]
        with patch.object(trial.stream,'tick',return_value=dict(
                status='EXISTING_RECONCILIATION_REQUIRED',failure_code='TESTNET_REQUEST_BUDGET_EXHAUSTED')):
            with self.assertRaisesRegex(BudgetError,'TESTNET_REQUEST_BUDGET_EXHAUSTED'):
                trial._reconcile_notifications(base,feed,role,route,self.card)
        with patch.object(trial.stream,'tick',return_value=dict(
                status='EXISTING_RECONCILIATION_REQUIRED',failure_code='UNOWNED_ACCOUNT_ORDER_NO_NEW_ENTRY')):
            self.assertFalse(trial._reconcile_notifications(base,feed,role,route,self.card))

class HistoricalTrialAdmissionTests(NoExternal):
    def completed_case(self,*,filled=False):
        from . import fill_wakeups
        from .test_fill_wakeups import Clock
        from .test_priority_fill_runtime import ready
        state=state_from_case(q='40',stop='40',take='40',expiry_seconds=300) if filled else state_from_case(q='0',expiry_seconds=300)
        binding=state['bindings'][0]
        state['evidence']['snapshot']['open_orders']=[]
        state['evidence']['snapshot']['terminal_orders']=[terminal(binding,'ENTRY','0')]
        if filled:
            state['evidence']['snapshot'].update(position_quantity='0',
                fills=[fill(binding,'ENTRY','40'),fill(binding,'STOP','40')],
                terminal_orders=[terminal(binding,'ENTRY','40'),terminal(binding,'STOP','40'),
                                 terminal(binding,'TAKE_PROFIT','0')])
        new=state_from_case(n=2,q='0',expiry_seconds=300)
        cid=new['bindings'][0]['card_id']
        state['originals'][cid]=new['originals'][cid]
        role=new['originals'][cid]['card']['account_role'];route=ROUTES2[role]
        environment={**env(),'HL_TESTNET_RUNTIME_MODE':'long_stream_testnet_v1',
            'HL_TESTNET_PROTECTION_TIMING_CARD_ID':cid,
            'HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS':str(T+90000)}
        venue=dispatch.TestnetVenue(environment)
        venue.now=Mock(return_value=T+100)
        clock=Clock();feed=fill_wakeups.FillWakeups({role:route['account']},clock=clock)
        ready(feed,route['account'])
        feed.finish_reconciliation(feed.begin_reconciliation(route['account']),complete=True)
        venue.fill_wakeups=feed
        current=[deepcopy(state)]
        store=Mock(domain='testnet');store.load.side_effect=lambda bucket:deepcopy(current[0])
        controller=dispatch.Controller(store,venue,ROUTES2,after_exit_policy=dispatch.AFTER_EXIT)
        def collect(bucket,prior):
            current[0]['revision']+=1
            current[0]['evidence']['snapshot']['at_ms']=venue.now()
            return deepcopy(current[0])
        with patch.object(controller,'_refresh_once',side_effect=collect):
            controller.refresh(state['bucket'])
        return controller,venue,feed,current,cid,role,route

    def prepare(self,controller,current,cid,**kwargs):
        return controller._prepare_cycle(current[0]['bucket'],send=True,
            allow_new_entries=True,allowed_entry_card_id=cid,**kwargs)

    def stale_case(self,*,filled=False):
        controller,venue,feed,current,cid,role,route=self.completed_case(filled=filled)
        venue.env['HL_TESTNET_PROTECTION_TIMING_STARTED_MS']=str(T)
        venue.retain_trial_terminal_history(current[0],controller._feed_stamp(route['account']))
        venue.now.return_value=T+16000
        self.assertIsNone(controller._trial_preparation_checkpoint(current[0],send=True,
            allow_new_entries=True,allowed_entry_card_id=cid))
        return controller,venue,feed,current,cid,role,route

    def test_stale_trial_funds_exact_330_and_retains_terminal_clocks_with_durable_provenance(self):
        from .request_budget import Budget,request_weight,BACKGROUND_LIMIT
        # Real finite InfoPlan/claim logic, with only the durable ledger and
        # public transport replaced. Both measured steady-load cases admit it.
        for baseline in (368,460):
            with self.subTest(baseline=baseline):
                controller,venue,feed,current,cid,role,route=self.stale_case(filled=baseline==460)
                before=deepcopy(current[0]);budget=Budget.__new__(Budget)
                funded=[];used=[baseline];reads=[];committed=[]
                def fund(entries,priority,deadline):
                    cost=sum(weight for _,_,weight in entries)
                    if used[0]+cost>BACKGROUND_LIMIT:
                        raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
                    used[0]+=cost;funded.append(cost)
                budget._fund_observation=Mock(side_effect=fund)
                budget._claim_observation=Mock();budget._release_unclaimed_observation=Mock()
                def read(reader,kind,account,**kwargs):
                    self.assertEqual(funded,[330])
                    body=dict(type=kind,user=account,**kwargs)
                    reader.budget.acquire('/info',body,priority='background')
                    reads.append(body)
                    return [] if kind=='frontendOpenOrders' else dict(assetPositions=[])
                def change(bucket,revision,event,now,update):
                    self.assertEqual(revision,current[0]['revision'])
                    value=deepcopy(current[0]);update(Mock(),value)
                    value['revision']+=1;current[0]=value;committed.append(event)
                    return deepcopy(value)
                controller.store.change.side_effect=change
                controller.store.pending_record.return_value=None
                with patch.object(venue,'request_budget',wraps=venue.request_budget), \
                     patch('hl_testnet_runtime.request_budget.Budget.from_env',return_value=budget), \
                     patch.object(dispatch.roles,'route_for',return_value=route), \
                     patch.object(dispatch.evidence.PublicReader,'read',new=read), \
                     patch.object(dispatch.evidence,'collect',side_effect=AssertionError('NO_SECOND_HISTORY_READ')), \
                     patch.object(venue,'sample',return_value=dict(mark_price='10',at_ms=T+16000)), \
                     patch.object(venue,'metadata',return_value={}), \
                     patch.object(controller,'_plan_cycle',return_value=dict(status='NO_ACTION_NEEDED')):
                    self.assertEqual(self.prepare(controller,current,cid)['status'],'NO_ACTION_NEEDED')
                self.assertEqual(funded,[330]);self.assertLessEqual(used[0],800)
                self.assertEqual([b['type'] for b in reads].count('frontendOpenOrders'),2)
                self.assertEqual([b['type'] for b in reads].count('clearinghouseState'),2)
                self.assertEqual(committed,['PUBLIC_RECONCILIATION'])
                snapshot=current[0]['evidence']['snapshot']
                for field in ('fills','terminal_orders','open_orders','position_quantity'):
                    self.assertEqual(snapshot[field],before['evidence']['snapshot'][field])
                self.assertEqual(snapshot['at_ms'],T+16000)
                provenance=current[0]['evidence']['immutable_terminal_inventory']
                self.assertEqual(provenance['historical_snapshot_at_ms'],T+100)
                self.assertEqual(provenance['historical_evidence_digest'],dispatch.life.digest(before['evidence']))
                self.assertEqual(provenance['inventory_observed_at_ms'],[T+16000,T+16000])
                self.assertEqual(provenance['original_trial_started_at_ms'],T)
                self.assertEqual(provenance['original_trial_expires_at_ms'],T+90000)
                stamp=controller._feed_stamp(route['account'])
                self.assertEqual(provenance['notification_boundary'],dict(generation=stamp[0],revision=stamp[1]))
                self.assertEqual(venue.sent,0);controller.store.prepare_and_begin.assert_not_called()
                budget._release_unclaimed_observation.assert_called_once()

    def test_stale_refused_full_cycle_wait_does_not_refuel_ledger_with_setup_http(self):
        controller,venue,feed,current,cid,role,route=self.stale_case()
        before=deepcopy(current[0]);costs=[]
        from .request_budget import request_weight
        def refused(bodies):
            costs.append(sum(request_weight('/info',b) for b in bodies))
            raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        with patch.object(venue,'_fund_info_plan',side_effect=refused), \
             patch.object(dispatch.roles,'route_for',return_value=route), \
             patch.object(controller,'refresh') as refresh, \
             patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
            for at in (T+16000,T+21000,T+26000):
                venue.now.return_value=at
                with self.assertRaises(BudgetError):self.prepare(controller,current,cid)
        self.assertEqual(costs,[330]*3)
        for operation in (refresh,sample,meta):operation.assert_not_called()
        self.assertEqual(current[0],before);self.assertEqual(venue.sent,0)
        controller.store.prepare_and_begin.assert_not_called()

    def test_invalid_retained_history_or_feed_reconciliation_blocks_before_funding_and_http(self):
        from .test_fill_wakeups import fills
        for variant in ('missing-anchor','expired','source','grant-start','grant-expiry',
                        'binding','original','terminal','fill','history','pending',
                        'hint','reconciled-revision','generation'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.stale_case()
                state=current[0];snapshot=state['evidence']['snapshot']
                if variant=='missing-anchor':del venue.trial_terminal_history
                elif variant=='expired':venue.now.return_value=T+90000
                elif variant=='source':state['originals'][cid]['card']['source_expires_at']='2020-01-01T00:00:00Z'
                elif variant=='grant-start':venue.env['HL_TESTNET_PROTECTION_TIMING_STARTED_MS']=str(T+1)
                elif variant=='grant-expiry':venue.env['HL_TESTNET_PROTECTION_TIMING_EXPIRES_MS']=str(T+90001)
                elif variant=='binding':state['bindings'][0]['orders']['ENTRY'].append('999')
                elif variant=='original':state['originals'][cid]['entry_unsent_no_retry']=True
                elif variant=='terminal':snapshot['terminal_orders'][0]['at_ms']+=1
                elif variant=='fill':snapshot['fills'].append(fill(state['bindings'][0],'ENTRY','1'))
                elif variant=='history':snapshot['history_complete']=False
                elif variant=='pending':
                    # This retains ordinary post-attempt observation ownership,
                    # never the inventory-only history path.
                    state['pending']='f'*64
                elif variant in ('hint','reconciled-revision'):
                    token=feed.begin_reconciliation(route['account'])
                    feed._receive(route['account'],token.generation,json.dumps(fills(
                        route['account'],rows=[dict(coin=state['symbol'],tid=88,oid=888,time=T+200)])))
                    if variant=='reconciled-revision':
                        feed.finish_reconciliation(feed.begin_reconciliation(route['account']),complete=True)
                        self.assertTrue(feed.entry_allowed(route['account']))
                elif variant=='generation':feed._opened(route['account'])
                with patch.object(venue,'_fund_info_plan') as fund, \
                     patch.object(controller,'refresh',side_effect=DispatchError('ORDINARY_RECOVERY_REQUIRED')) as refresh, \
                     patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
                    for _ in range(2):
                        with self.assertRaises(DispatchError):self.prepare(controller,current,cid)
                fund.assert_not_called();sample.assert_not_called();meta.assert_not_called()
                if variant=='pending':self.assertEqual(refresh.call_count,2)
                else:refresh.assert_not_called()

    def test_retained_history_requires_original_grant_full_read_and_all_terminal_certificates(self):
        for variant in ('before-start','incomplete-terminal-coverage','changed-feed-after-read'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.completed_case()
                venue.env['HL_TESTNET_PROTECTION_TIMING_STARTED_MS']=str(T)
                stamp=controller._feed_stamp(route['account'])
                if variant=='before-start':venue.env['HL_TESTNET_PROTECTION_TIMING_STARTED_MS']=str(T+101)
                elif variant=='incomplete-terminal-coverage':current[0]['bindings'][0]['orders']['STOP'].append('999')
                else:feed._opened(route['account'])
                with self.assertRaisesRegex(DispatchError,'TRIAL_CHECKPOINT_CHANGED_RECONCILE_FIRST'):
                    venue.retain_trial_terminal_history(current[0],stamp)
                self.assertNotIn('trial_terminal_history',vars(venue))

    def test_stale_plan_rechecks_state_and_feed_after_funding_before_http(self):
        for variant in ('revision','feed','grant','binding'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.stale_case();batch=Mock()
                def changed(bodies):
                    if variant=='revision':current[0]['revision']+=1
                    elif variant=='feed':feed._opened(route['account'])
                    elif variant=='grant':venue.now.return_value=T+90000
                    else:current[0]['bindings'][0]['orders']['ENTRY'].append('999')
                    return batch
                with patch.object(venue,'_fund_info_plan',side_effect=changed), \
                     patch.object(dispatch.roles,'route_for',return_value=route), \
                     patch.object(controller,'refresh') as refresh, \
                     patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
                    with self.assertRaisesRegex(DispatchError,'TRIAL_CHECKPOINT_CHANGED_RECONCILE_FIRST'):
                        self.prepare(controller,current,cid)
                for operation in (refresh,sample,meta):operation.assert_not_called()
                batch.close.assert_called_once()

    def test_post_attempt_observation_never_uses_retained_terminal_inventory(self):
        for variant in ('armed','bound','pending','working','newfill'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.stale_case()
                state=current[0]
                if variant=='armed':state['entry_timing_armed']={cid:T+200}
                elif variant=='bound':state['bindings'].append(state_from_case(n=2,q='0')['bindings'][0])
                elif variant=='pending':state['pending']='e'*64
                elif variant=='working':state['evidence']['snapshot']['open_orders']=state_from_case(q='0')['evidence']['snapshot']['open_orders']
                else:state['evidence']['snapshot']['fills'].append(fill(state['bindings'][0],'ENTRY','1'))
                with patch.object(dispatch.evidence,'collect',return_value={}) as collect, \
                     patch.object(venue,'request_budget',return_value=Mock()), \
                     patch.object(venue,'empty_snapshot',side_effect=AssertionError('HISTORY_CANNOT_BE_INVENTORY_ONLY')):
                    self.assertEqual(venue.collect_checkpoint(dict(bindings=state['bindings'],
                        snapshot=state['evidence']['snapshot']),pending=state['pending']),{})
                collect.assert_called_once()

    def test_change_during_inventory_or_checkpoint_discards_all_retained_evidence(self):
        from .test_fill_wakeups import fills
        for variant in ('hint','revision','checkpoint'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.stale_case()
                before=deepcopy(current[0]);batch=Mock();count=[0]
                venue.parallel_preflight=False
                def read(reader,kind,account):
                    count[0]+=1
                    if count[0]==2 and variant=='hint':
                        token=feed.begin_reconciliation(account)
                        feed._receive(account,token.generation,json.dumps(fills(account,
                            rows=[dict(coin=before['symbol'],tid=89,oid=889,time=T+16100)])))
                    if count[0]==4 and variant=='revision':current[0]['revision']+=1
                    return [] if kind=='frontendOpenOrders' else dict(assetPositions=[])
                def checkpoint(bucket,revision,event,now,update):
                    value=deepcopy(current[0]);value['revision']+=1
                    update(Mock(),value)
                    self.fail('CHANGED_CHECKPOINT_MUST_NOT_COMMIT')
                controller.store.change.side_effect=checkpoint
                with patch.object(venue,'_fund_info_plan',return_value=batch), \
                     patch.object(dispatch.roles,'route_for',return_value=route), \
                     patch.object(dispatch.evidence.PublicReader,'read',new=read), \
                     patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
                    with self.assertRaisesRegex(DispatchError,'TRIAL_CHECKPOINT_CHANGED_RECONCILE_FIRST'):
                        self.prepare(controller,current,cid)
                self.assertEqual(count[0],4)
                self.assertEqual(current[0]['evidence'],before['evidence'])
                self.assertNotIn('immutable_terminal_inventory',current[0]['evidence'])
                if variant!='checkpoint':controller.store.change.assert_not_called()
                sample.assert_not_called();meta.assert_not_called();batch.close.assert_called_once()
                controller.store.prepare_and_begin.assert_not_called();self.assertEqual(venue.sent,0)

    def test_fresh_historical_trial_reuses_committed_checkpoint_then_funds_286_reads(self):
        from .request_budget import request_weight
        controller,venue,feed,current,cid,role,route=self.completed_case()
        before=deepcopy(current[0]);batch=Mock()
        with patch.object(venue,'_fund_info_plan',return_value=batch) as fund, \
             patch.object(dispatch.roles,'route_for',return_value=route), \
             patch.object(controller,'refresh',side_effect=AssertionError('NO_DUPLICATE_REFRESH')) as refresh, \
             patch.object(venue,'sample',return_value=dict(mark_price='10',at_ms=T+100)), \
             patch.object(venue,'metadata',return_value={}), \
             patch.object(controller,'_plan_cycle',return_value=dict(status='NO_ACTION_NEEDED')) as plan:
            self.assertEqual(self.prepare(controller,current,cid)['status'],'NO_ACTION_NEEDED')
        self.assertEqual(sum(request_weight('/info',b) for b in fund.call_args.args[0]),286)
        self.assertEqual(plan.call_args.args[1],before)
        self.assertEqual(current[0],before)
        refresh.assert_not_called();batch.close.assert_called_once()
        self.assertTrue(feed.entry_allowed(route['account']))

    def test_historical_admission_refusal_spends_no_further_http_or_attempt(self):
        controller,venue,feed,current,cid,role,route=self.completed_case()
        with patch.object(venue,'_fund_info_plan',
                side_effect=BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')), \
             patch.object(dispatch.roles,'route_for',return_value=route), \
             patch.object(controller,'refresh') as refresh, \
             patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
            for _ in range(3):
                with self.assertRaises(BudgetError):self.prepare(controller,current,cid)
        for operation in (refresh,sample,meta):operation.assert_not_called()
        controller.store.prepare_and_begin.assert_not_called()
        self.assertEqual(venue.sent,0)

    def test_historical_reuse_denied_for_changed_stale_incomplete_unfinished_or_attempted_state(self):
        from .test_fill_wakeups import fills
        for variant in ('revision','hint','generation','stale','future','pending',
                        'history','orders','bindings','working','position','attempted',
                        'wrong-card','disabled','preview'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.completed_case()
                options=dict(send=True,allow_new_entries=True,allowed_entry_card_id=cid)
                state=current[0];snapshot=state['evidence']['snapshot']
                if variant=='revision':state['revision']+=1
                elif variant=='hint':
                    token=feed.begin_reconciliation(route['account'])
                    feed._receive(route['account'],token.generation,json.dumps(fills(
                        route['account'],rows=[dict(coin=state['symbol'],tid=88,oid=888,time=T+200)])))
                elif variant=='generation':feed._opened(route['account'])
                elif variant=='stale':venue.now.return_value=T+15101
                elif variant=='future':venue.now.return_value=T+99
                elif variant=='pending':state['pending']='e'*64
                elif variant=='history':snapshot['history_complete']=False
                elif variant=='orders':snapshot['orders_complete']=False
                elif variant=='bindings':state['evidence']['bindings']=[]
                elif variant=='working':snapshot['open_orders']=state_from_case(q='0')['evidence']['snapshot']['open_orders']
                elif variant=='position':snapshot['position_quantity']='1'
                elif variant=='attempted':state['entry_timing_armed']={cid:T}
                elif variant=='wrong-card':options['allowed_entry_card_id']='e'*64
                elif variant=='disabled':options['allow_new_entries']=False
                elif variant=='preview':options['send']=False
                self.assertIsNone(controller._trial_preparation_checkpoint(deepcopy(state),**options))

    def test_hint_or_revision_change_during_admission_releases_plan_without_http(self):
        from .test_fill_wakeups import fills
        for variant in ('revision','hint'):
            with self.subTest(variant=variant):
                controller,venue,feed,current,cid,role,route=self.completed_case();batch=Mock()
                def changed(bodies):
                    if variant=='revision':current[0]['revision']+=1
                    else:
                        token=feed.begin_reconciliation(route['account'])
                        feed._receive(route['account'],token.generation,json.dumps(fills(
                            route['account'],rows=[dict(coin=current[0]['symbol'],tid=89,oid=889,time=T+200)])))
                    return batch
                with patch.object(venue,'_fund_info_plan',side_effect=changed), \
                     patch.object(dispatch.roles,'route_for',return_value=route), \
                     patch.object(controller,'refresh') as refresh, \
                     patch.object(venue,'sample') as sample,patch.object(venue,'metadata') as meta:
                    with self.assertRaisesRegex(DispatchError,'TRIAL_CHECKPOINT_CHANGED_RECONCILE_FIRST'):
                        self.prepare(controller,current,cid)
                for operation in (refresh,sample,meta):operation.assert_not_called()
                batch.close.assert_called_once()

    def test_completed_trial_checkpoint_does_not_suppress_normal_or_emergency_refresh(self):
        controller,venue,feed,current,cid,role,route=self.completed_case()
        for urgent in (False,True):
            with patch.object(controller,'_refresh_once',return_value=deepcopy(current[0])) as collect:
                controller.refresh(current[0]['bucket'],emergency=urgent)
            collect.assert_called_once()

    def test_failed_new_collection_invalidates_prior_completed_trial_proof(self):
        controller,venue,feed,current,cid,role,route=self.completed_case()
        with patch.object(controller,'_refresh_once',
                side_effect=DispatchError('OBSERVATION_CHANGED_RETRY')):
            with self.assertRaises(DispatchError):controller.refresh(current[0]['bucket'])
        self.assertIsNone(controller._trial_preparation_checkpoint(current[0],send=True,
            allow_new_entries=True,allowed_entry_card_id=cid))

    def test_completed_cache_keeps_one_per_bucket_and_prunes_expired_proofs(self):
        controller,venue,feed,current,cid,role,route=self.completed_case()
        bucket=current[0]['bucket'];old=controller._observations.completed[bucket]
        with patch.object(controller,'_refresh_once',return_value=deepcopy(current[0])):
            controller.refresh(bucket)
        self.assertEqual(len(controller._observations.completed),1)
        self.assertIsNot(controller._observations.completed[bucket],old)
        controller._observations.completed['old-bucket']=old
        venue.now.return_value=T+15101
        current[0]['evidence']['snapshot']['at_ms']=venue.now()
        with patch.object(controller,'_refresh_once',return_value=deepcopy(current[0])):
            controller.refresh(bucket)
        self.assertEqual(set(controller._observations.completed),{bucket})
