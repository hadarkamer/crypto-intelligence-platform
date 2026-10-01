"""Trial orchestration regressions; no exchange, signer or service credentials."""
from contextlib import ExitStack
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch

from . import protection_timing_trial as trial, emergency_close as emergency
from .test_filled_quantity_dispatch import NoExternal,state_from_case,ROUTES2
from .test_long_stream_runtime import env
from .test_card_lifecycle import T,terminal,fill
from .filled_dispatch_store import DispatchError
from .request_budget import BudgetError


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
        feed.begin_reconciliation.side_effect=lambda account:SimpleNamespace(account=account)
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
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[-1][1]['allow_new_entries'],False)

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
