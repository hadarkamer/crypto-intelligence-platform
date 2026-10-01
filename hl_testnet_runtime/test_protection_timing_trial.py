"""Trial orchestration regressions; no exchange, signer or service credentials."""
from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

from . import protection_timing_trial as trial, emergency_close as emergency
from .test_filled_quantity_dispatch import NoExternal,state_from_case,ROUTES2
from .test_long_stream_runtime import env
from .test_card_lifecycle import T,terminal,fill


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

    def run_script(self,script,*,shutdown=True):
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
            def register(self,cid):return self.store.load(None)
            def cycle(self,bucket,**options):
                shared['calls'].append((shared['now'],options))
                new=script(len(shared['calls']),shared['now'])
                if new is not None:
                    shared['state']=deepcopy(new)
                shared['state']['evidence']['snapshot']['at_ms']=shared['now']
                return dict(status='WORKING')
        class ClockEvent:
            def wait(self,seconds):shared['now']+=100000
            def set(self):shared['shutdown']=True
        event=ClockEvent()
        environment={**env(),'HL_TESTNET_EMERGENCY_CLOSE':emergency.APPROVAL,
                     'HL_TESTNET_SHORT_ENTRY_ENABLED':'false'}
        def stop(stopped,timeout):
            stopped.set()
            return shutdown
        with ExitStack() as stack:
            stack.enter_context(patch.object(trial.dispatch,'controller_from_env',
                side_effect=[Base(),Controlled()]))
            stack.enter_context(patch.object(trial,'CardStore')).return_value.load.return_value=self.card
            stack.enter_context(patch.object(trial.stream,'_account_owned'))
            stack.enter_context(patch.object(trial.threading,'Event',return_value=event))
            stack.enter_context(patch.object(trial.time,'monotonic',
                side_effect=lambda:(shared['now']-T)/1000))
            stack.enter_context(patch.object(emergency,'start',return_value=True))
            stack.enter_context(patch.object(emergency,'healthy',return_value=True))
            stack.enter_context(patch.object(emergency,'stop_supervisor',side_effect=stop,create=True))
            result=trial.run_one(environment,self.cid)
        self.assertTrue(shared['shutdown'])
        return result,shared['calls']

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
