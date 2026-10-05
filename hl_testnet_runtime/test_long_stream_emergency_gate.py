"""A cold safety supervisor postpones only entry work, without HTTP loops."""
from copy import deepcopy
from datetime import datetime,timezone
from unittest.mock import Mock,patch

from . import long_stream_runtime as stream,emergency_close as emergency
from . import card_lifecycle as life
from .test_filled_quantity_dispatch import NoExternal,state_from_case
from .test_filled_quantity_exits import original,ROUTES
from .test_card_lifecycle import T,A,B
from .test_history_gap_recovery import final_state


def candidate(side):
    binding,record=original(side=side,expiry_seconds=600)
    return dict(bucket=life.digest(['testnet',binding['account'],binding['symbol']]),
        account=binding['account'],symbol=binding['symbol'],revision=0,
        bindings=[],originals={binding['card_id']:record},pending=None,
        evidence=dict(bindings=[],snapshot=dict(environment='testnet',
            account=binding['account'],symbol=binding['symbol'],at_ms=T,
            history_complete=True,orders_complete=True,position_quantity='0',
            fills=[],open_orders=[],terminal_orders=[])))


class EntrySupervisorSchedulingTests(NoExternal):
    def controller(self,states):
        controller=Mock();controller.venue.env=dict(HL_TESTNET_EMERGENCY_CLOSE=emergency.APPROVAL)
        controller.venue.now.return_value=T;controller.store.for_account.return_value=states
        return controller

    def call(self,controller,side='LONG',**kwargs):
        role='long_account' if side=='LONG' else 'short_account'
        return stream.tick(controller,ROUTES[role],datetime.fromtimestamp((T-1000)/1000,timezone.utc),
                           new_entries=True,role=role,**kwargs)

    def test_cold_supervisor_does_no_public_or_registration_work_for_fresh_unbound_both_roles(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                state=candidate(side);before=deepcopy(state);controller=self.controller([state])
                with patch.object(emergency,'healthy',return_value=False), \
                        patch.object(stream,'_maintain_bucket',side_effect=AssertionError('NO_FLAT_MAINTENANCE')) as maintain, \
                        patch.object(stream,'_account_owned',side_effect=AssertionError('NO_PRE_ENTRY_HTTP')) as owned, \
                        patch.object(stream.selection,'page',side_effect=AssertionError('NO_ENTRY_SCAN')) as scan:
                    for _ in range(3):
                        result=self.call(controller,side)
                        self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_SUPERVISOR')
                        self.assertEqual(result['maintenance_active'],0)
                        self.assertEqual(result['new_cards_registered'],0)
                        self.assertEqual(result['order_requests_sent'],0)
                self.assertEqual(state,before)
                controller.cycle.assert_not_called();controller.register.assert_not_called()
                controller.venue.sample.assert_not_called();controller.venue.metadata.assert_not_called()
                controller.venue.send.assert_not_called();controller.store.prepare_and_begin.assert_not_called()

    def test_cold_supervisor_does_not_create_new_empty_candidate_bucket(self):
        controller=self.controller([])
        with patch.object(emergency,'healthy',return_value=False), \
                patch.object(stream.selection,'page',side_effect=AssertionError('NO_ENTRY_SCAN')), \
                patch.object(stream,'_account_owned',side_effect=AssertionError('NO_PUBLIC_HTTP')):
            result=self.call(controller,'SHORT')
        self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_SUPERVISOR')
        controller.register.assert_not_called();controller.store.create_bucket.assert_not_called()

    def test_existing_exposure_working_entry_and_uncertainty_are_maintained_while_cold(self):
        for mode in ('exposure','working-entry','pending'):
            with self.subTest(mode=mode):
                state=state_from_case(q='100',stop='100',take='100')
                if mode=='working-entry':state=state_from_case(q='40',stop='40',take='40')
                elif mode=='pending':state=candidate('LONG');state['pending']='a'*64
                controller=self.controller([state])
                with patch.object(emergency,'healthy',return_value=False), \
                        patch.object(stream,'_maintain_bucket',return_value=iter([dict(status='NO_ACTION_NEEDED',order_requests_sent=0)])) as maintain:
                    result=self.call(controller)
                maintain.assert_called_once_with(controller,state['bucket'])
                self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_SUPERVISOR')
                self.assertEqual(result['maintenance_active'],1)

    def test_forced_dirty_notification_reconciliation_continues_for_flat_candidate(self):
        for side in ('LONG','SHORT'):
            state=candidate(side);controller=self.controller([state])
            with patch.object(emergency,'healthy',return_value=False), \
                    patch.object(stream,'_maintain_bucket',return_value=iter([dict(status='NO_ACTION_NEEDED',order_requests_sent=0)])) as maintain:
                result=self.call(controller,side,dirty_symbols=(state['symbol'],))
            maintain.assert_called_once_with(controller,state['bucket'])
            self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_SUPERVISOR')

    def test_aged_short_history_and_staged_recovery_continue_while_cold(self):
        state=final_state(side='SHORT');controller=self.controller([state])
        controller.venue.now.return_value=T+2*60*60*1000
        with patch.object(emergency,'healthy',return_value=False), \
                patch.object(stream,'_maintain_bucket',return_value=iter([dict(status='NO_ACTION_NEEDED',order_requests_sent=0)])) as maintain:
            result=self.call(controller,'SHORT')
        maintain.assert_called_once_with(controller,state['bucket'])
        state['history_gap_recovery']={};controller=self.controller([state])
        progress=dict(status='HISTORY_GAP_RECOVERY_PROGRESS',cursor_ms=T,remaining_ms=1000,order_requests_sent=0)
        with patch.object(emergency,'healthy',return_value=False), \
                patch.object(stream,'_recover_history',return_value=progress) as recovery, \
                patch.object(stream,'_maintain_bucket',side_effect=AssertionError('NO_ORDINARY_CYCLE')):
            result=self.call(controller,'SHORT')
        recovery.assert_called_once_with(controller,state)
        self.assertEqual(result['status'],'HISTORY_GAP_RECOVERY_PROGRESS')

    def test_maintenance_order_count_is_preserved_while_entries_wait(self):
        state=state_from_case(q='100',stop=None,take=None);controller=self.controller([state])
        with patch.object(emergency,'healthy',return_value=False), \
                patch.object(stream,'_maintain_bucket',return_value=iter([dict(status='ACCEPTED_UNVERIFIED',order_requests_sent=1)])):
            result=self.call(controller)
        self.assertEqual(result['order_requests_sent'],1)
        self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_SUPERVISOR')

    def test_supervisor_recovery_resumes_original_source_selection_and_existing_final_gates(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                state=candidate(side);before=deepcopy(state);controller=self.controller([state])
                controller.venue.env={}
                role='long_account' if side=='LONG' else 'short_account'
                controller.venue.env['HL_TESTNET_EMERGENCY_CLOSE']=emergency.APPROVAL
                controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
                with patch.object(emergency,'healthy',return_value=True), \
                        patch.object(stream.roles,'short_entry_scope',return_value=None), \
                        patch.object(stream,'_maintain_bucket',side_effect=AssertionError('NO_DUPLICATE_EMPTY_MAINTENANCE')), \
                        patch.object(stream.selection,'page',return_value=([],None)), \
                        patch.object(stream,'_account_owned',return_value=True) as owned:
                    result=self.call(controller,side)
                self.assertEqual(result['status'],'SWEEP_COMPLETE')
                owned.assert_called_once_with(controller.venue,ROUTES[role]['account'],[state],role=role)
                controller.cycle.assert_called_once_with(state['bucket'],send=True,allowed_entry_card_id=None)
                self.assertEqual(state,before)

    def test_expired_source_is_not_revived_by_healthy_supervisor(self):
        state=candidate('LONG');controller=self.controller([state])
        controller.venue.now.return_value=T+600000
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        with patch.object(emergency,'healthy',return_value=True), \
                patch.object(stream.selection,'page',return_value=([],None)), \
                patch.object(stream,'_account_owned',side_effect=AssertionError('NO_EXPIRED_CANDIDATE_HTTP')):
            result=self.call(controller)
        self.assertEqual(result['status'],'SWEEP_COMPLETE')
        controller.cycle.assert_not_called();controller.register.assert_not_called()

    def test_latched_circuit_skips_fresh_entry_http_but_keeps_live_exit_maintenance(self):
        for exposed in (False,True):
            state=state_from_case(q='100',stop=None,take=None) if exposed else candidate('LONG')
            controller=self.controller([state])
            class Store:
                def entry_blocker(self):return 'EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY'
                def for_account(self,account):return [state]
                def load(self,bucket):return state
            controller.store=Store()
            with patch.object(stream,'_maintain_bucket',return_value=iter([
                    dict(status='ACCEPTED_UNVERIFIED',order_requests_sent=1)])) as maintain, \
                    patch.object(stream.selection,'page',side_effect=AssertionError('NO_ENTRY_SCAN')), \
                    patch.object(stream,'_account_owned',side_effect=AssertionError('NO_PRE_ENTRY_HTTP')):
                result=self.call(controller)
            self.assertEqual(result['status'],'NEW_ENTRIES_WAITING_FOR_EMERGENCY_CIRCUIT')
            self.assertEqual(result['failure_code'],'EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY')
            self.assertEqual(result['order_requests_sent'],int(exposed))
            self.assertEqual(maintain.call_count,int(exposed))
            controller.register.assert_not_called()
