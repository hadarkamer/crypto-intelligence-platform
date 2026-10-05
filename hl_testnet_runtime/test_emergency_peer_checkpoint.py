"""A concurrent complete STOP proof can end a losing read without a resend."""
from copy import deepcopy
from unittest.mock import patch

from . import emergency_close as emergency, filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchError
from .test_filled_quantity_dispatch import NoExternal,state_from_case,ROUTES2
from .test_emergency_close import Venue
from .test_history_gap_recovery import MemoryStore
from .test_card_lifecycle import T


class PeerCheckpointTests(NoExternal):
    def case(self):
        state=state_from_case(q='40',stop='40',take='40')
        state['evidence']['snapshot']['at_ms']=T
        store=MemoryStore(state);venue=Venue();venue.t=T
        normal=dispatch.Controller(store,venue,ROUTES2)
        return state,store,venue,normal,emergency.Controller(normal,venue)

    def test_losing_normal_read_uses_current_durable_stop_proof_without_more_io(self):
        state,store,venue,normal,controller=self.case()
        current=deepcopy(state);current['revision']+=1
        store.values[state['bucket']]=current
        with patch.object(normal,'refresh',side_effect=DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')) as refresh, \
                patch.object(venue,'collect',side_effect=AssertionError('DUPLICATE_HTTP')):
            self.assertEqual(controller._refresh_normal(state),current)
        refresh.assert_called_once()
        self.assertEqual(venue.sent,0)

    def test_action_boundary_can_omit_work_from_a_newer_complete_stop_proof(self):
        state,store,venue,normal,controller=self.case()
        current=deepcopy(state);current['revision']+=1
        store.values[state['bucket']]=current
        with patch.object(venue,'sample',side_effect=AssertionError('UNNECESSARY_PRICE_HTTP')):
            result=controller._action_cycle(state['bucket'],state,metadata=None,send=True)
        self.assertEqual(result,dict(status='STOP_OBSERVED_OR_NO_EXPOSURE',order_requests_sent=0))
        self.assertEqual(store.load(state['bucket']),current)
        self.assertEqual(venue.sent,0)

    def test_stale_uncovered_pending_dirty_or_incident_proof_cannot_hide_race(self):
        for mode in ('stale','uncovered','pending','dirty','incident'):
            with self.subTest(mode=mode):
                state,store,venue,normal,controller=self.case()
                current=deepcopy(state);current['revision']+=1
                if mode=='stale':venue.t=T+5001
                if mode=='uncovered':
                    stops=set(current['bindings'][0]['orders']['STOP'])
                    current['evidence']['snapshot']['open_orders']=[o for o in
                        current['evidence']['snapshot']['open_orders'] if o['oid'] not in stops]
                if mode=='pending':current['pending']='unresolved-request'
                if mode=='incident':current['emergency']=dict(phase='ACTIVE')
                if mode=='dirty':
                    class DirtyFeed:
                        def dirty_symbols(self,account):return ('DOGE',)
                    venue.fill_wakeups=DirtyFeed()
                store.values[state['bucket']]=current
                with patch.object(normal,'refresh',side_effect=DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')):
                    with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
                        controller._refresh_normal(state)
                with self.assertRaisesRegex(DispatchError,'CONCURRENT_DISPATCH_RELOAD_REQUIRED'):
                    controller._action_cycle(state['bucket'],state,metadata=None,send=True)
                self.assertEqual(store.load(state['bucket']),current)
                self.assertEqual(venue.sent,0)
