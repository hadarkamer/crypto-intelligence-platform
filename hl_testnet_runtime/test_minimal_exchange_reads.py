"""Count omitted REST work while retaining actual execution/recovery fences."""
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from . import long_stream_runtime as stream, filled_quantity_dispatch as dispatch, trade_cards
from . import emergency_close as emergency
from .test_card_lifecycle import A, B, T, closed
from .test_filled_quantity_dispatch import NoExternal, state_from_case, ROUTES2, Venue
from .test_filled_quantity_exits import original
from .test_history_gap_recovery import MemoryStore


class CleanFeed:
    def __init__(self, *, clean=True): self.clean=clean
    def entry_allowed(self, account): return self.clean
    def dirty_symbols(self, account): return () if self.clean else None


class MinimalReadsTests(NoExternal):
    def case(self, side='LONG'):
        state=state_from_case(q='100',side=side,stop='100',take='100')
        _,record=original(2,side,expiry_seconds=120)
        card=record['card']
        controller=Mock()
        controller.venue.now.return_value=T+1
        controller.venue.env=({'HL_TESTNET_SHORT_TRIAL_CARD_ID':card['card_id']}
                              if side=='SHORT' else {})
        controller.venue.fill_wakeups=CleanFeed()
        controller.store.for_account.return_value=[state]
        return state,card,record,controller

    def tick(self, controller, side):
        return stream.tick(controller,dict(account=A if side=='LONG' else B),
            datetime.fromtimestamp((T-20000)/1000,timezone.utc),new_entries=True,
            role='long_account' if side=='LONG' else 'short_account',
            notification_continuity=True)

    def test_thirty_same_market_sweeps_spend_zero_entry_reads_both_accounts(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                state,card,record,controller=self.case(side)
                before=deepcopy(state);original_card=deepcopy(card)
                with patch.object(stream.selection,'page',return_value=([(card['card_id'],card['account_role'])],None)), \
                     patch.object(stream,'CardStore') as cards, \
                     patch.object(stream,'_account_owned',side_effect=AssertionError('DUPLICATE_ENTRY_INVENTORY')), \
                     patch.object(stream,'_maintain_bucket',side_effect=AssertionError('DUPLICATE_PROTECTED_POLL')):
                    cards.return_value.load.return_value=card
                    for _ in range(30):
                        result=self.tick(controller,side)
                        self.assertEqual(result['new_cards_waiting_for_market'],1)
                        self.assertEqual(result['order_requests_sent'],0)
                        self.assertEqual(result['new_cards_registered'],0)
                controller.register.assert_not_called()
                controller.cycle.assert_not_called()
                controller.venue.metadata.assert_not_called()
                self.assertEqual(state,before)
                self.assertEqual(card,original_card)

    def test_already_registered_candidate_in_occupied_market_does_no_second_entry_pass(self):
        state,card,record,controller=self.case()
        state['originals'][card['card_id']]=record
        with patch.object(stream.selection,'page',return_value=([],None)), \
             patch.object(stream,'_account_owned',side_effect=AssertionError('UNNECESSARY_ENTRY_READ')):
            self.tick(controller,'LONG')
        controller.cycle.assert_not_called()
        controller.register.assert_not_called()

    def test_another_coin_still_receives_fresh_ownership_and_entry_checks(self):
        state,card,record,controller=self.case()
        source=deepcopy(card['prepared']['source']);source['symbol']='ETH'
        card=trade_cards.prepare_card(source,{'universe':[{'name':'ETH','szDecimals':2}]},
            rule_id='SOFTWARE_TEST',threshold_pct='1.5',record_kind='received_alert',
            source_expires_at=card['source_expires_at'])
        controller.register.return_value=dict(bucket='f'*64,account=A,bindings=[],pending=None,
            originals={card['card_id']:dict(card=card)})
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        with patch.object(stream.selection,'page',return_value=([(card['card_id'],'long_account')],None)), \
             patch.object(stream,'CardStore') as cards, \
             patch.object(stream,'_account_owned',return_value=True) as owned:
            cards.return_value.load.return_value=card
            result=self.tick(controller,'LONG')
        owned.assert_called_once_with(controller.venue,A,[state],role='long_account')
        controller.register.assert_called_once_with(card['card_id'])
        controller.cycle.assert_called_once_with('f'*64,send=True,allowed_entry_card_id=None)
        self.assertEqual(result['new_cards_registered'],1)

    def test_closed_market_reconsiders_recorded_alert_with_original_source_clock(self):
        state,card,record,controller=self.case()
        state['evidence']['snapshot']=closed(state['bindings'][0])
        before=deepcopy(card)
        def register(cid):
            state['originals'][cid]=record
            return state
        controller.register.side_effect=register
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        with patch.object(stream.selection,'page',return_value=([(card['card_id'],'long_account')],None)), \
             patch.object(stream,'CardStore') as cards, \
             patch.object(stream,'_account_owned',return_value=True) as owned:
            cards.return_value.load.return_value=card
            result=self.tick(controller,'LONG')
        owned.assert_called_once()
        controller.register.assert_called_once_with(card['card_id'])
        controller.cycle.assert_called_once()
        self.assertEqual(result['new_cards_registered'],1)
        self.assertEqual(card,before)

    def test_local_final_flat_candidate_has_one_entry_pass_and_no_exit_prepass(self):
        state,card,record,controller=self.case()
        state['evidence']['snapshot']=closed(state['bindings'][0])
        state['originals'][card['card_id']]=record
        controller.cycle.return_value=dict(status='NO_ACTION_NEEDED',order_requests_sent=0)
        with patch.object(stream.selection,'page',return_value=([],None)), \
             patch.object(stream,'_account_owned',return_value=True) as owned, \
             patch.object(stream,'_maintain_bucket',side_effect=AssertionError('DUPLICATE_EXIT_PREPASS')):
            self.tick(controller,'LONG')
        owned.assert_called_once()
        controller.cycle.assert_called_once_with(state['bucket'],send=True,allowed_entry_card_id=None)

    def test_final_flat_notification_observation_needs_no_price_or_metadata_when_entries_off(self):
        state=state_from_case(q='100',stop='100',take='100')
        state['evidence']['snapshot']=closed(state['bindings'][0])
        store=MemoryStore(state);venue=Venue()
        controller=dispatch.Controller(store,venue,ROUTES2)
        with patch.object(controller,'refresh',return_value=state) as refresh, \
             patch.object(venue,'sample',side_effect=AssertionError('UNNECESSARY_PRICE_READ')), \
             patch.object(venue,'metadata',side_effect=AssertionError('UNNECESSARY_META_READ')):
            result=controller.cycle(state['bucket'],send=True,allow_new_entries=False)
        refresh.assert_called_once()
        self.assertEqual(result,dict(status='NO_ACTION_NEEDED',order_requests_sent=0))

    def test_old_final_idle_history_is_not_polled_but_dirty_feed_restores_recovery(self):
        for clean in (True,False):
            with self.subTest(clean=clean):
                state,_,_,controller=self.case('SHORT')
                state['evidence']['snapshot']=closed(state['bindings'][0])
                controller.venue.now.return_value=T+3*86400000
                controller.venue.fill_wakeups=CleanFeed(clean=clean)
                before=deepcopy(state)
                progress=dict(status='HISTORY_GAP_RECOVERY_PROGRESS',order_requests_sent=0,
                    state=state,cursor_ms=T+86400000,remaining_ms=2*86400000)
                with patch.object(stream.selection,'page',return_value=([],None)), \
                     patch.object(stream,'_recover_history',return_value=progress) as recovery:
                    result=self.tick(controller,'SHORT')
                if clean:
                    recovery.assert_not_called()
                    self.assertEqual(result['status'],'SWEEP_COMPLETE')
                else:
                    recovery.assert_called_once_with(controller,state)
                    self.assertEqual(result['status'],'HISTORY_GAP_RECOVERY_PROGRESS')
                self.assertEqual(state,before)

    def test_staged_history_pending_emergency_and_incomplete_predecessor_postpone_entry(self):
        state,_,_,_=self.case()
        for mode in ('working','history','pending','incident','missing_evidence'):
            with self.subTest(mode=mode):
                current=deepcopy(state)
                if mode=='history':current['history_gap_recovery']={}
                if mode=='pending':current['pending']='unresolved'
                if mode=='incident':current['emergency']=dict(phase='ACTIVE')
                if mode=='missing_evidence':current['evidence']=None
                self.assertTrue(stream._entry_market_blocked(current))

    def test_emergency_omits_idle_final_history_even_with_fresh_recorded_candidate(self):
        for side in ('LONG','SHORT'):
            state,card,record,controller=self.case(side)
            state['evidence']['snapshot']=closed(state['bindings'][0])
            state['originals'][card['card_id']]=record
            before=deepcopy(state)
            self.assertTrue(emergency._idle_flat_checkpoint(state,now_ms=T+20000,
                fill_wakeups=CleanFeed()))
            self.assertEqual(state,before)
            for mode in ('dirty','disconnected','gap','pending','live'):
                with self.subTest(side=side,mode=mode):
                    current=deepcopy(state);feed=CleanFeed()
                    if mode in ('dirty','disconnected'):feed.clean=False
                    elif mode=='gap':current['history_gap_recovery']={}
                    elif mode=='pending':current['pending']='unknown'
                    else:current=state_from_case(q='100',side=side,stop='100',take='100')
                    self.assertFalse(emergency._idle_flat_checkpoint(current,now_ms=T+20000,
                        fill_wakeups=feed))

    def test_enabled_entry_cycle_with_full_coverage_omits_price_and_metadata(self):
        state=state_from_case(q='100',stop='100',take='100')
        _,record=original(2,expiry_seconds=120)
        state['originals'][record['card']['card_id']]=record
        store=MemoryStore(state);venue=Venue()
        controller=dispatch.Controller(store,venue,ROUTES2)
        with patch.object(controller,'refresh',return_value=state) as refresh, \
             patch.object(venue,'sample',side_effect=AssertionError('UNNECESSARY_PRICE_READ')), \
             patch.object(venue,'metadata',side_effect=AssertionError('UNNECESSARY_META_READ')):
            result=controller.cycle(state['bucket'],send=True,allow_new_entries=True)
        refresh.assert_called_once()
        self.assertEqual(result,dict(status='NO_ACTION_NEEDED',order_requests_sent=0))
        self.assertEqual(venue.sent,0)

    def test_missing_coverage_and_working_entry_still_obtain_price_and_metadata(self):
        for kwargs in (dict(q='100',stop=None,take='100'),dict(q='40',stop='40',take='40')):
            with self.subTest(kwargs=kwargs):
                state=state_from_case(**kwargs);store=MemoryStore(state);venue=Venue()
                controller=dispatch.Controller(store,venue,ROUTES2)
                with patch.object(controller,'refresh',return_value=state), \
                     patch.object(venue,'sample',wraps=venue.sample) as sample, \
                     patch.object(venue,'metadata',wraps=venue.metadata) as metadata:
                    controller.cycle(state['bucket'],send=False,allow_new_entries=True)
                sample.assert_called_once();metadata.assert_called_once()
