"""Read-only reporting during real concurrent refreshes; no network or signing."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import threading
from unittest.mock import patch

from . import filled_quantity_dispatch as dispatch, long_stream_runtime as stream
from . import simple_execution as simple
from .test_card_lifecycle import T
from .test_filled_quantity_dispatch import NoExternal, ROUTES2, Venue, state_from_case
from .test_history_gap_recovery import MemoryStore
from .test_simple_execution import Feed


class VerificationReportingTests(NoExternal):
    def controller(self, side='LONG', **kw):
        state=state_from_case(q='100',side=side,stop='100',take='100',
                              expiry_seconds=600,**kw)
        simple.checkpoint(state,{'verification_passes':1},T)
        venue=Venue();venue.simple_execution=True;venue.fill_wakeups=Feed()
        venue.t=T+180001
        controller=dispatch.Controller(MemoryStore(state),venue,ROUTES2)
        role='long_account' if side=='LONG' else 'short_account'
        return controller,state,role

    def report(self, controller, role):
        return stream.observed_trades(controller,ROUTES2[role],role=role)[0]

    def test_real_refresh_reports_pending_then_emits_recovery_for_both_accounts(self):
        for side in ('LONG','SHORT'):
            with self.subTest(side=side):
                controller,state,role=self.controller(side)
                venue=controller.venue;reported={}
                venue.t=T+1
                first=self.report(controller,role)
                self.assertTrue(first['protection_verified'])
                self.assertTrue(stream._trade_report_changed(reported,first))
                venue.t=T+180001
                entered=threading.Event();release=threading.Event()
                def refresh(bucket, previous):
                    entered.set()
                    if not release.wait(2):raise AssertionError('REFRESH_NOT_RELEASED')
                    def update(conn,current):
                        current['evidence']['snapshot']['at_ms']=venue.now()
                        simple.checkpoint(current,{'verification_passes':2},venue.now())
                    return controller.store.change(bucket,previous['revision'],
                        'PUBLIC_RECONCILIATION',venue.now(),update)
                with patch.object(controller,'_refresh_once',side_effect=refresh) as collect:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        job=pool.submit(controller.refresh,state['bucket'])
                        try:
                            self.assertTrue(entered.wait(2))
                            pending=self.report(controller,role)
                            self.assertEqual(pending['verification_status'],'IN_PROGRESS')
                            self.assertFalse(pending['protection_verified'])
                            self.assertTrue(pending['last_known_protection_verified'])
                            self.assertEqual(pending['evidence_at_ms'],T)
                            self.assertFalse(pending['issues'])
                            self.assertTrue(stream._trade_report_changed(reported,pending))
                            self.assertFalse(stream._trade_report_changed(reported,pending))
                        finally:
                            release.set()
                        job.result(timeout=2)
                    collect.assert_called_once()
                recovered=self.report(controller,role)
                self.assertTrue(recovered['protection_verified'])
                self.assertTrue(stream._trade_report_changed(reported,recovered))
                self.assertFalse(stream._trade_report_changed(reported,recovered))
                self.assertEqual(venue.sent,0)

    def test_stalled_refresh_cannot_renew_original_audit_deadline(self):
        controller,state,role=self.controller()
        flight=dispatch._ObservationFlight(state['revision'],None,False,
            state=state,now_ms=controller.venue.now())
        controller._observation_flights[state['bucket']]=[flight]
        before=deepcopy(controller.store.load(state['bucket']))
        for elapsed in (15000,17000,60000):
            controller.venue.t=T+180001+elapsed
            # Simulate a replacement reader: its new start cannot reset due_ms.
            if elapsed>=17000:
                flight.started_at_ms=controller.venue.now()
            row=self.report(controller,role)
            self.assertEqual(row['verification_status'],'OVERDUE')
            self.assertIn('STALE_OR_FUTURE_SNAPSHOT',row['issues'])
            self.assertFalse(row['protection_verified'])
        self.assertEqual(controller.store.load(state['bucket']),before)

    def test_monotonic_timeout_survives_wall_clock_stall(self):
        controller,state,role=self.controller()
        flight=dispatch._ObservationFlight(state['revision'],None,False,
            state=state,now_ms=controller.venue.now())
        controller._observation_flights[state['bucket']]=[flight]
        with patch.object(dispatch.time,'monotonic',return_value=flight.started+15):
            self.assertEqual(self.report(controller,role)['verification_status'],'OVERDUE')

    def test_missing_exit_pending_failure_wrong_revision_and_other_market_stay_visible(self):
        for change in ('missing_stop','pending','failed','revision','other_market','future'):
            with self.subTest(change=change):
                controller,state,role=self.controller()
                if change=='missing_stop':
                    state['evidence']['snapshot']['open_orders'][0]['quantity']='99'
                elif change=='pending':state['pending']='a'*64
                elif change=='future':state['evidence']['snapshot']['at_ms']=controller.venue.now()+1
                controller.store.values[state['bucket']]=deepcopy(state)
                flight=dispatch._ObservationFlight(state['revision'],None,False,
                    state=state,now_ms=controller.venue.now())
                if change=='failed':flight.error=ValueError('FAILED')
                elif change=='revision':flight.revision+=1
                bucket='other' if change=='other_market' else state['bucket']
                controller._observation_flights[bucket]=[flight]
                row=self.report(controller,role)
                self.assertNotEqual(row['verification_status'],'IN_PROGRESS')
                self.assertTrue(row['issues'])
                self.assertFalse(row['protection_verified'])

    def test_oldest_overlapping_reader_bounds_reporting(self):
        controller,state,role=self.controller()
        old=dispatch._ObservationFlight(state['revision'],None,False,
            state=state,now_ms=controller.venue.now()-15000)
        new=dispatch._ObservationFlight(state['revision'],None,True,
            state=state,now_ms=controller.venue.now())
        controller._observation_flights[state['bucket']]=[old,new]
        self.assertEqual(self.report(controller,role)['verification_status'],'OVERDUE')

    def test_report_reloads_a_peer_commit_without_requesting_exchange_data(self):
        controller,previous,role=self.controller()
        def update(conn,current):
            current['evidence']['snapshot']['at_ms']=controller.venue.now()
            simple.checkpoint(current,{'verification_passes':2},controller.venue.now())
        controller.store.change(previous['bucket'],previous['revision'],
            'PUBLIC_RECONCILIATION',controller.venue.now(),update)
        with patch.object(controller.store,'for_account',return_value=[previous]), \
                patch.object(controller,'refresh',side_effect=AssertionError('REPORT_MUST_NOT_FETCH')):
            row=self.report(controller,role)
        self.assertTrue(row['protection_verified'])
        self.assertEqual(row['evidence_at_ms'],controller.venue.now())

    def test_repeated_failure_recovery_is_emitted_and_account_identity_is_separate(self):
        reports={}
        row=dict(card_id='same',account_role='long_account',state='OPEN',
                 protection_verified=True,closure_verified=False,issues=[],
                 verification_status='VERIFIED')
        for _ in range(3):
            self.assertTrue(stream._trade_report_changed(reports,row))
            self.assertFalse(stream._trade_report_changed(reports,row))
            failed={**row,'protection_verified':False,'issues':['STALE_OR_FUTURE_SNAPSHOT'],
                    'verification_status':'OVERDUE'}
            self.assertTrue(stream._trade_report_changed(reports,failed))
        self.assertTrue(stream._trade_report_changed(reports,{**row,'account_role':'short_account'}))

    def test_historical_report_requires_no_live_venue_or_clock(self):
        controller,state,role=self.controller()
        controller.venue=object()
        rows=stream.observed_trades(controller,ROUTES2[role],role=role,historical=True)
        self.assertEqual(rows[0]['verification_status'],'HISTORICAL')

