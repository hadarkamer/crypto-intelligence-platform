"""Worker diagnostics only: no threads, credentials, network or real venue."""
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch
from . import filled_trial_runtime as m
from . import test_filled_quantity_dispatch as fixture


class WorkerDiagnosticTests(fixture.NoExternal):
    def test_exception_after_attempt_does_not_report_zero_or_expose_error(self):
        controller=SimpleNamespace(venue=SimpleNamespace(sent=3))
        stop=Mock();stop.is_set.return_value=False;stop.wait.return_value=True
        def fail(*args,**kwargs):
            controller.venue.sent+=1
            raise ValueError('FAKE_SECRET_MUST_NOT_APPEAR')
        output=io.StringIO()
        with patch.object(m,'_stop',stop),patch.object(m,'tick',side_effect=fail),\
             patch('sys.stdout',output),patch.dict(m._health,dict(m._health)):
            m._loop(controller,'a'*64)
        report=json.loads(output.getvalue())['testnet_controlled_card']
        self.assertEqual(report['order_requests_sent'],1)
        self.assertEqual(report['status'],'RECONCILIATION_REQUIRED_NO_BLIND_RETRY')
        self.assertNotIn('FAKE_SECRET',output.getvalue())

    def test_verified_end_stops_without_waiting_or_starting_another_card(self):
        controller=SimpleNamespace(venue=SimpleNamespace(sent=0))
        stop=Mock();stop.is_set.return_value=False
        result=dict(status='NO_ACTION_NEEDED',finished=True,order_requests_sent=0,app_controls=False)
        with patch.object(m,'_stop',stop),patch.object(m,'tick',return_value=result) as tick,\
             patch('sys.stdout',io.StringIO()),patch.dict(m._health,dict(m._health)):
            m._loop(controller,'a'*64)
            tick.assert_called_once_with(controller,'a'*64,send=True)
        stop.wait.assert_not_called()
