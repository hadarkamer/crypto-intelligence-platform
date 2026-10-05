"""No network or order authority in the explicit recovery startup."""
from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
import unittest
from unittest.mock import Mock,patch

from . import startup_recovery as recovery,emergency_close as emergency
from .filled_dispatch_store import DispatchError
from .test_emergency_close import ContinuousReleaseTests
from .test_filled_quantity_dispatch import ROUTES2


class StartupRecoveryTests(unittest.TestCase):
    def normal(self):
        normal=Mock()
        normal.venue.env={**ContinuousReleaseTests.environment(),
            'HL_TESTNET_LONG_ENTRY_ENABLED':'false','HL_TESTNET_SHORT_ENTRY_ENABLED':'false'}
        normal.routes=deepcopy(ROUTES2)
        normal.store.for_account.return_value=[]
        return normal

    def test_disabled_mode_has_no_release_and_only_local_inventory(self):
        normal=self.normal()
        with patch.object(emergency,'Controller') as ctor,redirect_stdout(StringIO()):
            recovery.run(normal)
        ctor.assert_not_called()
        normal.venue.send.assert_not_called()

    def test_wrong_mode_or_either_enabled_account_fails_before_scan(self):
        for key,value in ((recovery.KEY,'wrong'),('HL_TESTNET_LONG_ENTRY_ENABLED','true'),
                          ('HL_TESTNET_SHORT_ENTRY_ENABLED','true')):
            normal=self.normal();normal.venue.env[recovery.KEY]=recovery.APPROVAL
            normal.venue.env[key]=value
            with self.assertRaisesRegex(DispatchError,'DISABLED_TESTNET_ENTRIES'):
                recovery.run(normal)
            normal.store.for_account.assert_not_called()

    def test_only_exact_closed_incident_calls_existing_independent_release_proof(self):
        normal=self.normal();normal.venue.env[recovery.KEY]=recovery.APPROVAL
        states=[dict(symbol='DOGE',account=ROUTES2['long_account']['account'],
            bucket='a'*64,bindings=[],pending=None,emergency=dict(phase='CLOSED_VERIFIED')),
            dict(symbol='BTC',bindings=[],pending=None,emergency=dict(phase='ACTIVE'))]
        normal.store.for_account.side_effect=[states,[]]
        before=deepcopy(normal.venue.env)
        with patch.object(emergency,'Controller') as ctor, \
                patch.object(emergency,'incident_identity',return_value='b'*64), \
                redirect_stdout(StringIO()) as output:
            ctor.return_value.release_closed_incident.return_value={'status':'CLOSED_INCIDENT_RELEASED'}
            recovery.run(normal)
        ctor.return_value.release_closed_incident.assert_called_once_with('a'*64,
            account=states[0]['account'],incident_id='b'*64)
        self.assertIn('ACTIVE_INCIDENT_RETAINED',output.getvalue())
        self.assertEqual(normal.venue.env,before)
        normal.venue.send.assert_not_called()

    def test_release_failure_retains_disabled_entries_and_redacts_details(self):
        normal=self.normal();normal.venue.env[recovery.KEY]=recovery.APPROVAL
        normal.store.for_account.side_effect=[[dict(symbol='DOGE',account=ROUTES2['long_account']['account'],
            bucket='a'*64,bindings=[],pending=None,emergency=dict(phase='CLOSED_VERIFIED'))],[]]
        with patch.object(emergency,'Controller') as ctor, \
                patch.object(emergency,'incident_identity',return_value='b'*64), \
                redirect_stdout(StringIO()) as output:
            ctor.return_value.release_closed_incident.side_effect=ValueError('PRIVATE_KEY_NEVER_LOG')
            recovery.run(normal)
        self.assertIn('INCIDENT_RETAINED_ENTRIES_DISABLED',output.getvalue())
        self.assertNotIn('PRIVATE_KEY',output.getvalue())
        self.assertEqual(normal.venue.env['HL_TESTNET_LONG_ENTRY_ENABLED'],'false')
        self.assertEqual(normal.venue.env['HL_TESTNET_SHORT_ENTRY_ENABLED'],'false')
