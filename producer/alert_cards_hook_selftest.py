"""The optional record-copy hook must not interrupt ordinary alert delivery."""
import io
import unittest
from unittest.mock import patch
import alert_cards_forwarder as forwarder
import manual_formula_alert_delivery as delivery


class HookTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_scope_starts_side_task_without_reinitializing(self):
        with patch.object(delivery,'_READY',True),patch.object(delivery,'_SCOPES',{123}),patch.object(forwarder,'maybe_start') as start,patch.object(delivery.store,'initialize_scope',side_effect=AssertionError('No repeated source initialization')):
            self.assertTrue(await delivery.initialize(123))
            start.assert_called_once_with(123)

    async def test_hook_failure_cannot_fail_alert_initialization(self):
        with patch.object(delivery,'_READY',True),patch.object(delivery,'_SCOPES',{123}),patch.object(forwarder,'maybe_start',side_effect=RuntimeError('PRIVATE_TEST_VALUE')),patch('sys.stdout',new_callable=io.StringIO) as output:
            self.assertTrue(await delivery.initialize(123))
            self.assertNotIn('PRIVATE_TEST_VALUE',output.getvalue())
            self.assertIn('FORWARD_HOOK_UNAVAILABLE',output.getvalue())

    async def test_failed_source_initialization_cannot_register_scope(self):
        with patch.object(delivery,'_READY',False),patch.object(delivery,'_RETRY_AFTER',0),patch.object(delivery.store,'schema_ready',return_value=False),patch.object(forwarder,'maybe_start') as start,patch('sys.stdout',new_callable=io.StringIO):
            self.assertFalse(await delivery.initialize(123))
            start.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
