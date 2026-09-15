"""Read-only lifecycle regression checks; configuration is synthetic."""
from io import StringIO
import json
import os
import unittest
from unittest.mock import patch
from . import app, guarded_execution


class ReviewStartupTests(unittest.TestCase):
    def test_legacy_check_not_replaced_by_review_environment(self):
        fake = {'status': 'fixture', 'order_requests_sent': 0}
        with patch.dict(os.environ, {'HL_TESTNET_REVIEW_SIGNAL': '{}'}), \
                patch.object(app, 'run_check', return_value=fake), patch('sys.stdout', new_callable=StringIO) as out:
            app.startup_check()
        self.assertEqual(json.loads(out.getvalue()), {'testnet_runtime': fake})
    def test_review_task_selected_at_worker_boot_only(self):
        with patch.dict(os.environ, {'HL_TESTNET_REVIEW_SIGNAL': '{}'}), patch.object(app.threading, 'Thread') as thread:
            app.start_read_only_check()
        self.assertIs(thread.call_args.kwargs['target'], app.review_configured_signal)
        thread.return_value.start.assert_called_once()
    def test_default_worker_remains_read_only_check(self):
        with patch.dict(os.environ, {'HL_TESTNET_REVIEW_SIGNAL': ''}), patch.object(app.threading, 'Thread') as thread:
            app.start_read_only_check()
        self.assertIs(thread.call_args.kwargs['target'], app.startup_check)
    def test_review_does_not_pass_secret_to_checker(self):
        with patch.dict(os.environ, {'HL_TESTNET_REVIEW_SIGNAL': '{}', 'HL_TESTNET_RUNTIME_MODE':'read_only',
                                     'HL_TESTNET_AGENT_KEY':'FAKE_SECRET'}), \
                patch.object(guarded_execution, 'review_only', return_value={'order_requests_sent':0}) as review, \
                patch('sys.stdout', new_callable=StringIO) as out:
            app.review_configured_signal()
        self.assertNotIn('FAKE_SECRET', str(review.call_args))
        self.assertNotIn('FAKE_SECRET', out.getvalue())
    def test_invalid_mode_never_dispatches_or_queries(self):
        with patch.dict(os.environ, {'HL_TESTNET_REVIEW_SIGNAL': '{}', 'HL_TESTNET_RUNTIME_MODE':'send'}), \
                patch.object(guarded_execution, 'review_only') as review, \
                patch('sys.stdout', new_callable=StringIO) as out:
            app.review_configured_signal()
        review.assert_not_called()
        self.assertEqual(json.loads(out.getvalue())['testnet_submission_review']['order_requests_sent'], 0)
