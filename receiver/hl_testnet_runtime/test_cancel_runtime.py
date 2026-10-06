"""No real threads, network, keys or database calls during startup routing tests."""
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from . import gunicorn_conf as conf, half_threshold_cancel as policy
from . import pending_cancel_executor as execution


class RoutingTests(unittest.TestCase):
    def test_review_startup_never_invokes_cancel_sender(self):
        env={'HL_TESTNET_RUNTIME_MODE':'read_only','HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1',
             policy.REVIEW_ENV:'review only'}
        with patch.dict(os.environ,env,clear=True),patch('threading.Thread') as thread, \
             patch.object(execution,'cancel_registered_once',side_effect=AssertionError('Never send')) as send:
            conf.post_worker_init(None)
        self.assertIs(thread.call_args.kwargs['target'],policy.startup_review)
        send.assert_not_called()
    def test_legacy_storage_still_selected_without_new_config(self):
        fn=Mock()
        with patch.dict(os.environ,{'HL_TESTNET_RUNTIME_MODE':'read_only',
                        'HL_TESTNET_JOURNAL_BACKEND':'staging_postgres_v1'},clear=True), \
             patch.dict(sys.modules,{'hl_testnet_runtime.persistent_execution':SimpleNamespace(startup_storage_check=fn)}), \
             patch('threading.Thread') as thread:
            conf.post_worker_init(None)
        self.assertIs(thread.call_args.kwargs['target'],fn)
    def test_entry_test_inspection_routing_is_not_replaced(self):
        fn=Mock()
        with patch.dict(os.environ,{'HL_TESTNET_RUNTIME_MODE':'inspect_testnet_attempt_v1',
                                   policy.REVIEW_ENV:'present'},clear=True), \
             patch.dict(sys.modules,{'hl_testnet_runtime.controlled_attempt':SimpleNamespace(startup_single_attempt=fn)}), \
             patch('threading.Thread') as thread:
            conf.post_worker_init(None)
        self.assertIs(thread.call_args.kwargs['target'],fn)


if __name__=='__main__':unittest.main()
