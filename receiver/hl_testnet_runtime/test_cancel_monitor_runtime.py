"""Runtime routing and public-interface checks; no real network or database."""
import json
import os
import unittest
from unittest.mock import patch, Mock
from . import pending_cancel_monitor as monitor, gunicorn_conf as conf, app
from .test_pending_cancel_monitor import env


class RuntimeTests(unittest.TestCase):
    def test_boot_selects_monitor_not_entry_sender(self):
        with patch.dict(os.environ,env(),clear=True), patch.object(monitor,'start') as start, \
             patch.object(app,'start_read_only_check') as old:
            conf.post_worker_init(None)
        start.assert_called_once();old.assert_not_called()
    def test_graceful_worker_exit_stops_loop(self):
        with patch.object(monitor,'stop') as stop:conf.worker_exit(None,None)
        stop.assert_called_once()
    def test_http_reports_cancellation_mode_without_triggering_work(self):
        with patch.dict(os.environ,env(),clear=True), patch.object(monitor,'start') as start, \
             patch.object(monitor,'health',return_value={'monitor_running':True}):
            status=Mock()
            body=b''.join(app.application({'REQUEST_METHOD':'GET','PATH_INFO':'/healthz'},status))
        report=json.loads(body)
        self.assertTrue(report['cancellation_monitor_configured'])
        self.assertFalse(report['read_only']);self.assertFalse(report['continuous_trading'])
        self.assertFalse(report['public_order_controls']);start.assert_not_called()
    def test_post_cannot_cancel_or_activate_monitor(self):
        with patch.dict(os.environ,env(),clear=True),patch.object(monitor,'start') as start:
            status=Mock()
            app.application({'REQUEST_METHOD':'POST','PATH_INFO':'/'},status)
        self.assertEqual(status.call_args.args[0],'405 Method Not Allowed');start.assert_not_called()


if __name__=='__main__':unittest.main()
