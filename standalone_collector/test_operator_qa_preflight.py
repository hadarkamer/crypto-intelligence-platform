"""Check sanitization and the exact configured QA Docker entry without any provider."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import operator_qa_preflight as qa


class QAPreflightTests(unittest.TestCase):
    def test_networked_policy_allows_only_neutral_fixture_origin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = root/'operator_chromium_policy.json'
            policy.write_text(json.dumps({'BrowserSignin':0,'URLAllowlist':['https://www.coinglass.com']}))
            with patch.object(qa,'HERE',root), patch.object(qa,'POLICY',root/'etc'/'qa.json'):
                qa.prepare_offline_policy()
                installed = json.loads(qa.POLICY.read_text())
                self.assertEqual(json.loads(policy.read_text()),installed)
                self.assertEqual(installed['URLAllowlist'],['about:blank','https://qa-source.invalid'])
                self.assertEqual(installed['URLBlocklist'],['*'])
                self.assertEqual(installed['ProxyServer'],'http://127.0.0.1:31497')
                self.assertEqual(installed['DnsOverHttpsMode'],'off')
                self.assertFalse(installed['QuicAllowed'])

    def test_status_never_exposes_raw_fixtures_paths_or_unrecognized_values(self):
        hostile = {'status':'some-token','reason':'http://private.invalid/',
                   'ordinary_chromium_sandbox':'unexpected string','debug':'arbitrary'}
        self.assertEqual(qa.checked_result(hostile,'dual_uid'),{'status':'check_failed'})
        self.assertEqual(qa.checked_result(hostile,'private_viewer'),
                         {'reason':'check_failed','isolated_operator_browser':False})

    def test_independent_capabilities_survive_partial_success(self):
        dual = {'status':'chromium_namespace_sandbox_unavailable','two_uid_confidential_boundary':True,
                'ordinary_chromium_sandbox':False,'provider_requests':0}
        self.assertEqual(qa.checked_result(dual,'dual_uid'),dual)
        complete = {'reason':'ready','isolated_operator_browser':True,'provider_requests':0}
        self.assertEqual(qa.checked_result(complete,'private_viewer'),complete)
