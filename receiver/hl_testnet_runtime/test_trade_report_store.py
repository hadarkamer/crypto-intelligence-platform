"""Authenticated private projection of the existing Testnet journal."""
import json
import unittest
from unittest.mock import patch

from . import app


class PrivateReportTests(unittest.TestCase):
    def test_report_endpoint_requires_token_and_get(self):
        responses=[]
        def request(token, method='GET', query=''):
            responses.clear()
            body=b''.join(app.application({'REQUEST_METHOD':method,
                'PATH_INFO':'/internal/testnet-trades/v1','QUERY_STRING':query,
                'HTTP_X_TRADE_REPORT_TOKEN':token},
                lambda status,headers:responses.append((status,headers))))
            return responses[0][0],body
        with patch.dict('os.environ',{'HL_TESTNET_REPORT_API_TOKEN':'x'*40}), \
             patch('hl_testnet_runtime.trade_report_store.load_trades',
                   return_value={'L':[],'S':[]}) as read:
            self.assertEqual(request('bad')[0],'404 Not Found')
            self.assertEqual(request('x'*40,'POST')[0],'404 Not Found')
            self.assertEqual(request('x'*40,query='all=1')[0],'404 Not Found')
            read.assert_not_called()
            status,body=request('x'*40)
            self.assertEqual(status,'200 OK')
            self.assertEqual(json.loads(body),{'L':[],'S':[]})
            read.assert_called_once_with()


if __name__=='__main__':
    unittest.main()
