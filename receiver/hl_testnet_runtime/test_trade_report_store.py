"""Authenticated private projection of the existing Testnet journal."""
import json
import unittest
from unittest.mock import patch

from . import app


class PrivateReportTests(unittest.TestCase):
    def test_root_health_uses_current_runtime_entry_status(self):
        for running, enabled in ((True, True), (True, False), (False, True)):
            with self.subTest(running=running, enabled=enabled), \
                 patch.dict('os.environ', {'HL_TESTNET_RUNTIME_MODE':'experimental_mainnet_v1'}), \
                 patch('hl_testnet_runtime.experimental_live_startup.health', return_value={
                     'running':running, 'new_entries_enabled':enabled}):
                body = b''.join(app.application({'REQUEST_METHOD':'GET', 'PATH_INFO':'/'}, lambda *a:None))
                self.assertEqual(json.loads(body)['continuous_trading'], running and enabled)

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


class CurrentJournalReportTests(unittest.TestCase):
    def setUp(self):
        from .test_execution_history_lifecycle import ExecutionHistoryLifecycleTests
        self.f = ExecutionHistoryLifecycleTests()
        self.f.setUp(); self.addCleanup(self.f.doCleanups)

    def test_unfilled_limit_is_not_reported_as_an_actual_trade(self):
        from .trade_report_store import _execution_trades
        self.f.start()
        self.assertEqual(_execution_trades(self.f.store), [])

    def test_owned_fills_and_both_working_exits_are_reported(self):
        from .trade_report_store import _execution_trades, _report_row
        value, oid = self.f.start(); self.f.protect(value, oid, '1')
        before = self.f.store.load()
        row, = _execution_trades(self.f.store)
        self.assertEqual(row['entry_quantity'], '1')
        self.assertEqual(row['actual_entry_price'], value['entry'])
        self.assertEqual(row['planned_prices'], {k: value[k] for k in ('entry','stop','take_profit')})
        self.assertTrue(row['protection_verified']); self.assertFalse(row['closure_verified'])
        card = self.f.store.trade_card(value['occurrence_id'])['card']
        card['protections'] = card['protections'][:1]
        self.assertFalse(_report_row(card)['protection_verified'])
        self.assertEqual(self.f.store.load(), before)

    def test_closed_trade_survives_archive_and_restart_exactly_once(self):
        from .trade_report_store import _execution_trades
        self.f.close(); expected = _execution_trades(self.f.store)
        self.assertTrue(expected[0]['closure_verified'])
        self.f.compact(); self.f.restart()
        self.assertEqual(_execution_trades(self.f.store), expected)
        self.assertEqual(len(expected), 1)

    def test_private_route_combines_old_history_and_current_journal_without_duplicates(self):
        from . import trade_report_store as report
        from .test_experimental_live_provider import ENV
        value, oid = self.f.start(); self.f.protect(value, oid, '1')
        current, = report._execution_trades(self.f.store)
        older = {**current, 'card_id': 'f'*64}
        env = {**ENV, 'HL_TESTNET_RUNTIME_MODE': 'experimental_mainnet_v1'}
        with patch.object(report.PostgresJournal, 'from_env'), patch.object(report, 'DispatchStore'), \
             patch('hl_testnet_runtime.experimental_live_state.TestnetExecutionState', return_value=self.f.store), \
             patch.object(report, 'observed_trades', side_effect=lambda *a, role, **kw: [older,current] if role=='long_account' else []):
            actual = report.load_trades(env)
        self.assertEqual(len(actual['L']), 2); self.assertEqual(actual['S'], [])
        self.assertEqual({r['card_id'] for r in actual['L']}, {older['card_id'], value['occurrence_id']})

    def test_manual_management_never_claims_bot_exit_coverage(self):
        from .trade_report_store import _report_row
        value, oid = self.f.start(); self.f.protect(value, oid, '1')
        card = self.f.store.trade_card(value['occurrence_id'])['card']
        card['manual_management'] = {'status':'MANUAL_MANAGEMENT'}
        card['summary'].update(remaining_quantity=None, actual_position_quantity='-2', historical_bot_remaining_quantity='1')
        row = _report_row(card)
        self.assertEqual(row['remaining_quantity'], '2')
        self.assertIn('MANUAL_MANAGEMENT', row['issues'])
        self.assertFalse(row['protection_verified'])
