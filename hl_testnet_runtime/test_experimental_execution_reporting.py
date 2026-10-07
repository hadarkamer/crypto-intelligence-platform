from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from . import experimental_execution_reporting as report
from . import experimental_execution_runtime as runtime
from .experimental_execution_state import ExecutionState
from .test_experimental_execution_runtime import SoftwareExchange, ROUTES, T
from experimental_execution_fixtures import r2732_message


class ReportingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.store = ExecutionState(Path(self.temp.name)/'report.isolated-experimental.sqlite3')
        self.store.initialize(ROUTES, not_before_ms=T-60000)
        self.venue = SoftwareExchange(T+10000)
        self.worker = runtime.IsolatedExecutionRuntime(self.store, self.venue, mode=runtime.MODE)
        self.message = r2732_message(entry=2.3, decision_ms=T-60000)
        self.worker.receive([self.message]); self.worker.run_once()
        for _ in range(3): self.worker.run_once()

    def test_open_trade_has_observed_average_and_unknown_final_profit(self):
        state = self.store.load(); original = deepcopy(state)
        result = report.project(state); row = result['trades'][0]
        self.assertEqual(state, original)
        self.assertEqual(row['planned_source_prices']['entry'], self.message['entry'])
        self.assertEqual(float(row['actual_average_entry']), 2.3)
        self.assertIsNone(row['actual_average_exit'])
        self.assertIsNone(row['gross_pnl_before_costs'])
        self.assertIsNone(row['net_pnl'])
        self.assertEqual(result['counts']['open'], 1)

    def test_final_actual_fill_prices_determine_gross_not_source_take(self):
        state = self.store.load(); trade = state['trades'][self.message['occurrence_id']]
        # The simulated fill helper records actual order price; mutate the
        # actual fill AFTER reconciliation solely to isolate report arithmetic.
        self.venue.fill(self.venue.oid('TAKE_PROFIT'), trade['quantity'])
        for _ in range(3): self.worker.run_once()
        state = self.store.load(); closed = state['trades'][self.message['occurrence_id']]
        for fill in closed['exit_fills'].values(): fill['price'] = '2.1'
        row = report.project(state)['trades'][0]
        self.assertEqual(row['status'], 'CLOSED')
        self.assertEqual(row['actual_average_exit'], '2.1')
        self.assertAlmostEqual(float(row['gross_pnl_before_costs']), .2*float(trade['quantity']))
        self.assertNotEqual(row['actual_average_exit'], row['planned_source_prices']['take_profit'])
        self.assertIsNone(row['net_pnl'])
        self.assertEqual(float(row['remaining_quantity']), 0)

    def test_incomplete_close_is_never_reported_as_final(self):
        state = self.store.load()
        state['trades'][self.message['occurrence_id']]['phase'] = 'CLOSED'
        with self.assertRaisesRegex(report.ReportError, 'FINAL_RECONCILIATION'):
            report.project(state)

    def test_project_cannot_label_simulation_real(self):
        state = self.store.load(); state['domain'] = 'testnet'
        with self.assertRaisesRegex(report.ReportError, 'ISOLATED_STATE'):
            report.project(state)


if __name__ == '__main__': unittest.main()
