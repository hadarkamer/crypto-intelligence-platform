"""Three already-existing formulas through approved entry and owned exits."""
from copy import deepcopy
import unittest

from approved_alert_fixtures import formula_alert
from . import experimental_live_runtime as live
from . import test_experimental_live_runtime as support
from experimental_execution_fixtures import BASE as DECISION
T = DECISION + 120000
from . import sol_g65_conditional_stop as g65
from .test_sol_g65_conditional_stop import bar
from . import experimental_execution_dispatch as boundary
from .test_experimental_execution_runtime import META


class FormulaExecutionTests(unittest.TestCase):
    real_pg = False
    setUp = support.RuntimeContract.setUp
    make_worker = support.RuntimeContract.make_worker
    cycle = support.RuntimeContract.cycle

    def test_all_three_enter_once_as_gtc_then_protect_and_close(self):
        for family in ('r2732','hype_row71205','sol_g65'):
            with self.subTest(family=family):
                stamp = T if family == 'sol_g65' else T-60000
                self.venue.__init__(stamp+1000)
                self.make_worker(not_before=stamp-180000)
                msg=formula_alert(family,approved_ms=stamp); cid=msg['occurrence_id']
                account=self.store.load()['routes']['short_account']
                self.venue.mark[self.venue._key(account,msg['symbol'])]=msg['entry']
                self.worker.receive([msg])
                self.assertEqual(self.cycle()['operation'],'ENTRY')
                req=self.venue.requests[0]; order=req['proposal']['action']['orders'][0]
                self.assertEqual(order['t'],{'limit':{'tif':'Gtc'}})
                boundary._terms(req,msg,META)
                self.assertFalse(self.store.load()['trades'][cid]['entry_fills'])
                self.worker.receive([msg]); self.cycle()
                self.assertEqual(len(self.venue.requests),1)
                oid=self.venue.oid('ENTRY'); self.venue.fill(oid,order['s'])
                self.assertEqual(self.cycle(False)['operation'],'CREATE_EXIT')
                self.assertEqual(self.cycle(False)['operation'],'CREATE_EXIT')
                self.cycle(False)
                self.assertEqual(self.store.load()['trades'][cid]['source'],msg)
                self.venue.fill(self.venue.oid('TAKE_PROFIT'),order['s'])
                self.assertEqual(self.cycle(False)['operation'],'CANCEL')
                self.cycle(False); self.cycle(False)
                self.assertEqual(self.store.load()['trades'][cid]['phase'],'CLOSED')

    def test_approved_g65_keeps_fill_bar_close_then_later_low_rule(self):
        msg=formula_alert('sol_g65',approved_ms=T)
        state=g65.initialize_from_contract(msg['occurrence_id'],msg)
        at=T-60000
        self.assertEqual(state['entry_at_ms'],at)
        state=g65.advance(state,[bar(at,open='100.6',high='100.7',low='98.8',close='100')],now_ms=T)
        self.assertIsNone(state['lock_effective_at_ms'])
        state=g65.advance(g65.validate(deepcopy(state)),
            [bar(T,open='100',high='100.1',low='98.8',close='100')],now_ms=T+60000)
        self.assertEqual(state['lock_effective_at_ms'],T+60000)
        self.assertIsNone(state['outcome'])

    def test_approved_g65_original_exits_precede_lock_on_fill_minute(self):
        msg=formula_alert('sol_g65',approved_ms=T)
        state=g65.initialize_from_contract(msg['occurrence_id'],msg)
        state=g65.advance(state,[bar(T-60000,open='100.6',high='101.3',low='98.8',close='98.8')],now_ms=T)
        self.assertEqual(state['outcome']['reason'],'INITIAL_STOP')
        self.assertIsNone(state['lock_effective_at_ms'])
