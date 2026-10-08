"""No exchange IO: owned-fill allocations and adversarial pooled-exit races."""
from copy import deepcopy
import json
import unittest
from . import experimental_allocations as allocations

ACCOUNT='0x'+'1'*40
CID='1'*64
PEER='2'*64


def binding(cid=CID, prefix=0, quantity='1'):
    return dict(occurrence_id=cid, account=ACCOUNT, account_role='long_account', symbol='HYPE',
        side='LONG', orders=[dict(order_id=str(prefix+i),kind=kind,quantity=quantity,reduce_only=i!=1)
            for i,kind in ((1,'ENTRY'),(2,'STOP'),(3,'TAKE_PROFIT'))])


def snapshot(bindings=None, at=1000):
    bindings=bindings or [binding()]
    orders=[];fills=[]
    for row in bindings:
        for order in row['orders']:
            entry=order['kind']=='ENTRY'
            orders.append(dict(order_id=order['order_id'],status='FILLED' if entry else 'OPEN',
                quantity=order['quantity'],filled_quantity=order['quantity'] if entry else '0',
                reduce_only=order['reduce_only'],side='BUY' if entry else 'SELL'))
            if entry:
                fills.append(dict(fill_id='f'+order['order_id'],order_id=order['order_id'],
                                  quantity=order['quantity'],price='98',at_ms=900))
    return dict(environment='testnet',account=ACCOUNT,account_role='long_account',symbol='HYPE',
        at_ms=at,history_complete=True,orders_complete=True,position_complete=True,
        position_quantity=str(len(bindings)),orders=orders,fills=fills)


class AllocationTests(unittest.TestCase):
    def check(self, bindings, sample, previous=None):
        return allocations.assess(bindings,sample,now_ms=sample['at_ms'],previous=previous)

    def test_single_and_multiple_owned_quantities_are_exact_without_dispatch(self):
        for bindings in ([binding()],[binding(),binding(PEER,10)]):
            result=self.check(bindings,snapshot(bindings))
            self.assertEqual(result['status'],'RECONCILED')
            for row in result['allocations'].values():
                self.assertEqual(row['remaining'],'1');self.assertTrue(row['quantity_coverage_observed'])
            self.assertFalse(result['shared_market_dispatch_enabled'])
            self.assertFalse(result['dispatch_enabled'])

    def test_racing_oco_exits_consume_peer_despite_reduce_only_and_are_quarantined(self):
        """Netted size 2 permits both of card A's size-1 exits to reduce fully.

        Exchange ends at zero although card B has no exit. Exact OIDs detect
        A exiting 2 from its own 1, but cannot prevent venue consumption. This
        is the concrete reason shared-market dispatch must remain disabled.
        """
        bindings=[binding(),binding(PEER,10)]
        sample=snapshot(bindings)
        baseline=self.check(bindings,sample)
        self.assertEqual(baseline['status'],'RECONCILED')
        for oid in ('2','3'):
            row=next(r for r in sample['orders'] if r['order_id']==oid)
            self.assertTrue(row['reduce_only'])
            row.update(status='FILLED',filled_quantity='1')
            sample['fills'].append(dict(fill_id='race'+oid,order_id=oid,quantity='1',price='98',at_ms=1100))
        sample.update(at_ms=1200,position_quantity='0')
        result=self.check(bindings,sample,baseline)
        self.assertEqual(result['diagnostics'],['EXIT_CONSUMED_ANOTHER_ALLOCATION'])
        self.assertEqual(result['status'],'QUARANTINED')
        self.assertFalse(result['shared_market_dispatch_enabled'])

    def test_own_partial_exit_leaves_peer_quantity_untouched(self):
        bindings=[binding(),binding(PEER,10)];sample=snapshot(bindings)
        sample['orders'][1]['filled_quantity']='.4'
        sample['fills'].append(dict(fill_id='partial',order_id='2',quantity='.4',price='95',at_ms=950))
        sample['position_quantity']='1.6'
        result=self.check(bindings,sample)
        self.assertEqual(result['status'],'RECONCILED')
        self.assertEqual(result['allocations'][CID]['remaining'],'0.6')
        self.assertEqual(result['allocations'][PEER]['remaining'],'1')
        self.assertFalse(result['allocations'][CID]['quantity_coverage_observed'])

    def test_short_account_requires_negative_position_and_opposite_exit_side(self):
        bindings=[binding()];sample=snapshot(bindings)
        bindings[0].update(account_role='short_account',side='SHORT')
        sample.update(account_role='short_account',position_quantity='-1')
        for order in sample['orders']:
            order['side']='SELL' if order['order_id']=='1' else 'BUY'
        self.assertEqual(self.check(bindings,sample)['status'],'RECONCILED')
        bindings[0]['side']='LONG'
        self.assertEqual(self.check(bindings,sample)['diagnostics'],['ACCOUNT_DIRECTION_FENCE'])

    def test_account_symbol_role_isolation(self):
        for key,value in (('account','0x'+'2'*40),('symbol','SOL'),('account_role','short_account')):
            bindings=[binding()];bindings[0][key]=value
            self.assertEqual(self.check(bindings,snapshot())['status'],'QUARANTINED')

    def test_missing_or_external_orders_fills_and_position_quarantined(self):
        changes=[lambda s:s['orders'].pop(), lambda s:s['orders'].append({**s['orders'][0],'order_id':'99'}),
            lambda s:s['fills'].append({**s['fills'][0],'order_id':'99','fill_id':'external'}),
            lambda s:s.update(position_quantity='1.1'),lambda s:s['fills'].clear()]
        for change in changes:
            sample=snapshot();change(sample)
            with self.subTest(change=change):
                self.assertEqual(self.check([binding()],sample)['status'],'QUARANTINED')

    def test_every_complete_evidence_flag_and_freshness_is_required(self):
        for key in ('history_complete','orders_complete','position_complete'):
            sample=snapshot();sample[key]=False
            self.assertEqual(self.check([binding()],sample)['status'],'QUARANTINED')
        for now in (999,16001):
            self.assertEqual(allocations.assess([binding()],snapshot(),now_ms=now)['status'],'QUARANTINED')

    def test_duplicate_ownership_and_duplicate_fill_quarantined(self):
        duplicate=binding(PEER)
        self.assertEqual(self.check([binding(),duplicate],snapshot())['diagnostics'],['ORDER_HAS_MULTIPLE_OWNERS'])
        sample=snapshot();sample['fills'].append(deepcopy(sample['fills'][0]))
        self.assertEqual(self.check([binding()],sample)['diagnostics'],['EXTERNAL_OR_DUPLICATE_FILL_QUARANTINE'])

    def test_restart_replay_and_monotonic_fill_history(self):
        bindings=[binding()];sample=snapshot();prior=self.check(bindings,sample)
        restored=json.loads(json.dumps(prior))
        self.assertEqual(self.check(bindings,sample,restored),prior)
        changed=deepcopy(sample);changed['at_ms']=2000;changed['fills'][0]['price']='97'
        self.assertEqual(self.check(bindings,changed,restored)['diagnostics'],['PREVIOUS_FILL_REMOVED_OR_CHANGED'])
        changed=deepcopy(sample);changed['at_ms']=999
        self.assertEqual(self.check(bindings,changed,restored)['diagnostics'],['EVIDENCE_REGRESSION'])

    def test_terminal_entry_cannot_reopen_after_restart(self):
        bindings=[binding()];sample=snapshot();prior=self.check(bindings,sample)
        sample['at_ms']=2000;sample['orders'][0].update(status='CANCELED')
        self.assertEqual(self.check(bindings,sample,prior)['diagnostics'],['FINAL_ORDER_REMOVED_OR_CHANGED'])

    def test_replacement_exit_adds_oid_without_reassigning_old_ownership(self):
        bindings=[binding()];sample=snapshot()
        sample['orders'][1]['status']='CANCELED'
        prior=self.check(bindings,sample)
        bindings[0]['orders'].append(dict(order_id='4',kind='STOP',quantity='1',reduce_only=True))
        sample['orders'].append(dict(order_id='4',status='OPEN',quantity='1',filled_quantity='0',reduce_only=True,side='SELL'))
        sample['at_ms']=2000
        result=self.check(bindings,sample,prior)
        self.assertEqual(result['status'],'RECONCILED')
        self.assertTrue(result['allocations'][CID]['quantity_coverage_observed'])
        bindings[0]['orders'][1]['kind']='TAKE_PROFIT'
        self.assertEqual(self.check(bindings,sample,prior)['diagnostics'],['PREVIOUS_OWNERSHIP_REMOVED_OR_CHANGED'])

    def test_finality_requires_exit_and_sibling_cancel_then_correct_position(self):
        bindings=[binding()];sample=snapshot()
        sample['orders'][1].update(status='FILLED',filled_quantity='1')
        sample['fills'].append(dict(fill_id='stop',order_id='2',quantity='1',price='95',at_ms=950))
        sample['position_quantity']='0'
        result=self.check(bindings,sample)
        self.assertEqual(result['status'],'RECONCILED')
        self.assertFalse(result['allocations'][CID]['all_orders_final'])
        sample['orders'][2]['status']='CANCELED';sample['at_ms']=2000
        result=self.check(bindings,sample,result)
        self.assertTrue(result['allocations'][CID]['all_orders_final'])
        self.assertFalse(result['shared_market_dispatch_enabled'])

    def test_rejected_entry_empty_fills_and_all_exits_terminal_final(self):
        bindings=[binding()];sample=snapshot()
        for row in sample['orders']:
            row.update(status='REJECTED',filled_quantity='0')
        sample.update(fills=[],position_quantity='0')
        result=self.check(bindings,sample)
        self.assertTrue(result['allocations'][CID]['all_orders_final'])

    def test_exit_before_own_entry_is_not_hidden_by_later_entry_fill(self):
        bindings=[binding()];sample=snapshot()
        sample['orders'][1].update(status='FILLED',filled_quantity='1')
        sample['orders'][2]['status']='CANCELED'
        sample['fills'].append(dict(fill_id='early',order_id='2',quantity='1',price='95',at_ms=800))
        sample['position_quantity']='0'
        result=self.check(bindings,sample)
        self.assertEqual(result['diagnostics'],['EXIT_PRECEDES_OWN_ENTRY_ALLOCATION'])

    def test_malformed_and_nan_evidence_cannot_authorize(self):
        for field,value in (('position_quantity','NaN'),('position_quantity','Infinity'),('orders',None),
                            ('account','invalid'),('at_ms',True),('environment','mainnet')):
            sample=snapshot();sample[field]=value
            self.assertEqual(allocations.assess([binding()],sample,now_ms=1000)['status'],'QUARANTINED')


if __name__=='__main__':
    unittest.main()
