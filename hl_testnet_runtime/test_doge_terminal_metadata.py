"""Regression for observed post-fill cleared trigger metadata, never new orders."""
from copy import deepcopy
import unittest
from unittest.mock import patch
from . import doge_lifecycle_review as m
from . import card_lifecycle as life
from .test_doge_lifecycle_review import Reader, record, A, T


def cleared_reader():
    reader=Reader()
    raw=list(reader.statuses.values())[1]['order']['order']
    raw.update(triggerPx='0.0',isTrigger=False,orderType='Take Profit Limit')
    return reader


class TerminalMetadataTests(unittest.TestCase):
    def setUp(self):
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('No exchange'))
        p.start();self.addCleanup(p.stop)

    def collect(self,reader):
        return m.collect(record(),A,reader,clock=lambda:T)

    def test_observed_filled_tp_shape_preserves_original_trigger(self):
        r=cleared_reader();raw=deepcopy(r.statuses)
        b,s,v,_=self.collect(r)
        self.assertTrue(v['cards'][0]['closure_verified'])
        self.assertEqual(b['prices']['take_profit'],'104')
        self.assertEqual(r.statuses,raw)
        self.assertEqual(v['cards'][0]['net_before_funding_usdc'],'19.8')

    def test_still_active_trigger_cannot_be_zero(self):
        r=cleared_reader();list(r.statuses.values())[1]['order']['order']['isTrigger']=True
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_missing_trigger_is_not_coerced_to_zero(self):
        r=cleared_reader();list(r.statuses.values())[1]['order']['order'].pop('triggerPx')
        with self.assertRaises(life.LifecycleError):self.collect(r)

    def test_wrong_terminal_type_is_not_accepted(self):
        r=cleared_reader();list(r.statuses.values())[1]['order']['order']['orderType']='Market'
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_nonzero_changed_trigger_is_not_accepted(self):
        r=cleared_reader();list(r.statuses.values())[1]['order']['order']['triggerPx']='105'
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_cleared_metadata_does_not_relax_order_ownership(self):
        r=cleared_reader();list(r.statuses.values())[1]['order']['order']['cloid']='0x'+'f'*32
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_sibling_cancel_with_cleared_stop_is_supported_only_when_fully_closed(self):
        r=cleared_reader();stop=list(r.statuses.values())[2]['order']
        stop['order'].update(triggerPx='0.0',isTrigger=False,orderType='Stop Market')
        self.assertTrue(self.collect(r)[2]['cards'][0]['closure_verified'])
        stop['status']='canceled'
        with self.assertRaises(m.ReviewError):self.collect(r)

    def test_cleared_metadata_does_not_relax_fill_completeness(self):
        r=cleared_reader();r.fills[1]['sz']='2'
        with self.assertRaisesRegex(m.ReviewError,'TERMINAL_QUANTITIES'):self.collect(r)


if __name__=='__main__':unittest.main(verbosity=2)
