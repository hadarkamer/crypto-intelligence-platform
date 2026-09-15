"""Offline synthetic tests only; no source or model requests."""
import contextlib
import io
import os
import unittest
from unittest.mock import patch
import august_replay as replay


def valid():
    return {'views':[{'image_index':i,'timeframe':tf,'mode':'Symbol','model':'Model 1',
        'btc':True,'readable':True,'obstruction':'none','account_ui':'unknown',
        'price_ticks':[76000,77000]} for i,tf in enumerate(('12h','24h'))]}

class Tests(unittest.TestCase):
    def test_two_separate_views(self):
        self.assertEqual(replay.validate_review(valid()),valid())
    def test_loading_is_not_success(self):
        v=valid();v['views'][0]['obstruction']='loading'
        with self.assertRaises(ValueError):replay.validate_review(v)
    def test_missing_numeric_evidence_is_not_success(self):
        v=valid();v['views'][0]['price_ticks']=[]
        with self.assertRaises(ValueError):replay.validate_review(v)
    def test_wrong_timeframe_cannot_be_labeled12h(self):
        v=valid();v['views'][0]['timeframe']='24h'
        with self.assertRaises(ValueError):replay.validate_review(v)
    def test_unreadable_is_a_valid_diagnostic_not_success(self):
        v=valid();v['views'][0].update(readable=False,obstruction='blur',price_ticks=[])
        self.assertFalse(replay.validate_review(v)['views'][0]['readable'])
    def test_account_data_rejected(self):
        v=valid();v['views'][0]['email']='PRIVATE'
        with self.assertRaises(ValueError):replay.validate_review(v)
    def test_boolean_prices_rejected(self):
        v=valid();v['views'][0]['price_ticks']=[True,False]
        with self.assertRaises(ValueError):replay.validate_review(v)
    def test_no_optin_does_nothing(self):
        with patch.dict(os.environ,{},clear=True),patch.object(replay,'_capture_once') as cap,contextlib.redirect_stdout(io.StringIO()):
            replay.main();cap.assert_not_called()
    def test_no_session_does_not_open_site(self):
        with patch.dict(os.environ,{'MODEL1_AUGUST_REPLAY':'20260915-original-two-views'},clear=True),patch.object(replay,'_capture_once') as cap,contextlib.redirect_stdout(io.StringIO()):
            replay.main();cap.assert_not_called()

if __name__=='__main__':unittest.main()
