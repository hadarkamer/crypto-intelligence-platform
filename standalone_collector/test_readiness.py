import unittest
from model1_readiness import valid_source, ensure_ready, SourceNotReady
from pathlib import Path
import tempfile
SOURCE='https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'

class Page:
    url=SOURCE
    def __init__(self,state): self.state=state; self.screenshots=0
    def wait_for_function(self,*args,**kwargs): pass
    def wait_for_timeout(self,*args): pass
    def evaluate(self,*args): return self.state
    def screenshot(self,path,**kwargs): self.screenshots+=1; Path(path).write_bytes(b'synthetic')

class Tests(unittest.TestCase):
    def test_correct_source(self): self.assertTrue(valid_source(SOURCE))
    def test_other_model(self): self.assertFalse(valid_source(SOURCE.replace('HeatMap','HeatMapNew')))
    def test_wrong_coin(self): self.assertFalse(valid_source(SOURCE.replace('BTC','ETH')))
    def test_duplicate_coin(self): self.assertFalse(valid_source(SOURCE+'&coin=BTC')))
    def test_nonsecure(self): self.assertFalse(valid_source(SOURCE.replace('https:','http:')))
    def test_wrong_host(self): self.assertFalse(valid_source(SOURCE.replace('www.coinglass.com','evil.example')))
    def test_ready(self):
        with tempfile.TemporaryDirectory() as d:
            p=Page({'ready':True,'label':'12 hour'})
            ensure_ready(p,'12h',Path(d)/'probe.png')
            self.assertEqual(p.screenshots,0)
    def test_loading_is_not_success(self):
        with tempfile.TemporaryDirectory() as d:
            p=Page({'ready':False,'reason':'loading-indicator','label':'12 hour'})
            with self.assertRaises(SourceNotReady): ensure_ready(p,'12h',Path(d)/'probe.png')
            self.assertEqual(p.screenshots,1)
    def test_wrong_label(self):
        with tempfile.TemporaryDirectory() as d:
            p=Page({'ready':True,'label':'24 hour'})
            with self.assertRaises(SourceNotReady): ensure_ready(p,'12h',Path(d)/'probe.png')
    def test_no_48h(self):
        with self.assertRaises(SourceNotReady): ensure_ready(Page({}),'48h',Path('unused'))

if __name__=='__main__': unittest.main()
