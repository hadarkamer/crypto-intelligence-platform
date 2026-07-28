import coinglass_oi_regime_service as svc

class Resp:
    def __init__(self, payload): self.payload=payload
    def raise_for_status(self): pass
    def json(self): return self.payload

def test_prefers_all(monkeypatch):
    monkeypatch.setattr(svc, '_api_key', lambda: 'x')
    monkeypatch.setattr(svc.requests, 'get', lambda *a, **k: Resp({'code':0,'data':[{'exchange':'All','open_interest_usd':300},{'exchange':'Hyperliquid','open_interest_usd':200}]}))
    assert svc.fetch_aggregated_oi('HYPE') == 300

def test_sums_exchanges_when_all_missing(monkeypatch):
    monkeypatch.setattr(svc, '_api_key', lambda: 'x')
    monkeypatch.setattr(svc.requests, 'get', lambda *a, **k: Resp({'code':0,'data':[{'exchange':'Hyperliquid','open_interest_usd':200},{'exchange':'Bybit','open_interest_usd':50},{'exchange':'Bad','open_interest_usd':None}]}))
    assert svc.fetch_aggregated_oi('HYPE') == 250

def test_long_unwinding_supports_maxpain_long():
    regime={'overall':{'state':'LONG_UNWINDING','label':'Long Unwinding','agreement':3}}
    assert svc.composite_conclusion(regime,'LONG').startswith('Price+OI תומך בכיוון LONG: Long Unwinding (3/5).')

def test_short_covering_supports_maxpain_short():
    regime={'overall':{'state':'SHORT_COVERING','label':'Short Covering','agreement':3}}
    assert svc.composite_conclusion(regime,'SHORT').startswith('Price+OI תומך בכיוון SHORT: Short Covering (3/5).')
