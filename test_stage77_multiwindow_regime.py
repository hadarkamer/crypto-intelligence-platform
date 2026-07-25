from datetime import datetime, timedelta, timezone
import coinglass_oi_regime_service as r

def row(now, mins, price, oi):
    return {"collected_at": (now-timedelta(minutes=mins)).isoformat(), "price": price, "open_interest_usd": oi}

def test_five_windows_and_majority():
    now=datetime.now(timezone.utc)
    hist=[row(now,1440,90,900),row(now,720,92,920),row(now,240,94,940),row(now,60,98,980),row(now,30,99,990)]
    w=r._window_results("BTC",100,1000,now,hist)
    assert list(w)==["30m","1h","4h","12h","24h"]
    assert all(x["state"]=="BULLISH_BUILDUP" for x in w.values())
    o=r._overall(w)
    assert o["agreement"]==5 and o["strength"]=="Strong"

def test_early_transition():
    now=datetime.now(timezone.utc)
    # broad bullish; last 1h/30m bearish buildup relative to current price/OI
    hist=[row(now,1440,90,900),row(now,720,92,920),row(now,240,94,940),row(now,60,102,980),row(now,30,101,990)]
    w=r._window_results("BTC",100,1000,now,hist)
    o=r._overall(w)
    assert o["state"]=="BULLISH_BUILDUP" and o["agreement"]==3
    assert r._early_transition(w,o) is True

def test_insufficient_long_windows_are_not_invented():
    now=datetime.now(timezone.utc)
    hist=[row(now,60,99,990),row(now,30,99.5,995)]
    w=r._window_results("BTC",100,1000,now,hist)
    assert w["30m"]["available"] and w["1h"]["available"]
    assert not w["4h"]["available"] and not w["24h"]["available"]
