"""Stage 77: independent multi-window Price + Open Interest regime layer.

Snapshots are collected every 30 minutes. Regime conclusions are calculated
independently for 30m, 1h, 4h, 12h and 24h. No Max-Pain score is modified.
No hand-tuned percentage/intensity thresholds are used: each window reports
raw percentage changes and the sign relationship between Price and OI.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional

import requests

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

API_BASE_URL = "https://open-api-v4.coinglass.com"
OI_ENDPOINT = "/api/futures/open-interest/exchange-list"
API_TIMEOUT_SECONDS = 15
COLLECTION_INTERVAL_MINUTES = 30
HISTORY_RETENTION_DAYS = 60
REGIME_WINDOWS_MINUTES = (30, 60, 240, 720, 1440)
WINDOW_LABELS = {30: "30m", 60: "1h", 240: "4h", 720: "12h", 1440: "24h"}
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "data/coinglass.db")

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_regime_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    open_interest_usd REAL NOT NULL,
    price_change_pct REAL,
    oi_change_pct REAL,
    state TEXT NOT NULL,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE(collected_at, symbol)
);
CREATE INDEX IF NOT EXISTS idx_oi_regime_symbol_time
ON oi_regime_snapshots(symbol, collected_at);
"""
POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_regime_snapshots (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    open_interest_usd DOUBLE PRECISION NOT NULL,
    price_change_pct DOUBLE PRECISION,
    oi_change_pct DOUBLE PRECISION,
    state TEXT NOT NULL,
    direction TEXT NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE(collected_at, symbol)
);
CREATE INDEX IF NOT EXISTS idx_oi_regime_symbol_time
ON oi_regime_snapshots(symbol, collected_at);
"""

@dataclass(frozen=True)
class RegimeResult:
    symbol: str
    state: str
    label: str
    direction: str
    price_change_pct: Optional[float]
    oi_change_pct: Optional[float]
    reason: str
    available: bool = True
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

def _api_key(): return os.getenv("COINGLASS_API_KEY", "").strip()
def _use_postgres(): return bool(DATABASE_URL and psycopg)

def init_db():
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute(POSTGRES_SCHEMA); conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA); conn.commit()

def _pct_change(new, old):
    if old is None or float(old) == 0.0: return None
    return (float(new)-float(old))/float(old)*100.0

def _is_zero(v): return math.isclose(float(v), 0.0, rel_tol=0.0, abs_tol=1e-12)

def _state_label(state):
    return {"BULLISH_BUILDUP":"Bullish Build-up","BEARISH_BUILDUP":"Bearish Build-up",
            "SHORT_COVERING":"Short Covering","LONG_UNWINDING":"Long Unwinding",
            "NEUTRAL_INCONCLUSIVE":"Neutral / Inconclusive","UNAVAILABLE":"אין מספיק נתונים"}.get(state,state or "לא ידוע")

def classify(symbol, price_change_pct, oi_change_pct):
    symbol=str(symbol or "").upper()
    if price_change_pct is None or oi_change_pct is None:
        return RegimeResult(symbol,"UNAVAILABLE","אין מספיק היסטוריה","NEUTRAL",price_change_pct,oi_change_pct,
                            "אין עדיין דגימת עבר לטווח הזה.",False)
    p,o=float(price_change_pct),float(oi_change_pct)
    if _is_zero(p) or _is_zero(o):
        parts=[]
        if _is_zero(p): parts.append("המחיר ללא שינוי")
        if _is_zero(o): parts.append("ה-OI ללא שינוי")
        return RegimeResult(symbol,"NEUTRAL_INCONCLUSIVE","Neutral / Inconclusive","NEUTRAL",round(p,6),round(o,6),"; ".join(parts)+"; אין מסקנה כיוונית.")
    if p>0 and o>0: st,di,rs="BULLISH_BUILDUP","LONG","המחיר וה-OI עלו יחד."
    elif p<0 and o>0: st,di,rs="BEARISH_BUILDUP","SHORT","המחיר ירד וה-OI עלה."
    elif p>0 and o<0: st,di,rs="SHORT_COVERING","LONG","המחיר עלה וה-OI ירד: Short Covering."
    else: st,di,rs="LONG_UNWINDING","SHORT","המחיר ירד וה-OI ירד: Long Unwinding."
    return RegimeResult(symbol,st,_state_label(st),di,round(p,6),round(o,6),rs)

def fetch_aggregated_oi(symbol):
    key=_api_key()
    if not key: raise RuntimeError("COINGLASS_API_KEY is not configured")
    r=requests.get(API_BASE_URL+OI_ENDPOINT,params={"symbol":str(symbol or "").upper()},headers={"CG-API-KEY":key,"accept":"application/json"},timeout=API_TIMEOUT_SECONDS)
    r.raise_for_status(); payload=r.json()
    if not isinstance(payload,dict) or str(payload.get("code")) not in {"0","200"}: raise RuntimeError(f"CoinGlass API error: {payload.get('msg') if isinstance(payload,dict) else 'invalid response'!r}")
    for row in payload.get("data") or []:
        if isinstance(row,dict) and str(row.get("exchange","")).strip().lower()=="all":
            try: v=float(row.get("open_interest_usd"))
            except (TypeError,ValueError): continue
            if v>0: return v
    raise ValueError(f"CoinGlass returned no aggregated OI for {symbol}")

def _history(symbol):
    init_db(); symbol=str(symbol or "").upper()
    sql="SELECT collected_at,price,open_interest_usd FROM oi_regime_snapshots WHERE symbol=? ORDER BY collected_at ASC"
    if _use_postgres():
        with psycopg.connect(DATABASE_URL,row_factory=dict_row) as conn: rows=conn.execute(sql.replace("?","%s"),(symbol,)).fetchall()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory=sqlite3.Row; rows=conn.execute(sql,(symbol,)).fetchall()
    return [dict(r) for r in rows]

def _as_utc(v):
    if isinstance(v,datetime):
        return v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(v).replace("Z","+00:00")).astimezone(timezone.utc)

def _reference_for_window(rows, now, minutes):
    """Use the newest stored sample at or before the requested lookback time."""
    target=now-timedelta(minutes=minutes)
    eligible=[r for r in rows if _as_utc(r["collected_at"])<=target]
    return eligible[-1] if eligible else None

def _window_results(symbol, price, oi, now=None, history_rows=None):
    now=now or datetime.now(timezone.utc); rows=_history(symbol) if history_rows is None else history_rows
    out={}
    for minutes in REGIME_WINDOWS_MINUTES:
        ref=_reference_for_window(rows,now,minutes)
        if not ref:
            res=classify(symbol,None,None)
        else:
            res=classify(symbol,_pct_change(price,ref["price"]),_pct_change(oi,ref["open_interest_usd"]))
        d=res.to_dict(); d["window_minutes"]=minutes; d["window_label"]=WINDOW_LABELS[minutes]; out[WINDOW_LABELS[minutes]]=d
    return out

def _overall(windows):
    directional=[w for w in windows.values() if w.get("available") and w.get("state") not in {"NEUTRAL_INCONCLUSIVE","UNAVAILABLE"}]
    counts={}
    for w in directional: counts[w["state"]]=counts.get(w["state"],0)+1
    if not counts:
        return {"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","strength":"Inconclusive","agreement":0,"valid_windows":len(directional)}
    state,n=max(counts.items(),key=lambda kv:kv[1])
    if n>=5: strength="Strong"
    elif n==4: strength="Strong / Confirmed"
    elif n==3: strength="Confirmed"
    else: strength="Mixed / Transition"
    if n<3: state="MIXED_TRANSITION"; label="Mixed / Transition"
    else: label=_state_label(state)
    return {"state":state,"label":label,"strength":strength,"agreement":n,"valid_windows":len(directional)}

def _early_transition(windows, overall):
    if overall.get("state") in {"MIXED_TRANSITION","NEUTRAL_INCONCLUSIVE"}: return False
    short=[windows.get("30m",{}),windows.get("1h",{})]
    broad=[windows.get("4h",{}),windows.get("12h",{}),windows.get("24h",{})]
    short_states=[x.get("state") for x in short if x.get("available") and x.get("state") not in {"NEUTRAL_INCONCLUSIVE","UNAVAILABLE"}]
    broad_states=[x.get("state") for x in broad if x.get("available") and x.get("state") not in {"NEUTRAL_INCONCLUSIVE","UNAVAILABLE"}]
    return len(short_states)==2 and short_states[0]==short_states[1] and len(broad_states)>=2 and short_states[0]!=overall.get("state")

def _insert_snapshot(symbol,price,oi,result):
    init_db(); now=datetime.now(timezone.utc); collected_at=now if _use_postgres() else now.isoformat()
    params=(collected_at,str(symbol).upper(),float(price),float(oi),result.price_change_pct,result.oi_change_pct,result.state,result.direction,result.reason)
    sql="INSERT INTO oi_regime_snapshots (collected_at,symbol,price,open_interest_usd,price_change_pct,oi_change_pct,state,direction,reason) VALUES (?,?,?,?,?,?,?,?,?)"
    if _use_postgres():
        with psycopg.connect(DATABASE_URL,row_factory=dict_row) as conn:
            conn.execute(sql.replace("?","%s"),params); conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < %s",(now-timedelta(days=HISTORY_RETENTION_DAYS),)); conn.commit()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(sql,params); conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < ?",((now-timedelta(days=HISTORY_RETENTION_DAYS)).isoformat(),)); conn.commit()

def collect_symbol(symbol,price):
    symbol=str(symbol or "").upper(); oi=fetch_aggregated_oi(symbol); now=datetime.now(timezone.utc); rows=_history(symbol)
    windows=_window_results(symbol,float(price),float(oi),now,rows); overall=_overall(windows); early=_early_transition(windows,overall)
    # Persist current sample. Legacy columns retain the 30m result for DB compatibility only.
    r30=windows["30m"]; legacy=RegimeResult(symbol,r30["state"],r30["label"],r30["direction"],r30["price_change_pct"],r30["oi_change_pct"],r30["reason"],r30["available"])
    _insert_snapshot(symbol,float(price),float(oi),legacy)
    return {"symbol":symbol,"price":float(price),"open_interest_usd":float(oi),"windows":windows,"overall":overall,"early_transition":early,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}

def collect_many(symbol_prices):
    out={}
    for symbol,price in sorted(symbol_prices.items()):
        try: out[symbol]=collect_symbol(symbol,price)
        except Exception as exc: out[symbol]={"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"נתוני Price + OI לא זמינים","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":str(exc),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    return out

def latest(symbol):
    symbol=str(symbol or "").upper(); rows=_history(symbol)
    if not rows: return {"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"אין נתוני Price + OI","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":"טרם נאספה דגימת Price + OI.","collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    current=rows[-1]; now=_as_utc(current["collected_at"])
    history_before=rows[:-1]
    windows=_window_results(symbol,float(current["price"]),float(current["open_interest_usd"]),now,history_before)
    overall=_overall(windows); early=_early_transition(windows,overall)
    return {"symbol":symbol,"price":float(current["price"]),"open_interest_usd":float(current["open_interest_usd"]),"windows":windows,"overall":overall,"early_transition":early,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}

def composite_conclusion(regime,alert_side):
    # Backward-compatible with the original single-window Stage 77 payload,
    # while normal operation uses the multi-window overall conclusion.
    overall=regime.get("overall") or {}; side=str(alert_side or "").upper()
    state=str(overall.get("state") or regime.get("state") or "")
    label=overall.get("label") or regime.get("label") or _state_label(state)
    agreement=overall.get("agreement")
    if state in {"","UNAVAILABLE","NEUTRAL_INCONCLUSIVE"}: return "אין כרגע מסקנת Price+OI כוללת; הציון הקיים נשאר עצמאי."
    if state=="MIXED_TRANSITION": return "Price+OI מציג Mixed / Transition; אין אישור כיווני כולל."
    if state=="SHORT_COVERING" and side=="LONG":
        return "המחיר עולה, אך ה-OI יורד: התנועה בכיוון LONG ללא אישור של בניית OI חדש."
    if state=="LONG_UNWINDING" and side=="SHORT":
        return "המחיר יורד, אך ה-OI יורד: התנועה בכיוון SHORT ללא אישור של בניית OI חדש."
    direction="LONG" if state in {"BULLISH_BUILDUP","SHORT_COVERING"} else "SHORT"
    relation="תומך" if side==direction else "מנוגד"
    suffix=f" ({agreement}/5)" if agreement is not None else ""
    return f"Price+OI {relation} בכיוון {side}: {label}{suffix}."

def attach_to_opportunities(items):
    cache={}
    for item in items:
        symbol=str(item.get("symbol","")).upper()
        if symbol not in cache: cache[symbol]=latest(symbol)
        item["market_regime"]=cache[symbol]
        item["composite_conclusion"]=composite_conclusion(cache[symbol],str(item.get("side","")))
    return items
