"""Stage 77: independent multi-window Price + Open Interest regime layer.

Snapshots are collected every 30 minutes. Regime conclusions are calculated
independently for 30m, 1h, 4h, 12h and 24h. No Max-Pain score is modified.
Historical Price/OI distributions are used as a symbol+timeframe-specific
minimum and strength reference. P25 is the minimum valid movement. No fixed
percentage threshold is shared between coins or timeframes.
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

import coinglass_history_backfill as history_reference

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
HISTORY_RETENTION_DAYS = 365
REFERENCE_TOLERANCE_MINUTES = 20
PRICE_OI_PASS_SECONDS = 30
PRICE_OI_WARNING_SECONDS = 60
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
    price_fetched_at TEXT,
    oi_fetched_at TEXT,
    time_gap_seconds REAL,
    data_quality_status TEXT,
    price_source TEXT,
    oi_source TEXT,
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
    price_fetched_at TIMESTAMPTZ,
    oi_fetched_at TIMESTAMPTZ,
    time_gap_seconds DOUBLE PRECISION,
    data_quality_status TEXT,
    price_source TEXT,
    oi_source TEXT,
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
            conn.execute(POSTGRES_SCHEMA)
            for column, ctype in (
                ("price_fetched_at", "TIMESTAMPTZ"),
                ("oi_fetched_at", "TIMESTAMPTZ"),
                ("time_gap_seconds", "DOUBLE PRECISION"),
                ("data_quality_status", "TEXT"),
                ("price_source", "TEXT"),
                ("oi_source", "TEXT"),
            ):
                conn.execute(f"ALTER TABLE oi_regime_snapshots ADD COLUMN IF NOT EXISTS {column} {ctype}")
            conn.commit()
    else:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(SQLITE_SCHEMA)
            existing={row[1] for row in conn.execute("PRAGMA table_info(oi_regime_snapshots)").fetchall()}
            for column, ctype in (
                ("price_fetched_at", "TEXT"),
                ("oi_fetched_at", "TEXT"),
                ("time_gap_seconds", "REAL"),
                ("data_quality_status", "TEXT"),
                ("price_source", "TEXT"),
                ("oi_source", "TEXT"),
            ):
                if column not in existing:
                    conn.execute(f"ALTER TABLE oi_regime_snapshots ADD COLUMN {column} {ctype}")
            conn.commit()

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

def _classify_with_historical_reference(symbol, window_label, price_change_pct, oi_change_pct):
    """Classify direction only when both movements clear their own historical P25.

    If no historical reference exists for this symbol/window, preserve the
    pre-reference Stage 77 classifier so non-backfilled symbols keep working.
    """
    base = classify(symbol, price_change_pct, oi_change_pct)
    d = base.to_dict()
    ref = history_reference.reference_for_window(symbol, window_label)
    price_dist = ref.get("price_abs_change_pct") or {}
    oi_dist = ref.get("oi_abs_change_pct") or {}
    price_strength = history_reference.strength_from_distribution(price_change_pct, price_dist)
    oi_strength = history_reference.strength_from_distribution(oi_change_pct, oi_dist)
    d["historical_reference_available"] = bool(price_strength.get("available") and oi_strength.get("available"))
    d["price_strength"] = price_strength
    d["oi_strength"] = oi_strength
    d["reference_samples"] = ref.get("samples")

    # No backfill/reference: keep existing live behavior unchanged.
    if not d["historical_reference_available"]:
        return d

    # P25 is not an invented percentage. It is the lower historical quartile
    # for this exact symbol and exact window. Both Price and OI must clear it
    # before the four directional Price+OI states are considered confirmed.
    price_valid = int(price_strength.get("rank", -1)) >= 1
    oi_valid = int(oi_strength.get("rank", -1)) >= 1
    d["price_minimum_valid"] = price_valid
    d["oi_minimum_valid"] = oi_valid

    if price_change_pct is None or oi_change_pct is None:
        return d

    if not (price_valid and oi_valid):
        ptxt = price_strength.get("label", "Unknown")
        otxt = oi_strength.get("label", "Unknown")
        reasons = []
        if not price_valid:
            reasons.append(f"Price מתחת ל-P25 ({ptxt})")
        if not oi_valid:
            reasons.append(f"OI מתחת ל-P25 ({otxt})")
        d.update({
            "state": "NEUTRAL_INCONCLUSIVE",
            "label": "Neutral / Inconclusive",
            "direction": "NEUTRAL",
            "reason": "; ".join(reasons) + "; אין אישור Price+OI כיווני.",
            "available": True,
        })
    return d


def fetch_aggregated_oi_with_meta(symbol):
    symbol=str(symbol or "").upper()
    key=_api_key()
    if not key: raise RuntimeError("COINGLASS_API_KEY is not configured")
    requested_at=datetime.now(timezone.utc)
    r=requests.get(API_BASE_URL+OI_ENDPOINT,params={"symbol":symbol},headers={"CG-API-KEY":key,"accept":"application/json"},timeout=API_TIMEOUT_SECONDS)
    received_at=datetime.now(timezone.utc)
    r.raise_for_status(); payload=r.json()
    if not isinstance(payload,dict) or str(payload.get("code")) not in {"0","200"}: raise RuntimeError(f"CoinGlass API error: {payload.get('msg') if isinstance(payload,dict) else 'invalid response'!r}")

    rows=[row for row in (payload.get("data") or []) if isinstance(row,dict)]
    exchange_values=[]
    for row in rows:
        exchange=str(row.get("exchange","")).strip()
        try: value=float(row.get("open_interest_usd"))
        except (TypeError,ValueError):
            continue
        if value<=0:
            continue
        exchange_values.append((exchange,value))
        if exchange.lower()=="all":
            return {"value":value,"fetched_at":received_at,"requested_at":requested_at,"source":"coinglass_all"}

    per_exchange=[(name,value) for name,value in exchange_values if name.lower()!="all"]
    if per_exchange:
        total=sum(value for _,value in per_exchange)
        return {"value":total,"fetched_at":received_at,"requested_at":requested_at,"source":"coinglass_exchange_sum"}

    returned_names=",".join(str(row.get("exchange","")).strip() or "unknown" for row in rows) or "none"
    raise ValueError(f"CoinGlass returned no usable OI for {symbol}; exchanges={returned_names}")


def fetch_aggregated_oi(symbol):
    return float(fetch_aggregated_oi_with_meta(symbol)["value"])

def _history(symbol):
    init_db(); symbol=str(symbol or "").upper()
    sql="SELECT collected_at,price,open_interest_usd,price_fetched_at,oi_fetched_at,time_gap_seconds,data_quality_status,price_source,oi_source FROM oi_regime_snapshots WHERE symbol=? ORDER BY collected_at ASC"
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

def _reference_for_window(rows, now, minutes, symbol=None):
    """Return a reference only when it is close enough to the requested window."""
    target=now-timedelta(minutes=minutes)
    eligible=[r for r in rows if _as_utc(r["collected_at"])<=target]
    ref=None
    if eligible:
        ref=dict(eligible[-1]); ref.setdefault("source", "live_snapshot")
    elif symbol:
        ref=history_reference.historical_point_at_or_before(symbol, target)
    if not ref:
        return None
    ref_time=_as_utc(ref["collected_at"])
    offset_seconds=(target-ref_time).total_seconds()
    ref["reference_offset_seconds"]=offset_seconds
    ref["reference_target_time"]=target.isoformat()
    if offset_seconds < 0 or offset_seconds > REFERENCE_TOLERANCE_MINUTES*60:
        return None
    return ref

def _window_results(symbol, price, oi, now=None, history_rows=None):
    now=now or datetime.now(timezone.utc); rows=_history(symbol) if history_rows is None else history_rows
    out={}
    for minutes in REGIME_WINDOWS_MINUTES:
        label=WINDOW_LABELS[minutes]
        ref=_reference_for_window(rows,now,minutes,symbol)
        if not ref:
            d=classify(symbol,None,None).to_dict()
            # Reference strength is meaningful only when a live delta exists.
            d["historical_reference_available"]=bool(history_reference.reference_for_window(symbol,label))
            d["price_strength"]={"available":False,"label":"Unknown","rank":None}
            d["oi_strength"]={"available":False,"label":"Unknown","rank":None}
            d["comparison_source"]="unavailable"
            d["reference_time"]=None
        else:
            pchange=_pct_change(price,ref["price"])
            ochange=_pct_change(oi,ref["open_interest_usd"])
            d=_classify_with_historical_reference(symbol,label,pchange,ochange)
            d["comparison_source"]=ref.get("source","live_snapshot")
            d["reference_time"]=_as_utc(ref["collected_at"]).isoformat()
            d["reference_offset_seconds"]=ref.get("reference_offset_seconds")
            d["reference_target_time"]=ref.get("reference_target_time")
        d["window_minutes"]=minutes; d["window_label"]=label; out[label]=d
    return out

def _overall(windows):
    """Aggregate the five regime windows without treating neutral as disagreement.

    Neutral/Inconclusive is a real classified outcome once a window is available.
    A lone directional window against four neutral windows must therefore remain
    Neutral/Inconclusive, not Mixed/Transition. Mixed is reserved for cases where
    directional evidence is genuinely split and no state reaches 3/5.
    """
    available=[w for w in windows.values() if w.get("available") and w.get("state") != "UNAVAILABLE"]
    total=len(available)
    if not available:
        return {"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","strength":"Inconclusive","agreement":0,"valid_windows":0}

    neutral_count=sum(1 for w in available if w.get("state") == "NEUTRAL_INCONCLUSIVE")
    directional=[w for w in available if w.get("state") != "NEUTRAL_INCONCLUSIVE"]
    counts={}
    for w in directional:
        counts[w["state"]]=counts.get(w["state"],0)+1

    # Neutral majority is itself the dominant conclusion.
    if neutral_count >= 3:
        strength = "Strong / Confirmed" if neutral_count >= 4 else "Confirmed"
        return {"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","strength":strength,
                "agreement":neutral_count,"valid_windows":total}

    if not counts:
        return {"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","strength":"Inconclusive",
                "agreement":neutral_count,"valid_windows":total}

    state,n=max(counts.items(),key=lambda kv:kv[1])
    if n>=5: strength="Strong"
    elif n==4: strength="Strong / Confirmed"
    elif n==3: strength="Confirmed"
    else: strength="Mixed / Transition"

    if n < 3:
        state="MIXED_TRANSITION"; label="Mixed / Transition"
    else:
        label=_state_label(state)
    return {"state":state,"label":label,"strength":strength,"agreement":n,"valid_windows":total}

def _significance_observations(windows):
    observations=[]
    for label,w in windows.items():
        if not w.get("historical_reference_available"):
            continue
        ps=w.get("price_strength") or {}; os_=w.get("oi_strength") or {}
        pr=ps.get("rank"); orank=os_.get("rank")
        if pr is None or orank is None:
            continue
        if pr == 0 and orank >= 2:
            observations.append({
                "window":label,
                "type":"OI_WITHOUT_PRICE_CONFIRMATION",
                "text":f"{label}: OI {os_.get('label')} אך Price חלש — בניית/סגירת OI ללא תנועת מחיר מאושרת.",
            })
        elif orank == 0 and pr >= 2:
            observations.append({
                "window":label,
                "type":"PRICE_WITHOUT_OI_CONFIRMATION",
                "text":f"{label}: Price {ps.get('label')} אך OI חלש — תנועת מחיר ללא אישור OI משמעותי.",
            })
    return observations


def _early_transition(windows, overall):
    if overall.get("state") in {"MIXED_TRANSITION","NEUTRAL_INCONCLUSIVE"}: return False
    short=[windows.get("30m",{}),windows.get("1h",{})]
    broad=[windows.get("4h",{}),windows.get("12h",{}),windows.get("24h",{})]
    short_states=[x.get("state") for x in short if x.get("available") and x.get("state") not in {"NEUTRAL_INCONCLUSIVE","UNAVAILABLE"}]
    broad_states=[x.get("state") for x in broad if x.get("available") and x.get("state") not in {"NEUTRAL_INCONCLUSIVE","UNAVAILABLE"}]
    return len(short_states)==2 and short_states[0]==short_states[1] and len(broad_states)>=2 and short_states[0]!=overall.get("state")

def _quality_status(price_time, oi_time):
    gap=abs((oi_time-price_time).total_seconds())
    if gap <= PRICE_OI_PASS_SECONDS: status="PASS"
    elif gap <= PRICE_OI_WARNING_SECONDS: status="WARNING"
    else: status="INVALID"
    return gap,status


def _insert_snapshot(symbol,price,oi,result,price_meta=None,oi_meta=None):
    init_db(); now=datetime.now(timezone.utc); collected_at=now if _use_postgres() else now.isoformat()
    price_meta=price_meta or {}; oi_meta=oi_meta or {}
    pt=_as_utc(price_meta.get("fetched_at") or now); ot=_as_utc(oi_meta.get("fetched_at") or now)
    gap,status=_quality_status(pt,ot)
    params=(collected_at,str(symbol).upper(),float(price),float(oi),result.price_change_pct,result.oi_change_pct,result.state,result.direction,result.reason,
            pt if _use_postgres() else pt.isoformat(),ot if _use_postgres() else ot.isoformat(),gap,status,
            str(price_meta.get("source") or "unknown"),str(oi_meta.get("source") or "unknown"))
    sql="INSERT INTO oi_regime_snapshots (collected_at,symbol,price,open_interest_usd,price_change_pct,oi_change_pct,state,direction,reason,price_fetched_at,oi_fetched_at,time_gap_seconds,data_quality_status,price_source,oi_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    if _use_postgres():
        with psycopg.connect(DATABASE_URL,row_factory=dict_row) as conn:
            conn.execute(sql.replace("?","%s"),params); conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < %s",(now-timedelta(days=HISTORY_RETENTION_DAYS),)); conn.commit()
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(sql,params); conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < ?",((now-timedelta(days=HISTORY_RETENTION_DAYS)).isoformat(),)); conn.commit()
    return {"price_fetched_at":pt.isoformat(),"oi_fetched_at":ot.isoformat(),"time_gap_seconds":gap,"data_quality_status":status,
            "price_source":price_meta.get("source") or "unknown","oi_source":oi_meta.get("source") or "unknown"}


def collect_symbol(symbol,price_input):
    symbol=str(symbol or "").upper()
    if isinstance(price_input,dict):
        price=float(price_input.get("price")); price_meta={"fetched_at":price_input.get("fetched_at_utc") or price_input.get("fetched_at"),"source":price_input.get("source")}
    else:
        price=float(price_input); price_meta={"fetched_at":datetime.now(timezone.utc),"source":"legacy_price"}
    oi_meta=fetch_aggregated_oi_with_meta(symbol); oi=float(oi_meta["value"]); now=datetime.now(timezone.utc); rows=_history(symbol)
    gap,status=_quality_status(_as_utc(price_meta.get("fetched_at") or now),_as_utc(oi_meta.get("fetched_at") or now))
    if status=="INVALID":
        windows={label:{**classify(symbol,None,None).to_dict(),"window_label":label,"window_minutes":minutes,"data_quality_status":status,"time_gap_seconds":gap}
                 for minutes,label in WINDOW_LABELS.items()}
        overall={"state":"UNAVAILABLE","label":"Price/OI timestamp gap too large","strength":"Unavailable","agreement":0,"valid_windows":0}
        early=False; observations=[]
    else:
        windows=_window_results(symbol,price,oi,now,rows); overall=_overall(windows); early=_early_transition(windows,overall); observations=_significance_observations(windows)
    r30=windows["30m"]; legacy=RegimeResult(symbol,r30["state"],r30["label"],r30["direction"],r30.get("price_change_pct"),r30.get("oi_change_pct"),r30["reason"],r30["available"])
    quality=_insert_snapshot(symbol,price,oi,legacy,price_meta,oi_meta)
    return {"symbol":symbol,"price":price,"open_interest_usd":oi,"windows":windows,"overall":overall,"early_transition":early,"significance_observations":observations,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES,**quality}

def collect_many(symbol_prices):
    out={}
    for symbol,price in sorted(symbol_prices.items()):
        try:
            out[symbol]=collect_symbol(symbol,price)
        except Exception as exc:
            print(f"[oi-regime] {symbol} failed: {type(exc).__name__}: {exc}")
            out[symbol]={"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"נתוני Price + OI לא זמינים","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":str(exc),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    return out

def latest(symbol):
    symbol=str(symbol or "").upper(); rows=_history(symbol)
    if not rows: return {"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"אין נתוני Price + OI","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":"טרם נאספה דגימת Price + OI.","collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    current=rows[-1]; now=_as_utc(current["collected_at"])
    history_before=rows[:-1]
    windows=_window_results(symbol,float(current["price"]),float(current["open_interest_usd"]),now,history_before)
    overall=_overall(windows); early=_early_transition(windows,overall); observations=_significance_observations(windows)
    return {"symbol":symbol,"price":float(current["price"]),"open_interest_usd":float(current["open_interest_usd"]),"windows":windows,"overall":overall,"early_transition":early,"significance_observations":observations,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES,
            "price_fetched_at":current.get("price_fetched_at"),"oi_fetched_at":current.get("oi_fetched_at"),
            "time_gap_seconds":current.get("time_gap_seconds"),"data_quality_status":current.get("data_quality_status"),
            "price_source":current.get("price_source"),"oi_source":current.get("oi_source")}

def composite_conclusion(regime,alert_side):
    # Backward-compatible with the original single-window Stage 77 payload,
    # while normal operation uses the multi-window overall conclusion.
    #
    # IMPORTANT: the Max-Pain LONG/SHORT label identifies the side expected
    # to be hurt, not the forecast price direction. Therefore the implied
    # price direction is the inverse of the alert label:
    #   Max-Pain LONG  -> longs hurt  -> price direction SHORT/down
    #   Max-Pain SHORT -> shorts hurt -> price direction LONG/up
    overall=regime.get("overall") or {}; side=str(alert_side or "").upper()
    implied_price_direction = "SHORT" if side == "LONG" else "LONG" if side == "SHORT" else side
    state=str(overall.get("state") or regime.get("state") or "")
    label=overall.get("label") or regime.get("label") or _state_label(state)
    agreement=overall.get("agreement")
    if state in {"","UNAVAILABLE","NEUTRAL_INCONCLUSIVE"}: return "אין כרגע מסקנת Price+OI כוללת; הציון הקיים נשאר עצמאי."
    if state=="MIXED_TRANSITION": return "Price+OI מציג Mixed / Transition; אין אישור כיווני כולל."
    direction="LONG" if state in {"BULLISH_BUILDUP","SHORT_COVERING"} else "SHORT"
    relation="תומך" if implied_price_direction==direction else "מנוגד"
    suffix=f" ({agreement}/5)" if agreement is not None else ""
    # Long Unwinding and Short Covering still participate in the same
    # support/opposition comparison. Their falling OI describes the quality of
    # the move; it must not bypass the directional conclusion.
    detail=""
    if state=="SHORT_COVERING":
        detail=" המחיר עולה, אך ה-OI יורד — ללא אישור של בניית OI חדש."
    elif state=="LONG_UNWINDING":
        detail=" המחיר יורד וה-OI יורד — ללא אישור של בניית OI חדש בכיוון הירידה."
    return f"Price+OI {relation} בכיוון {side}: {label}{suffix}.{detail}"

def attach_to_opportunities(items):
    cache={}
    for item in items:
        symbol=str(item.get("symbol","")).upper()
        if symbol not in cache: cache[symbol]=latest(symbol)
        item["market_regime"]=cache[symbol]
        item["composite_conclusion"]=composite_conclusion(cache[symbol],str(item.get("side","")))
    return items
