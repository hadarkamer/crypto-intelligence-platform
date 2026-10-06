"""Stage 77: independent multi-window Price + Open Interest regime layer.

Snapshots are collected every 30 minutes. Regime conclusions are calculated
independently for 30m, 1h, 4h, 12h and 24h. No Max-Pain score is modified.
Historical Price/OI distributions are used as a symbol+timeframe-specific
minimum and strength reference. P25 is the minimum valid movement. No fixed
percentage threshold is shared between coins or timeframes.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional

import requests

import time_family_engine

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
OI_FETCH_WORKERS = 4
REGIME_WINDOWS_MINUTES = (30, 60, 240, 720, 1440, 2880, 4320, 10080)
WINDOW_LABELS = {
    30: "30m", 60: "1h", 240: "4h", 720: "12h", 1440: "24h",
    2880: "48h", 4320: "72h", 10080: "7d",
}
DATABASE_URL = os.getenv("DATABASE_URL", "")
_SCHEMA_INITIALIZED_FOR = None
_SCHEMA_ADVISORY_LOCK_ID = 94837211
DB_PATH = os.getenv("DB_PATH", "coinglass.db")

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
    global _SCHEMA_INITIALIZED_FOR
    schema_key = ("postgres", DATABASE_URL) if _use_postgres() else ("sqlite", DB_PATH)
    if _SCHEMA_INITIALIZED_FOR == schema_key:
        return
    if _use_postgres():
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_ADVISORY_LOCK_ID,))
            exists = conn.execute(
                "SELECT to_regclass('public.oi_regime_snapshots') AS relation"
            ).fetchone()["relation"] is not None
            if not exists:
                conn.execute(POSTGRES_SCHEMA)
            rows = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='oi_regime_snapshots'"
            ).fetchall()
            existing = {str(row["column_name"]) for row in rows}
            for column, ctype in (
                    ("price_fetched_at", "TIMESTAMPTZ"),
                    ("oi_fetched_at", "TIMESTAMPTZ"),
                    ("time_gap_seconds", "DOUBLE PRECISION"),
                    ("data_quality_status", "TEXT"),
                    ("price_source", "TEXT"),
                    ("oi_source", "TEXT"),
                ):
                if column not in existing:
                    conn.execute(f"ALTER TABLE oi_regime_snapshots ADD COLUMN {column} {ctype}")
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
    _SCHEMA_INITIALIZED_FOR = schema_key

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

def _classify_with_historical_reference(symbol, window_label, price_change_pct, oi_change_pct, window_start=None, window_end=None):
    """Classify direction only when both movements clear their own historical P25.

    If no historical reference exists for this symbol/window, preserve the
    pre-reference Stage 77 classifier so non-backfilled symbols keep working.
    """
    base = classify(symbol, price_change_pct, oi_change_pct)
    d = base.to_dict()
    if window_start is None or window_end is None:
        ref = history_reference.reference_for_window(symbol, window_label)
    else:
        try:
            ref = history_reference.reference_for_window(symbol, window_label, window_start, window_end)
        except TypeError:
            # Backward-compatible with injected test/legacy two-argument providers.
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
    """Choose the snapshot nearest to the requested window within tolerance.

    Both the live snapshot table and the separate historical backfill table are
    considered. This prevents a stale live row from blocking a much closer
    historical candle, and avoids a systematic bias toward the candle before
    the requested time.
    """
    target = now - timedelta(minutes=minutes)
    candidates = []

    for row in rows:
        if str(row.get("data_quality_status") or "PASS").upper() == "INVALID":
            continue
        candidate = dict(row)
        candidate.setdefault("source", "live_snapshot")
        candidates.append(candidate)

    if symbol:
        historical = history_reference.historical_point_nearest(symbol, target)
        # Backward-compatible fallback for existing installations/tests that
        # provide only the previous at-or-before bridge.
        if not historical:
            historical = history_reference.historical_point_at_or_before(symbol, target)
        if historical:
            candidates.append(dict(historical))

    if not candidates:
        return None

    ref = min(
        candidates,
        key=lambda row: abs((_as_utc(row["collected_at"]) - target).total_seconds()),
    )
    ref_time = _as_utc(ref["collected_at"])
    signed_offset_seconds = (ref_time - target).total_seconds()
    absolute_offset_seconds = abs(signed_offset_seconds)
    if absolute_offset_seconds > REFERENCE_TOLERANCE_MINUTES * 60:
        return None

    ref["reference_offset_seconds"] = absolute_offset_seconds
    ref["reference_signed_offset_seconds"] = signed_offset_seconds
    ref["reference_target_time"] = target.isoformat()
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
            d=_classify_with_historical_reference(symbol,label,pchange,ochange,_as_utc(ref["collected_at"]),now)
            d["comparison_source"]=ref.get("source","live_snapshot")
            d["reference_time"]=_as_utc(ref["collected_at"]).isoformat()
            d["reference_offset_seconds"]=ref.get("reference_offset_seconds")
            d["reference_signed_offset_seconds"]=ref.get("reference_signed_offset_seconds")
            d["reference_target_time"]=ref.get("reference_target_time")
        d["window_minutes"]=minutes; d["window_label"]=label; out[label]=d
    return out

def _overall(windows):
    """Aggregate all available Price+OI windows by a true majority.

    The denominator is dynamic because a newly deployed installation may not
    yet have every long window. Neutral is a real outcome. A directional or
    neutral state is confirmed only when it reaches a strict majority of the
    available windows.
    """
    available=[w for w in windows.values() if w.get("available") and w.get("state") != "UNAVAILABLE"]
    total=len(available)
    if not available:
        return {"state":"NEUTRAL_INCONCLUSIVE","label":"Neutral / Inconclusive","strength":"Inconclusive","agreement":0,"valid_windows":0}

    majority=(total//2)+1
    counts={}
    for w in available:
        state=w.get("state") or "NEUTRAL_INCONCLUSIVE"
        counts[state]=counts.get(state,0)+1
    state,n=max(counts.items(),key=lambda kv:kv[1])

    if n < majority:
        return {"state":"MIXED_TRANSITION","label":"Mixed / Transition","strength":"Mixed / Transition",
                "agreement":n,"valid_windows":total}

    ratio=n/total if total else 0.0
    if n == total:
        strength="Strong"
    elif ratio >= 0.75:
        strength="Strong / Confirmed"
    else:
        strength="Confirmed"
    label="Neutral / Inconclusive" if state=="NEUTRAL_INCONCLUSIVE" else _state_label(state)
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
    broad=[windows.get("4h",{}),windows.get("12h",{}),windows.get("24h",{}),windows.get("48h",{}),windows.get("72h",{}),windows.get("7d",{})]
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
        for attempt in range(3):
            try:
                with psycopg.connect(DATABASE_URL,row_factory=dict_row) as conn:
                    conn.execute(sql.replace("?","%s"),params)
                    conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < %s",(now-timedelta(days=HISTORY_RETENTION_DAYS),))
                    conn.commit()
                break
            except psycopg.errors.DeadlockDetected:
                if attempt >= 2:
                    raise
                time.sleep(0.4 * (attempt + 1))
    else:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(sql,params); conn.execute("DELETE FROM oi_regime_snapshots WHERE collected_at < ?",((now-timedelta(days=HISTORY_RETENTION_DAYS)).isoformat(),)); conn.commit()
    return {"price_fetched_at":pt.isoformat(),"oi_fetched_at":ot.isoformat(),"time_gap_seconds":gap,"data_quality_status":status,
            "price_source":price_meta.get("source") or "unknown","oi_source":oi_meta.get("source") or "unknown"}


def _invalid_snapshot_parts(symbol, status, gap):
    windows={label:{**classify(symbol,None,None).to_dict(),"window_label":label,"window_minutes":minutes,"data_quality_status":status,"time_gap_seconds":gap}
             for minutes,label in WINDOW_LABELS.items()}
    overall={"state":"UNAVAILABLE","label":"Price/OI timestamp gap too large","strength":"Unavailable","agreement":0,"valid_windows":0}
    weighted={"direction":"NEUTRAL","score":0.0,"quality":"Unavailable","families":{}}
    return windows,overall,weighted


def _collect_symbol_with_oi_meta(symbol,price_input,oi_meta):
    symbol=str(symbol or "").upper()
    if isinstance(price_input,dict):
        price=float(price_input.get("price")); price_meta={"fetched_at":price_input.get("fetched_at_utc") or price_input.get("fetched_at"),"source":price_input.get("source")}
    else:
        price=float(price_input); price_meta={"fetched_at":datetime.now(timezone.utc),"source":"legacy_price"}
    oi=float(oi_meta["value"]); now=datetime.now(timezone.utc); rows=_history(symbol)
    gap,status=_quality_status(_as_utc(price_meta.get("fetched_at") or now),_as_utc(oi_meta.get("fetched_at") or now))
    if status=="INVALID":
        windows,overall,weighted=_invalid_snapshot_parts(symbol,status,gap)
        early=False; observations=[]
    else:
        windows=_window_results(symbol,price,oi,now,rows); weighted=time_family_engine.aggregate(windows,time_family_engine.oi_window_evaluator); overall=_overall(windows); overall.update({"weighted_direction":weighted["direction"],"weighted_score":weighted["score"],"weighted_quality":weighted["quality"]}); early=_early_transition(windows,overall); observations=_significance_observations(windows)
    r30=windows["30m"]; legacy=RegimeResult(symbol,r30["state"],r30["label"],r30["direction"],r30.get("price_change_pct"),r30.get("oi_change_pct"),r30["reason"],r30["available"])
    quality=_insert_snapshot(symbol,price,oi,legacy,price_meta,oi_meta)
    return {"symbol":symbol,"price":price,"open_interest_usd":oi,"windows":windows,"time_families":weighted["families"],"overall":overall,"early_transition":early,"significance_observations":observations,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES,**quality}


def collect_symbol(symbol,price_input):
    symbol=str(symbol or "").upper()
    return _collect_symbol_with_oi_meta(
        symbol,
        price_input,
        fetch_aggregated_oi_with_meta(symbol),
    )

def collect_many(symbol_prices):
    """Collect OI close to the shared live-price timestamp, then write serially.

    Only the network reads run concurrently. Classification and database writes
    remain serial, preserving the existing locking and schema behaviour while
    preventing late alphabetic symbols from exceeding the 60-second pairing
    tolerance solely because earlier CoinGlass requests occupied the queue.
    """
    out={}
    ordered=sorted(symbol_prices.items())
    oi_results={}
    oi_errors={}
    if ordered:
        workers=min(OI_FETCH_WORKERS,len(ordered))
        with ThreadPoolExecutor(max_workers=workers,thread_name_prefix="oi-fetch") as pool:
            futures={pool.submit(fetch_aggregated_oi_with_meta,symbol):symbol for symbol,_price in ordered}
            for future in as_completed(futures):
                symbol=futures[future]
                try:
                    oi_results[symbol]=future.result()
                except Exception as exc:
                    oi_errors[symbol]=exc

    for symbol,price in ordered:
        try:
            if symbol in oi_errors:
                raise oi_errors[symbol]
            out[symbol]=_collect_symbol_with_oi_meta(symbol,price,oi_results[symbol])
        except Exception as exc:
            print(f"[oi-regime] {symbol} failed: {type(exc).__name__}: {exc}")
            out[symbol]={"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"נתוני Price + OI לא זמינים","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":str(exc),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    return out

def latest(symbol):
    symbol=str(symbol or "").upper(); rows=_history(symbol)
    if not rows: return {"symbol":symbol,"windows":{},"overall":{"state":"UNAVAILABLE","label":"אין נתוני Price + OI","strength":"Unavailable","agreement":0,"valid_windows":0},"early_transition":False,"available":False,"reason":"טרם נאספה דגימת Price + OI.","collection_interval_minutes":COLLECTION_INTERVAL_MINUTES}
    current=rows[-1]; now=_as_utc(current["collected_at"])
    history_before=rows[:-1]
    quality_status=str(current.get("data_quality_status") or "PASS").upper()
    time_gap=current.get("time_gap_seconds")
    if quality_status=="INVALID":
        windows,overall,weighted=_invalid_snapshot_parts(symbol,quality_status,time_gap)
        early=False; observations=[]
    else:
        windows=_window_results(symbol,float(current["price"]),float(current["open_interest_usd"]),now,history_before)
        weighted=time_family_engine.aggregate(windows,time_family_engine.oi_window_evaluator); overall=_overall(windows); overall.update({"weighted_direction":weighted["direction"],"weighted_score":weighted["score"],"weighted_quality":weighted["quality"]}); early=_early_transition(windows,overall); observations=_significance_observations(windows)
    return {"symbol":symbol,"price":float(current["price"]),"open_interest_usd":float(current["open_interest_usd"]),"windows":windows,"time_families":weighted["families"],"overall":overall,"early_transition":early,"significance_observations":observations,"available":any(w.get("available") for w in windows.values()),"collection_interval_minutes":COLLECTION_INTERVAL_MINUTES,
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
    valid_windows=overall.get("valid_windows") or 5
    suffix=f" ({agreement}/{valid_windows})" if agreement is not None else ""
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
