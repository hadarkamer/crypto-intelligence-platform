"""Causal pre-entry Spot 1m descriptions; never future MFE/MAE or regimes.

Each lookback uses N complete minutes ending at the last closed minute before
the immutable entry. Return is last close / first open - 1. Volatility is the
population standard deviation of one-minute log returns, expressed in percent;
the first return starts at the first candle open. UP/DOWN/FLAT denotes exact
return sign only and is not a trend or sideways-market classification.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import math
from statistics import pstdev
from typing import Any, Iterable, Mapping

import canonical_price_path
from research_common_window_metrics import utc
from research_ordered_first_touch import _field, _finite_number

METHOD_VERSION = "past-price-spot-1m-v1"
LOOKBACKS = (("15m",15),("30m",30),("1h",60),("4h",240),("12h",720),("24h",1440))


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),default=str,allow_nan=False).encode()).hexdigest()


def direction(value: float) -> str:
    return "UP" if value>0 else "DOWN" if value<0 else "FLAT"


def prior_bounds(event_time: Any) -> tuple[datetime,datetime]:
    end_exclusive=utc(event_time).replace(second=0,microsecond=0)
    return end_exclusive-timedelta(days=1),end_exclusive-timedelta(milliseconds=1)


def _window(*,symbol:str,event_time:datetime,minutes:int,candles:Iterable[Any],path_result:Mapping[str,Any]) -> dict[str,Any]:
    boundary=event_time.replace(second=0,microsecond=0)
    start=boundary-timedelta(minutes=minutes)
    end=boundary-timedelta(milliseconds=1)
    result={"status":"DATA_MISSING","lookback_minutes":minutes,"window_start_utc":start,
        "window_end_utc":end,"event_time_utc":event_time,"gap_to_entry_seconds":(event_time-boundary).total_seconds(),
        "expected_candles":minutes,"path_samples":0,"path_complete":False,
        "source":None,"path_sha256":None,"missing_reason":None}
    try:
        route=canonical_price_path.validated_route(symbol,dict(path_result),require_complete=False)
        if str(path_result.get("symbol") or "").upper()!=symbol:
            raise ValueError("PRICE_PATH_SYMBOL_MISMATCH")
    except (TypeError,ValueError):
        result["missing_reason"]="INVALID_OR_MISSING_SPOT_PRICE_PROVENANCE"
        return result
    result["source"]=route
    path=[]
    bad=False
    for candle in candles:
        try:
            opened=utc(_field(candle,"open_time_utc"))
            closed=utc(_field(candle,"close_time_utc"))
            # Do not inspect prices outside this past window. In particular,
            # current and later candle extrema can never become past features.
            if opened<start or opened>=boundary or closed>end:
                continue
            values={key:_finite_number(_field(candle,key),name="candle."+key) for key in ("open","high","low","close")}
            if (opened.second or opened.microsecond or closed-opened!=timedelta(seconds=59,milliseconds=999)
                    or min(values.values())<=0 or values["high"]<max(values.values()) or values["low"]>min(values.values())):
                raise ValueError("INVALID_CLOSED_1M_CANDLE")
            path.append({"open_time_utc":opened,"close_time_utc":closed,**values})
        except (TypeError,ValueError,KeyError,AttributeError):
            bad=True
    path.sort(key=lambda item:item["open_time_utc"])
    result["path_samples"]=len(path)
    complete=not bad and [item["open_time_utc"] for item in path]==[start+timedelta(minutes=i) for i in range(minutes)]
    result["path_complete"]=complete
    result["data_quality_status"]=canonical_price_path.quality_status(dict(path_result),complete=complete)
    result["path_sha256"]=digest({"source":route,"candles":path})
    if not complete:
        result["missing_reason"]="INVALID_OR_INCOMPLETE_CLOSED_1M_LOOKBACK"
        return result
    first,last=path[0]["open"],path[-1]["close"]
    high,low=max(item["high"] for item in path),min(item["low"] for item in path)
    returns=[]
    previous=first
    for item in path:
        returns.append(math.log(item["close"]/previous))
        previous=item["close"]
    net=100*(last/first-1)
    streak={"UP":0,"DOWN":0}
    longest={"UP":0,"DOWN":0}
    previous_sign=None
    turns=0
    for item in returns:
        sign=direction(item)
        for key in streak:
            streak[key]=streak[key]+1 if sign==key else 0
            longest[key]=max(longest[key],streak[key])
        if sign!="FLAT":
            if previous_sign is not None and sign!=previous_sign: turns+=1
            previous_sign=sign
    result.update({"status":"READY","first_open":first,"last_close":last,"high":high,"low":low,
        "return_pct":net,"direction":direction(net),"range_pct":100*(high-low)/first,
        "volatility_pct":100*pstdev(returns),"realized_volatility_pct":100*math.sqrt(sum(x*x for x in returns)),
        "drawdown_from_high_pct":100*(last/high-1),"rebound_from_low_pct":100*(last/low-1),
        "up_minutes":sum(x>0 for x in returns),"down_minutes":sum(x<0 for x in returns),"flat_minutes":sum(x==0 for x in returns),
        "longest_up_streak_minutes":longest["UP"],"longest_down_streak_minutes":longest["DOWN"],"direction_changes":turns})
    return result


def calculate_past_price_features(*,symbol:str,event_time:Any,candles:Iterable[Any],path_result:Mapping[str,Any],
        btc_candles:Iterable[Any]=(),btc_path_result:Mapping[str,Any]|None=None,computed_at:Any) -> dict[str,Any]:
    symbol=str(symbol).upper()
    entry,computed=utc(event_time),utc(computed_at)
    if computed<entry: raise ValueError("Past features cannot be recorded before their entry exists")
    own_path=list(candles)
    btc_path=own_path if symbol=="BTC" else list(btc_candles)
    btc_route=path_result if symbol=="BTC" else btc_path_result or {}
    windows={}
    for name,minutes in LOOKBACKS:
        own=_window(symbol=symbol,event_time=entry,minutes=minutes,candles=own_path,path_result=path_result)
        btc=own if symbol=="BTC" else _window(symbol="BTC",event_time=entry,minutes=minutes,candles=btc_path,path_result=btc_route)
        cell={"asset":own,"btc":btc,"relative_strength_pct":None,"relative_strength_direction":None}
        if own["status"]==btc["status"]=="READY":
            cell["relative_strength_pct"]=own["return_pct"]-btc["return_pct"]
            cell["relative_strength_direction"]=direction(cell["relative_strength_pct"])
        windows[name]=cell
    return record_from_windows(symbol=symbol,event_time=entry,windows=windows,computed_at=computed)


def record_from_windows(*,symbol:str,event_time:Any,windows:Mapping[str,Any],computed_at:Any) -> dict[str,Any]:
    """Freeze a common versioned record, also used to retain completed windows."""
    entry,computed=utc(event_time),utc(computed_at)
    ready=sum(cell["asset"]["status"]==cell["btc"]["status"]=="READY" for cell in windows.values())
    valid_assets=sum(cell["asset"]["status"]=="READY" for cell in windows.values())
    valid_btc=sum(cell["btc"]["status"]=="READY" for cell in windows.values())
    signature={"method_version":METHOD_VERSION,"symbol":symbol,"event_time_utc":entry,"windows":windows}
    return {**signature,"computed_at_utc":computed,"feature_sha256":digest(signature),
        "status":"READY" if ready==len(LOOKBACKS) else "PARTIAL" if valid_assets or valid_btc else "DATA_MISSING",
        "asset_windows_ready":valid_assets,"btc_windows_ready":valid_btc,"paired_windows_ready":ready,
        "market_regime_status":"UNCLASSIFIED_NO_DOCUMENTED_REGIME_RULE",
        "direction_policy":"EXACT_SIGN_OF_PAST_WINDOW_RETURN_NOT_MARKET_REGIME",
        "volatility_method":"POPULATION_STD_OF_1M_LOG_RETURNS_PCT_FIRST_OPEN_REFERENCE",
        "relative_strength_method":"ASSET_RETURN_PCT_MINUS_BTC_RETURN_PCT_IDENTICAL_CLOSED_WINDOW",
        "boundary_policy":"LAST_FULL_CLOSED_MINUTE_BEFORE_ENTRY_EXCLUDE_ENTRY_MINUTE"}


def flatten_event_features(record:Mapping[str,Any]|None,analysis_direction:str) -> dict[str,Any]:
    if not record or record.get("method_version")!=METHOD_VERSION or analysis_direction not in {"LONG","SHORT"}:
        return {}
    features={"historical.closed_1m.method_version":METHOD_VERSION}
    def aligned(sign):
        return "FLAT" if sign=="FLAT" else "SUPPORTS" if (sign=="UP")== (analysis_direction=="LONG") else "OPPOSES"
    for name,minutes in LOOKBACKS:
        cell=(record.get("windows") or {}).get(name) or {}
        own,btc=cell.get("asset") or {},cell.get("btc") or {}
        prefix="historical.closed_1m."+name+"."
        def valid(window,symbol):
            try:
                entry=utc(record['event_time_utc'])
                boundary=entry.replace(second=0,microsecond=0)
                source=dict(window.get('source') or {})
                canonical_price_path.validated_route(symbol,{**source,'api_coin':source.get('instrument')},require_complete=False)
                for key in ('return_pct','range_pct','volatility_pct','realized_volatility_pct',
                        'drawdown_from_high_pct','rebound_from_low_pct','up_minutes','down_minutes','flat_minutes',
                        'longest_up_streak_minutes','longest_down_streak_minutes','direction_changes'):
                    _finite_number(window.get(key),name='past.'+key)
                return (window.get("status")=="READY" and window.get("path_complete") is True
                    and window.get("lookback_minutes")==minutes and window.get("path_samples")==minutes
                    and utc(window['window_start_utc'])==boundary-timedelta(minutes=minutes)
                    and utc(window['window_end_utc'])==boundary-timedelta(milliseconds=1)
                    and utc(window['event_time_utc'])==entry and source.get('symbol')==symbol
                    and window.get("data_quality_status")==canonical_price_path.quality_status(source,complete=True)
                    and window.get('direction')==direction(_finite_number(window.get('return_pct'),name='return_pct')))
            except (TypeError,ValueError,KeyError):return False
        own_valid,btc_valid=valid(own,record.get('symbol')),valid(btc,'BTC')
        if own_valid:
            for key in ("return_pct","direction","range_pct","volatility_pct","realized_volatility_pct",
                    "drawdown_from_high_pct","rebound_from_low_pct","up_minutes","down_minutes","flat_minutes",
                    "longest_up_streak_minutes","longest_down_streak_minutes","direction_changes"):
                features[prefix+key]=own[key]
            features[prefix+"alignment"]=aligned(own["direction"])
        if btc_valid:
            for key in ("return_pct","direction","range_pct","volatility_pct"):
                features[prefix+"btc_"+key]=btc[key]
            features[prefix+"btc_alignment"]=aligned(btc["direction"])
        if own_valid and btc_valid:
            relative=own['return_pct']-btc['return_pct']
            features[prefix+"relative_strength_pct"]=relative
            features[prefix+"relative_strength_direction"]=direction(relative)
    return features
