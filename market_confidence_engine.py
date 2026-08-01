"""Stage 89.1: deterministic evidence-agreement integration.

Three independent families are reported without arbitrary weights:
1. Price + OI positioning
2. Futures CVD flow
3. Spot CVD flow

The output is a count and verbal conclusion, never a probability or score.
It does not modify Max-Pain score, ranking, Watch activation or position size.
"""
from __future__ import annotations

from copy import deepcopy
import time
from typing import Any, Dict, List, Optional, Tuple

import coinglass_flow_engine
import coinglass_oi_regime_service

_FLOW_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_FLOW_CACHE_TTL_SECONDS = 300

def _cached_flow(symbol: str) -> Dict[str, Any]:
    symbol=str(symbol or "").upper(); now=time.monotonic(); cached=_FLOW_CACHE.get(symbol)
    if cached and now-cached[0] <= _FLOW_CACHE_TTL_SECONDS:
        return deepcopy(cached[1])
    result=coinglass_flow_engine.analyze_symbol(symbol)
    _FLOW_CACHE[symbol]=(now,deepcopy(result)); return result

def _price_direction_from_side(side: Any) -> str:
    side=str(side or "").upper()
    return "BEARISH" if side=="LONG" else "BULLISH" if side=="SHORT" else "NEUTRAL"

def _positioning_module(regime: Dict[str, Any]) -> Dict[str, Any]:
    overall=regime.get("overall") or {}; state=str(overall.get("state") or "UNAVAILABLE").upper()
    mapping={
        "BULLISH_BUILDUP":"BULLISH", "SHORT_COVERING":"BULLISH",
        "BEARISH_BUILDUP":"BEARISH", "LONG_UNWINDING":"BEARISH",
        "MIXED_TRANSITION":"MIXED",
    }
    direction=mapping.get(state,"NEUTRAL")
    available=bool(regime.get("available")) and str(regime.get("data_quality_status") or "PASS").upper()!="INVALID"
    if not available: direction="NEUTRAL"
    return {
        "family":"Price+OI", "available":available, "direction":direction,
        "state":state, "label":str(overall.get("label") or state).replace("_"," "),
        "agreement":int(overall.get("agreement") or 0),
        "valid_windows":int(overall.get("valid_windows") or 0),
    }

def _flow_module(data: Dict[str, Any], family: str) -> Dict[str, Any]:
    overall=data.get("overall") or {}; raw=str(overall.get("direction") or "NEUTRAL").upper()
    direction=raw if raw in {"BULLISH","BEARISH"} else "MIXED" if str(overall.get("state") or "").upper()=="MIXED" else "NEUTRAL"
    quality=data.get("quality") or {}; status=str(quality.get("status") or "NO_DATA").upper()
    available=bool(data.get("available")) and status in {"PASS","WARNING"}
    if not available: direction="NEUTRAL"
    return {
        "family":family, "available":available, "direction":direction,
        "state":str(overall.get("state") or "NO_DATA").upper(),
        "label":str(overall.get("state") or "No data").replace("_"," ").title(),
        "quality":status, "quality_reasons":list(quality.get("reasons") or []),
    }

def _conclusion(modules: Dict[str, Dict[str, Any]], expected: str) -> Dict[str, Any]:
    counts={"BULLISH":0,"BEARISH":0,"NEUTRAL":0,"MIXED":0}
    for module in modules.values():
        d=str(module.get("direction") or "NEUTRAL").upper()
        counts[d if d in counts else "NEUTRAL"]+=1
    bull,bear,neutral,mixed=counts["BULLISH"],counts["BEARISH"],counts["NEUTRAL"],counts["MIXED"]
    dominant="NEUTRAL"; code="NO_DIRECTIONAL_EVIDENCE"; label="אין כרגע עדות שוק כיוונית"
    if bull==3 or bear==3:
        dominant="BULLISH" if bull==3 else "BEARISH"; code="FULL_CONFIRMATION"; label=f"אישור {'שורי' if dominant=='BULLISH' else 'דובי'} מלא — 3/3"
    elif bull>=2 or bear>=2:
        dominant="BULLISH" if bull>=2 else "BEARISH"
        opposite=bear if dominant=="BULLISH" else bull
        if opposite:
            code="MAJORITY_WITH_CONFLICT"; label=f"רוב {'שורי' if dominant=='BULLISH' else 'דובי'} 2/3, עם מקור מתנגד"
        else:
            code="CLEAR_CONFIRMATION"; label=f"אישור {'שורי' if dominant=='BULLISH' else 'דובי'} ברור — 2/3"
    elif bull==1 and bear==0:
        dominant="BULLISH"; code="WEAK_EVIDENCE"; label="עדות שורית חלשה — מקור אחד בלבד"
    elif bear==1 and bull==0:
        dominant="BEARISH"; code="WEAK_EVIDENCE"; label="עדות דובית חלשה — מקור אחד בלבד"
    elif bull and bear:
        code="CONFLICT_NO_DIRECTION"; label="קונפליקט — אין אישור כיווני"
    elif mixed:
        code="MIXED_NO_DIRECTION"; label="תמונה מעורבת — אין אישור כיווני"

    relation="NO_CONFIRMATION"
    if expected in {"BULLISH","BEARISH"} and dominant in {"BULLISH","BEARISH"}:
        relation="SUPPORT" if dominant==expected else "CONFLICT"
    elif code in {"CONFLICT_NO_DIRECTION","MAJORITY_WITH_CONFLICT","MIXED_NO_DIRECTION"}:
        relation="CONFLICT"
    return {"counts":counts,"dominant_direction":dominant,"classification":code,"classification_label":label,"relation_to_alert":relation}

def combine(symbol: str, expected_price_direction: Optional[str], regime: Optional[Dict[str,Any]]=None, flow: Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
    symbol=str(symbol or "").upper(); expected=str(expected_price_direction or "NEUTRAL").upper()
    if expected in {"LONG","SHORT"}: expected="BULLISH" if expected=="LONG" else "BEARISH"
    regime=regime if regime is not None else coinglass_oi_regime_service.latest(symbol)
    flow=flow if flow is not None else _cached_flow(symbol)
    modules={
        "positioning":_positioning_module(regime),
        "futures_flow":_flow_module(flow.get("futures") or {},"Futures Flow"),
        "spot_flow":_flow_module(flow.get("spot") or {},"Spot Flow"),
    }
    result=_conclusion(modules,expected)
    return {"symbol":symbol,"expected_price_direction":expected,"modules":modules,**result,
            "note":"Read-only evidence agreement; Max-Pain score and ranking are unchanged."}


def max_pain_side_to_price_direction(max_pain_side: Any) -> str:
    """Backward-compatible helper: Max-Pain side is inverse price direction."""
    side=str(max_pain_side or "").upper()
    return "LONG" if side=="SHORT" else "SHORT" if side=="LONG" else "NEUTRAL"

def attach_to_opportunities(items: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    flow_cache={}; regime_cache={}
    for item in items:
        symbol=str(item.get("symbol") or "").upper()
        if symbol not in flow_cache:
            try: flow_cache[symbol]=_cached_flow(symbol)
            except Exception as exc:
                flow_cache[symbol]={"futures":{"available":False,"quality":{"status":"NO_DATA","reasons":[repr(exc)]}},"spot":{"available":False,"quality":{"status":"NO_DATA","reasons":[repr(exc)]}}}
        if symbol not in regime_cache:
            regime_cache[symbol]=item.get("market_regime") or coinglass_oi_regime_service.latest(symbol)
        expected=_price_direction_from_side(item.get("side"))
        item["flow_context"]=flow_cache[symbol]
        item["market_evidence"]=combine(symbol,expected,regime_cache[symbol],flow_cache[symbol])
    return items
