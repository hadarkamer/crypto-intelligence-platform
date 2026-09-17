"""Versioned record-only delivery contract. Standard library, no I/O on import.

Prices are copied from the exact delivered text, never invented from a threshold.
This contract conveys observed notifications, not authority to execute a trade.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import html
import json
import re

VERSION = 'delivered-alert-card-v1'
MAX_BYTES = 16384
PATH = '/internal/testnet-cards/v1'
HOST = 'hl-testnet-check-yoyo.onrender.com'
FIELDS = {'version','family','scope_hash','intent_id','rule_id','symbol','side',
          'source_at','delivered_at','expires_at','message_id','threshold_bps',
          'text','reference'}
REFERENCE = {'status','symbol','price','price_time_utc','anchor_time_utc','source'}
IDENT = re.compile(r'[A-Za-z0-9_.:-]{1,100}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')
NUMBER = r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?'

class WireError(ValueError):
    """Fixed diagnostic codes only."""


def encoded(value):
    try:
        body = json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if len(body)>MAX_BYTES: raise ValueError()
        return body
    except (ValueError,TypeError,RecursionError):
        raise WireError('DELIVERY_TOO_LARGE_OR_INVALID') from None


def decoded(raw):
    def pairs(items):
        result={}
        for k,v in items:
            if k in result: raise WireError('DUPLICATE_JSON_FIELD')
            result[k]=v
        return result
    def bad(_): raise WireError('NONFINITE_JSON')
    try:
        if not isinstance(raw,bytes) or not 0<len(raw)<=MAX_BYTES: raise ValueError()
        return json.loads(raw,object_pairs_hook=pairs,parse_constant=bad)
    except (ValueError,UnicodeError,RecursionError):
        raise WireError('INVALID_DELIVERY_JSON') from None


def moment(value):
    try:
        if not isinstance(value,str) or len(value)>40: raise ValueError()
        at=datetime.fromisoformat(value.replace('Z','+00:00'))
        if at.utcoffset() is None: raise ValueError()
        return at.astimezone(timezone.utc)
    except (ValueError,TypeError): raise WireError('SOURCE_TIME_REQUIRED') from None


def positive(value):
    try:
        if not isinstance(value,str) or len(value)>80 or not re.fullmatch(NUMBER,value): raise ValueError()
        n=Decimal(value)
        if not n.is_finite() or not Decimal('1e-15')<=n<=Decimal('1e15'): raise ValueError()
        return n
    except (ValueError,InvalidOperation): raise WireError('INVALID_SOURCE_NUMBER') from None


def signature(key, stamp, raw):
    if not isinstance(key,str) or not HEX.fullmatch(key): raise WireError('INTAKE_AUTH_NOT_CONFIGURED')
    return hmac.new(bytes.fromhex(key),str(stamp).encode()+b'\n'+raw,hashlib.sha256).hexdigest()


def authenticate(key, stamp, digest, raw, now):
    if (not isinstance(stamp,str) or not re.fullmatch(r'[0-9]{10}',stamp)
            or abs(now-int(stamp))>60 or not isinstance(digest,str) or not HEX.fullmatch(digest)):
        return False
    try: return hmac.compare_digest(signature(key,stamp,raw),digest)
    except WireError: return False


def ref_only(value):
    # Drop unrelated provider details; never forward the source environment.
    return {k:value.get(k) for k in REFERENCE} if isinstance(value,dict) else None


def manual_delivery(item, scope_hash):
    p=item.get('payload') or {}
    return dict(version=VERSION,family='manual',scope_hash=scope_hash,
        intent_id=item.get('intent_id'),rule_id=p.get('rule_id'),symbol=p.get('symbol'),side=p.get('direction'),
        source_at=p.get('event_time'),delivered_at=item.get('acknowledged_at'),expires_at=item.get('expires_at'),
        message_id=item.get('message_id'),threshold_bps=p.get('threshold_bps'),text=item.get('text'),
        reference=ref_only(p.get('price_reference')))


def dual_delivery(row, scope_hash):
    p=row.get('payload') or {}; obs=p.get('observation') or {}
    def iso(v): return v.isoformat() if isinstance(v,datetime) else v
    return dict(version=VERSION,family='dual_cvd65',scope_hash=scope_hash,
        intent_id=row.get('intent_id'),rule_id=row.get('rule_id'),symbol=row.get('symbol'),side=row.get('direction'),
        source_at=iso(row.get('source_at_utc')),delivered_at=iso(row.get('finished_at_utc')),expires_at=iso(row.get('expires_at')),
        message_id=None,threshold_bps=None,text=row.get('text'),reference=ref_only(obs.get('price_reference')))


def normalize(value):
    if not isinstance(value,dict) or set(value)!=FIELDS or value['version']!=VERSION:
        raise WireError('UNSUPPORTED_DELIVERY_CONTRACT')
    if value['family'] not in ('manual','dual_cvd65') or not isinstance(value['scope_hash'],str) or not HEX.fullmatch(value['scope_hash']):
        raise WireError('UNKNOWN_DELIVERY_SOURCE')
    for k in ('intent_id','rule_id'):
        if not isinstance(value[k],str) or not IDENT.fullmatch(value[k]): raise WireError('STABLE_DELIVERY_ID_REQUIRED')
    symbol,side=value['symbol'],value['side']
    if not isinstance(symbol,str) or not re.fullmatch(r'[A-Z][A-Z0-9]{0,19}',symbol) or side not in ('LONG','SHORT'):
        raise WireError('FINAL_SOURCE_DIRECTION_REQUIRED')
    source,delivered,expiry=(moment(value[k]) for k in ('source_at','delivered_at','expires_at'))
    if delivered<source or not 0<(expiry-source).total_seconds()<=600:
        raise WireError('INCONSISTENT_SOURCE_TIMES')
    mid=value['message_id']
    if value['family']=='manual' and (type(mid) is not int or mid<=0):
        raise WireError('DELIVERY_RECEIPT_REQUIRED')
    if value['family']=='dual_cvd65' and mid is not None:
        raise WireError('UNSUPPORTED_DELIVERY_RECEIPT')
    raw=value['text']
    if not isinstance(raw,str) or not 0<len(raw)<=12000: raise WireError('DELIVERED_TEXT_REQUIRED')
    # Current producer format is explicit. Unknown markup does not get guessed.
    plain=re.sub(r'</?(?:b|i|strong|em|code)>','',raw)
    if '<' in plain or '>' in plain: raise WireError('UNSUPPORTED_MESSAGE_FORMAT')
    plain=html.unescape(plain)
    def one(pattern):
        found=re.findall(pattern,plain,re.MULTILINE)
        if len(found)!=1: raise WireError('MISSING_OR_AMBIGUOUS_MESSAGE_FIELD')
        return found[0]
    heading=one(r'^([A-Z][A-Z0-9]{0,19})\s*\|[^\n]*?—\s*(LONG|SHORT)\s*$')
    if heading!=(symbol,side): raise WireError('MESSAGE_DIRECTION_MISMATCH')
    threshold=one(r'^(?:🧪\s*)?סף\s+('+NUMBER+r')%[^\n]*$')
    if positive(threshold)>100: raise WireError('INVALID_FORMULA_THRESHOLD')
    bps=value['threshold_bps']
    if bps is not None and (type(bps) is not int or Decimal(bps)!=Decimal(threshold)*100):
        raise WireError('MESSAGE_THRESHOLD_MISMATCH')
    entry=one(r'^שער בסיס[^\n]*:\s*('+NUMBER+r')\s*$')
    stop=one(r'^סטופלוס:\s*('+NUMBER+r')\s*$')
    take=one(r'^טייק פרופיט:\s*('+NUMBER+r')\s*$')
    e,s,t=map(positive,(entry,stop,take))
    if not (s<e<t if side=='LONG' else t<e<s): raise WireError('INCONSISTENT_SOURCE_PRICES')
    ref=value['reference']
    if not isinstance(ref,dict) or set(ref)!=REFERENCE or ref['status']!='READY' or ref['symbol']!=symbol:
        raise WireError('READY_SOURCE_REFERENCE_REQUIRED')
    if positive(ref['price'])!=e: raise WireError('MESSAGE_ENTRY_REFERENCE_MISMATCH')
    if moment(ref['price_time_utc'])>source or moment(ref['anchor_time_utc'])>source:
        raise WireError('REFERENCE_TIME_AFTER_SIGNAL')
    if not isinstance(ref['source'],str) or not IDENT.fullmatch(ref['source']): raise WireError('REFERENCE_SOURCE_REQUIRED')
    signal=dict(kind='SIGNAL',event_id=value['intent_id'],symbol=symbol,side=side,
                entry=entry,stop=stop,take_profit=take,at=value['source_at'])
    return dict(signal=signal,rule_id=value['rule_id'],threshold_pct=threshold,
                source_stream=value['family']+':'+value['scope_hash'])
