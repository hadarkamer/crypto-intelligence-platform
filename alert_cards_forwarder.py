"""Opt-in one-way records from delivered bot outboxes to Testnet card storage.

Does NOT change alert generation, Telegram transport, prices or source records.
Independent task, read-only SQL, no retries of Telegram or exchange operations.
Source retains manual delivery evidence for about an hour: longer outages are
reported as a coverage gap, not described as complete recovery.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import os
import re
import time
from urllib.parse import urlsplit

import alert_cards_wire as wire

SERVICE = 'srv-d94ek17lk1mc73b4tb90'
MODE = 'record_only_v1'
_TASK = None
_SCOPES = set()
DELAY = 30
MAX_PER_PASS = 16


def config(env):
    if env.get('ALERT_CARDS_FORWARD_MODE')!=MODE: return None
    key=env.get('ALERT_CARDS_FORWARD_SECRET','')
    if env.get('RENDER_SERVICE_ID')!=SERVICE or not wire.HEX.fullmatch(key):
        raise wire.WireError('FORWARD_CONFIGURATION_REQUIRED')
    fence=wire.moment(env.get('ALERT_CARDS_FORWARD_NOT_BEFORE',''))
    return key,fence


def read_delivered(scopes, fence):
    import psycopg
    from psycopg.rows import dict_row
    dsn=os.environ.get('DATABASE_URL','')
    # This process already has this DB connection. It never sends it to receiver.
    u=urlsplit(dsn)
    if (u.scheme not in ('postgres','postgresql') or u.hostname not in
        ('dpg-d94d641kh4rs73evvih0-a','dpg-d94d641kh4rs73evvih0-a.oregon-postgres.render.com')
            or u.path!='/crypto_intelligence_db'):
        raise wire.WireError('EXPECTED_SOURCE_DATABASE_REQUIRED')
    cutoff=max(fence,datetime.now(timezone.utc)-timedelta(hours=1))
    result=[]
    with psycopg.connect(dsn,connect_timeout=3,row_factory=dict_row,
        options='-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000') as conn:
        for scope in sorted(scopes):
            row=conn.execute('SELECT value FROM bot_settings WHERE key=%s',
                            ('manual-four-formulas-outbox-v1:'+scope,)).fetchone()
            if row:
                raw=row['value']
                if not isinstance(raw,str) or len(raw.encode())>1024*1024:
                    raise wire.WireError('SOURCE_STATE_TOO_LARGE')
                state=json.loads(raw)
                if state.get('version')!='manual-four-formulas-outbox-v1':
                    raise wire.WireError('UNSUPPORTED_SOURCE_VERSION')
                intents=state.get('intents')
                if not isinstance(intents,list): raise wire.WireError('SOURCE_STATE_INVALID')
                for item in intents:
                    if item.get('status')=='DELIVERED' and wire.moment(item['acknowledged_at'])>=cutoff:
                        result.append(wire.manual_delivery(item,scope))
            rows=conn.execute('''SELECT intent_id,rule_id,symbol,direction,source_at_utc,
                    expires_at,finished_at_utc,text,payload FROM dual_cvd65_intents
                WHERE subscription_scope=%s AND status='DELIVERED' AND finished_at_utc>=%s
                ORDER BY finished_at_utc,intent_id LIMIT 513''',('general-watch:'+scope,cutoff)).fetchall()
            if len(rows)>512: raise wire.WireError('SOURCE_PAGE_OVERFLOW_REQUIRES_REVIEW')
            result.extend(wire.dual_delivery(row,scope) for row in rows)
    return result


def post_record(value, key):
    raw=wire.encoded(value); stamp=str(int(time.time()))
    conn=http.client.HTTPSConnection(wire.HOST,timeout=4)
    try:
        conn.request('POST',wire.PATH,raw,{'Content-Type':'application/json',
            'X-Card-Timestamp':stamp,'X-Card-Signature':wire.signature(key,stamp,raw)})
        reply=conn.getresponse(); payload=reply.read(2049)
        if reply.status!=200 or len(payload)>2048:
            raise wire.WireError('CARD_RECEIVER_UNAVAILABLE')
        out=wire.decoded(payload)
        if (out.get('receipt_id')!=hashlib.sha256(raw).hexdigest()
                or out.get('record_only') is not True
                or out.get('status') not in ('RECORDED','DUPLICATE','REJECTED')):
            raise wire.WireError('CARD_ACKNOWLEDGEMENT_NOT_VERIFIED')
        return out['status']
    finally: conn.close()


class Forwarder:
    def __init__(self):
        self.done={}; self.retry_after={}; self.last_ok=None; self.started=time.monotonic()
        self.coverage_gap=False; self.cycles=0

    def pass_once(self, scopes, key, fence, *, reader=read_delivered, post=post_record):
        started=time.monotonic(); now=datetime.now(timezone.utc)
        report=dict(mode=MODE,status='FORWARD_PASS_COMPLETED',recorded=0,duplicates=0,
            rejected=0,deferred=0,attempted=0,source_records_changed=0,
            order_requests_sent=0,observed_at_utc=now.isoformat())
        self.cycles+=1
        if self.last_ok is not None and started-self.last_ok>=3600: self.coverage_gap=True
        try:
            values=reader(scopes,fence)
            present=set()
            for value in values:
                try: raw=wire.encoded(value)
                except wire.WireError:
                    report['rejected']+=1; continue
                identity=hashlib.sha256(raw).hexdigest(); present.add(identity)
                if identity in self.done or self.retry_after.get(identity,0)>time.monotonic(): continue
                if report['attempted']>=MAX_PER_PASS or time.monotonic()-started>12:
                    report['deferred']+=1; continue
                report['attempted']+=1
                try:
                    status=post(value,key)
                    self.done[identity]=now
                    report[{'RECORDED':'recorded','DUPLICATE':'duplicates','REJECTED':'rejected'}[status]]+=1
                    self.retry_after.pop(identity,None)
                except Exception:
                    self.retry_after[identity]=time.monotonic()+60
                    report['deferred']+=1
            # This cache is an optimization only; the receiver is durable.
            self.done={k:v for k,v in self.done.items() if k in present}
            self.retry_after={k:v for k,v in self.retry_after.items() if k in present}
            if len(self.done)+len(self.retry_after)>8192:
                self.done.clear();self.retry_after.clear()
            if report['deferred']: report['status']='FORWARD_PENDING_RETRY'
            else: self.last_ok=time.monotonic()
        except Exception:
            report['status']='FORWARD_SOURCE_UNAVAILABLE'
        report['coverage_gap_observed']=self.coverage_gap
        report['cycles']=self.cycles
        return report


async def _loop(key,fence):
    worker=Forwarder()
    while True:
        report=await asyncio.to_thread(worker.pass_once,tuple(_SCOPES),key,fence)
        # Counts only, never addresses, messages, URLs, environment or exceptions.
        if worker.cycles%10==1 or report['attempted'] or report['status']!='FORWARD_PASS_COMPLETED':
            print(json.dumps({'alert_cards_forwarder':report},sort_keys=True),flush=True)
        await asyncio.sleep(DELAY)


def maybe_start(chat_id):
    """Called from existing delivery initialization. Failures cannot block alerts."""
    global _TASK
    try:
        cfg=config(os.environ)
        if cfg is None or chat_id is None: return
        scope=hashlib.sha256(str(int(chat_id)).encode()).hexdigest()
        if len(_SCOPES)>=4 and scope not in _SCOPES:
            print('[alert-cards] SOURCE_SCOPE_LIMIT_REQUIRES_REVIEW',flush=True);return
        _SCOPES.add(scope)
        if _TASK is None or _TASK.done():
            _TASK=asyncio.get_running_loop().create_task(_loop(*cfg),name='delivered-alert-cards')
    except Exception:
        print('[alert-cards] FORWARD_INITIALIZATION_UNAVAILABLE',flush=True)
