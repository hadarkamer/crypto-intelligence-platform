"""Authenticated Testnet records only. No signing/dispatch/app-forwarding import.

The public health page remains read-only. This separate, default-disabled path
accepts one HMAC-authenticated delivered notification and persists a card.
Record persistence is not an order, execution queue, or account authorization.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
import threading
import time

import alert_cards_wire as wire
from . import trade_cards as cards
from .trade_card_store import CardStore
from .postgres_journal import PostgresJournal, JournalError

TABLE='hl_testnet_cards_v1.delivery_receipts'
RESOLUTIONS='hl_testnet_cards_v1.asset_receipt_resolutions'
_LOCK=threading.Lock()
_META=None
_META_AT=0.0


def enabled(env):
    mode=env.get('HL_TESTNET_RUNTIME_MODE')
    controlled_intake=(mode=='filled_card_controlled_v1'
        and env.get('HL_TESTNET_FILLED_AUTOWAIT')=='approved_next_u21_once_v1'
        and env.get('HL_TESTNET_FILLED_DISPATCH')=='approved_single_card_v1'
        and not env.get('HL_TESTNET_SAFETY_PIPELINE')
        and not env.get('HL_TESTNET_CARD_SYNC')
        and env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')=='disabled')
    return (env.get('HL_TESTNET_CARDS_INTAKE')=='record_only_v1'
        and env.get('RENDER_SERVICE_ID')=='srv-dakptbh594qs7395460g'
        and env.get('HL_TESTNET_CARDS_PHASE1')=='record_only_v1'
        and env.get('HL_TESTNET_JOURNAL_BACKEND')=='staging_postgres_v1'
        and (mode in ('read_only','cancel_monitor_testnet_v1') or controlled_intake)
        and bool(wire.HEX.fullmatch(env.get('HL_TESTNET_CARDS_INTAKE_SECRET',''))))


def initialize(journal):
    with journal._transaction() as conn:
        CardStore(journal).ready(conn)
        conn.execute('SELECT pg_advisory_xact_lock(%s)',(1729048191,))
        conn.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
            receipt_id text PRIMARY KEY CHECK(length(receipt_id)=64),
            status text NOT NULL CHECK(status IN ('RECORDED','REJECTED')),
            card_id text REFERENCES hl_testnet_cards_v1.cards(card_id),
            source jsonb, reason text, received_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
        conn.execute(f'REVOKE ALL ON {TABLE} FROM PUBLIC')
        # A reprocessed valid source may become a DATA card, never executable.
        # Preserve the original rejection and receipt time instead of erasing it.
        conn.execute(f'''CREATE TABLE IF NOT EXISTS {RESOLUTIONS} (
            receipt_id text PRIMARY KEY REFERENCES {TABLE}(receipt_id),
            previous_status text NOT NULL CHECK(previous_status='REJECTED'),
            previous_reason text NOT NULL CHECK(previous_reason='ASSET_UNAVAILABLE'),
            previous_received_at timestamptz NOT NULL,
            source jsonb NOT NULL,
            card_id text NOT NULL REFERENCES hl_testnet_cards_v1.cards(card_id),
            resolved_at timestamptz NOT NULL DEFAULT clock_timestamp())''')
        conn.execute(f'REVOKE ALL ON {RESOLUTIONS} FROM PUBLIC')


def metadata():
    global _META,_META_AT
    with _LOCK:
        if _META is None or time.monotonic()-_META_AT>=300:
            from .checks import InfoReader
            value=InfoReader().read('meta')
            if not isinstance(value,dict) or not isinstance(value.get('universe'),list):
                raise wire.WireError('METADATA_UNAVAILABLE')
            _META=value;_META_AT=time.monotonic()
        return _META


class ReceiptStore:
    def __init__(self,journal): self.journal=journal;self.cards=CardStore(journal)
    def get(self,identity):
        with self.journal._transaction() as conn:
            row=conn.execute(f'SELECT status,card_id,reason FROM {TABLE} WHERE receipt_id=%s',(identity,)).fetchone()
        return None if row is None else dict(status=row[0],card_id=row[1],reason=row[2])
    def save(self,identity,status,card_id,source,reason=None):
        with self.journal._transaction() as conn:
            conn.execute(f'''INSERT INTO {TABLE}(receipt_id,status,card_id,source,reason)
                VALUES(%s,%s,%s,%s::jsonb,%s) ON CONFLICT(receipt_id) DO NOTHING''',
                (identity,status,card_id,json.dumps(source,allow_nan=False),reason))
            row=conn.execute(f'SELECT status,card_id FROM {TABLE} WHERE receipt_id=%s',(identity,)).fetchone()
            if row!=(status,card_id): raise JournalError('DELIVERY_RECEIPT_CONFLICT')

    def resolve_asset_rejection(self,identity,card_id,source):
        """One specific DATA-only transition, retaining original evidence atomically.

        No generic error reset, no price rewrite, no reset of order reservations.
        A retry must match the source already received through authenticated intake.
        """
        with self.journal._transaction() as conn:
            row=conn.execute(f'''SELECT status,card_id,source,reason,received_at
                FROM {TABLE} WHERE receipt_id=%s FOR UPDATE''',(identity,)).fetchone()
            if row is None or row[2]!=source:
                raise JournalError('RECEIPT_SOURCE_CHANGED_NO_RECOVERY')
            if row[0]=='RECORDED' and row[1]==card_id:
                return False
            if row[0]!='REJECTED' or row[1] is not None or row[3]!='ASSET_UNAVAILABLE':
                raise JournalError('RECEIPT_NOT_ELIGIBLE_FOR_ASSET_RECOVERY')
            conn.execute(f'''INSERT INTO {RESOLUTIONS}
                (receipt_id,previous_status,previous_reason,previous_received_at,source,card_id)
                VALUES(%s,%s,%s,%s,%s::jsonb,%s)''',
                (identity,row[0],row[3],row[4],json.dumps(source,allow_nan=False),card_id))
            conn.execute(f'''UPDATE {TABLE} SET status='RECORDED',card_id=%s,reason=NULL
                WHERE receipt_id=%s''',(card_id,identity))
        return True


def accept(raw,store,*,read_metadata=metadata):
    identity=hashlib.sha256(raw).hexdigest()
    previous=store.get(identity)
    recover_asset=(previous is not None and previous['status']=='REJECTED'
                   and previous.get('reason')=='ASSET_UNAVAILABLE')
    if previous and not recover_asset:
        return dict(receipt_id=identity,status='DUPLICATE' if previous['status']=='RECORDED' else 'REJECTED',record_only=True)
    value=wire.decoded(raw)
    try:
        spec=wire.normalize(value)
    except wire.WireError as exc:
        store.save(identity,'REJECTED',None,None,str(exc))
        return dict(receipt_id=identity,status='REJECTED',record_only=True)
    try:
        try:
            card=cards.prepare_card(spec['signal'],read_metadata(),rule_id=spec['rule_id'],
                threshold_pct=spec['threshold_pct'],record_kind='received_alert',
                source_stream=spec['source_stream'],source_expires_at=spec.get('source_expires_at'))
            if not card['planning']['positive_quantity']:
                raise cards.CardError('ZERO_PLANNED_QUANTITY')
        except cards.CardError as exc:
            if str(exc)!='ASSET_UNAVAILABLE': raise
            # Persist valid source with NULL execution/quantity, NOT a fake market.
            from .unavailable_asset_cards import prepare
            card=prepare(value)
        saved=store.cards.record(card)
    except cards.CardError as exc:
        store.save(identity,'REJECTED',None,value,str(exc))
        return dict(receipt_id=identity,status='REJECTED',record_only=True)
    except JournalError as exc:
        if str(exc)!='SOURCE_OR_PLAN_CHANGED_NO_OVERWRITE': raise
        store.save(identity,'REJECTED',None,value,'SOURCE_OR_PLAN_CHANGED_NO_OVERWRITE')
        return dict(receipt_id=identity,status='REJECTED',record_only=True)
    # The card has committed before the receipt. A lost reply is safe to retry.
    if recover_asset:
        store.resolve_asset_rejection(identity,saved['card_id'],value)
    else:
        store.save(identity,'RECORDED',saved['card_id'],value)
    result=dict(receipt_id=identity,status='RECORDED' if saved['created'] else 'DUPLICATE',
                record_only=True,card_state=card['state'])
    print(json.dumps({'testnet_cards_intake':dict(status=result['status'],family=value['family'],
        account_role=card['account_role'],card_state=card['state'],order_requests_sent=0,
        source_time_utc=spec['signal']['at'],observed_at_utc=datetime.now(timezone.utc).isoformat())}),flush=True)
    return result


def application(environ,start_response):
    code='404 Not Found'; result={'status':'NOT_FOUND'}
    try:
        if not enabled(os.environ): pass
        elif environ.get('REQUEST_METHOD')!='POST' or environ.get('QUERY_STRING'):
            code='405 Method Not Allowed';result={'status':'POST_ONLY'}
        elif environ.get('CONTENT_TYPE')!='application/json':
            code='415 Unsupported Media Type';result={'status':'JSON_REQUIRED'}
        else:
            length=environ.get('CONTENT_LENGTH','')
            if not length.isdecimal() or not 0<int(length)<=wire.MAX_BYTES:
                code='413 Content Too Large';result={'status':'BOUNDED_BODY_REQUIRED'}
            else:
                raw=environ['wsgi.input'].read(int(length))
                if len(raw)!=int(length): raise wire.WireError('INCOMPLETE_REQUEST_BODY')
                if not wire.authenticate(os.environ.get('HL_TESTNET_CARDS_INTAKE_SECRET',''),
                        environ.get('HTTP_X_CARD_TIMESTAMP'),environ.get('HTTP_X_CARD_SIGNATURE'),raw,time.time()):
                    code='403 Forbidden';result={'status':'AUTHENTICATION_REQUIRED'}
                else:
                    store=ReceiptStore(PostgresJournal.from_env(os.environ))
                    result=accept(raw,store);code='200 OK'
    except wire.WireError:
        code='400 Bad Request';result={'status':'INVALID_DELIVERY'}
    except Exception:
        code='503 Service Unavailable';result={'status':'RECORDING_UNAVAILABLE_RETRY'}
    body=json.dumps(result,separators=(',',':')).encode()
    start_response(code,[('Content-Type','application/json'),('Content-Length',str(len(body))),
        ('Cache-Control','no-store'),('X-Content-Type-Options','nosniff')])
    return [body]
