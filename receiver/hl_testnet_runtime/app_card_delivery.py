"""Optional one-way Testnet card projection; never supplies trading instructions."""
from datetime import datetime, timezone
import base64
import hashlib
import http.client
import json
import time

from . import card_lifecycle as life, trade_cards
from .filled_dispatch_store import DispatchError

HOST = 'kgwsspccrlaoqcybnubd.supabase.co'
PATH = '/functions/v1/automated-cards-ingest'
WORKSPACE = '7f83043d-57ac-4bd4-b523-71697d5a3e76'
MODE = 'ed25519_signed_v1'


def number(value):
    if value is None:
        return None
    result = float(life.number(value, signed=True))
    if not -1e18 <= result <= 1e18:
        raise DispatchError('APP_PROJECTION_NUMBER_INVALID')
    return result


def project(card, state, *, created_at, pending_request=None):
    """Use durable observations only; no inferred PnL or app control fields."""
    card = trade_cards.validate_card(card)
    if (card['record_kind'] != 'received_alert'
            or card['account_role'] not in ('long_account','short_account')):
        raise DispatchError('APP_PROJECTION_SOURCE_REQUIRED')
    signal = card['prepared']['execution']
    payload = dict(symbol=card['prepared']['source']['symbol'],
        side='long' if card['account_role']=='long_account' else 'short',
        entry_price=number(signal['entry']) if signal else None,
        stop_price=number(signal['stop']) if signal else None,
        take_profit_price=number(signal['take_profit']) if signal else None,
        status=card['state'],
        quantity_entered=None, quantity_closed=None, quantity_remaining=None,
        realized_pnl=None, pnl_verified=False, closure_verified=False)
    observed = created_at
    revision = 1
    if state is not None and card['card_id'] in state['originals']:
        original=state['originals'][card['card_id']]['card']
        if (original != card or original['account_role'] != card['account_role']
                or state['symbol'] != card['prepared']['source']['symbol']):
            raise DispatchError('APP_PROJECTION_SOURCE_STATE_MISMATCH')
        revision = state['revision'] + 2
        if (pending_request is not None and
                pending_request['proposal']['card_id'] == card['card_id'] and
                pending_request['proposal']['operation'] == 'ENTRY' and
                pending_request['attempt_at_ms'] is not None):
            payload['status'] = 'WAITING_ENTRY'
            observed = datetime.fromtimestamp(pending_request['attempt_at_ms']/1000,
                                              timezone.utc)
        if state['evidence'] is not None and state['bindings']:
            snap = state['evidence']['snapshot']
            view = life.review(state['bindings'], snap, now_ms=snap['at_ms'])
            row = next((v for v in view['cards'] if v['card_id'] == card['card_id']), None)
            if row is not None:
                payload.update(status=row['state'],
                    quantity_entered=number(row['entry_quantity']),
                    quantity_closed=number(row['exit_quantity']),
                    quantity_remaining=number(row['remaining_quantity']),
                    closure_verified=row['closure_verified'])
                observed = datetime.fromtimestamp(snap['at_ms']/1000, timezone.utc)
    if isinstance(observed, datetime):
        observed = observed.isoformat()
    return dict(workspace_id=WORKSPACE, card_id=card['card_id'], revision=revision,
                payload=payload, observed_at=observed)


def records(journal, *, start_after=None, role='long_account'):
    """A bounded historical scan; failed sends retry from durable source on restart."""
    if role not in ('long_account','short_account'):
        raise DispatchError('APP_PROJECTION_ROLE_REQUIRED')
    after = start_after
    for _ in range(100):
        with journal._transaction() as conn:
            rows = conn.execute('''SELECT c.card_id,c.manifest,c.digest,c.created_at
                FROM hl_testnet_cards_v1.cards c
                WHERE EXISTS (SELECT 1 FROM hl_testnet_cards_v1.delivery_receipts r
                    WHERE r.card_id=c.card_id AND r.status='RECORDED')
                  AND (%s::text IS NULL OR c.card_id > %s)
                ORDER BY c.card_id LIMIT 65''',(after,after)).fetchall()
        for cid,manifest,digest,created in rows[:64]:
            if trade_cards.checksum(manifest) != digest or manifest['card_id'] != cid:
                raise DispatchError('APP_PROJECTION_SOURCE_INTEGRITY_FAILURE')
            if manifest.get('account_role') == role and manifest.get('record_kind') == 'received_alert':
                yield manifest,created
        if len(rows) <= 64:
            return
        after = rows[63][0]
    raise DispatchError('APP_PROJECTION_SCAN_BUDGET_REQUIRES_REVIEW')


def signed_post(payload, private_der):
    from Crypto.PublicKey import ECC
    from Crypto.Signature import eddsa
    if HOST != 'kgwsspccrlaoqcybnubd.supabase.co' or PATH != '/functions/v1/automated-cards-ingest':
        raise DispatchError('FIXED_APP_RECEIVER_REQUIRED')
    raw = json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    if len(raw) > 16384:
        raise DispatchError('APP_PROJECTION_TOO_LARGE')
    stamp = str(int(time.time()))
    key = ECC.import_key(base64.b64decode(private_der,validate=True))
    if key.curve != 'Ed25519' or not key.has_private():
        raise DispatchError('APP_SIGNING_KEY_INVALID')
    signature = eddsa.new(key,'rfc8032').sign(stamp.encode()+b'.'+raw)
    conn = http.client.HTTPSConnection(HOST,timeout=5)
    try:
        conn.request('POST',PATH,raw,{'Content-Type':'application/json',
            'X-Card-Timestamp':stamp,'X-Card-Signature':base64.b64encode(signature).decode()})
        reply = conn.getresponse()
        body = reply.read(513)
        if reply.status != 200 or len(body)>512 or json.loads(body).get('status') not in ('APPLIED','DUPLICATE'):
            raise DispatchError('APP_DELIVERY_NOT_ACKNOWLEDGED')
    finally:
        conn.close()


class Publisher:
    def __init__(self):
        self.sent = {}
        self.cursor = None
        self.last_status = 'DISABLED'
        self.retries = {}

    def pass_once(self, controller, route, key, *, post=signed_post,
                  role='long_account', clock=time.monotonic):
        states = {s['symbol']:s for s in controller.store.for_account(route['account'])}
        attempted = 0
        delivered = 0
        failed = False
        start = self.cursor
        for wrapping in (False,True):
            if wrapping and start is None:
                break
            for card, created in records(controller.store.journal,
                    start_after=None if wrapping else start,role=role):
                if wrapping and card['card_id'] > start:
                    break
                symbol = card['prepared']['source']['symbol']
                state = states.get(symbol)
                pending = (controller.store.request(state['pending'])
                    if state is not None and state['pending'] else None)
                value = project(card,state,created_at=created,pending_request=pending)
                # A new observation time/revision alone is not a changed trade.
                # Send only changed visible facts; restart safely replays the
                # durable revision through the receiver's idempotent upsert.
                marker = hashlib.sha256(json.dumps(value['payload'],sort_keys=True).encode()).hexdigest()
                cid=card['card_id']
                self.cursor=cid
                if self.sent.get(cid) == marker:
                    continue
                retry=self.retries.get(cid)
                if retry is not None and clock()<retry['due']:
                    continue
                attempted += 1
                try:
                    post(value,key)
                except Exception:
                    # A failed display cannot starve other cards/accounts or
                    # touch execution. A changed payload keeps the same delay.
                    count=min(5,(retry or {}).get('count',0)+1)
                    self.retries[cid]=dict(count=count,due=clock()+min(300,10*2**count))
                    failed=True
                else:
                    self.sent[cid] = marker
                    self.retries.pop(cid,None)
                    delivered += 1
                if attempted == 4:
                    self.last_status = 'DELIVERY_UNAVAILABLE_RETRY' if failed else 'DELIVERED'
                    return delivered
        self.last_status = 'DELIVERY_UNAVAILABLE_RETRY' if failed else 'DELIVERED' if attempted else 'UNCHANGED'
        return delivered
