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
    if card['record_kind'] != 'received_alert' or card['account_role'] != 'long_account':
        raise DispatchError('APP_PROJECTION_SOURCE_REQUIRED')
    signal = card['prepared']['execution']
    payload = dict(symbol=card['prepared']['source']['symbol'], side='long',
        entry_price=number(signal['entry']) if signal else None,
        stop_price=number(signal['stop']) if signal else None,
        take_profit_price=number(signal['take_profit']) if signal else None,
        status=card['state'],
        quantity_entered=None, quantity_closed=None, quantity_remaining=None,
        realized_pnl=None, pnl_verified=False, closure_verified=False)
    observed = created_at
    revision = 1
    if state is not None and card['card_id'] in state['originals']:
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


def records(journal):
    """A bounded historical scan; failed sends retry from durable source on restart."""
    after = None
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
            if manifest.get('account_role') == 'long_account' and manifest.get('record_kind') == 'received_alert':
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
        self.last_status = 'DISABLED'

    def pass_once(self, controller, route, key, *, post=signed_post):
        states = {s['symbol']:s for s in controller.store.for_account(route['account'])}
        attempted = 0
        for card, created in records(controller.store.journal):
            symbol = card['prepared']['source']['symbol']
            state = states.get(symbol)
            pending = (controller.store.request(state['pending'])
                if state is not None and state['pending'] else None)
            value = project(card,state,created_at=created,pending_request=pending)
            fingerprint = hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()
            marker = (value['revision'],fingerprint)
            if self.sent.get(card['card_id']) == marker:
                continue
            if attempted >= 4:
                break
            post(value,key)
            self.sent[card['card_id']] = marker
            attempted += 1
        self.last_status = 'DELIVERED' if attempted else 'UNCHANGED'
        return attempted
