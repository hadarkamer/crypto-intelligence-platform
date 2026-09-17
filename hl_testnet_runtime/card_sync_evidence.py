"""Read-only Testnet deltas for already registered lifecycle bindings.

Never creates a binding by coin/direction or accepts an alert as an execution.
No signer, order sender, wallet keys, transfers or account changes. Retain old
fills and query a bounded overlapping interval; unknown activity is not hidden.
"""
from copy import deepcopy
from decimal import Decimal
import http.client
import json
import time
from . import card_lifecycle as life, checks

HOST = 'api.hyperliquid-testnet.xyz'
DAY_MS = 86400000
OVERLAP_MS = 60000


class SyncError(life.LifecycleError):
    """Fixed codes only."""


def now_ms():
    return time.time_ns() // 1000000


def facts(snapshot):
    value = deepcopy(snapshot)
    value.pop('at_ms', None)
    for name, key in (('fills','fill_id'), ('open_orders','oid'), ('terminal_orders','oid')):
        value[name] = sorted(value[name], key=lambda row: row[key])
    return value


class PublicReader:
    """Only fixed public /info requests. Bounded calls, bytes and request time."""
    def __init__(self):
        self.calls = 0

    def read(self, kind, account, *, oid=None, start=None, end=None):
        body = {'type':kind, 'user':life.address(account)}
        if kind == 'orderStatus' and start is None and end is None:
            life.ident(oid, r'[0-9]{1,20}')
            if not 0 < int(oid) < 2**64: raise SyncError('INVALID_ORDER_ID')
            body['oid'] = int(oid)
        elif kind == 'userFillsByTime' and oid is None:
            life.moment(start); life.moment(end)
            if not 0 <= end-start <= DAY_MS: raise SyncError('HISTORY_GAP_REQUIRES_REVIEW')
            body.update(startTime=start, endTime=end, aggregateByTime=False)
        elif kind in ('clearinghouseState','frontendOpenOrders') and oid is None and start is None and end is None:
            pass
        else:
            raise SyncError('READ_TYPE_NOT_ALLOWED')
        if HOST != 'api.hyperliquid-testnet.xyz' or self.calls >= 200:
            raise SyncError('READ_BUDGET_EXCEEDED')
        self.calls += 1
        connection = http.client.HTTPSConnection(HOST, timeout=4)
        try:
            connection.request('POST','/info',json.dumps(body).encode(),{'Content-Type':'application/json'})
            response = connection.getresponse()
            if response.status != 200: raise SyncError('PUBLIC_READ_UNAVAILABLE')
            raw = response.read(checks.MAX_BYTES+1)
            if len(raw) > checks.MAX_BYTES: raise SyncError('PUBLIC_RESPONSE_TOO_LARGE')
            return checks.decode(raw)
        except (OSError,http.client.HTTPException):
            raise SyncError('PUBLIC_READ_UNAVAILABLE') from None
        finally:
            connection.close()


def history(reader, account, start, end, *, depth=0):
    rows = reader.read('userFillsByTime',account,start=start,end=end)
    if not isinstance(rows,list) or any(not isinstance(r,dict) for r in rows):
        raise SyncError('INVALID_FILL_HISTORY')
    times = {life.moment(r.get('time')) for r in rows}
    if any(not start <= t <= end for t in times): raise SyncError('FILL_TIME_OUTSIDE_WINDOW')
    # Handle both documented bounds without dropping equal-timestamp boundary fills.
    if len(rows) >= 2000 or len(times) >= 500:
        if start == end or depth >= 32: raise SyncError('FILL_HISTORY_TRUNCATED')
        middle = (start+end)//2
        return (history(reader,account,start,middle,depth=depth+1)
                + history(reader,account,middle+1,end,depth=depth+1))
    return rows


def merge_fills(previous, raw, account, symbol, start, end):
    by_id = {f['fill_id']:deepcopy(f) for f in previous}
    recent = {}
    for row in raw:
        if row.get('coin') != symbol: continue
        tid, oid = row.get('tid'),row.get('oid')
        if type(tid) is not int or tid < 0 or type(oid) is not int or not 0 < oid < 2**64:
            raise SyncError('EXACT_FILL_IDENTIFIERS_REQUIRED')
        q, px = row.get('sz'),row.get('px')
        life.number(q,positive=True); life.number(px,positive=True)
        life.number(row.get('fee'),signed=True); life.ident(row.get('feeToken'))
        if row.get('side') not in ('A','B'): raise SyncError('INVALID_FILL_SIDE')
        at = life.moment(row.get('time'))
        if not start <= at <= end: raise SyncError('FILL_TIME_OUTSIDE_WINDOW')
        fid = 'hl:'+str(tid)
        value = dict(account=account,symbol=symbol,oid=str(oid),fill_id=fid,
            quantity=q,price=px,fee=row['fee'],fee_token=row['feeToken'],side=row['side'],at_ms=at)
        for known in (by_id,recent):
            if fid in known and known[fid] != value: raise SyncError('FILL_FACT_CHANGED')
        recent[fid] = value
    if any(start <= f['at_ms'] <= end and f['fill_id'] not in recent for f in previous):
        raise SyncError('PREVIOUS_FILL_MISSING_IN_OVERLAP')
    by_id.update(recent)
    return sorted(by_id.values(),key=lambda row:(row['at_ms'],row['fill_id']))


CANCELED = frozenset(('canceled','marginCanceled','vaultWithdrawalCanceled','openInterestCapCanceled',
    'selfTradeCanceled','reduceOnlyCanceled','siblingFilledCanceled','delistedCanceled',
    'liquidatedCanceled','scheduledCancel'))
REJECTED = frozenset(('rejected','tickRejected','minTradeNtlRejected','perpMarginRejected',
    'reduceOnlyRejected','badAloPxRejected','iocCancelRejected','badTriggerPxRejected',
    'marketOrderNoLiquidityRejected','positionIncreaseAtOpenInterestCapRejected',
    'positionFlipAtOpenInterestCapRejected','tooAggressiveAtOpenInterestCapRejected',
    'openInterestIncreaseRejected','insufficientSpotBalanceRejected','oracleRejected','perpMaxPositionRejected'))


def observe(bindings, previous, reader, start, end):
    account,symbol = life.validate_snapshot(previous)
    links = {oid:(b,leg) for b in bindings for leg in life.LEGS for oid in b['orders'][leg]}
    old_terminal = {r['oid']:r for r in previous['terminal_orders']}
    responses = {oid:reader.read('orderStatus',account,oid=oid) for oid in links}
    fills = merge_fills(previous['fills'],history(reader,account,start,end),account,symbol,start,end)
    totals = {}
    for row in fills: totals[row['oid']] = totals.get(row['oid'],Decimal(0))+life.number(row['quantity'])
    raw_inventory = reader.read('frontendOpenOrders',account)
    position = reader.read('clearinghouseState',account)
    if not isinstance(raw_inventory,list) or len(raw_inventory)>10000 or any(not isinstance(x,dict) for x in raw_inventory):
        raise SyncError('INVALID_ORDER_INVENTORY')
    inventory = {}
    for row in raw_inventory:
        if row.get('coin') != symbol: continue
        oid = row.get('oid')
        if type(oid) is not int or str(oid) in inventory: raise SyncError('INVALID_INVENTORY_ORDER_ID')
        inventory[str(oid)] = row
    if set(inventory)-set(links): raise SyncError('UNASSIGNED_EXCHANGE_ORDER')
    terminals,opens = [],[]
    for oid,(binding,leg) in links.items():
        raw = responses[oid]
        if not isinstance(raw,dict) or raw.get('status')!='order': raise SyncError('ORDER_STATUS_UNRESOLVED')
        envelope = raw.get('order',{}); order = envelope.get('order',{})
        status = envelope.get('status'); stamp = life.moment(envelope.get('statusTimestamp'))
        if stamp > end: raise SyncError('OBSERVATION_CHANGED_RETRY')
        entry_side = 'B' if binding['side']=='LONG' else 'A'
        side = entry_side if leg=='ENTRY' else ('A' if entry_side=='B' else 'B')
        if (type(order.get('oid')) is not int or str(order['oid'])!=oid or order.get('coin')!=symbol
                or order.get('side')!=side or type(order.get('reduceOnly')) is not bool
                or order['reduceOnly']!=(leg!='ENTRY')):
            raise SyncError('ORDER_BINDING_MISMATCH')
        original = life.number(order.get('origSz'),positive=True)
        filled = totals.get(oid,Decimal(0))
        if filled > original: raise SyncError('FILLS_EXCEED_ORIGINAL_SIZE')
        state = 'FILLED' if status=='filled' else 'CANCELED' if status in CANCELED else 'REJECTED' if status in REJECTED else None
        if state:
            if oid in inventory: raise SyncError('OBSERVATION_CHANGED_RETRY')
            if state=='FILLED' and filled!=original: raise SyncError('FILLED_ORDER_HISTORY_INCOMPLETE')
            value = dict(account=account,symbol=symbol,oid=oid,state=state,
                         filled_quantity=life.text(filled),at_ms=stamp)
            if oid in old_terminal:
                old = old_terminal[oid]
                if (old['state']!=state or old['at_ms']!=stamp
                        or life.number(old['filled_quantity'])!=filled):
                    raise SyncError('TERMINAL_FACT_CHANGED')
                value = deepcopy(old)  # Preserve immutable decimal representation.
            terminals.append(value)
            continue
        if oid in old_terminal: raise SyncError('TERMINAL_ORDER_BECAME_ACTIVE')
        if status!='open' or oid not in inventory: raise SyncError('ORDER_ACTIVATION_UNRESOLVED')
        actual = inventory[oid]
        fields = ('oid','coin','side','sz','limitPx','triggerPx','reduceOnly','orderType','isTrigger')
        if any(actual.get(k)!=order.get(k) for k in fields): raise SyncError('OBSERVATION_CHANGED_RETRY')
        remaining = life.number(order.get('sz'),positive=True)
        if remaining+filled!=original: raise SyncError('OPEN_ORDER_HISTORY_INCOMPLETE')
        expected_type = 'Limit' if leg=='ENTRY' else 'Take Profit Limit' if leg=='TAKE_PROFIT' else 'Stop Market'
        if order.get('orderType')!=expected_type: raise SyncError('ORDER_TYPE_REQUIRES_REVIEW')
        opens.append(dict(account=account,symbol=symbol,oid=oid,quantity=order['sz'],price=order['limitPx'],
            trigger_price=None if leg=='ENTRY' else order.get('triggerPx'),side=side,
            reduce_only=order['reduceOnly'],state='ACTIVE',
            order_type='LIMIT' if leg=='ENTRY' else 'TP_LIMIT' if leg=='TAKE_PROFIT' else 'SL_MARKET'))
    if not isinstance(position,dict) or not isinstance(position.get('assetPositions'),list):
        raise SyncError('INVALID_POSITION_STATE')
    matches = [r['position'] for r in position['assetPositions'] if isinstance(r,dict)
               and isinstance(r.get('position'),dict) and r['position'].get('coin')==symbol]
    if len(matches)>1: raise SyncError('DUPLICATE_POSITION')
    quantity = matches[0]['szi'] if matches else '0'
    life.number(quantity,signed=True)
    return dict(environment='testnet',account=account,symbol=symbol,at_ms=end,history_complete=True,
        orders_complete=True,position_quantity=quantity,fills=fills,
        open_orders=sorted(opens,key=lambda r:r['oid']),terminal_orders=sorted(terminals,key=lambda r:r['oid']))


def collect(evidence, reader, *, cursor_ms=None, clock=now_ms, elapsed=time.monotonic):
    bindings,previous = deepcopy(evidence['bindings']),deepcopy(evidence['snapshot'])
    life.validate_bindings(bindings)
    account,symbol = life.validate_snapshot(previous)
    if any(life.address(b['account'])!=account or b['symbol']!=symbol for b in bindings):
        raise SyncError('MIXED_BUCKET_BINDINGS')
    if not previous['history_complete'] or not previous['orders_complete']:
        raise SyncError('VERIFIED_HISTORY_BOOTSTRAP_REQUIRED')
    cursor = previous['at_ms'] if cursor_ms is None else life.moment(cursor_ms)
    if cursor < previous['at_ms']: raise SyncError('CHECKPOINT_BEHIND_EVIDENCE')
    end,started = clock(),elapsed()
    if not 0 <= end-cursor < DAY_MS-OVERLAP_MS: raise SyncError('HISTORY_GAP_REQUIRES_REVIEW')
    start = max(1,cursor-OVERLAP_MS)
    first = observe(bindings,previous,reader,start,end)
    second = observe(bindings,previous,reader,start,end)
    if first != second: raise SyncError('OBSERVATION_CHANGED_RETRY')
    if elapsed()-started>15: raise SyncError('OBSERVATION_TOO_SLOW')
    result = life.review(bindings,second,now_ms=clock())
    return dict(bindings=bindings,snapshot=second,report=result,cursor_ms=end)
