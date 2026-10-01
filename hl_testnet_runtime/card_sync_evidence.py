"""Read-only Testnet deltas for already registered lifecycle bindings.

Never creates a binding by coin/direction or accepts an alert as an execution.
No signer, order sender, wallet keys, transfers or account changes. Retain old
fills and query a bounded overlapping interval; unknown activity is not hidden.
"""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, localcontext
import http.client
import json
import time
import threading
from . import card_lifecycle as life, checks

HOST = 'api.hyperliquid-testnet.xyz'
DAY_MS = 86400000
OVERLAP_MS = 60000
MAX_CATCHUP_MS = 2*DAY_MS
MAX_OBSERVATION_READ_WORKERS = 12


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
    def __init__(self, *, parallel=False, parallel_workers=4, budget=None, priority='background'):
        if type(parallel_workers) is not int or not 1<=parallel_workers<=MAX_OBSERVATION_READ_WORKERS:
            raise SyncError('PUBLIC_READ_PARALLEL_BOUND_INVALID')
        self.calls = 0
        self.parallel = parallel is True
        self.parallel_workers = parallel_workers
        self._calls_lock = threading.Lock()
        from .request_budget import Budget
        import os
        self.budget = budget if budget is not None else Budget.from_env(os.environ)
        self.priority = priority

    def observation_inputs(self, account, oids, start, end):
        """Overlap independent info reads, never the two verification passes.

        The dispatcher opts in; the separately weighted read-only worker keeps
        its serial reader. No order transport or persistent state is concurrent.
        """
        if not self.parallel:
            return observation_inputs(self, account, oids, start, end)
        # Historical terminal identities remain mandatory in BOTH passes. A
        # fixed opt-in fan-out can avoid serial waves during a bounded recovery.
        # Ordinary dispatch retains four workers: faster repeated sweeps need
        # a separate per-IP rate budget. Call count, HTTP timeout and the total
        # reader budget remain unchanged, including for an explicit wider pass.
        pool = ThreadPoolExecutor(max_workers=self.parallel_workers)
        try:
            fills = pool.submit(history, self, account, start, end)
            inventory = pool.submit(self.read, 'frontendOpenOrders', account)
            position = pool.submit(self.read, 'clearinghouseState', account)
            statuses = {oid: pool.submit(self.read, 'orderStatus', account, oid=oid)
                        for oid in oids}
            return ({oid: future.result() for oid, future in statuses.items()},
                    fills.result(), inventory.result(), position.result())
        finally:
            # Join started reads and discard queued work on failure. No reads
            # from a failed observation may survive into another dispatch cycle.
            pool.shutdown(wait=True, cancel_futures=True)

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
        with self._calls_lock:
            if HOST != 'api.hyperliquid-testnet.xyz' or self.calls >= 200:
                raise SyncError('READ_BUDGET_EXCEEDED')
            self.calls += 1
        permit = (self.budget.acquire('/info', body, priority=self.priority, host=HOST)
                  if self.budget is not None else None)
        connection = http.client.HTTPSConnection(HOST, timeout=4)
        try:
            if permit is not None:
                permit.check()
            connection.request('POST','/info',json.dumps(body).encode(),{'Content-Type':'application/json'})
            response = connection.getresponse()
            if response.status != 200: raise SyncError('PUBLIC_READ_UNAVAILABLE')
            raw = response.read(checks.MAX_BYTES+1)
            if len(raw) > checks.MAX_BYTES: raise SyncError('PUBLIC_RESPONSE_TOO_LARGE')
            decoded = checks.decode(raw)
            if permit is not None:
                permit.finish(decoded)
            return decoded
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
        if not isinstance(row.get('coin'),str): raise SyncError('INVALID_FILL_SYMBOL')
        if row['coin'] != symbol: continue
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


def observation_inputs(reader, account, oids, start, end):
    """Serial compatibility for offline venues and independently budgeted readers."""
    responses = {oid: reader.read('orderStatus', account, oid=oid) for oid in oids}
    raw_fills = history(reader, account, start, end)
    inventory = reader.read('frontendOpenOrders', account)
    position = reader.read('clearinghouseState', account)
    return responses, raw_fills, inventory, position


def _fill_facts_by_oid(fills):
    facts={}
    for fill in fills:
        facts.setdefault(fill['oid'],{})[fill['fill_id']]=fill
    return facts


def terminal_certificates(bindings, previous):
    """Validate immutable terminal facts from a complete durable checkpoint.

    This is an internal opt-in contract: the caller must supply the integrity-
    checked committed checkpoint, never an acknowledgement or an uncommitted
    first verification pass. Fresh inventory and overlapping fills must still
    prove that these exact final identities have not reappeared or changed.
    """
    life.validate_bindings(bindings)
    account,symbol=life.validate_snapshot(previous)
    if not previous['history_complete'] or not previous['orders_complete']:
        raise SyncError('VERIFIED_HISTORY_BOOTSTRAP_REQUIRED')
    links={oid for b in bindings if life.address(b['account'])==account and b['symbol']==symbol
           for leg in life.LEGS for oid in b['orders'][leg]}
    opened={row['oid'] for row in previous['open_orders']}
    old_facts=_fill_facts_by_oid(previous['fills'])
    certificates={}
    with localcontext() as ctx:
        ctx.prec=80
        for row in previous['terminal_orders']:
            oid=row['oid']
            fills=old_facts.get(oid,{}).values()
            quantity=sum((life.number(f['quantity']) for f in fills),Decimal(0))
            if (oid not in links or oid in opened
                    or life.number(row['filled_quantity'])!=quantity
                    or any(f['at_ms']>row['at_ms'] for f in fills)
                    or (row['state']=='FILLED' and quantity<=0)
                    or (row['state']=='REJECTED' and quantity!=0)):
                raise SyncError('TERMINAL_CERTIFICATE_NOT_VERIFIED')
            certificates[oid]=deepcopy(row)
    return certificates


def observe(bindings, previous, reader, start, end, *, plain_take_profit_oids=(),
            reuse_verified_terminals=False):
    account,symbol = life.validate_snapshot(previous)
    plain = life.plain_tp_ids(bindings,account,symbol,plain_take_profit_oids)
    links = {oid:(b,leg) for b in bindings for leg in life.LEGS for oid in b['orders'][leg]}
    old_terminal = {r['oid']:r for r in previous['terminal_orders']}
    certificates=terminal_certificates(bindings,previous) if reuse_verified_terminals else {}
    current_oids={oid:link for oid,link in links.items() if oid not in certificates}
    read_inputs = getattr(reader, 'observation_inputs', None)
    responses, raw_fills, raw_inventory, position = (
        read_inputs(account, current_oids, start, end) if callable(read_inputs)
        else observation_inputs(reader, account, current_oids, start, end))
    fills = merge_fills(previous['fills'],raw_fills,account,symbol,start,end)
    old_facts=_fill_facts_by_oid(previous['fills']) if certificates else {}
    new_facts=_fill_facts_by_oid(fills) if certificates else {}
    totals = {}; last_fill = {}
    with localcontext() as ctx:
        ctx.prec = 80
        for row in fills:
            totals[row['oid']] = totals.get(row['oid'],Decimal(0))+life.number(row['quantity'])
            last_fill[row['oid']] = max(last_fill.get(row['oid'],0),row['at_ms'])
    if not isinstance(raw_inventory,list) or len(raw_inventory)>10000 or any(not isinstance(x,dict) for x in raw_inventory):
        raise SyncError('INVALID_ORDER_INVENTORY')
    inventory = {}
    for row in raw_inventory:
        if not isinstance(row.get('coin'),str): raise SyncError('INVALID_ORDER_SYMBOL')
        if row['coin'] != symbol: continue
        oid = row.get('oid')
        if type(oid) is not int or not 0<oid<2**64 or str(oid) in inventory:
            raise SyncError('INVALID_INVENTORY_ORDER_ID')
        inventory[str(oid)] = row
    if set(inventory)-set(links): raise SyncError('UNASSIGNED_EXCHANGE_ORDER')
    terminals,opens = [],[]
    for oid,(binding,leg) in links.items():
        if oid in certificates:
            certificate=certificates[oid]
            if oid in inventory:
                raise SyncError('TERMINAL_ORDER_BECAME_ACTIVE')
            if (new_facts.get(oid,{})!=old_facts.get(oid,{})
                    or totals.get(oid,Decimal(0))!=life.number(certificate['filled_quantity'])
                    or last_fill.get(oid,0)>certificate['at_ms']):
                raise SyncError('TERMINAL_FACT_CHANGED')
            terminals.append(deepcopy(certificate))
            continue
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
        if oid in plain and (order.get('orderType')!='Limit' or order.get('isTrigger') is not False
                or order.get('isPositionTpsl',False) is not False
                or life.number(order.get('limitPx'),positive=True)!=life.number(binding['prices']['take_profit'])):
            raise SyncError('REGISTERED_PLAIN_TP_TERMS_MISMATCH')
        original = life.number(order.get('origSz'),positive=True)
        filled = totals.get(oid,Decimal(0))
        if filled > original: raise SyncError('FILLS_EXCEED_ORIGINAL_SIZE')
        state = 'FILLED' if status=='filled' else 'CANCELED' if status in CANCELED else 'REJECTED' if status in REJECTED else None
        if state:
            if oid in inventory: raise SyncError('OBSERVATION_CHANGED_RETRY')
            if last_fill.get(oid,0)>stamp: raise SyncError('FILL_AFTER_TERMINAL_STATUS')
            if state=='FILLED' and filled!=original: raise SyncError('FILLED_ORDER_HISTORY_INCOMPLETE')
            if state=='REJECTED' and filled!=0: raise SyncError('REJECTED_ORDER_HAS_FILLS')
            value = dict(account=account,symbol=symbol,oid=oid,state=state,
                         filled_quantity=life.text(filled),at_ms=stamp)
            if oid in old_terminal:
                old = old_terminal[oid]
                if (old['state']!=state or old['at_ms']!=stamp
                        or life.number(old['filled_quantity'])!=filled):
                    raise SyncError('TERMINAL_FACT_CHANGED')
                value = deepcopy(old)
            terminals.append(value)
            continue
        if oid in old_terminal: raise SyncError('TERMINAL_ORDER_BECAME_ACTIVE')
        if status!='open' or oid not in inventory: raise SyncError('ORDER_ACTIVATION_UNRESOLVED')
        actual = inventory[oid]
        fields = ('oid','coin','side','sz','limitPx','triggerPx','reduceOnly','orderType','isTrigger')
        for key in fields:
            if actual.get(key) == order.get(key):
                continue
            if key == 'sz':
                # On a partially filled order, orderStatus may still report
                # the original size while frontendOpenOrders reports what is
                # left. Accept only when independently observed fills account
                # for the entire difference; never choose one view by itself.
                try:
                    inventory_remaining = life.number(actual.get('sz'),positive=True)
                    status_size = life.number(order.get('sz'),positive=True)
                    if (filled > 0 and status_size == original
                            and inventory_remaining + filled == original):
                        continue
                except life.LifecycleError:
                    pass
            # Expose only a fixed field/classification code. The order id,
            # account and raw exchange values remain out of logs. Do not
            # accept either observation until the discrepancy is understood.
            suffix = 'DIFF'
            if key in ('sz','limitPx','triggerPx'):
                try:
                    if Decimal(actual[key]) == Decimal(order[key]):
                        suffix = 'FORMAT_ONLY'
                except (KeyError,TypeError,ValueError,ArithmeticError):
                    pass
            raise SyncError(f'ORDER_VIEW_{key.upper()}_{suffix}')
        remaining = life.number(actual.get('sz'),positive=True)
        with localcontext() as ctx:
            ctx.prec = 80
            if remaining+filled!=original: raise SyncError('OPEN_ORDER_HISTORY_INCOMPLETE')
        expected_type = 'Limit' if leg=='ENTRY' or oid in plain else 'Take Profit Limit' if leg=='TAKE_PROFIT' else 'Stop Market'
        if order.get('orderType')!=expected_type: raise SyncError('ORDER_TYPE_REQUIRES_REVIEW')
        opens.append(dict(account=account,symbol=symbol,oid=oid,quantity=actual['sz'],price=order['limitPx'],
            trigger_price=None if leg=='ENTRY' or oid in plain else order.get('triggerPx'),side=side,
            reduce_only=order['reduceOnly'],state='ACTIVE',
            order_type='LIMIT' if leg=='ENTRY' or oid in plain else 'TP_LIMIT' if leg=='TAKE_PROFIT' else 'SL_MARKET'))
    if not isinstance(position,dict) or not isinstance(position.get('assetPositions'),list):
        raise SyncError('INVALID_POSITION_STATE')
    matches=[]; seen_coins=set()
    for row in position['assetPositions']:
        if (not isinstance(row,dict) or not isinstance(row.get('position'),dict)
                or not isinstance(row['position'].get('coin'),str)):
            raise SyncError('INVALID_POSITION_STATE')
        item=row['position']; coin=item['coin']
        if coin in seen_coins: raise SyncError('DUPLICATE_POSITION')
        seen_coins.add(coin); life.number(item.get('szi'),signed=True)
        if coin==symbol: matches.append(item)
    quantity = matches[0]['szi'] if matches else '0'
    return dict(environment='testnet',account=account,symbol=symbol,at_ms=end,history_complete=True,
        orders_complete=True,position_quantity=quantity,fills=fills,
        open_orders=sorted(opens,key=lambda r:r['oid']),terminal_orders=sorted(terminals,key=lambda r:r['oid']))


def collect(evidence, reader, *, cursor_ms=None, clock=now_ms, elapsed=time.monotonic,
            plain_take_profit_oids=(), reuse_verified_terminals=False):
    if type(reuse_verified_terminals) is not bool:
        raise SyncError('TERMINAL_CERTIFICATE_OPT_IN_INVALID')
    bindings,previous = deepcopy(evidence['bindings']),deepcopy(evidence['snapshot'])
    life.validate_bindings(bindings)
    account,symbol = life.validate_snapshot(previous)
    plain = life.plain_tp_ids(bindings,account,symbol,plain_take_profit_oids)
    if any(life.address(b['account'])!=account or b['symbol']!=symbol for b in bindings):
        raise SyncError('MIXED_BUCKET_BINDINGS')
    if not previous['history_complete'] or not previous['orders_complete']:
        raise SyncError('VERIFIED_HISTORY_BOOTSTRAP_REQUIRED')
    if reuse_verified_terminals:
        terminal_certificates(bindings,previous)
    cursor = previous['at_ms'] if cursor_ms is None else life.moment(cursor_ms)
    if cursor < previous['at_ms']: raise SyncError('CHECKPOINT_BEHIND_EVIDENCE')
    end,started = clock(),elapsed()
    if not 0 <= end-cursor <= MAX_CATCHUP_MS: raise SyncError('HISTORY_GAP_REQUIRES_REVIEW')
    # Reconstruct a short missed interval before taking the current order and
    # position snapshot. Each bounded window is observed twice; nothing is
    # persisted until the final full lifecycle review succeeds.
    while end-cursor >= DAY_MS-OVERLAP_MS:
        stop = cursor+DAY_MS-2*OVERLAP_MS
        start = max(1,cursor-OVERLAP_MS)
        first_fills = merge_fills(previous['fills'],history(reader,account,start,stop),
                                  account,symbol,start,stop)
        second_fills = merge_fills(previous['fills'],history(reader,account,start,stop),
                                   account,symbol,start,stop)
        if first_fills != second_fills: raise SyncError('OBSERVATION_CHANGED_RETRY')
        previous['fills'] = first_fills
        cursor = stop
    start = max(1,cursor-OVERLAP_MS)
    # Both passes use only the ORIGINAL durable certificates. A newly final
    # order in pass one must still receive an independent status read in pass
    # two before its complete checkpoint can ever be reused by a later cycle.
    first = observe(bindings,previous,reader,start,end,plain_take_profit_oids=plain,
                    reuse_verified_terminals=reuse_verified_terminals)
    second = observe(bindings,previous,reader,start,end,plain_take_profit_oids=plain,
                     reuse_verified_terminals=reuse_verified_terminals)
    if first != second: raise SyncError('OBSERVATION_CHANGED_RETRY')
    if elapsed()-started>15: raise SyncError('OBSERVATION_TOO_SLOW')
    result = life.review(bindings,second,now_ms=clock(),plain_take_profit_oids=plain)
    return dict(bindings=bindings,snapshot=second,report=result,cursor_ms=end)
