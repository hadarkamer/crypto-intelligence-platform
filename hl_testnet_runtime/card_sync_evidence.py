"""Read-only Testnet deltas for already registered lifecycle bindings.

Never creates a binding by coin/direction or accepts an alert as an execution.
No signer, order sender, wallet keys, transfers or account changes. Retain old
fills and query a bounded overlapping interval; unknown activity is not hidden.
"""
from copy import deepcopy
from contextlib import contextmanager, nullcontext
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
    def __init__(self, *, parallel=False, parallel_workers=4, budget=None, priority='background', reuse_cycle=False):
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
        self._observation_budget = None
        self.reuse_cycle = reuse_cycle is True

    @contextmanager
    def observation_batch(self, bodies):
        """Fund the complete known two-pass observation before its first HTTP.

        Individual one-second permissions are claimed just before transport.
        Recursive history splits fund both child pages atomically before either
        is sent; they retain the same bounded original collection deadline.
        """
        if self._observation_budget is not None:
            raise SyncError('NESTED_PUBLIC_OBSERVATION_NOT_ALLOWED')
        if self.budget is None:
            yield
            return
        batch=self.budget.reserve_observation(bodies,priority=self.priority,host=HOST)
        self._observation_budget=batch
        try:
            yield
        finally:
            self._observation_budget=None
            batch.close()

    def reserve_history_split(self,account,start,middle,end):
        batch=self._observation_budget
        if batch is not None:
            batch.reserve_extra([
                dict(type='userFillsByTime',user=account,startTime=start,endTime=middle,
                     aggregateByTime=False),
                dict(type='userFillsByTime',user=account,startTime=middle+1,endTime=end,
                     aggregateByTime=False)])

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
        from .simple_execution import read
        return read(body, lambda:self._transport(body), cycle=self.reuse_cycle)

    def _transport(self, body):
        with self._calls_lock:
            if HOST != 'api.hyperliquid-testnet.xyz' or self.calls >= 200:
                raise SyncError('READ_BUDGET_EXCEEDED')
            self.calls += 1
        budget=self._observation_budget or self.budget
        permit = (budget.acquire('/info', body, priority=self.priority, host=HOST)
                  if budget is not None else None)
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


class ObservationReadCache:
    """Collection-local reuse inside each independent verification pass.

    Pass zero and pass one never share a transport response. Cache hits preserve
    the collection's original end timestamp; failures are never cached. The
    underlying reader still funds all missing reads through its unchanged budget.
    """
    def __init__(self, reader):
        self.reader = reader
        self.samples = ({}, {})

    @staticmethod
    def _key(kind, account, *, oid=None, start=None, end=None):
        return (kind, account, str(oid) if oid is not None else None, start, end)

    def pass_reader(self, number):
        if number not in (0, 1):
            raise SyncError('VERIFICATION_PASS_COUNT_INVALID')
        owner = self
        class PassReader:
            def read(self, kind, account, *, oid=None, start=None, end=None):
                key = owner._key(kind, account, oid=oid, start=start, end=end)
                if key not in owner.samples[number]:
                    owner.samples[number][key] = deepcopy(owner.reader.read(
                        kind, account, oid=oid, start=start, end=end))
                return deepcopy(owner.samples[number][key])
            def reserve_history_split(self, account, start, middle, end):
                children=[owner._key('userFillsByTime',account,start=a,end=b)
                          for a,b in ((start,middle),(middle+1,end))]
                if all(key in owner.samples[number] for key in children):
                    return
                method = getattr(owner.reader, 'reserve_history_split', None)
                if callable(method):
                    method(account, start, middle, end)
        return PassReader()

    def _complete_history(self, key, number):
        """A cached split page is reusable only with its complete child tree.

        Failed earlier lanes may have fetched a parent but not all children.
        Evict each incomplete parent so the next funded plan declares and
        actually claims that parent before requesting additional split credit.
        """
        samples=self.samples[number]
        if key not in samples:return False
        rows=samples[key]
        if not isinstance(rows,list) or any(not isinstance(r,dict) for r in rows):
            return True  # Validation still rejects the original malformed raw response.
        times={r.get('time') for r in rows if type(r.get('time')) is int}
        if len(rows)<2000 and len(times)<500:return True
        kind,account,oid,start,end=key
        if start==end:return True  # The normal truncation error remains mandatory.
        middle=(start+end)//2
        complete=[self._complete_history(self._key(kind,account,start=a,end=b),number)
                  for a,b in ((start,middle),(middle+1,end))]
        if not all(complete):
            samples.pop(key,None)
            return False
        return True

    def missing_plan(self, bodies):
        # The collector schedules an identical request once per independent
        # pass. Count each body's occurrences, never collapse the two passes.
        seen = {}; missing = []
        for body in bodies:
            key = self._key(body['type'], body['user'], oid=body.get('oid'),
                            start=body.get('startTime'), end=body.get('endTime'))
            number = seen.get(key, 0)
            seen[key] = number + 1
            if number >= 2:
                raise SyncError('OBSERVATION_PLAN_PASS_AMBIGUOUS')
            if body['type']=='userFillsByTime':self._complete_history(key,number)
            if key not in self.samples[number]:
                missing.append(body)
        return missing


def history(reader, account, start, end, *, depth=0):
    rows = reader.read('userFillsByTime',account,start=start,end=end)
    if not isinstance(rows,list) or any(not isinstance(r,dict) for r in rows):
        raise SyncError('INVALID_FILL_HISTORY')
    times = {life.moment(r.get('time')) for r in rows}
    if any(not start <= t <= end for t in times): raise SyncError('FILL_TIME_OUTSIDE_WINDOW')
    if len(rows) >= 2000 or len(times) >= 500:
        if start == end or depth >= 32: raise SyncError('FILL_HISTORY_TRUNCATED')
        middle = (start+end)//2
        reserve=getattr(type(reader),'reserve_history_split',None)
        if callable(reserve):reader.reserve_history_split(account,start,middle,end)
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
           for leg in life.order_legs(b) for oid in b['orders'][leg]}
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


def triggered_take_profit(binding, leg, oid, order, inventory, fills, original,
                          filled, activation_at, end):
    """Prove the venue's trigger-parent -> same-OID resting limit transition.

    orderStatus retains the original trigger, while frontendOpenOrders describes
    its live limit after activation. Neither view alone proves live coverage or
    a fill. Keep the absent live trigger explicit instead of restoring the old
    trigger metadata. This adapter never accepts an entry or a market stop as
    a resting take-profit, nor discovers another order by coin/client id.
    """
    target=life.number(binding['prices']['take_profit'],positive=True)
    if (leg!='TAKE_PROFIT' or order.get('orderType')!='Take Profit Limit'
            or order.get('isTrigger') is not True
            or order.get('isPositionTpsl',False) is not False
            or life.number(order.get('triggerPx'),positive=True)!=target
            or life.number(order.get('limitPx'),positive=True)!=target
            or life.number(order.get('sz'),positive=True)!=original):
        raise SyncError('TRIGGERED_TAKE_ORIGINAL_TERMS_MISMATCH')
    cloid=life.ident(order.get('cloid'),r'0x[0-9a-f]{32}')
    placed=life.moment(order.get('timestamp'))
    if placed>activation_at:
        raise SyncError('TRIGGERED_TAKE_ACTIVATION_TIME_INVALID')
    own_fills=[row for row in fills if row['oid']==oid]
    if any(not activation_at<=row['at_ms']<=end for row in own_fills):
        raise SyncError('TRIGGERED_TAKE_FILL_TIME_INVALID')
    actual=inventory.get(oid)
    if actual is None:
        # A trigger event is not a close. Exact original-size fills, the
        # independently complete live inventory, and two agreeing collection
        # passes prove completion. Its real last-fill time is the terminal
        # evidence time; the earlier trigger event is never a fill timestamp.
        if filled!=original or not own_fills:
            raise SyncError('ORDER_ACTIVATION_UNRESOLVED')
        return dict(account=life.address(binding['account']),symbol=binding['symbol'],oid=oid,
                    state='FILLED',filled_quantity=life.text(filled),
                    at_ms=max(row['at_ms'] for row in own_fills))
    if (actual.get('cloid')!=cloid
            or actual.get('isTrigger') is not False
            or actual.get('isPositionTpsl',False) is not False
            or actual.get('triggerCondition')!='Triggered'
            or life.number(actual.get('triggerPx'))!=0
            or life.moment(actual.get('timestamp'))!=activation_at
            or actual.get('orderType')!='Take Profit Limit'
            or actual.get('coin')!=order.get('coin')
            or actual.get('side')!=order.get('side')
            or actual.get('reduceOnly') is not True
            or life.number(actual.get('origSz'),positive=True)!=original
            or life.number(actual.get('limitPx'),positive=True)!=target):
        raise SyncError('TRIGGERED_TAKE_LIVE_TERMS_MISMATCH')
    remaining=life.number(actual.get('sz'),positive=True)
    with localcontext() as ctx:
        ctx.prec=80
        if remaining+filled!=original:
            raise SyncError('OPEN_ORDER_HISTORY_INCOMPLETE')
    return dict(account=life.address(binding['account']),symbol=binding['symbol'],oid=oid,
        quantity=actual['sz'],price=actual['limitPx'],trigger_price=None,
        side=actual['side'],reduce_only=True,state='ACTIVE',
        order_type='TRIGGERED_TP_LIMIT')


def observe(bindings, previous, reader, start, end, *, plain_take_profit_oids=(),
            reuse_verified_terminals=False, inventory_guard=None):
    account,symbol = life.validate_snapshot(previous)
    plain = life.plain_tp_ids(bindings,account,symbol,plain_take_profit_oids)
    links = {oid:(b,leg) for b in bindings for leg in life.order_legs(b) for oid in b['orders'][leg]}
    old_terminal = {r['oid']:r for r in previous['terminal_orders']}
    certificates=terminal_certificates(bindings,previous) if reuse_verified_terminals else {}
    current_oids={oid:link for oid,link in links.items() if oid not in certificates}
    read_inputs = getattr(reader, 'observation_inputs', None)
    responses, raw_fills, raw_inventory, position = (
        read_inputs(account, current_oids, start, end) if callable(read_inputs)
        else observation_inputs(reader, account, current_oids, start, end))
    if inventory_guard is not None:
        inventory_guard(raw_inventory,position)
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
        if status=='triggered':
            if oid in plain:
                raise SyncError('REGISTERED_PLAIN_TP_TERMS_MISMATCH')
            value=triggered_take_profit(binding,leg,oid,order,inventory,fills,
                                        original,filled,stamp,end)
            if value['state']=='ACTIVE':
                if oid in old_terminal:raise SyncError('TERMINAL_ORDER_BECAME_ACTIVE')
                opens.append(value)
            else:
                if oid in old_terminal and old_terminal[oid]!=value:
                    raise SyncError('TERMINAL_FACT_CHANGED')
                terminals.append(value)
            continue
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
                # Trigger-parent status can later converge to 'filled'. A
                # durable full-fill certificate timed at the real last fill
                # remains immutable when the venue publishes a later status
                # timestamp for the same completed quantity. Never extend the
                # evidence clock, replace a canceled/rejected fact, or accept
                # a status that precedes its already proven execution.
                final_fill_time=(state=='FILLED' and old['state']=='FILLED'
                    and old['at_ms']==last_fill.get(oid,0)
                    and old['at_ms']<=stamp and filled==original)
                if (old['state']!=state or (old['at_ms']!=stamp and not final_fill_time)
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


def _planned_observation_reads(bindings, previous, cursor, end, *, reuse_verified_terminals,
                               verification_passes=2):
    """Exact initial request multiset; unknown split history pages are excluded."""
    account,_=life.validate_snapshot(previous)
    certificates=(terminal_certificates(bindings,previous) if reuse_verified_terminals else {})
    oids=sorted({oid for binding in bindings for leg in life.order_legs(binding)
                 for oid in binding['orders'][leg]}-set(certificates))
    bodies=[]
    def fills(start, stop):
        return dict(type='userFillsByTime',user=account,startTime=start,endTime=stop,
                    aggregateByTime=False)
    while end-cursor >= DAY_MS-OVERLAP_MS:
        stop=cursor+DAY_MS-2*OVERLAP_MS
        bodies.extend([fills(max(1,cursor-OVERLAP_MS),stop)]*2)
        cursor=stop
    start=max(1,cursor-OVERLAP_MS)
    one_pass=[fills(start,end),dict(type='frontendOpenOrders',user=account),
              dict(type='clearinghouseState',user=account)]
    one_pass.extend(dict(type='orderStatus',user=account,oid=int(oid)) for oid in oids)
    bodies.extend(one_pass*verification_passes)
    return bodies


def collect(evidence, reader, *, cursor_ms=None, clock=now_ms, elapsed=time.monotonic,
            plain_take_profit_oids=(), reuse_verified_terminals=False, verification_passes=2,
            observation_cache=None, observation_end_ms=None):
    if type(verification_passes) is not int or verification_passes not in (1, 2):
        raise SyncError('VERIFICATION_PASS_COUNT_INVALID')
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
    end,started = clock() if observation_end_ms is None else life.moment(observation_end_ms),elapsed()
    if end > clock(): raise SyncError('OBSERVATION_TIME_IN_FUTURE')
    if not 0 <= end-cursor <= MAX_CATCHUP_MS: raise SyncError('HISTORY_GAP_REQUIRES_REVIEW')
    plan=getattr(type(reader),'observation_batch',None)
    bodies=_planned_observation_reads(bindings,previous,cursor,end,
        reuse_verified_terminals=reuse_verified_terminals,verification_passes=verification_passes)
    if observation_cache is not None:
        if type(observation_cache) is not ObservationReadCache or observation_cache.reader is not reader:
            raise SyncError('EXACT_OBSERVATION_CACHE_OWNER_REQUIRED')
        bodies=observation_cache.missing_plan(bodies)
    context=(reader.observation_batch(bodies) if callable(plan) and bodies else nullcontext())
    with context:
        return _collect_funded(bindings,previous,reader,cursor,end,started,clock,elapsed,
            plain=plain,reuse_verified_terminals=reuse_verified_terminals,
            verification_passes=verification_passes,observation_cache=observation_cache)


def retained_anchor(reader, account, fill):
    """Prove one exact old fill remains inside the venue's retained history.

    A returned old fill bounds the account's latest-10,000-fill retention. An
    empty historical interval alone provides no such bound. Keep the original
    normalized identity and terms; an unrelated fill at that time is no proof.
    """
    stamp=life.moment(fill['at_ms'])
    if fill['account']!=life.address(account):
        raise SyncError('HISTORY_RETENTION_ANCHOR_ACCOUNT_MISMATCH')
    rows=merge_fills([fill],history(reader,account,stamp,stamp),account,
                     fill['symbol'],stamp,stamp)
    if fill not in rows:
        raise SyncError('HISTORY_RETENTION_ANCHOR_UNAVAILABLE')


@contextmanager
def retained_observation(reader, account, anchor, bodies):
    """Fund each read-only retention phase without holding later worst cases.

    The two history/inventory passes remain one fully funded atomic plan.
    Each independent retention bracket is funded immediately before its HTTP.
    No partial phase supplies evidence or a durable cursor: callers commit only
    after both brackets, both passes, and their original time/CAS/feed guards.
    Quota denial or a failed refund leaves the old proof and ENTRY fence intact.
    """
    stamp=life.moment(anchor['at_ms'])
    body=dict(type='userFillsByTime',user=account,startTime=stamp,endTime=stamp,
              aggregateByTime=False)
    from .request_budget import Budget,BudgetError,request_weight
    owner=vars(reader).get('budget')
    if isinstance(owner,Budget):
        # A successful retained anchor returns at least one fill (minimum 21
        # weight). Avoid spending that read repeatedly while the next complete
        # two-pass plan cannot fit even after a successful size refund. This
        # read-only hint neither reserves capacity nor grants an HTTP permit.
        # Concurrent use/larger responses still require ordinary admission.
        requested=max(request_weight('/info',body),
                      sum(request_weight('/info',item) for item in bodies)+21)
        available=owner.capacity(requested_weight=requested,priority=reader.priority)
        if not available['eligible']:
            raise BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED',
                **{key:available[key] for key in ('used_weight','requested_weight',
                                                'ceiling','retry_after_ms')})
    plan=getattr(type(reader),'observation_batch',None)
    def phase(reads):
        return reader.observation_batch(reads) if callable(plan) else nullcontext()
    with phase([body]):
        retained_anchor(reader,account,anchor)
    with phase(bodies):
        yield
    with phase([body]):
        retained_anchor(reader,account,anchor)


def _collect_retained(evidence, reader, *, cursor_ms, anchor, clock=now_ms,
                     elapsed=time.monotonic, inventory_guard=None):
    """Final bounded two-pass collection bracketed by a retained old fill.

    Both history/inventory passes are funded together, between independently
    funded retention checks. Nothing is committed from a partial observation.
    The caller must have durably reviewed every interval before cursor_ms.
    """
    bindings,previous=deepcopy(evidence['bindings']),deepcopy(evidence['snapshot'])
    life.validate_bindings(bindings)
    account,symbol=life.validate_snapshot(previous)
    if any(life.address(b['account'])!=account or b['symbol']!=symbol for b in bindings):
        raise SyncError('MIXED_BUCKET_BINDINGS')
    terminal_certificates(bindings,previous)
    cursor=life.moment(cursor_ms)
    end,started=clock(),elapsed()
    if cursor<previous['at_ms'] or not 0<=end-cursor<=DAY_MS//2:
        raise SyncError('HISTORY_RECOVERY_FINAL_WINDOW_INVALID')
    stamp=life.moment(anchor['at_ms'])
    if stamp>previous['at_ms']:
        raise SyncError('HISTORY_RETENTION_ANCHOR_TOO_NEW')
    bodies=_planned_observation_reads(bindings,previous,cursor,end,
                                     reuse_verified_terminals=True)
    with retained_observation(reader,account,anchor,bodies):
        result=_collect_funded(bindings,previous,reader,cursor,end,started,clock,elapsed,
                              plain=(),reuse_verified_terminals=True,
                              inventory_guard=inventory_guard)
    if elapsed()-started>15:
        raise SyncError('OBSERVATION_TOO_SLOW')
    return result


def _collect_funded(bindings, previous, reader, cursor, end, started, clock, elapsed,
                    *, plain, reuse_verified_terminals, inventory_guard=None, verification_passes=2, observation_cache=None):
    account,symbol=life.validate_snapshot(previous)
    pass_readers=([observation_cache.pass_reader(i) for i in (0,1)]
                  if observation_cache is not None else [reader,reader])
    # Reconstruct a short missed interval before taking the current order and
    # position snapshot. Each bounded window is observed twice; nothing is
    # persisted until the final full lifecycle review succeeds.
    while end-cursor >= DAY_MS-OVERLAP_MS:
        stop = cursor+DAY_MS-2*OVERLAP_MS
        start = max(1,cursor-OVERLAP_MS)
        first_fills = merge_fills(previous['fills'],history(pass_readers[0],account,start,stop),
                                  account,symbol,start,stop)
        second_fills = merge_fills(previous['fills'],history(pass_readers[1],account,start,stop),
                                   account,symbol,start,stop)
        if first_fills != second_fills: raise SyncError('OBSERVATION_CHANGED_RETRY')
        previous['fills'] = first_fills
        cursor = stop
    start = max(1,cursor-OVERLAP_MS)
    # A full audit compares two independent passes. The fast stream uses one
    # cross-checked inventory/status/fill/position pass for immediate protection;
    # it never accepts a transport acknowledgement as an observed execution.
    # Each pass uses only certificates that existed before this collection.
    guard={} if inventory_guard is None else dict(inventory_guard=inventory_guard)
    first = observe(bindings,previous,pass_readers[0],start,end,plain_take_profit_oids=plain,
                    reuse_verified_terminals=reuse_verified_terminals,**guard)
    second = (observe(bindings,previous,pass_readers[1],start,end,plain_take_profit_oids=plain,
                     reuse_verified_terminals=reuse_verified_terminals,**guard)
              if verification_passes == 2 else first)
    if first != second: raise SyncError('OBSERVATION_CHANGED_RETRY')
    if elapsed()-started>15: raise SyncError('OBSERVATION_TOO_SLOW')
    result = life.review(bindings,second,now_ms=clock(),plain_take_profit_oids=plain)
    return dict(bindings=bindings,snapshot=second,report=result,cursor_ms=end,
                verification_passes=verification_passes)
