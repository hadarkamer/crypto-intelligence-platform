"""Durable position recovery decisions for the existing execution owner.

Pure journal transitions only: no sender, scheduler, wallet or network client.
Read reservations are conservative and committed before collection, so a crash
can spend a reservation but cannot reset a round's bounds.
"""
from copy import deepcopy
from decimal import Decimal

from . import card_lifecycle as life, doge_partial_execution as doge
from .experimental_execution_runtime import _lane

WINDOW_MS = 60000
EXTRA_SENDS = 2
READ_CALLS = 24
READ_WEIGHT = 720
BATCH_CALLS = 8
BATCH_WEIGHT = 240
RETRY_MS = 1000
DONE = ('OBSERVED', 'ABORTED_UNSENT')
PAUSED = ('WAITING', 'MANUAL', 'RESUME_REQUESTED')
EXITS = ('CREATE_EXIT', 'AMEND_EXIT')
TRANSIENT = frozenset(('TESTNET_REQUEST_BUDGET_EXHAUSTED',
    'TESTNET_REQUEST_BUDGET_BUSY', 'TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED',
    'TESTNET_OBSERVATION_BATCH_EXPIRED', 'DISPATCH_CONTEXT_CHANGED_RECONCILE_FIRST',
    'FRESH_COLLECTOR_CHECKPOINT_REQUIRED_BEFORE_DISPATCH', 'FINAL_EVIDENCE_EXPIRED', 'RECOVERY_ROUND_DEADLINE'))


def records(state):
    return state.setdefault('position_recovery', {})


def pending(state, cid):
    return [r for r in state['requests'].values()
            if r['proposal']['card_id'] == cid and r['phase'] not in DONE]


def paused(state, cid):
    trade = state['trades'].get(cid, {})
    return bool(trade.get('manual_management') or
                state.get('position_recovery', {}).get(cid, {}).get('phase') in PAUSED)


def _target(proposal):
    # DOGE's two frozen target prices are two different required exits.
    from .filled_quantity_dispatch import requested_order
    return proposal['leg'] + (':' + requested_order(proposal['action'])['p']
        if proposal['leg'] == 'TAKE_PROFIT' else '')


def _facts(state, trade, context, now):
    from .experimental_execution_runtime import _lane, _remaining, FINAL
    lane = _lane(trade['account'], trade['symbol'])
    sample = context.get('external_observation', {}).get('accounts', {}).get(trade['account'])
    quantity = life.text(_remaining(trade))
    stamp = state.get('snapshots', {}).get(lane, {}).get('at_ms')
    if sample and 0 <= now - sample['observed_at_ms'] <= 15000:
        quantity = sample['positions'].get(trade['symbol'], '0')
        stamp = sample['observed_at_ms']
    known = (lane not in context.get('blocked_lanes', {})
             and type(stamp) is int and 0 <= now - stamp <= 15000
             and not pending(state, trade['cid']))
    closed = trade['phase'] in FINAL and Decimal(quantity) == 0 and known
    legs = {'STOP': 'UNKNOWN', 'TAKE_PROFIT': 'UNKNOWN'}
    if known and _remaining(trade) > 0:
        wanted = (doge.desired_orders(trade) if doge.is_doge(trade['source']) else
            [('STOP', trade.get('desired_stop', trade['prices']['stop']), quantity),
             ('TAKE_PROFIT', trade['prices']['take_profit'], quantity)])
        for leg in legs:
            targets = [(price, Decimal(q)) for l, price, q in wanted if l == leg]
            own = [o for oid, o in trade['orders'].items()
                   if trade['order_legs'][oid] == leg and o['status'] == 'OPEN']
            def matches(o, price, q):
                wire = o['wire_order']
                left = Decimal(wire['s']) - sum((Decimal(f['quantity']) for f in o['fills']), Decimal(0))
                return wire['r'] is True and Decimal(wire['p']) == Decimal(price) and left == abs(q)
            legs[leg] = ('PRESENT' if len(own) == len(targets) and all(
                sum(matches(o, price, q) for o in own) == 1 for price, q in targets) else 'MISSING')
    elif sample and type(stamp) is int and 0 <= now-stamp <= 15000:
        # Complete fresh inventory can prove absence, never execution success.
        if not any(o['symbol'] == trade['symbol'] for o in sample['orders']):
            legs = {leg: 'MISSING' for leg in legs}
    return dict(quantity=quantity, observed_at_ms=stamp, exits=legs, closed=closed,
                complete=known, unresolved_requests=[r['request_id'] for r in pending(state, trade['cid'])])


def _start(state, trade, now, previous=None):
    episode = 1 if previous is None else previous['episode'] + 1
    row = dict(cid=trade['cid'], account=trade['account'], symbol=trade['symbol'],
        role=trade['role'], episode=episode, event_id=life.digest([trade['cid'], episode])[:24],
        round=1, phase='RECOVERING', started_ms=now, deadline_ms=now+WINDOW_MS,
        sends={}, read_calls=0, read_weight=0, next_ms=now, retryable=True,
        ordinary_targets=sorted({_target(r['proposal'])
            for r in state['requests'].values() if r['proposal']['card_id']==trade['cid']
            and r['proposal']['operation'] in EXITS}), notices=[], facts=_facts(state,trade,{},now))
    records(state)[trade['cid']] = row
    return row


def inherited_incidents(state, now):
    """Fence already-known open uncertainty before an upgraded worker collects.

    A saved position is a reason to investigate, never fresh authority to send.
    This avoids an unbounded first history pull for an old unresolved position.
    """
    from .experimental_execution_runtime import _remaining, FINAL
    result=[]
    for cid,trade in state['trades'].items():
        if cid in state.get('position_recovery',{}) or paused(state,cid) or trade['phase'] in FINAL: continue
        saved=state.get('external_activity',{}).get('accounts',{}).get(trade['account'],{})
        exposure=Decimal(saved.get('positions',{}).get(trade['symbol'],'0'))
        if (_remaining(trade)>0 or exposure!=0) and any(now-r['attempt_at_ms']>=5000 for r in pending(state,cid)):
            result.append(cid)
    return result


def adopt_incidents(state, now):
    for cid in inherited_incidents(state,now):_start(state,state['trades'][cid],now)


def failed_attempt(state, request, now):
    """Record a failed/ambiguous ordinary exit before its next observation."""
    p=request['proposal']
    if p['operation'] not in EXITS or paused(state,p['card_id']): return
    row=state.get('position_recovery',{}).get(p['card_id'])
    if row is None or row['phase']=='RESOLVED':
        _start(state,state['trades'][p['card_id']],now,row)


def _finish(state, row, phase, now, reason):
    row.update(phase=phase, reason=reason, finished_ms=now)
    identity = life.digest([row['event_id'], row['round']])[:24]
    notice = next((n for n in row['notices'] if n['id'] == identity), None)
    result = dict(phase=phase, reason=reason, facts=deepcopy(row['facts']),
                  retryable=row['retryable'], round=row['round'])
    if notice is None:
        row['notices'].append(dict(id=identity, revision=1, result=result,
            delivery='PENDING', attempts=0, message_id=None, retry_at_ms=now))
        state['events'].append(dict(kind='POSITION_RECOVERY_RESULT', at_ms=now,
            occurrence_id=row['cid'], event_id=row['event_id'], round=row['round'], reason=reason))
    elif notice['result'] != result:
        notice.update(result=result, revision=notice['revision']+1, retry_at_ms=now)
        if notice['delivery'] not in ('SENDING', 'UNKNOWN'):
            notice['delivery'] = 'PENDING'


def advance(state, context, now):
    """Observe outcomes first, then permit a bounded round; never place orders."""
    for cid, trade in state['trades'].items():
        row = state.get('position_recovery', {}).get(cid)
        if trade.get('manual_management'):
            if row: row['phase'] = 'MANUAL'
            continue
        facts = _facts(state, trade, context, now)
        own = [r for r in state['requests'].values() if r['proposal']['card_id'] == cid]
        after = (row or {}).get('finished_ms', 0)
        failed = [r for r in own if r['proposal']['operation'] in EXITS
            and r['attempt_at_ms'] > after and (
                r['phase'] == 'ABORTED_UNSENT' or (r.get('reply') or {}).get('state') == 'REJECTED'
                or r['phase'] not in DONE and now-r['attempt_at_ms'] >= 5000)]
        unresolved_exposure = Decimal(facts['quantity']) != 0 and not facts['complete']
        healthy = facts['closed'] or all(v == 'PRESENT' for v in facts['exits'].values())
        if row is None or row['phase'] == 'RESOLVED':
            if healthy or not (failed or unresolved_exposure): continue
            row = _start(state, trade, now, row)
        row['facts'] = facts
        if row['phase'] == 'MANUAL': continue
        if healthy:
            _finish(state, row, 'RESOLVED', now, 'POSITION_CLOSED' if facts['closed'] else 'EXITS_PRESENT')
            continue
        if row['phase'] == 'RESUME_REQUESTED':
            row.update(phase='RECOVERING', round=row['round']+1, started_ms=now,
                deadline_ms=now+WINDOW_MS, sends={}, read_calls=0, read_weight=0,
                next_ms=now)
        if row['phase'] != 'RECOVERING': continue
        bad = [r for r in failed if (r.get('reply') or {}).get('state') == 'REJECTED'
               or r['phase'] == 'ABORTED_UNSENT' and r.get('unsent_reason') not in TRANSIENT]
        if bad:
            row['retryable'] = False
            _finish(state, row, 'WAITING', now, 'ACTION_REQUIRES_REVIEW')
        elif context.get('blocked_lanes', {}).get(_lane(trade['account'], trade['symbol'])) == 'RECOVERY_HISTORY_GAP_REQUIRES_REVIEW':
            row['retryable'] = False
            _finish(state, row, 'WAITING', now, 'RECOVERY_HISTORY_GAP_REQUIRES_REVIEW')
        elif now >= row['deadline_ms']:
            _finish(state, row, 'WAITING', now, 'ROUND_DEADLINE')


def permit_proposal(state, proposal, now):
    cid = proposal['card_id']
    if paused(state, cid): return False
    row = state.get('position_recovery', {}).get(cid)
    if row is None or row['phase'] == 'RESOLVED': return True
    if row['phase'] != 'RECOVERING' or now >= row['deadline_ms'] or now < row['next_ms']:
        return False
    if proposal['operation'] in EXITS:
        if row['sends'].get(_target(proposal), 0) >= EXTRA_SENDS:
            _finish(state, row, 'WAITING', now, 'SEND_LIMIT')
            return False
    return True


def transport_allowed(state, request, now):
    cid=request['proposal']['card_id']
    if paused(state,cid): return False
    row=state.get('position_recovery',{}).get(cid)
    if row is None or row['phase']=='RESOLVED': return True
    return (row['phase']=='RECOVERING' and now<row['deadline_ms']
            and request.get('recovery_round',[row['event_id'],row['round']])==[row['event_id'],row['round']])


def attempted(state, request):
    row = state.get('position_recovery', {}).get(request['proposal']['card_id'])
    if row and row['phase'] == 'RECOVERING' and request['proposal']['operation'] in EXITS:
        key = _target(request['proposal'])
        if key not in row['ordinary_targets']:
            row['ordinary_targets'].append(key)
        else:
            row['sends'][key] = row['sends'].get(key, 0) + 1
        row['next_ms'] = request['attempt_at_ms'] + RETRY_MS
        request['recovery_round'] = [row['event_id'], row['round']]


def reserve_reads(state, now):
    """Reserve one finite collection envelope before any additional inquiry."""
    grants = {}
    for cid, row in state.get('position_recovery', {}).items():
        if row['phase'] != 'RECOVERING' or now < row['next_ms']: continue
        if now >= row['deadline_ms']:
            _finish(state, row, 'WAITING', now, 'ROUND_DEADLINE'); continue
        if row['read_calls']+BATCH_CALLS > READ_CALLS or row['read_weight']+BATCH_WEIGHT > READ_WEIGHT:
            _finish(state, row, 'WAITING', now, 'INQUIRY_LIMIT'); continue
        row['read_calls'] += BATCH_CALLS; row['read_weight'] += BATCH_WEIGHT
        grants[cid] = dict(calls=BATCH_CALLS, weight=BATCH_WEIGHT, deadline=row['deadline_ms'])
    return grants


def choose(state, *, token, round_number, choice, actor, allowed_users, now):
    if str(actor) not in {str(u) for u in allowed_users}:
        raise ValueError('RECOVERY_USER_NOT_AUTHORIZED')
    if choice not in ('auto', 'manual'): raise ValueError('RECOVERY_CHOICE_INVALID')
    row = next((r for r in state.get('position_recovery', {}).values() if r['event_id'] == token), None)
    if row is None or row['round'] != round_number or row['phase'] != 'WAITING':
        return dict(status='STALE_OR_ALREADY_APPLIED')
    if choice == 'auto' and not row['retryable']: return dict(status='REVIEW_REQUIRED')
    trade = state['trades'].get(row['cid'])
    if trade is None: return dict(status='POSITION_NOT_ACTIVE')
    row.update(phase='RESUME_REQUESTED' if choice == 'auto' else 'MANUAL',
               choice=dict(actor=str(actor), at_ms=now, value=choice))
    for notice in row['notices']:
        if notice['result']['round'] == round_number:
            notice['result']['phase'] = row['phase']
            notice['revision'] += 1
            if notice['delivery'] not in ('SENDING','UNKNOWN'):
                notice.update(delivery='PENDING',retry_at_ms=now)
    if choice == 'manual':
        from . import experimental_external_activity as external
        value = state.setdefault('external_activity', dict(version=external.VERSION,
            accounts={}, human_managed={}, events=[], evicted_events=0))
        value['human_managed'][external.lane(trade['account'], trade['symbol'])] = dict(
            account=trade['account'], symbol=trade['symbol'], at_ms=now, status=external.HUMAN,
            reason='EXPLICIT_TRADER_CHOICE', automatic_resume=False, card_ids=[trade['cid']])
        trade['manual_management'] = dict(status=external.HUMAN, started_at_ms=now,
            reason='EXPLICIT_TRADER_CHOICE', accounting_allocation='EXTERNAL_ACTIVITY_UNALLOCATED')
    state['events'].append(dict(kind='POSITION_RECOVERY_CHOICE', at_ms=now,
        occurrence_id=row['cid'], event_id=token, round=round_number, choice=choice, actor=str(actor)))
    return dict(status='RECORDED')


class InquiryMeter:
    """Cap this already-reserved envelope; the existing shared budget still applies."""
    def __init__(self, grant, clock):
        self.calls, self.weight, self.deadline = grant['calls'], grant['weight'], grant['deadline']
        self.clock = clock

    def debit(self, kind):
        from .request_budget import request_weight
        cost = request_weight('/info', dict(type=kind))
        if self.clock() >= self.deadline or self.calls < 1 or self.weight < cost:
            raise ValueError('RECOVERY_INQUIRY_LIMIT')
        self.calls -= 1; self.weight -= cost


class InquiryReader:
    """Use the existing reader and admission, with a round-local upper bound."""
    def __init__(self, reader, meter):
        self.reader, self.meter = reader, meter

    def read(self, kind, account, **kwargs):
        self.meter.debit(kind)
        return self.reader.read(kind, account, **kwargs)

    def observation_batch(self, bodies):
        from contextlib import nullcontext
        method = getattr(self.reader, 'observation_batch', None)
        return method(bodies) if callable(method) else nullcontext()

    def reserve_history_split(self, *args):
        method = getattr(self.reader, 'reserve_history_split', None)
        if callable(method): method(*args)
