"""Opt-in isolated experimental execution worker, never registered by the app.

A software exchange is injected at the ONLY transport boundary. The worker
performs real durable transactions, atomic account-wide admission, order/fill
reconciliation, protection and finality. No signer, HTTP client or real venue is
constructed here. Live transport/release/emergency-supervisor integration is NOT
implemented or authorized by this module; readiness advertises that limitation.

The existing same-symbol fence remains: pooled reduce-only orders cannot provide
independent per-formula OCO guarantees. Unknown submissions never get retried.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN

import experimental_execution_contract as contract
from . import card_lifecycle as life, checks, price_precision, request_budget
from . import filled_quantity_dispatch as wire, r2732_entry, r2732_conditional_stop
from . import maxpain_execution, experimental_allocations, two_account_execution as roles
from .experimental_plan_store import reduce_source
from .experimental_execution_state import ExecutionState, PostgresExecutionState, StateError
from .risk_policy import budget, assert_new_entry_budget

VERSION = 'experimental-isolated-worker-v1'
MODE = 'explicit_software_exchange_only_v1'
FINAL = frozenset(('CLOSED', 'CANCELED_WITHOUT_FILL'))
TERMINAL = frozenset(('FILLED', 'CANCELED', 'REJECTED'))


class RuntimeError(StateError):
    pass


def readiness():
    return dict(version=VERSION, mode='software_only', live_dispatch_enabled=False,
        final_live_gate_connected=False, signer_connected=False, production_ready=False,
        shared_symbol_parallel_enabled=False, real_exchange_requests_sent=0)


def _lane(account, symbol):
    return life.digest([account, symbol])


def _number(value):
    return life.number(value, positive=True)


def _sum(fills):
    return sum((_number(x['quantity']) for x in fills.values()), Decimal(0))


def _remaining(trade):
    return _sum(trade['entry_fills']) - _sum(trade['exit_fills'])


def _source_active(source, now):
    msg = source['source']
    return (source['entry_permission'] == 'WAITING' and source['cancellation'] is None
        and msg['kind'] != 'CANCEL' and msg['source_state'] == 'PENDING'
        and contract.moment_ms(msg['arm_at']) <= now < contract.moment_ms(msg['valid_until'])
        and (msg['expires_at'] is None or now < contract.moment_ms(msg['expires_at'])))


def _range(value, msg, now, *, account=None):
    if (not isinstance(value, dict) or value.get('environment') != ('mainnet' if account is None else 'testnet')
            or value.get('symbol') != msg['symbol'] or value.get('history_complete') is not True
            or value.get('reference_at_ms') != contract.moment_ms(msg['source_at'])
            or not isinstance(value.get('at_ms'), int) or not 0 <= now-value['at_ms'] <= 15000
            or value['at_ms'] < value['reference_at_ms']
            or account is None and value.get('price_source') != msg['policy']['source_price']
            or account is not None and (value.get('account') != account or value.get('price_kind') != 'MARK')):
        raise RuntimeError('AUTHORITATIVE_FRESH_SOURCE_AND_TESTNET_PATHS_REQUIRED')
    low, high, price = (_number(value[k]) for k in ('low', 'high', 'price'))
    if not low <= price <= high:
        raise RuntimeError('INVALID_COMPLETE_PRICE_RANGE')
    return low, high, price


class _Permit:
    """One-second local permit; weight was committed in the aggregate ledger."""
    def __init__(self, clock, at_ms):
        self.clock, self.at_ms, self.used = clock, at_ms, False

    def validate(self):
        if self.used or not 0 <= self.clock()-self.at_ms <= request_budget.PERMIT_MS:
            raise request_budget.BudgetError('TESTNET_REQUEST_BUDGET_PERMIT_EXPIRED')

    def check(self):
        self.validate()
        self.used = True


class IsolatedExecutionRuntime:
    def __init__(self, store, venue, *, mode=None):
        if mode != MODE or type(store) not in (ExecutionState, PostgresExecutionState):
            raise RuntimeError('EXPLICIT_ISOLATED_WORKER_REQUIRED')
        if (getattr(venue, 'domain', None) != 'software'
                or getattr(venue, 'software_only', None) is not True
                or isinstance(venue, wire.TestnetVenue)):
            raise RuntimeError('SOFTWARE_EXCHANGE_REQUIRED_NO_LIVE_ADAPTER')
        self.store, self.venue = store, venue
        store.load()

    def receive(self, messages):
        now = self.venue.now()
        values = [contract.validate(m) for m in messages]
        def transition(state):
            receipts=[]
            for msg in values:
                cid = msg['occurrence_id']
                source, changed = reduce_source(state['sources'].get(cid), msg,
                    now=contract.iso_ms(now), not_before=contract.iso_ms(state['not_before_ms']), domain='software')
                if changed:
                    state['sources'][cid] = source
                    if msg['family']=='sol_g65' and cid not in state.setdefault('formula_states',{}):
                        from . import sol_g65_conditional_stop
                        state['formula_states'][cid]=sol_g65_conditional_stop.initialize_from_contract(cid,msg)
                    state['events'].append(dict(at_ms=now, kind='SOURCE_'+msg['kind'], occurrence_id=cid))
                receipts.append(dict(occurrence_id=cid,revision=source['revision'],status='RECORDED' if changed else 'DUPLICATE',record_only=True,entry_permission=source['entry_permission']))
            return dict(recorded=len(values),receipts=receipts,**readiness())
        return self.store.mutate(transition)

    def _snapshot(self, state, snapshot, now):
        required = {'environment','account','symbol','at_ms','history_complete','orders_complete',
                    'position_complete','position_quantity','orders'}
        if (not isinstance(snapshot, dict) or set(snapshot) != required or snapshot['environment'] != 'testnet'
                or snapshot['account'] not in state['routes'].values()
                or any(snapshot[k] is not True for k in ('history_complete','orders_complete','position_complete'))
                or type(snapshot['at_ms']) is not int or not 0 <= now-snapshot['at_ms'] <= 15000
                or not isinstance(snapshot['orders'], list)):
            raise RuntimeError('FRESH_COMPLETE_ACCOUNT_INVENTORY_REQUIRED')
        lane = _lane(snapshot['account'], snapshot['symbol'])
        prior = state['snapshots'].get(lane)
        if prior and snapshot['at_ms'] < prior['at_ms']:
            raise RuntimeError('EXCHANGE_SNAPSHOT_REGRESSION')
        requests = {r['proposal']['action'].get('orders', [{}])[0].get('c'): r
            for r in state['requests'].values()
            if r['proposal']['action']['type'] == 'order' and r['proposal']['account'] == snapshot['account']
            and r['proposal']['symbol'] == snapshot['symbol']}
        for r in state['requests'].values():
            p = r['proposal']
            if p['action']['type'] == 'batchModify' and p['account'] == snapshot['account'] and p['symbol'] == snapshot['symbol']:
                requests[p['action']['modifies'][0]['order']['c']] = r
        seen, fills_seen = set(), set()
        for row in snapshot['orders']:
            fields = {'oid','cloid','wire_order','status','at_ms','fills'}
            if not isinstance(row, dict) or set(row) != fields or row['oid'] in seen or row['status'] not in TERMINAL | {'OPEN'}:
                raise RuntimeError('EXACT_ORDER_INVENTORY_REQUIRED')
            life.ident(row['oid'], r'[1-9][0-9]{0,19}')
            if int(row['oid']) >= 2**64:
                raise RuntimeError('EXACT_ORDER_INVENTORY_REQUIRED')
            seen.add(row['oid'])
            request = requests.get(row['cloid'])
            if request is None or request['phase'] == 'ABORTED_UNSENT':
                raise RuntimeError('UNOWNED_ORDER_OR_FILL_REQUIRES_RECONCILIATION')
            p = request['proposal']; action = p['action']
            expected = action['orders'][0] if action['type'] == 'order' else action['modifies'][0]['order']
            if (row['wire_order'] != expected or row['at_ms'] < request['attempt_at_ms']
                    or row['at_ms'] > snapshot['at_ms'] or request['observed_oid'] not in (None, row['oid'])):
                raise RuntimeError('ORDER_TERMS_OR_REQUEST_OWNERSHIP_MISMATCH')
            trade = state['trades'][p['card_id']]
            old = trade['orders'].get(row['oid'])
            if old and old['status'] in TERMINAL and row != old:
                raise RuntimeError('TERMINAL_ORDER_CHANGED')
            fills = {}
            for fill in row['fills']:
                if (set(fill) != {'fill_id','quantity','price','at_ms'} or fill['fill_id'] in fills_seen
                        or type(fill['at_ms']) is not int or not request['attempt_at_ms'] <= fill['at_ms'] <= snapshot['at_ms']):
                    raise RuntimeError('EXACT_MONOTONIC_FILL_HISTORY_REQUIRED')
                life.ident(fill['fill_id']); _number(fill['quantity']); _number(fill['price'])
                if p['leg'] == 'ENTRY':
                    limit = _number(expected['p'])
                    if _number(fill['price']) > limit if expected['b'] else _number(fill['price']) < limit:
                        raise RuntimeError('ENTRY_FILL_WORSE_THAN_FROZEN_LIMIT')
                fills_seen.add(fill['fill_id']); fills[fill['fill_id']] = deepcopy(fill)
            total = _sum(fills)
            if total > _number(expected['s']) or row['status'] == 'FILLED' and total != _number(expected['s']):
                raise RuntimeError('ORDER_FILLED_QUANTITY_MISMATCH')
            if row['status'] == 'REJECTED' and total:
                raise RuntimeError('REJECTED_ORDER_HAS_FILL')
            if old and any(fills.get(f['fill_id']) != f for f in old['fills']):
                raise RuntimeError('PREVIOUS_FILL_REMOVED_OR_CHANGED')
            target = trade['entry_fills'] if p['leg'] == 'ENTRY' else trade['exit_fills']
            for fid, fill in fills.items():
                full = {**fill, 'order_id': row['oid']}
                if fid in target and target[fid] != full:
                    raise RuntimeError('PREVIOUS_FILL_REMOVED_OR_CHANGED')
                target[fid] = full
            trade['orders'][row['oid']] = deepcopy(row)
            trade['order_legs'][row['oid']] = p['leg']
            request['phase'], request['observed_oid'] = 'OBSERVED', row['oid']
        for trade in state['trades'].values():
            if trade['account'] != snapshot['account'] or trade['symbol'] != snapshot['symbol']:
                continue
            if set(trade['orders'])-seen:
                raise RuntimeError('OWNED_ORDER_MISSING_FROM_COMPLETE_HISTORY')
            if _remaining(trade) < 0:
                raise RuntimeError('EXIT_EXCEEDED_OWNED_QUANTITY_PEER_RISK')
            pending = [r for r in state['requests'].values() if r['proposal']['card_id'] == trade['cid']
                and r['phase'] not in ('OBSERVED','ABORTED_UNSENT')]
            if _remaining(trade) == 0 and trade['orders'] and not pending and all(o['status'] in TERMINAL for o in trade['orders'].values()):
                phase = 'CLOSED' if trade['entry_fills'] else 'CANCELED_WITHOUT_FILL'
                if trade['phase'] != phase:
                    state['events'].append(dict(at_ms=now, kind=phase, occurrence_id=trade['cid']))
                trade['phase'] = phase
            elif trade['entry_fills']:
                trade['phase'] = 'OPEN' if not trade['exit_fills'] else 'PARTIALLY_CLOSED'
        # Cancellation replies never prove terminality; only the exact original OID does.
        for request in state['requests'].values():
            p = request['proposal']
            if p['action']['type'] == 'cancel' and p['account'] == snapshot['account'] and p['symbol'] == snapshot['symbol']:
                oid = str(p['action']['cancels'][0]['o'])
                observed = next((o for o in snapshot['orders'] if o['oid'] == oid), None)
                if observed and observed['status'] in TERMINAL and snapshot['at_ms'] >= request['attempt_at_ms']:
                    request['phase'], request['observed_oid'] = 'OBSERVED', oid
        expected_position = sum((_remaining(t)*(1 if t['side'] == 'LONG' else -1)
            for t in state['trades'].values() if t['account'] == snapshot['account'] and t['symbol'] == snapshot['symbol']), Decimal(0))
        if expected_position != life.number(snapshot['position_quantity'], signed=True):
            raise RuntimeError('POSITION_NOT_RECONCILED_TO_EXACT_OWNED_FILLS')
        self._allocations(state, snapshot, now)
        state['snapshots'][lane] = deepcopy(snapshot)

    def _allocations(self, state, snapshot, now):
        bindings, rows, fills = [], [], []
        for trade in state['trades'].values():
            if trade['account'] != snapshot['account'] or trade['symbol'] != snapshot['symbol'] or not trade['orders']:
                continue
            if not any(trade['order_legs'][oid] == 'ENTRY' for oid in trade['orders']):
                raise RuntimeError('OBSERVED_ENTRY_OWNERSHIP_REQUIRED')
            bindings.append(dict(occurrence_id=trade['cid'], account=trade['account'], account_role=trade['role'],
                symbol=trade['symbol'], side=trade['side'], orders=[dict(order_id=oid,kind=trade['order_legs'][oid],
                    quantity=o['wire_order']['s'], reduce_only=o['wire_order']['r']) for oid,o in trade['orders'].items()]))
        if not bindings:
            return
        for o in snapshot['orders']:
            rows.append(dict(order_id=o['oid'],status=o['status'],quantity=o['wire_order']['s'],
                filled_quantity=life.text(sum((_number(f['quantity']) for f in o['fills']),Decimal(0))),
                reduce_only=o['wire_order']['r'],side='BUY' if o['wire_order']['b'] else 'SELL'))
            fills += [{**f,'order_id':o['oid']} for f in o['fills']]
        role = next(r for r,a in state['routes'].items() if a == snapshot['account'])
        assessment = experimental_allocations.assess(bindings, dict(environment='testnet',account=snapshot['account'],
            account_role=role,symbol=snapshot['symbol'],at_ms=snapshot['at_ms'],history_complete=True,
            orders_complete=True,position_complete=True,position_quantity=snapshot['position_quantity'],orders=rows,fills=fills), now_ms=now)
        if assessment['status'] != 'RECONCILED':
            raise RuntimeError('ALLOCATION_RECONCILIATION_FAILED')

    def _proposal(self, state, trade, leg, action, quantity, now, *, operation):
        return dict(version=VERSION,card_id=trade['cid'],account=trade['account'],role=trade['role'],symbol=trade['symbol'],
            leg=leg,operation=operation,quantity=quantity,action=wire.canonical_wire_action(action),
            source_at=trade['source']['source_at'],source_expires_at=trade['source']['expires_at'],
            basis=life.digest(state['snapshots']),observed_at_ms=now)

    def _order(self, state, trade, leg, quantity, price, meta, now, *, old_oid=None):
        index, decimals = wire.asset(meta, trade['symbol'])
        if trade['asset'] != dict(index=index,decimals=decimals):
            raise RuntimeError('CONTRACT_METADATA_CHANGED')
        wire.precise(price,quantity,decimals)
        c = '0x'+life.digest([VERSION,trade['cid'],leg,len(state['requests']),price,quantity])[:32]
        order = dict(a=index,b=trade['side']=='LONG' if leg=='ENTRY' else trade['side']=='SHORT',p=price,s=quantity,
            r=leg!='ENTRY',t=dict(limit=dict(tif='Gtc' if trade['source']['family'] in ('maxpain','sol_g65') else 'Ioc'))
                if leg=='ENTRY' else dict(trigger=dict(isMarket=leg=='STOP',triggerPx=price,tpsl='sl' if leg=='STOP' else 'tp')),c=c)
        action = dict(type='order',orders=[order],grouping='na') if old_oid is None else dict(type='batchModify',modifies=[dict(oid=int(old_oid),order=order)])
        return self._proposal(state,trade,leg,action,quantity,now,operation='ENTRY' if leg=='ENTRY' else 'CREATE_EXIT' if old_oid is None else 'AMEND_EXIT')

    def _cancel(self,state,trade,oid,now):
        return self._proposal(state,trade,trade['order_legs'][oid],dict(type='cancel',cancels=[dict(a=trade['asset']['index'],o=int(oid))]),'0',now,operation='CANCEL')

    def _mark(self, trade, context, now, *, max_age=15000):
        value=context['marks'][_lane(trade['account'],trade['symbol'])]
        if (not isinstance(value,dict) or set(value)!={'environment','account','symbol','at_ms','mark_price'}
                or value['environment']!='testnet' or value['account']!=trade['account'] or value['symbol']!=trade['symbol']
                or type(value['at_ms']) is not int or not 0<=now-value['at_ms']<=max_age):
            raise RuntimeError('EXACT_FRESH_TESTNET_MARK_SAMPLE_REQUIRED')
        _number(value['mark_price'])
        return value

    def _conditional(self, trade, context, now):
        family = trade['source']['family']
        if family not in ('r2732','sol_g65'):
            return trade['prices']['stop']
        module = r2732_conditional_stop
        if family == 'sol_g65':
            from . import sol_g65_conditional_stop as module
        if trade['condition'] is None:
            trade['condition'] = module.initialize_from_contract(trade['cid'],trade['source'])
        bars = context.get('bars',{}).get(trade['cid'], [])
        trade['condition'] = module.advance(trade['condition'],bars,now_ms=now,price_source=trade['source']['policy']['source_price'])
        condition = trade['condition']
        if (condition.get('status') in ('OPEN','AWAITING_CLOSED_BAR') and condition.get('outcome') is None
                and condition.get('lock_effective_at_ms') is not None and condition['lock_effective_at_ms'] <= now
                and 0 <= now-condition['cursor_ms']-60000 <= 15000):
            return price_precision.round_price(trade['source']['policy']['locked_stop'],trade['asset']['decimals'])
        return trade['desired_stop']

    def _maintain(self, state, context, now):
        for trade in sorted(state['trades'].values(),key=lambda t:t['cid']):
            if trade['phase'] in FINAL:
                continue
            if any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT') and r['proposal']['card_id']==trade['cid'] for r in state['requests'].values()):
                continue
            active = [(oid,o) for oid,o in trade['orders'].items() if o['status']=='OPEN']
            remaining = _remaining(trade)
            if remaining:
                try:
                    trade['desired_stop'] = self._conditional(trade,context,now)
                except (ValueError,KeyError,TypeError):
                    trade['source_condition_error']='SOURCE_CANDLE_RECONCILIATION_REQUIRED'
                    # A source error must not erase the last verified venue stop
                    # or delay initial protection at the original frozen stop.
                # Protect the actually filled quantity before new admissions/cancellations.
                for leg,price in (('STOP',trade['desired_stop']),('TAKE_PROFIT',trade['prices']['take_profit'])):
                    own = [(oid,o) for oid,o in active if trade['order_legs'][oid]==leg]
                    if len(own)>1:
                        raise RuntimeError('MULTIPLE_ACTIVE_EXITS_REQUIRE_RECONCILIATION')
                    if not own or _number(own[0][1]['wire_order']['s'])-sum((_number(f['quantity']) for f in own[0][1]['fills']),Decimal(0)) != remaining or _number(own[0][1]['wire_order']['p']) != _number(price):
                        mark_sample=self._mark(trade,context,now)
                        mark = _number(mark_sample['mark_price'])
                        crossed = mark <= _number(price) if trade['side']=='LONG' and leg=='STOP' or trade['side']=='SHORT' and leg=='TAKE_PROFIT' else mark >= _number(price)
                        if crossed:
                            trade['emergency_reason']='ORIGINAL_EXIT_CROSSED'
                            for oid,o in active:
                                if trade['order_legs'][oid]=='ENTRY':
                                    return self._cancel(state,trade,oid,now)
                            from .emergency_close import close_price, MAX_REQUESTS
                            closes=[r for r in state['requests'].values() if r['proposal']['card_id']==trade['cid'] and r['proposal']['operation']=='EMERGENCY_CLOSE']
                            if len(closes)>=MAX_REQUESTS or any(r['phase']!='OBSERVED' or trade['orders'].get(r['observed_oid'],{}).get('status') not in TERMINAL for r in closes):
                                trade['emergency_reason']='EMERGENCY_CLOSE_RECONCILIATION_OR_LIMIT_REQUIRED'
                                continue
                            mark_sample=self._mark(trade,context,now,max_age=5000)
                            px=close_price(mark_sample['mark_price'],trade['asset']['decimals'],buy=trade['side']=='SHORT')
                            order=dict(a=trade['asset']['index'],b=trade['side']=='SHORT',p=px,s=life.text(remaining),r=True,
                                t=dict(limit=dict(tif='Ioc')),c='0x'+life.digest([VERSION,trade['cid'],'EMERGENCY_CLOSE',len(closes)])[:32])
                            result=self._proposal(state,trade,'STOP',dict(type='order',orders=[order],grouping='na'),life.text(remaining),now,operation='EMERGENCY_CLOSE')
                            result['sample']=dict(mark_price=mark_sample['mark_price'],at_ms=mark_sample['at_ms'])
                            return result
                        return self._order(state,trade,leg,life.text(remaining),price,context['metadata'],now,old_oid=own[0][0] if own else None)
            source = state['sources'][trade['cid']]
            retire = not _source_active(source,now) or bool(trade['exit_fills'])
            if any(trade['order_legs'][oid]=='ENTRY' for oid,o in active):
                mark=_number(self._mark(trade,context,now)['mark_price'])
                msg=trade['source']
                if msg['family']=='maxpain' and (mark>=_number(msg['original_target']) if msg['side']=='LONG' else mark<=_number(msg['original_target'])):
                    trade['entry_retired_reason']='TESTNET_TARGET_SAFETY_CANCEL_REMAINDER'
                if msg['family']=='sol_g65' and mark<=_number(msg['policy']['pending_cancel_price']):
                    trade['entry_retired_reason']='TESTNET_PENDING_CANCEL_BARRIER'
                retire=retire or trade.get('entry_retired_reason') is not None
            for oid,o in active:
                if (trade['order_legs'][oid]=='ENTRY' and retire) or (remaining==0 and trade['order_legs'][oid]!='ENTRY'):
                    return self._cancel(state,trade,oid,now)
        return None

    def _admit(self, state, cid, context, now):
        source = state['sources'][cid]; msg = source['source']
        if not _source_active(source,now) or cid in state['trades'] or cid in state.get('source_condition_errors',{}):
            return None
        if msg['family']=='sol_g65':
            condition=state.get('formula_states',{}).get(cid)
            if (condition is None or condition['phase']!='PENDING' or condition['outcome'] is not None
                    or condition['status'] in ('PRICE_GAP','AMBIGUOUS','EXIT_RECONCILIATION_REQUIRED')):
                return None
        role = 'long_account' if msg['side']=='LONG' else 'short_account'; account=state['routes'][role]
        peers = [t for t in state['trades'].values() if t['account']==account and t['phase'] not in FINAL]
        if any(t['symbol']==msg['symbol'] for t in peers):
            return None  # Net-position exchange cannot guarantee independent OCO.
        if msg['family'] in ('r2732','hype_row71205','sol_g65') and any(t['source']['family']==msg['family'] for t in peers):
            return None
        lane = _lane(account,msg['symbol'])
        if (lane not in state['snapshots'] or not 0<=now-state['snapshots'][lane]['at_ms']<=15000
                or life.number(state['snapshots'][lane]['position_quantity'],signed=True)!=0):
            raise RuntimeError('EMPTY_OWNED_MARKET_EVIDENCE_REQUIRED')
        paths = context['ranges'][cid]
        slo,shi,smark = _range(paths['source'],msg,now)
        dlo,dhi,mark = _range(paths['testnet'],msg,now,account=account)
        metadata=context['metadata']; index,decimals=wire.asset(metadata,msg['symbol'])
        raw = dict(kind='SIGNAL',event_id=cid,symbol=msg['symbol'],side=msg['side'],entry=msg['entry'],stop=msg['stop'],take_profit=msg['take_profit'],at=msg['source_at'])
        prepared=price_precision.prepare_signal(raw,metadata); p=prepared['execution']
        prices={k:p[k] for k in ('entry','stop','take_profit')}
        entry,stop,take=(_number(prices[k]) for k in ('entry','stop','take_profit'))
        if not min(stop,take)<mark<max(stop,take):
            raise RuntimeError('TESTNET_MARK_OUTSIDE_ORIGINAL_EXITS')
        if msg['family']=='r2732':
            rs=r2732_entry.initial({k:dict(account=a) for k,a in state['routes'].items()},not_before_ms=state['not_before_ms'])
            relevant={k:s for k,s in state['sources'].items() if s['source']['family']=='r2732'}
            rs['records']={k:dict(source_record=deepcopy(s),request=None) for k,s in relevant.items()}
            rs['latest_source_ms']=max(contract.moment_ms(s['source']['source_at']) for s in relevant.values())
            owned=dict(environment='testnet',account=account,account_role=role,at_ms=now,complete=True,unresolved_request=False,
                occurrences=[dict(occurrence_id=t['cid'],request_id=t['entry_request'],family=t['source']['family'],symbol=t['symbol'],phase=t['phase'],
                    remaining_quantity=life.text(_remaining(t)),working_orders=any(o['status']=='OPEN' for o in t['orders'].values())) for t in peers])
            rp=r2732_entry.entry_admission(rs,cid,metadata,paths['testnet'],paths['source'],owned,now_ms=now)
            quantity=rp['quantity']
        else:
            # Both producer and testnet paths must still precede this occurrence's barriers.
            if msg['side']=='LONG':
                consumed=shi>=_number(msg['original_target'] if msg['family']=='maxpain' else msg['take_profit']) or slo<=_number(msg['entry'])
            else:
                consumed=slo<=_number(msg['original_target'] if msg['family']=='maxpain' else msg['take_profit']) or shi>=_number(msg['entry'])
            if msg['family']=='maxpain':
                if consumed or (mark<=entry if msg['side']=='LONG' else mark>=entry):
                    raise RuntimeError('PROSPECTIVE_ENTRY_ALREADY_CONSUMED')
                peers_source=[s['source'] for k,s in state['sources'].items() if k!=cid and s['source']['rule_id']==msg['rule_id']
                    and (k in state['trades'] and state['trades'][k]['phase'] not in FINAL)]
                if maxpain_execution.overlap_admission(msg,peers_source)!='FORMULA_OVERLAP_ALLOWED':
                    raise RuntimeError('FORMULA_OVERLAP_NOT_ALLOWED')
            elif msg['family']=='sol_g65':
                if (shi>=_number(msg['entry']) or slo<=_number(msg['policy']['pending_cancel_price'])
                        or dhi>=entry or mark>=entry):
                    raise RuntimeError('G65_PENDING_ENTRY_OR_CANCEL_ALREADY_CONSUMED')
            elif msg['family']=='hype_row71205':
                reference=contract.moment_ms(msg['source_at'])
                if not reference<=now<contract.moment_ms(msg['expires_at']) or shi>=_number(msg['stop']) or slo<=_number(msg['take_profit']) or dhi>=stop or dlo<=take:
                    raise RuntimeError('HYPE_ORIGINAL_ENTRY_WINDOW_CONSUMED')
            else:
                raise RuntimeError('APPROVED_FORMULA_ADAPTER_REQUIRED')
            step=Decimal(1).scaleb(-decimals)
            quantity=life.text((budget()/abs(entry-stop)/step).to_integral_value(rounding=ROUND_DOWN)*step)
        if _number(quantity)*abs(entry-stop)>budget():
            raise RuntimeError('CURRENT_RISK_BUDGET_EXCEEDED')
        cap=context['capacity'][cid]
        if (cap.get('account')!=account or type(cap.get('at_ms')) is not int or not 0<=now-cap['at_ms']<=15000 or cap.get('action_headroom',0)<roles.ENTRY_ACTION_HEADROOM
                or cap.get('account_mode_verified') is not True):
            raise RuntimeError('FRESH_EXACT_ACCOUNT_CAPACITY_REQUIRED')
        checks.plan_check(dict(symbol=msg['symbol'],side=msg['side'],**prices),msg['symbol'],decimals,
            _number(cap['unheld']),_number(cap['available']),_number(cap['max_size']),active=cap['active'],metadata_max_leverage=cap['max_leverage'])
        trade=dict(cid=cid,source=deepcopy(msg),account=account,role=role,symbol=msg['symbol'],side=msg['side'],
            quantity=quantity,prices=prices,asset=dict(index=index,decimals=decimals),phase='OUTCOME_UNKNOWN',
            orders={},order_legs={},entry_fills={},exit_fills={},entry_request=None,condition=None,desired_stop=prices['stop'])
        # Reuse the independent current-risk defense with the same frozen terms.
        bracket=[]
        for leg,price in (('ENTRY',prices['entry']),('STOP',prices['stop']),('TAKE_PROFIT',prices['take_profit'])):
            order=self._order(state,trade,leg,quantity,price,metadata,now)['action']['orders'][0]
            if leg=='ENTRY':order['t']={'limit':{'tif':'Gtc'}}
            bracket.append(order)
        assert_new_entry_budget(dict(type='order',orders=bracket,grouping='normalTpsl'))
        state['trades'][cid]=trade
        return self._order(state,trade,'ENTRY',quantity,prices['entry'],metadata,now)

    def _reserve(self,state,proposal,now):
        weight=request_budget.request_weight('/exchange',dict(action=proposal['action']))
        state['budget']=[x for x in state['budget'] if now-x['at_ms']<request_budget.WINDOW_MS]
        ceiling=request_budget.BACKGROUND_LIMIT if proposal['leg']=='ENTRY' and proposal['operation']=='ENTRY' else request_budget.LIMIT
        if sum(x['weight'] for x in state['budget'])+weight>ceiling:
            raise request_budget.BudgetError('TESTNET_REQUEST_BUDGET_EXHAUSTED')
        nonce=max(now,state.get('last_nonce',0)+1)
        if nonce>now+1000:
            raise RuntimeError('DURABLE_NONCE_WINDOW_EXHAUSTED_DEFER')
        state['last_nonce']=nonce
        rid=life.digest([VERSION,proposal,len(state['requests']),now])
        request=dict(request_id=rid,domain='software',bucket=_lane(proposal['account'],proposal['symbol']),proposal=proposal,
            nonce=nonce,attempt_at_ms=now,prepared_at_ms=now,attempts=1,phase='OUTCOME_UNKNOWN',reply=None,observed_oid=None)
        state['requests'][rid]=request
        state['budget'].append(dict(at_ms=now,weight=weight))
        if proposal['operation']=='ENTRY':state['trades'][proposal['card_id']]['entry_request']=rid
        state['events'].append(dict(at_ms=now,kind='ATTEMPT_COMMITTED',request_id=rid,occurrence_id=proposal['card_id']))
        return request

    def report(self):
        from .experimental_execution_reporting import project
        return project(self.store.load())

    def run_once(self, *, entries_enabled=True):
        """One observed cycle; entry halt never disables existing protection."""
        before=self.store.load()
        context=self.venue.collect(deepcopy(before))
        now=self.venue.now(); life.moment(now)
        def transition(state):
            if context.get('basis_revision')!=state['revision']:
                raise RuntimeError('CONCURRENT_OBSERVATION_RELOAD_REQUIRED')
            if (context.get('inventory_complete') is not True or context.get('inventory_accounts')!=sorted(state['routes'].values())
                    or type(context.get('inventory_at_ms')) is not int or not 0<=now-context['inventory_at_ms']<=15000):
                raise RuntimeError('COMPLETE_FRESH_TWO_ACCOUNT_INVENTORY_REQUIRED')
            for snapshot in context['snapshots']:
                self._snapshot(state,snapshot,now)
            needed={_lane(t['account'],t['symbol']) for t in state['trades'].values() if t['phase'] not in FINAL}
            observed={_lane(s['account'],s['symbol']) for s in context['snapshots']}
            if not needed<=observed:
                raise RuntimeError('FULL_ACTIVE_INVENTORY_REQUIRED')
            for cid,condition in state.get('formula_states',{}).items():
                from . import sol_g65_conditional_stop
                try:
                    condition=sol_g65_conditional_stop.advance(condition,context.get('bars',{}).get(cid,[]),now_ms=now,price_source=state['sources'][cid]['source']['policy']['source_price'])
                    state.setdefault('source_condition_errors',{}).pop(cid,None)
                except (ValueError,KeyError,TypeError):
                    state.setdefault('source_condition_errors',{})[cid]='SOURCE_CANDLE_RECONCILIATION_REQUIRED'
                state['formula_states'][cid]=condition
                if cid in state['trades']:state['trades'][cid]['condition']=deepcopy(condition)
            proposal=self._maintain(state,context,now)
            unknown=any(r['phase'] not in ('OBSERVED','ABORTED_UNSENT') for r in state['requests'].values())
            if proposal is None and entries_enabled and not unknown:
                for cid in sorted(state['sources'],key=lambda k:(state['sources'][k]['source']['source_at'],k)):
                    proposal=self._admit(state,cid,context,now)
                    if proposal is not None:break
            return self._reserve(state,proposal,now) if proposal else None
        request=self.store.mutate(transition)
        if request is None:
            return dict(status='OBSERVED_NO_ACTION',**readiness())
        # Final local source check after commit; canceled or stale entries are
        # definitely unsent, with their tombstone retained across restart.
        def final_check(state):
            current=state['requests'][request['request_id']]
            if current!=request:raise RuntimeError('EXACT_DURABLE_ATTEMPT_REQUIRED')
            if (request['proposal']['operation']=='ENTRY' and not _source_active(state['sources'][request['proposal']['card_id']],self.venue.now())):
                current['phase']='ABORTED_UNSENT'
                state['trades'][request['proposal']['card_id']]['phase']='CANCELED_WITHOUT_FILL'
                return False
            return True
        if not self.store.mutate(final_check):
            return dict(status='CANCELED_BEFORE_TRANSPORT',**readiness())
        admission=wire.TransportAdmission(request['proposal'],_Permit(self.venue.now,now))
        admission.bind(request)
        try:
            reply=wire.send_admitted(self.venue,request,admission)
        except Exception:
            # Uncertain outcome, including lost reply: preserve committed request.
            return dict(status='OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED',**readiness())
        def receipt(state):
            current=state['requests'][request['request_id']]
            if current['phase']=='OUTCOME_UNKNOWN':current['reply']=deepcopy(reply)
        self.store.mutate(receipt)
        return dict(status='SOFTWARE_ATTEMPT_RECORDED_AWAITING_OBSERVATION',operation=request['proposal']['operation'],**readiness())
