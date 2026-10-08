"""Pure adapter from existing collector proofs to prospective worker evidence.

No network/reader is created here. Caller must use existing budget-funded
collectors for complete account snapshots and exact persisted-CLOID orderStatus
lookups. The adapter never infers a fill from source candles, mark or ACK.
Historical MARK coverage is a separate prerequisite; trade candles or isolated
current mark samples cannot substitute for a continuously observed mark window.
"""
from copy import deepcopy
from decimal import Decimal

from . import card_lifecycle as life, filled_quantity_dispatch as wire


class EvidenceError(ValueError):
    pass


def normalize_snapshot(state, snapshot, lookups, *, now_ms):
    """Convert existing complete life.snapshot plus independent exact lookups.

    Missing lookup/unknownOid retains uncertainty: no fabricated terminal state
    is returned. Existing known OIDs can never disappear from complete history.
    Exact exchange terms are proved by the unchanged legacy wire.identity gate.
    """
    life.validate_snapshot(snapshot)
    if (snapshot['history_complete'] is not True or snapshot['orders_complete'] is not True
            or not 0<=now_ms-snapshot['at_ms']<=15000
            or snapshot['account'] not in state['routes'].values()
            or not isinstance(lookups,dict)):
        raise EvidenceError('FRESH_COMPLETE_OWN_ACCOUNT_EVIDENCE_REQUIRED')
    role=next(r for r,a in state['routes'].items() if a==snapshot['account'])
    quantity=life.number(snapshot['position_quantity'],signed=True)
    if quantity and (quantity>0)!=(role=='long_account'):
        raise EvidenceError('ACCOUNT_DIRECTION_MISMATCH')
    opened={o['oid']:o for o in snapshot['open_orders']}
    terminal={o['oid']:o for o in snapshot['terminal_orders']}
    if set(opened)&set(terminal):
        raise EvidenceError('ORDER_CANNOT_BE_OPEN_AND_TERMINAL')
    fills={};fill_ids=set()
    for fill in snapshot['fills']:
        if fill['fill_id'] in fill_ids:
            raise EvidenceError('DUPLICATE_FILL_ID')
        fill_ids.add(fill['fill_id']);fills.setdefault(fill['oid'],[]).append(fill)
    observations={}
    for request in state['requests'].values():
        p=request['proposal']
        if (p['account']!=snapshot['account'] or p['symbol']!=snapshot['symbol']
                or p['action']['type']=='cancel' or request['phase']=='ABORTED_UNSENT'):
            continue
        order=wire.requested_order(p['action']);raw=lookups.get(order['c'])
        if raw is None or raw=={'status':'unknownOid'}:
            if request.get('observed_oid') is not None:
                raise EvidenceError('PREVIOUSLY_OWNED_ORDER_LOOKUP_MISSING')
            continue
        oid=wire.identity(raw,request,now_ms)
        if request.get('observed_oid') not in (None,oid):
            raise EvidenceError('PREVIOUSLY_OWNED_ORDER_ID_CHANGED')
        if oid in observations:
            raise EvidenceError('ORDER_BOUND_TO_MULTIPLE_REQUESTS')
        if raw['order']['statusTimestamp']>snapshot['at_ms']:
            raise EvidenceError('ORDER_LOOKUP_NEWER_THAN_ACCOUNT_SNAPSHOT')
        row=opened.get(oid) or terminal.get(oid)
        if row is None:
            raise EvidenceError('LOOKUP_ORDER_ABSENT_FROM_COMPLETE_SNAPSHOT')
        own_fills=fills.get(oid,[])
        if any(f['at_ms']<request['attempt_at_ms'] for f in own_fills):
            raise EvidenceError('FILL_PRECEDES_OWNED_ATTEMPT')
        total=sum((life.number(f['quantity'],positive=True) for f in own_fills),Decimal(0))
        wanted_side='B' if order['b'] else 'A'
        if any(f['side']!=wanted_side for f in own_fills):
            raise EvidenceError('FILL_DIRECTION_CHANGED')
        if total>life.number(order['s']):
            raise EvidenceError('FILLS_EXCEED_OWNED_ORDER_SIZE')
        raw_status=raw['order']['status']
        activated=raw_status=='triggered'
        if activated:
            # wire.identity binds the original TP trigger to its persisted
            # request. The complete collector separately proved the same-OID
            # live limit (or exact full fills); keep that transition explicit.
            # A bare LIMIT label, a market stop, or an activation ACK is not
            # accepted as protection/finality.
            original=raw['order']['order'];trigger=order['t'].get('trigger')
            activation=raw['order']['statusTimestamp']
            if (p['leg']!='TAKE_PROFIT' or trigger is None
                    or trigger.get('tpsl')!='tp' or trigger.get('isMarket') is not False
                    or order['r'] is not True
                    or life.number(order['p'])!=life.number(trigger['triggerPx'])
                    or life.number(original.get('sz'),positive=True)!=life.number(order['s'])
                    or life.moment(original.get('timestamp'))>activation
                    or any(f['at_ms']<activation for f in own_fills)):
                raise EvidenceError('TRIGGERED_TAKE_PROOF_INVALID')
        if oid in opened:
            status='OPEN'
            if not activated and raw_status!='open':
                raise EvidenceError('LOOKUP_AND_SNAPSHOT_TERMINALITY_CONFLICT')
            if (row['side']!=wanted_side or row['reduce_only'] is not order['r']
                    or life.number(row['quantity'])+total!=life.number(order['s'])
                    or life.number(row['price'])!=life.number(order['p'])
                    or row['state']!='ACTIVE'):
                raise EvidenceError('OPEN_ORDER_TERMS_OR_QUANTITY_CHANGED')
            trigger=order['t'].get('trigger')
            typ='LIMIT' if trigger is None else 'SL_MARKET' if trigger['tpsl']=='sl' else 'TP_LIMIT'
            if activated:
                valid_trigger=(row['order_type']=='TRIGGERED_TP_LIMIT' and row['trigger_price'] is None)
            else:
                valid_trigger=(row['order_type']==typ and row['trigger_price']==(None if trigger is None else trigger['triggerPx']))
            if not valid_trigger:
                raise EvidenceError('OPEN_ORDER_TRIGGER_TERMS_CHANGED')
        else:
            status=row['state']
            if life.number(row['filled_quantity'])!=total:
                raise EvidenceError('TERMINAL_FILL_HISTORY_INCOMPLETE')
            triggered_full=(activated and status=='FILLED' and bool(own_fills)
                and total==life.number(order['s']) and row['at_ms']==max(f['at_ms'] for f in own_fills))
            if ((status=='FILLED' and raw_status!='filled' and not triggered_full)
                    or status=='CANCELED' and raw_status not in wire.evidence.CANCELED
                    or status=='REJECTED' and raw_status not in wire.evidence.REJECTED):
                raise EvidenceError('LOOKUP_AND_SNAPSHOT_TERMINALITY_CONFLICT')
            if (any(f['at_ms']>row['at_ms'] for f in own_fills)
                    or status=='FILLED' and total!=life.number(order['s'])
                    or status=='REJECTED' and total):
                raise EvidenceError('TERMINAL_FILL_PROOF_INVALID')
            if not activated and row['at_ms']!=raw['order']['statusTimestamp']:
                preserved_fill_certificate=(status=='FILLED' and bool(own_fills)
                    and row['at_ms']==max(f['at_ms'] for f in own_fills)
                    and row['at_ms']<raw['order']['statusTimestamp'])
                if not preserved_fill_certificate:
                    raise EvidenceError('TERMINAL_CERTIFICATE_TIME_CONFLICT')
        # A later venue status convergence must not rewrite a committed
        # last-fill terminal certificate and fail immutable-history replay.
        stamp=(row['at_ms'] if oid in terminal else
            max([raw['order']['statusTimestamp']]+[f['at_ms'] for f in own_fills]))
        observations[oid]=dict(oid=oid,cloid=order['c'],wire_order=deepcopy(order),status=status,at_ms=stamp,
            fills=[{k:f[k] for k in ('fill_id','quantity','price','at_ms')} for f in own_fills])
    if set(observations)!=(set(opened)|set(terminal)|set(fills)):
        raise EvidenceError('UNOWNED_OR_UNPROVEN_ORDER_IN_ACCOUNT_HISTORY')
    return dict(environment='testnet',account=snapshot['account'],symbol=snapshot['symbol'],
        at_ms=snapshot['at_ms'],history_complete=True,orders_complete=True,position_complete=True,
        position_quantity=snapshot['position_quantity'],orders=[observations[k] for k in sorted(observations,key=int)])


def mark_sample(raw, *, account, symbol, observed_at_ms, now_ms):
    """Read official metaAndAssetCtxs; preserve its original observation time."""
    from .experimental_market_context import MarketSnapshot
    try:
        return MarketSnapshot(raw,observed_at_ms=observed_at_ms).mark(
            account=account,symbol=symbol,now_ms=now_ms)
    except ValueError as exc:
        raise EvidenceError(str(exc)) from None


def require_mark_window(window, *, account, symbol, reference_at_ms, now_ms):
    """Validate recorder proof; never derive complete history from sampled marks.

    The production recorder must supply this explicit continuity contract from
    the reference boundary. No such proof can be reconstructed from an alert or
    a current metaAndAssetCtxs reply, and gaps permanently reject that occurrence.
    """
    life.shape(window,'environment account symbol price_kind reference_at_ms at_ms history_complete high low price')
    if (window['environment']!='testnet' or window['account']!=account or window['symbol']!=symbol
            or window['price_kind']!='MARK' or window['reference_at_ms']!=reference_at_ms
            or window['history_complete'] is not True or type(window['at_ms']) is not int
            or not reference_at_ms<=window['at_ms']<=now_ms or now_ms-window['at_ms']>15000):
        raise EvidenceError('CONTINUOUS_REFERENCE_MARK_WINDOW_REQUIRED')
    lo,hi,price=(life.number(window[k],positive=True) for k in ('low','high','price'))
    if not lo<=price<=hi:
        raise EvidenceError('MARK_WINDOW_GEOMETRY_INVALID')
    return deepcopy(window)
