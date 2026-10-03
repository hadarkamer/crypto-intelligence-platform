"""Durable Testnet-only loss-of-stop containment, disabled unless explicitly enabled.

A separate request lane can reduce confirmed exposure while a normal stop reply
is uncertain. All lanes share bucket locks and agent nonces. No request is ever
replayed. IOC exits are recorded as STOP exits only AFTER exact intent identity,
terminal status and two comparable public fill/position observations agree.
"""
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import json
import threading

from . import card_lifecycle as life, filled_quantity_dispatch as dispatch
from .filled_dispatch_store import DispatchError, DefinitelyUnsent, SCHEMA
from .card_lifecycle_store import _continues
from .dispatch_concurrency import market_lane

VERSION = 'testnet-emergency-close-v1'
APPROVAL = 'approved_testnet_v1'
CONTINUOUS_RELEASE = 'continuous_testnet_v1'
DEADLINE_MS = 5000  # Testnet experiment setting, not a Mainnet latency guarantee.
SLIPPAGE = Decimal('0.01')
LOCK = 1729048241
MAX_REQUESTS = 128
DONE = ('OBSERVED', 'EXPIRED_NO_PUBLIC_ORDER', 'EXPIRED_NO_PUBLIC_TERMINAL', 'ABORTED_UNSENT')
_health = dict(running=False, last_pass_at_ms=None, last_status='DISABLED')
_thread = None
_stop_event = None
_lock = threading.Lock()


def continuous_configuration(env):
    """Explicit continuous Testnet scope; absent retains the old one-card trial.

    This selects a safety integration, not ENTRY permission. Source freshness,
    account ownership, live notification reconciliation, observed quantities,
    admission and all durable attempt fences still run at their own boundaries.
    """
    release = env.get('HL_TESTNET_EMERGENCY_RELEASE', '')
    if release == '':
        return False
    if (release != CONTINUOUS_RELEASE
            or env.get('HL_TESTNET_EMERGENCY_CLOSE') != APPROVAL
            or env.get('RENDER_SERVICE_ID') != dispatch.roles.SERVICE
            or env.get('HL_TESTNET_RUNTIME_MODE') != 'long_stream_testnet_v1'
            or env.get('HL_TESTNET_FILLED_DISPATCH') != 'approved_long_stream_v1'
            or env.get('HL_TESTNET_LONG_STREAM') != 'approved_alerts_v1'
            or env.get('HL_TESTNET_SHORT_STREAM') != 'approved_alerts_v1'
            or env.get('HL_TESTNET_LONG_ENTRY_ENABLED') not in ('true', 'false')
            or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED') not in ('true', 'false')
            or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION') != 'disabled'
            or env.get('HL_TESTNET_JOURNAL_BACKEND') != 'staging_postgres_v1'
            or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY') != dispatch.AFTER_EXIT
            or env.get('HL_TESTNET_SAFETY_PIPELINE')
            or env.get('HL_TESTNET_CARD_SYNC')
            or env.get('HL_TESTNET_FILLED_CARD_ID')):
        raise DispatchError('CONTINUOUS_EMERGENCY_TESTNET_CONFIGURATION_REQUIRED')
    return True


def continuous_storage(venue, *, verify_schema=False):
    """Normal and emergency work must share the same real durable quota/journal.

    Constructing the budget performs local fixed database identity checks. The
    explicit startup verifies initialized schemas; individual reservations keep
    verifying the durable policy. Never make schema repairs in an ENTRY gate.
    """
    from .postgres_journal import PostgresJournal
    from .request_budget import Budget, ready
    store = vars(venue).get('store')
    if (venue.domain != 'testnet' or not isinstance(store, dispatch.DispatchStore)
            or store.domain != 'testnet' or not isinstance(store.journal, PostgresJournal)
            or store.journal._ci is not False or dispatch.HOST != 'api.hyperliquid-testnet.xyz'):
        raise DispatchError('CONTINUOUS_EMERGENCY_DURABLE_TESTNET_STORE_REQUIRED')
    budget = Budget.from_env(venue.env)
    if budget is None or budget.journal._parameters != store.journal._parameters:
        raise DispatchError('CONTINUOUS_EMERGENCY_SHARED_REQUEST_JOURNAL_REQUIRED')
    if verify_schema:
        with store.journal._transaction() as conn:
            store.ready(conn)
            ready(conn, expected_database=store.journal._parameters['dbname'])
    return budget


def fence(conn, state, operation):
    """Called under the common global+bucket locks before normal reserve/begin."""
    if state.get('emergency') is not None:
        raise DispatchError('EMERGENCY_BUCKET_MANAGED_BY_SEPARATE_LANE')
    if operation == 'ENTRY' and conn.execute(
            f"SELECT 1 FROM {SCHEMA}.buckets WHERE value->'emergency' IS NOT NULL LIMIT 1").fetchone():
        raise DispatchError('EMERGENCY_CIRCUIT_LATCHED_NO_NEW_ENTRY')
    if operation == 'ENTRY' and ('history_gap_recovery' in state or
            (state.get('account') and conn.execute(f'''SELECT 1 FROM {SCHEMA}.buckets
                WHERE value->>'account'=%s
                AND value->'history_gap_recovery' IS NOT NULL LIMIT 1''',
                (state['account'],)).fetchone())):
        # The stream scheduler is only one caller. Persisted missing-history
        # work fences every new ENTRY on this account under the common lock;
        # protection, cancellation and reduction keep their existing authority.
        raise DispatchError('ACCOUNT_HISTORY_GAP_REQUIRES_RECONCILIATION')


def view(state, now_ms):
    ids = [r['observed_oid'] for r in (state.get('emergency') or {}).get('requests', [])
           if r['proposal']['operation'] == 'EMERGENCY_CLOSE'
           and r['phase'] == 'OBSERVED' and r.get('observed_oid')]
    return life.review(state['bindings'], state['evidence']['snapshot'], now_ms=now_ms,
                       emergency_stop_oids=ids)


def incident_identity(state):
    """Bind an explicit operator release to the full immutable incident record."""
    incident=state.get('emergency')
    if not isinstance(incident,dict) or incident.get('version')!=VERSION:
        raise DispatchError('EXACT_CLOSED_EMERGENCY_INCIDENT_REQUIRED')
    return life.digest([VERSION,state['account'],state['symbol'],incident])


def closed_release_proof(state, *, now_ms, not_before_ms):
    """A final incident can be archived only with newly observed flat finality.

    Require ordinary lifecycle finality too: archival must never make an old
    emergency-only exception unreadable by the ordinary controller.
    """
    try:
        incident=state.get('emergency');evidence=state.get('evidence')
        if (not isinstance(incident,dict) or incident.get('version')!=VERSION
                or incident.get('phase')!='CLOSED_VERIFIED' or incident.get('provisional') is True
                or state['pending'] is not None or incident['pending_close'] is not None
                or incident['pending_cancel'] is not None or evidence is None
                or evidence['bindings']!=state['bindings']
                or not state['bindings']
                or any(r.get('phase') not in DONE for r in incident['requests'])):
            return False
        if not incident['requests']:
            # An operator's independently proved exit can close an incident
            # before this bot sends anything. Require its durable exact audit.
            binding=next(b for b in state['bindings'] if b['card_id']==incident['card_id'])
            audits=[a for a in state.get('manual_exit_audit',[]) if
                    a.get('card_id')==incident['card_id'] and a.get('origin')=='MANUAL'
                    and a.get('order_id') in binding['orders'].get('MANUAL_EXIT',[])
                    and a.get('entry_or_exit_order_sent') is False
                    and a.get('observed_at_ms')==incident.get('closed_at_ms')
                    and a.get('proof_digest')==incident.get('closure_proof_digest')]
            if incident.get('closure_origin')!='MANUAL' or len(audits)!=1:
                return False
        snap=evidence['snapshot']
        if (snap['account']!=state['account'] or snap['symbol']!=state['symbol']
                or any(b['account']!=state['account'] or b['symbol']!=state['symbol']
                       for b in state['bindings'])
                or snap['history_complete'] is not True or snap['orders_complete'] is not True
                or type(incident.get('closed_at_ms')) is not int
                or not incident['latched_at_ms']<=incident['closed_at_ms']<=snap['at_ms']
                or not not_before_ms<=snap['at_ms']<=now_ms or now_ms-snap['at_ms']>5000
                or life.number(snap['position_quantity'],signed=True)!=0 or snap['open_orders']):
            return False
        report=view(state,now_ms)
        normal=life.review(state['bindings'],snap,now_ms=now_ms)
        for check in (report,normal):
            if (check['bucket_issues'] or not check['cards']
                    or any(row['issues'] or row['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')
                           or life.number(row['remaining_quantity'],signed=True)!=0
                           for row in check['cards'])):
                return False
        selected=next(row for row in report['cards'] if row['card_id']==incident['card_id'])
        return selected['closure_verified'] is True and life.number(selected['entry_quantity'])>0
    except (KeyError,TypeError,ValueError,StopIteration,life.LifecycleError):
        return False


def _release_inventory(account, symbol, role, states, orders, positions):
    """Validate exact account ownership, allowing unrelated known positions."""
    if (not isinstance(orders,list) or len(orders)>10000
            or not isinstance(positions,dict) or not isinstance(positions.get('assetPositions'),list)
            or len(positions['assetPositions'])>10000
            or role not in dispatch.roles.ROLES):
        raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_INVENTORY_INVALID')
    by_symbol={state['symbol']:state for state in states}
    if (len(by_symbol)!=len(states) or symbol not in by_symbol
            or any(state['account']!=account or state['pending'] is not None
                   or any(b['account']!=account or b['symbol']!=state['symbol'] or b['role']!=role
                          for b in state['bindings']) for state in states)):
        raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_REQUEST_OR_OWNER_CHANGED')
    known={(state['symbol'],oid) for state in states for b in state['bindings']
           for ids in b['orders'].values() for oid in ids}
    expected={state['symbol']:(life.number(state['evidence']['snapshot']['position_quantity'],signed=True)
              if state['evidence'] is not None else Decimal(0)) for state in states}
    if any(q and (q>0)!=(role=='long_account') for q in expected.values()):
        raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_DIRECTION_INVALID')
    owned=[]
    for row in orders:
        if (not isinstance(row,dict) or not isinstance(row.get('coin'),str)
                or type(row.get('oid')) is not int or row['coin']==symbol
                or (row['coin'],str(row['oid'])) not in known):
            raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_ORDER_NOT_VERIFIED')
        owned.append((row['coin'],row['oid']))
    if len(set(owned))!=len(owned):
        raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_ORDER_NOT_VERIFIED')
    actual={}
    for row in positions['assetPositions']:
        p=row.get('position') if isinstance(row,dict) else None
        if (not isinstance(p,dict) or not isinstance(p.get('coin'),str) or p['coin'] in actual):
            raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_INVENTORY_INVALID')
        q=life.number(p.get('szi'),signed=True)
        if q!=expected.get(p['coin'],Decimal(0)) or q and (q>0)!=(role=='long_account'):
            raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_POSITION_NOT_VERIFIED')
        actual[p['coin']]=q
    if any(q and coin not in actual for coin,q in expected.items()) or actual.get(symbol,Decimal(0))!=0:
        raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_POSITION_NOT_VERIFIED')
    return dict(orders=sorted(owned),positions=sorted((coin,life.text(q)) for coin,q in actual.items() if q))


def _uncovered_tranches(state, *, now_ms):
    """Oldest still-uncovered fill tranche; a new partial fill cannot reset it."""
    if not state['bindings'] or state['evidence'] is None:
        return
    snap = state['evidence']['snapshot']
    report = life.review(state['bindings'], snap, now_ms=snap['at_ms'])
    for binding in state['bindings']:
        row = next(v for v in report['cards'] if v['card_id'] == binding['card_id'])
        remaining = life.number(row['remaining_quantity'], signed=True)
        covered = life.number(row['stop_quantity_observed'])
        if remaining <= 0 or covered >= remaining:
            continue
        entries = sorted((f for f in snap['fills'] if f['oid'] in binding['orders']['ENTRY']),
                         key=lambda f: (f['at_ms'], f['fill_id']))
        skip = life.number(row['exit_quantity']) + covered
        oldest = None
        for fill in entries:
            quantity = life.number(fill['quantity'])
            if skip >= quantity:
                skip -= quantity
                continue
            oldest = fill['at_ms']
            break
        if oldest is None or now_ms < oldest:
            raise DispatchError('UNPROTECTED_FILL_TIMELINE_INVALID')
        yield binding,oldest,remaining-covered


def reconciliation_wait_ms(state, *, now_ms):
    """Joining another reader cannot extend any original uncovered deadline."""
    wait=250
    for _,oldest,_ in _uncovered_tranches(state,now_ms=now_ms):
        wait=min(wait,max(0,oldest+DEADLINE_MS-now_ms))
    return wait


def trigger(state, *, now_ms, mark=None):
    for binding,oldest,quantity in _uncovered_tranches(state,now_ms=now_ms):
        crossed = False
        if mark is not None:
            px, stop = life.number(mark, positive=True), life.number(binding['prices']['stop'])
            crossed = px <= stop if binding['side'] == 'LONG' else px >= stop
        if crossed or now_ms-oldest >= DEADLINE_MS:
            return dict(card_id=binding['card_id'],
                        reason='STOP_LEVEL_PASSED_UNPROTECTED' if crossed else 'STOP_VERIFICATION_DEADLINE',
                        uncovered_since_ms=oldest, uncovered_quantity=life.text(quantity))
    return None


def provisional_stop_proof(state, *, now_ms):
    """Only fresh complete STOP proof may withdraw an unsent early freeze."""
    incident=state.get('emergency')
    evidence=state.get('evidence')
    if (incident is None or incident.get('provisional') is not True
            or incident['phase']!='ACTIVE' or incident['requests']
            or incident['pending_close'] is not None or incident['pending_cancel'] is not None
            or state['pending'] is not None or evidence is None
            or evidence['bindings']!=state['bindings']
            or any(binding['account']!=state['account'] or binding['symbol']!=state['symbol']
                   for binding in state['bindings'])):
        return False
    snap=evidence['snapshot']
    if (snap['account']!=state['account'] or snap['symbol']!=state['symbol']
            or snap['history_complete'] is not True or snap['orders_complete'] is not True
            or not incident['latched_at_ms']<=snap['at_ms']<=now_ms
            or now_ms-snap['at_ms']>5000):
        return False
    report=view(state,now_ms)
    if report['bucket_issues'] or trigger(state,now_ms=now_ms) is not None:
        return False
    by_card={binding['card_id']:binding for binding in state['bindings']}
    opened={order['oid'] for order in snap['open_orders']}
    active=sum(life.number(row['remaining_quantity'],signed=True)>0 for row in report['cards'])
    for row in report['cards']:
        remaining=life.number(row['remaining_quantity'],signed=True)
        if remaining>0:
            # A late STOP can be proven before the normal lane creates TP.
            # Normal maintenance may create or resize TP while the exact STOP
            # remains active. Every other ownership/quantity issue still blocks.
            if (set(row['issues'])-{'TAKE_PROFIT_COVERAGE_MISSING','TAKE_PROFIT_EXCEEDS_CARD_REMAINDER'}
                    or life.number(row['stop_quantity_observed'])!=remaining):
                return False
            if 'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER' in row['issues']:
                take_ids=set(by_card[row['card_id']]['orders']['TAKE_PROFIT'])
                takes=[order for order in snap['open_orders'] if order['oid'] in take_ids]
                # An oversized aggregate reduce-only exit must not consume a
                # different card's allocation, or require an ambiguous repair.
                if active!=1 or len(takes)!=1 or takes[0]['state']!='ACTIVE':
                    return False
        else:
            owned={oid for ids in by_card[row['card_id']]['orders'].values() for oid in ids}
            if (remaining!=0 or row['issues']
                    or row['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL') or opened&owned):
                return False
    return bool(report['cards'])


def provisional_cleanup_proof(state, *, now_ms):
    """Fresh filled exits can authorize cancellation of owned flat leftovers."""
    incident=state.get('emergency');evidence=state.get('evidence')
    if (incident is None or incident.get('provisional') is not True
            or incident['phase']!='ACTIVE' or incident['requests']
            or incident['pending_close'] is not None or incident['pending_cancel'] is not None
            or evidence is None or evidence['bindings']!=state['bindings']):
        return False
    snap=evidence['snapshot']
    if (snap['account']!=state['account'] or snap['symbol']!=state['symbol']
            or snap['history_complete'] is not True or snap['orders_complete'] is not True
            or not incident['latched_at_ms']<=snap['at_ms']<=now_ms
            or now_ms-snap['at_ms']>5000
            or life.number(snap['position_quantity'],signed=True)!=0):
        return False
    report=view(state,now_ms)
    if (report['bucket_issues'] or not report['cards']
            or any(life.number(row['remaining_quantity'],signed=True)!=0
                   or set(row['issues'])-{'FLAT_WITH_WORKING_ORDERS'}
                   or (row['card_id']!=incident['card_id'] and (row['issues']
                       or row['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')))
                   for row in report['cards'])):
        return False
    binding,row=safe_card(state,incident['card_id'],now_ms)
    exits=set(binding['orders']['STOP']+binding['orders']['TAKE_PROFIT'])
    owned={oid for ids in binding['orders'].values() for oid in ids}
    return (life.number(row['entry_quantity'])>0
            and life.number(row['exit_quantity'])==life.number(row['entry_quantity'])
            and any(order['oid'] in owned for order in snap['open_orders'])
            and any(order['oid'] in exits and order['state']=='FILLED'
                    and life.number(order['filled_quantity'])>0 for order in snap['terminal_orders']))


def recent_flat_unassigned_checkpoint(state, *, now_ms):
    """Bound duplicate REST probing of already-observed flat manual activity.

    At most FIVE seconds, matching the emergency quantity freshness bound.
    Never applies to exposure, working orders, pending requests or incomplete
    evidence. It grants no action and keeps the incident/entry latch unchanged.
    """
    ev = state.get('evidence'); incident = state.get('emergency')
    if (not isinstance(incident, dict) or incident.get('phase') != 'ACTIVE'
            or incident.get('pending_close') or incident.get('pending_cancel')
            or state.get('pending') or not ev or ev['bindings'] != state['bindings']):
        return False
    snap = ev['snapshot']
    if (snap['account'] != state['account'] or snap['symbol'] != state['symbol']
            or not snap['history_complete'] or not snap['orders_complete']
            or snap['open_orders'] or life.number(snap['position_quantity'], signed=True) != 0
            or not 0 <= now_ms-snap['at_ms'] < 5000):
        return False
    report = life.review(state['bindings'], snap, now_ms=now_ms)
    return set(report['bucket_issues']) == {
        'POSITION_DOES_NOT_MATCH_CARDS', 'UNASSIGNED_EXCHANGE_ACTIVITY'}


def recent_normal_checkpoint(state, *, now_ms, fill_wakeups=None):
    """A successful normal checkpoint can briefly serve both supervisors.

    A running or stalled read provides no authority here. Events are only dirty
    hints: they prevent this optimization but never supply fill/stop evidence.
    An uncovered tranche also requires independent checking before its deadline,
    since a current price can already have crossed the original stop.
    """
    if state.get('emergency') is not None or state['evidence'] is None:
        return False
    evidence=state['evidence'];snapshot=evidence['snapshot']
    if (evidence['bindings'] != state['bindings']
            or snapshot['account'] != state['account'] or snapshot['symbol'] != state['symbol']
            or not snapshot['history_complete'] or not snapshot['orders_complete']
            or not 0 <= now_ms-snapshot['at_ms'] < 15000):
        return False
    if fill_wakeups is not None:
        dirty=fill_wakeups.dirty_symbols(state['account'])
        if dirty is None or state['symbol'] in dirty:
            return False
    report=life.review(state['bindings'],snapshot,now_ms=now_ms)
    if report['bucket_issues']:
        return False
    for row in report['cards']:
        remaining=life.number(row['remaining_quantity'],signed=True)
        if remaining < 0 or (remaining > 0 and
                life.number(row['stop_quantity_observed']) != remaining):
            return False
    if now_ms-snapshot['at_ms'] < DEADLINE_MS:
        return True
    # Extend only duplicate probing of a quiet fully protected position. The
    # underlying quantity/price bounds for an actual close remain five seconds.
    # Working entries, uncertainty or a disconnected feed retain the base bound.
    if (fill_wakeups is None or state.get('pending') is not None
            or not dispatch._fully_protected_no_work(state,now_ms)):
        return False
    healthy_feed=getattr(type(fill_wakeups),'entry_allowed',None)
    return (callable(healthy_feed)
            and fill_wakeups.entry_allowed(state['account']) is True)


def fresh_stop_coverage(state, *, now_ms):
    """A complete fresh STOP proof needs no mark or static asset metadata.

    This only suppresses unrelated reads after public reconciliation. It does
    not postpone reconciliation, extend a timestamp, or authorize an order.
    A younger uncovered partial fill must still check the current price.
    """
    evidence=state.get('evidence')
    if (state.get('emergency') is not None or state.get('pending') is not None
            or evidence is None or evidence['bindings']!=state['bindings']
            or not state['bindings']):
        return False
    snapshot=evidence['snapshot']
    if (snapshot['account']!=state['account'] or snapshot['symbol']!=state['symbol']
            or snapshot['history_complete'] is not True or snapshot['orders_complete'] is not True
            or not 0<=now_ms-snapshot['at_ms']<=DEADLINE_MS
            or any(b['account']!=state['account'] or b['symbol']!=state['symbol']
                   for b in state['bindings'])):
        return False
    report=view(state,now_ms)
    if report['bucket_issues'] or not report['cards']:
        return False
    for row in report['cards']:
        remaining=life.number(row['remaining_quantity'],signed=True)
        if remaining>0:
            # TP work belongs to the ordinary lane. It cannot make an exact,
            # active original STOP less protective of this observed tranche.
            if (life.number(row['stop_quantity_observed'])!=remaining
                    or set(row['issues'])-{'TAKE_PROFIT_COVERAGE_MISSING'}):
                return False
        elif (remaining!=0 or row['issues']
                or row['state'] not in ('CLOSED','CANCELED_WITHOUT_FILL')):
            return False
    return True


def close_price(mark, decimals, *, buy):
    """Aggressive IOC within 1% of the sampled mark, rounded towards the mark."""
    px = life.number(mark, positive=True) * (1+SLIPPAGE if buy else 1-SLIPPAGE)
    exponent = max(-(6-decimals), px.adjusted()-4) if px < Decimal('100000') else 0
    step = Decimal(1).scaleb(exponent)
    px = px.quantize(step, rounding=ROUND_DOWN if buy else ROUND_UP)
    mark_px=life.number(mark,positive=True)
    if px <= 0 or (px < mark_px if buy else px > mark_px):
        raise DispatchError('EMERGENCY_PRICE_NOT_REPRESENTABLE')
    return life.text(px)


def safe_card(state, cid, now_ms):
    """Close only a uniquely owned remainder reconciled with the net position."""
    report = view(state, now_ms)
    if report['bucket_issues']:
        raise DispatchError('EMERGENCY_POSITION_OR_HISTORY_NOT_VERIFIED')
    row = next(v for v in report['cards'] if v['card_id'] == cid)
    allowed = {'STOP_COVERAGE_MISSING', 'TAKE_PROFIT_COVERAGE_MISSING',
               'STOP_EXCEEDS_CARD_REMAINDER', 'TAKE_PROFIT_EXCEEDS_CARD_REMAINDER',
               'FLAT_WITH_WORKING_ORDERS', 'BOTH_EXIT_LEGS_FILLED_REVIEW'}
    if any(set(v['issues'])-allowed for v in report['cards']):
        raise DispatchError('EMERGENCY_CARD_QUANTITY_NOT_VERIFIED')
    active = [v for v in report['cards'] if life.number(v['remaining_quantity'], signed=True) > 0]
    if active and (len(active) != 1 or active[0]['card_id'] != cid):
        raise DispatchError('EMERGENCY_SHARED_MARKET_NOT_EXCLUSIVE')
    binding = next(b for b in state['bindings'] if b['card_id'] == cid)
    original = state['originals'][cid]['card']
    rebound = life.binding_from_card(original, state['account'],
        {binding['role']: {'account': state['account']}}, binding['orders'])
    if rebound != binding:
        raise DispatchError('EMERGENCY_IMMUTABLE_CARD_CHANGED')
    return binding, row


def record_timing(state,request,lookup,now_ms):
    """Keep venue activation separate from completed public observation.

    Never use acknowledgement time. Pre-existing stops without their exact
    creation lookup retain unknown venue latency, not an invented measurement.
    Observation precedes the database commit; it is not a persistence timer.
    """
    if not state['bindings'] or state['evidence'] is None:
        return
    report=life.review(state['bindings'],state['evidence']['snapshot'],now_ms=now_ms)
    if report['bucket_issues']:
        return
    timing=state.setdefault('protection_timing',{})
    snap=state['evidence']['snapshot']
    for binding in state['bindings']:
        entries=[f for f in snap['fills'] if f['oid'] in binding['orders']['ENTRY']]
        if not entries:
            continue
        cid=binding['card_id'];first=min(f['at_ms'] for f in entries)
        armed=state.get('entry_timing_armed',{}).get(cid)
        if armed is None or first<armed:
            continue
        item=timing.setdefault(cid,dict(first_fill_at_ms=first,first_fill_observed_at_ms=now_ms,
            timing_semantics='public_observation_before_commit'))
        row=next(v for v in report['cards'] if v['card_id']==cid)
        remaining=life.number(row['remaining_quantity'],signed=True)
        if (remaining<=0 or life.number(row['stop_quantity_observed'])!=remaining
                or set(row['issues'])-{'TAKE_PROFIT_COVERAGE_MISSING','TAKE_PROFIT_EXCEEDS_CARD_REMAINDER'}
                or 'stop_observed_at_ms' in item or 'stop_verified_at_ms' in item):
            continue
        activated=None
        if (request and request['proposal']['card_id']==cid and request['proposal']['leg']=='STOP'
                and request['proposal']['operation'] in ('CREATE_EXIT','MODIFY_EXIT')
                and isinstance(lookup,dict) and lookup.get('status')=='order'
                and lookup['order'].get('status')=='open'
                and str(lookup['order']['order']['oid']) in binding['orders']['STOP']):
            stamp=life.moment(lookup['order']['statusTimestamp'])
            if first<=stamp<=now_ms:
                activated=stamp
        item.update(stop_observed_at_ms=now_ms,stop_public_status_at_ms=activated,
                    quantity_at_stop_verification=life.text(remaining),
                    fill_to_stop_public_ms=activated-first if activated is not None else None,
                    fill_to_stop_observation_ms=now_ms-first)


class Controller:
    def __init__(self, normal, venue=None):
        self.normal, self.store = normal, normal.store
        self.venue = venue or Venue(normal.venue.env)
        self.venue.store = self.store
        fill_wakeups=vars(normal.venue).get('fill_wakeups')
        if fill_wakeups is not None:
            self.venue.fill_wakeups=fill_wakeups
        if self.venue.domain != self.store.domain:
            raise DispatchError('SOFTWARE_AND_ACCOUNT_STORAGE_MUST_NOT_MIX')

    def _save(self, state, event, update):
        return self.store.change(state['bucket'], state['revision'], event,
                                 self.venue.now(), update)

    def release_closed_incident(self, bucket, *, account, incident_id):
        """Explicit audited Testnet release; never called by startup or trading.

        Keep continuous entries disabled, preserve the complete incident, and
        revalidate two agreeing public account inventories before exact CAS.
        Nothing signs, submits or enables an order here.
        """
        life.ident(bucket,r'[0-9a-f]{64}');life.address(account)
        life.ident(incident_id,r'[0-9a-f]{64}')
        env=getattr(self.normal.venue,'env',{})
        expected=dict(HL_TESTNET_EMERGENCY_CLOSE=APPROVAL,
            RENDER_SERVICE_ID=dispatch.roles.SERVICE,HL_TESTNET_RUNTIME_MODE='long_stream_testnet_v1',
            HL_TESTNET_TWO_ACCOUNT_EXECUTION='disabled',HL_TESTNET_FILLED_DISPATCH='approved_long_stream_v1',
            HL_TESTNET_LONG_STREAM='approved_alerts_v1',HL_TESTNET_FILLED_AFTER_EXIT_POLICY=dispatch.AFTER_EXIT,
            HL_TESTNET_LONG_ENTRY_ENABLED='false',HL_TESTNET_SHORT_ENTRY_ENABLED='false')
        if (any(env.get(key)!=value for key,value in expected.items())
                or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_CARD_SYNC')):
            raise DispatchError('EXPLICIT_TESTNET_RELEASE_REQUIRES_ENTRIES_DISABLED')
        started=self.venue.now();state=self.store.load(bucket)
        if state['account']!=account or incident_identity(state)!=incident_id:
            raise DispatchError('EMERGENCY_RELEASE_INCIDENT_OR_ACCOUNT_CHANGED')
        incident=deepcopy(state['emergency'])
        if incident.get('phase')!='CLOSED_VERIFIED':
            raise DispatchError('EMERGENCY_RELEASE_FINAL_CLOSURE_REQUIRED')
        state=self._refresh_normal(state)
        if incident_identity(state)!=incident_id or not closed_release_proof(
                state,now_ms=self.venue.now(),not_before_ms=started):
            raise DispatchError('EMERGENCY_RELEASE_FRESH_FINALITY_REQUIRED')
        binding=next(b for b in state['bindings'] if b['card_id']==incident['card_id'])
        role=binding['role']
        if self.normal.routes.get(role,{}).get('account')!=account:
            raise DispatchError('EMERGENCY_RELEASE_INCIDENT_OR_ACCOUNT_CHANGED')
        states=self.store.for_account(account)
        fingerprints=sorted((item['bucket'],item['revision'],life.digest(item)) for item in states)
        from .card_sync_evidence import PublicReader
        budget=getattr(self.venue,'request_budget',None)
        reader=PublicReader(budget=budget() if callable(budget) else None,priority='protection')
        samples=[]
        for _ in range(2):
            at=self.venue.now()
            orders,positions=dispatch.joined_public_reads(
                lambda:reader.read('frontendOpenOrders',account),
                lambda:reader.read('clearinghouseState',account))
            samples.append(dict(at_ms=at,inventory=_release_inventory(
                account,state['symbol'],role,states,orders,positions)))
        if samples[0]['inventory']!=samples[1]['inventory']:
            raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_INVENTORY_CHANGED')
        def update(conn,current):
            now=self.venue.now()
            if (current['account']!=account or current.get('emergency')!=incident
                    or incident_identity(current)!=incident_id
                    or not closed_release_proof(current,now_ms=now,not_before_ms=started)
                    or any(not started<=sample['at_ms']<=now or now-sample['at_ms']>5000 for sample in samples)):
                raise DispatchError('EMERGENCY_RELEASE_FRESH_FINALITY_REQUIRED')
            rows=conn.execute(f'''SELECT bucket,revision,value,digest FROM {SCHEMA}.buckets
                WHERE value->>'account'=%s ORDER BY bucket''',(account,)).fetchall()
            observed=[]
            for other_bucket,revision,value,digest in rows:
                if life.digest(value)!=digest:
                    raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_STATE_CHANGED')
                observed.append((other_bucket,revision,digest))
            if observed!=fingerprints:
                raise DispatchError('EMERGENCY_RELEASE_ACCOUNT_STATE_CHANGED')
            if conn.execute(f'''SELECT 1 FROM {SCHEMA}.requests r JOIN {SCHEMA}.buckets b
                    ON b.bucket=r.bucket WHERE b.value->>'account'=%s
                    AND r.phase NOT IN ('OBSERVED','ABORTED_UNSENT') LIMIT 1''',(account,)).fetchone():
                raise DispatchError('EMERGENCY_RELEASE_UNRESOLVED_ACCOUNT_REQUEST')
            history=current.setdefault('emergency_history',[])
            if not isinstance(history,list) or len(history)>=MAX_REQUESTS:
                raise DispatchError('EMERGENCY_RELEASE_ARCHIVE_REQUIRES_REVIEW')
            history.append(dict(incident=deepcopy(incident),incident_id=incident_id,
                account=account,symbol=current['symbol'],released_at_ms=now,
                final_evidence_digest=life.digest(current['evidence']),
                account_inventory_digest=life.digest(samples[0]['inventory']),
                account_inventory_observed_at_ms=[sample['at_ms'] for sample in samples]))
            del current['emergency']
            return None
        released=self._save(state,'EMERGENCY_FINAL_INCIDENT_EXPLICITLY_ARCHIVED_ENTRIES_STAY_DISABLED',update)
        return dict(status='CLOSED_INCIDENT_RELEASED',bucket=bucket,account=account,
            card_id=incident['card_id'],incident_id=incident_id,revision=released['revision'],
            continuous_entry_flags_changed=False,order_requests_sent=0)

    def latch(self, state, cause, *, provisional=False):
        def update(conn, current):
            if current.get('emergency') is not None:
                raise DispatchError('EMERGENCY_ALREADY_LATCHED_RELOAD')
            current['emergency'] = dict(version=VERSION, phase='ACTIVE',
                latched_at_ms=self.venue.now(), requests=[], pending_close=None,
                pending_cancel=None, **deepcopy(cause))
            if provisional:
                current['emergency']['provisional']=True
            return None
        return self._save(state, 'EMERGENCY_LATCHED_NEW_ENTRIES_BLOCKED', update)

    def _withdraw_provisional(self, state):
        expected=deepcopy(state['emergency'])
        def update(conn,current):
            if (current.get('emergency')!=expected
                    or not provisional_stop_proof(current,now_ms=self.venue.now())):
                raise DispatchError('PROVISIONAL_STOP_PROOF_CHANGED_RECONCILE_REQUIRED')
            del current['emergency']
            return None
        return self._save(state,'UNSENT_EMERGENCY_WITHDRAWN_AFTER_FRESH_STOP_PROOF',update)

    def _confirm_provisional(self, state):
        expected=deepcopy(state['emergency'])
        def update(conn,current):
            if (current.get('emergency')!=expected
                    or expected.get('provisional') is not True
                    or expected['requests'] or expected['pending_close'] is not None
                    or expected['pending_cancel'] is not None):
                raise DispatchError('PROVISIONAL_EMERGENCY_CHANGED_RECONCILE_REQUIRED')
            now=self.venue.now();snap=current['evidence']['snapshot']
            if not 0<=now-snap['at_ms']<=5000:
                raise DispatchError('EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED')
            if (current['evidence']['bindings']!=current['bindings']
                    or snap['history_complete'] is not True or snap['orders_complete'] is not True
                    or snap['account']!=current['account'] or snap['symbol']!=current['symbol']
                    or view(current,now)['bucket_issues']
                    or (trigger(current,now_ms=now) is None
                        and not provisional_cleanup_proof(current,now_ms=now))):
                raise DispatchError('EMERGENCY_POSITION_OR_HISTORY_NOT_VERIFIED')
            current['emergency']['provisional']=False
            return None
        return self._save(state,'EMERGENCY_CONFIRMED_AFTER_FRESH_UNPROTECTED_PROOF',update)

    def _refresh_normal(self, state):
        """Resolve an accepted delayed stop; unknown replies do not bar the close lane."""
        try:
            wait=reconciliation_wait_ms(state,now_ms=self.venue.now())
            return self.normal.refresh(state['bucket'],emergency=True,emergency_wait_ms=wait)
        except (DispatchError, life.LifecycleError) as exc:
            if str(exc) not in {'OUTCOME_UNRESOLVED_NO_NEW_REQUEST', 'CONFLICT_REQUIRES_REVIEW',
                    'REJECTION_HISTORY_WINDOW_REQUIRES_REVIEW', 'REJECTION_FILL_FOUND_NO_RELEASE'}:
                raise
            state = self.store.load(state['bucket'])
            if not state['bindings'] or state['evidence'] is None:
                raise
            evidence = self.venue.collect(state['evidence'])
            def update(conn, current):
                _continues(current['evidence'], evidence)
                current['evidence'] = dict(bindings=current['bindings'], snapshot=evidence['snapshot'])
                return None
            return self._save(state, 'EMERGENCY_PUBLIC_CHECKPOINT', update)

    def _retire_normal(self,state):
        if not state['pending']:
            return state
        request=self.store.request(state['pending'])
        if request['phase']=='PREPARED' and request['attempts']==0:
            def unsent(conn,current):
                r=self.store.pending_record(conn,current)
                if r!=request:
                    raise DispatchError('EMERGENCY_NORMAL_REQUEST_CHANGED')
                r.update(phase='ABORTED_UNSENT',abort_reason='EMERGENCY_SUPERSEDED_UNSENT')
                current['pending']=None
                return r
            return self._save(state,'EMERGENCY_NORMAL_UNSENT_INTENT_RETIRED',unsent)
        if (request['phase'] not in ('OUTCOME_UNKNOWN','ACK_UNVERIFIED','REJECTED')
                or request['proposal']['leg'] not in ('STOP','TAKE_PROFIT')
                or request['proposal']['action']['type'] not in ('order','batchModify')
                or request['attempt_at_ms'] is None
                or self.venue.now()-request['attempt_at_ms']<120000):
            return state
        cloid=dispatch.requested_order(request['proposal']['action'])['c']
        if any(self.venue.lookup(state['account'],cloid)!={'status':'unknownOid'} for _ in range(2)):
            return state
        evidence=self.venue.collect(state['evidence'])
        safe_card({**state,'evidence':evidence},state['emergency']['card_id'],self.venue.now())
        def expired(conn,current):
            r=self.store.pending_record(conn,current)
            if r!=request:
                raise DispatchError('EMERGENCY_NORMAL_REQUEST_CHANGED')
            if current['evidence']['snapshot']!=evidence['snapshot']:
                _continues(current['evidence'],evidence)
                current['evidence']=dict(bindings=current['bindings'],snapshot=evidence['snapshot'])
            r.update(phase='OBSERVED',terminal_state='NO_PUBLIC_ORDER_AFTER_SIGNATURE_EXPIRY',
                     observed_at_ms=evidence['snapshot']['at_ms'])
            current['pending']=None
            return r
        return self._save(state,'EMERGENCY_UNKNOWN_NORMAL_EXIT_RECONCILED_AFTER_EXPIRY',expired)

    def _requests(self, state):
        return state['emergency']['requests']

    def _resolve_close(self, state):
        eid = state['emergency']['pending_close']
        if eid is None:
            return state
        request = next(r for r in self._requests(state) if r['request_id'] == eid)
        raw = self.venue.lookup(state['account'], request['proposal']['action']['orders'][0]['c'])
        if raw == {'status':'unknownOid'}:
            # A response timeout never authorizes another IOC. Even a rejection
            # waits beyond signature expiry for two independent no-order reads.
            if self.venue.now()-request['attempt_at_ms'] < 120000:
                raise DispatchError('EMERGENCY_CLOSE_OUTCOME_UNKNOWN_NO_RESEND')
            if self.venue.lookup(state['account'], request['proposal']['action']['orders'][0]['c']) != raw:
                raise DispatchError('EMERGENCY_ORDER_LOOKUP_CHANGED')
            evidence = self.venue.collect(state['evidence'])
            safe_card({**state, 'evidence': evidence}, state['emergency']['card_id'], self.venue.now())
            def expired(conn, current):
                r = next(r for r in self._requests(current) if r['request_id'] == eid)
                r.update(phase='EXPIRED_NO_PUBLIC_ORDER', observed_at_ms=evidence['snapshot']['at_ms'])
                current['emergency']['pending_close'] = None
                current['evidence'] = dict(bindings=current['bindings'], snapshot=evidence['snapshot'])
                return None
            return self._save(state, 'EMERGENCY_EXPIRED_INTENT_RECONCILED_NO_REPLAY', expired)
        oid = dispatch.identity(raw, request, self.venue.now())
        bindings = deepcopy(state['bindings'])
        binding = next(b for b in bindings if b['card_id'] == request['proposal']['card_id'])
        if oid not in binding['orders']['STOP']:
            binding['orders']['STOP'].append(oid)
        evidence = self.venue.collect(dict(bindings=bindings, snapshot=state['evidence']['snapshot']))
        terminal = next((o for o in evidence['snapshot']['terminal_orders'] if o['oid']==oid), None)
        if terminal is None or evidence['snapshot']['at_ms'] <= request['attempt_at_ms']:
            raise DispatchError('EMERGENCY_TERMINAL_AND_FILLS_NOT_VERIFIED')
        def resolved(conn, current):
            _continues(current['evidence'], evidence)
            r = next(r for r in self._requests(current) if r['request_id'] == eid)
            r.update(phase='OBSERVED', observed_oid=oid,
                     observed_at_ms=evidence['snapshot']['at_ms'],
                     terminal_state=terminal['state'], filled_quantity=terminal['filled_quantity'])
            current['bindings'] = bindings
            current['evidence'] = dict(bindings=bindings, snapshot=evidence['snapshot'])
            current['emergency']['pending_close'] = None
            return None
        return self._save(state, 'EMERGENCY_IOC_PUBLIC_FILL_RECONCILED', resolved)

    def _resolve_cancel(self, state):
        eid = state['emergency']['pending_cancel']
        if eid is None:
            return state
        request = next(r for r in self._requests(state) if r['request_id']==eid)
        terminal = next((o for o in state['evidence']['snapshot']['terminal_orders']
                         if o['oid']==request['proposal']['old_oid']), None)
        if terminal is None:
            # Only a fresh two-pass checkpoint of the same still-active owned
            # order after signature expiry may release this cancellation lane.
            if (self.venue.now()-request['attempt_at_ms']<120000
                    or state['evidence']['snapshot']['at_ms']<=request['attempt_at_ms']+15000
                    or not any(o['oid']==request['proposal']['old_oid']
                               for o in state['evidence']['snapshot']['open_orders'])):
                return state
            def expired(conn,current):
                r=next(r for r in self._requests(current) if r['request_id']==eid)
                r.update(phase='EXPIRED_NO_PUBLIC_TERMINAL',observed_at_ms=current['evidence']['snapshot']['at_ms'])
                current['emergency']['pending_cancel']=None
                return None
            return self._save(state,'EMERGENCY_CANCEL_EXPIRED_STILL_ACTIVE_RECONCILED',expired)
        if terminal['at_ms'] < request['proposal']['observed_at_ms']:
            raise DispatchError('EMERGENCY_CANCEL_TERMINAL_TIME_INVALID')
        def resolved(conn, current):
            r = next(r for r in self._requests(current) if r['request_id']==eid)
            r.update(phase='OBSERVED', observed_at_ms=current['evidence']['snapshot']['at_ms'],
                     terminal_state=terminal['state'], cancellation_caused_terminal_state=False)
            current['emergency']['pending_cancel'] = None
            return None
        return self._save(state, 'EMERGENCY_OWNED_ORDER_TERMINAL_RECONCILED', resolved)

    def proposal(self, state, *, metadata=None, sample=None):
        emergency = state['emergency']; cid = emergency['card_id']; now = self.venue.now()
        binding, row = safe_card(state, cid, now)
        snapshot = state['evidence']['snapshot']
        quantity = life.number(row['remaining_quantity'], signed=True)
        requests = self._requests(state)
        attempted_cancels = {r['proposal']['old_oid'] for r in requests
                             if r['proposal']['operation']=='EMERGENCY_CANCEL' and r['phase'] not in DONE}
        entry_ids = set(binding['orders']['ENTRY'])
        owned = {oid for leg in life.LEGS for oid in binding['orders'][leg]}
        candidates = [o for o in snapshot['open_orders'] if o['oid'] in owned
                      and (o['oid'] in entry_ids or quantity==0)
                      and o['oid'] not in attempted_cancels]
        candidates.sort(key=lambda o: (o['oid'] not in entry_ids, int(o['oid'])))
        if (not (candidates and emergency['pending_cancel'] is None)
                and not (quantity>0 and emergency['pending_close'] is None)):
            return None
        # Only actual close/cancel planning needs asset metadata. A slow read
        # keeps the original quantity and price clocks; authorize() will reject
        # expired evidence before an attempted order can become durable.
        index, decimals = dispatch.asset(
            self.venue.metadata() if metadata is None else metadata, state['symbol'])
        if candidates and emergency['pending_cancel'] is None:
            order = candidates[0]; operation='EMERGENCY_CANCEL'
            old_oid=order['oid']; q='0'
            leg = next(leg for leg in life.LEGS if old_oid in binding['orders'][leg])
            action=dict(type='cancel', cancels=[dict(a=index, o=int(old_oid))])
            sample=None
        elif quantity>0 and emergency['pending_close'] is None:
            operation='EMERGENCY_CLOSE'; old_oid=None; leg='STOP'; q=life.text(quantity)
            if sample is None:
                sample = self.venue.sample(state['account'], state['symbol'])
            if not 0<=self.venue.now()-sample['at_ms']<=5000:
                raise DispatchError('EMERGENCY_PRICE_SAMPLE_EXPIRED')
            price=close_price(sample['mark_price'], decimals, buy=binding['side']=='SHORT')
            dispatch.precise(price, q, decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],cid,len(requests)+1])[:32]
            action=dict(type='order', grouping='na', orders=[dict(a=index,
                b=binding['side']=='SHORT', p=price, s=q, r=True,
                t=dict(limit=dict(tif='Ioc')), c=cloid)])
        else:
            return None
        return dict(version=VERSION, card_id=cid, role=binding['role'], account=state['account'],
                    symbol=state['symbol'], bucket=state['bucket'], operation=operation, leg=leg,
                    action=action, quantity=q, old_oid=old_oid,
                    observed_at_ms=snapshot['at_ms'], basis=life.digest(state['evidence']),
                    asset_index=index, size_decimals=decimals, sample=sample)

    def _begin(self, state, proposal):
        if state['emergency'].get('provisional') is True:
            raise DispatchError('PROVISIONAL_EMERGENCY_HAS_NO_SEND_AUTHORITY')
        self.venue.authorize(state, proposal)
        admission=dispatch.reserve_transport(self.venue,proposal)
        self.venue.authorize(state, proposal)
        now = self.venue.now()
        def begin(conn, current):
            if current['emergency'].get('provisional') is True:
                raise DispatchError('PROVISIONAL_EMERGENCY_HAS_NO_SEND_AUTHORITY')
            if len(self._requests(current)) >= MAX_REQUESTS:
                raise DispatchError('EMERGENCY_ACTION_BUDGET_REQUIRES_REVIEW')
            if current['emergency']['phase'] != 'ACTIVE' or proposal['basis']!=life.digest(current['evidence']):
                raise DispatchError('EMERGENCY_PLAN_CHANGED_NO_SEND')
            safe_card(current, proposal['card_id'], now)
            if self.proposal_without_io(current, proposal) is not True:
                raise DispatchError('EMERGENCY_WIRE_NOT_VERIFIED')
            key = 'pending_close' if proposal['operation']=='EMERGENCY_CLOSE' else 'pending_cancel'
            if current['emergency'][key] is not None:
                raise DispatchError('EMERGENCY_PENDING_REQUEST_NO_REPEAT')
            route=self.normal.routes[proposal['role']]
            agent=life.address(route['agent'])
            nonce=conn.execute(f'''INSERT INTO {SCHEMA}.nonces VALUES(%s,%s)
                ON CONFLICT(agent) DO UPDATE SET nonce=GREATEST({SCHEMA}.nonces.nonce+1,EXCLUDED.nonce)
                RETURNING nonce''',(agent,now)).fetchone()[0]
            if nonce>now+1000:
                raise DispatchError('NONCE_CLOCK_REQUIRES_REVIEW')
            eid=life.digest([VERSION,current['bucket'],len(self._requests(current))+1,proposal])
            request=dict(request_id=eid, domain=self.store.domain, bucket=current['bucket'],
                         phase='OUTCOME_UNKNOWN', proposal=deepcopy(proposal), attempts=1,
                         nonce=nonce, prepared_at_ms=now, attempt_at_ms=now,
                         reply=None, observed_oid=None)
            current['emergency']['requests'].append(request)
            current['emergency'][key]=eid
            return None
        # Capture the exact acknowledged commit, not a later load which another
        # worker may already have advanced. An uncertain COMMIT returns nothing
        # and therefore never grants this process a sender token.
        state,_=self.store.change(state['bucket'],state['revision'],
            'EMERGENCY_ATTEMPT_BEGUN',now,begin,_return_committed_request=True)
        request=deepcopy(self._requests(state)[-1])
        if type(admission) is dispatch.TransportAdmission:
            admission.bind(request)
        return dispatch.AdmittedAttempt((state,request),admission)

    def _abort_unsent(self,state,request,certificate):
        if type(certificate) is not DefinitelyUnsent or not certificate.matches(request):
            raise DispatchError('EXACT_UNSENT_CERTIFICATE_REQUIRED')
        now=self.venue.now()
        def abort(conn,current):
            key='pending_close' if request['proposal']['operation']=='EMERGENCY_CLOSE' else 'pending_cancel'
            r=next((r for r in self._requests(current)
                    if r['request_id']==request['request_id']),None)
            if (current['emergency'][key]!=request['request_id'] or r is None
                    or not certificate.matches(r) or now<r['attempt_at_ms']):
                raise DispatchError('UNSENT_ATTEMPT_CHANGED_NO_RELEASE')
            r.update(phase='ABORTED_UNSENT',abort_reason=certificate.reason,aborted_at_ms=now)
            current['emergency'][key]=None
            return None
        return self.store.change(state['bucket'],state['revision'],
            'EMERGENCY_EXACT_PRE_HTTP_ATTEMPT_ABORTED_UNSENT',now,abort)

    @staticmethod
    def proposal_without_io(state, proposal):
        binding,row=safe_card(state,proposal['card_id'],proposal['observed_at_ms'])
        if (proposal['version']!=VERSION or proposal['account']!=state['account']
                or proposal['symbol']!=state['symbol'] or proposal['role']!=binding['role']
                or proposal['bucket']!=state['bucket'] or proposal['basis']!=life.digest(state['evidence'])
                or proposal['observed_at_ms']!=state['evidence']['snapshot']['at_ms']):
            return False
        index,decimals=proposal['asset_index'],proposal['size_decimals']
        draft=state['originals'][proposal['card_id']]['draft']
        if index!=draft['entry_action']['orders'][0]['a'] or decimals!=draft['size_decimals']:
            return False
        if type(index) is not int or not 0<=index<10000 or type(decimals) is not int or not 0<=decimals<=6:
            return False
        if proposal['operation']=='EMERGENCY_CLOSE':
            sample=proposal['sample']
            if not isinstance(sample,dict) or set(sample)!={'mark_price','at_ms'}:
                return False
            q=life.text(life.number(row['remaining_quantity'],positive=True))
            price=close_price(sample['mark_price'],decimals,buy=binding['side']=='SHORT')
            dispatch.precise(price,q,decimals)
            cloid='0x'+life.digest([VERSION,state['bucket'],proposal['card_id'],len(state['emergency']['requests'])+1])[:32]
            # Before send the persisted request already occupies the last slot.
            matching=[r for r in state['emergency']['requests'] if r['proposal']==proposal]
            if matching:
                cloid=matching[0]['proposal']['action']['orders'][0]['c']
            expected=dict(type='order',grouping='na',orders=[dict(a=index,b=binding['side']=='SHORT',
                p=price,s=q,r=True,t=dict(limit=dict(tif='Ioc')),c=cloid)])
            return (proposal['action']==expected and proposal['quantity']==q
                    and proposal['leg']=='STOP' and proposal['old_oid'] is None)
        if proposal['operation']=='EMERGENCY_CANCEL':
            oid=proposal['old_oid']
            expected=dict(type='cancel',cancels=[dict(a=index,o=int(oid))])
            return (proposal['action']==expected and oid in binding['orders'][proposal['leg']]
                and any(o['oid']==oid for o in state['evidence']['snapshot']['open_orders'])
                and proposal['quantity']=='0' and proposal['sample'] is None)
        return False

    @market_lane
    def _initial_state(self, bucket, *, send):
        state=self.store.load(bucket)
        # Freeze new entries from known durable uncovered-fill proof before any
        # potentially slow I/O. This latch never authorizes a send: final public
        # quantity, ownership and fresh price still have to be verified below.
        if send and state.get('emergency') is None:
            cause=trigger(state,now_ms=self.venue.now())
            if cause:
                state=self.latch(state,cause,provisional=True)
        return state

    def cycle(self, bucket, *, send=False):
        # Commit the entry freeze under a brief lane, then release it for public
        # observation. A normal reader must be able to commit while this worker
        # joins its flight; holding the action lane would force every join to
        # time out and repeat the same full read.
        state=self._initial_state(bucket,send=send)
        if recent_flat_unassigned_checkpoint(state, now_ms=self.venue.now()):
            # A flat unassigned close remains blocked for operator adoption.
            # No signing, entry release or closure claim is made from this hint.
            return dict(status='MANUAL_EXIT_RECONCILIATION_REQUIRED', order_requests_sent=0)
        # A known incident needs metadata before its final quantity checkpoint.
        # A slow metadata read must not repeatedly expire otherwise usable
        # quantity proof. Quiet protected buckets have no incident and spend
        # no metadata request. New incidents discovered below may fetch it late;
        # their original clocks still expire safely, and the next pass prefetches.
        incident=state.get('emergency')
        # An unresolved close cannot authorize another plan during its original
        # no-resend window. Resolve it before spending metadata on a new plan.
        # A pending cancellation may still need a protective close, so it does
        # not suppress the known-incident prefetch.
        metadata=(self.venue.metadata() if incident is not None
                  and incident['pending_close'] is None else None)
        before_revision=state['revision']
        if state.get('emergency') is not None:
            state=self._resolve_close(state)
        try:
            if state['revision']==before_revision:
                state=self._refresh_normal(state)
        except (DispatchError, life.LifecycleError):
            # Freeze new entries using already stored fill evidence. Never send
            # a close from that old evidence; a fresh verified checkpoint is required.
            self._initial_state(bucket,send=send)
            raise
        return self._action_cycle(bucket,state,metadata=metadata,send=send)

    @market_lane
    def _action_cycle(self, bucket, state, *, metadata, send):
        # Public observation never lends authority to a changed durable bucket.
        # Normal/emergency actions and checkpoint commits share this lane; the
        # exact recheck preserves the collected quantity/identity/nonce basis.
        if self.store.load(bucket)!=state:
            raise DispatchError('CONCURRENT_DISPATCH_RELOAD_REQUIRED')
        sample=None;sample_basis=None
        if (state.get('emergency') or {}).get('provisional') is True:
            if provisional_stop_proof(state,now_ms=self.venue.now()):
                if send:
                    self._withdraw_provisional(state)
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE',order_requests_sent=0)
            if (trigger(state,now_ms=self.venue.now()) is None
                    and not provisional_cleanup_proof(state,now_ms=self.venue.now())):
                # A still-uncertain or incomplete proof cannot release ENTRY,
                # and full observed STOP coverage does not authorize a close.
                return dict(status='PROVISIONAL_STOP_RECONCILIATION_REQUIRED',order_requests_sent=0)
            if send:
                state=self._confirm_provisional(state)
        if state.get('emergency') is None:
            if fresh_stop_coverage(state,now_ms=self.venue.now()):
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE',order_requests_sent=0)
            try:
                sample=self.venue.sample(state['account'],state['symbol']) if state['bindings'] else None
            except Exception:
                cause=trigger(state,now_ms=self.venue.now())
                if cause and send:self.latch(state,cause)
                raise
            sample_basis=life.digest(state['evidence'])
            fresh_sample=(sample is not None and 0<=self.venue.now()-sample['at_ms']<=5000)
            cause=trigger(state,now_ms=self.venue.now(), mark=sample['mark_price'] if fresh_sample else None)
            if cause is None:
                return dict(status='STOP_OBSERVED_OR_NO_EXPOSURE',order_requests_sent=0)
            if not send:
                return dict(status='EMERGENCY_PREVIEW',cause=cause,order_requests_sent=0)
            state=self.latch(state,cause)
        state=self._retire_normal(state)
        state=self._resolve_cancel(state)
        if sample_basis!=life.digest(state['evidence']):
            sample=None
        proposal=self.proposal(state,metadata=metadata,sample=sample)
        if proposal is None:
            report=view(state,self.venue.now())
            row=next(r for r in report['cards'] if r['card_id']==state['emergency']['card_id'])
            if row['closure_verified'] and not state['emergency']['pending_close'] and not state['emergency']['pending_cancel'] and not state['pending']:
                if state['emergency']['phase']!='CLOSED_VERIFIED':
                    def closed(conn,current):
                        current['emergency'].update(phase='CLOSED_VERIFIED', closed_at_ms=current['evidence']['snapshot']['at_ms'])
                        return None
                    state=self._save(state,'EMERGENCY_CLOSURE_VERIFIED_CIRCUIT_REMAINS_LATCHED',closed)
            return dict(status=('FLAT_AWAITING_ORDER_FINALITY' if row['remaining_quantity']=='0' and state['emergency']['phase']!='CLOSED_VERIFIED' else state['emergency']['phase']),
                        card_id=state['emergency']['card_id'],remaining_quantity=row['remaining_quantity'],order_requests_sent=0)
        if not send:
            return dict(status='EMERGENCY_PREVIEW',proposal=proposal,order_requests_sent=0)
        prepared=self._begin(state,proposal)
        state,request=prepared
        before=self.venue.sent
        try:
            raw=dispatch.send_admitted(self.venue,request,getattr(prepared,'admission',None))
            route=self.normal.routes[proposal['role']]
            reply=dispatch.normalized_reply(raw,proposal['action']['type'],account=state['account'],agent=route['agent'])
        except DefinitelyUnsent as certificate:
            try:
                self._abort_unsent(state,request,certificate)
            except Exception:
                return dict(status='OUTCOME_UNKNOWN',operation=proposal['operation'],
                            card_id=proposal['card_id'],order_requests_sent=0)
            return dict(status='ABORTED_UNSENT',operation=proposal['operation'],
                        card_id=proposal['card_id'],reason=certificate.reason,order_requests_sent=0)
        except Exception:
            reply=dict(state='OUTCOME_UNKNOWN',code=None,oid=None)
        def replied(conn,current):
            r=next(r for r in self._requests(current) if r['request_id']==request['request_id'])
            r['reply']=reply
            return None
        self._save(state,'EMERGENCY_REPLY_NOT_FILL_PROOF',replied)
        return dict(status=reply['state'],operation=proposal['operation'],card_id=proposal['card_id'],
                    order_requests_sent=self.venue.sent-before)


class Venue(dispatch.TestnetVenue):
    def authorize(self,state,proposal):
        env=self.env
        if (env.get('HL_TESTNET_EMERGENCY_CLOSE')!=APPROVAL
                or env.get('RENDER_SERVICE_ID')!=dispatch.roles.SERVICE
                or env.get('HL_TESTNET_RUNTIME_MODE')!='long_stream_testnet_v1'
                or env.get('HL_TESTNET_TWO_ACCOUNT_EXECUTION')!='disabled'
                or env.get('HL_TESTNET_FILLED_DISPATCH')!='approved_long_stream_v1'
                or env.get('HL_TESTNET_LONG_STREAM')!='approved_alerts_v1'
                or env.get('HL_TESTNET_FILLED_AFTER_EXIT_POLICY')!=dispatch.AFTER_EXIT
                or env.get('HL_TESTNET_SAFETY_PIPELINE') or env.get('HL_TESTNET_CARD_SYNC')):
            raise DispatchError('EMERGENCY_TESTNET_NOT_AUTHORIZED')
        binding,_=safe_card(state,proposal['card_id'],self.now())
        route=dispatch.roles.route_for(env,binding['role'],state['account'])
        dispatch.roles.wallet_for_role(env,binding['role'],route['account'],route['agent'])
        if not Controller.proposal_without_io(state,proposal):
            raise DispatchError('EMERGENCY_WIRE_NOT_VERIFIED')
        if (proposal['operation']=='EMERGENCY_CLOSE'
                and not 0<=self.now()-proposal['sample']['at_ms']<=5000):
            raise DispatchError('EMERGENCY_PRICE_SAMPLE_EXPIRED')
        if not 0<=self.now()-proposal['observed_at_ms']<=5000:
            raise DispatchError('EMERGENCY_FINAL_QUANTITY_EVIDENCE_EXPIRED')
        return route

    def _gate(self,proposal,after_exit_policy):
        state=self.store.load(proposal['bucket'])
        emergency=state.get('emergency')
        if not emergency or emergency['version']!=VERSION or emergency['phase']!='ACTIVE':
            raise DispatchError('DURABLE_EMERGENCY_INCIDENT_REQUIRED')
        matches=[r for r in emergency['requests'] if r['proposal']==proposal and r['phase']=='OUTCOME_UNKNOWN' and r['attempts']==1]
        if len(matches)!=1 or proposal['basis']!=life.digest(state['evidence']):
            raise DispatchError('DURABLE_EMERGENCY_INTENT_CHANGED')
        return self.authorize(state,proposal)


def healthy(now_ms):
    with _lock:
        return (_health['running'] and _health['last_status']=='PASS_COMPLETE' and _health['last_pass_at_ms'] is not None
                and 0<=now_ms-_health['last_pass_at_ms']<=15000)


def health():
    with _lock:
        return deepcopy(_health)


def start(normal, streams, stop_event, *, only_bucket=None):
    global _thread,_stop_event
    env=normal.venue.env
    continuous = continuous_configuration(env)
    if env.get('HL_TESTNET_EMERGENCY_CLOSE','')=='':
        return False
    if env.get('HL_TESTNET_EMERGENCY_CLOSE')!=APPROVAL:
        raise DispatchError('EMERGENCY_APPROVAL_MODE_INVALID')
    if continuous:
        if only_bucket is not None:
            raise DispatchError('CONTINUOUS_EMERGENCY_CANNOT_LIMIT_SUPERVISION_TO_ONE_BUCKET')
        selected = {role:route['account'] for role,route,*_ in streams}
        expected = {role:dispatch.roles.route_for(env,role)['account'] for role in dispatch.roles.ROLES}
        if len(streams)!=2 or selected!=expected or len(set(selected.values()))!=2:
            raise DispatchError('CONTINUOUS_EMERGENCY_BOTH_ACCOUNTS_REQUIRED')
        if vars(normal.venue).get('fill_wakeups') is None:
            raise DispatchError('CONTINUOUS_EMERGENCY_FILL_NOTIFICATIONS_REQUIRED')
        continuous_storage(normal.venue, verify_schema=True)
    elif env.get('HL_TESTNET_LONG_ENTRY_ENABLED')!='false' or env.get('HL_TESTNET_SHORT_ENTRY_ENABLED')!='false':
        raise DispatchError('EMERGENCY_RELEASE_REQUIRES_CONTINUOUS_ENTRIES_DISABLED')
    if only_bucket is not None:
        life.ident(only_bucket, r'[0-9a-f]{64}')
        scoped = normal.store.load(only_bucket)
        if scoped['account'] not in {route['account'] for _,route,*_ in streams}:
            raise DispatchError('EMERGENCY_SCOPE_ACCOUNT_NOT_SELECTED')
    controller=Controller(normal)
    def loop():
        with _lock:
            _health.update(running=True,last_status='STARTING',last_pass_at_ms=None)
        while not stop_event.is_set():
            status='PASS_COMPLETE'
            for _,route,*_ in streams:
                try:
                    for state in controller.store.for_account(route['account']):
                        if only_bucket is not None and state['bucket'] != only_bucket:
                            continue
                        from .long_stream_runtime import _unfinished,idle_flat
                        from datetime import datetime,timezone
                        if not _unfinished(state,datetime.fromtimestamp(controller.venue.now()/1000,timezone.utc)):
                            continue
                        if recent_normal_checkpoint(state,now_ms=controller.venue.now(),
                                fill_wakeups=vars(controller.venue).get('fill_wakeups')):
                            continue
                        if not state.get('emergency') and idle_flat(state):
                            continue
                        if state.get('emergency',{}).get('phase')=='CLOSED_VERIFIED':
                            continue
                        # Reconcile entry/stop independently of the ordinary
                        # sweep. A normal lane stall cannot stop this worker.
                        before=controller.venue.sent
                        try:
                            result=controller.cycle(state['bucket'],send=True)
                            if result.get('operation')=='EMERGENCY_CANCEL':
                                controller.cycle(state['bucket'],send=True)
                            if result['status']!='STOP_OBSERVED_OR_NO_EXPOSURE':
                                print(json.dumps({'testnet_emergency_close':result},sort_keys=True),flush=True)
                        except Exception as exc:
                            from .long_stream_runtime import _safe_failure
                            status='RECONCILIATION_REQUIRED_NO_BLIND_RETRY'
                            print(json.dumps({'testnet_emergency_close':dict(status=status,
                                order_requests_sent=controller.venue.sent-before,**_safe_failure(exc))},sort_keys=True),flush=True)
                except Exception:
                    status='STORAGE_OR_SCAN_UNAVAILABLE'
            with _lock:
                _health.update(last_pass_at_ms=controller.venue.now(),last_status=status)
            stop_event.wait(1)
        with _lock:
            _health['running']=False
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop_event=stop_event
        _thread=threading.Thread(target=loop,daemon=True,name='testnet-emergency-stop-supervisor')
        _thread.start()
    return True


def stop_supervisor(stop_event, *, timeout=5):
    """Stop and join the caller's supervisor; a timeout is not verified shutdown."""
    if isinstance(timeout,bool) or not isinstance(timeout,(int,float)) or not 0<=timeout<=30:
        raise DispatchError('EMERGENCY_SHUTDOWN_TIMEOUT_INVALID')
    with _lock:
        thread=_thread
        if thread is not None and _stop_event is not stop_event:
            raise DispatchError('EMERGENCY_SUPERVISOR_STOP_EVENT_MISMATCH')
    stop_event.set()
    if thread is None:
        return True
    if thread is threading.current_thread():
        return False
    thread.join(timeout)
    return not thread.is_alive()
