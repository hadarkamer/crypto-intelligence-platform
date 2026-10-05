"""Bounded read reuse and delayed audits for the two-account Testnet stream.

Cached account configuration never grants permission to send. Per-cycle market
inputs expire after fifteen seconds; final source, feed, state and sender fences
remain mandatory. Quiet scheduling never renews evidence or authorizes an action.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
import threading
import time

AUDIT_DELAY_MS = 180000
STATIC_SECONDS = 300
CYCLE_SECONDS = 15
STATIC_TYPES = frozenset(('meta', 'userRole', 'userAbstraction'))
CYCLE_TYPES = frozenset(('frontendOpenOrders', 'clearinghouseState',
    'spotClearinghouseState', 'activeAssetData', 'userRateLimit'))
_CYCLE = ContextVar('testnet_simple_cycle_reads', default=None)


def enabled(env):
    return (env.get('RENDER_SERVICE_ID') == 'srv-dakptbh594qs7395460g'
            and env.get('HL_TESTNET_RUNTIME_MODE') == 'long_stream_testnet_v1'
            and not env.get('HL_TESTNET_PROTECTION_TIMING_CARD_ID'))


def key(body):
    import json
    return json.dumps(body, sort_keys=True, separators=(',', ':'))


class ReadCache:
    def __init__(self, kinds, seconds, *, clock=time.monotonic):
        self.kinds = kinds
        self.seconds = seconds
        self.clock = clock
        self.lock = threading.RLock()
        self.values = {}
        self.fetch_locks = {}
        self.generation = 0

    def has(self, body):
        with self.lock:
            row = self.values.get(key(body))
            return (body.get('type') in self.kinds and row is not None
                    and 0 <= self.clock()-row[0] < self.seconds)

    def started(self, body):
        with self.lock:
            row=self.values.get(key(body))
            return row[0] if self.has(body) else None

    def read(self, body, fetch):
        if body.get('type') not in self.kinds:
            return fetch()
        with self.lock:
            identity = key(body)
            fetch_lock = self.fetch_locks.setdefault(identity, threading.Lock())
        # Independent reads retain their parallelism; only the same question
        # joins a single request. Never hold the cache-wide lock during HTTP.
        with fetch_lock:
            with self.lock:
                if self.has(body):
                    return deepcopy(self.values[identity][1])
                generation = self.generation
            # Start the lifetime before HTTP, not after a delayed response.
            started = self.clock()
            value = fetch()
            with self.lock:
                if generation == self.generation:
                    self.values[identity] = (started, deepcopy(value))
            return value

    def clear(self):
        with self.lock:
            self.values.clear()
            self.generation += 1


STATIC_READS = ReadCache(STATIC_TYPES, STATIC_SECONDS)


@contextmanager
def cycle_reads(active):
    if not active:
        yield
        return
    token = _CYCLE.set(ReadCache(CYCLE_TYPES, CYCLE_SECONDS))
    try:
        yield
    finally:
        _CYCLE.reset(token)


def read(body, fetch, *, static=False, cycle=False):
    memo = _CYCLE.get() if cycle else None
    if memo is not None and body.get('type') in CYCLE_TYPES:
        return memo.read(body, fetch)
    if static and body.get('type') in STATIC_TYPES:
        return STATIC_READS.read(body, fetch)
    return fetch()


def missing_reads(bodies):
    """Fund only HTTP calls, not reused values, before starting a read plan."""
    memo = _CYCLE.get()
    result = []
    seen = set()
    for body in bodies:
        kind = body.get('type')
        if STATIC_READS.has(body) or (memo is not None and memo.has(body)):
            continue
        identity = key(body)
        reusable = kind in STATIC_TYPES or (memo is not None and kind in CYCLE_TYPES)
        if reusable and identity in seen:
            continue
        seen.add(identity)
        result.append(body)
    return result


def cycle_age_ms(body):
    memo=_CYCLE.get()
    started=memo.started(body) if memo is not None else None
    return max(0,int((memo.clock()-started)*1000)) if started is not None else 0


def protected(state):
    from . import card_lifecycle as life
    snapshot = (state.get('evidence') or {}).get('snapshot') or {}
    at = snapshot.get('at_ms')
    if (type(at) is not int or state.get('emergency') is not None or state.get('pending')
            or not state.get('bindings') or state['evidence']['bindings']!=state['bindings']
            or not snapshot.get('history_complete') or not snapshot.get('orders_complete')):
        return False
    report=life.review(state['bindings'],snapshot,now_ms=at)
    if report['bucket_issues'] or not report['cards']:
        return False
    exposed=False
    for row in report['cards']:
        remaining=life.number(row['remaining_quantity'],signed=True)
        if (row['issues'] or remaining<0
                or life.number(row['stop_quantity_observed'])!=remaining
                or life.number(row['take_profit_quantity_observed'])!=remaining):
            return False
        exposed=exposed or remaining>0
    return exposed


def source_expired_with_working_entry(state, now_ms):
    from .source_window import timestamp
    entries={oid for b in state['bindings'] for oid in b['orders']['ENTRY']}
    working={row['oid'] for row in state['evidence']['snapshot']['open_orders']}&entries
    for b in state['bindings']:
        if working.intersection(b['orders']['ENTRY']):
            expiry=state['originals'][b['card_id']]['card'].get('source_expires_at')
            if expiry is not None and now_ms>=int(timestamp(expiry).timestamp()*1000):
                return True
    return False


def audit_due(state):
    saved = state.get('simple_execution_audit')
    if isinstance(saved, dict) and type(saved.get('due_ms')) is int:
        return saved['due_ms']
    snapshot = (state.get('evidence') or {}).get('snapshot') or {}
    at = snapshot.get('at_ms')
    return at + AUDIT_DELAY_MS if type(at) is int and protected(state) else None


def quiet(state, *, now_ms, feed):
    if (getattr(feed, 'simple_execution', False) is not True
            or not protected(state) or 'history_gap_recovery' in state):
        return False
    snapshot = state['evidence']['snapshot']
    due = audit_due(state)
    return (due is not None and snapshot['at_ms'] <= now_ms < due
            and not source_expired_with_working_entry(state,now_ms)
            and feed.entry_allowed(state['account']) is True
            and feed.dirty_symbols(state['account']) == ())


def waiting_entry(state, *, now_ms, feed):
    """A clean, unfilled entry needs only the existing cancellation-price poll."""
    from . import card_lifecycle as life
    if (getattr(feed,'simple_execution',False) is not True or state.get('pending')
            or state.get('emergency') is not None or not state.get('bindings')
            or not state.get('evidence') or 'history_gap_recovery' in state
            or state['evidence']['bindings']!=state['bindings']
            or feed.entry_allowed(state['account']) is not True
            or feed.dirty_symbols(state['account'])!=()):
        return None
    snap=state['evidence']['snapshot']
    if (not snap['history_complete'] or not snap['orders_complete']
            or not snap['at_ms']<=now_ms<snap['at_ms']+AUDIT_DELAY_MS
            or life.number(snap['position_quantity'],signed=True)!=0
            or source_expired_with_working_entry(state,now_ms)):
        return None
    report=life.review(state['bindings'],snap,now_ms=snap['at_ms'])
    if report['bucket_issues'] or any(row['issues'] for row in report['cards']):
        return None
    opened={o['oid'] for o in snap['open_orders']}
    candidates=[b for b in state['bindings'] if opened.intersection(b['orders']['ENTRY'])]
    if len(candidates)!=1 or any(opened.intersection(b['orders'][leg])
            for b in state['bindings'] for leg in ('STOP','TAKE_PROFIT')):
        return None
    candidate=candidates[0]
    row=next(row for row in report['cards'] if row['card_id']==candidate['card_id'])
    return candidate if row['entry_quantity']=='0' and row['exit_quantity']=='0' else None


def checkpoint(state, collected, now_ms):
    """Keep the original audit deadline across notifications and quick checks."""
    if collected is not None and collected.get('verification_passes') == 2:
        state['simple_execution_audit'] = dict(
            verified_at_ms=state['evidence']['snapshot']['at_ms'],
            due_ms=now_ms + AUDIT_DELAY_MS)
    elif protected(state) and 'simple_execution_audit' not in state:
        state['simple_execution_audit'] = dict(verified_at_ms=None,
                                             due_ms=now_ms + AUDIT_DELAY_MS)
