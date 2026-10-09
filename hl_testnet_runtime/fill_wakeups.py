"""Bounded Testnet notifications and pending fills for durable reconciliation.

Callbacks only validate and stage fills; the runtime must commit their reduction.
Each account has its own socket because ``orderUpdates`` omits its user. Startup,
disconnect, malformed data and a notification invalidate the entry gate until a
complete current REST observation has been saved for the same generation/revision.

Protocol: Hyperliquid's official websocket, subscriptions and heartbeat docs.
The pinned hyperliquid-python-sdk 0.24.0 already depends on websocket-client.
"""
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
import json
import re
import threading
import time

from . import passive_timing
from .card_sync_evidence import normalize_fill


TESTNET_WS = 'wss://api.hyperliquid-testnet.xyz/ws'
CHANNELS = frozenset(('userFills', 'orderUpdates'))
MAX_FRAME_BYTES = 1024 * 1024
MAX_BATCH_ROWS = 10000
MAX_SYMBOLS = 64
MAX_SEEN = 256
MAX_PENDING_FILLS = 4096
PING_INTERVAL = 25.0
PONG_TIMEOUT = 10.0
BOOTSTRAP_TIMEOUT = 15.0
IDLE_TIMEOUT = 45.0
REST_CURRENT_SECONDS = 15.0
# Two configured sockets, at least five seconds between reconnects: even a
# server that repeatedly completes bootstrap then drops cannot exceed the
# documented 30 new websocket connections/minute for this process.
RECONNECT_MIN_SECONDS = 5.0
RECONNECT_MAX_SECONDS = 30.0


@dataclass(frozen=True)
class ReconciliationToken:
    """A local notification boundary; contains no exchange evidence."""
    account: str
    generation: int
    revision: int
    started: float


@dataclass(frozen=True)
class PendingFillBatch:
    """Detached, non-destructive sample; acknowledge only after durable commit.

    Sequence numbers never reset at a socket reconnect. They identify precisely
    the sampled rows, so an old acknowledgement cannot remove a later arrival.
    Fills are individual facts, never proof of complete account history.
    """
    account: str
    items: tuple

    @property
    def fills(self):
        return tuple(deepcopy(fill) for _, fill in self.items)


def _address(value):
    if (not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', value)
            or int(value[2:], 16) == 0):
        raise ValueError('EXACT_TESTNET_NOTIFICATION_ACCOUNT_REQUIRED')
    return value.lower()


def _symbol(value):
    return isinstance(value, str) and bool(re.fullmatch(r'[A-Za-z0-9:@._-]{1,64}', value))


def _integer(value):
    return type(value) is int and value >= 0


def _connector(url, **kwargs):
    # Lazy import keeps pure offline tests independent from SDK installation.
    from websocket import create_connection
    return create_connection(url, **kwargs)


class FillWakeups:
    """Two isolated sockets, bounded dirty hints and a fail-closed entry gate.

    The caller obtains a token before a full REST reconciliation and calls
``finish_reconciliation(token, complete=True)`` only AFTER its complete current
observation is durably saved. Events concurrent with that observation prevent
    clearing the gate. ``dirty_symbols`` is a scheduling hint, never fill evidence.
    ``entry_allowed`` proves notification continuity since that reconciliation,
    not freshness of exchange evidence. Existing entry/source/quantity/attempt
    authorization must independently retain its strict REST freshness clocks.
"""
    def __init__(self, routes, *, wake_event=None, clock=time.monotonic,
                 connector=None):
        if (not isinstance(routes, dict) or not routes
                or not set(routes) <= {'long_account', 'short_account'}):
            raise ValueError('EXPLICIT_TESTNET_NOTIFICATION_ROLES_REQUIRED')
        self._routes = {role: _address(account) for role, account in routes.items()}
        if len(set(self._routes.values())) != len(self._routes):
            raise ValueError('SEPARATE_TESTNET_NOTIFICATION_ACCOUNTS_REQUIRED')
        self._clock = clock
        self._connector = connector or _connector
        self.wake_event = wake_event if wake_event is not None else threading.Event()
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._threads = []
        self._sockets = {}
        self._states = {account: dict(generation=0, revision=0, connected=False,
            acknowledged=set(), snapshot=False, reconciled=False, dirty_all=True,
            dirty_symbols=set(), seen=OrderedDict(), pending_fills=OrderedDict(),
            fill_sequence=0, last_receive=None,
            opened=None, ping_at=None, last_ping=None, last_rest=None,
            protocol_failure=None,
            status='STARTUP_RECONCILIATION_REQUIRED')
            for account in self._routes.values()}

    def _account(self, account):
        result = self._routes.get(account, account)
        result = _address(result)
        if result not in self._states:
            raise ValueError('UNCONFIGURED_TESTNET_NOTIFICATION_ACCOUNT')
        return result

    def _notify_locked(self, state, *, symbols=(), all_symbols=False):
        state['revision'] += 1
        state['reconciled'] = False
        if all_symbols:
            state['dirty_all'] = True
            state['dirty_symbols'].clear()
        elif not state['dirty_all']:
            state['dirty_symbols'].update(symbols)
            if len(state['dirty_symbols']) > MAX_SYMBOLS:
                state['dirty_symbols'].clear()
                state['dirty_all'] = True
        self.wake_event.set()

    def _ready_locked(self, state):
        return (state['connected'] and state['snapshot']
                and state['acknowledged'] == CHANNELS)

    def _live_locked(self, state, now):
        # Gates remain fail-closed even if the transport thread is delayed
        # before detecting its expired heartbeat and closing the socket.
        return (state['last_receive'] is not None
                and 0 <= now - state['last_receive'] < IDLE_TIMEOUT
                and (state['ping_at'] is None
                     or 0 <= now - state['ping_at'] < PONG_TIMEOUT))

    def _gap_locked(self, state, code, protocol_failure=None):
        state['connected'] = False
        state['status'] = code
        state['protocol_failure'] = protocol_failure
        self._notify_locked(state, all_symbols=True)

    def _opened(self, account):
        """Transport session boundary; old-session messages cannot change it."""
        with self._lock:
            state = self._states[account]
            state['generation'] += 1
            state.update(connected=not self._stop.is_set(), acknowledged=set(),
                         snapshot=False, opened=self._clock(),
                         last_receive=self._clock(), last_ping=self._clock(),
                         ping_at=None, protocol_failure=None,
                         status='SNAPSHOT_RECONCILIATION_REQUIRED')
            state['seen'].clear()
            self._notify_locked(state, all_symbols=True)
            return state['generation']

    def _disconnected(self, account, generation, code='DISCONNECTED_RECONCILIATION_REQUIRED',
                      protocol_failure=None):
        changed = False
        with self._lock:
            state = self._states[account]
            if state['generation'] == generation and state['connected']:
                self._gap_locked(state, code, protocol_failure)
                changed = True
        if changed:
            self._record_gap(account, generation, code)

    def _record_gap(self, account, generation, code):
        try:
            passive_timing.record('connection_gap',
                account_role=next(role for role, value in self._routes.items() if value == account),
                generation=generation, reason=code)
        except Exception:
            pass  # Diagnostics cannot change notification continuity.

    def begin_reconciliation(self, account):
        account = self._account(account)
        with self._lock:
            state = self._states[account]
            return ReconciliationToken(account, state['generation'],
                                       state['revision'], self._clock())

    def finish_reconciliation(self, token, *, complete=False):
        if not isinstance(token, ReconciliationToken) or complete is not True:
            return False
        with self._lock:
            state = self._states.get(token.account)
            now = self._clock()
            if (state is None or self._stop.is_set()
                    or not self._ready_locked(state)
                    or (token.generation, token.revision) !=
                       (state['generation'], state['revision'])
                    or not 0 <= now - token.started <= REST_CURRENT_SECONDS
                    or not self._live_locked(state, now)):
                return False
            state.update(reconciled=True, dirty_all=False,
                         last_rest=now, status='CONNECTED_RECONCILED')
            state['dirty_symbols'].clear()
            return True

    def entry_allowed(self, account):
        account = self._account(account)
        with self._lock:
            state = self._states[account]
            now = self._clock()
            return (not self._stop.is_set() and self._ready_locked(state)
                    and state['reconciled']
                    and self._live_locked(state, now))

    def pending_accounts(self):
        with self._lock:
            return tuple(account for account, state in self._states.items()
                         if not state['reconciled'])

    def pending_fills(self, account=None):
        """Peek at buffered individual facts without consuming or clearing gates.

        Without an account, return batches only for accounts with pending fills.
        An explicit account returns its batch, including an empty batch.
        """
        account = self._account(account) if account is not None else None
        with self._lock:
            def batch(key):
                return PendingFillBatch(key, tuple(deepcopy(value)
                    for value in self._states[key]['pending_fills'].values()))
            if account is not None:
                return batch(account)
            return tuple(batch(key) for key, state in self._states.items()
                         if state['pending_fills'])

    def acknowledge_fills(self, batch):
        """Remove only captured unchanged rows after the runtime commits them.

        This acknowledges buffer delivery only, never continuity, current
        positions, order status or complete-history coverage.
        """
        if (not isinstance(batch, PendingFillBatch)
                or not isinstance(batch.account, str) or not isinstance(batch.items, tuple)):
            return False
        with self._lock:
            state = self._states.get(batch.account)
            if state is None:
                return False
            captured = set()
            for item in batch.items:
                if (not isinstance(item, tuple) or len(item) != 2
                        or type(item[0]) is not int or not isinstance(item[1], dict)):
                    return False
                sequence, fill = item
                fid = fill.get('fill_id')
                if not isinstance(fid, str):
                    return False
                current = state['pending_fills'].get(fid)
                if current is not None and current == (sequence, fill):
                    captured.add(fid)
            for fid in captured:
                del state['pending_fills'][fid]
            return True

    def dirty_symbols(self, account):
        """None means reconcile the entire account; otherwise a bounded tuple."""
        account = self._account(account)
        with self._lock:
            state = self._states[account]
            return None if state['dirty_all'] else tuple(sorted(state['dirty_symbols']))

    def health(self):
        with self._lock:
            return {role: dict(status=self._states[account]['status'],
                protocol_failure=self._states[account]['protocol_failure'],
                generation=self._states[account]['generation'],
                revision=self._states[account]['revision'],
                connected=self._states[account]['connected'],
                snapshot_received=self._states[account]['snapshot'],
                subscriptions_acknowledged=len(self._states[account]['acknowledged']),
                reconciliation_required=not self._states[account]['reconciled'],
                entry_allowed=self.entry_allowed(account),
                pending_fill_count=len(self._states[account]['pending_fills']),
                pending_symbols=len(self._states[account]['dirty_symbols']),
                all_symbols=self._states[account]['dirty_all'])
                for role, account in self._routes.items()}

    def _receive(self, account, generation, raw):
        # Stage bounded scalar diagnostic identities; the diagnostic sink is never
        # called while holding the feed lock. Snapshots/duplicates are excluded.
        received = passive_timing.stamp()
        events = [] if received is not None else None
        gaps = [] if received is not None else None
        accepted = self._receive_message(account, generation, raw,
                                         timing_events=events, timing_gaps=gaps)
        try:
            for reason in gaps or ():
                self._record_gap(account, generation, reason)
            if accepted and events:
                role = next(role for role, value in self._routes.items() if value == account)
                events = tuple(dict.fromkeys(events))
                for identity, revision, batch_count in events:
                    channel, symbol, *parts = identity
                    if channel == 'userFills':
                        exchange_at, fill_id, order_id = parts
                        fields = dict(fill_id=str(fill_id), order_id=str(order_id))
                    else:
                        order_id, exchange_at, status = parts
                        fields = dict(order_id=str(order_id), status=status)
                    passive_timing.record('notification_received',
                        at_ms=received[0], mono_ns=received[1], account_role=role,
                        symbol=symbol, generation=generation, revision=revision,
                        notification_type=channel, exchange_at_ms=exchange_at,
                        batch_count=batch_count, captured_count=len(events), **fields)
        except Exception:
            pass
        return accepted

    def _receive_message(self, account, generation, raw, *, timing_events=None, timing_gaps=None):
        """Validate a whole frame, then stage canonical fills and bounded hints."""
        try:
            if not isinstance(raw, (str, bytes)) or len(raw) > MAX_FRAME_BYTES:
                raise ValueError()
            # The official Python SDK handles this exact transport banner.
            # It is not a subscription acknowledgement or a fills snapshot.
            banner=raw in ('Websocket connection established.',
                          b'Websocket connection established.')
            message = dict(channel='_transportBanner') if banner else json.loads(raw)
            if not isinstance(message, dict):
                raise ValueError()
        except (TypeError, ValueError, UnicodeError, RecursionError):
            self._disconnected(account, generation, 'MALFORMED_NOTIFICATION_RECONCILIATION_REQUIRED',
                               'INVALID_JSON_OR_FRAME')
            return False
        with self._lock:
            state = self._states[account]
            if (self._stop.is_set() or generation != state['generation']
                    or not state['connected']):
                return False
            try:
                channel = message.get('channel')
                data = message.get('data')
                reason='UNEXPECTED_CHANNEL'
                if channel == 'pong':
                    state['ping_at'] = None
                elif channel == '_transportBanner' and banner:
                    pass
                elif channel == 'subscriptionResponse':
                    reason='INVALID_ACK_ENVELOPE'
                    if not isinstance(data, dict) or data.get('method') != 'subscribe':
                        raise ValueError()
                    subscription = data.get('subscription')
                    reason='INVALID_ACK_SUBSCRIPTION'
                    if (not isinstance(subscription, dict)
                            or subscription.get('type') not in CHANNELS):
                        raise ValueError()
                    kind=subscription['type']
                    allowed={'type','user'} | ({'aggregateByTime'} if kind=='userFills' else set())
                    if not {'type','user'}<=set(subscription)<=allowed:
                        raise ValueError()
                    reason='ACK_ACCOUNT_MISMATCH'
                    if _address(subscription['user'])!=account:
                        raise ValueError()
                    # Omitted aggregation defaults to false. Accept its
                    # documented explicit normalization, not a changed request.
                    reason='ACK_PARAMETERS_CHANGED'
                    if ('aggregateByTime' in subscription
                            and subscription['aggregateByTime'] is not False):
                        raise ValueError()
                    state['acknowledged'].add(kind)
                elif channel in CHANNELS:
                    # Data may race the independent subscription ACK. Validate
                    # and stage bounded hints, but readiness still requires both
                    # exact ACKs AND an explicit initial fills snapshot.
                    snapshot = False
                    if channel == 'userFills':
                        reason='INVALID_FILLS_ENVELOPE'
                        if not isinstance(data,dict):
                            raise ValueError()
                        reason='FILLS_ACCOUNT_MISMATCH'
                        if _address(data.get('user'))!=account:
                            raise ValueError()
                        reason='INVALID_SNAPSHOT_FLAG'
                        if ('isSnapshot' in data and type(data['isSnapshot']) is not bool):
                            raise ValueError()
                        # Official WsUserFills declares this flag optional. Its
                        # absence never substitutes for an initial snapshot.
                        snapshot = data.get('isSnapshot', False)
                        rows = data.get('fills')
                    else:
                        rows = data
                    reason='INVALID_NOTIFICATION_BATCH'
                    if not isinstance(rows, list) or len(rows) > MAX_BATCH_ROWS:
                        raise ValueError()
                    hints = set()
                    identities = []
                    incoming_fills = OrderedDict()
                    fill_fingerprints = {}
                    fill_identities = {}
                    for row in rows:
                        if channel == 'userFills':
                            reason='INVALID_FILL_IDENTITY'
                            if (not isinstance(row, dict) or not _symbol(row.get('coin'))
                                    or not all(_integer(row.get(k)) for k in ('time', 'tid', 'oid'))):
                                raise ValueError()
                            symbol = row['coin']
                            identity = (channel, symbol, row['time'], row['tid'], row['oid'])
                            # Historical identity-only fixtures remain wake-only.
                            # Any attempted economic payload must fully validate;
                            # no partial price/quantity can become fill evidence.
                            if set(row) - {'coin', 'time', 'tid', 'oid'}:
                                reason = 'INVALID_FILL_FACT'
                                value = normalize_fill(row, account, symbol, 1, 10**15-1)
                                fid = value['fill_id']
                                reason = 'FILL_FACT_CHANGED'
                                known = state['pending_fills'].get(fid)
                                seen_fact = state['seen'].get(identity)
                                if ((known is not None and known[1] != value)
                                        or (seen_fact is not None and seen_fact != value)
                                        or (fid in incoming_fills and incoming_fills[fid] != value)):
                                    raise ValueError()
                                incoming_fills[fid] = value
                                fill_fingerprints[identity] = value
                                fill_identities[fid] = identity
                        else:
                            reason='INVALID_ORDER_HINT'
                            order = row.get('order') if isinstance(row, dict) else None
                            if (not isinstance(order, dict) or not _symbol(order.get('coin'))
                                    or not _integer(order.get('oid'))
                                    or not _integer(row.get('statusTimestamp'))
                                    or not isinstance(row.get('status'), str)
                                    or not re.fullmatch(r'[A-Za-z]{1,64}', row['status'])):
                                raise ValueError()
                            symbol = order['coin']
                            identity = (channel, symbol, order['oid'], row['statusTimestamp'], row['status'])
                        if identity not in state['seen']:
                            hints.add(symbol)
                            identities.append(identity)
                    # Validate the whole bounded batch before mutating readiness.
                    # Reuse the existing bounded notification cache: after
                    # commit, an identical same-session replay should neither
                    # restage the fact nor cause another REST wakeup. Eviction
                    # or reconnect can replay it; the durable reducer still
                    # deduplicates against the committed lifecycle facts.
                    fresh_fills = [(fid, value) for fid, value in incoming_fills.items()
                                   if fid not in state['pending_fills']
                                   and state['seen'].get(fill_identities[fid]) != value]
                    reason = 'PENDING_FILL_BUFFER_FULL'
                    if len(state['pending_fills']) + len(fresh_fills) > MAX_PENDING_FILLS:
                        raise ValueError()
                    for fid, value in fresh_fills:
                        state['fill_sequence'] += 1
                        state['pending_fills'][fid] = (state['fill_sequence'], value)
                        hints.add(value['symbol'])
                    for identity in identities:
                        state['seen'][identity] = fill_fingerprints.get(identity)
                        if len(state['seen']) > MAX_SEEN:
                            state['seen'].popitem(last=False)
                    for identity, value in fill_fingerprints.items():
                        if identity in state['seen']:
                            state['seen'][identity] = value
                    if snapshot:
                        state['snapshot'] = True
                        state['status'] = 'SNAPSHOT_RECONCILIATION_REQUIRED'
                        self._notify_locked(state, all_symbols=True)
                    elif hints:
                        state['status'] = 'NOTIFICATION_RECONCILIATION_REQUIRED'
                        self._notify_locked(state, symbols=hints)
                        if timing_events is not None:
                            # Diagnostics may sample an oversized batch; trading
                            # continues to process every validated identity.
                            try:
                                timing_events.extend((identity, state['revision'], len(identities))
                                                     for identity in identities[:64])
                            except Exception:
                                pass
                else:
                    raise ValueError()
                state['last_receive'] = self._clock()
                return True
            except (TypeError, ValueError, KeyError):
                self._gap_locked(state, 'MALFORMED_NOTIFICATION_RECONCILIATION_REQUIRED',reason)
                if timing_gaps is not None:
                    timing_gaps.append('MALFORMED_NOTIFICATION_RECONCILIATION_REQUIRED')
                return False

    def _heartbeat(self, account, generation):
        """Return only PING/WAIT/CLOSE; the transport sends no other operation."""
        with self._lock:
            state = self._states[account]
            if (self._stop.is_set() or state['generation'] != generation
                    or not state['connected']):
                return 'CLOSE'
            now = self._clock()
            if (now - state['last_receive'] >= IDLE_TIMEOUT
                    or (state['ping_at'] is not None and now - state['ping_at'] >= PONG_TIMEOUT)
                    or (not self._ready_locked(state) and now - state['opened'] >= BOOTSTRAP_TIMEOUT)):
                self._gap_locked(state, 'NOTIFICATION_GAP_RECONCILIATION_REQUIRED')
            elif state['ping_at'] is None and now - state['last_ping'] >= PING_INTERVAL:
                state['ping_at'] = now
                state['last_ping'] = now
                return 'PING'
            else:
                return 'WAIT'
        self._record_gap(account, generation, 'NOTIFICATION_GAP_RECONCILIATION_REQUIRED')
        return 'CLOSE'

    def _worker(self, account):
        delay = RECONNECT_MIN_SECONDS
        while not self._stop.is_set():
            socket = None
            generation = None
            try:
                socket = self._connector(TESTNET_WS, timeout=3, enable_multithread=True)
                socket.settimeout(1)
                if self._stop.is_set():
                    break
                with self._lock:
                    self._sockets[account] = socket
                generation = self._opened(account)
                for channel in sorted(CHANNELS):
                    subscription = dict(type=channel, user=account)
                    if channel == 'userFills':
                        subscription['aggregateByTime'] = False
                    socket.send(json.dumps(dict(method='subscribe',
                        subscription=subscription)))
                while not self._stop.is_set():
                    heartbeat = self._heartbeat(account, generation)
                    if heartbeat == 'CLOSE':
                        break
                    if heartbeat == 'PING':
                        socket.send('{"method":"ping"}')
                    try:
                        raw = socket.recv()
                    except Exception as exc:
                        # websocket-client's timeout type derives from WebSocketException,
                        # not TimeoutError. Names are inspected locally, never logged.
                        if isinstance(exc, TimeoutError) or type(exc).__name__ == 'WebSocketTimeoutException':
                            continue
                        raise
                    if raw in ('', b'') or not self._receive(account, generation, raw):
                        break
                    with self._lock:
                        if self._ready_locked(self._states[account]):
                            delay = RECONNECT_MIN_SECONDS
            except Exception:
                # No raw server text, account secrets or exception payload is logged.
                pass
            finally:
                if generation is not None:
                    self._disconnected(account, generation)
                if socket is not None:
                    try:
                        socket.close(timeout=1)
                    except Exception:
                        pass
                with self._lock:
                    if self._sockets.get(account) is socket:
                        self._sockets.pop(account, None)
            if self._stop.wait(delay):
                break
            delay = min(RECONNECT_MAX_SECONDS, delay * 2)

    def start(self):
        with self._lock:
            if self._stop.is_set():
                raise ValueError('STOPPED_TESTNET_NOTIFICATIONS_CANNOT_RESTART')
            if self._threads:
                return
            for account in self._routes.values():
                thread = threading.Thread(target=self._worker, args=(account,),
                                          name='testnet-fill-wakeup', daemon=True)
                self._threads.append(thread)
                thread.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            sockets = list(self._sockets.values())
            threads = list(self._threads)
            for state in self._states.values():
                self._gap_locked(state, 'STOPPED')
        for socket in sockets:
            try:
                socket.close(timeout=1)
            except Exception:
                pass
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=4)
