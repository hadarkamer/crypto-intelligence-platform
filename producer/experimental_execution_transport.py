"""Explicit, default-off transport for experimental Testnet plan records.

The separate, default-off experimental_execution_forwarder schedules this
module. Configuration is passed explicitly; this module never discovers live
credentials or an endpoint from environment.
This sends authenticated records, never exchange orders or source-state writes.
Receiver idempotency and source leases make retry safe. In-memory cancellation
retries survive source outages within a process; after restart source history is
reread and missing heartbeats revoke permission through the finite source lease.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import http.client
import json
import re
import time
from urllib.parse import urlsplit

import approved_alert_contract as contract
import experimental_execution_bridge as bridge

PATH = '/internal/testnet-experimental-plans/v1'
MODE = 'experimental_plan_record_transport_v1'
APPROVED_MODE = 'experimental_approved_alert_transport_v2'
MAX_RESPONSE_BYTES = 8192
MAX_BUFFER = 8192
MAX_PER_TICK = 16
MAX_TICK_SECONDS = 12
# Leave delivery time for retained cancellations when the source is slow.
# Each source statement is independently limited to three seconds, so the
# reader may return up to one statement after this soft deadline.
MAX_SOURCE_SECONDS = 6
MAX_SCOPES = 32


class TransportError(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise TransportError(reason)


def endpoint_host(endpoint, allowed_hosts):
    """Exact configured HTTPS origin and protocol path; never follow redirects."""
    _require(isinstance(endpoint, str) and isinstance(allowed_hosts, (set, frozenset, tuple, list))
        and 0 < len(allowed_hosts) <= 16, 'TRANSPORT_ENDPOINT_CONFIG')
    try:
        parsed = urlsplit(endpoint)
        host = parsed.hostname
        _require(parsed.scheme == 'https' and not parsed.username and not parsed.password
            and parsed.port in (None, 443) and parsed.path == PATH
            and not parsed.query and not parsed.fragment and bool(host)
            and re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', host) is not None
            and '..' not in host and host in allowed_hosts, 'TRANSPORT_ENDPOINT_CONFIG')
        return host
    except (ValueError, TypeError):
        raise TransportError('TRANSPORT_ENDPOINT_CONFIG') from None


@dataclass(frozen=True)
class Config:
    endpoint: str
    allowed_hosts: tuple
    secret: str = field(repr=False)
    fence: datetime
    mode: str = ''

    def validate(self):
        _require(self.mode in (MODE, APPROVED_MODE), 'TRANSPORT_DISABLED')
        endpoint_host(self.endpoint, self.allowed_hosts)
        _require(isinstance(self.secret, str) and contract.HEX.fullmatch(self.secret), 'TRANSPORT_SECRET_CONFIG')
        _require(isinstance(self.fence, datetime), 'TRANSPORT_FENCE_CONFIG')
        contract.moment_ms(self.fence.isoformat())
        return self


def signature(secret, stamp, raw):
    _require(isinstance(secret, str) and contract.HEX.fullmatch(secret), 'TRANSPORT_SECRET_CONFIG')
    return hmac.new(bytes.fromhex(secret), PATH.encode() + b'\n' + str(stamp).encode()
                    + b'\n' + raw, hashlib.sha256).hexdigest()


def encoded(message):
    return json.dumps(contract.validate(message), sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode()


def source_content(message):
    """Match receiver source-digest semantics across recipient-local episodes."""
    result = contract.immutable(message)
    result.update({key: message[key] for key in contract.TEMPORAL})
    return result


def _decoded_ack(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            _require(key not in result, 'TRANSPORT_ACK_INVALID')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(TransportError('TRANSPORT_ACK_INVALID')))


def post_plan(message, config, *, now_ms, connection_factory=None):
    """One bounded TLS POST; injectable connection is solely for isolated tests."""
    config.validate()
    _require(contract.is_approved(message) == (config.mode == APPROVED_MODE), 'TRANSPORT_PROTOCOL_MODE')
    _require(type(now_ms) is int and now_ms > 0, 'TRANSPORT_CLOCK_INVALID')
    host = endpoint_host(config.endpoint, config.allowed_hosts)
    raw = encoded(message)
    _require(0 < len(raw) <= contract.MAX_BYTES, 'TRANSPORT_BODY_INVALID')
    stamp = str(now_ms // 1000)
    _require(re.fullmatch(r'[0-9]{10}', stamp) is not None, 'TRANSPORT_CLOCK_INVALID')
    connection = None
    try:
        connection = (connection_factory or http.client.HTTPSConnection)(host, port=443, timeout=3)
        connection.request('POST', PATH, body=raw, headers={'Content-Type': 'application/json',
            'Content-Length': str(len(raw)), 'X-Plan-Timestamp': stamp,
            'X-Plan-Signature': signature(config.secret, stamp, raw)})
        response = connection.getresponse()
        body = response.read(MAX_RESPONSE_BYTES + 1)
        _require(response.status == 200, 'TRANSPORT_HTTP_UNACKNOWLEDGED')
        _require(response.getheader('Content-Type', '').split(';', 1)[0].strip().lower() == 'application/json'
            and isinstance(body, bytes) and 0 < len(body) <= MAX_RESPONSE_BYTES, 'TRANSPORT_ACK_INVALID')
        ack = _decoded_ack(body)
        _require(isinstance(ack, dict) and set(ack) ==
            {'occurrence_id', 'revision', 'status', 'record_only', 'entry_permission'}, 'TRANSPORT_ACK_INVALID')
        _require(ack['occurrence_id'] == message['occurrence_id'] and ack['record_only'] is True
            and type(ack['revision']) is int and ack['revision'] >= 1
            and ack['entry_permission'] in ('WAITING', 'RETIRED') and ack['status'] in ('RECORDED', 'DUPLICATE'),
            'TRANSPORT_ACK_INVALID')
        return ack
    except TransportError:
        raise
    except Exception:
        raise TransportError('TRANSPORT_DELIVERY_UNAVAILABLE') from None
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


class Sender:
    """Manual tick only. Retries are bounded, cancellations first, then FIFO.

    Last-attempt order rotates failed records, preventing one failing receiver
    request from monopolizing the budget. Acknowledged tombstones suppress stale
    source plans; queued cancellations are never evicted for lease expiry.
    """
    def __init__(self):
        self.pending = {}
        self.acked = {}
        self.last_attempt = {}
        self.serial = 0
        self.source_offset = 0

    def _merge(self, values):
        _require(isinstance(values, list) and len(values) <= MAX_BUFFER, 'TRANSPORT_SOURCE_OVERFLOW')
        validated = [contract.validate(v) for v in values]
        pending, acked = dict(self.pending), self.acked
        for value in validated:
            identity = value['occurrence_id']
            previous = pending.get(identity) or acked.get(identity)
            if previous:
                _require(contract.plan_digest(previous) == contract.plan_digest(value), 'TRANSPORT_SOURCE_CONFLICT')
                if previous['kind'] == 'CANCEL':
                    continue
                if value['kind'] != 'CANCEL' or previous['kind'] == 'CANCEL':
                    if value['source_sequence'] <= previous['source_sequence']:
                        _require(value['source_sequence'] != previous['source_sequence'] or
                                 source_content(value) == source_content(previous),
                                 'TRANSPORT_SOURCE_CONFLICT')
                        continue
            _require(identity in pending or len(pending) < MAX_BUFFER, 'TRANSPORT_BUFFER_FULL')
            pending[identity] = value
        self.pending = pending

    def tick(self, scopes, config=None, *, now_ms=None, reader=None, post=None, monotonic=None,
             acknowledge=None):
        report = dict(status='DISABLED', attempted=0, recorded=0, duplicates=0, deferred=0,
                      source_records_changed=0, exchange_requests_sent=0)
        if config is None or config.mode not in (MODE, APPROVED_MODE):
            return report
        config.validate()
        _require(isinstance(scopes, (set, frozenset, tuple, list)) and len(scopes) <= MAX_SCOPES
            and all(isinstance(scope, str) and contract.HEX.fullmatch(scope) for scope in scopes),
            'TRANSPORT_SCOPES_INVALID')
        now_ms = int(time.time()*1000) if now_ms is None else now_ms
        _require(type(now_ms) is int and now_ms > 0, 'TRANSPORT_CLOCK_INVALID')
        now = datetime.fromtimestamp(now_ms/1000, timezone.utc)
        monotonic = monotonic or time.monotonic
        started = monotonic()
        approved_mode = config.mode == APPROVED_MODE
        read = reader or (bridge.read_approved if approved_mode else bridge.read_experimental)
        source_env = {'EXPERIMENTAL_EXECUTION_BRIDGE_MODE': bridge.APPROVED_MODE if approved_mode else bridge.MODE}
        source_ok, source_errors, refreshed, blocked = True, 0, set(), set()
        try:
            kwargs = dict(now=now, env=source_env,
                deadline_monotonic=started+MAX_SOURCE_SECONDS, clock=monotonic)
            if approved_mode:
                kwargs['source_offset'] = self.source_offset
            values = read(scopes, config.fence, **kwargs)
            _require(isinstance(values, list) and len(values) <= MAX_BUFFER, 'TRANSPORT_SOURCE_OVERFLOW')
            if approved_mode:
                self.source_offset = getattr(values, 'next_source_offset', self.source_offset)
                blocked.update(getattr(values, 'blocked_occurrences', ()))
                source_errors += getattr(values, 'source_errors', 0)
                # Group first: a conflicting later copy must not allow the
                # earlier copy to be sent from this same read.
                groups = {}
                for value in values:
                    try:
                        _require(contract.is_approved(value), 'TRANSPORT_PROTOCOL_MODE')
                        valid = contract.validate(value)
                        groups.setdefault(valid['occurrence_id'], []).append(valid)
                    except (ValueError, TypeError, KeyError, ArithmeticError):
                        source_errors += 1
                        if isinstance(value, dict) and isinstance(value.get('occurrence_id'), str):
                            blocked.add(value['occurrence_id'])
                for identity, group in groups.items():
                    if identity in blocked:
                        continue
                    try:
                        self._merge(group)
                        refreshed.add(identity)
                    except (ValueError, TypeError, KeyError, ArithmeticError):
                        blocked.add(identity)
                        source_errors += 1
            else:
                _require(all(not contract.is_approved(v) for v in values), 'TRANSPORT_PROTOCOL_MODE')
                self._merge(values)
        except Exception:
            source_ok = False
        for identity, value in list(self.pending.items()):
            if (not contract.is_approved(value) and value['kind'] != 'CANCEL'
                    and now_ms >= contract.moment_ms(value['valid_until'])):
                self.pending.pop(identity)
                self.last_attempt.pop(identity, None)
        ordered = sorted(self.pending, key=lambda identity: (
            self.pending[identity]['kind'] != 'CANCEL', self.last_attempt.get(identity, -1), identity))
        for identity in ordered:
            value = self.pending[identity]
            if report['attempted'] >= MAX_PER_TICK or monotonic()-started >= MAX_TICK_SECONDS:
                break
            if value['kind'] != 'CANCEL' and (not source_ok or
                    (approved_mode and (identity not in refreshed or identity in blocked))):
                continue
            report['attempted'] += 1
            self.serial += 1
            self.last_attempt[identity] = self.serial
            try:
                ack = (post or post_plan)(value, config, now_ms=now_ms)
                _require(isinstance(ack, dict) and ack.get('occurrence_id') == identity
                    and ack.get('record_only') is True and ack.get('status') in ('RECORDED', 'DUPLICATE'),
                    'TRANSPORT_ACK_INVALID')
                if approved_mode:
                    # A lost source acknowledgement only retries the same
                    # occurrence. Receiver tombstones prevent another order.
                    (acknowledge or bridge.acknowledge_approved)(scopes, value, env=source_env)
                report['recorded' if ack['status']=='RECORDED' else 'duplicates'] += 1
                self.acked[identity] = value
                self.pending.pop(identity)
                self.last_attempt.pop(identity, None)
            except Exception:
                pass
        if len(self.acked) > MAX_BUFFER:
            # This bounded memo is an optimization. Receiver tombstones remain
            # durable; re-sending an evicted record cannot authorize a new entry.
            self.acked = dict(list(self.acked.items())[-MAX_BUFFER:])
        report['source_errors'] = source_errors
        report['deferred'] = len(self.pending)
        report['status'] = ('SOURCE_UNAVAILABLE' if not source_ok else
                            'SOURCE_PARTIAL' if source_errors else 'PENDING_RETRY' if self.pending else 'COMPLETED')
        return report
