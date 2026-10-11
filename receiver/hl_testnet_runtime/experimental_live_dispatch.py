"""Explicit, default-off Testnet sender for prospective execution requests.

No import, constructor or source intake signs or performs HTTP. The application
must supply its durable request reader, trusted final-context collector and
current release authority. Only an admission minted by this instance from the
existing PostgreSQL request budget can reach the fixed Testnet endpoint.
Neither a missing reply nor a local exception authorizes a second attempt.
"""
from copy import deepcopy
import http.client
import json
import re
import threading
import time
import uuid
import weakref

from . import card_lifecycle as life, checks, request_budget
from . import experimental_execution_dispatch as boundary
from . import experimental_live_release as releases
from . import filled_quantity_dispatch as wire, two_account_execution as roles

HOST = 'api.hyperliquid-testnet.xyz'
APPROVAL = 'approved_testnet_v1'


class LiveDispatchError(wire.DispatchError):
    """Fixed redacted codes only; never remote response or key material."""


class DefinitelyNotSubmitted(wire.DefinitelyUnsent):
    """Exact committed transport claim whose local code never entered HTTP.

    The worker may retire this one unsent request and restore protection work.
    A transport exception, unknown claim COMMIT or a reloaded request can never
    obtain this certificate.
    """
    def __init__(self, request, cause):
        self._identity = self.identity(request)
        self._claim = request['transport_claim']
        code = str(cause)
        self.reason = code if re.fullmatch(r'[A-Z][A-Z0-9_]{0,99}', code) else 'LOCAL_PRETRANSPORT_VALIDATION_FAILED'
        delay = getattr(cause, 'retry_after_ms', None)
        self.retry_after_ms = delay if type(delay) is int and 0 <= delay <= request_budget.WINDOW_MS else None
        wire.DispatchError.__init__(self, self.reason)

    def matches(self, request):
        return (request.get('transport_claim') == self._claim and super().matches(request))


def _signature(value):
    if (not isinstance(value, dict) or set(value) != {'r', 's', 'v'}
            or type(value['v']) is not int or value['v'] not in (27, 28)
            or any(not isinstance(value[k], str)
                or not re.fullmatch(r'0x[0-9a-fA-F]{1,64}', value[k]) for k in ('r', 's'))):
        raise LiveDispatchError('INVALID_LOCAL_SIGNATURE')
    return deepcopy(value)


class LiveDispatchPort:
    domain = 'testnet'
    software_only = False

    def __init__(self, env, *, context_loader, request_loader, release_loader, claim_transport,
                 budget, clock=None):
        if env.get('HL_TESTNET_EXPERIMENTAL_DISPATCH') != APPROVAL:
            raise LiveDispatchError('EXPERIMENTAL_LIVE_DISPATCH_NOT_RELEASED')
        if type(budget) is not request_budget.Budget:
            raise LiveDispatchError('TESTNET_SHARED_REQUEST_BUDGET_REQUIRED')
        if not all(callable(f) for f in (context_loader, request_loader, release_loader, claim_transport)):
            raise LiveDispatchError('TRUSTED_DURABLE_CONTEXT_AND_RELEASE_READERS_REQUIRED')
        self.env = env
        self.context_loader, self.request_loader = context_loader, request_loader
        self.release_loader, self.budget = release_loader, budget
        self.claim_transport = claim_transport
        self.clock = clock or (lambda: time.time_ns() // 1000000)
        self.sent = 0
        self._admissions = weakref.WeakKeyDictionary()
        self._guard = threading.Lock()
        # Public route validation does not read, derive or inspect private keys.
        accounts = [roles.route_for(env, role)['account'] for role in roles.ROLES]
        if len(set(accounts)) != 2:
            raise LiveDispatchError('INDEPENDENT_ACCOUNT_ROUTES_REQUIRED')

    def now(self):
        return self.clock()

    def _release(self, proposal=None):
        if self.env.get('HL_TESTNET_EXPERIMENTAL_DISPATCH') != APPROVAL:
            raise LiveDispatchError('EXPERIMENTAL_LIVE_DISPATCH_NOT_RELEASED')
        try:
            value = deepcopy(self.release_loader())
            expected = {role: roles.route_for(self.env, role)['account'] for role in roles.ROLES}
            valid = (isinstance(value, dict) and value.get('domain') == 'testnet'
                and value.get('dispatch_enabled') is True and value.get('protection_enabled') is True
                and type(value.get('entries_enabled')) is bool
                and value.get('routes') == expected
                and type(value.get('not_before_ms')) is int
                and releases.entry_window_valid(value)
                and value['not_before_ms'] <= self.now())
            if not valid:
                raise ValueError()
            life.ident(value['release_id'], r'[0-9a-f]{64}')
        except Exception:
            raise LiveDispatchError('EXACT_CURRENT_TESTNET_RELEASE_REQUIRED') from None
        if proposal is not None and proposal['operation'] == 'ENTRY':
            if not releases.entry_enabled(value,self.now()):
                raise LiveDispatchError('EXPERIMENTAL_ENTRY_RELEASE_CLOSED')
        return value

    def reserve_transport(self, proposal):
        """Acquire the unchanged one-use shared budget immediately before final checks."""
        self._release(proposal)
        action = wire.canonical_wire_action(proposal['action'])
        roles.route_for(self.env, proposal['role'], proposal['account'])
        permit = self.budget.acquire('/exchange', dict(action=action), host=HOST,
            priority='background' if proposal['operation'] == 'ENTRY' else 'protection')
        admission = wire.TransportAdmission(proposal, permit)
        with self._guard:
            self._admissions[admission] = False
        return admission

    def _durable(self, request):
        if request.get('domain') != 'testnet':
            raise LiveDispatchError('DURABLE_TESTNET_REQUEST_REQUIRED')
        wire.DefinitelyUnsent.identity(request)
        try:
            current = self.request_loader(request['request_id'])
        except Exception:
            raise LiveDispatchError('DURABLE_REQUEST_READ_FAILED') from None
        if current != request:
            raise LiveDispatchError('EXACT_DURABLE_ATTEMPT_REQUIRED')

    def _context(self, request, release):
        try:
            value = deepcopy(self.context_loader(deepcopy(request)))
        except Exception as exc:
            from .experimental_live_provider import ProviderError
            if isinstance(exc, ProviderError) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,99}', str(exc)):
                raise LiveDispatchError(str(exc)) from None
            raise LiveDispatchError('FINAL_TRUSTED_CONTEXT_UNAVAILABLE') from None
        # A collector must not substitute another environment or route for the
        # one from which the actual signer will be constructed.
        for role in roles.ROLES:
            expected = roles.route_for(self.env, role)
            roles.route_for(value['env'], role, expected['account'], expected['agent'])
        if (value['safety']['not_before_ms'] != release['not_before_ms']
                or request['prepared_at_ms'] < release['not_before_ms']):
            raise LiveDispatchError('RELEASE_FENCE_CHANGED')
        if (request['proposal']['operation']=='ENTRY'
                and release.get('entry_policy')==releases.CONTINUOUS_ENTRY
                and value['source']['family']=='maxpain'
                and not boundary.contract.is_approved(value['source'])):
            raise LiveDispatchError('APPROVED_ALERT_REQUIRED_FOR_CONTINUOUS_MAXPAIN')
        return value

    def send(self, request, *, admission=None):
        """Send one exact committed attempt; always reconcile before new work."""
        value = deepcopy(request)
        self._durable(value)
        if value.get('transport_claim') is not None:
            raise LiveDispatchError('DURABLE_TRANSPORT_ATTEMPT_ALREADY_CLAIMED')
        if admission is not None:
            if type(admission) is not wire.TransportAdmission:
                raise LiveDispatchError('EXACT_SINGLE_USE_TRANSPORT_ADMISSION_REQUIRED')
            with self._guard:
                if admission not in self._admissions or self._admissions[admission]:
                    raise LiveDispatchError('EXACT_SINGLE_USE_SHARED_BUDGET_ADMISSION_REQUIRED')
                self._admissions[admission] = True
            admission.validate(value)
        claim = uuid.uuid4().hex
        try:
            claimed = self.claim_transport(deepcopy(value), claim)
        except Exception:
            # A lost COMMIT must never produce a signer or unsent certificate.
            raise LiveDispatchError('DURABLE_TRANSPORT_CLAIM_UNCONFIRMED') from None
        if claimed != {**value, 'transport_claim': claim}:
            raise LiveDispatchError('EXACT_DURABLE_TRANSPORT_CLAIM_REQUIRED')
        value = deepcopy(claimed)
        try:
            self._durable(value)
            release = self._release(value['proposal'])
            context = self._context(value, release)
            first = boundary.review(value, context, now_ms=self.now())
            p = value['proposal']
            try:
                wallet = roles.wallet_for_role(self.env, p['role'], p['account'], first['agent'])
                if life.address(wallet.address) != first['agent']:
                    raise LiveDispatchError('ROLE_SIGNER_MISMATCH')
                from hyperliquid.utils.signing import sign_l1_action
                action = deepcopy(first['action'])
                expires = value['nonce'] + 15000
                signature = _signature(sign_l1_action(wallet, action, None, value['nonce'], expires, False))
            except Exception:
                raise LiveDispatchError('ROLE_LOCAL_SIGNING_FAILED') from None
            if action != first['action']:
                raise LiveDispatchError('SIGNER_MUTATED_FROZEN_ACTION')
            body = json.dumps(dict(action=action, nonce=value['nonce'], signature=signature,
                expiresAfter=expires), allow_nan=False, separators=(',', ':')).encode()
            if len(body) > 16384:
                raise LiveDispatchError('REQUEST_TOO_LARGE')
            # Preparation and the durable one-use claim precede the short permit.
            # Budget denial is still definitely unsent; final source, ownership
            # and release checks run AFTER admission, retaining original clocks.
            if admission is None:
                admission = self.reserve_transport(p)
                admission.bind(value)
                with self._guard:
                    self._admissions[admission] = True
                admission.validate(value)
            self._durable(value)
            latest_release = self._release(p)
            latest_context = self._context(value, latest_release)
            second = boundary.review(value, latest_context, now_ms=self.now())
            if release != latest_release or first['context_digest'] != second['context_digest']:
                raise LiveDispatchError('DISPATCH_CONTEXT_CHANGED_RECONCILE_FIRST')
            admission.consume(value)
        except Exception as exc:
            raise DefinitelyNotSubmitted(value, exc) from None
        # Once HTTP is entered, all transport/parse failures have unknown outcome.
        # Fixed host, path, timeout, no redirect handling and no retry loop.
        connection = None
        try:
            connection = http.client.HTTPSConnection(HOST, timeout=4)
            self.sent += 1
            connection.request('POST', '/exchange', body, {'Content-Type': 'application/json'})
            response = connection.getresponse()
            raw = response.read(checks.MAX_BYTES + 1)
            if response.status != 200 or len(raw) > checks.MAX_BYTES:
                raise ValueError()
            reply = wire.normalized_reply(checks.decode(raw), action['type'],
                account=p['account'], agent=first['agent'])
            # Remote human text is never needed for execution or persisted here.
            return {k: v for k, v in reply.items() if k in ('state', 'code', 'oid', 'rejection_subject')}
        except Exception:
            raise LiveDispatchError('TRANSPORT_OUTCOME_UNKNOWN') from None
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
