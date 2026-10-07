"""Explicit isolated authenticated gateway into the worker's atomic inbox.

It is deliberately not a WSGI application or registered HTTP route. The test
transport invokes accept_authenticated directly. Authenticating a message only
records it; no call executes run_once, signs, or submits any exchange request.
"""
from . import experimental_plan_intake as intake
from .experimental_execution_runtime import IsolatedExecutionRuntime, MODE, RuntimeError
import experimental_execution_contract as contract


class IsolatedGateway:
    def __init__(self, runtime, *, key, mode=None):
        if type(runtime) is not IsolatedExecutionRuntime or mode != MODE or not isinstance(key,str) or not intake.HEX.fullmatch(key):
            raise RuntimeError('EXPLICIT_ISOLATED_AUTHENTICATED_GATEWAY_REQUIRED')
        self.runtime,self.key=runtime,key

    def ingest(self, message, *, now, not_before):
        state=self.runtime.store.load()
        if (contract.moment_ms(now)!=self.runtime.venue.now()
                or contract.moment_ms(not_before)!=state['not_before_ms']):
            raise RuntimeError('EXACT_GATEWAY_CLOCK_AND_RELEASE_FENCE_REQUIRED')
        return self.runtime.receive([message])['receipts'][0]

    def accept_authenticated(self, raw, headers, *, path=intake.PATH, method='POST'):
        if path!=intake.PATH or method!='POST':
            raise RuntimeError('EXACT_ISOLATED_GATEWAY_PATH_REQUIRED')
        now=self.runtime.venue.now()
        if not intake.authenticate(self.key,headers.get('X-Plan-Timestamp'),headers.get('X-Plan-Signature'),raw,now/1000):
            raise RuntimeError('EXPERIMENTAL_GATEWAY_AUTHENTICATION_REQUIRED')
        state=self.runtime.store.load()
        return intake.accept(raw,self,now=contract.iso_ms(now),not_before=contract.iso_ms(state['not_before_ms']))
