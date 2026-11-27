"""Run the existing sequential map reader against async Playwright pages on one owner loop.

Each reader has a separate greenlet. Only public Playwright calls that return
awaitables yield to the loop. Local pixel/number processing stays serial.
"""
import asyncio
from contextlib import contextmanager
from contextvars import copy_context, ContextVar
from inspect import isawaitable

from greenlet import getcurrent, greenlet
from model1_execution import StageFailure


_ACCESS = ContextVar("coinglass_cycle_access",default=None)


def wait(value):
    return getcurrent().parent.switch(value) if isawaitable(value) else value


async def run_reader(function, *args, **kwargs):
    fiber = greenlet(lambda: function(*args, **kwargs))
    fiber.gr_context = copy_context()
    pending = fiber.switch()
    while not fiber.dead:
        try:
            result = await pending
        except BaseException as error:
            pending = fiber.throw(error)
        else:
            pending = fiber.switch(result)
    return pending


def unwrap(value):
    if isinstance(value, ReaderPageAPI):
        return value.raw
    if isinstance(value, dict):
        return {k: unwrap(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return type(value)(unwrap(v) for v in value)
    return value


class ReaderPageAPI:
    """Convert public async handles, including nested handles passed to evaluate()."""

    def __init__(self, raw, cache):
        self.raw, self.cache = raw, cache
        self.listeners = {}

    def wrap(self, value):
        if isinstance(value, dict):
            return {key: self.wrap(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return type(value)(self.wrap(item) for item in value)
        if type(value).__module__.startswith('playwright.async_api.'):
            if value not in self.cache:
                proxy = ReaderPageAPI(value, self.cache)
                self.cache[value] = proxy
                return proxy
            return self.cache[value]
        return value

    def __getattr__(self, name):
        member = getattr(self.raw, name)
        if not callable(member):
            return self.wrap(member)
        def call(*args, **kwargs):
            access = _ACCESS.get()
            # Stop subsequent navigation and map actions after another tab
            # observes a provider block. Disposal and teardown must still run.
            if access and name not in ('dispose', 'close', 'remove_listener', 'is_closed'):
                access()
            return self.wrap(wait(member(*unwrap(args), **unwrap(kwargs))))
        return call

    def on(self, event, callback):
        async def listener(*values):
            await run_reader(callback, *(self.wrap(v) for v in values))
        self.listeners[event, callback] = listener
        self.raw.on(event, listener)

    def remove_listener(self, event, callback):
        self.raw.remove_listener(event, self.listeners.pop((event, callback)))


class SharedAccount:
    """Only one official account renewal per drain of the existing queue."""

    def __init__(self, context, account):
        self.context, self.account = context, account
        self.renewal = asyncio.Lock()
        self.generation = 0
        self.cooldown = False
        self.blocked = None
        self.renewed = False
        self.checkpointed = False

    @contextmanager
    def observing(self, page):
        from session_capture import _managed_pages
        self.account.attach(self.context, page)
        try:
            yield
        finally:
            _managed_pages.pop(page, None)
            self.account.page = self.account.context = None
            self.account.verified = False

    def allowed(self):
        if self.cooldown:
            raise StageFailure('source_rate_limited')
        if self.blocked:
            raise StageFailure(self.blocked)

    async def authenticate(self, page, original):
        from dedicated_map_flow import _authenticate, _record_block
        async with self.renewal:
            self.allowed()
            if self.generation != original:
                return False
            try:
                if self.renewed:
                    await run_reader(_record_block, self.account, 'auth_gate_after_verification')
                self.renewed = True
                with self.observing(page):
                    await run_reader(_authenticate, self.account, self.context, page)
            except StageFailure as error:
                self.blocked = error.code
                raise
            self.generation += 1
            self.checkpointed = True
            return True

    async def read(self, page, target, output, observe):
        token = _ACCESS.set(self.allowed)
        try:
            return await self._read(page,target,output,observe)
        finally:
            _ACCESS.reset(token)

    async def _read(self, page, target, output, observe):
        from dedicated_map_flow import (HARD_BLOCKERS, PROTECTED_PROBE, _checked_action,
            _record_block, open_verified_map, read_selected_map)
        from functools import partial
        self.allowed()
        reader = partial(read_selected_map, base_only=True, observe=observe)
        # Checkpoint the restored account on a protected target in the same
        # tab. No extra navigation or unsolicited second authentication.
        async def select():
            self.allowed()
            if target == PROTECTED_PROBE and not self.checkpointed:
                async with self.renewal:
                    self.allowed()
                    if not self.checkpointed:
                        with self.observing(page):
                            selection = await run_reader(_checked_action, open_verified_map, page, target)
                            report_selection(selection)
                            await run_reader(self.account.checkpoint, self.context)
                        self.checkpointed = True
                        return
            selection = await run_reader(_checked_action, open_verified_map, page, target)
            report_selection(selection)

        def report_selection(result):
            if isinstance(result, dict) and isinstance(result.get('range_selection'),dict) and observe:
                observe('range_selection',result['range_selection'])

        try:
            original = self.generation
            if self.generation or self.account.state.get('cookies') or self.account.state.get('origins'):
                try:
                    await select()
                    return await run_reader(_checked_action, reader, page, target, output)
                except StageFailure as error:
                    if error.code != 'source_login_required':
                        raise
            just_proved = await self.authenticate(page, original)
            if not just_proved or target != PROTECTED_PROBE:
                await select()
            return await run_reader(_checked_action, reader, page, target, output)
        except StageFailure as error:
            if error.code == 'source_rate_limited':
                if not self.cooldown:
                    self.cooldown = True
                    self.account.source_cooldown(limited=True,
                        retry_after_seconds=getattr(error, 'retry_after_seconds', None))
            elif error.code in HARD_BLOCKERS:
                if not self.blocked:
                    self.blocked = error.code
                    await run_reader(_record_block, self.account, error.code, error)
            elif error.code == 'source_login_required' and self.renewed:
                self.blocked = 'auth_gate_after_verification'
                await run_reader(_record_block, self.account, self.blocked)
            raise
