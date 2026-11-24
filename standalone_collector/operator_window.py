"""Authenticated single-owner viewer and bounded private database transitions."""
import asyncio
import base64
import hashlib
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
import hmac
import os
from pathlib import Path
import re
import secrets
import time
from urllib.parse import urlsplit
from uuid import UUID

from aiohttp import WSMsgType, web

from operator_desktop import ASSETS, Desktop, probe
from operator_login import LoginIncidentError, begin_window, close_window, finish_verified_window
from session_runtime import ManagedSession
from session_state import WORK_LOCK

PREFIX = '/coinglass/operator'
COOKIE = '__Secure-cg-operator'
HEADERS = {'Cache-Control':'no-store', 'Referrer-Policy':'no-referrer',
           'X-Content-Type-Options':'nosniff', 'X-Frame-Options':'DENY',
           'Permissions-Policy':'camera=(), microphone=(), geolocation=()',
           'Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'self'; "
               "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
               "form-action 'none'; object-src 'none'"}
HERE = Path(__file__).resolve().parent


def uuid_value(value):
    if not isinstance(value,str):
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def https_origin(value):
    try:
        parts = urlsplit(value)
        if (parts.scheme=='https' and parts.hostname and not parts.username and not parts.password
                and parts.path in ('','/') and not parts.query and not parts.fragment
                and parts.port in (443,None)):
            return 'https://'+parts.netloc
    except ValueError:
        pass
    return None


def response(data,status=200):
    return web.json_response(data,headers=HEADERS,status=status)


class Database:
    def __init__(self,store):
        self.store = store

    def _lock(self,conn):
        if not conn.execute('SELECT pg_try_advisory_lock(%s) AS held',(WORK_LOCK,)).fetchone()['held']:
            raise LoginIncidentError('collector_busy')

    def reserve(self,incident):
        with self.store.connect() as conn:
            window_id,expires = begin_window(conn,incident)
            row = conn.execute("""
                SELECT symbol, model, timeframe FROM collector_private.coinglass_login_jobs
                WHERE incident_id=%s ORDER BY (timeframe='12H') DESC, job_id LIMIT 1
                """,(incident,)).fetchone()
            if row is None:
                raise LoginIncidentError('login_window_unavailable')
            session = ManagedSession.from_environment(conn,window_id)
            target = (row['symbol'], row['model'], row['timeframe'])
            return window_id, expires, target, session.store.revision()

    def finish(self,window,state):
        with self.store.connect() as conn:
            self._lock(conn)
            session = ManagedSession.from_environment(conn,window.id)
            return finish_verified_window(session.store, window.incident, window.id, window.target,
                                          state, window.revision, window.human_wait_ms)

    def release(self,window):
        with self.store.connect() as conn:
            close_window(conn,window.incident,window.id)


@dataclass
class Window:
    id: str
    incident: str
    expires: datetime
    target: tuple
    revision: int
    ticket: str
    redeem_before: float
    challenge: str = ''
    desktop: object = None
    viewer: str = ''
    attached_at: float = 0.0
    human_wait_ms: int | None = None
    attached: bool = False
    connected_once: bool = False
    task: object = None


class WindowManager:
    def __init__(self,db,owner,origin,app_origin,enabled,desktop=Desktop,probe_fn=probe):
        self.db,self.owner,self.origin,self.app_origin = db,owner,origin,app_origin
        self.enabled,self.desktop,self.probe_fn = enabled,desktop,probe_fn
        self.ready = False
        self.current = None
        self.lock = asyncio.Lock()

    async def lifecycle(self,app):
        async def check():
            if self.enabled and self.owner and self.origin and self.app_origin:
                with suppress(Exception):
                    self.ready = bool(await self.probe_fn())
        checker = asyncio.create_task(check())
        try:
            yield
        finally:
            self.ready = False
            checker.cancel()
            await asyncio.gather(checker,return_exceptions=True)
            if self.current is not None:
                await self.stop(self.current)

    async def stop(self,window):
        if window.task:
            window.task.cancel()
            await asyncio.gather(window.task,return_exceptions=True)

    async def open(self,incident,owner,challenge):
        if not self.owner or not owner or not hmac.compare_digest(self.owner,owner):
            return response({'error':'forbidden'},403)
        if not incident:
            return response({'error':'invalid_incident_id'},400)
        if not valid_proof(challenge):
            return response({'error':'invalid_viewer_challenge'},400)
        if not self.ready:
            return response({'error':'private_browser_transport_unavailable'},503)
        async with self.lock:
            if self.current is not None:
                return response({'error':'login_window_in_progress'},409)
            try:
                wid,expiry,target,revision = await asyncio.to_thread(self.db.reserve,incident)
            except LoginIncidentError as exc:
                return response({'error':str(exc)},409)
            except Exception:
                return response({'error':'private_browser_transport_unavailable'},503)
            window = Window(wid,incident,expiry,target,revision,secrets.token_urlsafe(32),time.monotonic()+75)
            window.challenge = challenge
            self.current = window
            try:
                window.desktop = await self.desktop.start(*target)
            except BaseException as exc:
                try:
                    await asyncio.to_thread(self.db.release,window)
                finally:
                    self.current = None
                if isinstance(exc,asyncio.CancelledError):
                    raise
                return response({'error':'private_browser_transport_unavailable'},503)
            window.task = asyncio.create_task(self._watch(window))
            return response({'path':PREFIX+'#'+window.ticket,'expires_at':expiry.isoformat()})

    async def _watch(self,window):
        async def run():
            # Expire unused tickets and abandoned first connections promptly.
            while not window.connected_once:
                if time.monotonic() >= window.redeem_before:
                    return
                await asyncio.sleep(.25)
            state = await window.desktop.verified_state()
            while True:
                try:
                    await asyncio.to_thread(self.db.finish,window,state)
                    return
                except LoginIncidentError as exc:
                    if str(exc)!='collector_busy':
                        raise
                    await asyncio.sleep(1)
        try:
            async with asyncio.timeout(max(0,(window.expires-datetime.now(timezone.utc)).total_seconds())):
                await run()
        except Exception:
            pass
        finally:
            window.viewer = window.ticket = window.challenge = ''
            try:
                await window.desktop.close()
            finally:
                try:
                    await asyncio.to_thread(self.db.release,window)
                finally:
                    if self.current is window:
                        self.current = None

    def same_origin(self,request):
        return self.origin and request.headers.get('Origin')==self.origin

    def authorized(self,request):
        window = self.current
        cookie = request.cookies.get(COOKIE,'')
        if (window and window.viewer and len(cookie)==len(window.viewer) and cookie.isascii()
                and hmac.compare_digest(cookie,window.viewer)
                and datetime.now(timezone.utc)<window.expires):
            return window
        return None

    async def admit(self,request):
        if (not self.app_origin or request.headers.get('Origin')!=self.app_origin
                or request.content_type!='application/x-www-form-urlencoded'
                or request.content_length is None or request.content_length>512):
            return response({'error':'forbidden'},403)
        try:
            body = await request.post()
        except (ValueError,UnicodeError):
            return response({'error':'forbidden'},403)
        ticket = body.get('ticket')
        verifier = body.get('verifier')
        window = self.current
        if not (window and window.ticket and valid_proof(ticket) and valid_proof(verifier)
                and len(body)==2 and time.monotonic()<window.redeem_before
                and hmac.compare_digest(ticket,window.ticket)
                and hmac.compare_digest(viewer_challenge(verifier),window.challenge)):
            return response({'error':'expired'},403)
        window.ticket = window.challenge = ''
        window.viewer = secrets.token_urlsafe(32)
        result = web.HTTPSeeOther(location=PREFIX,headers=HEADERS)
        result.set_cookie(COOKIE,window.viewer,httponly=True,secure=True,samesite='Strict',
                          path=PREFIX,max_age=max(1,int((window.expires-datetime.now(timezone.utc)).total_seconds())))
        raise result

    async def verify(self,request):
        window = self.authorized(request)
        if not self.same_origin(request) or not window or not window.attached:
            return response({'error':'forbidden'},403)
        try:
            await window.desktop.verify_now()
        except OSError:
            return response({'error':'window_closed'},409)
        return response({'accepted':True})

    async def display(self,request):
        window = self.authorized(request)
        if not self.same_origin(request) or not window or window.connected_once or window.attached:
            return response({'error':'forbidden'},403)
        window.attached = True
        ws = web.WebSocketResponse(protocols=('binary',),compress=False,max_msg_size=256*1024,
                                   heartbeat=30)
        writer = None
        tasks = []
        client_closed = True
        try:
            reader,writer = await window.desktop.connect_display()
            await ws.prepare(request)
            window.attached_at = time.monotonic()
            window.connected_once = True
            async def downstream():
                while data := await reader.read(65536):
                    await ws.send_bytes(data)
            async def upstream():
                async for msg in ws:
                    if msg.type!=WSMsgType.BINARY:
                        break
                    writer.write(msg.data)
                    await writer.drain()
            source = asyncio.create_task(downstream())
            viewer = asyncio.create_task(upstream())
            tasks = [source,viewer]
            finished,_ = await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
            # A server-side EOF normally means the dedicated browser is now verifying the map.
            client_closed = viewer in finished or (source in finished and source.exception() is not None)
            if not client_closed:
                window.human_wait_ms = min(600000,round(1000*(time.monotonic()-window.attached_at)))
            return ws
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            if writer:
                writer.close()
            if ws.prepared:
                await ws.close()
            window.attached = False
            if client_closed:
                await self.stop(window)

    async def asset(self,request):
        if not self.authorized(request):
            return response({'error':'forbidden'},403)
        name = request.match_info['name']
        if not re.fullmatch(r'(?:core|vendor)/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_.-]+\.js',name):
            raise web.HTTPNotFound(headers=HEADERS)
        path = (ASSETS/name).resolve()
        if not path.is_relative_to(ASSETS.resolve()) or not path.is_file():
            raise web.HTTPNotFound(headers=HEADERS)
        return web.FileResponse(path,headers={**HEADERS,'Content-Type':'text/javascript'})


def valid_proof(value):
    return isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9_-]{43}',value) is not None


def viewer_challenge(verifier):
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode('ascii')).digest()).rstrip(b'=').decode('ascii')


async def json_body(request):
    if request.content_type!='application/json' or request.content_length is None or request.content_length>1024:
        raise web.HTTPBadRequest(headers=HEADERS)
    try:
        value = await request.json()
        if isinstance(value,dict):
            return value
    except (UnicodeError,ValueError):
        pass
    raise web.HTTPBadRequest(headers=HEADERS)


def register_window(application,store,active):
    owner = uuid_value(os.environ.get('COINGLASS_OPERATOR_ID'))
    origin = https_origin(os.environ.get('COINGLASS_OPERATOR_ORIGIN') or os.environ.get('RENDER_EXTERNAL_URL',''))
    app_origin = https_origin(os.environ.get('COINGLASS_APP_ORIGIN',''))
    manager = WindowManager(Database(store),owner,origin,app_origin,active and
                            os.environ.get('COINGLASS_OPERATOR_DESKTOP_ENABLED','').lower()=='true')
    application.cleanup_ctx.append(manager.lifecycle)
    async def static(request):
        names = {'':'operator_viewer.html','/viewer.js':'operator_viewer.js','/viewer.css':'operator_viewer.css'}
        return web.FileResponse(HERE/names[request.path[len(PREFIX):]],headers=HEADERS)
    for suffix in ('','/viewer.js','/viewer.css'):
        application.router.add_get(PREFIX+suffix,static)
    application.router.add_post(PREFIX+'/admit',manager.admit)
    application.router.add_post(PREFIX+'/verify',manager.verify)
    application.router.add_get(PREFIX+'/display',manager.display)
    application.router.add_get(PREFIX+'/assets/{name:.*}',manager.asset)
    return manager
