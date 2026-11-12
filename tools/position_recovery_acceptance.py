"""Opt-in local Telegram acceptance: real private UI + PostgreSQL + software venue.

No production application is started. The sole existing runtime/service executes
against SoftwareExchange; the Telegram component is loaded from its own checkout.
Run with --help. A bot token is requested with getpass, never argv or a log.
"""
import argparse
import asyncio
from copy import deepcopy
from decimal import Decimal
import getpass
import importlib.util
import logging
import os
from pathlib import Path
import secrets
import socket
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from wsgiref.simple_server import make_server,WSGIRequestHandler

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--telegram-source',type=Path,required=True)
    parser.add_argument('--expected-test-bot',help='Exact newly created test bot username (without @)')
    parser.add_argument('--private-user-id',type=int)
    parser.add_argument('--repeat-failure',action='store_true')
    parser.add_argument('--dry-run',action='store_true',help='Validate preparation only; NOT interactive acceptance')
    args=parser.parse_args()
    from hl_testnet_runtime.postgres_journal import PostgresJournal
    url=os.environ.get('HL_JOURNAL_CI_URL','')
    PostgresJournal.for_ci(url) # Enforces loopback, hl_journal_ci and no production SSL endpoint.
    if not args.dry_run and (not args.expected_test_bot or not args.private_user_id or args.private_user_id<=0):
        parser.error('Exact separate test bot username and private user ID are required')
    module_path=args.telegram_source.resolve()/'position_recovery_telegram.py'
    spec=importlib.util.spec_from_file_location('acceptance_telegram',module_path)
    telegram=importlib.util.module_from_spec(spec);spec.loader.exec_module(telegram)
    from hl_testnet_runtime import test_experimental_live_runtime as support
    from hl_testnet_runtime.test_experimental_execution_runtime import SoftwareExchange
    from hl_testnet_runtime.experimental_live_provider import LiveEvidenceProvider
    from hl_testnet_runtime.experimental_live_dispatch import LiveDispatchPort,DefinitelyNotSubmitted,LiveDispatchError
    from hl_testnet_runtime.experimental_live_service import ConnectionService
    from hl_testnet_runtime.test_approved_alert_lifecycle import alert
    from hl_testnet_runtime import position_recovery_http as api
    class Harness(support.RuntimeContract):real_pg=True
    harness=Harness();harness.Provider=LiveEvidenceProvider;harness.Port=LiveDispatchPort
    harness.venue=SoftwareExchange(int(time.time()*1000));harness.reservations=[]
    support.T=harness.venue.now() # Synthetic fixture release uses this clock only.
    harness.make_worker(harness.venue.now()-120000)
    harness.release['entry_expires_at_ms']=harness.venue.now()+3600000
    message=alert(symbol='SOL',side='SHORT',approved_ms=(harness.venue.now()//60000)*60000,cycle='private-software-acceptance')
    harness.start(message);cid=message['occurrence_id']
    # Approved alerts use a resting order: create a real synthetic fill first.
    oid=harness.venue.oid('ENTRY');harness.venue.fill(oid,harness.venue.orders[oid]['view']['wire_order']['s'])
    original_send=harness.port.send
    def fail_exit(request,*,admission=None):
        if request['proposal']['operation'] in ('CREATE_EXIT','AMEND_EXIT'):
            claimed=harness.store.claim_transport(request,'c'*32)
            raise DefinitelyNotSubmitted(claimed,LiveDispatchError('FINAL_EVIDENCE_EXPIRED'))
        return original_send(request,admission=admission)
    harness.port.send=fail_exit
    for _ in range(5):harness.venue.t+=1001;harness.cycle(False)
    row=harness.store.load()['position_recovery'][cid]
    if row['phase']!='WAITING':raise RuntimeError('SYNTHETIC_WAITING_SCENARIO_FAILED')
    if not args.repeat_failure:harness.port.send=original_send
    if args.dry_run:
        print('PREPARATION_PASSED: real PostgreSQL, synthetic position, bounded round, waiting decision. UI NOT TESTED.')
        return
    token=getpass.getpass('Separate TEST bot token (hidden; kept in memory): ')
    env=dict(POSITION_RECOVERY_ENABLED='true',POSITION_RECOVERY_SOFTWARE_ACCEPTANCE='true',
        POSITION_RECOVERY_API_SECRET=secrets.token_hex(32),POSITION_RECOVERY_USER_IDS=str(args.private_user_id),
        POSITION_RECOVERY_CHAT_ID=str(args.private_user_id))
    service=ConnectionService(harness.worker,key='9'*64,release_loader=lambda:deepcopy(harness.release))
    stop=threading.Event()
    class QuietHandler(WSGIRequestHandler):
        def log_message(self,*args):pass
    def application(environ,start_response):
        if environ.get('PATH_INFO')!=api.PATH:
            start_response('404 Not Found',[]);return [b'']
        return api.application(environ,start_response,store=harness.store,
            clock=lambda:int(time.time()*1000),env=env,wake=service._wake)
    server=make_server('127.0.0.1',0,application,handler_class=QuietHandler)
    env['POSITION_RECOVERY_ENDPOINT']='http://127.0.0.1:'+str(server.server_port)+api.PATH
    from telegram.ext import Application,CommandHandler
    from telegram.request import HTTPXRequest
    original_tick=service.tick
    def tick():
        harness.venue.t=max(harness.venue.t,int(time.time()*1000))
        result=original_tick();telegram.tick();return result
    service.tick=tick
    threads=[]
    async def start(app):
        me=await app.bot.get_me()
        if me.username.lower()!=args.expected_test_bot.lstrip('@').lower():
            raise RuntimeError('TEST_BOT_IDENTITY_MISMATCH')
        telegram.bind(app,asyncio.get_running_loop())
        for target in (server.serve_forever,lambda:service.run(stop,interval_seconds=1)):
            thread=threading.Thread(target=target,daemon=True);thread.start();threads.append(thread)
        print('TEST READY: use the separate private Telegram chat. Ctrl-C stops all local services.')
    async def restart(update,context):
        if (update.effective_chat.type!='private' or update.effective_chat.id!=args.private_user_id
                or update.effective_user.id!=args.private_user_id):return
        with service._tick_lock:
            harness.restart();service.runtime=harness.worker
            current=harness.store.load()['position_recovery'][cid]
        await update.message.reply_text('מנהל הבדיקה נטען מחדש מהמסד. מצב: '+current['phase']+
                                       ' · סבב: '+str(current['round']))
    async def shutdown(app):
        stop.set();service._wake.set()
        if threads:server.shutdown()
        server.server_close()
        for thread in threads:thread.join(timeout=5)
    async def error(update,context):print('TEST_UI_ERROR: inspect state; no secret or raw exception is logged.')
    logging.getLogger('httpx').setLevel(logging.CRITICAL)
    logging.getLogger('httpcore').setLevel(logging.CRITICAL)
    logging.getLogger('telegram').setLevel(logging.CRITICAL)
    app=(Application.builder().token(token).request(HTTPXRequest(httpx_kwargs={'trust_env':False}))
        .get_updates_request(HTTPXRequest(httpx_kwargs={'trust_env':False}))
        .post_init(start).post_shutdown(shutdown).build())
    app.add_handler(CommandHandler('restart',restart));app.add_error_handler(error)
    # No signing capability exists in this harness. DNS additionally restricts
    # external requests to Telegram; the record endpoint and DB stay loopback.
    original_dns=socket.getaddrinfo
    def dns(host,*a,**kw):
        name=host.decode() if isinstance(host,bytes) else host
        if name not in ('api.telegram.org','127.0.0.1','localhost'):
            raise OSError('SOFTWARE_ACCEPTANCE_HOST_DENIED')
        return original_dns(host,*a,**kw)
    with patch.dict(os.environ,env,clear=True),patch('socket.getaddrinfo',side_effect=dns),\
         patch('hyperliquid_testnet_executor._wallet',side_effect=AssertionError('NO_SIGNER')),\
         patch('hyperliquid_testnet_executor._signed_body',side_effect=AssertionError('NO_SIGNER')):
        app.run_polling(drop_pending_updates=True)

if __name__=='__main__':
    try:main()
    except KeyboardInterrupt:pass
    except Exception:
        # Third-party exceptions may include a bot token or request URL.
        print('ACCEPTANCE_SETUP_OR_RUNTIME_FAILED: no credentials are logged.',file=sys.stderr)
        raise SystemExit(1) from None
