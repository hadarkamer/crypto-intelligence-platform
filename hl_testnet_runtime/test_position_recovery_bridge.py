"""Real loopback HTTP + HMAC + PostgreSQL + source callback + sole runtime.

Telegram transport is a test double. This is NOT interactive Telegram acceptance.
Set RECOVERY_TELEGRAM_SOURCE to the separately versioned source checkout.
"""
import asyncio
import importlib.util
import os
from pathlib import Path
import socket
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock,patch
from wsgiref.simple_server import make_server,WSGIRequestHandler
from . import test_experimental_live_runtime as support, position_recovery_http as api
from .test_position_recovery import RecoveryContract

_connect=socket.create_connection

@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL') and os.environ.get('RECOVERY_TELEGRAM_SOURCE'),
    'requires real loopback PostgreSQL and the matched Telegram checkout')
class PostgresBridgeTests(unittest.TestCase):
    real_pg=True
    make_worker=support.RuntimeContract.make_worker
    cycle=support.RuntimeContract.cycle
    start=support.RuntimeContract.start
    restart=support.RuntimeContract.restart
    unsent=RecoveryContract.unsent
    incident=RecoveryContract.incident
    row=RecoveryContract.row
    wait=RecoveryContract.wait

    def setUp(self):
        support.RuntimeContract.setUp(self);self.wait()
        path=Path(os.environ['RECOVERY_TELEGRAM_SOURCE'])/'position_recovery_telegram.py'
        spec=importlib.util.spec_from_file_location('matched_recovery_telegram',path)
        self.t=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.t)
        self.env=dict(POSITION_RECOVERY_ENABLED='true',POSITION_RECOVERY_SOFTWARE_ACCEPTANCE='true',
            POSITION_RECOVERY_API_SECRET='a'*64,POSITION_RECOVERY_USER_IDS='7',POSITION_RECOVERY_CHAT_ID='7')
        self.wake=threading.Event()
        class Quiet(WSGIRequestHandler):
            def log_message(self,*args):pass
        def app(e,s):return api.application(e,s,store=self.store,clock=lambda:int(time.time()*1000),env=self.env,wake=self.wake)
        self.server=make_server('127.0.0.1',0,app,handler_class=Quiet)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.stop_server)
        self.env['POSITION_RECOVERY_ENDPOINT']='http://127.0.0.1:'+str(self.server.server_port)+api.PATH
        def local(address,*args,**kwargs):
            if address!=('127.0.0.1',self.server.server_port):raise AssertionError('ONLY_LOCAL_RECORD_ENDPOINT')
            return _connect(address,*args,**kwargs)
        p=patch('socket.create_connection',side_effect=local);p.start();self.addCleanup(p.stop)
        p=patch.dict(os.environ,self.env);p.start();self.addCleanup(p.stop)
        self.bot=SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=31)),edit_message_text=AsyncMock())

    def stop_server(self):
        self.server.shutdown();self.server.server_close();self.thread.join(timeout=2)

    def update(self,choice='auto',user=7):
        return SimpleNamespace(effective_user=SimpleNamespace(id=user),effective_chat=SimpleNamespace(id=user,type='private'),
            callback_query=SimpleNamespace(data=f"pr:{self.row()['event_id']}:1:{choice}",answer=AsyncMock(),edit_message_reply_markup=AsyncMock()))

    def test_result_choice_restart_owner_execution_and_final_presentation(self):
        asyncio.run(self.t.deliver_once(self.bot,env=self.env))
        self.assertEqual(self.row()['notices'][0]['delivery'],'SENT')
        self.assertIsNotNone(self.bot.send_message.call_args.kwargs['reply_markup'])
        before=len(self.store.load()['requests']);update=self.update()
        asyncio.run(self.t.callback(update,None));asyncio.run(self.t.callback(update,None))
        self.assertEqual(self.row()['phase'],'RESUME_REQUESTED');self.assertTrue(self.wake.is_set())
        self.assertEqual(len(self.store.load()['requests']),before)
        self.restart()
        for _ in range(4):self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'RESOLVED');self.assertEqual(self.row()['round'],2)
        for _ in range(3):asyncio.run(self.t.deliver_once(self.bot,env=self.env))
        self.assertEqual(self.bot.send_message.await_count,2)
        self.assertIsNone(self.bot.send_message.call_args.kwargs['reply_markup'])
        self.assertIn('טופל',self.bot.send_message.call_args.kwargs['text'])
        self.assertTrue(all(n['delivery']=='SENT' for n in self.row()['notices']))
        self.assertEqual(len(self.venue.requests),3)
        before=len(self.store.load()['requests']);asyncio.run(self.t.callback(update,None))
        self.assertEqual(len(self.store.load()['requests']),before)

    def test_private_manual_and_unauthorized_click_persist_without_dispatch(self):
        before=self.row();asyncio.run(self.t.callback(self.update(user=8),None))
        self.assertEqual(self.row(),before);self.assertFalse(self.wake.is_set())
        asyncio.run(self.t.callback(self.update('manual'),None));self.restart()
        before=len(self.store.load()['requests']);self.venue.t+=1001;self.cycle(False)
        self.assertEqual(self.row()['phase'],'MANUAL');self.assertEqual(len(self.store.load()['requests']),before)
        self.assertTrue(self.store.load()['trades'][self.cid]['manual_management'])
