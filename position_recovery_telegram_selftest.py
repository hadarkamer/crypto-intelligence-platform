import asyncio
import ast
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock,Mock,patch
import position_recovery_telegram as t
from telegram.error import TimedOut,RetryAfter

ENV=dict(POSITION_RECOVERY_ENABLED='true',POSITION_RECOVERY_API_SECRET='a'*64,
 POSITION_RECOVERY_USER_IDS='7',POSITION_RECOVERY_CHAT_ID='7',
 POSITION_RECOVERY_ENDPOINT='https://hl-testnet-check-yoyo.onrender.com'+t.PATH)

def notice(phase='WAITING'):
 return dict(status='CLAIMED',id='a'*24,event='b'*24,revision=1,claim='c'*32,chat='7',
  symbol='HYPE',role='short_account',current_phase=phase,message_id=None,
  result=dict(phase=phase,round=1,retryable=True,facts=dict(closed=False,observed_at_ms=1000000,
   exits={'STOP':'UNKNOWN','TAKE_PROFIT':'MISSING'})))

class TelegramTests(unittest.IsolatedAsyncioTestCase):
 def webhook(self):
  # Exercise the actual HTTP handler without importing main's collectors.
  module=ast.parse(Path(__file__).with_name('main.py').read_text())
  handler=next(n for n in module.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='telegram_webhook')
  from telegram import Update
  from aiohttp import web
  namespace=dict(Update=Update,web=web,PROCESSED_UPDATE_IDS={51},PROCESSED_UPDATE_ORDER=[51],MAX_PROCESSED_UPDATE_IDS=100)
  exec(compile(ast.Module(body=[handler],type_ignores=[]),'main.py','exec'),namespace)
  return namespace['telegram_webhook'],namespace

 def request(self):
  payload=dict(update_id=51,callback_query=dict(id='callback-51',chat_instance='instance',
   data='pr:'+('b'*24)+':1:manual',**{'from':dict(id=7,is_bot=False,first_name='Trader')},
   message=dict(message_id=31,date=1000000,chat=dict(id=7,type='private'))))
  bot=SimpleNamespace(answer_callback_query=AsyncMock(),edit_message_reply_markup=AsyncMock())
  app=SimpleNamespace(bot=bot,create_task=Mock())
  return SimpleNamespace(json=AsyncMock(return_value=payload),app={'bot_app':app},
   headers={'X-Telegram-Bot-Api-Secret-Token':t.webhook_secret(ENV)}),app

 async def test_forged_telegram_origin_cannot_record_authorized_user_choice(self):
  handler,_=self.webhook();request,app=self.request()
  for headers in ({},{'X-Telegram-Bot-Api-Secret-Token':'wrong'},
                  {'X-Telegram-Bot-Api-Secret-Token':ENV['POSITION_RECOVERY_API_SECRET']}):
   request.headers=headers
   with patch.dict(t.os.environ,ENV,clear=True),patch.object(t,'record') as record:
    self.assertEqual((await handler(request)).status,403);record.assert_not_called()
  app.create_task.assert_not_called()

 async def test_webhook_waits_for_saved_choice_before_http_ack(self):
  handler,namespace=self.webhook();request,app=self.request()
  started,finish=asyncio.Event(),asyncio.Event()
  async def save(*args):
   started.set();await finish.wait();return {'status':'RECORDED'}
  with patch.dict(t.os.environ,ENV,clear=True),patch.object(t.asyncio,'to_thread',side_effect=save):
   task=asyncio.create_task(handler(request));await started.wait()
   self.assertFalse(task.done());finish.set();response=await task
  self.assertEqual(response.status,200);app.create_task.assert_not_called()
  self.assertEqual(namespace['PROCESSED_UPDATE_ORDER'],[51])

 async def test_unconfirmed_choice_is_retried_even_with_cached_update_id(self):
  handler,_=self.webhook();request,app=self.request()
  with patch.dict(t.os.environ,ENV,clear=True),patch.object(t,'record',side_effect=[OSError('unavailable'),{'status':'RECORDED'},{'status':'STALE_OR_ALREADY_APPLIED'}]) as record:
   self.assertEqual((await handler(request)).status,503)
   self.assertEqual((await handler(request)).status,200)
   self.assertEqual((await handler(request)).status,200)
   self.assertEqual(record.call_count,3)
  app.create_task.assert_not_called()

 async def test_saved_choice_popup_failure_does_not_request_redelivery(self):
  handler,_=self.webhook();request,app=self.request()
  app.bot.answer_callback_query.side_effect=TimedOut()
  with patch.dict(t.os.environ,ENV,clear=True),patch.object(t,'record',return_value={'status':'RECORDED'}) as record:
   self.assertEqual((await handler(request)).status,200);record.assert_called_once()
  app.create_task.assert_not_called()

 async def test_restart_preserves_pending_updates_and_successor_webhook(self):
  module=ast.parse(Path(__file__).with_name('main.py').read_text())
  main=next(n for n in module.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='main')
  start=next(i for i,n in enumerate(main.body) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='webhook_url' for x in n.targets))
  body=main.body[start:start+4]
  final=next(n.finalbody for n in main.body if isinstance(n,ast.Try) and any(isinstance(c,ast.While) for c in n.body))
  cleanup=next(n for n in final if isinstance(n,ast.If) and ast.unparse(n.test)=='not recovery_enabled')
  for enabled in ('true','false'):
   bot=SimpleNamespace(delete_webhook=AsyncMock(),set_webhook=AsyncMock())
   fn=ast.AsyncFunctionDef(name='lifecycle',args=ast.arguments(posonlyargs=[],args=[],kwonlyargs=[],kw_defaults=[],defaults=[]),body=body+[cleanup],decorator_list=[])
   namespace=dict(os=t.os,bot_app=SimpleNamespace(bot=bot),PUBLIC_URL='https://source.example',position_recovery_telegram=t)
   exec(compile(ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[])),'main.py','exec'),namespace)
   with patch.dict(t.os.environ,dict(ENV,POSITION_RECOVERY_ENABLED=enabled),clear=True):await namespace['lifecycle']()
   extra={'secret_token':t.webhook_secret(ENV)} if enabled=='true' else {}
   bot.set_webhook.assert_awaited_once_with(url='https://source.example/telegram',drop_pending_updates=enabled!='true',**extra)
   self.assertEqual(bot.delete_webhook.await_count,0 if enabled=='true' else 2)

 async def test_private_authorization_and_duplicate_result(self):
  q=SimpleNamespace(data='pr:'+('b'*24)+':1:auto',answer=AsyncMock(),edit_message_reply_markup=AsyncMock())
  u=SimpleNamespace(effective_user=SimpleNamespace(id=7),effective_chat=SimpleNamespace(id=7,type='private'),callback_query=q)
  with patch.dict(t.os.environ,ENV,clear=True),patch.object(t,'record',return_value={'status':'RECORDED'}) as record:
   await t.callback(u,None);self.assertEqual(record.call_args.args[0]['choice'],'auto');q.edit_message_reply_markup.assert_awaited_once()
   u.effective_user.id=8;await t.callback(u,None);self.assertEqual(record.call_count,1)
   u.effective_user.id=7;u.effective_chat.type='group';await t.callback(u,None);self.assertEqual(record.call_count,1)

 async def test_delivery_failure_is_unknown_not_success_and_no_resend_loop(self):
  bot=SimpleNamespace(send_message=AsyncMock(side_effect=TimedOut()),edit_message_text=AsyncMock())
  with patch.object(t,'record',side_effect=[notice(),{'status':'RECORDED'}]) as record:
   await t.deliver_once(bot,env=ENV)
   self.assertEqual(record.call_args.args[0]['status'],'UNKNOWN');self.assertEqual(bot.send_message.await_count,1)

 async def test_explicit_rejection_is_recorded_for_bounded_retry(self):
  bot=SimpleNamespace(send_message=AsyncMock(side_effect=RetryAfter(30)))
  with patch.object(t,'record',side_effect=[notice(),{'status':'RECORDED'}]) as record:
   await t.deliver_once(bot,env=ENV);self.assertEqual(record.call_args.args[0]['status'],'REJECTED')

 async def test_late_result_edits_existing_message(self):
  n=notice('RESOLVED');n['message_id']=31
  bot=SimpleNamespace(send_message=AsyncMock(),edit_message_text=AsyncMock())
  with patch.object(t,'record',side_effect=[n,{'status':'RECORDED'}]) as record:
   await t.deliver_once(bot,env=ENV);bot.send_message.assert_not_awaited()
   self.assertEqual(bot.edit_message_text.call_args.kwargs['message_id'],31)
   self.assertEqual(record.call_args.args[0]['message_id'],31)

 def test_unknown_wording_no_false_absence_and_buttons_only_when_waiting(self):
  n=notice();text,buttons=t.view(n)
  self.assertIn('לא ניתן לקבוע',text);self.assertNotIn('SL: חסר',text)
  self.assertEqual(len(buttons.inline_keyboard[0]),2)
  for phase in ('RECOVERING','RESUME_REQUESTED','MANUAL','RESOLVED'):
   n['current_phase']=phase;self.assertIsNone(t.view(n)[1])
  n=notice();n['result']['retryable']=False
  self.assertEqual(len(t.view(n)[1].inline_keyboard[0]),1)

 def test_entry_refusal_has_reason_and_no_action_buttons(self):
  n=notice('ENTRY_REFUSED');n['result']['reason']='MARKET_OUTCOME_UNKNOWN'
  n['result']['facts']={'observed_at_ms':1000000}
  text,buttons=t.view(n)
  self.assertIn('האות החדש לא נפתח',text);self.assertIn('תוצאת הוראה קודמת עדיין אינה ידועה',text)
  self.assertIsNone(buttons)

 def test_default_off_and_untrusted_endpoint_fail_closed(self):
  self.assertIsNone(t.configuration({}))
  for url in ('https://evil.example'+t.PATH,'https://hl-testnet-check-yoyo.onrender.com/else',
              'http://127.0.0.1:9999'+t.PATH):
   with self.assertRaises(ValueError):t.configuration(dict(ENV,POSITION_RECOVERY_ENDPOINT=url))
  self.assertIsNotNone(t.configuration(dict(ENV,POSITION_RECOVERY_SOFTWARE_ACCEPTANCE='true',
    POSITION_RECOVERY_ENDPOINT='http://127.0.0.1:9999'+t.PATH)))

if __name__=='__main__':unittest.main()
