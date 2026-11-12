import asyncio
from copy import deepcopy
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

 def test_default_off_and_untrusted_endpoint_fail_closed(self):
  self.assertIsNone(t.configuration({}))
  for url in ('https://evil.example'+t.PATH,'https://hl-testnet-check-yoyo.onrender.com/else',
              'http://127.0.0.1:9999'+t.PATH):
   with self.assertRaises(ValueError):t.configuration(dict(ENV,POSITION_RECOVERY_ENDPOINT=url))
  self.assertIsNotNone(t.configuration(dict(ENV,POSITION_RECOVERY_SOFTWARE_ACCEPTANCE='true',
    POSITION_RECOVERY_ENDPOINT='http://127.0.0.1:9999'+t.PATH)))

if __name__=='__main__':unittest.main()
