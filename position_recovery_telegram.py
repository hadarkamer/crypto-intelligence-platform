"""Private record-only position choices and durable result delivery.

Uses the existing source forwarder's cadence and Telegram application's loop.
No exchange imports, signing keys, execution calls or additional worker.
"""
import asyncio
import hashlib
import hmac
import json
import os
import re
import time
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
from telegram.error import BadRequest, RetryAfter, Forbidden

PATH = '/internal/position-recovery/v1'
_BINDING = None
_PENDING = None


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('RECOVERY_REDIRECT_REFUSED')


def configuration(env):
    if env.get('POSITION_RECOVERY_ENABLED') != 'true': return None
    secret = env.get('POSITION_RECOVERY_API_SECRET', '')
    endpoint = env.get('POSITION_RECOVERY_ENDPOINT', '')
    parts = urlsplit(endpoint)
    users = {u.strip() for u in env.get('POSITION_RECOVERY_USER_IDS','').split(',') if u.strip().isdecimal()}
    chat = env.get('POSITION_RECOVERY_CHAT_ID', '')
    local = env.get('POSITION_RECOVERY_SOFTWARE_ACCEPTANCE') == 'true'
    valid_host = (parts.hostname == '127.0.0.1' and parts.scheme == 'http' if local else
                  parts.hostname == 'hl-testnet-check-yoyo.onrender.com' and parts.scheme == 'https' and parts.port in (None,443))
    if (not re.fullmatch('[a-f0-9]{64}',secret) or not users or chat not in users
            or not valid_host or parts.path != PATH or parts.query or parts.fragment or parts.username or parts.password):
        raise ValueError('RECOVERY_CONFIGURATION_REQUIRED')
    return dict(secret=secret, endpoint=endpoint, users=users, chat=chat)


def record(command, env=None):
    cfg = configuration(os.environ if env is None else env)
    if cfg is None: raise ValueError('RECOVERY_DISABLED')
    body = json.dumps(command,separators=(',',':'),allow_nan=False).encode()
    stamp = str(int(time.time()))
    sig = hmac.new(cfg['secret'].encode(),PATH.encode()+b'\n'+stamp.encode()+b'\n'+body,hashlib.sha256).hexdigest()
    req = Request(cfg['endpoint'], data=body, headers={'Content-Type':'application/json',
        'X-Recovery-Timestamp':stamp, 'X-Recovery-Signature':sig}, method='POST')
    with build_opener(NoRedirect()).open(req,timeout=5) as response:
        raw = response.read(16385)
        if response.status != 200 or len(raw)>16384: raise ValueError('RECOVERY_RECORDING_UNAVAILABLE')
    value = json.loads(raw)
    if not isinstance(value,dict): raise ValueError('RECOVERY_RECORD_INVALID')
    return value


def view(notice):
    result = notice['result']; facts = result['facts']
    account = 'לונג' if notice['role']=='long_account' else 'שורט'
    lines = [f"{notice['symbol']} · חשבון {account} · סבב {result['round']}"]
    if result['phase'] in ('MANUAL','RESUME_REQUESTED'):
        lines.append('נשמרה בחירה בניהול ידני. הבוט לא ייזום פעולות נוספות לפוזיציה.' if result['phase']=='MANUAL' else
                     'נשמרה בקשת סבב נוסף. מנהל העסקאות יבדוק את המצב העדכני.')
    elif result['phase']=='RESOLVED':
        lines.append('הפוזיציה נסגרה; אין צורך בהצבת יציאות.' if facts['closed'] else
                     'טופל: הוראות היציאה הנדרשות אומתו לאחר התקלה. אין צורך בפעולה מצד הסוחר.')
    else:
        lines.append('סבב ההשלמה הסתיים ללא השלמת הטיפול.')
        for leg,label in [('STOP','SL'),('TAKE_PROFIT','יעד רווח')]:
            lines.append({'PRESENT':f'{label}: קיים ותואם לפוזיציה.',
                          'MISSING':f'{label}: חסר בתצפית האחרונה.',
                          'UNKNOWN':f'{label}: לא ניתן לקבוע אם ההוראה התקבלה.'}[facts['exits'][leg]])
        if not result['retryable']:
            lines.append('נדרשת בדיקה של הנתונים או התנאים לפני ניסיון נוסף.')
    from datetime import datetime, timezone
    stamp=facts.get('observed_at_ms')
    lines.append('זמן התצפית (UTC): '+(datetime.fromtimestamp(stamp/1000,timezone.utc).strftime('%d/%m %H:%M:%S') if stamp else 'לא זמין'))
    buttons=[]
    if result['phase']=='WAITING' and notice['current_phase']=='WAITING':
        prefix=f"pr:{notice['event']}:{result['round']}:"
        if result['retryable']: buttons.append(InlineKeyboardButton('המשך טיפול אוטומטי',callback_data=prefix+'auto'))
        buttons.append(InlineKeyboardButton('אני ממשיך ידנית',callback_data=prefix+'manual'))
    return '\n'.join(lines), InlineKeyboardMarkup([buttons]) if buttons else None


async def deliver_once(bot, *, env=None):
    env=dict(os.environ if env is None else env)
    if configuration(env) is None: return
    notice=await asyncio.to_thread(record,{'op':'claim'},env)
    if notice['status']!='CLAIMED': return
    status='UNKNOWN'; message_id=None
    try:
        text,markup=view(notice)
        if notice['message_id']:
            await bot.edit_message_text(chat_id=notice['chat'],message_id=notice['message_id'],
                                        text=text,reply_markup=markup)
            message_id=notice['message_id']
        else:
            message=await bot.send_message(chat_id=notice['chat'],text=text,reply_markup=markup)
            message_id=message.message_id
        status='SENT'
    except (RetryAfter, Forbidden):
        status='REJECTED'
    except BadRequest as exc:
        if notice['message_id'] and 'message is not modified' in str(exc).lower():
            status='SENT';message_id=notice['message_id']
        else: status='REJECTED'
    except Exception:
        pass  # Response loss remains unknown: never resend a possibly delivered message.
    await asyncio.to_thread(record,dict(op='ack',id=notice['id'],claim=notice['claim'],
        revision=notice['revision'],status=status,message_id=message_id),env)


async def callback(update, context):
    cfg=configuration(os.environ)
    user,chat,q=update.effective_user,update.effective_chat,update.callback_query
    if (cfg is None or not user or not chat or chat.type!='private'
            or chat.id!=user.id or str(user.id) not in cfg['users']):
        await q.answer('הפעולה אינה מורשית.',show_alert=True);return
    match=re.fullmatch(r'pr:([a-f0-9]{24}):([1-9][0-9]{0,5}):(auto|manual)',q.data or '')
    if not match:
        await q.answer('הבחירה אינה תקפה.',show_alert=True);return
    try:
        answer=await asyncio.to_thread(record,dict(op='choose',event=match[1],round=int(match[2]),
            choice=match[3],actor=str(user.id),chat=str(chat.id)))
    except Exception:
        await q.answer('לא התקבל אישור שמירה. אפשר לנסות שוב; לא תיווצר בקשה כפולה.',show_alert=True);return
    messages={'RECORDED':'הבחירה נשמרה.', 'REVIEW_REQUIRED':'נדרשת בדיקה לפני ניסיון נוסף.',
              'STALE_OR_ALREADY_APPLIED':'הבחירה כבר טופלה או שהסבב הסתיים.',
              'POSITION_NOT_ACTIVE':'הפוזיציה אינה פעילה.'}
    await q.answer(messages.get(answer['status'],'הבחירה לא התקבלה.'),show_alert=True)
    if answer['status'] in ('RECORDED','STALE_OR_ALREADY_APPLIED','POSITION_NOT_ACTIVE'):
        await q.edit_message_reply_markup(reply_markup=None)


def bind(app, loop):
    global _BINDING
    app.add_handler(CallbackQueryHandler(callback,pattern=r'^pr:'))
    _BINDING=(app.bot,loop)


def tick():
    """Called by the existing source loop; a Telegram timeout cannot delay it."""
    global _PENDING
    if _BINDING is None or os.environ.get('POSITION_RECOVERY_ENABLED')!='true': return
    if _PENDING is not None and not _PENDING.done(): return
    bot,loop=_BINDING
    _PENDING=asyncio.run_coroutine_threadsafe(deliver_once(bot),loop)
    # Consume exceptions without credential-bearing third-party error text.
    _PENDING.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
