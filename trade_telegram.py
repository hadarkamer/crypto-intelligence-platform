"""Private, read-only Telegram views of bot-owned Testnet trades.

Fetches a private read-only projection from the separate Testnet service.
Stored observations are historical; their timestamp is always displayed.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from html import escape
import json
import os
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler

IL = ZoneInfo('Asia/Jerusalem')
PAGE_SIZE = 12
ROLES = {'L': 'long_account', 'S': 'short_account'}
LABELS = {'L': 'LONG', 'S': 'SHORT'}
REPORT_URL = 'https://hl-testnet-check-yoyo.onrender.com/internal/testnet-trades/v1'


def authorized(update, env=None):
    env = os.environ if env is None else env
    user = update.effective_user
    chat = update.effective_chat
    uid = env.get('HL_TESTNET_REPORT_TELEGRAM_USER_ID', '')
    return bool(uid.isdecimal() and user and chat and chat.type == 'private'
                and str(user.id) == uid and chat.id == user.id)


def load_trades(env=None):
    """Fetch a read-only projection from the separate Testnet service."""
    env = dict(os.environ if env is None else env)
    token = env.get('HL_TESTNET_REPORT_API_TOKEN', '')
    if len(token) < 32 or len(token) > 256:
        raise ValueError('REPORT_API_TOKEN_UNAVAILABLE')
    request = Request(REPORT_URL, headers={'X-Trade-Report-Token': token,
                                          'Accept':'application/json'})
    with urlopen(request, timeout=5) as response:
        if response.status != 200:
            raise ValueError('REPORT_API_UNAVAILABLE')
        raw = response.read(262145)
    if len(raw) > 262144:
        raise ValueError('REPORT_TOO_LARGE')
    result = json.loads(raw)
    if (not isinstance(result, dict) or set(result) != set(ROLES)
            or any(not isinstance(result[key], list) or len(result[key]) > 1024
                   or any(not isinstance(row, dict) or row.get('account_role') != ROLES[key]
                          for row in result[key]) for key in ROLES)):
        raise ValueError('REPORT_INVALID')
    return result


def when(ms, *, day=False):
    if ms is None:
        return '—'
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(IL)
    return dt.strftime('%d/%m %H:%M' if day else '%H:%M')


def price(value):
    if value is None:
        return '—'
    return str(value)


def status(row):
    if row['closure_verified']:
        return 'סגורה ✓'
    if Decimal(row['remaining_quantity']) != 0:
        return 'פתוחה' if not row['issues'] else 'פתוחה · בדיקה'
    return 'סגירה בבדיקה'


def selection(trades, role, kind):
    keys = (role,) if role in ROLES else ('L', 'S')
    return [(key, row) for key in keys for row in trades[key]
            if kind == 'all' or (row['closure_verified'] if kind == 'closed'
                                 else not row['closure_verified'])]


def list_view(trades, role='A', kind='open', page=0):
    if role not in ('A', *ROLES) or kind not in ('open', 'closed', 'all'):
        raise ValueError('INVALID_REPORT_FILTER')
    rows = selection(trades, role, kind)
    pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(0, page), pages - 1)
    start = page * PAGE_SIZE
    title = {'open': 'פתוחות / דרוש אימות', 'closed': 'סגורות ומאומתות',
             'all': 'כל העסקאות'}[kind]
    lines = [f'<b>עסקאות Testnet · {title}</b> ({page+1}/{pages})',
             '<pre>חשבון מטבע  כניסה       כמות       מצב</pre>']
    for key, row in rows[start:start+PAGE_SIZE]:
        code = row['symbol'][:6].ljust(6)
        label = 'סגור' if row['closure_verified'] else 'פתוח' if Decimal(row['remaining_quantity']) != 0 else 'בדיקה'
        lines.append('<pre>' + escape(f'{LABELS[key]:5} {code} {when(row["first_entry_at_ms"], day=True):11} '
                        f'{row["remaining_quantity"][:10]:10} {label}') + '</pre>')
    if not rows:
        lines.append('אין עסקאות ברשומות השמורות למסנן הזה.')
    lines.append('לחצי על עסקה לפירוט. הנתונים מבוססים על תצפית שמורה, לא על בדיקת בורסה חיה.')
    buttons = [
        [InlineKeyboardButton('פתוחות', callback_data=f'tr:{role}:open:0'),
         InlineKeyboardButton('סגורות', callback_data=f'tr:{role}:closed:0'),
         InlineKeyboardButton('הכול', callback_data=f'tr:{role}:all:0')],
    ]
    for key, row in rows[start:start+PAGE_SIZE]:
        buttons.append([InlineKeyboardButton(f'{LABELS[key]} {row["symbol"]} · {when(row["first_entry_at_ms"], day=True)}',
                       callback_data=f'td:{key}:{row["card_id"][:16]}')])
    nav = []
    if page:
        nav.append(InlineKeyboardButton('◀ הקודם', callback_data=f'tr:{role}:{kind}:{page-1}'))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton('הבא ▶', callback_data=f'tr:{role}:{kind}:{page+1}'))
    if nav:
        buttons.append(nav)
    return '\n'.join(lines), InlineKeyboardMarkup(buttons)


def detail(trades, key, prefix):
    if key not in ROLES or len(prefix) != 16:
        raise ValueError('INVALID_TRADE_ID')
    matches = [row for row in trades[key] if row['card_id'].startswith(prefix)]
    if len(matches) != 1:
        raise ValueError('TRADE_NOT_FOUND_OR_AMBIGUOUS')
    row = matches[0]
    planned = row['planned_prices']
    stamp = when(row['evidence_at_ms'], day=True)
    lines = [f'<b>{LABELS[key]} · {escape(row["symbol"])} · {status(row)}</b>',
             f'כניסה: {when(row["first_entry_at_ms"], day=True)} · סגירה: {when(row["last_exit_at_ms"], day=True)}',
             f'כמות שנפתחה: {escape(row["entry_quantity"])} · נסגרה: {escape(row["exit_quantity"])} · נותרה: {escape(row["remaining_quantity"])}',
             f'תכנון: כניסה {escape(price(planned.get("entry")))} · יעד {escape(price(planned.get("take_profit")))} · סטופ {escape(price(planned.get("stop")))}',
             f'ביצוע ממוצע: כניסה {escape(price(row["actual_entry_price"]))} · יציאה {escape(price(row["actual_exit_price"]))}',
             f'הגנה מאומתת בתצפית: {"כן" if row["protection_verified"] else "לא"} · הוראות פעילות שנצפו: {len(row["active_order_ids"])}',
             f'תצפית אחרונה: {stamp} (שעון ישראל)']
    if row['issues']:
        lines.append('חריגות: ' + escape(', '.join(row['issues'][:6])))
    if not row['closure_verified']:
        lines.append('מצב הסגירה דורש אימות; תצפית שמורה אינה מצב חי בבורסה.')
    return '\n'.join(lines)


def daily_view(trades, target, page=0):
    if not isinstance(target, date):
        raise ValueError('INVALID_DATE')
    included = {}
    for key in ('L', 'S'):
        included[key] = [r for r in trades[key] if any(ms is not None and
               datetime.fromtimestamp(ms/1000, timezone.utc).astimezone(IL).date() == target
               for ms in (r['first_entry_at_ms'], r['last_exit_at_ms']))]
    all_rows = [(key, row) for key in ('L','S') for row in included[key]]
    pages = max(1, (len(all_rows)+PAGE_SIZE-1)//PAGE_SIZE)
    page = min(max(0,page),pages-1)
    visible = all_rows[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
    lines = [f'<b>סיכום עסקאות Testnet · {target.isoformat()} · שעון ישראל</b> ({page+1}/{pages})']
    for key in ('L', 'S'):
        lines.append(f'\n<b>חשבון {LABELS[key]}</b>')
        lines.append('<pre>מטבע  כניסה       סגירה       כמות     מחיר כנ׳  מחיר יצ׳</pre>')
        own = [row for k,row in visible if k == key]
        for row in own:
            lines.append('<pre>' + escape(f'{row["symbol"][:6]:6} {when(row["first_entry_at_ms"], day=True):11} '
                f'{when(row["last_exit_at_ms"], day=True):11} {row["entry_quantity"][:9]:9} '
                f'{price(row["actual_entry_price"])[:10]:10} {price(row["actual_exit_price"])[:10]}') + '</pre>')
            lines.append(f'נותרה {escape(row["remaining_quantity"])} · {status(row)} · יעד {escape(price(row["planned_prices"].get("take_profit")))} · סטופ {escape(price(row["planned_prices"].get("stop")))}')
        if not included[key]:
            lines.append('אין כניסות או סגירות שנצפו ביום זה.')
    lines.append(f'\n{len(all_rows)} עסקאות עם כניסה או סגירה ביום זה. סגירה מוצגת כמאומתת רק כשאומתה ביומן.')
    nav=[]
    if page:
        nav.append(InlineKeyboardButton('◀ הקודם', callback_data=f'dy:{target.isoformat()}:{page-1}'))
    if page+1 < pages:
        nav.append(InlineKeyboardButton('הבא ▶', callback_data=f'dy:{target.isoformat()}:{page+1}'))
    return '\n'.join(lines), InlineKeyboardMarkup([nav]) if nav else None


async def _reply(update, text, markup=None):
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=markup)


async def trade_id(update, context):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != 'private' or chat.id != user.id:
        return
    await update.message.reply_text(f'מזהה המשתמש שלך בטלגרם: {user.id}. העתיקי אותו לשיחה שבה ביקשת להפעיל את דוחות העסקאות.')


async def command(update, context):
    if not authorized(update):
        await update.message.reply_text('תצוגת העסקאות זמינה רק בצ׳אט הפרטי המורשה.')
        return
    names = {'long_trades': ('L', 'all'), 'short_trades': ('S', 'all'),
             'open_trades': ('A', 'open'), 'closed_trades': ('A', 'closed')}
    try:
        trades = await asyncio.to_thread(load_trades)
        if update.message.text.split()[0].split('@')[0][1:] == 'daily_trades':
            if len(context.args) > 1:
                raise ValueError('INVALID_DATE')
            day = date.fromisoformat(context.args[0]) if context.args else datetime.now(IL).date()
            body, buttons = daily_view(trades, day)
            await _reply(update, body, buttons)
        else:
            key = update.message.text.split()[0].split('@')[0][1:]
            body, buttons = list_view(trades, *names[key])
            await _reply(update, body, buttons)
    except (ValueError, KeyError):
        await update.message.reply_text('התאריך צריך להיות YYYY-MM-DD, או שנתוני היומן אינם זמינים כרגע.')
    except Exception:
        await update.message.reply_text('יומן ה־Testnet אינו זמין כעת. לא נשלחה הוראת מסחר.')


async def callback(update, context):
    query = update.callback_query
    await query.answer()
    if not authorized(update):
        return
    try:
        trades = await asyncio.to_thread(load_trades)
        parts = query.data.split(':')
        if len(parts) == 4 and parts[0] == 'tr' and parts[3].isdecimal():
            body, buttons = list_view(trades, parts[1], parts[2], int(parts[3]))
            await _reply(update, body, buttons)
        elif len(parts) == 3 and parts[0] == 'td':
            await _reply(update, detail(trades, parts[1], parts[2]))
        elif len(parts) == 3 and parts[0] == 'dy' and parts[2].isdecimal():
            body, buttons = daily_view(trades, date.fromisoformat(parts[1]), int(parts[2]))
            await _reply(update, body, buttons)
    except Exception:
        await _reply(update, 'הנתונים אינם זמינים כעת. אפשר לנסות שוב דרך פקודת העסקאות.')


def register(app):
    app.add_handler(CommandHandler('trade_id', trade_id))
    for name in ('long_trades', 'short_trades', 'open_trades', 'closed_trades', 'daily_trades'):
        app.add_handler(CommandHandler(name, command))
    app.add_handler(CallbackQueryHandler(callback, pattern=r'^(tr|td|dy):'))
