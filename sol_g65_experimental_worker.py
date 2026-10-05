"""Prospective SOL g65/k49 notifications; one pending/open observation, no orders."""
from __future__ import annotations
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from math import isfinite
from pathlib import Path
import time
from zoneinfo import ZoneInfo
import requests

import alert_delivery_policy as policy
import binance_spot_price_path as source
import sol_g65_experimental_signal as signal
import sol_g65_experimental_store as store
from hype_row71205_experimental_worker import PriceCache as BaseCache
from watch_transition_delivery import subscription_scope

MINUTE = 60000
CONFIG_SHA256 = hashlib.sha256(Path(signal.__file__).read_bytes() +
    store.CONFIG_VERSION.encode() + b'BTC_SOL_BINANCE_SPOT_TRADE_1M;NOTIFICATION_ONLY').hexdigest()


def now_ms():
    return int(time.time()*1000)


def dt(value):
    return datetime.fromtimestamp(value/1000, timezone.utc)


def ms(value):
    return int(store.utc(value).timestamp()*1000)


def fetch_rows(symbol, start, end):
    """Strict single bounded source request; no redirect or exchange fallback."""
    if symbol not in {'BTC', 'SOL'} or start % MINUTE or end % MINUTE or not 0 < end-start <= 1000*MINUTE:
        raise ValueError('BTC/SOL minute request must be bounded to 1..1000 bars')
    response = requests.get(source.BINANCE_SPOT_BASE_URL+source.BINANCE_SPOT_KLINES_ENDPOINT,
        params={'symbol': symbol+'USDT', 'interval': '1m', 'startTime': start,
                'endTime': end-1, 'limit': (end-start)//MINUTE},
        timeout=source.REQUEST_TIMEOUT_SECONDS, allow_redirects=False)
    if response.status_code != 200:
        raise ValueError(symbol+'_BINANCE_HTTP_'+str(response.status_code))
    if len(getattr(response, 'content', b'')) > 2_000_000:
        raise ValueError('Oversized price payload')
    page = response.json()
    if not isinstance(page, list) or len(page) != (end-start)//MINUTE:
        raise ValueError('Incomplete minute coverage')
    rows = []
    for i, raw in enumerate(page):
        when = start+i*MINUTE
        if len(raw)<7 or int(raw[0]) != when or int(raw[6]) != when+MINUTE-1:
            raise ValueError('Noncontiguous minute coverage')
        o,h,l,c = [float(v) for v in raw[1:5]]
        if not all(isfinite(v) and v>0 for v in (o,h,l,c)) or not l <= min(o,c) <= max(o,c) <= h:
            raise ValueError('Invalid OHLC price')
        rows.append([when,o,h,l,c])
    return rows


class PriceCache(BaseCache):
    def __init__(self, fetch=fetch_rows):
        self.fetch, self.rows = fetch, {'SOL': {}, 'BTC': {}}


def render_alert(p):
    when = dt(ms(p['entry_at'])).astimezone(ZoneInfo('Asia/Jerusalem')).strftime('%Y-%m-%d %H:%M')
    return (
        '🧪 <b>SOL · g65/k49 · SHORT · קידום סטופ — ניסיוני, לא למסחר</b>\n'
        f"רמת כניסה במעקב: <b>{p['entry_price']:.8g}</b> · דקת נגיעה בישראל: {when}\n"
        f"סטופ התחלתי: <b>{p['original_stop_price']:.8g}</b> · טייק: <b>{p['take_price']:.8g}</b>\n"
        f"בהגעה ל־<b>{p['lock_trigger_price']:.8g}</b> הסטופ מתקדם ל־<b>{p['lock_stop_price']:.8g}</b>, "
        'החל מהדקה הבאה ולאחר בדיקת הסטופ והטייק המקוריים.\n'
        'התנאי: BTC ירד ב־1% לפחות ובפחות מ־2% ב־12 השעות הסגורות הקודמות. '
        'בדיקה כל חצי שעה; מחיר המקור הוא פתיחת הדקה הבאה. ממתינים לעליית SOL של 0.5% ממנו.\n'
        'מותרת ממתינה או פתוחה אחת בנוסחה זו, ללא מגבלת המתנה או החזקה. '
        'מקור: Binance Spot, נרות עסקאות של דקה.\n'
        'התראה ומעקב בלבד; נגיעה היסטורית אינה אישור ביצוע או מחיר זמין כעת. אין הוראות מסחר או גודל פוזיציה.'
    )


class SolG65Worker:
    def __init__(self, *, cache=None, clock=now_ms):
        self.cache, self.clock = cache or PriceCache(), clock
        self.task = self.bot = self.subscription = None
        self.scopes = set()
        self.last_poll = None
        self.retry_ms = 0
        self.runtime = {'rule_id': signal.RULE_ID, 'formula_id': signal.FORMULA_ID,
                        'config_sha256': CONFIG_SHA256, 'ready': False, 'state': 'NOT_STARTED',
                        'last_error_type': None, 'active_position': None, 'delivered': 0}

    def status(self):
        return {**deepcopy(self.runtime), 'running': bool(self.task and not self.task.done()),
                'delivery_allowed_by_profile': policy.sol_g65_experimental_enabled(),
                'overlap_cap': 1, 'cap_includes_pending': True, 'pending_ttl_hours': None,
                'holding_time_limit': None, 'price_source': 'BINANCE_SPOT_TRADE_1M',
                'notification_only': True, 'live_order_execution': False,
                'position_notional_cap': None, 'profit_lock_trigger_take_fraction': .75,
                'profit_lock_take_fraction': .25}

    def start(self, bot, subscription):
        self.bot, self.subscription = bot, subscription
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run(), name='sol-g65-experimental')

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def allowed(self, chat_id):
        enabled,current = self.subscription() if self.subscription else (False,None)
        return bool(enabled and current == chat_id and policy.sol_g65_experimental_enabled())

    async def db(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, config_sha256=CONFIG_SHA256, **kwargs)

    def evidence_error(self, exc):
        # Do not repeatedly probe a restricted exchange endpoint.
        delay = 6*60*MINUTE if '_HTTP_451' in str(exc) or '_HTTP_403' in str(exc) else 5*MINUTE
        self.retry_ms = self.clock()+delay
        self.runtime.update(ready=False, state='EVIDENCE_UNAVAILABLE', last_error_type=type(exc).__name__,
                            next_retry_at=dt(self.retry_ms).isoformat())
        print(f'[sol-g65] evidence unavailable type={type(exc).__name__}', flush=True)

    async def run(self):
        while True:
            try:
                if self.clock() >= self.retry_ms:
                    await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.evidence_error(exc)
            await asyncio.sleep(10)

    async def prices(self, symbol, start, end):
        if end > self.clock()//MINUTE*MINUTE:
            raise ValueError('Closed cache cannot include unfinished minute')
        return await asyncio.to_thread(self.cache.fill, symbol, start, end)

    async def monitor(self, scope, state):
        active = state.get('active')
        if active and active.get('monitor_status') != 'AMBIGUOUS':
            start = ms(active['bar_cursor'])+MINUTE
            end = min(self.clock()//MINUTE*MINUTE, start+1000*MINUTE)
            if start < end:
                rows = await self.prices('SOL',start,end)
                bars = [dict(open_at=dt(r[0]),open=r[1],high=r[2],low=r[3],close=r[4]) for r in rows]
                await self.db(store.advance_position,scope,bars,dt(self.clock()),position_id=active['position_id'])
                state = await asyncio.to_thread(store.snapshot,scope)
                self.cache.prune({'SOL': end-2*MINUTE})
        self.runtime.update(active_position=state.get('active'), counts=state.get('counts',{}),
                            activated_at=state['activated_at'],last_decision=state.get('last_decision'))
        return state

    async def deliver(self, scope, chat_id):
        if not self.allowed(chat_id):
            return
        state = await asyncio.to_thread(store.snapshot,scope)
        active = state.get('active')
        if not active or active.get('phase') != 'OPEN' or active.get('monitor_status') == 'AMBIGUOUS':
            return
        pending = next((i for i in state['intents'] if i['status']=='PENDING' and i['position_id']==active['position_id']),None)
        if not pending or self.clock() >= ms(pending['expires_at']):
            return
        # Provisional extrema veto stale alerts only; never outcomes/features.
        rows = await asyncio.to_thread(self.cache.fetch,'SOL',ms(active['entry_at']),self.clock()//MINUTE*MINUTE+MINUTE)
        if any(r[2]>=active['original_stop_price'] or r[3]<=active['lock_trigger_price'] for r in rows):
            await self.db(store.cancel_pending,scope,active['position_id'],dt(self.clock()),reason='BARRIER_OR_LOCK_BEFORE_SEND')
            return
        intent = await self.db(store.claim_pending,scope,dt(self.clock()),expected_position_id=active['position_id'])
        if not intent:
            return
        message_id,terminal = None,'FAILED'
        if self.allowed(chat_id) and self.clock()<ms(intent['expires_at']):
            try:
                message = await asyncio.wait_for(self.bot.send_message(chat_id=chat_id,text=render_alert(intent['payload']),parse_mode='HTML'),timeout=20)
                message_id = getattr(message,'message_id',None)
                terminal = 'DELIVERED' if type(message_id) is int and message_id>0 else 'UNKNOWN'
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                terminal = 'FAILED' if type(exc).__name__ in {'BadRequest','Forbidden'} else 'UNKNOWN'
        ok = await self.db(store.finish_attempt,scope,intent['intent_id'],intent['attempt_token'],terminal,dt(self.clock()),message_id=message_id)
        if not ok:
            raise RuntimeError('SOL g65 delivery acknowledgement not persisted')
        self.runtime['last_delivery_status'] = terminal
        if terminal=='DELIVERED':
            self.runtime['delivered'] += 1
            self.runtime['last_delivery_at'] = dt(self.clock()).isoformat()

    async def tick(self):
        enabled,chat_id = self.subscription()
        if chat_id is None:
            self.runtime['state']='WATCH_OFF'
            return
        scope = subscription_scope(chat_id)
        allowed = self.allowed(chat_id)
        if scope not in self.scopes:
            if not allowed:
                return
            state = await self.db(store.initialize_scope,scope,dt(self.clock()))
            self.scopes.add(scope)
        else:
            if self.last_poll == (scope,self.clock()//MINUTE):
                return
            state = await asyncio.to_thread(store.snapshot,scope)
        self.last_poll = (scope,self.clock()//MINUTE)
        state = await self.monitor(scope,state)
        self.runtime.update(ready=True,last_error_type=None,next_retry_at=None)
        if not allowed:
            self.runtime['state']='WATCH_OFF'
            return
        await self.deliver(scope,chat_id)
        d = signal.last_closed_decision_ms(self.clock())
        reference = d+MINUTE
        self.runtime['next_decision_at'] = dt(d+signal.DECISION_STEP_MS).isoformat()
        if d<=ms(state['activated_at']) or state.get('decision_cursor') and d<=ms(state['decision_cursor']):
            self.runtime['state']='MONITORING' if state.get('active') else 'WAITING_NEXT_SLOT'
            return
        if self.clock()<reference:
            self.runtime['state']='WAITING_REFERENCE_MINUTE'
            return
        reason = ('STALE_SLOT_SKIPPED' if self.clock()>=reference+90000 else
                  'ACTIVE_POSITION' if state.get('active') else
                  'DECISION_NOT_AFTER_LAST_EXIT' if state.get('last_exit_at') and d<=ms(state['last_exit_at']) else None)
        if reason:
            result = await self.db(store.record_no_signal,scope,dt(d),dt(self.clock()),reason=reason)
        else:
            start = signal.required_history_start_ms(d)['BTC']
            rows = await self.prices('BTC',start,d)
            evaluation = signal.evaluate_signal(rows,d)
            if not evaluation['valid'] or not evaluation['signal']:
                result = await self.db(store.record_no_signal,scope,dt(d),dt(self.clock()),reason=evaluation['reason'])
            else:
                ref_rows = await asyncio.to_thread(self.cache.fetch,'SOL',reference,reference+MINUTE)
                if len(ref_rows)!=1 or ref_rows[0][0]!=reference:
                    raise ValueError('Reference minute OPEN unavailable')
                if not self.allowed(chat_id):
                    return
                result = await self.db(store.reserve_signal,scope,dt(d),dt(reference),ref_rows[0][1],evaluation,dt(self.clock()))
            self.cache.prune({'BTC':start,'SOL':self.clock()//MINUTE*MINUTE-2*MINUTE})
        self.runtime.update(state=result['status'],last_decision_at=dt(d).isoformat())
        state = await asyncio.to_thread(store.snapshot,scope)
        self.runtime.update(active_position=state.get('active'),counts=state.get('counts',{}),last_decision=state.get('last_decision'))


WORKER = SolG65Worker()
