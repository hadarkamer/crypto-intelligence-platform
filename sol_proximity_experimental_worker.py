"""Prospective SOL MaxPain notification monitor. No order/forwarder integration."""
from __future__ import annotations
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path
import time
from zoneinfo import ZoneInfo
import requests
import alert_delivery_policy as policy
import binance_spot_price_path as source
from hype_row71205_experimental_worker import PriceCache
from watch_transition_delivery import subscription_scope
import sol_proximity_experimental_signal as signal
import sol_proximity_experimental_store as store

MINUTE = signal.MINUTE
CONFIG_SHA256 = hashlib.sha256(Path(signal.__file__).read_bytes()+signal.CONFIG_VERSION.encode()
                               +b'BINANCE_SPOT_SOLUSDT_TRADE_1M;NOTIFICATION_ONLY').hexdigest()


def now_ms():
    return int(time.time()*1000)


def dt(ms):
    return datetime.fromtimestamp(ms/1000, timezone.utc)


def fetch_rows(symbol, start, end):
    """One bounded canonical Binance Spot request, including optional live veto."""
    if symbol != 'SOL' or start % MINUTE or end % MINUTE or not 0 < end-start <= 1000*MINUTE:
        raise ValueError('SOL minute fetch must be bounded to 1..1000 rows')
    response = requests.get(source.BINANCE_SPOT_BASE_URL+source.BINANCE_SPOT_KLINES_ENDPOINT,
                            params={'symbol': 'SOLUSDT', 'interval': '1m', 'startTime': start,
                                    'endTime': end-1, 'limit': (end-start)//MINUTE},
                            timeout=source.REQUEST_TIMEOUT_SECONDS, allow_redirects=False)
    if response.status_code != 200:
        raise source.BinanceSpotPathError('SOL_BINANCE_HTTP_'+str(response.status_code))
    if len(getattr(response, 'content', b'')) > 2_000_000:
        raise ValueError('Oversized Binance response')
    payload = response.json()
    if not isinstance(payload, list) or len(payload) != (end-start)//MINUTE:
        raise ValueError('Missing SOL minute coverage')
    rows = []
    for i, raw in enumerate(payload):
        t = start+i*MINUTE
        if int(raw[0]) != t or int(raw[6]) != t+MINUTE-1:
            raise ValueError('Noncontiguous SOL price data')
        c = source._parse_candle(raw, 1)
        if not all(math.isfinite(v) for v in (c.open, c.high, c.low, c.close)):
            raise ValueError('Nonfinite SOL price')
        rows.append([t, c.open, c.high, c.low, c.close])
    return rows


class SolPriceCache(PriceCache):
    def __init__(self, fetch=fetch_rows):
        self.fetch, self.rows = fetch, {'SOL': {}}


def render_alert(p):
    direction = 'LONG' if p['direction'] == 1 else 'SHORT'
    when = dt(p['fill_ms']).astimezone(ZoneInfo('Asia/Jerusalem')).strftime('%Y-%m-%d %H:%M')
    return (
        f"🧪 <b>SOL · MaxPain proximity &gt;15 · {direction} — ניסיוני, לא למסחר</b>\n"
        f"רמת הכניסה במעקב נגעה במחיר: <b>{p['fill_price']:.8g}</b>\n"
        f"דקת הנגיעה בישראל: {when}\n"
        f"סטופ: <b>{p['stop_price']:.8g}</b> · טייק MaxPain: <b>{p['take_price']:.8g}</b>\n"
        f"מחיר המקור: {p['source_price']:.8g} · ציון קרבה: {p['score']:.2f} · טווח מקור: {p['timeframe']}\n"
        "הכניסה לאחר תנועה נגדית של פי 2 מהמרחק המקורי ליעד; הסטופ במרחק פי 5 ממחיר המקור. "
        "המתנה לכניסה עד 24 שעות, ולאחריה אין מגבלת זמן החזקה ואין קידום סטופ.\n"
        "מותרות פוזיציות במעקב במקביל רק ביעדים המרוחקים זה מזה ביותר מ־0.2%. "
        "מקור המחיר: Binance Spot, נרות עסקאות של דקה.\n"
        "התראה ומעקב בלבד; הנגיעה ההיסטורית אינה אישור מילוי פקודה או מחיר זמין כעת. "
        "אין הוראת מסחר, גודל פוזיציה או תקרת 5,000$ בהתראה."
    )


class SolProximityWorker:
    def __init__(self, *, cache=None, clock=now_ms):
        self.cache, self.clock = cache or SolPriceCache(), clock
        self.task = self.bot = self.subscription = None
        self.scopes, self.lock = set(), asyncio.Lock()
        self.retry_ms, self.last_poll, self.last_price_poll = 0, None, None
        self.runtime = {'rule_id': signal.RULE_ID, 'ready': False, 'state': 'NOT_STARTED',
                        'config_sha256': CONFIG_SHA256, 'delivered': 0}

    def status(self):
        return {**deepcopy(self.runtime), 'running': bool(self.task and not self.task.done()),
                'delivery_allowed_by_profile': policy.sol_proximity_experimental_enabled(),
                'notification_only': True, 'live_order_execution': False,
                'position_notional_cap': None, 'position_sizing': 'NOT_APPLICABLE_ALERT_ONLY',
                'price_source': 'BINANCE_SPOT_SOLUSDT_TRADE_1M', 'minimum_proximity_score': '>15 rounded2dp',
                'overlap_rule': 'distinct targets >0.2%; no liquidity-growth exception',
                'operational_state_capacity': signal.MAX_ACTIVE,
                'pending_ttl_hours': 24, 'holding_time_limit': None,
                'bootstrap_policy': 'existing targets unverified until complete absence then return'}

    def start(self, bot, subscription):
        self.bot, self.subscription = bot, subscription
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run(), name='sol-proximity-experimental')

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def allowed(self, chat_id):
        enabled, current = self.subscription() if self.subscription else (False, None)
        return bool(enabled and current == chat_id and policy.sol_proximity_experimental_enabled())

    async def db(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, config_sha256=CONFIG_SHA256, **kwargs)

    async def initialize(self, scope):
        if scope not in self.scopes:
            await self.db(store.initialize_scope, scope, self.clock())
            self.scopes.add(scope)
        return await asyncio.to_thread(store.snapshot, scope)

    def evidence_error(self, exc):
        self.retry_ms = self.clock()+5*MINUTE
        self.runtime.update(ready=False, state='EVIDENCE_UNAVAILABLE', last_error_type=type(exc).__name__,
                            next_retry_at=dt(self.retry_ms).isoformat())
        print(f'[sol-proximity] evidence unavailable type={type(exc).__name__}', flush=True)

    async def monitor(self, scope, state):
        start = state['bar_cursor_ms']+MINUTE
        end = min(self.clock()//MINUTE*MINUTE, start+1000*MINUTE)
        if start < end:
            slot = (scope, self.clock()//MINUTE)
            if self.last_price_poll == slot:
                return state
            if state['episodes'] or state['active']:
                rows = await asyncio.to_thread(self.cache.fill, 'SOL', start, end)
                await self.db(store.advance, scope, rows, self.clock())
                self.last_price_poll = slot
            else:
                # With no source baseline there is no historical exposure.
                await self.db(store.transact, scope, self.clock(), action=lambda s: s.update(bar_cursor_ms=end-MINUTE))
        state = await asyncio.to_thread(store.snapshot, scope)
        self.runtime.update(active_pending=sum(p['status'] == 'PENDING' for p in state['active']),
                            active_open=sum(p['status'] == 'OPEN' for p in state['active']),
                            active_unknown=sum(p['status'] == 'UNKNOWN' for p in state['active']),
                            counts=state['counts'], last_cycle_id=state['last_cycle_id'],
                            last_snapshot_complete=state['last_snapshot_complete'],
                            bootstrap_unverified_targets=sum(e.get('bootstrap_unverified', False) and e['present'] for e in state['episodes'].values()),
                            closed_through=dt(state['bar_cursor_ms']+MINUTE).isoformat())
        return state

    async def observe(self, bundle, chat_id):
        """Called by existing shared Watch. No DOM/derivatives collection here."""
        if not self.allowed(chat_id) or self.clock() < self.retry_ms:
            return
        async with self.lock:
            try:
                now = self.clock()
                decoded = signal.decode_bundle(bundle, now)
                scope = subscription_scope(chat_id)
                state = await self.initialize(scope)
                state = await self.monitor(scope, state)
                if state['bar_cursor_ms'] < self.clock()//MINUTE*MINUTE-MINUTE:
                    self.runtime.update(state='RECOVERING_PRICE_HISTORY')
                    return
                # Live current minute is only a conservative pre-intake veto;
                # it never serves as a closed-bar entry/exit or future feature.
                start = min((min(r['observed_ms'], r['price_ms'])//MINUTE*MINUTE for r in decoded['rows']), default=now//MINUTE*MINUTE)
                end = self.clock()//MINUTE*MINUTE+MINUTE
                guards = await asyncio.to_thread(self.cache.fetch, 'SOL', start, end) if decoded['rows'] else []
                if not self.allowed(chat_id):
                    return
                outcome = await self.db(store.ingest, scope, decoded, guards, self.clock())
                self.runtime.update(ready=True, state=outcome, last_error_type=None, next_retry_at=None)
                self.last_poll = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.evidence_error(exc)

    async def deliver(self, scope, chat_id):
        if not self.allowed(chat_id):
            return False
        intent = await self.db(store.claim_pending, scope, self.clock())
        if not intent:
            return False
        outcome, message_id, send_attempted, source_failed = 'UNKNOWN', None, False, False
        try:
            p = intent['payload']
            end = self.clock()//MINUTE*MINUTE+MINUTE
            rows = await asyncio.to_thread(self.cache.fetch, 'SOL', p['fill_ms'], end)
            hit_barrier = any((l <= p['stop_price'] or h >= p['take_price']) if p['direction'] == 1
                              else (h >= p['stop_price'] or l <= p['take_price']) for t, o, h, l, c in rows)
            if hit_barrier:
                outcome = 'CANCELLED_STALE_PRICE'
            elif not self.allowed(chat_id) or self.clock() >= intent['expires_ms']:
                outcome = 'FAILED'
            else:
                send_attempted = True
                msg = await asyncio.wait_for(self.bot.send_message(chat_id=chat_id, text=render_alert(p), parse_mode='HTML'), timeout=20)
                message_id = getattr(msg, 'message_id', None)
                outcome = 'DELIVERED' if type(message_id) is int and message_id > 0 else 'UNKNOWN'
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not send_attempted:
                source_failed = True
                self.evidence_error(exc)
                outcome = 'FAILED'
            else:
                outcome = 'FAILED' if type(exc).__name__ in ('BadRequest', 'Forbidden') else 'UNKNOWN'
        saved = await self.db(store.finish_attempt, scope, intent['intent_id'], intent['attempt_token'], outcome,
                              self.clock(), message_id=message_id)
        if not saved:
            raise RuntimeError('SOL delivery acknowledgement not persisted')
        self.runtime['last_delivery_status'] = outcome
        if outcome == 'DELIVERED':
            self.runtime['delivered'] += 1
            self.runtime['last_delivery_at'] = dt(self.clock()).isoformat()
        print(f'[sol-proximity] delivery status={outcome}', flush=True)
        return not source_failed

    async def tick(self):
        if not self.subscription:
            return
        enabled, chat_id = self.subscription()
        if chat_id is None:
            return
        scope = subscription_scope(chat_id)
        if not self.allowed(chat_id) and scope not in self.scopes:
            return
        minute = self.clock()//MINUTE
        if self.last_poll == (scope, minute):
            async with self.lock:
                for _ in range(4):
                    if not await self.deliver(scope, chat_id):
                        break
            return
        async with self.lock:
            state = await self.initialize(scope)
            state = await self.monitor(scope, state)
            if state['bar_cursor_ms'] >= self.clock()//MINUTE*MINUTE-MINUTE:
                # Bounded number of deliveries; subsequent fresh intents are
                # drained on the next 10-second worker tick, without price poll.
                for _ in range(4):
                    if not await self.deliver(scope, chat_id):
                        break
                self.last_poll = (scope, minute)
                if self.clock() < self.retry_ms:
                    return
                self.runtime.update(ready=True, state='MONITORING' if state['initialized_universe'] else 'WAITING_WATCH_BASELINE',
                                    last_error_type=None, next_retry_at=None)
            else:
                self.runtime.update(ready=False, state='RECOVERING_PRICE_HISTORY')
            self.cache.prune({'SOL': self.clock()-1005*MINUTE})

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


WORKER = SolProximityWorker()
