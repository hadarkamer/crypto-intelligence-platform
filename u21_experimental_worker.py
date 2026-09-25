"""Prospective U21 notifications; no trading API and no historical alert replay."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from math import isfinite
from pathlib import Path
import time

import requests
import alert_delivery_policy as policy
import binance_spot_price_path as source
import u21_experimental_signal as signal
import u21_experimental_store as store
from watch_transition_delivery import subscription_scope

MINUTE = 60000
CONFIG_SHA256 = hashlib.sha256(
    Path(signal.__file__).read_bytes() + store.CONFIG_VERSION.encode()
).hexdigest()


def now_ms():
    return int(time.time() * 1000)


def dt(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def ms(value):
    return int(store.utc(value).timestamp() * 1000)


def fetch_rows(symbol, start, end):
    """Fetch exact minute rows [start,end); caller controls closed-bar cutoff."""
    rows = []
    while start < end:
        response = requests.get(
            source.BINANCE_SPOT_BASE_URL + source.BINANCE_SPOT_KLINES_ENDPOINT,
            params={"symbol": symbol + "USDT", "interval": "1m", "startTime": start,
                    "endTime": end - 1, "limit": min(1000, (end - start) // MINUTE)},
            timeout=source.REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list) or not page:
            raise ValueError("Missing Binance minute data")
        for raw in page:
            if len(raw) < 7 or int(raw[0]) != start or int(raw[6]) != start + MINUTE - 1:
                raise ValueError("Noncontiguous Binance minute data")
            prices = [float(x) for x in raw[1:5]]
            if not all(isfinite(x) and x > 0 for x in prices):
                raise ValueError("Invalid Binance price")
            o, h, l, c = prices
            if h < max(o, l, c) or l > min(o, h, c):
                raise ValueError("Invalid Binance OHLC")
            rows.append([start, o, h, l, c])
            start += MINUTE
            if start >= end:
                break
    return rows


class PriceCache:
    def __init__(self, fetch=fetch_rows):
        self.fetch = fetch
        self.rows = {"XRP": {}, "BTC": {}}

    def fill(self, symbol, start, end):
        """Only closed minutes belong in the cache. Gaps are retried explicitly."""
        data = self.rows[symbol]
        cursor = start
        while cursor < end:
            if cursor in data:
                cursor += MINUTE
                continue
            stop = cursor + MINUTE
            while stop < end and stop not in data and stop - cursor < 1000 * MINUTE:
                stop += MINUTE
            got = self.fetch(symbol, cursor, stop)
            if [r[0] for r in got] != list(range(cursor, stop, MINUTE)):
                raise ValueError("Incomplete closed minute coverage")
            data.update({r[0]: r for r in got})
            cursor = stop
        return [data[t] for t in range(start, end, MINUTE)]

    def prune(self, minimums):
        for symbol, start in minimums.items():
            self.rows[symbol] = {t: r for t, r in self.rows[symbol].items() if t >= start}


def render_alert(price, entry):
    return (
        "🧪 <b>U21 · XRP · SHORT — ניסיונית, ללא חפיפה</b>\n"
        f"מחיר ייחוס לכניסה: <b>{price:.8g}</b>\n"
        f"זמן הייחוס: {dt(entry).strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"סטופ: <b>{price * 1.005:.8g}</b> (+0.5%)\n"
        f"טייק: <b>{price * .92:.8g}</b> (−8%) · יחס 1:16\n"
        "התנאי: XRP באזור 41.22%–83.93% מטווח המסחר הרגיל האחרון בארה״ב, "
        "וכ־0.416%–1.742% מתחת לשיאו.\n"
        "בדיקה כל 15 דקות; הייחוס הוא פתיחת הדקה שאחרי זמן הבדיקה. "
        "לא תישלח כניסה נוספת עד לסיום מעקב הפוזיציה.\n"
        "זוהי התראה בלבד; מחיר הייחוס אינו אישור ביצוע עסקה."
    )


class U21Worker:
    def __init__(self, *, cache=None, clock=now_ms):
        self.cache = cache or PriceCache()
        self.clock = clock
        self.task = None
        self.bot = None
        self.subscription = None
        self.scopes = set()
        self.last_poll_minute = None
        self.runtime = {"rule_id": "U21_XRP_SHORT", "formula_id": signal.FORMULA_ID,
                        "config_sha256": CONFIG_SHA256, "ready": False, "state": "NOT_STARTED",
                        "last_error_type": None, "active_position": None, "delivered": 0}

    def status(self):
        return {**deepcopy(self.runtime), "running": bool(self.task and not self.task.done()),
                "delivery_allowed_by_profile": policy.u21_experimental_enabled(),
                "calendar_supported_through": "2026-12-31", "overlap_cap": 1,
                "stop_pct": .5, "take_pct": 8, "price_source": "BINANCE_SPOT_1M"}

    def start(self, bot, subscription):
        self.bot, self.subscription = bot, subscription
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run(), name="u21-xrp-experimental")

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def allowed(self, chat_id):
        enabled, current = self.subscription()
        return bool(enabled and current == chat_id and policy.u21_experimental_enabled())

    async def db(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, config_sha256=CONFIG_SHA256, **kwargs)

    async def run(self):
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.runtime.update(ready=False, state="EVIDENCE_UNAVAILABLE",
                                    last_error_type=type(exc).__name__)
                print(f"[u21] evidence unavailable type={type(exc).__name__}", flush=True)
            await asyncio.sleep(10)

    async def monitor(self, scope, state, closed_end):
        active = state.get("active")
        # Bounded recovery after an outage; no new entry while recovery is incomplete.
        if active and active.get("monitor_status") != "AMBIGUOUS":
            start = ms(active["bar_cursor"]) + MINUTE
            end = min(closed_end, start + 2000 * MINUTE)
            if start < end:
                rows = await asyncio.to_thread(self.cache.fill, "XRP", start, end)
                bars = [{"open_at": dt(r[0]), "open": r[1], "high": r[2],
                         "low": r[3], "close": r[4]} for r in rows]
                await self.db(store.advance_position, scope, bars, dt(self.clock()),
                              position_id=active["position_id"])
                state = await asyncio.to_thread(store.snapshot, scope)
        self.runtime.update(active_position=state.get("active"),
                            activated_at=state["activated_at"],
                            last_decision=state.get("last_decision"),
                            counts=state.get("counts", {}), last_exit_at=state.get("last_exit_at"))
        return state

    async def deliver(self, scope, chat_id):
        if not self.allowed(chat_id):
            return
        state = await asyncio.to_thread(store.snapshot, scope)
        pending = next((i for i in state["intents"] if i["status"] == "PENDING"), None)
        if pending and state.get("active"):
            position = state["active"]
            if self.clock() < ms(pending["expires_at"]):
                # Live extrema only veto stale notification; they never resolve
                # a position or enter historical signal features.
                end = self.clock() // MINUTE * MINUTE + MINUTE
                rows = await asyncio.to_thread(self.cache.fetch, "XRP", ms(position["entry_at"]), end)
                if any(r[2] >= position["stop_price"] or r[3] <= position["take_price"] for r in rows):
                    await self.db(store.cancel_pending, scope, position["position_id"], dt(self.clock()),
                                  reason="BARRIER_ALREADY_OBSERVED_BEFORE_SEND")
                    self.runtime["last_delivery_status"] = "BARRIER_ALREADY_OBSERVED_BEFORE_SEND"
                    return
        intent = await self.db(store.claim_pending, scope, dt(self.clock()))
        if not intent:
            return
        message_id = None
        if not self.allowed(chat_id) or self.clock() >= ms(intent["expires_at"]):
            terminal = "FAILED"
        else:
            try:
                message = await asyncio.wait_for(self.bot.send_message(
                    chat_id=chat_id, text=intent["text"], parse_mode="HTML"), timeout=20)
                message_id = getattr(message, "message_id", None)
                terminal = "DELIVERED" if type(message_id) is int and message_id > 0 else "UNKNOWN"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                terminal = "FAILED" if type(exc).__name__ in {"BadRequest", "Forbidden"} else "UNKNOWN"
        acknowledged = await self.db(store.finish_attempt, scope, intent["intent_id"], intent["attempt_token"],
                                     terminal, dt(self.clock()), message_id=message_id)
        if not acknowledged:
            raise RuntimeError("U21 delivery acknowledgement not persisted")
        self.runtime["last_delivery_status"] = terminal
        if terminal == "DELIVERED":
            self.runtime["delivered"] += 1
            self.runtime["last_delivery_at"] = dt(self.clock()).isoformat()
        print(f"[u21] delivery status={terminal}", flush=True)

    async def tick(self):
        enabled, chat_id = self.subscription()
        if chat_id is None:
            self.runtime.update(state="WATCH_OFF")
            return
        scope = subscription_scope(chat_id)
        allowed = self.allowed(chat_id)
        if scope not in self.scopes:
            if not allowed:
                return
            state = await self.db(store.initialize_scope, scope, dt(self.clock()))
            self.scopes.add(scope)
        else:
            # Cheap polling is minute-aligned. Delivery retries never resend IN_FLIGHT.
            minute = self.clock() // MINUTE
            if self.last_poll_minute == (scope, minute):
                return
            state = await asyncio.to_thread(store.snapshot, scope)
        current = self.clock()
        closed_end = current - current % MINUTE
        self.last_poll_minute = (scope, current // MINUTE)
        state = await self.monitor(scope, state, closed_end)
        self.runtime.update(ready=True, last_error_type=None)
        if not allowed:
            self.runtime.update(state="WATCH_OFF")
            return
        await self.deliver(scope, chat_id)
        decision = signal.last_closed_decision_ms(self.clock())
        entry = decision + MINUTE
        self.runtime["next_decision_at"] = dt(decision + 15 * MINUTE).isoformat()

        # Bootstrap before the next slot and incrementally maintain only required history.
        starts = signal.required_history_start_ms(decision)
        if not self.cache.rows["BTC"] or not self.cache.rows["XRP"]:
            self.runtime.update(state="WARMING_HISTORY")
            await asyncio.gather(*(asyncio.to_thread(self.cache.fill, s, starts[s], decision)
                                   for s in ("XRP", "BTC")))
        if decision <= ms(state["activated_at"]) or (
                state.get("decision_cursor") and decision <= ms(state["decision_cursor"])):
            self.runtime.update(state="WAITING_NEXT_SLOT")
            return
        if self.clock() < entry:
            self.runtime.update(state="WAITING_ENTRY_MINUTE")
            return
        reason = None
        if self.clock() >= entry + 90000:
            reason = "STALE_SLOT_SKIPPED"
        elif state.get("active"):
            reason = "ACTIVE_POSITION"
        elif state.get("last_exit_at") and decision <= ms(state["last_exit_at"]):
            reason = "DECISION_NOT_AFTER_LAST_EXIT"
        if reason:
            result = await self.db(store.record_no_signal, scope, dt(decision), dt(self.clock()), reason=reason)
        else:
            rows = await asyncio.gather(*(asyncio.to_thread(self.cache.fill, s, starts[s], decision)
                                          for s in ("XRP", "BTC")))
            evaluation = signal.evaluate_signal(rows[0], rows[1], decision)
            if not evaluation.get("valid") or not evaluation.get("signal"):
                result = await self.db(store.record_no_signal, scope, dt(decision), dt(self.clock()),
                                      reason=evaluation.get("reason", "NO_MATCH"))
            else:
                # This row may be unfinished: ONLY its immutable OPEN is used.
                entry_rows = await asyncio.to_thread(self.cache.fetch, "XRP", entry, entry + MINUTE)
                if len(entry_rows) != 1 or entry_rows[0][0] != entry:
                    raise ValueError("Entry open unavailable")
                price = entry_rows[0][1]
                if not self.allowed(chat_id):
                    return
                result = await self.db(store.reserve_signal, scope, dt(decision), dt(entry), price,
                                       evaluation, dt(self.clock()), text=render_alert(price, entry))
                state = await asyncio.to_thread(store.snapshot, scope)
                await self.monitor(scope, state, self.clock() // MINUTE * MINUTE)
                await self.deliver(scope, chat_id)
        self.runtime.update(state=result["status"], last_decision_at=dt(decision).isoformat())
        self.cache.prune(starts)
        state = await asyncio.to_thread(store.snapshot, scope)
        self.runtime.update(active_position=state.get("active"), counts=state.get("counts", {}),
                            last_decision=state.get("last_decision"))
        print(f"[u21] decision={dt(decision).isoformat()} status={result['status']}", flush=True)


WORKER = U21Worker()
