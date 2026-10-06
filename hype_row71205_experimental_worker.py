"""Prospective ROW71205 notifications; no trading API and no historical alert replay."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time

import alert_delivery_policy as policy
from zoneinfo import ZoneInfo
import hype_row71205_experimental_signal as signal
import hype_row71205_experimental_store as store
import experimental_hyperliquid_source as hype_source
from watch_transition_delivery import subscription_scope

MINUTE = 60000
PRICE_CONTRACT = b"HYPE:HYPERLIQUID_PERPETUAL_TRADE_1M_V1;BTC:HYPERLIQUID_PERPETUAL_TRADE_1M_V1;NO_FALLBACK"
FORMULA_ID = "HYPE_ROW71205_ALL_HYPERLIQUID_SOURCE_VARIANT_V2"
FORMULA_VERSION = "hype-row71205-all-hyperliquid-short-sl005-tp02-cap1-v2"
PRICE_SOURCE = store.PRICE_SOURCE
EVIDENCE_STATUS = "PROSPECTIVE_SOURCE_VARIANT_NOT_HISTORICALLY_VALIDATED"
RESTRICTED_SOURCE_RETRY_MS = 6 * 60 * MINUTE


class PriceEvidenceError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


CONFIG_SHA256 = hashlib.sha256(
    Path(signal.__file__).read_bytes() + store.CONFIG_VERSION.encode() + PRICE_CONTRACT
).hexdigest()


def now_ms():
    return int(time.time() * 1000)


def dt(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def ms(value):
    return int(store.utc(value).timestamp() * 1000)


def fetch_rows(symbol, start, end):
    """Both traded coin and BTC context use one explicit perpetual venue."""
    if symbol not in {'HYPE', 'BTC'}:
        raise ValueError('ROW71205 allows only Hyperliquid HYPE and BTC perpetuals')
    try:
        return hype_source.fetch_rows(symbol, start, end)
    except hype_source.HyperliquidSourceError as exc:
        raise PriceEvidenceError(exc.code) from None


class PriceCache:
    def __init__(self, fetch=fetch_rows):
        self.fetch = fetch
        self.rows = {"HYPE": {}, "BTC": {}}

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
    levels = signal.build_levels(price)
    return (
        "🧪 <b>ROW71205 · HYPE · SHORT · Hyperliquid — ניסיוני, לא למסחר</b>\n"
        "פוזיציה אחת במעקב בנוסחה זו · ללא נעילת רווח\n"
        f"מחיר ייחוס לכניסה: <b>{price:.8g}</b>\n"
        f"זמן הייחוס בישראל: {dt(entry).astimezone(ZoneInfo('Asia/Jerusalem')).strftime('%Y-%m-%d %H:%M')}\n"
        f"סטופ: <b>{levels['stop_loss']:.8g}</b> (+0.5%)\n"
        f"טייק: <b>{levels['take_profit']:.8g}</b> (−2%) · יחס 1:4\n"
        "התנאים: BTC עלה ביותר מ־0% ופחות מ־0.25% ב־12 שעות; "
        "טווח HYPE ב־24 שעות אינו עולה על 5.1%; היחס בין טווח 4 השעות "
        "לטווח 24 השעות גדול מ־0.5; תשואת HYPE ב־4 שעות אינה גבוהה משל BTC.\n"
        "בדיקה כל 30 דקות; מחיר הייחוס הוא פתיחת הדקה שאחרי ההחלטה. "
        "מקור HYPE ו־BTC: חוזים ב־Hyperliquid, נרות עסקאות.\n"
        "גרסת מקור חדשה; תוצאות המחקר על Binance אינן נתונים מאומתים לגרסה זו.\n"
        "לא תישלח כניסה נוספת בנוסחה זו עד סיום המעקב; אין הגבלת זמן החזקה. "
        "התראה ומעקב בלבד, ללא הוראת מסחר; מחיר הייחוס אינו אישור ביצוע עסקה."
    )


class Row71205Worker:
    def __init__(self, *, cache=None, clock=now_ms):
        self.cache = cache or PriceCache()
        self.clock = clock
        self.task = None
        self.bot = None
        self.subscription = None
        self.scopes = set()
        self.last_poll_minute = None
        self.next_retry_ms = 0
        self.runtime = {"rule_id": "HYPE_ROW71205_SHORT", "formula_id": FORMULA_ID,
                        "config_sha256": CONFIG_SHA256, "ready": False, "state": "NOT_STARTED",
                        "last_error_type": None, "active_position": None, "delivered": 0}

    def status(self):
        return {**deepcopy(self.runtime), "running": bool(self.task and not self.task.done()),
                "delivery_allowed_by_profile": policy.hype_row71205_experimental_enabled(),
                "overlap_cap": 1, "decision_step_minutes": 30,
                "stop_pct": .5, "take_pct": 2, "profit_lock": False,
                "price_source": PRICE_SOURCE,
                "formula_version": FORMULA_VERSION,
                "research_parent_formula_id": signal.FORMULA_ID,
                "evidence_status": EVIDENCE_STATUS,
                "source_retention_minutes": 5000,
                "btc_price_source": hype_source.price_source("BTC"),
                "source_contract_version": store.SOURCE_CONTRACT_VERSION,
                "notification_only": True, "live_order_execution": False,
                "position_notional_cap": None, "position_sizing": "NOT_APPLICABLE_ALERT_ONLY"}

    def start(self, bot, subscription):
        self.bot, self.subscription = bot, subscription
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run(), name="hype71205-experimental")

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    def allowed(self, chat_id):
        enabled, current = self.subscription()
        return bool(enabled and current == chat_id and policy.hype_row71205_experimental_enabled())

    async def db(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, config_sha256=CONFIG_SHA256, **kwargs)

    async def prices(self, method, symbol, start, end):
        if method == "fill" and end > self.clock() // MINUTE * MINUTE:
            raise PriceEvidenceError("HYPE_CLOSED_CACHE_REQUEST_INCLUDES_UNFINISHED_BAR")
        rows = await asyncio.to_thread(getattr(self.cache, method), symbol, start, end)
        if symbol == "HYPE":
            self.runtime.update(hype_source_available=True,
                                last_source_checked_at=dt(self.clock()).isoformat())
        return rows

    async def run(self):
        while True:
            try:
                if self.clock() >= self.next_retry_ms:
                    await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Fail closed; an unavailable official source is never replaced.
                # Rate-limit both HTTP restrictions and transient source/DB errors.
                restricted = str(getattr(exc, "code", "")).endswith(("_HTTP_451", "_HTTP_403"))
                self.next_retry_ms = self.clock() + (RESTRICTED_SOURCE_RETRY_MS if restricted else 5 * MINUTE)
                self.runtime.update(ready=False, state="EVIDENCE_UNAVAILABLE",
                                    last_error_type=type(exc).__name__,
                                    last_error_code=getattr(exc, "code", None),
                                    next_retry_at=dt(self.next_retry_ms).isoformat())
                if "HYPERLIQUID_" in str(getattr(exc, "code", "")):
                    self.runtime["hype_source_available"] = False
                print(f"[hype71205] evidence unavailable type={type(exc).__name__} "
                      f"code={getattr(exc, 'code', None)} "
                      f"retry_at={self.runtime['next_retry_at']}", flush=True)
            await asyncio.sleep(10)

    async def monitor(self, scope, state, closed_end):
        active = state.get("active")
        # Bounded recovery after an outage; no new entry while recovery is incomplete.
        if active and active.get("monitor_status") != "AMBIGUOUS":
            start = ms(active["bar_cursor"]) + MINUTE
            end = min(closed_end, start + 2000 * MINUTE)
            if start < end:
                rows = await self.prices("fill", "HYPE", start, end)
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
        if not pending or not state.get("active") or pending["position_id"] != state["active"]["position_id"]:
            return
        if pending and state.get("active"):
            position = state["active"]
            if self.clock() < ms(pending["expires_at"]):
                # Live extrema only veto stale notification; they never resolve
                # a position or enter historical signal features.
                end = self.clock() // MINUTE * MINUTE + MINUTE
                rows = await self.prices("fetch", "HYPE", ms(position["entry_at"]), end)
                if any(r[2] >= position["stop_price"] or r[3] <= position["take_price"] for r in rows):
                    await self.db(store.cancel_pending, scope, position["position_id"], dt(self.clock()),
                                  reason="BARRIER_ALREADY_OBSERVED_BEFORE_SEND")
                    self.runtime["last_delivery_status"] = "BARRIER_ALREADY_OBSERVED_BEFORE_SEND"
                    return
        intent = await self.db(store.claim_pending, scope, dt(self.clock()),
                               expected_position_id=pending["position_id"])
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
            raise RuntimeError("ROW71205 delivery acknowledgement not persisted")
        self.runtime["last_delivery_status"] = terminal
        if terminal == "DELIVERED":
            self.runtime["delivered"] += 1
            self.runtime["last_delivery_at"] = dt(self.clock()).isoformat()
        print(f"[hype71205] delivery status={terminal}", flush=True)

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
            state = await self.db(store.initialize_scope, scope, dt(self.clock()),
                                  migrate_all_prices=True)
            self.runtime["source_migration"] = state.get("all_prices_source_migration")
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
        if not allowed:
            self.runtime.update(ready=False, state="WATCH_OFF")
            return
        await self.deliver(scope, chat_id)
        decision = signal.last_closed_decision_ms(self.clock())
        entry = decision + MINUTE
        self.runtime["next_decision_at"] = dt(decision + 30 * MINUTE).isoformat()

        # Bootstrap before the next slot and incrementally maintain only required history.
        starts = signal.required_history_start_ms(decision)
        if not self.cache.rows["BTC"] or not self.cache.rows["HYPE"]:
            self.runtime.update(state="WARMING_HISTORY")
            # Source readiness is checked on startup even before a new decision.
            # Warm the required HYPE route first so a restriction does not cause
            # unnecessary BTC requests or a silently substituted market.
            await self.prices("fill", "HYPE", starts["HYPE"], decision)
            await self.prices("fill", "BTC", starts["BTC"], decision)
        if not self.runtime.get("ready"):
            # A warm cache or a no-op monitor is not proof of source recovery.
            await self.prices("fetch", "HYPE", closed_end - MINUTE, closed_end)
        self.runtime.update(ready=True, last_error_type=None, last_error_code=None,
                            next_retry_at=None)
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
            rows = await asyncio.gather(*(self.prices("fill", s, starts[s], decision)
                                          for s in ("HYPE", "BTC")))
            evaluation = signal.evaluate_signal(rows[0], rows[1], decision)
            # Reuse the frozen mathematical rule, but never label this venue as
            # the Binance research source or inherit its historical performance.
            evaluation.update(formula_id=FORMULA_ID, version=FORMULA_VERSION,
                              hype_price_source=PRICE_SOURCE, btc_price_source=hype_source.price_source("BTC"),
                              source_contract_version=store.SOURCE_CONTRACT_VERSION,
                              research_parent_formula_id=signal.FORMULA_ID,
                              evidence_status=EVIDENCE_STATUS)
            if not evaluation.get("valid") or not evaluation.get("signal"):
                result = await self.db(store.record_no_signal, scope, dt(decision), dt(self.clock()),
                                      reason=evaluation.get("reason", "NO_MATCH"))
            else:
                # This row may be unfinished: ONLY its immutable OPEN is used.
                entry_rows = await self.prices("fetch", "HYPE", entry, entry + MINUTE)
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
        print(f"[hype71205] decision={dt(decision).isoformat()} status={result['status']}", flush=True)


WORKER = Row71205Worker()
