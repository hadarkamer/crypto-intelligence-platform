"""Bounded notification worker using the existing initialized Telegram bot."""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import os
from typing import Any

import alert_delivery_policy as delivery_policy
import research_ordered_experimental as contract
import research_ordered_experimental_store as store

_TRUE = {"1","true","yes","on"}
_ENABLED = os.getenv("RESEARCH_ORDERED_EXPERIMENTAL_ALERTS_ENABLED","").strip().lower() in _TRUE
_POLL = 30
try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None


def enabled() -> bool:
    return _ENABLED and delivery_policy.other_experimental_alerts_enabled()


def _database_url() -> str:
    return os.getenv("RESEARCH_DATABASE_URL","").strip() or (os.getenv("DATABASE_URL","").strip()
        if os.getenv("RESEARCH_USE_PRIMARY_DATABASE","").strip().lower() in _TRUE else "")


def _transaction(fn, *args, **kwargs):
    with psycopg.connect(_database_url(),row_factory=dict_row,connect_timeout=5,
                        options="-c statement_timeout=15000 -c lock_timeout=1000") as conn:
        return fn(conn,*args,**kwargs)


class OrderedExperimentalWorker:
    def __init__(self):
        self._bot = None
        self._task = None
        self._watch_busy = lambda: False
        self._delivery_lock = asyncio.Lock()
        self.metrics = {"runs":0,"sent":0,"unknown":0,"failures":0,"last_error":None,"last_summary":None}

    def bind_telegram(self, bot: Any):
        self._bot = bot

    def bind_watch_busy(self, predicate):
        """Suspend autonomous sends while a half-hour Watch batch is active."""
        self._watch_busy = predicate

    def status(self):
        return {"enabled":enabled(),"configured":bool(_database_url()),
                "delivery_allowed_by_profile":delivery_policy.other_experimental_alerts_enabled(),
                "running":bool(self._task and not self._task.done()),"telegram_connected":self._bot is not None,
                "delivery_version":contract.VERSION,"live_effect":"EXPERIMENTAL_NOTIFICATION_ONLY" if enabled() else "NONE",
                "human_formula_approval_required":False,"trade_execution":False,"poll_seconds":_POLL,
                "qualification_ttl_minutes":30,"trigger_ttl_minutes":10,"metrics":dict(self.metrics)}

    async def start(self):
        if not enabled():
            return False
        if psycopg is None or not _database_url() or self._bot is None:
            raise RuntimeError("Experimental research database/Telegram binding missing")
        if not await asyncio.to_thread(_transaction,store.available):
            raise RuntimeError("Experimental migration 037 missing")
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._run(),name="ordered-v7-experimental-notifications")
        return True

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self):
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics["failures"] += 1
                self.metrics["last_error"] = type(exc).__name__
            await asyncio.sleep(_POLL)

    async def drain_for_watch(self, chat_id: int, *, may_deliver=None):
        """Send currently qualified notifications before ordinary Watch output.

        Waiting on the shared lock finishes any earlier poll transport first.
        The busy callback keeps subsequent polls out until the whole Watch
        batch ends. Newly qualified notifications still use the existing
        autonomous route outside that batch; no source/TTL gate is changed.
        """
        return await self.run_once(chat_id=chat_id, max_deliveries=256,
                                   allow_watch=True, may_deliver=may_deliver)

    async def run_once(self, *, chat_id=None, max_deliveries=2,
                       allow_watch=False, may_deliver=None):
        if not enabled() or self._bot is None:
            return {"sent":0,"disabled":True}
        async with self._delivery_lock:
            if not allow_watch and self._watch_busy():
                return {"sent":0,"watch_busy":True}
            if may_deliver is not None and not may_deliver():
                return {"sent":0,"delivery_stopped":True}
            delivery_allowed = lambda: (delivery_policy.other_experimental_alerts_enabled()
                and (allow_watch or not self._watch_busy())
                and (may_deliver is None or may_deliver()))
            return await self._deliver(chat_id=chat_id, max_deliveries=max_deliveries,
                                       may_deliver=delivery_allowed, allow_watch=allow_watch)

    async def _deliver(self, *, chat_id, max_deliveries, may_deliver, allow_watch):
        if not delivery_policy.other_experimental_alerts_enabled():
            return {"sent":0,"disabled":True}
        summary = await asyncio.to_thread(_transaction,store.enqueue,
            now=datetime.now(timezone.utc),scope_limit=32 if allow_watch else 8)
        summary.update(sent=0,unknown=0)
        for _ in range(max_deliveries):
            if (not delivery_policy.other_experimental_alerts_enabled()
                    or (may_deliver is not None and not may_deliver())):
                summary["delivery_stopped"] = True
                break
            item = await asyncio.to_thread(_transaction,store.claim,
                now=datetime.now(timezone.utc),chat_id=chat_id)
            if not item:
                break
            if (not delivery_policy.other_experimental_alerts_enabled()
                    or (may_deliver is not None and not may_deliver())):
                await asyncio.to_thread(_transaction,store.release_unsent,item)
                summary["delivery_stopped"] = True
                break
            text = contract.render(item["payload"])
            if not await asyncio.to_thread(_transaction,store.begin_send,item,now=datetime.now(timezone.utc)):
                continue
            if (not delivery_policy.other_experimental_alerts_enabled()
                    or (may_deliver is not None and not may_deliver())):
                # This live owner knows no transport call has started. A
                # recovered SENDING lease still becomes UNKNOWN, as before.
                await asyncio.to_thread(_transaction,store.release_unsent,item)
                summary["delivery_stopped"] = True
                break
            # Cancellation/crash leaves committed SENDING; expiry becomes
            # UNKNOWN, so recovery cannot send the same notification twice.
            try:
                message = await self._bot.send_message(chat_id=int(item["chat_id"]),text=text,
                    connect_timeout=10,read_timeout=20,write_timeout=20,pool_timeout=5)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await asyncio.to_thread(_transaction,store.finish,item,now=datetime.now(timezone.utc),
                                        message_id=None,error=type(exc).__name__)
                summary["unknown"] += 1
            else:
                message_id = getattr(message,"message_id",None)
                recorded = await asyncio.to_thread(_transaction,store.finish,item,now=datetime.now(timezone.utc),message_id=message_id)
                summary["sent" if recorded and type(message_id) is int else "unknown"] += 1
        self.metrics["runs"] += 1
        self.metrics["sent"] += summary["sent"]
        self.metrics["unknown"] += summary["unknown"]
        self.metrics["last_error"] = None
        self.metrics["last_summary"] = summary
        return summary


WORKER = OrderedExperimentalWorker()
