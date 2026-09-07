"""Bounded notification worker using the existing initialized Telegram bot."""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import os
from typing import Any

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
    return _ENABLED


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
        self.metrics = {"runs":0,"sent":0,"unknown":0,"failures":0,"last_error":None,"last_summary":None}

    def bind_telegram(self, bot: Any):
        self._bot = bot

    def status(self):
        return {"enabled":_ENABLED,"configured":bool(_database_url()),
                "running":bool(self._task and not self._task.done()),"telegram_connected":self._bot is not None,
                "delivery_version":contract.VERSION,"live_effect":"EXPERIMENTAL_NOTIFICATION_ONLY" if _ENABLED else "NONE",
                "human_formula_approval_required":False,"trade_execution":False,"poll_seconds":_POLL,
                "qualification_ttl_minutes":30,"trigger_ttl_minutes":10,"metrics":dict(self.metrics)}

    async def start(self):
        if not _ENABLED:
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

    async def run_once(self):
        if not _ENABLED or self._bot is None:
            return {"sent":0,"disabled":True}
        summary = await asyncio.to_thread(_transaction,store.enqueue,now=datetime.now(timezone.utc))
        summary.update(sent=0,unknown=0)
        for _ in range(2):
            item = await asyncio.to_thread(_transaction,store.claim,now=datetime.now(timezone.utc))
            if not item:
                break
            text = contract.render(item["payload"])
            if not await asyncio.to_thread(_transaction,store.begin_send,item,now=datetime.now(timezone.utc)):
                continue
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
