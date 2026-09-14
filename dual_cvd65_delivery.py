"""User-requested experimental dual-CVD alerts from the ordinary Watch capture."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import time

import dual_cvd65_alert as alert
import dual_cvd65_store as store
from watch_transition_delivery import subscription_scope

_READY = False
_NEXT_INIT = 0.0
_SCOPES = set()
_DRAIN_LOCK = asyncio.Lock()
_STATUS = {
    "rule_id": alert.RULE_ID, "version": alert.VERSION,
    "notification_symbols": list(alert.NOTIFICATION_SYMBOLS), "threshold_pct": 2,
    "experimental": True, "statistically_qualified": False,
    "ready": False, "last_error_type": None, "evidence_gaps": 0,
    "last_record_status": "NOT_OBSERVED", "last_watch_scan_id": None,
    "last_counts": {}, "created_intents": 0, "delivered": 0,
    "unknown": 0, "failed": 0, "orphaned_attempts": 0,
    "last_delivery_at_utc": None,
}


def status():
    return deepcopy(_STATUS)


def _now():
    return datetime.now(timezone.utc)


def _gap(stage, exc):
    global _READY, _NEXT_INIT
    _READY = False
    _NEXT_INIT = time.monotonic() + 60
    _STATUS.update(ready=False, last_error_type=stage + ":" + type(exc).__name__)
    _STATUS["evidence_gaps"] += 1
    print(f"[dual-cvd65] gap stage={stage} error_type={type(exc).__name__}", flush=True)


async def initialize(chat_id=None):
    """Check staged schema and persist activation before the first new scan."""
    global _READY
    if not _READY and time.monotonic() < _NEXT_INIT:
        return False
    try:
        if not _READY:
            if not await asyncio.to_thread(store.schema_ready):
                raise RuntimeError("dual CVD schema is not ready")
            _READY = True
        if chat_id is not None:
            scope = subscription_scope(chat_id)
            if scope not in _SCOPES:
                await asyncio.to_thread(store.initialize_scope, scope, _now())
                _SCOPES.add(scope)
    except Exception as exc:
        _gap("initialize", exc)
        return False
    _STATUS.update(ready=True, last_error_type=None)
    return True


async def record_watch(chat_id, bundle, *, watch_scan_id, decision_time):
    """Freeze one decision from the full capture before ordinary Watch sends."""
    _STATUS.update(last_watch_scan_id=watch_scan_id, last_record_status="PERSISTENCE_UNAVAILABLE")
    if not await initialize(chat_id):
        return None
    try:
        evaluation = alert.evaluate_bundle(bundle, decision_time)
        if evaluation.get("watch_scan_id") != watch_scan_id:
            _STATUS["last_record_status"] = "INVALID_CAPTURE"
            return None
        result = await asyncio.to_thread(
            store.record_cycle, subscription_scope(chat_id), evaluation, _now(),
        )
    except Exception as exc:
        _STATUS["last_record_status"] = "EVIDENCE_GAP"
        _gap("record", exc)
        return None
    _STATUS.update(last_record_status=result["record_status"],
                   last_counts=result.get("counts", {}), last_error_type=None)
    _STATUS["created_intents"] += result.get("created_intents", 0)
    print(f"[dual-cvd65] recorded watch={watch_scan_id} status={result['record_status']} "
          f"intents={result.get('created_intents', 0)} counts={result.get('counts', {})}", flush=True)
    return result


async def _finish(intent, terminal):
    try:
        if not await asyncio.to_thread(store.finish_attempt, intent["intent_id"],
                                     intent["attempt_token"], terminal, _now()):
            raise RuntimeError("attempt acknowledgement was not persisted")
    except Exception as exc:
        # The durable IN_FLIGHT reservation prevents re-sending even if the
        # network succeeded but its acknowledgement could not be persisted.
        _gap("acknowledgement", exc)


async def drain(bot, chat_id, *, limit=2, may_deliver=None):
    """One transport attempt per intent; uncertain outcomes are never retried."""
    if _DRAIN_LOCK.locked() or not await initialize(chat_id):
        return 0
    sent = 0
    scope = subscription_scope(chat_id)
    async with _DRAIN_LOCK:
        try:
            _STATUS["orphaned_attempts"] += await asyncio.to_thread(store.settle_orphans, scope, _now())
        except Exception as exc:
            _gap("orphan_settlement", exc)
            return 0
        for _ in range(min(max(int(limit), 0), 2)):
            if may_deliver is None or not may_deliver():
                break
            try:
                pending = await asyncio.to_thread(store.claim_pending, scope, _now(), limit=1)
            except Exception as exc:
                _gap("claim", exc)
                break
            if not pending:
                break
            intent = pending[0]
            # Also guard transport against an in-flight claim from an older
            # policy; terminal evidence remains available and cannot replay.
            if intent.get("symbol") not in alert.NOTIFICATION_SYMBOLS:
                await _finish(intent, "FAILED")
                _STATUS["failed"] += 1
                continue
            # Subscription may have changed during the DB await. This is the
            # last synchronous check before scheduling the transport call.
            if not may_deliver():
                try:
                    if not await asyncio.to_thread(store.release_unattempted, intent["intent_id"],
                                                  intent["attempt_token"], _now()):
                        raise RuntimeError("unattempted reservation was not released")
                except Exception as exc:
                    _gap("unattempted_release", exc)
                break
            expiry = intent["expires_at"]
            if not isinstance(expiry, datetime):
                expiry = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
            if expiry <= _now():
                await _finish(intent, "FAILED")
                _STATUS["failed"] += 1
                continue
            try:
                # A queued ZEC intent may have frozen its text before this
                # presentation change. Do not alter its immutable evidence.
                text = intent["text"]
                if not text.startswith(alert.THRESHOLD_HEADER):
                    text = alert.THRESHOLD_HEADER + text
                message = await asyncio.wait_for(bot.send_message(chat_id=chat_id, text=text,
                                                                 parse_mode="HTML"), timeout=20)
                message_id = getattr(message, "message_id", None)
                terminal = "DELIVERED" if type(message_id) is int and message_id > 0 else "UNKNOWN"
            except asyncio.CancelledError:
                # A process shutdown leaves a committed reservation, settled
                # UNKNOWN by the supervisor after restart, never replayed.
                raise
            except Exception as exc:
                terminal = "FAILED" if type(exc).__name__ in {"BadRequest", "Forbidden"} else "UNKNOWN"
            _STATUS[terminal.lower()] += 1
            if terminal == "DELIVERED":
                sent += 1
                _STATUS["last_delivery_at_utc"] = _now().isoformat()
            await _finish(intent, terminal)
            print(f"[dual-cvd65] delivery status={terminal} watch={intent['watch_scan_id']}", flush=True)
            if not _READY:
                break
    return sent
