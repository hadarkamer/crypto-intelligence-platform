"""Deliver frozen MP65/formula intents once, preserving original source lineage."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import time

import maxpain_cvd_short_alert
import research_event_runtime
import watch_transition_store as store

_READY = False
_NEXT_INIT = 0.0
_DRAIN_LOCK = asyncio.Lock()
_STATUS = {
    "scope": "GENERAL_WATCH_MP65_AND_FORMULA_V1",
    "policy_version": store.POLICY_VERSION,
    "delivery_policy_version": store.DELIVERY_POLICY_VERSION,
    "ready": False, "evidence_gaps": 0, "last_error_type": None,
    "last_record_status": "NOT_OBSERVED", "last_watch_scan_id": None,
    "last_recorded_at_utc": None, "last_counts": {},
    "created_intents": 0, "delivered": 0, "unknown": 0, "failed": 0,
    "last_delivery_at_utc": None, "orphaned_attempts": 0, "last_cycle": {},
}


def status():
    return deepcopy(_STATUS)


def cycle_result(watch_scan_id):
    cycle = dict(_STATUS["last_cycle"])
    if cycle.get("watch_scan_id") != watch_scan_id:
        return {"status": "NOT_RECORDED", "watch_scan_id": watch_scan_id}
    incomplete = (cycle["evidence_gaps"] or cycle["unknown"] or cycle["failed"]
                  or cycle["record_status"] not in {"RECORDED", "REPLAY"})
    cycle["status"] = ("INCOMPLETE" if incomplete else
                       "PENDING" if cycle["created_intents"] > cycle["delivered"] else "COMPLETE")
    return cycle


def subscription_scope(chat_id):
    # Keep recipient identifiers out of operational logs and public health.
    digest = hashlib.sha256(str(int(chat_id)).encode()).hexdigest()
    return "general-watch:" + digest


def _gap(stage, exc, *, database=False):
    global _READY, _NEXT_INIT
    _STATUS["evidence_gaps"] += 1
    if _STATUS["last_cycle"]:
        _STATUS["last_cycle"]["evidence_gaps"] += 1
    _STATUS["last_error_type"] = stage + ":" + type(exc).__name__
    if database:
        _READY = False
        _NEXT_INIT = time.monotonic() + 60
        _STATUS["ready"] = False
    print(f"[watch-transitions] gap stage={stage} error_type={type(exc).__name__}", flush=True)


async def initialize():
    global _READY, _NEXT_INIT
    if _READY:
        return True
    if time.monotonic() < _NEXT_INIT:
        return False
    try:
        await asyncio.to_thread(store.init_schema)
    except Exception as exc:
        _gap("schema", exc, database=True)
        return False
    _READY = True
    _STATUS["ready"] = True
    return True


async def record_watch(chat_id, items, *, watch_scan_id, decision_time, render_score65):
    """Commit crossings and their frozen messages before any Watch messages."""
    _STATUS["last_watch_scan_id"] = watch_scan_id
    if _STATUS["last_cycle"].get("watch_scan_id") != watch_scan_id:
        _STATUS["last_cycle"] = {
            "watch_scan_id": watch_scan_id, "record_status": "PERSISTENCE_UNAVAILABLE",
            "created_intents": 0, "delivered": 0, "unknown": 0, "failed": 0, "evidence_gaps": 0,
        }
    if not await initialize():
        _STATUS["last_record_status"] = "PERSISTENCE_UNAVAILABLE"
        return None
    context = research_event_runtime.watch_context_snapshot()
    source_time = decision_time.isoformat()

    def intent_factory(crossing_items):
        intents = [{
            "kind": "MAX_PAIN_SCORE_65", "signal_key": store.signal_key(item),
            "payload": {"text": render_score65(item), "item": item,
                        "watch_context": context, "decision_time": source_time},
        } for item in crossing_items]
        # This is the existing V1 selector: freeze the first both-CVD match
        # before testing short-family quality, with no later substitution.
        for match in maxpain_cvd_short_alert.select_matches(crossing_items):
            intents.append({
                "kind": maxpain_cvd_short_alert.FORMULA_ID,
                "signal_key": store.signal_key(match.item),
                "payload": {"text": maxpain_cvd_short_alert.render_message(match, decision_time),
                            "match": asdict(match), "watch_context": context,
                            "decision_time": source_time},
            })
        return intents

    try:
        result = await asyncio.to_thread(
            store.record_cycle, subscription_scope(chat_id), watch_scan_id,
            decision_time, items, intent_factory,
        )
    except Exception as exc:
        _STATUS["last_record_status"] = "EVIDENCE_GAP"
        _gap("record", exc, database=True)
        return None
    replay = result.get("idempotent_existing", False)
    _STATUS["last_record_status"] = (
        "REPLAY" if replay else "OLDER_IGNORED" if result.get("rejected_older") else "RECORDED"
    )
    _STATUS["last_recorded_at_utc"] = datetime.now(timezone.utc).isoformat()
    _STATUS["last_cycle"]["record_status"] = _STATUS["last_record_status"]
    _STATUS["last_cycle"]["created_intents"] = len(result.get("intents", []))
    _STATUS["last_counts"] = result.get("counts", {})
    _STATUS["created_intents"] += 0 if replay else len(result.get("intents", []))
    if not replay:
        for item in result.get("resets", []):
            try:
                research_event_runtime.capture_score65_reset(item, event_time=decision_time, persist=True)
            except Exception as exc:
                _gap("reset_capture", exc)
    print(
        f"[watch-transitions] recorded watch={watch_scan_id} "
        f"status={_STATUS['last_record_status']} intents={len(result.get('intents', []))} "
        f"counts={_STATUS['last_counts']}", flush=True,
    )
    return result


def _capture(intent, delivery_status, attempted_at, delivered_at):
    payload = intent["payload"]
    context = dict(payload.get("watch_context") or {})
    context.update({"watch_scan_id": intent["watch_scan_id"],
                    "watch_transition_intent_id": intent["intent_id"],
                    "watch_transition_policy": intent["policy_version"],
                    "watch_transition_episode": intent["episode"]})
    token = research_event_runtime.set_watch_context(**context)
    try:
        kwargs = dict(event_time=payload["decision_time"], persist=True,
                      delivery_status=delivery_status,
                      delivery_attempted_at_utc=attempted_at, delivered_at_utc=delivered_at)
        if intent["kind"] == "MAX_PAIN_SCORE_65":
            research_event_runtime.capture_score65_delivery(payload["item"], **kwargs)
        else:
            match = maxpain_cvd_short_alert.FormulaMatch(**payload["match"])
            research_event_runtime.capture_formula_match(match, **kwargs)
    finally:
        research_event_runtime.reset_watch_context(token)


async def drain(bot, chat_id, *, limit=32, may_deliver=None, kinds=None, wait_for_lock=False):
    """Claim immediately before one attempt. UNKNOWN/FAILED are terminal.

    Only unattempted, unexpired PENDING messages can recover after a restart.
    A crashed IN_FLIGHT attempt is settled UNKNOWN by the store, never resent.
    """
    if (not wait_for_lock and _DRAIN_LOCK.locked()) or not await initialize():
        return 0
    formula_sent = 0
    async with _DRAIN_LOCK:
        if may_deliver is not None and not may_deliver():
            return 0
        try:
            _STATUS["orphaned_attempts"] += await asyncio.to_thread(
                store.settle_orphans, subscription_scope(chat_id), datetime.now(timezone.utc),
            )
        except Exception as exc:
            _gap("orphan_settlement", exc, database=True)
            return 0
        for _ in range(min(max(int(limit), 0), 128)):
            if may_deliver is not None and not may_deliver():
                break
            try:
                pending = await asyncio.to_thread(
                    store.claim_pending, subscription_scope(chat_id), datetime.now(timezone.utc), limit=1,
                    **({"kinds": kinds} if kinds is not None else {}),
                )
            except Exception as exc:
                _gap("claim", exc, database=True)
                break
            if not pending:
                break
            intent = pending[0]
            if may_deliver is not None and not may_deliver():
                # Eligibility can change while the database claim is awaited.
                # No network attempt has begun: release only this reservation.
                try:
                    released = await asyncio.to_thread(
                        store.release_unattempted, intent["intent_id"], intent["attempt_token"],
                    )
                    if not released:
                        raise RuntimeError("unattempted reservation was not released")
                except Exception as exc:
                    _gap("unattempted_release", exc, database=True)
                break
            attempted_at = intent["attempted_at"]
            delivered_at = None
            error_type = None
            try:
                await bot.send_message(chat_id=chat_id, text=intent["payload"]["text"], parse_mode="HTML")
                delivered_at = datetime.now(timezone.utc)
                terminal = "DELIVERED"
            except asyncio.CancelledError:
                # IN_FLIGHT remains a durable uncertain attempt if process stops.
                raise
            except Exception as exc:
                from telegram.error import BadRequest, Forbidden
                terminal = "FAILED" if isinstance(exc, (BadRequest, Forbidden)) else "UNKNOWN"
                error_type = type(exc).__name__
            _STATUS[terminal.lower()] += 1
            if _STATUS["last_cycle"].get("watch_scan_id") == intent["watch_scan_id"]:
                _STATUS["last_cycle"][terminal.lower()] += 1
            if terminal == "DELIVERED":
                _STATUS["last_delivery_at_utc"] = delivered_at.isoformat()
                formula_sent += int(intent["kind"] == maxpain_cvd_short_alert.FORMULA_ID)
            try:
                updated = await asyncio.to_thread(
                    store.complete_attempt, intent["intent_id"], intent["attempt_token"],
                    status=terminal, attempted_at=attempted_at,
                    acknowledged_at=delivered_at, error_type=error_type,
                )
                if not updated:
                    raise RuntimeError("attempt acknowledgement was not persisted")
            except Exception as exc:
                # A Telegram acknowledgement is still real if the DB write fails.
                # Capture it, leave durable IN_FLIGHT uncertain, never resend it.
                _gap("acknowledgement", exc, database=True)
            try:
                _capture(intent, "DELIVERY_FAILED" if terminal == "FAILED" else terminal,
                         attempted_at, delivered_at)
            except Exception as exc:
                _gap("delivery_capture", exc)
            print(f"[watch-transitions] delivery kind={intent['kind']} status={terminal} "
                  f"watch={intent['watch_scan_id']} intent={intent['intent_id']}", flush=True)
            if not _READY:
                break
    return formula_sent
