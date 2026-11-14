"""Existing Telegram supervisor adapter for saved approved plans.

Claims commit before transport. Telegram failure/unknown acknowledgement never
modifies the execution outbox and never sends or repeats an exchange order.
"""
import asyncio
from copy import deepcopy
import time

import selected_six_formula_store as store
import selected_six_formula_source as source
from watch_transition_store import _connect
from watch_transition_delivery import subscription_scope

_LOCKS = {}
_STATUS = dict(version=store.VERSION, ready=False, last_error_type=None,
               delivered=0, unknown=0, captured=0)


def status():
    import approved_alert_contract
    return dict(deepcopy(_STATUS), new_entry_roster=approved_alert_contract.new_entry_roster(),
                spec_sha256=store.SPEC_SHA256, config_sha256=store.CONFIG_SHA256)


def _now():
    return int(time.time() * 1000)


def _initialize(chat_id):
    with _connect() as conn:
        store.initialize(conn, subscription_scope(chat_id), _now())


async def initialize(chat_id):
    if chat_id is None:
        return False
    try:
        await asyncio.to_thread(_initialize, chat_id)
        _STATUS.update(ready=True, last_error_type=None)
        return True
    except Exception as exc:
        _STATUS.update(ready=False, last_error_type=type(exc).__name__)
        return False


def _capture(chat_id, score_bundle, decision_bundle, raw_snapshot, received_at_ms):
    with _connect() as conn:
        return source.capture_watch(conn, subscription_scope(chat_id), score_bundle,
                                    decision_bundle, raw_snapshot, received_at_ms)


async def capture_watch(chat_id, score_bundle, decision_bundle, raw_snapshot, received_at_ms=None):
    # Frozen receipt time is selected before thread scheduling; replay/callback
    # time cannot change the existing captured source identity.
    received = _now() if received_at_ms is None else received_at_ms
    try:
        result = await asyncio.to_thread(_capture, chat_id, score_bundle,
                                         decision_bundle, raw_snapshot, received)
        _STATUS['captured'] += result['added']
        _STATUS['last_mapping_errors'] = result['errors']
        return result
    except Exception as exc:
        _STATUS['last_error_type'] = type(exc).__name__
        return dict(added=0, error=type(exc).__name__)


def _claim(chat_id):
    with _connect() as conn:
        return store.claim(conn, subscription_scope(chat_id), _now())


def _finish(chat_id, intent, outcome):
    with _connect() as conn:
        return store.finish(conn, subscription_scope(chat_id), intent['intent_id'],
                            intent['attempt_token'], outcome, _now())


async def run_once(bot, chat_id, *, may_deliver=None):
    import alert_delivery_policy
    if may_deliver is None or not may_deliver():
        return 0
    lock = _LOCKS.setdefault(int(chat_id), asyncio.Lock())
    if lock.locked():
        return 0
    sent = 0
    async with lock:
        for _ in range(2):
            if not may_deliver():
                break
            try:
                intent = await asyncio.to_thread(_claim, chat_id)
            except Exception as exc:
                _STATUS['last_error_type'] = type(exc).__name__
                break
            if intent is None:
                break
            outcome = 'UNKNOWN'
            if (may_deliver() and _now() < intent['expires_ms']
                    and alert_delivery_policy.selected_rule_enabled(intent['payload']['rule_id'])):
                try:
                    reply = await asyncio.wait_for(bot.send_message(chat_id=int(chat_id), text=intent['text']), 20)
                    if type(getattr(reply, 'message_id', None)) is int and reply.message_id > 0:
                        outcome = 'DELIVERED'
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _STATUS['last_error_type'] = type(exc).__name__
            try:
                await asyncio.to_thread(_finish, chat_id, intent, outcome)
            except Exception as exc:
                # IN_FLIGHT becomes UNKNOWN after restart; never resend an
                # attempt whose Telegram acknowledgement may have been lost.
                _STATUS['last_error_type'] = type(exc).__name__
            _STATUS[outcome.lower()] += 1
            sent += int(outcome == 'DELIVERED')
    return sent
