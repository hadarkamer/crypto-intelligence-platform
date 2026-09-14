"""Four explicitly authorized experimental notifications, never trade execution."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import time

import manual_formula_alert as rules
import manual_formula_alert_store as store

_LOCK = asyncio.Lock()
_READY = False
_SCOPES = set()
_RETRY_AFTER = 0.0
_STATUS = {'version': rules.VERSION, 'rule_ids': list(rules.RULES),
           'experimental': True, 'statistically_qualified': False,
           'trade_execution': False, 'ready': False, 'last_error_type': None,
           'runs': 0, 'created_intents': 0, 'delivered': 0, 'unknown': 0,
           'failed': 0, 'cancelled': 0, 'last_summary': None}


def status():
    return deepcopy(_STATUS)


def _now():
    return datetime.now(timezone.utc)


def _gap(stage, exc):
    global _READY, _RETRY_AFTER
    _READY = False
    _RETRY_AFTER = time.monotonic() + 30
    _STATUS.update(ready=False, last_error_type=stage + ':' + type(exc).__name__)
    print(f'[manual-formulas] gap stage={stage} error_type={type(exc).__name__}', flush=True)


async def initialize(chat_id=None):
    global _READY
    if not _READY and time.monotonic() < _RETRY_AFTER:
        return False
    try:
        if not _READY:
            if not await asyncio.to_thread(store.schema_ready):
                raise RuntimeError('Existing settings/research source schema unavailable')
            _READY = True
        if chat_id is not None and int(chat_id) not in _SCOPES:
            result = await asyncio.to_thread(store.initialize_scope, int(chat_id))
            _SCOPES.add(int(chat_id))
            _STATUS['activated_at'] = result['activated_at']
    except Exception as exc:
        _gap('initialize', exc)
        return False
    _STATUS.update(ready=True, last_error_type=None)
    return True


async def _finish(chat_id, intent, terminal, message_id=None):
    try:
        saved = await asyncio.to_thread(store.finish, chat_id, intent['intent_id'],
                                        intent['attempt_token'], terminal, _now(), message_id=message_id)
        if not saved:
            raise RuntimeError('Attempt acknowledgement not persisted')
    except Exception as exc:
        # IN_FLIGHT remains durable and will become UNKNOWN, never replayed.
        _gap('acknowledgement', exc)


async def run_once(bot, chat_id, *, may_deliver=None):
    """Bounded collection and two attempts; destination checked after every await."""
    if _LOCK.locked() or may_deliver is None or not may_deliver():
        return 0
    if not await initialize(chat_id):
        return 0
    sent = 0
    async with _LOCK:
        if not may_deliver():
            return 0
        try:
            result = await asyncio.to_thread(store.collect, chat_id, _now())
            _STATUS['last_summary'] = result
            _STATUS['created_intents'] += result.get('created_intents', 0)
            _STATUS['runs'] += 1
        except Exception as exc:
            _gap('collect', exc)
            return 0
        for _ in range(2):
            if not may_deliver():
                break
            try:
                intent = await asyncio.to_thread(store.claim, chat_id, _now())
            except Exception as exc:
                _gap('claim', exc)
                break
            if intent is None:
                break
            # This synchronous gate is immediately before entering transport.
            if not may_deliver() or store.utc(intent['expires_at']) <= _now():
                await _finish(chat_id, intent, 'CANCELLED')
                _STATUS['cancelled'] += 1
                break
            message_id = None
            try:
                message = await asyncio.wait_for(bot.send_message(chat_id=int(chat_id), text=intent['text'], parse_mode='HTML'), timeout=20)
                message_id = getattr(message, 'message_id', None)
                terminal = 'DELIVERED' if type(message_id) is int and message_id > 0 else 'UNKNOWN'
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                terminal = 'FAILED' if type(exc).__name__ in {'BadRequest', 'Forbidden'} else 'UNKNOWN'
            await _finish(chat_id, intent, terminal, message_id)
            _STATUS[terminal.lower()] += 1
            if terminal == 'DELIVERED':
                sent += 1
                _STATUS['last_delivery_at'] = _now().isoformat()
            print(f'[manual-formulas] delivery rule={intent["payload"]["rule_id"]} status={terminal}', flush=True)
            if not _READY:
                break
    return sent
