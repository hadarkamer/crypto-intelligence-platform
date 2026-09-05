"""Regressions for the isolated experimental Formula Telegram opt-in."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parent


class _CommandHandler:
    def __init__(self, command, callback):
        self.commands = frozenset((str(command),))
        self.callback = callback


class _ContextTypes:
    DEFAULT_TYPE = object


telegram = types.ModuleType("telegram")
telegram.Update = object
telegram_ext = types.ModuleType("telegram.ext")
telegram_ext.Application = object
telegram_ext.CommandHandler = _CommandHandler
telegram_ext.ContextTypes = _ContextTypes
sys.modules["telegram"] = telegram
sys.modules["telegram.ext"] = telegram_ext


def _stub(name: str, **attributes):
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


live_calls: list[tuple[int, bool, int | None]] = []
experimental_calls: list[tuple[int, bool, int | None]] = []
experimental_schema = {"schema_present": True, "ready": True}
experimental_active = {"value": False}
formula_runtime = {
    "discovery_enabled": True,
    "shadow_enabled": True,
    "live_alerts_enabled": False,
    "experimental_alerts_enabled": True,
    "experimental_running": True,
    "dataset_mode": "auto",
    "live_delivery_gate": {"telegram_delivery_connected": True},
    "experimental_delivery_gate": {
        "telegram_delivery_connected": True
    },
}


def _live_set(chat_id, *, active, requested_by_user_id=None):
    live_calls.append((int(chat_id), bool(active), requested_by_user_id))
    return {"active": bool(active)}


def _experimental_set(chat_id, *, active, requested_by_user_id=None):
    experimental_calls.append(
        (int(chat_id), bool(active), requested_by_user_id)
    )
    experimental_active["value"] = bool(active)
    return {"active": bool(active)}


async def _unused_ask(*_args, **_kwargs):
    return "unused"


_stub(
    "ai_agent",
    status=lambda: {"configured": True, "model": "test", "tools": []},
    ask=_unused_ask,
    reset_conversation=lambda _conversation_id: None,
)
_stub(
    "ai_alert_research",
    archive_status=lambda: {"schema_present": True, "delivered_alerts": 0},
)
_stub("market_session_baseline", is_active_market=lambda _now: True)
_stub(
    "research_event_runtime",
    status=lambda: {"persistence": {"enabled": True}},
)
_stub(
    "research_outcome_worker",
    WORKER=types.SimpleNamespace(
        status=lambda: {
            "enabled": True,
            "price_paths": {"interval": "1m"},
            "method": "test",
        }
    ),
)
_stub(
    "research_formula_store",
    schema_status=lambda: {
        "schema_present": True,
        "shadow_formulas": 2,
        "live_formulas": 1,
    },
    set_alert_subscription=_live_set,
    alert_subscription_status=lambda _chat_id: {"active": True},
)
_stub(
    "research_formula_worker",
    WORKER=types.SimpleNamespace(
        status=lambda: dict(formula_runtime)
    ),
)
_stub(
    "research_historical_replay",
    status=lambda: {"schema_present": True, "coverage": {}},
)
_stub(
    "research_stage4_experimental_store",
    schema_status=lambda: dict(experimental_schema),
    set_alert_subscription=_experimental_set,
    alert_subscription_status=lambda _chat_id: {
        "active": experimental_active["value"]
    },
)

import ai_telegram


class _Message:
    def __init__(self) -> None:
        self.replies: list[str] = []
        self.chat_id = 4242

    async def reply_text(self, text: str) -> None:
        self.replies.append(str(text))


class _Update:
    def __init__(self) -> None:
        self.message = _Message()
        self.effective_chat = types.SimpleNamespace(id=4242)
        self.effective_user = types.SimpleNamespace(id=73)


class _Context:
    args: list[str] = []


class _Application:
    def __init__(self) -> None:
        self.handlers: list[_CommandHandler] = []

    def add_handler(self, handler: _CommandHandler) -> None:
        self.handlers.append(handler)


async def _run_commands() -> None:
    help_update = _Update()
    await ai_telegram.ai_cmd(help_update, _Context())
    help_text = help_update.message.replies[-1]
    assert "/ai_alerts_on" in help_text
    assert "/ai_experimental_on" in help_text
    assert "ניסיוני, לא מאושר למסחר" in help_text
    assert "אינו מבצע מסחר" in help_text

    on_update = _Update()
    await ai_telegram.ai_experimental_on_cmd(on_update, _Context())
    assert experimental_calls == [(4242, True, 73)]
    assert not live_calls
    on_text = on_update.message.replies[-1]
    assert "נרשם במפורש" in on_text
    assert "ניסיוני, לא מאושר למסחר" in on_text
    assert "לא נדרש אישור אנושי נפרד לכל נוסחה" in on_text
    assert "אינה הופכת ל-LIVE" in on_text
    assert "אינו מבצע מסחר" in on_text

    missing_user_update = _Update()
    missing_user_update.effective_user = None
    await ai_telegram.ai_experimental_on_cmd(missing_user_update, _Context())
    assert experimental_calls == [(4242, True, 73)]
    assert "לא ניתן לאמת משתמש Telegram" in (
        missing_user_update.message.replies[-1]
    )

    experimental_status_update = _Update()
    await ai_telegram.ai_experimental_status_cmd(
        experimental_status_update, _Context()
    )
    experimental_status_text = experimental_status_update.message.replies[-1]
    assert "ניסיוני, לא מאושר למסחר" in experimental_status_text
    assert "הצ'אט הזה: פעיל" in experimental_status_text
    assert "מנוע ניסיוני: פעיל" in experimental_status_text
    assert "Telegram מחובר: כן" in experimental_status_text
    assert "אינו הופך נוסחה ל-LIVE" in experimental_status_text
    assert "אינו מבצע מסחר" in experimental_status_text

    formula_runtime["experimental_running"] = False
    stopped_status_update = _Update()
    await ai_telegram.ai_experimental_status_cmd(
        stopped_status_update, _Context()
    )
    assert "מנוע ניסיוני: כבוי" in stopped_status_update.message.replies[-1]

    combined_status_update = _Update()
    await ai_telegram.ai_alerts_status_cmd(combined_status_update, _Context())
    combined_status_text = combined_status_update.message.replies[-1]
    assert "מסלול LIVE מאושר למסירה" in combined_status_text
    assert "מסלול ניסיוני — ניסיוני, לא מאושר למסחר" in combined_status_text
    assert "מנוע ניסיוני: כבוי" in combined_status_text
    assert "אין הפיכת נוסחה ל-LIVE ואין מסחר" in combined_status_text

    ai_status_update = _Update()
    await ai_telegram.ai_status_cmd(ai_status_update, _Context())
    ai_status_text = ai_status_update.message.replies[-1]
    assert "מסלול LIVE מאושר למסירה" in ai_status_text
    assert "מסלול ניסיוני: כבוי" in ai_status_text
    assert "ניסיוני, לא מאושר למסחר" in ai_status_text
    assert "אין אישור אנושי נפרד לכל נוסחה" in ai_status_text

    formula_runtime["experimental_running"] = True

    off_update = _Update()
    await ai_telegram.ai_experimental_off_cmd(off_update, _Context())
    assert experimental_calls[-1] == (4242, False, 73)
    assert not live_calls
    assert "מסלול LIVE, אם קיים, לא השתנה" in off_update.message.replies[-1]

    live_on_update = _Update()
    await ai_telegram.ai_alerts_on_cmd(live_on_update, _Context())
    assert live_calls == [(4242, True, 73)]
    assert experimental_calls == [(4242, True, 73), (4242, False, 73)]
    assert "אינו מפעיל את המסלול הניסיוני" in live_on_update.message.replies[-1]

    experimental_schema["schema_present"] = True
    experimental_schema["ready"] = False
    blocked_update = _Update()
    await ai_telegram.ai_experimental_on_cmd(blocked_update, _Context())
    assert experimental_calls == [(4242, True, 73), (4242, False, 73)]
    assert "עדיין לא הותקן" in blocked_update.message.replies[-1]

    blocked_off_update = _Update()
    await ai_telegram.ai_experimental_off_cmd(blocked_off_update, _Context())
    assert experimental_calls == [(4242, True, 73), (4242, False, 73)]
    assert "עדיין לא הותקן" in blocked_off_update.message.replies[-1]

    unsafe_status_update = _Update()
    await ai_telegram.ai_experimental_status_cmd(
        unsafe_status_update, _Context()
    )
    unsafe_status_text = unsafe_status_update.message.replies[-1]
    assert "מאגר: לא הותקן" in unsafe_status_text
    assert "הצ'אט הזה: כבוי" in unsafe_status_text

    unsafe_combined_update = _Update()
    await ai_telegram.ai_alerts_status_cmd(
        unsafe_combined_update, _Context()
    )
    assert "מאגר: לא הותקן" in unsafe_combined_update.message.replies[-1]

    unsafe_ai_status_update = _Update()
    await ai_telegram.ai_status_cmd(unsafe_ai_status_update, _Context())
    assert "מאגר: לא הותקן" in unsafe_ai_status_update.message.replies[-1]


def run() -> None:
    source = (ROOT / "ai_telegram.py").read_text(encoding="utf-8")
    assert "import research_stage4_experimental_store" in source
    assert "research_formula_store.set_alert_subscription" in source
    assert "research_stage4_experimental_store.set_alert_subscription" in source

    asyncio.run(_run_commands())

    app = _Application()
    ai_telegram.register_ai_handlers(app)
    commands = {
        command
        for handler in app.handlers
        for command in handler.commands
    }
    assert {
        "ai_experimental_on",
        "ai_experimental_off",
        "ai_experimental_status",
    }.issubset(commands)
    print("research_experimental_telegram_commands_selftest: PASS")


if __name__ == "__main__":
    run()
