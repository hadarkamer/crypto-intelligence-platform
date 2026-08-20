"""Apply the candidate-only Research Event dry-run hooks to main.py.

This script is intentionally narrow and idempotent. It refuses to edit when an
expected source anchor is missing, so upstream main.py changes cannot silently
produce a misplaced hook.
"""
from pathlib import Path

PATH = Path("main.py")
text = PATH.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    if new in text:
        print(f"{label}: already applied")
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one anchor, found {count}")
    text = text.replace(old, new, 1)
    print(f"{label}: applied")


replace_once(
    "import market_confidence_engine\nfrom collections import defaultdict",
    "import market_confidence_engine\nimport research_event_runtime\nfrom collections import defaultdict",
    "research runtime import",
)

replace_once(
    '''async def _send_alert_with_confirmation(bot, chat_id: int, card: str, item: Dict[str, Any]) -> None:\n    await bot.send_message(chat_id=chat_id, text=card, parse_mode="HTML")\n    for separate in _special_transition_messages(item):''',
    '''async def _send_alert_with_confirmation(bot, chat_id: int, card: str, item: Dict[str, Any]) -> None:\n    await bot.send_message(chat_id=chat_id, text=card, parse_mode="HTML")\n    # Research sidecar runs only after the normal card was actually delivered.\n    # It is memory-only in this Candidate and may never block Telegram alerts.\n    try:\n        research_event_runtime.capture_sent_maxpain(item)\n    except Exception as exc:\n        print(f"[research-dry-run] sent alert hook failed: {exc!r}", flush=True)\n    for separate in _special_transition_messages(item):''',
    "sent Max-Pain alert hook",
)

replace_once(
    '''        displayable_items = [\n            item\n            for item in all_items\n            if _is_displayable_opportunity(item)\n        ]\n        # A Magnet-only subscriber must not consume regular or combined alert''',
    '''        displayable_items = [\n            item\n            for item in all_items\n            if _is_displayable_opportunity(item)\n        ]\n        # Mirror the same independent transition inputs into the Candidate\n        # research tracker before Telegram formatting mutates its own state.\n        # This also preserves weakening/reset transitions for delayed/inverse\n        # research, but performs no database writes.\n        if general_enabled:\n            try:\n                research_event_runtime.capture_special_transitions(displayable_items)\n            except Exception as exc:\n                print(f"[research-dry-run] special transition hook failed: {exc!r}", flush=True)\n        # A Magnet-only subscriber must not consume regular or combined alert''',
    "Watch special-transition hook",
)

replace_once(
    '''        if not previous_signals or current_signals - previous_signals:\n            messages.append(_combined_confirmation_message(candidate))\n        COMBINED_CONFIRMATION_STATE[key] = current_signals''',
    '''        if not previous_signals or current_signals - previous_signals:\n            messages.append(_combined_confirmation_message(candidate))\n            # Capture only the exact Combined transition already approved by\n            # the existing bot logic; active unchanged setups are not duplicated.\n            try:\n                research_event_runtime.capture_combined_confirmation(candidate)\n            except Exception as exc:\n                print(f"[research-dry-run] combined hook failed: {exc!r}", flush=True)\n        COMBINED_CONFIRMATION_STATE[key] = current_signals''',
    "Combined Confirmation hook",
)

replace_once(
    '''        for message in await _build_magnet_report(\n            symbol,\n            rows,\n            derivatives_snapshot=derivatives_snapshot,\n        ):''',
    '''        # Reuse this exact shared Watch generation for research. No new\n        # scrape, DB refresh or market-data request is started by this hook.\n        try:\n            research_event_runtime.capture_magnet_watch_symbol(\n                symbol,\n                rows,\n                derivatives_snapshot,\n            )\n        except Exception as exc:\n            print(f"[research-dry-run] magnet hook failed {symbol}: {exc!r}", flush=True)\n        for message in await _build_magnet_report(\n            symbol,\n            rows,\n            derivatives_snapshot=derivatives_snapshot,\n        ):''',
    "Magnet Watch hook",
)

PATH.write_text(text, encoding="utf-8")
print("Research Event runtime hooks: ready")
