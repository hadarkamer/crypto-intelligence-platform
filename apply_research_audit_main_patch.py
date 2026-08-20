"""One-time idempotent patch for candidate-only Research Event hook semantics.

This script is used by a temporary GitHub Actions workflow on ai-agent-candidate.
It never targets production main branch and only changes Research sidecar hooks.
"""
from pathlib import Path

path = Path("main.py")
text = path.read_text(encoding="utf-8")
original = text

combined_old = '''    candidates = _combined_confirmation_candidates(items, rows)\n    active_keys = {str(candidate["key"]) for candidate in candidates}\n'''
combined_new = '''    candidates = _combined_confirmation_candidates(items, rows)\n    # Research-only lifecycle tracking: preserve Combined weakening, component\n    # loss and deactivation even when Telegram correctly emits no new alert.\n    # This sidecar does not change COMBINED_CONFIRMATION_STATE or strategy logic.\n    try:\n        research_event_runtime.capture_combined_state_changes(candidates)\n    except Exception as exc:\n        print(f"[research-dry-run] combined state hook failed: {exc!r}", flush=True)\n    active_keys = {str(candidate["key"]) for candidate in candidates}\n'''
if combined_new not in text:
    if combined_old not in text:
        raise SystemExit("Combined hook anchor not found; refusing unsafe patch")
    text = text.replace(combined_old, combined_new, 1)

send_old = '''async def _send_alert_with_confirmation(bot, chat_id: int, card: str, item: Dict[str, Any]) -> None:\n    await bot.send_message(chat_id=chat_id, text=card, parse_mode="HTML")\n    # Research sidecar runs only after the normal card was actually delivered.\n    # It is memory-only in this Candidate and may never block Telegram alerts.\n    try:\n        research_event_runtime.capture_sent_maxpain(item)\n    except Exception as exc:\n        print(f"[research-dry-run] sent alert hook failed: {exc!r}", flush=True)\n'''
send_new = '''async def _send_alert_with_confirmation(bot, chat_id: int, card: str, item: Dict[str, Any]) -> None:\n    # Freeze the Research Event decision timestamp BEFORE Telegram network latency.\n    # The sidecar is still emitted only after successful delivery, but its market/news\n    # join anchor remains the exact decision/observation time.\n    decision_time = datetime.now(timezone.utc)\n    await bot.send_message(chat_id=chat_id, text=card, parse_mode="HTML")\n    try:\n        research_event_runtime.capture_sent_maxpain(item, event_time=decision_time)\n    except Exception as exc:\n        print(f"[research-dry-run] sent alert hook failed: {exc!r}", flush=True)\n'''
if send_new not in text:
    if send_old not in text:
        raise SystemExit("Max-Pain send hook anchor not found; refusing unsafe patch")
    text = text.replace(send_old, send_new, 1)

if text != original:
    path.write_text(text, encoding="utf-8")
    print("Research audit main patch: APPLIED")
else:
    print("Research audit main patch: ALREADY APPLIED")
