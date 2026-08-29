"""Conservative importer for a Telegram Desktop JSON export.

Imported messages stay in ``research_legacy_alert_messages``.  They are never
inserted into ``research_events`` because an old Telegram message does not
contain the complete immutable engine snapshot required for formula training.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, Iterable, Mapping

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


_TRUE = {"1", "true", "yes", "on"}
_KNOWN_SYMBOLS = ("BTC", "ETH", "SOL", "HYPE", "DOGE", "BNB", "XRP", "ZEC")


def _database_url() -> str:
    dedicated = os.getenv("RESEARCH_DATABASE_URL", "").strip()
    use_primary = os.getenv("RESEARCH_USE_PRIMARY_DATABASE", "").strip().lower() in _TRUE
    if dedicated:
        return dedicated
    if use_primary:
        return os.getenv("DATABASE_URL", "").strip()
    return ""


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, Mapping):
                parts.append(str(item.get("text") or ""))
        return "".join(parts)
    return str(value or "")


def _utc(message: Mapping[str, Any]) -> datetime:
    unix_value = message.get("date_unixtime")
    if unix_value not in (None, ""):
        return datetime.fromtimestamp(int(unix_value), tz=timezone.utc)
    parsed = datetime.fromisoformat(str(message.get("date") or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iter_chats(payload: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    if isinstance(payload.get("messages"), list):
        yield str(payload.get("name") or payload.get("id") or "telegram_export"), payload
    chats = payload.get("chats")
    if isinstance(chats, Mapping):
        chats = chats.get("list")
    if isinstance(chats, list):
        for chat in chats:
            if isinstance(chat, Mapping) and isinstance(chat.get("messages"), list):
                yield str(chat.get("name") or chat.get("id") or "telegram_chat"), chat


def _parse_hint(text: str) -> Dict[str, Any]:
    upper = text.upper()
    symbols = [symbol for symbol in _KNOWN_SYMBOLS if re.search(rf"\b{symbol}\b", upper)]
    direction = None
    if re.search(r"\bLONG\b|לונג", upper):
        direction = "LONG"
    if re.search(r"\bSHORT\b|שורט", upper):
        direction = "SHORT" if direction is None else None
    event_type = None
    for pattern, label in (
        (r"COMBINED|משולב", "COMBINED_CONFIRMATION"),
        (r"MAGNET|מגנט", "MAGNET"),
        (r"MAX\s*PAIN|מאקס\s*פיין", "MAX_PAIN"),
        (r"SPOT\s*CVD", "SPOT_CVD_HIGH"),
        (r"FUTURES\s*CVD", "FUTURES_CVD_HIGH"),
        (r"\bOI\b", "OI_PRICE_HIGH"),
    ):
        if re.search(pattern, upper):
            event_type = label
            break
    return {
        "symbol": symbols[0] if len(symbols) == 1 else None,
        "direction": direction,
        "event_type": event_type,
        "all_symbol_hints": symbols,
    }


def _rows(payload: Mapping[str, Any]) -> Iterable[Dict[str, Any]]:
    for chat_name, chat in _iter_chats(payload):
        for message in chat.get("messages") or []:
            if not isinstance(message, Mapping) or message.get("type") not in {None, "message"}:
                continue
            text = _text(message.get("text")).strip()
            if not text:
                continue
            hint = _parse_hint(text)
            if not any((hint["symbol"], hint["direction"], hint["event_type"])):
                continue
            timestamp = _utc(message)
            source_message_id = str(message.get("id") or "")
            fingerprint_payload = "\n".join(
                (chat_name, source_message_id, timestamp.isoformat(), text)
            )
            fingerprint = hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()
            parsed_count = sum(
                value is not None
                for value in (hint["symbol"], hint["direction"], hint["event_type"])
            )
            yield {
                "source_fingerprint": fingerprint,
                "source_chat": chat_name,
                "source_message_id": source_message_id,
                "message_time_utc": timestamp,
                "message_text": text,
                "parsed_symbol": hint["symbol"],
                "parsed_direction": hint["direction"],
                "parsed_event_type": hint["event_type"],
                "parse_status": "PARTIAL" if parsed_count else "UNPARSED",
                "parsed_metadata": json.dumps(
                    {
                        "parser_version": "telegram-legacy-hint-v1",
                        "all_symbol_hints": hint["all_symbol_hints"],
                        "training_eligible": False,
                        "reason": "legacy message lacks a complete immutable engine snapshot",
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }


def import_export(path: Path, *, apply: bool = False) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Telegram export root must be a JSON object")
    rows = list(_rows(payload))
    summary = {
        "source": str(path),
        "candidate_messages": len(rows),
        "applied": False,
        "inserted": 0,
        "training_eligible": False,
    }
    if not apply:
        return summary
    if os.getenv("RESEARCH_LEGACY_IMPORT_APPLY", "").strip().lower() not in _TRUE:
        raise RuntimeError(
            "Refusing import: set RESEARCH_LEGACY_IMPORT_APPLY=1 explicitly"
        )
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("Research database is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is unavailable")
    inserted = 0
    with psycopg.connect(database_url, connect_timeout=5) as conn:
        for row in rows:
            result = conn.execute(
                """
                INSERT INTO research_legacy_alert_messages (
                    source_fingerprint, source_chat, source_message_id,
                    message_time_utc, message_text, parsed_symbol,
                    parsed_direction, parsed_event_type, parse_status,
                    parsed_metadata
                ) VALUES (
                    %(source_fingerprint)s, %(source_chat)s, %(source_message_id)s,
                    %(message_time_utc)s, %(message_text)s, %(parsed_symbol)s,
                    %(parsed_direction)s, %(parsed_event_type)s, %(parse_status)s,
                    %(parsed_metadata)s::jsonb
                ) ON CONFLICT (source_fingerprint) DO NOTHING
                RETURNING legacy_message_id
                """,
                row,
            ).fetchone()
            inserted += 1 if result else 0
        conn.commit()
    summary.update({"applied": True, "inserted": inserted})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(import_export(args.path, apply=args.apply), indent=2, default=str))


if __name__ == "__main__":
    main()
