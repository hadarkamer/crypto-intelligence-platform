"""Deterministic checks for the production-only analytical AI integration."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path

# The self-test must never connect to or write a real Research database.
os.environ["RESEARCH_PERSISTENCE_ENABLED"] = "0"
os.environ["RESEARCH_OUTCOME_ENRICHMENT_ENABLED"] = "0"
os.environ["RESEARCH_USE_PRIMARY_DATABASE"] = "0"
os.environ.pop("RESEARCH_DATABASE_URL", None)

import ai_agent
import ai_alert_research
import ai_telegram
import ai_tools
import research_event_capture
import research_event_runtime
import research_event_store
import research_outcome_worker


EXPECTED_TOOLS = [
    "get_oi_state",
    "get_cvd_state",
    "get_market_state",
    "research_market_history",
    "get_market_context_at_time",
    "research_alert_history",
    "get_alert_context",
    "get_ai_capabilities",
]


def run() -> None:
    assert ai_tools.tool_names() == EXPECTED_TOOLS
    assert all(spec.get("name") not in {"web_search", "scan_coinglass_market"} for spec in ai_tools.TOOL_SPECS)
    payload = ai_agent.AGENT._base_payload([{"role": "user", "content": "test"}])
    assert payload["tools"] == ai_tools.TOOL_SPECS
    assert not any(tool.get("type") in {"web_search", "code_interpreter"} for tool in payload["tools"])

    archive = ai_alert_research.archive_status()
    assert archive["configured"] is False
    assert archive["schema_present"] is False

    event = research_event_capture.build_generic_alert_event(
        symbol="BTC",
        event_type="SELFTEST_ALERT",
        direction="LONG",
        event_time="2026-08-28T10:00:00Z",
        current_price=100.0,
        score=80.0,
    )
    delivered_at = datetime(2026, 8, 28, 10, 0, 2, tzinfo=timezone.utc)
    row = research_event_store.serialize_event(
        event,
        capture_stage="TELEGRAM_ALERT",
        delivery_status="DELIVERED",
        delivery_attempted_at_utc="2026-08-28T10:00:01Z",
        delivered_at_utc=delivered_at,
    )
    assert row["delivery_status"] == "DELIVERED"
    assert row["capture_stage"] == "TELEGRAM_ALERT"
    assert research_event_store.WRITER.enabled is False

    # Explicit live hooks enqueue delivery metadata; default dry-run hooks do not.
    original_enabled = research_event_store._ENABLED
    original_writer = research_event_store.WRITER
    try:
        research_event_store._ENABLED = True
        research_event_store.WRITER = research_event_store.AsyncResearchEventWriter(capacity=4)
        item = {
            "symbol": "BTC",
            "timeframe": "24h",
            "side": "SHORT",
            "types": ["NEAR_MAX_PAIN"],
            "score": 82.0,
            "current_price": 100.0,
            "target_price": 105.0,
        }
        research_event_runtime.capture_sent_maxpain(
            item,
            event_time="2026-08-28T10:05:00Z",
        )
        assert research_event_store.WRITER.queue.qsize() == 0
        research_event_runtime.capture_sent_maxpain(
            item,
            event_time="2026-08-28T10:06:00Z",
            persist=True,
            delivery_status="DELIVERED",
            delivered_at_utc="2026-08-28T10:06:02Z",
        )
        queued = research_event_store.WRITER.queue.get_nowait()
        assert queued["delivery_status"] == "DELIVERED"
        assert queued["capture_stage"] == "TELEGRAM_ALERT"

        research_event_runtime.capture_manual_maxpain_sample(
            item,
            event_time="2026-08-28T10:07:00Z",
            persist=True,
        )
        manual_sample = research_event_store.WRITER.queue.get_nowait()
        assert manual_sample["event_kind"] == "DECISION_SAMPLE"
        assert manual_sample["event_type"] == "MANUAL_MAX_PAIN_SCAN"
        assert manual_sample["capture_stage"] == "TELEGRAM_MANUAL_SCAN"
        assert manual_sample["delivery_status"] == "NOT_APPLICABLE"
    finally:
        research_event_store.WRITER = original_writer
        research_event_store._ENABLED = original_enabled

    raw_long, adjusted_long = research_outcome_worker.calculate_returns(100.0, 105.0, "LONG")
    raw_short, adjusted_short = research_outcome_worker.calculate_returns(100.0, 95.0, "SHORT")
    assert round(raw_long, 8) == 5.0 and round(adjusted_long or 0.0, 8) == 5.0
    assert round(raw_short, 8) == -5.0 and round(adjusted_short or 0.0, 8) == 5.0
    assert research_outcome_worker.WORKER.enabled is False

    cleaned = ai_telegram._plain_telegram_text("## כותרת\n**מודגש** ו-`קוד`")
    assert cleaned == "כותרת\nמודגש ו-קוד"

    root = Path(__file__).resolve().parent
    main_text = (root / "main.py").read_text(encoding="utf-8")
    assert "ai_telegram.register_ai_handlers(bot_app)" in main_text
    assert "research_event_store.WRITER.start()" in main_text
    assert "research_outcome_worker.WORKER.start()" in main_text
    assert "special_transitions_precomputed=True" in main_text
    assert "scan_coinglass_market" not in main_text
    manual_helper = main_text.split("async def _reply_alert_with_archive", 1)[1].split(
        "def _alert_card", 1
    )[0]
    assert "capture_manual_maxpain_sample" in manual_helper
    assert "capture_sent_maxpain" not in manual_helper
    assert "capture_special_transitions" not in manual_helper

    migration = (root / "migrations" / "001_research_archive_v1.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS research_events" in migration
    assert "CREATE TABLE IF NOT EXISTS research_alert_outcomes" in migration
    assert "event_fingerprint CHAR(64) NOT NULL UNIQUE" in migration
    assert "PRIMARY KEY (event_id, horizon_minutes)" in migration

    print("Production AI analytical integration self-test: PASS")


if __name__ == "__main__":
    run()
