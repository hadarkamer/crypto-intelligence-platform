from __future__ import annotations

from datetime import datetime, timezone

import research_event_capture
import research_event_store


def main() -> None:
    decision_time = datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc)
    delivered_time = datetime(2026, 8, 20, 10, 0, 2, tzinfo=timezone.utc)
    event = research_event_capture.build_generic_alert_event(
        symbol="BTC",
        event_type="FUTURES_CVD_HIGH",
        direction="LONG",
        source_side="BULLISH",
        event_time=decision_time,
        score=67.5,
        current_price=65000,
        categories=["FUTURES_CVD", "QUALITY_65"],
        engine_snapshot={"module": {"score": 67.5}},
        strategy_version="selftest",
        code_version="selftest",
    )
    row = research_event_store.serialize_event(
        event,
        capture_stage="TRANSITION_APPROVED",
        delivery_status="DELIVERED",
        delivery_attempted_at_utc=decision_time,
        delivered_at_utc=delivered_time,
    )
    assert row["symbol"] == "BTC"
    assert row["event_type"] == "FUTURES_CVD_HIGH"
    assert row["source_side"] == "BULLISH"
    assert row["delivery_status"] == "DELIVERED"
    assert row["alert_time_utc"] == event.alert_time_utc
    assert row["delivered_at_utc"] == delivered_time
    assert row["runtime_session_id"] == research_event_store.RUNTIME_SESSION_ID
    assert len(row["event_fingerprint"]) == 64
    status = research_event_store.WRITER.status()
    assert status["schema_auto_create"] is False
    assert status["transient_failure_policy"] == "retry_current_batch_with_backoff"

    # CI intentionally supplies no enable flag / research DB URL. Importing and
    # serializing must therefore remain incapable of writing anything.
    assert research_event_store.persistence_status()["enabled"] is False
    assert research_event_store.WRITER.enqueue(event) is False
    assert research_event_store.WRITER.status()["queue_size"] == 0

    sample = research_event_capture.build_decision_sample(
        symbol="BTC",
        sample_type="NEAR_THRESHOLD",
        direction="LONG",
        score=64.9,
        event_time=decision_time,
    )
    sample_row = research_event_store.serialize_event(sample)
    assert sample_row["delivery_status"] == "NOT_APPLICABLE"

    print("Research Event persistence self-test: PASS")
    print(research_event_store.persistence_status())


if __name__ == "__main__":
    main()
