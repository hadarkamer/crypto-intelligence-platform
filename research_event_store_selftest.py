from __future__ import annotations

from datetime import datetime, timezone

import research_event_capture
import research_event_store


def main() -> None:
    event = research_event_capture.build_generic_alert_event(
        symbol="BTC",
        event_type="FUTURES_CVD_HIGH",
        direction="LONG",
        event_time=datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc),
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
        delivery_status="APPROVED_FOR_DELIVERY",
    )
    assert row["symbol"] == "BTC"
    assert row["event_type"] == "FUTURES_CVD_HIGH"
    assert row["delivery_status"] == "APPROVED_FOR_DELIVERY"
    assert len(row["event_fingerprint"]) == 64
    assert research_event_store.WRITER.status()["schema_auto_create"] is False

    # CI intentionally supplies no enable flag / research DB URL. Importing and
    # serializing must therefore remain incapable of writing anything.
    assert research_event_store.persistence_status()["enabled"] is False
    assert research_event_store.WRITER.enqueue(event) is False
    assert research_event_store.WRITER.status()["queue_size"] == 0

    print("Research Event persistence self-test: PASS")
    print(research_event_store.persistence_status())


if __name__ == "__main__":
    main()
