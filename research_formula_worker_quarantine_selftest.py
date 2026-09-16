"""Network-free regressions for the ordered-first-touch Formula quarantine."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, Mock, patch

import research_formula_store
import research_formula_worker


class _ForbiddenTelegramBot:
    async def send_message(self, **kwargs):  # pragma: no cover - must not run
        raise AssertionError(f"quarantined Formula worker attempted delivery: {kwargs}")


async def _check() -> None:
    worker = research_formula_worker.FormulaResearchWorker()
    originals = {
        "discovery": research_formula_worker._DISCOVERY_ENABLED,
        "shadow": research_formula_worker._SHADOW_ENABLED,
        "live": research_formula_worker._LIVE_ALERTS_ENABLED,
        "schema_status": research_formula_store.schema_status,
        "load_pending": research_formula_store.load_pending_live_deliveries,
    }
    calls = {"schema": 0, "pending": 0}

    def forbidden_schema_status():
        calls["schema"] += 1
        raise AssertionError("quarantined worker queried the Formula schema")

    def forbidden_pending_deliveries():
        calls["pending"] += 1
        raise AssertionError("quarantined worker loaded live deliveries")

    research_formula_worker._DISCOVERY_ENABLED = True
    research_formula_worker._SHADOW_ENABLED = True
    research_formula_worker._LIVE_ALERTS_ENABLED = True
    research_formula_store.schema_status = forbidden_schema_status
    research_formula_store.load_pending_live_deliveries = forbidden_pending_deliveries
    try:
        assert research_formula_worker._NATIVE_FORMULA_OUTCOME_METHOD_VERSION == (
            "no-dwell-first-touch-v6"
        )
        assert research_formula_worker._REQUIRED_OUTCOME_METHOD_VERSION == (
            "ordered-first-touch-v7"
        )
        assert research_formula_worker._NATIVE_PIPELINE_COMPATIBLE is False

        worker.bind_telegram(_ForbiddenTelegramBot())
        assert await worker.start() is False
        await asyncio.sleep(0)

        status = worker.status()
        assert status["execution_state"] == (
            "QUARANTINED_AWAITING_ORDERED_FIRST_TOUCH_V7"
        )
        assert status["execution_enabled"] is False
        assert status["quarantined"] is True
        assert "awaiting explicit ordered-first-touch-v7 support" in (
            status["quarantine_reason"]
        )
        assert status["configured"] == {
            "discovery_enabled": True,
            "shadow_enabled": True,
            "live_alerts_enabled": True,
        }
        assert status["discovery_enabled"] is False
        assert status["shadow_enabled"] is False
        assert status["live_alerts_enabled"] is False
        assert status["running"] is False
        assert status["discovery_running"] is False
        assert status["shadow_running"] is False
        assert status["live_delivery_gate"]["environment_enabled"] is False
        assert status["live_delivery_gate"]["environment_configured"] is True
        assert status["live_delivery_gate"]["telegram_delivery_connected"] is False

        assert await worker._deliver_pending_live_alerts() == {
            "sent": 0,
            "failed": 0,
        }
        assert calls == {"schema": 0, "pending": 0}
        assert worker.metrics.discovery_cycles == 0
        assert worker.metrics.discovery_runs == 0
        assert worker.metrics.shadow_cycles == 0
        assert worker.metrics.shadow_checks == 0
        assert worker.metrics.live_deliveries_sent == 0
        assert worker.metrics.live_deliveries_failed == 0
    finally:
        await worker.stop()
        research_formula_worker._DISCOVERY_ENABLED = originals["discovery"]
        research_formula_worker._SHADOW_ENABLED = originals["shadow"]
        research_formula_worker._LIVE_ALERTS_ENABLED = originals["live"]
        research_formula_store.schema_status = originals["schema_status"]
        research_formula_store.load_pending_live_deliveries = originals["load_pending"]


async def _check_delivery_policy(profile: str) -> None:
    # Exercise the profile independently of the current outcome quarantine.
    with (
        patch.object(research_formula_worker, "_NATIVE_PIPELINE_COMPATIBLE", True),
        patch.object(research_formula_worker, "_DISCOVERY_ENABLED", True),
        patch.object(research_formula_worker, "_SHADOW_ENABLED", True),
        patch.object(research_formula_worker, "_LIVE_ALERTS_ENABLED", True),
        patch.object(research_formula_store, "load_pending_live_deliveries") as load,
        patch.object(research_formula_store, "mark_live_delivery") as mark,
        patch.dict(os.environ, {"ALERT_DELIVERY_PROFILE": profile}),
    ):
        worker = research_formula_worker.FormulaResearchWorker()
        bot = Mock(send_message=AsyncMock())
        worker.bind_telegram(bot)
        assert await worker._deliver_pending_live_alerts() == {"sent": 0, "failed": 0}
        load.assert_not_called()
        mark.assert_not_called()
        bot.send_message.assert_not_awaited()
        state = worker.status()
        assert research_formula_worker.delivery_policy.ordinary_alerts_enabled() is (
            profile == "ORDINARY_AND_SELECTED_EXPERIMENTAL"
        )
        assert state["discovery_enabled"] is True
        assert state["shadow_enabled"] is True
        assert state["live_alerts_enabled"] is False
        assert state["live_delivery_gate"]["delivery_profile_allowed"] is False
        assert "ALERT_DELIVERY_PROFILE" in state["live_delivery_gate"]["reason"]

        # A profile change while awaiting the pending read still blocks sending.
        def pending_after_profile_change():
            os.environ["ALERT_DELIVERY_PROFILE"] = profile
            return [{"delivery_id": 1, "chat_id": 2}]

        os.environ["ALERT_DELIVERY_PROFILE"] = "ALL"
        load.side_effect = pending_after_profile_change
        assert await worker._deliver_pending_live_alerts() == {"sent": 0, "failed": 0}
        load.assert_called_once_with()
        mark.assert_not_called()
        bot.send_message.assert_not_awaited()

        os.environ["ALERT_DELIVERY_PROFILE"] = "ALL"
        load.side_effect = None
        load.return_value = [{"delivery_id": 1, "chat_id": 2}]
        with patch.object(worker, "_live_alert_text", return_value="fixture"):
            assert await worker._deliver_pending_live_alerts() == {"sent": 1, "failed": 0}
        bot.send_message.assert_awaited_once_with(chat_id=2, text="fixture")
        mark.assert_called_once_with(1, sent=True)
        assert worker.status()["live_alerts_enabled"] is True


def run() -> None:
    asyncio.run(_check())
    for profile in ("SELECTED_EXPERIMENTAL_ONLY", "ORDINARY_AND_SELECTED_EXPERIMENTAL"):
        asyncio.run(_check_delivery_policy(profile))
    print("Formula worker ordered-first-touch quarantine self-test: PASS")


if __name__ == "__main__":
    run()
