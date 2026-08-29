"""Side-effect-free checks for the guarded first-touch canary worker."""

import os

import research_first_touch_backfill_worker as worker


def run() -> None:
    saved = {
        key: os.environ.get(key)
        for key in (
            "FIRST_TOUCH_BACKFILL_ENABLED",
            "HISTORICAL_REPLAY_BACKFILL",
            "FIRST_TOUCH_BACKFILL_SYMBOLS",
            "FIRST_TOUCH_BACKFILL_HORIZONS",
            "FIRST_TOUCH_BACKFILL_MAX_ANCHORS",
        )
    }
    try:
        os.environ.pop("FIRST_TOUCH_BACKFILL_ENABLED", None)
        os.environ.pop("HISTORICAL_REPLAY_BACKFILL", None)
        status = worker.WORKER.status()
        assert status["enabled"] is False
        assert status["historical_write_guard_enabled"] is False
        assert status["telegram_delivery"] is False
        assert status["automatic_live_promotion"] is False

        os.environ["FIRST_TOUCH_BACKFILL_SYMBOLS"] = "btc,eth,BTC"
        os.environ["FIRST_TOUCH_BACKFILL_HORIZONS"] = "1440,60,broken"
        os.environ["FIRST_TOUCH_BACKFILL_MAX_ANCHORS"] = "99999"
        status = worker.WORKER.status()
        assert status["symbols"] == ["BTC", "ETH"]
        assert status["horizons_minutes"] == [60, 1440]
        assert status["max_anchors"] == 2000
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    print("First-touch backfill worker self-test: PASS")


if __name__ == "__main__":
    run()
