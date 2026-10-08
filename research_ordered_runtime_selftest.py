"""Network-free lifecycle checks for the additive v7 research workers."""

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import main


class Worker:
    def __init__(self, name, calls, *, fail_start=False, fail_stop=False):
        self.name, self.calls = name, calls
        self.fail_start, self.fail_stop = fail_start, fail_stop

    async def start(self):
        self.calls.append((self.name, "start"))
        if self.fail_start:
            raise RuntimeError("schema unavailable")
        return True

    async def stop(self):
        self.calls.append((self.name, "stop"))
        if self.fail_stop:
            raise RuntimeError("shutdown failure")

    def status(self):
        return {"name": self.name}


async def check():
    calls = []
    btc = Worker("btc", calls, fail_start=True)
    formula = Worker("formula", calls)
    no_horizon = Worker("no-horizon", calls, fail_start=True)
    snapshot = Worker("snapshot", calls, fail_stop=True)
    experimental = Worker("experimental", calls)
    watch_intake = Worker("watch-intake", calls)
    watch_measurement = Worker("watch-measurement", calls)
    watch_formulas = Worker("watch-formulas", calls)
    watch_timeframes = Worker("watch-timeframes", calls)
    with patch.object(main, "research_watch_scan_intake", SimpleNamespace(WORKER=watch_intake)), \
         patch.object(main, "research_watch_scan_measurement_worker", SimpleNamespace(WORKER=watch_measurement)), \
         patch.object(main, "research_watch_scan_formula_worker", SimpleNamespace(WORKER=watch_formulas)), \
         patch.object(main, "research_watch_scan_formula_timeframe_worker", SimpleNamespace(WORKER=watch_timeframes)), \
         patch.object(main, "research_btc_episode_worker", SimpleNamespace(WORKER=btc)), \
         patch.object(main, "research_formula_ordered_worker", SimpleNamespace(WORKER=formula)), \
         patch.object(main, "research_no_horizon_worker", SimpleNamespace(WORKER=no_horizon)), \
         patch.object(main, "research_ordered_experimental_worker", SimpleNamespace(WORKER=experimental)), \
         patch.object(main, "research_snapshot_sync_worker", SimpleNamespace(WORKER=snapshot)):
        blocked = await main._start_ordered_research_workers(schema_ready=False)
        assert not calls and all(not row["started"] for row in blocked.values())
        result = await main._start_ordered_research_workers(schema_ready=True)
        assert calls == [("watch-intake", "start"), ("watch-measurement", "start"), ("watch-formulas", "start"), ("watch-timeframes", "start"), ("btc", "start"), ("formula", "start"), ("no-horizon", "start"), ("experimental", "start"), ("snapshot", "start")]
        assert result["watch-scan-intake"]["started"]
        assert result["watch-scan-measurement"]["started"]
        assert result["watch-scan-formulas"]["started"]
        assert result["watch-scan-timeframe-formulas"]["started"]
        assert not result["btc-episodes"]["started"]
        assert result["formula-ordered-v7"]["started"]
        assert not result["no-horizon-research"]["started"]
        assert result["snapshot-sync"]["started"]
        assert result["ordered-experimental"]["started"]
        await main._stop_ordered_research_workers()
        assert calls[-9:] == [("watch-intake", "stop"), ("watch-measurement", "stop"), ("watch-formulas", "stop"), ("watch-timeframes", "stop"), ("experimental", "stop"), ("snapshot", "stop"), ("formula", "stop"), ("no-horizon", "stop"), ("btc", "stop")]


if __name__ == "__main__":
    asyncio.run(check())
    print("ordered research lifecycle checks passed")
