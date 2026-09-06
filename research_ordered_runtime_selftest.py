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
    snapshot = Worker("snapshot", calls, fail_stop=True)
    with patch.object(main, "research_btc_episode_worker", SimpleNamespace(WORKER=btc)), \
         patch.object(main, "research_formula_ordered_worker", SimpleNamespace(WORKER=formula)), \
         patch.object(main, "research_snapshot_sync_worker", SimpleNamespace(WORKER=snapshot)):
        blocked = await main._start_ordered_research_workers(schema_ready=False)
        assert not calls and all(not row["started"] for row in blocked.values())
        result = await main._start_ordered_research_workers(schema_ready=True)
        assert calls == [("btc", "start"), ("formula", "start"), ("snapshot", "start")]
        assert not result["btc-episodes"]["started"]
        assert result["formula-ordered-v7"]["started"]
        assert result["snapshot-sync"]["started"]
        await main._stop_ordered_research_workers()
        assert calls[-3:] == [("snapshot", "stop"), ("formula", "stop"), ("btc", "stop")]


if __name__ == "__main__":
    asyncio.run(check())
    print("ordered research lifecycle checks passed")
