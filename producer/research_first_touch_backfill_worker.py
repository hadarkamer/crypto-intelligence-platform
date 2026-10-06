"""Guarded one-shot enrichment of historical replay rows with first-touch v6.

The production bot may host this bounded task, but it never starts merely
because the code was deployed.  Two independent operator switches are
required: ``FIRST_TOUCH_BACKFILL_ENABLED=1`` and the existing
``HISTORICAL_REPLAY_BACKFILL=1`` writer guard.  The default cohort is a small
BTC canary whose end time is frozen before the task starts; Telegram and LIVE
formula delivery are not imported or called by this module.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
import sys
from typing import Any, Dict, Optional


_TRUE = {"1", "true", "yes", "on"}
_SUPPORTED_HORIZONS = (60, 240, 720, 1440)


def _enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in _TRUE


def _symbols() -> tuple[str, ...]:
    raw = os.getenv("FIRST_TOUCH_BACKFILL_SYMBOLS", "BTC")
    values: list[str] = []
    for item in raw.split(","):
        symbol = item.strip().upper()
        if not symbol:
            continue
        if len(symbol) > 20 or not symbol.replace("-", "").isalnum():
            raise ValueError(f"invalid first-touch backfill symbol: {symbol}")
        if symbol not in values:
            values.append(symbol)
    return tuple(values or ("BTC",))


def _horizons() -> tuple[int, ...]:
    values: list[int] = []
    for raw in os.getenv(
        "FIRST_TOUCH_BACKFILL_HORIZONS", "60,240,720,1440"
    ).split(","):
        try:
            value = int(raw.strip())
        except ValueError:
            continue
        if value in _SUPPORTED_HORIZONS and value not in values:
            values.append(value)
    if not values:
        raise ValueError("first-touch backfill requires a supported horizon")
    return tuple(sorted(values))


def _max_anchors() -> int:
    # This is intentionally bounded even if an environment value is mistyped.
    return max(
        1,
        min(2000, int(os.getenv("FIRST_TOUCH_BACKFILL_MAX_ANCHORS", "250"))),
    )


@dataclass
class BackfillMetrics:
    started_at_utc: Optional[str] = None
    frozen_end_utc: Optional[str] = None
    completed_at_utc: Optional[str] = None
    running: bool = False
    completed: bool = False
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class FirstTouchBackfillWorker:
    def __init__(self) -> None:
        self.metrics = BackfillMetrics()
        self._task: Optional[asyncio.Task] = None
        self._process: Optional[asyncio.subprocess.Process] = None

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": _enabled("FIRST_TOUCH_BACKFILL_ENABLED"),
            "historical_write_guard_enabled": _enabled(
                "HISTORICAL_REPLAY_BACKFILL"
            ),
            "mode": "ONE_SHOT_BOUNDED_CANARY",
            "symbols": list(_symbols()),
            "horizons_minutes": list(_horizons()),
            "max_anchors": _max_anchors(),
            "automatic_live_promotion": False,
            "telegram_delivery": False,
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not _enabled("FIRST_TOUCH_BACKFILL_ENABLED"):
            return False
        if not _enabled("HISTORICAL_REPLAY_BACKFILL"):
            raise RuntimeError(
                "FIRST_TOUCH_BACKFILL_ENABLED also requires "
                "HISTORICAL_REPLAY_BACKFILL=1"
            )
        if self._task and not self._task.done():
            return True
        self._task = asyncio.create_task(
            self._run(), name="first-touch-historical-canary"
        )
        return True

    async def stop(self) -> None:
        if not self._task or self._task.done():
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        now = datetime.now(timezone.utc)
        horizons = _horizons()
        import research_historical_replay

        frozen_end = research_historical_replay.fully_closed_end(
            horizons, now=now
        )
        self.metrics.started_at_utc = now.isoformat()
        self.metrics.frozen_end_utc = frozen_end.isoformat()
        self.metrics.running = True
        self.metrics.completed = False
        self.metrics.error = None
        try:
            child_env = os.environ.copy()
            child_env.update(
                {
                    "HISTORICAL_REPLAY_END_UTC": frozen_end.isoformat(),
                    "HISTORICAL_REPLAY_SYMBOLS": ",".join(_symbols()),
                    "HISTORICAL_REPLAY_HORIZONS": ",".join(
                        str(value) for value in horizons
                    ),
                    "HISTORICAL_REPLAY_CHUNK_DAYS": "2",
                    "HISTORICAL_REPLAY_MAX_ANCHORS": str(_max_anchors()),
                    "HISTORICAL_REPLAY_API_PAUSE_SECONDS": str(
                        max(
                            0.0,
                            min(
                                2.0,
                                float(
                                    os.getenv(
                                        "FIRST_TOUCH_BACKFILL_API_PAUSE_SECONDS",
                                        "0.10",
                                    )
                                ),
                            ),
                        )
                    ),
                }
            )
            self._process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "research_historical_replay",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=child_env,
            )
            output_bytes, _ = await self._process.communicate()
            output = output_bytes.decode("utf-8", errors="replace")
            for line in output.splitlines():
                print(f"[first-touch-backfill-child] {line}", flush=True)
            if self._process.returncode != 0:
                raise RuntimeError(
                    f"historical replay child exited {self._process.returncode}"
                )
            result: Optional[Dict[str, Any]] = None
            for line in reversed(output.splitlines()):
                try:
                    parsed = json.loads(line)
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(parsed, dict):
                    result = parsed
                    break
            if result is None:
                raise RuntimeError("historical replay child returned no JSON result")
            self.metrics.result = result
            self.metrics.completed = True
        except asyncio.CancelledError:
            if self._process and self._process.returncode is None:
                self._process.terminate()
                try:
                    await asyncio.wait_for(self._process.wait(), timeout=15.0)
                except asyncio.TimeoutError:
                    self._process.kill()
                    await self._process.wait()
            raise
        except Exception as exc:
            self.metrics.error = f"{type(exc).__name__}: {exc}"
            print(
                f"[first-touch-backfill] one-shot canary failed: {exc!r}",
                flush=True,
            )
        finally:
            self._process = None
            self.metrics.running = False
            self.metrics.completed_at_utc = datetime.now(timezone.utc).isoformat()


WORKER = FirstTouchBackfillWorker()
