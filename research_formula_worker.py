"""Fail-open background discovery and live Shadow evaluation workers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, Optional

import research_feature_matrix
import research_formula_engine
import research_formula_store


_TRUE = {"1", "true", "yes", "on"}
_DISCOVERY_ENABLED = os.getenv("FORMULA_DISCOVERY_ENABLED", "").strip().lower() in _TRUE
_SHADOW_ENABLED = os.getenv("FORMULA_SHADOW_ENABLED", "").strip().lower() in _TRUE
_LIVE_ALERTS_ENABLED = os.getenv("FORMULA_LIVE_ALERTS_ENABLED", "").strip().lower() in _TRUE
_DISCOVERY_INTERVAL_SECONDS = max(
    3600, int(os.getenv("FORMULA_DISCOVERY_INTERVAL_SECONDS", "21600"))
)
_DISCOVERY_STARTUP_DELAY_SECONDS = max(
    15, int(os.getenv("FORMULA_DISCOVERY_STARTUP_DELAY_SECONDS", "180"))
)
_SHADOW_POLL_SECONDS = max(30, int(os.getenv("FORMULA_SHADOW_POLL_SECONDS", "60")))
_LOOKBACK_DAYS = max(1, min(3650, int(os.getenv("FORMULA_DISCOVERY_LOOKBACK_DAYS", "3650"))))
_DATASET_LIMIT = max(100, min(5000, int(os.getenv("FORMULA_DISCOVERY_DATASET_LIMIT", "2000"))))


def _horizons() -> tuple[int, ...]:
    values = []
    for raw in os.getenv("FORMULA_DISCOVERY_HORIZONS", "60,240,720,1440").split(","):
        try:
            value = int(raw.strip())
        except ValueError:
            continue
        if value in {60, 240, 720, 1440} and value not in values:
            values.append(value)
    return tuple(values or (240,))


def _conditions(value: Any) -> list[Dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = []
    return [dict(item) for item in value or [] if isinstance(item, dict)]


@dataclass
class FormulaWorkerMetrics:
    discovery_cycles: int = 0
    discovery_runs: int = 0
    candidates_evaluated: int = 0
    formulas_persisted: int = 0
    shadow_cycles: int = 0
    shadow_checks: int = 0
    shadow_hits: int = 0
    failures: int = 0
    last_discovery_utc: Optional[str] = None
    last_shadow_utc: Optional[str] = None
    last_error: Optional[str] = None


class FormulaResearchWorker:
    def __init__(self) -> None:
        self.metrics = FormulaWorkerMetrics()
        self._discovery_task: Optional[asyncio.Task] = None
        self._shadow_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._schema_ready = False

    def status(self) -> Dict[str, Any]:
        return {
            "discovery_enabled": _DISCOVERY_ENABLED,
            "shadow_enabled": _SHADOW_ENABLED,
            "live_alerts_enabled": _LIVE_ALERTS_ENABLED,
            "running": bool(
                (self._discovery_task and not self._discovery_task.done())
                or (self._shadow_task and not self._shadow_task.done())
            ),
            "schema_ready": self._schema_ready,
            "discovery_running": bool(
                self._discovery_task and not self._discovery_task.done()
            ),
            "shadow_running": bool(self._shadow_task and not self._shadow_task.done()),
            "horizons_minutes": list(_horizons()),
            "lookback_days": _LOOKBACK_DAYS,
            "dataset_limit": _DATASET_LIMIT,
            "discovery_interval_seconds": _DISCOVERY_INTERVAL_SECONDS,
            "shadow_poll_seconds": _SHADOW_POLL_SECONDS,
            "automatic_stage_ceiling": "SHADOW",
            "live_delivery_gate": {
                "environment_enabled": _LIVE_ALERTS_ENABLED,
                "formula_approval_required": True,
                "telegram_delivery_connected": False,
                "reason": "live delivery remains locked pending a separate explicit approval and destination",
            },
            "canonical_outcomes": "Binance Spot USDT 1m closed candles",
            "metrics": self.metrics.__dict__.copy(),
        }

    async def start(self) -> bool:
        if not (_DISCOVERY_ENABLED or _SHADOW_ENABLED):
            return False
        schema = await asyncio.to_thread(research_formula_store.schema_status)
        if not schema.get("schema_present"):
            self._schema_ready = False
            raise RuntimeError(
                f"Formula Research schema is not installed: {schema.get('missing_tables')}"
            )
        self._schema_ready = True
        self._stopping = False
        if _DISCOVERY_ENABLED and not (
            self._discovery_task and not self._discovery_task.done()
        ):
            self._discovery_task = asyncio.create_task(
                self._discovery_loop(), name="formula-discovery-worker"
            )
        if _SHADOW_ENABLED and not (
            self._shadow_task and not self._shadow_task.done()
        ):
            self._shadow_task = asyncio.create_task(
                self._shadow_loop(), name="formula-shadow-worker"
            )
        return True

    async def stop(self) -> None:
        self._stopping = True
        tasks = [task for task in (self._discovery_task, self._shadow_task) if task]
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._discovery_task = None
        self._shadow_task = None

    async def _discovery_loop(self) -> None:
        await asyncio.sleep(_DISCOVERY_STARTUP_DELAY_SECONDS)
        while not self._stopping:
            try:
                await asyncio.to_thread(self.run_discovery_once)
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[formula-discovery] cycle failed open: {exc!r}", flush=True)
            await asyncio.sleep(_DISCOVERY_INTERVAL_SECONDS)

    async def _shadow_loop(self) -> None:
        await asyncio.sleep(20)
        while not self._stopping:
            try:
                await asyncio.to_thread(self.run_shadow_once)
            except Exception as exc:
                self.metrics.failures += 1
                self.metrics.last_error = f"{type(exc).__name__}: {exc}"
                print(f"[formula-shadow] cycle failed open: {exc!r}", flush=True)
            await asyncio.sleep(_SHADOW_POLL_SECONDS)

    def run_discovery_once(self) -> Dict[str, Any]:
        results = []
        self.metrics.discovery_cycles += 1
        for horizon in _horizons():
            dataset = research_feature_matrix.load_formula_dataset(
                lookback_days=_LOOKBACK_DAYS,
                horizon_minutes=horizon,
                limit=_DATASET_LIMIT,
            )
            if not dataset.get("available") or int(dataset.get("sample_size") or 0) < 2:
                results.append(
                    {
                        "horizon_minutes": horizon,
                        "skipped": True,
                        "reason": dataset.get("reason") or "insufficient verified outcomes",
                        "sample_size": int(dataset.get("sample_size") or 0),
                        "coverage": dataset.get("coverage") or {},
                    }
                )
                continue
            discovery = research_formula_engine.discover_formulas(
                dataset["rows"],
                horizon_minutes=horizon,
                feature_schema_version=dataset["feature_schema_version"],
            )
            if not discovery.get("available"):
                results.append(
                    {
                        "horizon_minutes": horizon,
                        "skipped": True,
                        "reason": discovery.get("reason"),
                        "sample_size": discovery.get("sample_size"),
                    }
                )
                continue
            persisted = research_formula_store.persist_discovery_run(
                dataset=dataset,
                discovery=discovery,
                lookback_days=_LOOKBACK_DAYS,
            )
            self.metrics.discovery_runs += 1
            self.metrics.candidates_evaluated += int(
                discovery.get("candidates_evaluated") or 0
            )
            self.metrics.formulas_persisted += int(
                persisted.get("formulas_persisted") or 0
            )
            results.append(
                {
                    **persisted,
                    "sample_size": discovery.get("sample_size"),
                    "discovery_sample_size": discovery.get("discovery_sample_size"),
                    "holdout_sample_size": discovery.get("holdout_sample_size"),
                    "candidates_evaluated": discovery.get("candidates_evaluated"),
                    "coverage": dataset.get("coverage") or {},
                }
            )
        now = datetime.now(timezone.utc).isoformat()
        self.metrics.last_discovery_utc = now
        self.metrics.last_error = None
        print(f"[formula-discovery] completed: {results}", flush=True)
        return {"completed_at_utc": now, "results": results}

    def run_shadow_once(self) -> Dict[str, Any]:
        self.metrics.shadow_cycles += 1
        work = research_formula_store.load_shadow_work()
        event_ids = sorted(
            {
                int(event["event_id"])
                for formula in work
                for event in formula.get("events") or []
            }
        )
        feature_rows = research_feature_matrix.load_shadow_feature_rows(event_ids)
        checked = 0
        matched = 0
        for formula in work:
            conditions = _conditions(formula.get("conditions"))
            results = []
            for event in formula.get("events") or []:
                event_id = int(event["event_id"])
                row = feature_rows.get(event_id)
                if row is None:
                    continue
                is_match = research_formula_engine.formula_matches(
                    row,
                    direction=formula["direction"],
                    conditions=conditions,
                )
                features = research_formula_engine.extract_decision_features(row)
                results.append(
                    {
                        "event_id": event_id,
                        "alert_time_utc": event.get("alert_time_utc"),
                        "matched": is_match,
                        "input_snapshot": {
                            "formula_key_features": {
                                condition["feature"]: features.get(condition["feature"])
                                for condition in conditions
                            },
                            "conditions": conditions,
                            "feature_schema_version": formula["feature_schema_version"],
                        },
                    }
                )
            persisted = research_formula_store.record_shadow_results(
                formula=formula,
                results=results,
            )
            checked += persisted["checked"]
            matched += persisted["matched"]
        self.metrics.shadow_checks += checked
        self.metrics.shadow_hits += matched
        now = datetime.now(timezone.utc).isoformat()
        self.metrics.last_shadow_utc = now
        self.metrics.last_error = None
        if checked or matched:
            print(
                f"[formula-shadow] checked={checked}; matched={matched}; delivery=NOT_SENT",
                flush=True,
            )
        return {
            "completed_at_utc": now,
            "active_formulas": len(work),
            "events_loaded": len(event_ids),
            "checked": checked,
            "matched": matched,
            "delivery": "NOT_SENT",
        }


WORKER = FormulaResearchWorker()
