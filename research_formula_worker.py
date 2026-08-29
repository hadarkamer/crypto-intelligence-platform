"""Fail-open background discovery and live Shadow evaluation workers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, Mapping, Optional

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
    live_candidates_queued: int = 0
    live_deliveries_sent: int = 0
    live_deliveries_failed: int = 0
    formulas_promoted_live: int = 0
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
        self._telegram_bot: Any = None

    def bind_telegram(self, bot: Any) -> None:
        """Bind the initialized Telegram bot used for durable live delivery."""
        self._telegram_bot = bot

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
            "automatic_stage_ceiling": "LIVE_AFTER_FUTURE_SHADOW_POLICY",
            "live_delivery_gate": {
                "environment_enabled": _LIVE_ALERTS_ENABLED,
                "formula_validation_required": True,
                "telegram_delivery_connected": self._telegram_bot is not None,
                "chat_subscription_required": True,
                "reason": (
                    "delivery requires owner-policy validation, runtime enablement "
                    "and /ai_alerts_on in the destination chat"
                ),
            },
            "canonical_outcomes": (
                "Binance Spot USDT 1m; HYPE via Hyperliquid HYPE/USDT spot (@107) 1m"
            ),
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
                await self._deliver_pending_live_alerts()
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
        queued = 0
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
            queued += int(persisted.get("queued") or 0)
        promotion = research_formula_store.promote_eligible_shadow_formulas()
        promoted = len(promotion.get("promoted") or [])
        self.metrics.shadow_checks += checked
        self.metrics.shadow_hits += matched
        self.metrics.live_candidates_queued += queued
        self.metrics.formulas_promoted_live += promoted
        now = datetime.now(timezone.utc).isoformat()
        self.metrics.last_shadow_utc = now
        self.metrics.last_error = None
        if checked or matched:
            print(
                f"[formula-shadow] checked={checked}; matched={matched}; "
                f"queued={queued}; promoted_live={promoted}",
                flush=True,
            )
        return {
            "completed_at_utc": now,
            "active_formulas": len(work),
            "events_loaded": len(event_ids),
            "checked": checked,
            "matched": matched,
            "queued_live_deliveries": queued,
            "promotion": promotion,
            "delivery": (
                "ENABLED_FOR_SUBSCRIBED_CHATS"
                if _LIVE_ALERTS_ENABLED
                else "DISABLED_BY_ENVIRONMENT"
            ),
        }

    @staticmethod
    def _as_mapping(value: Any) -> Mapping[str, Any]:
        if isinstance(value, Mapping):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, Mapping) else {}
        return {}

    @classmethod
    def _live_alert_text(cls, delivery: Mapping[str, Any]) -> str:
        validation = cls._as_mapping(delivery.get("shadow_validation_metrics"))
        metrics = cls._as_mapping(validation.get("metrics"))
        holdout = cls._as_mapping(delivery.get("holdout_metrics"))

        def number(source: Mapping[str, Any], key: str, digits: int = 2) -> str:
            try:
                return f"{float(source.get(key)):.{digits}f}"
            except (TypeError, ValueError):
                return "-"

        horizon = int(delivery.get("horizon_minutes") or 0)
        horizon_label = {60: "1h", 240: "4h", 720: "12h", 1440: "24h"}.get(
            horizon, f"{horizon}m"
        )
        direction = str(delivery.get("direction") or "-").upper()
        direction_icon = "🟢" if direction == "LONG" else "🔴"
        target = delivery.get("target_price")
        target_text = f"{float(target):,.6g}" if target not in (None, "") else "לא הוגדר"
        current = delivery.get("current_price")
        current_text = f"{float(current):,.6g}" if current not in (None, "") else "-"
        rarity = str(holdout.get("rarity_class") or "-")
        movement_percentile_key = (
            "session_adjusted_mfe_percentile_pct"
            if metrics.get("session_adjusted_mfe_percentile_pct") is not None
            else "median_mfe_percentile_pct"
        )
        return (
            "🧠 התראת טרייד AI — נוסחה מאומתת\n"
            f"{direction_icon} {delivery.get('symbol')} {direction} | אופק {horizon_label}\n"
            f"אירוע #{delivery.get('event_id')} | {delivery.get('event_type')}\n"
            f"מחיר בעת ההתראה: {current_text} | יעד הבוט: {target_text}\n\n"
            f"נוסחה #{delivery.get('formula_id')} v{delivery.get('formula_version')}\n"
            f"{delivery.get('formula_text')}\n\n"
            "אימות עתידי ב-Shadow:\n"
            f"דגימות: {int(metrics.get('sample_size') or 0)} | נדירות Holdout: {rarity}\n"
            f"שיעור כיוון נכון: {number(metrics, 'hit_rate_pct')}% "
            f"(Wilson תחתון {number(metrics, 'wilson_95_lower_pct')}%)\n"
            f"מהלך חיובי חציוני MFE: {number(metrics, 'median_mfe_pct', 3)}% | "
            f"תנועה נגדית p90 MAE: {number(metrics, 'mae_p90_pct', 3)}%\n"
            f"אחוזון רוחב מהלך מותאם Session: {number(metrics, movement_percentile_key)} | "
            f"MFE/MAE: {number(metrics, 'median_mfe_mae_ratio')}\n\n"
            "התראה מחקרית אוטונומית בלבד — הבוט לא ביצע עסקה."
        )

    async def _deliver_pending_live_alerts(self) -> Dict[str, int]:
        if not _LIVE_ALERTS_ENABLED or self._telegram_bot is None:
            return {"sent": 0, "failed": 0}
        pending = await asyncio.to_thread(
            research_formula_store.load_pending_live_deliveries
        )
        sent = 0
        failed = 0
        for delivery in pending:
            try:
                await self._telegram_bot.send_message(
                    chat_id=int(delivery["chat_id"]),
                    text=self._live_alert_text(delivery),
                )
            except Exception as exc:
                failed += 1
                await asyncio.to_thread(
                    research_formula_store.mark_live_delivery,
                    int(delivery["delivery_id"]),
                    sent=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                sent += 1
                await asyncio.to_thread(
                    research_formula_store.mark_live_delivery,
                    int(delivery["delivery_id"]),
                    sent=True,
                )
        self.metrics.live_deliveries_sent += sent
        self.metrics.live_deliveries_failed += failed
        return {"sent": sent, "failed": failed}


WORKER = FormulaResearchWorker()
