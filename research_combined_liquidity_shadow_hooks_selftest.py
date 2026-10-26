"""Differential producer checks for the separate, fail-open DOGE shadow hook."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import ast
import hashlib
import inspect
import queue
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import research_event_runtime as runtime
import research_event_store as event_store

# Exact functions from main 3b3745bdaf777e95ddf24bcb2b26c097cad7b2ae,
# before this additive observation. No fixture depends on a local checkout.
FROZEN_EMIT_SOURCE = 'def _emit(\n    event: research_event_capture.ResearchEvent,\n    *,\n    persist: bool = False,\n    capture_stage: str = "OBSERVED",\n    delivery_status: str = "",\n    delivery_attempted_at_utc: Any = None,\n    delivered_at_utc: Any = None,\n) -> bool:\n    """Always retain a bounded memory copy; persist only on an explicit live hook.\n\n    Shadow Replay and deterministic self-tests use the default ``persist=False``\n    and therefore can never leak reconstructed history into the production\n    Research Archive.\n    """\n    event = _with_watch_context(event)\n    planned = _PLANNED_CAPTURE.get()\n    if planned is not None:\n        if event.event_kind == "ALERT":\n            data = event.to_dict()\n            data.update(event_id="watch:" + event.event_fingerprint,\n                        delivery_status="NOT_ATTEMPTED",\n                        capture_stage="WATCH_PLANNED_ALERT", source_scope="LIVE",\n                        planned_source_capture_stage=capture_stage)\n            planned.append(data)\n        return True\n    remembered = SINK.emit(event)\n    queued = False\n    if persist:\n        queued = research_event_store.WRITER.enqueue(\n            event,\n            capture_stage=capture_stage,\n            delivery_status=delivery_status,\n            delivery_attempted_at_utc=delivery_attempted_at_utc,\n            delivered_at_utc=delivered_at_utc,\n        )\n        # Sheets is an independent fail-open copy. Only Telegram events that\n        # were actually delivered are exposed as live alerts in the workbook.\n        if str(delivery_status or "").upper() == "DELIVERED":\n            try:\n                google_sheets_sync.enqueue_delivered_event(\n                    event,\n                    delivered_at_utc=delivered_at_utc,\n                )\n            except Exception as exc:\n                print(f"[google-sheets] alert enqueue failed open: {exc!r}", flush=True)\n    return bool(remembered or queued)'
FROZEN_COMBINED_SOURCE = 'def capture_combined_confirmation(\n    candidate: Mapping[str, Any], *, event_time: Any = None,\n    persist: bool = False, delivery_status: str = "DELIVERED",\n    delivery_attempted_at_utc: Any = None, delivered_at_utc: Any = None,\n) -> bool:\n    """Capture a Combined alert occurrence only after main.py approved its Telegram transition."""\n    try:\n        top_item = candidate.get("top_item") or {}\n        source_side = str(candidate.get("side") or "").upper()\n        signal_keys = sorted(str(x) for x in (candidate.get("signal_keys") or set()))\n        event = research_event_capture.build_generic_alert_event(\n            symbol=str(candidate.get("symbol") or "").upper(),\n            event_type="COMBINED_CONFIRMATION",\n            direction=_price_direction_from_alert_side(source_side),\n            source_side=source_side,\n            event_time=_now(event_time),\n            timeframe=str(top_item.get("timeframe") or "") or None,\n            score=top_item.get("score", top_item.get("priority")),\n            current_price=top_item.get("current_price"),\n            target_price=top_item.get("target_price"),\n            categories=signal_keys,\n            engine_snapshot={\n                "alert_side": source_side,\n                "signal_count": candidate.get("signal_count"),\n                "signal_keys": signal_keys,\n                "normal_confirmations": candidate.get("normal_confirmations") or [],\n                "strong_confirmations": candidate.get("strong_confirmations") or [],\n                "high_scores": candidate.get("high_scores") or [],\n                "anomaly_setups": candidate.get("anomaly_setups") or [],\n                "liquidity_imbalances": candidate.get("liquidity_imbalances") or [],\n                "derivatives_high": candidate.get("derivatives_high") or [],\n                "magnet": candidate.get("magnet") or {},\n                "top_item_components": top_item.get("components") or {},\n                "maxpain_timeframes": research_event_capture.compact_maxpain_timeframes(\n                    top_item.get("maxpain_timeframes")\n                ),\n                "market_evidence": top_item.get("market_evidence") or {},\n                "top_item_confirmation": top_item.get("maxpain_confirmation") or {},\n                "top_item_average_score_all_timeframes": top_item.get("average_score_all_timeframes"),\n                "top_item_price_source": top_item.get("price_source"),\n                "top_item_price_pair": top_item.get("price_pair"),\n            },\n            setup_identity={\n                "alert_side": source_side,\n                "signal_families": sorted({key.split(":", 1)[0] for key in signal_keys}),\n            },\n        )\n        return _emit(\n            event,\n            persist=persist,\n            capture_stage="TELEGRAM_COMBINED_ALERT",\n            delivery_status=delivery_status,\n            delivery_attempted_at_utc=delivery_attempted_at_utc,\n            delivered_at_utc=delivered_at_utc,\n        )\n    except Exception as exc:\n        print(f"[research-dry-run] combined capture failed: {exc!r}", flush=True)\n        return False'

NOW = datetime(2026, 10, 26, 14, 0, 0, 123456, tzinfo=timezone.utc)
ATTEMPT = NOW + timedelta(seconds=1)
DELIVERED = NOW + timedelta(seconds=2)


def candidate(symbol="DOGE"):
    point = {"timeframe": "48h", "source_side": "SHORT", "score": 59.69,
             "target_price": 0.078814, "near_amount": 492970.0, "far_amount": 595720.0,
             "near_share_pct": 45.28102582002223, "components": {"relative_gap": 3.5}}
    return {"symbol": symbol, "side": "SHORT", "signal_count": 3,
            "signal_keys": {"ANOMALY:48h", "ANOMALY:12h", "ANOMALY:3d"},
            "anomaly_setups": [{"timeframe": "48h"}],
            "top_item": {"symbol": symbol, "side": "SHORT", "timeframe": "48h", "score": 59.69,
                         "current_price": 0.07748, "target_price": 0.078814,
                         "near_amount": 492970.0, "far_amount": 595720.0,
                         "near_share_pct": 45.28102582002223,
                         "average_score_all_timeframes": 38.43,
                         "maxpain_timeframes": [point], "components": {"relative_gap": 3.5}}}


class Sink:
    def __init__(self, trace, remembered):
        self.trace, self.remembered = trace, remembered

    def emit(self, event):
        self.trace.append(("sink", event.to_dict()))
        return self.remembered


class Writer:
    def __init__(self, trace, queued, error=None):
        self.trace, self.queued, self.error = trace, queued, error

    def enqueue(self, event, **kwargs):
        self.trace.append(("writer", event_store.serialize_event(event, **kwargs)))
        if self.error is not None:
            raise self.error
        return self.queued


class Shadow:
    def __init__(self, trace, *, error=None):
        self.trace, self.error = trace, error
        self.observations, self.skips = [], []

    def submit_producer_capture(self, event, selected_top_item, **metadata):
        self.trace.append(("shadow",))
        if self.error is not None:
            raise self.error
        self.observations.append({"event": deepcopy(event.to_dict()),
                                  "selected_top_item": deepcopy(selected_top_item),
                                  "metadata": deepcopy(metadata)})
        return {"status": "QUEUED"}

    def record_skip(self, reason):
        self.skips.append(reason)
        if self.error is not None:
            raise self.error


def legacy_namespace():
    namespace = dict(runtime.__dict__)
    exec(FROZEN_EMIT_SOURCE, namespace)
    exec(FROZEN_COMBINED_SOURCE, namespace)
    return namespace


def exercise(*, old=False, remembered=True, queued=True, persist=True,
             delivery="DELIVERED", preview=False, symbol="DOGE", shadow_error=None,
             queue_error=None, source=None):
    trace = []
    shadow = Shadow(trace, error=shadow_error)
    writer = Writer(trace, queued, queue_error)
    sink = Sink(trace, remembered)
    sheets = SimpleNamespace(enqueue_delivered_event=lambda event, **kwargs: trace.append(("sheets", event.to_dict(), kwargs)))
    source = deepcopy(candidate(symbol)) if source is None else source
    planned = [] if preview else None
    namespace = legacy_namespace() if old else None
    if old:
        namespace["SINK"] = sink
        namespace["google_sheets_sync"] = sheets
        fn = namespace["capture_combined_confirmation"]
    else:
        fn = runtime.capture_combined_confirmation
    token = runtime.set_watch_context(watch_scan_id="shared-watch:test-producer", watch_cycle_number=17,
                                      snapshot_set_id=42, operational_score_sha256="a" * 64)
    plan_token = runtime._PLANNED_CAPTURE.set(planned)
    try:
        with patch.object(runtime, "SINK", sink), patch.object(event_store, "WRITER", writer), \
                patch.object(runtime, "google_sheets_sync", sheets), patch.object(runtime, "combined_liquidity_shadow", shadow):
            result = fn(source, event_time=NOW, persist=persist, delivery_status=delivery,
                        delivery_attempted_at_utc=ATTEMPT,
                        delivered_at_utc=DELIVERED if delivery.strip().upper() == "DELIVERED" else None)
    finally:
        runtime._PLANNED_CAPTURE.reset(plan_token)
        runtime.reset_watch_context(token)
    return {"trace": trace, "shadow": shadow, "result": result, "planned": planned, "source": source}


class HookTests(unittest.TestCase):
    def test_frozen_baseline_function_provenance(self):
        self.assertEqual(hashlib.sha256(FROZEN_EMIT_SOURCE.encode()).hexdigest(), "db5be0f4dbb76fa35b61be8c66dbb0bf326b5de332ee89ddbca5e1299f65716c")
        self.assertEqual(hashlib.sha256(FROZEN_COMBINED_SOURCE.encode()).hexdigest(), "8e0f727391100999afab65f1571011eceeaf6b420e332dbdaa339d7d9c6d0392")

    def test_only_expected_function_additions(self):
        node = ast.parse(inspect.getsource(runtime._emit)).body[0]
        index = next(i for i, arg in enumerate(node.args.kwonlyargs) if arg.arg == "combined_liquidity_source")
        node.args.kwonlyargs.pop(index)
        node.args.kw_defaults.pop(index)
        persist = next(n for n in node.body if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == "persist")
        persist.body = [n for n in persist.body if not any(isinstance(x, ast.Attribute) and x.attr == "submit_producer_capture" for x in ast.walk(n))]
        self.assertEqual(ast.dump(node, include_attributes=False), ast.dump(ast.parse(FROZEN_EMIT_SOURCE).body[0], include_attributes=False))
        combined = ast.parse(inspect.getsource(runtime.capture_combined_confirmation)).body[0]
        for call in (n for n in ast.walk(combined) if isinstance(n, ast.Call)):
            call.keywords = [kw for kw in call.keywords if not (kw.arg is None and any(
                isinstance(node, ast.Constant) and node.value == "combined_liquidity_source" for node in ast.walk(kw.value)))]
        self.assertEqual(ast.dump(combined, include_attributes=False), ast.dump(ast.parse(FROZEN_COMBINED_SOURCE).body[0], include_attributes=False))

    def test_original_traces_values_and_or_result_unchanged(self):
        for remembered in (False, True):
            for queued in (False, True):
                for delivery in ("DELIVERED", "DELIVERY_FAILED", "APPROVED_FOR_DELIVERY", "UNKNOWN"):
                    with self.subTest(remembered=remembered, queued=queued, delivery=delivery):
                        args = dict(remembered=remembered, queued=queued, delivery=delivery)
                        old, new = exercise(old=True, **args), exercise(**args)
                        self.assertEqual([call for call in new["trace"] if call[0] != "shadow"], old["trace"])
                        self.assertEqual(new["result"], old["result"])
                        self.assertEqual(new["result"], bool(remembered or queued))
                        self.assertEqual(len(new["shadow"].observations), int(queued and delivery == "DELIVERED"))

    def test_shadow_runs_after_original_sink_writer_and_sheets(self):
        result = exercise(remembered=False, queued=True)
        self.assertEqual([call[0] for call in result["trace"]], ["sink", "writer", "sheets", "shadow"])
        self.assertTrue(result["result"])

    def test_original_queue_refusal_is_counted_without_shadow_observation(self):
        result = exercise(queued=False)
        self.assertEqual(result["shadow"].observations, [])
        self.assertEqual(result["shadow"].skips, ["SOURCE_NOT_QUEUED"])

    def test_preview_is_identical_and_never_calls_sink_writer_or_shadow(self):
        old, new = exercise(old=True, preview=True), exercise(preview=True)
        self.assertTrue(new["result"])
        self.assertEqual(new["planned"], old["planned"])
        self.assertEqual(new["trace"], [])
        self.assertEqual(new["shadow"].observations, [])
        self.assertEqual(new["shadow"].skips, [])

    def test_dry_run_is_identical_and_never_submits_or_counts_skip(self):
        old, new = exercise(old=True, persist=False), exercise(persist=False)
        self.assertEqual(new["trace"], old["trace"])
        self.assertEqual(new["shadow"].observations, [])
        self.assertEqual(new["shadow"].skips, [])

    def test_other_symbols_are_unchanged_and_excluded_from_doge_contract(self):
        old, new = exercise(old=True, symbol="BTC"), exercise(symbol="BTC")
        self.assertEqual(new["trace"], old["trace"])
        self.assertEqual(new["shadow"].observations, [])
        self.assertEqual(new["shadow"].skips, [])

    def test_only_doge_passes_the_new_private_emit_argument(self):
        expected = {"persist": True, "capture_stage": "TELEGRAM_COMBINED_ALERT", "delivery_status": "DELIVERED",
                    "delivery_attempted_at_utc": ATTEMPT, "delivered_at_utc": DELIVERED}
        for symbol in ("BTC", "ETH", "DOGE"):
            with self.subTest(symbol=symbol):
                source = candidate(symbol)
                with patch.object(runtime, "_emit", return_value=True) as emit:
                    self.assertTrue(runtime.capture_combined_confirmation(source, event_time=NOW, persist=True,
                                    delivery_status="DELIVERED", delivery_attempted_at_utc=ATTEMPT,
                                    delivered_at_utc=DELIVERED))
                actual_expected = dict(expected)
                if symbol == "DOGE":
                    actual_expected["combined_liquidity_source"] = source["top_item"]
                self.assertEqual(emit.call_args.kwargs, actual_expected)

    def test_watch_context_and_original_missing_root_contract_preserved(self):
        result = exercise()
        captured = result["shadow"].observations[0]
        snapshot = captured["event"]["engine_snapshot"]
        self.assertEqual(snapshot["watch_scan_id"], "shared-watch:test-producer")
        self.assertEqual(snapshot["snapshot_set_id"], 42)
        self.assertEqual(captured["event"]["source_side"], "SHORT")
        self.assertEqual(captured["event"]["direction"], "LONG")
        self.assertEqual(snapshot["displayed_direction"], "SHORT")
        self.assertEqual(snapshot["analysis_direction"], "LONG")
        self.assertFalse({"near_amount", "far_amount", "near_share_pct"} & set(snapshot))
        self.assertEqual(len(snapshot["maxpain_timeframes"]), 1)
        self.assertEqual(captured["selected_top_item"]["near_amount"], 492970)
        self.assertEqual(captured["metadata"], {"event_writer_queue_accepted": True,
                         "capture_stage": "TELEGRAM_COMBINED_ALERT", "delivery_status": "DELIVERED",
                         "delivery_attempted_at_utc": ATTEMPT, "delivered_at_utc": DELIVERED})

    def test_caller_mutation_after_capture_does_not_change_eager_payload(self):
        source = candidate()
        result = exercise(source=source)
        source["top_item"]["near_amount"] = -1
        source["top_item"]["maxpain_timeframes"][0]["near_amount"] = -1
        captured = result["shadow"].observations[0]
        self.assertEqual(captured["selected_top_item"]["near_amount"], 492970)
        self.assertEqual(captured["event"]["engine_snapshot"]["maxpain_timeframes"][0]["near_amount"], 492970)

    def test_observer_and_skip_failures_preserve_original_result(self):
        for queued in (False, True):
            with self.subTest(queued=queued):
                result = exercise(queued=queued, shadow_error=RuntimeError("private observer failure"))
                self.assertTrue(result["result"])
                self.assertEqual(len([call for call in result["trace"] if call[0] == "writer"]), 1)

    def test_original_writer_exception_is_not_replaced(self):
        marker = RuntimeError("original source writer failure")
        old, new = exercise(old=True, queue_error=marker), exercise(queue_error=marker)
        self.assertFalse(new["result"])
        self.assertEqual(new["trace"], old["trace"])
        self.assertEqual(new["shadow"].observations, [])

    def test_missing_shadow_import_does_not_suppress_original_event(self):
        trace = []
        with patch.object(runtime, "combined_liquidity_shadow", None), patch.object(runtime, "SINK", Sink(trace, True)), \
                patch.object(event_store, "WRITER", Writer(trace, True)), \
                patch.object(runtime.google_sheets_sync, "enqueue_delivered_event", lambda *args, **kwargs: None):
            self.assertTrue(runtime.capture_combined_confirmation(candidate(), event_time=NOW, persist=True,
                            delivered_at_utc=DELIVERED, delivery_status="DELIVERED"))
        self.assertEqual([call[0] for call in trace], ["sink", "writer"])

    def test_non_combined_family_has_no_shadow_even_if_optional_source_is_supplied(self):
        event = runtime.research_event_capture.build_generic_alert_event(symbol="DOGE", event_type="MAX_PAIN_ALERT", direction="LONG",
                event_time=NOW, current_price=1, target_price=2, engine_snapshot={}, setup_identity={})
        trace, shadow = [], Shadow([])
        with patch.object(runtime, "combined_liquidity_shadow", shadow), patch.object(runtime, "SINK", Sink(trace, True)), \
                patch.object(event_store, "WRITER", Writer(trace, True)), \
                patch.object(runtime.google_sheets_sync, "enqueue_delivered_event", lambda *args, **kwargs: None):
            self.assertTrue(runtime._emit(event, persist=True, delivery_status="DELIVERED", delivered_at_utc=DELIVERED,
                            combined_liquidity_source=candidate()["top_item"]))
        self.assertEqual(shadow.observations, [])

    def test_status_is_nested_redacted_and_fail_open(self):
        expected = {"state": "ARMED", "enrolled_in_existing_study": False}
        with patch.object(runtime, "combined_liquidity_shadow", SimpleNamespace(status=lambda: expected)):
            self.assertEqual(runtime.status()["combined_liquidity_shadow"], expected)
        for value in (None, SimpleNamespace(status=lambda: 1 / 0)):
            with patch.object(runtime, "combined_liquidity_shadow", value):
                self.assertEqual(runtime.status()["combined_liquidity_shadow"],
                                 {"state": "UNAVAILABLE", "enrolled_in_existing_study": False})

    def test_real_core_freezes_exact_writer_material_without_source_enrichment(self):
        import research_combined_liquidity_shadow as core
        import research_consumed_inputs as graph
        pending, trace = queue.Queue(maxsize=4), []
        source = candidate()
        metrics = deepcopy(core._METRICS)
        with patch.object(runtime, "combined_liquidity_shadow", core), \
                patch.object(runtime, "SINK", Sink(trace, True)), \
                patch.object(event_store, "WRITER", Writer(trace, True)), \
                patch.object(runtime.google_sheets_sync, "enqueue_delivered_event", lambda *args, **kwargs: None), \
                patch.object(core, "PROCESS_STARTED_AT_UTC", (NOW - timedelta(minutes=1)).isoformat()), \
                patch.object(core, "utc_now", return_value=(DELIVERED + timedelta(seconds=1)).isoformat()), \
                patch.object(core, "_ensure_worker", return_value=True), \
                patch.object(core, "_QUEUE", pending), patch.object(core, "_METRICS", metrics):
            token = runtime.set_watch_context(watch_scan_id="shared-watch:real-core", snapshot_set_id=42)
            try:
                self.assertTrue(runtime.capture_combined_confirmation(source, event_time=NOW, persist=True,
                                delivery_status="DELIVERED", delivery_attempted_at_utc=ATTEMPT,
                                delivered_at_utc=DELIVERED))
            finally:
                runtime.reset_watch_context(token)
        observation, size = pending.get_nowait()
        self.assertGreater(size, 0)
        source["top_item"]["near_amount"] = -1
        package = core.package_from_observation(observation)
        self.assertEqual(observation["manifest"]["capture_status"], "CAPTURED")
        payloads = {header["kind"]: value for header, value in graph.event_payloads(package)}
        producer = payloads["producer_input"]
        self.assertEqual(producer["selected_top_item"]["near_amount"], 492970)
        self.assertFalse({"near_amount", "far_amount", "near_share_pct"} & set(producer["event"]["engine_snapshot"]))
        expected_source = core.normalize_source_material(next(call[1] for call in trace if call[0] == "writer"))
        self.assertEqual(producer["source_material"], expected_source)
        context = payloads["derived_context"]
        self.assertEqual(context["shadow.combined_liquidity.capture_status"], "VALID")
        self.assertAlmostEqual(context["shadow.combined_liquidity.selected_share_pct"], 45.28102582002223)
        self.assertEqual(context["shadow.combined_liquidity.alignment"], "BALANCED")
        self.assertIs(context["enrolled_in_existing_study"], False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
