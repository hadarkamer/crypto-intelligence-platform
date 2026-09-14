"""Offline API/contract tests. SQL tests use ONLY isolated TEST_DATABASE_URL."""
import copy
import json
import os
import sys
import tempfile
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from collection_bridge import BridgeError, JobStore, SCHEMA, envelope, register_collection_routes, request_fields
from collection_model1_task import normalize, main as run_task, SOURCE

TOKEN = "synthetic-test-token-never-a-real-secret-42"


def sample():
    def zone(low, high, strength="strong", confidence="high"):
        return {"low_price": low, "high_price": high, "relative_strength": strength, "confidence": confidence}
    return {"symbol": "BTC", "analysis_mode": "visual_screenshot", "model": "test-model",
            "scans": [{"timeframe": "12h", "current_price_estimate": 100,
                       "current_price_confidence": "high", "short_summary": "synthetic",
                       "above_price": {"main_zone": zone(110, 115), "secondary_zones": []},
                       "below_price": {"main_zone": zone(85, 90, "medium"), "secondary_zones": []}}]}


def convert(raw=None, **kwargs):
    values = dict(timeframe="12H", run_id=str(uuid4()), captured_at=datetime.now(timezone.utc).isoformat(), image=b"synthetic-image")
    values.update(kwargs)
    return normalize(sample() if raw is None else raw, **values)


class ContractTests(unittest.TestCase):
    def test_request_is_exact_and_uuid(self):
        rid = str(uuid4())
        self.assertEqual(request_fields({"request_id": rid, "timeframe": "12H"}), (rid, "12H"))
        for body in ({"request_id": rid, "timeframe": "48H"}, {"request_id": "bad", "timeframe": "12H"},
                     {"request_id": rid, "timeframe": "12H", "url": "https://bad.example"}, [], None):
            with self.subTest(body=body), self.assertRaises(BridgeError):
                request_fields(body)

    def test_legacy_output_converts_without_another_model(self):
        result = convert()
        self.assertEqual(result["schema_version"], SCHEMA)
        self.assertEqual(result["provider"], "OpenAI")
        self.assertEqual(result["quality"], "visual_estimate")
        self.assertEqual([x["intensity"] for x in result["zones"]], ["many", "normal"])
        self.assertIsNone(result["source_updated_at"])
        self.assertIsNone(result["usage"])
        self.assertEqual(len(result["evidence"]["sha256"]), 64)

    def test_wrong_identity_rejected(self):
        for patch_value in ({"symbol": "ETH"}, {"analysis_mode": "text"}, {"model": ""}, {"scans": []}):
            with self.subTest(patch_value=patch_value), self.assertRaises(ValueError):
                convert({**sample(), **patch_value})
        for tf in ("24H", "48H"):
            with self.subTest(tf=tf), self.assertRaises(ValueError):
                convert(timeframe=tf)

    def test_uncertain_current_price_rejected(self):
        for confidence in ("medium", "low", None):
            raw = sample()
            raw["scans"][0]["current_price_confidence"] = confidence
            with self.subTest(confidence=confidence), self.assertRaises(ValueError):
                convert(raw)

    def test_bad_prices_do_not_get_coerced(self):
        for value in (True, "100", None, 0, -1, float("nan"), float("inf")):
            raw = sample()
            raw["scans"][0]["current_price_estimate"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                convert(raw)

    def test_crossing_or_reversed_zones_rejected(self):
        for low, high in ((99, 110), (115, 110), (100, 101)):
            raw = sample()
            raw["scans"][0]["above_price"]["main_zone"].update(low_price=low, high_price=high)
            with self.subTest(low=low), self.assertRaises(ValueError):
                convert(raw)

    def test_unreadable_zones_not_filled_with_zero(self):
        raw = sample()
        raw["scans"][0]["above_price"]["main_zone"]["low_price"] = None
        self.assertEqual([z["side"] for z in convert(raw)["zones"]], ["below"])
        raw["scans"][0]["below_price"]["main_zone"]["confidence"] = "low"
        with self.assertRaises(ValueError):
            convert(raw)

    def test_repeated_zone_not_duplicated(self):
        raw = sample()
        side = raw["scans"][0]["above_price"]
        side["secondary_zones"].append(copy.deepcopy(side["main_zone"]))
        self.assertEqual(len(convert(raw)["zones"]), 2)

    def test_explicit_strength_mapping(self):
        for strength, expected in (("very_strong", "many"), ("strong", "many"), ("medium", "normal"), ("weak", "few")):
            raw = sample()
            raw["scans"][0]["above_price"]["main_zone"]["relative_strength"] = strength
            self.assertEqual(convert(raw)["zones"][0]["intensity"], expected)

    def test_envelope_never_exposes_nonready_result(self):
        row = {"id": uuid4(), "timeframe": "12H", "status": "failed", "result": {"private": "bad"}, "error_code": "scan_failed"}
        self.assertIsNone(envelope(row)["result"])
        self.assertNotIn("private", json.dumps(envelope(row)))


class FakeStore:
    def __init__(self):
        self.starts, self.gets, self.rows = 0, 0, {}
    def start(self, rid, tf):
        self.starts += 1
        if rid not in self.rows:
            self.rows[rid] = {"schema_version": SCHEMA, "job_id": rid, "timeframe": tf, "status": "queued", "result": None, "error": None}
        elif self.rows[rid]["timeframe"] != tf:
            raise BridgeError("idempotency_conflict", "conflict", 409)
        return self.rows[rid]
    def get(self, jid):
        self.gets += 1
        return self.rows.get(jid)
    def evidence(self, jid):
        return None


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = FakeStore()
        app = web.Application()
        register_collection_routes(app, store=self.store, token=TOKEN, enabled=True, start_worker=False)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.headers = {"Authorization": "Bearer " + TOKEN}

    async def asyncTearDown(self):
        await self.client.close()

    async def test_unauthorized_starts_nothing(self):
        for headers in ({}, {"Authorization": "Bearer wrong"}):
            response = await self.client.post("/api/collection/model1/jobs", headers=headers, json={"request_id": str(uuid4()), "timeframe": "12H"})
            self.assertEqual(response.status, 401)
        self.assertEqual(self.store.starts, 0)

    async def test_start_then_get_poll_is_read_only(self):
        rid = str(uuid4())
        response = await self.client.post("/api/collection/model1/jobs", headers=self.headers, json={"request_id": rid, "timeframe": "12H"})
        self.assertEqual(response.status, 202)
        for _ in range(3):
            r = await self.client.get("/api/collection/model1/jobs/" + rid, headers=self.headers)
            self.assertEqual((await r.json())["status"], "queued")
        self.assertEqual(self.store.starts, 1)
        self.assertEqual(self.store.gets, 3)

    async def test_invalid_inputs_no_store_writes(self):
        for body in ({"request_id": str(uuid4()), "timeframe": "48H"}, {"url": "bad"}):
            r = await self.client.post("/api/collection/model1/jobs", headers=self.headers, json=body)
            self.assertEqual(r.status, 400)
        self.assertEqual(self.store.starts, 0)

    async def test_unknown_job_does_not_start_one(self):
        r = await self.client.get("/api/collection/model1/jobs/" + str(uuid4()), headers=self.headers)
        self.assertEqual(r.status, 404)
        self.assertEqual(self.store.starts, 0)

    async def test_disabled_api_does_not_initialize_provider(self):
        app = web.Application()
        register_collection_routes(app, store=self.store, token=TOKEN, enabled=False, start_worker=False)
        async with TestClient(TestServer(app)) as client:
            r = await client.post("/api/collection/model1/jobs", headers=self.headers, json={"request_id": str(uuid4()), "timeframe": "12H"})
            self.assertEqual(r.status, 503)
        self.assertEqual(self.store.starts, 0)

    async def test_short_token_disables_api(self):
        app = web.Application()
        register_collection_routes(app, store=self.store, token="short", enabled=True, start_worker=False)
        async with TestClient(TestServer(app)) as client:
            r = await client.get("/api/collection/model1/jobs/" + str(uuid4()), headers={"Authorization": "Bearer short"})
            self.assertEqual(r.status, 503)

    async def test_oversize_body_is_rejected(self):
        r = await self.client.post("/api/collection/model1/jobs", headers=self.headers, data=" " * 3000)
        self.assertEqual(r.status, 413)
        self.assertEqual(self.store.starts, 0)

    async def test_store_error_is_sanitized(self):
        def fail(*args):
            raise RuntimeError("PRIVATE_DATABASE_URL")
        self.store.get = fail
        r = await self.client.get("/api/collection/model1/jobs/" + str(uuid4()), headers=self.headers)
        self.assertEqual(r.status, 503)
        self.assertNotIn("PRIVATE_DATABASE_URL", await r.text())


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "requires isolated TEST_DATABASE_URL, never production DATABASE_URL")
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.store = JobStore(os.environ["TEST_DATABASE_URL"], hourly_limit=2)
        self.store.initialize()
        with self.store.connect() as c:
            c.execute("TRUNCATE public.ai_collection_bridge_requests, public.ai_collection_bridge_jobs")

    def test_same_request_is_durable_and_no_new_job(self):
        rid = str(uuid4())
        first = self.store.start(rid, "12H")
        second = JobStore(os.environ["TEST_DATABASE_URL"]).start(rid, "12H")
        self.assertEqual(first["job_id"], second["job_id"])
        with self.assertRaises(BridgeError):
            self.store.start(rid, "24H")

    def test_parallel_requests_share_active_job(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _: self.store.start(str(uuid4()), "12H"), range(4)))
        self.assertEqual(len({v["job_id"] for v in values}), 1)

    def test_result_persisted_and_reused_before_app_save(self):
        first = self.store.start(str(uuid4()), "12H")
        held, row = self.store.claim()
        try:
            result = convert(run_id=first["job_id"])
            self.store.finish(first["job_id"], result, b"synthetic")
        finally:
            held.close()
        second = self.store.start(str(uuid4()), "12H")
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["result"], result)
        self.assertEqual(self.store.get(first["job_id"])["result"], result)

    def test_failure_not_retried_by_poll_or_same_request(self):
        rid = str(uuid4())
        first = self.store.start(rid, "12H")
        held, row = self.store.claim()
        try:
            self.store.finish(first["job_id"], None, None, "source_or_analysis_failed")
        finally:
            held.close()
        self.assertEqual(self.store.start(rid, "12H")["status"], "failed")
        self.assertEqual(self.store.get(first["job_id"])["status"], "failed")
        self.assertIsNone(self.store.claim())

    def test_global_worker_lock_prevents_double_execution(self):
        self.store.start(str(uuid4()), "12H")
        self.store.start(str(uuid4()), "24H")
        held, row = self.store.claim()
        try:
            self.assertIsNone(self.store.claim())
        finally:
            held.close()

    def test_hourly_budget_blocks_new_capture_not_cache(self):
        for tf in ("12H", "24H"):
            r = self.store.start(str(uuid4()), tf)
            held, row = self.store.claim()
            try:
                self.store.finish(r["job_id"], None, None, "failed")
            finally:
                held.close()
        with self.assertRaises(BridgeError) as ctx:
            self.store.start(str(uuid4()), "12H")
        self.assertEqual(ctx.exception.status, 429)

    def test_interrupted_running_job_is_failed_not_replayed(self):
        r = self.store.start(str(uuid4()), "12H")
        held, row = self.store.claim()
        held.close()
        with self.store.connect() as c:
            c.execute("UPDATE public.ai_collection_bridge_jobs SET started_at=now()-interval '16 minutes' WHERE id=%s", (r["job_id"],))
        self.assertIsNone(self.store.claim())
        self.assertEqual(self.store.get(r["job_id"])["status"], "failed")


class LegacyReuseTests(unittest.TestCase):
    def test_existing_public_functions_called_once_and_result_saved(self):
        calls = []
        def capture(output_dir, *, timeframes=("12h", "24h"), url=None):
            calls.append(("capture", timeframes))
            Path(output_dir).mkdir(parents=True)
            image = Path(output_dir) / "chart.png"
            image.write_bytes(b"\x89PNG\r\n\x1a\nsynthetic")
            return [{"image": str(image), "timeframe": timeframes[0], "liquidity_threshold": 0.85}]
        def analyze(images, *, symbol="BTC", timeout_seconds=120):
            calls.append(("analysis", len(images)))
            self.assertNotIn("liquidity_threshold", images[0])
            return sample()
        modules = {
          "market_vision.coinglass_heatmap_capture": types.SimpleNamespace(capture_heatmaps=capture, COINGLASS_HEATMAP_URL=SOURCE),
          "market_vision.openai_heatmap_scanner": types.SimpleNamespace(analyze_heatmap_images=analyze),
        }
        with tempfile.TemporaryDirectory() as root, patch.dict(sys.modules, modules):
            rid = str(uuid4())
            run_task("12H", rid, root)
            result = json.loads((Path(root)/"result.json").read_text())
            self.assertEqual(result["run_id"], rid)
            self.assertTrue((Path(root)/"image.png").is_file())
        self.assertEqual(calls, [("capture", ("12h",)), ("analysis", 1)])

    def test_failed_capture_never_calls_analysis_or_retries(self):
        calls = []
        def capture(*args, **kwargs):
            calls.append("capture")
            raise RuntimeError("synthetic source failure")
        def analyze(*args, **kwargs):
            calls.append("analysis")
        modules = {
          "market_vision.coinglass_heatmap_capture": types.SimpleNamespace(capture_heatmaps=capture, COINGLASS_HEATMAP_URL=SOURCE),
          "market_vision.openai_heatmap_scanner": types.SimpleNamespace(analyze_heatmap_images=analyze),
        }
        with tempfile.TemporaryDirectory() as root, patch.dict(sys.modules, modules):
            with self.assertRaises(RuntimeError):
                run_task("12H", str(uuid4()), root)
            self.assertFalse((Path(root)/"result.json").exists())
        self.assertEqual(calls, ["capture"])


if __name__ == "__main__":
    unittest.main()
