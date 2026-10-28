"""Ordinary child failures retain private diagnostics after temporary-file cleanup.

Run the installed scanner, task wrapper, store SQL and authenticated read routes.
Only the process launcher and PostgreSQL connection are replaced; there is no
browser, source request, inference, production connection or worksheet write.
"""
import asyncio
import base64
import contextlib
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import web

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runtime"))
import capture_failure_diagnostics as failure_diagnostics
import collection_bridge as bridge
import heatmap_hover
import heatmap_models
import model1_execution as execution

JOB = "00000000-0000-4000-8000-000000000001"
OTHER_JOB = "00000000-0000-4000-8000-000000000002"
TOKEN = "synthetic-token-for-offline-test-only-0000"
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jR3sAAAAASUVORK5CYII="
)


class JsonParameter:
    """Stand in for the driver's JSON adapter, never for store SQL itself."""
    def __init__(self, value):
        self.obj = value


class MemoryPostgres:
    def __init__(self, directories):
        self.directories = directories
        self.commands = []
        self.evidence = {}
        self.job = {
            "id": JOB, "timeframe": "24H", "heatmap_model": 2,
            "status": "running", "result": None, "image_png": None,
            "error_code": None,
        }

    def connect(self, _store):
        database = self

        class Connection:
            row = None

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, parameters=None):
                statement = " ".join(sql.split())
                database.commands.append((statement, parameters))
                if statement.startswith("INSERT INTO public.ai_collection_bridge_evidence"):
                    assert database.directories[0].exists(), "retain before temporary cleanup"
                    assert "VALUES(%s,%s,%s)" in statement
                    jid, image, diagnostic = parameters
                    database.evidence[jid] = {
                        "image_png": bytes(image) if image is not None else None,
                        # A serialized copy cannot depend on live exception objects.
                        "diagnostic": json.loads(json.dumps(diagnostic.obj, allow_nan=False)),
                    }
                elif statement.startswith("UPDATE public.ai_collection_bridge_evidence SET image_png=NULL"):
                    assert "interval '6 hours'" in statement
                elif statement.startswith("UPDATE public.ai_collection_bridge_jobs SET status=%s"):
                    status, result, image, error, jid = parameters
                    assert jid == JOB and database.job["status"] == "running"
                    database.job.update(status=status, result=result.obj if result else None,
                                        image_png=image, error_code=error)
                elif statement.startswith("SELECT COALESCE(j.image_png,e.image_png)"):
                    assert "j.status IN ('ready','failed')" in statement
                    assert "interval '6 hours'" in statement
                    jid, model = parameters
                    self.row = (
                        {"image_png": database.job["image_png"] or database.evidence[jid]["image_png"]}
                        if jid == JOB and model == 2 else None
                    )
                elif statement.startswith("SELECT id,timeframe,status,result,error_code,heatmap_model"):
                    jid, model = parameters
                    self.row = dict(database.job) if jid == JOB and model == 2 else None
                else:
                    raise AssertionError("Unexpected database operation: " + statement)
                return self

            def fetchone(self):
                return self.row

        return Connection()


def route_handler(application, path):
    return next(route.handler for route in application.router.routes()
                if route.method == "GET" and route.resource.canonical == path)


class FailureDiagnosticIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, image=PNG, corrupt=None, diagnostic_error=False):
        directories = []
        reports = []
        database = MemoryPostgres(directories)

        async def launch(*args, **kwargs):
            root = Path(args[4])
            directories.append(root)
            (root / "capture").mkdir()
            if image is not None:
                (root / "capture/coinglass_btc_heatmap_24h.png").write_bytes(image)

            def main(*_args):
                execution.stage("capture")
                with execution.capture_operation("hover_collection"):
                    error = heatmap_hover.HoverError("hover_candle_not_found")
                    error.private_text = "PRIVATE COOKIE URL RESPONSE BODY"
                    raise error

            with contextlib.ExitStack() as stack:
                stack.enter_context(patch.object(heatmap_models, "HEATMAP_MODEL", 2))
                if diagnostic_error:
                    stack.enter_context(patch.object(
                        failure_diagnostics, "save_failure_diagnostics",
                        side_effect=OSError("PRIVATE DIAGNOSTIC WRITE ERROR"),
                    ))
                returncode = execution.run_task(main, "24H", JOB, root)
            self.assertEqual(returncode, 1)
            report = json.loads((root / "error.json").read_text())
            reports.append(report)
            self.assertLess((root / "error.json").stat().st_size, 4096)
            sidecar = root / "capture_failure.json"
            if corrupt:
                self.assertTrue(sidecar.is_file())
                corrupt(sidecar)
            return SimpleNamespace(returncode=returncode, wait=AsyncMock(return_value=returncode))

        connect = lambda store: database.connect(store)
        with patch.object(asyncio, "create_subprocess_exec", side_effect=launch) as create, \
             patch.object(bridge.JobStore, "connect", new=connect), \
             patch.dict(sys.modules, {"psycopg.types.json": SimpleNamespace(Jsonb=JsonParameter)}), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(bridge.BridgeError) as failed:
                await bridge.run_existing_scanner("24H", JOB, 2)
            self.assertEqual((failed.exception.code, failed.exception.status),
                             ("source_capture_failed", 502))
            create.assert_called_once()
            self.assertFalse(directories[0].exists())
            store = bridge.JobStore("synthetic-no-network", heatmap_model=2)
            # The worker's existing failure branch writes no accepted result.
            store.finish(JOB, None, None, failed.exception.code)
            self.assertEqual(database.job["status"], "failed")
            self.assertIsNone(database.job["result"])

            application = web.Application()
            bridge.register_collection_routes(
                application, store=store, token=TOKEN, enabled=True, start_worker=False,
            )
            request = SimpleNamespace(headers={"Authorization": "Bearer " + TOKEN},
                                      match_info={"job_id": JOB})
            prefix = "/api/collection/model2/jobs/{job_id}"
            response = await route_handler(application, prefix)(request)
            self.assertEqual(response.status, 200)
            public = json.loads(response.body)
            self.assertEqual(public["status"], "failed")
            self.assertIsNone(public["result"])
            self.assertEqual(public["error"]["code"], "source_capture_failed")
            self.assertNotIn("capture_failure", response.text)
            evidence = await route_handler(application, prefix + "/evidence")(request)
            self.assertEqual(evidence.status, 200 if image else 404)
            if image:
                self.assertEqual(evidence.body, image)
                self.assertEqual(evidence.content_type, "image/png")
                self.assertEqual(evidence.headers["Cache-Control"], "no-store")
            before = len(database.commands)
            unauthorized = SimpleNamespace(headers={}, match_info={"job_id": JOB})
            denied = await route_handler(application, prefix + "/evidence")(unauthorized)
            self.assertEqual(denied.status, 401)
            self.assertEqual(len(database.commands), before)

        diagnostic = database.evidence[JOB]["diagnostic"]
        self.assertFalse(diagnostic["assessment_validated"])
        self.assertEqual(reports[0]["validation_reason"], "hover_candle_not_found")
        self.assertNotIn("PRIVATE", json.dumps(diagnostic) + output.getvalue())
        self.assertNotIn(TOKEN, output.getvalue())
        self.assertFalse(any("INSERT INTO public.ai_collection_bridge_jobs" in sql
                             for sql, _ in database.commands))
        return diagnostic

    async def test_normal_failure_keeps_bound_metadata_and_png_after_cleanup(self):
        diagnostic = await self.exercise()
        detail = diagnostic["capture_failure"]
        self.assertEqual(detail["format_version"], "capture-failure.v1")
        self.assertEqual((detail["job_id"], detail["heatmap_model"], detail["timeframe"]),
                         (JOB, 2, "24H"))
        self.assertEqual(detail["reason"], "hover_candle_not_found")
        self.assertEqual(detail["image_sha256"], diagnostic["image_sha256"])
        self.assertEqual(len(detail["reader_sha256"]), 64)

    async def test_normal_failure_without_png_keeps_diagnostic_and_no_fake_evidence(self):
        diagnostic = await self.exercise(image=None)
        self.assertFalse(diagnostic["image_retained"])
        self.assertEqual(diagnostic["capture_failure"]["job_id"], JOB)
        self.assertIsNone(diagnostic["capture_failure"]["image_sha256"])

    async def test_unbound_or_oversized_sidecar_cannot_replace_the_original_failure(self):
        def wrong_job(path):
            value = json.loads(path.read_text())
            value["job_id"] = OTHER_JOB
            path.write_text(json.dumps(value))

        def wrong_image(path):
            value = json.loads(path.read_text())
            value["image_sha256"] = "0" * 64
            path.write_text(json.dumps(value))

        def oversized(path):
            path.write_text("PRIVATE" * 3000)

        for corrupt in (wrong_job, wrong_image, oversized):
            with self.subTest(corrupt=corrupt.__name__):
                diagnostic = await self.exercise(corrupt=corrupt)
                self.assertNotIn("capture_failure", diagnostic)

    async def test_diagnostic_exception_does_not_change_failure_or_png_retention(self):
        diagnostic = await self.exercise(diagnostic_error=True)
        self.assertNotIn("capture_failure", diagnostic)
        self.assertTrue(diagnostic["image_retained"])


if __name__ == "__main__":
    unittest.main()
