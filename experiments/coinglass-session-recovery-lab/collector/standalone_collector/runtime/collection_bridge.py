"""Optional, authenticated Model-1 API for the EXISTING candidate scanner.

No trading tables, Telegram commands, scheduled scans, or provider keys are changed.
POST creates/reuses a durable job. GET never starts capture or model inference.
The two new PostgreSQL tables are initialized only when explicitly enabled.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from aiohttp import web
from store_diagnostics import log_store_failure
from heatmap_models import model_number, schema_version, source_url, child_environment
from model1_cache_policy import latest_ready, error_response

SCHEMA = "coinglass-model1.v1"
SOURCE = "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol"
TIMEFRAMES = ("12H", "24H", "48H")
CREATE_LOCK = 748230101
WORK_LOCK = 748230102
MAX_IMAGE = 4 * 1024 * 1024
MAX_RESULT = 256 * 1024


class BridgeError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code, self.status = code, status


def request_fields(body: Any) -> tuple[str, str]:
    if not isinstance(body, dict) or set(body) != {"request_id", "timeframe"}:
        raise BridgeError("invalid_request", "Required fields: request_id, timeframe")
    try:
        rid = str(UUID(body["request_id"]))
    except (ValueError, AttributeError, TypeError):
        raise BridgeError("invalid_request_id", "request_id must be a UUID") from None
    tf = body["timeframe"]
    if tf not in TIMEFRAMES:
        raise BridgeError("unsupported_timeframe", "Only 12H, 24H and 48H are supported")
    return rid, tf


def envelope(row: dict[str, Any]) -> dict[str, Any]:
    err = row.get("error_code")
    return {"schema_version": schema_version(row.get("heatmap_model", 1)), "job_id": str(row["id"]),
            "timeframe": row["timeframe"], "status": row["status"],
            "result": row.get("result") if row["status"] == "ready" else None,
            "error": {"code": err, "message": "האיסוף לא הושלם; לא הופעל ניסיון חוזר אוטומטי."} if err else None}


class JobStore:
    """Small isolated PostgreSQL job ledger, using the existing database service."""
    def __init__(self, dsn: str, heatmap_model: int = 1):
        self.dsn = dsn
        self.heatmap_model = model_number(heatmap_model)

    def connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5,
                               options="-c statement_timeout=8000 -c lock_timeout=3000")

    def initialize(self) -> None:
        with self.connect() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_jobs (
                id uuid PRIMARY KEY, timeframe text NOT NULL CHECK(timeframe IN ('12H','24H','48H')),
                status text NOT NULL CHECK(status IN ('queued','running','ready','failed')),
                created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz,
                finished_at timestamptz, result jsonb, image_png bytea, error_code text)""")
            c.execute("ALTER TABLE public.ai_collection_bridge_jobs ADD COLUMN IF NOT EXISTS heatmap_model integer NOT NULL DEFAULT 1 CHECK (heatmap_model IN (1,2,3))")
            c.execute("DROP INDEX IF EXISTS public.ai_collection_bridge_one_active_tf")
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_one_active_model_tf
                ON public.ai_collection_bridge_jobs(heatmap_model,timeframe) WHERE status IN ('queued','running')""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_requests (
                id uuid PRIMARY KEY, timeframe text NOT NULL, job_id uuid NOT NULL
                REFERENCES public.ai_collection_bridge_jobs(id) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now())""")
            c.execute("ALTER TABLE public.ai_collection_bridge_requests ADD COLUMN IF NOT EXISTS heatmap_model integer NOT NULL DEFAULT 1 CHECK (heatmap_model IN (1,2,3))")
            row = c.execute("""SELECT pg_get_constraintdef(oid) AS definition
                FROM pg_constraint WHERE conrelid='public.ai_collection_bridge_jobs'::regclass
                AND conname='ai_collection_bridge_jobs_timeframe_check'""").fetchone()
            if row and "48H" not in row['definition']:
                c.execute("""ALTER TABLE public.ai_collection_bridge_jobs
                    DROP CONSTRAINT ai_collection_bridge_jobs_timeframe_check,
                    ADD CONSTRAINT ai_collection_bridge_jobs_timeframe_check
                    CHECK(timeframe IN ('12H','24H','48H'))""")
            c.execute("REVOKE ALL ON public.ai_collection_bridge_jobs, public.ai_collection_bridge_requests FROM PUBLIC")
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_evidence (
                job_id uuid PRIMARY KEY REFERENCES public.ai_collection_bridge_jobs(id) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now(), image_png bytea,
                diagnostic jsonb NOT NULL)""")
            c.execute("REVOKE ALL ON public.ai_collection_bridge_evidence FROM PUBLIC")


    def retain_evidence(self, jid, image, diagnostic):
        from psycopg.types.json import Jsonb
        with self.connect() as c:
            c.execute("""INSERT INTO public.ai_collection_bridge_evidence(job_id,image_png,diagnostic)
                VALUES(%s,%s,%s) ON CONFLICT(job_id) DO UPDATE SET
                image_png=EXCLUDED.image_png,diagnostic=EXCLUDED.diagnostic""",
                (jid,image,Jsonb(diagnostic)))
            c.execute("""UPDATE public.ai_collection_bridge_evidence SET image_png=NULL
                WHERE created_at<now()-interval '6 hours' AND image_png IS NOT NULL""")

    def start(self, rid: str, tf: str) -> dict[str, Any]:
        with self.connect() as c:
            c.execute("SELECT pg_advisory_xact_lock(%s)", (CREATE_LOCK,))
            prior = c.execute("""SELECT r.timeframe AS requested_timeframe, r.heatmap_model AS requested_model, j.*
                FROM public.ai_collection_bridge_requests r JOIN public.ai_collection_bridge_jobs j
                ON j.id=r.job_id WHERE r.id=%s""", (rid,)).fetchone()
            if prior:
                if prior["requested_timeframe"] != tf or prior.get("requested_model", 1) != self.heatmap_model:
                    raise BridgeError("idempotency_conflict", "Request id belongs to another timeframe", 409)
                return envelope(prior)
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
                error_code='interrupted', finished_at=now() WHERE status='running'
                AND started_at < now()-interval '15 minutes'""")
            c.execute("UPDATE public.ai_collection_bridge_jobs SET image_png=NULL WHERE created_at < now()-interval '6 hours' AND image_png IS NOT NULL")
            c.execute("DELETE FROM public.ai_collection_bridge_jobs WHERE created_at < now()-interval '7 days' AND status IN ('ready','failed')")
            row = c.execute("""SELECT * FROM public.ai_collection_bridge_jobs WHERE heatmap_model=%s AND timeframe=%s
                AND (status IN ('queued','running') OR (status='ready'
                AND result->>'selection_policy'='prominent_right_edge.v1'
                AND result->'hover_snapshot'->>'version'='displayed-hover.v1'
                AND (result->>'captured_at')::timestamptz >= now()-interval '15 minutes'
                AND (result->>'captured_at')::timestamptz <= now()))
                ORDER BY created_at DESC LIMIT 1""", (self.heatmap_model, tf)).fetchone()
            if row is None:
                row = c.execute("""INSERT INTO public.ai_collection_bridge_jobs(id,timeframe,status,heatmap_model)
                    VALUES(%s,%s,'queued',%s) RETURNING *""", (str(uuid4()), tf, self.heatmap_model)).fetchone()
            c.execute("INSERT INTO public.ai_collection_bridge_requests(id,timeframe,job_id,heatmap_model) VALUES(%s,%s,%s,%s)", (rid, tf, row["id"], self.heatmap_model))
            return envelope(row)

    def latest(self, tf: str) -> dict[str, Any] | None:
        return latest_ready(self, tf)

    def get(self, jid: str) -> dict[str, Any] | None:
        with self.connect() as c:
            row = c.execute("SELECT id,timeframe,status,result,error_code,heatmap_model FROM public.ai_collection_bridge_jobs WHERE id=%s AND heatmap_model=%s", (jid,self.heatmap_model)).fetchone()
            return envelope(row) if row else None

    def evidence(self, jid: str) -> bytes | None:
        with self.connect() as c:
            row = c.execute("""SELECT COALESCE(j.image_png,e.image_png) AS image_png
                FROM public.ai_collection_bridge_jobs j
                LEFT JOIN public.ai_collection_bridge_evidence e ON e.job_id=j.id
                WHERE j.id=%s AND j.heatmap_model=%s AND j.status IN ('ready','failed')
                AND j.created_at > now()-interval '6 hours'""", (jid,self.heatmap_model)).fetchone()
            return bytes(row["image_png"]) if row and row["image_png"] else None

    def claim(self) -> tuple[Any, dict[str, Any]] | None:
        c = self.connect()
        try:
            if not c.execute("SELECT pg_try_advisory_lock(%s) AS ok", (WORK_LOCK,)).fetchone()["ok"]:
                c.close()
                return None
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
                error_code='interrupted',finished_at=now() WHERE status='running'
                AND started_at < now()-interval '15 minutes'""")
            if c.execute("SELECT 1 FROM public.ai_collection_bridge_jobs WHERE status='running' LIMIT 1").fetchone():
                c.close()
                return None
            row = c.execute("""UPDATE public.ai_collection_bridge_jobs SET status='running',started_at=now()
                WHERE id=(SELECT id FROM public.ai_collection_bridge_jobs WHERE status='queued'
                ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *""",).fetchone()
            c.commit()
            if not row:
                c.close()
                return None
            return c, row
        except BaseException:
            c.close()
            raise

    def finish(self, jid: str, result: dict[str, Any] | None, image: bytes | None,
               error: str | None = None) -> None:
        from psycopg.types.json import Jsonb
        with self.connect() as c:
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET status=%s,result=%s,
                image_png=%s,error_code=%s,finished_at=now() WHERE id=%s AND status='running'""",
                ("failed" if error else "ready", Jsonb(result) if result else None, image, error, jid))


async def run_existing_scanner(tf: str, jid: str, heatmap_model: int = 1) -> tuple[dict[str, Any], bytes]:
    """One child process, one existing capture, one existing OpenAI analysis. No retries."""
    with tempfile.TemporaryDirectory(prefix="collection-model1-") as tmp:
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(__file__).with_name("collection_model1_task.py")), tf, jid, tmp,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL, env=child_environment(heatmap_model))
        try:
            await asyncio.wait_for(process.wait(), timeout=360 if heatmap_model == 3 else 300)
            # Preserve private evidence before TemporaryDirectory removes the files.
            # A stored screenshot is diagnostic evidence, NOT acceptance of prices.
            from model1_evidence_format import evidence_payload
            retained_image, diagnostic = evidence_payload(tmp, tf, job_id=jid, heatmap_model=heatmap_model)
            try:
                evidence_store = JobStore(os.getenv("DATABASE_URL", ""))
                await asyncio.to_thread(evidence_store.retain_evidence, jid, retained_image, diagnostic)
            except Exception:
                raise BridgeError("evidence_store_failed", "Private evidence could not be retained", 503) from None
            print("MODEL1_EVIDENCE " + json.dumps({"job_id": jid,
                "image_retained": retained_image is not None, "diagnostic_retained": True}), flush=True)

            if process.returncode != 0:
                from model1_execution import read_failure
                code = read_failure(Path(tmp) / "error.json", jid, process.returncode)
                raise BridgeError(code, "Collection failed at the recorded stage", 502)
            path = Path(tmp) / "result.json"
            shot = Path(tmp) / "image.png"
            if not path.is_file() or path.stat().st_size > MAX_RESULT or not shot.is_file() or shot.stat().st_size > MAX_IMAGE:
                raise BridgeError("invalid_result", "Result size or evidence invalid", 502)
            result = json.loads(path.read_text(encoding="utf-8"))
            image = shot.read_bytes()
            if not image.startswith(b"\x89PNG\r\n\x1a\n"):
                raise BridgeError("invalid_evidence", "Image evidence invalid", 502)
            if (result.get("schema_version") != schema_version(heatmap_model) or result.get("heatmap_model") != heatmap_model or result.get("run_id") != jid or
                result.get("timeframe") != tf or result.get("source_url") != source_url(heatmap_model) or
                result.get("selection_policy") != "prominent_right_edge.v1" or
                (result.get("hover_snapshot") or {}).get("version") != "displayed-hover.v1" or
                result.get("evidence", {}).get("sha256") != hashlib.sha256(image).hexdigest()):
                raise BridgeError("invalid_result", "Mismatched result provenance", 502)
            return result, image
        except asyncio.TimeoutError:
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
            try:
                from model1_evidence_format import evidence_payload
                retained_image, diagnostic = evidence_payload(tmp, tf, job_id=jid, heatmap_model=heatmap_model)
                diagnostic["failure_code"] = "scan_timeout"
                diagnostic["timeout_seconds"] = 360 if heatmap_model == 3 else 300
                evidence_store = JobStore(os.getenv("DATABASE_URL", ""))
                await asyncio.to_thread(evidence_store.retain_evidence, jid, retained_image, diagnostic)
                print("MODEL1_EVIDENCE " + json.dumps({"job_id": jid,
                    "image_retained": retained_image is not None, "diagnostic_retained": True,
                    "failure_code": "scan_timeout"}), flush=True)
            except Exception:
                print("MODEL1_EVIDENCE " + json.dumps({"job_id": jid,
                    "image_retained": False, "diagnostic_retained": False,
                    "failure_code": "scan_timeout"}), flush=True)
            raise BridgeError("scan_timeout", "Capture deadline exceeded", 504) from None
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()


async def worker(store: JobStore) -> None:
    import ai_market_vision
    while True:
        held = None
        jid = None
        try:
            async with ai_market_vision._SCAN_LOCK:
                claimed = await asyncio.to_thread(store.claim)
                if claimed:
                    held, row = claimed
                    jid = str(row["id"])
                    try:
                        result, image = await run_existing_scanner(row["timeframe"], jid, row.get("heatmap_model",1))
                        await asyncio.to_thread(store.finish, jid, result, image)
                    except BridgeError as exc:
                        await asyncio.to_thread(store.finish, jid, None, None, exc.code)
                    except asyncio.CancelledError:
                        await asyncio.to_thread(store.finish, jid, None, None, "interrupted")
                        raise
                    except Exception:
                        await asyncio.to_thread(store.finish, jid, None, None, "scan_failed")
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            if held is not None:
                await asyncio.to_thread(held.close)
        await asyncio.sleep(2)


def register_collection_routes(app: web.Application, *, store: Any = None,
                               token: str | None = None, enabled: bool | None = None,
                               start_worker: bool = True, heatmap_model: int = 1) -> None:
    """Attach to candidate HTTP server. Disabled by default; does not initialize providers."""
    heatmap_model = model_number(heatmap_model)
    active = (os.getenv("COLLECTION_BRIDGE_ENABLED", "").lower() == "true") if enabled is None else enabled
    bridge_token = os.getenv("COINGLASS_COLLECTOR_TOKEN", "") if token is None else token
    dsn = os.getenv("DATABASE_URL", "")
    db = store or (JobStore(dsn, heatmap_model=heatmap_model) if dsn else None)
    configured = active and len(bridge_token.encode()) >= 32 and db is not None

    def authorize(request: web.Request) -> None:
        if not configured:
            raise BridgeError("not_configured", "החיבור לרכיב האיסוף עדיין אינו פעיל.", 503)
        value = request.headers.get("Authorization", "")
        if len(value) > 1024 or not hmac.compare_digest(value.encode(), ("Bearer " + bridge_token).encode()):
            raise BridgeError("unauthorized", "Unauthorized", 401)

    async def dispatch(request: web.Request, action: str) -> web.Response:
        try:
            authorize(request)
            if action == "latest":
                tf = request.query.get("timeframe")
                if tf not in TIMEFRAMES:
                    raise BridgeError("unsupported_timeframe", "Unsupported timeframe")
                value = await asyncio.to_thread(db.latest, tf)
                if value is None:
                    raise BridgeError("no_cached_result", "No saved observation for this timeframe", 404)
                status = 200
            elif action == "start":
                if request.content_length is not None and request.content_length > 2048:
                    raise BridgeError("invalid_request", "Request too large", 413)
                raw = bytearray()
                while len(raw) <= 2048:
                    chunk = await request.content.read(2049 - len(raw))
                    if not chunk:
                        break
                    raw.extend(chunk)
                if len(raw) > 2048:
                    raise BridgeError("invalid_request", "Request too large", 413)
                try:
                    rid, tf = request_fields(json.loads(raw))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    raise BridgeError("invalid_request", "Malformed JSON") from None
                value = await asyncio.to_thread(db.start, rid, tf)
                status = 202 if value["status"] in {"queued", "running"} else 200
            else:
                try:
                    jid = str(UUID(request.match_info["job_id"]))
                except ValueError:
                    raise BridgeError("invalid_job_id", "Invalid job identifier") from None
                if action == "evidence":
                    image = await asyncio.to_thread(db.evidence, jid)
                    if image is None:
                        raise BridgeError("not_found", "Evidence unavailable or expired", 404)
                    return web.Response(body=image, content_type="image/png", headers={"Cache-Control": "no-store"})
                value = await asyncio.to_thread(db.get, jid)
                if value is None:
                    raise BridgeError("not_found", "Job not found", 404)
                status = 200
            return web.json_response(value, status=status, headers={"Cache-Control": "no-store"})
        except BridgeError as exc:
            body, headers = error_response(exc)
            return web.json_response(body, status=exc.status, headers=headers)
        except Exception as error:
            log_store_failure(action,error)
            return web.json_response({"error": {"code": "store_unavailable", "message": "Job store unavailable"}}, status=503)

    async def post(request):
        return await dispatch(request, "start")

    async def get(request):
        return await dispatch(request, "get")

    async def latest(request):
        return await dispatch(request, "latest")

    async def image(request):
        return await dispatch(request, "evidence")

    app.router.add_post(f"/api/collection/model{heatmap_model}/jobs", post)
    app.router.add_get(f"/api/collection/model{heatmap_model}/jobs/latest", latest)
    app.router.add_get(f"/api/collection/model{heatmap_model}/jobs/{{job_id}}", get)
    app.router.add_get(f"/api/collection/model{heatmap_model}/jobs/{{job_id}}/evidence", image)

    if configured and start_worker:
        async def context(_app):
            await asyncio.to_thread(db.initialize)
            task = asyncio.create_task(worker(db), name="model1-collection-jobs")
            yield
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        app.cleanup_ctx.append(context)


_register_one_model = register_collection_routes
def register_collection_routes(app, *, store=None, token=None, enabled=None, start_worker=True):
    _register_one_model(app,store=store,token=token,enabled=enabled,start_worker=start_worker,heatmap_model=1)
    for model in (2,3):
        scoped = JobStore(store.dsn,heatmap_model=model) if store is not None else None
        _register_one_model(app,store=scoped,token=token,enabled=enabled,start_worker=False,heatmap_model=model)
