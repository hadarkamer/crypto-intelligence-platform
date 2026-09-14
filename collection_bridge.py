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

SCHEMA = "coinglass-model1.v1"
SOURCE = "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol"
TIMEFRAMES = ("12H", "24H")  # Legacy helper does NOT correctly select 48H.
CREATE_LOCK = 748230101
WORK_LOCK = 748230102
MAX_IMAGE = 4 * 1024 * 1024
MAX_RESULT = 64 * 1024


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
        raise BridgeError("unsupported_timeframe", "Only 12H and 24H are supported")
    return rid, tf


def envelope(row: dict[str, Any]) -> dict[str, Any]:
    err = row.get("error_code")
    return {"schema_version": SCHEMA, "job_id": str(row["id"]),
            "timeframe": row["timeframe"], "status": row["status"],
            "result": row.get("result") if row["status"] == "ready" else None,
            "error": {"code": err, "message": "האיסוף לא הושלם; לא הופעל ניסיון חוזר אוטומטי."} if err else None}


class JobStore:
    """Small isolated PostgreSQL job ledger, using the existing database service."""
    def __init__(self, dsn: str, hourly_limit: int = 4):
        self.dsn = dsn
        self.hourly_limit = max(1, min(int(hourly_limit), 20))

    def connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5,
                               options="-c statement_timeout=8000 -c lock_timeout=3000")

    def initialize(self) -> None:
        with self.connect() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_jobs (
                id uuid PRIMARY KEY, timeframe text NOT NULL CHECK(timeframe IN ('12H','24H')),
                status text NOT NULL CHECK(status IN ('queued','running','ready','failed')),
                created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz,
                finished_at timestamptz, result jsonb, image_png bytea, error_code text)""")
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ai_collection_bridge_one_active_tf
                ON public.ai_collection_bridge_jobs(timeframe) WHERE status IN ('queued','running')""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.ai_collection_bridge_requests (
                id uuid PRIMARY KEY, timeframe text NOT NULL, job_id uuid NOT NULL
                REFERENCES public.ai_collection_bridge_jobs(id) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now())""")
            c.execute("REVOKE ALL ON public.ai_collection_bridge_jobs, public.ai_collection_bridge_requests FROM PUBLIC")

    def start(self, rid: str, tf: str) -> dict[str, Any]:
        with self.connect() as c:
            c.execute("SELECT pg_advisory_xact_lock(%s)", (CREATE_LOCK,))
            prior = c.execute("""SELECT r.timeframe AS requested_timeframe, j.*
                FROM public.ai_collection_bridge_requests r JOIN public.ai_collection_bridge_jobs j
                ON j.id=r.job_id WHERE r.id=%s""", (rid,)).fetchone()
            if prior:
                if prior["requested_timeframe"] != tf:
                    raise BridgeError("idempotency_conflict", "Request id belongs to another timeframe", 409)
                return envelope(prior)
            c.execute("""UPDATE public.ai_collection_bridge_jobs SET status='failed',
                error_code='interrupted', finished_at=now() WHERE status='running'
                AND started_at < now()-interval '15 minutes'""")
            c.execute("UPDATE public.ai_collection_bridge_jobs SET image_png=NULL WHERE created_at < now()-interval '6 hours' AND image_png IS NOT NULL")
            c.execute("DELETE FROM public.ai_collection_bridge_jobs WHERE created_at < now()-interval '7 days' AND status IN ('ready','failed')")
            requests = c.execute("SELECT count(*) AS n FROM public.ai_collection_bridge_requests WHERE created_at > now()-interval '1 hour'").fetchone()["n"]
            if requests >= 200:
                raise BridgeError("rate_limited", "Too many requests", 429)
            row = c.execute("""SELECT * FROM public.ai_collection_bridge_jobs WHERE timeframe=%s
                AND (status IN ('queued','running') OR (status='ready'
                AND (result->>'captured_at')::timestamptz > now()-interval '10 minutes'))
                ORDER BY created_at DESC LIMIT 1""", (tf,)).fetchone()
            if row is None:
                n = c.execute("SELECT count(*) AS n FROM public.ai_collection_bridge_jobs WHERE created_at > now()-interval '1 hour'").fetchone()["n"]
                if n >= self.hourly_limit:
                    raise BridgeError("rate_limited", "Hourly capture limit reached", 429)
                row = c.execute("""INSERT INTO public.ai_collection_bridge_jobs(id,timeframe,status)
                    VALUES(%s,%s,'queued') RETURNING *""", (str(uuid4()), tf)).fetchone()
            c.execute("INSERT INTO public.ai_collection_bridge_requests(id,timeframe,job_id) VALUES(%s,%s,%s)", (rid, tf, row["id"]))
            return envelope(row)

    def get(self, jid: str) -> dict[str, Any] | None:
        with self.connect() as c:
            row = c.execute("SELECT id,timeframe,status,result,error_code FROM public.ai_collection_bridge_jobs WHERE id=%s", (jid,)).fetchone()
            return envelope(row) if row else None

    def evidence(self, jid: str) -> bytes | None:
        with self.connect() as c:
            row = c.execute("""SELECT image_png FROM public.ai_collection_bridge_jobs
                WHERE id=%s AND status='ready' AND created_at > now()-interval '6 hours'""", (jid,)).fetchone()
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


async def run_existing_scanner(tf: str, jid: str) -> tuple[dict[str, Any], bytes]:
    """One child process, one existing capture, one existing OpenAI analysis. No retries."""
    with tempfile.TemporaryDirectory(prefix="collection-model1-") as tmp:
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(__file__).with_name("collection_model1_task.py")), tf, jid, tmp,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(process.wait(), timeout=300)
            if process.returncode != 0:
                raise BridgeError("source_or_analysis_failed", "Source capture or analysis was not usable", 502)
            path = Path(tmp) / "result.json"
            shot = Path(tmp) / "image.png"
            if not path.is_file() or path.stat().st_size > MAX_RESULT or not shot.is_file() or shot.stat().st_size > MAX_IMAGE:
                raise BridgeError("invalid_result", "Result size or evidence invalid", 502)
            result = json.loads(path.read_text(encoding="utf-8"))
            image = shot.read_bytes()
            if not image.startswith(b"\x89PNG\r\n\x1a\n"):
                raise BridgeError("invalid_evidence", "Image evidence invalid", 502)
            if (result.get("schema_version") != SCHEMA or result.get("run_id") != jid or
                result.get("timeframe") != tf or result.get("source_url") != SOURCE or
                result.get("evidence", {}).get("sha256") != hashlib.sha256(image).hexdigest()):
                raise BridgeError("invalid_result", "Mismatched result provenance", 502)
            return result, image
        except asyncio.TimeoutError:
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
                        result, image = await run_existing_scanner(row["timeframe"], jid)
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
                               start_worker: bool = True) -> None:
    """Attach to candidate HTTP server. Disabled by default; does not initialize providers."""
    active = (os.getenv("COLLECTION_BRIDGE_ENABLED", "").lower() == "true") if enabled is None else enabled
    bridge_token = os.getenv("COINGLASS_COLLECTOR_TOKEN", "") if token is None else token
    dsn = os.getenv("DATABASE_URL", "")
    db = store or (JobStore(dsn, int(os.getenv("COLLECTION_BRIDGE_HOURLY_LIMIT", "4"))) if dsn else None)
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
            if action == "start":
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
            return web.json_response({"error": {"code": exc.code, "message": str(exc)}}, status=exc.status,
                                     headers={"Cache-Control": "no-store"})
        except Exception:
            return web.json_response({"error": {"code": "store_unavailable", "message": "Job store unavailable"}}, status=503)

    async def post(request):
        return await dispatch(request, "start")

    async def get(request):
        return await dispatch(request, "get")

    async def image(request):
        return await dispatch(request, "evidence")

    app.router.add_post("/api/collection/model1/jobs", post)
    app.router.add_get("/api/collection/model1/jobs/{job_id}", get)
    app.router.add_get("/api/collection/model1/jobs/{job_id}/evidence", image)

    if configured and start_worker:
        async def context(_app):
            await asyncio.to_thread(db.initialize)
            task = asyncio.create_task(worker(db), name="model1-collection-jobs")
            yield
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        app.cleanup_ctx.append(context)
