"""App-only cache policy and read-only saved-observation endpoint.

Reading an existing result never creates a request, invokes a source/model or
refreshes its timestamp. Automatic reuse remains bounded to 15 minutes; older
observations (at most six hours) are available ONLY for explicit review/import.
No quotas, source permissions or worksheet authorization are changed here.
"""
from __future__ import annotations
from datetime import timedelta, timezone
import math
from pathlib import Path

AUTO_REUSE_MINUTES = 15
REVIEW_MAX_HOURS = 6


def latest_ready(store, timeframe):
    from collection_bridge import envelope
    with store.connect() as connection:
        row = connection.execute("""SELECT id,timeframe,status,result,error_code
            FROM public.ai_collection_bridge_jobs
            WHERE timeframe=%s AND status='ready' AND result IS NOT NULL
              AND (result->>'captured_at')::timestamptz >= now()-interval '6 hours'
              AND (result->>'captured_at')::timestamptz <= now()
            ORDER BY (result->>'captured_at')::timestamptz DESC,created_at DESC
            LIMIT 1""", (timeframe,)).fetchone()
    return envelope(row) if row else None


def capture_limit_error(connection, limit):
    """Use DB time and the actual rolling window, including reduced limits."""
    from collection_bridge import BridgeError
    error = BridgeError('rate_limited', 'Hourly capture limit reached', 429)
    row = connection.execute("""SELECT created_at + interval '1 hour' AS retry_at,
                    now() AS checked_at
                FROM public.ai_collection_bridge_jobs
                WHERE created_at > now()-interval '1 hour'
                ORDER BY created_at DESC OFFSET %s LIMIT 1""",
                (max(0, int(limit)-1),)).fetchone()
    if row:
        retry_at = row['retry_at'] + timedelta(seconds=1)
        seconds = max(1, math.ceil((retry_at-row['checked_at']).total_seconds()))
        error.retry_after_seconds = seconds
        error.retry_at = retry_at.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    return error


def error_response(error):
    payload = {'error': {'code': error.code, 'message': str(error)}}
    headers = {'Cache-Control': 'no-store'}
    seconds = getattr(error, 'retry_after_seconds', None)
    retry_at = getattr(error, 'retry_at', None)
    if error.status == 429 and type(seconds) is int and 0 < seconds <= 3601 and isinstance(retry_at, str):
        payload['error'].update(retry_after_seconds=seconds, retry_at=retry_at)
        headers['Retry-After'] = str(seconds)
    return payload, headers


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError('Cache adapter source layout changed; refusing partial update')
    return text.replace(old, new, 1)


def install(runtime: Path):
    path = runtime/'collection_bridge.py'
    text = path.read_text()
    text = replace_once(text, 'from aiohttp import web',
        'from aiohttp import web\nfrom model1_cache_policy import latest_ready, capture_limit_error, error_response')
    text = replace_once(text,
        "(result->>'captured_at')::timestamptz > now()-interval '10 minutes'",
        "(result->>'captured_at')::timestamptz >= now()-interval '15 minutes'\n"
        "                AND (result->>'captured_at')::timestamptz <= now()")
    text = replace_once(text,
        '                    raise BridgeError("rate_limited", "Hourly capture limit reached", 429)',
        '                    raise capture_limit_error(c, self.hourly_limit)')
    text = replace_once(text,
        '    def get(self, jid: str) -> dict[str, Any] | None:',
        '    def latest(self, tf: str) -> dict[str, Any] | None:\n'
        '        return latest_ready(self, tf)\n\n'
        '    def get(self, jid: str) -> dict[str, Any] | None:')
    text = replace_once(text,
        '            if action == "start":',
        '            if action == "latest":\n'
        '                tf = request.query.get("timeframe")\n'
        '                if tf not in TIMEFRAMES:\n'
        '                    raise BridgeError("unsupported_timeframe", "Unsupported timeframe")\n'
        '                value = await asyncio.to_thread(db.latest, tf)\n'
        '                if value is None:\n'
        '                    raise BridgeError("no_cached_result", "No saved observation for this timeframe", 404)\n'
        '                status = 200\n'
        '            elif action == "start":')
    text = replace_once(text,
        '            return web.json_response({"error": {"code": exc.code, "message": str(exc)}}, status=exc.status,\n'
        '                                     headers={"Cache-Control": "no-store"})',
        '            body, headers = error_response(exc)\n'
        '            return web.json_response(body, status=exc.status, headers=headers)')
    text = replace_once(text,
        '    async def get(request):\n        return await dispatch(request, "get")',
        '    async def get(request):\n        return await dispatch(request, "get")\n\n'
        '    async def latest(request):\n        return await dispatch(request, "latest")')
    text = replace_once(text,
        '    app.router.add_get("/api/collection/model1/jobs/{job_id}", get)',
        '    app.router.add_get("/api/collection/model1/jobs/latest", latest)\n'
        '    app.router.add_get("/api/collection/model1/jobs/{job_id}", get)')
    compile(text, str(path), 'exec')
    path.write_text(text, encoding='utf-8')
