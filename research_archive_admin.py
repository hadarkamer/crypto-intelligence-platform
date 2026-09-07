"""One-shot authenticated delivery of a reviewed archive into isolated tables.

The route is disabled unless an explicit environment flag and secret are set.
Uploaded chunks, the ZIP and the extracted SQLite live only in the runtime's
ephemeral directory.  Both container and SQLite SHA256 values are verified
before the existing strict archive importer is invoked.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import threading
from typing import Any
import zipfile

from aiohttp import web

import research_telegram_archive_runtime_importer as importer


_TRUE = {"1", "true", "yes", "on"}
ENABLED = os.getenv("RESEARCH_ARCHIVE_IMPORT_ENABLED", "").strip().lower() in _TRUE
SECRET = os.getenv("RESEARCH_ARCHIVE_IMPORT_SECRET", "").strip()
ROOT = Path(os.getenv("RESEARCH_ARCHIVE_IMPORT_TMP", "/tmp/research-archive-import-v1"))
MAX_CHUNK_BYTES = 8 * 1024 * 1024
MAX_CHUNKS = 256
SQLITE_MEMBER = "archive_reconstructed_research.sqlite"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_state: dict[str, Any] = {"status": "DISABLED" if not ENABLED else "READY", "report": None, "error": None}
_state_lock = threading.Lock()


def status() -> dict[str, Any]:
    with _state_lock:
        return {**_state, "enabled": ENABLED, "configured": bool(SECRET),
                "max_chunk_bytes": MAX_CHUNK_BYTES, "source_scope": "ARCHIVE_ONLY"}


def _authorized(request: web.Request) -> bool:
    supplied = request.headers.get("Authorization", "")
    expected = "Bearer " + SECRET
    return bool(ENABLED and SECRET and secrets.compare_digest(supplied, expected))


def _require_auth(request: web.Request) -> None:
    if not _authorized(request):
        raise web.HTTPNotFound()


def _sha256(path: Path) -> str:
    hashed = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hashed.update(block)
    return hashed.hexdigest()


def _session(request: web.Request) -> str:
    value = request.match_info.get("session", "").lower()
    if not _HEX64.fullmatch(value):
        raise web.HTTPBadRequest(text="invalid session")
    return value


async def put_chunk(request: web.Request) -> web.Response:
    _require_auth(request)
    session = _session(request)
    try:
        index = int(request.match_info["index"])
        expected = request.headers["X-Chunk-SHA256"].lower()
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(text="invalid chunk identity")
    if not 0 <= index < MAX_CHUNKS or not _HEX64.fullmatch(expected):
        raise web.HTTPBadRequest(text="invalid chunk identity")
    body = await request.read()
    if not body or len(body) > MAX_CHUNK_BYTES:
        raise web.HTTPRequestEntityTooLarge(max_size=MAX_CHUNK_BYTES, actual_size=len(body))
    actual = hashlib.sha256(body).hexdigest()
    if not secrets.compare_digest(actual, expected):
        raise web.HTTPBadRequest(text="chunk sha256 mismatch")
    directory = ROOT / session / "chunks"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{index:04d}.part"
    if target.exists():
        if _sha256(target) != expected or target.stat().st_size != len(body):
            raise web.HTTPConflict(text="existing chunk differs")
    else:
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(body)
        temporary.replace(target)
    return web.json_response({"ok": True, "index": index, "bytes": len(body), "sha256": actual})


def _join_and_extract(session: str, payload: dict[str, Any]) -> tuple[Path, str, str]:
    zip_sha = str(payload.get("zip_sha256") or "").lower()
    sqlite_sha = str(payload.get("sqlite_sha256") or "").lower()
    run_key = str(payload.get("run_key") or "").lower()
    chunks = payload.get("chunk_count")
    if not all(_HEX64.fullmatch(value) for value in (session, zip_sha, sqlite_sha, run_key)):
        raise ValueError("invalid finalize digest")
    if session != zip_sha or type(chunks) is not int or not 1 <= chunks <= MAX_CHUNKS:
        raise ValueError("invalid finalize contract")
    directory = ROOT / session
    paths = [directory / "chunks" / f"{index:04d}.part" for index in range(chunks)]
    if any(not path.is_file() for path in paths):
        raise ValueError("missing upload chunk")
    archive = directory / "reviewed-archive.zip"
    temporary = archive.with_suffix(".tmp")
    with temporary.open("wb") as output:
        for path in paths:
            with path.open("rb") as source:
                shutil.copyfileobj(source, output, length=1024 * 1024)
    temporary.replace(archive)
    if _sha256(archive) != zip_sha:
        raise ValueError("ZIP sha256 mismatch")
    sqlite_path = directory / SQLITE_MEMBER
    sqlite_tmp = sqlite_path.with_suffix(".tmp")
    with zipfile.ZipFile(archive) as bundle:
        names = bundle.namelist()
        if SQLITE_MEMBER not in names or names.count(SQLITE_MEMBER) != 1:
            raise ValueError("reviewed SQLite member missing or duplicated")
        info = bundle.getinfo(SQLITE_MEMBER)
        if info.is_dir() or info.file_size > 2_000_000_000:
            raise ValueError("invalid SQLite member")
        with bundle.open(info) as source, sqlite_tmp.open("wb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)
    sqlite_tmp.replace(sqlite_path)
    if _sha256(sqlite_path) != sqlite_sha:
        raise ValueError("SQLite sha256 mismatch")
    return sqlite_path, sqlite_sha, run_key


def _import(session: str, payload: dict[str, Any]) -> None:
    try:
        sqlite_path, sqlite_sha, run_key = _join_and_extract(session, payload)
        from research_telegram_export_import import _database_url
        import psycopg
        from psycopg.rows import dict_row
        url = _database_url()
        if not url:
            raise RuntimeError("research database is not configured")
        with psycopg.connect(url, row_factory=dict_row, connect_timeout=5,
                             options="-c statement_timeout=30000 -c lock_timeout=1000") as conn:
            report = importer.import_artifact(conn, sqlite_path, expected_sha256=sqlite_sha,
                                              expected_run_key=run_key, batch_size=50)
        with _state_lock:
            _state.update(status="COMPLETE", report=report, error=None)
    except Exception as exc:
        with _state_lock:
            _state.update(status="FAILED", error=f"{type(exc).__name__}: {exc}")
    finally:
        if status()["status"] == "COMPLETE":
            with suppress(Exception):
                shutil.rmtree(ROOT / session)


async def finalize(request: web.Request) -> web.Response:
    _require_auth(request)
    session = _session(request)
    try:
        payload = await request.json()
    except (json.JSONDecodeError, TypeError):
        raise web.HTTPBadRequest(text="invalid JSON")
    with _state_lock:
        if _state["status"] == "IMPORTING":
            raise web.HTTPConflict(text="an import is already running")
        _state.update(status="IMPORTING", report=None, error=None, session=session)
    asyncio.create_task(asyncio.to_thread(_import, session, payload), name="reviewed-archive-import")
    return web.json_response({"ok": True, "status": "IMPORTING", "session": session}, status=202)


async def get_status(request: web.Request) -> web.Response:
    _require_auth(request)
    return web.json_response(status())


def register_routes(app: web.Application) -> None:
    if not ENABLED or not SECRET:
        return
    app.router.add_put("/internal/research/archive/{session}/chunks/{index}", put_chunk)
    app.router.add_post("/internal/research/archive/{session}/finalize", finalize)
    app.router.add_get("/internal/research/archive/status", get_status)
