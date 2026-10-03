"""Concrete least-privilege PostgreSQL dependencies for Stage-8 Shadow.

Importing this module is inert and imports only the Python standard library.
``build_dependencies()`` is the sole production factory: it lazy-loads psycopg
and the four read-side Stage-8 modules.  Connections are caller-configured,
read-only, repeatable-read, bounded, rolled back and closed; this module has no
writer, outcome evaluator, delivery, Telegram, LIVE, outbox or trading method.
"""
from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
import hashlib
import importlib
import math
import re
from typing import Any, Callable, Mapping, Sequence


VERSION = "stage8-shadow-postgres-read-dependencies-v1"
EXPECTED_ROLE = "research_stage8_reader_v1"
CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MILLISECONDS = 10_000.0
LOCK_TIMEOUT_MILLISECONDS = 1_000.0
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_TIMEOUT = re.compile(
    r"\s*([0-9]+(?:\.[0-9]+)?)\s*(us|ms|s|min|h|d)?\s*", re.I,
)
_REQUIRED_CONNECTION_FIELDS = frozenset({
    "host", "port", "dbname", "user", "password", "sslmode",
})
_ALLOWED_CONNECTION_FIELDS = _REQUIRED_CONNECTION_FIELDS | frozenset({
    "hostaddr", "sslrootcert", "channel_binding", "target_session_attrs",
})
_FORBIDDEN_CONNECTION_FIELDS = frozenset({
    "service", "servicefile", "passfile", "options",
})
_VERIFY_SQL = """/* stage8-shadow:verify-read-transaction */ SELECT
    current_setting('transaction_read_only') AS read_only,
    current_setting('transaction_isolation') AS isolation,
    current_setting('statement_timeout') AS statement_timeout,
    current_setting('lock_timeout') AS lock_timeout,
    current_user AS database_role,
    pg_backend_pid() AS backend_pid,
    transaction_timestamp() AS transaction_started_at_utc,
    pg_current_snapshot()::text AS database_snapshot_id"""


class ShadowPostgresError(RuntimeError):
    """Sanitized failure at the concrete read-only database boundary."""


@dataclass(frozen=True)
class _Runtime:
    psycopg: Any
    conninfo_to_dict: Callable[[str], Mapping[str, Any]]
    dict_row: Any
    registry: Any
    coverage: Any
    projection_adapter: Any
    source_audit: Any


def _load_runtime() -> _Runtime:
    """Load external/runtime modules only after the explicit factory is called."""
    psycopg = importlib.import_module("psycopg")
    conninfo = importlib.import_module("psycopg.conninfo")
    rows = importlib.import_module("psycopg.rows")
    registry = importlib.import_module("research_stage8_registry")
    coverage = importlib.import_module("research_stage8_coverage_receipt")
    projection_adapter = importlib.import_module(
        "research_stage8_projection_db_adapter"
    )
    source_audit = importlib.import_module(
        "research_operational_score_source_audit"
    )
    if (getattr(registry, "READER_ROLE", None) != EXPECTED_ROLE
            or not callable(getattr(
                registry, "registry_reference_from_connection", None
            ))
            or not callable(getattr(
                coverage, "read_bounded_attempt_cohort_from_connection", None
            ))
            or not callable(getattr(
                projection_adapter,
                "project_exact_binding_attempts_from_connection", None,
            ))
            or not callable(getattr(
                source_audit, "transaction_identity_from_fields", None
            ))):
        raise ShadowPostgresError("STAGE8_SHADOW_READ_RUNTIME_CONTRACT_MISMATCH")
    return _Runtime(
        psycopg=psycopg,
        conninfo_to_dict=conninfo.conninfo_to_dict,
        dict_row=rows.dict_row,
        registry=registry,
        coverage=coverage,
        projection_adapter=projection_adapter,
        source_audit=source_audit,
    )


def _explicit_connection_fields(
    raw: str, parser: Callable[[str], Mapping[str, Any]],
) -> dict[str, str]:
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_URL_NOT_EXACT")
    try:
        parsed = parser(raw)
    except Exception as exc:
        raise ShadowPostgresError(
            "STAGE8_SHADOW_DATABASE_TARGET_NOT_FULLY_EXPLICIT"
        ) from exc
    if (not isinstance(parsed, Mapping)
            or _FORBIDDEN_CONNECTION_FIELDS.intersection(parsed)
            or set(parsed).difference(_ALLOWED_CONNECTION_FIELDS)
            or any(not isinstance(parsed.get(key), str) or not parsed[key]
                   for key in _REQUIRED_CONNECTION_FIELDS)
            or any(not isinstance(value, str) or not value
                   for value in parsed.values())):
        raise ShadowPostgresError(
            "STAGE8_SHADOW_DATABASE_TARGET_NOT_FULLY_EXPLICIT"
        )
    host, port = parsed["host"], parsed["port"]
    if ("," in host or "," in port or not port.isdecimal()
            or not 1 <= int(port) <= 65535):
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_TARGET_NOT_EXACT")
    if "," in parsed.get("hostaddr", ""):
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_TARGET_NOT_EXACT")
    sslmode = parsed["sslmode"]
    if sslmode not in {"disable", "require", "verify-ca", "verify-full"}:
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_SSLMODE_NOT_EXPLICIT")
    if sslmode in {"verify-ca", "verify-full"} and not parsed.get("sslrootcert"):
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_CA_NOT_EXPLICIT")
    if parsed["user"] != EXPECTED_ROLE:
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_DSN_ROLE_MISMATCH")
    result = {
        key: str(parsed[key]) for key in sorted(_ALLOWED_CONNECTION_FIELDS)
        if key in parsed
    }
    result.setdefault("channel_binding", "prefer")
    result.setdefault("target_session_attrs", "any")
    return result


def _timeout_ms(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("invalid timeout")
    if isinstance(value, (int, float)):
        amount, unit = float(value), "ms"
    else:
        match = _TIMEOUT.fullmatch(str(value))
        if match is None:
            raise ValueError("invalid timeout")
        amount = float(match.group(1))
        unit = (match.group(2) or "ms").lower()
    multiplier = {
        "us": 0.001,
        "ms": 1.0,
        "s": 1_000.0,
        "min": 60_000.0,
        "h": 3_600_000.0,
        "d": 86_400_000.0,
    }[unit]
    result = amount * multiplier
    if not math.isfinite(result):
        raise ValueError("invalid timeout")
    return result


def _one_mapping(cursor: Any) -> dict[str, Any]:
    row = cursor.fetchone()
    if not isinstance(row, Mapping) or cursor.fetchone() is not None:
        raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_ATTESTATION_INVALID")
    return dict(row)


class _ReadOnlySession(AbstractContextManager[Any]):
    def __init__(
        self, owner: "PostgresReadOnlyShadowDependencies", *,
        parameters: Mapping[str, str], target_sha256: str, expected_role: str,
    ) -> None:
        self._owner = owner
        self._parameters = dict(parameters)
        self._target_sha256 = target_sha256
        self._expected_role = expected_role

    def __enter__(self) -> Any:
        return self._owner._enter(
            parameters=self._parameters, target_sha256=self._target_sha256,
            expected_role=self._expected_role,
        )

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self._owner._exit(had_body_error=exc_type is not None)
        return False


class PostgresReadOnlyShadowDependencies:
    """Five-method read-only implementation consumed by the Shadow protocol."""

    def __init__(self, runtime: _Runtime) -> None:
        self._runtime = runtime
        self._active: Any | None = None
        self._target_sha256: str | None = None
        self._expected_role: str | None = None
        self._verified = False

    def open_read_only_session(
        self, *, database_url: str, expected_role: str,
        database_target_sha256: str,
    ) -> AbstractContextManager[Any]:
        if expected_role != EXPECTED_ROLE:
            raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_ROLE_MISMATCH")
        if (not isinstance(database_url, str)
                or not isinstance(database_target_sha256, str)
                or _HASH.fullmatch(database_target_sha256) is None
                or hashlib.sha256(database_url.encode("utf-8")).hexdigest()
                != database_target_sha256):
            raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_TARGET_HASH_MISMATCH")
        parameters = _explicit_connection_fields(
            database_url, self._runtime.conninfo_to_dict,
        )
        return _ReadOnlySession(
            self, parameters=parameters,
            target_sha256=database_target_sha256, expected_role=expected_role,
        )

    def _enter(
        self, *, parameters: Mapping[str, str], target_sha256: str,
        expected_role: str,
    ) -> Any:
        if self._active is not None:
            raise ShadowPostgresError("STAGE8_SHADOW_SESSION_ALREADY_ACTIVE")
        connection = None
        try:
            connection = self._runtime.psycopg.connect(
                **dict(parameters), row_factory=self._runtime.dict_row,
                connect_timeout=CONNECT_TIMEOUT_SECONDS,
                options=(
                    "-c default_transaction_read_only=on "
                    "-c statement_timeout=10000 -c lock_timeout=1000"
                ),
                prepare_threshold=None,
            )
            connection.autocommit = False
            connection.isolation_level = (
                self._runtime.psycopg.IsolationLevel.REPEATABLE_READ
            )
            connection.read_only = True
        except Exception as exc:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            raise ShadowPostgresError(
                "STAGE8_SHADOW_DATABASE_CONNECTION_FAILED"
            ) from exc
        self._active = connection
        self._target_sha256 = target_sha256
        self._expected_role = expected_role
        self._verified = False
        return connection

    def _exit(self, *, had_body_error: bool) -> None:
        connection = self._active
        self._active = None
        self._target_sha256 = None
        self._expected_role = None
        self._verified = False
        if connection is None:
            return
        cleanup_failed = False
        try:
            connection.rollback()
        except Exception:
            cleanup_failed = True
        try:
            connection.close()
        except Exception:
            cleanup_failed = True
        if cleanup_failed and not had_body_error:
            raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_CLEANUP_FAILED")

    def _require_active(self, session: Any, *, verified: bool = True) -> Any:
        if session is not self._active or (verified and not self._verified):
            raise ShadowPostgresError("STAGE8_SHADOW_DATABASE_SESSION_NOT_VERIFIED")
        return session

    def verify_read_only_session(self, session: Any) -> Mapping[str, Any]:
        connection = self._require_active(session, verified=False)
        try:
            row = _one_mapping(connection.execute(_VERIFY_SQL))
            statement_timeout = _timeout_ms(row.get("statement_timeout"))
            lock_timeout = _timeout_ms(row.get("lock_timeout"))
            identity = self._runtime.source_audit.transaction_identity_from_fields(
                backend_pid=row.get("backend_pid"),
                transaction_started_at_utc=row.get("transaction_started_at_utc"),
                database_snapshot_id=row.get("database_snapshot_id"),
            )
        except Exception as exc:
            raise ShadowPostgresError(
                "STAGE8_SHADOW_DATABASE_ATTESTATION_INVALID"
            ) from exc
        if (row.get("read_only") not in (True, "on")
                or str(row.get("isolation") or "").lower().replace("_", " ")
                != "repeatable read"
                or row.get("database_role") != self._expected_role
                or getattr(connection, "autocommit", None) is not False
                or getattr(connection, "read_only", None) is not True
                or not 0 < statement_timeout <= STATEMENT_TIMEOUT_MILLISECONDS
                or not 0 < lock_timeout <= LOCK_TIMEOUT_MILLISECONDS):
            raise ShadowPostgresError(
                "STAGE8_SHADOW_DATABASE_ATTESTATION_INVALID"
            )
        self._verified = True
        return {
            "status": "VERIFIED_READ_ONLY_REPEATABLE_READ",
            "read_only": True,
            "transaction_isolation": "REPEATABLE READ",
            "database_role": row["database_role"],
            "database_target_sha256": self._target_sha256,
            "transaction_identity_sha256":
                identity["transaction_identity_sha256"],
            "backend_pid": identity["backend_pid"],
            "transaction_started_at_utc":
                identity["transaction_started_at_utc"],
            "database_snapshot_id": identity["database_snapshot_id"],
        }

    def registry_reference_from_connection(
        self, session: Any, exact_binding: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return self._runtime.registry.registry_reference_from_connection(
            self._require_active(session), exact_binding,
        )

    def read_bounded_attempt_cohort_from_connection(
        self, session: Any, *, start_utc: str, end_utc: str,
        symbols: Sequence[str], page_size: int, max_pages: int,
    ) -> Mapping[str, Any]:
        return self._runtime.coverage.read_bounded_attempt_cohort_from_connection(
            self._require_active(session), start_utc=start_utc, end_utc=end_utc,
            symbols=symbols, page_size=page_size, max_pages=max_pages,
        )

    def project_exact_binding_attempts_from_connection(
        self, session: Any, *, exact_binding: Mapping[str, Any],
        attempt_ids: Sequence[int],
    ) -> Mapping[str, Any]:
        return (
            self._runtime.projection_adapter
            .project_exact_binding_attempts_from_connection(
                self._require_active(session), exact_binding=exact_binding,
                attempt_ids=attempt_ids,
            )
        )


def build_dependencies() -> PostgresReadOnlyShadowDependencies:
    """Explicit opt-in factory; performs no connection or environment lookup."""
    return PostgresReadOnlyShadowDependencies(_load_runtime())


__all__ = [
    "VERSION", "EXPECTED_ROLE", "CONNECT_TIMEOUT_SECONDS",
    "STATEMENT_TIMEOUT_MILLISECONDS", "LOCK_TIMEOUT_MILLISECONDS",
    "ShadowPostgresError", "PostgresReadOnlyShadowDependencies",
    "build_dependencies",
]
