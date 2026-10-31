"""Offline work identity foundation; not wired into the collector runtime.

No source, database, API, environment or scheduler access. These identifiers
support future database uniqueness; they do not implement ownership or retries.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import re
from uuid import UUID, NAMESPACE_URL, uuid5


SCHEMA = "coinglass-work.v1"
_NAMESPACE = uuid5(NAMESPACE_URL, SCHEMA)
_SYMBOL = re.compile(r"[A-Z0-9]{1,20}\Z", re.ASCII)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_FIELDS = frozenset({"schema", "work_id", "tenant_id", "source_scope_id",
                     "batch_id", "coin", "heatmap_model", "timeframe",
                     "config_digest"})


def _uuid(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Identity must be a UUID string")
    parsed = UUID(value)
    if parsed.int == 0:
        raise ValueError("Nil identity is not allowed")
    return str(parsed)


def _coin(value: str) -> str:
    if not isinstance(value, str) or not value.isascii():
        raise ValueError("Coin must be an ASCII symbol")
    normalized = value.upper()
    if not _SYMBOL.fullmatch(normalized):
        raise ValueError("Invalid coin symbol")
    return normalized


def _digest(value: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError("Expected a lowercase SHA-256 digest")
    return value


def _canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


@dataclass(frozen=True, slots=True)
class WorkSpec:
    """One immutable command within an authorized source scope and batch.

    config_digest binds the reviewed capture/analysis configuration snapshot.
    It intentionally is not part of work_id: changed configuration in the same
    logical job is a conflict, not permission to run another paid analysis.
    Source scope is an opaque, server-authorized identifier, never a secret.
    """

    tenant_id: str
    source_scope_id: str
    batch_id: str
    coin: str
    heatmap_model: int
    timeframe: str
    config_digest: str

    def __post_init__(self) -> None:
        for name in ("tenant_id", "source_scope_id", "batch_id"):
            object.__setattr__(self, name, _uuid(getattr(self, name)))
        object.__setattr__(self, "coin", _coin(self.coin))
        if type(self.heatmap_model) is not int or self.heatmap_model not in (1, 2, 3):
            raise ValueError("Unsupported heatmap model")
        if type(self.timeframe) is not str or self.timeframe not in ("12H", "24H", "48H"):
            raise ValueError("Unsupported timeframe")
        _digest(self.config_digest)

    def _identity(self) -> dict:
        return {"schema": SCHEMA, "tenant_id": self.tenant_id,
                "source_scope_id": self.source_scope_id, "batch_id": self.batch_id,
                "coin": self.coin, "heatmap_model": self.heatmap_model,
                "timeframe": self.timeframe}

    @property
    def work_id(self) -> str:
        return str(uuid5(_NAMESPACE, _canonical(self._identity())))

    def to_record(self) -> dict:
        return {**self._identity(), "work_id": self.work_id,
                "config_digest": self.config_digest}

    @classmethod
    def from_record(cls, record: dict) -> WorkSpec:
        if type(record) is not dict or set(record) != _FIELDS:
            raise ValueError("Unexpected work record fields")
        if record["schema"] != SCHEMA:
            raise ValueError("Unsupported work schema")
        spec = cls(**{k: v for k, v in record.items()
                      if k not in {"schema", "work_id"}})
        if record["work_id"] != spec.work_id:
            raise ValueError("Work identifier does not match its command")
        return spec

    def require_same_command(self, record: dict) -> None:
        """Reject another work item or a conflicting configuration snapshot."""
        if self != self.from_record(record):
            raise ValueError("Work command conflict")

    def analysis_key(self, image_sha256: str, evidence_sha256: str) -> str:
        """Bind analysis to one command and exact submitted evidence.

        This is not proof of source identity, authorization, or permission to
        retry. A durable phase/ownership guard must still authorize the call.
        """
        return sha256(_canonical({
            "schema": "coinglass-analysis-binding.v1", "work": self.to_record(),
            "image_sha256": _digest(image_sha256),
            "evidence_sha256": _digest(evidence_sha256),
        }).encode("ascii")).hexdigest()


def build_batch(*, tenant_id: str, source_scope_id: str, batch_id: str,
                coins: tuple[str, ...], config_digest: str) -> tuple[WorkSpec, ...]:
    """Expand an explicit 1–8 coin configuration into nine commands per coin.

    No coin defaults, schedule creation, external availability claims or writes.
    Caller must supply the same persisted batch_id on retry and authorize the
    coin list/source scope. Coin order does not affect work identity.
    """
    if type(coins) is not tuple or not 1 <= len(coins) <= 8:
        raise ValueError("Provide an explicit tuple of one to eight coins")
    normalized = tuple(_coin(coin) for coin in coins)
    if len(set(normalized)) != len(normalized):
        raise ValueError("Duplicate coin in batch configuration")
    return tuple(WorkSpec(tenant_id, source_scope_id, batch_id, coin, model,
                          timeframe, config_digest)
                 for coin in normalized for model in (1, 2, 3)
                 for timeframe in ("12H", "24H", "48H"))
