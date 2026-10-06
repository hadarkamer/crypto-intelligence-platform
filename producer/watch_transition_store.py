"""Durable MP65 hysteresis and conservative, at-most-one-attempt delivery.

No market reads or Telegram calls belong here. A transaction freezes transitions,
its replay receipt and outbox intents together. An ambiguous delivery is never
retried automatically. Callers handle persistence errors without stopping Watch.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from uuid import uuid4

POLICY_VERSION = "maxpain-score65-reset60-bootstrap-v1"
DELIVERY_POLICY_VERSION = "pending-10m-one-attempt-orphan-2m-v1"
DELIVERY_TTL = timedelta(minutes=10)
ORPHAN_TIMEOUT = timedelta(minutes=2)
BATCH_RETENTION = timedelta(hours=48)
TERMINAL_INTENT_RETENTION = timedelta(days=30)
PRUNE_LIMIT = 128
CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MS = 8000
LOCK_TIMEOUT_MS = 3000
MAX_ITEMS = 2048
MAX_STATE_KEYS = 8192
MAX_INTENTS = 2048
MAX_PAYLOAD_BYTES = 256 * 1024
MAX_CLAIM_LIMIT = 128
KINDS = {"MAX_PAIN_SCORE_65", "FORMULA_MP65_CVD_SHORT"}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watch_transition_scopes (
 subscription_scope text NOT NULL, policy_version text NOT NULL,
 last_observed_at timestamptz, state jsonb NOT NULL DEFAULT '{}'::jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(subscription_scope, policy_version)
);
CREATE TABLE IF NOT EXISTS watch_transition_batches (
 subscription_scope text NOT NULL, policy_version text NOT NULL,
 watch_scan_id text NOT NULL, observed_at timestamptz NOT NULL,
 input_sha256 text NOT NULL, result_state jsonb NOT NULL,
 result_metadata jsonb NOT NULL,
 crossing_keys jsonb NOT NULL, rejected_older boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(subscription_scope, policy_version, watch_scan_id)
);
CREATE INDEX IF NOT EXISTS watch_transition_batches_created
 ON watch_transition_batches(subscription_scope, created_at);
CREATE TABLE IF NOT EXISTS watch_transition_intents (
 intent_id text PRIMARY KEY, subscription_scope text NOT NULL,
 policy_version text NOT NULL, delivery_policy_version text NOT NULL,
 watch_scan_id text NOT NULL, signal_key text NOT NULL, episode bigint NOT NULL,
 kind text NOT NULL CHECK(kind IN ('MAX_PAIN_SCORE_65','FORMULA_MP65_CVD_SHORT')),
 ordinal integer NOT NULL, payload jsonb NOT NULL,
 observed_at timestamptz NOT NULL, expires_at timestamptz NOT NULL,
 status text NOT NULL DEFAULT 'PENDING'
   CHECK(status IN ('PENDING','IN_FLIGHT','DELIVERED','UNKNOWN','FAILED','EXPIRED')),
 attempt_token text, attempted_at timestamptz, acknowledged_at timestamptz,
 error_type text, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(subscription_scope, policy_version, kind, signal_key, episode)
);
CREATE INDEX IF NOT EXISTS watch_transition_intents_pending
 ON watch_transition_intents(subscription_scope, status, created_at, ordinal);
CREATE INDEX IF NOT EXISTS watch_transition_intents_batch
 ON watch_transition_intents(subscription_scope, policy_version, watch_scan_id, ordinal);
CREATE INDEX IF NOT EXISTS watch_transition_intents_terminal_created
 ON watch_transition_intents(subscription_scope, created_at)
 WHERE status IN ('DELIVERED','UNKNOWN','FAILED','EXPIRED');
"""


def _utc(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value):
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _freeze(value):
    """JSON-safe immutable-by-copy evidence, including unknown numeric inputs."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, (float, Decimal)):
        if not math.isfinite(value):
            return None
        return int(value) if value == int(value) else float(value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON evidence keys must be strings")
        return {key: _freeze(member) for key, member in value.items()}
    if isinstance(value, (list, tuple)):
        return [_freeze(member) for member in value]
    raise ValueError("unsupported evidence value type: " + type(value).__name__)


def canonical(value):
    return json.dumps(_freeze(value), sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def signal_key(item):
    parts = [str(item.get("symbol") or "").upper(), str(item.get("timeframe") or ""), str(item.get("side") or "").upper()]
    if not all(parts) or any("|" in part for part in parts):
        raise ValueError("signal requires symbol, timeframe and side")
    return "|".join(parts)


def _score(item):
    value = item.get("score", item.get("priority"))
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def evaluate_state(previous_state, items):
    """Pure three-valued observation with persistent 65 / below-60 hysteresis.

    UNKNOWN means no trustworthy baseline. UNKNOWN observations preserve a
    previously trustworthy active value. First high observations bootstrap true.
    """
    state = _freeze(previous_state)
    crossing = []
    seen = set()
    for item in items:
        key = signal_key(item)
        if key in seen:
            raise ValueError("duplicate signal key in Watch batch")
        seen.add(key)
        prior = state.get(key) or {"active": None, "episode": 0}
        active, episode = prior["active"], int(prior["episode"])
        score = _score(item)
        observation = "UNKNOWN"
        if score is not None and score >= 65:
            observation = "TRUE"
            if active is False:
                episode += 1
                crossing.append(_freeze(item))
            active = True
        elif score is not None and score < 60:
            observation = "FALSE"
            active = False
        elif score is not None:
            observation = "HYSTERESIS"
        state[key] = {"active": active, "episode": episode, "observation": observation}
    if len(state) > MAX_STATE_KEYS:
        raise ValueError("transition state capacity exceeded")
    return state, crossing


def _result_metadata(previous_state, state, items, crossing, older=False):
    counts = dict(evaluated=0, crossings=0, resets=0, bootstrapped=0,
                  bootstrapped_true=0, bootstrapped_false=0, unknown=0)
    resets = []
    if not older:
        counts['evaluated'], counts['crossings'] = len(items), len(crossing)
        for item in items:
            key = signal_key(item)
            before = (previous_state.get(key) or {}).get('active')
            after = state[key]['active']
            if before is True and after is False:
                resets.append(key)
            if before is None and after is not None:
                counts['bootstrapped'] += 1
                counts['bootstrapped_true' if after else 'bootstrapped_false'] += 1
            if state[key]['observation'] == 'UNKNOWN' or after is None:
                counts['unknown'] += 1
        counts['resets'] = len(resets)
    return {'reset_keys': resets, 'counts': counts}


def _name(value, label):
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError(label + " must be a nonempty bounded string")
    return value


def _connect(database_url=None):
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required for durable Watch state")
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=CONNECT_TIMEOUT_SECONDS,
                          options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c lock_timeout={LOCK_TIMEOUT_MS}")


def init_schema(database_url=None):
    with _connect(database_url) as conn:
        # Serialize concurrent cold-start DDL without taking any market-data lock.
        conn.execute("SELECT pg_advisory_xact_lock(170650013)")
        conn.execute(_SCHEMA, prepare=False)
    return {"policy_version": POLICY_VERSION, "delivery_policy_version": DELIVERY_POLICY_VERSION}


def _intent_rows(conn, scope, policy, watch_id):
    rows = conn.execute("""SELECT * FROM watch_transition_intents
        WHERE subscription_scope=%s AND policy_version=%s AND watch_scan_id=%s
        ORDER BY ordinal LIMIT %s""", (scope, policy, watch_id, MAX_INTENTS)).fetchall()
    return [_public_intent(row) for row in rows]


def _public_intent(row):
    result = dict(row)
    for field in ("observed_at", "expires_at", "attempted_at", "acknowledged_at", "created_at"):
        result[field] = _iso(result[field]) if result.get(field) is not None else None
    result["source_time"] = result["observed_at"]
    return result


def _prune_history(conn, scope, moment):
    # State and its monotonic watermark are never pruned. Therefore a replay of
    # a receipt removed here cannot cause another transition or outbox intent.
    batches = conn.execute("""WITH old AS (
        SELECT subscription_scope,policy_version,watch_scan_id FROM watch_transition_batches
        WHERE subscription_scope=%s AND created_at<%s
        ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED
      ) DELETE FROM watch_transition_batches AS b USING old
        WHERE b.subscription_scope=old.subscription_scope AND b.policy_version=old.policy_version
        AND b.watch_scan_id=old.watch_scan_id""", (scope, moment-BATCH_RETENTION, PRUNE_LIMIT)).rowcount
    intents = conn.execute("""WITH old AS (
        SELECT intent_id FROM watch_transition_intents
        WHERE subscription_scope=%s AND created_at<%s
        AND status IN ('DELIVERED','UNKNOWN','FAILED','EXPIRED')
        ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED
      ) DELETE FROM watch_transition_intents AS i USING old WHERE i.intent_id=old.intent_id""",
      (scope, moment-TERMINAL_INTENT_RETENTION, PRUNE_LIMIT)).rowcount
    return {'batches': batches, 'terminal_intents': intents}


def prune_history(subscription_scope, now=None, database_url=None):
    scope = _name(subscription_scope, 'subscription_scope')
    moment = _utc(now) if now is not None else datetime.now(timezone.utc)
    with _connect(database_url) as conn:
        return _prune_history(conn, scope, moment)


def record_cycle(subscription_scope, watch_scan_id, observed_at, items, intent_factory,
                 database_url=None, policy_version=POLICY_VERSION):
    """Atomically record one coherent observation and its eligible intentions.

    Result shape: state maps signal keys to {active: bool|None, episode: int,
    observation: str}; crossing_items and resets are frozen input card mappings
    in input order; reset_keys contains their compact keys. counts reports
    evaluated/crossings/resets/bootstrapped/bootstrapped_true/bootstrapped_false/
    unknown. intents contains full records, including payload, status and
    intent_id. All returned intent timestamps (also source_time) are ISO UTC
    strings. idempotent_existing replays the saved receipt; rejected_older also
    covers a different Watch ID at an already recorded timestamp.

    The receipt stores only state, counts and crossing/reset keys, not cards.
    Input hashing includes frozen observed_at and items, never factory output
    or processing timestamps generated within this module.
    """
    from psycopg.types.json import Jsonb
    scope = _name(subscription_scope, "subscription_scope")
    policy = _name(policy_version, "policy_version")
    watch_id = _name(watch_scan_id, "watch_scan_id")
    observed = _utc(observed_at)
    frozen_items = _freeze(list(items))
    if len(frozen_items) > MAX_ITEMS:
        raise ValueError("Watch batch item capacity exceeded")
    # Validate keys before obtaining the transaction lock.
    by_key = {signal_key(item): item for item in frozen_items}
    if len(by_key) != len(frozen_items):
        raise ValueError("duplicate signal key in Watch batch")
    input_hash = _digest({"observed_at": _iso(observed), "items": frozen_items})
    with _connect(database_url) as conn:
        conn.execute("""INSERT INTO watch_transition_scopes(subscription_scope,policy_version)
            VALUES (%s,%s) ON CONFLICT DO NOTHING""", (scope, policy))
        current = conn.execute("""SELECT state,last_observed_at FROM watch_transition_scopes
            WHERE subscription_scope=%s AND policy_version=%s FOR UPDATE""", (scope, policy)).fetchone()
        existing = conn.execute("""SELECT input_sha256,result_state,result_metadata,crossing_keys,rejected_older
            FROM watch_transition_batches WHERE subscription_scope=%s AND policy_version=%s AND watch_scan_id=%s""",
            (scope, policy, watch_id)).fetchone()
        if existing:
            if existing["input_sha256"] != input_hash:
                raise ValueError("Watch batch identity collision")
            return {"state": existing["result_state"],
                    "crossing_items": [_freeze(by_key[key]) for key in existing["crossing_keys"]],
                    "intents": _intent_rows(conn, scope, policy, watch_id),
                    "idempotent_existing": True, "rejected_older": existing["rejected_older"],
                    'resets': [_freeze(by_key[key]) for key in existing['result_metadata']['reset_keys']],
                    **existing['result_metadata']}
        # Equal timestamps with a different Watch ID are conflicting snapshots,
        # not a later observation. Identical-ID retries were handled above.
        older = current["last_observed_at"] is not None and observed <= current["last_observed_at"]
        state, crossing = (current["state"], []) if older else evaluate_state(current["state"], frozen_items)
        metadata = _result_metadata(current['state'], state, frozen_items, crossing, older)
        descriptors = [] if older else _freeze(list(intent_factory(_freeze(crossing))))
        if len(descriptors) > MAX_INTENTS:
            raise ValueError("Watch intent capacity exceeded")
        crossing_keys = [signal_key(item) for item in crossing]
        unique_intents = set()
        for ordinal, descriptor in enumerate(descriptors):
            kind, key, payload = descriptor.get("kind"), descriptor.get("signal_key"), descriptor.get("payload")
            if kind not in KINDS or key not in crossing_keys or not isinstance(payload, dict):
                raise ValueError("intent must reference a crossing signal and supported kind")
            if (kind, key) in unique_intents:
                raise ValueError("duplicate intent for the same signal episode")
            unique_intents.add((kind, key))
            if len(canonical(payload).encode("utf-8")) > MAX_PAYLOAD_BYTES:
                raise ValueError("Watch intent payload capacity exceeded")
            episode = state[key]["episode"]
            intent_id = _digest([scope, policy, kind, key, episode])
            conn.execute("""INSERT INTO watch_transition_intents
                (intent_id,subscription_scope,policy_version,delivery_policy_version,watch_scan_id,
                 signal_key,episode,kind,ordinal,payload,observed_at,expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (intent_id, scope, policy, DELIVERY_POLICY_VERSION, watch_id, key, episode, kind,
                 ordinal, Jsonb(payload), observed, observed + DELIVERY_TTL))
        conn.execute("""INSERT INTO watch_transition_batches
            (subscription_scope,policy_version,watch_scan_id,observed_at,input_sha256,result_state,result_metadata,crossing_keys,rejected_older)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (scope, policy, watch_id, observed, input_hash, Jsonb(state), Jsonb(metadata), Jsonb(crossing_keys), older))
        if not older:
            conn.execute("""UPDATE watch_transition_scopes SET state=%s,last_observed_at=%s,updated_at=now()
                WHERE subscription_scope=%s AND policy_version=%s""", (Jsonb(state), observed, scope, policy))
        _prune_history(conn, scope, datetime.now(timezone.utc))
        return {"state": state, "crossing_items": crossing,
                "intents": _intent_rows(conn, scope, policy, watch_id),
                "idempotent_existing": False, "rejected_older": older,
                'resets': [_freeze(by_key[key]) for key in metadata['reset_keys']], **metadata}


def claim_pending(subscription_scope, now, limit=32, database_url=None, *, kinds=None):
    scope = _name(subscription_scope, "subscription_scope")
    moment = _utc(now)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_CLAIM_LIMIT:
        raise ValueError("invalid pending claim limit")
    selected_kinds = None if kinds is None else [_name(kind, "kind") for kind in kinds]
    if selected_kinds == []:
        return []
    with _connect(database_url) as conn:
        conn.execute("""UPDATE watch_transition_intents SET status='EXPIRED'
            WHERE subscription_scope=%s AND status='PENDING' AND expires_at<=%s""", (scope, moment))
        rows = conn.execute("""SELECT intent_id FROM watch_transition_intents
            WHERE subscription_scope=%s AND status='PENDING' AND observed_at<=%s AND expires_at>%s
              AND (%s::text[] IS NULL OR kind=ANY(%s::text[]))
            ORDER BY created_at,ordinal,intent_id LIMIT %s FOR UPDATE SKIP LOCKED""",
            (scope, moment, moment, selected_kinds, selected_kinds, limit)).fetchall()
        claimed = []
        for row in rows:
            token = uuid4().hex
            result = conn.execute("""UPDATE watch_transition_intents
                SET status='IN_FLIGHT',attempt_token=%s,attempted_at=%s
                WHERE intent_id=%s AND status='PENDING' RETURNING *""", (token, moment, row["intent_id"])).fetchone()
            if result:
                claimed.append(_public_intent(result))
        return claimed


def complete_attempt(intent_id, attempt_token, status, attempted_at, acknowledged_at=None,
                     error_type=None, database_url=None):
    if status not in {"DELIVERED", "UNKNOWN", "FAILED"}:
        raise ValueError("invalid delivery outcome")
    _name(intent_id, "intent_id")
    _name(attempt_token, "attempt_token")
    attempted = _utc(attempted_at)
    acknowledged = _utc(acknowledged_at) if acknowledged_at is not None else None
    if status == "DELIVERED" and acknowledged is None:
        raise ValueError("DELIVERED requires an acknowledgment timestamp")
    if acknowledged is not None and acknowledged < attempted:
        raise ValueError("acknowledgment precedes attempt")
    if error_type is not None:
        # Error class/category only; callers must never store transport secrets.
        error_type = _name(error_type, "error_type")
    with _connect(database_url) as conn:
        row = conn.execute("""UPDATE watch_transition_intents
            SET status=%s,attempted_at=%s,acknowledged_at=%s,error_type=%s
            WHERE intent_id=%s AND attempt_token=%s AND status='IN_FLIGHT'
            RETURNING intent_id""", (status, attempted, acknowledged, error_type, intent_id, attempt_token)).fetchone()
        return bool(row)


def release_unattempted(intent_id, attempt_token, database_url=None):
    """Release a claim only when the caller has not attempted any network send.

    This is a pre-send cancellation primitive, never a retry after a Telegram
    call, timeout, or ambiguous completion. The exact current claim token is
    required. A subsequent claim rechecks expiry and creates a different token.
    """
    _name(intent_id, 'intent_id')
    _name(attempt_token, 'attempt_token')
    with _connect(database_url) as conn:
        row = conn.execute("""UPDATE watch_transition_intents
            SET status='PENDING',attempt_token=NULL,attempted_at=NULL,
                acknowledged_at=NULL,error_type=NULL
            WHERE intent_id=%s AND attempt_token=%s AND status='IN_FLIGHT'
            RETURNING intent_id""", (intent_id, attempt_token)).fetchone()
        return bool(row)


def settle_orphans(subscription_scope, now=None, database_url=None):
    scope = _name(subscription_scope, "subscription_scope")
    moment = _utc(now) if now is not None else datetime.now(timezone.utc)
    with _connect(database_url) as conn:
        result = conn.execute("""WITH settled AS (
            UPDATE watch_transition_intents SET status='UNKNOWN',error_type='OrphanedAttempt'
            WHERE subscription_scope=%s AND status='IN_FLIGHT' AND attempted_at<=%s RETURNING intent_id
          ) SELECT count(*) AS count FROM settled""", (scope, moment - ORPHAN_TIMEOUT)).fetchone()
        return result['count']
