"""One-snapshot read adapter for the isolated DOGE liquidity paper observer.

Only retained source rows, causal parent membership and already persisted
Combined capture objects are read. No outcome, order or notification access.
Missing source proofs remain explicit and retryable. The complete bounded
source population is retained separately from the valid liquidity family.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

import research_liquidity_forward_protocol as reference

VERSION = "doge-liquidity-one-snapshot-source-v1"
STUDY_ID = "doge-liquidity-source-extension-entry2-short-forward-v1"
FORMULA_VERSION = "doge-native-plus-combined-liquidity-v1"
PROTOCOL_SHA256 = "dc0c69b9a1037e1dbb1e12c0ce7da6a278782a1a59b87bbb75dd27a19a2c621b"
NATIVE_EXTRACTOR_SHA256 = "a1e92c01e54d744dfd23966bad8d39b13c93a1e731cfcc0cc7ec3228434bd94b"
MAX_SOURCE_EVENTS = 4096
MAX_RESPONSE_BYTES = 24 * 1024 * 1024
MAX_OBSERVATIONS_PER_EVENT = 8
MAX_LINKS_PER_OBSERVATION = 8
MAX_DERIVATION_SECONDS = 8
PROTOCOL_PATH = Path(__file__).resolve().parent / "research_protocols" / "doge_liquidity_source_extension_v1.json"

# The +1 source row exposes overflow. It is never silently dropped. Every
# source, parent and opaque Combined prerequisite belongs to this SQL view.
SQL_TEMPLATE = rf"""
WITH bounded_events AS MATERIALIZED (
 SELECT e.* FROM research_events e
 WHERE e.symbol='DOGE' AND e.direction='SHORT'
   AND e.event_kind='ALERT' AND e.delivery_status='DELIVERED'
   AND e.alert_time_utc >= %s::timestamptz
   AND e.alert_time_utc <= statement_timestamp()
   AND (position('MAX_PAIN' IN e.event_type)>0 OR e.event_type='COMBINED_CONFIRMATION')
 ORDER BY e.alert_time_utc,e.event_id LIMIT %s
), source_budget AS MATERIALIZED (
 SELECT COALESCE(sum(2*octet_length(row_to_json(e)::text)+16384+
   COALESCE((SELECT sum(2*octet_length(b.manifest_json)+16384+
     COALESCE((SELECT sum(2*(4*((octet_length(bo.payload)::bigint+2)/3)+512))
       FROM research_combined_shadow_objects bo
       WHERE bo.sha256 IN (SELECT jsonb_array_elements_text(b.manifest_json::jsonb->'object_sha256s'))),0)+
     COALESCE((SELECT sum(2*octet_length(bl.receipt_json)+1024)
       FROM (SELECT receipt_json FROM research_combined_shadow_links
             WHERE observation_id=b.observation_id ORDER BY created_at_utc,link_receipt_id
             LIMIT {MAX_LINKS_PER_OBSERVATION + 1}) bl),0))
     FROM (SELECT observation_id,manifest_json FROM research_combined_shadow_observations
       WHERE e.event_type='COMBINED_CONFIRMATION'
         AND manifest_json::jsonb->>'event_fingerprint'=e.event_fingerprint
         AND manifest_json::jsonb->>'version'='doge-combined-selected-slot-shadow-v1'
       ORDER BY created_at_utc,observation_id LIMIT {MAX_OBSERVATIONS_PER_EVENT + 1}) b),0)),0)::bigint
     AS estimated_response_bytes
 FROM bounded_events e
), source_records AS MATERIALIZED (
 SELECT e.event_id,row_to_json(e)::text AS event_json_text,
 CASE WHEN bm.event_id IS NULL THEN NULL ELSE json_build_object(
   'event_id',e.event_id,'btc_parent_movement_id',bm.btc_parent_movement_id,
   'episode_policy_version',bm.episode_policy_version,'membership_status',bm.membership_status,
   'parent_evidence_eligible',p.evidence_eligible,'parent_start_time_utc',p.start_time_utc,
   'parent_end_time_utc',p.end_time_utc,'parent_confirmed_at_utc',p.confirmed_at_utc,
   'membership_created_at_utc',bm.created_at_utc,'parent_created_at_utc',p.created_at_utc,
   'btc_bar_created_at_utc',bar.created_at_utc,'btc_observed_close_utc',bm.btc_observed_close_utc,
   'membership_decision_time_utc',bm.decision_time_utc,'btc_bar_open_time_utc',bar.open_time_utc,
   'parent_boundary_reason',p.boundary_reason)::text END AS parent_json_text,
 COALESCE((SELECT json_agg(row_to_json(o) ORDER BY o.created_at_utc,o.observation_id)
 FROM (SELECT obs.observation_id,obs.root_sha256,obs.manifest_json AS manifest_json_text,
    obs.created_at_utc,
    COALESCE((SELECT json_agg(json_build_object('sha256',obj.sha256,'codec',obj.codec,
          'raw_bytes',obj.raw_bytes,'compressed_b64',replace(encode(obj.payload,'base64'),chr(10),'')) ORDER BY obj.sha256)
        FROM research_combined_shadow_objects obj
        WHERE obj.sha256 IN (SELECT jsonb_array_elements_text(obs.manifest_json::jsonb->'object_sha256s'))),'[]'::json) AS objects,
    COALESCE((SELECT json_agg(row_to_json(l) ORDER BY l.created_at_utc,l.link_receipt_id)
        FROM (SELECT link.link_receipt_id,link.receipt_json AS receipt_json_text,link.created_at_utc
          FROM research_combined_shadow_links link WHERE link.observation_id=obs.observation_id
          ORDER BY link.created_at_utc,link.link_receipt_id LIMIT {MAX_LINKS_PER_OBSERVATION + 1}) l),'[]'::json) AS links
  FROM research_combined_shadow_observations obs
  WHERE e.event_type='COMBINED_CONFIRMATION'
    AND obs.manifest_json::jsonb->>'event_fingerprint'=e.event_fingerprint
    AND obs.manifest_json::jsonb->>'version'='doge-combined-selected-slot-shadow-v1'
  ORDER BY obs.created_at_utc,obs.observation_id LIMIT {MAX_OBSERVATIONS_PER_EVENT + 1}) o),'[]'::json) AS combined_observations
 FROM bounded_events e
 LEFT JOIN research_event_btc_movements bm ON bm.event_id=e.event_id
   AND bm.episode_policy_version='btc-parent-close-reversal-200bps-v1'
 LEFT JOIN research_btc_parent_movements p ON p.btc_parent_movement_id=bm.btc_parent_movement_id
   AND p.episode_policy_version=bm.episode_policy_version
 LEFT JOIN research_btc_price_bars bar ON bar.close_time_utc=bm.btc_observed_close_utc
 WHERE (SELECT estimated_response_bytes FROM source_budget)<={MAX_RESPONSE_BYTES}
   AND (SELECT count(*) FROM bounded_events)<={MAX_SOURCE_EVENTS}
)
SELECT json_build_object('server_statement_utc',statement_timestamp(),
 'population_count',(SELECT count(*) FROM bounded_events),
 'estimated_response_bytes',(SELECT estimated_response_bytes FROM source_budget),
 'budget_exceeded',(SELECT estimated_response_bytes FROM source_budget)>{MAX_RESPONSE_BYTES},
 'records',COALESCE((SELECT json_agg(row_to_json(r) ORDER BY r.event_id) FROM source_records r),'[]'::json))::text
""".strip()
SQL_SHA256 = hashlib.sha256(SQL_TEMPLATE.encode("utf-8")).hexdigest()


class SourceRejected(RuntimeError):
    """Bounded safe error codes only: never include a database URL."""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _clock(value):
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise SourceRejected("INVALID_RECORDED_CLOCK") from None


@lru_cache(maxsize=1)
def load_protocol():
    value = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
    if reference.protocol_digest(value) != PROTOCOL_SHA256:
        raise SourceRejected("FROZEN_PROTOCOL_DRIFT")
    return value


def _connect(dsn):
    options = dict(connect_timeout=5, application_name="liquidity_forward_source",
                   keepalives=1, keepalives_idle=5, keepalives_interval=2,
                   keepalives_count=2, tcp_user_timeout=10000)
    try:
        import psycopg2
    except ImportError:
        import psycopg
        return psycopg.connect(dsn, **options)
    return psycopg2.connect(dsn, **options)


def _registry(code_registry):
    if not isinstance(code_registry, dict) or not code_registry:
        raise SourceRejected("SOURCE_CODE_REGISTRY_REQUIRED")
    for commit, pins in code_registry.items():
        if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40,64}", commit):
            raise SourceRejected("SOURCE_CODE_REGISTRY_COMMIT")
        if not isinstance(pins, dict) or set(pins) != reference.SHADOW_CODE_FILES:
            raise SourceRejected("SOURCE_CODE_REGISTRY_SET")
        if any(not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha) for sha in pins.values()):
            raise SourceRejected("SOURCE_CODE_REGISTRY_SHA")
    return code_registry


def _assemble(row):
    links = row.get("links", [])
    if not isinstance(links, list) or len(links) > MAX_LINKS_PER_OBSERVATION:
        raise SourceRejected("COMBINED_LINK_OVERFLOW")
    objects = {}
    for item in row.get("objects", []):
        sha = item["sha256"]
        if sha in objects:
            raise SourceRejected("COMBINED_DUPLICATE_OBJECT")
        objects[sha] = {key: item[key] for key in ("codec", "raw_bytes", "compressed_b64")}
    receipts = []
    for item in links:
        receipt = json.loads(item["receipt_json_text"])
        # These are declared database annotations alongside the unchanged
        # receipt JSON retained in source_closure, never replacement clocks.
        receipt["database_created_at_utc"] = item["created_at_utc"]
        receipts.append(receipt)
    return {"observation_id": row["observation_id"], "root_sha256": row["root_sha256"],
            "manifest": json.loads(row["manifest_json_text"]), "objects": objects,
            "database_created_at_utc": row["created_at_utc"], "link_receipts": receipts}


def _envelope(record, receipt, registry):
    event = json.loads(record["event_json_text"])
    if not isinstance(event, dict) or type(event.get("event_id")) is not int:
        raise SourceRejected("SOURCE_EVENT_SCHEMA")
    if record["event_id"] != event["event_id"]:
        raise SourceRejected("SOURCE_EVENT_ID_JOIN")
    # research_events calls the physical timestamp created_at. The reference
    # uses an explicit alias. Full original row text remains in the closure.
    if "created_at_utc" not in event:
        event["created_at_utc"] = event.get("created_at")
    for key in ("engine_snapshot", "categories"):
        if isinstance(event.get(key), str):
            event[key] = json.loads(event[key])
    base = {"event": event, "reader_receipt": deepcopy(receipt)}
    quality = {"event_id": event["event_id"], "event_fingerprint": event.get("event_fingerprint"),
               "status": None, "reason": None, "pending_transport_or_pin": False,
               "selected_observation_id": None}
    pins = registry.get(event.get("code_version"))
    if pins is None:
        quality.update(status="PENDING", reason="SOURCE_COMMIT_NOT_IN_DURABLE_REGISTRY", pending_transport_or_pin=True)
        return base, quality
    if pins.get("research_ordered_question_catalog.py") != NATIVE_EXTRACTOR_SHA256:
        quality.update(status="UNVERIFIED", reason="NATIVE_EXTRACTOR_CODE_DRIFT", pending_transport_or_pin=True)
        return base, quality
    if event.get("event_type") != "COMBINED_CONFIRMATION":
        point = reference.extract_point(base)
        quality.update(status=point["capture_status"], reason=point["reason"])
        return base, quality
    observations = record.get("combined_observations", [])
    if not isinstance(observations, list) or len(observations) > MAX_OBSERVATIONS_PER_EVENT:
        raise SourceRejected("COMBINED_OBSERVATION_OVERFLOW")
    reasons = []
    for row in observations:
        observation = _assemble(row)
        proposal = {**base, "combined_observation": observation}
        point = reference.extract_point(proposal)
        if point["capture_status"] != "VALID":
            reasons.append(point["reason"])
            continue
        identities = reference._objects(observation)["identity"]
        if identities["code_versions"] != pins:
            reasons.append("COMBINED_LIVE_SOURCE_CODE_PIN_MISMATCH")
            continue
        if (identities["runtime"].get("python_implementation") != "CPython" or
                identities["runtime"].get("python_version") != "3.11.9"):
            reasons.append("COMBINED_PRODUCER_RUNTIME_NOT_CPYTHON_3_11_9")
            continue
        try:
            reference._receipt(proposal)
        except (ValueError, TypeError, KeyError):
            reasons.append("COMBINED_PROOF_NOT_AVAILABLE_AT_READER_RECEIPT")
            continue
        quality.update(status="VALID", reason=point["reason"], selected_observation_id=observation["observation_id"])
        return proposal, quality
    quality.update(status="PENDING", reason=reasons[0] if reasons else "COMBINED_POSITIVE_PROOF_NOT_YET_OBSERVED", pending_transport_or_pin=True)
    return base, quality


def _liquidity_marker(envelope):
    event = envelope["event"]
    times = [_clock(event[name]) for name in ("alert_time_utc", "delivered_at_utc", "created_at_utc")]
    observation = envelope.get("combined_observation")
    if observation:
        manifest = observation["manifest"]
        times += [_clock(manifest["producer_captured_at_utc"]), _clock(observation["database_created_at_utc"])]
        times += [_clock(header["recorded_at_utc"]) for header in manifest["events"]]
        for link in observation["link_receipts"]:
            if link["link_status"] == "SOURCE_LINKED":
                times += [_clock(link["observed_at_utc"]), _clock(link["database_created_at_utc"])]
    return max(times).isoformat()


def derive_packet(envelope, policy, receipt, *, bootstrap=False, eligible_keys=(), code_registry=None):
    """Derive matches from one retained response, without database side effects."""
    began = time.monotonic()
    protocol = load_protocol()
    if policy.get("protocol_sha256") != PROTOCOL_SHA256 or policy.get("version") != STUDY_ID:
        raise SourceRejected("OBSERVER_POLICY_IDENTITY")
    epoch = _clock(policy["study_start_utc"])
    if epoch < _clock(protocol["epoch"]["activation_not_before_utc"]):
        raise SourceRejected("ACTIVATION_BEFORE_PROTOCOL")
    scopes = policy.get("scopes")
    if not isinstance(scopes, list) or len(scopes) != 1 or scopes[0].get("symbol") != "DOGE" or scopes[0].get("direction") != "SHORT":
        raise SourceRejected("PRIMARY_SCOPE_DRIFT")
    scope = scopes[0]
    if (not isinstance(scope.get("scope_key"), str) or not scope["scope_key"] or
            scope.get("candidate_key") != protocol["hypothesis"]["candidate_key"]):
        raise SourceRejected("CANDIDATE_SCOPE_IDENTITY")
    registry = _registry(code_registry)
    records = envelope.get("records")
    count = envelope.get("population_count")
    if (envelope.get("budget_exceeded") is not False or
            type(envelope.get("estimated_response_bytes")) is not int or
            not 0 <= envelope["estimated_response_bytes"] <= MAX_RESPONSE_BYTES):
        raise SourceRejected("SOURCE_SQL_BYTE_BUDGET")
    if not isinstance(records, list) or type(count) is not int or count > MAX_SOURCE_EVENTS:
        raise SourceRejected("SOURCE_POPULATION_OVERFLOW")
    if count != len(records) or len({row["event_id"] for row in records}) != count:
        raise SourceRejected("SOURCE_PARENT_JOIN_NOT_UNIQUE")
    if len(_json(envelope).encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise SourceRejected("SOURCE_RESPONSE_BYTE_BOUND")
    source_closure = {"version": VERSION, "query_sha256": SQL_SHA256,
                      "query_parameters": {"study_start_utc": policy["study_start_utc"], "row_overflow_limit": MAX_SOURCE_EVENTS + 1},
                      "scope": "All post-epoch retained delivered DOGE SHORT native/Combined rows visible in this one SQL snapshot",
                      "population_count": count, "records": deepcopy(records), "code_registry": deepcopy(registry)}
    activation = {"study_id": STUDY_ID, "protocol_sha256": PROTOCOL_SHA256,
                  "activated_at_utc": policy["study_start_utc"],
                  "activation_receipt_id": "durable-policy:" + _hash(policy)}
    sources, qualities, parents = [], [], {}
    for record in records:
        current, quality = _envelope(record, receipt, registry)
        event = current["event"]
        if event.get("symbol") != "DOGE" or event.get("direction") != "SHORT" or event.get("event_kind") != "ALERT" or event.get("delivery_status") != "DELIVERED":
            raise SourceRejected("SQL_PRIMARY_SCOPE_MISMATCH")
        if _clock(event["alert_time_utc"]) < epoch:
            raise SourceRejected("SQL_PRE_EPOCH_SOURCE")
        sources.append(current)
        qualities.append(quality)
        parent = json.loads(record["parent_json_text"]) if record.get("parent_json_text") else None
        if parent is not None:
            parents[event["event_id"]] = parent
    quality_by_id = {row["event_id"]: row for row in qualities}
    valid_history = [row for row in sources if quality_by_id[row["event"]["event_id"]]["status"] == "VALID"]
    skipped = set(eligible_keys)
    matches, decisions = [], []
    for current in sources:
        if time.monotonic() - began > MAX_DERIVATION_SECONDS:
            raise SourceRejected("SOURCE_DERIVATION_DEADLINE")
        event = current["event"]
        event_id, when = event["event_id"], _clock(event["alert_time_utc"])
        if scope["scope_key"] + ":" + str(event_id) in skipped:
            continue
        quality = quality_by_id[event_id]
        if quality["status"] != "VALID":
            decisions.append({"event_id": event_id, "status": "DEFERRED", "reason": quality["reason"]})
            continue
        history = [row for row in valid_history if row["event"]["event_id"] != event_id and when - timedelta(minutes=30) <= _clock(row["event"]["alert_time_utc"]) < when]
        pending = [row for row in sources if row["event"]["event_id"] != event_id and when - timedelta(minutes=30) <= _clock(row["event"]["alert_time_utc"]) < when and quality_by_id[row["event"]["event_id"]]["pending_transport_or_pin"]]
        parent = parents.get(event_id)
        result = reference.evaluate(current, history, activation=activation, parent=parent, protocol=protocol)
        decisions.append({"event_id": event_id, "status": result["status"], "reason": result["reason"],
                          "pending_prior_count": len(pending), "source_history_uncertain": bool(pending)})
        if result["status"] != "CONFORMANCE_MATCH":
            continue
        conformance_input = {"current": current, "history": history, "parent": parent, "activation": activation}
        marker = _liquidity_marker(current)
        row = {"scope_key": scope["scope_key"], "candidate_key": scope["candidate_key"],
               "symbol": "DOGE", "formula_direction": "SHORT", "event_id": event_id,
               "source_event_id": event_id, "event_fingerprint": event["event_fingerprint"],
               "source_direction": event["direction"], "event_kind": event["event_kind"],
               "event_type": event["event_type"], "alert_time_utc": event["alert_time_utc"],
               "match_source_time_utc": event["alert_time_utc"],
               "delivered_at_utc": event["delivered_at_utc"], "delivery_status": event["delivery_status"],
               "event_created_at_utc": event["created_at_utc"], "liquidity_evidence_created_at_utc": marker,
               "current_price": event.get("current_price"),
               "candidate_definition_sha256": PROTOCOL_SHA256, "candidate_formula_version": FORMULA_VERSION,
               "protocol_sha256": PROTOCOL_SHA256, "source_point_status": "VALID",
               "conformance_status": "CONFORMANCE_MATCH", "computed_entry_ordinal": 2,
               "conformance_evidence_sha256": _hash(conformance_input),
               "source_contract": result["point"]["source_contract"],
               "source_point": result["point"], "sequence_features": result["features"],
               "history_event_ids": [old["event"]["event_id"] for old in history],
               "included_history": result["included_history"], "excluded_history": result["excluded_history"],
               "pending_prior_count": len(pending), "source_history_uncertain": bool(pending),
               "pending_prior_event_ids": [old["event"]["event_id"] for old in pending],
               "quality_refs": [{"event_id": old["event"]["event_id"],
                                 "reason": quality_by_id[old["event"]["event_id"]]["reason"]} for old in pending],
               "reader_receipt_id": receipt["receipt_id"],
               "source_observation_id": quality["selected_observation_id"],
               "source_closure_event_id": event_id,
               **deepcopy(parent)}
        matches.append(row)
    rows = sorted(matches, key=lambda row: (row["match_source_time_utc"], row["event_id"]))
    parents_seen = sorted({parent["btc_parent_movement_id"] for parent in parents.values()
                           if parent.get("membership_status") == "LIVE" and isinstance(parent.get("btc_parent_movement_id"), str) and parent["btc_parent_movement_id"]})
    return {"poll_id": receipt["receipt_id"], "request_started_utc": receipt["request_started_utc"],
            "response_received_utc": receipt["response_received_utc"], "db_server_statement_utc": envelope["server_statement_utc"],
            "source": "postgres_read_only_repeatable_read", "source_version": VERSION,
            "source_query_sha256": SQL_SHA256, "source_complete": True, "bootstrap": bool(bootstrap),
            "rows": rows, "observed_sources": [row["event"] for row in sources],
            "observed_parent_ids": parents_seen, "source_qualities": qualities, "decisions": decisions,
            "source_closure": source_closure, "source_closure_sha256": _hash(source_closure),
            "source_population_count": count, "valid_liquidity_points": len(valid_history),
            "sql_estimated_response_bytes": envelope["estimated_response_bytes"],
            "pending_liquidity_points": sum(bool(row["pending_transport_or_pin"]) for row in qualities),
            "clock_scope": "Actual independent observer response receipt; not source creation, first insert or execution",
            "marker_mapping": "liquidity_evidence_created_at_utc is max actual recorded source and selected proof prerequisites, never response receipt or an old match row",
            "derivation_elapsed_seconds": time.monotonic() - began}


def fetch_candidates(dsn, policy, template=None, *, bootstrap=False, eligible_keys=(), code_registry=None, connect=None):
    """Read all bounded source prerequisites in one repeatable-read snapshot."""
    if template is not None and template.strip() != SQL_TEMPLATE:
        raise SourceRejected("SOURCE_SQL_TEMPLATE_DRIFT")
    conn = (connect or _connect)(dsn)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            started, timer = _utc(), time.monotonic()
            cursor.execute(SQL_TEMPLATE, (policy["study_start_utc"], MAX_SOURCE_EVENTS + 1))
            raw = cursor.fetchone()[0]
            received, elapsed = _utc(), time.monotonic() - timer
        conn.rollback()
    finally:
        conn.close()
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > MAX_RESPONSE_BYTES:
            raise SourceRejected("SOURCE_RESPONSE_BYTE_BOUND")
        envelope = json.loads(raw)
    else:
        envelope = raw
    receipt = {"receipt_id": str(uuid.uuid4()), "request_started_utc": started,
               "response_received_utc": received}
    packet = derive_packet(envelope, policy, receipt, bootstrap=bootstrap,
                           eligible_keys=eligible_keys, code_registry=code_registry)
    packet["monotonic_elapsed_seconds"] = elapsed
    return packet
