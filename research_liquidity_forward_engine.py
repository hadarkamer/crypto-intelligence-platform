"""Isolated DOGE liquidity-source extension, checkpointed by its own study ID.

The quote/capacity model is deliberately forked from the existing paper engine;
the existing study implementation and its configuration remain untouched.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from collections import Counter

PROTOCOL_SHA256 = "dc0c69b9a1037e1dbb1e12c0ce7da6a278782a1a59b87bbb75dd27a19a2c621b"
STUDY_ID = "doge-liquidity-source-extension-entry2-short-forward-v1"
CANDIDATE_KEY = "doge-native-plus-combined-liquidity-v1:liquidity_ENTRY_2:NORMAL:SHORT"
FORMULA_VERSION = "doge-native-plus-combined-liquidity-v1"
PARENT_VERSION = "btc-parent-close-reversal-200bps-v1"
PREPARATION_EVENT_IDS = frozenset((455744, 468953, 469375, 471121, 471332, 471958, 473857, 474774, 475132))
SOURCE_CODE_FILES = ("research_combined_liquidity_shadow.py", "research_event_runtime.py",
                     "research_event_capture.py", "research_event_store.py", "research_consumed_inputs.py",
                     "research_ordered_question_catalog.py", "main.py", "analysis.py",
                     "research_watch_decision_capture.py")
REQUIRED_MARKERS = frozenset(("alert_time_utc", "delivered_at_utc", "event_created_at_utc",
    "liquidity_evidence_created_at_utc", "membership_created_at_utc", "parent_created_at_utc",
    "parent_confirmed_at_utc", "btc_bar_created_at_utc", "btc_observed_close_utc"))
MAX_SOURCE_CLOSURE_BYTES = 64 * 1024 * 1024
MAX_OBSERVED_SOURCES = 8192


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _time(value):
    if not isinstance(value, str):
        raise ValueError("UTC timestamp required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Explicit UTC required")
    return result.astimezone(timezone.utc)


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("Boolean price")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("Invalid price") from None
    if not result.is_finite():
        raise ValueError("Nonfinite price")
    return result


class PaperStudy:
    """One DOGE quote-observation book with a durable source-receipt journal.

    Local receipt time is the availability clock. Source creation timestamps
    are eligibility metadata and deterministic ties inside a single response.
    They never backdate entry or replace an already consumed parent.
    """

    def __init__(self, db_path, policy: dict, *, resume=True):
        self.policy = copy.deepcopy(policy)
        self.scope_list = policy.get("scopes", policy.get("scope"))
        if not isinstance(self.scope_list, list):
            raise ValueError("Policy requires scopes list")
        self.scopes = {s["scope_key"]: s for s in self.scope_list}
        if len(self.scopes) != 1 or len(self.scope_list) != 1:
            raise ValueError("Exactly one isolated DOGE scope required")
        scope = self.scope_list[0]
        if (scope.get("symbol") != "DOGE" or scope.get("direction") != "SHORT" or
                scope.get("candidate_key") != CANDIDATE_KEY or
                scope.get("candidate_definition_sha256") != PROTOCOL_SHA256 or
                scope.get("formula_version") != FORMULA_VERSION or scope.get("window_minutes") != 720 or
                scope.get("threshold_bps") != 50 or not isinstance(scope.get("scope_key"), str) or
                not 0 < len(scope["scope_key"]) <= 256):
            raise ValueError("Frozen DOGE hypothesis differs from protocol")
        if policy.get("protocol_sha256") != PROTOCOL_SHA256 or policy.get("version") != STUDY_ID:
            raise ValueError("Frozen liquidity protocol identity required")
        self.start = _time(policy["study_start_utc"])
        if self.start < _time("2026-10-29T12:30:58+00:00"):
            raise ValueError("Study activation predates frozen protocol")
        self.markers = tuple(policy["required_markers"])
        if len(self.markers) != len(REQUIRED_MARKERS) or set(self.markers) != REQUIRED_MARKERS:
            raise ValueError("Nine explicit liquidity-source markers required")
        self.books = tuple(policy["books"])
        if self.books != ("DOGE_only",):
            raise ValueError("One DOGE-only book required")
        self.width = _decimal(policy["barrier_bps"]) / 10000
        self.cost = _decimal(policy["net_cost_pp"])
        if (self.width != Decimal("0.005") or self.cost != Decimal("0.1") or
                _decimal(policy["entry_expiry_seconds"]) != 90 or
                _decimal(policy["quote_max_elapsed_seconds"]) != 5 or
                _decimal(policy["max_quote_gap_seconds"]) != 30 or
                _decimal(policy.get("poll_seconds", 10)) != 10):
            raise ValueError("Frozen paper measurement settings differ from protocol")
        if (policy.get("quote_source") != "BINANCE_SPOT_BOOK_TICKER" or
                policy.get("quote_symbols") != {"DOGE": "DOGEUSDT"}):
            raise ValueError("Frozen DOGE public quote source required")
        for key in ("entry_expiry_seconds", "quote_max_elapsed_seconds", "max_quote_gap_seconds"):
            if float(policy[key]) <= 0:
                raise ValueError(f"Positive {key} required")
        if self.width <= 0 or self.cost < 0:
            raise ValueError("Invalid barrier or cost")
        self.policy_json = _json(self.policy)
        self.policy_sha256 = hashlib.sha256(self.policy_json.encode()).hexdigest()
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), timeout=10, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY,value TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS journal (event_id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,utc TEXT,input_key TEXT UNIQUE,payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL)")
        self.db.execute("CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal BEGIN SELECT RAISE(ABORT,'append-only journal'); END")
        self.db.execute("CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal BEGIN SELECT RAISE(ABORT,'append-only journal'); END")
        self.db.execute("CREATE TABLE IF NOT EXISTS projection (id INTEGER PRIMARY KEY CHECK(id=1),state_json TEXT NOT NULL)")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT value FROM metadata WHERE name='policy_sha256'").fetchone()
            if old:
                if old[0] != self.policy_sha256:
                    raise ValueError("Frozen policy digest differs from existing study")
                state = self._load()
                if resume:
                    self._emit("PROCESS_RESTART", state["last_response_utc"], {"policy_sha256": self.policy_sha256})
                    for name, book in state["books"].items():
                        if book["active"]:
                            self._uncertain(book["active"], "PROCESS_RESTART", name, state["last_response_utc"])
                    self._save(state)
            else:
                state = {"last_response_utc": None, "bootstrap_complete": False,
                         "bootstrap_event_keys": [], "bootstrap_parent_ids": [],
                         "source_code_registries": {},
                         "first_seen": {}, "last_quotes": {}, "observation_sequence": 0,
                         "last_observation_rejected": None,
                         "books": {name: {"choices": {}, "reservation": None, "active": None, "closed": []} for name in self.books}}
                self.db.execute("INSERT INTO metadata VALUES('policy_sha256',?)", (self.policy_sha256,))
                self.db.execute("INSERT INTO metadata VALUES('policy_json',?)", (self.policy_json,))
                self._emit("STUDY_CREATED", _iso(self.start), {"policy_sha256": self.policy_sha256, "policy": policy})
                self._save(state)
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            self.db.close()
            raise

    def _load(self):
        row = self.db.execute("SELECT state_json FROM projection WHERE id=1").fetchone()
        return json.loads(row[0])

    def resume_observer(self):
        """Record actual restart time and retain uncertainty for open positions."""
        now = _iso(datetime.now(timezone.utc))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            state = self._load()
            self._emit("PROCESS_RESTART", now, {"policy_sha256": self.policy_sha256})
            for name, book in state["books"].items():
                if book["active"]:
                    self._uncertain(book["active"], "PROCESS_RESTART", name, now)
            self._save(state)
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def _save(self, state):
        self.db.execute("INSERT INTO projection(id,state_json) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json", (_json(state),))

    def _emit(self, kind, utc, payload, input_key=None):
        text = _json(payload)
        self.db.execute("INSERT INTO journal(kind,utc,input_key,payload_json,payload_sha256) VALUES(?,?,?,?,?)",
                        (kind, utc, input_key, text, hashlib.sha256(text.encode()).hexdigest()))

    def _uncertain(self, position, reason, book_name, utc):
        if reason not in position["uncertainty_reasons"]:
            position["uncertainty_reasons"].append(reason)
            position["path_uncertain"] = True
            self._emit("PATH_UNCERTAIN", utc, {"book": book_name, "case_id": position["case_id"], "reason": reason})

    def source_code_registry(self):
        """Return only byte pins from durably checkpointed runtime identities."""
        return {commit: copy.deepcopy(row["code_versions"])
                for commit, row in self._load().get("source_code_registries", {}).items()}

    def observe_runtime_identity(self, packet):
        """Register current process bytes before its first external source read.

        The worker must checkpoint this transition before asking the source
        adapter to use it. Identical commit pins are idempotent across restart;
        a conflicting byte set for the same commit is rejected.
        """
        commit = packet.get("runtime_commit")
        codes = packet.get("code_versions")
        if (packet.get("protocol_sha256") != PROTOCOL_SHA256 or
                not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit) or
                not isinstance(codes, dict) or set(codes) != set(SOURCE_CODE_FILES) or
                any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in codes.values()) or
                packet.get("python_implementation") != "CPython" or packet.get("python_version") != "3.11.9"):
            raise ValueError("Invalid source runtime byte registry")
        observed = _time(packet["observed_at_utc"])
        if observed < self.start:
            raise ValueError("Runtime identity predates new study epoch")
        material = {key: copy.deepcopy(packet[key]) for key in
                    ("runtime_commit", "code_versions", "python_implementation", "python_version")}
        material["attestation_scope"] = "LIVE_PROCESS_FILE_BYTES"
        materials = {commit: material}
        seeds = packet.get("trusted_seed_registries", {})
        if not isinstance(seeds, dict):
            raise ValueError("Invalid trusted seed registries")
        for seed_commit, seed_codes in seeds.items():
            if (not isinstance(seed_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", seed_commit) or
                    not isinstance(seed_codes, dict) or set(seed_codes) != set(SOURCE_CODE_FILES) or
                    any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in seed_codes.values())):
                raise ValueError("Invalid trusted seed source byte registry")
            if seed_commit == commit:
                if seed_codes != codes:
                    raise ValueError("Trusted seed conflicts with live source bytes")
                continue
            materials[seed_commit] = {"runtime_commit": seed_commit, "code_versions": copy.deepcopy(seed_codes),
                "python_implementation": "CPython", "python_version": "3.11.9",
                "attestation_scope": "TRUSTED_REPOSITORY_SEED_FILE_BYTES"}
        self.db.execute("BEGIN IMMEDIATE")
        try:
            state = self._load()
            registry = state.setdefault("source_code_registries", {})
            for registered_commit, identity in materials.items():
                identity_sha = hashlib.sha256(_json(identity).encode()).hexdigest()
                previous = registry.get(registered_commit)
                if previous is not None:
                    # A seed may later become the active process, or the same
                    # pins may be supplied as a seed after an actual restart.
                    if (previous["code_versions"] != identity["code_versions"] or
                            previous["python_implementation"] != identity["python_implementation"] or
                            previous["python_version"] != identity["python_version"]):
                        raise ValueError("Conflicting source bytes under an existing runtime commit")
                else:
                    registry[registered_commit] = {**identity, "identity_sha256": identity_sha,
                                                  "observed_at_utc": _iso(observed)}
                    self._emit("SOURCE_RUNTIME_IDENTITY", _iso(observed), registry[registered_commit],
                               input_key="SOURCE_RUNTIME_IDENTITY:" + registered_commit)
            self._save(state)
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return self.summary()

    def _source_receipt_material(self, packet):
        if packet.get("source_complete") is not True:
            raise ValueError("Source packet is incomplete")
        closure = packet.get("source_closure")
        if not isinstance(closure, dict):
            raise ValueError("Full source closure required")
        encoded = _json(closure).encode()
        if not 0 < len(encoded) <= MAX_SOURCE_CLOSURE_BYTES:
            raise ValueError("Source closure exceeds protocol resource bound")
        closure_sha = hashlib.sha256(encoded).hexdigest()
        if packet.get("source_closure_sha256") != closure_sha:
            raise ValueError("Source closure hash mismatch")
        observed = packet.get("observed_sources")
        if not isinstance(observed, list) or len(observed) > MAX_OBSERVED_SOURCES:
            raise ValueError("Bounded raw observed source array required")
        if "records" in closure:
            closed_sources = []
            for record in closure["records"]:
                source = json.loads(record["event_json_text"])
                if "created_at_utc" not in source:
                    source["created_at_utc"] = source.get("created_at")
                for name in ("engine_snapshot", "categories"):
                    if isinstance(source.get(name), str):
                        source[name] = json.loads(source[name])
                closed_sources.append(source)
            if _json(closed_sources) != _json(observed):
                raise ValueError("Observed sources differ from retained SQL closure")
        journal_packet = dict(packet)
        journal_packet.pop("source_closure")
        # The complete original rows are retained in the content-addressed
        # closure. A receipt retains their identities without repeated bodies.
        journal_packet["observed_sources"] = [{"event_id": row.get("event_id"),
                                               "event_fingerprint": row.get("event_fingerprint")}
                                              for row in observed]
        return journal_packet, closure, closure_sha

    def _apply(self, packet, kind):
        closure = None
        closure_sha = None
        journal_packet = packet
        if kind == "CANDIDATES":
            journal_packet, closure, closure_sha = self._source_receipt_material(packet)
        text = _json(journal_packet)
        digest = hashlib.sha256(text.encode()).hexdigest()
        identifier = packet.get("poll_id") if kind == "CANDIDATES" else packet.get("packet_id", packet.get("quote_id"))
        identifier = str(identifier) if identifier is not None else digest
        key = f"{kind}:{identifier}"
        self.db.execute("BEGIN IMMEDIATE")
        try:
            previous = self.db.execute("SELECT payload_sha256 FROM journal WHERE input_key=?", (key,)).fetchone()
            if previous:
                if previous[0] != digest:
                    raise ValueError("Duplicate input identity with conflicting payload")
                self.db.execute("COMMIT")
                result = self.summary()
                result["idempotent_duplicate"] = True
                return result
            state = self._load()
            if closure is not None:
                closure_key = "SOURCE_CLOSURE:" + closure_sha
                prior_closure = self.db.execute("SELECT payload_sha256 FROM journal WHERE input_key=?", (closure_key,)).fetchone()
                if prior_closure:
                    if prior_closure[0] != closure_sha:
                        raise ValueError("Source closure content-address collision")
                else:
                    self._emit("SOURCE_CLOSURE", packet.get("response_received_utc"), closure, closure_key)
            self._emit("INPUT_" + kind, packet.get("response_received_utc"), journal_packet, key)
            self.db.execute("SAVEPOINT decide")
            try:
                started = _time(packet["request_started_utc"])
                received = _time(packet["response_received_utc"])
                if started > received or started < self.start or received < self.start:
                    raise ValueError("Observation chronology is invalid")
                if state["last_response_utc"] and received < _time(state["last_response_utc"]):
                    raise ValueError("Response time reversal")
                working = copy.deepcopy(state)
                working["observation_sequence"] += 1
                working["last_observation_rejected"] = None
                if kind == "CANDIDATES":
                    self._candidates(working, packet, started, received)
                else:
                    self._quotes(working, packet, started, received)
                working["last_response_utc"] = _iso(received)
                self._save(working)
                self.db.execute("RELEASE decide")
            except (ValueError, KeyError, TypeError) as error:
                self.db.execute("ROLLBACK TO decide")
                self.db.execute("RELEASE decide")
                state["last_observation_rejected"] = str(error)
                self._emit("INPUT_REJECTED", packet.get("response_received_utc"), {"input_key": key, "reason": str(error)})
                self._save(state)
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return self.summary()

    def observe_candidates(self, packet: dict):
        return self._apply(packet, "CANDIDATES")

    def observe_quotes(self, packet: dict):
        return self._apply(packet, "QUOTES")

    def _advance(self, state, until, inclusive=False):
        """Resolve planned gates before later exits; equal-time quotes exit first."""
        for name, book in state["books"].items():
            choices = sorted((c for c in book["choices"].values() if c["status"] == "PLANNED"),
                             key=lambda c: (c["planned_entry_utc"], c["selection_sequence"], c["event_id"], c["symbol"]))
            for choice in choices:
                planned = _time(choice["planned_entry_utc"])
                if planned > until or (planned == until and not inclusive):
                    continue
                self._expire(book, name, planned)
                if book["active"] is not None or book["reservation"] is not None:
                    choice["status"] = "BLOCKED_CAPACITY"
                    self._emit("CAPACITY_BLOCKED", choice["planned_entry_utc"], {"book": name, "case_id": choice["case_id"], "parent_id": choice["parent_id"], "no_queue": True, "no_fallback": True})
                else:
                    choice["status"] = "RESERVED"
                    book["reservation"] = choice["parent_id"]
                    self._emit("ENTRY_RESERVED", choice["planned_entry_utc"], {"book": name, "case_id": choice["case_id"]})
            self._expire(book, name, until)
            if book["active"]:
                active = book["active"]
                if (until - _time(active["last_quote_received_utc"])).total_seconds() > float(self.policy["max_quote_gap_seconds"]):
                    self._uncertain(active, "QUOTE_GAP", name, _iso(until))

    def _expire(self, book, name, when):
        if book["reservation"]:
            choice = book["choices"][book["reservation"]]
            if _time(choice["entry_expiry_utc"]) < when:
                choice["status"] = "EXPIRED_NO_ENTRY_QUOTE"
                book["reservation"] = None
                self._emit("ENTRY_EXPIRED", choice["entry_expiry_utc"], {"book": name, "case_id": choice["case_id"], "parent_consumed": True})

    def _eligible(self, row, received):
        scope = self.scopes.get(row.get("scope_key"))
        if scope is None:
            return None, "SCOPE_UNKNOWN"
        try:
            if row.get("protocol_sha256") != PROTOCOL_SHA256:
                return None, "POLICY_SOURCE_DRIFT"
            if (row.get("source_point_status") != "VALID" or row.get("conformance_status") != "CONFORMANCE_MATCH" or
                    type(row.get("computed_entry_ordinal")) is not int or row.get("computed_entry_ordinal") != 2 or
                    not isinstance(row.get("conformance_evidence_sha256"), str) or
                    not re.fullmatch(r"[0-9a-f]{64}", row["conformance_evidence_sha256"])):
                return None, "CONFORMANCE_NOT_VALIDATED"
            if (scope.get("candidate_definition_sha256") is not None and
                    row.get("candidate_definition_sha256") != scope["candidate_definition_sha256"]):
                return None, "POLICY_SOURCE_DRIFT"
            if (scope.get("formula_version") is not None and
                    row.get("candidate_formula_version") != scope["formula_version"]):
                return None, "POLICY_SOURCE_DRIFT"
            if row.get("symbol") != scope["symbol"] or row.get("formula_direction") != scope["direction"]:
                return None, "SCOPE_IDENTITY"
            if row.get("candidate_key") != scope["candidate_key"]:
                return None, "CANDIDATE_KEY"
            event_id = row.get("event_id")
            if not isinstance(event_id, int) or isinstance(event_id, bool) or row.get("source_event_id") != event_id:
                return None, "SOURCE_ID"
            if event_id in PREPARATION_EVENT_IDS:
                return None, "PREPARATION_EVENT_EXCLUDED"
            if row.get("event_kind") != "ALERT" or row.get("delivery_status") != "DELIVERED":
                return None, "NOT_DELIVERED_ALERT"
            if row.get("membership_status") != "LIVE" or row.get("parent_evidence_eligible") is not True:
                return None, "PARENT_MEMBERSHIP"
            if row.get("episode_policy_version") != PARENT_VERSION:
                return None, "PARENT_POLICY_VERSION"
            if not row.get("btc_parent_movement_id"):
                return None, "PARENT_ID_MISSING"
            parent_start = _time(row["parent_start_time_utc"])
            if parent_start <= self.start:
                return None, "OLD_PARENT"
            times = {k: _time(row[k]) for k in self.markers}
            if not parent_start <= times["parent_confirmed_at_utc"] <= times["alert_time_utc"]:
                return None, "SOURCE_CAUSALITY"
            if times["alert_time_utc"] < self.start:
                return None, "OLD_SOURCE"
            if times["alert_time_utc"] < self.start + timedelta(minutes=30):
                return None, "SOURCE_IN_30M_WARMUP"
            if max(times.values()) > received:
                return None, "FUTURE_SOURCE_MARKER"
            if times["alert_time_utc"] != _time(row["match_source_time_utc"]):
                return None, "MATCH_SOURCE_TIME"
            age = (times["alert_time_utc"] - times["btc_observed_close_utc"]).total_seconds()
            if not 0 <= age < 60 or times["parent_confirmed_at_utc"] > times["alert_time_utc"]:
                return None, "SOURCE_CAUSALITY"
            return {"scope": scope, "max_marker": max(times.values()), "event_id": event_id,
                    "symbol": scope["symbol"], "parent_id": row["btc_parent_movement_id"],
                    "source_metadata": {key: copy.deepcopy(row.get(key)) for key in
                        ("source_contract", "conformance_evidence_sha256", "pending_prior_count",
                         "source_history_uncertain", "quality_refs")}}, None
        except (ValueError, KeyError, TypeError):
            return None, "MISSING_OR_INVALID_TIMESTAMP"

    def _candidates(self, state, packet, started, received):
        self._advance(state, received)
        if not state.get("source_code_registries"):
            raise ValueError("Source runtime registry required before source receipt")
        rows = packet["rows"]
        if not isinstance(rows, list) or len(rows) > MAX_OBSERVED_SOURCES:
            raise ValueError("Bounded candidate rows require list")
        if len({row.get("event_id") for row in rows}) != len(rows):
            raise ValueError("Duplicate candidate event identity")
        bootstrap = bool(packet.get("bootstrap"))
        if not state["bootstrap_complete"] and not bootstrap:
            raise ValueError("First candidate observation must be excluded bootstrap")
        if state["bootstrap_complete"] and bootstrap:
            raise ValueError("Bootstrap already frozen")
        raw_seen = set()
        population = packet.get("source_population_count", len(packet["observed_sources"]))
        pending = packet.get("pending_liquidity_points", 0)
        if (type(population) is not int or population != len(packet["observed_sources"]) or
                type(pending) is not int or not 0 <= pending <= population):
            raise ValueError("Source population diagnostics are inconsistent")
        state["last_source_closure_sha256"] = packet["source_closure_sha256"]
        state["pending_source_count"] = pending
        scope_key = self.scope_list[0]["scope_key"]
        for source in packet["observed_sources"]:
            event_id = source.get("event_id")
            fingerprint = source.get("event_fingerprint")
            if type(event_id) is not int or event_id <= 0 or not isinstance(fingerprint, str) or not fingerprint:
                raise ValueError("Raw source identity is invalid")
            if event_id in raw_seen:
                raise ValueError("Duplicate raw source event identity")
            raw_seen.add(event_id)
            identity = f"{scope_key}:{event_id}"
            record = state["first_seen"].setdefault(identity, {"raw_first_seen_utc": _iso(received),
                "raw_first_poll_id": packet["poll_id"], "first_eligible_seen_utc": None,
                "event_fingerprint": fingerprint,
                "raw_first_source_closure_sha256": packet["source_closure_sha256"]})
            if record.get("event_fingerprint", fingerprint) != fingerprint:
                raise ValueError("Raw event fingerprint changed under the same event ID")
            if bootstrap and identity not in state["bootstrap_event_keys"]:
                state["bootstrap_event_keys"].append(identity)
        if bootstrap:
            for parent_id in packet.get("observed_parent_ids", []):
                if not isinstance(parent_id, str) or not parent_id:
                    raise ValueError("Bootstrap parent identity is invalid")
                if parent_id not in state["bootstrap_parent_ids"]:
                    state["bootstrap_parent_ids"].append(parent_id)
        eligible = []
        for row in rows:
            if row.get("event_id") not in raw_seen:
                raise ValueError("Candidate has no source in its observed source closure")
            identity = f"{scope_key}:{row.get('event_id')}"
            parent = row.get("btc_parent_movement_id")
            record = state["first_seen"].setdefault(identity, {"raw_first_seen_utc": _iso(received),
                                                             "raw_first_poll_id": packet["poll_id"],
                                                             "first_eligible_seen_utc": None})
            if bootstrap:
                if identity not in state["bootstrap_event_keys"]:
                    state["bootstrap_event_keys"].append(identity)
                if parent and parent not in state["bootstrap_parent_ids"]:
                    state["bootstrap_parent_ids"].append(parent)
                continue
            if identity in state["bootstrap_event_keys"] or parent in state["bootstrap_parent_ids"]:
                record["last_ineligible_reason"] = "BOOTSTRAP_EXCLUDED"
                continue
            candidate, reason = self._eligible(row, received)
            if reason:
                record["last_ineligible_reason"] = reason
                if reason == "POLICY_SOURCE_DRIFT":
                    self._emit("POLICY_SOURCE_DRIFT", _iso(received), {"event_key": identity, "scope_key": row.get("scope_key")})
                continue
            if record["first_eligible_seen_utc"] is None:
                record["first_eligible_seen_utc"] = _iso(received)
                record["first_eligible_poll_id"] = packet["poll_id"]
                record["first_eligible_marker_max_utc"] = _iso(candidate["max_marker"])
                record["first_eligible_source_closure_sha256"] = packet["source_closure_sha256"]
                record["first_eligible_conformance_evidence_sha256"] = row["conformance_evidence_sha256"]
                eligible.append(candidate)
        if bootstrap:
            state["bootstrap_complete"] = True
            self._emit("BOOTSTRAP_EXCLUDED", _iso(received), {"events": len(state["bootstrap_event_keys"]), "parents": len(state["bootstrap_parent_ids"])})
            return
        # All newly eligible rows in this response share actual receipt availability.
        # Historical marker maximum is a frozen deterministic within-response tie.
        eligible.sort(key=lambda c: (c["max_marker"], c["event_id"], c["symbol"]))
        for candidate in eligible:
            for name in self.books:
                if name == "DOGE_only" and candidate["symbol"] != "DOGE":
                    continue
                book = state["books"][name]
                parent = candidate["parent_id"]
                if parent in book["choices"]:
                    continue
                planned = received.replace(second=0, microsecond=0) + timedelta(minutes=1)
                choice = {"case_id": f"{candidate['symbol']}liquidityforward-{candidate['event_id']}",
                          "event_id": candidate["event_id"], "symbol": candidate["symbol"],
                          "parent_id": parent, "direction": candidate["scope"]["direction"],
                          "window_minutes": candidate["scope"]["window_minutes"],
                          "source_marker_max_utc": _iso(candidate["max_marker"]),
                          "selected_at_utc": _iso(received), "planned_entry_utc": _iso(planned),
                          "entry_expiry_utc": _iso(planned + timedelta(seconds=float(self.policy["entry_expiry_seconds"]))),
                          "selection_sequence": state["observation_sequence"], "status": "PLANNED"}
                choice.update(candidate["source_metadata"])
                choice["eligible_receipt_id"] = packet["poll_id"]
                choice["source_closure_sha256"] = packet["source_closure_sha256"]
                book["choices"][parent] = choice
                self._emit("PARENT_CHOICE_FROZEN", _iso(received), {"book": name, **choice})

    def _quotes(self, state, packet, started, received):
        if not state["bootstrap_complete"]:
            raise ValueError("Bootstrap required before quote processing")
        self._advance(state, received)
        elapsed = (received - started).total_seconds()
        valid = {}
        monotonic = packet.get("monotonic_elapsed_seconds")
        monotonic_bad = False
        if monotonic is not None:
            try:
                monotonic_value = _decimal(monotonic)
                monotonic_bad = monotonic_value <= 0 or monotonic_value > _decimal(self.policy["quote_max_elapsed_seconds"])
            except ValueError:
                monotonic_bad = True
        if packet.get("source") != self.policy.get("quote_source", "BINANCE_SPOT_BOOK_TICKER"):
            self._emit("QUOTE_REJECTED", _iso(received), {"reason": "QUOTE_SOURCE_IDENTITY"})
        elif monotonic_bad:
            self._emit("QUOTE_REJECTED", _iso(received), {"reason": "MONOTONIC_REQUEST_DURATION"})
        elif elapsed > float(self.policy["quote_max_elapsed_seconds"]):
            self._emit("QUOTE_REJECTED", _iso(received), {"reason": "REQUEST_TOO_SLOW", "elapsed_seconds": elapsed})
        else:
            seen = set()
            for quote in packet["quotes"]:
                symbol = quote.get("symbol")
                try:
                    if symbol != "DOGE" or symbol in seen:
                        raise ValueError("Unknown or duplicate quote symbol")
                    seen.add(symbol)
                    bid, ask = _decimal(quote["bid"]), _decimal(quote["ask"])
                    if bid <= 0 or ask < bid:
                        raise ValueError("Invalid/crossed bid ask")
                    last = state["last_quotes"].get(symbol)
                    if last and started < _time(last["request_started_utc"]):
                        raise ValueError("Quote request order reversed")
                    valid[symbol] = {"bid": str(bid), "ask": str(ask), "midpoint": str((bid + ask) / 2),
                                     "request_started_utc": _iso(started), "response_received_utc": _iso(received),
                                     "source": packet.get("source")}
                except (ValueError, KeyError, TypeError) as error:
                    valid.pop(symbol, None)
                    self._emit("QUOTE_REJECTED", _iso(received), {"symbol": symbol, "reason": str(error)})
        # Existing open positions close first at equal time. Earlier planned
        # gates were already resolved, so a later exit cannot free an old slot.
        for name, book in state["books"].items():
            position = book["active"]
            if position and position["symbol"] in valid:
                quote = valid[position["symbol"]]
                if started >= _time(position["entry_received_utc"]):
                    position["last_quote_received_utc"] = _iso(received)
                    deadline = _time(position["deadline_utc"])
                    status = None
                    mid = _decimal(quote["midpoint"])
                    sign = Decimal(1) if position["direction"] == "LONG" else Decimal(-1)
                    gross = sign * (mid - _decimal(position["entry_midpoint"])) / _decimal(position["entry_midpoint"]) * 100
                    if started >= deadline:
                        status = "OBSERVED_TIMEOUT"
                    elif received >= deadline:
                        self._emit("WAIT_POST_DEADLINE_QUOTE", _iso(received), {"book": name, "case_id": position["case_id"]})
                    elif gross >= self.width * 100:
                        status = "OBSERVED_TARGET"
                    elif gross <= -self.width * 100:
                        status = "OBSERVED_STOP"
                    if status:
                        position.update(status=status, exit_received_utc=_iso(received), exit_quote=quote,
                                        exit_midpoint=str(mid), gross_return_pct=str(gross),
                                        modeled_cost_pp=str(self.cost), net_return_pct=str(gross - self.cost))
                        book["closed"].append(position)
                        book["active"] = None
                        book["choices"][position["parent_id"]]["status"] = status
                        self._emit("PAPER_EXIT", _iso(received), {"book": name, **position})
        self._advance(state, received, inclusive=True)
        for name, book in state["books"].items():
            parent = book["reservation"]
            if parent:
                choice = book["choices"][parent]
                quote = valid.get(choice["symbol"])
                if quote and started >= _time(choice["planned_entry_utc"]) and received <= _time(choice["entry_expiry_utc"]):
                    choice["status"] = "OPEN"
                    book["active"] = {**choice, "entry_received_utc": _iso(received), "entry_quote": quote,
                                      "entry_midpoint": quote["midpoint"],
                                      "deadline_utc": _iso(received + timedelta(minutes=choice["window_minutes"])),
                                      "last_quote_received_utc": _iso(received),
                                      "path_uncertain": False, "uncertainty_reasons": []}
                    book["reservation"] = None
                    self._emit("PAPER_ENTRY", _iso(received), {"book": name, **book["active"]})
        state["last_quotes"].update(valid)

    def summary(self):
        state = self._load()
        output = {"policy_sha256": self.policy_sha256, "study_start_utc": self.policy["study_start_utc"],
                  "bootstrap_complete": state["bootstrap_complete"], "last_response_utc": state["last_response_utc"],
                  "last_observation_rejected": state["last_observation_rejected"],
                  "raw_first_seen_count": len(state["first_seen"]),
                  "first_eligible_seen_count": sum(r["first_eligible_seen_utc"] is not None for r in state["first_seen"].values()),
                  "journal_events": self.db.execute("SELECT COUNT(*) FROM journal").fetchone()[0],
                  "protocol_sha256": PROTOCOL_SHA256,
                  "study_id": STUDY_ID,
                  "source_runtime_commits": sorted(state.get("source_code_registries", {})),
                  "source_code_registry_count": len(state.get("source_code_registries", {})),
                  "source_receipt_count": self.db.execute("SELECT COUNT(*) FROM journal WHERE input_key LIKE 'CANDIDATES:%'").fetchone()[0],
                  "source_closure_count": self.db.execute("SELECT COUNT(*) FROM journal WHERE input_key LIKE 'SOURCE_CLOSURE:%'").fetchone()[0],
                  "raw_source_count": len(state["first_seen"]),
                  "pending_source_count": state.get("pending_source_count", 0),
                  "last_source_closure_sha256": state.get("last_source_closure_sha256"),
                  "warmup_until_utc": _iso(self.start + timedelta(minutes=30)),
                  "paper_model": "Observed future quote midpoint, fixed modeled all-in0.10pp; not executable fills; all completed trades reported including uncertain paths.",
                  "uncertainty_flag_scope": "path_uncertain is quote-path uncertainty; source_history_uncertain is retained separately and never hides completed cases.",
                  "books": {}}
        for name, book in state["books"].items():
            def totals(rows):
                net = sum((_decimal(r["net_return_pct"]) for r in rows), Decimal(0))
                return {"n": len(rows), "counts": dict(Counter(r["status"] for r in rows)),
                        "mean_net_return_pct": float(net / len(rows)) if rows else None,
                        "net_sum_unit_notional_pct": float(net)}
            output["books"][name] = {
                "choices": copy.deepcopy(book["choices"]), "active": copy.deepcopy(book["active"]),
                "reservation": book["reservation"], "closed": copy.deepcopy(book["closed"]),
                "choice_status_counts": dict(Counter(c["status"] for c in book["choices"].values())),
                "completed_all": totals(book["closed"]),
                "completed_path_uncertain": totals([r for r in book["closed"] if r["path_uncertain"]]),
                "completed_without_uncertainty_flag": totals([r for r in book["closed"] if not r["path_uncertain"]]),
                "completed_source_history_uncertain": totals([r for r in book["closed"] if r.get("source_history_uncertain")]),
                "completed_without_source_or_quote_uncertainty": totals([r for r in book["closed"] if not r["path_uncertain"] and not r.get("source_history_uncertain")]),
            }
        return output

    def eligible_event_keys(self):
        """Only durably eligible events may be omitted by the transport.

        Incomplete/ineligible observations must remain queryable on later
        polls; this is deliberately neither a max-ID nor a parent watermark.
        """
        state = self._load()
        return sorted(key for key, value in state["first_seen"].items()
                      if value["first_eligible_seen_utc"] is not None)

    def close(self):
        self.db.close()
