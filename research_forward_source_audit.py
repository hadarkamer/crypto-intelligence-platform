"""Independent replay of recorded candidate eligibility and immutable choices.

This replays the nine recorded prerequisite gates. It does not recompute
formula numeric predicates, upstream capture, capacity/execution, or every
empty input/quote/restart. Global input ordinals and export counts are checked
metadata from the same bounded database snapshot, not a full journal hash chain.
"""
from __future__ import annotations
import copy
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta, timezone

SOURCE_AUDIT_VERSION = "forward-source-audit-v1"
SOURCE_DECISION_KINDS = {"PARENT_CHOICE_FROZEN", "BOOTSTRAP_EXCLUDED", "POLICY_SOURCE_DRIFT", "INPUT_REJECTED"}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _obj(value):
    return json.loads(value) if isinstance(value, str) else value


def _time(value):
    if not isinstance(value, str):
        raise ValueError("Explicit UTC string required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Explicit UTC required")
    return result.astimezone(timezone.utc)


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _eligible(row, received, scopes, markers, start):
    scope = scopes.get(row.get("scope_key"))
    if scope is None:
        return None, "SCOPE_UNKNOWN"
    try:
        if scope.get("candidate_definition_sha256") is not None and row.get("candidate_definition_sha256") != scope["candidate_definition_sha256"]:
            return None, "POLICY_SOURCE_DRIFT"
        if scope.get("formula_version") is not None and row.get("candidate_formula_version") != scope["formula_version"]:
            return None, "POLICY_SOURCE_DRIFT"
        if row.get("symbol") != scope["symbol"] or row.get("formula_direction") != scope["direction"]:
            return None, "SCOPE_IDENTITY"
        if row.get("candidate_key") != scope["candidate_key"]:
            return None, "CANDIDATE_KEY"
        event = row.get("event_id")
        if not isinstance(event, int) or isinstance(event, bool) or row.get("source_event_id") != event:
            return None, "SOURCE_ID"
        if row.get("event_kind") != "ALERT" or row.get("delivery_status") != "DELIVERED":
            return None, "NOT_DELIVERED_ALERT"
        if row.get("membership_status") != "LIVE" or not row.get("parent_evidence_eligible"):
            return None, "PARENT_MEMBERSHIP"
        if not row.get("btc_parent_movement_id"):
            return None, "PARENT_ID_MISSING"
        parent_start = _time(row["parent_start_time_utc"])
        if parent_start <= start:
            return None, "OLD_PARENT"
        clocks = {key: _time(row[key]) for key in markers}
        if not parent_start <= clocks["parent_confirmed_at_utc"] <= clocks["alert_time_utc"]:
            return None, "SOURCE_CAUSALITY"
        if clocks["alert_time_utc"] < start:
            return None, "OLD_SOURCE"
        if max(clocks.values()) > received:
            return None, "FUTURE_SOURCE_MARKER"
        if clocks["alert_time_utc"] != _time(row["match_source_time_utc"]):
            return None, "MATCH_SOURCE_TIME"
        age = (clocks["alert_time_utc"] - clocks["btc_observed_close_utc"]).total_seconds()
        if not 0 <= age < 60 or clocks["parent_confirmed_at_utc"] > clocks["alert_time_utc"]:
            return None, "SOURCE_CAUSALITY"
        return {"scope": scope, "max_marker": max(clocks.values()), "event_id": event,
                "symbol": scope["symbol"], "parent_id": row["btc_parent_movement_id"]}, None
    except (ValueError, KeyError, TypeError):
        return None, "MISSING_OR_INVALID_TIMESTAMP"


def audit_source(snapshot, evidence):
    """Fail closed on missing/tampered/inconsistent bounded source evidence."""
    result = {"version": SOURCE_AUDIT_VERSION, "status": "PENDING_SOURCE_EVIDENCE", "checks": [],
              "failures": [], "summary": {}, "limitations": [
                  "Formula numeric predicates and upstream capture completeness are not recomputed.",
                  "Capacity, quote paths and restart history are outside this source replay.",
                  "Empty-input/quote counts and global ordinals are supplied bounded SQL metadata.",
                  "This is not a replay or hash-chain verification of the complete journal."]}
    if evidence is None:
        return result
    checks, failures = result["checks"], result["failures"]
    def check(code, okay):
        checks.append({"code": code, "passed": bool(okay)})
        if not okay:
            failures.append({"code": code})
        return bool(okay)
    try:
        policy, state = _obj(snapshot["policy_json"]), _obj(snapshot["state_json"])
        cut = snapshot["journal_seq"]
        check("SOURCE_CUTOFF", type(cut) is int and evidence["source_journal_seq"] == cut)
        check("SOURCE_EXPORT_NOT_TRUNCATED", evidence.get("selection_complete", True) is True)
        check("SOURCE_POLICY_DIGEST", _hash(policy) == snapshot["policy_sha256"])
        scopes = {x["scope_key"]: x for x in policy["scopes"]}
        markers = tuple(policy["required_markers"])
        check("NINE_DISTINCT_REQUIRED_MARKERS", len(markers) == len(set(markers)) == 9)
        start = _time(policy["study_start_utc"])
        packets = evidence.get("packets", evidence.get("candidate_packets"))
        decisions = evidence["decisions"]
        if not isinstance(packets, list) or not isinstance(decisions, list):
            raise ValueError("Source arrays required")
        counts = evidence["counts"]
        check("JOURNAL_PREFIX_COUNTS", counts["total_journal_rows"] == counts["max_journal_seq"] == cut and counts["min_journal_seq"] == 1)
        check("GLOBAL_INPUT_COUNTS", counts["total_input_packets"] == counts["candidate_packets_total"] + counts["quote_packets_total"])
        check("CANDIDATE_EMPTY_NONEMPTY_COUNTS", counts["candidate_empty_packets_total"] + counts["candidate_nonempty_packets_total"] == counts["candidate_packets_total"])
        check("EXPORTED_SOURCE_PACKET_COUNT", len(packets) == counts["candidate_selected_packets_total"])
        check("EXPORTED_SOURCE_DECISION_COUNT", len(decisions) == counts["source_decision_rows_total"])
        all_seqs, all_inputs = set(), set()
        for row in packets + decisions:
            seq = row["seq"]
            check("SOURCE_ROW_SEQUENCE", type(seq) is int and 1 <= seq <= cut and seq not in all_seqs)
            all_seqs.add(seq)
            check("SOURCE_PAYLOAD_DIGEST", _hash(_obj(row["payload_json"])) == row["payload_sha256"])
        packets = sorted(packets, key=lambda r: r["seq"])
        decisions = sorted(decisions, key=lambda r: r["seq"])
        check("SOURCE_DECISION_KINDS", all(d["kind"] in SOURCE_DECISION_KINDS and d.get("input_key") is None for d in decisions))
        rejected = [d for d in decisions if d["kind"] == "INPUT_REJECTED"]
        rejected_keys = [_obj(d["payload_json"])["input_key"] for d in rejected]
        check("REJECTED_INPUT_COUNT", len(rejected) == counts["rejected_input_packets"])
        check("REJECTED_IDENTITIES_UNIQUE", len(set(rejected_keys)) == len(rejected_keys) and all(isinstance(k, str) and (k.startswith("CANDIDATES:") or k.startswith("QUOTES:")) for k in rejected_keys))
        check("SUCCESSFUL_OBSERVATION_COUNT", state["observation_sequence"] == counts["total_input_packets"] - len(rejected))
        seen, bootstrap_keys, bootstrap_parents = {}, [], []
        chosen = {name: {} for name in policy["books"]}
        expected_decisions = []
        reasons = Counter()
        bootstrap_complete = False
        accepted_bootstraps, raw_bootstraps, nonempty, row_count = 0, 0, 0, 0
        previous_seq, previous_ordinal, previous_received = 0, 0, None
        def emit(kind, received, payload, packet_seq):
            expected_decisions.append({"kind": kind, "utc": _iso(received), "payload_json": payload, "packet_seq": packet_seq})
        for packet_index, journal in enumerate(packets):
            p = _obj(journal["payload_json"])
            check("SOURCE_PACKET_KIND", journal["kind"] == "INPUT_CANDIDATES")
            identifier = p.get("poll_id")
            identifier = str(identifier) if identifier is not None else _hash(p)
            identity = "CANDIDATES:" + identifier
            check("SOURCE_INPUT_IDENTITY", journal.get("input_key") == identity and identity not in all_inputs)
            all_inputs.add(identity)
            rows = p["rows"]
            if not isinstance(rows, list):
                raise ValueError("Candidate row array required")
            bootstrap = bool(p.get("bootstrap"))
            raw_bootstraps += int(bootstrap)
            nonempty += int(bool(rows))
            row_count += len(rows)
            check("SOURCE_EXPORT_SELECTION_RULE", bootstrap or bool(rows))
            ordinal, rejected_before = journal["input_ordinal"], journal["rejected_input_count_before"]
            prefix_rejected = sum(d["seq"] < journal["seq"] for d in rejected)
            check("RAW_INPUT_ORDINAL", type(ordinal) is int and previous_ordinal < ordinal <= counts["total_input_packets"] and ordinal - previous_ordinal <= journal["seq"] - previous_seq)
            check("REJECTED_PREFIX_ORDINAL", type(rejected_before) is int and rejected_before == prefix_rejected)
            observation = ordinal - prefix_rejected
            check("SUCCESSFUL_INPUT_ORDINAL", journal["observation_sequence"] == observation)
            previous_seq, previous_ordinal = journal["seq"], ordinal
            next_input = journal["next_input_seq"]
            check("NEXT_INPUT_BOUND", (type(next_input) is int and journal["seq"] < next_input <= cut) or (next_input is None and ordinal == counts["total_input_packets"]))
            if identity in rejected_keys:
                check("REJECTED_SOURCE_DECISION_AFTER_INPUT", any(d["seq"] > journal["seq"] and _obj(d["payload_json"])["input_key"] == identity for d in rejected))
                continue
            started, received = _time(p["request_started_utc"]), _time(p["response_received_utc"])
            check("SOURCE_PACKET_RECEIPT", _time(journal["utc"]) == received)
            check("SOURCE_ACCEPTED_CHRONOLOGY", start <= received and started <= received and (previous_received is None or previous_received <= received))
            previous_received = received
            check("SOURCE_BOOTSTRAP_ORDER", bootstrap != bootstrap_complete)
            if bootstrap != (not bootstrap_complete):
                raise ValueError("Rejected bootstrap transition is not recorded")
            newly_ready = []
            decision_start = len(expected_decisions)
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("Rejected candidate row is not recorded")
                key = f"{row.get('scope_key')}:{row.get('event_id')}"
                parent = row.get("btc_parent_movement_id")
                record = seen.setdefault(key, {"raw_first_seen_utc": _iso(received), "raw_first_poll_id": p["poll_id"], "first_eligible_seen_utc": None})
                if bootstrap:
                    if key not in bootstrap_keys:
                        bootstrap_keys.append(key)
                    if parent and parent not in bootstrap_parents:
                        bootstrap_parents.append(parent)
                    continue
                if key in bootstrap_keys or parent in bootstrap_parents:
                    record["last_ineligible_reason"] = "BOOTSTRAP_EXCLUDED"
                    reasons["BOOTSTRAP_EXCLUDED"] += 1
                    continue
                candidate, reason = _eligible(row, received, scopes, markers, start)
                if reason:
                    record["last_ineligible_reason"] = reason
                    reasons[reason] += 1
                    if reason == "POLICY_SOURCE_DRIFT":
                        emit("POLICY_SOURCE_DRIFT", received, {"event_key": key, "scope_key": row.get("scope_key")}, journal["seq"])
                    continue
                if record["first_eligible_seen_utc"] is None:
                    record.update(first_eligible_seen_utc=_iso(received), first_eligible_poll_id=p["poll_id"], first_eligible_marker_max_utc=_iso(candidate["max_marker"]))
                    newly_ready.append(candidate)
            if bootstrap:
                bootstrap_complete = True
                accepted_bootstraps += 1
                emit("BOOTSTRAP_EXCLUDED", received, {"events": len(bootstrap_keys), "parents": len(bootstrap_parents)}, journal["seq"])
                upper = journal["next_input_seq"] if journal["next_input_seq"] is not None else cut + 1
                for generated in expected_decisions[decision_start:]:generated["before_seq"] = upper
                continue
            for candidate in sorted(newly_ready, key=lambda c: (c["max_marker"], c["event_id"], c["symbol"])):
                for name in policy["books"]:
                    if name == "DOGE_only" and candidate["symbol"] != "DOGE" or name == "ETH_only" and candidate["symbol"] != "ETH":
                        continue
                    parent = candidate["parent_id"]
                    if parent in chosen[name]:
                        continue
                    planned = received.replace(second=0, microsecond=0) + timedelta(minutes=1)
                    choice = {"case_id": f"{candidate['symbol']}forward-{candidate['event_id']}", "event_id": candidate["event_id"], "symbol": candidate["symbol"], "parent_id": parent,
                              "direction": candidate["scope"]["direction"], "window_minutes": candidate["scope"]["window_minutes"],
                              "source_marker_max_utc": _iso(candidate["max_marker"]), "selected_at_utc": _iso(received), "planned_entry_utc": _iso(planned),
                              "entry_expiry_utc": _iso(planned + timedelta(seconds=float(policy["entry_expiry_seconds"]))), "selection_sequence": observation, "status": "PLANNED"}
                    chosen[name][parent] = choice
                    emit("PARENT_CHOICE_FROZEN", received, {"book": name, **choice}, journal["seq"])
            upper = journal["next_input_seq"] if journal["next_input_seq"] is not None else cut + 1
            for generated in expected_decisions[decision_start:]:generated["before_seq"] = upper
        check("RAW_BOOTSTRAP_EXPORT_COUNT", raw_bootstraps == counts["candidate_bootstrap_packets_total"])
        check("NONEMPTY_EXPORT_COUNT", nonempty == counts["candidate_nonempty_packets_total"])
        if "candidate_packet_rows" in counts:
            check("SOURCE_ROW_OBSERVATION_COUNT", row_count == counts["candidate_packet_rows"])
        if "invalid_rows" in counts:
            check("NO_INVALID_SOURCE_ROW_ENVELOPES", counts["invalid_rows"] == 0)
        if "candidate_invalid_rows_packets_total" in counts:
            check("NO_INVALID_CANDIDATE_ARRAY_PACKETS", counts["candidate_invalid_rows_packets_total"] == 0)
        if "max_candidate_selected_seq" in counts:
            check("SOURCE_SELECTED_MAXIMUM", counts["max_candidate_selected_seq"] == max((p["seq"] for p in packets), default=None))
        if "max_source_decision_seq" in counts:
            check("SOURCE_DECISION_MAXIMUM", counts["max_source_decision_seq"] == max((d["seq"] for d in decisions), default=None))
        check("ACCEPTED_BOOTSTRAP_ONCE", accepted_bootstraps == int(bool(state["bootstrap_complete"])))
        check("BOOTSTRAP_COMPLETE_PROJECTION", bootstrap_complete == state["bootstrap_complete"])
        check("BOOTSTRAP_EVENT_PROJECTION", bootstrap_keys == state["bootstrap_event_keys"])
        check("BOOTSTRAP_PARENT_PROJECTION", bootstrap_parents == state["bootstrap_parent_ids"])
        check("FULL_RAW_FIRST_SEEN_PROJECTION", seen == state["first_seen"])
        for name, choices in chosen.items():
            actual = state["books"][name]["choices"]
            check("CHOICE_PARENT_SET_" + name, set(choices) == set(actual))
            for parent, choice in choices.items():
                check("IMMUTABLE_SOURCE_CHOICE_" + name, parent in actual and all(actual[parent].get(k) == v for k, v in choice.items() if k != "status"))
        actual_source_decisions = [d for d in decisions if d["kind"] != "INPUT_REJECTED"]
        check("SOURCE_GENERATED_DECISION_COUNT", len(actual_source_decisions) == len(expected_decisions))
        for expected, actual in zip(expected_decisions, actual_source_decisions):
            check("SOURCE_GENERATED_DECISION", actual["kind"] == expected["kind"] and _time(actual["utc"]) == _time(expected["utc"]) and _obj(actual["payload_json"]) == expected["payload_json"] and expected["packet_seq"] < actual["seq"] < expected["before_seq"])
        eligible = [v for v in seen.values() if v.get("first_eligible_seen_utc") is not None]
        delayed = [(_time(v["first_eligible_seen_utc"]) - _time(v["raw_first_seen_utc"])).total_seconds() for v in eligible if _time(v["first_eligible_seen_utc"]) > _time(v["raw_first_seen_utc"])]
        result["summary"] = {"candidate_packets_selected": len(packets), "candidate_row_observations": row_count,
                             "raw_distinct_events": len(seen), "eligible_distinct_events": len(eligible),
                             "bootstrap_event_count": len(bootstrap_keys), "bootstrap_parent_count": len(bootstrap_parents),
                             "delayed_eligible_events": len(delayed), "max_readiness_wait_seconds": max(delayed) if delayed else 0,
                             "frozen_choice_count": sum(len(v) for v in chosen.values()), "ineligible_observation_reason_counts": dict(reasons),
                             "recorded_rejected_inputs": len(rejected), "source_journal_seq": cut,
                             "rejection_review_required": bool(rejected),
                             "first_seen_projection_sha256": _hash(seen), "source_choice_projection_sha256": _hash(chosen)}
    except (ValueError, KeyError, TypeError, ArithmeticError, AttributeError):
        check("MALFORMED_OR_INCOMPLETE_SOURCE_EVIDENCE", False)
    result["status"] = "FAIL" if failures else "PENDING_SOURCE_EVIDENCE" if result["summary"].get("rejection_review_required") else "PASS"
    _json(result)
    return result
