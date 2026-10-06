"""Deterministic research publication of a reverified prospective validation.

Publishing always invokes the existing validator over its complete evidence.
Rendering verifies the envelope and its exact projection, not database origin
or market truth. No publication authorizes a runtime, message, or trade.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import html
from pathlib import Path

import research_no_horizon_contract as contracts
import research_no_horizon_first_touch as first_touch
import research_no_horizon_ranking as ranking
import research_no_horizon_validation as validation

VERSION = "no-horizon-verified-research-publication-v1"
_AUTHORITY = {"runtime_authorized": False, "telegram_authorized": False, "trading_authorized": False}
_CLAIMS = {**validation._LIMITATIONS, "database_origin_authenticated_by_this_tool": False}
_require, _same = ranking._require, ranking._same


def implementation():
    return {"publisher_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "validation": validation.implementation()}


def _build(value, result):
    """Bind verified records and copy metrics; do not estimate or rerank."""
    plan, result = deepcopy(value), deepcopy(result)
    ranking._sealed(plan, "plan_sha256")
    ranking._sealed(result, "report_sha256")
    _require(plan["version"] == result["validation_version"] == validation.VERSION and
        _same(plan["implementation"], validation.implementation()) and
        _same(result["implementation"], plan["implementation"]), "PUBLICATION_IMPLEMENTATION_MISMATCH")
    for key in ("plan_sha256", "selection_report_sha256", "request_id", "declaration_sha256",
                "policy", "selected_scopes"):
        _require(_same(result[key], plan[key]), "PUBLICATION_PLAN_RESULT_BINDING_MISMATCH")
    ranking._false(plan, _AUTHORITY)
    ranking._false(result, (*_AUTHORITY, *_CLAIMS))
    _require(result["registration_timing_verified"] is True and
        _same(result["timing_evidence"], validation._registration(
            plan, result["timing_evidence"]["registration_info"])), "PUBLICATION_REGISTRATION_MISMATCH")
    complete, qualified = result["validation_complete"], result["qualified_scope_ids"]
    rows, original = result["rows"], result["original_verification"]
    _require(type(complete) is bool and len(rows) == len(plan["selected_scopes"]) and
        qualified == [row["scope_id"] for row in rows if row["prospective_gate_passed"]] and
        (complete or not qualified), "PUBLICATION_SCOPE_DENOMINATOR_MISMATCH")
    originals = original["rows"] if original is not None else [None] * len(rows)
    _require(len(originals) == len(rows), "PUBLICATION_ORIGINAL_SCOPE_DENOMINATOR_MISMATCH")
    summary = []
    for ordinal, (row, scope, before) in enumerate(zip(rows, plan["selected_scopes"], originals)):
        _require(type(row["ordinal"]) is int and row["ordinal"] == ordinal and
            all(_same(row[key], scope[key]) for key in ranking._SCOPE_KEYS) and
            row["candidate_version"] == scope["candidate"]["definition_sha256"],
            "PUBLICATION_SCOPE_IDENTITY_MISMATCH")
        ranking._false(row, _AUTHORITY)
        if before is not None:
            _require(before["ordinal"] == ordinal and
                all(_same(before[key], row[key]) for key in ranking._SCOPE_KEYS) and
                before["status"] == row["original_status"], "PUBLICATION_ORIGINAL_SCOPE_MISMATCH")
        fresh_gate = row["fresh_gate"]
        _require(type(row["prospective_gate_passed"]) is bool and
            row["prospective_gate_passed"] is (complete and bool(
                fresh_gate and fresh_gate["experimental_eligible"])), "PUBLICATION_QUALIFICATION_MISMATCH")
        summary.append({key: deepcopy(row[key]) for key in ("ordinal", *ranking._SCOPE_KEYS,
            "candidate_version", "original_status", "fresh_parent_count", "excluded_parent_count",
            "fresh_gate", "prospective_gate_passed", "exclusion_reasons")})
        summary[-1].update(original_gate=deepcopy(before["gate"]) if before else None,
            original_outcome_status_counts=deepcopy(before["outcome_status_counts"]) if before else None,
            parent_exclusion_reasons=sorted({reason for parent in row["excluded_parents"]
                                            for reason in parent["reasons"]}),
            global_qualified=row["scope_id"] in qualified)
    _require(_same(result["denominator"], {"declared_scopes": len(rows),
        "processed_scopes": sum(row["original_status"] in ranking._TERMINAL for row in rows),
        "qualified_scopes": len(qualified)}), "PUBLICATION_COMPLETION_DENOMINATOR_MISMATCH")
    acquired = result["acquisition_evidence"]
    publication = {"publication_version": VERSION, "implementation": implementation(),
        "validation_plan": plan, "validation_result": result,
        "source_hashes": {"validation_plan_sha256": plan["plan_sha256"],
            "selection_plan_sha256": plan["selection_plan_sha256"],
            "selection_report_sha256": plan["selection_report_sha256"],
            "declaration_sha256": plan["declaration_sha256"],
            "validation_result_sha256": result["report_sha256"],
            "acquisition_report_sha256": acquired["report_sha256"],
            "executor_report_sha256": original["report_sha256"] if original else None,
            "executor_plan_id": acquired["executor_plan_id"], "anchor_sha256": acquired["anchor_sha256"]},
        "scope_summary": summary,
        "state": "INCOMPLETE" if not complete else (
            "COMPLETE_QUALIFIED" if qualified else "COMPLETE_NO_QUALIFICATION"),
        "limitations": deepcopy(result["limitations"]), **_CLAIMS, **_AUTHORITY}
    return {**publication, "publication_sha256": contracts.digest(publication)}


def publish_reports(value, *, selection_plan, selection_reports, registration_info,
                    acquisition_report, executor_report=None):
    """Publish complete supplied evidence after existing structural verification."""
    result = validation.evaluate_reports(value, selection_plan=selection_plan,
        selection_reports=selection_reports, registration_info=registration_info,
        acquisition_report=acquisition_report, executor_report=executor_report)
    return _build(value, result)


def publish_plan(acquisition_backend, executor_backend, value, *, selection_plan, selection_reports):
    """Publish the validator's derived request and executor chain from stores."""
    result = validation.evaluate_plan(acquisition_backend, executor_backend, value,
        selection_plan=selection_plan, selection_reports=selection_reports)
    return _build(value, result)


def _cell(value):
    if value is None:
        return "unavailable"
    if type(value) is bool:
        return "yes" if value else "no"
    text = html.escape(str(value), quote=False).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return "".join("\\" + char if char in "\\`*_{}[]()#+-.!|" else char for char in text)


def _percentage(value):
    return "unavailable" if value is None else f"{value:.2f}%"


def _table(headers, rows):
    return ["| " + " | ".join(_cell(item) for item in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
        *["| " + " | ".join(_cell(item) for item in row) + " |" for row in rows]]


def _reasons(values):
    return "; ".join(values) if values else "none"


def render_markdown(publication):
    """Verify seals and exact copied projection; hashes do not authenticate origin."""
    try:
        ranking._sealed(publication, "publication_sha256")
        rebuilt = _build(publication["validation_plan"], publication["validation_result"])
        _require(_same(publication, rebuilt), "PUBLICATION_ENVELOPE_OR_PROJECTION_MISMATCH")
        result, plan = publication["validation_result"], publication["validation_plan"]
        declaration, denominator = plan["declaration"], result["denominator"]
        lines = ["# No-horizon prospective research report", "",
            "State: " + publication["state"], "",
            "Validation complete: " + _cell(result["validation_complete"]) + ". "
            + f"Declared scopes: {denominator['declared_scopes']}; processed scopes: "
            + f"{denominator['processed_scopes']}; qualified scopes: {denominator['qualified_scopes']}.", "",
            "Completion describes processing of every frozen scope, including blocked scopes. "
            "A gate pass is a descriptive research result. BTC parent grouping does not prove "
            "statistical independence; profitability is not validated; multiple testing is not adjusted.", "",
            "Runtime authorization: no. Telegram authorization: no. Trading authorization: no.", "",
            "## Registration and observation", ""]
        info = result["timing_evidence"]["registration_info"]
        lines.extend(_table(["Field", "Value"], [
            ["Symbol", declaration["symbol"]],
            ["Training maximum cutoff UTC", plan["training_max_cutoff_utc"]],
            ["Database registration UTC", info["created_at_utc"]],
            ["Validation source start UTC", declaration["source_start_utc"]],
            ["Validation source end UTC", declaration["source_end_utc"]],
            ["Validation outcome cutoff UTC", declaration["cutoff_utc"]],
            ["Registration timing verified", result["registration_timing_verified"]],
            ["Acquisition status", result["acquisition_evidence"]["status"]]]))
        lines.extend(["", "## Every frozen scope", ""])
        lines.extend(_table(["Ordinal", "Candidate", "Base / analysis", "Threshold %", "Original status",
            "Fresh / excluded parents", "Qualified"], [[row["ordinal"], row["candidate_key"],
                row["base_direction"] + " / " + row["analysis_direction"], row["threshold_pct"],
                row["original_status"], _cell(row["fresh_parent_count"]) + " / "
                + _cell(row["excluded_parent_count"]), row["global_qualified"]]
            for row in publication["scope_summary"]]))
        for row in publication["scope_summary"]:
            before, fresh = row["original_gate"], row["fresh_gate"]
            lines.extend(["", f"## Scope {row['ordinal']}", "",
                "Scope ID: " + _cell(row["scope_id"]), "",
                "Candidate definition SHA-256: " + _cell(row["candidate_version"]), ""])
            metrics = []
            for label, key in (("Selected parents", "selected_parents"), ("Resolved parents", "resolved_parents")):
                metrics.append([label, before[key] if before else None, fresh[key] if fresh else None])
            for status in first_touch.STATUSES:
                counts = row["original_outcome_status_counts"]
                metrics.append([status, counts[status] if counts is not None else None,
                    fresh["status_counts"][status] if fresh else None])
            for label, key in (("Hit rate", "hit_rate_pct"), ("Wilson 95% lower bound", "wilson_95_lower_pct")):
                metrics.append([label, _percentage(before["probability"][key]) if before else None,
                    _percentage(fresh["probability"][key]) if fresh else None])
            metrics.append(["Atomic gate passed", before["experimental_eligible"] if before else None,
                            fresh["experimental_eligible"] if fresh else None])
            lines.extend(_table(["Metric", "Original evidence", "Fresh evidence"], metrics))
            lines.extend(["", "Original gate blockers: " + _cell(_reasons(before["blockers"]))
                if before else "Original gate blockers: unavailable", "",
                "Fresh gate blockers: " + _cell(_reasons(fresh["blockers"]))
                if fresh else "Fresh gate blockers: unavailable", "",
                "Validation exclusion reasons: " + _cell(_reasons(row["exclusion_reasons"])), "",
                "Whole-parent exclusion reasons: " + _cell(_reasons(row["parent_exclusion_reasons"])), "",
                "Global qualification: " + _cell(row["global_qualified"]) + "."])
        lines.extend(["", "## Bound evidence", ""])
        lines.extend(_table(["Identity", "Value"], [["Publication SHA-256", publication["publication_sha256"]],
            ["Request ID", result["request_id"]], *[[key, publication["source_hashes"][key]]
                for key in sorted(publication["source_hashes"])]]))
        lines.extend(["", "## Evidence limitations", "",
            "The JSON publication retains the complete frozen plan, candidate definitions, "
            "validation records, parent identifiers, original blocked evidence, and limitations. "
            "This Markdown is an exact presentation projection, with percentage rounding only. "
            "Hashes bind content and do not prove database origin or external market provenance.", "",
            *["- " + _cell(reason) for reason in publication["limitations"]]])
        return "\n".join(lines) + "\n"
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError("PUBLICATION_MALFORMED_EVIDENCE") from exc
