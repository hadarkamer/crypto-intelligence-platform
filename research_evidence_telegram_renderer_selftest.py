"""Deterministic regressions for the pure EvidenceSnapshot Telegram renderer."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import research_evidence_contract as contract
import research_evidence_telegram_renderer as renderer


ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures" / "evidence"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _snapshot(fixture: dict) -> contract.EvidenceSnapshot:
    assessment = contract.FormulaAssessment.from_acceptance(
        fixture["assessment"], phase=fixture["phase"]
    )
    return contract.EvidenceSnapshot.build(
        formula_contract=fixture["formula_contract"],
        assessment=assessment,
        assessed_at_utc=fixture["assessed_at_utc"],
        formula_family_id=fixture.get("formula_family_id"),
        matched_market_episode_ids=fixture["matched_market_episode_ids"],
        control_market_episode_ids=fixture["control_market_episode_ids"],
        matched_parent_market_episode_ids=(
            fixture["matched_parent_market_episode_ids"]
        ),
        control_parent_market_episode_ids=(
            fixture["control_parent_market_episode_ids"]
        ),
        raw_match_count=fixture["raw_match_count"],
        raw_control_count=fixture["raw_control_count"],
        matched_n_eff=fixture["matched_n_eff"],
        control_n_eff=fixture["control_n_eff"],
        metrics=fixture["metrics"],
        evidence=fixture["evidence"],
        provenance=fixture["provenance"],
    )


def _raises(expected: str, function) -> None:
    try:
        function()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def run() -> None:
    current_fixture = _fixture("current_v7_probability.json")
    current = _snapshot(current_fixture)
    current_text = renderer.render_evidence_snapshot(current)
    assert current_text == renderer.render_evidence_snapshot(current.to_dict())
    assert renderer.EXPERIMENTAL_LABEL in current_text
    assert "חוזה: V7 נוכחי" in current_text
    assert "מטבע: רב־מטבעי" in current_text
    assert "כיוון: לונג (LONG)" in current_text
    assert "אופק: 4 שעות" in current_text
    assert "מסלול קבלה: הסתברות" in current_text
    assert "שיעור הצלחה משוקלל לעדכניות: 72.5%" in current_text
    assert "MFE חציוני: 1.8%" in current_text
    assert "MAE חציוני: 0.7%" in current_text
    assert "Market Episodes: התאמות 2 (הורים 1; raw 7)" in current_text
    assert "ביקורת 2 (הורים 2; raw 9)" in current_text
    assert "N_eff: התאמות 1 | ביקורת 2" in current_text
    assert "MAE p90: 2.4%" in current_text
    assert "MAE p95: 3.1%" in current_text
    assert "DRY RUN בלבד" in current_text

    retained_v7_1 = _snapshot(_fixture("retained_v7_1_probability.json"))
    retained_text = renderer.render_evidence_snapshot(retained_v7_1)
    assert "חוזה: V7.1 שמור — קריאה בלבד" in retained_text
    retained_dry_run = renderer.dry_run_evidence_snapshots([retained_v7_1])
    assert retained_dry_run["messages"][0]["compatibility"] == (
        contract.RETAINED_V7_1_READ_ONLY
    )

    legacy = _snapshot(_fixture("legacy_v6_shadow.json"))
    legacy_text = renderer.render_evidence_snapshot(legacy)
    assert "חוזה: Legacy Shadow — קריאה בלבד" in legacy_text
    assert "כיוון: שורט (SHORT)" in legacy_text
    assert "אופק: 12 שעות" in legacy_text
    assert "מסלול קבלה: אין — מחקר בלבד" in legacy_text
    assert "הסתברות: לא נמסרה ב־snapshot" in legacy_text
    assert "סיכון: לא נמסר ב־snapshot" in legacy_text

    newer_fixture = deepcopy(current_fixture)
    newer_fixture["formula_contract"]["formula_key"] = "3" * 64
    newer_fixture["assessed_at_utc"] = "2026-08-30T22:00:00Z"
    newer_fixture["metrics"]["recency_weighted_hit_rate_pct"] = 73.0
    newer_fixture["evidence"]["symbol"] = "BTC"
    newer = _snapshot(newer_fixture)
    dry_run = renderer.dry_run_evidence_snapshots(
        [current, current.to_dict(), newer]
    )
    assert dry_run["mode"] == "DRY_RUN"
    assert dry_run["input_snapshots"] == 3
    assert dry_run["verified_unique_snapshots"] == 2
    assert dry_run["duplicates_suppressed"] == 1
    assert dry_run["families_rendered"] == 1
    assert dry_run["delivery_attempts"] == 0
    assert dry_run["delivery_channel"] == "NONE"
    assert dry_run["live_effect"] == "NONE"
    message = dry_run["messages"][0]
    assert message["representative_snapshot_id"] == newer.snapshot_id
    assert message["aggregated_snapshot_count"] == 2
    assert message["aggregated_formula_count"] == 2
    assert len(message["snapshot_ids"]) == 2
    assert len(message["formula_keys"]) == 2
    assert "מטבע: BTC" in message["text"]
    assert "שיעור הצלחה משוקלל לעדכניות: 73%" in message["text"]
    assert "איגוד משפחה: 2 Snapshots מאומתים | 2 נוסחאות" in message["text"]

    reordered = renderer.dry_run_evidence_snapshots(
        [newer.to_dict(), current, current]
    )
    assert reordered == dry_run

    empty = renderer.dry_run_evidence_snapshots([])
    assert empty["families_rendered"] == 0
    assert empty["messages"] == []

    conflicting_fixture = deepcopy(current_fixture)
    conflicting_fixture["formula_contract"]["formula_key"] = "4" * 64
    conflicting_fixture["formula_contract"]["direction"] = "SHORT"
    conflicting = _snapshot(conflicting_fixture)
    _raises(
        "family contains incompatible",
        lambda: renderer.dry_run_evidence_snapshots([current, conflicting]),
    )

    delivery_tamper = current.to_dict()
    delivery_tamper["live_eligible"] = True
    _raises(
        "may not authorize delivery",
        lambda: renderer.render_evidence_snapshot(delivery_tamper),
    )
    _raises(
        "iterable of EvidenceSnapshots",
        lambda: renderer.dry_run_evidence_snapshots(current.to_dict()),
    )

    source = (ROOT / "research_evidence_telegram_renderer.py").read_text(
        encoding="utf-8"
    )
    for forbidden in (
        "from telegram",
        "import telegram",
        "send_message(",
        "reply_text(",
        "research_formula_worker",
        "research_formula_store",
        "research_formula_acceptance",
        "requests.",
        "aiohttp.",
    ):
        assert forbidden not in source

    print("research_evidence_telegram_renderer_selftest: ok")


if __name__ == "__main__":
    run()
