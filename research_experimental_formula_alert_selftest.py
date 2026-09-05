"""Deterministic adversarial checks for pure experimental Stage-4 alerts."""

from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
from pathlib import Path

import research_experimental_formula_alert as alerts
import research_signal_formula_exploration as exploration
import research_stage4_candidate_search as search


UTC = timezone.utc
AS_OF = datetime(2026, 9, 5, 12, 5, tzinfo=UTC)
HORIZON = 60


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _features(*names: str) -> search._CompactFeatureMapping:
    mask = sum(
        1 << search._BOOLEAN_FEATURES.index(name)
        for name in names
    )
    return search._CompactFeatureMapping(mask, None)


def _observation(
    index: int,
    decision: datetime,
    *,
    symbol: str = "BTC",
    direction: str = "LONG",
    feature_names: tuple[str, ...],
    parent_seed: str | None = None,
    outcome_status: str = "AVAILABLE",
):
    if outcome_status == "AVAILABLE":
        outcome = search._CompactOutcome(
            "AVAILABLE",
            None,
            HORIZON,
            search._CompactOutcomePath(1.5, 2.0, 0.5),
        )
    else:
        outcome = search._CompactOutcome(
            "OUTCOME_UNAVAILABLE",
            "PENDING_HORIZON",
            HORIZON,
            None,
        )
    return search.CompactStage4CandidateObservation(
        observation_id=_hash(f"observation-{index}-{symbol}-{direction}"),
        projection_event_id=index,
        projection_decision_time_utc=decision.isoformat(),
        symbol=symbol,
        direction=direction,
        features=_features(*feature_names),
        wave_binding=search._CompactWaveBinding(
            "BOUND", None, _hash(parent_seed or f"wave-{index}")
        ),
        outcome=outcome,
    )


def _current_observation(
    index: int,
    decision: datetime,
    *,
    symbol: str = "BTC",
    direction: str = "LONG",
    feature_names: tuple[str, ...],
    parent_seed: str = "current-wave",
) -> search.CompactCurrentStage4Observation:
    source_ids = (10_000 + index,) if feature_names else ()
    source_fingerprints = (
        (_hash(f"current-source-{index}-{symbol}-{direction}"),)
        if feature_names
        else ()
    )
    cycle_minute = 30 if decision.minute >= 30 else 0
    cycle = decision.replace(
        minute=cycle_minute,
        second=0,
        microsecond=0,
    )
    return search.CompactCurrentStage4Observation(
        observation_id=_hash(f"current-observation-{index}-{symbol}-{direction}"),
        projection_event_id=index,
        projection_event_fingerprint=_hash(
            f"current-projection-{index}-{symbol}-{direction}"
        ),
        snapshot_set_id=20_000 + index,
        snapshot_key=_hash(f"current-snapshot-{index}"),
        projection_decision_time_utc=decision.isoformat(),
        archive_cycle_time_utc=cycle.isoformat(),
        symbol=symbol,
        direction=direction,
        features=_features(*feature_names),
        source_event_ids=source_ids,
        source_event_fingerprints=source_fingerprints,
        wave_binding=search._CompactWaveBinding(
            "BOUND", None, _hash(parent_seed)
        ),
    )


def _fixture():
    max_pain = exploration.FEATURE_MAX_PAIN_CONFIRMED
    magnet = exploration.FEATURE_MAGNET_CONFIRMED
    observations = []
    for index in range(1, 7):
        observations.append(
            _observation(
                index,
                AS_OF - timedelta(hours=4, minutes=-10 * index),
                feature_names=(max_pain, magnet),
            )
        )
    # Candidate evidence is frozen at AS_OF.  A separate current projection is
    # allowed to arrive later and contains no outcome member at all.
    current = _current_observation(
        100,
        AS_OF + timedelta(minutes=25),
        feature_names=(max_pain, magnet),
        parent_seed="current-wave",
    )
    result = search.search_experimental_candidates(
        observations,
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
        config=search.Stage4SearchConfig(wall_budget_ms=5_000),
    )
    assert len(result["eligible_candidates"]) == 1
    assert len(result["eligible_candidate_variants"]) >= 2
    envelope = alerts.compact_eligible_search_envelope(result)
    return observations, current, result, envelope


def _raises(expected: str, callback) -> None:
    try:
        callback()
    except ValueError as exc:
        assert expected in str(exc), exc
    else:
        raise AssertionError(f"expected ValueError containing {expected!r}")


def _check_happy_path_and_renderer() -> None:
    _, current, _, envelope = _fixture()
    built = alerts.build_experimental_alerts(
        [current],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )
    assert len(built) == 1
    alert = built[0]
    payload = alert.to_dict()
    source = envelope.to_dict()
    top_candidate = source["eligible_candidates"][0]
    assert payload["formula"]["candidate_key"] == top_candidate["candidate_key"]
    assert payload["selection"]["search_rank"] == 1
    assert payload["symbol"] == "BTC"
    assert payload["direction"] == "LONG"
    assert payload["horizon_minutes"] == HORIZON
    assert payload["btc_parent_movement_id"] == _hash("current-wave")
    assert payload["current_snapshot"]["status"] == "FROZEN_BOUND_FRESH"
    assert payload["current_snapshot"]["observation_id"] == current.observation_id
    assert payload["current_snapshot"]["projection_event_fingerprint"] == (
        current.projection_event_fingerprint
    )
    assert payload["current_snapshot"]["snapshot_set_id"] == current.snapshot_set_id
    assert payload["current_snapshot"]["snapshot_key"] == current.snapshot_key
    assert payload["current_snapshot"]["archive_cycle_time_utc"] == (
        current.archive_cycle_time_utc
    )
    assert payload["current_snapshot"]["btc_parent_movement_id"] == (
        current.wave_binding.btc_parent_movement_id
    )
    assert payload["current_snapshot"]["trigger_snapshot_sha256"] == (
        payload["current_snapshot"]["current_snapshot_sha256"]
    )
    assert len(payload["trigger_key"]) == 64
    assert len(payload["current_trigger_receipt_sha256"]) == 64
    assert payload["expires_at_utc"] == (
        AS_OF + timedelta(minutes=60)
    ).isoformat()
    assert all(
        result["passed"] is True
        for result in payload["current_snapshot"]["condition_results"]
    )
    def payload_keys(value):
        if isinstance(value, dict):
            for key, item in value.items():
                yield key
                yield from payload_keys(item)
        elif isinstance(value, list):
            for item in value:
                yield from payload_keys(item)

    assert not {"outcome", "path"}.intersection(payload_keys(payload))
    assert payload["authority"] == {
        "delivery_channel": "TELEGRAM_EXPERIMENTAL_ONLY",
        "formula_registry_effect": "NONE",
        "human_formula_approval_required": False,
        "live_eligible": False,
        "trade_execution_allowed": False,
        "telegram_delivery_allowed": True,
    }
    assert payload["disclaimer"] == "ניסיוני, לא מאושר למסחר"
    assert payload["current_trigger_policy_version"] == (
        alerts.CURRENT_TRIGGER_POLICY_VERSION
    )
    candidate_snapshot = payload["candidate_snapshot"]
    assert candidate_snapshot["candidate_key"] == payload["formula"][
        "candidate_key"
    ]
    assert candidate_snapshot["experimental_formula_eligible"] is True
    assert candidate_snapshot["formula_registry_effect"] == "NONE"
    assert candidate_snapshot["delivery_channel"] == "NONE"
    assert candidate_snapshot["live_eligible"] is False
    assert candidate_snapshot["telegram_delivery_allowed"] is False
    assert candidate_snapshot["trade_execution_allowed"] is False
    assert payload["evidence"]["independent_movement_count"] >= 5
    assert set(payload["evidence"]["accepted_paths"]) == {
        "PROBABILITY",
        "ASYMMETRY",
    }
    assert alerts.ExperimentalFormulaAlert.from_dict(payload) == alert
    again = alerts.build_experimental_alerts(
        [current],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=31),
    )
    assert again == built, "wall-clock validation must not change alert identity"

    text = alerts.render_experimental_telegram_alert(alert)
    assert text == alerts.render_experimental_telegram_alert(payload)
    assert text.startswith("🧪 ניסיוני, לא מאושר למסחר\n")
    for required in (
        "מטבע: BTC",
        "כיוון: לונג (LONG)",
        "אופק זמן: שעה אחת",
        "תנאי הנוסחה:",
        "מספר תנועות עצמאיות:",
        "גלי מחיר BTC נפרדים",
        "מסלול ראיות: הסתברות + אי־סימטריה",
        "הסתברות:",
        "Wilson 95% תחתון",
        "אי־סימטריה:",
        "MFE חציוני",
        "MAE חציוני",
        "למה ההתראה עדיין ניסיונית:",
        "אין עדיין holdout עצמאי.",
        "לא הוכח יתרון ביחס לקבוצת ביקורת מקבילה.",
        "לא בשלות לאישור מסחר.",
        "רק 6 תנועות עצמאיות",
        "סף הבשלות של 20",
        "משפחת Magnet אחת",
        "אין הרשאת LIVE ואין ביצוע מסחר אוטומטי.",
    ):
        assert required in text, required
    for condition in payload["formula"]["conditions"]:
        assert condition["feature"] in text
    assert "DRY RUN" not in text
    assert text.splitlines()[-1] == "ניסיוני, לא מאושר למסחר"
    assert len(text) <= alerts.MAX_TELEGRAM_TEXT_LENGTH

    tampered = deepcopy(payload)
    tampered["evidence"]["metrics"]["hit_rate_pct"] = 1.0
    _raises(
        "payload hash mismatch",
        lambda: alerts.render_experimental_telegram_alert(tampered),
    )


def _check_current_match_and_group_selection() -> None:
    observations, current, result, envelope = _fixture()
    no_features = replace(
        current,
        observation_id=_hash("current-no-match"),
        features=_features(),
        source_event_ids=(),
        source_event_fingerprints=(),
    )
    assert alerts.build_experimental_alerts(
        [no_features],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    ) == []

    eth = replace(
        current,
        observation_id=_hash("eth-current"),
        projection_event_id=101,
        projection_event_fingerprint=_hash("eth-current-projection"),
        snapshot_set_id=20_101,
        snapshot_key=_hash("eth-current-snapshot"),
        symbol="ETH",
        source_event_ids=(10_101,),
        source_event_fingerprints=(_hash("eth-current-source"),),
    )
    two_symbols = alerts.build_experimental_alerts(
        [eth, current],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )
    assert [(item.to_dict()["symbol"]) for item in two_symbols] == ["BTC", "ETH"]

    duplicate = replace(
        current,
        observation_id=_hash("later-current-duplicate"),
        projection_event_id=102,
        projection_event_fingerprint=_hash("later-current-projection"),
        snapshot_set_id=20_102,
        snapshot_key=_hash("later-current-snapshot"),
        source_event_ids=(10_102,),
        source_event_fingerprints=(_hash("later-current-source"),),
    )
    deduplicated = alerts.build_experimental_alerts(
        [current, duplicate],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )
    assert len(deduplicated) == 1
    assert deduplicated[0].to_dict()["current_snapshot"][
        "projection_event_id"
    ] == 102
    original_payload = alerts.build_experimental_alerts(
        [current], envelope, current_time_utc=AS_OF + timedelta(minutes=30)
    )[0].to_dict()
    duplicate_payload = alerts.build_experimental_alerts(
        [duplicate], envelope, current_time_utc=AS_OF + timedelta(minutes=30)
    )[0].to_dict()
    assert original_payload["trigger_key"] == duplicate_payload["trigger_key"]
    assert original_payload["alert_id"] != duplicate_payload["alert_id"]

    older = replace(
        current,
        observation_id=_hash("older-current"),
        projection_event_id=99,
        projection_event_fingerprint=_hash("older-current-projection"),
        snapshot_set_id=20_099,
        snapshot_key=_hash("older-current-snapshot"),
        projection_decision_time_utc=(AS_OF + timedelta(minutes=20)).isoformat(),
        archive_cycle_time_utc=(AS_OF - timedelta(minutes=5)).isoformat(),
        source_event_ids=(10_099,),
        source_event_fingerprints=(_hash("older-current-source"),),
    )
    _raises(
        "non-latest",
        lambda: alerts.build_experimental_alerts(
            [current, older],
            envelope,
            current_time_utc=AS_OF + timedelta(minutes=30),
        ),
    )

    # Display collapse retains only one historical champion, but the current
    # matcher must consume every eligible condition variant.  Select the
    # single-feature variant that is absent from the display champion.
    display_key = result["eligible_candidates"][0]["candidate_key"]
    alternative = next(
        candidate
        for candidate in result["eligible_candidate_variants"]
        if candidate["candidate_key"] != display_key
        and len(candidate["conditions"]) == 1
    )
    alternative_current = replace(
        current,
        observation_id=_hash("alternative-current"),
        projection_event_id=103,
        projection_event_fingerprint=_hash("alternative-current-projection"),
        snapshot_set_id=20_103,
        snapshot_key=_hash("alternative-current-snapshot"),
        features=_features(alternative["conditions"][0]["feature"]),
        source_event_ids=(10_103,),
        source_event_fingerprints=(_hash("alternative-current-source"),),
    )
    variant_alerts = alerts.build_experimental_alerts(
        [alternative_current],
        envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )
    assert len(variant_alerts) == 1
    assert variant_alerts[0].to_dict()["formula"]["candidate_key"] == (
        alternative["candidate_key"]
    )

    empty_result = search.search_experimental_candidates(
        observations[:4],
        horizon_minutes=HORIZON,
        analysis_as_of_utc=AS_OF,
        config=search.Stage4SearchConfig(wall_budget_ms=5_000),
    )
    assert empty_result["counts"]["eligible_candidate_variants"] == 0
    empty_envelope = alerts.compact_eligible_search_envelope(empty_result)
    assert empty_envelope.to_dict()["eligible_candidates"] == []
    assert alerts.build_experimental_alerts(
        [current],
        empty_envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    ) == []


def _check_fresh_bound_fail_closed() -> None:
    _, current, _, envelope = _fixture()
    now = AS_OF + timedelta(minutes=30)
    stale = replace(
        current,
        projection_decision_time_utc=(
            now
            - timedelta(minutes=alerts.MAX_CURRENT_SNAPSHOT_AGE_MINUTES + 1)
        ).isoformat(),
        archive_cycle_time_utc=(now - timedelta(minutes=70)).replace(
            minute=0, second=0, microsecond=0
        ).isoformat(),
    )
    _raises(
        "not fresh",
        lambda: alerts.build_experimental_alerts(
            [stale], envelope, current_time_utc=now
        ),
    )
    future = replace(
        current,
        projection_decision_time_utc=(now + timedelta(seconds=1)).isoformat(),
    )
    _raises(
        "not fresh",
        lambda: alerts.build_experimental_alerts(
            [future], envelope, current_time_utc=now
        ),
    )
    unavailable = replace(
        current,
        observation_id=_hash("unavailable-eth-current"),
        projection_event_id=104,
        projection_event_fingerprint=_hash("unavailable-eth-projection"),
        snapshot_set_id=20_104,
        snapshot_key=_hash("unavailable-eth-snapshot"),
        symbol="ETH",
        source_event_ids=(10_104,),
        source_event_fingerprints=(_hash("unavailable-eth-source"),),
        wave_binding=search._CompactWaveBinding(
            "UNAVAILABLE", "NO_PARENT_WAVE", None
        ),
    )
    assert alerts.build_experimental_alerts(
        [unavailable], envelope, current_time_utc=now
    ) == []
    mixed = alerts.build_experimental_alerts(
        [unavailable, current], envelope, current_time_utc=now
    )
    assert len(mixed) == 1
    assert mixed[0].to_dict()["symbol"] == "BTC"

    malformed_wave = replace(
        unavailable,
        wave_binding=search._CompactWaveBinding(
            "UNBOUND", "NON_TERMINAL", None
        ),
    )
    _raises(
        "not terminal",
        lambda: alerts.build_experimental_alerts(
            [current, malformed_wave], envelope, current_time_utc=now
        ),
    )
    _raises(
        "not current",
        lambda: alerts.build_experimental_alerts(
            [current],
            envelope,
            current_time_utc=(
                AS_OF
                + timedelta(
                    minutes=(
                        HORIZON
                        * alerts.SEARCH_FRESHNESS_CADENCE_MULTIPLIER
                        + 1
                    )
                )
            ),
        ),
    )
    _raises(
        "not current",
        lambda: alerts.build_experimental_alerts(
            [current], envelope, current_time_utc=AS_OF - timedelta(seconds=1)
        ),
    )

    expired_but_valid_snapshot = replace(
        current,
        projection_decision_time_utc=(
            now - timedelta(minutes=alerts.ALERT_EXPIRY_MINUTES)
        ).isoformat(),
        archive_cycle_time_utc=now.replace(
            minute=0, second=0, microsecond=0
        ).isoformat(),
    )
    assert alerts.build_experimental_alerts(
        [expired_but_valid_snapshot], envelope, current_time_utc=now
    ) == []


def _check_search_and_envelope_tampering() -> None:
    _, _, result, envelope = _fixture()
    wrong_receipt = deepcopy(result)
    wrong_receipt["analysis_as_of_utc"] = (
        AS_OF - timedelta(minutes=1)
    ).isoformat()
    _raises(
        "receipt hash mismatch",
        lambda: alerts.compact_eligible_search_envelope(wrong_receipt),
    )

    insufficient = deepcopy(result)
    first_eligible = insufficient["eligible_candidate_variants"][0]
    first_eligible["occurrence_counts"]["completed"] = 4
    insufficient["search_receipt_sha256"] = search.candidate_search_receipt_sha256(
        insufficient
    )
    _raises(
        "five-movement gate",
        lambda: alerts.compact_eligible_search_envelope(insufficient),
    )

    self_consistent_weak = deepcopy(result)
    weak_metrics = self_consistent_weak["eligible_candidate_variants"][0][
        "metrics"
    ]
    weak_metrics.update(
        {
            "successes": 0,
            "hit_rate_pct": 0.0,
            "wilson_95_lower_pct": 0.0,
            "probability_exact_binomial_p_value": 1.0,
            "favorable_dominance_successes": 0,
            "favorable_dominance_rate_pct": 0.0,
            "favorable_dominance_wilson_95_lower_pct": 0.0,
            "asymmetry_exact_binomial_p_value": 1.0,
        }
    )
    self_consistent_weak["search_receipt_sha256"] = (
        search.candidate_search_receipt_sha256(self_consistent_weak)
    )
    _raises(
        "accepted paths are inconsistent",
        lambda: alerts.compact_eligible_search_envelope(self_consistent_weak),
    )

    elevated = deepcopy(result)
    elevated["telegram_delivery_allowed"] = True
    elevated["search_receipt_sha256"] = search.candidate_search_receipt_sha256(
        elevated
    )
    _raises(
        "authority boundary",
        lambda: alerts.compact_eligible_search_envelope(elevated),
    )

    zero_mae = deepcopy(result)
    zero_mae_candidate = zero_mae["eligible_candidate_variants"][0]
    zero_mae["eligible_candidate_variants"] = [zero_mae_candidate]
    zero_mae["counts"]["eligible_candidate_variants"] = 1
    zero_mae_candidate["metrics"]["median_mae_pct"] = 0.0
    zero_mae_candidate["metrics"]["median_mfe_mae_ratio"] = None
    zero_mae_candidate["metrics"]["median_mfe_mae_ratio_state"] = (
        "UNBOUNDED_ZERO_MAE"
    )
    zero_mae["search_receipt_sha256"] = search.candidate_search_receipt_sha256(
        zero_mae
    )
    zero_mae_envelope = alerts.compact_eligible_search_envelope(zero_mae)
    _, current, _, _ = _fixture()
    zero_mae_alert = alerts.build_experimental_alerts(
        [current],
        zero_mae_envelope,
        current_time_utc=AS_OF + timedelta(minutes=30),
    )[0]
    assert "יחס MFE/MAE בלתי חסום (MAE=0)" in (
        alerts.render_experimental_telegram_alert(zero_mae_alert)
    )

    compact_tamper = envelope.to_dict()
    compact_tamper["eligible_candidates"][0]["conditions"][0]["value"] = False
    _raises(
        "hash mismatch",
        lambda: alerts.CompactEligibleSearchEnvelope.from_dict(compact_tamper),
    )


def _check_pure_boundary_and_descriptor() -> None:
    decision_helpers = (
        alerts._validated_current_observation,
        alerts._condition_result,
        alerts.build_experimental_alerts,
    )
    for helper in decision_helpers:
        tree = ast.parse(inspect.getsource(helper))
        assert not any(
            isinstance(node, ast.Attribute) and node.attr == "outcome"
            for node in ast.walk(tree)
        ), helper.__name__
        assert not any(
            isinstance(node, ast.Subscript)
            and isinstance(node.slice, ast.Constant)
            and node.slice.value == "outcome"
            for node in ast.walk(tree)
        ), helper.__name__

    source = Path(alerts.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import research_formula_store",
        "import research_formula_worker",
        "from telegram",
        "import telegram",
        "send_message(",
        "reply_text(",
        "psycopg",
        "requests.",
        "aiohttp.",
    ):
        assert forbidden not in source, forbidden
    descriptor = alerts.descriptor()
    assert descriptor["experimental_label"] == "ניסיוני, לא מאושר למסחר"
    assert descriptor["minimum_independent_movements"] == 5
    assert descriptor["max_current_snapshot_age_minutes"] == 45
    assert descriptor["search_freshness_cadence_multiplier"] == 2
    assert descriptor["alert_expiry_minutes"] == 35
    assert descriptor["current_decision_reads_outcomes"] is False
    assert descriptor["database_effect"] == "NONE"
    assert descriptor["telegram_effect"] == "NONE"
    assert descriptor["live_eligible"] is False
    assert descriptor["trade_execution_allowed"] is False


def run() -> None:
    _check_happy_path_and_renderer()
    _check_current_match_and_group_selection()
    _check_fresh_bound_fail_closed()
    _check_search_and_envelope_tampering()
    _check_pure_boundary_and_descriptor()
    print("research_experimental_formula_alert_selftest: PASS")


if __name__ == "__main__":
    run()
