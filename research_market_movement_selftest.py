"""Deterministic regressions for the pure Market Movement Wave v5 contract."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import math
import random

import research_market_movement as movement


UTC = timezone.utc
START = datetime(2026, 8, 29, 0, 2, tzinfo=UTC)


def _provenance(symbol: str = "BTC") -> dict:
    if symbol == "HYPE":
        return {
            "source": "hyperliquid_spot_@107",
            "upstream_source": "hyperliquid",
            "quality_status": "PASS",
            "price_exchange": "hyperliquid",
            "price_market": "spot",
            "price_pair": "HYPE/USDT",
            "price_instrument_id": "@107",
            "price_timeframe": "1m",
            "fallback_used": False,
            "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
        }
    return {
        "source": "binance_spot",
        "upstream_source": "binance_spot",
        "quality_status": "PASS",
        "price_exchange": "binance",
        "price_market": "spot",
        "price_pair": f"{symbol}USDT",
        "price_instrument_id": f"{symbol}USDT",
        "price_timeframe": "1m",
        "fallback_used": False,
        "fallback_policy": "PROVIDER_ATTESTED_NO_FALLBACK",
    }


def _anchor(
    index: int,
    price,
    *,
    symbol: str = "BTC",
    decision_seconds: int = 7,
) -> movement.NeutralPriceAnchor:
    eligible = START + timedelta(minutes=30 * index)
    closed = eligible - timedelta(microseconds=1000)
    return movement.NeutralPriceAnchor.build_prospective(
        symbol=symbol,
        eligible_at_utc=eligible,
        decision_time_utc=eligible + timedelta(seconds=decision_seconds),
        source_price_candle_open_utc=eligible - timedelta(minutes=1),
        source_price_candle_close_utc=closed,
        observed_at_utc=closed,
        refresh_completed_at_utc=eligible + timedelta(seconds=2),
        price=price,
        source_provenance=_provenance(symbol),
    )


def _legacy_slot(
    *,
    sampler: str = "prospective-neutral-anchor-v4-decision-features-frozen",
    symbol: str = "BTC",
) -> dict:
    slot_open = datetime(2026, 8, 29, 0, 0, tzinfo=UTC)
    slot_close = slot_open + timedelta(minutes=30)
    eligible = slot_close + timedelta(minutes=2)
    decision = eligible + timedelta(seconds=11)
    observed = eligible - timedelta(microseconds=1000)
    record = {
        "sampler_version": sampler,
        "coverage_policy_version": "selftest-frozen-coverage-v1",
        "coverage_snapshot": {
            "symbol": symbol,
            "eligible": True,
            "frozen_at_utc": "2026-08-29T00:31:00.000000Z",
        },
        "symbol": symbol,
        "source_candle_open_utc": slot_open,
        "source_candle_close_utc": slot_close,
        "base_eligible_at_utc": eligible,
        "expires_at_utc": eligible + timedelta(minutes=30),
        "decision_time_utc": decision,
        "source_timestamps": {
            "official_price": {
                "observed_at_utc": movement._iso(observed),
                "refresh_completed_at_utc": movement._iso(
                    eligible + timedelta(seconds=3)
                ),
            },
            "price_oi": {
                "observation_time_utc": movement._iso(eligible),
                "refresh_completed_at_utc": movement._iso(eligible),
                "price_fetched_at_utc": movement._iso(slot_close),
                "oi_fetched_at_utc": movement._iso(slot_close),
            },
            "futures_cvd": {
                "source_candle_time_utc": movement._iso(slot_open),
                "refresh_completed_at_utc": movement._iso(eligible),
            },
            "spot_cvd": {
                "source_candle_time_utc": movement._iso(slot_open),
                "refresh_completed_at_utc": movement._iso(eligible),
            },
        },
        "source_provenance": {
            "official_price": _provenance(symbol),
            "price_oi": {
                "source_table": "oi_regime_snapshots",
                "quality_status": "PASS",
            },
            "futures_cvd": {
                "source": "coinglass_futures_aggregated_cvd",
                "quality_status": "PASS",
            },
            "spot_cvd": {
                "source": "coinglass_spot_aggregated_cvd",
                "quality_status": "PASS",
            },
        },
        "frozen_inputs": {
            "official_price": {"price": 100.25},
            "price_oi": {"price_close": 100.0, "oi_close_usd": 1_000_000.0},
            "futures_cvd": {"continuous_cum_vol_delta_usd": 12_000.0},
            "spot_cvd": {"continuous_cum_vol_delta_usd": -2_000.0},
            "max_pain": {
                "evaluation_status": "UNEVALUABLE",
                "reason": "no prior coherent snapshot",
                "features": {},
            },
        },
        "created_at_utc": decision + timedelta(milliseconds=20),
    }
    if sampler == movement.LEGACY_V4_SAMPLER_VERSION:
        # Use the production feature-freeze builder so this is a real v4 slot
        # shape, not a synthetic pair of policy/hash placeholders.
        from research_prospective_anchors_selftest import _feature_bundle_entry

        record.update(deepcopy(_feature_bundle_entry(decision, symbol=symbol)))
    else:
        # A SELECT after migration 013 returns these nullable columns for v3.
        record.update(
            {
                "decision_feature_bundle": None,
                "feature_bundle_policy_version": None,
                "feature_bundle_sha256": None,
            }
        )
    record["input_fingerprint"] = (
        movement.compute_authorized_legacy_input_fingerprint(record)
    )
    return record


def _production_input_fingerprint(record: dict) -> str:
    """Independent oracle: the live v3/v4 sampler's production helper."""

    import research_prospective_anchors as prospective

    return prospective.compute_input_fingerprint(
        sampler_version=record.get("sampler_version"),
        coverage_policy_version=record.get("coverage_policy_version"),
        coverage_snapshot=record.get("coverage_snapshot"),
        symbol=record.get("symbol"),
        source_candle_open_utc=record.get("source_candle_open_utc"),
        source_candle_close_utc=record.get("source_candle_close_utc"),
        base_eligible_at_utc=record.get("base_eligible_at_utc"),
        expires_at_utc=record.get("expires_at_utc"),
        evaluation_status=prospective.EVALUABLE,
        decision_time_utc=record.get("decision_time_utc"),
        source_timestamps=record.get("source_timestamps"),
        source_provenance=record.get("source_provenance"),
        frozen_inputs=record.get("frozen_inputs"),
        feature_bundle_policy_version=record.get(
            "feature_bundle_policy_version"
        ),
        feature_bundle_sha256=record.get("feature_bundle_sha256"),
    )


def _raises(fragment: str, callback) -> None:
    try:
        callback()
    except (TypeError, ValueError) as exc:
        assert fragment.lower() in str(exc).lower(), (fragment, str(exc))
    else:
        raise AssertionError(f"expected rejection containing {fragment!r}")


def _transition_signature(history: movement.MovementHistory) -> tuple:
    return (
        history.canonical_receipts(),
        tuple(item.to_dict()["post_state"] for item in history.transitions),
        tuple(item.to_dict() for item in history.memberships),
    )


def run() -> None:
    # Keep Python's accepted symbol alphabet identical to the SQL archive.
    for non_ascii_symbol in ("ÉTH", "ßTC", "ſOL", "ıCP"):
        _raises(
            "invalid neutral-price symbol",
            lambda value=non_ascii_symbol: movement._symbol(value),
        )

    # Exact live neutral-price validation preserves the real decision time.
    anchor = _anchor(0, 100)
    assert anchor.eligible_at_utc == START
    assert anchor.decision_time_utc == START + timedelta(seconds=7)
    assert anchor.source_price_candle_open_utc == START - timedelta(minutes=1)
    assert START - timedelta(seconds=1) <= anchor.observed_at_utc < START
    assert anchor.price_candle_identity_basis == (
        movement.PROSPECTIVE_PRICE_CANDLE_IDENTITY_BASIS
    )
    round_trip = movement.NeutralPriceAnchor.from_dict(
        json.loads(json.dumps(anchor.to_dict()))
    )
    assert round_trip == anchor
    reordered = dict(reversed(list(anchor.to_dict().items())))
    assert movement.NeutralPriceAnchor.from_dict(reordered) == anchor

    evaluable = movement.evaluate_prospective_anchor(
        symbol="BTC",
        eligible_at_utc=START,
        decision_time_utc=START + timedelta(seconds=7),
        price_candle={
            "open_time_utc": START - timedelta(minutes=1),
            "close_time_utc": START - timedelta(microseconds=1000),
            "observed_at_utc": START - timedelta(microseconds=1000),
            "refresh_completed_at_utc": START + timedelta(seconds=2),
            "price": 100,
        },
        source_provenance=_provenance(),
    )
    assert evaluable.attempt.evaluation_status == movement.EVALUABLE
    assert evaluable.anchor == anchor
    invalid_source = deepcopy(_provenance())
    invalid_source["fallback_used"] = True
    rejected = movement.evaluate_prospective_anchor(
        symbol="BTC",
        eligible_at_utc=START,
        decision_time_utc=START + timedelta(seconds=7),
        price_candle={
            "open_time_utc": START - timedelta(minutes=1),
            "close_time_utc": START - timedelta(microseconds=1000),
            "observed_at_utc": START - timedelta(microseconds=1000),
            "refresh_completed_at_utc": START + timedelta(seconds=2),
            "price": 100,
        },
        source_provenance=invalid_source,
    )
    assert rejected.attempt.evaluation_status == movement.UNEVALUABLE
    assert rejected.anchor is None

    # HYPE has one and only one official route.
    hype = _anchor(0, 45.5, symbol="HYPE")
    assert hype.price_instrument_id == "@107"
    wrong_hype = deepcopy(_provenance("HYPE"))
    wrong_hype["upstream_source"] = "binance_spot"
    _raises(
        "HYPE requires",
        lambda: movement.NeutralPriceAnchor.build_prospective(
            symbol="HYPE",
            eligible_at_utc=START,
            decision_time_utc=START + timedelta(seconds=7),
            source_price_candle_open_utc=START - timedelta(minutes=1),
            source_price_candle_close_utc=START - timedelta(microseconds=1000),
            observed_at_utc=START - timedelta(microseconds=1000),
            refresh_completed_at_utc=START + timedelta(seconds=2),
            price=45.5,
            source_provenance=wrong_hype,
        ),
    )

    # Fail closed on malformed lattice/timing/value inputs.
    base = anchor.to_dict()
    for field, corrupt, message in (
        ("eligible_at_utc", "2026-08-29T00:03:00Z", ":02/:32"),
        ("eligible_at_utc", "2026-08-29T00:02:00", "timezone"),
        ("source_price_candle_open_utc", "2026-08-29T00:00:00Z", "open"),
        ("source_price_candle_close_utc", "2026-08-29T00:02:00Z", "final second"),
        ("observed_at_utc", "2026-08-29T00:01:59.998Z", "must equal"),
        ("refresh_completed_at_utc", "2026-08-29T00:01:59Z", "refresh"),
        ("decision_time_utc", "2026-08-29T00:32:00Z", "capture window"),
        ("price", 0, "positive"),
        ("price", True, "positive"),
        ("price", float("nan"), "positive"),
        ("price", float("inf"), "positive"),
    ):
        candidate = deepcopy(base)
        candidate[field] = corrupt
        # Remove content-addresses so the causal-field validation is reached.
        candidate["anchor_id"] = "0" * 64
        candidate["anchor_receipt_sha256"] = "0" * 64
        _raises(message, lambda item=candidate: movement.NeutralPriceAnchor.from_dict(item))
    forged = deepcopy(base)
    forged["anchor_receipt_sha256"] = "0" * 64
    _raises("forged", lambda: movement.NeutralPriceAnchor.from_dict(forged))
    unknown = {**base, "outcome_label": {"mfe_pct": 999}}
    _raises("unknown", lambda: movement.NeutralPriceAnchor.from_dict(unknown))

    # Exact real v4 legacy envelope: no price-candle open/close in provenance,
    # only the frozen provider close under source_timestamps.official_price.
    legacy_record = _legacy_slot()
    from research_prospective_feature_freeze import (
        compute_feature_bundle_sha256,
        validate_feature_bundle,
    )

    assert "price_candle_open_utc" not in legacy_record["source_provenance"]["official_price"]
    assert "price_candle_close_utc" not in legacy_record["source_provenance"]["official_price"]
    assert legacy_record["feature_bundle_policy_version"] == (
        movement.LEGACY_V4_FEATURE_BUNDLE_POLICY_VERSION
    )
    bundle_valid, bundle_reason = validate_feature_bundle(
        legacy_record["decision_feature_bundle"],
        expected_sha256=legacy_record["feature_bundle_sha256"],
        expected_symbol="BTC",
        expected_decision_time_utc=legacy_record["decision_time_utc"],
    )
    assert bundle_valid, bundle_reason
    assert legacy_record["feature_bundle_sha256"] == (
        compute_feature_bundle_sha256(legacy_record["decision_feature_bundle"])
    )
    assert legacy_record["input_fingerprint"] == _production_input_fingerprint(
        legacy_record
    )
    legacy = movement.NeutralPriceAnchor.from_authorized_legacy_slot(legacy_record)
    assert legacy.origin == "AUTHORIZED_LEGACY_V3_V4"
    assert legacy.source_price_candle_open_utc == (
        legacy.eligible_at_utc - timedelta(minutes=1)
    )
    assert legacy.price_candle_identity_basis == (
        movement.HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
    )
    assert legacy.price_instrument_id == "BTCUSDT"
    assert movement.NeutralPriceAnchor.from_dict(legacy.to_dict()) == legacy
    legacy_v3 = _legacy_slot(
        sampler=movement.LEGACY_V3_SAMPLER_VERSION
    )
    # A v3 row loaded after migration 013 has null v4 columns, while v4 owns a
    # complete, hash-bound decision bundle. Both fingerprints must remain
    # byte-compatible with the production sampler helper.
    assert legacy_v3["decision_feature_bundle"] is None
    assert legacy_v3["feature_bundle_policy_version"] is None
    assert legacy_v3["feature_bundle_sha256"] is None
    assert legacy_v3["input_fingerprint"] == _production_input_fingerprint(
        legacy_v3
    )
    assert movement.NeutralPriceAnchor.from_authorized_legacy_slot(legacy_v3)

    for malformed_max_pain in (
        None,
        {},
        {"evaluation_status": movement.EVALUABLE, "features": []},
        {"evaluation_status": "UNKNOWN", "features": {}},
    ):
        malformed_legacy = deepcopy(legacy_record)
        if malformed_max_pain is None:
            malformed_legacy["frozen_inputs"].pop("max_pain")
        else:
            malformed_legacy["frozen_inputs"]["max_pain"] = malformed_max_pain
        malformed_legacy["input_fingerprint"] = (
            movement.compute_authorized_legacy_input_fingerprint(
                malformed_legacy
            )
        )
        _raises(
            "max_pain",
            lambda item=malformed_legacy: (
                movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
            ),
        )

    duplicated_bundle = deepcopy(legacy_record)
    duplicated_bundle["frozen_inputs"]["decision_feature_bundle"] = deepcopy(
        duplicated_bundle["decision_feature_bundle"]
    )
    duplicated_bundle["input_fingerprint"] = (
        movement.compute_authorized_legacy_input_fingerprint(duplicated_bundle)
    )
    _raises(
        "must not be duplicated",
        lambda: movement.NeutralPriceAnchor.from_authorized_legacy_slot(
            duplicated_bundle
        ),
    )

    for field in (
        "feature_bundle_policy_version",
        "feature_bundle_sha256",
        "decision_feature_bundle",
    ):
        mislabeled_v3 = deepcopy(legacy_v3)
        mislabeled_v3[field] = deepcopy(legacy_record[field])
        mislabeled_v3["input_fingerprint"] = (
            movement.compute_authorized_legacy_input_fingerprint(mislabeled_v3)
        )
        _raises(
            "legacy v3",
            lambda item=mislabeled_v3: (
                movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
            ),
        )

    for field, fragment in (
        ("feature_bundle_policy_version", "v4 feature bundle policy"),
        ("feature_bundle_sha256", "feature_bundle_sha256"),
        ("decision_feature_bundle", "v4 decision feature bundle"),
    ):
        incomplete_v4 = deepcopy(legacy_record)
        incomplete_v4.pop(field)
        incomplete_v4["input_fingerprint"] = (
            movement.compute_authorized_legacy_input_fingerprint(incomplete_v4)
        )
        _raises(
            fragment,
            lambda item=incomplete_v4: (
                movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
            ),
        )
    bundle_hash_mismatch = deepcopy(legacy_record)
    bundle_hash_mismatch["decision_feature_bundle"][
        "feature_schema_version"
    ] = "tampered-feature-schema"
    _raises(
        "bundle hash mismatch",
        lambda: movement.NeutralPriceAnchor.from_authorized_legacy_slot(
            bundle_hash_mismatch
        ),
    )

    created_boundary = deepcopy(legacy_record)
    created_boundary["created_at_utc"] = (
        created_boundary["decision_time_utc"] + timedelta(minutes=5)
    )
    assert movement.NeutralPriceAnchor.from_authorized_legacy_slot(created_boundary)
    created_too_late = deepcopy(created_boundary)
    created_too_late["created_at_utc"] += timedelta(microseconds=1)
    _raises(
        "created_at",
        lambda: movement.NeutralPriceAnchor.from_authorized_legacy_slot(
            created_too_late
        ),
    )

    # Formula/outcome fields are not inputs. Frozen causal-family mutations are.
    decorated = deepcopy(legacy_record)
    decorated["formula_match"] = {"formula_id": "ignored"}
    decorated["outcome_label"] = {
        "path_success": True,
        "mfe_pct": 999,
        "mae_pct": 0,
    }
    assert (
        movement.NeutralPriceAnchor.from_authorized_legacy_slot(decorated)
        == legacy
    )
    changed_family = deepcopy(legacy_record)
    changed_family["frozen_inputs"]["price_oi"]["oi_close_usd"] += 1
    _raises(
        "input_fingerprint",
        lambda: movement.NeutralPriceAnchor.from_authorized_legacy_slot(changed_family),
    )

    marker_containers = (
        lambda row: row,
        lambda row: row["source_timestamps"]["official_price"],
        lambda row: row["source_provenance"]["official_price"],
        lambda row: row["frozen_inputs"]["official_price"],
    )
    for select_container in marker_containers:
        for marker in ("OLD_DERIVATION", "", None):
            conflicting = deepcopy(legacy_record)
            select_container(conflicting)["price_candle_identity_basis"] = marker
            # A present marker is contract evidence, so absent and null are not
            # equivalent. Recompute cannot make an invalid marker acceptable.
            conflicting["input_fingerprint"] = (
                movement.compute_authorized_legacy_input_fingerprint(conflicting)
            )
            _raises(
                "identity_basis",
                lambda item=conflicting: (
                    movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
                ),
            )
        canonical_marker = deepcopy(legacy_record)
        select_container(canonical_marker)["price_candle_identity_basis"] = (
            movement.HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
        )
        canonical_marker["input_fingerprint"] = (
            movement.compute_authorized_legacy_input_fingerprint(canonical_marker)
        )
        assert movement.NeutralPriceAnchor.from_authorized_legacy_slot(
            canonical_marker
        )

    timestamp_candle_evidence = deepcopy(legacy_record)
    timestamp_candle_evidence["source_timestamps"]["official_price"].update(
        {
            "price_candle_identity_basis": (
                movement.HISTORICAL_PRICE_CANDLE_IDENTITY_BASIS
            ),
            "candle_open_time_utc": movement._iso(
                timestamp_candle_evidence["base_eligible_at_utc"]
                - timedelta(minutes=1)
            ),
            "price_candle_open_time_utc": movement._iso(
                timestamp_candle_evidence["base_eligible_at_utc"]
                - timedelta(minutes=1)
            ),
            "candle_close_time_utc": timestamp_candle_evidence[
                "source_timestamps"
            ]["official_price"]["observed_at_utc"],
            "price_candle_close_time_utc": timestamp_candle_evidence[
                "source_timestamps"
            ]["official_price"]["observed_at_utc"],
            "price_observed_at_utc": timestamp_candle_evidence[
                "source_timestamps"
            ]["official_price"]["observed_at_utc"],
        }
    )
    timestamp_candle_evidence["input_fingerprint"] = (
        movement.compute_authorized_legacy_input_fingerprint(
            timestamp_candle_evidence
        )
    )
    assert movement.NeutralPriceAnchor.from_authorized_legacy_slot(
        timestamp_candle_evidence
    )
    for field, value, fragment in (
        ("candle_open_time_utc", "2026-08-29T00:30:00Z", "open conflicts"),
        ("candle_close_time_utc", "2026-08-29T00:31:58Z", "close conflicts"),
        (
            "price_candle_open_time_utc",
            "2026-08-29T00:30:00Z",
            "open conflicts",
        ),
        (
            "price_candle_close_time_utc",
            "2026-08-29T00:31:58Z",
            "close conflicts",
        ),
        (
            "price_observed_at_utc",
            "2026-08-29T00:31:58Z",
            "close conflicts",
        ),
    ):
        conflicting_candle = deepcopy(timestamp_candle_evidence)
        conflicting_candle["source_timestamps"]["official_price"][field] = value
        conflicting_candle["input_fingerprint"] = (
            movement.compute_authorized_legacy_input_fingerprint(
                conflicting_candle
            )
        )
        _raises(
            fragment,
            lambda item=conflicting_candle: (
                movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
            ),
        )

    # Repository-native candle aliases are evidence in every official-price
    # envelope, not only in the timestamps map.
    for select_container in marker_containers:
        for field, value, fragment in (
            (
                "price_candle_open_time_utc",
                "2026-08-29T00:30:00Z",
                "open conflicts",
            ),
            (
                "price_candle_close_time_utc",
                "2026-08-29T00:31:58Z",
                "close conflicts",
            ),
            (
                "price_observed_at_utc",
                "2026-08-29T00:31:58Z",
                "close conflicts",
            ),
        ):
            conflicting_alias = deepcopy(legacy_record)
            select_container(conflicting_alias)[field] = value
            conflicting_alias["input_fingerprint"] = (
                movement.compute_authorized_legacy_input_fingerprint(
                    conflicting_alias
                )
            )
            _raises(
                fragment,
                lambda item=conflicting_alias: (
                    movement.NeutralPriceAnchor.from_authorized_legacy_slot(item)
                ),
            )

    for mutate, fragment in (
        (lambda row: row.update(sampler_version="v2"), "authorized"),
        (
            lambda row: row.update(
                source_candle_open_utc=datetime(2026, 8, 28, 23, 0, tzinfo=UTC),
                source_candle_close_utc=datetime(2026, 8, 28, 23, 30, tzinfo=UTC),
                base_eligible_at_utc=datetime(2026, 8, 28, 23, 32, tzinfo=UTC),
                expires_at_utc=datetime(2026, 8, 29, 0, 2, tzinfo=UTC),
                decision_time_utc=datetime(2026, 8, 28, 23, 32, 5, tzinfo=UTC),
            ),
            "predates",
        ),
        (
            lambda row: row["source_provenance"]["official_price"].update(
                fallback_used=True
            ),
            "fallback",
        ),
        (
            lambda row: row["source_timestamps"]["official_price"].update(
                observed_at_utc="2026-08-29T00:31:58.999000Z"
            ),
            "final second",
        ),
        (
            lambda row: row.update(
                created_at_utc=row["decision_time_utc"] - timedelta(seconds=1)
            ),
            "created_at",
        ),
    ):
        bad = deepcopy(legacy_record)
        mutate(bad)
        if bad.get("sampler_version") in movement.AUTHORIZED_LEGACY_SAMPLERS:
            bad["input_fingerprint"] = (
                movement.compute_authorized_legacy_input_fingerprint(bad)
            )
        _raises(
            fragment,
            lambda item=bad: movement.NeutralPriceAnchor.from_authorized_legacy_slot(item),
        )

    # Bootstrap direction, one miss, recovery, and unbounded same-wave trend.
    first = movement.replay([_anchor(0, 100)])
    assert first.cursor is not None
    assert first.cursor.state.direction == movement.PENDING_DIRECTION
    assert first.cursor.state.member_count == 1
    recovered = movement.replay(
        [_anchor(0, 100), _anchor(1, 110), _anchor(2, 105), _anchor(3, 120)]
    )
    assert recovered.cursor.state.direction == movement.UP_DIRECTION
    assert recovered.cursor.state.consecutive_non_extremes == 0
    assert recovered.cursor.state.member_count == 4
    assert len({member.movement_id for member in recovered.memberships}) == 1

    rising = [_anchor(index, 100 + index) for index in range(60)]
    beyond_24h = movement.replay(rising)
    assert beyond_24h.cursor.state.member_count == 60
    assert len({member.movement_id for member in beyond_24h.memberships}) == 1
    assert beyond_24h.memberships[-1].eligible_at_utc - START > timedelta(hours=24)

    # The first non-extreme stays in the old movement. The second closes it
    # before membership and exclusively starts the new movement.
    rolled = movement.replay(
        [_anchor(0, 100), _anchor(1, 110), _anchor(2, 105), _anchor(3, 104)]
    )
    assert [item.transition_type for item in rolled.transitions] == [
        movement.OPENED,
        movement.DIRECTION_ESTABLISHED,
        movement.NON_EXTREME_OBSERVED,
        movement.MOVEMENT_CLOSED,
        movement.OPENED_AFTER_DIRECTION_END,
    ]
    old_id = rolled.memberships[0].movement_id
    new_id = rolled.memberships[-1].movement_id
    assert old_id != new_id
    assert [item.movement_id for item in rolled.memberships] == [
        old_id,
        old_id,
        old_id,
        new_id,
    ]
    terminal = rolled.transitions[-2].post_state
    assert terminal.close_reason == movement.TWO_CONSECUTIVE_NON_EXTREMES
    assert terminal.member_count == 3
    assert rolled.cursor.state.start_price == 104
    assert rolled.cursor.state.direction == movement.PENDING_DIRECTION

    # Equality counts as no new extreme, including while direction is pending.
    flat = movement.replay([_anchor(0, 100), _anchor(1, 100), _anchor(2, 100)])
    assert len({item.movement_id for item in flat.memberships}) == 2
    assert flat.cursor.state.started_anchor_id == _anchor(2, 100).anchor_id
    starts_above = movement.replay(
        [_anchor(0, 100), _anchor(1, 110), _anchor(2, 109), _anchor(3, 108)]
    )
    assert starts_above.cursor.state.start_price > starts_above.transitions[0].post_state.start_price
    starts_below = movement.replay(
        [_anchor(0, 100), _anchor(1, 90), _anchor(2, 91), _anchor(3, 92)]
    )
    assert starts_below.cursor.state.start_price < starts_below.transitions[0].post_state.start_price

    # A missing scheduled slot censors; the later valid anchor starts fresh.
    gap = movement.replay([_anchor(0, 100), _anchor(2, 101)])
    assert [item.transition_type for item in gap.transitions] == [
        movement.OPENED,
        movement.MOVEMENT_CLOSED,
        movement.OPENED_AFTER_DATA_GAP,
    ]
    gap_terminal = gap.transitions[1].post_state
    assert gap_terminal.close_reason == movement.DATA_GAP_CENSORED
    assert gap_terminal.close_boundary_eligible_at_utc == START + timedelta(minutes=30)
    assert len({item.movement_id for item in gap.memberships}) == 2

    # Batch replay is input-order independent, retry-idempotent, and no-fork.
    source = [_anchor(0, 100), _anchor(1, 101), _anchor(2, 102), _anchor(3, 101)]
    canonical = movement.replay(source)
    shuffled = list(source)
    random.Random(73).shuffle(shuffled)
    assert _transition_signature(movement.replay(shuffled)) == _transition_signature(canonical)
    assert _transition_signature(movement.replay([*source, source[1]])) == _transition_signature(canonical)
    conflicting = _anchor(1, 999)
    assert conflicting.anchor_id == source[1].anchor_id
    assert conflicting.anchor_receipt_sha256 != source[1].anchor_receipt_sha256
    _raises("conflicting", lambda: movement.replay([*source, conflicting]))
    opened = movement.advance(None, source[0])
    _raises("strictly increasing", lambda: movement.advance(opened.cursor, source[0]))

    # BTC's asset-local stream and the global BTC parent use the same frozen
    # price facts but always have distinct identities, movements, and receipts.
    btc_inputs = [_anchor(0, 100), _anchor(1, 101), _anchor(2, 102)]
    local = movement.replay(
        btc_inputs, identity=movement.MovementIdentity.for_symbol("BTC")
    )
    parent = movement.replay(
        btc_inputs, identity=movement.MovementIdentity.btc_parent()
    )
    assert local.identity.stream_id != parent.identity.stream_id
    assert local.cursor.state.movement_id != parent.cursor.state.movement_id
    assert local.cursor.state.direction == parent.cursor.state.direction
    assert local.cursor.state.member_count == parent.cursor.state.member_count
    _raises(
        "symbol",
        lambda: movement.replay(
            [_anchor(0, 50, symbol="ETH")],
            identity=movement.MovementIdentity.btc_parent(),
        ),
    )

    # Every append-only projection round-trips and rejects forged identities.
    for transition in rolled.transitions:
        assert movement.MovementTransition.from_dict(transition.to_dict()) == transition
    for membership in rolled.memberships:
        assert movement.MovementMembership.from_dict(membership.to_dict()) == membership
    assert movement.MovementState.from_dict(rolled.cursor.state.to_dict()) == rolled.cursor.state
    assert movement.MovementCursor.from_dict(rolled.cursor.to_dict()) == rolled.cursor
    forged_state = rolled.cursor.state.to_dict()
    forged_state["movement_id"] = "0" * 64
    _raises("movement_id", lambda: movement.MovementState.from_dict(forged_state))
    forged_transition = rolled.transitions[0].to_dict()
    forged_transition["transition_receipt_sha256"] = "0" * 64
    _raises("forged", lambda: movement.MovementTransition.from_dict(forged_transition))

    # Canonical price spelling cannot split receipts; causal time changes do.
    assert _anchor(0, 100).anchor_receipt_sha256 == _anchor(0, "100.000").anchor_receipt_sha256
    assert _anchor(0, 100, decision_seconds=8).anchor_receipt_sha256 != anchor.anchor_receipt_sha256

    print("research_market_movement_selftest: PASS")


if __name__ == "__main__":
    run()
