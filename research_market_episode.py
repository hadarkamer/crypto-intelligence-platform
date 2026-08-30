"""Outcome-blind market-episode grouping for independent formula evidence.

One broad market move can create simultaneous matches in several symbols and
at several adjacent 30-minute anchors.  Those rows remain fully auditable, but
they must not be counted as independent statistical proof.  This module owns
the deterministic grouping policy shared by historical discovery and future
Shadow readiness. Episode keys are formula-local because the first matching
forecast start anchors the episode; callers must never add sample counts across
different formulas or horizons as if their episode keys proved independence.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from statistics import median
from typing import Any, Dict, Mapping, Sequence


POLICY_VERSION = (
    "market-episode-v4-fixed-window-directional-price-reset-outcome-blind"
)
MINIMUM_EPISODE_MINUTES = 1440


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def row_time_utc(row: Mapping[str, Any]) -> datetime:
    """Return the frozen forecast start used by the episode contract."""

    event = row.get("event") if isinstance(row.get("event"), Mapping) else row
    return _utc(
        event.get("forecast_start_time_utc")
        or row.get("forecast_start_time_utc")
        or event.get("alert_time_utc")
        or row.get("alert_time_utc")
        or event.get("decision_anchor_time_utc")
        or row.get("decision_anchor_time_utc")
    )


# Private alias retained so older internal references stay compact.
_row_time = row_time_utc


def _row_id(row: Mapping[str, Any]) -> int:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else row
    try:
        return int(event.get("event_id") or row.get("event_id") or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _row_direction(row: Mapping[str, Any]) -> str:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else row
    return str(event.get("direction") or row.get("direction") or "").upper()


def _row_symbol(row: Mapping[str, Any]) -> str:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else row
    return str(event.get("symbol") or row.get("symbol") or "").upper()


def _row_price(row: Mapping[str, Any]) -> float | None:
    event = row.get("event") if isinstance(row.get("event"), Mapping) else row
    for value in (
        event.get("current_price"),
        row.get("current_price"),
        event.get("reference_price"),
        row.get("reference_price"),
    ):
        finite = _finite(value)
        if finite is not None and finite > 0.0:
            return finite
    return None


def _directional_price_reset(
    episode: Mapping[str, Any], cohort: Sequence[Mapping[str, Any]]
) -> bool:
    """Require a decision-time reset before one trend can prove itself again."""

    references = episode.get("reference_prices") or {}
    direction = str(episode.get("direction") or "").upper()
    comparisons = []
    for row in cohort:
        symbol = _row_symbol(row)
        current = _row_price(row)
        reference = _finite(references.get(symbol))
        if symbol and reference is not None:
            if current is None:
                return False
            comparisons.append((current, reference))
    if not comparisons:
        return False
    if direction == "LONG":
        return all(current <= reference for current, reference in comparisons)
    if direction == "SHORT":
        return all(current >= reference for current, reference in comparisons)
    return False


def episode_minutes(horizon_minutes: int) -> int:
    """Return the minimum separation needed for a new evidence episode."""

    if isinstance(horizon_minutes, bool):
        raise ValueError("horizon_minutes must be a positive integer")
    horizon = int(horizon_minutes)
    if horizon <= 0:
        raise ValueError("horizon_minutes must be a positive integer")
    return max(MINIMUM_EPISODE_MINUTES, horizon)


def group_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    presorted: bool = False,
) -> list[Dict[str, Any]]:
    """Group matches into immutable, outcome-blind market episodes.

    Membership reads only forecast-start timestamps and stable identifiers. It never
    reads a formula result, price outcome, MFE, MAE or success label. The first
    row starts one fixed half-open window. Matches inside that window remain
    auditable members but never extend it. At or after the immutable end, a new
    episode opens only when comparable symbols have returned to/past their
    episode-start price against the formula direction. A later non-reset match
    is retained only as correlated diagnostic evidence; it cannot reopen a
    finalized episode, change its key, or count as another evidence unit.
    """

    fixed_episode_minutes = episode_minutes(horizon_minutes)
    copied = [dict(row) for row in rows]
    # Validate every timestamp explicitly before sorting.  Python does not
    # invoke a sort key for a one-item list, which otherwise allowed a single
    # malformed row to enter an episode unnoticed.
    for row in copied:
        _row_time(row)
    directions = {_row_direction(row) for row in copied if _row_direction(row)}
    if len(directions) > 1:
        raise ValueError(
            "market-episode callers must partition the full batch by formula direction"
        )
    sort_key = lambda row: (_row_time(row), _row_id(row))
    if presorted and all(
        sort_key(left) <= sort_key(right)
        for left, right in zip(copied, copied[1:])
    ):
        ordered = copied
    else:
        # A public result must not depend on input iteration order.  The
        # presorted flag is therefore an optimization hint, never a contract.
        ordered = sorted(copied, key=sort_key)
    groups: list[Dict[str, Any]] = []
    cohorts: list[tuple[datetime, list[Dict[str, Any]]]] = []
    for row in ordered:
        timestamp = _row_time(row)
        if not cohorts or cohorts[-1][0] != timestamp:
            cohorts.append((timestamp, []))
        cohorts[-1][1].append(row)

    current: Dict[str, Any] | None = None
    for timestamp, cohort in cohorts:
        opens_new = current is None
        if current is not None and timestamp >= current["end_time_utc"]:
            opens_new = _directional_price_reset(current, cohort)
            if not opens_new:
                current["correlated_rows"].extend(cohort)
                continue
        if opens_new:
            start = timestamp
            direction = _row_direction(cohort[0])
            current = {
                "episode_key": "",
                "start_time_utc": start,
                "end_time_utc": start
                + timedelta(minutes=fixed_episode_minutes),
                "direction": direction,
                "rows": [],
                "correlated_rows": [],
                "reference_prices": {
                    symbol: price
                    for row in cohort
                    for symbol, price in [(_row_symbol(row), _row_price(row))]
                    if symbol and price is not None
                },
            }
            groups.append(current)
        for row in cohort:
            row_direction = _row_direction(row)
            if (
                current["direction"]
                and row_direction
                and row_direction != current["direction"]
            ):
                raise ValueError(
                    "market-episode callers must partition rows by formula direction"
                )
            current["rows"].append(row)
    for episode in groups:
        episode["evidence_rows"] = [
            row
            for row in episode["rows"]
            if _row_time(row) == episode["start_time_utc"]
        ]
        raw_key = "|".join(
            (
                POLICY_VERSION,
                str(fixed_episode_minutes),
                str(episode.get("direction") or "UNKNOWN"),
                episode["start_time_utc"].isoformat(),
                episode["end_time_utc"].isoformat(),
                json.dumps(
                    episode.get("reference_prices") or {},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        )
        episode["episode_key"] = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return groups


def _group_control_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    presorted: bool = False,
) -> list[Dict[str, Any]]:
    """Partition eligible controls into fixed, non-overlapping time cohorts.

    Controls are formula nonmatches, so a dense neutral anchor stream must not
    become one perpetually open inactivity episode. Their cohorts therefore use
    fixed outcome-blind windows and never consult price resets or outcomes.
    """

    fixed_episode_minutes = episode_minutes(horizon_minutes)
    copied = [dict(row) for row in rows]
    for row in copied:
        _row_time(row)
    directions = {_row_direction(row) for row in copied if _row_direction(row)}
    if len(directions) > 1:
        raise ValueError(
            "market-episode callers must partition controls by formula direction"
        )
    sort_key = lambda row: (_row_time(row), _row_id(row))
    ordered = (
        copied
        if presorted
        and all(
            sort_key(left) <= sort_key(right)
            for left, right in zip(copied, copied[1:])
        )
        else sorted(copied, key=sort_key)
    )
    groups: list[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None
    for row in ordered:
        timestamp = _row_time(row)
        if current is None or timestamp >= current["end_time_utc"]:
            start = timestamp
            current = {
                "episode_key": "",
                "start_time_utc": start,
                "end_time_utc": start
                + timedelta(minutes=fixed_episode_minutes),
                "direction": _row_direction(row),
                "rows": [],
                "correlated_rows": [],
                "reference_prices": {},
            }
            groups.append(current)
        current["rows"].append(row)
    for episode in groups:
        episode["evidence_rows"] = [
            row
            for row in episode["rows"]
            if _row_time(row) == episode["start_time_utc"]
        ]
        raw_key = "|".join(
            (
                POLICY_VERSION,
                "CONTROL",
                str(fixed_episode_minutes),
                str(episode.get("direction") or "UNKNOWN"),
                episode["start_time_utc"].isoformat(),
                episode["end_time_utc"].isoformat(),
            )
        )
        episode["episode_key"] = hashlib.sha256(
            raw_key.encode("utf-8")
        ).hexdigest()
    return groups


def finalization_time(
    episode: Mapping[str, Any], *, horizon_minutes: int
) -> datetime:
    """Earliest time membership is closed and the evidence anchor matured."""

    evidence = episode_evidence_rows(episode)
    evidence_maturity = _row_time(evidence[0]) + timedelta(
        minutes=int(horizon_minutes)
    )
    return max(_utc(episode["end_time_utc"]), evidence_maturity)


def is_finalized(
    episode: Mapping[str, Any], *, horizon_minutes: int, as_of_utc: Any
) -> bool:
    """Whether membership is closed and every possible member could mature."""

    return _utc(as_of_utc) >= finalization_time(
        episode, horizon_minutes=horizon_minutes
    )


def partition_finalized(
    episodes: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    as_of_utc: Any,
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    """Split episodes into immutable statistical evidence and open monitoring."""

    finalized: list[Dict[str, Any]] = []
    open_episodes: list[Dict[str, Any]] = []
    for episode in episodes:
        target = (
            finalized
            if is_finalized(
                episode,
                horizon_minutes=horizon_minutes,
                as_of_utc=as_of_utc,
            )
            else open_episodes
        )
        target.append(dict(episode))
    return finalized, open_episodes


def episode_evidence_rows(
    episode: Mapping[str, Any],
) -> list[Dict[str, Any]]:
    """Return the outcome-blind earliest decision cohort for one episode.

    Later rows remain auditable members but never influence the statistical
    result. This guarantees that consecutive episode outcome windows do not
    overlap for the supported horizons, while simultaneous symbols at the
    earliest anchor still share total evidence weight one.
    """

    cached = episode.get("evidence_rows")
    if isinstance(cached, Sequence) and not isinstance(cached, (str, bytes)):
        return [dict(row) for row in cached]
    rows = [dict(row) for row in episode.get("rows") or []]
    if not rows:
        return []
    earliest = min(_row_time(row) for row in rows)
    return [row for row in rows if _row_time(row) == earliest]


def _overlap(
    left_start: datetime,
    left_end: datetime,
    right_start: datetime,
    right_end: datetime,
) -> bool:
    return left_start < right_end and right_start < left_end


def select_independent(
    matches: Sequence[Mapping[str, Any]],
    controls: Sequence[Mapping[str, Any]],
    *,
    horizon_minutes: int,
    presorted: bool = False,
) -> Dict[str, Any]:
    """Choose deterministic match/control representatives without outcomes."""

    directions = {
        _row_direction(row)
        for row in (*matches, *controls)
        if _row_direction(row)
    }
    if len(directions) > 1:
        raise ValueError(
            "market-episode callers must partition matches and controls by direction"
        )

    match_episodes = group_rows(
        matches, horizon_minutes=horizon_minutes, presorted=presorted
    )
    retained_matches = [
        row
        for episode in match_episodes
        for row in episode_evidence_rows(episode)
    ]
    match_member_ids = {
        _row_id(row) for episode in match_episodes for row in episode["rows"]
    }
    match_member_ids.update(
        _row_id(row)
        for episode in match_episodes
        for row in episode.get("correlated_rows") or []
    )
    match_evidence_ids = {
        _row_id(row)
        for episode in match_episodes
        for row in episode_evidence_rows(episode)
    }
    eligible_controls = []
    excluded_controls = []
    fixed_episode_minutes = episode_minutes(horizon_minutes)
    horizon = timedelta(minutes=fixed_episode_minutes)
    copied_controls = [dict(item) for item in controls]
    control_sort_key = lambda item: (_row_time(item), _row_id(item))
    ordered_controls = (
        copied_controls
        if presorted
        and all(
            control_sort_key(left) <= control_sort_key(right)
            for left, right in zip(copied_controls, copied_controls[1:])
        )
        else sorted(copied_controls, key=control_sort_key)
    )
    match_intervals: list[tuple[datetime, datetime]] = []
    for row in sorted(
        (dict(item) for item in matches),
        key=lambda item: (_row_time(item), _row_id(item)),
    ):
        start = _row_time(row)
        end = start + horizon
        if match_intervals and start < match_intervals[-1][1]:
            previous_start, previous_end = match_intervals[-1]
            match_intervals[-1] = (previous_start, max(previous_end, end))
        else:
            match_intervals.append((start, end))
    match_index = 0
    for row in ordered_controls:
        start = _row_time(row)
        end = start + horizon
        while (
            match_index < len(match_intervals)
            and match_intervals[match_index][1] <= start
        ):
            match_index += 1
        overlaps_match = (
            match_index < len(match_intervals)
            and _overlap(
                start,
                end,
                match_intervals[match_index][0],
                match_intervals[match_index][1],
            )
        )
        if overlaps_match:
            excluded_controls.append(_row_id(row))
        else:
            eligible_controls.append(row)
    control_episodes = _group_control_rows(
        eligible_controls,
        horizon_minutes=horizon_minutes,
        presorted=True,
    )
    retained_controls = [
        row
        for episode in control_episodes
        for row in episode_evidence_rows(episode)
    ]
    control_evidence_ids = {
        str(episode["episode_key"]): {
            _row_id(row) for row in episode_evidence_rows(episode)
        }
        for episode in control_episodes
    }
    return {
        "rows": retained_matches + retained_controls,
        "matches": retained_matches,
        "controls": retained_controls,
        "match_episodes": match_episodes,
        "control_episodes": control_episodes,
        "excluded_match_event_ids": sorted(
            match_member_ids - match_evidence_ids
        ),
        "excluded_control_event_ids": sorted(
            set(excluded_controls)
            | {
                _row_id(row)
                for episode in control_episodes
                for row in episode["rows"]
                if _row_id(row)
                not in control_evidence_ids[str(episode["episode_key"])]
            }
        ),
        "policy_version": POLICY_VERSION,
        "identity_scope": (
            "formula-local; cross-formula and cross-horizon sample counts "
            "must not be added"
        ),
    }


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def aggregate_metric_episode(
    rows: Sequence[Mapping[str, Any]],
    *,
    episode_key: str,
    episode_start_utc: datetime,
    episode_end_utc: datetime | None = None,
) -> Dict[str, Any]:
    """Aggregate completed member outcomes after outcome-blind membership.

    The episode receives total statistical weight one. Numeric path measures
    use the member median; success and target booleans use a conservative
    majority (a tie is false). This avoids selecting a lucky symbol merely
    because its database identifier happened to sort first.
    """

    if not rows:
        raise ValueError("market episode has no metric rows")
    observed_members = [dict(row) for row in rows]
    if not observed_members:
        raise ValueError("market episode has no metric rows")
    earliest = min(_row_time(row) for row in observed_members)
    members = [
        row for row in observed_members if _row_time(row) == earliest
    ]
    labels = [
        dict(row.get("outcome_label") or {})
        for row in members
        if isinstance(row.get("outcome_label"), Mapping)
    ]
    if len(labels) != len(members):
        raise ValueError("market episode contains a missing outcome label")
    for label in labels:
        status = str(label.get("first_touch_status") or "").upper()
        success = label.get("path_success")
        if not (
            (status == "HIT" and success is True)
            or (status == "MISS" and success is False)
        ):
            raise ValueError(
                "market episode evidence cohort is not terminal and complete"
            )
        if _finite(label.get("mfe_pct")) is None or _finite(label.get("mae_pct")) is None:
            raise ValueError(
                "market episode evidence cohort is missing finite MFE/MAE"
            )

    numeric_keys = (
        "directional_return_pct",
        "mfe_pct",
        "mae_pct",
        "time_to_first_progress_seconds",
        "time_to_mfe_seconds",
        "target_progress_ratio",
        "session_active_ratio",
        "session_weekend_ratio",
    )
    aggregated: Dict[str, Any] = {}
    for key in numeric_keys:
        values = [value for label in labels if (value := _finite(label.get(key))) is not None]
        aggregated[key] = median(values) if values else None

    def majority(key: str) -> bool | None:
        values = [label.get(key) for label in labels if isinstance(label.get(key), bool)]
        if not values:
            return None
        return sum(bool(value) for value in values) > len(values) / 2.0

    path_success = majority("path_success")
    aggregated["path_success"] = path_success
    aggregated["first_touch_status"] = (
        "HIT" if path_success is True else "MISS" if path_success is False else None
    )
    aggregated["target_reached"] = majority("target_reached")
    ambiguity = [
        label.get("qualifying_candle_order_ambiguous")
        for label in labels
        if isinstance(label.get("qualifying_candle_order_ambiguous"), bool)
    ]
    aggregated["qualifying_candle_order_ambiguous"] = (
        any(ambiguity) if ambiguity else None
    )
    paired_edges = [
        mfe - mae
        for label in labels
        for mfe, mae in [
            (_finite(label.get("mfe_pct")), _finite(label.get("mae_pct")))
        ]
        if mfe is not None and mae is not None
    ]
    member_mae_values = [
        mae
        for label in labels
        for mae in [_finite(label.get("mae_pct"))]
        if mae is not None
    ]
    aggregated["adverse_tail_mae_pct"] = (
        max(member_mae_values) if member_mae_values else None
    )
    aggregated["adverse_tail_policy"] = (
        "maximum member MAE in the outcome-blind earliest forecast cohort"
    )
    aggregated["paired_favorable_minus_adverse_pct"] = (
        median(paired_edges) if paired_edges else None
    )
    aggregated["favorable_dominance"] = (
        sum(edge > 0.0 for edge in paired_edges) > len(paired_edges) / 2.0
        if paired_edges
        else None
    )
    first_label = labels[0]
    aggregated["horizon_minutes"] = first_label.get("horizon_minutes")
    active_ratio = aggregated.get("session_active_ratio")
    aggregated["session_composition"] = (
        "ACTIVE_ONLY"
        if active_ratio is not None and active_ratio >= 1.0 - 1e-9
        else "WEEKEND_ONLY"
        if active_ratio is not None and active_ratio <= 1e-9
        else "MIXED"
    )
    aggregated["session_segments"] = []
    width_candidates = []
    for label in labels:
        reference = label.get("movement_width_reference")
        if not isinstance(reference, Mapping):
            continue
        factor = _finite(reference.get("floor_scale_factor"))
        if factor is not None:
            width_candidates.append((factor, dict(reference)))
    aggregated["movement_width_reference"] = (
        max(width_candidates, key=lambda item: item[0])[1]
        if width_candidates
        else first_label.get("movement_width_reference")
    )

    first_event = dict(members[0].get("event") or {})
    member_ids = sorted(_row_id(row) for row in observed_members)
    evidence_member_ids = sorted(_row_id(row) for row in members)
    evidence_member_symbols = sorted(
        {
            str((row.get("event") or {}).get("symbol") or "").upper()
            for row in members
            if str((row.get("event") or {}).get("symbol") or "").strip()
        }
    )
    observed_member_symbols = sorted(
        {
            str((row.get("event") or {}).get("symbol") or "").upper()
            for row in observed_members
            if str((row.get("event") or {}).get("symbol") or "").strip()
        }
    )
    evidence_member_event_types = sorted(
        {
            str((row.get("event") or {}).get("event_type") or "")
            for row in members
            if str((row.get("event") or {}).get("event_type") or "").strip()
        }
    )
    observed_member_event_types = sorted(
        {
            str((row.get("event") or {}).get("event_type") or "")
            for row in observed_members
            if str((row.get("event") or {}).get("event_type") or "").strip()
        }
    )
    horizon_minutes = int(_finite(aggregated.get("horizon_minutes")) or 1)
    membership_end = _utc(episode_end_utc) if episode_end_utc else (
        _utc(episode_start_utc)
        + timedelta(minutes=episode_minutes(horizon_minutes))
    )
    maturity_time = max(
        membership_end,
        _utc(episode_start_utc) + timedelta(minutes=horizon_minutes),
    )
    first_event.update(
        {
            "event_id": evidence_member_ids[0],
            "alert_time_utc": episode_start_utc,
            "forecast_start_time_utc": episode_start_utc,
            "symbol": "MARKET_EPISODE",
            "market_episode_key": episode_key,
            "market_episode_start_time_utc": episode_start_utc,
            "market_episode_end_time_utc": membership_end,
            "market_episode_finalization_time_utc": maturity_time,
            "market_episode_member_event_ids": member_ids,
            "market_episode_evidence_event_ids": evidence_member_ids,
            "market_episode_member_symbols": evidence_member_symbols,
            "market_episode_member_event_types": evidence_member_event_types,
            "market_episode_evidence_member_symbols": evidence_member_symbols,
            "market_episode_evidence_member_event_types": (
                evidence_member_event_types
            ),
            "market_episode_observed_member_symbols": observed_member_symbols,
            "market_episode_observed_member_event_types": (
                observed_member_event_types
            ),
        }
    )
    return {
        **members[0],
        "event": first_event,
        "outcome_label": aggregated,
        "market_episode_key": episode_key,
        "market_episode_member_count": len(observed_members),
        "market_episode_evidence_member_count": len(members),
    }


def annotate_rows(
    rows: Sequence[Mapping[str, Any]], *, horizon_minutes: int
) -> list[Dict[str, Any]]:
    """Return copies carrying a stable outcome-blind episode identity."""

    annotated: list[Dict[str, Any]] = []
    for episode in group_rows(rows, horizon_minutes=horizon_minutes):
        for row in episode["rows"]:
            annotated.append(
                {
                    **dict(row),
                    "market_episode_key": episode["episode_key"],
                    "market_episode_start_utc": episode["start_time_utc"],
                    "market_episode_end_utc": episode["end_time_utc"],
                    "market_episode_policy_version": POLICY_VERSION,
                }
            )
    return annotated
