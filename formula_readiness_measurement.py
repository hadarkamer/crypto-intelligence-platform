"""Observation-only timing contract for the unchanged MP65/CVD-short formula.

No network, scoring, state transitions or delivery calls. A snapshot/cache read
proves availability by that time, never the provider's first arrival time.
"""
from datetime import datetime, timedelta, timezone

VERSION = "formula-readiness-v1"


def utc(value):
    if value is None:
        return None
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.utcoffset() is None:
        raise ValueError("Timing requires an explicit timezone")
    return value.astimezone(timezone.utc)


def next_minute(value):
    return (utc(value).replace(second=0, microsecond=0) + timedelta(minutes=1)).isoformat()


def build_observation(cycle, symbol, decision, attempted, delivered, delivery_status):
    observed = (cycle.get("cvd_observations_by_symbol") or {}).get(symbol, {})
    names = ("cycle_started_at_utc", "watch_inputs_ready_at_utc",
             "dom_collection_returned_at_utc", "derivatives_refresh_returned_at_utc",
             "snapshot_complete_at_utc", "signal_ready_at_utc")
    clocks = {key: utc(cycle.get(key)) for key in names}
    clocks.update({key: utc(observed.get(key)) for key in (
        "cvd_observed_at_utc", "futures_source_close_at_utc", "spot_source_close_at_utc")})
    clocks.update(decision_at_utc=utc(decision), attempted_at_utc=utc(attempted),
                  delivery_ack_at_utc=utc(delivered))
    result = {"version": VERSION, "status": "INCOMPLETE",
              "ready_basis": "FIRST_OBSERVED_COMPLETED_FORMULA_EVALUATION",
              "cvd_basis": "SNAPSHOT_READ_OR_CACHE_READ_NOT_PROVIDER_ARRIVAL",
              "delivery_basis": "DEDICATED_MESSAGE_API_ACK_NOT_USER_RECEIPT",
              "provider_first_arrival_known": False,
              "earlier_counterfactual_ready_at_utc": None,
              **{key: value.isoformat() if value else None for key, value in clocks.items()}}
    required = (*names, "cvd_observed_at_utc", "futures_source_close_at_utc",
                "spot_source_close_at_utc", "decision_at_utc", "attempted_at_utc")
    if any(clocks[key] is None for key in required):
        return result
    ready = clocks["signal_ready_at_utc"]
    ordered = (
        (clocks["cycle_started_at_utc"], clocks["watch_inputs_ready_at_utc"],
         clocks["dom_collection_returned_at_utc"], clocks["snapshot_complete_at_utc"], ready),
        (clocks["cycle_started_at_utc"], clocks["derivatives_refresh_returned_at_utc"],
         clocks["cvd_observed_at_utc"], clocks["snapshot_complete_at_utc"], ready,
         clocks["decision_at_utc"], clocks["attempted_at_utc"]),
        (clocks["futures_source_close_at_utc"], clocks["cvd_observed_at_utc"]),
        (clocks["spot_source_close_at_utc"], clocks["cvd_observed_at_utc"]),
    )
    if any(a > b for seq in ordered for a, b in zip(seq, seq[1:])):
        result["status"] = "INVALID_CLOCK_ORDER"
        return result
    result["ready_entry_minute_utc"] = next_minute(ready)
    result["post_inputs_collection_seconds"] = (
        clocks["dom_collection_returned_at_utc"] - clocks["watch_inputs_ready_at_utc"]
    ).total_seconds()
    # This interval includes archive work but is not proven removable latency.
    ack = clocks["delivery_ack_at_utc"]
    if delivery_status != "DELIVERED" or ack is None:
        result["status"] = "NOT_DELIVERED"
    elif ack < clocks["attempted_at_utc"]:
        result["status"] = "INVALID_CLOCK_ORDER"
    else:
        result.update(status="VALID", delivery_entry_minute_utc=next_minute(ack),
                      ready_to_ack_seconds=(ack-ready).total_seconds())
    return result
