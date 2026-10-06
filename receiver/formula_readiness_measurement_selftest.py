"""No backdating, false deliveries, or implicit timezone conversions."""
from copy import deepcopy
import formula_readiness_measurement as m


def test_contract():
    def t(seconds):
        return f"2026-09-10T12:{seconds // 60:02d}:{seconds % 60:02d}+00:00"
    cycle = dict(zip(("cycle_started_at_utc", "watch_inputs_ready_at_utc",
                     "dom_collection_returned_at_utc", "derivatives_refresh_returned_at_utc",
                     "snapshot_complete_at_utc", "signal_ready_at_utc"),
                    map(t, (0, 30, 45, 20, 52, 57))))
    cycle["cvd_observations_by_symbol"] = {"BTC": {
        "cvd_observed_at_utc": t(50), "futures_source_close_at_utc": t(0),
        "spot_source_close_at_utc": t(0)}}
    original = deepcopy(cycle)
    result = m.build_observation(cycle, "BTC", t(57), t(59), t(66), "DELIVERED")
    assert result["status"] == "VALID"
    assert result["ready_to_ack_seconds"] == 9
    assert result["ready_entry_minute_utc"] == t(60)
    assert result["delivery_entry_minute_utc"] == t(120)
    assert result["earlier_counterfactual_ready_at_utc"] is None
    assert result["post_inputs_collection_seconds"] == 15
    assert cycle == original
    assert m.next_minute(t(60)) == t(120)
    failed = m.build_observation(cycle, "BTC", t(57), t(59), None, "DELIVERY_FAILED")
    assert failed["status"] == "NOT_DELIVERED" and "delivery_entry_minute_utc" not in failed
    assert m.build_observation(cycle, "XRP", t(57), t(59), t(66), "DELIVERED")["status"] == "INCOMPLETE"
    cycle["signal_ready_at_utc"] = t(10)
    assert m.build_observation(cycle, "BTC", t(57), t(59), t(66), "DELIVERED")["status"] == "INVALID_CLOCK_ORDER"
    try:
        m.next_minute("2026-09-10T12:00:00")
    except ValueError:
        pass
    else:
        raise AssertionError("Naive times must not pass")


if __name__ == "__main__":
    test_contract()
    print("formula readiness measurement selftest passed")
