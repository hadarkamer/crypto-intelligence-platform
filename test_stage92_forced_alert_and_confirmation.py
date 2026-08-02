import alert_engine
from test_stage57_calculations import btc_rows


def test_forced_alert_uses_requested_side_without_changing_formula():
    rows = btc_rows()
    automatic = alert_engine.build_opportunities(rows, limit=100)
    forced = alert_engine.build_opportunities(
        rows, limit=100, forced_symbol="BTC", forced_side="SHORT"
    )
    assert forced
    assert all(item["side"] == "SHORT" for item in forced if item["symbol"] == "BTC")

    auto_by_tf = {item["timeframe"]: item for item in automatic}
    forced_by_tf = {item["timeframe"]: item for item in forced}
    for timeframe, item in forced_by_tf.items():
        auto = auto_by_tf[timeframe]
        if auto["side"] == "SHORT":
            assert item["score"] == auto["score"]
        else:
            assert item["score"] == auto["opposite_score"]
        assert item["opposite_score"] is not None

