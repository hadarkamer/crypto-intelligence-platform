import magnet_v1


def _evidence(*, support=2, opposition=0, strong=True, early=False, oi=False):
    return {
        "core_supporting_families": support,
        "core_opposing_families": opposition,
        "confirmation": {
            "supporting_families": support,
            "opposing_families": opposition,
            "strong_core": strong,
            "early_shift_opposes": early,
            "oi_opposes": oi,
        },
    }


def _magnet(mq, edge):
    return {"magnet_quality": mq, "liquidity_edge_pct": edge}


def test_confirmation_is_computed_but_mq_below_60_stays_observation():
    out = magnet_v1.evaluate_confirmation(_magnet(55, 30), _evidence())
    assert out["derivatives"]["status"] == "STRONG_CONFIRMED"
    assert out["status"] == "OBSERVATION"


def test_neutral_liquidity_does_not_block_confirmation():
    out = magnet_v1.evaluate_confirmation(_magnet(60, 0), _evidence(strong=False))
    assert out["status"] == "CONFIRMED"


def test_exact_minus_10_liquidity_is_conflict():
    out = magnet_v1.evaluate_confirmation(_magnet(80, -10), _evidence())
    assert out["status"] == "LIQUIDITY_CONFLICT"
    assert out["derivatives"]["status"] == "STRONG_CONFIRMED"


def test_strong_confirmation_requires_all_three_strong_gates():
    assert magnet_v1.evaluate_confirmation(_magnet(75, 20), _evidence())["status"] == "STRONG_CONFIRMED"
    assert magnet_v1.evaluate_confirmation(_magnet(74.99, 20), _evidence())["status"] == "CONFIRMED"
    assert magnet_v1.evaluate_confirmation(_magnet(75, 19.99), _evidence())["status"] == "CONFIRMED"
    assert magnet_v1.evaluate_confirmation(_magnet(75, 20), _evidence(strong=False))["status"] == "CONFIRMED"


def test_derivatives_conflict_vetoes_magnet_confirmation():
    out = magnet_v1.evaluate_confirmation(_magnet(90, 40), _evidence(early=True))
    assert out["derivatives"]["status"] == "CONFLICT"
    assert out["status"] == "NOT_CONFIRMED"


def test_missing_liquidity_does_not_silently_confirm():
    out = magnet_v1.evaluate_confirmation(_magnet(90, None), _evidence())
    assert out["status"] == "LIQUIDITY_UNAVAILABLE"


def test_magnet_side_maps_to_confirmation_price_direction():
    assert magnet_v1.expected_price_direction("UPPER") == "BULLISH"
    assert magnet_v1.expected_price_direction("LOWER") == "BEARISH"
