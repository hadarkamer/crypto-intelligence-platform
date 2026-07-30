from pathlib import Path

import alert_engine


def test_proximity_points_are_zero_outside_scoring_band():
    assert alert_engine._target_proximity_points(0.50, 4.0) == 0.0
    assert alert_engine._target_proximity_points(4.50, 4.0) == 0.0


def test_engine_no_longer_filters_outside_distance_band():
    source = Path("alert_engine.py").read_text()
    assert 'if selected_distance < 0.8 or selected_distance > selected_allowed:\n            continue' not in source
    assert 'receive 0 proximity points' in source


def test_display_layer_keeps_all_nonnegative_distances():
    source = Path("main.py").read_text()
    assert 'return distance >= 0.0' in source
    assert 'return distance >= MIN_DISPLAY_DISTANCE_PCT' not in source
