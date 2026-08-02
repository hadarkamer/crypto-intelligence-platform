from pathlib import Path
import coinglass_flow_foundation as flow


def test_default_flow_collection_interval_is_five_minutes(monkeypatch):
    assert flow.FLOW_COLLECTION_INTERVAL_MINUTES == max(5, int(__import__('os').getenv('FLOW_COLLECTION_INTERVAL_MINUTES', '5')))


def test_display_uses_candle_close_and_actual_age():
    source = Path('main.py').read_text()
    assert 'candle_close = stamp + timedelta(minutes=30)' in source
    assert 'גיל בפועל' in source


def test_flow_loop_compensates_for_runtime():
    source = Path('main.py').read_text()
    assert 'cycle_started = time.monotonic()' in source
    assert 'interval_seconds - elapsed' in source
