from pathlib import Path


def test_hype_is_forced_into_live_oi_collection_symbol_set():
    source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
    expected = 'symbols = sorted(set(_latest_active_symbols()) | {"HYPE"})'
    assert expected in source
