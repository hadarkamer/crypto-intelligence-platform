from pathlib import Path


def _source():
    return Path(__file__).with_name("main.py").read_text(encoding="utf-8")


def test_top8_symbols_are_exact_fixed_list():
    source = _source()
    assert 'TOP8_SYMBOLS = {"BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP"}' in source


def test_top8_commands_are_registered():
    source = _source()
    assert 'CommandHandler("alerts_top8", alert_check_top8)' in source
    assert 'CommandHandler("watch_on_top8", watch_on_top8)' in source


def test_top8_filter_is_applied_to_alerts_and_watch():
    source = _source()
    assert 'top8_all_items = _filter_top8_items(all_items)' in source
    assert 'if top8_only:\n            all_items = _filter_top8_items(all_items)' in source
