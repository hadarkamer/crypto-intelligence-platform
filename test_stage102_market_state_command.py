from pathlib import Path


def test_market_state_command_is_exposed_and_documented():
    source = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
    assert "async def market_state_cmd(" in source
    assert 'CommandHandler("market_state", market_state_cmd)' in source
    assert "/market_state BTC [LONG|SHORT]" in source
