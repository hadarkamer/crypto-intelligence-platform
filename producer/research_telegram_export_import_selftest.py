"""Network-free date-window and dedup identity checks for Telegram imports."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import research_telegram_export_import as importer


def run() -> None:
    payload = {
        "name": "research chat",
        "messages": [
            {
                "id": 1,
                "type": "message",
                "date": "2026-09-03T20:59:59+00:00",
                "text": "BTC LONG MAX PAIN",
            },
            {
                "id": 2,
                "type": "message",
                "date": "2026-09-03T21:00:00+00:00",
                "text": "ETH SHORT MAGNET",
            },
            {
                "id": 3,
                "type": "message",
                "date": "2026-09-06T16:00:00+00:00",
                "text": "SOL LONG SPOT CVD",
            },
        ],
    }
    with TemporaryDirectory() as directory:
        path = Path(directory) / "result.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        summary = importer.import_export(
            path,
            since="2026-09-04T00:00:00+03:00",
            until="2026-09-06T19:00:00+03:00",
        )
    assert summary["parsed_alert_messages"] == 3
    assert summary["candidate_messages"] == 2
    assert summary["excluded_before_window"] == 1
    assert summary["excluded_after_window"] == 0
    try:
        importer.import_export(Path("missing.json"), apply=True)
    except FileNotFoundError:
        pass
    print("Telegram export date-window self-test: PASS")


if __name__ == "__main__":
    run()
