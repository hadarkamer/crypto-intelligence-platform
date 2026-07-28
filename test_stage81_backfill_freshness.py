from datetime import datetime, timezone

import coinglass_history_backfill as history


def test_backfill_completion_timestamp_persists(tmp_path, monkeypatch):
    db_path = tmp_path / "history.db"
    monkeypatch.setattr(history, "DATABASE_URL", "")
    monkeypatch.setattr(history, "DB_PATH", str(db_path))
    completed = datetime(2026, 7, 28, 12, 30, tzinfo=timezone.utc)

    history.record_backfill_run("manual", 8, 8, completed)
    last = history.last_backfill_run()

    assert last is not None
    assert last["source"] == "manual"
    assert int(last["ok_count"]) == 8
    assert int(last["total_count"]) == 8
    assert str(last["completed_at"]).startswith("2026-07-28T12:30:00")
