"""Regression: a failed cumulative rebuild must not publish zero placeholders."""
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import ast
import os
from pathlib import Path
import sqlite3
import tempfile
import uuid
from unittest.mock import patch

import coinglass_flow_foundation as foundation
import coinglass_flow_engine as engine


def exercise(connect, postgres):
    with ExitStack() as stack:
        stack.enter_context(patch.object(foundation, "_use_postgres", return_value=postgres))
        stack.enter_context(patch.object(engine, "_use_postgres", return_value=postgres))
        stack.enter_context(patch.object(foundation, "init_db"))
        if postgres:
            stack.enter_context(patch.object(foundation.psycopg, "connect", side_effect=connect))
        else:
            stack.enter_context(patch.object(foundation.sqlite3, "connect", side_effect=connect))
        with connect() as conn:
            if postgres:
                conn.execute(foundation.POSTGRES_SCHEMA)
            else:
                conn.executescript(foundation.SQLITE_SCHEMA)
            conn.commit()
        base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(days=3)
        rows = {int((base+timedelta(minutes=30*i)).timestamp()*1000):
                (10.0, 3_740_000_010.0 if i == 0 else 20.0, 0.0, 0.0)
                for i in range(4)}
        keys = sorted(rows)
        foundation._store("XRP", "futures", {k: rows[k] for k in keys[:3]})
        actual = engine._load_rows("XRP", "futures")
        assert [r["continuous_cvd"] for r in actual] == [-3_740_000_000, -3_740_000_010, -3_740_000_020]
        repair = foundation._repair_continuous_in_transaction

        def check_isolation(conn, table, symbol):
            assert len(engine._load_rows("XRP", "futures")) == 3
            raise RuntimeError("simulated failure after raw insert")

        with patch.object(foundation, "_repair_continuous_in_transaction", side_effect=check_isolation):
            try:
                foundation._store("XRP", "futures", {keys[3]: rows[keys[3]]})
                raise AssertionError("failure expected")
            except RuntimeError:
                pass
        assert len(engine._load_rows("XRP", "futures")) == 3
        foundation._store("XRP", "futures", {keys[3]: rows[keys[3]]})
        assert engine._load_rows("XRP", "futures")[-1]["continuous_cvd"] == -3_740_000_030

        # Reproduce an old interrupted rebuild. Its fake +3.74B must never score.
        with connect() as conn:
            conn.execute("UPDATE futures_taker_history SET continuous_cum_vol_delta_usd=0")
            conn.commit()
        with patch.object(engine, "_window_state", side_effect=AssertionError("invalid series evaluated")):
            result = engine.analyze_market("XRP", "futures")
        assert not result["available"]
        assert result["overall"]["weighted_score"] == 0
        with patch.object(foundation, "_is_current", return_value=True), patch.object(
            foundation, "_fetch_chunk", side_effect=AssertionError("unnecessary provider download")
        ):
            foundation.backfill_symbol("XRP", "futures")
        actual = engine._load_rows("XRP", "futures")
        assert engine._quality(actual)["continuous_cvd_check"]
        # Interior corruption must be caught even when the last value is correct.
        actual[1]["continuous_cvd"] = 0
        assert not engine._quality(actual)["continuous_cvd_check"]
        with connect() as conn:
            if postgres:
                before = conn.execute("SELECT candle_time,xmin::text FROM futures_taker_history ORDER BY candle_time").fetchall()
                repair(conn, "futures_taker_history", "XRP")
                after = conn.execute("SELECT candle_time,xmin::text FROM futures_taker_history ORDER BY candle_time").fetchall()
                assert before == after, "unchanged history rewritten"
            else:
                before = conn.total_changes
                repair(conn, "futures_taker_history", "XRP")
                assert conn.total_changes == before


def main():
    real_sqlite_connect = sqlite3.connect
    with tempfile.TemporaryDirectory() as folder:
        db = str(Path(folder)/"cvd.db")
        exercise(lambda *a, **kw: real_sqlite_connect(db), False)
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("RESEARCH_TEST_POSTGRES_URL")
    if dsn:
        import psycopg
        from psycopg.rows import dict_row
        real_connect = psycopg.connect
        schema = "cvd_atomic_"+uuid.uuid4().hex
        with real_connect(dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{schema}"')
        try:
            exercise(lambda *a, **kw: real_connect(dsn, options=f"-c search_path={schema}", row_factory=dict_row), True)
        finally:
            with real_connect(dsn, autocommit=True) as conn:
                conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        print("PostgreSQL atomic CVD checks passed")
    # Exercise the real renderer without starting the bot's module-level workers.
    tree = ast.parse(Path(__file__).with_name("main.py").read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_regime_block")
    scope = {"Dict": dict, "Any": object}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "main.py", "exec"), scope)
    text = scope["_regime_block"]({"market_regime": {"data_quality_status": "READ_ERROR"}})
    assert "קריאת הנתונים נכשלה" in text and "60" not in text
    print("Atomic CVD and read-error regression checks passed")


if __name__ == "__main__":
    main()
